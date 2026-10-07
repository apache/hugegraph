/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hugegraph.backend.store.hstore;

import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.LinkedHashSet;
import java.nio.ByteBuffer;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hugegraph.id.Id.IdType;
import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.backend.BinaryId;
import org.apache.hugegraph.backend.serializer.BinaryBackendEntry;
import org.apache.hugegraph.backend.serializer.BinarySerializer;
import org.apache.hugegraph.serializer.BytesBuffer;
import org.apache.hugegraph.serializer.OlapKey;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.config.ConfigOption;
import org.apache.hugegraph.iterator.CIter;
import org.apache.hugegraph.struct.schema.PropertyKey;
import org.apache.hugegraph.struct.schema.VertexLabel;
import org.apache.hugegraph.structure.HugeVertex;
import org.apache.hugegraph.type.define.DataType;
import org.apache.hugegraph.type.define.WriteType;
import org.apache.hugegraph.id.IdGenerator;
import org.apache.hugegraph.backend.page.PageInfo;
import org.apache.hugegraph.backend.page.PageState;
import org.apache.hugegraph.query.Condition;
import org.apache.hugegraph.query.ConditionQuery;
import org.apache.hugegraph.query.IdPrefixQuery;
import org.apache.hugegraph.query.IdRangeQuery;
import org.apache.hugegraph.query.Query;
import org.apache.hugegraph.query.IdQuery;
import org.apache.hugegraph.backend.store.BackendEntry;
import org.apache.hugegraph.backend.BackendColumn;
import org.apache.hugegraph.backend.store.BackendEntry.BackendColumnIterator;
import org.apache.hugegraph.backend.store.BackendEntryIterator;
import org.apache.hugegraph.store.HgOwnerKey;
import org.apache.hugegraph.store.client.util.HgStoreClientConst;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.type.define.Directions;
import org.apache.hugegraph.type.define.GraphMode;
import org.apache.hugegraph.type.define.HugeKeys;
import org.junit.Assert;
import org.junit.Test;

public class HstoreTableTest {

    @Test
    public void testHstoreDoesNotAdvertiseInputIdOrdering() {
        Assert.assertFalse(new HstoreFeatures()
                                   .supportsQuerySortByInputIds());
    }

    @Test
    public void testRangeIndexPageStateUsesNextUnreadPhysicalKey() {
        Query query = new Query(HugeType.RANGE_INT_INDEX);
        query.page("");
        query.limit(1L);

        BackendEntryIterator iterator = HstoreTable.newEntryIterator(
                new TestColumnIterator(1, 2), query);

        Assert.assertTrue(iterator.hasNext());
        BackendEntry entry = iterator.next();
        Assert.assertArrayEquals(keyBytes(1), entry.id().asBytes());

        PageState pageState = PageInfo.pageState(iterator);
        Assert.assertArrayEquals(keyBytes(2), pageState.position());
        Assert.assertEquals(1L, pageState.total());
    }

    @Test
    public void testRangeIndexPagingUsesPagePositionAsInclusiveScanStart() {
        byte[] originalStart = keyBytes(1);
        byte[] pagePosition = keyBytes(2);
        IdRangeQuery query = rangeIndexQuery();

        query.page("");
        Assert.assertArrayEquals(originalStart,
                                 HstoreTable.rangeIndexScanStart(
                                         query, originalStart));

        query.page(new PageState(pagePosition, 0, 1).toString());
        Assert.assertArrayEquals(pagePosition,
                                 HstoreTable.rangeIndexScanStart(
                                         query, originalStart));
        int type = HstoreTable.rangeIndexScanType(
                query, HstoreSessions.Session.SCAN_GT_BEGIN |
                       HstoreSessions.Session.SCAN_LT_END);
        Assert.assertTrue(HstoreSessions.Session.matchScanType(
                HstoreSessions.Session.SCAN_GTE_BEGIN, type));
        Assert.assertTrue(HstoreSessions.Session.matchScanType(
                HstoreSessions.Session.SCAN_LT_END, type));
    }

    @Test
    public void testOrderedRangeScanIsScopedToOrderSensitiveIndexes() {
        IdRangeQuery query = rangeIndexQuery();
        Assert.assertFalse(HstoreTable.shouldUseOrderedRangeScan(query));

        query.limit(10L);
        Assert.assertTrue(HstoreTable.shouldUseOrderedRangeScan(query));

        query = rangeIndexQuery();
        query.offset(1L);
        Assert.assertTrue(HstoreTable.shouldUseOrderedRangeScan(query));

        query = rangeIndexQuery();
        query.page("");
        Assert.assertTrue(HstoreTable.shouldUseOrderedRangeScan(query));

        query = new IdRangeQuery(HugeType.VERTEX, null,
                                 IdGenerator.of(keyBytes(1), IdType.STRING),
                                 true,
                                 IdGenerator.of(keyBytes(9), IdType.STRING),
                                 false);
        query.limit(10L);
        Assert.assertFalse(HstoreTable.shouldUseOrderedRangeScan(query));
    }

    @Test
    public void testRangeScanBudgetIncludesOneLookaheadRecord() {
        IdRangeQuery query = rangeIndexQuery();
        Assert.assertEquals(HgStoreClientConst.NO_LIMIT,
                            HstoreTable.rangeScanBudget(query));

        query.limit(10L);
        Assert.assertEquals(11L, HstoreTable.rangeScanBudget(query));

        query.offset(3L);
        Assert.assertEquals(14L, HstoreTable.rangeScanBudget(query));
    }

    @Test
    public void testRangeQueryWithoutUserpropsDoesNotPushConditions() {
        // Sort-key prefix/range queries keep sysprop conditions only (owner
        // vertex, direction, label, sort values); those are enforced by the
        // key range already and must not be pushed to the store, whose row
        // decoder cannot parse the server's raw property layout (issue #3090)
        ConditionQuery origin = new ConditionQuery(HugeType.EDGE);
        origin.eq(HugeKeys.OWNER_VERTEX, IdGenerator.of("v1"));
        origin.eq(HugeKeys.DIRECTION, Directions.OUT);
        origin.eq(HugeKeys.LABEL, IdGenerator.of(1L));
        origin.gte(HugeKeys.SORT_VALUES, "ETC!");
        origin.lt(HugeKeys.SORT_VALUES, "ETC~");
        int before = origin.conditions().size();

        ScanRecordingSession session = new ScanRecordingSession();
        this.newTestTable().queryByRange(session, edgeRangeQuery(origin));

        Assert.assertTrue(session.scanCalled);
        Assert.assertNull(session.lastQueryBytes);
        Assert.assertEquals(before, origin.conditions().size());
    }

    @Test
    public void testRangeQueryWithUserpropsPushesCopyAndKeepsOrigin() {
        ConditionQuery origin = new ConditionQuery(HugeType.EDGE);
        origin.eq(HugeKeys.OWNER_VERTEX, IdGenerator.of("v1"));
        origin.query(Condition.eq(IdGenerator.of(7L), 100));
        int before = origin.conditions().size();

        ScanRecordingSession session = new ScanRecordingSession();
        this.newTestTable().queryByRange(session, edgeRangeQuery(origin));

        Assert.assertTrue(session.scanCalled);
        Assert.assertNotNull(session.lastQueryBytes);
        // the pushed-down query is a copy: the origin query keeps all its
        // conditions for core-side filtering after the scan returns
        Assert.assertEquals(before, origin.conditions().size());
        // pushed payload: user-prop condition survives, owner-vertex is
        // dropped, and the back reference to the origin query is cleared
        ConditionQuery pushed = ConditionQuery.fromBytes(session.lastQueryBytes);
        Assert.assertNull(pushed.condition(HugeKeys.OWNER_VERTEX));
        Assert.assertFalse(pushed.userpropConditions().isEmpty());
        Assert.assertNull(pushed.originQuery());
    }

    @Test
    public void testPrefixListQueryPushesCopyAndKeepsOrigin() {
        // prepareConditionQueryList() is called from queryByPrefixList() and
        // from the streaming query(Session, Iterator, String); neither has a
        // live caller in the server today, so this pins the method contract:
        // one origin query is shared by every prefix query of the batch
        ConditionQuery origin = new ConditionQuery(HugeType.EDGE);
        origin.eq(HugeKeys.OWNER_VERTEX, IdGenerator.of("v1"));
        origin.eq(HugeKeys.DIRECTION, Directions.OUT);
        origin.eq(HugeKeys.LABEL, IdGenerator.of(1L));
        origin.query(Condition.eq(IdGenerator.of(7L), 100));
        int before = origin.conditions().size();
        List<IdPrefixQuery> queries = Arrays.asList(
                new IdPrefixQuery(origin, IdGenerator.of(keyBytes(1),
                                                         IdType.STRING)),
                new IdPrefixQuery(origin, IdGenerator.of(keyBytes(2),
                                                         IdType.STRING)));

        ScanRecordingSession session = new ScanRecordingSession();
        List<BackendColumnIterator> iterators = this.newTestTable()
                .queryByPrefixList(session, queries, "g+oe");

        Assert.assertTrue(session.scanCalled);
        Assert.assertEquals(2, session.lastOwnerKeys.size());
        Assert.assertEquals(2, iterators.size());
        Assert.assertNotNull(session.lastQueryBytes);
        // the shared origin query keeps every condition, including the
        // owner vertex that the pushed copy drops
        Assert.assertEquals(before, origin.conditions().size());
        Assert.assertNotNull(origin.condition(HugeKeys.OWNER_VERTEX));
        ConditionQuery pushed = ConditionQuery.fromBytes(session.lastQueryBytes);
        Assert.assertNull(pushed.condition(HugeKeys.OWNER_VERTEX));
        Assert.assertNotNull(pushed.condition(HugeKeys.LABEL));
        Assert.assertFalse(pushed.userpropConditions().isEmpty());
        Assert.assertNull(pushed.originQuery());
    }

    @Test
    public void testMergedOlapInsertPreservesStandaloneEntryAndIsolatesProperties() {
        HstoreTables.OlapTable table = new HstoreTables.OlapTable("g");
        OlapSession session = new OlapSession();
        BinaryBackendEntry score = olapEntry(9, 1, 99);
        BinaryBackendEntry rank = olapEntry(10, 1, 123);
        byte[] standaloneKey = score.columns().iterator().next().name;
        table.insert(session, score, false); // Actual HstoreStore.mutate entry point.
        table.insert(session, rank);
        Assert.assertEquals(2, session.values.size());
        Assert.assertArrayEquals(standaloneKey, score.columns().iterator().next().name);
        Assert.assertNull(session.values.get(ByteBuffer.wrap(standaloneKey)));
        Assert.assertTrue(OlapKey.matchesProperty(session.values.get(ByteBuffer.wrap(
                OlapKey.format(IdGenerator.of(9), IdGenerator.of(1)))), IdGenerator.of(9)));
        for (byte[] owner : session.owners) {
            Assert.assertArrayEquals(IdGenerator.of(1).asBytes(), owner);
        }
    }

    @Test
    public void testOlapPointFallbackAndDeleteValidateLegacyPropertyIdentity() {
        HstoreTables.OlapTable table = new HstoreTables.OlapTable("g");
        OlapSession session = new OlapSession();
        BinaryBackendEntry legacy = olapEntry(9, 1, 42);
        BackendColumn old = legacy.columns().iterator().next();
        session.values.put(ByteBuffer.wrap(old.name), old.value);
        Id scoreKey = new BinaryId(OlapKey.format(IdGenerator.of(9), IdGenerator.of(1)), IdGenerator.of(1));
        Id rankKey = new BinaryId(OlapKey.format(IdGenerator.of(10), IdGenerator.of(1)), IdGenerator.of(1));
        try (BackendColumnIterator result = table.getById(session, scoreKey)) {
            Assert.assertTrue(result.hasNext());
            Assert.assertArrayEquals(scoreKey.asBytes(), result.next().name);
        }
        try (BackendColumnIterator result = table.getById(session, rankKey)) {
            Assert.assertFalse(result.hasNext());
        }
        IdQuery query = new IdQuery(HugeType.VERTEX);
        query.query(scoreKey);
        query.query(rankKey);
        Iterator<BackendEntry> queried = table.queryOlap(session, query);
        Assert.assertTrue(queried.hasNext());
        Assert.assertEquals(IdGenerator.of(1), queried.next().originId());
        Assert.assertFalse(queried.hasNext());
        table.delete(session, olapEntry(10, 1, 123));
        Assert.assertNotNull(session.values.get(ByteBuffer.wrap(old.name)));
        table.delete(session, legacy);
        Assert.assertNull(session.values.get(ByteBuffer.wrap(old.name)));
    }

    @Test
    public void testOlapConditionalUpdatesUsePropertyScopedExistenceForOldAndNewRows() {
        HstoreTables.OlapTable table = new HstoreTables.OlapTable("g");
        OlapSession session = new OlapSession();
        BinaryBackendEntry oldScore = olapEntry(9, 1, 42);
        BackendColumn old = oldScore.columns().iterator().next();
        session.values.put(ByteBuffer.wrap(old.name), old.value);
        BinaryBackendEntry score = olapEntry(9, 1, 99);
        BinaryBackendEntry rank = olapEntry(10, 1, 123);
        byte[] scoreKey = OlapKey.format(IdGenerator.of(9), IdGenerator.of(1));
        byte[] rankKey = OlapKey.format(IdGenerator.of(10), IdGenerator.of(1));
        Assert.assertTrue(table.queryExist(session, score));
        Assert.assertFalse(table.queryExist(session, rank));
        table.updateIfPresent(session, rank);
        Assert.assertNull(session.values.get(ByteBuffer.wrap(rankKey)));
        table.updateIfAbsent(session, rank);
        Assert.assertNotNull(session.values.get(ByteBuffer.wrap(rankKey)));
        table.updateIfPresent(session, score);
        Assert.assertArrayEquals(score.columns().iterator().next().value,
                                session.values.get(ByteBuffer.wrap(scoreKey)));
        table.updateIfAbsent(session, olapEntry(9, 1, 101));
        Assert.assertArrayEquals(score.columns().iterator().next().value,
                                session.values.get(ByteBuffer.wrap(scoreKey)));
        Assert.assertArrayEquals(old.value, session.values.get(ByteBuffer.wrap(old.name)));
        table.delete(session, score);
        Assert.assertFalse(table.queryExist(session, score));
        Assert.assertTrue(table.queryExist(session, rank));
        table.updateIfPresent(session, score);
        Assert.assertNull(session.values.get(ByteBuffer.wrap(scoreKey)));
        Assert.assertEquals(1, session.values.size());
    }

    @Test
    public void testOlapMergeBatchesOnlySelectedVerticesAndPrefersNamespacedRows() throws Exception {
        HstoreTables.OlapTable table = new HstoreTables.OlapTable("g");
        OlapSession session = new OlapSession();
        BinaryBackendEntry legacy = olapEntry(9, 1, 42);
        BackendColumn old = legacy.columns().iterator().next();
        session.values.put(ByteBuffer.wrap(old.name), old.value);
        table.insert(session, olapEntry(9, 1, 99));
        table.insert(session, olapEntry(10, 1, 123));
        BackendColumn legacySecond = olapEntry(9, 2, 43).columns().iterator().next();
        session.values.put(ByteBuffer.wrap(legacySecond.name), legacySecond.value);
        List<BackendEntry> base = new ArrayList<>();
        for (int i = 1; i <= 130; i++) {
            base.add(vertexEntry(i));
        }
        EntrySource source = new EntrySource(base);
        Iterator<BackendEntry> merged = table.mergeEntries(session, source,
                Set.of(IdGenerator.of(9), IdGenerator.of(10)), false);
        Assert.assertEquals(3, merged.next().columnsSize());
        Assert.assertEquals(1, session.batchCalls);
        Assert.assertEquals(128 * 3, session.requested.get(0).size());
        for (HgOwnerKey key : session.requested.get(0)) {
            Assert.assertFalse(Arrays.equals(key.getOwner(), IdGenerator.of(129).asBytes()));
        }
        int count = 1;
        while (merged.hasNext()) {
            Assert.assertEquals(IdGenerator.of(++count), merged.next().originId());
        }
        Assert.assertEquals(130, count);
        Assert.assertEquals(2, base.get(1).columnsSize()); // Legacy score must not also be merged as rank.
        Assert.assertEquals(1, base.get(2).columnsSize());
        Assert.assertEquals(2, session.batchCalls);
        Assert.assertEquals(2, session.batchCloses);
        Assert.assertEquals(1, source.closes);
        BackendColumn score = base.get(0).columns().stream()
                .filter(column -> Arrays.equals(column.name, OlapKey.format(IdGenerator.of(9), IdGenerator.of(1))))
                .findFirst().get();
        Assert.assertArrayEquals(olapEntry(9, 1, 99).columns().iterator().next().value, score.value);
        // The old row remains unchanged and has not been rewritten during reads.
        Assert.assertArrayEquals(old.value, session.values.get(ByteBuffer.wrap(old.name)));
    }

    @Test
    public void testOlapMergeCapsTotalKeysForManyPropertiesAndRetainsLastChunkRows() throws Exception {
        HstoreTables.OlapTable table = new HstoreTables.OlapTable("g");
        OlapSession session = new OlapSession();
        Set<Id> properties = manyOlapProperties();
        BackendColumn old = olapEntry(9999, 1, 42).columns().iterator().next();
        session.values.put(ByteBuffer.wrap(old.name), old.value);
        table.insert(session, olapEntry(10000, 1, 99));
        EntrySource source = new EntrySource(List.of(vertexEntry(1), vertexEntry(2)));
        Iterator<BackendEntry> merged = table.mergeEntries(session, source, properties, false);
        BackendEntry first = merged.next();
        Assert.assertEquals(1, source.consumed); // Many properties reduce the selected vertex batch.
        Assert.assertEquals(3, first.columnsSize());
        Assert.assertEquals(10, session.batchCalls);
        Assert.assertEquals(10, session.batchCloses);
        int oldRequests = 0;
        for (List<HgOwnerKey> requested : session.requested) {
            Assert.assertTrue(requested.size() <= 1024);
            for (HgOwnerKey key : requested) {
                Assert.assertArrayEquals(IdGenerator.of(1).asBytes(), key.getOwner());
                if (Arrays.equals(old.name, key.getKey())) {
                    oldRequests++;
                }
            }
        }
        Assert.assertEquals(1, oldRequests);
        BinaryBackendEntry decoded = (BinaryBackendEntry) first;
        Assert.assertArrayEquals(old.value, decoded.column(
                OlapKey.format(IdGenerator.of(9999), IdGenerator.of(1))).value);
        Assert.assertArrayEquals(olapEntry(10000, 1, 99).columns().iterator().next().value,
                decoded.column(OlapKey.format(IdGenerator.of(10000), IdGenerator.of(1))).value);
        ((AutoCloseable) merged).close();
        Assert.assertEquals(1, source.closes);
        Assert.assertEquals(1, source.consumed);
    }

    @Test
    public void testOlapPropertyChunksCloseOnLaterBatchFailureWithoutReadingAhead() {
        HstoreTables.OlapTable table = new HstoreTables.OlapTable("g");
        OlapSession session = new OlapSession();
        session.failBatchAt = 3;
        EntrySource source = new EntrySource(List.of(vertexEntry(1), vertexEntry(2)));
        Iterator<BackendEntry> merged = table.mergeEntries(session, source, manyOlapProperties(), true);
        try {
            merged.hasNext();
            Assert.fail("A later property chunk failure must propagate");
        } catch (IllegalStateException expected) {
            Assert.assertEquals("batch failed", expected.getMessage());
        }
        Assert.assertEquals(1, source.consumed);
        Assert.assertEquals(1, source.closes);
        Assert.assertEquals(3, session.batchCalls);
        Assert.assertEquals(3, session.batchCloses);
        for (List<HgOwnerKey> requested : session.requested) {
            Assert.assertTrue(requested.size() <= 1024);
        }
    }

    private static Set<Id> manyOlapProperties() {
        Set<Id> properties = new LinkedHashSet<>();
        for (int i = 1; i <= 10000; i++) {
            properties.add(IdGenerator.of(i));
        }
        return properties;
    }

    @Test
    public void testOlapMergePreservesPagingMetadataAndClosesOnEarlyCloseOrFailure() throws Exception {
        HstoreTables.OlapTable table = new HstoreTables.OlapTable("g");
        OlapSession session = new OlapSession();
        EntrySource source = new EntrySource(List.of(vertexEntry(1), vertexEntry(2)));
        Iterator<BackendEntry> merged = table.mergeEntries(session, source, Set.of(IdGenerator.of(9)), true);
        Assert.assertEquals(IdGenerator.of(1), merged.next().originId());
        Assert.assertEquals(1, source.consumed);
        Assert.assertEquals(1, ((CIter<?>) merged).metadata("position"));
        ((AutoCloseable) merged).close();
        Assert.assertEquals(1, source.closes);
        Assert.assertEquals(1, session.batchCloses);
        Assert.assertFalse(merged.hasNext());
        EntrySource failed = new EntrySource(List.of(vertexEntry(1)));
        session.failBatch = true;
        Iterator<BackendEntry> failure = table.mergeEntries(session, failed, Set.of(IdGenerator.of(9)), false);
        try {
            failure.hasNext();
            Assert.fail("Native batch failure must propagate");
        } catch (IllegalStateException expected) {
            Assert.assertEquals("batch failed", expected.getMessage());
        }
        Assert.assertEquals(1, failed.closes);
        Assert.assertEquals(2, session.batchCloses);
        EntrySource empty = new EntrySource(List.of(vertexEntry(1)));
        Assert.assertSame(empty, table.mergeEntries(session, empty, Set.of(), false));
        Assert.assertSame(empty, table.mergeEntries(session, empty, null, false));
        Assert.assertEquals(2, session.batchCalls);
    }

    private static BinaryBackendEntry vertexEntry(int vertex) {
        BinaryBackendEntry entry = new BinaryBackendEntry(HugeType.VERTEX,
                BytesBuffer.allocate(16).writeId(IdGenerator.of(vertex)).bytes());
        entry.column(entry.id().asBytes(), new byte[]{0});
        return entry;
    }

    private static BinaryBackendEntry olapEntry(int property, int vertex, int value) {
        HugeConfig config = new HugeConfig(Map.of());
        HugeGraph graph = (HugeGraph) Proxy.newProxyInstance(HugeGraph.class.getClassLoader(),
                new Class<?>[]{HugeGraph.class}, (proxy, method, args) -> {
                    if (method.getName().equals("option")) {
                        return ((ConfigOption<?>) args[0]).defaultValue();
                    }
                    if (method.getName().equals("configuration")) {
                        return config;
                    }
                    return null;
                });
        PropertyKey key = new PropertyKey(graph, IdGenerator.of(property), "property" + property);
        key.dataType(DataType.INT);
        key.writeType(WriteType.OLAP_COMMON);
        HugeVertex olap = new HugeVertex(graph, IdGenerator.of(vertex), VertexLabel.OLAP_VL);
        olap.addProperty(key, value);
        return (BinaryBackendEntry) new BinarySerializer(config).writeOlapVertex(olap);
    }

    private static final class EntrySource implements CIter<BackendEntry> {
        private final Iterator<BackendEntry> source;
        private int consumed;
        private int closes;

        private EntrySource(List<BackendEntry> entries) {
            this.source = entries.iterator();
        }

        @Override
        public boolean hasNext() {
            return this.source.hasNext();
        }

        @Override
        public BackendEntry next() {
            this.consumed++;
            return this.source.next();
        }

        @Override
        public void close() {
            this.closes++;
        }

        @Override
        public Object metadata(String meta, Object... args) {
            return this.consumed;
        }
    }

    private static final class OlapSession extends ScanRecordingSession {
        private final Map<ByteBuffer, byte[]> values = new HashMap<>();
        private final List<byte[]> owners = new ArrayList<>();
        private final List<List<HgOwnerKey>> requested = new ArrayList<>();
        private int batchCalls;
        private int batchCloses;
        private boolean failBatch;
        private int failBatchAt;

        @Override
        public void put(String table, byte[] owner, byte[] key, byte[] value) {
            this.owners.add(owner);
            this.values.put(ByteBuffer.wrap(key), value);
        }

        @Override
        public byte[] get(String table, byte[] owner, byte[] key) {
            this.owners.add(owner);
            return this.values.get(ByteBuffer.wrap(key));
        }

        @Override
        public void delete(String table, byte[] owner, byte[] key) {
            this.values.remove(ByteBuffer.wrap(key));
        }

        @Override
        public BackendColumnIterator getWithBatchExact(String table, List<HgOwnerKey> keys) {
            this.batchCalls++;
            this.requested.add(keys);
            Iterator<BackendColumn> result = keys.stream().filter(key ->
                    this.values.containsKey(ByteBuffer.wrap(key.getKey())))
                    .map(key -> BackendColumn.of(key.getKey(), this.values.get(ByteBuffer.wrap(key.getKey()))))
                    .iterator();
            return new BackendColumnIterator() {
                @Override
                public boolean hasNext() {
                    if (failBatch || (failBatchAt > 0 && batchCalls == failBatchAt)) {
                        throw new IllegalStateException("batch failed");
                    }
                    return result.hasNext();
                }

                @Override
                public BackendColumn next() {
                    return result.next();
                }

                @Override
                public byte[] position() {
                    return null;
                }

                @Override
                public void close() {
                    batchCloses++;
                }
            };
        }
    }

    private HstoreTable newTestTable() {
        HstoreTable table = new HstoreTable("hugegraph", "g+oe");
        table.ownerByQueryDelegate = (type, id) -> new byte[]{0};
        return table;
    }

    private static IdRangeQuery edgeRangeQuery(ConditionQuery origin) {
        return new IdRangeQuery(HugeType.EDGE_OUT, origin,
                                IdGenerator.of(keyBytes(1), IdType.STRING),
                                true,
                                IdGenerator.of(keyBytes(9), IdType.STRING),
                                false);
    }

    private static IdRangeQuery rangeIndexQuery() {
        return new IdRangeQuery(HugeType.RANGE_INT_INDEX, null,
                                IdGenerator.of(keyBytes(1), IdType.STRING),
                                true,
                                IdGenerator.of(keyBytes(9), IdType.STRING),
                                false);
    }

    private static byte[] keyBytes(int key) {
        byte[] bytes = new byte[9];
        bytes[0] = HugeType.RANGE_INT_INDEX.code();
        bytes[8] = (byte) key;
        return bytes;
    }

    private static class ScanRecordingSession extends HstoreSessions.Session {

        private boolean scanCalled = false;
        private byte[] lastQueryBytes = null;
        private List<HgOwnerKey> lastOwnerKeys = null;

        @Override
        public BackendColumnIterator scan(String table, byte[] ownerKeyFrom,
                                          byte[] ownerKeyTo, byte[] keyFrom,
                                          byte[] keyTo, int scanType,
                                          byte[] query, byte[] position) {
            this.scanCalled = true;
            this.lastQueryBytes = query;
            return new TestColumnIterator();
        }

        @Override
        public List<BackendColumnIterator> scan(String table,
                                                List<HgOwnerKey> keys,
                                                int scanType, long limit,
                                                byte[] query) {
            this.scanCalled = true;
            this.lastQueryBytes = query;
            this.lastOwnerKeys = keys;
            List<BackendColumnIterator> iterators = new ArrayList<>();
            for (int i = 0; i < keys.size(); i++) {
                iterators.add(new TestColumnIterator());
            }
            return iterators;
        }

        @Override
        public void open() {
        }

        @Override
        public void close() {
        }

        @Override
        public Object commit() {
            return null;
        }

        @Override
        public void rollback() {
        }

        @Override
        public boolean hasChanges() {
            return false;
        }

        @Override
        public void createTable(String tableName) {
        }

        @Override
        public void dropTable(String tableName) {
        }

        @Override
        public boolean existsTable(String tableName) {
            return true;
        }

        @Override
        public void truncateTable(String tableName) {
        }

        @Override
        public void deleteGraph() {
        }

        @Override
        public Pair<byte[], byte[]> keyRange(String table) {
            return null;
        }

        @Override
        public void put(String table, byte[] ownerKey, byte[] key,
                        byte[] value) {
        }

        @Override
        public void increase(String table, byte[] ownerKey, byte[] key,
                             byte[] value) {
        }

        @Override
        public void delete(String table, byte[] ownerKey, byte[] key) {
        }

        @Override
        public void deletePrefix(String table, byte[] ownerKey, byte[] key) {
        }

        @Override
        public void deleteRange(String table, byte[] ownerKeyFrom,
                                byte[] ownerKeyTo, byte[] keyFrom,
                                byte[] keyTo) {
        }

        @Override
        public byte[] get(String table, byte[] key) {
            return new byte[0];
        }

        @Override
        public byte[] get(String table, byte[] ownerKey, byte[] key) {
            return new byte[0];
        }

        @Override
        public BackendColumnIterator scan(String table) {
            throw new UnsupportedOperationException();
        }

        @Override
        public BackendColumnIterator scan(String table, byte[] ownerKey,
                                          byte[] prefix) {
            throw new UnsupportedOperationException();
        }

        @Override
        public BackendEntry.BackendIterator<BackendColumnIterator> scan(
                String table, Iterator<HgOwnerKey> keys, int scanType,
                Query queryParam, byte[] query) {
            throw new UnsupportedOperationException();
        }

        @Override
        public BackendColumnIterator scan(String table, byte[] ownerKeyFrom,
                                          byte[] ownerKeyTo, byte[] keyFrom,
                                          byte[] keyTo, int scanType) {
            throw new UnsupportedOperationException();
        }

        @Override
        public BackendColumnIterator scan(String table, byte[] ownerKeyFrom,
                                          byte[] ownerKeyTo, byte[] keyFrom,
                                          byte[] keyTo, int scanType,
                                          byte[] query) {
            throw new UnsupportedOperationException();
        }

        @Override
        public BackendColumnIterator scan(String table, int codeFrom,
                                          int codeTo, int scanType,
                                          byte[] query) {
            throw new UnsupportedOperationException();
        }

        @Override
        public BackendColumnIterator scan(String table, int codeFrom,
                                          int codeTo, int scanType,
                                          byte[] query, byte[] position) {
            throw new UnsupportedOperationException();
        }

        @Override
        public BackendColumnIterator scan(String table,
                                          byte[] conditionQueryToByte) {
            throw new UnsupportedOperationException();
        }

        @Override
        public BackendColumnIterator getWithBatch(String table,
                                                  List<HgOwnerKey> keys) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void merge(String table, byte[] ownerKey, byte[] key,
                          byte[] value) {
        }

        @Override
        public void setMode(GraphMode mode) {
        }

        @Override
        public void truncate() throws Exception {
        }

        @Override
        public void beginTx() {
        }

        @Override
        public int getActiveStoreSize() {
            return 0;
        }
    }

    private static final class TestColumnIterator
            implements BackendColumnIterator {

        private final List<Integer> keys;
        private int offset;
        private byte[] position;

        private TestColumnIterator(Integer... keys) {
            this.keys = Arrays.asList(keys);
            this.offset = 0;
            this.position = null;
        }

        @Override
        public boolean hasNext() {
            return this.offset < this.keys.size();
        }

        @Override
        public BackendColumn next() {
            if (!this.hasNext()) {
                throw new NoSuchElementException();
            }
            byte[] key = keyBytes(this.keys.get(this.offset++));
            this.position = key;
            return BackendColumn.of(key, key);
        }

        @Override
        public void close() {
            // pass
        }

        @Override
        public byte[] position() {
            return this.position;
        }
    }
}
