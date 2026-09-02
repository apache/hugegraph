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

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hugegraph.backend.id.Id.IdType;
import org.apache.hugegraph.backend.id.IdGenerator;
import org.apache.hugegraph.backend.page.PageInfo;
import org.apache.hugegraph.backend.page.PageState;
import org.apache.hugegraph.backend.query.Condition;
import org.apache.hugegraph.backend.query.ConditionQuery;
import org.apache.hugegraph.backend.query.IdRangeQuery;
import org.apache.hugegraph.backend.query.Query;
import org.apache.hugegraph.backend.store.BackendEntry;
import org.apache.hugegraph.backend.store.BackendEntry.BackendColumn;
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

    private static final class ScanRecordingSession extends HstoreSessions.Session {

        private boolean scanCalled = false;
        private byte[] lastQueryBytes = null;

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
        public List<BackendColumnIterator> scan(String table,
                                                List<HgOwnerKey> keys,
                                                int scanType, long limit,
                                                byte[] query) {
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
