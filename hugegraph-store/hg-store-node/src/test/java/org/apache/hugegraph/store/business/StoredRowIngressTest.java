/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hugegraph.store.business;

import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.HashMap;
import java.nio.ByteBuffer;
import java.util.Properties;
import java.util.Set;

import org.apache.hugegraph.HugeGraphSupplier;
import org.apache.hugegraph.backend.BackendColumn;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.id.IdGenerator;
import org.apache.hugegraph.id.EdgeId;
import org.apache.hugegraph.query.Condition;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.query.ConditionQuery;
import org.apache.hugegraph.rocksdb.access.RocksDBSession;
import org.apache.hugegraph.rocksdb.access.ScanIterator;
import org.apache.hugegraph.serializer.BinaryElementSerializer;
import org.apache.hugegraph.serializer.BytesBuffer;
import org.apache.hugegraph.serializer.DirectBinarySerializer;
import org.apache.hugegraph.serializer.OlapKey;
import org.apache.hugegraph.store.client.query.QueryExecutor;
import org.apache.hugegraph.store.cmd.HgCmdClient;
import org.apache.hugegraph.store.cmd.HgCmdProcessor;
import org.apache.hugegraph.store.cmd.request.BatchPutRequest;
import org.apache.hugegraph.store.cmd.response.BatchPutResponse;
import org.apache.hugegraph.store.grpc.common.Kv;
import org.apache.hugegraph.store.grpc.query.QueryRequest;
import org.apache.hugegraph.store.grpc.query.QueryResultFormat;
import org.apache.hugegraph.store.grpc.query.ScanType;
import org.apache.hugegraph.store.HgStoreEngine;
import org.apache.hugegraph.store.node.grpc.query.QueryUtil;
import org.apache.hugegraph.store.node.grpc.query.model.PipelineResult;
import org.apache.hugegraph.store.node.grpc.query.stages.TtlCheckStage;
import org.apache.hugegraph.store.node.grpc.query.stages.OlapStage;
import org.apache.hugegraph.struct.schema.EdgeLabel;
import org.apache.hugegraph.struct.schema.IndexLabel;
import org.apache.hugegraph.struct.schema.PropertyKey;
import org.apache.hugegraph.struct.schema.VertexLabel;
import org.apache.hugegraph.store.util.HgStoreException;
import org.apache.hugegraph.structure.BaseElement;
import org.apache.hugegraph.structure.BaseVertex;
import org.apache.hugegraph.structure.BaseEdge;
import org.apache.hugegraph.structure.Index;
import org.apache.hugegraph.structure.builder.IndexBuilder;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.type.define.Cardinality;
import org.apache.hugegraph.type.define.DataType;
import org.apache.hugegraph.type.define.Directions;
import org.apache.hugegraph.type.define.EdgeLabelType;
import org.apache.hugegraph.type.define.Frequency;
import org.apache.hugegraph.type.define.IndexType;
import org.apache.hugegraph.type.define.WriteType;
import org.apache.hugegraph.util.Bytes;
import org.junit.Assert;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import com.google.protobuf.ByteString;

public class StoredRowIngressTest {

    private static final String FIXTURE = "compatibility/2f827d6e8/core-rows.properties";
    private final BinaryElementSerializer serializer = new BinaryElementSerializer();
    private final DirectBinarySerializer directSerializer = new DirectBinarySerializer();
    private Properties rows;
    private HugeGraphSupplier graph;

    @BeforeEach
    public void setUp() throws Exception {
        this.rows = new Properties();
        try (InputStream input = getClass().getClassLoader().getResourceAsStream(FIXTURE)) {
            Assert.assertNotNull("Frozen baseline BinarySerializer rows must be present", input);
            this.rows.load(input);
        }
        this.graph = Mockito.mock(HugeGraphSupplier.class);
        Mockito.when(this.graph.configuration()).thenReturn(new HugeConfig(Map.of()));
        property(1, Cardinality.SINGLE, WriteType.OLTP);
        property(2, Cardinality.LIST, WriteType.OLTP);
        property(3, Cardinality.SET, WriteType.OLTP);
        property(9, Cardinality.SINGLE, WriteType.OLAP_COMMON);
        property(16, DataType.LONG, Cardinality.SINGLE, WriteType.OLTP);
        property(17, DataType.TEXT, Cardinality.SINGLE, WriteType.OLTP);
        VertexLabel vertex = new VertexLabel(this.graph, id(4), "person");
        vertex.properties(Set.of(id(1), id(2), id(3)));
        vertex.ttl(60_000L);
        Mockito.when(this.graph.vertexLabelOrNone(id(4))).thenReturn(vertex);
        Mockito.when(this.graph.vertexLabel(id(4))).thenReturn(vertex);
        EdgeLabel edge = new EdgeLabel(this.graph, id(5), "knows");
        edge.properties(Set.of(id(1), id(2), id(3)));
        edge.sourceLabel(id(4));
        edge.targetLabel(id(4));
        edge.frequency(Frequency.MULTIPLE);
        edge.ttl(60_000L);
        Mockito.when(this.graph.edgeLabelOrNone(id(5))).thenReturn(edge);
        Mockito.when(this.graph.edgeLabel(id(5))).thenReturn(edge);
        index(6, IndexType.SECONDARY);
        index(7, IndexType.RANGE_INT);
        index(8, IndexType.RANGE_LONG);
        index(15, IndexType.SECONDARY);
    }

    @Test
    public void testFilterIteratorReadsFrozenServerVertexAndEdges() {
        for (String row : List.of("vertex", "edge.out", "edge.in")) {
            BackendColumn column = column(row);
            RocksDBSession.BackendColumn stored = RocksDBSession.BackendColumn.of(column.name, column.value);
            ConditionQuery condition = new ConditionQuery(row.equals("vertex") ? HugeType.VERTEX : HugeType.EDGE);
            condition.query(Condition.eq(id(1), 7));
            FilterIterator<?> iterator = new FilterIterator<>(iterator(stored), condition, this.graph);
            Assert.assertTrue(row, iterator.hasNext());
            Assert.assertSame(stored, iterator.next());
            Assert.assertFalse(iterator.hasNext());
            iterator.close();

            ConditionQuery noMatch = new ConditionQuery(condition.resultType());
            noMatch.query(Condition.eq(id(1), 8));
            FilterIterator<?> rejected = new FilterIterator<>(iterator(stored), noMatch, this.graph);
            Assert.assertFalse(row, rejected.hasNext());
            rejected.close();
        }
    }

    @Test
    public void testSchemaLookupFailureClosesAlreadyOpenedFilterIterator() {
        ScanIterator source = Mockito.mock(ScanIterator.class);
        ConditionQuery query = new ConditionQuery(HugeType.VERTEX);
        query.query(Condition.eq(id(1), 7));
        HgStoreEngine engine = HgStoreEngine.getInstance();
        boolean previousClosing = engine.isClosing().getAndSet(false);
        try {
            FilterIterator.of(source, query.bytes(), "invalid-namespace");
            Assert.fail("Malformed metadata namespace must fail initialization");
        } catch (HgStoreException expected) {
            Assert.assertTrue(expected.getMessage().contains("Graph must include"));
            Mockito.verify(source).close();
        } finally {
            engine.isClosing().set(previousClosing);
        }
    }

    @Test
    public void testSelectionCountsActualMatchesAndPreservesCollectionsAndTtl() {
        for (String row : List.of("vertex", "edge.out", "edge.in")) {
            BackendColumn column = column(row);
            RocksDBSession.BackendColumn stored = RocksDBSession.BackendColumn.of(column.name, column.value);
            boolean isVertex = row.equals("vertex");
            // Requested count equals stored count, but one requested ID is absent.
            SelectIterator iterator = new SelectIterator(iterator(stored), List.of(2, 3, 99),
                                                          this.graph, isVertex);
            RocksDBSession.BackendColumn selected = iterator.next();
            Assert.assertArrayEquals(stored.name, selected.name);
            BaseElement element = QueryUtil.parseEntry(this.graph,
                    BackendColumn.of(selected.name, selected.value), isVertex);
            Assert.assertEquals(2, element.sizeOfProperties());
            Assert.assertEquals(List.of(7, -1, 7), element.getPropertyValue(id(2)));
            Assert.assertEquals(Set.of(-1, 7), element.getPropertyValue(id(3)));
            Assert.assertNull(element.getPropertyValue(id(1)));
            Assert.assertEquals(expiry(), element.expiredTime());
            iterator.close();
        }
    }

    @Test
    public void testSelectionOfMissingPropertiesWritesZeroCountAndKeepsTtl() {
        BackendColumn column = column("vertex");
        RocksDBSession.BackendColumn stored = RocksDBSession.BackendColumn.of(column.name, column.value);
        SelectIterator iterator = new SelectIterator(iterator(stored), List.of(99), this.graph, true);
        RocksDBSession.BackendColumn selected = iterator.next();
        BaseVertex vertex = this.serializer.parseSchemaVertex(this.graph,
                BackendColumn.of(selected.name, selected.value), null);
        Assert.assertEquals(0, vertex.sizeOfProperties());
        Assert.assertEquals(expiry(), vertex.expiredTime());
        iterator.close();
    }

    @Test
    public void testOlapMergeKeepsPersistentSchemaFormatAndTtl() {
        BackendColumn merged = QueryUtil.combineColumn(this.graph, column("vertex"), List.of(column("olap")));
        BaseVertex vertex = this.serializer.parseSchemaVertex(this.graph, merged, null);
        Assert.assertEquals(4, vertex.sizeOfProperties());
        Assert.assertEquals(Integer.valueOf(42), vertex.getPropertyValue(id(9)));
        Assert.assertEquals(expiry(), vertex.expiredTime());
    }

    @Test
    public void testDirectTtlReaderUsesFrozenRowSchemaAndIndexNameSuffix() {
        BackendColumn vertex = column("vertex");
        Assert.assertEquals(expiry(), this.directSerializer.parseSchemaVertex(
                this.graph, vertex.name, vertex.value).expiredTime());
        for (String row : List.of("edge.out", "edge.in")) {
            BackendColumn edge = column(row);
            Assert.assertEquals(expiry(), this.directSerializer.parseSchemaEdge(
                    this.graph, edge.name, edge.value).expiredTime());
        }
        for (String row : List.of("index.secondary", "index.int", "index.long")) {
            BackendColumn index = column(row);
            Assert.assertEquals(expiry(), this.directSerializer.parseSchemaIndex(
                    this.graph, index.name, index.value).expiredTime());
            Assert.assertEquals(expiry(), this.serializer.parseSchemaIndex(this.graph, index).expiredTime());
        }
    }

    @Test
    public void testTtlPipelineAcceptsLiveFixtureAndRejectsExpiredFixture() throws Exception {
        Field now = TtlCheckStage.class.getDeclaredField("now");
        now.setAccessible(true);
        for (String row : List.of("vertex", "edge.out", "edge.in")) {
            TtlCheckStage stage = new TtlCheckStage();
            stage.init(row.equals("vertex"), this.graph);
            BackendColumn column = column(row);
            PipelineResult input = new PipelineResult(RocksDBSession.BackendColumn.of(column.name, column.value));
            now.setLong(stage, Long.parseLong(this.rows.getProperty("now")));
            Assert.assertSame(row, input, stage.handle(input));
            now.setLong(stage, expiry() + 1);
            Assert.assertNull(row, stage.handle(input));
        }
    }

    @Test
    public void testClientDecodesPersistentRowsAndTaggedElementResponsesExplicitly() throws Exception {
        QueryExecutor executor = new QueryExecutor(null, this.graph, 60_000L);
        Method decode = QueryExecutor.class.getDeclaredMethod("fromKv", String.class, Kv.class,
                                                               boolean.class, boolean.class);
        decode.setAccessible(true);
        BackendColumn stored = column("vertex");
        BaseVertex persistent = (BaseVertex) decode.invoke(executor, "g+v", kv(stored), false, true);
        Assert.assertNotNull(persistent);
        Assert.assertEquals(List.of(7, -1, 7), persistent.getPropertyValue(id(2)));
        BackendColumn tagged = this.serializer.writeVertex(persistent);
        BaseVertex response = (BaseVertex) decode.invoke(executor, "g+v", kv(tagged), false, false);
        Assert.assertNotNull(response);
        Set<?> expectedGroups = persistent.getPropertyValue(id(3));
        Set<?> actualGroups = response.getPropertyValue(id(3));
        Assert.assertEquals(expectedGroups, actualGroups);
        Assert.assertEquals(expiry(), response.expiredTime());
    }

    @Test
    public void testUntimedAndSystemRowsDoNotRequireTrailingTtl() {
        VertexLabel untimed = new VertexLabel(this.graph, id(10), "untimed");
        untimed.properties(Set.of(id(1), id(2), id(3)));
        Mockito.when(this.graph.vertexLabel(id(10))).thenReturn(untimed);
        Mockito.when(this.graph.vertexLabelOrNone(id(10))).thenReturn(untimed);
        EdgeLabel edge = new EdgeLabel(this.graph, id(11), "untimedEdge");
        edge.properties(Set.of(id(1), id(2), id(3)));
        edge.sourceLabel(id(4));
        edge.targetLabel(id(4));
        edge.frequency(Frequency.MULTIPLE);
        Mockito.when(this.graph.edgeLabel(id(11))).thenReturn(edge);
        Mockito.when(this.graph.edgeLabelOrNone(id(11))).thenReturn(edge);
        for (int number : List.of(12, 13, 14)) {
            IndexLabel label = new IndexLabel(this.graph, id(number), "untimedIndex" + number);
            label.baseType(HugeType.VERTEX_LABEL);
            label.baseValue(id(10));
            label.indexType(number == 12 ? IndexType.SECONDARY :
                            number == 13 ? IndexType.RANGE_INT : IndexType.RANGE_LONG);
            label.indexFields(id(number == 12 ? 17 : number == 13 ? 1 : 16));
            Mockito.when(this.graph.indexLabel(id(number))).thenReturn(label);
        }
        BackendColumn vertex = column("vertex.untimed");
        Assert.assertEquals(0L, this.serializer.parseSchemaVertex(this.graph, vertex, null).expiredTime());
        BackendColumn untimedEdge = column("edge.untimed");
        Assert.assertEquals(0L, this.serializer.parseSchemaEdge(this.graph, untimedEdge, null, true).expiredTime());
        for (String row : List.of("index.secondary.untimed", "index.int.untimed", "index.long.untimed",
                                  "index.system")) {
            BackendColumn column = column(row);
            Assert.assertEquals(row, 0L, this.serializer.parseSchemaIndex(this.graph, column).expiredTime());
            Assert.assertEquals(row, 0L, this.directSerializer.parseSchemaIndex(
                    this.graph, column.name, column.value).expiredTime());
        }
    }

    @Test
    public void testRebuiltIndexWriterMatchesFrozenServerColumnsAndStoreReader() {
        BaseVertex vertex = this.serializer.parseSchemaVertex(this.graph, column("vertex"), null);
        vertex.addProperty(this.graph.propertyKey(id(16)), 7L);
        vertex.addProperty(this.graph.propertyKey(id(17)), "alice");
        IndexBuilder builder = new IndexBuilder(this.graph);
        Map<Integer, String> fixtures = Map.of(6, "index.secondary", 7, "index.int", 8, "index.long");
        for (Map.Entry<Integer, String> fixture : fixtures.entrySet()) {
            List<Index> indexes = builder.buildIndex(vertex, this.graph.indexLabel(id(fixture.getKey())));
            Assert.assertEquals(1, indexes.size());
            assertPersistentIndex(indexes.get(0), fixture.getValue());
        }
        vertex.addProperty(this.graph.propertyKey(id(17)), "long-field-".repeat(20));
        List<Index> longString = builder.buildIndex(vertex, this.graph.indexLabel(id(15)));
        Assert.assertEquals(1, longString.size());
        assertPersistentIndex(longString.get(0), "index.hashed");
    }

    @Test
    public void testSchemaIndexWriterHashingMatchesFrozenServerColumn() {
        // Also verify hashing at the serializer boundary without depending on the builder.
        // The frozen Server producer supplies the same complete field value.
        Index index = new Index(this.graph, this.graph.indexLabel(id(15)), true);
        index.fieldValues("long-field-".repeat(20));
        index.elementIds(id(1), expiry());
        assertPersistentIndex(index, "index.hashed");
    }

    @Test
    public void testStringIndexBranchesPreserveLongValuesAndCanonicalEscapes() {
        BaseVertex vertex = this.serializer.parseSchemaVertex(this.graph, column("vertex"), null);
        IndexBuilder builder = new IndexBuilder(this.graph);
        String longValue = "long-field-".repeat(20);
        for (IndexType type : List.of(IndexType.SECONDARY, IndexType.SHARD, IndexType.UNIQUE)) {
            IndexLabel label = new IndexLabel(this.graph, id(20), "stringIndex");
            label.baseType(HugeType.VERTEX_LABEL);
            label.baseValue(id(4));
            label.indexType(type);
            label.indexFields(id(17));
            vertex.addProperty(this.graph.propertyKey(id(17)), longValue);
            Assert.assertEquals(type.name(), longValue, builder.buildIndex(vertex, label).get(0).fieldValues());
            vertex.addProperty(this.graph.propertyKey(id(17)), "");
            Assert.assertEquals(type.name(), "\u0002", builder.buildIndex(vertex, label).get(0).fieldValues());
            vertex.addProperty(this.graph.propertyKey(id(17)), "prefix!tail");
            Assert.assertEquals(type.name(), "prefix`!tail", builder.buildIndex(vertex, label).get(0).fieldValues());
            vertex.addProperty(this.graph.propertyKey(id(17)), "\u0001illegal");
            try {
                builder.buildIndex(vertex, label);
                Assert.fail("Reserved leading index symbols must remain illegal: " + type);
            } catch (IllegalArgumentException expected) {
                Assert.assertTrue(expected.getMessage().contains("Illegal leading char"));
            }
        }
    }

    @Test
    public void testUniqueNullableFieldUsesCanonicalNullSentinel() {
        property(18, DataType.TEXT, Cardinality.SINGLE, WriteType.OLTP);
        this.graph.vertexLabel(id(4)).nullableKeys(id(18));
        BaseVertex vertex = this.serializer.parseSchemaVertex(this.graph, column("vertex"), null);
        vertex.addProperty(this.graph.propertyKey(id(17)), "prefix");
        IndexLabel label = new IndexLabel(this.graph, id(21), "nullableUnique");
        label.baseType(HugeType.VERTEX_LABEL);
        label.baseValue(id(4));
        label.indexType(IndexType.UNIQUE);
        label.indexFields(id(17), id(18));
        List<Index> indexes = new IndexBuilder(this.graph).buildIndex(vertex, label);
        Assert.assertEquals(1, indexes.size());
        Assert.assertEquals("prefix!\u0001", indexes.get(0).fieldValues());
    }

    @Test
    public void testFrozenLegacyStoreTtlEnvelopeRemainsReadable() throws Exception {
        Properties legacy = new Properties();
        try (InputStream input = getClass().getClassLoader().getResourceAsStream(
                "compatibility/2f827d6e8/store-index-rows.properties")) {
            Assert.assertNotNull("Frozen baseline Store index writer output must be present", input);
            legacy.load(input);
        }
        for (String row : List.of("secondary", "int", "long", "system")) {
            BackendColumn column = BackendColumn.of(Bytes.fromHex(legacy.getProperty(row + ".name")),
                                                    Bytes.fromHex(legacy.getProperty(row + ".value")));
            long expectedExpiry = Long.parseLong(legacy.getProperty(row + ".expiry"));
            Assert.assertEquals(row, expectedExpiry,
                                this.serializer.parseSchemaIndex(this.graph, column).expiredTime());
            Assert.assertEquals(row, expectedExpiry, this.directSerializer.parseSchemaIndex(
                    this.graph, column.name, column.value).expiredTime());
        }
    }

    @Test
    public void testOlapStageReadsFrozenLegacyRowsWithoutCrossPropertyLeakage() throws Exception {
        property(18, Cardinality.SINGLE, WriteType.OLAP_COMMON);
        BackendColumn legacy = column("olap");
        Map<ByteBuffer, byte[]> rows = new HashMap<>();
        rows.put(ByteBuffer.wrap(legacy.name), legacy.value);
        for (boolean tagged : List.of(false, true)) {
            BaseVertex merged = runOlapStage(rows, tagged, id(9), id(18));
            Assert.assertEquals(42, (Object) merged.getProperty(id(9)).value());
            Assert.assertNull(merged.getProperty(id(18)));
            Assert.assertArrayEquals(legacy.value, rows.get(ByteBuffer.wrap(legacy.name)));
            Assert.assertEquals(1, rows.size());
        }
    }

    @Test
    public void testOlapStageMergesMultiplePropertiesAndPrefersNamespacedValues() throws Exception {
        property(18, Cardinality.SINGLE, WriteType.OLAP_COMMON);
        BackendColumn legacy = column("olap");
        Map<ByteBuffer, byte[]> rows = new HashMap<>();
        rows.put(ByteBuffer.wrap(legacy.name), legacy.value);
        rows.put(ByteBuffer.wrap(OlapKey.format(id(9), id(1))), schemaOlapValue(id(9), 99));
        rows.put(ByteBuffer.wrap(OlapKey.format(id(18), id(1))), schemaOlapValue(id(18), 123));
        for (boolean tagged : List.of(false, true)) {
            BaseVertex merged = runOlapStage(rows, tagged, id(9), id(18));
            Assert.assertEquals(99, (Object) merged.getProperty(id(9)).value());
            Assert.assertEquals(123, (Object) merged.getProperty(id(18)).value());
            Assert.assertArrayEquals(legacy.value, rows.get(ByteBuffer.wrap(legacy.name)));
        }
    }

    private byte[] schemaOlapValue(Id property, int value) {
        BaseVertex vertex = new BaseVertex(id(1), VertexLabel.OLAP_VL);
        vertex.addProperty(this.graph.propertyKey(property), value);
        BytesBuffer buffer = BytesBuffer.allocate(16);
        this.serializer.formatSchemaProperty(vertex.getProperty(property), buffer);
        return buffer.bytes();
    }

    private BaseVertex runOlapStage(Map<ByteBuffer, byte[]> rows, boolean tagged, Id... properties) throws Exception {
        String graphName = "CODEC_TEST/olap/g";
        BusinessHandler handler = Mockito.mock(BusinessHandler.class);
        Mockito.when(handler.doGet(Mockito.eq(graphName), Mockito.anyInt(), Mockito.eq("g+olap"),
                                   Mockito.any(byte[].class))).thenAnswer(call ->
                rows.get(ByteBuffer.wrap(call.getArgument(3))));
        Field mock = BusinessHandlerImpl.class.getDeclaredField("mockGraphSupplier");
        mock.setAccessible(true);
        HugeGraphSupplier previous = (HugeGraphSupplier) mock.get(null);
        BusinessHandlerImpl.setMockGraphSupplier(this.graph);
        OlapStage stage = new OlapStage(handler);
        try {
            List<ByteString> requested = new ArrayList<>();
            for (Id property : properties) {
                requested.add(ByteString.copyFrom(BytesBuffer.allocate(16).writeId(property).bytes()));
            }
            stage.init(graphName, "g+v", requested);
            BackendColumn vertex = column("vertex");
            BaseVertex decoded = this.serializer.parseSchemaVertex(this.graph, vertex, null);
            PipelineResult input = tagged ? new PipelineResult(decoded) :
                    new PipelineResult(RocksDBSession.BackendColumn.of(vertex.name, vertex.value));
            Assert.assertSame(input, stage.handle(input));
            if (tagged) {
                return (BaseVertex) input.getElement();
            }
            return this.serializer.parseSchemaVertex(this.graph,
                    BackendColumn.of(input.getColumn().name, input.getColumn().value), null);
        } finally {
            stage.close();
            BusinessHandlerImpl.setMockGraphSupplier(previous);
        }
    }

    private void assertPersistentIndex(Index rebuilt, String fixture) {
        BackendColumn expected = column(fixture);
        BackendColumn written = this.serializer.writeSchemaIndex(rebuilt);
        // DataManager's persisted batch normalizes an absent payload to zero bytes.
        byte[] value = written.value == null ? new byte[0] : written.value;
        Assert.assertArrayEquals(fixture, expected.name, written.name);
        Assert.assertArrayEquals(fixture, expected.value, value);
        Index decoded = this.serializer.parseSchemaIndex(this.graph, BackendColumn.of(written.name, value));
        Assert.assertEquals(rebuilt.elementId(), decoded.elementId());
        Assert.assertEquals(rebuilt.fieldValues(), decoded.fieldValues());
        Assert.assertEquals(expiry(), decoded.expiredTime());
    }

    @Test
    public void testParentRequestedRebuildIndexesChildRowsAndExcludesUnrelatedLabels() throws Exception {
        EdgeLabel parent = new EdgeLabel(this.graph, id(100), "parent");
        EdgeLabel child = new EdgeLabel(this.graph, id(101), "child");
        child.edgeLabelType(EdgeLabelType.SUB);
        child.fatherId(parent.id());
        EdgeLabel unrelated = new EdgeLabel(this.graph, id(102), "unrelated");
        for (EdgeLabel label : List.of(parent, child, unrelated)) {
            label.properties(Set.of(id(17)));
            // Real sublabels persist their own endpoint links; they do not inherit them on read.
            label.sourceLabel(id(4));
            label.targetLabel(id(4));
            Assert.assertEquals(id(4), label.sourceLabel());
            Assert.assertEquals(id(4), label.targetLabel());
            Mockito.when(this.graph.edgeLabel(label.id())).thenReturn(label);
            Mockito.when(this.graph.edgeLabelOrNone(label.id())).thenReturn(label);
        }
        IndexLabel index = new IndexLabel(this.graph, id(103), "parentIndex");
        index.baseType(HugeType.EDGE_LABEL);
        index.baseValue(parent.id());
        index.indexType(IndexType.SECONDARY);
        index.indexFields(id(17));
        Mockito.when(this.graph.indexLabel(index.id())).thenReturn(index);
        BaseEdge childEdge = new BaseEdge(new EdgeId(id(1), Directions.OUT, parent.id(), child.id(), "", id(2)), child);
        childEdge.isOutEdge(true);
        childEdge.addProperty(this.graph.propertyKey(id(17)), "child record");
        BaseEdge otherEdge = new BaseEdge(new EdgeId(id(3), Directions.OUT, unrelated.id(), unrelated.id(), "", id(4)),
                                          unrelated);
        otherEdge.isOutEdge(true);
        otherEdge.addProperty(this.graph.propertyKey(id(17)), "unrelated record");
        ScanIterator source = Mockito.mock(ScanIterator.class);
        Mockito.when(source.hasNext()).thenReturn(true, true, false);
        Mockito.when(source.next()).thenReturn(rebuildInput(childEdge), rebuildInput(otherEdge));
        BusinessHandler handler = Mockito.mock(BusinessHandler.class);
        String graphName = "CODEC_TEST/rebuild/g";
        Mockito.when(handler.scan(graphName, "g+oe", 0, 65536)).thenReturn(source);
        List<BatchPutRequest.KV> written = new ArrayList<>();
        HgCmdClient client = Mockito.mock(HgCmdClient.class);
        Mockito.when(client.batchPut(Mockito.any(BatchPutRequest.class))).thenAnswer(call -> {
            BatchPutRequest request = call.getArgument(0);
            written.addAll(request.getEntries());
            BatchPutResponse response = new BatchPutResponse();
            response.setStatus(HgCmdProcessor.Status.OK);
            return response;
        });
        DataManagerImpl manager = new DataManagerImpl();
        manager.setBusinessHandler(handler);
        manager.setCmdClient(client);
        Metapb.BuildIndexParam request = Metapb.BuildIndexParam.newBuilder().setGraph(graphName)
                .setLabelId(ByteString.copyFrom(BytesBuffer.allocate(8).writeId(parent.id()).bytes()))
                .setIsVertexLabel(false)
                .setIndexLabel(ByteString.copyFrom(BytesBuffer.allocate(8).writeId(index.id()).bytes())).build();
        Metapb.Partition partition = Metapb.Partition.newBuilder().setId(9999).setStartKey(0).setEndKey(65536).build();
        Field mock = BusinessHandlerImpl.class.getDeclaredField("mockGraphSupplier");
        mock.setAccessible(true);
        HugeGraphSupplier previous = (HugeGraphSupplier) mock.get(null);
        BusinessHandlerImpl.setMockGraphSupplier(this.graph);
        try {
            Assert.assertTrue(manager.doBuildIndex(request, partition).isOk());
            Assert.assertEquals(1, written.size());
            BatchPutRequest.KV row = written.get(0);
            Index decoded = this.serializer.parseSchemaIndex(this.graph,
                                                              BackendColumn.of(row.getKey(), row.getValue()));
            Assert.assertEquals(parent.id(), decoded.indexLabel().baseValue());
            Assert.assertEquals(childEdge.id(), decoded.elementId());
            Assert.assertEquals("child record", decoded.fieldValues());
            Mockito.verify(source).close();
        } finally {
            BusinessHandlerImpl.setMockGraphSupplier(previous);
        }
    }

    private RocksDBSession.BackendColumn rebuildInput(BaseEdge edge) {
        byte[] name = this.serializer.formatEdgeName(edge);
        // Range scans remove the graph prefix but retain the physical hash-code suffix.
        return RocksDBSession.BackendColumn.of(Arrays.copyOf(name, name.length + Short.BYTES),
                                                this.serializer.formatSchemaEdgeValue(edge));
    }

    @Test
    public void testResponseFormatMatchesDeserializationPlanning() {
        QueryRequest.Builder request = QueryRequest.newBuilder().setScanType(ScanType.TABLE_SCAN);
        Assert.assertFalse(QueryResultFormat.returnsTaggedElements(request.build()));
        request.setCheckTtl(true);
        Assert.assertFalse(QueryResultFormat.returnsTaggedElements(request.build()));
        request.addProperty(ByteString.copyFromUtf8("property"));
        Assert.assertTrue(QueryResultFormat.requiresElementDeserialization(request.build()));
        Assert.assertTrue(QueryResultFormat.returnsTaggedElements(request.build()));
        request.clearProperty().addOrderBy(ByteString.copyFromUtf8("order"));
        Assert.assertTrue(QueryResultFormat.returnsTaggedElements(request.build()));
        request.clearOrderBy().setCondition(ByteString.copyFromUtf8("condition"));
        Assert.assertTrue(QueryResultFormat.returnsTaggedElements(request.build()));
    }

    private void property(int value, Cardinality cardinality, WriteType writeType) {
        property(value, DataType.INT, cardinality, writeType);
    }

    private void property(int value, DataType type, Cardinality cardinality, WriteType writeType) {
        PropertyKey key = new PropertyKey(this.graph, id(value), "property" + value);
        key.dataType(type);
        key.cardinality(cardinality);
        key.writeType(writeType);
        Mockito.when(this.graph.propertyKey(id(value))).thenReturn(key);
    }

    private void index(int value, IndexType type) {
        IndexLabel label = new IndexLabel(this.graph, id(value), "index" + value);
        label.baseType(HugeType.VERTEX_LABEL);
        label.baseValue(id(4));
        label.indexType(type);
        label.indexFields(id(type == IndexType.RANGE_LONG ? 16 : type == IndexType.SECONDARY ? 17 : 1));
        Mockito.when(this.graph.indexLabel(id(value))).thenReturn(label);
    }

    private BackendColumn column(String row) {
        return BackendColumn.of(Bytes.fromHex(this.rows.getProperty(row + ".column.0.name")),
                                Bytes.fromHex(this.rows.getProperty(row + ".column.0.value")));
    }

    private long expiry() {
        return Long.parseLong(this.rows.getProperty("expiry"));
    }

    private static Id id(int value) {
        return IdGenerator.of(value);
    }

    private static Kv kv(BackendColumn column) {
        return Kv.newBuilder().setKey(ByteString.copyFrom(column.name))
                 .setValue(ByteString.copyFrom(column.value)).build();
    }

    private static ScanIterator iterator(RocksDBSession.BackendColumn column) {
        ScanIterator iterator = Mockito.mock(ScanIterator.class);
        Mockito.when(iterator.hasNext()).thenReturn(true, false);
        Mockito.when(iterator.next()).thenReturn(column);
        return iterator;
    }
}
