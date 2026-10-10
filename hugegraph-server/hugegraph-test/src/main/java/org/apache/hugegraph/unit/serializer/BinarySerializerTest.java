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

package org.apache.hugegraph.unit.serializer;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Date;
import java.util.Properties;
import java.util.Set;

import org.apache.hugegraph.backend.BackendColumn;
import org.apache.hugegraph.backend.serializer.AbstractSerializer;
import org.apache.hugegraph.backend.serializer.BinaryBackendEntry;
import org.apache.hugegraph.backend.serializer.BinarySerializer;
import org.apache.hugegraph.backend.serializer.TextSerializer;
import org.apache.hugegraph.backend.store.BackendEntry;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.id.EdgeId;
import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.id.IdGenerator;
import org.apache.hugegraph.query.Condition;
import org.apache.hugegraph.query.ConditionQuery;
import org.apache.hugegraph.query.IdPrefixQuery;
import org.apache.hugegraph.serializer.BytesBuffer;
import org.apache.hugegraph.struct.schema.IndexLabel;
import org.apache.hugegraph.struct.schema.PropertyKey;
import org.apache.hugegraph.struct.schema.Userdata;
import org.apache.hugegraph.structure.HugeEdge;
import org.apache.hugegraph.structure.HugeVertex;
import org.apache.hugegraph.structure.Index.IdWithExpiredTime;
import org.apache.hugegraph.structure.Index;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.testutil.Whitebox;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.type.define.Cardinality;
import org.apache.hugegraph.type.define.DataType;
import org.apache.hugegraph.type.define.Directions;
import org.apache.hugegraph.type.define.HugeKeys;
import org.apache.hugegraph.type.define.IdStrategy;
import org.apache.hugegraph.type.define.IndexType;
import org.apache.hugegraph.unit.BaseUnitTest;
import org.apache.hugegraph.unit.FakeObjects;
import org.apache.hugegraph.util.Bytes;
import org.apache.hugegraph.util.DateUtil;
import org.junit.Test;
import org.mockito.Mockito;

public class BinarySerializerTest extends BaseUnitTest {

    @Test
    public void testEdgeQueryLabelCandidateContract() {
        for (AbstractSerializer serializer : Arrays.asList(
                new BinarySerializer(FakeObjects.newConfig()),
                new TextSerializer(FakeObjects.newConfig()))) {
            ConditionQuery eq = new ConditionQuery(HugeType.EDGE);
            eq.eq(HugeKeys.OWNER_VERTEX, IdGenerator.of(1));
            eq.eq(HugeKeys.DIRECTION, Directions.OUT);
            eq.eq(HugeKeys.LABEL, IdGenerator.of(2));
            ConditionQuery in = new ConditionQuery(HugeType.EDGE);
            in.eq(HugeKeys.OWNER_VERTEX, IdGenerator.of(1));
            in.eq(HugeKeys.DIRECTION, Directions.OUT);
            in.query(Condition.in(HugeKeys.LABEL, Arrays.asList(IdGenerator.of(2))));
            Assert.assertEquals(((IdPrefixQuery) serializer.writeQuery(eq)).prefix(),
                                ((IdPrefixQuery) serializer.writeQuery(in)).prefix());

            ConditionQuery multiple = new ConditionQuery(HugeType.EDGE);
            multiple.eq(HugeKeys.OWNER_VERTEX, IdGenerator.of(1));
            multiple.eq(HugeKeys.DIRECTION, Directions.OUT);
            multiple.query(Condition.in(HugeKeys.LABEL,
                                         Arrays.asList(IdGenerator.of(2), IdGenerator.of(3))));
            Assert.assertThrows(IllegalStateException.class, () -> serializer.writeQuery(multiple));
        }
    }

    @Test
    public void testLegacyStoredLabelExpiryReadEliminateAndAppendKeys() throws IOException {
        Properties core = labelFixtures("core-rows.properties");
        Properties store = labelFixtures("store-index-rows.properties");
        long expiry = Long.parseLong(core.getProperty("expiry"));
        FakeObjects objects = new FakeObjects("legacy-fixture");
        Mockito.when(objects.graph().now()).thenReturn(expiry + 1L);
        BinarySerializer serializer = new BinarySerializer();
        for (String name : new String[]{"system", "system.edge"}) {
            boolean edge = name.endsWith("edge");
            HugeType base = edge ? HugeType.EDGE : HugeType.VERTEX;
            IndexLabel label = IndexLabel.label(base);
            HugeType indexType = edge ? HugeType.EDGE_LABEL_INDEX : HugeType.VERTEX_LABEL_INDEX;
            Id element = edge ? new EdgeId(IdGenerator.of(1L), Directions.OUT,
                                          IdGenerator.of(5L), IdGenerator.of(5L), "2026!x",
                                          IdGenerator.of(2L)) : IdGenerator.of(1L);
            byte[] key = Bytes.fromHex(store.getProperty(name + ".name"));
            byte[] value = Bytes.fromHex(store.getProperty(name + ".value"));
            byte[] rowId = Bytes.fromHex(core.getProperty("index." + name + ".id"));
            BinaryBackendEntry stored = new BinaryBackendEntry(indexType, rowId);
            stored.column(key, value);
            ConditionQuery query = new ConditionQuery(indexType);
            query.eq(HugeKeys.FIELD_VALUES, edge ? 5L : 4L);
            Index read = serializer.readIndex(objects.graph(), query, stored);
            Assert.assertEquals(element, read.elementId());
            Assert.assertEquals(expiry, read.expiredTime());
            Assert.assertFalse(read.hasTtl());
            Set<IdWithExpiredTime> expired = read.expiredElementIds();
            Assert.assertEquals(1, expired.size());
            Assert.assertTrue(read.elementIds().isEmpty());
            Index removal = read.clone();
            removal.resetElementIds();
            for (IdWithExpiredTime record : expired) {
                removal.elementIds(record.id(), record.expiredTime());
            }
            BackendEntry removed = serializer.writeIndex(removal);
            Assert.assertEquals(1, removed.columnsSize());
            BackendColumn removedColumn = removed.columns().iterator().next();
            Assert.assertArrayEquals(key, removedColumn.name);
            Assert.assertNotNull(removedColumn.value);
            Assert.assertArrayEquals(BytesBuffer.BYTES_EMPTY, removedColumn.value);
            Assert.assertTrue(read.elementIds().isEmpty());

            Index append = new Index(objects.graph(), label);
            append.fieldValues(edge ? 5L : 4L);
            append.elementIds(element, expiry);
            BackendEntry appended = serializer.writeIndex(append);
            BackendColumn appendedColumn = appended.columns().iterator().next();
            Assert.assertArrayEquals(key, appendedColumn.name);
            Assert.assertArrayEquals(Bytes.fromHex(core.getProperty(
                                    "index." + name + ".column.0.name")), appendedColumn.name);
            Assert.assertNotNull(appendedColumn.value);
            Assert.assertArrayEquals(Bytes.fromHex(core.getProperty(
                                    "index." + name + ".column.0.value")), appendedColumn.value);
            // Core append and TTL elimination address the original Store cell,
            // so they cannot introduce a second key carrying an expiry suffix.
            Assert.assertArrayEquals(removedColumn.name, appendedColumn.name);
            Index appendedRead = serializer.readIndex(objects.graph(), query, appended);
            Assert.assertEquals(1, appendedRead.elementIds().size());
            Assert.assertEquals(element, appendedRead.elementId());
            Assert.assertEquals(0L, appendedRead.expiredTime());
        }
    }

    @Test
    public void testOrdinaryIndexWriterProducesNonNullKvValue() {
        FakeObjects objects = new FakeObjects();
        Id fieldId = IdGenerator.of(1L);
        Id baseId = IdGenerator.of(2L);
        objects.newPropertyKey(fieldId, "age", DataType.INT, Cardinality.SINGLE);
        objects.newVertexLabel(baseId, "person", IdStrategy.CUSTOMIZE_NUMBER, fieldId);
        IndexLabel label = objects.newIndexLabel(IdGenerator.of(3L), "by-age", HugeType.VERTEX_LABEL,
                                                 baseId, IndexType.SECONDARY, fieldId);
        Index index = new Index(objects.graph(), label);
        index.fieldValues("42");
        index.elementIds(IdGenerator.of(4L));

        BackendEntry entry = new BinarySerializer().writeIndex(index);
        Assert.assertEquals(1, entry.columnsSize());
        BackendColumn column = entry.columns().iterator().next();
        Assert.assertNotNull(column.name);
        Assert.assertNotNull(column.value);
        Assert.assertArrayEquals(BytesBuffer.BYTES_EMPTY, column.value);
    }

    private static Properties labelFixtures(String name) throws IOException {
        Properties fixtures = new Properties();
        try (InputStream input = BinarySerializerTest.class.getResourceAsStream(
                                "/compatibility/2f827d6e8/" + name)) {
            Assert.assertNotNull(input);
            fixtures.load(input);
        }
        return fixtures;
    }

    @Test
    public void testVertex() {
        HugeConfig config = FakeObjects.newConfig();
        BinarySerializer ser = new BinarySerializer(config);
        HugeEdge edge = new FakeObjects().newEdge(123, 456);

        BackendEntry entry1 = ser.writeVertex(edge.sourceVertex());
        HugeVertex vertex1 = ser.readVertex(edge.graph(), entry1);
        Assert.assertEquals(edge.sourceVertex(), vertex1);
        assertCollectionEquals(edge.sourceVertex().getProperties(),
                               vertex1.getProperties());

        BackendEntry entry2 = ser.writeVertex(edge.targetVertex());
        HugeVertex vertex2 = ser.readVertex(edge.graph(), entry2);
        Assert.assertEquals(edge.targetVertex(), vertex2);
        assertCollectionEquals(edge.targetVertex().getProperties(),
                               vertex2.getProperties());

        vertex2.element().removed(true);
        Assert.assertTrue(vertex2.removed());
        BackendEntry entry3 = ser.writeVertex(vertex2);
        Assert.assertEquals(0, entry3.columnsSize());

        Assert.assertNull(ser.readVertex(edge.graph(), null));
    }

    @Test
    public void testEdge() {
        HugeConfig config = FakeObjects.newConfig();
        BinarySerializer ser = new BinarySerializer(config);

        FakeObjects objects = new FakeObjects();
        HugeEdge edge1 = objects.newEdge(123, 456);
        HugeEdge edge2 = objects.newEdge(147, 789);

        BackendEntry entry1 = ser.writeEdge(edge1);
        HugeVertex vertex1 = ser.readVertex(edge1.graph(), entry1);
        Assert.assertEquals(1, vertex1.getEdges().size());
        HugeEdge edge = vertex1.getEdges().iterator().next();
        Assert.assertEquals(edge1, edge);
        assertCollectionEquals(edge1.getProperties(), edge.getProperties());

        BackendEntry entry2 = ser.writeEdge(edge2);
        HugeVertex vertex2 = ser.readVertex(edge1.graph(), entry2);
        Assert.assertEquals(1, vertex2.getEdges().size());
        edge = vertex2.getEdges().iterator().next();
        Assert.assertEquals(edge2, edge);
        assertCollectionEquals(edge2.getProperties(), edge.getProperties());
    }

    @Test
    public void testVertexForPartition() {
        BinarySerializer ser = new BinarySerializer(true, true, true);
        HugeEdge edge = new FakeObjects().newEdge("123", "456");

        BackendEntry entry1 = ser.writeVertex(edge.sourceVertex());
        HugeVertex vertex1 = ser.readVertex(edge.graph(), entry1);
        Assert.assertEquals(edge.sourceVertex(), vertex1);
        assertCollectionEquals(edge.sourceVertex().getProperties(),
                               vertex1.getProperties());

        BackendEntry entry2 = ser.writeVertex(edge.targetVertex());
        HugeVertex vertex2 = ser.readVertex(edge.graph(), entry2);
        Assert.assertEquals(edge.targetVertex(), vertex2);
        assertCollectionEquals(edge.targetVertex().getProperties(),
                               vertex2.getProperties());

        vertex2.element().removed(true);
        Assert.assertTrue(vertex2.removed());
        BackendEntry entry3 = ser.writeVertex(vertex2);
        Assert.assertEquals(0, entry3.columnsSize());

        Assert.assertNull(ser.readVertex(edge.graph(), null));
    }

    @Test
    public void testPropertyKeyUserdataCreateTimeRoundTripsAsDate() {
        HugeConfig config = FakeObjects.newConfig();
        BinarySerializer ser = new BinarySerializer(config);

        FakeObjects objects = new FakeObjects();
        PropertyKey original = objects.newPropertyKey(IdGenerator.of(1L),
                                                      "name");
        Date created = DateUtil.parse("2026-05-14 10:11:12.345");
        original.userdata(Userdata.CREATE_TIME, created);

        BackendEntry entry = ser.writePropertyKey(original);
        PropertyKey reloaded = ser.readPropertyKey(objects.graph(), entry);

        Object value = reloaded.userdata().get(Userdata.CREATE_TIME);
        Assert.assertTrue("CREATE_TIME should be a Date after round-trip, " +
                          "was " + (value == null ? "null" : value.getClass()),
                          value instanceof Date);
        Assert.assertEquals(created, value);
    }

    @Test
    public void testPropertyKeyDefaultValueRoundTripsAsDate() {
        HugeConfig config = FakeObjects.newConfig();
        BinarySerializer ser = new BinarySerializer(config);

        FakeObjects objects = new FakeObjects();
        PropertyKey original = objects.newPropertyKey(IdGenerator.of(1L),
                                                      "name", DataType.DATE);
        Date defaultValue = DateUtil.parse("2026-05-14 10:11:12.345");
        original.userdata(Userdata.DEFAULT_VALUE, defaultValue);

        BackendEntry entry = ser.writePropertyKey(original);
        PropertyKey reloaded = ser.readPropertyKey(objects.graph(), entry);

        Object value = reloaded.defaultValue();
        Assert.assertTrue("DEFAULT_VALUE should be a Date after round-trip, " +
                          "was " + (value == null ? "null" : value.getClass()),
                          value instanceof Date);
        Assert.assertEquals(defaultValue, value);
    }

    @Test
    public void testPropertyKeySetDefaultValueRoundTripsAsDate() {
        HugeConfig config = FakeObjects.newConfig();
        BinarySerializer ser = new BinarySerializer(config);

        FakeObjects objects = new FakeObjects();
        PropertyKey original = objects.newPropertyKey(IdGenerator.of(2L),
                                                      "tags", DataType.DATE);
        original.cardinality(Cardinality.SET);

        String dateStr = "2026-05-14 10:11:12.345";
        Date expected = DateUtil.parse(dateStr);
        // ArrayList<String> with duplicates — what JSON deserialization produces
        original.userdata(Userdata.DEFAULT_VALUE, Arrays.asList(dateStr, dateStr));

        BackendEntry entry = ser.writePropertyKey(original);
        PropertyKey reloaded = ser.readPropertyKey(objects.graph(), entry);

        Object value = reloaded.defaultValue();
        Assert.assertTrue("DEFAULT_VALUE should be a Set after round-trip, was " +
                          (value == null ? "null" : value.getClass()),
                          value instanceof Set);
        Set<?> values = (Set<?>) value;
        Assert.assertEquals("duplicates must be collapsed", 1, values.size());
        Assert.assertTrue(values.contains(expected));
    }

    @Test
    public void testEdgeForPartition() {
        BinarySerializer ser = new BinarySerializer(true, true, true);

        FakeObjects objects = new FakeObjects();
        HugeEdge edge1 = objects.newEdge("123", "456");
        HugeEdge edge2 = objects.newEdge("147", "789");

        BackendEntry entry1 = ser.writeEdge(edge1);
        HugeVertex vertex1 = ser.readVertex(edge1.graph(), ser.parse(entry1));
        Assert.assertEquals(1, vertex1.getEdges().size());
        HugeEdge edge = vertex1.getEdges().iterator().next();
        Assert.assertEquals(edge1, edge);
        assertCollectionEquals(edge1.getProperties(), edge.getProperties());

        BackendEntry entry2 = ser.writeEdge(edge2);
        HugeVertex vertex2 = ser.readVertex(edge1.graph(), ser.parse(entry2));
        Assert.assertEquals(1, vertex2.getEdges().size());
        edge = vertex2.getEdges().iterator().next();
        Assert.assertEquals(edge2, edge);
        assertCollectionEquals(edge2.getProperties(), edge.getProperties());
    }
}
