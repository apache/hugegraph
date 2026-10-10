/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hugegraph.compatibility;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hugegraph.HugeGraphSupplier;
import org.apache.hugegraph.backend.BackendColumn;
import org.apache.hugegraph.id.EdgeId;
import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.type.define.Directions;
import org.apache.hugegraph.id.IdGenerator;
import org.apache.hugegraph.serializer.BinaryElementSerializer;
import org.apache.hugegraph.serializer.DirectBinarySerializer;
import org.apache.hugegraph.struct.schema.EdgeLabel;
import org.apache.hugegraph.struct.schema.IndexLabel;
import org.apache.hugegraph.struct.schema.PropertyKey;
import org.apache.hugegraph.struct.schema.VertexLabel;
import org.apache.hugegraph.structure.BaseEdge;
import org.apache.hugegraph.structure.BaseElement;
import org.apache.hugegraph.structure.BaseVertex;
import org.apache.hugegraph.structure.Index;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.type.define.Cardinality;
import org.apache.hugegraph.type.define.DataType;
import org.apache.hugegraph.type.define.Frequency;
import org.apache.hugegraph.type.define.IndexType;
import org.apache.hugegraph.type.define.WriteType;
import org.apache.hugegraph.util.Bytes;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/** Test complete rows written by the pinned pre-consolidation BinarySerializer. */
public class LegacyCoreRowCodecTest {

    private final BinaryElementSerializer serializer = new BinaryElementSerializer();
    private final DirectBinarySerializer direct = new DirectBinarySerializer();
    private final Map<Long, PropertyKey> properties = new HashMap<>();
    private final Map<Long, VertexLabel> vertices = new HashMap<>();
    private final Map<Long, EdgeLabel> edges = new HashMap<>();
    private final Map<Long, IndexLabel> indexes = new HashMap<>();
    private Properties rows;
    private HugeGraphSupplier graph;
    private long expiry;

    @Before
    public void prepare() throws IOException {
        this.rows = new Properties();
        try (InputStream input = this.getClass().getResourceAsStream(
                                "/compatibility/2f827d6e8/core-rows.properties")) {
            Assert.assertNotNull(input);
            this.rows.load(input);
        }
        this.expiry = Long.parseLong(this.rows.getProperty("expiry"));
        this.graph = (HugeGraphSupplier) Proxy.newProxyInstance(
                HugeGraphSupplier.class.getClassLoader(), new Class<?>[]{HugeGraphSupplier.class},
                (proxy, method, args) -> {
                    switch (method.getName()) {
                        case "now": return Long.parseLong(this.rows.getProperty("now"));
                        case "name":
                        case "spaceGraphName": return "legacy-fixture";
                        case "propertyKey": return this.properties.get(((Id) args[0]).asLong());
                        case "vertexLabel":
                        case "vertexLabelOrNone": return this.vertices.get(((Id) args[0]).asLong());
                        case "edgeLabel":
                        case "edgeLabelOrNone": return this.edges.get(((Id) args[0]).asLong());
                        case "indexLabel": return this.indexes.get(((Id) args[0]).asLong());
                        default: throw new AssertionError("Unexpected supplier call: " + method);
                    }
                });
        this.property(1L, "age", DataType.INT, Cardinality.SINGLE);
        this.property(2L, "scores", DataType.INT, Cardinality.LIST);
        this.property(3L, "groups", DataType.INT, Cardinality.SET);
        this.property(9L, "rank", DataType.INT, Cardinality.SINGLE)
            .writeType(WriteType.OLAP_COMMON);
        this.property(16L, "weight", DataType.LONG, Cardinality.SINGLE);
        this.property(17L, "name", DataType.TEXT, Cardinality.SINGLE);
        this.vertexLabel(4L, "person", 60000L);
        this.vertexLabel(10L, "timeless", 0L);
        this.edgeLabel(5L, "knows", 60000L);
        this.edgeLabel(11L, "untimed", 0L);
        this.indexLabel(6L, IndexType.SECONDARY, 4L);
        this.indexLabel(7L, IndexType.RANGE_INT, 4L);
        this.indexLabel(8L, IndexType.RANGE_LONG, 4L);
        this.indexLabel(12L, IndexType.SECONDARY, 10L);
        this.indexLabel(13L, IndexType.RANGE_INT, 10L);
        this.indexLabel(14L, IndexType.RANGE_LONG, 10L);
        this.indexLabel(15L, IndexType.SECONDARY, 4L);
    }

    @Test
    public void testLegacyVertexRowsAndDirectTtl() {
        for (String name : new String[]{"vertex", "vertex.untimed"}) {
            BackendColumn column = this.column(name);
            BaseVertex vertex = this.serializer.parseSchemaVertex(this.graph, column, null);
            this.assertProperties(vertex);
            Assert.assertEquals(name.endsWith("untimed") ? 3L : 1L, vertex.id().asLong());
            long expectedExpiry = name.endsWith("untimed") ? 0L : this.expiry;
            Assert.assertEquals(expectedExpiry, vertex.expiredTime());
            Assert.assertArrayEquals(column.value, this.serializer.formatSchemaVertexValue(vertex));
            Assert.assertEquals(expectedExpiry, this.direct.parseSchemaVertex(
                                                this.graph, column.name, column.value).expiredTime());
        }
    }

    @Test
    public void testLegacyEdgeRowsAndDirectTtl() {
        for (String name : new String[]{"edge.out", "edge.in", "edge.untimed"}) {
            BackendColumn column = this.column(name);
            BaseEdge edge = this.serializer.parseSchemaEdge(this.graph, column, null, true);
            this.assertProperties(edge);
            Assert.assertEquals(!name.equals("edge.in"), edge.isOutEdge());
            Assert.assertEquals(name.equals("edge.in") ? 2L : 1L, edge.ownerVertexId().asLong());
            long expectedExpiry = name.endsWith("untimed") ? 0L : this.expiry;
            Assert.assertEquals(expectedExpiry, edge.expiredTime());
            Assert.assertArrayEquals(column.name, this.serializer.formatEdgeName(edge));
            Assert.assertArrayEquals(column.value, this.serializer.formatSchemaEdgeValue(edge));
            Assert.assertEquals(expectedExpiry, this.direct.parseSchemaEdge(
                                                this.graph, column.name, column.value).expiredTime());
        }
    }

    @Test
    public void testLegacyVertexWithOlapMergedRow() {
        BaseVertex vertex = this.serializer.parseSchemaVertexFromCols(
                            this.graph, this.column("vertex"), this.column("olap"));
        Assert.assertEquals(1L, vertex.id().asLong());
        Assert.assertEquals(this.expiry, vertex.expiredTime());
        this.assertProperties(vertex);
        Assert.assertEquals(42, (int) vertex.getPropertyValue(IdGenerator.of(9L)));
    }

    @Test
    public void testLegacyStandaloneOlapRow() {
        BaseVertex vertex = this.serializer.parseSchemaVertexOlap(
                            this.graph, this.column("olap"), null);
        Assert.assertEquals(1L, vertex.id().asLong());
        Assert.assertEquals(42, (int) vertex.getPropertyValue(IdGenerator.of(9L)));
        Assert.assertEquals(0L, vertex.expiredTime());
    }

    @Test
    public void testSharedIndexWriterRetainsLegacyCoreBytes() {
        for (String name : new String[]{"index.secondary", "index.int", "index.long",
                                       "index.secondary.untimed", "index.int.untimed",
                                       "index.long.untimed", "index.hashed", "index.system", "index.system.edge"}) {
            BackendColumn legacy = this.column(name);
            Index decoded = this.serializer.parseSchemaIndex(this.graph, legacy);
            BackendColumn current = this.serializer.writeSchemaIndex(decoded);
            Assert.assertArrayEquals(name + " key", legacy.name, current.name);
            Assert.assertNotNull(name + " persistent value", current.value);
            Assert.assertArrayEquals(name + " value", legacy.value, current.value);
        }
    }

    @Test
    public void testOriginalStoreIndexTtlRows() throws IOException {
        Properties legacy = new Properties();
        try (InputStream input = this.getClass().getResourceAsStream(
                                "/compatibility/2f827d6e8/store-index-rows.properties")) {
            Assert.assertNotNull(input);
            legacy.load(input);
        }
        for (String name : new String[]{"secondary", "int", "long", "system", "system.edge"}) {
            BackendColumn column = BackendColumn.of(Bytes.fromHex(legacy.getProperty(name + ".name")),
                                                    Bytes.fromHex(legacy.getProperty(name + ".value")));
            Index decoded = this.serializer.parseSchemaIndex(this.graph, column);
            Id expectedElement = name.equals("system.edge") ? edgeElementId() : IdGenerator.of(1L);
            Assert.assertEquals(expectedElement, decoded.elementId());
            Assert.assertEquals(this.expiry, decoded.expiredTime());
            Assert.assertEquals(this.expiry, this.direct.parseSchemaIndex(
                                            this.graph, column.name, column.value).expiredTime());
            // The still-supported tagged writer must also retain old Store bytes.
            BackendColumn tagged = this.serializer.writeIndex(decoded);
            Assert.assertArrayEquals(column.name, tagged.name);
            Assert.assertArrayEquals(column.value, tagged.value);
            if (name.startsWith("system")) {
                BackendColumn current = this.serializer.writeSchemaIndex(decoded);
                Assert.assertArrayEquals(column.name, current.name);
                Assert.assertArrayEquals(column.value, current.value);
                Assert.assertFalse(decoded.hasTtl());
            }
        }
    }

    @Test
    public void testPositiveLabelExpiryUsesStableStorageKeyAndCoreFlag() throws IOException {
        Properties storage = new Properties();
        try (InputStream input = this.getClass().getResourceAsStream(
                                "/compatibility/2f827d6e8/store-index-rows.properties")) {
            Assert.assertNotNull(input);
            storage.load(input);
        }
        for (String name : new String[]{"system", "system.edge"}) {
            HugeType base = name.endsWith("edge") ? HugeType.EDGE : HugeType.VERTEX;
            Id elementId = name.endsWith("edge") ? edgeElementId() : IdGenerator.of(1L);
            Index index = new Index(this.graph, IndexLabel.label(base), true);
            index.fieldValues(name.endsWith("edge") ? 5L : 4L);
            index.elementIds(elementId, this.expiry);
            Assert.assertFalse(index.hasTtl());
            BackendColumn oldCore = this.column("index." + name);
            BackendColumn core = this.serializer.formatSchemaIndex(index.type(), index.indexLabelId(),
                                 index.fieldValues(), elementId, true, false, this.expiry);
            Assert.assertArrayEquals(oldCore.name, core.name);
            Assert.assertNotNull(core.value);
            Assert.assertArrayEquals(oldCore.value, core.value);
            BackendColumn stored = this.serializer.writeSchemaIndex(index);
            Assert.assertArrayEquals(Bytes.fromHex(storage.getProperty(name + ".name")), stored.name);
            Assert.assertArrayEquals(Bytes.fromHex(storage.getProperty(name + ".value")), stored.value);
            Assert.assertArrayEquals("Storage append retains Core elimination key", core.name, stored.name);
            Assert.assertEquals(this.expiry, this.direct.parseSchemaIndex(
                                            this.graph, stored.name, stored.value).expiredTime());
            Assert.assertEquals(Long.valueOf(this.expiry), this.serializer.storedLabelIndexExpiredTime(
                                                         index.type(), true, stored.value));
            Assert.assertNull(this.serializer.storedLabelIndexExpiredTime(index.type(), false, stored.value));
            Assert.assertNull(this.serializer.storedLabelIndexExpiredTime(
                              HugeType.SECONDARY_INDEX, true, stored.value));
            Assert.assertNull(this.serializer.storedLabelIndexExpiredTime(
                              index.type(), true, Arrays.copyOf(stored.value, stored.value.length + 1)));
        }
    }

    private static Id edgeElementId() {
        return new EdgeId(IdGenerator.of(1L), Directions.OUT, IdGenerator.of(5L),
                          IdGenerator.of(5L), "2026!x", IdGenerator.of(2L));
    }

    @Test
    public void testLegacyIndexRowsAndDirectTtl() {
        for (String name : new String[]{"index.secondary", "index.int", "index.long",
                                       "index.secondary.untimed", "index.int.untimed",
                                       "index.long.untimed", "index.hashed", "index.system", "index.system.edge"}) {
            BackendColumn column = this.column(name);
            Index index = this.serializer.parseSchemaIndex(this.graph, column);
            Assert.assertEquals(name.equals("index.system.edge") ? edgeElementId() : IdGenerator.of(1L),
                                index.elementId());
            long expectedExpiry = name.endsWith("untimed") || name.startsWith("index.system") ?
                                  0L : this.expiry;
            Assert.assertEquals(expectedExpiry, index.expiredTime());
            Assert.assertEquals(expectedExpiry, this.direct.parseSchemaIndex(
                                                this.graph, column.name, column.value).expiredTime());
            if (name.equals("index.hashed")) {
                Assert.assertEquals("long-field-".repeat(20), index.fieldValues());
            } else if (name.startsWith("index.secondary")) {
                Assert.assertEquals("alice", index.fieldValues());
            } else if (name.startsWith("index.int")) {
                Assert.assertEquals(7, index.fieldValues());
            } else if (name.startsWith("index.long")) {
                Assert.assertEquals(7L, index.fieldValues());
            }
        }
    }

    private void assertProperties(BaseElement element) {
        Assert.assertEquals(7, (int) element.getPropertyValue(IdGenerator.of(1L)));
        Assert.assertEquals(Arrays.asList(7, -1, 7), element.getPropertyValue(IdGenerator.of(2L)));
        Assert.assertEquals(new HashSet<>(Arrays.asList(7, -1)),
                            element.getPropertyValue(IdGenerator.of(3L)));
    }

    private BackendColumn column(String name) {
        return BackendColumn.of(Bytes.fromHex(this.rows.getProperty(name + ".column.0.name")),
                                Bytes.fromHex(this.rows.getProperty(name + ".column.0.value")));
    }

    private PropertyKey property(long id, String name, DataType type, Cardinality cardinality) {
        PropertyKey key = new PropertyKey(this.graph, IdGenerator.of(id), name);
        key.dataType(type);
        key.cardinality(cardinality);
        this.properties.put(id, key);
        return key;
    }

    private void vertexLabel(long id, String name, long ttl) {
        VertexLabel label = new VertexLabel(this.graph, IdGenerator.of(id), name);
        label.ttl(ttl);
        label.properties(IdGenerator.of(1L), IdGenerator.of(2L), IdGenerator.of(3L));
        this.vertices.put(id, label);
    }

    private void edgeLabel(long id, String name, long ttl) {
        EdgeLabel label = new EdgeLabel(this.graph, IdGenerator.of(id), name);
        label.ttl(ttl);
        label.frequency(Frequency.MULTIPLE);
        label.links(Pair.of(IdGenerator.of(4L), IdGenerator.of(4L)));
        label.properties(IdGenerator.of(1L), IdGenerator.of(2L), IdGenerator.of(3L));
        this.edges.put(id, label);
    }

    private void indexLabel(long id, IndexType type, long baseId) {
        IndexLabel label = new IndexLabel(this.graph, IdGenerator.of(id), "index-" + id);
        label.indexType(type);
        label.baseType(HugeType.VERTEX_LABEL);
        label.baseValue(IdGenerator.of(baseId));
        label.indexFields(IdGenerator.of(type == IndexType.RANGE_LONG ? 16L :
                                        type == IndexType.SECONDARY ? 17L : 1L));
        this.indexes.put(id, label);
    }
}
