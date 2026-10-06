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

package org.apache.hugegraph.unit.config;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.hugegraph.backend.id.IdGenerator;
import org.apache.hugegraph.backend.store.Shard;
import org.apache.hugegraph.backend.tx.ISchemaTransaction;
import org.apache.hugegraph.io.HugeGraphTypeSerializerRegistryBuilder;
import org.apache.hugegraph.schema.EdgeLabel;
import org.apache.hugegraph.schema.IndexLabel;
import org.apache.hugegraph.schema.PropertyKey;
import org.apache.hugegraph.schema.SchemaManager;
import org.apache.hugegraph.schema.VertexLabel;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.type.define.Cardinality;
import org.apache.hugegraph.type.define.DataType;
import org.apache.hugegraph.type.define.Directions;
import org.apache.hugegraph.type.define.Frequency;
import org.apache.hugegraph.type.define.GraphMode;
import org.apache.hugegraph.type.define.IdStrategy;
import org.apache.hugegraph.type.define.IndexType;
import org.apache.hugegraph.unit.BaseUnitTest;
import org.apache.hugegraph.unit.FakeObjects;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.util.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.util.message.ResponseStatusCode;
import org.apache.tinkerpop.gremlin.util.ser.GraphBinaryMessageSerializerV1;
import org.junit.Test;
import org.mockito.Mockito;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

public class GraphBinarySchemaCompatibilityTest extends BaseUnitTest {

    @Test
    public void testShardUsesExistingGraphSONFieldsWithStandardClient() throws Exception {
        Shard shard = new Shard("start", "end", Long.MAX_VALUE);
        Assert.assertEquals(Map.of("start", "start", "end", "end", "length", Long.MAX_VALUE),
                            roundTrip(shard));
    }

    @Test
    public void testShardsInsideListsAndMapsWithStandardClient() throws Exception {
        Shard shard = new Shard("a", "z", 1048576L);
        Map<String, Object> expected = Map.of("start", "a", "end", "z", "length", 1048576L);
        Assert.assertEquals(List.of(expected), roundTrip(List.of(shard)));
        Assert.assertEquals(Map.of("splits", List.of(expected)),
                            roundTrip(Map.of("splits", List.of(shard))));
    }

    @Test
    public void testShardPreservesEmptyAndNullBoundaries() throws Exception {
        Assert.assertEquals(Map.of("start", "", "end", "", "length", 0L),
                            roundTrip(new Shard("", "", 0L)));
        Map<?, ?> result = (Map<?, ?>) roundTrip(new Shard(null, null, Long.MAX_VALUE));
        Assert.assertEquals(3, result.size());
        Assert.assertTrue(result.containsKey("start"));
        Assert.assertTrue(result.containsKey("end"));
        Assert.assertNull(result.get("start"));
        Assert.assertNull(result.get("end"));
        Assert.assertEquals(Long.MAX_VALUE, result.get("length"));
    }

    @Test
    public void testHugeGraphEnumsUseNamesWithStandardClient() throws Exception {
        Assert.assertEquals("OUT", roundTrip(Directions.OUT));
        // DataType constants have individual enum subclasses.
        Assert.assertEquals("TEXT", roundTrip(DataType.TEXT));
        Assert.assertEquals("SINGLE", roundTrip(Cardinality.SINGLE));
        Assert.assertEquals("NONE", roundTrip(GraphMode.NONE));
        Assert.assertEquals("VERTEX_LABEL", roundTrip(HugeType.VERTEX_LABEL));
    }

    @Test
    public void testNestedEnumsPreserveNativeTinkerPopEnums() throws Exception {
        Map<Object, Object> result = Map.of(
                Directions.OUT, List.of(DataType.TEXT, Direction.OUT, Order.asc));
        Assert.assertEquals(Map.of("OUT", List.of("TEXT", Direction.OUT, Order.asc)),
                            roundTrip(result));
        Assert.assertEquals(Direction.IN, roundTrip(Direction.IN));
        Assert.assertEquals(Order.desc, roundTrip(Order.desc));
    }

    @Test
    public void testEmptySchemaManagerUsesAllFourLists() throws Exception {
        FakeObjects objects = new FakeObjects();
        SchemaManager schema = schemaManager(objects);
        Assert.assertEquals(Map.of("propertykeys", List.of(),
                                   "vertexlabels", List.of(),
                                   "edgelabels", List.of(),
                                   "indexlabels", List.of()), roundTrip(schema));
    }

    @Test
    public void testSchemaManagerConvertsAllSchemasAndHidesInternalNames() throws Exception {
        FakeObjects objects = new FakeObjects();
        PropertyKey key = objects.newPropertyKey(IdGenerator.of(1), "name");
        PropertyKey hidden = objects.newPropertyKey(IdGenerator.of(2), "~internal");
        VertexLabel vertex = objects.newVertexLabel(IdGenerator.of(3), "person",
                                                    IdStrategy.AUTOMATIC, key.id());
        EdgeLabel edge = objects.newEdgeLabel(IdGenerator.of(4), "knows", Frequency.SINGLE,
                                              vertex.id(), vertex.id(), key.id());
        IndexLabel index = objects.newIndexLabel(IdGenerator.of(5), "by_name",
                                                 HugeType.VERTEX_LABEL, vertex.id(),
                                                 IndexType.SECONDARY, key.id());
        Mockito.when(objects.graph().propertyKeys()).thenReturn(List.of(key, hidden));
        Mockito.when(objects.graph().vertexLabels()).thenReturn(List.of(vertex));
        Mockito.when(objects.graph().edgeLabels()).thenReturn(List.of(edge));
        Mockito.when(objects.graph().indexLabels()).thenReturn(List.of(index));

        Map<?, ?> result = (Map<?, ?>) roundTrip(schemaManager(objects));
        assertSchema(result, "propertykeys", "name");
        assertSchema(result, "vertexlabels", "person");
        assertSchema(result, "edgelabels", "knows");
        assertSchema(result, "indexlabels", "by_name");
        Map<?, ?> property = (Map<?, ?>) ((List<?>) result.get("propertykeys")).get(0);
        Assert.assertEquals("TEXT", property.get("data_type"));
        Assert.assertEquals("SINGLE", property.get("cardinality"));
    }

    @Test
    public void testSchemaManagerAndSchemaElementsInsideCollections() throws Exception {
        FakeObjects objects = new FakeObjects();
        PropertyKey key = objects.newPropertyKey(IdGenerator.of(1), "name");
        Mockito.when(objects.graph().propertyKeys()).thenReturn(List.of(key));
        SchemaManager schema = schemaManager(objects);
        Map<?, ?> result = (Map<?, ?>) roundTrip(Map.of("nested", List.of(schema, key)));
        List<?> nested = (List<?>) result.get("nested");
        assertSchema((Map<?, ?>) nested.get(0), "propertykeys", "name");
        Assert.assertEquals("name", ((Map<?, ?>) nested.get(1)).get("name"));
    }

    @Test
    public void testNestedSchemaMetadataKeepsStandardWireValues() throws Exception {
        FakeObjects objects = new FakeObjects();
        PropertyKey key = objects.newPropertyKey(IdGenerator.of(1), "name");
        PropertyKey child = objects.newPropertyKey(IdGenerator.of(2), "age", DataType.INT);
        key.userdata("nested", Map.of("key", child,
                                    "values", List.of(Directions.OUT, Direction.OUT, Order.asc)));
        Map<?, ?> result = (Map<?, ?>) roundTrip(key);
        Map<?, ?> metadata = (Map<?, ?>) ((Map<?, ?>) result.get("user_data")).get("nested");
        Assert.assertEquals("age", ((Map<?, ?>) metadata.get("key")).get("name"));
        Assert.assertEquals(List.of("OUT", Direction.OUT, Order.asc), metadata.get("values"));
    }

    private static SchemaManager schemaManager(FakeObjects objects) {
        return new SchemaManager(Mockito.mock(ISchemaTransaction.class), objects.graph());
    }

    private static void assertSchema(Map<?, ?> result, String type, String name) {
        List<?> schemas = (List<?>) result.get(type);
        Assert.assertEquals(1, schemas.size());
        Assert.assertEquals(name, ((Map<?, ?>) schemas.get(0)).get("name"));
    }

    private static Object roundTrip(Object value) throws Exception {
        GraphBinaryMessageSerializerV1 server = new GraphBinaryMessageSerializerV1();
        server.configure(Map.of("builder", HugeGraphTypeSerializerRegistryBuilder.class.getName()),
                         Collections.emptyMap());
        GraphBinaryMessageSerializerV1 client = new GraphBinaryMessageSerializerV1();
        ResponseMessage response = ResponseMessage.build(UUID.randomUUID())
                                                  .code(ResponseStatusCode.SUCCESS)
                                                  .result(value)
                                                  .create();
        ByteBuf buffer = server.serializeResponseAsBinary(response, ByteBufAllocator.DEFAULT);
        try {
            return client.deserializeResponse(buffer).getResult().getData();
        } finally {
            buffer.release();
        }
    }
}
