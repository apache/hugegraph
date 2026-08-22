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

package org.apache.hugegraph.unit.util;

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hugegraph.backend.id.EdgeId;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.id.IdGenerator;
import org.apache.hugegraph.schema.EdgeLabel;
import org.apache.hugegraph.schema.IndexLabel;
import org.apache.hugegraph.schema.PropertyKey;
import org.apache.hugegraph.schema.VertexLabel;
import org.apache.hugegraph.structure.HugeEdge;
import org.apache.hugegraph.structure.HugeEdgeProperty;
import org.apache.hugegraph.structure.HugeProperty;
import org.apache.hugegraph.structure.HugeVertex;
import org.apache.hugegraph.structure.HugeVertexProperty;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.testutil.Utils;
import org.apache.hugegraph.testutil.Whitebox;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.type.define.Cardinality;
import org.apache.hugegraph.type.define.DataType;
import org.apache.hugegraph.type.define.Directions;
import org.apache.hugegraph.type.define.Frequency;
import org.apache.hugegraph.type.define.IdStrategy;
import org.apache.hugegraph.type.define.IndexType;
import org.apache.hugegraph.unit.BaseUnitTest;
import org.apache.hugegraph.unit.FakeObjects;
import org.apache.hugegraph.util.JsonUtil;
import org.apache.hugegraph.util.collection.CollectionFactory;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.MutablePath;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Tree;
import org.apache.tinkerpop.shaded.jackson.core.type.TypeReference;
import org.eclipse.collections.api.map.primitive.MutableIntObjectMap;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableList;

public class JsonUtilTest extends BaseUnitTest {

    @Test
    public void testSerializeStringId() {
        Id id = IdGenerator.of("123456");
        String json = JsonUtil.toJson(id);
        Assert.assertEquals("\"123456\"", json);
    }

    @Test
    public void testSerializeLongId() {
        Id id = IdGenerator.of(123456L);
        String json = JsonUtil.toJson(id);
        Assert.assertEquals("123456", json);
    }

    @Test
    public void testSerializeUuidId() {
        UUID uuid = UUID.randomUUID();
        Id id = IdGenerator.of(uuid);
        String json = JsonUtil.toJson(id);
        Assert.assertEquals("\"" + uuid + "\"", json);
    }

    @Test
    public void testSerializeEdgeId() {
        Id id = new EdgeId(IdGenerator.of("1:marko"), Directions.OUT,
                           IdGenerator.of(1),
                           IdGenerator.of(1), "",
                           IdGenerator.of("1:josh"));
        String json = JsonUtil.toJson(id);
        Assert.assertEquals("\"S1:marko>1>1>>S1:josh\"", json);
    }

    @Test
    public void testSerializePropertyKey() {
        FakeObjects fakeObject = new FakeObjects();
        PropertyKey name = fakeObject.newPropertyKey(IdGenerator.of(1), "name");
        String json = JsonUtil.toJson(name);
        Assert.assertEquals("{\"id\":1,\"name\":\"name\"," +
                            "\"data_type\":\"TEXT\"," +
                            "\"cardinality\":\"SINGLE\"," +
                            "\"aggregate_type\":\"NONE\"," +
                            "\"write_type\":\"OLTP\"," +
                            "\"properties\":[],\"status\":\"CREATED\"," +
                            "\"user_data\":{}}", json);

        PropertyKey rate = fakeObject.newPropertyKey(IdGenerator.of(2), "rate",
                                                     DataType.INT,
                                                     Cardinality.LIST);
        json = JsonUtil.toJson(rate);
        Assert.assertEquals("{\"id\":2,\"name\":\"rate\"," +
                            "\"data_type\":\"INT\",\"cardinality\":\"LIST\"," +
                            "\"aggregate_type\":\"NONE\"," +
                            "\"write_type\":\"OLTP\"," +
                            "\"properties\":[],\"status\":\"CREATED\"," +
                            "\"user_data\":{}}", json);
    }

    @Test
    public void testSerializeVertexLabel() {
        FakeObjects fakeObject = new FakeObjects();
        PropertyKey name = fakeObject.newPropertyKey(IdGenerator.of(1), "name");
        PropertyKey age = fakeObject.newPropertyKey(IdGenerator.of(2), "age",
                                                    DataType.INT,
                                                    Cardinality.SINGLE);
        PropertyKey city = fakeObject.newPropertyKey(IdGenerator.of(3), "city");

        VertexLabel vl = fakeObject.newVertexLabel(IdGenerator.of(1), "person",
                                                   IdStrategy.CUSTOMIZE_NUMBER,
                                                   name.id(), age.id(),
                                                   city.id());
        Mockito.when(fakeObject.graph().mapPkId2Name(vl.properties()))
               .thenReturn(Arrays.asList(name.name(), age.name(), city.name()));

        String json = JsonUtil.toJson(vl);
        Assert.assertEquals("{\"id\":1,\"name\":\"person\"," +
                            "\"id_strategy\":\"CUSTOMIZE_NUMBER\"," +
                            "\"primary_keys\":[],\"nullable_keys\":[]," +
                            "\"index_labels\":[]," +
                            "\"properties\":[\"name\",\"age\",\"city\"]," +
                            "\"status\":\"CREATED\"," +
                            "\"ttl\":0,\"enable_label_index\":true," +
                            "\"user_data\":{}}", json);
    }

    @Test
    public void testSerializeEdgeLabel() {
        FakeObjects fakeObject = new FakeObjects();
        PropertyKey name = fakeObject.newPropertyKey(IdGenerator.of(1), "name");
        PropertyKey age = fakeObject.newPropertyKey(IdGenerator.of(2), "age",
                                                    DataType.INT,
                                                    Cardinality.SINGLE);
        PropertyKey city = fakeObject.newPropertyKey(IdGenerator.of(3), "city");
        PropertyKey date = fakeObject.newPropertyKey(IdGenerator.of(4), "date",
                                                     DataType.DATE);
        PropertyKey weight = fakeObject.newPropertyKey(IdGenerator.of(5),
                                                       "weight",
                                                       DataType.DOUBLE);

        VertexLabel vl = fakeObject.newVertexLabel(IdGenerator.of(1), "person",
                                                   IdStrategy.CUSTOMIZE_NUMBER,
                                                   name.id(), age.id(),
                                                   city.id());

        EdgeLabel el = fakeObject.newEdgeLabel(IdGenerator.of(1), "knows",
                                               Frequency.SINGLE,
                                               vl.id(), vl.id(),
                                               date.id(), weight.id());

        Mockito.when(fakeObject.graph().vertexLabel(vl.id())).thenReturn(vl);
        Mockito.when(fakeObject.graph().mapPkId2Name(el.properties()))
               .thenReturn(Arrays.asList(date.name(), weight.name()));
        Mockito.when(fakeObject.graph().mapPairId2Name(el.links()))
               .thenReturn(Collections.singleton(Pair.of(name.name(), name.name())));

        String json = JsonUtil.toJson(el);
        Assert.assertEquals("{\"id\":1,\"name\":\"knows\"," +
                            "\"edgelabel_type\":\"NORMAL\"," +
                            "\"source_label\":\"person\"," +
                            "\"target_label\":\"person\"," +
                            "\"links\":[{\"name\":\"name\"}]," +
                            "\"frequency\":\"SINGLE\",\"sort_keys\":[]," +
                            "\"nullable_keys\":[],\"index_labels\":[]," +
                            "\"properties\":[\"date\",\"weight\"]," +
                            "\"status\":\"CREATED\"," +
                            "\"ttl\":0,\"enable_label_index\":true," +
                            "\"user_data\":{}}", json);
    }

    @Test
    public void testSerializeIndexLabel() {
        FakeObjects fakeObject = new FakeObjects();
        PropertyKey name = fakeObject.newPropertyKey(IdGenerator.of(1), "name");
        PropertyKey age = fakeObject.newPropertyKey(IdGenerator.of(2), "age",
                                                    DataType.INT,
                                                    Cardinality.SINGLE);
        PropertyKey city = fakeObject.newPropertyKey(IdGenerator.of(3), "city");

        VertexLabel vl = fakeObject.newVertexLabel(IdGenerator.of(1), "person",
                                                   IdStrategy.CUSTOMIZE_NUMBER,
                                                   name.id(), age.id(),
                                                   city.id());

        IndexLabel il = fakeObject.newIndexLabel(IdGenerator.of(1),
                                                 "personByAgeAndCity",
                                                 HugeType.VERTEX_LABEL,
                                                 vl.id(),
                                                 IndexType.SECONDARY,
                                                 age.id(), city.id());

        Mockito.when(fakeObject.graph().vertexLabel(vl.id())).thenReturn(vl);
        Mockito.when(fakeObject.graph().mapPkId2Name(il.indexFields()))
               .thenReturn(Arrays.asList(age.name(), city.name()));

        String json = JsonUtil.toJson(il);
        Assert.assertEquals("{\"id\":1," +
                            "\"name\":\"personByAgeAndCity\"," +
                            "\"base_type\":\"VERTEX_LABEL\"," +
                            "\"base_value\":\"person\"," +
                            "\"index_type\":\"SECONDARY\"," +
                            "\"fields\":[\"age\",\"city\"]," +
                            "\"status\":\"CREATED\"," +
                            "\"user_data\":{}}", json);
    }

    @Test
    public void testSerializeVertexWithNumberId() {
        FakeObjects fakeObject = new FakeObjects();
        PropertyKey name = fakeObject.newPropertyKey(IdGenerator.of(1), "name");
        PropertyKey age = fakeObject.newPropertyKey(IdGenerator.of(2), "age",
                                                    DataType.INT,
                                                    Cardinality.SINGLE);
        PropertyKey city = fakeObject.newPropertyKey(IdGenerator.of(3), "city");

        VertexLabel vl = fakeObject.newVertexLabel(IdGenerator.of(1), "person",
                                                   IdStrategy.CUSTOMIZE_NUMBER,
                                                   name.id(), age.id(),
                                                   city.id());

        Id id = IdGenerator.of(123456L);
        HugeVertex vertex = new HugeVertex(fakeObject.graph(), id, vl);

        MutableIntObjectMap<HugeProperty<?>> properties =
                CollectionFactory.newIntObjectMap(
                        name.id(), new HugeVertexProperty<>(vertex, name, "marko"),
                        age.id(), new HugeVertexProperty<>(vertex, age, 29),
                        city.id(), new HugeVertexProperty<>(vertex, city, "Beijing")
                );
        Whitebox.setInternalState(vertex, "properties", properties);

        String json = JsonUtil.toJson(vertex);
        Assert.assertEquals("{\"id\":123456,\"label\":\"person\"," +
                            "\"type\":\"vertex\",\"properties\":{\"" +
                            "name\":\"marko\",\"age\":29," +
                            "\"city\":\"Beijing\"}}", json);
    }

    @Test
    public void testSerializeEdge() {
        FakeObjects fakeObject = new FakeObjects();
        PropertyKey name = fakeObject.newPropertyKey(IdGenerator.of(1), "name");
        PropertyKey age = fakeObject.newPropertyKey(IdGenerator.of(2), "age",
                                                    DataType.INT,
                                                    Cardinality.SINGLE);
        PropertyKey city = fakeObject.newPropertyKey(IdGenerator.of(3), "city");
        PropertyKey date = fakeObject.newPropertyKey(IdGenerator.of(4), "date",
                                                     DataType.DATE);
        PropertyKey weight = fakeObject.newPropertyKey(IdGenerator.of(5),
                                                       "weight",
                                                       DataType.DOUBLE);

        VertexLabel vl = fakeObject.newVertexLabel(IdGenerator.of(1), "person",
                                                   IdStrategy.CUSTOMIZE_NUMBER,
                                                   name.id(), age.id(),
                                                   city.id());

        EdgeLabel el = fakeObject.newEdgeLabel(IdGenerator.of(1), "knows",
                                               Frequency.SINGLE,
                                               vl.id(), vl.id(),
                                               date.id(), weight.id());

        HugeVertex source = new HugeVertex(fakeObject.graph(),
                                           IdGenerator.of(123456), vl);
        HugeVertex target = new HugeVertex(fakeObject.graph(),
                                           IdGenerator.of(987654), vl);

        Id id = EdgeId.parse("L123456>1>1>>L987654");
        HugeEdge edge = new HugeEdge(fakeObject.graph(), id, el);
        Whitebox.setInternalState(edge, "sourceVertex", source);
        Whitebox.setInternalState(edge, "targetVertex", target);

        Date dateValue = Utils.date("2019-03-12");
        MutableIntObjectMap<HugeProperty<?>> properties =
                CollectionFactory.newIntObjectMap(
                        date.id(), new HugeEdgeProperty<>(edge, date, dateValue),
                        weight.id(), new HugeEdgeProperty<>(edge, weight, 0.8)
                );
        Whitebox.setInternalState(edge, "properties", properties);

        String json = JsonUtil.toJson(edge);
        Assert.assertEquals("{\"id\":\"L123456>1>1>>L987654\"," +
                            "\"label\":\"knows\",\"type\":\"edge\"," +
                            "\"outV\":123456,\"outVLabel\":\"person\"," +
                            "\"inV\":987654,\"inVLabel\":\"person\"," +
                            "\"properties\":{\"date\":" +
                            "\"2019-03-12 00:00:00.000\"," +
                            "\"weight\":0.8}}", json);
    }

    @Test
    public void testSerializePath() {
        Path path = MutablePath.make()
                               .extend("marko", Set.of("a"))
                               .extend(29, Set.of("b"));

        String json = JsonUtil.toJson(path);

        Assert.assertEquals("{\"labels\":[[\"a\"],[\"b\"]]," +
                            "\"objects\":[\"marko\",29]}", json);
    }

    @Test
    public void testSerializeTree() {
        Tree<String> child = new Tree<>();
        child.put("lop", new Tree<>());
        Tree<String> tree = new Tree<>();
        tree.put("marko", child);

        String json = JsonUtil.toJson(tree);

        Assert.assertEquals("[{\"key\":\"marko\",\"value\":[{" +
                            "\"key\":\"lop\",\"value\":[]}]}]", json);
    }

    @Test
    public void testDeserializeList() {
        String json = "[\"1\", \"2\", \"3\"]";
        TypeReference<?> typeRef = new TypeReference<List<Integer>>() {
        };
        Assert.assertEquals(ImmutableList.of(1, 2, 3),
                            JsonUtil.fromJson(json, typeRef));
    }
}
