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

package org.apache.hugegraph.struct.schema;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.hugegraph.id.EdgeId;
import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.id.IdGenerator;
import org.apache.hugegraph.id.SplicingIdGenerator;
import org.apache.hugegraph.query.Condition;
import org.apache.hugegraph.query.IdQuery;
import org.apache.hugegraph.structure.BaseEdge;
import org.apache.hugegraph.structure.BaseVertex;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.type.define.HugeKeys;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;

public class OrderedSchemaFieldsTest {

    @Test
    public void testPrimaryKeyOrderDrivesVertexIdAndQuery() throws Exception {
        VertexLabel label = VertexLabel.fromMap(map("{\"id\":1,\"name\":\"person\"," +
                                                    "\"idStrategy\":\"PRIMARY_KEY\"," +
                                                    "\"primaryKeys\":[2,17]}"), null);
        Assert.assertEquals(Arrays.asList(id(2), id(17)), label.primaryKeys());
        BaseVertex vertex = new BaseVertex(null, label);
        vertex.addProperty(key(17), "first");
        vertex.addProperty(key(2), "second");
        Assert.assertEquals(Arrays.asList("second", "first"), vertex.primaryValues());
        Assert.assertEquals("second!first", vertex.name());
        Id expected = IdGenerator.of("1:second!first");
        vertex.id(SplicingIdGenerator.instance().generate(vertex));
        Assert.assertEquals(expected, vertex.id());
        IdQuery query = new IdQuery(HugeType.VERTEX);
        query.query(expected);
        Assert.assertTrue(query.test(vertex));
    }

    @Test
    public void testSortKeyOrderDrivesEdgeIdAndQuery() throws Exception {
        EdgeLabel label = EdgeLabel.fromMap(map("{\"id\":3,\"name\":\"knows\"," +
                                                "\"sortKeys\":[2,17],\"links\":[{\"1\":2}]}"), null);
        Assert.assertEquals(Arrays.asList(id(2), id(17)), label.sortKeys());
        BaseEdge edge = new BaseEdge(label, true);
        edge.addProperty(key(17), "first");
        edge.addProperty(key(2), "second");
        edge.vertices(true, new BaseVertex(id(1)), new BaseVertex(id(2)));
        edge.assignId();
        Assert.assertEquals(Arrays.asList("second", "first"), edge.sortValues());
        Assert.assertEquals("second!first", ((EdgeId) edge.id()).sortValues());
        Assert.assertTrue(Condition.eq(HugeKeys.SORT_VALUES, "second!first").test(edge));
        Assert.assertFalse(Condition.eq(HugeKeys.SORT_VALUES, "first!second").test(edge));
    }

    @Test
    public void testWriterAcceptedRepeatedKeysAreNotSilentlyDiscarded() throws Exception {
        VertexLabel vertex = VertexLabel.fromMap(map("{\"id\":1,\"name\":\"person\"," +
                                                      "\"primaryKeys\":[2,17,2]}"), null);
        EdgeLabel edge = EdgeLabel.fromMap(map("{\"id\":3,\"name\":\"knows\"," +
                                                "\"sortKeys\":[2,17,2]}"), null);
        List<Id> expected = Arrays.asList(id(2), id(17), id(2));
        Assert.assertEquals(expected, vertex.primaryKeys());
        Assert.assertEquals(expected, edge.sortKeys());
    }

    private static Id id(int value) {
        return IdGenerator.of(value);
    }

    private static PropertyKey key(int value) {
        return new PropertyKey(null, id(value), "key" + value);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> map(String json) throws Exception {
        return new ObjectMapper().readValue(json, Map.class);
    }
}
