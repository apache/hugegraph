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

package org.apache.hugegraph.core;

import java.util.ArrayList;
import java.util.List;

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.struct.schema.EdgeLabel;
import org.apache.hugegraph.schema.SchemaManager;
import org.apache.hugegraph.testutil.Assert;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

public class ParentAndSubEdgeCoreTest extends BaseCoreTest {

    @Before
    public void initSchema() {
        Assume.assumeTrue("Not support father and sub edge label",
                          this.storeFeatures().supportsFatherAndSubEdgeLabel());

        SchemaManager schema = graph().schema();

        LOG.debug("===============  propertyKey  ================");

        schema.propertyKey("id").asInt().create();
        schema.propertyKey("name").asText().create();
        schema.propertyKey("time").asText().create();
        schema.propertyKey("timestamp").asLong().create();
        schema.propertyKey("age").asInt().valueSingle().create();
        schema.propertyKey("comment").asText().valueSet().create();
        schema.propertyKey("contribution").asText().create();
        schema.propertyKey("score").asInt().create();
        schema.propertyKey("lived").asText().create();
        schema.propertyKey("city").asText().create();
        schema.propertyKey("amount").asFloat().create();

        LOG.debug("===============  vertexLabel  ================");

        schema.vertexLabel("person")
              .properties("name", "age", "city")
              .primaryKeys("name")
              .enableLabelIndex(false)
              .create();

        schema.vertexLabel("company")
              .properties("name", "city")
              .primaryKeys("name")
              .enableLabelIndex(false)
              .create();

        schema.vertexLabel("author")
              .properties("id", "name", "age", "lived")
              .primaryKeys("id")
              .enableLabelIndex(false)
              .create();

        LOG.debug("===============  edgeLabel  ================");

        EdgeLabel elFather =
                schema.edgeLabel("transfer").asBase()
                      .properties("time", "amount")
                      .multiTimes()
                      .sortKeys("time")
                      .create();

        EdgeLabel transfer1 = schema.edgeLabel("transfer-1")
                                    .withBase("transfer").multiTimes()
                                    .link("person", "person")
                                    .properties("time", "amount")
                                    .sortKeys("time")
                                    .create();

        EdgeLabel transfer2 = schema.edgeLabel("transfer-2")
                                    .withBase("transfer").multiTimes()
                                    .link("person", "company")
                                    .properties("time", "amount")
                                    .sortKeys("time")
                                    .create();

        EdgeLabel transfer3 = schema.edgeLabel("transfer-3")
                                    .withBase("transfer").multiTimes()
                                    .link("person", "author")
                                    .properties("time", "amount")
                                    .sortKeys("time")
                                    .create();

        graph().schema().indexLabel("transferByAmount").onE("transfer")
               .by("amount").secondary().ifNotExist().create();
        graph().schema().indexLabel("transfer-1ByAmount").onE("transfer-1")
               .by("amount").secondary().ifNotExist().create();
        graph().schema().indexLabel("transfer-2ByAmount").onE("transfer-2")
               .by("amount").secondary().ifNotExist().create();
        graph().schema().indexLabel("transfer-3ByAmount").onE("transfer-3")
               .by("amount").secondary().ifNotExist().create();

        schema.edgeLabel("know").multiTimes()
              .sourceLabel("person")
              .targetLabel("person")
              .enableLabelIndex(true)
              .properties("time")
              .sortKeys("time")
              .create();

        graph().schema().indexLabel("knowByTime").onE("know")
               .by("time").secondary().ifNotExist().create();
    }

    private List<Vertex> init10Edges() {
        HugeGraph graph = graph();

        Vertex person1 = graph.addVertex(T.label, "person",
                                         "age", 19,
                                         "city", "Beijing",
                                         "name", "person1");
        Vertex person2 = graph.addVertex(T.label, "person",
                                         "age", 20,
                                         "city", "Shanghai",
                                         "name", "person2");
        Vertex person3 = graph.addVertex(T.label, "person",
                                         "age", 19,
                                         "city", "Nanjing",
                                         "name", "person3");

        Vertex baidu = graph.addVertex(T.label, "company",
                                       "name", "Baidu",
                                       "city", "Beijing");
        Vertex huawei = graph.addVertex(T.label, "company",
                                        "name", "Huawei",
                                        "city", "Shanghai");
        Vertex tencent = graph.addVertex(T.label, "company",
                                         "name", "Tencent",
                                         "city", "Shenzhen");

        Vertex james = graph.addVertex(T.label, "author", "id", 1,
                                       "name", "James Gosling", "age", 62,
                                       "lived", "Canadian");
        Vertex guido = graph.addVertex(T.label, "author", "id", 2,
                                       "name", "Guido van Rossum", "age", 61,
                                       "lived", "California");

        person1.addEdge("transfer-1", person2, "time", "2021-11-11",
                        "amount", 9.00);
        person1.addEdge("transfer-1", person3, "time", "2021-11-22",
                        "amount", 10.00);

        person2.addEdge("transfer-1", person3, "time", "2022-1-1",
                        "amount", 10.00);
        person2.addEdge("transfer-1", person3, "time", "2022-1-2",
                        "amount", 10.00);

        person2.addEdge("transfer-2", baidu, "time", "2022-1-1",
                        "amount", 10.00);
        person2.addEdge("transfer-2", baidu, "time", "2022-1-2",
                        "amount", 10.00);
        person2.addEdge("transfer-2", baidu, "time", "2022-1-3",
                        "amount", 10.00);

        person3.addEdge("transfer-2", baidu, "time", "2022-1-4",
                        "amount", 10.00);
        person3.addEdge("transfer-2", tencent, "time", "2022-1-1",
                        "amount", 10.00);
        person3.addEdge("transfer-2", tencent, "time", "2022-1-2",
                        "amount", 9.00);

        person1.addEdge("know", person2, "time", "2022-1-1");

        graph.tx().commit();
        List<Vertex> list = new ArrayList<>();
        list.add(person1);
        list.add(person2);
        list.add(person3);
        return list;
    }

    @Test
    public void testQueryParentAndSubEdgesWithHasLabel() {
        Assume.assumeTrue("Not support father and sub edge label",
                          this.storeFeatures().supportsFatherAndSubEdgeLabel());

        // Simple hasLabel Type Query
        init10Edges();

        // normal edge
        List<Edge> edges = graph().traversal().E().hasLabel("know").toList();
        Assert.assertEquals(1, edges.size());

        // father edge
        edges = graph().traversal().E().hasLabel("transfer").toList();
        Assert.assertEquals(10, edges.size());

        // sub edge
        edges = graph().traversal().E().hasLabel("transfer-1").toList();
        Assert.assertEquals(4, edges.size());

        edges = graph().traversal().E().hasLabel("transfer-2").toList();
        Assert.assertEquals(6, edges.size());
    }

    @Test
    public void testQueryParentAndSubEdgesWithHasLabelAndConditions() {
        Assume.assumeTrue("Not support father and sub edge label",
                          this.storeFeatures().supportsFatherAndSubEdgeLabel());

        // hasLabel + Conditional Filtering Type Query
        init10Edges();

        // normal edge
        List<Edge> edges = graph().traversal().E().hasLabel("know")
                                  .has("time", "2022-1-1")
                                  .toList();

        Assert.assertEquals(1, edges.size());

        // father edge
        edges = graph().traversal().E().hasLabel("transfer").has("amount",
                                                                 10.00).toList();

        Assert.assertEquals(8, edges.size());

        // sub edge
        edges = graph().traversal().E().hasLabel("transfer-1")
                       .has("amount", 10.00)
                       .toList();
        Assert.assertEquals(3, edges.size());

        edges = graph().traversal().E().hasLabel("transfer-2")
                       .has("amount", 10.00)
                       .toList();
        Assert.assertEquals(5, edges.size());
    }

    @Test
    public void testQueryParentAndSubEdgesWithVertexOut() {
        Assume.assumeTrue("Not support father and sub edge label",
                          this.storeFeatures().supportsFatherAndSubEdgeLabel());

        // g.V("id").outE("label")
        List<Vertex> list = init10Edges();
        Vertex person1 = list.get(0);
        Vertex person2 = list.get(1);
        Vertex person3 = list.get(2);

        List<Edge> edges;
        edges = graph().traversal().V(person1.id())
                       .outE("transfer").toList();
        Assert.assertEquals(2, edges.size());

        edges = graph().traversal().V(person1.id())
                       .outE("transfer-1").toList();
        Assert.assertEquals(2, edges.size());

        edges = graph().traversal().V(person1.id())
                       .outE("transfer-2").toList();
        Assert.assertEquals(0, edges.size());

        edges = graph().traversal().V(person1.id())
                       .outE("transfer", "know").toList();
        Assert.assertEquals(2 + 1, edges.size());

        edges = graph().traversal().V(person2.id())
                       .outE("transfer-1").toList();
        Assert.assertEquals(2, edges.size());

        edges = graph().traversal().V(person2.id())
                       .outE("transfer").toList();
        Assert.assertEquals(5, edges.size());

        edges = graph().traversal().V(person2.id())
                       .outE("transfer-1").toList();
        Assert.assertEquals(2, edges.size());

        edges = graph().traversal().V(person2.id())
                       .outE("transfer-2").toList();
        Assert.assertEquals(3, edges.size());
    }

    @Test
    public void testQueryParentAndSubEdgesWithVertexOutAndConditions() {
        Assume.assumeTrue("Not support father and sub edge label",
                          this.storeFeatures().supportsFatherAndSubEdgeLabel());

        // g.V("id").outE("label").has("amount",10.00)
        List<Vertex> list = init10Edges();
        Vertex person1 = list.get(0);
        Vertex person2 = list.get(1);
        Vertex person3 = list.get(2);

        List<Edge> edges;
        edges = graph().traversal().V(person1.id())
                       .outE("transfer")
                       .has("amount", 10.00).toList();
        Assert.assertEquals(1, edges.size());

        edges = graph().traversal().V(person1.id())
                       .outE("transfer-1")
                       .has("amount", 10.00).toList();
        Assert.assertEquals(1, edges.size());
    }

    @Test
    public void testQueryParentAndSubEdgesWithVertexOutAndSortKeys() {
        Assume.assumeTrue("Not support father and sub edge label",
                          this.storeFeatures().supportsFatherAndSubEdgeLabel());

        // g.V("id").outE("label").has("sortKeys","value")
        List<Vertex> list = init10Edges();
        Vertex person1 = list.get(0);
        Vertex person2 = list.get(1);
        Vertex person3 = list.get(2);

        int transferEdges = graph().traversal().V(person2.id())
                                   .outE("transfer")
                                   .has("time", "2022-1-1").toList().size();
        Assert.assertEquals(2, transferEdges);

        int transfer1Edges = graph().traversal().V(person2.id())
                                    .outE("transfer-1")
                                    .has("time", "2022-1-1").toList().size();
        int transfer2Edges = graph().traversal().V(person2.id())
                                    .outE("transfer-2")
                                    .has("time", "2022-1-1").toList().size();
        Assert.assertEquals(transferEdges, transfer1Edges + transfer2Edges);
    }

    @Test
    public void testQueryParentAndSubEdgesWithVertexOutAndSortKeysAndConditions() {
        Assume.assumeTrue("Not support father and sub edge label",
                          this.storeFeatures().supportsFatherAndSubEdgeLabel());

        // g.V("id").outE("label").has("sortKeys","value").has(K,V)
        List<Vertex> list = init10Edges();
        Vertex person1 = list.get(0);
        Vertex person2 = list.get(1);
        Vertex person3 = list.get(2);

        int transferEdges = graph().traversal().V(person2.id())
                                   .outE("transfer")
                                   .has("time", "2022-1-1")
                                   .has("amount", 10.00)
                                   .toList().size();
        Assert.assertEquals(2, transferEdges);

        int transfer1Edges = graph().traversal().V(person2.id())
                                    .outE("transfer-1")
                                    .has("time", "2022-1-1")
                                    .has("amount", 10.00)
                                    .toList().size();
        int transfer2Edges = graph().traversal().V(person2.id())
                                    .outE("transfer-2")
                                    .has("time", "2022-1-1")
                                    .has("amount", 10.00)
                                    .toList().size();
        Assert.assertEquals(transferEdges, transfer1Edges + transfer2Edges);
    }
}
