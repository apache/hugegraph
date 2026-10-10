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

package org.apache.hugegraph.example;

import java.util.Arrays;
import java.util.List;

import org.apache.hugegraph.HugeFactory;
import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.exception.BackendException;
import org.apache.hugegraph.backend.tx.GraphTransaction;
import org.apache.hugegraph.meta.MetaManager;
import org.apache.hugegraph.struct.schema.EdgeLabel;
import org.apache.hugegraph.schema.SchemaManager;
import org.apache.hugegraph.testutil.Whitebox;
import org.apache.hugegraph.util.Log;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;

import com.google.common.collect.ImmutableList;

public class Example4 {

    /* This example serves a simple test of a parent-child type of EdgeLabel */
    private static final Logger LOG = Log.logger(Example4.class);

    private static final MetaManager metaManager = MetaManager.instance();

    public static void main(String[] args) {
        LOG.info(
                "Example4 start! This example serves a simple test of a parent-child type of " +
                "EdgeLabel");
        metaManager.connect("hg", MetaManager.MetaDriverType.PD,
                            null, null, null,
                            ImmutableList.of("127.0.0.1:8686"));

        HugeGraph graph = ExampleUtil.loadGraph();
        Example4.showFeatures(graph);
        Example4.loadSchema(graph);
        Example4.loadData(graph);
        Example4.testQueryEdge(graph);

        try {
            Example4.thread(graph);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        try {
            graph.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        HugeFactory.shutdown(30L);
    }

    private static void thread(HugeGraph graph) throws InterruptedException {
        Thread t = new Thread(() -> {
            // Default tx
            graph.addVertex(T.label, "book", "name", "java-11");
            graph.addVertex(T.label, "book", "name", "java-12");
            graph.tx().commit();

            // New tx
            GraphTransaction tx = Whitebox.invoke(graph.getClass(),
                                                  "openGraphTransaction",
                                                  graph);

            tx.addVertex(T.label, "book", "name", "java-21");
            tx.addVertex(T.label, "book", "name", "java-22");
            tx.commit();
            tx.close();

            // This will close the schema tx
            Whitebox.invoke(graph.getClass(), "closeTx", graph);
        });
        t.start();
        t.join();
    }

    public static void showFeatures(final HugeGraph graph) {
        LOG.info("SupportsPersistence: {}",
                 graph.features().graph().supportsPersistence());
    }

    public static void loadSchema(final HugeGraph graph) {

        SchemaManager schema = graph.schema();
        // Schema changes will be commit directly into the back-end
        LOG.info("===============  propertyKey  ================");
        schema.propertyKey("id").asInt().create();
        schema.propertyKey("name").asText().create();
        schema.propertyKey("gender").asText().create();
        schema.propertyKey("instructions").asText().create();
        schema.propertyKey("category").asText().create();
        schema.propertyKey("year").asInt().create();
        schema.propertyKey("time").asText().create();
        schema.propertyKey("timestamp").asDate().create();
        schema.propertyKey("ISBN").asText().create();
        schema.propertyKey("calories").asInt().create();
        schema.propertyKey("amount").asText().create();
        schema.propertyKey("stars").asInt().create();
        schema.propertyKey("age").asInt().valueSingle().create();
        schema.propertyKey("comment").asText().valueSet().create();
        schema.propertyKey("contribution").asText().valueSet().create();
        schema.propertyKey("nickname").asText().valueList().create();
        schema.propertyKey("lived").asText().create();
        schema.propertyKey("country").asText().valueSet().create();
        schema.propertyKey("city").asText().create();
        schema.propertyKey("sensor_id").asUUID().create();
        schema.propertyKey("versions").asInt().valueList().create();

        LOG.info("===============  vertexLabel  ================");

        schema.vertexLabel("person")
              .properties("name", "age", "city")
              .primaryKeys("name")
              .create();
        schema.vertexLabel("author")
              .properties("id", "name", "age", "lived")
              .primaryKeys("id").create();
        schema.vertexLabel("language").properties("name", "versions")
              .primaryKeys("name").create();
        schema.vertexLabel("recipe").properties("name", "instructions")
              .primaryKeys("name").create();
        schema.vertexLabel("book").properties("name")
              .primaryKeys("name").create();
        schema.vertexLabel("reviewer").properties("name", "timestamp")
              .primaryKeys("name").create();

        schema.vertexLabel("company")
              .properties("name")
              .primaryKeys("name").create();

        // vertex label must have the properties that specified in primary key
        schema.vertexLabel("FridgeSensor").properties("city")
              .primaryKeys("city").create();

        LOG.info("===============  vertexLabel & index  ================");
        schema.indexLabel("personByCity")
              .onV("person").secondary().by("city").create();
        schema.indexLabel("personByAge")
              .onV("person").range().by("age").create();

        schema.indexLabel("authorByLived")
              .onV("author").search().by("lived").create();

        LOG.info("===============  edgeLabel  ================");

        schema.edgeLabel("authored").singleTime()
              .link("author", "book")
              .properties("contribution", "comment")
              .nullableKeys("comment")
              .create();

        // Create a parent EdgeLabel
        EdgeLabel elFather = schema.edgeLabel("transfer").asBase().create();

        LOG.info(String.format("Parent type created: %s, its id is: %s",
                               elFather.name(), elFather.id().asString()));

        // Create two child EdgeLabels
        EdgeLabel son1 =
                schema.edgeLabel("transfer-1").withBase("transfer").multiTimes()
                      .link("author", "person")
                      .properties("time").sortKeys("time").create();

        LOG.info(String.format("Child type created: %s, its id is: %s, its parent id is %s",
                               son1.name(), son1.id().asString(),
                               son1.fatherId().asString()));

        EdgeLabel son2 =
                schema.edgeLabel("transfer-2").withBase("transfer").multiTimes()
                      .link("author", "company")
                      .properties("time").sortKeys("time").create();

        LOG.info(String.format("Child type created: %s, its id is: %s, its parent id is %s",
                               son2.name(), son2.id().asString(),
                               son2.fatherId().asString()));

        schema.edgeLabel("write").multiTimes().properties("time")
              .link("author", "book")
              .sortKeys("time")
              .create();

        schema.edgeLabel("look").multiTimes().properties("timestamp")
              .link("person", "book")
              .sortKeys("timestamp")
              .create();

        schema.edgeLabel("created").singleTime()
              .link("author", "language")
              .create();

        schema.edgeLabel("rated")
              .link("reviewer", "recipe")
              .create();
    }

    public static void loadData(final HugeGraph graph) {
        // will auto open tx (would not auto commit)
        graph.addVertex(T.label, "book", "name", "java-3");

        graph.addVertex(T.label, "person", "name", "Baby",
                        "city", "Hongkong", "age", 3);
        graph.addVertex(T.label, "person", "name", "James",
                        "city", "Beijing", "age", 19);
        graph.addVertex(T.label, "person", "name", "Tom Cat",
                        "city", "Beijing", "age", 20);
        graph.addVertex(T.label, "person", "name", "Lisa",
                        "city", "Beijing", "age", 20);
        graph.addVertex(T.label, "person", "name", "Hebe",
                        "city", "Taipei", "age", 21);

        graph.tx().commit();

        // must commit manually with new backend tx (independent of tinkerpop)
        GraphTransaction tx = Whitebox.invoke(graph.getClass(),
                                              "openGraphTransaction",
                                              graph);

        LOG.info("===============  addVertex  ================");
        Vertex james = tx.addVertex(T.label, "author", "id", 1,
                                    "name", "James Gosling", "age", 62,
                                    "lived", "San Francisco Bay Area");

        Vertex java = tx.addVertex(T.label, "language", "name", "java",
                                   "versions", Arrays.asList(6, 7, 8));
        Vertex book1 = tx.addVertex(T.label, "book", "name", "java-1");
        Vertex book2 = tx.addVertex(T.label, "book", "name", "java-2");
        Vertex book3 = tx.addVertex(T.label, "book", "name", "java-3");

        Vertex baidu = tx.addVertex(T.label, "company", "name", "baidu");
        Vertex yanHong = tx.addVertex(T.label, "person", "name", "yanHong",
                                      "city", "Beijing", "age", 45);

        Edge edgeTransfer1 =
                james.addEdge("transfer-1", yanHong, "time", "2022-1-1");
        james.addEdge("transfer-1", yanHong, "time", "2022-1-2");
        james.addEdge("transfer-1", yanHong, "time", "2022-1-3");

        Edge edgeTransfer2 =
                james.addEdge("transfer-2", baidu, "time", "2022-2-2");
        james.addEdge("transfer-2", baidu, "time", "2022-2-1");
        james.addEdge("transfer-2", baidu, "time", "2022-2-2");
        james.addEdge("transfer-2", baidu, "time", "2022-2-3");
        james.addEdge("created", java);
        james.addEdge("authored", book1,
                      "contribution", "1990-1-1",
                      "comment", "it's a good book",
                      "comment", "it's a good book",
                      "comment", "it's a good book too");
        james.addEdge("authored", book2, "contribution", "2017-4-28");

        Edge edge1 = james.addEdge("write", book2, "time", "2017-4-28");
        Edge edge2 = james.addEdge("write", book3, "time", "2016-1-1");
        Edge edge3 = james.addEdge("write", book3, "time", "2017-4-28");

        // commit data changes
        try {
            tx.commit();
        } catch (BackendException e) {
            e.printStackTrace();
            try {
                tx.rollback();
            } catch (BackendException e2) {
                e2.printStackTrace();
            }
        } finally {
            tx.close();
        }

        // use the manually open transaction (tinkerpop tx)
        graph.tx().open();
        graph.addVertex(T.label, "book", "name", "java-3");
        graph.addVertex(T.label, "book", "name", "java-4");
        Vertex vertex1 = graph.addVertex(T.label, "book", "name", "java-5");
        System.out.println(vertex1.id().toString());

        graph.tx().commit();
    }

    public static void testQueryEdge(final HugeGraph graph) {
        GraphTraversal<Edge, Edge> edges = graph.traversal().E();
        List<Edge> list = edges.toList();

        GraphTraversal<Vertex, Vertex> vertexs = graph.traversal().V();
        List<Vertex> list1 = vertexs.toList();

        GraphTraversal<Vertex, Edge> vertexEdgeGraphTraversal =
                graph.traversal().V("2:11").outE("write");
        LOG.info("The number of write edges is: " + vertexEdgeGraphTraversal.toList().size());

        // Three types of queries
        // First, query edges for person-to-person transfers
        GraphTraversal<Vertex, Edge> transfer1 =
                graph.traversal().V("2:11").outE("transfer-1")
                     .has("time", "2022-1-2");
        // transfer_1.toList().size();
        LOG.info("The number of person-to-person transfer edges (transfer1) for james is: " + transfer1.toList().size());

        // Second, query edges for person-to-company transfers
        GraphTraversal<Vertex, Edge> transfer2 =
                graph.traversal().V("2:11").outE("transfer-2");
        // transfer_2.toList().size();
        LOG.info("The number of person-to-company transfer edges (transfer2) for james is: " + transfer2.toList().size());

        // Third, query transfer edges
        GraphTraversal<Vertex, Edge> transfer =
                graph.traversal().V("2:11").outE("transfer");
        // transfer.toList().size();
        LOG.info("The number of transfer edges (transfer) is: " + transfer.toList().size());

        GraphTraversal<Vertex, Edge> writeAndTransfer1 =
                graph.traversal().V("2:11").outE("write", "transfer-1");
        LOG.info(
                "Mixed query: graph.traversal().V(\"2:11\").outE(\"write\", \"transfer-1\") => The total number of write and transfer1 edges is: "
                + writeAndTransfer1.toList().size());

        GraphTraversal<Vertex, Edge> writeAndTransfer1WithLimit =
                graph.traversal().V("2:11")
                     .outE("write", "transfer-1")
                     .limit(2);
        LOG.info(
                "Limited mixed query: graph.traversal().V(\"2:11\").outE(\"write\", \"transfer-1\").limit(2); => "
                + "The total number of write and transfer1 edges is: "
                + writeAndTransfer1WithLimit.toList().size());

        GraphTraversal<Vertex, Edge> res = graph.traversal().V("2:11")
                                                .outE("write", "transfer-1",
                                                      "transfer-2", "transfer");
        LOG.info(
                "Mixed query: graph.traversal().V(\"2:11\").outE(\"write\", \"transfer-1\", "
                + "\"transfer-2\", \"transfer\") The total number of edges is: "
                + res.toList().size());

        System.out.println("graph.traversal().E().hasLabel(\"write\").toList"
                           + "().size():" +
                           graph.traversal().E().hasLabel("write").toList()
                                .size());

    }
}

