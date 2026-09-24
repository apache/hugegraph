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

import java.io.File;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.Objects;

import org.apache.commons.io.FileUtils;
import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.id.IdGenerator;
import org.apache.hugegraph.backend.query.Query;
import org.apache.hugegraph.backend.store.ram.RamTable;
import org.apache.hugegraph.backend.tx.GraphTransaction;
import org.apache.hugegraph.schema.SchemaManager;
import org.apache.hugegraph.structure.HugeEdge;
import org.apache.hugegraph.structure.HugeVertex;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.testutil.Whitebox;
import org.apache.hugegraph.type.define.Directions;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

public class RamTableTest extends BaseCoreTest {

    private Object ramtable;

    @Override
    @Before
    public void setup() {
        super.setup();

        HugeGraph graph = this.graph();

        Assume.assumeTrue("Ramtable is not supported by backend",
                          graph.backendStoreFeatures().supportsScanKeyPrefix());
        this.ramtable = Whitebox.getInternalState(graph, "ramtable");
        if (this.ramtable == null) {
            Whitebox.setInternalState(graph, "ramtable",
                                      new RamTable(graph, 2000, 1200));
        }

        graph.schema().vertexLabel("vl1").useCustomizeNumberId().create();
        graph.schema().vertexLabel("vl2").useCustomizeNumberId().create();
        graph.schema().edgeLabel("el1")
             .sourceLabel("vl1")
             .targetLabel("vl1")
             .create();
        graph.schema().edgeLabel("el2")
             .sourceLabel("vl2")
             .targetLabel("vl2")
             .create();
    }

    @Override
    @After
    public void teardown() throws Exception {
        super.teardown();

        File export = Paths.get(RamTable.EXPORT_PATH).toFile();
        if (export.exists()) {
            FileUtils.forceDelete(export);
        }

        HugeGraph graph = this.graph();
        Whitebox.setInternalState(graph, "ramtable", this.ramtable);
    }

    @Test
    public void testReloadAndQuery() throws Exception {
        // FIXME: The legacy HStore guard and related coverage debt are tracked in
        // https://github.com/apache/hugegraph/issues/3090
        Assume.assumeTrue("skip this test for hstore",
                          Objects.equals("hstore", System.getProperty("backend")));

        HugeGraph graph = this.graph();

        // insert vertices and edges
        for (int i = 0; i < 100; i++) {
            Vertex v1 = graph.addVertex(T.label, "vl1", T.id, i);
            Vertex v2 = graph.addVertex(T.label, "vl1", T.id, i + 100);
            v1.addEdge("el1", v2);
        }
        graph.tx().commit();

        for (int i = 1000; i < 1100; i++) {
            Vertex v1 = graph.addVertex(T.label, "vl2", T.id, i);
            Vertex v2 = graph.addVertex(T.label, "vl2", T.id, i + 100);
            v1.addEdge("el2", v2);
        }
        graph.tx().commit();

        // reload ramtable
        Whitebox.invoke(graph.getClass(), "reloadRamtable", graph);

        // query edges
        for (int i = 0; i < 100; i++) {
            Iterator<Edge> edges = this.edgesOfVertex(IdGenerator.of(i),
                                                      Directions.OUT, null);
            Assert.assertTrue(edges.hasNext());
            HugeEdge edge = (HugeEdge) edges.next();
            Assert.assertEquals(i + 100, edge.id().otherVertexId().asLong());
            Assert.assertEquals(Directions.OUT, edge.direction());
            Assert.assertEquals("el1", edge.label());

            Assert.assertFalse(edges.hasNext());
        }
        for (int i = 1000; i < 1100; i++) {
            Iterator<Edge> edges = this.edgesOfVertex(IdGenerator.of(i),
                                                      Directions.OUT, null);
            Assert.assertTrue(edges.hasNext());
            HugeEdge edge = (HugeEdge) edges.next();
            Assert.assertEquals(i + 100, edge.id().otherVertexId().asLong());
            Assert.assertEquals(Directions.OUT, edge.direction());
            Assert.assertEquals("el2", edge.label());

            Assert.assertFalse(edges.hasNext());
        }
    }

    @Test
    public void testReloadFromFileAndQuery() throws Exception {
        // FIXME: The legacy HStore guard and related coverage debt are tracked in
        // https://github.com/apache/hugegraph/issues/3090
        Assume.assumeTrue("skip this test for hstore",
                          Objects.equals("hstore", System.getProperty("backend")));

        HugeGraph graph = this.graph();

        // insert vertices and edges
        for (int i = 0; i < 100; i++) {
            Vertex v1 = graph.addVertex(T.label, "vl1", T.id, i);
            Vertex v2 = graph.addVertex(T.label, "vl1", T.id, i + 100);
            v1.addEdge("el1", v2);
        }
        graph.tx().commit();

        for (int i = 1000; i < 1100; i++) {
            Vertex v1 = graph.addVertex(T.label, "vl2", T.id, i);
            Vertex v2 = graph.addVertex(T.label, "vl2", T.id, i + 100);
            v1.addEdge("el2", v2);
        }
        graph.tx().commit();

        // reload ramtable
        Whitebox.invoke(graph.getClass(), "reloadRamtable", graph);

        // query edges
        for (int i = 0; i < 100; i++) {
            Iterator<Edge> edges = this.edgesOfVertex(IdGenerator.of(i),
                                                      Directions.OUT, null);
            Assert.assertTrue(edges.hasNext());
            HugeEdge edge = (HugeEdge) edges.next();
            Assert.assertEquals(i + 100, edge.id().otherVertexId().asLong());
            Assert.assertEquals(Directions.OUT, edge.direction());
            Assert.assertEquals("el1", edge.label());

            Assert.assertFalse(edges.hasNext());
        }
        for (int i = 1000; i < 1100; i++) {
            Iterator<Edge> edges = this.edgesOfVertex(IdGenerator.of(i),
                                                      Directions.OUT, null);
            Assert.assertTrue(edges.hasNext());
            HugeEdge edge = (HugeEdge) edges.next();
            Assert.assertEquals(i + 100, edge.id().otherVertexId().asLong());
            Assert.assertEquals(Directions.OUT, edge.direction());
            Assert.assertEquals("el2", edge.label());

            Assert.assertFalse(edges.hasNext());
        }

        // reload ramtable from file
        Whitebox.invoke(graph.getClass(), "reloadRamtable", graph, true);

        // query edges again
        for (int i = 0; i < 100; i++) {
            Iterator<Edge> edges = this.edgesOfVertex(IdGenerator.of(i),
                                                      Directions.OUT, null);
            Assert.assertTrue(edges.hasNext());
            HugeEdge edge = (HugeEdge) edges.next();
            Assert.assertEquals(i + 100, edge.id().otherVertexId().asLong());
            Assert.assertEquals(Directions.OUT, edge.direction());
            Assert.assertEquals("el1", edge.label());

            Assert.assertFalse(edges.hasNext());
        }
        for (int i = 1000; i < 1100; i++) {
            Iterator<Edge> edges = this.edgesOfVertex(IdGenerator.of(i),
                                                      Directions.OUT, null);
            Assert.assertTrue(edges.hasNext());
            HugeEdge edge = (HugeEdge) edges.next();
            Assert.assertEquals(i + 100, edge.id().otherVertexId().asLong());
            Assert.assertEquals(Directions.OUT, edge.direction());
            Assert.assertEquals("el2", edge.label());

            Assert.assertFalse(edges.hasNext());
        }
    }

    @Test
    public void testReloadAndQueryWithMultiEdges() throws Exception {
        // FIXME: The legacy HStore guard and related coverage debt are tracked in
        // https://github.com/apache/hugegraph/issues/3090
        Assume.assumeTrue("skip this test for hstore",
                          Objects.equals("hstore", System.getProperty("backend")));

        HugeGraph graph = this.graph();

        // insert vertices and edges
        for (int i = 0; i < 100; i++) {
            Vertex v1 = graph.addVertex(T.label, "vl1", T.id, i);
            Vertex v2 = graph.addVertex(T.label, "vl1", T.id, i + 100);
            Vertex v3 = graph.addVertex(T.label, "vl1", T.id, i + 200);
            v1.addEdge("el1", v2);
            v1.addEdge("el1", v3);
            v3.addEdge("el1", v1);
        }
        graph.tx().commit();

        for (int i = 1000; i < 1100; i++) {
            Vertex v1 = graph.addVertex(T.label, "vl2", T.id, i);
            Vertex v2 = graph.addVertex(T.label, "vl2", T.id, i + 100);
            Vertex v3 = graph.addVertex(T.label, "vl2", T.id, i + 200);
            v1.addEdge("el2", v2);
            v1.addEdge("el2", v3);
            v2.addEdge("el2", v3);
        }
        graph.tx().commit();

        // reload ramtable
        Whitebox.invoke(graph.getClass(), "reloadRamtable", graph);

        // query edges by OUT
        for (int i = 0; i < 100; i++) {
            Iterator<Edge> edges = this.edgesOfVertex(IdGenerator.of(i),
                                                      Directions.OUT, null);
            Assert.assertTrue(edges.hasNext());
            HugeEdge edge = (HugeEdge) edges.next();
            Assert.assertEquals(i + 100, edge.id().otherVertexId().asLong());
            Assert.assertEquals(Directions.OUT, edge.direction());
            Assert.assertEquals("el1", edge.label());

            Assert.assertTrue(edges.hasNext());
            edge = (HugeEdge) edges.next();
            Assert.assertEquals(i + 200, edge.id().otherVertexId().asLong());
            Assert.assertEquals(Directions.OUT, edge.direction());
            Assert.assertEquals("el1", edge.label());

            Assert.assertFalse(edges.hasNext());
        }
        // query edges by BOTH
        for (int i = 0; i < 100; i++) {
            Iterator<Edge> edges = this.edgesOfVertex(IdGenerator.of(i),
                                                      Directions.BOTH, null);
            Assert.assertTrue(edges.hasNext());
            HugeEdge edge = (HugeEdge) edges.next();
            Assert.assertEquals(i + 100, edge.id().otherVertexId().asLong());
            Assert.assertEquals(Directions.OUT, edge.direction());
            Assert.assertEquals("el1", edge.label());

            Assert.assertTrue(edges.hasNext());
            edge = (HugeEdge) edges.next();
            Assert.assertEquals(i + 200, edge.id().otherVertexId().asLong());
            Assert.assertEquals(Directions.OUT, edge.direction());
            Assert.assertEquals("el1", edge.label());

            Assert.assertTrue(edges.hasNext());
            edge = (HugeEdge) edges.next();
            Assert.assertEquals(i + 200, edge.id().otherVertexId().asLong());
            Assert.assertEquals(Directions.IN, edge.direction());
            Assert.assertEquals("el1", edge.label());

            Assert.assertFalse(edges.hasNext());
        }
        // query edges by IN
        for (int i = 0; i < 100; i++) {
            Iterator<Edge> edges = this.edgesOfVertex(IdGenerator.of(i),
                                                      Directions.IN, null);
            Assert.assertTrue(edges.hasNext());
            HugeEdge edge = (HugeEdge) edges.next();
            Assert.assertEquals(i + 200, edge.id().otherVertexId().asLong());
            Assert.assertEquals(Directions.IN, edge.direction());
            Assert.assertEquals("el1", edge.label());

            Assert.assertFalse(edges.hasNext());
        }

        // query edges by OUT
        for (int i = 1000; i < 1100; i++) {
            Iterator<Edge> edges = this.edgesOfVertex(IdGenerator.of(i),
                                                      Directions.OUT, null);
            Assert.assertTrue(edges.hasNext());
            HugeEdge edge = (HugeEdge) edges.next();
            Assert.assertEquals(i + 100, edge.id().otherVertexId().asLong());
            Assert.assertEquals(Directions.OUT, edge.direction());
            Assert.assertEquals("el2", edge.label());

            Assert.assertTrue(edges.hasNext());
            edge = (HugeEdge) edges.next();
            Assert.assertEquals(i + 200, edge.id().otherVertexId().asLong());
            Assert.assertEquals(Directions.OUT, edge.direction());
            Assert.assertEquals("el2", edge.label());

            Assert.assertFalse(edges.hasNext());
        }
        // query edges by BOTH
        for (int i = 1000; i < 1100; i++) {
            Iterator<Edge> edges = this.edgesOfVertex(IdGenerator.of(i),
                                                      Directions.BOTH, null);
            Assert.assertTrue(edges.hasNext());
            HugeEdge edge = (HugeEdge) edges.next();
            Assert.assertEquals(i + 100, edge.id().otherVertexId().asLong());
            Assert.assertEquals(Directions.OUT, edge.direction());
            Assert.assertEquals("el2", edge.label());

            Assert.assertTrue(edges.hasNext());
            edge = (HugeEdge) edges.next();
            Assert.assertEquals(i + 200, edge.id().otherVertexId().asLong());
            Assert.assertEquals(Directions.OUT, edge.direction());
            Assert.assertEquals("el2", edge.label());

            Assert.assertFalse(edges.hasNext());
        }
        // query edges by IN
        for (int i = 1000; i < 1100; i++) {
            Iterator<Edge> edges = this.edgesOfVertex(IdGenerator.of(i),
                                                      Directions.IN, null);
            Assert.assertFalse(edges.hasNext());
        }
    }

    @Test
    public void testReloadAndQueryWithBigVertex() throws Exception {
        // FIXME: The legacy HStore guard and related coverage debt are tracked in
        // https://github.com/apache/hugegraph/issues/3090
        Assume.assumeTrue("skip this test for hstore",
                          Objects.equals("hstore", System.getProperty("backend")));

        HugeGraph graph = this.graph();

        // only enable this test when ram > 20G
        boolean enableBigRamTest = false;
        long big1 = 2400000000L;
        long big2 = 4200000000L;
        if (!enableBigRamTest) {
            big1 = 100L;
            big2 = 1000L;
        }

        // insert vertices and edges
        for (int i = 0; i < 100; i++) {
            Vertex v1 = graph.addVertex(T.label, "vl1", T.id, i + big1);
            Vertex v2 = graph.addVertex(T.label, "vl1", T.id, i + big1 + 100);
            v1.addEdge("el1", v2);
        }
        graph.tx().commit();

        for (int i = 0; i < 100; i++) {
            Vertex v1 = graph.addVertex(T.label, "vl2", T.id, i + big2);
            Vertex v2 = graph.addVertex(T.label, "vl2", T.id, i + big2);
            v1.addEdge("el2", v2);
        }
        graph.tx().commit();

        // reload ramtable
        Whitebox.invoke(graph.getClass(), "reloadRamtable", graph);

        // query edges
        for (int i = 0; i < 100; i++) {
            long source = i + big1;
            Iterator<Edge> edges = this.edgesOfVertex(IdGenerator.of(source),
                                                      Directions.OUT, null);
            Assert.assertTrue(edges.hasNext());
            HugeEdge edge = (HugeEdge) edges.next();
            Assert.assertEquals(source,
                                edge.id().ownerVertexId().asLong());
            Assert.assertEquals(i + big1 + 100,
                                edge.id().otherVertexId().asLong());
            Assert.assertEquals(Directions.OUT, edge.direction());
            Assert.assertEquals("el1", edge.label());

            Assert.assertFalse(edges.hasNext());
        }
        for (int i = 0; i < 100; i++) {
            long source = i + big2;
            Iterator<Edge> edges = this.edgesOfVertex(IdGenerator.of(source),
                                                      Directions.OUT, null);
            Assert.assertTrue(edges.hasNext());
            HugeEdge edge = (HugeEdge) edges.next();
            Assert.assertEquals(source,
                                edge.id().ownerVertexId().asLong());
            Assert.assertEquals(i + big2,
                                edge.id().otherVertexId().asLong());
            Assert.assertEquals(Directions.OUT, edge.direction());
            Assert.assertEquals("el2", edge.label());

            Assert.assertFalse(edges.hasNext());
        }
    }

    @Test
    public void testReloadAndQueryWithProperty() throws Exception {
        // FIXME: The legacy HStore guard and related coverage debt are tracked in
        // https://github.com/apache/hugegraph/issues/3090
        Assume.assumeTrue("skip this test for hstore",
                          Objects.equals("hstore", System.getProperty("backend")));

        HugeGraph graph = this.graph();
        SchemaManager schema = graph.schema();

        schema.propertyKey("name")
              .asText()
              .create();
        schema.vertexLabel("person")
              .properties("name")
              .useCustomizeNumberId()
              .create();
        schema.edgeLabel("next")
              .sourceLabel("person")
              .targetLabel("person")
              .properties("name")
              .create();

        GraphTraversalSource g = graph.traversal();
        g.addV("person").property(T.id, 1).property("name", "A").as("a")
         .addV("person").property(T.id, 2).property("name", "B").as("b")
         .addV("person").property(T.id, 3).property("name", "C").as("c")
         .addV("person").property(T.id, 4).property("name", "D").as("d")
         .addV("person").property(T.id, 5).property("name", "E").as("e")
         .addV("person").property(T.id, 6).property("name", "F").as("f")
         .addE("next").from("a").to("b").property("name", "ab")
         .addE("next").from("b").to("c").property("name", "bc")
         .addE("next").from("b").to("d").property("name", "bd")
         .addE("next").from("c").to("d").property("name", "cd")
         .addE("next").from("c").to("e").property("name", "ce")
         .addE("next").from("d").to("e").property("name", "de")
         .addE("next").from("e").to("f").property("name", "ef")
         .addE("next").from("f").to("d").property("name", "fd")
         .iterate();
        graph.tx().commit();

        Object ramtable = Whitebox.getInternalState(graph, "ramtable");
        Assert.assertNotNull("The ramtable is not enabled", ramtable);

        // reload ramtable
        Whitebox.invoke(graph.getClass(), "reloadRamtable", graph);

        GraphTraversal<Vertex, Vertex> vertices;
        HugeVertex vertex;
        GraphTraversal<Vertex, Edge> edges;
        HugeEdge edge;

        // A
        vertices = g.V(1).out();
        Assert.assertTrue(vertices.hasNext());
        vertex = (HugeVertex) vertices.next();
        Assert.assertFalse(vertex.isPropLoaded());
        Assert.assertEquals(2L, vertex.id().asObject());
        Assert.assertEquals("B", vertex.value("name"));
        Assert.assertFalse(vertices.hasNext());

        edges = g.V(1).outE();
        Assert.assertTrue(edges.hasNext());
        edge = (HugeEdge) edges.next();
        Assert.assertFalse(edge.isPropLoaded());
        Assert.assertEquals(Directions.OUT, edge.id().direction());
        Assert.assertEquals("ab", edge.value("name"));
        Assert.assertFalse(edges.hasNext());

        vertices = g.V(1).in();
        Assert.assertFalse(vertices.hasNext());

        edges = g.V(1).inE();
        Assert.assertFalse(edges.hasNext());

        vertices = g.V(1).both();
        Assert.assertTrue(vertices.hasNext());
        vertex = (HugeVertex) vertices.next();
        Assert.assertEquals(2L, vertex.id().asObject());
        Assert.assertEquals("B", vertex.value("name"));
        Assert.assertFalse(vertices.hasNext());

        edges = g.V(1).bothE();
        Assert.assertTrue(edges.hasNext());
        edge = (HugeEdge) edges.next();
        Assert.assertEquals(Directions.OUT, edge.id().direction());
        Assert.assertEquals("ab", edge.value("name"));
        Assert.assertFalse(edges.hasNext());

        // B
        vertices = g.V(2).out();
        Assert.assertTrue(vertices.hasNext());
        vertex = (HugeVertex) vertices.next();
        Assert.assertEquals(3L, vertex.id().asObject());
        Assert.assertEquals("C", vertex.value("name"));
        Assert.assertTrue(vertices.hasNext());
        vertex = (HugeVertex) vertices.next();
        Assert.assertEquals(4L, vertex.id().asObject());
        Assert.assertEquals("D", vertex.value("name"));
        Assert.assertFalse(vertices.hasNext());

        edges = g.V(2).outE();
        Assert.assertTrue(edges.hasNext());
        edge = (HugeEdge) edges.next();
        Assert.assertEquals(Directions.OUT, edge.id().direction());
        Assert.assertEquals("bc", edge.value("name"));
        Assert.assertTrue(edges.hasNext());
        edge = (HugeEdge) edges.next();
        Assert.assertEquals(Directions.OUT, edge.id().direction());
        Assert.assertEquals("bd", edge.value("name"));
        Assert.assertFalse(edges.hasNext());

        vertices = g.V(2).in();
        Assert.assertTrue(vertices.hasNext());
        vertex = (HugeVertex) vertices.next();
        Assert.assertEquals(1L, vertex.id().asObject());
        Assert.assertEquals("A", vertex.value("name"));
        Assert.assertFalse(vertices.hasNext());

        edges = g.V(2).inE();
        Assert.assertTrue(edges.hasNext());
        edge = (HugeEdge) edges.next();
        Assert.assertEquals(Directions.IN, edge.id().direction());
        Assert.assertEquals("ab", edge.value("name"));
        Assert.assertFalse(edges.hasNext());

        vertices = g.V(2).both();
        Assert.assertTrue(vertices.hasNext());
        vertex = (HugeVertex) vertices.next();
        Assert.assertEquals(3L, vertex.id().asObject());
        Assert.assertEquals("C", vertex.value("name"));
        Assert.assertTrue(vertices.hasNext());
        vertex = (HugeVertex) vertices.next();
        Assert.assertEquals(4L, vertex.id().asObject());
        Assert.assertEquals("D", vertex.value("name"));
        Assert.assertTrue(vertices.hasNext());
        vertex = (HugeVertex) vertices.next();
        Assert.assertEquals(1L, vertex.id().asObject());
        Assert.assertEquals("A", vertex.value("name"));
        Assert.assertFalse(vertices.hasNext());

        edges = g.V(2).bothE();
        Assert.assertTrue(edges.hasNext());
        edge = (HugeEdge) edges.next();
        Assert.assertEquals(Directions.OUT, edge.id().direction());
        Assert.assertEquals("bc", edge.value("name"));
        Assert.assertTrue(edges.hasNext());
        edge = (HugeEdge) edges.next();
        Assert.assertEquals(Directions.OUT, edge.id().direction());
        Assert.assertEquals("bd", edge.value("name"));
        Assert.assertTrue(edges.hasNext());
        edge = (HugeEdge) edges.next();
        Assert.assertEquals(Directions.IN, edge.id().direction());
        Assert.assertEquals("ab", edge.value("name"));
        Assert.assertFalse(edges.hasNext());

        // C
        vertices = g.V(3).out();
        Assert.assertTrue(vertices.hasNext());
        vertex = (HugeVertex) vertices.next();
        Assert.assertEquals(4L, vertex.id().asObject());
        Assert.assertEquals("D", vertex.value("name"));
        Assert.assertTrue(vertices.hasNext());
        vertex = (HugeVertex) vertices.next();
        Assert.assertEquals(5L, vertex.id().asObject());
        Assert.assertEquals("E", vertex.value("name"));
        Assert.assertFalse(vertices.hasNext());

        edges = g.V(3).outE();
        Assert.assertTrue(edges.hasNext());
        edge = (HugeEdge) edges.next();
        Assert.assertEquals(Directions.OUT, edge.id().direction());
        Assert.assertEquals("cd", edge.value("name"));
        Assert.assertTrue(edges.hasNext());
        edge = (HugeEdge) edges.next();
        Assert.assertEquals(Directions.OUT, edge.id().direction());
        Assert.assertEquals("ce", edge.value("name"));
        Assert.assertFalse(edges.hasNext());

        vertices = g.V(3).in();
        Assert.assertTrue(vertices.hasNext());
        vertex = (HugeVertex) vertices.next();
        Assert.assertEquals(2L, vertex.id().asObject());
        Assert.assertEquals("B", vertex.value("name"));
        Assert.assertFalse(vertices.hasNext());

        edges = g.V(3).inE();
        Assert.assertTrue(edges.hasNext());
        edge = (HugeEdge) edges.next();
        Assert.assertEquals(Directions.IN, edge.id().direction());
        Assert.assertEquals("bc", edge.value("name"));
        Assert.assertFalse(edges.hasNext());

        vertices = g.V(3).both();
        Assert.assertTrue(vertices.hasNext());
        vertex = (HugeVertex) vertices.next();
        Assert.assertEquals(4L, vertex.id().asObject());
        Assert.assertEquals("D", vertex.value("name"));
        Assert.assertTrue(vertices.hasNext());
        vertex = (HugeVertex) vertices.next();
        Assert.assertEquals(5L, vertex.id().asObject());
        Assert.assertEquals("E", vertex.value("name"));
        Assert.assertTrue(vertices.hasNext());
        vertex = (HugeVertex) vertices.next();
        Assert.assertEquals(2L, vertex.id().asObject());
        Assert.assertEquals("B", vertex.value("name"));
        Assert.assertFalse(vertices.hasNext());

        edges = g.V(3).bothE();
        Assert.assertTrue(edges.hasNext());
        edge = (HugeEdge) edges.next();
        Assert.assertEquals(Directions.OUT, edge.id().direction());
        Assert.assertEquals("cd", edge.value("name"));
        Assert.assertTrue(edges.hasNext());
        edge = (HugeEdge) edges.next();
        Assert.assertEquals(Directions.OUT, edge.id().direction());
        Assert.assertEquals("ce", edge.value("name"));
        Assert.assertTrue(edges.hasNext());
        edge = (HugeEdge) edges.next();
        Assert.assertEquals(Directions.IN, edge.id().direction());
        Assert.assertEquals("bc", edge.value("name"));
        Assert.assertFalse(edges.hasNext());

        // D
        vertices = g.V(4).out();
        Assert.assertTrue(vertices.hasNext());
        vertex = (HugeVertex) vertices.next();
        Assert.assertEquals(5L, vertex.id().asObject());
        Assert.assertEquals("E", vertex.value("name"));
        Assert.assertFalse(vertices.hasNext());

        edges = g.V(4).outE();
        Assert.assertTrue(edges.hasNext());
        edge = (HugeEdge) edges.next();
        Assert.assertEquals(Directions.OUT, edge.id().direction());
        Assert.assertEquals("de", edge.value("name"));
        Assert.assertFalse(edges.hasNext());

        vertices = g.V(4).in();
        Assert.assertTrue(vertices.hasNext());
        vertex = (HugeVertex) vertices.next();
        Assert.assertEquals(2L, vertex.id().asObject());
        Assert.assertEquals("B", vertex.value("name"));
        Assert.assertTrue(vertices.hasNext());
        vertex = (HugeVertex) vertices.next();
        Assert.assertEquals(3L, vertex.id().asObject());
        Assert.assertEquals("C", vertex.value("name"));
        Assert.assertTrue(vertices.hasNext());
        vertex = (HugeVertex) vertices.next();
        Assert.assertEquals(6L, vertex.id().asObject());
        Assert.assertEquals("F", vertex.value("name"));
        Assert.assertFalse(vertices.hasNext());

        edges = g.V(4).inE();
        Assert.assertTrue(edges.hasNext());
        edge = (HugeEdge) edges.next();
        Assert.assertEquals(Directions.IN, edge.id().direction());
        Assert.assertEquals("bd", edge.value("name"));
        Assert.assertTrue(edges.hasNext());
        edge = (HugeEdge) edges.next();
        Assert.assertEquals(Directions.IN, edge.id().direction());
        Assert.assertEquals("cd", edge.value("name"));
        Assert.assertTrue(edges.hasNext());
        edge = (HugeEdge) edges.next();
        Assert.assertEquals(Directions.IN, edge.id().direction());
        Assert.assertEquals("fd", edge.value("name"));
        Assert.assertFalse(edges.hasNext());

        vertices = g.V(4).both();
        Assert.assertTrue(vertices.hasNext());
        vertex = (HugeVertex) vertices.next();
        Assert.assertEquals(5L, vertex.id().asObject());
        Assert.assertEquals("E", vertex.value("name"));
        Assert.assertTrue(vertices.hasNext());
        vertex = (HugeVertex) vertices.next();
        Assert.assertEquals(2L, vertex.id().asObject());
        Assert.assertEquals("B", vertex.value("name"));
        Assert.assertTrue(vertices.hasNext());
        vertex = (HugeVertex) vertices.next();
        Assert.assertEquals(3L, vertex.id().asObject());
        Assert.assertEquals("C", vertex.value("name"));
        Assert.assertTrue(vertices.hasNext());
        vertex = (HugeVertex) vertices.next();
        Assert.assertEquals(6L, vertex.id().asObject());
        Assert.assertEquals("F", vertex.value("name"));
        Assert.assertFalse(vertices.hasNext());

        edges = g.V(4).bothE();
        Assert.assertTrue(edges.hasNext());
        edge = (HugeEdge) edges.next();
        Assert.assertEquals(Directions.OUT, edge.id().direction());
        Assert.assertEquals("de", edge.value("name"));
        Assert.assertTrue(edges.hasNext());
        edge = (HugeEdge) edges.next();
        Assert.assertEquals(Directions.IN, edge.id().direction());
        Assert.assertEquals("bd", edge.value("name"));
        Assert.assertTrue(edges.hasNext());
        edge = (HugeEdge) edges.next();
        Assert.assertEquals(Directions.IN, edge.id().direction());
        Assert.assertEquals("cd", edge.value("name"));
        Assert.assertTrue(edges.hasNext());
        edge = (HugeEdge) edges.next();
        Assert.assertEquals(Directions.IN, edge.id().direction());
        Assert.assertEquals("fd", edge.value("name"));
        Assert.assertFalse(edges.hasNext());

        // E
        vertices = g.V(5).out();
        Assert.assertTrue(vertices.hasNext());
        vertex = (HugeVertex) vertices.next();
        Assert.assertEquals(6L, vertex.id().asObject());
        Assert.assertEquals("F", vertex.value("name"));
        Assert.assertFalse(vertices.hasNext());

        edges = g.V(5).outE();
        Assert.assertTrue(edges.hasNext());
        edge = (HugeEdge) edges.next();
        Assert.assertEquals(Directions.OUT, edge.id().direction());
        Assert.assertEquals("ef", edge.value("name"));
        Assert.assertFalse(edges.hasNext());

        vertices = g.V(5).in();
        Assert.assertTrue(vertices.hasNext());
        vertex = (HugeVertex) vertices.next();
        Assert.assertEquals(3L, vertex.id().asObject());
        Assert.assertEquals("C", vertex.value("name"));
        Assert.assertTrue(vertices.hasNext());
        vertex = (HugeVertex) vertices.next();
        Assert.assertEquals(4L, vertex.id().asObject());
        Assert.assertEquals("D", vertex.value("name"));
        Assert.assertFalse(vertices.hasNext());

        edges = g.V(5).inE();
        Assert.assertTrue(edges.hasNext());
        edge = (HugeEdge) edges.next();
        Assert.assertEquals(Directions.IN, edge.id().direction());
        Assert.assertEquals("ce", edge.value("name"));
        Assert.assertTrue(edges.hasNext());
        edge = (HugeEdge) edges.next();
        Assert.assertEquals(Directions.IN, edge.id().direction());
        Assert.assertEquals("de", edge.value("name"));
        Assert.assertFalse(edges.hasNext());

        vertices = g.V(5).both();
        Assert.assertTrue(vertices.hasNext());
        vertex = (HugeVertex) vertices.next();
        Assert.assertEquals(6L, vertex.id().asObject());
        Assert.assertEquals("F", vertex.value("name"));
        Assert.assertTrue(vertices.hasNext());
        Assert.assertTrue(vertices.hasNext());
        vertex = (HugeVertex) vertices.next();
        Assert.assertEquals(3L, vertex.id().asObject());
        Assert.assertEquals("C", vertex.value("name"));
        Assert.assertTrue(vertices.hasNext());
        vertex = (HugeVertex) vertices.next();
        Assert.assertEquals(4L, vertex.id().asObject());
        Assert.assertEquals("D", vertex.value("name"));
        Assert.assertFalse(vertices.hasNext());

        edges = g.V(5).bothE();
        Assert.assertTrue(edges.hasNext());
        edge = (HugeEdge) edges.next();
        Assert.assertEquals(Directions.OUT, edge.id().direction());
        Assert.assertEquals("ef", edge.value("name"));
        Assert.assertTrue(edges.hasNext());
        edge = (HugeEdge) edges.next();
        Assert.assertEquals(Directions.IN, edge.id().direction());
        Assert.assertEquals("ce", edge.value("name"));
        Assert.assertTrue(edges.hasNext());
        edge = (HugeEdge) edges.next();
        Assert.assertEquals(Directions.IN, edge.id().direction());
        Assert.assertEquals("de", edge.value("name"));
        Assert.assertFalse(edges.hasNext());

        // F
        vertices = g.V(6).out();
        Assert.assertTrue(vertices.hasNext());
        vertex = (HugeVertex) vertices.next();
        Assert.assertEquals(4L, vertex.id().asObject());
        Assert.assertEquals("D", vertex.value("name"));
        Assert.assertFalse(vertices.hasNext());

        edges = g.V(6).outE();
        Assert.assertTrue(edges.hasNext());
        edge = (HugeEdge) edges.next();
        Assert.assertEquals(Directions.OUT, edge.id().direction());
        Assert.assertEquals("fd", edge.value("name"));
        Assert.assertFalse(edges.hasNext());

        vertices = g.V(6).in();
        Assert.assertTrue(vertices.hasNext());
        vertex = (HugeVertex) vertices.next();
        Assert.assertEquals(5L, vertex.id().asObject());
        Assert.assertEquals("E", vertex.value("name"));
        Assert.assertFalse(vertices.hasNext());

        edges = g.V(6).inE();
        Assert.assertTrue(edges.hasNext());
        edge = (HugeEdge) edges.next();
        Assert.assertEquals(Directions.IN, edge.id().direction());
        Assert.assertEquals("ef", edge.value("name"));
        Assert.assertFalse(edges.hasNext());

        vertices = g.V(6).both();
        Assert.assertTrue(vertices.hasNext());
        vertex = (HugeVertex) vertices.next();
        Assert.assertEquals(4L, vertex.id().asObject());
        Assert.assertEquals("D", vertex.value("name"));
        Assert.assertTrue(vertices.hasNext());
        vertex = (HugeVertex) vertices.next();
        Assert.assertEquals(5L, vertex.id().asObject());
        Assert.assertEquals("E", vertex.value("name"));
        Assert.assertFalse(vertices.hasNext());

        edges = g.V(6).bothE();
        Assert.assertTrue(edges.hasNext());
        edge = (HugeEdge) edges.next();
        Assert.assertEquals(Directions.OUT, edge.id().direction());
        Assert.assertEquals("fd", edge.value("name"));
        Assert.assertTrue(edges.hasNext());
        edge = (HugeEdge) edges.next();
        Assert.assertEquals(Directions.IN, edge.id().direction());
        Assert.assertEquals("ef", edge.value("name"));
        Assert.assertFalse(edges.hasNext());
    }

    private Iterator<Edge> edgesOfVertex(Id source, Directions dir, Id label) {
        Id[] labels = {};
        if (label != null) {
            labels = new Id[]{label};
        }

        Query query = GraphTransaction.constructEdgesQuery(source, dir, labels);
        return this.graph().edges(query);
    }
}
