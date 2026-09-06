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

package org.apache.hugegraph.unit.cache;

import java.util.Iterator;

import org.apache.hugegraph.HugeException;
import org.apache.hugegraph.HugeFactory;
import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.backend.id.IdGenerator;
import org.apache.hugegraph.backend.query.Condition;
import org.apache.hugegraph.backend.query.ConditionQuery;
import org.apache.hugegraph.backend.store.ram.RamTable;
import org.apache.hugegraph.schema.EdgeLabel;
import org.apache.hugegraph.schema.SchemaManager;
import org.apache.hugegraph.schema.VertexLabel;
import org.apache.hugegraph.structure.HugeEdge;
import org.apache.hugegraph.structure.HugeVertex;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.type.define.Directions;
import org.apache.hugegraph.type.define.HugeKeys;
import org.apache.hugegraph.unit.FakeObjects;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

public class RamTableTest {

    // max value is 4 billion
    private static final int VERTEX_SIZE = 10000000;
    private static final int EDGE_SIZE = 20000000;

    private HugeGraph graph;

    @Before
    public void setup() {
        this.graph = HugeFactory.open(FakeObjects.newConfig());
        SchemaManager schema = this.graph.schema();

        schema.propertyKey("p3").asText().create();

        schema.vertexLabel("vl1").useCustomizeNumberId().create();
        schema.vertexLabel("vl2").useCustomizeNumberId().create();
        schema.vertexLabel("vl3").useCustomizeStringId().create();

        schema.edgeLabel("el1")
              .sourceLabel("vl1")
              .targetLabel("vl1")
              .create();
        schema.edgeLabel("el2")
              .sourceLabel("vl2")
              .targetLabel("vl2")
              .create();
        schema.edgeLabel("el3")
              .sourceLabel("vl3")
              .targetLabel("vl3")
              .properties("p3")
              .multiTimes()
              .sortKeys("p3")
              .create();
    }

    @After
    public void teardown() throws Exception {
        this.graph.close();
    }

    private HugeGraph graph() {
        return this.graph;
    }

    @Test
    public void testMatchedLabelCandidateContract() {
        RamTable table = new RamTable(this.graph(), 10, 20);
        table.addEdge(true, 1, 2, Directions.OUT, 1);
        ConditionQuery query = new ConditionQuery(HugeType.EDGE);
        query.eq(HugeKeys.OWNER_VERTEX, IdGenerator.of(1));
        query.query(Condition.in(HugeKeys.LABEL, ImmutableList.of(IdGenerator.of(1))));
        Assert.assertTrue(table.matched(query));

        query = new ConditionQuery(HugeType.EDGE);
        query.eq(HugeKeys.OWNER_VERTEX, IdGenerator.of(1));
        query.query(Condition.in(HugeKeys.LABEL,
                                 ImmutableList.of(IdGenerator.of(1), IdGenerator.of(2))));
        Assert.assertFalse(table.matched(query));
        query.eq(HugeKeys.LABEL, IdGenerator.of(3));
        Assert.assertFalse(table.matched(query));
    }

    @Test
    public void testAddAndQuery() throws Exception {
        HugeGraph graph = this.graph();
        int el1 = (int) graph.edgeLabel("el1").id().asLong();
        int el2 = (int) graph.edgeLabel("el2").id().asLong();

        RamTable table = new RamTable(graph, VERTEX_SIZE, EDGE_SIZE);
        long oldSize = table.edgesSize();
        // insert edges
        for (int i = 0; i < VERTEX_SIZE; i++) {
            table.addEdge(true, i, i, Directions.OUT, el1);
            Assert.assertEquals(oldSize + 2 * i + 1, table.edgesSize());

            table.addEdge(false, i, i + 1, Directions.IN, el2);
            Assert.assertEquals(oldSize + 2 * i + 2, table.edgesSize());
        }

        // query by BOTH
        for (int i = 0; i < VERTEX_SIZE; i++) {
            Iterator<HugeEdge> edges = table.query(i, Directions.BOTH, 0);

            Assert.assertTrue(edges.hasNext());
            HugeEdge edge1 = edges.next();
            Assert.assertEquals(i, edge1.id().ownerVertexId().asLong());
            Assert.assertEquals(i, edge1.id().otherVertexId().asLong());
            Assert.assertEquals(Directions.OUT, edge1.direction());
            Assert.assertEquals("el1", edge1.label());

            Assert.assertTrue(edges.hasNext());
            HugeEdge edge2 = edges.next();
            Assert.assertEquals(i, edge2.id().ownerVertexId().asLong());
            Assert.assertEquals(i + 1L, edge2.id().otherVertexId().asLong());
            Assert.assertEquals(Directions.IN, edge2.direction());
            Assert.assertEquals("el2", edge2.label());

            Assert.assertFalse(edges.hasNext());
        }
        // query by OUT
        for (int i = 0; i < VERTEX_SIZE; i++) {
            Iterator<HugeEdge> edges = table.query(i, Directions.OUT, el1);

            Assert.assertTrue(edges.hasNext());
            HugeEdge edge1 = edges.next();
            Assert.assertEquals(i, edge1.id().ownerVertexId().asLong());
            Assert.assertEquals(i, edge1.id().otherVertexId().asLong());
            Assert.assertEquals(Directions.OUT, edge1.direction());
            Assert.assertEquals("el1", edge1.label());

            Assert.assertFalse(edges.hasNext());
        }
        // query by IN
        for (int i = 0; i < VERTEX_SIZE; i++) {
            Iterator<HugeEdge> edges = table.query(i, Directions.IN, el2);

            Assert.assertTrue(edges.hasNext());
            HugeEdge edge1 = edges.next();
            Assert.assertEquals(i, edge1.id().ownerVertexId().asLong());
            Assert.assertEquals(i + 1L, edge1.id().otherVertexId().asLong());
            Assert.assertEquals(Directions.IN, edge1.direction());
            Assert.assertEquals("el2", edge1.label());

            Assert.assertFalse(edges.hasNext());
        }

        // query by BOTH & label 1
        for (int i = 0; i < VERTEX_SIZE; i++) {
            Iterator<HugeEdge> edges = table.query(i, Directions.BOTH, el1);

            Assert.assertTrue(edges.hasNext());
            HugeEdge edge1 = edges.next();
            Assert.assertEquals(i, edge1.id().ownerVertexId().asLong());
            Assert.assertEquals(i, edge1.id().otherVertexId().asLong());
            Assert.assertEquals(Directions.OUT, edge1.direction());
            Assert.assertEquals("el1", edge1.label());

            Assert.assertFalse(edges.hasNext());
        }
        // query by BOTH & label 2
        for (int i = 0; i < VERTEX_SIZE; i++) {
            Iterator<HugeEdge> edges = table.query(i, Directions.BOTH, el2);

            Assert.assertTrue(edges.hasNext());
            HugeEdge edge1 = edges.next();
            Assert.assertEquals(i, edge1.id().ownerVertexId().asLong());
            Assert.assertEquals(i + 1L, edge1.id().otherVertexId().asLong());
            Assert.assertEquals(Directions.IN, edge1.direction());
            Assert.assertEquals("el2", edge1.label());

            Assert.assertFalse(edges.hasNext());
        }

        // query non-exist vertex
        Iterator<HugeEdge> edges = table.query(VERTEX_SIZE, Directions.BOTH, 0);
        Assert.assertFalse(edges.hasNext());
    }

    @Test
    public void testAddAndQueryWithoutAdjEdges() throws Exception {
        HugeGraph graph = this.graph();
        int el1 = (int) graph.edgeLabel("el1").id().asLong();
        int el2 = (int) graph.edgeLabel("el2").id().asLong();

        RamTable table = new RamTable(graph, VERTEX_SIZE, EDGE_SIZE);
        long oldSize = table.edgesSize();
        // insert edges
        for (int i = 0; i < VERTEX_SIZE; i++) {
            if (i % 3 != 0) {
                // don't insert edges for 2/3 vertices
                continue;
            }

            table.addEdge(true, i, i, Directions.OUT, el1);
            Assert.assertEquals(oldSize + i + 1, table.edgesSize());

            table.addEdge(false, i, i, Directions.OUT, el2);
            Assert.assertEquals(oldSize + i + 2, table.edgesSize());

            table.addEdge(false, i, i + 1, Directions.IN, el2);
            Assert.assertEquals(oldSize + i + 3, table.edgesSize());
        }

        // query by BOTH
        for (int i = 0; i < VERTEX_SIZE; i++) {
            Iterator<HugeEdge> edges = table.query(i, Directions.BOTH, 0);

            if (i % 3 != 0) {
                Assert.assertFalse(edges.hasNext());
                continue;
            }

            Assert.assertTrue(edges.hasNext());
            HugeEdge edge1 = edges.next();
            Assert.assertEquals(i, edge1.id().ownerVertexId().asLong());
            Assert.assertEquals(i, edge1.id().otherVertexId().asLong());
            Assert.assertEquals(Directions.OUT, edge1.direction());
            Assert.assertEquals("el1", edge1.label());

            Assert.assertTrue(edges.hasNext());
            HugeEdge edge2 = edges.next();
            Assert.assertEquals(i, edge2.id().ownerVertexId().asLong());
            Assert.assertEquals(i, edge2.id().otherVertexId().asLong());
            Assert.assertEquals(Directions.OUT, edge2.direction());
            Assert.assertEquals("el2", edge2.label());

            Assert.assertTrue(edges.hasNext());
            HugeEdge edge3 = edges.next();
            Assert.assertEquals(i, edge3.id().ownerVertexId().asLong());
            Assert.assertEquals(i + 1L, edge3.id().otherVertexId().asLong());
            Assert.assertEquals(Directions.IN, edge3.direction());
            Assert.assertEquals("el2", edge3.label());

            Assert.assertFalse(edges.hasNext());
        }
    }

    @Test
    public void testAddInvalidVertexOrEdge() {
        HugeGraph graph = this.graph();
        VertexLabel vl3 = graph.vertexLabel("vl3");
        EdgeLabel el3 = graph.edgeLabel("el3");

        VertexLabel vl2 = graph.vertexLabel("vl2");
        EdgeLabel el2 = graph.edgeLabel("el2");

        RamTable table = new RamTable(graph, VERTEX_SIZE, EDGE_SIZE);

        HugeVertex ownerVertex = new HugeVertex(graph, IdGenerator.of(1), vl3);
        HugeEdge edge1 = HugeEdge.constructEdge(ownerVertex, true, el3, "marko",
                                                IdGenerator.of(2));
        Assert.assertThrows(HugeException.class, () -> {
            table.addEdge(true, edge1);
        }, e -> {
            Assert.assertContains("Only edge label without sortkey is " +
                                  "supported by ramtable, but got 'el3(id=3)'",
                                  e.getMessage());
        });

        HugeVertex v1 = new HugeVertex(graph, IdGenerator.of("s1"), vl2);
        HugeEdge edge2 = HugeEdge.constructEdge(v1, true, el2, "marko",
                                                IdGenerator.of("s2"));
        Assert.assertThrows(HugeException.class, () -> {
            table.addEdge(true, edge2);
        }, e -> {
            Assert.assertContains("Only number id is supported by ramtable, " +
                                  "but got string id 's1'", e.getMessage());
        });

        HugeVertex v2 = new HugeVertex(graph, IdGenerator.of(2), vl2);
        HugeEdge edge3 = HugeEdge.constructEdge(v2, true, el2, "marko",
                                                IdGenerator.of("s2"));
        Assert.assertThrows(HugeException.class, () -> {
            table.addEdge(true, edge3);
        }, e -> {
            Assert.assertContains("Only number id is supported by ramtable, " +
                                  "but got string id 's2'", e.getMessage());
        });
    }
}
