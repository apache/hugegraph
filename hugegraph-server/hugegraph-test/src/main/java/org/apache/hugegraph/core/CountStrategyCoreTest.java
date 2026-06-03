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

import org.apache.hugegraph.schema.SchemaManager;
import org.apache.hugegraph.testutil.Assert;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

public class CountStrategyCoreTest extends BaseCoreTest {

    private void initSchema() {
        SchemaManager schema = graph().schema();
        schema.propertyKey("name").asText().create();
        schema.vertexLabel("person").properties("name")
              .nullableKeys("name").create();
        schema.vertexLabel("software").properties("name")
              .nullableKeys("name").create();
        schema.edgeLabel("knows").link("person", "person").create();
        schema.edgeLabel("created").link("person", "software").create();
    }

    private void initGraph() {
        Vertex marko = graph().addVertex(T.label, "person", "name", "marko");
        Vertex josh = graph().addVertex(T.label, "person", "name", "josh");
        Vertex lop = graph().addVertex(T.label, "software", "name", "lop");

        marko.addEdge("knows", josh);
        marko.addEdge("created", lop);
        commitTx();
    }

    private void initTextRangeSchema(boolean withEdge) {
        SchemaManager schema = graph().schema();
        schema.propertyKey("vp4").asText().create();
        schema.propertyKey("ep4").asText().create();
        schema.propertyKey("age").asInt().create();
        schema.vertexLabel("vl1").properties("vp4", "age")
              .nullableKeys("vp4", "age").create();
        if (withEdge) {
            schema.edgeLabel("el2").properties("ep4")
                  .nullableKeys("ep4").link("vl1", "vl1").create();
        }
    }

    @Test
    public void testWhereCountLtNegativeIsAlwaysFalse() {
        this.initSchema();
        this.initGraph();

        long count = graph().traversal().E().outV()
                            .repeat(__.both()).times(1)
                            .where(__.outE().count().is(P.lt(-3)))
                            .count().next();

        Assert.assertEquals(0L, count);
    }

    @Test
    public void testWhereCountOutsideNegativeKeepsOriginalSemantics() {
        this.initSchema();
        this.initGraph();

        long direct = graph().traversal().V()
                           .both("created")
                           .inE("created")
                           .where(__.bothV().count().is(P.outside(-3, -5)))
                           .count().next();
        long viaMatch = graph().traversal().V()
                             .repeat(__.both("created")).times(1)
                             .inE("created")
                             .match(__.as("start")
                                      .where(__.bothV().count()
                                               .is(P.outside(-3, -5)))
                                      .as("end"))
                             .select("end")
                             .count().next();

        Assert.assertEquals(1L, direct);
        Assert.assertEquals(direct, viaMatch);
    }

    @Test
    public void testRepeatUntilCountLtNegativeIsAlwaysFalse() {
        this.initSchema();
        this.initGraph();

        long count = graph().traversal().E()
                            .hasLabel("knows")
                            .outV()
                            .repeat(__.out())
                            .until(__.outE().count().is(P.lt(-1)))
                            .count().next();

        Assert.assertEquals(0L, count);
    }

    @Test
    public void testWhereCountWithinNegativeCollectionIsAlwaysFalse() {
        this.initSchema();
        this.initGraph();

        long count = graph().traversal().V()
                            .where(__.outE().count().is(P.within(-3, -5)))
                            .count().next();

        Assert.assertEquals(0L, count);
    }

    @Test
    public void testWhereCountGteNegativeDoesNotBuildInvalidRange() {
        this.initSchema();
        this.initGraph();

        long count = graph().traversal().E()
                            .bothV()
                            .where(__.out("knows", "created")
                                     .count().is(P.gte(-3)))
                            .count().next();

        Assert.assertEquals(4L, count);
    }

    @Test
    public void testRepeatAfterTextRangeFilterWithEmptyResult() {
        this.initTextRangeSchema(true);

        Vertex v1 = graph().addVertex(T.label, "vl1", "vp4", "a", "age", 1);
        Vertex v2 = graph().addVertex(T.label, "vl1", "vp4", "b", "age", 2);
        v1.addEdge("el2", v2);
        commitTx();

        long direct = graph().traversal().V().has("vp4", P.lt(""))
                           .repeat(__.out("el2")).emit().times(1)
                           .count().next();
        long viaMatch = graph().traversal().V()
                             .match(__.as("start").has("vp4", P.lt(""))
                                      .out("el2").as("m"))
                             .select("m").count().next();

        Assert.assertEquals(0L, direct);
        Assert.assertEquals(direct, viaMatch);
    }

    @Test
    public void testTextRangeFilterKeepsMixedGraphHasStep() {
        this.initTextRangeSchema(false);

        graph().addVertex(T.label, "vl1", "vp4", "a", "age", 1);
        graph().addVertex(T.label, "vl1", "vp4", "b", "age", 2);
        commitTx();

        long direct = graph().traversal().V()
                           .hasLabel("vl1")
                           .has("vp4", P.lt(""))
                           .has("age", 1)
                           .count().next();
        long viaMatch = graph().traversal().V()
                             .match(__.as("v").hasLabel("vl1")
                                      .has("vp4", P.lt(""))
                                      .has("age", 1))
                             .select("v").count().next();

        Assert.assertEquals(0L, direct);
        Assert.assertEquals(direct, viaMatch);
    }

    @Test
    public void testTextRangeFilterKeepsMixedVertexHasStep() {
        this.initTextRangeSchema(true);

        Vertex v1 = graph().addVertex(T.label, "vl1", "vp4", "a", "age", 1);
        Vertex v2 = graph().addVertex(T.label, "vl1", "vp4", "b", "age", 2);
        v1.addEdge("el2", v2);
        commitTx();

        long direct = graph().traversal().V(v1.id()).out("el2")
                           .hasLabel("vl1")
                           .has("vp4", P.lt(""))
                           .has("age", 2)
                           .count().next();
        long viaMatch = graph().traversal().V(v1.id()).out("el2")
                             .match(__.as("v").hasLabel("vl1")
                                      .has("vp4", P.lt(""))
                                      .has("age", 2))
                             .select("v").count().next();

        Assert.assertEquals(0L, direct);
        Assert.assertEquals(direct, viaMatch);
    }

    @Test
    public void testTextRangeFilterKeepsEdgeGraphHasStep() {
        this.initTextRangeSchema(true);

        Vertex v1 = graph().addVertex(T.label, "vl1", "vp4", "a", "age", 1);
        Vertex v2 = graph().addVertex(T.label, "vl1", "vp4", "b", "age", 2);
        v1.addEdge("el2", v2, "ep4", "a");
        commitTx();

        long direct = graph().traversal().E()
                           .has("ep4", P.lt(""))
                           .count().next();
        long viaMatch = graph().traversal().E()
                             .match(__.as("e").has("ep4", P.lt("")))
                             .select("e").count().next();

        Assert.assertEquals(0L, direct);
        Assert.assertEquals(direct, viaMatch);
    }

}
