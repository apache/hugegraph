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
import org.apache.hugegraph.traversal.optimize.HugeGraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Edge;
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

    private void initMatchNoIndexSchema() {
        SchemaManager schema = graph().schema();
        schema.propertyKey("vp2").asBoolean().create();
        schema.propertyKey("vp3").asLong().create();
        schema.propertyKey("vp4").asText().create();
        schema.propertyKey("ep2").asBoolean().create();
        schema.vertexLabel("vl1").properties("vp2", "vp4")
              .nullableKeys("vp2", "vp4").create();
        schema.vertexLabel("vl0").properties("vp3")
              .nullableKeys("vp3").create();
        schema.edgeLabel("el1").link("vl1", "vl0")
              .properties("ep2").nullableKeys("ep2").create();
    }

    private void initMatchNoIndexGraph() {
        Vertex v1 = graph().addVertex(T.label, "vl1", "vp2", true,
                                      "vp4", "foo");
        Vertex v2 = graph().addVertex(T.label, "vl1", "vp2", false,
                                      "vp4", "J2O");
        Vertex v3 = graph().addVertex(T.label, "vl0",
                                      "vp3", 4592737712018141719L);
        Vertex v4 = graph().addVertex(T.label, "vl0",
                                      "vp3", 4592737712018141717L);

        v1.addEdge("el1", v3, "ep2", true);
        v2.addEdge("el1", v4, "ep2", false);
        commitTx();
    }

    private static HugeGraphStep<?, ?> applyAndGetGraphStep(
            GraphTraversal<?, ?> traversal) {
        traversal.asAdmin().applyStrategies();
        return (HugeGraphStep<?, ?>) traversal.asAdmin().getStartStep();
    }

    private static boolean hasRemainingHasStep(GraphTraversal<?, ?> traversal,
                                               String key) {
        for (Step<?, ?> step : traversal.asAdmin().getSteps()) {
            if (!(step instanceof HasStep)) {
                continue;
            }
            HasContainerHolder holder = (HasContainerHolder) step;
            for (HasContainer has : holder.getHasContainers()) {
                if (key.equals(has.getKey())) {
                    return true;
                }
            }
        }
        return false;
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

    @Test
    public void testMatchWithNoIndexConditionMatchesDirectTraversal() {
        this.initMatchNoIndexSchema();
        this.initMatchNoIndexGraph();

        long direct = graph().traversal().V()
                           .has("vp4", P.neq("J2O"))
                           .has("vl1", "vp2", P.gte(false))
                           .has("vp2")
                           .has("vl0", "vp3", P.gt(4592737712018141718L))
                           .out("el1")
                           .count().next();
        long viaMatch = graph().traversal().V()
                             .has("vp4", P.neq("J2O"))
                             .has("vl1", "vp2", P.gte(false))
                             .match(__.<Vertex>as("start0")
                                      .has("vp2")
                                      .has("vl0", "vp3",
                                           P.gt(4592737712018141718L))
                                      .repeat(__.out("el1"))
                                      .times(1)
                                      .as("m0"))
                             .<Vertex>select("m0").count().next();

        Assert.assertEquals(0L, direct);
        Assert.assertEquals(direct, viaMatch);
    }

    @Test
    public void testMatchWithIndexedRangeConditionStillExtractsHas() {
        this.initMatchNoIndexSchema();
        graph().schema().indexLabel("vl1ByVp2").onV("vl1")
               .by("vp2").secondary().create();
        this.initMatchNoIndexGraph();

        GraphTraversal<Vertex, Long> traversal = graph().traversal().V()
                                                        .has("vp2", P.lt(true))
                                                        .match(__.<Vertex>as("s")
                                                                 .has("vp2")
                                                                 .as("m"))
                                                        .<Vertex>select("m")
                                                        .count();

        HugeGraphStep<?, ?> graphStep = applyAndGetGraphStep(traversal);
        Assert.assertEquals(1, graphStep.getHasContainers().size());
        Assert.assertEquals("vp2", graphStep.getHasContainers().get(0).getKey());
        Assert.assertEquals(1L, traversal.next());
    }

    @Test
    public void testMatchWithNoIndexConditionKeepsExtractingNextHas() {
        this.initMatchNoIndexSchema();
        graph().schema().indexLabel("vl1ByVp2").onV("vl1")
               .by("vp2").secondary().create();
        this.initMatchNoIndexGraph();

        GraphTraversal<Vertex, Long> traversal = graph().traversal().V()
                                                        .has("vp4", P.neq("J2O"))
                                                        .has("vp2", true)
                                                        .match(__.<Vertex>as("s")
                                                                 .has("vp2")
                                                                 .as("m"))
                                                        .<Vertex>select("m")
                                                        .count();

        HugeGraphStep<?, ?> graphStep = applyAndGetGraphStep(traversal);
        Assert.assertEquals(1, graphStep.getHasContainers().size());
        Assert.assertEquals("vp2", graphStep.getHasContainers().get(0).getKey());
        Assert.assertTrue(hasRemainingHasStep(traversal, "vp4"));
        Assert.assertFalse(hasRemainingHasStep(traversal, "vp2"));
        Assert.assertEquals(1L, traversal.next());
    }

    @Test
    public void testMatchWithBooleanExistsConditionKeepsHas() {
        this.initMatchNoIndexSchema();
        graph().schema().indexLabel("vl1ByVp2").onV("vl1")
               .by("vp2").secondary().create();
        this.initMatchNoIndexGraph();

        GraphTraversal<Vertex, Long> traversal = graph().traversal().V()
                                                        .has("vp2", P.neq(null))
                                                        .match(__.<Vertex>as("s")
                                                                 .has("vp2", true)
                                                                 .as("m"))
                                                        .<Vertex>select("m")
                                                        .count();

        HugeGraphStep<?, ?> graphStep = applyAndGetGraphStep(traversal);
        Assert.assertEquals(0, graphStep.getHasContainers().size());
        Assert.assertEquals(1L, traversal.next());
    }

    @Test
    public void testMatchWithIdentityKeepsNoIndexConditionLocal() {
        this.initMatchNoIndexSchema();
        this.initMatchNoIndexGraph();

        GraphTraversal<Vertex, Long> traversal = graph().traversal().V()
                                                        .has("vp4", P.neq("J2O"))
                                                        .identity()
                                                        .match(__.<Vertex>as("s")
                                                                 .has("vp2")
                                                                 .as("m"))
                                                        .<Vertex>select("m")
                                                        .count();

        HugeGraphStep<?, ?> graphStep = applyAndGetGraphStep(traversal);
        Assert.assertEquals(0, graphStep.getHasContainers().size());
        Assert.assertTrue(hasRemainingHasStep(traversal, "vp4"));
        Assert.assertEquals(1L, traversal.next());
    }

    @Test
    public void testMatchWithIndexedEdgeRangeConditionStillExtractsHas() {
        this.initMatchNoIndexSchema();
        graph().schema().indexLabel("el1ByEp2").onE("el1")
               .by("ep2").secondary().create();
        this.initMatchNoIndexGraph();

        GraphTraversal<Edge, Long> traversal = graph().traversal().E()
                                                      .has("ep2", P.lt(true))
                                                      .match(__.<Edge>as("s")
                                                               .has("ep2")
                                                               .as("m"))
                                                      .<Edge>select("m")
                                                      .count();

        HugeGraphStep<?, ?> graphStep = applyAndGetGraphStep(traversal);
        Assert.assertEquals(1, graphStep.getHasContainers().size());
        Assert.assertEquals("ep2", graphStep.getHasContainers().get(0).getKey());
        Assert.assertEquals(1L, traversal.next());
    }

    @Test
    public void testMatchWithIndexedNumericRangeConditionStillExtractsHas() {
        this.initMatchNoIndexSchema();
        graph().schema().indexLabel("vl0ByVp3").onV("vl0")
               .by("vp3").range().create();
        this.initMatchNoIndexGraph();

        GraphTraversal<Vertex, Long> traversal = graph().traversal().V()
                                                        .has("vp3",
                                                             P.gt(4592737712018141718L))
                                                        .match(__.<Vertex>as("s")
                                                                 .has("vp3")
                                                                 .as("m"))
                                                        .<Vertex>select("m")
                                                        .count();

        HugeGraphStep<?, ?> graphStep = applyAndGetGraphStep(traversal);
        Assert.assertEquals(1, graphStep.getHasContainers().size());
        Assert.assertEquals("vp3", graphStep.getHasContainers().get(0).getKey());
        Assert.assertFalse(hasRemainingHasStep(traversal, "vp3"));
        Assert.assertEquals(1L, traversal.next());
    }

    @Test
    public void testMatchWithIndexedNumericNeqConditionKeepsHas() {
        this.initMatchNoIndexSchema();
        graph().schema().indexLabel("vl0ByVp3").onV("vl0")
               .by("vp3").range().create();
        graph().schema().indexLabel("vl1ByVp2").onV("vl1")
               .by("vp2").secondary().create();
        this.initMatchNoIndexGraph();

        GraphTraversal<Vertex, Long> traversal = graph().traversal().V()
                                                        .has("vp3",
                                                             P.neq(4592737712018141719L))
                                                        .has("vp2", true)
                                                        .match(__.<Vertex>as("s")
                                                                 .has("vp2")
                                                                 .as("m"))
                                                        .<Vertex>select("m")
                                                        .count();

        HugeGraphStep<?, ?> graphStep = applyAndGetGraphStep(traversal);
        Assert.assertEquals(1, graphStep.getHasContainers().size());
        Assert.assertEquals("vp2", graphStep.getHasContainers().get(0).getKey());
        Assert.assertTrue(hasRemainingHasStep(traversal, "vp3"));
        Assert.assertEquals(0L, traversal.next());
    }

    @Test
    public void testMatchWithSystemRangeConditionMatchesDirectTraversal() {
        this.initMatchNoIndexSchema();
        this.initMatchNoIndexGraph();

        long direct = graph().traversal().V()
                           .hasLabel("vl1")
                           .has("vp2")
                           .count().next();
        GraphTraversal<Vertex, Long> traversal = graph().traversal().V()
                                                        .hasLabel(P.neq("vl0"))
                                                        .match(__.<Vertex>as("s")
                                                                 .has("vp2")
                                                                 .as("m"))
                                                        .<Vertex>select("m")
                                                        .count();

        HugeGraphStep<?, ?> graphStep = applyAndGetGraphStep(traversal);
        Assert.assertTrue(hasRemainingHasStep(traversal, T.label.getAccessor()));
        Assert.assertEquals(direct, traversal.next().longValue());
    }

    @Test
    public void testMatchWithUniqueBooleanRangeConditionKeepsHas() {
        this.initMatchNoIndexSchema();
        graph().schema().indexLabel("vl1ByUniqueVp2").onV("vl1")
               .by("vp2").unique().create();
        this.initMatchNoIndexGraph();

        GraphTraversal<Vertex, Long> traversal = graph().traversal().V()
                                                        .has("vp2", P.lt(true))
                                                        .match(__.<Vertex>as("s")
                                                                 .has("vp2")
                                                                 .as("m"))
                                                        .<Vertex>select("m")
                                                        .count();

        HugeGraphStep<?, ?> graphStep = applyAndGetGraphStep(traversal);
        Assert.assertEquals(0, graphStep.getHasContainers().size());
        Assert.assertTrue(hasRemainingHasStep(traversal, "vp2"));
        Assert.assertEquals(1L, traversal.next());
    }
}
