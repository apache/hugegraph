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

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hugegraph.backend.query.Aggregate;
import org.apache.hugegraph.backend.query.Aggregate.AggregateFunc;
import org.apache.hugegraph.backend.query.Query;
import org.apache.hugegraph.backend.tx.GraphTransaction;
import org.apache.hugegraph.exception.NoIndexException;
import org.apache.hugegraph.schema.SchemaManager;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.traversal.optimize.HugeCountStep;
import org.apache.hugegraph.traversal.optimize.HugeCountStrategy;
import org.apache.hugegraph.traversal.optimize.HugeGraphStep;
import org.apache.hugegraph.type.HugeType;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.TextP;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;
import org.junit.Test;

public class CountStrategyCoreTest extends BaseCoreTest {

    private void initSchema() {
        SchemaManager schema = graph().schema();
        schema.propertyKey("name").asText().create();
        schema.propertyKey("none").asText().create();
        schema.vertexLabel("person").properties("name", "none")
              .nullableKeys("name", "none").create();
        schema.vertexLabel("software").properties("name", "none")
              .nullableKeys("name", "none").create();
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
            HasContainerHolder<?, ?> holder =
                    (HasContainerHolder<?, ?>) step;
            for (HasContainer has : holder.getHasContainers()) {
                if (key.equals(has.getKey())) {
                    return true;
                }
            }
        }
        return false;
    }

    private void assertNegatedBooleanPredicate(long expected,
                                                P<Boolean> predicate) {
        GraphTraversal<Vertex, Long> traversal = graph().traversal().V()
                                                        .has("vp2",
                                                             P.not(predicate))
                                                        .count();
        traversal.asAdmin().applyStrategies();

        Assert.assertTrue(hasRemainingHasStep(traversal, "vp2"));
        Assert.assertEquals(expected, traversal.next().longValue());
    }

    private static void assertUncommittedRangeUnsupported(
            GraphTraversal<?, ?> traversal) {
        Assert.assertThrows(IllegalArgumentException.class, traversal::next,
                            e -> {
                                Assert.assertContains("offset/limit", e.getMessage());
                                Assert.assertContains("uncommitted records", e.getMessage());
                            });
    }

    private static void assertNegatedCountHighRange(long expected,
                                                    P<Long> predicate) {
        GraphTraversal<?, Long> traversal = __.count().is(P.not(predicate));
        HugeCountStrategy.instance().apply(traversal.asAdmin());

        Step<?, ?> firstStep = traversal.asAdmin().getStartStep();
        Assert.assertInstanceOf(RangeGlobalStep.class, firstStep);
        Assert.assertEquals(expected,
                            ((RangeGlobalStep<?>) firstStep).getHighRange());
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

    private void initConnectiveRangeNoIndexSchema() {
        SchemaManager schema = graph().schema();
        schema.propertyKey("ep4").asFloat().create();
        schema.vertexLabel("vl1").create();
        schema.edgeLabel("el2").properties("ep4")
              .nullableKeys("ep4").link("vl1", "vl1").create();
        schema.edgeLabel("el3").properties("ep4")
              .nullableKeys("ep4").link("vl1", "vl1").create();
    }

    private void initNegatedDoubleSchema() {
        SchemaManager schema = graph().schema();
        schema.propertyKey("score").asDouble().create();
        schema.vertexLabel("sample").properties("score").create();
        schema.indexLabel("sampleByScore").onV("sample")
              .by("score").range().create();
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
    public void testWhereCountNestedConnectivePredicate() {
        this.initSchema();
        Vertex source = graph().addVertex(T.label, "person", "name", "source");
        Vertex target = graph().addVertex(T.label, "person", "name", "target");
        source.addEdge("knows", target);
        commitTx();

        long count = graph().traversal().V(source.id())
                            .where(__.both("knows").count()
                                     .is(P.<Long>outside(1L, 18L)
                                          .and(P.gte(0L))))
                            .count().next();

        Assert.assertEquals(0L, count);
    }

    @Test
    public void testWhereCountNegatedNestedConnectivePredicate() {
        this.initSchema();
        Vertex source = graph().addVertex(T.label, "person", "name", "source");
        Vertex target = graph().addVertex(T.label, "person", "name", "target");
        source.addEdge("knows", target);
        commitTx();

        long count = graph().traversal().V(source.id())
                            .where(__.both("knows").count()
                                     .is(P.not(P.<Long>outside(1L, 18L)
                                                 .and(P.gte(0L)))))
                            .count().next();

        Assert.assertEquals(1L, count);
    }

    @Test
    public void testWhereCountNegatedScalarPredicatesKeepSemantics() {
        this.initSchema();
        Vertex source = graph().addVertex(T.label, "person", "name", "source");
        Vertex first = graph().addVertex(T.label, "person", "name", "first");
        Vertex second = graph().addVertex(T.label, "person", "name", "second");
        source.addEdge("knows", first);
        source.addEdge("knows", second);
        commitTx();

        long notEqZero = graph().traversal().V(source.id())
                                .where(__.out("knows").count()
                                         .is(P.not(P.eq(0L))))
                                .count().next();
        long notNeqOne = graph().traversal().V(source.id())
                                .where(__.out("knows").count()
                                         .is(P.not(P.neq(1L))))
                                .count().next();
        long notLtTwo = graph().traversal().V(source.id())
                                .where(__.out("knows").count()
                                         .is(P.not(P.lt(2L))))
                                .count().next();
        long notLteOne = graph().traversal().V(source.id())
                                 .where(__.out("knows").count()
                                          .is(P.not(P.lte(1L))))
                                 .count().next();
        long notGtOne = graph().traversal().V(source.id())
                                .where(__.out("knows").count()
                                         .is(P.not(P.gt(1L))))
                                .count().next();
        long notGteThree = graph().traversal().V(source.id())
                                   .where(__.out("knows").count()
                                            .is(P.not(P.gte(3L))))
                                   .count().next();

        Assert.assertEquals(1L, notEqZero);
        Assert.assertEquals(0L, notNeqOne);
        Assert.assertEquals(1L, notLtTwo);
        Assert.assertEquals(1L, notLteOne);
        Assert.assertEquals(0L, notGtOne);
        Assert.assertEquals(1L, notGteThree);
    }

    @Test
    public void testNegatedScalarPredicatesUseComplementedHighRange() {
        assertNegatedCountHighRange(3L, P.eq(2L));
        assertNegatedCountHighRange(3L, P.neq(2L));
        assertNegatedCountHighRange(2L, P.lt(2L));
        assertNegatedCountHighRange(3L, P.lte(2L));
        assertNegatedCountHighRange(3L, P.gt(2L));
        assertNegatedCountHighRange(2L, P.gte(2L));
    }

    @Test
    public void testNegatedTextPredicateStaysLocal() {
        this.initTextRangeSchema(false);
        graph().schema().indexLabel("vl1ByVp4").onV("vl1")
               .by("vp4").secondary().create();
        graph().addVertex(T.label, "vl1", "vp4", "marko", "age", 29);
        graph().addVertex(T.label, "vl1", "vp4", "josh", "age", 32);
        commitTx();

        GraphTraversal<Vertex, Long> traversal = graph().traversal().V()
                                                        .hasLabel("vl1")
                                                        .has("vp4",
                                                             TextP.containing("ar")
                                                                  .negate())
                                                        .count();
        applyAndGetGraphStep(traversal);

        Assert.assertTrue(hasRemainingHasStep(traversal, "vp4"));
        Assert.assertEquals(1L, traversal.next().longValue());
    }

    @Test
    public void testNegatedNaNPredicatesKeepGremlinSemantics() {
        this.initNegatedDoubleSchema();
        graph().addVertex(T.label, "sample", "score", 1.0D);
        graph().addVertex(T.label, "sample", "score", Double.NaN);
        commitTx();

        long notLtNaN = graph().traversal().V()
                               .hasLabel("sample")
                               .has("score", P.not(P.lt(Double.NaN)))
                               .count().next();
        long notEqNaN = graph().traversal().V()
                               .hasLabel("sample")
                               .has("score", P.not(P.eq(Double.NaN)))
                               .count().next();

        Assert.assertEquals(2L, notLtNaN);
        Assert.assertEquals(2L, notEqNaN);
    }

    @Test
    public void testOptimizedGraphCountCanBeResetAndReused() {
        this.initSchema();
        this.initGraph();

        GraphTraversal<Vertex, Long> traversal = graph().traversal().V().count();

        Assert.assertEquals(3L, traversal.next());

        traversal.asAdmin().reset();

        Assert.assertEquals(3L, traversal.next());
    }

    @Test
    public void testOptimizedGraphCountEqualityIgnoresExecutionState() {
        this.initSchema();
        this.initGraph();

        GraphTraversal<Vertex, Long> first = graph().traversal().V().count();
        GraphTraversal<Vertex, Long> second = graph().traversal().V().count();
        first.asAdmin().applyStrategies();
        second.asAdmin().applyStrategies();

        Step<?, ?> firstStep = first.asAdmin().getEndStep();
        Step<?, ?> secondStep = second.asAdmin().getEndStep();
        Assert.assertInstanceOf(HugeCountStep.class, firstStep);
        Assert.assertInstanceOf(HugeCountStep.class, secondStep);
        Assert.assertEquals(firstStep, secondStep);

        int hashCode = firstStep.hashCode();
        Set<Step<?, ?>> steps = new HashSet<>();
        steps.add(firstStep);

        Assert.assertEquals(3L, first.next());

        Assert.assertEquals(hashCode, firstStep.hashCode());
        Assert.assertEquals(firstStep, secondStep);
        Assert.assertTrue(steps.contains(firstStep));
    }

    @Test
    public void testOptimizedGraphCountIncludesUncommittedRecords() {
        this.initSchema();
        graph().schema().indexLabel("personByName")
               .onV("person").by("name").create();

        graph().addVertex(T.label, "person", "name", "marko");

        long count = graph().traversal().V()
                            .hasLabel("person")
                            .has("name", "marko")
                            .count().next();

        Assert.assertEquals(1L, count);
    }

    @Test
    public void testWhereCountFlatAndContradictionEmpty() {
        this.initSchema();
        Vertex source = graph().addVertex(T.label, "person", "name", "source");
        commitTx();

        long count = graph().traversal().V(source.id())
                            .where(__.both("knows").count()
                                     .is(P.<Long>eq(0L).and(P.neq(0L))))
                            .count().next();

        Assert.assertEquals(0L, count);
    }

    @Test
    public void testWhereCountFlatAndContradictionNonEmpty() {
        this.initSchema();
        Vertex source = graph().addVertex(T.label, "person", "name", "source");
        Vertex target = graph().addVertex(T.label, "person", "name", "target");
        source.addEdge("knows", target);
        commitTx();

        long count = graph().traversal().V(source.id())
                            .where(__.both("knows").count()
                                     .is(P.<Long>eq(0L).and(P.neq(0L))))
                            .count().next();

        Assert.assertEquals(0L, count);
    }

    @Test
    public void testWhereCountFlatOrTautologyEmpty() {
        this.initSchema();
        Vertex source = graph().addVertex(T.label, "person", "name", "source");
        commitTx();

        long count = graph().traversal().V(source.id())
                            .where(__.both("knows").count()
                                     .is(P.<Long>eq(0L).or(P.neq(0L))))
                            .count().next();

        Assert.assertEquals(1L, count);
    }

    @Test
    public void testWhereCountFlatOrTautologyNonEmpty() {
        this.initSchema();
        Vertex source = graph().addVertex(T.label, "person", "name", "source");
        Vertex target = graph().addVertex(T.label, "person", "name", "target");
        source.addEdge("knows", target);
        commitTx();

        long count = graph().traversal().V(source.id())
                            .where(__.both("knows").count()
                                     .is(P.<Long>eq(0L).or(P.neq(0L))))
                            .count().next();

        Assert.assertEquals(1L, count);
    }

    @Test
    public void testWhereCountFlatConnectiveStillGetsRangeBound() {
        this.initSchema();
        this.initGraph();

        GraphTraversal<Vertex, Long> traversal = graph().traversal().V()
                                                        .where(__.out().count()
                                                                 .is(P.between(1, 18)))
                                                        .count();
        traversal.asAdmin().applyStrategies();

        boolean foundRangeStep = false;
        for (Step<?, ?> step : traversal.asAdmin().getSteps()) {
            if (step instanceof TraversalParent) {
                for (Traversal.Admin<?, ?> inner :
                     ((TraversalParent) step).getLocalChildren()) {
                    for (Step<?, ?> innerStep : inner.getSteps()) {
                        if (innerStep instanceof RangeGlobalStep) {
                            foundRangeStep = true;
                            break;
                        }
                    }
                }
            }
        }
        Assert.assertTrue("Expected RangeGlobalStep for flat ConnectiveP " +
                          "between(1,18)", foundRangeStep);

        long count = traversal.next();
        Assert.assertEquals(1L, count);
    }

    @Test
    public void testVertexLimitCountRejectsUncommittedAddition() {
        this.initSchema();
        graph().addVertex(T.label, "person", "name", "marko");

        assertUncommittedRangeUnsupported(
                graph().traversal().V().limit(1L).count());
    }

    @Test
    public void testVertexRangeCountRejectsUncommittedDeletion() {
        this.initSchema();
        graph().schema().indexLabel("personByName")
               .onV("person").by("name").create();
        this.initGraph();
        Vertex marko = graph().traversal().V()
                              .hasLabel("person")
                              .has("name", "marko")
                              .next();
        marko.remove();

        assertUncommittedRangeUnsupported(
                graph().traversal().V().range(1L, 3L).count());
    }

    @Test
    public void testQueryNumberKeepsOriginalAggregate() {
        this.initSchema();
        graph().addVertex(T.label, "person", "name", "marko");

        Query query = new Query(HugeType.VERTEX);
        Aggregate aggregate = new Aggregate(AggregateFunc.COUNT, null);
        query.aggregate(aggregate);

        Assert.assertEquals(1L, graph().queryNumber(query).longValue());
        Assert.assertSame(aggregate, query.aggregate());
    }

    @Test
    public void testUncommittedVertexCountClosesIteratorOnFailure() {
        FailingCloseableIterator<Vertex> vertices =
                new FailingCloseableIterator<>();
        AtomicBoolean dirty = new AtomicBoolean(true);
        GraphTransaction transaction =
                this.newFailingCountTransaction(vertices, null, dirty);

        try {
            Query query = countQuery(HugeType.VERTEX);
            Assert.assertThrows(IllegalStateException.class,
                                () -> transaction.queryNumber(query));
            Assert.assertTrue(vertices.closed());
        } finally {
            dirty.set(false);
            transaction.close();
        }
    }

    @Test
    public void testUncommittedEdgeCountClosesIteratorOnFailure() {
        FailingCloseableIterator<Edge> edges =
                new FailingCloseableIterator<>();
        AtomicBoolean dirty = new AtomicBoolean(true);
        GraphTransaction transaction =
                this.newFailingCountTransaction(null, edges, dirty);

        try {
            Query query = countQuery(HugeType.EDGE);
            Assert.assertThrows(IllegalStateException.class,
                                () -> transaction.queryNumber(query));
            Assert.assertTrue(edges.closed());
        } finally {
            dirty.set(false);
            transaction.close();
        }
    }

    @Test
    public void testOptimizedEdgeCountIncludesUncommittedRecords() {
        this.initSchema();
        graph().schema().indexLabel("personByName")
               .onV("person").by("name").create();
        this.initGraph();

        Vertex josh = graph().traversal().V()
                             .hasLabel("person").has("name", "josh").next();
        Vertex marko = graph().traversal().V()
                              .hasLabel("person").has("name", "marko").next();
        josh.addEdge("knows", marko);

        long count = graph().traversal().E().hasLabel("knows").count().next();

        Assert.assertEquals(2L, count);
    }

    private static Query countQuery(HugeType type) {
        Query query = new Query(type);
        query.aggregate(new Aggregate(AggregateFunc.COUNT, null));
        return query;
    }

    private GraphTransaction newFailingCountTransaction(
            Iterator<Vertex> vertices, Iterator<Edge> edges,
            AtomicBoolean dirty) {
        return new GraphTransaction(params(), params().loadGraphStore()) {

            @Override
            public boolean hasUpdate() {
                return dirty.get();
            }

            @Override
            public Iterator<Vertex> queryVertices(Query query) {
                return vertices;
            }

            @Override
            public Iterator<Edge> queryEdges(Query query) {
                return edges;
            }
        };
    }

    private static final class FailingCloseableIterator<T>
            implements CloseableIterator<T> {

        private boolean closed;

        @Override
        public boolean hasNext() {
            throw new IllegalStateException("Injected iterator failure");
        }

        @Override
        public T next() {
            throw new IllegalStateException("Injected iterator failure");
        }

        @Override
        public void close() {
            this.closed = true;
        }

        public boolean closed() {
            return this.closed;
        }
    }

    @Test
    public void testEdgeRangeCountRejectsUncommittedAddition() {
        this.initSchema();
        graph().schema().indexLabel("personByName")
               .onV("person").by("name").create();
        this.initGraph();
        Vertex josh = graph().traversal().V()
                             .hasLabel("person")
                             .has("name", "josh")
                             .next();
        Vertex marko = graph().traversal().V()
                              .hasLabel("person")
                              .has("name", "marko")
                              .next();
        josh.addEdge("knows", marko);

        assertUncommittedRangeUnsupported(
                graph().traversal().E().range(1L, 3L).count());
    }

    @Test
    public void testEdgeLimitCountRejectsUncommittedDeletion() {
        this.initSchema();
        this.initGraph();
        Edge edge = graph().traversal().E().hasLabel("knows").next();
        edge.remove();

        assertUncommittedRangeUnsupported(
                graph().traversal().E().limit(1L).count());
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
    public void testTextRangeFilterExtractsIndexedGraphHasContainers() {
        this.initTextRangeSchema(false);
        graph().schema().indexLabel("vl1ByAge").onV("vl1")
               .by("age").secondary().create();

        graph().addVertex(T.label, "vl1", "vp4", "a", "age", 1);
        graph().addVertex(T.label, "vl1", "vp4", "b", "age", 2);
        commitTx();

        GraphTraversal<Vertex, Long> traversal = graph().traversal().V()
                                                        .hasLabel("vl1")
                                                        .has("vp4", P.lt(""))
                                                        .has("age", 1)
                                                        .count();
        HugeGraphStep<?, ?> graphStep = applyAndGetGraphStep(traversal);

        Assert.assertEquals(2, graphStep.getHasContainers().size());
        Assert.assertTrue(graphStep.getHasContainers().stream().anyMatch(
                has -> T.label.getAccessor().equals(has.getKey())));
        Assert.assertTrue(graphStep.getHasContainers().stream().anyMatch(
                has -> "age".equals(has.getKey())));
        Assert.assertTrue(hasRemainingHasStep(traversal, "vp4"));
        Assert.assertFalse(hasRemainingHasStep(traversal, "age"));
        Assert.assertEquals(0L, traversal.next().longValue());
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
    public void testConnectiveLabelAfterNoIndexRangeMatchesMatchTraversal() {
        this.initConnectiveRangeNoIndexSchema();

        Vertex v1 = graph().addVertex(T.label, "vl1");
        Vertex v2 = graph().addVertex(T.label, "vl1");
        Vertex v3 = graph().addVertex(T.label, "vl1");
        v1.addEdge("el2", v2, "ep4", 0.1F);
        v1.addEdge("el2", v3, "ep4", 0.5F);
        v1.addEdge("el3", v2, "ep4", 0.1F);
        commitTx();

        Assert.assertEquals(2L, graph().traversal().E()
                                    .hasLabel("el2").count().next());

        GraphTraversal<Edge, Long> directTraversal = graph().traversal().E()
                                                           .has("ep4",
                                                                P.lt(0.32696354F))
                                                           .and(__.hasLabel("el2"))
                                                           .count();
        HugeGraphStep<?, ?> graphStep = applyAndGetGraphStep(directTraversal);
        Assert.assertEquals(1, graphStep.getHasContainers().size());
        Assert.assertEquals(T.label.getAccessor(),
                            graphStep.getHasContainers().get(0).getKey());
        Assert.assertTrue(hasRemainingHasStep(directTraversal, "ep4"));
        long direct = directTraversal.next();
        long viaMatch = graph().traversal().E()
                             .has("ep4", P.lt(0.32696354F))
                             .match(__.<Edge>as("start1")
                                      .and(__.hasLabel("el2"))
                                      .as("m1"))
                             .<Edge>select("m1").count().next();

        Assert.assertEquals(1L, direct);
        Assert.assertEquals(direct, viaMatch);
    }

    @Test
    public void testOrConnectiveLabelsAfterNoIndexRangeMatchesLabelTraversal() {
        this.initConnectiveRangeNoIndexSchema();

        Vertex v1 = graph().addVertex(T.label, "vl1");
        Vertex v2 = graph().addVertex(T.label, "vl1");
        Vertex v3 = graph().addVertex(T.label, "vl1");
        v1.addEdge("el2", v2, "ep4", 0.1F);
        v1.addEdge("el2", v3, "ep4", 0.5F);
        v1.addEdge("el3", v2, "ep4", 0.1F);
        commitTx();

        long count = graph().traversal().E()
                          .has("ep4", P.lt(0.32696354F))
                          .or(__.hasLabel("el2"), __.hasLabel("el3"))
                          .count().next();

        Assert.assertEquals(2L, count);
    }

    @Test
    public void testPropertyBeforeLabelNoIndexRangeStillThrows() {
        this.initConnectiveRangeNoIndexSchema();

        Vertex v1 = graph().addVertex(T.label, "vl1");
        Vertex v2 = graph().addVertex(T.label, "vl1");
        v1.addEdge("el2", v2, "ep4", 0.1F);
        commitTx();

        Assert.assertThrows(NoIndexException.class, () -> {
            graph().traversal().E()
                   .has("ep4", P.lt(0.32696354F))
                   .hasLabel("el2")
                   .count().next();
        });
    }

    @Test
    public void testNonLabelConnectiveAfterNoIndexRangeStillThrows() {
        this.initConnectiveRangeNoIndexSchema();

        Vertex v1 = graph().addVertex(T.label, "vl1");
        Vertex v2 = graph().addVertex(T.label, "vl1");
        v1.addEdge("el2", v2, "ep4", 0.1F);
        commitTx();

        Assert.assertThrows(NoIndexException.class, () -> {
            graph().traversal().E()
                   .has("ep4", P.lt(0.32696354F))
                   .and(__.has("ep4", P.gt(0.0F)))
                   .count().next();
        });
    }

    @Test
    public void testNegativeConnectiveLabelAfterNoIndexRangeStaysLocal() {
        this.initConnectiveRangeNoIndexSchema();

        Vertex v1 = graph().addVertex(T.label, "vl1");
        Vertex v2 = graph().addVertex(T.label, "vl1");
        v1.addEdge("el2", v2, "ep4", 0.1F);
        commitTx();

        GraphTraversal<Edge, Edge> traversal = graph().traversal().E()
                                                     .has("ep4",
                                                          P.lt(0.32696354F))
                                                     .and(__.hasLabel(P.neq("el2")));

        HugeGraphStep<?, ?> graphStep = applyAndGetGraphStep(traversal);
        for (HasContainer has : graphStep.getHasContainers()) {
            Assert.assertNotEquals(T.label.getAccessor(), has.getKey());
        }
        Assert.assertTrue(hasRemainingHasStep(traversal, T.label.getAccessor()));
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
    public void testMatchWithNegatedBooleanPredicateKeepsHas() {
        this.initMatchNoIndexSchema();
        graph().schema().indexLabel("vl1ByVp2").onV("vl1")
               .by("vp2").secondary().create();
        this.initMatchNoIndexGraph();

        GraphTraversal<Vertex, Long> traversal = graph().traversal().V()
                                                        .has("vp2",
                                                             P.not(P.eq(true)))
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

    @Test
    public void testNegatedBooleanComparisonsKeepGremlinSemantics() {
        this.initMatchNoIndexSchema();
        graph().schema().indexLabel("vl1ByVp2").onV("vl1")
               .by("vp2").secondary().create();
        this.initMatchNoIndexGraph();

        this.assertNegatedBooleanPredicate(1L, P.eq(true));
        this.assertNegatedBooleanPredicate(1L, P.eq(false));
        this.assertNegatedBooleanPredicate(1L, P.neq(true));
        this.assertNegatedBooleanPredicate(1L, P.neq(false));
        this.assertNegatedBooleanPredicate(1L, P.lt(true));
        this.assertNegatedBooleanPredicate(2L, P.lt(false));
        this.assertNegatedBooleanPredicate(0L, P.lte(true));
        this.assertNegatedBooleanPredicate(1L, P.lte(false));
        this.assertNegatedBooleanPredicate(2L, P.gt(true));
        this.assertNegatedBooleanPredicate(1L, P.gt(false));
        this.assertNegatedBooleanPredicate(1L, P.gte(true));
        this.assertNegatedBooleanPredicate(0L, P.gte(false));
        this.assertNegatedBooleanPredicate(1L,
                                           P.eq(true).and(P.gte(false)));
        this.assertNegatedBooleanPredicate(0L,
                                           P.eq(true).or(P.lt(true)));
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
    public void testMatchWithNegatedNumericRangeConditionKeepsHas() {
        this.initMatchNoIndexSchema();
        graph().schema().indexLabel("vl0ByVp3").onV("vl0")
               .by("vp3").range().create();
        graph().schema().indexLabel("vl1ByVp2").onV("vl1")
               .by("vp2").secondary().create();
        this.initMatchNoIndexGraph();

        GraphTraversal<Vertex, Long> traversal = graph().traversal().V()
                                                        .has("vp3", P.not(P.lte(
                                                                4592737712018141718L)))
                                                        .has("vp2", true)
                                                        .match(__.<Vertex>as("s")
                                                                 .has("vp2")
                                                                 .as("m"))
                                                        .<Vertex>select("m")
                                                        .count();

        HugeGraphStep<?, ?> graphStep = applyAndGetGraphStep(traversal);
        Assert.assertEquals(0, graphStep.getHasContainers().size());
        Assert.assertTrue(hasRemainingHasStep(traversal, "vp3"));
        Assert.assertTrue(hasRemainingHasStep(traversal, "vp2"));
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

    @Test
    public void testConnectiveAndCountIsZero() {
        this.initSchema();
        this.initGraph();

        long count = graph().traversal().V()
                            .filter(__.and(__.out().count().is(0),
                                           __.in().count().is(0)))
                            .count().next();

        Assert.assertEquals(0L, count);
    }

    @Test
    public void testConnectiveOrCountIsZero() {
        this.initSchema();
        this.initGraph();

        long count = graph().traversal().V()
                            .filter(__.or(__.out().count().is(0),
                                          __.in().count().is(0)))
                            .count().next();

        Assert.assertEquals(3L, count);
    }

    @Test
    public void testWhereOrWithMultiStepCountIsZero() {
        this.initSchema();
        this.initGraph();

        long count = graph().traversal().V()
                            .where(__.or(__.out("created").out("knows")
                                           .count().is(0),
                                         __.has("none")))
                            .count().next();

        Assert.assertEquals(3L, count);
    }

    @Test
    public void testWhereOrWithMultipleCountIsZero() {
        this.initSchema();
        this.initGraph();

        long count = graph().traversal().V()
                            .where(__.or(__.out("created").out("knows")
                                           .count().is(0),
                                         __.has("none").count().is(0)))
                            .count().next();

        Assert.assertEquals(3L, count);
    }
}
