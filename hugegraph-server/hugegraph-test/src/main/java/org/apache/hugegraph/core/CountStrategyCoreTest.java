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
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hugegraph.HugeException;
import org.apache.hugegraph.backend.query.Aggregate;
import org.apache.hugegraph.backend.query.Aggregate.AggregateFunc;
import org.apache.hugegraph.backend.query.Condition;
import org.apache.hugegraph.backend.query.ConditionQuery;
import org.apache.hugegraph.backend.query.Query;
import org.apache.hugegraph.backend.tx.GraphTransaction;
import org.apache.hugegraph.exception.NoIndexException;
import org.apache.hugegraph.schema.SchemaManager;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.traversal.optimize.ConditionP;
import org.apache.hugegraph.traversal.optimize.HugeCountStep;
import org.apache.hugegraph.traversal.optimize.HugeCountStepStrategy;
import org.apache.hugegraph.traversal.optimize.HugeCountStrategy;
import org.apache.hugegraph.traversal.optimize.HugeGraphStep;
import org.apache.hugegraph.traversal.optimize.HugeGraphStepStrategy;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.type.define.HugeKeys;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.TextP;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.CountGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderGlobalStep;
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
            HasContainerHolder holder =
                    (HasContainerHolder) step;
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

    private static void assertUnoptimizedCount(long expected,
                                                GraphTraversal<?, Long> traversal) {
        traversal.asAdmin().applyStrategies();

        Assert.assertInstanceOf(CountGlobalStep.class, traversal.asAdmin().getEndStep());
        Assert.assertFalse(traversal.asAdmin().getSteps().stream()
                                   .anyMatch(step -> step instanceof HugeCountStep));
        Assert.assertEquals(Collections.singletonList(expected), traversal.toList());
    }

    private void assertRepeatedGraphCounts(long vertices) {
        assertUnoptimizedCount(vertices * vertices,
                               graph().traversal().V().V().count());
        assertUnoptimizedCount(2L * vertices,
                               graph().traversal().inject(1, 2).V().count());
        // Repeated inputs can be bulked, but each must still contribute a scan.
        assertUnoptimizedCount(2L * vertices,
                               graph().traversal().inject(1, 1).barrier().V().count());

        GraphTraversal<Vertex, Long> root = graph().traversal().V().count();
        root.asAdmin().applyStrategies();
        Assert.assertInstanceOf(HugeCountStep.class, root.asAdmin().getStartStep());
        Assert.assertEquals(Collections.singletonList(vertices), root.toList());
    }

    @Test
    public void testRepeatedGraphCountOnEmptyGraph() {
        this.initSchema();

        this.assertRepeatedGraphCounts(0L);
    }

    @Test
    public void testRepeatedGraphCountOnSingleVertex() {
        this.initSchema();
        graph().addVertex(T.label, "person", "name", "marko");
        commitTx();

        this.assertRepeatedGraphCounts(1L);
    }

    @Test
    public void testRepeatedGraphCountOnMultipleVertices() {
        this.initSchema();
        this.initGraph();

        this.assertRepeatedGraphCounts(3L);
    }

    @Test
    public void testRepeatedGraphCountWithIds() {
        this.initSchema();
        this.initGraph();
        Object id = graph().traversal().V().next().id();

        assertUnoptimizedCount(3L, graph().traversal().V().V(id).count());
        assertUnoptimizedCount(2L, graph().traversal().inject(1, 2).V(id).count());
        assertUnoptimizedCount(1L, graph().traversal().V(id).V(id).count());
    }

    @Test
    public void testCountAfterOrderByPresentProperty() {
        this.initSchema();
        this.initGraph();

        GraphTraversal<Vertex, Long> traversal = graph().traversal().V()
                                                        .order().by("name").count();
        traversal.asAdmin().applyStrategies();

        Assert.assertFalse(traversal.asAdmin().getEndStep() instanceof HugeCountStep);
        Assert.assertTrue(traversal.asAdmin().getSteps().stream()
                                   .anyMatch(step -> step instanceof OrderGlobalStep));
        Assert.assertEquals(3L, traversal.next().longValue());
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
    public void testSystemCountsIgnoreUncommittedVertices() {
        this.initSchema();
        for (HugeType type : new HugeType[]{HugeType.TASK, HugeType.SERVER}) {
            Query query = new Query(type);
            query.aggregate(new Aggregate(AggregateFunc.COUNT, null));
            long before = graph().queryNumber(query).longValue();
            graph().addVertex(T.label, "person", "name", "uncommitted");
            Assert.assertEquals(before, graph().queryNumber(query).longValue());
            graph().tx().rollback();
        }
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
        CountCloseableIterator<Vertex> vertices =
                new CountCloseableIterator<>(true);
        AtomicBoolean dirty = new AtomicBoolean(true);
        GraphTransaction transaction =
                this.newCountTransaction(vertices, null, dirty);

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
        CountCloseableIterator<Edge> edges =
                new CountCloseableIterator<>(true);
        AtomicBoolean dirty = new AtomicBoolean(true);
        GraphTransaction transaction =
                this.newCountTransaction(null, edges, dirty);

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
    public void testCommittedVertexCountClosesIterator() {
        this.assertCommittedVertexCountClosesIterator(false);
    }

    @Test
    public void testCommittedVertexCountClosesIteratorOnFailure() {
        this.assertCommittedVertexCountClosesIterator(true);
    }

    private void assertCommittedVertexCountClosesIterator(boolean fail) {
        SchemaManager schema = graph().schema();
        schema.propertyKey("name").asText().create();
        schema.vertexLabel("person").properties("name")
              .primaryKeys("name").create();

        ConditionQuery query = new ConditionQuery(HugeType.VERTEX);
        query.eq(HugeKeys.LABEL, graph().vertexLabel("person").id());
        query.query(Condition.eq(graph().propertyKey("name").id(), "marko"));
        query.aggregate(new Aggregate(AggregateFunc.COUNT, null));

        CountCloseableIterator<Vertex> vertices = new CountCloseableIterator<>(fail);
        GraphTransaction transaction = this.newCountTransaction(
                vertices, null, new AtomicBoolean(false));
        try {
            // Primary-key optimization produces an IdQuery that must scan to count.
            if (fail) {
                Assert.assertThrows(IllegalStateException.class,
                                    () -> transaction.queryNumber(query));
            } else {
                Assert.assertEquals(1L, transaction.queryNumber(query).longValue());
            }
            Assert.assertTrue(vertices.closed());
        } finally {
            transaction.close();
        }
    }

    @Test
    public void testLabelPredicatesWithNullHostileCollections() {
        this.initSchema();
        Vertex person = graph().addVertex(T.label, "person", "name", "marko");
        Vertex software = graph().addVertex(T.label, "software", "name", "market");
        commitTx();

        List<Collection<String>> values = List.of(List.of("person"), Set.of("person"));
        for (Collection<String> labels : values) {
            Assert.assertEquals(Collections.singletonList(person),
                                graph().traversal().V().hasLabel(P.within(labels)).toList());
            Assert.assertEquals(1L, graph().traversal().V().hasLabel(P.within(labels))
                                         .count().next().longValue());
            GraphTraversal<Vertex, Vertex> without = graph().traversal().V()
                    .hasLabel(P.without(labels)).has("name", TextP.containing("ar"));
            without.asAdmin().applyStrategies();
            Assert.assertTrue(hasRemainingHasStep(without, T.label.getAccessor()));
            Assert.assertEquals(Collections.singletonList(software), without.toList());
        }
    }

    @Test
    public void testLabelPredicatesWithMutableNullValuesStayLocal() {
        this.initSchema();
        Vertex person = graph().addVertex(T.label, "person", "name", "marko");
        Vertex software = graph().addVertex(T.label, "software", "name", "market");
        commitTx();

        Collection<String> list = new ArrayList<>(List.of("person"));
        list.add(null);
        Collection<String> set = new HashSet<>(list);
        for (Collection<String> labels : List.of(list, set)) {
            GraphTraversal<Vertex, Vertex> within = graph().traversal().V()
                    .hasLabel(P.within(labels));
            within.asAdmin().applyStrategies();
            Assert.assertTrue(hasRemainingHasStep(within, T.label.getAccessor()));
            Assert.assertEquals(Collections.singletonList(person), within.toList());
            GraphTraversal<Vertex, Vertex> without = graph().traversal().V()
                    .hasLabel(P.without(labels));
            without.asAdmin().applyStrategies();
            Assert.assertTrue(hasRemainingHasStep(without, T.label.getAccessor()));
            Assert.assertEquals(Collections.singletonList(software), without.toList());
        }
    }

    @Test
    public void testMixedTextNativeIdFilterKeepsAllConditionsLocal() {
        this.initSchema();
        Vertex josh = graph().addVertex(T.label, "person", "name", "josh");
        Vertex marko = graph().addVertex(T.label, "person", "name", "marko");
        commitTx();

        GraphTraversalSource[] sources = {
                graph().traversal(),
                graph().traversal().withoutStrategies(HugeGraphStepStrategy.class,
                                                     HugeCountStepStrategy.class)
        };
        for (GraphTraversalSource source : sources) {
            assertMixedTextNativeIdFilter(source, marko.id(), "ar",
                                         Collections.singletonList(marko));
            assertMixedTextNativeIdFilter(source, josh.id(), "ar",
                                         Collections.emptyList());
            assertMixedTextNativeIdFilter(source, marko.id(), "osh",
                                         Collections.emptyList());
            Assert.assertEquals(Collections.singletonList(marko),
                                source.V().hasId(marko.id()).toList());
            Assert.assertEquals(1L, source.V().hasId(P.eq(marko.id()))
                                         .count().next().longValue());
        }
    }

    private static void assertMixedTextNativeIdFilter(GraphTraversalSource source,
                                                       Object id, String text,
                                                       List<Vertex> expected) {
        GraphTraversal<Vertex, Vertex> traversal = nativeIdTextQuery(source, id, text);
        traversal.asAdmin().applyStrategies();
        assertMixedTextNativeIdPlan(traversal);
        Assert.assertEquals(expected, traversal.toList());

        GraphTraversal<Vertex, Long> count = nativeIdTextQuery(source, id, text).count();
        assertUnoptimizedCount(expected.size(), count);
        assertMixedTextNativeIdPlan(count);
        Assert.assertEquals(expected, nativeIdTextQuery(source, id, text)
                                      .limit(1).toList());
        Assert.assertEquals(Collections.emptyList(),
                            nativeIdTextQuery(source, id, text).range(1, 2).toList());
    }

    private static GraphTraversal<Vertex, Vertex> nativeIdTextQuery(
            GraphTraversalSource source, Object id, String text) {
        return source.V().hasId(ConditionP.eq(id))
                     .has("name", TextP.containing(text));
    }

    private static void assertMixedTextNativeIdPlan(GraphTraversal<?, ?> traversal) {
        Assert.assertTrue(hasRemainingHasStep(traversal, T.id.getAccessor()));
        Assert.assertTrue(hasRemainingHasStep(traversal, "name"));
        Assert.assertEquals(0, ((GraphStep<?, ?>) traversal.asAdmin()
                                                       .getStartStep()).getIds().length);
    }

    @Test
    public void testMixedTextEdgeFilterKeepsAllConditionsLocal() {
        this.initSchema();
        graph().schema().propertyKey("weight").asInt().create();
        graph().schema().edgeLabel("rated").link("person", "person")
               .properties("name", "weight").create();
        Vertex source = graph().addVertex(T.label, "person", "name", "source");
        Vertex target = graph().addVertex(T.label, "person", "name", "target");
        source.addEdge("rated", target, "name", "marko", "weight", 1);
        Vertex other = graph().addVertex(T.label, "person", "name", "other");
        Edge expected = source.addEdge("rated", other,
                                       "name", "marko", "weight", 2);
        commitTx();

        List<Edge> edges = graph().traversal().V(source.id()).outE("rated")
                                  .has("name", TextP.containing("ar"))
                                  .has("weight", 2).toList();
        Assert.assertEquals(1, edges.size());
        Assert.assertEquals(expected.id(), edges.get(0).id());
        Assert.assertEquals(1L, graph().traversal().V(source.id()).outE("rated")
                                      .has("name", TextP.containing("ar"))
                                      .has("weight", 2).count().next().longValue());
    }

    @Test
    public void testUpdatedThenRemovedVertexIsAbsentFromCounts() {
        this.initSchema();
        Vertex vertex = graph().addVertex(T.label, "person", "name", "before");
        commitTx();
        Object id = vertex.id();
        vertex = graph().traversal().V(id).next();
        vertex.property("name", "after");
        vertex.remove();

        this.assertRemovedVertexCounts(id);
        graph().tx().rollback();
        Assert.assertEquals(1L, graph().traversal().V(id).count().next().longValue());
        Assert.assertEquals(1L, graph().traversal().V().hasLabel("person")
                                      .count().next().longValue());
        Assert.assertEquals("before", graph().traversal().V(id).values("name").next());
    }

    private void assertRemovedVertexCounts(Object id) {
        Assert.assertEquals(0L, graph().traversal().V(id).count().next().longValue());
        Assert.assertEquals(0L, graph().traversal().V().hasLabel("person")
                                      .count().next().longValue());
        Assert.assertEquals(0L, graph().traversal().V().count().next().longValue());
        Assert.assertTrue(graph().traversal().V().toList().isEmpty());
        Assert.assertEquals(0L, graph().traversal().V()
                                      .has("name", TextP.containing("after"))
                                      .count().next().longValue());
    }

    @Test
    public void testUpdatedThenRemovedEdgeIsAbsentFromCounts() {
        this.assertUpdatedThenRemovedEdgeCounts(false);
    }

    @Test
    public void testUpdatedThenRemovedSelfLoopIsAbsentFromCounts() {
        this.assertUpdatedThenRemovedEdgeCounts(true);
    }

    private void assertUpdatedThenRemovedEdgeCounts(boolean selfLoop) {
        this.initSchema();
        graph().schema().edgeLabel("rated").link("person", "person")
               .properties("name").create();
        Vertex source = graph().addVertex(T.label, "person", "name", "source");
        Vertex target = selfLoop ? source :
                        graph().addVertex(T.label, "person", "name", "target");
        Edge edge = source.addEdge("rated", target, "name", "before");
        commitTx();
        Object id = edge.id();
        edge = graph().traversal().V(target.id()).inE("rated").next();
        edge.property("name", "after");
        edge.remove();

        this.assertRemovedEdgeCounts(id, source, target);
        graph().tx().rollback();
        Assert.assertEquals(1L, graph().traversal().E(id).count().next().longValue());
        Assert.assertEquals(1L, graph().traversal().E().hasLabel("rated")
                                      .count().next().longValue());
        Assert.assertEquals(selfLoop ? 2L : 1L,
                            graph().traversal().V(source.id()).bothE("rated")
                                   .count().next().longValue());
        Assert.assertEquals("before", graph().traversal().E(id).values("name").next());
    }

    private void assertRemovedEdgeCounts(Object id, Vertex source, Vertex target) {
        Assert.assertEquals(0L, graph().traversal().E(id).count().next().longValue());
        Assert.assertEquals(0L, graph().traversal().V(source.id()).bothE("rated")
                                      .count().next().longValue());
        Assert.assertTrue(graph().traversal().V(source.id()).bothE("rated")
                                 .toList().isEmpty());
        Assert.assertEquals(0L, graph().traversal().V(source.id()).outE("rated")
                                      .count().next().longValue());
        Assert.assertEquals(0L, graph().traversal().V(target.id()).inE("rated")
                                      .count().next().longValue());
        Assert.assertEquals(0L, graph().traversal().E().hasLabel("rated")
                                      .count().next().longValue());
        Assert.assertEquals(0L, graph().traversal().E().count().next().longValue());
        Assert.assertTrue(graph().traversal().E().toList().isEmpty());
    }

    @Test
    public void testDirtyVertexIndexCountRemainsUnsupported() {
        this.initSchema();
        graph().schema().indexLabel("personByName").onV("person")
               .by("name").secondary().create();
        Vertex vertex = graph().addVertex(T.label, "person", "name", "before");
        commitTx();
        vertex = graph().traversal().V(vertex.id()).next();
        vertex.property("name", "after");

        Assert.assertEquals(1L, graph().traversal().V(vertex.id())
                                      .count().next().longValue());
        Assert.assertEquals(1L, graph().traversal().V().count().next().longValue());
        assertDirtyIndexCountUnsupported(graph().traversal().V()
                .hasLabel("person").has("name", "before").count());
        this.assertDirtyIndexLabelCount(graph().traversal().V()
                .hasLabel("person").count());
    }

    @Test
    public void testDirtyEdgeIndexCountRemainsUnsupported() {
        this.initSchema();
        graph().schema().edgeLabel("rated").link("person", "person")
               .properties("name").create();
        graph().schema().indexLabel("ratedByName").onE("rated")
               .by("name").secondary().create();
        Vertex source = graph().addVertex(T.label, "person", "name", "source");
        Vertex target = graph().addVertex(T.label, "person", "name", "target");
        Edge edge = source.addEdge("rated", target, "name", "before");
        commitTx();
        edge = graph().traversal().E(edge.id()).next();
        edge.property("name", "after");

        Assert.assertEquals(1L, graph().traversal().E(edge.id())
                                      .count().next().longValue());
        Assert.assertEquals(1L, graph().traversal().E().count().next().longValue());
        assertDirtyIndexCountUnsupported(graph().traversal().E()
                .hasLabel("rated").has("name", "before").count());
        this.assertDirtyIndexLabelCount(graph().traversal().E()
                .hasLabel("rated").count());
    }

    private void assertDirtyIndexLabelCount(GraphTraversal<?, Long> traversal) {
        if (storeFeatures().supportsQueryByLabel()) {
            Assert.assertEquals(1L, traversal.next().longValue());
        } else {
            assertDirtyIndexCountUnsupported(traversal);
        }
    }

    private static void assertDirtyIndexCountUnsupported(GraphTraversal<?, Long> traversal) {
        Assert.assertThrows(HugeException.class, traversal::next, e -> {
            Assert.assertContains("Can't do index query when there are " +
                                  "changes in transaction", e.getMessage());
        });
    }

    @Test
    public void testRemovedVertexCanBeReaddedWithSameId() {
        this.initSchema();
        graph().schema().vertexLabel("reused").properties("name")
               .useCustomizeStringId().create();
        Vertex vertex = graph().addVertex(T.label, "reused", T.id, "same",
                                         "name", "before");
        commitTx();
        vertex = graph().traversal().V(vertex.id()).next();
        vertex.property("name", "changed");
        vertex.remove();
        Vertex replacement = graph().addVertex(T.label, "reused", T.id, "same",
                                               "name", "again");

        Assert.assertEquals(1L, graph().traversal().V(replacement.id())
                                      .count().next().longValue());
        Assert.assertEquals(1L, graph().traversal().V().hasLabel("reused")
                                      .count().next().longValue());
        Assert.assertEquals(Collections.singletonList(replacement),
                            graph().traversal().V().toList());
        Assert.assertEquals("again", graph().traversal().V(replacement.id())
                                           .values("name").next());
    }

    @Test
    public void testReaddedVertexShadowsUpdatedVertexBeforeLabelMatching() {
        SchemaManager schema = graph().schema();
        schema.propertyKey("name").asText().create();
        schema.vertexLabel("person").properties("name")
              .useCustomizeStringId().create();
        schema.vertexLabel("software").properties("name")
              .useCustomizeStringId().create();
        Vertex vertex = graph().addVertex(T.label, "person", T.id, "same",
                                         "name", "before");
        commitTx();
        Object id = vertex.id();
        vertex = graph().traversal().V(id).next();
        vertex.property("name", "after");
        vertex.remove();
        Vertex replacement = graph().addVertex(T.label, "software", T.id, "same",
                                               "name", "again");

        Assert.assertEquals(0L, graph().traversal().V(id).hasLabel("person")
                                      .count().next().longValue());
        Assert.assertTrue(graph().traversal().V(id).hasLabel("person")
                                 .toList().isEmpty());
        Assert.assertEquals(0L, graph().traversal().V().hasLabel("person")
                                      .count().next().longValue());
        Assert.assertTrue(graph().traversal().V().hasLabel("person")
                                 .toList().isEmpty());
        Assert.assertEquals(1L, graph().traversal().V(id).hasLabel("software")
                                      .count().next().longValue());
        Assert.assertEquals(1L, graph().traversal().V().hasLabel("software")
                                      .count().next().longValue());
        Assert.assertEquals(Collections.singletonList(replacement),
                            graph().traversal().V().hasLabel("software").toList());
        Assert.assertEquals(Collections.singletonList(replacement),
                            graph().traversal().V(id).toList());
        Assert.assertEquals(Collections.singletonList(replacement),
                            graph().traversal().V().toList());
        Assert.assertEquals(1L, graph().traversal().V().count().next().longValue());
        Assert.assertEquals("again", graph().traversal().V(id).values("name").next());

        graph().tx().rollback();
        Assert.assertEquals(1L, graph().traversal().V().hasLabel("person")
                                      .count().next().longValue());
        Assert.assertEquals(0L, graph().traversal().V().hasLabel("software")
                                      .count().next().longValue());
        Assert.assertEquals("before", graph().traversal().V(id).values("name").next());
    }

    @Test
    public void testRemovedEdgeCanBeReaddedWithSameId() {
        this.initSchema();
        graph().schema().edgeLabel("rated").link("person", "person")
               .properties("name").create();
        Vertex source = graph().addVertex(T.label, "person", "name", "source");
        Vertex target = graph().addVertex(T.label, "person", "name", "target");
        Edge edge = source.addEdge("rated", target, "name", "before");
        commitTx();
        edge = graph().traversal().E(edge.id()).next();
        edge.property("name", "changed");
        edge.remove();
        Edge replacement = source.addEdge("rated", target, "name", "again");
        Assert.assertEquals(edge.id(), replacement.id());
        Assert.assertEquals(1L, graph().traversal().E(replacement.id())
                                      .count().next().longValue());
        Assert.assertEquals(1L, graph().traversal().E().hasLabel("rated")
                                      .count().next().longValue());
        Assert.assertEquals(Collections.singletonList(replacement),
                            graph().traversal().E().toList());
        Assert.assertEquals("again", graph().traversal().E(replacement.id())
                                           .values("name").next());
        Assert.assertTrue(graph().traversal().E().hasLabel("rated")
                                 .has("name", TextP.containing("changed"))
                                 .toList().isEmpty());
        Assert.assertEquals(0L, graph().traversal().E().hasLabel("rated")
                                      .has("name", TextP.containing("changed"))
                                      .count().next().longValue());
        Assert.assertEquals(0L, graph().traversal().E(replacement.id())
                                      .has("name", TextP.containing("changed"))
                                      .count().next().longValue());
        Assert.assertEquals(Collections.singletonList(replacement),
                            graph().traversal().E().hasLabel("rated")
                                   .has("name", TextP.containing("again")).toList());
        Assert.assertEquals(1L, graph().traversal().E().hasLabel("rated")
                                      .has("name", TextP.containing("again"))
                                      .count().next().longValue());

        graph().tx().rollback();
        Assert.assertEquals(1L, graph().traversal().E(replacement.id())
                                      .count().next().longValue());
        Assert.assertEquals("before", graph().traversal().E(replacement.id())
                                            .values("name").next());
    }

    @Test
    public void testAddedSelfLoopCountKeepsBothDirections() {
        this.initSchema();
        Vertex vertex = graph().addVertex(T.label, "person", "name", "loop");
        vertex.addEdge("knows", vertex);
        this.assertSelfLoopCounts(vertex);
        commitTx();
        this.assertSelfLoopCounts(vertex);
    }

    @Test
    public void testUpdatedSelfLoopCountKeepsBothDirections() {
        this.initSchema();
        graph().schema().edgeLabel("rated").link("person", "person")
               .properties("name").create();
        Vertex vertex = graph().addVertex(T.label, "person", "name", "loop");
        Edge loop = vertex.addEdge("rated", vertex, "name", "before");
        commitTx();
        loop = graph().traversal().E(loop.id()).next();
        loop.property("name", "after");
        this.assertSelfLoopCounts(vertex);
        commitTx();
        this.assertSelfLoopCounts(vertex);
        loop.remove();
        Assert.assertEquals(0L, graph().traversal().V(vertex.id())
                                      .bothE().count().next().longValue());
    }

    private void assertSelfLoopCounts(Vertex vertex) {
        Assert.assertEquals(2L, graph().traversal().V(vertex.id())
                                      .bothE().count().next().longValue());
        Assert.assertEquals(2, graph().traversal().V(vertex.id())
                                     .bothE().toList().size());
        Assert.assertEquals(1L, graph().traversal().V(vertex.id())
                                      .outE().count().next().longValue());
        Assert.assertEquals(1L, graph().traversal().V(vertex.id())
                                      .inE().count().next().longValue());
        Assert.assertEquals(1L, graph().traversal().E().count().next().longValue());
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

    private GraphTransaction newCountTransaction(
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

    private static final class CountCloseableIterator<T>
            implements CloseableIterator<T> {

        private final boolean fail;
        private boolean consumed;
        private boolean closed;

        private CountCloseableIterator(boolean fail) {
            this.fail = fail;
        }

        @Override
        public boolean hasNext() {
            if (this.fail) {
                throw new IllegalStateException("Injected iterator failure");
            }
            return !this.consumed;
        }

        @Override
        public T next() {
            if (!this.hasNext()) {
                throw new NoSuchElementException();
            }
            this.consumed = true;
            return null;
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
