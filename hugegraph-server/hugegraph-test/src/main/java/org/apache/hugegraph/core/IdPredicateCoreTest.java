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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;

import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.traversal.optimize.HugeCountStepStrategy;
import org.apache.hugegraph.traversal.optimize.HugeGraphStepStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.GType;
import org.apache.tinkerpop.gremlin.process.traversal.NotP;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.TextP;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

public class IdPredicateCoreTest extends BaseCoreTest {

    private static final UUID UUID1 = UUID.fromString("835e1153-9281-4957-8691-cf79258e90eb");
    private static final UUID UUID2 = UUID.fromString("835e1153-9281-4957-8691-cf79258e90ee");
    private static final UUID MISSING = UUID.fromString("835e1153-9281-4957-8691-cf79258e90ef");

    private GraphTraversalSource[] sources() {
        return new GraphTraversalSource[] {
                graph().traversal(),
                graph().traversal().withoutStrategies(HugeGraphStepStrategy.class,
                                                     HugeCountStepStrategy.class)
        };
    }

    private Vertex[] initUuidGraph() {
        graph().schema().propertyKey("name").asText().create();
        graph().schema().vertexLabel("uuid").useCustomizeUuidId().properties("name").create();
        Vertex first = graph().addVertex(T.label, "uuid", T.id, UUID1, "name", "first");
        Vertex second = graph().addVertex(T.label, "uuid", T.id, UUID2, "name", "second");
        commitTx();
        return new Vertex[] {first, second};
    }

    private static HasContainer localIdContainer(GraphTraversal<?, ?> traversal) {
        traversal.asAdmin().applyStrategies();
        Assert.assertInstanceOf(GraphStep.class, traversal.asAdmin().getStartStep());
        Assert.assertEquals(0, ((GraphStep<?, ?>) traversal.asAdmin().getStartStep()).getIds().length);
        for (Step<?, ?> step : traversal.asAdmin().getSteps()) {
            if (!(step instanceof HasStep)) {
                continue;
            }
            for (HasContainer has : ((HasStep<?>) step).getHasContainers()) {
                if (T.id.getAccessor().equals(has.getKey())) {
                    return has;
                }
            }
        }
        throw new AssertionError("The local ID predicate must remain in HasStep");
    }

    private static void assertLocalId(GraphTraversalSource source, P<?> predicate,
                                      Vertex... expected) {
        GraphTraversal<Vertex, Vertex> traversal = source.V().hasId(predicate);
        localIdContainer(traversal);
        List<Vertex> actual = traversal.toList();
        Assert.assertEquals(expected.length, actual.size());
        Assert.assertEquals(new HashSet<>(Arrays.asList(expected)), new HashSet<>(actual));
    }

    @Test
    public void testNegatedUuidId() {
        Vertex[] vertices = this.initUuidGraph();
        GraphTraversal<Vertex, Vertex> equality = graph().traversal().V().hasId(P.eq(UUID1));
        equality.asAdmin().applyStrategies();
        Assert.assertEquals(1, ((GraphStep<?, ?>) equality.asAdmin().getStartStep()).getIds().length);
        Assert.assertEquals(Collections.singletonList(vertices[0]), equality.toList());
        for (GraphTraversalSource source : this.sources()) {
            Assert.assertEquals(Collections.singletonList(vertices[0]),
                                source.V().hasId(P.eq(vertices[0].id())).toList());
            assertLocalId(source, P.not(P.neq(UUID1)), vertices[0]);
            assertLocalId(source, P.not(P.eq(UUID1)), vertices[1]);
            assertLocalId(source, P.not(P.neq(MISSING)));
            assertLocalId(source, P.not(P.neq(vertices[0].id())), vertices[0]);
        }
    }

    @Test
    public void testNegatedUuidKeepsWholeHasStep() {
        Vertex[] vertices = this.initUuidGraph();
        for (GraphTraversalSource source : this.sources()) {
            GraphTraversal<Vertex, Vertex> traversal = source.V()
                    .hasId(P.not(P.neq(UUID1))).has("name", "first");
            HasContainer id = localIdContainer(traversal);
            Assert.assertInstanceOf(NotP.class, id.getPredicate());
            for (Step<?, ?> step : traversal.asAdmin().getSteps()) {
                if (step instanceof HasStep) {
                    List<HasContainer> containers = ((HasStep<?>) step).getHasContainers();
                    Assert.assertEquals(2, containers.size());
                    Assert.assertEquals(T.id.getAccessor(), containers.get(0).getKey());
                    Assert.assertEquals("name", containers.get(1).getKey());
                }
            }
            Assert.assertEquals(Collections.singletonList(vertices[0]), traversal.toList());
        }
    }

    private void assertUuidEqRetainedByProperty(P<?> property) {
        Vertex[] vertices = this.initUuidGraph();
        P<UUID> predicate = P.eq(UUID1);
        for (GraphTraversalSource source : this.sources()) {
            GraphTraversal<Vertex, Vertex> traversal = source.V()
                    .hasId(predicate).has("name", property);
            HasContainer id = localIdContainer(traversal);
            Assert.assertEquals(Collections.singletonList(vertices[0]), traversal.toList());
            Assert.assertNotSame(predicate, id.getPredicate());
            Assert.assertEquals(UUID1, predicate.getValue());
        }
    }

    @Test
    public void testUuidEqRetainedByNegatedProperty() {
        this.assertUuidEqRetainedByProperty(P.not(P.neq("first")));
    }

    @Test
    public void testUuidEqRetainedByTextProperty() {
        this.assertUuidEqRetainedByProperty(TextP.containing("ir"));
    }

    @Test
    public void testUuidEqRetainedByTypePredicate() {
        this.assertUuidEqRetainedByProperty(P.typeOf(GType.STRING));
    }

    @Test
    public void testNegatedUuidCollection() {
        Vertex[] vertices = this.initUuidGraph();
        List<Object> operands = Arrays.asList(UUID1, vertices[1].id());
        P<Object> predicate = P.not(P.without(operands));
        for (GraphTraversalSource source : this.sources()) {
            assertLocalId(source, predicate, vertices);
            assertLocalId(source, P.not(P.within(UUID1)), vertices[1]);
            assertLocalId(source, P.not(P.without(MISSING)));
        }
        Assert.assertEquals(operands, ((NotP<?>) predicate).negate().getValue());
        Assert.assertInstanceOf(UUID.class, operands.get(0));
    }

    @Test
    public void testNestedUuidPredicatesPreserveOriginal() {
        Vertex[] vertices = this.initUuidGraph();
        P<UUID> predicate = P.not(P.neq(UUID1)).and(P.not(P.eq(UUID2)));
        P<UUID> original = predicate.clone();
        for (GraphTraversalSource source : this.sources()) {
            assertLocalId(source, predicate, vertices[0]);
            assertLocalId(source, P.not(P.neq(MISSING)).or(P.not(P.neq(UUID2))), vertices[1]);
            assertLocalId(source, new NotP<>(P.neq(UUID1).and(P.neq(UUID2))), vertices);
            assertLocalId(source, new NotP<>(new NotP<>(P.eq(UUID1))), vertices[0]);
        }
        Assert.assertEquals(original, predicate);
        Assert.assertTrue(predicate.test(UUID1));
        Assert.assertFalse(predicate.test(UUID2));
    }

    @Test
    public void testNegatedUuidPredicateClone() {
        Vertex[] vertices = this.initUuidGraph();
        P<UUID> predicate = P.not(P.neq(UUID1));
        GraphTraversal<Vertex, Vertex> traversal = graph().traversal().V().hasId(predicate);
        HasContainer has = localIdContainer(traversal);
        Assert.assertInstanceOf(NotP.class, has.getPredicate());
        Assert.assertNotSame(predicate, has.getPredicate());
        Assert.assertEquals(UUID1, predicate.getValue());
        GraphTraversal.Admin<Vertex, Vertex> clone = traversal.asAdmin().clone();
        Assert.assertEquals(Collections.singletonList(vertices[0]), traversal.toList());
        Assert.assertEquals(Collections.singletonList(vertices[0]), clone.toList());
        Assert.assertEquals(Collections.singletonList(vertices[0]),
                            graph().traversal().V().hasId(predicate).toList());
        Assert.assertEquals(UUID1, predicate.getValue());
    }

    @Test
    public void testNegatedNumericIdPreservesComparison() {
        graph().schema().vertexLabel("number").useCustomizeNumberId().create();
        Vertex first = graph().addVertex(T.label, "number", T.id, 42L);
        Vertex second = graph().addVertex(T.label, "number", T.id, 43L);
        commitTx();
        for (GraphTraversalSource source : this.sources()) {
            assertLocalId(source, P.not(P.neq(42)), first);
            assertLocalId(source, P.not(P.neq(42L)), first);
            assertLocalId(source, P.not(P.neq(42.0)), first);
            assertLocalId(source, P.not(P.neq(42.5)));
            assertLocalId(source, P.not(P.eq(42.5)), first, second);
            assertLocalId(source, P.not(P.without(42, 43L)), first, second);
        }
    }

    @Test
    public void testNegatedStringIdPreservesStringMode() {
        graph().schema().vertexLabel("string").useCustomizeStringId().create();
        Vertex first = graph().addVertex(T.label, "string", T.id, "first");
        Vertex second = graph().addVertex(T.label, "string", T.id, "second");
        commitTx();
        for (GraphTraversalSource source : this.sources()) {
            assertLocalId(source, P.not(P.neq("first")), first);
            assertLocalId(source, P.not(P.eq("first")), second);
            assertLocalId(source, P.not(P.without("first", "second")), first, second);
            assertLocalId(source, P.not(P.within("missing")), first, second);
            assertLocalId(source, new NotP<>(new NotP<>(P.eq("first"))), first);
        }
    }

    @Test
    public void testHeterogeneousIdCollections() {
        graph().schema().propertyKey("name").asText().create();
        graph().schema().vertexLabel("number").useCustomizeNumberId().properties("name").create();
        graph().schema().vertexLabel("string").useCustomizeStringId().properties("name").create();
        graph().schema().vertexLabel("uuid").useCustomizeUuidId().properties("name").create();
        Vertex number = graph().addVertex(T.label, "number", T.id, 42L, "name", "member");
        Vertex string = graph().addVertex(T.label, "string", T.id, "alpha", "name", "member");
        Vertex uuid = graph().addVertex(T.label, "uuid", T.id, UUID1, "name", "member");
        commitTx();
        for (GraphTraversalSource source : this.sources()) {
            this.assertHeterogeneousIdCollection(source, Arrays.asList(42, "alpha"), uuid, number, string);
            this.assertHeterogeneousIdCollection(source, Arrays.asList(UUID1, "alpha"), number, uuid, string);
            this.assertHeterogeneousIdCollection(source, Arrays.asList(number.id(), "alpha"), uuid, number, string);
            // Pure String collections retain HasContainer's string-ID mode.
            assertLocalId(source, P.not(P.without("42", "alpha")), number, string);
            assertLocalId(source, P.not(P.without(42.5, "alpha")), string);
            assertLocalId(source, P.not(P.without("42", uuid.id())), uuid);
        }
    }

    @Test
    public void testHeterogeneousNamedIdCollectionPreservesBindings() {
        graph().schema().vertexLabel("number").useCustomizeNumberId().create();
        graph().schema().vertexLabel("string").useCustomizeStringId().create();
        Vertex number = graph().addVertex(T.label, "number", T.id, 42L);
        Vertex string = graph().addVertex(T.label, "string", T.id, "alpha");
        commitTx();
        for (GraphTraversalSource source : this.sources()) {
            P<Object> leaf = P.without(Arrays.asList(GValue.of("targetId", 42L), "alpha"));
            Collection<?> bindings = new HashSet<>(leaf.getGValues());
            P<Object> predicate = P.not(leaf);
            P<Object> clone = predicate.clone();
            GraphTraversal<Vertex, Vertex> traversal = source.V().hasId(predicate);
            GraphTraversal.Admin<Vertex, Vertex> traversalClone = traversal.asAdmin().clone();
            Assert.assertEquals(new HashSet<>(Arrays.asList(number, string)), new HashSet<>(traversal.toList()));
            Assert.assertEquals(new HashSet<>(Arrays.asList(number, string)),
                                new HashSet<>(traversalClone.toList()));
            assertLocalId(source, predicate, number, string);
            assertLocalId(source, clone, number, string);
            Assert.assertEquals(new HashSet<>(Arrays.asList(42L, "alpha")),
                                new HashSet<>((Collection<?>) leaf.getValue()));
            Assert.assertTrue(leaf.isParameterized());
            Assert.assertTrue(((NotP<?>) clone).negate().isParameterized());
            Assert.assertEquals(bindings, new HashSet<>(((NotP<?>) clone).negate().getGValues()));
            Assert.assertEquals(bindings, new HashSet<>(leaf.getGValues()));
            leaf.updateVariable("targetId", 43L);
            assertLocalId(source, predicate, string);
            Assert.assertEquals(new HashSet<>(Arrays.asList(43L, "alpha")),
                                new HashSet<>((Collection<?>) leaf.getValue()));
        }
    }

    @Test
    public void testMixedEdgeIdCollections() {
        graph().schema().vertexLabel("edge-id").useCustomizeNumberId().create();
        graph().schema().edgeLabel("mixed-id-edge").sourceLabel("edge-id").targetLabel("edge-id").create();
        Vertex first = graph().addVertex(T.label, "edge-id", T.id, 101L);
        Vertex second = graph().addVertex(T.label, "edge-id", T.id, 102L);
        Vertex third = graph().addVertex(T.label, "edge-id", T.id, 103L);
        Edge one = first.addEdge("mixed-id-edge", second);
        Edge two = first.addEdge("mixed-id-edge", third);
        Edge excluded = second.addEdge("mixed-id-edge", third);
        commitTx();
        List<List<Object>> collections = Arrays.asList(Arrays.asList(one.id().toString(), two.id()),
                                                       Arrays.asList(one.id(), two.id().toString()),
                                                       Arrays.asList(one.id(), two.id()),
                                                       Arrays.asList(one.id().toString(), two.id().toString()));
        for (GraphTraversalSource source : this.sources()) {
            for (List<Object> ids : collections) {
                Assert.assertEquals(new HashSet<>(Arrays.asList(one.id(), two.id())),
                                    new HashSet<>(graph().traversal().E().hasId(P.within(ids)).id().toList()));
                GraphTraversal<Edge, Edge> local = source.E().hasId(P.not(P.without(ids)));
                localIdContainer(local);
                Assert.assertEquals(new HashSet<>(Arrays.asList(one, two)), new HashSet<>(local.toList()));
                Assert.assertEquals(2L, source.E().hasId(P.not(P.without(ids))).count().next().longValue());
                Assert.assertEquals(Collections.singletonList(excluded),
                                    source.E().hasId(P.not(P.within(ids))).toList());
                Assert.assertEquals(1L, source.E().hasId(P.not(P.within(ids))).count().next().longValue());
            }
            Assert.assertEquals(Collections.singletonList(one), source.E()
                    .hasId(P.not(P.without(Arrays.asList("invalid", one.id())))).toList());
        }
    }

    private void assertHeterogeneousIdCollection(GraphTraversalSource source, List<Object> operands,
                                                 Vertex excluded, Vertex... expected) {
        P<Object> within = P.within(operands);
        P<Object> original = within.clone();
        GraphTraversal<Vertex, Vertex> pushed = graph().traversal().V().hasId(within);
        pushed.asAdmin().applyStrategies();
        Assert.assertEquals(operands.size(),
                            ((GraphStep<?, ?>) pushed.asAdmin().getStartStep()).getIds().length);
        Assert.assertEquals(new HashSet<>(Arrays.asList(expected)), new HashSet<>(pushed.toList()));
        assertLocalId(source, P.not(P.without(operands)), expected);
        assertLocalId(source, P.not(within), excluded);
        Assert.assertEquals((long) expected.length,
                            source.V().hasId(P.not(P.without(operands))).count().next().longValue());
        Assert.assertEquals(1L, source.V().hasId(P.not(within)).count().next().longValue());
        for (P<Object> predicate : Arrays.asList(within, P.without(operands))) {
            Vertex[] result = predicate == within ? expected : new Vertex[] {excluded};
            GraphTraversal<Vertex, Vertex> traversal = source.V().hasId(predicate)
                    .has("name", P.typeOf(GType.STRING));
            localIdContainer(traversal);
            List<Vertex> actual = traversal.toList();
            Assert.assertEquals(result.length, actual.size());
            Assert.assertEquals(new HashSet<>(Arrays.asList(result)), new HashSet<>(actual));
            Assert.assertEquals((long) result.length, source.V().hasId(predicate)
                    .has("name", P.typeOf(GType.STRING)).count().next().longValue());
        }
        Assert.assertEquals(original, within);
        Assert.assertEquals(operands, within.getValue());
    }

    @Test
    public void testIdTypeOperandRemainsSemanticMarker() {
        Vertex[] vertices = this.initUuidGraph();
        P<Object> predicate = P.not(P.typeOf(GType.STRING));
        for (GraphTraversalSource source : this.sources()) {
            GraphTraversal<Vertex, Vertex> traversal = source.V().hasId(predicate);
            HasContainer has = localIdContainer(traversal);
            Assert.assertEquals(GType.STRING, has.getPredicate().getValue());
            Assert.assertEquals(new HashSet<>(Arrays.asList(vertices)),
                                new HashSet<>(traversal.toList()));
        }
        Assert.assertEquals(GType.STRING, predicate.getValue());
    }

    @Test
    public void testCombinedStringIdPredicates() {
        graph().schema().vertexLabel("string").useCustomizeStringId().create();
        Vertex first = graph().addVertex(T.label, "string", T.id, "first");
        Vertex second = graph().addVertex(T.label, "string", T.id, "second");
        Vertex third = graph().addVertex(T.label, "string", T.id, "third");
        commitTx();
        for (GraphTraversalSource source : this.sources()) {
            P<String> firstNot = P.not(P.eq("first"));
            P<String> secondNot = P.not(P.eq("second"));
            P<String> conjunction = firstNot.and(secondNot);
            Assert.assertEquals(Collections.singletonList(third), source.V()
                    .hasId(firstNot).hasId(secondNot).toList());
            assertLocalId(source, conjunction, third);
            Assert.assertEquals(1L, source.V().hasId(conjunction).count().next().longValue());
            P<String> disjunction = P.not(P.neq("first")).or(P.not(P.neq("second")));
            assertLocalId(source, disjunction, first, second);
            Assert.assertEquals(2L, source.V().hasId(disjunction).count().next().longValue());
            Assert.assertEquals(Collections.singletonList(third),
                                source.V().hasId(new NotP<>(disjunction)).toList());
            Assert.assertEquals(1L, source.V().hasId(new NotP<>(disjunction))
                                        .count().next().longValue());
            assertLocalId(source, P.not(P.gt("second")), first, second);
            Assert.assertTrue(conjunction.test("third"));
            Assert.assertFalse(conjunction.test("first"));
        }
    }

    @Test
    public void testNamedUuidIdPredicateStateIsolation() {
        Vertex[] vertices = this.initUuidGraph();
        for (GraphTraversalSource source : this.sources()) {
            P<UUID> predicate = P.eq(GValue.of("id", UUID1));
            P<UUID> clone = predicate.clone();
            GraphTraversal<Vertex, Vertex> traversal = source.V().hasId(predicate)
                    .has("name", P.typeOf(GType.STRING));
            GraphTraversal.Admin<Vertex, Vertex> traversalClone = traversal.asAdmin().clone();
            Assert.assertEquals(Collections.singletonList(vertices[0]), traversal.toList());
            Assert.assertEquals(Collections.singletonList(vertices[0]), traversalClone.toList());
            for (int i = 0; i < 3; i++) {
                Assert.assertEquals(Collections.singletonList(vertices[0]), source.V()
                        .hasId(predicate).has("name", P.typeOf(GType.STRING)).toList());
                Assert.assertEquals(Collections.singletonList(vertices[0]), source.V()
                        .hasId(clone).has("name", P.typeOf(GType.STRING)).toList());
                Assert.assertEquals(UUID1, predicate.getValue());
                Assert.assertEquals(UUID1, clone.getValue());
                Assert.assertTrue(predicate.isParameterized());
            }
            predicate.updateVariable("id", UUID2);
            Assert.assertEquals(Collections.singletonList(vertices[1]), source.V()
                    .hasId(predicate).has("name", P.typeOf(GType.STRING)).toList());
            Assert.assertEquals(UUID2, predicate.getValue());
        }
    }

    @Test
    public void testNamedPropertyPredicateStateIsolation() {
        graph().schema().propertyKey("name").asText().create();
        graph().schema().vertexLabel("uuid").useCustomizeUuidId().properties("name").create();
        Vertex[] vertices = {
                graph().addVertex(T.label, "uuid", T.id, UUID1, "name", "first"),
                graph().addVertex(T.label, "uuid", T.id, UUID2, "name", "second")
        };
        commitTx();
        for (GraphTraversalSource source : this.sources()) {
            P<String> predicate = P.eq(GValue.of("name", "first"));
            P<String> clone = predicate.clone();
            GraphTraversal<Vertex, Vertex> traversal = source.V(vertices[0].id(), vertices[1].id())
                    .has("name", predicate);
            GraphTraversal.Admin<Vertex, Vertex> traversalClone = traversal.asAdmin().clone();
            Assert.assertEquals(Collections.singletonList(vertices[0]), traversal.toList());
            Assert.assertEquals(Collections.singletonList(vertices[0]), traversalClone.toList());
            for (int i = 0; i < 3; i++) {
                Assert.assertEquals(Collections.singletonList(vertices[0]),
                                    source.V(vertices[0].id(), vertices[1].id()).has("name", predicate).toList());
                Assert.assertEquals(Collections.singletonList(vertices[0]),
                                    source.V(vertices[0].id(), vertices[1].id()).has("name", clone).toList());
                Assert.assertEquals("first", predicate.getValue());
                Assert.assertEquals("first", clone.getValue());
                Assert.assertTrue(predicate.isParameterized());
            }
            predicate.updateVariable("name", "second");
            Assert.assertEquals(Collections.singletonList(vertices[1]),
                                source.V(vertices[0].id(), vertices[1].id()).has("name", predicate).toList());
            Assert.assertEquals("second", predicate.getValue());
        }
    }

    @Test
    public void testNamedIdCollectionPredicateStateIsolation() {
        graph().schema().vertexLabel("number").useCustomizeNumberId().create();
        Vertex first = graph().addVertex(T.label, "number", T.id, 42L);
        Vertex second = graph().addVertex(T.label, "number", T.id, 43L);
        Vertex third = graph().addVertex(T.label, "number", T.id, 44L);
        commitTx();
        for (GraphTraversalSource source : this.sources()) {
            P<Long> leaf = P.within(GValue.of("targetId", 42L));
            Collection<?> bindings = new HashSet<>(leaf.getGValues());
            P<Long> predicate = P.not(leaf);
            P<Long> clone = predicate.clone();
            GraphTraversal<Vertex, Vertex> traversal = source.V().hasId(predicate);
            GraphTraversal.Admin<Vertex, Vertex> traversalClone = traversal.asAdmin().clone();
            Assert.assertEquals(new HashSet<>(Arrays.asList(second, third)),
                                new HashSet<>(traversal.toList()));
            Assert.assertEquals(new HashSet<>(Arrays.asList(second, third)),
                                new HashSet<>(traversalClone.toList()));
            assertLocalId(source, predicate, second, third);
            assertLocalId(source, clone, second, third);
            Assert.assertEquals(Collections.singletonList(42L), leaf.getValue());
            Assert.assertEquals(Collections.singletonList(42L), clone.getValue());
            Assert.assertTrue(leaf.isParameterized());
            Assert.assertEquals(bindings, new HashSet<>(leaf.getGValues()));
            leaf.updateVariable("targetId", 43L);
            assertLocalId(source, predicate, first, third);
            Assert.assertEquals(Collections.singletonList(43L), leaf.getValue());
        }
    }

    @Test
    public void testMixedNamedPropertyCollectionPredicateStateIsolation() {
        graph().schema().propertyKey("age").asInt().create();
        graph().schema().vertexLabel("number").useCustomizeNumberId().properties("age").create();
        Vertex first = graph().addVertex(T.label, "number", T.id, 42L, "age", 20);
        Vertex second = graph().addVertex(T.label, "number", T.id, 43L, "age", 30);
        Vertex third = graph().addVertex(T.label, "number", T.id, 44L, "age", 40);
        commitTx();
        for (GraphTraversalSource source : this.sources()) {
            P<Object> predicate = P.within(Arrays.asList(GValue.of("age", 20), 40));
            Collection<?> bindings = new HashSet<>(predicate.getGValues());
            Collection<?> values = new HashSet<>((Collection<?>) predicate.getValue());
            P<Object> clone = predicate.clone();
            GraphTraversal<Vertex, Vertex> traversal = source.V(first.id(), second.id(), third.id())
                    .has("age", predicate);
            GraphTraversal.Admin<Vertex, Vertex> traversalClone = traversal.asAdmin().clone();
            Assert.assertEquals(new HashSet<>(Arrays.asList(first, third)),
                                new HashSet<>(traversal.toList()));
            Assert.assertEquals(new HashSet<>(Arrays.asList(first, third)),
                                new HashSet<>(traversalClone.toList()));
            Assert.assertEquals(new HashSet<>(Arrays.asList(first, third)), new HashSet<>(source
                    .V(first.id(), second.id(), third.id()).has("age", predicate).toList()));
            Assert.assertEquals(new HashSet<>(Arrays.asList(first, third)), new HashSet<>(source
                    .V(first.id(), second.id(), third.id()).has("age", clone).toList()));
            Assert.assertEquals(values, new HashSet<>((Collection<?>) predicate.getValue()));
            Assert.assertEquals(values, new HashSet<>((Collection<?>) clone.getValue()));
            Assert.assertTrue(predicate.isParameterized());
            Assert.assertTrue(clone.isParameterized());
            Assert.assertEquals(bindings, new HashSet<>(predicate.getGValues()));
            Assert.assertEquals(bindings, new HashSet<>(clone.getGValues()));
            predicate.updateVariable("age", 30);
            Assert.assertEquals(new HashSet<>(Arrays.asList(second, third)), new HashSet<>(source
                    .V(first.id(), second.id(), third.id()).has("age", predicate).toList()));
        }
    }

}
