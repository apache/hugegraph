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

package org.apache.hugegraph.traversal.optimize;

import java.util.Collections;
import java.util.Set;

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.id.IdGenerator;
import org.apache.hugegraph.exception.NotFoundException;
import org.apache.hugegraph.schema.PropertyKey;
import org.apache.hugegraph.schema.VertexLabel;
import org.apache.hugegraph.structure.HugeVertex;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.type.define.DataType;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.AndStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.OrStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.InlineFilterStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.junit.Test;
import org.mockito.Mockito;

public class TraversalUtilOptimizeTest {

    @Test
    public void testCanExtractHasContainerWithoutGraph() {
        Assert.assertTrue(TraversalUtil.canExtractHasContainer(
                null, new HasContainer("~label", P.eq("person"))));
        Assert.assertTrue(TraversalUtil.canExtractHasContainer(
                null, new HasContainer("~id", P.eq("1"))));
        Assert.assertFalse(TraversalUtil.canExtractHasContainer(
                null, new HasContainer("name", P.eq("marko"))));
    }

    @Test
    public void testCanExtractHasContainerWithMissingPropertyKey() {
        HugeGraph graph = Mockito.mock(HugeGraph.class);
        Mockito.when(graph.propertyKey("missing"))
               .thenThrow(new NotFoundException("missing"));

        Assert.assertFalse(TraversalUtil.canExtractHasContainer(
                graph, new HasContainer("missing", P.eq("marko"))));
    }

    @Test
    public void testIndexLabelOrNullWithMissingIndexLabel() {
        HugeGraph graph = Mockito.mock(HugeGraph.class);
        Id id = IdGenerator.of(1L);
        Mockito.when(graph.indexLabel(id))
               .thenThrow(new IllegalArgumentException("missing"));

        Assert.assertNull(TraversalUtil.indexLabelOrNull(graph, id));
    }

    @Test
    public void testCanExtractHasContainerWithNonTextProperty() {
        HugeGraph graph = Mockito.mock(HugeGraph.class);
        PropertyKey age = propertyKey(1L, "age", DataType.INT);
        Mockito.when(graph.propertyKey("age")).thenReturn(age);

        Assert.assertTrue(TraversalUtil.canExtractHasContainer(
                graph, new HasContainer("age", P.eq(1))));
    }

    @Test
    public void testCanExtractHasContainerWithTextRangePredicate() {
        HugeGraph graph = Mockito.mock(HugeGraph.class);
        PropertyKey name = propertyKey(1L, "name", DataType.TEXT);
        Mockito.when(graph.propertyKey("name")).thenReturn(name);

        Assert.assertFalse(TraversalUtil.canExtractHasContainer(
                graph, new HasContainer("name", P.lt(""))));
        Assert.assertFalse(TraversalUtil.canExtractHasContainer(
                graph, new HasContainer("name", P.gte("marko"))));
        Assert.assertFalse(TraversalUtil.canExtractHasContainer(
                graph, new HasContainer("name", P.between("josh", "marko"))));
        Assert.assertTrue(TraversalUtil.canExtractHasContainer(
                graph, new HasContainer("name", P.eq("marko"))));
    }

    @Test
    public void testExtractHasContainerKeepsTextRangeGraphHasStep() {
        HugeGraph graph = Mockito.mock(HugeGraph.class);
        PropertyKey name = propertyKey(1L, "name", DataType.TEXT);
        Mockito.when(graph.propertyKey("name")).thenReturn(name);

        Traversal.Admin<?, ?> traversal = traversal(__.V()
                                                     .has("name", P.lt("marko")),
                                                   graph);
        HugeGraphStep<?, ?> newStep = replaceGraphStep(traversal);

        TraversalUtil.extractHasContainer(newStep, traversal);

        Assert.assertTrue(newStep.getHasContainers().isEmpty());
        Assert.assertTrue(hasStepExists(traversal));
    }

    @Test
    public void testExtractHasContainerKeepsTextRangeWithoutGraph() {
        Traversal.Admin<?, ?> traversal = __.V()
                                           .has("name", P.lt("marko"))
                                           .asAdmin();
        HugeGraphStep<?, ?> newStep = replaceGraphStep(traversal);

        TraversalUtil.extractHasContainer(newStep, traversal);

        Assert.assertTrue(newStep.getHasContainers().isEmpty());
        Assert.assertTrue(hasStepExists(traversal));
    }

    @Test
    public void testExtractHasContainerKeepsMatchRangeWithoutGraph() {
        Traversal.Admin<?, ?> traversal = __.V()
                                           .has("age", P.gt(18))
                                           .match(__.as("v").identity().as("m"))
                                           .asAdmin();
        HugeGraphStep<?, ?> newStep = replaceGraphStep(traversal);

        TraversalUtil.extractHasContainer(newStep, traversal);

        Assert.assertTrue(newStep.getHasContainers().isEmpty());
        Assert.assertTrue(hasStepExists(traversal));
    }

    @Test
    public void testExtractHasContainerExtractsPositiveLabelOnlyOrStep() {
        Traversal.Admin<?, ?> traversal = __.V()
                                           .has("age", P.gt(18))
                                           .or(__.hasLabel("person"),
                                               __.hasLabel(P.within("software")))
                                           .asAdmin();
        HugeGraphStep<?, ?> newStep = replaceGraphStep(traversal);

        TraversalUtil.extractHasContainer(newStep, traversal);

        Assert.assertTrue(hasContainer(newStep, T.label.getAccessor()));
        Assert.assertTrue(hasStepExists(traversal, "age"));
        Assert.assertFalse(stepExists(traversal, OrStep.class));
    }

    @Test
    public void testExtractHasContainerKeepsUnsafeLabelAfterPositiveLabelOr() {
        HugeGraph graph = Mockito.mock(HugeGraph.class);
        PropertyKey age = propertyKey(1L, "age", DataType.INT);
        PropertyKey city = propertyKey(2L, "city", DataType.TEXT);
        Mockito.when(graph.propertyKey("age")).thenReturn(age);
        Mockito.when(graph.propertyKey("city")).thenReturn(city);
        Mockito.when(graph.vertexLabels()).thenReturn(Collections.emptyList());

        Traversal.Admin<?, ?> traversal = traversal(
                __.V().has("age", P.gt(1))
                  .or(__.hasLabel("person"), __.hasLabel("software"))
                  .has(T.label, P.neq("author"))
                  .has("city", "Beijing"), graph);
        HugeGraphStep<?, ?> newStep = replaceGraphStep(traversal);

        TraversalUtil.extractHasContainer(newStep, traversal);

        Assert.assertTrue(newStep.getHasContainers().isEmpty());
        Assert.assertTrue(hasStepExists(traversal, "age"));
        Assert.assertTrue(hasStepExists(traversal, T.label.getAccessor()));
        Assert.assertTrue(hasStepExists(traversal, "city"));
        Assert.assertTrue(stepExists(traversal, OrStep.class));
    }

    @Test
    public void testExtractHasContainerKeepsMixedKeyOrUnsafeLabelLocal() {
        HugeGraph graph = Mockito.mock(HugeGraph.class);
        PropertyKey city = propertyKey(2L, "city", DataType.TEXT);
        PropertyKey status = propertyKey(3L, "status", DataType.TEXT);
        Mockito.when(graph.propertyKey("city")).thenReturn(city);
        Mockito.when(graph.propertyKey("status")).thenReturn(status);
        Mockito.when(graph.vertexLabels()).thenReturn(Collections.emptyList());

        Traversal.Admin<?, ?> traversal = traversal(
                __.V().has("city", "Beijing")
                  .or(__.has(T.label, P.neq("author")),
                      __.has("status", "active")), graph);
        HugeGraphStep<?, ?> newStep = replaceGraphStep(traversal);

        TraversalUtil.extractHasContainer(newStep, traversal);

        Assert.assertTrue(newStep.getHasContainers().isEmpty());
        Assert.assertTrue(hasStepExists(traversal, "city"));
        Assert.assertTrue(stepExists(traversal, OrStep.class));
    }

    @Test
    public void testExtractHasContainerKeepsGlobalChildUnsafeLabelLocal() {
        HugeGraph graph = Mockito.mock(HugeGraph.class);
        PropertyKey city = propertyKey(2L, "city", DataType.TEXT);
        Mockito.when(graph.propertyKey("city")).thenReturn(city);

        Traversal.Admin<?, ?> traversal = traversal(
                __.V().has("city", "Beijing")
                  .union(__.has(T.label, P.neq("author")),
                         __.identity()), graph);
        HugeGraphStep<?, ?> newStep = replaceGraphStep(traversal);

        TraversalUtil.extractHasContainer(newStep, traversal);

        Assert.assertTrue(newStep.getHasContainers().isEmpty());
        Assert.assertTrue(hasStepExists(traversal, "city"));
    }

    @Test
    public void testExtractHasContainerKeepsUnsupportedOrLabelLocal() {
        Traversal.Admin<?, ?> traversal = __.V()
                                           .has("age", P.gt(18))
                                           .or(__.hasLabel(P.neq("person")),
                                               __.hasLabel("software"))
                                           .asAdmin();
        HugeGraphStep<?, ?> newStep = replaceGraphStep(traversal);

        TraversalUtil.extractHasContainer(newStep, traversal);

        Assert.assertFalse(hasContainer(newStep, T.label.getAccessor()));
        Assert.assertTrue(hasStepExists(traversal, "age"));
        Assert.assertTrue(stepExists(traversal, OrStep.class));
    }

    @Test
    public void testExtractHasContainerKeepsNonLabelOrLocal() {
        Traversal.Admin<?, ?> traversal = __.V()
                                           .has("age", P.gt(18))
                                           .or(__.has("name", "marko"),
                                               __.hasLabel("software"))
                                           .asAdmin();
        HugeGraphStep<?, ?> newStep = replaceGraphStep(traversal);

        TraversalUtil.extractHasContainer(newStep, traversal);

        Assert.assertFalse(hasContainer(newStep, T.label.getAccessor()));
        Assert.assertTrue(hasStepExists(traversal, "age"));
        Assert.assertTrue(stepExists(traversal, OrStep.class));
    }

    @Test
    public void testExtractHasContainerRequiresSingleLabelOnlyOrChild() {
        Traversal.Admin<?, ?> traversal = __.V()
                                           .has("age", P.gt(18))
                                           .or(__.hasLabel("person")
                                               .has("name", "marko"),
                                               __.hasLabel("software"))
                                           .asAdmin();
        HugeGraphStep<?, ?> newStep = replaceGraphStep(traversal);

        TraversalUtil.extractHasContainer(newStep, traversal);

        Assert.assertFalse(hasContainer(newStep, T.label.getAccessor()));
        Assert.assertTrue(hasStepExists(traversal, "age"));
        Assert.assertTrue(stepExists(traversal, OrStep.class));
    }

    @Test
    public void testExtractHasContainerSkipsOrWhenPredicateIsNotSensitive() {
        Traversal.Admin<?, ?> traversal = __.V()
                                           .has("name", "marko")
                                           .or(__.hasLabel("person"),
                                               __.hasLabel("software"))
                                           .asAdmin();
        HugeGraphStep<?, ?> newStep = replaceGraphStep(traversal);

        TraversalUtil.extractHasContainer(newStep, traversal);

        Assert.assertFalse(hasContainer(newStep, T.label.getAccessor()));
        Assert.assertTrue(stepExists(traversal, OrStep.class));
    }

    @Test
    public void testExtractHasContainerKeepsTextBetweenGraphHasStep() {
        HugeGraph graph = Mockito.mock(HugeGraph.class);
        PropertyKey name = propertyKey(1L, "name", DataType.TEXT);
        Mockito.when(graph.propertyKey("name")).thenReturn(name);

        Traversal.Admin<?, ?> traversal = traversal(__.V()
                                                     .has("name", P.between(
                                                             "josh", "marko")),
                                                   graph);
        HugeGraphStep<?, ?> newStep = replaceGraphStep(traversal);

        TraversalUtil.extractHasContainer(newStep, traversal);

        Assert.assertTrue(newStep.getHasContainers().isEmpty());
        Assert.assertTrue(hasStepExists(traversal));
    }

    @Test
    public void testExtractHasContainerRemovesSafeGraphHasStep() {
        HugeGraph graph = Mockito.mock(HugeGraph.class);
        PropertyKey age = propertyKey(1L, "age", DataType.INT);
        Mockito.when(graph.propertyKey("age")).thenReturn(age);

        Traversal.Admin<?, ?> traversal = traversal(__.V().has("age", 18),
                                                   graph);
        HugeGraphStep<?, ?> newStep = replaceGraphStep(traversal);

        TraversalUtil.extractHasContainer(newStep, traversal);

        Assert.assertEquals(1, newStep.getHasContainers().size());
        Assert.assertFalse(hasStepExists(traversal));
    }

    @Test
    public void testExtractHasContainerKeepsGraphChainWithUnsafeLabel() {
        HugeGraph graph = Mockito.mock(HugeGraph.class);
        PropertyKey age = propertyKey(1L, "age", DataType.INT);
        Mockito.when(graph.propertyKey("age")).thenReturn(age);

        Traversal.Admin<?, ?> traversal = traversal(
                __.V().has(T.label, P.neq("author")).barrier()
                  .has("age", 18), graph);
        HugeGraphStep<?, ?> newStep = replaceGraphStep(traversal);

        TraversalUtil.extractHasContainer(newStep, traversal);

        Assert.assertTrue(newStep.getHasContainers().isEmpty());
        Assert.assertEquals(2, countHasSteps(traversal));

        traversal = traversal(
                __.V().has("age", 18).barrier()
                  .has(T.label, P.neq("author")), graph);
        newStep = replaceGraphStep(traversal);

        TraversalUtil.extractHasContainer(newStep, traversal);

        Assert.assertTrue(newStep.getHasContainers().isEmpty());
        Assert.assertEquals(2, countHasSteps(traversal));
    }

    @Test
    public void testExtractHasContainerKeepsTextRangeVertexHasStep() {
        HugeGraph graph = Mockito.mock(HugeGraph.class);
        PropertyKey name = propertyKey(1L, "name", DataType.TEXT);
        Mockito.when(graph.propertyKey("name")).thenReturn(name);

        Traversal.Admin<?, ?> traversal = traversal(__.V().out()
                                                     .has("name", P.lt("marko")),
                                                   graph);
        HugeVertexStep<?> newStep = replaceVertexStep(traversal);

        TraversalUtil.extractHasContainer(newStep, traversal);

        Assert.assertTrue(newStep.getHasContainers().isEmpty());
        Assert.assertTrue(hasStepExists(traversal));
    }

    @Test
    public void testExtractHasContainerRemovesSafeVertexHasStep() {
        HugeGraph graph = Mockito.mock(HugeGraph.class);
        PropertyKey age = propertyKey(1L, "age", DataType.INT);
        Mockito.when(graph.propertyKey("age")).thenReturn(age);

        Traversal.Admin<?, ?> traversal = traversal(__.V().out().has("age", 18),
                                                   graph);
        HugeVertexStep<?> newStep = replaceVertexStep(traversal);

        TraversalUtil.extractHasContainer(newStep, traversal);

        Assert.assertEquals(1, newStep.getHasContainers().size());
        Assert.assertFalse(hasStepExists(traversal));
    }

    @Test
    public void testExtractHasContainerKeepsVertexChainWithUnsafeLabel() {
        HugeGraph graph = Mockito.mock(HugeGraph.class);
        PropertyKey age = propertyKey(1L, "age", DataType.INT);
        Mockito.when(graph.propertyKey("age")).thenReturn(age);

        Traversal.Admin<?, ?> traversal = traversal(
                __.V().out().has(T.label, P.neq("author")).barrier()
                  .has("age", 18), graph);
        HugeVertexStep<?> newStep = replaceVertexStep(traversal);

        TraversalUtil.extractHasContainer(newStep, traversal);

        Assert.assertTrue(newStep.getHasContainers().isEmpty());
        Assert.assertEquals(2, countHasSteps(traversal));

        traversal = traversal(
                __.V().out().has("age", 18).barrier()
                  .has(T.label, P.neq("author")), graph);
        newStep = replaceVertexStep(traversal);

        TraversalUtil.extractHasContainer(newStep, traversal);

        Assert.assertTrue(newStep.getHasContainers().isEmpty());
        Assert.assertEquals(2, countHasSteps(traversal));
    }

    @Test
    public void testExtractHasContainerKeepsVertexMixedKeyOrUnsafeLabelLocal() {
        HugeGraph graph = Mockito.mock(HugeGraph.class);
        PropertyKey age = propertyKey(1L, "age", DataType.INT);
        PropertyKey city = propertyKey(2L, "city", DataType.TEXT);
        Mockito.when(graph.propertyKey("age")).thenReturn(age);
        Mockito.when(graph.propertyKey("city")).thenReturn(city);

        Traversal.Admin<?, ?> traversal = traversal(
                __.V().out().has("age", 18)
                  .or(__.has(T.label, P.neq("author")),
                      __.has("city", "Beijing")), graph);
        HugeVertexStep<?> newStep = replaceVertexStep(traversal);

        TraversalUtil.extractHasContainer(newStep, traversal);

        Assert.assertTrue(newStep.getHasContainers().isEmpty());
        Assert.assertTrue(hasStepExists(traversal, "age"));
        Assert.assertTrue(stepExists(traversal, OrStep.class));
    }

    @Test
    public void testExtractHasContainerKeepsTrailingUnsafeVertexLabelLocal() {
        HugeGraph graph = Mockito.mock(HugeGraph.class);
        PropertyKey age = propertyKey(1L, "age", DataType.INT);
        Mockito.when(graph.propertyKey("age")).thenReturn(age);

        Traversal.Admin<?, ?> traversal = traversal(
                __.V().out().has("age", P.gt(18))
                  .or(__.hasLabel("person"), __.hasLabel("software"))
                  .has(T.label, P.neq("author")), graph);
        HugeVertexStep<?> newStep = replaceVertexStep(traversal);

        TraversalUtil.extractHasContainer(newStep, traversal);

        Assert.assertTrue(newStep.getHasContainers().isEmpty());
        Assert.assertTrue(hasStepExists(traversal, "age"));
        Assert.assertTrue(stepExists(traversal, OrStep.class));
    }

    @Test
    public void testIsPositiveLabelContainer() {
        Assert.assertTrue(TraversalUtil.isPositiveLabelContainer(
                new HasContainer(T.label.getAccessor(), P.eq("person"))));
        Assert.assertTrue(TraversalUtil.isPositiveLabelContainer(
                new HasContainer(T.label.getAccessor(),
                                 P.within("person", "software"))));

        Assert.assertFalse(TraversalUtil.isPositiveLabelContainer(
                new HasContainer("name", P.eq("person"))));
        Assert.assertFalse(TraversalUtil.isPositiveLabelContainer(
                new HasContainer(T.label.getAccessor(), P.neq("person"))));
        Assert.assertFalse(TraversalUtil.isPositiveLabelContainer(
                new HasContainer(T.label.getAccessor(),
                                 P.without("person"))));
        Assert.assertFalse(TraversalUtil.isPositiveLabelContainer(
                new HasContainer(T.label.getAccessor(),
                                 P.within(Collections.emptyList()))));
    }

    @Test
    public void testPageIsConsumedBeforeUnsafeLabel() {
        HugeGraph graph = Mockito.mock(HugeGraph.class);
        Traversal.Admin<?, ?> traversal = traversal(
                __.V().has("~page", "").limit(2).has(T.label, P.neq("author")), graph);
        HugeGraphStep<?, ?> graphStep = replaceGraphStep(traversal);
        TraversalUtil.extractHasContainer(graphStep, traversal);
        Assert.assertTrue(graphStep.queryInfo().paging());
        Assert.assertFalse(hasStepExists(traversal, "~page"));
        Assert.assertTrue(hasStepExists(traversal, T.label.getAccessor()));

        traversal = traversal(
                __.V().outE().has("~page", "").limit(2).has(T.label, P.neq("knows")), graph);
        HugeVertexStep<?> vertexStep = replaceVertexStep(traversal);
        TraversalUtil.extractHasContainer(vertexStep, traversal);
        Assert.assertTrue(vertexStep.queryInfo().paging());
        Assert.assertFalse(hasStepExists(traversal, "~page"));
        Assert.assertTrue(hasStepExists(traversal, T.label.getAccessor()));
    }

    @Test
    public void testLocalSearchWithoutGraphKeepsPredicate() {
        ConditionP search = Text.contains("alpha");
        Traversal.Admin<?, ?> traversal = __.V().has("body", search)
                                           .limit(10).hasLabel(P.neq("author")).asAdmin();
        HugeGraphStep<?, ?> graphStep = replaceGraphStep(traversal);
        HasContainer original = ((HasStep<?>) graphStep.getNextStep()).getHasContainers().get(0);
        TraversalUtil.extractHasContainer(graphStep, traversal);
        Assert.assertTrue(graphStep.getHasContainers().isEmpty());
        Assert.assertSame(original, ((HasStep<?>) graphStep.getNextStep()).getHasContainers().get(0));
        Assert.assertSame(search, original.getPredicate());
        Assert.assertTrue(search.test("alpha beta"));
        Assert.assertFalse(search.test("beta"));

        traversal = __.outE().has("body", Text.contains("alpha"))
                      .limit(10).hasLabel(P.neq("knows")).asAdmin();
        HugeVertexStep<?> vertexStep = replaceVertexStep(traversal);
        original = ((HasStep<?>) vertexStep.getNextStep()).getHasContainers().get(0);
        TraversalUtil.extractHasContainer(vertexStep, traversal);
        Assert.assertTrue(vertexStep.getHasContainers().isEmpty());
        Assert.assertSame(original, ((HasStep<?>) vertexStep.getNextStep()).getHasContainers().get(0));
    }

    @Test
    public void testLocalLabelIdWithoutGraphAndAfterClone() {
        Id id = IdGenerator.of(12L);
        Traversal.Admin<?, ?> traversal = __.V().has(T.label, id).limit(10)
                                           .hasLabel(P.neq("other")).asAdmin();
        HugeGraphStep<?, ?> source = replaceGraphStep(traversal);
        TraversalUtil.extractHasContainer(source, traversal);
        HasContainer filter = ((HasStep<?>) source.getNextStep()).getHasContainers().get(0);
        HugeVertex vertex = Mockito.mock(HugeVertex.class);
        Mockito.when(vertex.schemaLabel()).thenReturn(new VertexLabel(null, id, "v"));
        Assert.assertTrue(filter.test(vertex));
        Assert.assertTrue(filter.clone().test(vertex));
        Mockito.when(vertex.schemaLabel()).thenReturn(new VertexLabel(null, IdGenerator.of(13L), "v"));
        Assert.assertFalse(filter.test(vertex));
        Assert.assertFalse(filter.clone().test(vertex));
    }

    @Test
    public void testUnsafeLabelKeepsPointLookupPlan() {
        for (GraphTraversal<?, ?> query : new GraphTraversal<?, ?>[]{
                __.V().hasId(1).limit(10).hasLabel(P.neq("other")),
                EmptyGraph.instance().traversal().E().hasId(1).limit(10).hasLabel(P.neq("other")),
                __.V().hasId(P.within(1, 2)).hasLabel(P.neq("other"))}) {
            Traversal.Admin<?, ?> admin = query.asAdmin();
            HugeGraphStep<?, ?> source = replaceGraphStep(admin);
            TraversalUtil.extractHasContainer(source, admin);
            Assert.assertTrue(source.getIds().length > 0);
            Assert.assertEquals(1, source.getIds()[0]);
            Assert.assertFalse(hasStepExists(admin, T.id.getAccessor()));
        }
        Traversal.Admin<?, ?> admin = __.V(1, 2).hasId(2)
                                       .hasLabel(P.neq("other")).asAdmin();
        HugeGraphStep<?, ?> source = replaceGraphStep(admin);
        TraversalUtil.extractHasContainer(source, admin);
        Assert.assertEquals(2, source.getIds().length);
        Assert.assertTrue(hasStepExists(admin, T.id.getAccessor()));

        admin = __.V().hasId(P.neq(1)).hasLabel(P.neq("other")).asAdmin();
        source = replaceGraphStep(admin);
        TraversalUtil.extractHasContainer(source, admin);
        Assert.assertEquals(0, source.getIds().length);
        Assert.assertTrue(hasStepExists(admin, T.id.getAccessor()));
    }

    @Test
    public void testLabelAfterElementChangeKeepsSourceIndexPlan() {
        HugeGraph graph = Mockito.mock(HugeGraph.class);
        Mockito.when(graph.propertyKey("city")).thenReturn(propertyKey(2L, "city", DataType.TEXT));
        for (GraphTraversal<?, ?> query : new GraphTraversal<?, ?>[]{
                __.V().has("city", "Beijing").out().hasLabel(P.neq("author")),
                __.V().has("city", "Beijing").out().where(__.not(__.hasLabel("author"))),
                __.V().has("city", "Beijing").outE().inV().hasLabel(P.neq("author")),
                __.V().has("city", "Beijing").properties().hasLabel(P.neq("author"))}) {
            Traversal.Admin<?, ?> admin = traversal(query, graph);
            HugeGraphStep<?, ?> source = replaceGraphStep(admin);
            TraversalUtil.extractHasContainer(source, admin);
            Assert.assertTrue(hasContainer(source, "city"));
            Assert.assertFalse(hasStepExists(admin, "city"));
        }
        for (GraphTraversal<?, ?> query : new GraphTraversal<?, ?>[]{
                __.V().has("city", "Beijing").as("a").out().select("a").hasLabel(P.neq("author")),
                __.V().has("city", "Beijing").out().path().unfold().hasLabel(P.neq("author")),
                __.V().has("city", "Beijing").out().filter(__.select("a").hasLabel(P.neq("author")))}) {
            Traversal.Admin<?, ?> admin = traversal(query, graph);
            HugeGraphStep<?, ?> source = replaceGraphStep(admin);
            TraversalUtil.extractHasContainer(source, admin);
            Assert.assertFalse(hasContainer(source, "city"));
            Assert.assertTrue(hasStepExists(admin, "city"));
        }
    }

    @Test
    public void testChildSourceKeepsPropertyBeforeParentUnsafeLabel() {
        HugeGraph graph = Mockito.mock(HugeGraph.class);
        Mockito.when(graph.propertyKey("city")).thenReturn(propertyKey(2L, "city", DataType.TEXT));
        GraphTraversal<?, ?> child = __.V().has("city", "Beijing");
        traversal(__.inject(1).union(child).hasLabel(P.neq("author")), graph);
        Traversal.Admin<?, ?> admin = traversal(child, graph);
        HugeGraphStep<?, ?> graphStep = replaceGraphStep(admin);
        TraversalUtil.extractHasContainer(graphStep, admin);
        Assert.assertTrue(graphStep.getHasContainers().isEmpty());
        Assert.assertTrue(hasStepExists(admin, "city"));

        child = __.out().has("city", "Beijing");
        traversal(__.V().local(child).hasLabel(P.neq("author")), graph);
        admin = traversal(child, graph);
        HugeVertexStep<?> vertexStep = replaceVertexStep(admin);
        TraversalUtil.extractHasContainer(vertexStep, admin);
        Assert.assertTrue(vertexStep.getHasContainers().isEmpty());
        Assert.assertTrue(hasStepExists(admin, "city"));
    }

    @Test
    public void testChildSourceStillExtractsWithoutDownstreamUnsafeLabel() {
        HugeGraph graph = Mockito.mock(HugeGraph.class);
        Mockito.when(graph.propertyKey("city")).thenReturn(propertyKey(2L, "city", DataType.TEXT));
        GraphTraversal<?, ?> child = __.V().has("city", "Beijing");
        traversal(__.inject(1).union(child).hasLabel("person"), graph);
        Traversal.Admin<?, ?> admin = traversal(child, graph);
        HugeGraphStep<?, ?> graphStep = replaceGraphStep(admin);
        TraversalUtil.extractHasContainer(graphStep, admin);
        Assert.assertTrue(hasContainer(graphStep, "city"));
        Assert.assertFalse(hasStepExists(admin, "city"));
    }

    @Test
    public void testNotContextOnlyBlocksLabelDependentPushdown() {
        HugeGraph graph = Mockito.mock(HugeGraph.class);
        Mockito.when(graph.propertyKey("city")).thenReturn(propertyKey(2L, "city", DataType.TEXT));
        Traversal.Admin<?, ?> traversal = traversal(
                __.V().has("city", "Beijing").not(__.hasLabel("author")), graph);
        HugeGraphStep<?, ?> source = replaceGraphStep(traversal);
        TraversalUtil.extractHasContainer(source, traversal);
        Assert.assertTrue(source.getHasContainers().isEmpty());
        Assert.assertTrue(hasStepExists(traversal, "city"));

        traversal = traversal(__.V().has("city", "Beijing").not(__.has("name", "Tom")), graph);
        source = replaceGraphStep(traversal);
        TraversalUtil.extractHasContainer(source, traversal);
        Assert.assertTrue(hasContainer(source, "city"));
        Assert.assertFalse(hasStepExists(traversal, "city"));
    }

    @Test
    public void testConnectiveLabelStepStrategyApplyPost() {
        Set<Class<? extends TraversalStrategy.OptimizationStrategy>> post =
                HugeConnectiveLabelStepStrategy.instance().applyPost();

        Assert.assertEquals(Collections.singleton(InlineFilterStrategy.class),
                            post);
    }

    @Test
    public void testConnectiveLabelStepStrategyMarksAndChildren() {
        Traversal.Admin<?, ?> traversal = __.V()
                                           .has("age", P.gt(18))
                                           .and(__.hasLabel("person"))
                                           .asAdmin();

        HugeConnectiveLabelStepStrategy.instance().apply(traversal);

        Assert.assertTrue(hasMarkedLocalChild(traversal, AndStep.class));
    }

    @Test
    public void testConnectiveLabelStepStrategyMarksOrChildren() {
        Traversal.Admin<?, ?> traversal = __.V()
                                           .has("age", P.gt(18))
                                           .or(__.hasLabel("software"),
                                               __.hasLabel(P.within("person")))
                                           .asAdmin();

        HugeConnectiveLabelStepStrategy.instance().apply(traversal);

        Assert.assertTrue(hasMarkedLocalChild(traversal, OrStep.class));
    }

    @Test
    public void testConnectiveLabelStepStrategySkipsWithoutPreviousHasStep() {
        Traversal.Admin<?, ?> traversal = __.V()
                                           .and(__.hasLabel("person"))
                                           .asAdmin();

        HugeConnectiveLabelStepStrategy.instance().apply(traversal);

        Assert.assertFalse(hasMarkedLocalChild(traversal, AndStep.class));
    }

    @Test
    public void testConnectiveLabelStepStrategySkipsUnsupportedChildren() {
        Traversal.Admin<?, ?> traversal = __.V()
                                           .has("age", P.gt(18))
                                           .and(__.has("name", "marko"))
                                           .or(__.hasLabel(P.without("person")),
                                               __.has("name", "marko"))
                                           .asAdmin();

        HugeConnectiveLabelStepStrategy.instance().apply(traversal);

        Assert.assertFalse(hasMarkedLocalChild(traversal, AndStep.class));
        Assert.assertFalse(hasMarkedLocalChild(traversal, OrStep.class));
    }

    private static PropertyKey propertyKey(long id, String name,
                                           DataType dataType) {
        Id keyId = IdGenerator.of(id);
        PropertyKey key = new PropertyKey(null, keyId, name);
        key.dataType(dataType);
        return key;
    }

    private static Traversal.Admin<?, ?> traversal(GraphTraversal<?, ?> traversal,
                                                   HugeGraph graph) {
        Traversal.Admin<?, ?> admin = traversal.asAdmin();
        admin.setGraph(graph);
        return admin;
    }

    private static HugeGraphStep<?, ?> replaceGraphStep(Traversal.Admin<?, ?> traversal) {
        GraphStep<?, ?> origin = (GraphStep<?, ?>) traversal.getStartStep();
        HugeGraphStep<?, ?> newStep = new HugeGraphStep<>(origin);
        replaceStep(origin, newStep, traversal);
        return newStep;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static HugeVertexStep<?> replaceVertexStep(Traversal.Admin<?, ?> traversal) {
        VertexStep<Vertex> origin = null;
        for (Step<?, ?> step : traversal.getSteps()) {
            if (step instanceof VertexStep) {
                origin = (VertexStep) step;
                break;
            }
        }
        Assert.assertNotNull(origin);
        HugeVertexStep<?> newStep = new HugeVertexStep<>(origin);
        replaceStep(origin, newStep, traversal);
        return newStep;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static void replaceStep(Step<?, ?> origin, Step<?, ?> newStep,
                                    Traversal.Admin<?, ?> traversal) {
        TraversalHelper.replaceStep((Step) origin, (Step) newStep, traversal);
    }

    private static boolean hasContainer(HugeGraphStep<?, ?> step, String key) {
        for (HasContainer has : step.getHasContainers()) {
            if (key.equals(has.getKey())) {
                return true;
            }
        }
        return false;
    }

    private static boolean hasStepExists(Traversal.Admin<?, ?> traversal) {
        return countHasSteps(traversal) > 0;
    }

    private static int countHasSteps(Traversal.Admin<?, ?> traversal) {
        int count = 0;
        for (Step<?, ?> step : traversal.getSteps()) {
            if (step instanceof HasStep) {
                count++;
            }
        }
        return count;
    }

    private static boolean hasStepExists(Traversal.Admin<?, ?> traversal,
                                         String key) {
        for (Step<?, ?> step : traversal.getSteps()) {
            if (!(step instanceof HasStep)) {
                continue;
            }
            HasStep<?> hasStep = (HasStep<?>) step;
            for (HasContainer has : hasStep.getHasContainers()) {
                if (key.equals(has.getKey())) {
                    return true;
                }
            }
        }
        return false;
    }

    private static boolean stepExists(Traversal.Admin<?, ?> traversal,
                                      Class<?> clazz) {
        for (Step<?, ?> step : traversal.getSteps()) {
            if (clazz.isInstance(step)) {
                return true;
            }
        }
        return false;
    }

    private static boolean hasMarkedLocalChild(Traversal.Admin<?, ?> traversal,
                                               Class<?> clazz) {
        for (Step<?, ?> step : traversal.getSteps()) {
            if (!clazz.isInstance(step)) {
                continue;
            }
            TraversalParent parent = (TraversalParent) step;
            for (Traversal.Admin<?, ?> child : parent.getLocalChildren()) {
                for (Step<?, ?> childStep : child.getSteps()) {
                    if (!childStep.getLabels().isEmpty()) {
                        return true;
                    }
                }
            }
        }
        return false;
    }
}
