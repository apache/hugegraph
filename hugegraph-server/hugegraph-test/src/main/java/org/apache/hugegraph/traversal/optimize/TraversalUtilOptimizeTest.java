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

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.id.IdGenerator;
import org.apache.hugegraph.exception.NotFoundException;
import org.apache.hugegraph.schema.PropertyKey;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.type.define.DataType;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Vertex;
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

    private static boolean hasStepExists(Traversal.Admin<?, ?> traversal) {
        for (Step<?, ?> step : traversal.getSteps()) {
            if (step instanceof HasStep) {
                return true;
            }
        }
        return false;
    }
}
