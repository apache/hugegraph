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
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.AddPropertyStep;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.junit.Test;

public class PrimaryKeyStrategyCoreTest extends BaseCoreTest {

    private static final String LABEL = "person";

    private static final String META_PROPERTIES_ERROR =
            VertexProperty.Exceptions.metaPropertiesNotSupported().getMessage();
    private static final String USER_SUPPLIED_IDS_ERROR =
            VertexProperty.Exceptions.userSuppliedIdsNotSupported().getMessage();

    private void initSchema() {
        SchemaManager schema = graph().schema();
        schema.propertyKey("name").asText().create();
        schema.propertyKey("country").asText().create();
        schema.propertyKey("since").asText().create();
        schema.vertexLabel(LABEL).properties("name", "country", "since")
              .primaryKeys("name").nullableKeys("country", "since").create();
    }

    /**
     * Applying the strategy must stay side effect free: the metadata-bearing step is left in the
     * traversal so that HugeVertex.property() can reject it when the step really runs.
     */
    @Test
    public void testKeepsMetaPropertyStepUnfolded() {
        this.initSchema();

        GraphTraversal<Vertex, Vertex> traversal = graph().traversal().addV(LABEL)
                                                          .property("country", "cn", "since", "2024");
        traversal.asAdmin().applyStrategies();

        Assert.assertTrue(traversal.asAdmin().getSteps().stream()
                                   .anyMatch(AddPropertyStep.class::isInstance));
        Assert.assertEquals(0L, graph().traversal().V().hasLabel(LABEL).count().next());
    }

    @Test
    public void testStartStepRejectsMetaProperties() {
        this.initSchema();

        GraphTraversal<Vertex, Vertex> traversal = graph().traversal().addV(LABEL)
                                                          .property("name", "marko")
                                                          .property("country", "cn", "since", "2024");

        this.assertRejectedWhenExecuted(traversal, META_PROPERTIES_ERROR);
    }

    @Test
    public void testMidTraversalStepRejectsMetaProperties() {
        this.initSchema();

        GraphTraversal<Integer, Vertex> traversal = graph().traversal().inject(1).addV(LABEL)
                                                           .property("name", "marko")
                                                           .property("country", "cn", "since", "2024");

        this.assertRejectedWhenExecuted(traversal, META_PROPERTIES_ERROR);
    }

    /**
     * The metadata-bearing step carries the primary key, so the strategy still has to fold
     * T.key/T.value into addV(). Otherwise the vertex creation fails first with an
     * IllegalArgumentException about the missing primary key, and the traversal never reaches the
     * metadata validation this test is about.
     */
    @Test
    public void testPrimaryKeyIsFoldedIntoAddVBeforeMetaPropertyCheck() {
        this.initSchema();

        GraphTraversal<Vertex, Vertex> traversal = graph().traversal().addV(LABEL)
                                                          .property("name", "marko", "since", "2024");

        this.assertRejectedWhenExecuted(traversal, META_PROPERTIES_ERROR);
    }

    @Test
    public void testUserSuppliedVertexPropertyIdRejected() {
        this.initSchema();

        GraphTraversal<Vertex, Vertex> traversal = graph().traversal().addV(LABEL)
                                                          .property("name", "marko", T.id, 1);

        this.assertRejectedWhenExecuted(traversal, USER_SUPPLIED_IDS_ERROR);
    }

    /**
     * TinkerPop applies strategies to coalesce() children before a branch is selected, so eager
     * validation would fail this traversal even though unfold() answers it and the addV() fallback
     * never runs.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testUnreachableCoalesceBranchIsNotValidated() {
        this.initSchema();
        graph().addVertex(T.label, LABEL, "name", "marko");
        commitTx();

        GraphTraversal<Vertex, Vertex> traversal =
                graph().traversal().V().hasLabel(LABEL).fold()
                       .coalesce(__.unfold(),
                                 __.addV(LABEL).property("name", "marko", "since", "2024"));

        this.assertFallbackNotExecuted(traversal);
    }

    @Test
    public void testUnreachableChooseBranchIsNotValidated() {
        this.initSchema();
        graph().addVertex(T.label, LABEL, "name", "marko");
        commitTx();

        GraphTraversal<Vertex, Vertex> traversal =
                graph().traversal().V().hasLabel(LABEL).fold()
                       .choose(__.unfold().count().is(0L),
                               __.addV(LABEL).property("name", "marko", "since", "2024"),
                               __.unfold());

        this.assertFallbackNotExecuted(traversal);
    }

    @Test
    public void testFoldsSingleCardinalityProperties() {
        this.initSchema();

        GraphTraversal<Vertex, Vertex> traversal =
                graph().traversal().addV(LABEL)
                       .property(VertexProperty.Cardinality.single, "name", "marko")
                       .property(VertexProperty.Cardinality.single, "country", "cn");
        traversal.asAdmin().applyStrategies();
        Assert.assertFalse(traversal.asAdmin().getSteps().stream()
                                    .anyMatch(AddPropertyStep.class::isInstance));

        Vertex vertex = traversal.next();
        commitTx();

        Assert.assertEquals("marko", vertex.value("name"));
        Assert.assertEquals("cn", vertex.value("country"));
        Assert.assertEquals(1L, graph().traversal().V().hasLabel(LABEL).count().next());
    }

    private void assertRejectedWhenExecuted(GraphTraversal<?, Vertex> traversal, String expectedError) {
        Assert.assertThrows(UnsupportedOperationException.class, traversal::next,
                            e -> Assert.assertEquals(expectedError, e.getMessage()));

        // The addV() step already ran, so only the rollback guarantees that nothing survives
        graph().tx().rollback();
        Assert.assertEquals(0L, graph().traversal().V().hasLabel(LABEL).count().next());
    }

    private void assertFallbackNotExecuted(GraphTraversal<?, Vertex> traversal) {
        Vertex vertex = traversal.next();
        commitTx();

        Assert.assertEquals("marko", vertex.value("name"));
        Assert.assertFalse(vertex.property("since").isPresent());
        Assert.assertEquals(1L, graph().traversal().V().hasLabel(LABEL).count().next());
    }
}
