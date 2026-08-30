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
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.AddPropertyStep;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.junit.Test;

public class PrimaryKeyStrategyCoreTest extends BaseCoreTest {

    private static final String LABEL = "person";

    private void initSchema() {
        SchemaManager schema = graph().schema();
        schema.propertyKey("name").asText().create();
        schema.propertyKey("country").asText().create();
        schema.vertexLabel(LABEL)
              .properties("name", "country")
              .primaryKeys("name")
              .nullableKeys("country")
              .create();
    }

    @Test
    public void testStartStepRejectsMetaProperties() {
        this.initSchema();
        GraphTraversal<?, Vertex> traversal = graph().traversal()
                                                   .addV(LABEL)
                                                   .property("name", "marko",
                                                             "country", "cn");
        this.assertMetaPropertiesRejected(traversal);
    }

    @Test
    public void testMidTraversalStepRejectsMetaProperties() {
        this.initSchema();
        GraphTraversal<?, Vertex> traversal = graph().traversal()
                                                   .inject(1)
                                                   .addV(LABEL)
                                                   .property("name", "marko",
                                                             "country", "cn");
        this.assertMetaPropertiesRejected(traversal);
    }

    @Test
    public void testFoldsSingleCardinalityProperties() {
        this.initSchema();

        GraphTraversal<Vertex, Vertex> traversal = graph().traversal()
                                                       .addV(LABEL)
                                                       .property(
                                                         VertexProperty.Cardinality.single,
                                                         "name", "marko")
                                                       .property(
                                                         VertexProperty.Cardinality.single,
                                                         "country", "cn");
        traversal.asAdmin().applyStrategies();
        Assert.assertFalse(traversal.asAdmin().getSteps().stream()
                                    .anyMatch(AddPropertyStep.class::isInstance));

        Vertex vertex = traversal.next();
        commitTx();

        Assert.assertEquals("marko", vertex.value("name"));
        Assert.assertEquals("cn", vertex.value("country"));
        Assert.assertEquals(1L, graph().traversal().V()
                                           .hasLabel(LABEL).count().next());
    }

    private void assertMetaPropertiesRejected(
            GraphTraversal<?, Vertex> traversal) {
        Assert.assertThrows(UnsupportedOperationException.class,
                            traversal::next,
                            e -> Assert.assertEquals(
                                    "Properties on a vertex property is not " +
                                    "supported",
                                    e.getMessage()));
        Assert.assertEquals(0L, graph().traversal().V()
                                      .hasLabel(LABEL).count().next());
    }
}
