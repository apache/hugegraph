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
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

public class PrimaryKeyStrategyCoreTest extends BaseCoreTest {

    private static final String PRIMARY_LABEL = "primary";
    private static final String AUTOMATIC_LABEL = "automatic";

    private void initSchema() {
        SchemaManager schema = graph().schema();
        schema.propertyKey("name").asText().create();
        schema.propertyKey("country").asText().create();
        schema.vertexLabel(PRIMARY_LABEL)
              .properties("name", "country")
              .primaryKeys("name")
              .nullableKeys("country")
              .create();
        schema.vertexLabel(AUTOMATIC_LABEL)
              .useAutomaticId()
              .properties("name", "country")
              .nullableKeys("name", "country")
              .create();
    }

    @Test
    public void testStartStepRejectsMetaPropertiesForPrimaryKeyLabel() {
        this.assertMetaPropertiesRejected(PRIMARY_LABEL, false);
    }

    @Test
    public void testStartStepRejectsMetaPropertiesForAutomaticLabel() {
        this.assertMetaPropertiesRejected(AUTOMATIC_LABEL, false);
    }

    @Test
    public void testMidTraversalStepRejectsMetaPropertiesForPrimaryKeyLabel() {
        this.assertMetaPropertiesRejected(PRIMARY_LABEL, true);
    }

    @Test
    public void testMidTraversalStepRejectsMetaPropertiesForAutomaticLabel() {
        this.assertMetaPropertiesRejected(AUTOMATIC_LABEL, true);
    }

    @Test
    public void testStartStepCreatesPrimaryKeyVertexWithOneProperty() {
        this.initSchema();

        Vertex vertex = graph().traversal()
                               .addV(PRIMARY_LABEL)
                               .property("name", "marko")
                               .next();
        commitTx();

        Assert.assertEquals("marko", vertex.value("name"));
        Assert.assertEquals(1L, graph().traversal().V()
                                           .hasLabel(PRIMARY_LABEL)
                                           .count().next());
    }

    @Test
    public void testMidTraversalStepCreatesAutomaticVertexWithTwoProperties() {
        this.initSchema();

        Vertex vertex = graph().traversal()
                               .inject(1)
                               .addV(AUTOMATIC_LABEL)
                               .property("name", "marko")
                               .property("country", "cn")
                               .next();
        commitTx();

        Assert.assertEquals("marko", vertex.value("name"));
        Assert.assertEquals("cn", vertex.value("country"));
        Assert.assertEquals(1L, graph().traversal().V()
                                           .hasLabel(AUTOMATIC_LABEL)
                                           .count().next());
    }

    private void assertMetaPropertiesRejected(String label,
                                              boolean midTraversal) {
        this.initSchema();

        GraphTraversal<?, Vertex> traversal = midTraversal ?
                                               graph().traversal()
                                                      .inject(1)
                                                      .addV(label) :
                                               graph().traversal().addV(label);
        traversal.property("name", "marko", "country", "cn");

        Assert.assertThrows(UnsupportedOperationException.class,
                            traversal::next,
                            e -> Assert.assertEquals(
                                    "Properties on a vertex property is not " +
                                    "supported",
                                    e.getMessage()));
        Assert.assertEquals(0L, graph().traversal().V()
                                       .hasLabel(label).count().next());
    }
}
