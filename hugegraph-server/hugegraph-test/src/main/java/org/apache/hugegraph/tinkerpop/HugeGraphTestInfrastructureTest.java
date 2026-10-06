/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
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

package org.apache.hugegraph.tinkerpop;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.schema.PropertyKey;
import org.apache.hugegraph.schema.SchemaManager;
import org.apache.hugegraph.schema.VertexLabel;
import org.apache.hugegraph.testutil.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class HugeGraphTestInfrastructureTest {

    @Test
    public void testProviderContextLifecycle() {
        HugeGraphProviderContext context = new HugeGraphProviderContext();
        ProcessTestGraphProvider provider = context.provider();
        try {
            Assert.assertSame(provider, context.provider());

            context.clear();
            context.clear();

            Assert.assertNotSame(provider, context.provider());
        } finally {
            context.clear();
        }
    }

    @Test
    public void testExactScenarioCount() {
        HugeGraphScenarioCountPlugin.assertScenariosExecuted(361);

        Assert.assertThrows(AssertionError.class, () -> {
            HugeGraphScenarioCountPlugin.assertScenariosExecuted(360);
        }, e -> {
            Assert.assertContains("expected exactly 361", e.getMessage());
        });
        Assert.assertThrows(AssertionError.class, () -> {
            HugeGraphScenarioCountPlugin.assertScenariosExecuted(362);
        }, e -> {
            Assert.assertContains("expected exactly 361", e.getMessage());
        });
    }

    @Test
    public void testScenarioNameFilterExcludesUnsupportedLimitMerge() {
        Assert.assertFalse("g_V_limitX1X_mergeEXnullvarX"
                           .matches(HugeGraphFeatureTest.NAMES));
        Assert.assertTrue("g_mergeEXnullvarX"
                          .matches(HugeGraphFeatureTest.NAMES));
    }

    @Test
    public void testHStoreCleanupTruncatesDataBeforeClearingSchema() {
        HugeGraph graph = Mockito.mock(HugeGraph.class);
        SchemaManager schema = Mockito.mock(SchemaManager.class);
        PropertyKey propertyKey = Mockito.mock(PropertyKey.class);
        Mockito.when(graph.schema()).thenReturn(schema);
        Mockito.when(schema.getPropertyKeys())
               .thenReturn(Collections.singletonList(propertyKey));
        Mockito.when(graph.backend()).thenReturn("hstore");

        CleanupTestGraph testGraph = new CleanupTestGraph(graph);
        testGraph.clearAll("");

        Assert.assertTrue(testGraph.backendTruncated);
        Assert.assertTrue(testGraph.schemaCleared);
        Assert.assertEquals(Arrays.asList("truncate", "schema"),
                            testGraph.cleanupSteps);
    }

    @Test
    public void testHStoreLoadCleanupDoesNotTruncateBackend() {
        HugeGraph graph = Mockito.mock(HugeGraph.class);
        Mockito.when(graph.backend()).thenReturn("hstore");

        CleanupTestGraph testGraph = new CleanupTestGraph(graph);
        testGraph.clearForLoad();

        Assert.assertFalse(testGraph.backendTruncated);
        Assert.assertTrue(testGraph.schemaCleared);
        Assert.assertEquals(Collections.singletonList("schema"),
                            testGraph.cleanupSteps);
    }

    @Test
    public void testHStoreCleanupDoesNotSkipSchemaWithoutPropertyKeys() {
        HugeGraph graph = Mockito.mock(HugeGraph.class);
        SchemaManager schema = Mockito.mock(SchemaManager.class);
        VertexLabel vertexLabel = Mockito.mock(VertexLabel.class);
        Mockito.when(graph.schema()).thenReturn(schema);
        Mockito.when(schema.getPropertyKeys())
               .thenReturn(Collections.emptyList());
        Mockito.when(schema.getVertexLabels())
               .thenReturn(Collections.singletonList(vertexLabel));
        Mockito.when(graph.backend()).thenReturn("hstore");

        CleanupTestGraph testGraph = new CleanupTestGraph(graph);
        testGraph.clearAll("");

        Assert.assertTrue(testGraph.backendTruncated);
        Assert.assertTrue(testGraph.schemaCleared);
    }

    private static class CleanupTestGraph extends TestGraph {

        private boolean backendTruncated;
        private boolean schemaCleared;
        private final List<String> cleanupSteps;

        private CleanupTestGraph(HugeGraph graph) {
            super(graph);
            this.cleanupSteps = new ArrayList<>();
        }

        @Override
        protected void truncateBackend() {
            this.backendTruncated = true;
            this.cleanupSteps.add("truncate");
        }

        @Override
        protected void clearSchema() {
            this.schemaCleared = true;
            this.cleanupSteps.add("schema");
        }
    }
}
