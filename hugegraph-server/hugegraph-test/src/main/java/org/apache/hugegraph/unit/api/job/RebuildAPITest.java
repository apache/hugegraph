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

package org.apache.hugegraph.unit.api.job;

import java.util.Map;

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.api.job.RebuildAPI;
import org.apache.hugegraph.core.GraphManager;
import org.apache.hugegraph.exception.NotFoundException;
import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.id.IdGenerator;
import org.apache.hugegraph.schema.SchemaManager;
import org.apache.hugegraph.struct.schema.VertexLabel;
import org.apache.hugegraph.struct.schema.EdgeLabel;
import org.apache.hugegraph.struct.schema.IndexLabel;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class RebuildAPITest {

    private GraphManager manager;
    private SchemaManager schema;
    private RebuildAPI api;

    @Before
    public void setup() {
        this.manager = Mockito.mock(GraphManager.class);
        HugeGraph graph = Mockito.mock(HugeGraph.class);
        this.schema = Mockito.mock(SchemaManager.class);
        Mockito.when(this.manager.graph("DEFAULT", "hugegraph")).thenReturn(graph);
        Mockito.when(graph.schema()).thenReturn(this.schema);
        this.api = new RebuildAPI();
    }

    @Test
    public void testMissingVertexLabelHasNotFoundSemantics() {
        VertexLabel.Builder builder = Mockito.mock(VertexLabel.Builder.class);
        Mockito.when(this.schema.vertexLabel("missing")).thenReturn(builder);
        assertMissing(() -> this.api.vertexLabelRebuild(this.manager, "DEFAULT", "hugegraph", "missing"),
                      "vertex label");
    }

    @Test
    public void testMissingEdgeLabelHasNotFoundSemantics() {
        EdgeLabel.Builder builder = Mockito.mock(EdgeLabel.Builder.class);
        Mockito.when(this.schema.edgeLabel("missing")).thenReturn(builder);
        assertMissing(() -> this.api.edgeLabelRebuild(this.manager, "DEFAULT", "hugegraph", "missing"),
                      "edge label");
    }

    @Test
    public void testMissingIndexLabelHasNotFoundSemantics() {
        IndexLabel.Builder builder = Mockito.mock(IndexLabel.Builder.class);
        Mockito.when(this.schema.indexLabel("missing")).thenReturn(builder);
        assertMissing(() -> this.api.indexLabelRebuild(this.manager, "DEFAULT", "hugegraph", "missing"),
                      "index label");
    }

    @Test
    public void testExistingLabelsKeepTheirTaskIds() {
        Id id = IdGenerator.of(321L);
        VertexLabel.Builder vertex = Mockito.mock(VertexLabel.Builder.class);
        EdgeLabel.Builder edge = Mockito.mock(EdgeLabel.Builder.class);
        IndexLabel.Builder index = Mockito.mock(IndexLabel.Builder.class);
        Mockito.when(this.schema.vertexLabel("person")).thenReturn(vertex);
        Mockito.when(this.schema.edgeLabel("knows")).thenReturn(edge);
        Mockito.when(this.schema.indexLabel("personByName")).thenReturn(index);
        Mockito.when(vertex.rebuildIndex()).thenReturn(id);
        Mockito.when(edge.rebuildIndex()).thenReturn(id);
        Mockito.when(index.rebuild()).thenReturn(id);

        Assert.assertEquals(Map.of("task_id", id),
                            this.api.vertexLabelRebuild(this.manager, "DEFAULT", "hugegraph", "person"));
        Assert.assertEquals(Map.of("task_id", id),
                            this.api.edgeLabelRebuild(this.manager, "DEFAULT", "hugegraph", "knows"));
        Assert.assertEquals(Map.of("task_id", id),
                            this.api.indexLabelRebuild(this.manager, "DEFAULT", "hugegraph", "personByName"));
    }

    private static void assertMissing(Runnable request, String type) {
        try {
            request.run();
            Assert.fail("Expected a missing schema label");
        } catch (NotFoundException error) {
            Assert.assertTrue(error.getMessage().contains(type));
            Assert.assertTrue(error.getMessage().contains("missing"));
            Assert.assertFalse(error.getMessage().contains("task_id"));
        }
    }
}
