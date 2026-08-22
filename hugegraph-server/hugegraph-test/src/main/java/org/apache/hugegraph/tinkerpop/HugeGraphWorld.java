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

import java.lang.annotation.Annotation;
import java.util.Locale;
import java.util.Map;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.MapConfiguration;
import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.schema.SchemaManager;
import org.apache.hugegraph.type.define.DataType;
import org.apache.hugegraph.type.define.IdStrategy;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.features.World;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;

import io.cucumber.java.Scenario;

public class HugeGraphWorld implements World {

    private static final HugeGraphProviderContext PROVIDER_CONTEXT =
            new HugeGraphProviderContext();

    private final ProcessTestGraphProvider provider;
    private Scenario scenario;
    private Graph graph;
    private Configuration configuration;

    public HugeGraphWorld() {
        this.provider = PROVIDER_CONTEXT.provider();
    }

    static void clearProvider() {
        PROVIDER_CONTEXT.clear();
    }

    @Override
    public void beforeEachScenario(Scenario scenario) {
        this.scenario = scenario;
    }

    @Override
    public GraphTraversalSource getGraphTraversalSource(
            LoadGraphWith.GraphData graphData) {
        if (this.scenario == null) {
            throw new IllegalStateException("Scenario has not been initialized");
        }
        if (this.graph != null) {
            this.clearGraph();
        }

        Map<String, Object> config = this.provider.getBaseConfiguration(
                graphName(graphData), HugeGraphFeatureTest.class,
                this.scenario.getName(), graphData);
        this.configuration = new MapConfiguration(config);
        this.graph = this.provider.openTestGraph(this.configuration);
        this.prepareGraph(graphData);
        return this.provider.traversal(this.graph);
    }

    @Override
    public void afterEachScenario() {
        this.clearGraph();
    }

    @Override
    public String convertIdToScript(Object id,
                                    Class<? extends Element> type) {
        return this.provider.convertId(id, type);
    }

    private void clearGraph() {
        if (this.graph == null) {
            return;
        }

        try {
            this.provider.clear(this.graph, this.configuration);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to clear test graph", e);
        } finally {
            this.graph = null;
            this.configuration = null;
        }
    }

    private void prepareGraph(LoadGraphWith.GraphData graphData) {
        TestGraph testGraph = (TestGraph) this.graph;
        if (graphData == null) {
            testGraph.clearAll("");
            testGraph.initModernSchema(IdStrategy.AUTOMATIC);
            this.prepareGherkinSchema(testGraph);
            testGraph.tx().commit();
            testGraph.autoPerson(true);
            return;
        }

        this.provider.loadGraphData(testGraph, new GraphDataLoader(graphData),
                                    HugeGraphFeatureTest.class,
                                    this.scenario.getName());
        this.prepareGherkinSchema(testGraph);
        testGraph.tx().commit();
    }

    private void prepareGherkinSchema(TestGraph testGraph) {
        HugeGraph graph = testGraph.hugegraph();
        SchemaManager schema = graph.schema();
        schema.propertyKey("birthday").dataType(DataType.OBJECT)
              .ifNotExist().create();
        schema.propertyKey("created").ifNotExist().create();
        schema.propertyKey("matched").ifNotExist().create();
        schema.vertexLabel("a").useAutomaticId().ifNotExist().create();
        schema.vertexLabel("b").useAutomaticId().ifNotExist().create();
        schema.vertexLabel("prefix_person").useAutomaticId()
              .ifNotExist().create();
        this.prepareVertexLabel(graph, schema, "person");
        this.prepareVertexLabel(graph, schema, "software");
        this.prepareVertexLabel(graph, schema, TestGraph.DEFAULT_VL);
        if (graph.existsVertexLabel("person")) {
            schema.vertexLabel("person").properties("birthday")
                  .nullableKeys("birthday").append();
        }
        this.prepareEdgeLabel(graph, schema, "knows");
        this.prepareEdgeLabel(graph, schema, "created");
        if (graph.existsVertexLabel("person")) {
            schema.edgeLabel("self").link("person", "person")
                  .properties("weight", "created", "matched")
                  .nullableKeys("weight", "created", "matched")
                  .ifNotExist().create();
            this.prepareEdgeLabel(graph, schema, "self");
        }
    }

    private void prepareVertexLabel(HugeGraph graph, SchemaManager schema,
                                    String label) {
        if (!graph.existsVertexLabel(label)) {
            return;
        }
        schema.vertexLabel(label).properties("created", "matched")
              .nullableKeys("created", "matched").append();
        schema.indexLabel(label + "ByCreated").onV(label).by("created")
              .secondary().ifNotExist().create();
        schema.indexLabel(label + "ByMatched").onV(label).by("matched")
              .secondary().ifNotExist().create();
    }

    private void prepareEdgeLabel(HugeGraph graph, SchemaManager schema,
                                  String label) {
        if (!graph.existsEdgeLabel(label)) {
            return;
        }
        schema.edgeLabel(label).properties("created", "matched")
              .nullableKeys("created", "matched").append();
        schema.indexLabel(label + "ByCreated").onE(label).by("created")
              .secondary().ifNotExist().create();
        schema.indexLabel(label + "ByMatched").onE(label).by("matched")
              .secondary().ifNotExist().create();
    }

    private static String graphName(LoadGraphWith.GraphData graphData) {
        if (graphData == null) {
            return "gherkin_empty_standard";
        }
        return "gherkin_" + graphData.name().toLowerCase(Locale.ROOT) +
               "_standard";
    }

    private static final class GraphDataLoader implements LoadGraphWith {

        private final GraphData graphData;

        private GraphDataLoader(GraphData graphData) {
            this.graphData = graphData;
        }

        @Override
        public GraphData value() {
            return this.graphData;
        }

        @Override
        public Class<? extends Annotation> annotationType() {
            return LoadGraphWith.class;
        }
    }
}
