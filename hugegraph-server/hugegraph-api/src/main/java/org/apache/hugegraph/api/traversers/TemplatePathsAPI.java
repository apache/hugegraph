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

package org.apache.hugegraph.api.traversers;

import static org.apache.hugegraph.traversal.algorithm.HugeTraverser.DEFAULT_CAPACITY;
import static org.apache.hugegraph.traversal.algorithm.HugeTraverser.DEFAULT_PATHS_LIMIT;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.core.GraphManager;
import org.apache.hugegraph.traversal.algorithm.HugeTraverser;
import org.apache.hugegraph.traversal.algorithm.TemplatePathsTraverser;
import org.apache.hugegraph.traversal.algorithm.steps.RepeatEdgeStep;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Log;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;

import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.inject.Singleton;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Context;

@Path("graphspaces/{graphspace}/graphs/{graph}/traversers/templatepaths")
@Singleton
@Tag(name = "TemplatePathsAPI")
public class TemplatePathsAPI extends TraverserAPI {

    private static final Logger LOG = Log.logger(TemplatePathsAPI.class);

    private static List<RepeatEdgeStep> steps(HugeGraph g,
                                              List<TemplatePathStep> steps) {
        List<RepeatEdgeStep> edgeSteps = new ArrayList<>(steps.size());
        for (TemplatePathStep step : steps) {
            edgeSteps.add(repeatEdgeStep(g, step));
        }
        return edgeSteps;
    }

    private static RepeatEdgeStep repeatEdgeStep(HugeGraph graph,
                                                 TemplatePathStep step) {
        return new RepeatEdgeStep(graph, step.direction, step.labels,
                                  step.properties, step.maxDegree,
                                  step.skipDegree, step.maxTimes);
    }

    @POST
    @Timed
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String post(@Context GraphManager manager,
                       @PathParam("graphspace") String graphSpace,
                       @PathParam("graph") String graph,
                       Request request) {
        E.checkArgumentNotNull(request, "The request body can't be null");
        E.checkArgumentNotNull(request.sources,
                               "The sources of request can't be null");
        E.checkArgumentNotNull(request.targets,
                               "The targets of request can't be null");
        E.checkArgument(request.steps != null && !request.steps.isEmpty(),
                        "The steps of request can't be empty");

        LOG.debug("Graph [{}] get template paths from source vertices '{}', " +
                  "target vertices '{}', with steps '{}', " +
                  "capacity '{}', limit '{}', with_vertex '{}' and with_edge '{}'",
                  graph, request.sources, request.targets, request.steps,
                  request.capacity, request.limit, request.withVertex, request.withEdge);

        ApiMeasurer measure = new ApiMeasurer();

        HugeGraph g = graph(manager, graphSpace, graph);
        Iterator<Vertex> sources = request.sources.vertices(g);
        Iterator<Vertex> targets = request.targets.vertices(g);
        List<RepeatEdgeStep> steps = steps(g, request.steps);

        TemplatePathsTraverser traverser = new TemplatePathsTraverser(g);
        TemplatePathsTraverser.WrappedPathSet wrappedPathSet =
                traverser.templatePaths(sources, targets, steps,
                                        request.withRing, request.capacity,
                                        request.limit);
        measure.addIterCount(traverser.vertexIterCounter.get(),
                             traverser.edgeIterCounter.get());

        Set<HugeTraverser.Path> paths = wrappedPathSet.paths();

        Iterator<?> iterVertex;
        Set<Id> vertexIds = new HashSet<>();
        for (HugeTraverser.Path path : paths) {
            vertexIds.addAll(path.vertices());
        }
        if (request.withVertex && !vertexIds.isEmpty()) {
            iterVertex = g.vertices(vertexIds.toArray());
            measure.addIterCount(vertexIds.size(), 0L);
        } else {
            iterVertex = vertexIds.iterator();
        }

        Iterator<?> iterEdge;
        Set<Edge> edges = wrappedPathSet.edges();
        if (request.withEdge && !edges.isEmpty()) {
            iterEdge = edges.iterator();
        } else {
            iterEdge = HugeTraverser.EdgeRecord.getEdgeIds(edges).iterator();
        }

        return manager.serializer(g, measure.measures())
                      .writePaths("paths", paths, false,
                                  iterVertex, iterEdge);
    }

    private static class Request {

        @JsonProperty("sources")
        public Vertices sources;
        @JsonProperty("targets")
        public Vertices targets;
        @JsonProperty("steps")
        public List<TemplatePathStep> steps;
        @JsonProperty("with_ring")
        public boolean withRing = false;
        @JsonProperty("capacity")
        public long capacity = Long.parseLong(DEFAULT_CAPACITY);
        @JsonProperty("limit")
        public int limit = Integer.parseInt(DEFAULT_PATHS_LIMIT);
        @JsonProperty("with_vertex")
        public boolean withVertex = false;
        @JsonProperty("with_edge")
        public boolean withEdge = false;

        @Override
        public String toString() {
            return String.format("TemplatePathsRequest{sources=%s,targets=%s," +
                                 "steps=%s,withRing=%s,capacity=%s,limit=%s," +
                                 "withVertex=%s,withEdge=%s}",
                                 this.sources, this.targets, this.steps,
                                 this.withRing, this.capacity, this.limit,
                                 this.withVertex, this.withEdge);
        }
    }

    protected static class TemplatePathStep extends Step {

        @JsonProperty("max_times")
        public int maxTimes = 1;

        @Override
        public String toString() {
            return String.format("TemplatePathStep{direction=%s,labels=%s," +
                                 "properties=%s,maxDegree=%s,skipDegree=%s," +
                                 "maxTimes=%s}",
                                 this.direction, this.labels, this.properties,
                                 this.maxDegree, this.skipDegree,
                                 this.maxTimes);
        }
    }
}
