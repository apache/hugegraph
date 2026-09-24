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

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.core.GraphManager;
import org.apache.hugegraph.traversal.algorithm.HugeTraverser;
import org.apache.hugegraph.traversal.algorithm.MultiNodeShortestPathTraverser;
import org.apache.hugegraph.traversal.algorithm.steps.EdgeStep;
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

@Path("graphspaces/{graphspace}/graphs/{graph}/traversers/multinodeshortestpath")
@Singleton
@Tag(name = "MultiNodeShortestPathAPI")
public class MultiNodeShortestPathAPI extends TraverserAPI {

    private static final Logger LOG = Log.logger(MultiNodeShortestPathAPI.class);

    @POST
    @Timed
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String post(@Context GraphManager manager,
                       @PathParam("graphspace") String graphSpace,
                       @PathParam("graph") String graph,
                       Request request) {
        E.checkArgumentNotNull(request, "The request body can't be null");
        E.checkArgumentNotNull(request.vertices,
                               "The vertices of request can't be null");
        E.checkArgument(request.step != null,
                        "The steps of request can't be null");

        LOG.debug("Graph [{}] get multiple node shortest path from " +
                  "vertices '{}', with step '{}', max_depth '{}', capacity " +
                  "'{}' and with_vertex '{}'",
                  graph, request.vertices, request.step, request.maxDepth,
                  request.capacity, request.withVertex);

        ApiMeasurer measure = new ApiMeasurer();

        HugeGraph g = graph(manager, graphSpace, graph);
        Iterator<Vertex> vertices = request.vertices.vertices(g);

        EdgeStep step = step(g, request.step);

        MultiNodeShortestPathTraverser.WrappedListPath wrappedListPath;
        try (MultiNodeShortestPathTraverser traverser =
                     new MultiNodeShortestPathTraverser(g)) {
            wrappedListPath = traverser.multiNodeShortestPath(vertices, step,
                                                              request.maxDepth,
                                                              request.capacity);
            measure.addIterCount(traverser.vertexIterCounter.get(),
                                 traverser.edgeIterCounter.get());
        }

        List<HugeTraverser.Path> paths = wrappedListPath.paths();

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
        Set<Edge> edges = wrappedListPath.edges();
        if (request.withEdge && !edges.isEmpty()) {
            iterEdge = wrappedListPath.edges().iterator();
        } else {
            iterEdge = HugeTraverser.EdgeRecord.getEdgeIds(edges).iterator();
        }

        return manager.serializer(g, measure.measures())
                      .writePaths("paths", paths,
                                  false, iterVertex, iterEdge);
    }

    private static class Request {

        @JsonProperty("vertices")
        public Vertices vertices;
        @JsonProperty("step")
        public Step step;
        @JsonProperty("max_depth")
        public int maxDepth;
        @JsonProperty("capacity")
        public long capacity = Long.parseLong(DEFAULT_CAPACITY);
        @JsonProperty("with_vertex")
        public boolean withVertex = false;
        @JsonProperty("with_edge")
        public boolean withEdge = false;

        @Override
        public String toString() {
            return String.format("Request{vertices=%s,step=%s,maxDepth=%s" +
                                 "capacity=%s,withVertex=%s,withEdge=%s}",
                                 this.vertices, this.step, this.maxDepth,
                                 this.capacity, this.withVertex, this.withEdge);
        }
    }
}
