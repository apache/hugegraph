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
import static org.apache.hugegraph.traversal.algorithm.HugeTraverser.DEFAULT_ELEMENTS_LIMIT;
import static org.apache.hugegraph.traversal.algorithm.HugeTraverser.DEFAULT_MAX_DEGREE;
import static org.apache.hugegraph.traversal.algorithm.HugeTraverser.NO_LIMIT;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.api.graph.EdgeAPI;
import org.apache.hugegraph.api.graph.VertexAPI;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.query.QueryResults;
import org.apache.hugegraph.core.GraphManager;
import org.apache.hugegraph.structure.HugeVertex;
import org.apache.hugegraph.traversal.algorithm.HugeTraverser;
import org.apache.hugegraph.traversal.algorithm.KoutTraverser;
import org.apache.hugegraph.traversal.algorithm.records.KoutRecords;
import org.apache.hugegraph.traversal.algorithm.steps.Steps;
import org.apache.hugegraph.type.define.Directions;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Log;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.slf4j.Logger;

import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.inject.Singleton;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Context;

@Path("graphspaces/{graphspace}/graphs/{graph}/traversers/kout")
@Singleton
@Tag(name = "KoutAPI")
public class KoutAPI extends TraverserAPI {

    private static final Logger LOG = Log.logger(KoutAPI.class);

    @GET
    @Timed
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String get(@Context GraphManager manager,
                      @Parameter(description = "The graph space name")
                      @PathParam("graphspace") String graphSpace,
                      @Parameter(description = "The graph name")
                      @PathParam("graph") String graph,
                      @Parameter(description = "The source vertex ID")
                      @QueryParam("source") String source,
                      @Parameter(description = "The direction of traversal")
                      @QueryParam("direction") String direction,
                      @Parameter(description = "The edge label to traverse")
                      @QueryParam("label") String edgeLabel,
                      @Parameter(description = "The maximum depth of traversal")
                      @QueryParam("max_depth") int depth,
                      @Parameter(description = "Whether to find nearest vertices first")
                      @QueryParam("nearest")
                      @DefaultValue("true") boolean nearest,
                      @Parameter(description = "Whether to return only count")
                      @QueryParam("count_only")
                      @DefaultValue("false") boolean count_only,
                      @Parameter(description = "The maximum degree of vertices")
                      @QueryParam("max_degree")
                      @DefaultValue(DEFAULT_MAX_DEGREE) long maxDegree,
                      @Parameter(description = "The capacity of the traversal")
                      @QueryParam("capacity")
                      @DefaultValue(DEFAULT_CAPACITY) long capacity,
                      @Parameter(description = "The maximum number of results")
                      @QueryParam("limit")
                      @DefaultValue(DEFAULT_ELEMENTS_LIMIT) int limit) {
        LOG.debug("Graph [{}] get k-out from '{}' with " +
                  "direction '{}', edge label '{}', max depth '{}', nearest " +
                  "'{}', max degree '{}', capacity '{}' and limit '{}'",
                  graph, source, direction, edgeLabel, depth,
                  nearest, maxDegree, capacity, limit);

        ApiMeasurer measure = new ApiMeasurer();

        Id sourceId = VertexAPI.checkAndParseVertexId(source);
        Directions dir = Directions.convert(EdgeAPI.parseDirection(direction));

        HugeGraph g = graph(manager, graphSpace, graph);

        Set<Id> ids;
        try (KoutTraverser traverser = new KoutTraverser(g)) {
            ids = traverser.kout(sourceId, dir, edgeLabel, depth,
                                 nearest, maxDegree, capacity, limit);
            measure.addIterCount(traverser.vertexIterCounter.get(),
                                 traverser.edgeIterCounter.get());
        }

        if (count_only) {
            return manager.serializer(g, measure.measures())
                          .writeMap(ImmutableMap.of("vertices_size", ids.size()));
        }
        return manager.serializer(g, measure.measures()).writeList("vertices", ids);
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
        E.checkArgumentNotNull(request.source,
                               "The source of request can't be null");
        E.checkArgument(request.steps != null,
                        "The steps of request can't be null");
        if (request.countOnly) {
            E.checkArgument(!request.withVertex && !request.withPath && !request.withEdge,
                            "Can't return vertex, edge or path when count only");
        }
        HugeTraverser.checkTraverseMode(request.traverseMode);

        LOG.debug("Graph [{}] get customized kout from source vertex '{}', " +
                  "with steps '{}', max_depth '{}', nearest '{}', " +
                  "count_only '{}', capacity '{}', limit '{}', " +
                  "with_vertex '{}', with_path '{}' and with_edge '{}'",
                  graph, request.source, request.steps, request.maxDepth,
                  request.nearest, request.countOnly, request.capacity,
                  request.limit, request.withVertex, request.withPath,
                  request.withEdge);

        ApiMeasurer measure = new ApiMeasurer();

        HugeGraph g = graph(manager, graphSpace, graph);
        Id sourceId = HugeVertex.getIdValue(request.source);

        Steps steps = steps(g, request.steps);
        KoutRecords results;
        try (KoutTraverser traverser = new KoutTraverser(g)) {
            if (HugeTraverser.isTraverseModeDFS(request.traverseMode)) {
                results = traverser.dfsKout(sourceId, steps,
                                            request.maxDepth,
                                            request.nearest,
                                            request.capacity,
                                            request.limit);
            } else {
                results = traverser.customizedKout(sourceId, steps,
                                                   request.maxDepth,
                                                   request.nearest,
                                                   request.capacity,
                                                   request.limit);
            }
            measure.addIterCount(traverser.vertexIterCounter.get(),
                                 traverser.edgeIterCounter.get());
        }
        long size = results.size();
        if (request.limit != NO_LIMIT && size > request.limit) {
            size = request.limit;
        }
        List<Id> neighbors = request.countOnly ?
                             ImmutableList.of() : results.ids(request.limit);

        HugeTraverser.PathSet paths = new HugeTraverser.PathSet();
        if (request.withPath) {
            paths.addAll(results.paths(request.limit));
        }

        if (request.countOnly) {
            return manager.serializer(g, measure.measures())
                          .writeNodesWithPath("kout", neighbors, size, paths,
                                              QueryResults.emptyIterator(),
                                              QueryResults.emptyIterator());
        }

        Iterator<?> iterVertex;
        Set<Id> vertexIds = new HashSet<>(neighbors);
        if (request.withPath) {
            for (HugeTraverser.Path p : results.paths(request.limit)) {
                vertexIds.addAll(p.vertices());
            }
        }
        if (request.withVertex && !vertexIds.isEmpty()) {
            iterVertex = g.vertices(vertexIds.toArray());
            measure.addIterCount(vertexIds.size(), 0L);
        } else {
            iterVertex = vertexIds.iterator();
        }

        Iterator<?> iterEdge = Collections.emptyIterator();
        if (request.withPath) {
            Set<Edge> edges = results.edgeResults().getEdges(paths);
            if (request.withEdge) {
                iterEdge = edges.iterator();
            } else {
                iterEdge = HugeTraverser.EdgeRecord.getEdgeIds(edges).iterator();
            }
        }

        return manager.serializer(g, measure.measures())
                      .writeNodesWithPath("kout", neighbors, size, paths,
                                          iterVertex, iterEdge);
    }

    private static class Request {

        @JsonProperty("source")
        public Object source;
        @JsonProperty("steps")
        public TraverserAPI.VESteps steps;
        @JsonProperty("max_depth")
        public int maxDepth;
        @JsonProperty("nearest")
        public boolean nearest = true;
        @JsonProperty("count_only")
        public boolean countOnly = false;
        @JsonProperty("capacity")
        public long capacity = Long.parseLong(DEFAULT_CAPACITY);
        @JsonProperty("limit")
        public int limit = Integer.parseInt(DEFAULT_ELEMENTS_LIMIT);
        @JsonProperty("with_vertex")
        public boolean withVertex = false;
        @JsonProperty("with_path")
        public boolean withPath = false;
        @JsonProperty("with_edge")
        public boolean withEdge = false;
        @JsonProperty("traverse_mode")
        public String traverseMode = HugeTraverser.TRAVERSE_MODE_BFS;

        @Override
        public String toString() {
            return String.format("KoutRequest{source=%s,steps=%s,maxDepth=%s" +
                                 "nearest=%s,countOnly=%s,capacity=%s," +
                                 "limit=%s,withVertex=%s,withPath=%s," +
                                 "withEdge=%s,traverseMode=%s}", this.source,
                                 this.steps, this.maxDepth, this.nearest,
                                 this.countOnly, this.capacity, this.limit,
                                 this.withVertex, this.withPath, this.withEdge,
                                 this.traverseMode);
        }
    }
}
