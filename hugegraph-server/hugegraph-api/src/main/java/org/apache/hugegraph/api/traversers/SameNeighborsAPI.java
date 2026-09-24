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

import static org.apache.hugegraph.traversal.algorithm.HugeTraverser.DEFAULT_ELEMENTS_LIMIT;
import static org.apache.hugegraph.traversal.algorithm.HugeTraverser.DEFAULT_MAX_DEGREE;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.api.API;
import org.apache.hugegraph.api.graph.EdgeAPI;
import org.apache.hugegraph.api.graph.VertexAPI;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.core.GraphManager;
import org.apache.hugegraph.structure.HugeVertex;
import org.apache.hugegraph.traversal.algorithm.SameNeighborTraverser;
import org.apache.hugegraph.type.define.Directions;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.inject.Singleton;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Context;

@Path("graphspaces/{graphspace}/graphs/{graph}/traversers/sameneighbors")
@Singleton
@Tag(name = "SameNeighborsAPI")
public class SameNeighborsAPI extends API {

    private static final Logger LOG = Log.logger(SameNeighborsAPI.class);

    @GET
    @Timed
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String get(@Context GraphManager manager,
                      @PathParam("graphspace") String graphSpace,
                      @PathParam("graph") String graph,
                      @QueryParam("vertex") String vertex,
                      @QueryParam("other") String other,
                      @QueryParam("direction") String direction,
                      @QueryParam("label") String edgeLabel,
                      @QueryParam("max_degree")
                      @DefaultValue(DEFAULT_MAX_DEGREE) long maxDegree,
                      @QueryParam("limit")
                      @DefaultValue(DEFAULT_ELEMENTS_LIMIT) int limit) {
        LOG.debug("Graph [{}] get same neighbors between '{}' and '{}' with " +
                  "direction {}, edge label {}, max degree '{}' and limit '{}'",
                  graph, vertex, other, direction, edgeLabel, maxDegree, limit);

        ApiMeasurer measure = new ApiMeasurer();

        Id sourceId = VertexAPI.checkAndParseVertexId(vertex);
        Id targetId = VertexAPI.checkAndParseVertexId(other);
        Directions dir = Directions.convert(EdgeAPI.parseDirection(direction));

        HugeGraph g = graph(manager, graphSpace, graph);
        SameNeighborTraverser traverser = new SameNeighborTraverser(g);
        Set<Id> neighbors = traverser.sameNeighbors(sourceId, targetId, dir,
                                                    edgeLabel, maxDegree, limit);

        measure.addIterCount(traverser.vertexIterCounter.get(),
                             traverser.edgeIterCounter.get());

        return manager.serializer(g, measure.measures())
                      .writeList("same_neighbors", neighbors);
    }

    @POST
    @Timed
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String sameNeighbors(@Context GraphManager manager,
                                @PathParam("graphspace") String graphSpace,
                                @PathParam("graph") String graph,
                                Request request) {
        LOG.debug("Graph [{}] get same neighbors among batch, '{}'", graph, request.toString());

        ApiMeasurer measure = new ApiMeasurer();

        Directions dir = Directions.convert(EdgeAPI.parseDirection(request.direction));
        HugeGraph g = graph(manager, graphSpace, graph);
        SameNeighborTraverser traverser = new SameNeighborTraverser(g);

        List<Object> vertexList = request.vertexList;
        E.checkArgument(vertexList.size() >= 2, "vertex_list size can't " +
                                                "be less than 2");

        List<Id> vertexIds = new ArrayList<>();
        for (Object obj : vertexList) {
            vertexIds.add(HugeVertex.getIdValue(obj));
        }

        Set<Id> neighbors = traverser.sameNeighbors(vertexIds, dir, request.labels,
                                                    request.maxDegree, request.limit);
        measure.addIterCount(traverser.vertexIterCounter.get(),
                             traverser.edgeIterCounter.get());

        Iterator<?> iterVertex;
        Set<Id> ids = new HashSet<>(neighbors);
        ids.addAll(vertexIds);
        if (request.withVertex && !ids.isEmpty()) {
            iterVertex = g.vertices(ids.toArray());
        } else {
            iterVertex = ids.iterator();
        }
        return manager.serializer(g, measure.measures())
                      .writeMap(ImmutableMap.of("same_neighbors", neighbors,
                                                "vertices", iterVertex));
    }

    private static class Request {

        @JsonProperty("max_degree")
        public long maxDegree = Long.parseLong(DEFAULT_MAX_DEGREE);
        @JsonProperty("limit")
        public int limit = Integer.parseInt(DEFAULT_ELEMENTS_LIMIT);
        @JsonProperty("vertex_list")
        private List<Object> vertexList;
        @JsonProperty("direction")
        private String direction;
        @JsonProperty("labels")
        private List<String> labels;
        @JsonProperty("with_vertex")
        private boolean withVertex = false;

        @Override
        public String toString() {
            return String.format("SameNeighborsBatchRequest{vertex_list=%s," +
                                 "direction=%s,label=%s,max_degree=%d," +
                                 "limit=%d,with_vertex=%s",
                                 this.vertexList, this.direction, this.labels,
                                 this.maxDegree, this.limit, this.withVertex);
        }
    }
}
