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

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.api.API;
import org.apache.hugegraph.api.graph.EdgeAPI;
import org.apache.hugegraph.core.GraphManager;
import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.structure.HugeVertex;
import org.apache.hugegraph.traversal.algorithm.HugeTraverser;
import org.apache.hugegraph.traversal.algorithm.SameNeighborTraverser;
import org.apache.hugegraph.type.define.Directions;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Log;
import org.apache.hugegraph.util.TinkerPopUtil;
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

@Path("graphspaces/{graphspace}/graphs/{graph}/traversers/sameneighborsbatch")
@Singleton
@Tag(name = "SameNeighborsBatchAPI")
public class SameNeighborsBatchAPI extends API {

    private static final Logger LOG = Log.logger(SameNeighborsBatchAPI.class);

    @POST
    @Timed
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String post(@Context GraphManager manager,
                       @PathParam("graphspace") String graphSpace,
                       @PathParam("graph") String graph,
                       Request request) {
        E.checkArgumentNotNull(request, "The request body can't be null");
        E.checkArgumentNotNull(request.vertexList, "The vertex_list can't be null");
        E.checkArgument(!request.vertexList.isEmpty(), "The vertex_list can't be empty");
        HugeTraverser.checkDegree(request.maxDegree);
        HugeTraverser.checkLimit(request.limit);
        E.checkArgument(request.limit <= Integer.MAX_VALUE,
                        "The limit must be <= %s, but got: %s",
                        Integer.MAX_VALUE, request.limit);
        int limit = Math.toIntExact(request.limit);
        Directions dir = TinkerPopUtil.direction(EdgeAPI.parseDirection(request.direction));

        // Validate all pairs before accessing the graph or starting traversal.
        List<List<Id>> pairs = new ArrayList<>(request.vertexList.size());
        for (List<Object> pair : request.vertexList) {
            E.checkArgumentNotNull(pair, "The vertex pair can't be null");
            E.checkArgument(pair.size() == 2,
                            "Each vertex pair must contain exactly 2 ids");
            List<Id> ids = new ArrayList<>(2);
            for (Object value : pair) {
                E.checkArgumentNotNull(value, "The vertex id can't be null");
                if (value instanceof Number) {
                    E.checkArgument(!(value instanceof Float || value instanceof Double),
                                    "Numeric vertex ids must be whole numbers in the long range");
                    try {
                        new BigDecimal(value.toString()).longValueExact();
                    } catch (ArithmeticException | NumberFormatException e) {
                        throw new IllegalArgumentException(
                                "Numeric vertex ids must be whole numbers in the long range", e);
                    }
                }
                try {
                    ids.add(HugeVertex.getIdValue(value));
                } catch (UnsupportedOperationException e) {
                    throw new IllegalArgumentException(
                            "The vertex id must be formatted as Number/String/UUID", e);
                }
            }
            pairs.add(ids);
        }

        LOG.debug("Graph [{}] get same neighbors for vertex pairs '{}'", graph, request);
        ApiMeasurer measure = new ApiMeasurer();
        HugeGraph g = graph(manager, graphSpace, graph);
        SameNeighborTraverser traverser = new SameNeighborTraverser(g);
        List<Set<Id>> results = new ArrayList<>(pairs.size());
        for (List<Id> pair : pairs) {
            results.add(traverser.sameNeighbors(pair.get(0), pair.get(1), dir,
                                                request.label, request.maxDegree, limit));
        }
        // The traverser counters accumulate across pairs; add them only once.
        measure.addIterCount(traverser.vertexIterCounter.get(),
                             traverser.edgeIterCounter.get());
        return manager.serializer(g, measure.measures())
                      .writeList("same_neighbors", results);
    }

    private static class Request {

        @JsonProperty("vertex_list")
        public List<List<Object>> vertexList;
        @JsonProperty("direction")
        public String direction;
        @JsonProperty("label")
        public String label;
        @JsonProperty("max_degree")
        public long maxDegree = Long.parseLong(DEFAULT_MAX_DEGREE);
        @JsonProperty("limit")
        public long limit = Long.parseLong(DEFAULT_ELEMENTS_LIMIT);

        @Override
        public String toString() {
            return String.format("Request{vertex_list=%s,direction=%s,label=%s," +
                                 "max_degree=%s,limit=%s}", this.vertexList,
                                 this.direction, this.label, this.maxDegree, this.limit);
        }
    }
}
