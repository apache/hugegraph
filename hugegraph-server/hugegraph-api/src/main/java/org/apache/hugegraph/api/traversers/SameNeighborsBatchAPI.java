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
import java.util.List;
import java.util.Set;

import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.api.API;
import org.apache.hugegraph.api.graph.EdgeAPI;
import org.apache.hugegraph.core.GraphManager;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.server.RestServer;
import org.apache.hugegraph.structure.HugeVertex;
import org.apache.hugegraph.traversal.algorithm.SameNeighborTraverser;
import org.apache.hugegraph.type.define.Directions;
import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.inject.Singleton;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Context;

@Path("graphspaces/{graphspace}/graphs/{graph}/traversers/sameneighborsbatch")
@Singleton
@Tag(name = "SameNeighborsBatchAPI")
public class SameNeighborsBatchAPI extends API {

    private static final Logger LOG = Log.logger(RestServer.class);

    @POST
    @Timed
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String sameNeighborsBatch(@Context GraphManager manager,
                                     @PathParam("graphspace") String graphSpace,
                                     @PathParam("graph") String graph,
                                     Request req) {
        LOG.debug("Graph [{}] get same neighbors batch, '{}'", graph, req.toString());

        Directions dir = Directions.convert(EdgeAPI.parseDirection(req.direction));

        ApiMeasurer measure = new ApiMeasurer();
        HugeGraph g = graph(manager, graph);
        SameNeighborTraverser traverser = new SameNeighborTraverser(g);

        List<Set<Id>> result = new ArrayList<>();
        for (List<Object> vertexPair : req.vertexList) {
            E.checkArgument(vertexPair.size() == 2,
                    "vertex_list size must be 2");
            Id sourceId = HugeVertex.getIdValue(vertexPair.get(0));
            Id targetId = HugeVertex.getIdValue(vertexPair.get(1));
            Set<Id> neighbors = traverser.sameNeighbors(sourceId, targetId, dir,
                    req.label, req.maxDegree, req.limit);
            result.add(neighbors);
        }
        measure.addIterCount(traverser.vertexIterCounter.get(),
                traverser.edgeIterCounter.get());

        return manager.serializer(g, measure.measures()).writeList("same_neighbors", result);
    }

    private static class Request {
        @JsonProperty("vertex_list")
        private List<List<Object>> vertexList;
        @JsonProperty("direction")
        private String direction;
        @JsonProperty("label")
        private String label;
        @JsonProperty("max_degree")
        public long maxDegree = Long.parseLong(DEFAULT_MAX_DEGREE);
        @JsonProperty("limit")
        public int limit = Integer.parseInt(DEFAULT_ELEMENTS_LIMIT);

        @Override
        public String toString() {
            ObjectMapper om = new ObjectMapper();
            String vertexStr = "";
            try {
                vertexStr = om.writeValueAsString(this.vertexList);
            } catch (Exception e) {
            }
            return String.format("SameNeighborsBatchRequest{vertex=%s,direction=%s,label=%s," +
                            "max_degree=%d,limit=%d", vertexStr, this.direction,
                    this.label, this.maxDegree, this.limit);
        }
    }
}
