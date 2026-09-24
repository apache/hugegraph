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
import static org.apache.hugegraph.traversal.algorithm.HugeTraverser.DEFAULT_MAX_DEGREE;
import static org.apache.hugegraph.traversal.algorithm.HugeTraverser.DEFAULT_MAX_DEPTH;
import static org.apache.hugegraph.traversal.algorithm.HugeTraverser.DEFAULT_PATHS_LIMIT;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.api.API;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.core.GraphManager;
import org.apache.hugegraph.structure.HugeVertex;
import org.apache.hugegraph.traversal.algorithm.NeighborRankTraverser;
import org.apache.hugegraph.type.define.Directions;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.inject.Singleton;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Context;

@Path("graphspaces/{graphspace}/graphs/{graph}/traversers/neighborrank")
@Singleton
@Tag(name = "NeighborRankAPI")
public class NeighborRankAPI extends API {

    private static final Logger LOG = Log.logger(NeighborRankAPI.class);

    @POST
    @Timed
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String neighborRank(@Context GraphManager manager,
                               @PathParam("graphspace") String graphSpace,
                               @PathParam("graph") String graph,
                               RankRequest request) {
        E.checkArgumentNotNull(request, "The rank request body can't be null");
        E.checkArgumentNotNull(request.source,
                               "The source of rank request can't be null");
        E.checkArgument(request.steps != null && !request.steps.isEmpty(),
                        "The steps of rank request can't be empty");
        E.checkArgument(request.steps.size() <= DEFAULT_MAX_DEPTH,
                        "The steps length of rank request can't exceed %s",
                        DEFAULT_MAX_DEPTH);
        E.checkArgument(request.alpha > 0 && request.alpha <= 1.0,
                        "The alpha of rank request must be in range (0, 1], " +
                        "but got '%s'", request.alpha);

        LOG.debug("Graph [{}] get neighbor rank from '{}' with steps '{}', " +
                  "alpha '{}' and capacity '{}'", graph, request.source,
                  request.steps, request.alpha, request.capacity);

        Id sourceId = HugeVertex.getIdValue(request.source);
        HugeGraph g = graph(manager, graphSpace, graph);

        List<NeighborRankTraverser.Step> steps = steps(g, request);
        NeighborRankTraverser traverser;
        traverser = new NeighborRankTraverser(g, request.alpha,
                                              request.capacity);
        List<Map<Id, Double>> ranks = traverser.neighborRank(sourceId, steps);
        return manager.serializer().writeList("ranks", ranks);
    }

    private static List<NeighborRankTraverser.Step> steps(HugeGraph graph,
                                                          RankRequest req) {
        List<NeighborRankTraverser.Step> steps = new ArrayList<>();
        for (Step step : req.steps) {
            steps.add(step.jsonToStep(graph));
        }
        return steps;
    }

    private static class RankRequest {

        @JsonProperty("source")
        private Object source;
        @JsonProperty("steps")
        private List<Step> steps;
        @JsonProperty("alpha")
        private double alpha;
        @JsonProperty("capacity")
        public long capacity = Long.parseLong(DEFAULT_CAPACITY);

        @Override
        public String toString() {
            return String.format("RankRequest{source=%s,steps=%s,alpha=%s," +
                                 "capacity=%s}", this.source, this.steps,
                                 this.alpha, this.capacity);
        }
    }

    private static class Step {

        @JsonProperty("direction")
        public Directions direction;
        @JsonProperty("labels")
        public List<String> labels;
        @JsonAlias("degree")
        @JsonProperty("max_degree")
        public long maxDegree = Long.parseLong(DEFAULT_MAX_DEGREE);
        @JsonProperty("skip_degree")
        public long skipDegree = 0L;
        @JsonProperty("top")
        public int top = Integer.parseInt(DEFAULT_PATHS_LIMIT);

        public static final int DEFAULT_CAPACITY_PER_LAYER = 100000;

        @Override
        public String toString() {
            return String.format("Step{direction=%s,labels=%s,maxDegree=%s," +
                                 "top=%s}", this.direction, this.labels,
                                 this.maxDegree, this.top);
        }

        private NeighborRankTraverser.Step jsonToStep(HugeGraph g) {
            return new NeighborRankTraverser.Step(g, this.direction,
                                                  this.labels,
                                                  this.maxDegree,
                                                  this.skipDegree,
                                                  this.top,
                                                  DEFAULT_CAPACITY_PER_LAYER);
        }
    }
}
