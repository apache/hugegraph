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

import static org.apache.hugegraph.traversal.algorithm.HugeTraverser.DEFAULT_LIMIT;
import static org.apache.hugegraph.traversal.algorithm.HugeTraverser.DEFAULT_MAX_DEGREE;
import static org.apache.hugegraph.traversal.algorithm.HugeTraverser.DEFAULT_MAX_DEPTH;
import static org.apache.hugegraph.traversal.algorithm.HugeTraverser.NO_LIMIT;

import java.util.Map;

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.api.API;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.core.GraphManager;
import org.apache.hugegraph.structure.HugeVertex;
import org.apache.hugegraph.traversal.algorithm.HugeTraverser;
import org.apache.hugegraph.traversal.algorithm.PersonalRankTraverser;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.inject.Singleton;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Context;

@Path("graphspaces/{graphspace}/graphs/{graph}/traversers/personalrank")
@Singleton
@Tag(name = "PersonalRankAPI")
public class PersonalRankAPI extends API {

    private static final Logger LOG = Log.logger(PersonalRankAPI.class);

    private static final double DEFAULT_DIFF = 0.0001;
    private static final double DEFAULT_ALPHA = 0.85;
    private static final int DEFAULT_DEPTH = 5;

    @POST
    @Timed
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String personalRank(@Context GraphManager manager,
                               @PathParam("graphspace") String graphSpace,
                               @PathParam("graph") String graph,
                               RankRequest request) {
        E.checkArgumentNotNull(request, "The rank request body can't be null");
        E.checkArgument(request.source != null,
                        "The source vertex id of rank request can't be null");
        E.checkArgument(request.label != null,
                        "The edge label of rank request can't be null");
        E.checkArgument(request.alpha > 0 && request.alpha <= 1.0,
                        "The alpha of rank request must be in range (0, 1], " +
                        "but got '%s'", request.alpha);
        E.checkArgument(request.maxDiff > 0 && request.maxDiff <= 1.0,
                        "The max diff of rank request must be in range " +
                        "(0, 1], but got '%s'", request.maxDiff);
        E.checkArgument(request.maxDegree > 0L || request.maxDegree == NO_LIMIT,
                        "The max degree of rank request must be > 0 " +
                        "or == -1, but got: %s", request.maxDegree);
        E.checkArgument(request.limit > 0L || request.limit == NO_LIMIT,
                        "The limit of rank request must be > 0 or == -1, " +
                        "but got: %s", request.limit);
        E.checkArgument(request.maxDepth > 1L &&
                        request.maxDepth <= DEFAULT_MAX_DEPTH,
                        "The max depth of rank request must be " +
                        "in range (1, %s], but got '%s'",
                        DEFAULT_MAX_DEPTH, request.maxDepth);

        LOG.debug("Graph [{}] get personal rank from '{}' with " +
                  "edge label '{}', alpha '{}', maxDegree '{}', " +
                  "max depth '{}' and sorted '{}'",
                  graph, request.source, request.label, request.alpha,
                  request.maxDegree, request.maxDepth, request.sorted);

        Id sourceId = HugeVertex.getIdValue(request.source);
        HugeGraph g = graph(manager, graphSpace, graph);

        PersonalRankTraverser traverser;
        traverser = new PersonalRankTraverser(g, request.alpha, request.maxDegree,
                                              request.maxDepth);
        Map<Id, Double> ranks = traverser.personalRank(sourceId, request.label,
                                                       request.withLabel);
        ranks = HugeTraverser.topN(ranks, request.sorted, request.limit);
        return manager.serializer().writeMap(ranks);
    }

    private static class RankRequest {

        @JsonProperty("source")
        private Object source;
        @JsonProperty("label")
        private String label;
        @JsonProperty("alpha")
        private double alpha = DEFAULT_ALPHA;
        // TODO: used for future enhancement
        @JsonProperty("max_diff")
        private double maxDiff = DEFAULT_DIFF;
        @JsonProperty("max_degree")
        private long maxDegree = Long.parseLong(DEFAULT_MAX_DEGREE);
        @JsonProperty("limit")
        private int limit = Integer.parseInt(DEFAULT_LIMIT);
        @JsonProperty("max_depth")
        private int maxDepth = DEFAULT_DEPTH;
        @JsonProperty("with_label")
        private PersonalRankTraverser.WithLabel withLabel =
                PersonalRankTraverser.WithLabel.BOTH_LABEL;
        @JsonProperty("sorted")
        private boolean sorted = true;

        @Override
        public String toString() {
            return String.format("RankRequest{source=%s,label=%s,alpha=%s," +
                                 "maxDiff=%s,maxDegree=%s,limit=%s," +
                                 "maxDepth=%s,withLabel=%s,sorted=%s}",
                                 this.source, this.label, this.alpha,
                                 this.maxDiff, this.maxDegree, this.limit,
                                 this.maxDepth, this.withLabel, this.sorted);
        }
    }
}
