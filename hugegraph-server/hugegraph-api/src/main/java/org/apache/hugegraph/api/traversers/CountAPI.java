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

import static org.apache.hugegraph.traversal.algorithm.HugeTraverser.DEFAULT_MAX_DEGREE;
import static org.apache.hugegraph.traversal.algorithm.HugeTraverser.DEFAULT_SKIP_DEGREE;
import static org.apache.hugegraph.traversal.algorithm.HugeTraverser.NO_LIMIT;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.api.API;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.core.GraphManager;
import org.apache.hugegraph.structure.HugeVertex;
import org.apache.hugegraph.traversal.algorithm.CountTraverser;
import org.apache.hugegraph.traversal.algorithm.steps.EdgeStep;
import org.apache.hugegraph.type.define.Directions;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.inject.Singleton;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Context;

@Path("graphspaces/{graphspace}/graphs/{graph}/traversers/count")
@Singleton
@Tag(name = "CountAPI")
public class CountAPI extends API {

    private static final Logger LOG = Log.logger(CountAPI.class);

    @POST
    @Timed
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String post(@Context GraphManager manager,
                       @PathParam("graphspace") String graphSpace,
                       @PathParam("graph") String graph,
                       CountRequest request) {
        LOG.debug("Graph [{}] get count from '{}' with request {}",
                  graph, request);

        E.checkArgumentNotNull(request.source,
                               "The source of request can't be null");
        Id sourceId = HugeVertex.getIdValue(request.source);
        E.checkArgumentNotNull(request.steps != null &&
                               !request.steps.isEmpty(),
                               "The steps of request can't be null or empty");
        E.checkArgumentNotNull(request.dedupSize == NO_LIMIT ||
                               request.dedupSize >= 0L,
                               "The dedup size of request " +
                               "must >= 0 or == -1, but got: '%s'",
                               request.dedupSize);

        HugeGraph g = graph(manager, graphSpace, graph);
        List<EdgeStep> steps = steps(g, request);
        CountTraverser traverser = new CountTraverser(g);
        long count = traverser.count(sourceId, steps, request.containsTraversed,
                                     request.dedupSize);

        return manager.serializer().writeMap(ImmutableMap.of("count", count));
    }

    private static List<EdgeStep> steps(HugeGraph graph, CountRequest request) {
        int stepSize = request.steps.size();
        List<EdgeStep> steps = new ArrayList<>(stepSize);
        for (Step step : request.steps) {
            steps.add(step.jsonToStep(graph));
        }
        return steps;
    }

    private static class CountRequest {

        @JsonProperty("source")
        public Object source;
        @JsonProperty("steps")
        public List<Step> steps;
        @JsonProperty("contains_traversed")
        public boolean containsTraversed = false;
        @JsonProperty("dedup_size")
        public long dedupSize = 1000000L;

        @Override
        public String toString() {
            return String.format("CountRequest{source=%s,steps=%s," +
                                 "containsTraversed=%s,dedupSize=%s}",
                                 this.source, this.steps,
                                 this.containsTraversed, this.dedupSize);
        }
    }

    private static class Step {

        @JsonProperty("direction")
        public Directions direction = Directions.BOTH;
        @JsonProperty("labels")
        public List<String> labels;
        @JsonProperty("properties")
        public Map<String, Object> properties;
        @JsonAlias("degree")
        @JsonProperty("max_degree")
        public long maxDegree = Long.parseLong(DEFAULT_MAX_DEGREE);
        @JsonProperty("skip_degree")
        public long skipDegree = Long.parseLong(DEFAULT_SKIP_DEGREE);

        @Override
        public String toString() {
            return String.format("Step{direction=%s,labels=%s,properties=%s" +
                                 "maxDegree=%s,skipDegree=%s}",
                                 this.direction, this.labels, this.properties,
                                 this.maxDegree, this.skipDegree);
        }

        private EdgeStep jsonToStep(HugeGraph graph) {
            return new EdgeStep(graph, this.direction, this.labels,
                                this.properties, this.maxDegree,
                                this.skipDegree);
        }
    }
}
