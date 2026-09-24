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
import static org.apache.hugegraph.traversal.algorithm.HugeTraverser.DEFAULT_LIMIT;
import static org.apache.hugegraph.traversal.algorithm.HugeTraverser.DEFAULT_MAX_DEGREE;

import java.util.Map;

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.api.graph.EdgeAPI;
import org.apache.hugegraph.api.graph.VertexAPI;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.core.GraphManager;
import org.apache.hugegraph.structure.HugeVertex;
import org.apache.hugegraph.traversal.algorithm.JaccardSimilarTraverser;
import org.apache.hugegraph.traversal.algorithm.steps.EdgeStep;
import org.apache.hugegraph.type.define.Directions;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

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

@Path("graphspaces/{graphspace}/graphs/{graph}/traversers/jaccardsimilarity")
@Singleton
@Tag(name = "JaccardSimilarityAPI")
public class JaccardSimilarityAPI extends TraverserAPI {

    private static final Logger LOG = Log.logger(JaccardSimilarityAPI.class);

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
                      @DefaultValue(DEFAULT_MAX_DEGREE) long maxDegree) {
        LOG.debug("Graph [{}] get jaccard similarity between '{}' and '{}' " +
                  "with direction {}, edge label {} and max degree '{}'",
                  graph, vertex, other, direction, edgeLabel, maxDegree);

        ApiMeasurer measure = new ApiMeasurer();

        Id sourceId = VertexAPI.checkAndParseVertexId(vertex);
        Id targetId = VertexAPI.checkAndParseVertexId(other);
        Directions dir = Directions.convert(EdgeAPI.parseDirection(direction));

        HugeGraph g = graph(manager, graphSpace, graph);
        double similarity;
        try (JaccardSimilarTraverser traverser =
                     new JaccardSimilarTraverser(g)) {
            similarity = traverser.jaccardSimilarity(sourceId, targetId, dir,
                                                     edgeLabel, maxDegree);
            measure.addIterCount(traverser.vertexIterCounter.get(),
                                 traverser.edgeIterCounter.get());
        }

        return manager.serializer(g, measure.measures())
                      .writeMap(ImmutableMap.of("jaccard_similarity", similarity));
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
        E.checkArgumentNotNull(request.vertex,
                               "The source vertex of request can't be null");
        E.checkArgument(request.step != null,
                        "The steps of request can't be null");
        E.checkArgument(request.top >= 0,
                        "The top must be >= 0, but got: %s", request.top);

        LOG.debug("Graph [{}] get jaccard similars from source vertex '{}', " +
                  "with step '{}', top '{}' and capacity '{}'",
                  graph, request.vertex, request.step,
                  request.top, request.capacity);

        ApiMeasurer measure = new ApiMeasurer();

        HugeGraph g = graph(manager, graphSpace, graph);
        Id sourceId = HugeVertex.getIdValue(request.vertex);

        EdgeStep step = step(g, request.step);

        Map<Id, Double> results;
        try (JaccardSimilarTraverser traverser =
                     new JaccardSimilarTraverser(g)) {
            results = traverser.jaccardSimilars(sourceId, step, request.top,
                                                request.capacity);
            measure.addIterCount(traverser.vertexIterCounter.get(),
                                 traverser.edgeIterCounter.get());
        }
        return manager.serializer(g, measure.measures())
                      .writeMap(ImmutableMap.of("jaccard_similarity", results));
    }

    private static class Request {

        @JsonProperty("vertex")
        public Object vertex;
        @JsonProperty("step")
        public TraverserAPI.Step step;
        @JsonProperty("top")
        public int top = Integer.parseInt(DEFAULT_LIMIT);
        @JsonProperty("capacity")
        public long capacity = Long.parseLong(DEFAULT_CAPACITY);

        @Override
        public String toString() {
            return String.format("Request{vertex=%s,step=%s,top=%s," +
                                 "capacity=%s}", this.vertex, this.step,
                                 this.top, this.capacity);
        }
    }
}
