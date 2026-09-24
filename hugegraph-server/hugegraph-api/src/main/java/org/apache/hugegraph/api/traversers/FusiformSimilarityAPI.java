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
import static org.apache.hugegraph.traversal.algorithm.HugeTraverser.DEFAULT_PATHS_LIMIT;
import static org.apache.hugegraph.traversal.algorithm.HugeTraverser.NO_LIMIT;

import java.util.Iterator;
import java.util.Set;

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.api.API;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.core.GraphManager;
import org.apache.hugegraph.traversal.algorithm.FusiformSimilarityTraverser;
import org.apache.hugegraph.traversal.algorithm.FusiformSimilarityTraverser.SimilarsMap;
import org.apache.hugegraph.type.define.Directions;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Log;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;
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

@Path("graphspaces/{graphspace}/graphs/{graph}/traversers/fusiformsimilarity")
@Singleton
@Tag(name = "FusiformSimilarityAPI")
public class FusiformSimilarityAPI extends API {

    private static final Logger LOG = Log.logger(FusiformSimilarityAPI.class);

    @POST
    @Timed
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String post(@Context GraphManager manager,
                       @PathParam("graphspace") String graphSpace,
                       @PathParam("graph") String graph,
                       FusiformSimilarityRequest request) {
        E.checkArgumentNotNull(request, "The fusiform similarity " +
                                        "request body can't be null");
        E.checkArgumentNotNull(request.sources,
                               "The sources of fusiform similarity " +
                               "request can't be null");
        if (request.direction == null) {
            request.direction = Directions.BOTH;
        }
        E.checkArgument(request.minNeighbors > 0,
                        "The min neighbor count must be > 0, but got: %s",
                        request.minNeighbors);
        E.checkArgument(request.maxDegree > 0L || request.maxDegree == NO_LIMIT,
                        "The max degree of request must be > 0 or == -1, " +
                        "but got: %s", request.maxDegree);
        E.checkArgument(request.alpha > 0 && request.alpha <= 1.0,
                        "The alpha of request must be in range (0, 1], " +
                        "but got '%s'", request.alpha);
        E.checkArgument(request.minSimilars >= 1,
                        "The min similar count of request must be >= 1, " +
                        "but got: %s", request.minSimilars);
        E.checkArgument(request.top >= 0,
                        "The top must be >= 0, but got: %s", request.top);

        LOG.debug("Graph [{}] get fusiform similars from '{}' with " +
                  "direction '{}', edge label '{}', min neighbor count '{}', " +
                  "alpha '{}', min similar count '{}', group property '{}' " +
                  "and min group count '{}'",
                  graph, request.sources, request.direction, request.label,
                  request.minNeighbors, request.alpha, request.minSimilars,
                  request.groupProperty, request.minGroups);

        ApiMeasurer measure = new ApiMeasurer();
        HugeGraph g = graph(manager, graphSpace, graph);
        Iterator<Vertex> sources = request.sources.vertices(g);
        E.checkArgument(sources != null && sources.hasNext(),
                        "The source vertices can't be empty");

        FusiformSimilarityTraverser traverser = new FusiformSimilarityTraverser(g);
        SimilarsMap result = traverser.fusiformSimilarity(
                sources, request.direction, request.label,
                request.minNeighbors, request.alpha,
                request.minSimilars, request.top,
                request.groupProperty, request.minGroups,
                request.maxDegree, request.capacity,
                request.limit, request.withIntermediary);

        CloseableIterator.closeIterator(sources);

        measure.addIterCount(traverser.vertexIterCounter.get(),
                             traverser.edgeIterCounter.get());

        Iterator<?> iterVertex;
        Set<Id> vertexIds = result.vertices();
        if (request.withVertex && !vertexIds.isEmpty()) {
            iterVertex = g.vertices(vertexIds.toArray());
            measure.addIterCount(vertexIds.size(), 0);
        } else {
            iterVertex = vertexIds.iterator();
        }

        return manager.serializer(g, measure.measures())
                      .writeSimilars(result, iterVertex);
    }

    private static class FusiformSimilarityRequest {

        @JsonProperty("sources")
        public Vertices sources;
        @JsonProperty("label")
        public String label;
        @JsonProperty("direction")
        public Directions direction;
        @JsonProperty("min_neighbors")
        public int minNeighbors;
        @JsonProperty("alpha")
        public double alpha;
        @JsonProperty("min_similars")
        public int minSimilars = 1;
        @JsonProperty("top")
        public int top;
        @JsonProperty("group_property")
        public String groupProperty;
        @JsonProperty("min_groups")
        public int minGroups;
        @JsonProperty("max_degree")
        public long maxDegree = Long.parseLong(DEFAULT_MAX_DEGREE);
        @JsonProperty("capacity")
        public long capacity = Long.parseLong(DEFAULT_CAPACITY);
        @JsonProperty("limit")
        public int limit = Integer.parseInt(DEFAULT_PATHS_LIMIT);
        @JsonProperty("with_intermediary")
        public boolean withIntermediary = false;
        @JsonProperty("with_vertex")
        public boolean withVertex = false;

        @Override
        public String toString() {
            return String.format("FusiformSimilarityRequest{sources=%s," +
                                 "label=%s,direction=%s,minNeighbors=%s," +
                                 "alpha=%s,minSimilars=%s,top=%s," +
                                 "groupProperty=%s,minGroups=%s," +
                                 "maxDegree=%s,capacity=%s,limit=%s," +
                                 "withIntermediary=%s,withVertex=%s}",
                                 this.sources, this.label, this.direction,
                                 this.minNeighbors, this.alpha,
                                 this.minSimilars, this.top,
                                 this.groupProperty, this.minGroups,
                                 this.maxDegree, this.capacity, this.limit,
                                 this.withIntermediary, this.withVertex);
        }
    }
}
