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

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.api.API;
import org.apache.hugegraph.api.graph.EdgeAPI;
import org.apache.hugegraph.api.graph.VertexAPI;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.core.GraphManager;
import org.apache.hugegraph.traversal.algorithm.PredictionTraverser;
import org.apache.hugegraph.type.define.Directions;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.JsonUtil;

import com.codahale.metrics.annotation.Timed;
import com.google.common.collect.ImmutableMap;

import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.inject.Singleton;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Context;

/**
 * ResourceAllocation is one of the prediction algorithms in graph, you can get
 * more info and definition in:
 * https://arxiv.org/pdf/0901.0553.pdf
 */
@Path("graphspaces/{graphspace}/graphs/{graph}/traversers/resourceallocation")
@Singleton
@Tag(name = "ResourceAllocationAPI")
public class ResourceAllocationAPI extends API {

    @GET
    @Timed
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String create(@Context GraphManager manager,
                         @PathParam("graphspace") String graphSpace,
                         @PathParam("graph") String graph,
                         @QueryParam("vertex") String current,
                         @QueryParam("other") String other,
                         @QueryParam("direction") String direction,
                         @QueryParam("label") String edgeLabel,
                         @QueryParam("max_degree")
                         @DefaultValue(DEFAULT_MAX_DEGREE) long maxDegree,
                         @QueryParam("limit")
                         @DefaultValue(DEFAULT_ELEMENTS_LIMIT) int limit) {
        LOG.debug("Graph [{}] get resource allocation between '{}' and '{}' " +
                  "with direction {}, edge label {}, max degree '{}' and " +
                  "limit '{}'", graph, current, other, direction, edgeLabel,
                  maxDegree, limit);

        Id sourceId = VertexAPI.checkAndParseVertexId(current);
        Id targetId = VertexAPI.checkAndParseVertexId(other);
        E.checkArgument(!current.equals(other),
                        "The source and target vertex id can't be same");
        Directions dir = Directions.convert(EdgeAPI.parseDirection(direction));

        HugeGraph g = graph(manager, graphSpace, graph);
        try (PredictionTraverser traverser = new PredictionTraverser(g)) {
            double score = traverser.resourceAllocation(sourceId, targetId, dir,
                                                        edgeLabel, maxDegree,
                                                        limit);
            return JsonUtil.toJson(ImmutableMap.of("resource_allocation", score));
        }
    }
}
