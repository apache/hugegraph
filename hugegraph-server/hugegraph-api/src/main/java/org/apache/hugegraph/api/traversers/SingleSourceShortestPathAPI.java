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

import java.util.Iterator;
import java.util.Set;

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.api.API;
import org.apache.hugegraph.api.graph.EdgeAPI;
import org.apache.hugegraph.api.graph.VertexAPI;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.core.GraphManager;
import org.apache.hugegraph.traversal.algorithm.HugeTraverser;
import org.apache.hugegraph.traversal.algorithm.SingleSourceShortestPathTraverser;
import org.apache.hugegraph.type.define.Directions;
import org.apache.hugegraph.util.Log;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.slf4j.Logger;

import com.codahale.metrics.annotation.Timed;

import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.inject.Singleton;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Context;

@Path("graphspaces/{graphspace}/graphs/{graph}/traversers/singlesourceshortestpath")
@Singleton
@Tag(name = "SingleSourceShortestPathAPI")
public class SingleSourceShortestPathAPI extends API {

    private static final Logger LOG = Log.logger(SingleSourceShortestPathAPI.class);

    @GET
    @Timed
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String get(@Context GraphManager manager,
                      @PathParam("graphspace") String graphSpace,
                      @PathParam("graph") String graph,
                      @QueryParam("source") String source,
                      @QueryParam("direction") String direction,
                      @QueryParam("label") String edgeLabel,
                      @QueryParam("weight") String weight,
                      @QueryParam("max_degree")
                      @DefaultValue(DEFAULT_MAX_DEGREE) long maxDegree,
                      @QueryParam("skip_degree")
                      @DefaultValue("0") long skipDegree,
                      @QueryParam("with_vertex")
                      @DefaultValue("false") boolean withVertex,
                      @QueryParam("with_edge")
                      @DefaultValue("false") boolean withEdge,
                      @QueryParam("capacity")
                      @DefaultValue(DEFAULT_CAPACITY) long capacity,
                      @QueryParam("limit")
                      @DefaultValue(DEFAULT_PATHS_LIMIT) int limit) {
        LOG.debug("Graph [{}] get single source shortest path from '{}' " +
                  "with direction {}, edge label {}, weight property {}, " +
                  "max degree '{}', capacity '{}', limit '{}', " +
                  "with_vertex '{}' and with_edge '{}'",
                  graph, source, direction, edgeLabel,
                  weight, maxDegree, capacity, limit, withVertex, withEdge);

        ApiMeasurer measure = new ApiMeasurer();

        Id sourceId = VertexAPI.checkAndParseVertexId(source);
        Directions dir = Directions.convert(EdgeAPI.parseDirection(direction));

        HugeGraph g = graph(manager, graphSpace, graph);
        SingleSourceShortestPathTraverser traverser =
                new SingleSourceShortestPathTraverser(g);
        SingleSourceShortestPathTraverser.WeightedPaths paths =
                traverser.singleSourceShortestPaths(
                        sourceId, dir, edgeLabel, weight,
                        maxDegree, skipDegree, capacity, limit);
        measure.addIterCount(traverser.vertexIterCounter.get(),
                             traverser.edgeIterCounter.get());

        Iterator<?> iterVertex;
        Set<Id> vertexIds = paths.vertices();
        if (withVertex && !vertexIds.isEmpty()) {
            iterVertex = g.vertices(vertexIds.toArray());
            measure.addIterCount(vertexIds.size(), 0L);
        } else {
            iterVertex = vertexIds.iterator();
        }

        Iterator<?> iterEdge;
        Set<Edge> edges = paths.getEdges();
        if (withEdge && !edges.isEmpty()) {
            iterEdge = edges.iterator();
        } else {
            iterEdge = HugeTraverser.EdgeRecord.getEdgeIds(edges).iterator();
        }

        return manager.serializer(g, measure.measures())
                      .writeWeightedPaths(paths, iterVertex, iterEdge);
    }
}
