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

import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.api.API;
import org.apache.hugegraph.api.graph.EdgeAPI;
import org.apache.hugegraph.api.graph.VertexAPI;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.query.QueryResults;
import org.apache.hugegraph.core.GraphManager;
import org.apache.hugegraph.traversal.algorithm.HugeTraverser;
import org.apache.hugegraph.traversal.algorithm.SingleSourceShortestPathTraverser;
import org.apache.hugegraph.type.define.Directions;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Log;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.slf4j.Logger;

import com.codahale.metrics.annotation.Timed;

import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.inject.Singleton;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Context;

@Path("graphspaces/{graphspace}/graphs/{graph}/traversers/weightedshortestpath")
@Singleton
@Tag(name = "WeightedShortestPathAPI")
public class WeightedShortestPathAPI extends API {

    private static final Logger LOG = Log.logger(WeightedShortestPathAPI.class);

    @GET
    @Timed
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String get(@Context GraphManager manager,
                      @Parameter(description = "The graph space name")
                      @PathParam("graphspace") String graphSpace,
                      @Parameter(description = "The graph name")
                      @PathParam("graph") String graph,
                      @Parameter(description = "The source vertex ID")
                      @QueryParam("source") String source,
                      @Parameter(description = "The target vertex ID")
                      @QueryParam("target") String target,
                      @Parameter(description = "The direction of traversal")
                      @QueryParam("direction") String direction,
                      @Parameter(description = "The edge label to traverse")
                      @QueryParam("label") String edgeLabel,
                      @Parameter(description = "The weight property name")
                      @QueryParam("weight") String weight,
                      @Parameter(description = "The maximum degree of vertices")
                      @QueryParam("max_degree")
                      @DefaultValue(DEFAULT_MAX_DEGREE) long maxDegree,
                      @Parameter(description = "The degree to skip") @QueryParam("skip_degree")
                      @DefaultValue("0") long skipDegree,
                      @Parameter(description = "Whether to include vertex details")
                      @QueryParam("with_vertex")
                      @DefaultValue("false") boolean withVertex,
                      @Parameter(description = "Whether to include edge details")
                      @QueryParam("with_edge")
                      @DefaultValue("false") boolean withEdge,
                      @Parameter(description = "The capacity of the traversal")
                      @QueryParam("capacity")
                      @DefaultValue(DEFAULT_CAPACITY) long capacity) {
        LOG.debug("Graph [{}] get weighted shortest path between '{}' and " +
                  "'{}' with direction {}, edge label {}, weight property {}, " +
                  "max degree '{}', skip degree '{}', capacity '{}', " +
                  "with_vertex '{}' and with_edge '{}'",
                  graph, source, target, direction, edgeLabel, weight,
                  maxDegree, skipDegree, capacity, withVertex, withEdge);

        ApiMeasurer measure = new ApiMeasurer();
        Id sourceId = VertexAPI.checkAndParseVertexId(source);
        Id targetId = VertexAPI.checkAndParseVertexId(target);
        Directions dir = Directions.convert(EdgeAPI.parseDirection(direction));
        E.checkArgumentNotNull(weight, "The weight property can't be null");

        HugeGraph g = graph(manager, graphSpace, graph);
        SingleSourceShortestPathTraverser traverser =
                new SingleSourceShortestPathTraverser(g);

        SingleSourceShortestPathTraverser.NodeWithWeight node =
                traverser.weightedShortestPath(sourceId, targetId,
                                               dir, edgeLabel, weight,
                                               maxDegree, skipDegree, capacity);
        measure.addIterCount(traverser.vertexIterCounter.get(),
                             traverser.edgeIterCounter.get());

        if (node == null) {
            return manager.serializer(g, measure.measures())
                          .writeWeightedPath(null,
                                             QueryResults.emptyIterator(),
                                             QueryResults.emptyIterator());
        }

        Iterator<?> iterVertex;
        List<Id> vertexIds = node.node().path();
        if (withVertex && !vertexIds.isEmpty()) {
            iterVertex = g.vertices(vertexIds.toArray());
            measure.addIterCount(vertexIds.size(), 0L);
        } else {
            iterVertex = vertexIds.iterator();
        }

        Iterator<?> iterEdge;
        Set<Edge> edges = node.getEdges();
        if (withEdge && !edges.isEmpty()) {
            iterEdge = edges.iterator();
        } else {
            iterEdge = HugeTraverser.EdgeRecord.getEdgeIds(edges).iterator();
        }

        return manager.serializer(g, measure.measures())
                      .writeWeightedPath(node, iterVertex, iterEdge);
    }
}
