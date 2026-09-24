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

import java.util.Iterator;

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.api.graph.VertexAPI;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.core.GraphManager;
import org.apache.hugegraph.traversal.algorithm.EdgeExistenceTraverser;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Log;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.slf4j.Logger;

import com.codahale.metrics.annotation.Timed;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.inject.Singleton;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Context;

@Path("graphspaces/{graphspace}/graphs/{graph}/traversers/edgeexist")
@Singleton
@Tag(name = "EdgeExistenceAPI")
public class EdgeExistenceAPI extends TraverserAPI {

    private static final Logger LOG = Log.logger(EdgeExistenceAPI.class);
    private static final String DEFAULT_EMPTY = "";

    @GET
    @Timed
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @Operation(summary = "get edges from 'source' to 'target' vertex")
    public String get(@Context GraphManager manager,
                      @PathParam("graphspace") String graphSpace,
                      @PathParam("graph") String graph,
                      @QueryParam("source") String source,
                      @QueryParam("target") String target,
                      @QueryParam("label") String edgeLabel,
                      @QueryParam("sort_values")
                      @DefaultValue(DEFAULT_EMPTY) String sortValues,
                      @QueryParam("limit")
                      @DefaultValue(DEFAULT_LIMIT) long limit) {
        LOG.debug("Graph [{}] get edgeExistence with " +
                  "source '{}', target '{}', edgeLabel '{}', sortValue '{}', limit '{}'",
                  graph, source, target, edgeLabel, sortValues, limit);

        E.checkArgumentNotNull(source, "The source can't be null");
        E.checkArgumentNotNull(target, "The target can't be null");

        Id sourceId = VertexAPI.checkAndParseVertexId(source);
        Id targetId = VertexAPI.checkAndParseVertexId(target);
        HugeGraph hugegraph = graph(manager, graphSpace, graph);
        EdgeExistenceTraverser traverser = new EdgeExistenceTraverser(hugegraph);
        Iterator<Edge> edges = traverser.queryEdgeExistence(sourceId, targetId, edgeLabel,
                                                            sortValues, limit);

        return manager.serializer(hugegraph).writeEdges(edges, false);
    }
}
