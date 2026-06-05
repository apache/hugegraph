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

package org.apache.hugegraph.api.job;

import java.util.Map;

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.api.API;
import org.apache.hugegraph.api.filter.RedirectFilter;
import org.apache.hugegraph.api.filter.StatusFilter.Status;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.core.GraphManager;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

import com.codahale.metrics.annotation.Timed;
import com.google.common.collect.ImmutableMap;

import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.annotation.security.RolesAllowed;
import jakarta.inject.Singleton;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Context;

@Path("graphspaces/{graphspace}/graphs/{graph}/jobs/rebuild")
@Singleton
@Tag(name = "RebuildAPI")
public class RebuildAPI extends API {

    private static final Logger LOG = Log.logger(RebuildAPI.class);

    @PUT
    @Timed
    @Path("vertexlabels/{name}")
    @Status(Status.ACCEPTED)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"space", "$graphspace=$graphspace $owner=$graph " +
                            "$action=index_label_write"})
    @RedirectFilter.RedirectMasterRole
    public Map<String, Id> vertexLabelRebuild(@Context GraphManager manager,
                                              @Parameter(description = "The graphspace name")
                                              @PathParam("graphspace")
                                              String graphSpace,
                                              @Parameter(description = "The graph name")
                                              @PathParam("graph") String graph,
                                              @Parameter(description = "The vertex label to rebuild")
                                              @PathParam("name") String name) {
        LOG.debug("Graph [{}] rebuild vertex label: {}", graph, name);

        HugeGraph g = graph(manager, graphSpace, graph);
        return ImmutableMap.of("task_id",
                               g.schema().vertexLabel(name).rebuildIndex());
    }

    @PUT
    @Timed
    @Path("edgelabels/{name}")
    @Status(Status.ACCEPTED)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"space", "$graphspace=$graphspace $owner=$graph " +
                            "$action=index_label_write"})
    public Map<String, Id> edgeLabelRebuild(@Context GraphManager manager,
                                            @Parameter(description = "The graphspace name")
                                            @PathParam("graphspace")
                                            String graphSpace,
                                            @Parameter(description = "The graph name")
                                            @PathParam("graph") String graph,
                                            @Parameter(description = "The edge label name to rebuild")
                                            @PathParam("name") String name) {
        LOG.debug("Graph [{}] rebuild edge label: {}", graph, name);

        HugeGraph g = graph(manager, graphSpace, graph);
        return ImmutableMap.of("task_id",
                               g.schema().edgeLabel(name).rebuildIndex());
    }

    @PUT
    @Timed
    @Path("indexlabels/{name}")
    @Status(Status.ACCEPTED)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"space", "$graphspace=$graphspace $owner=$graph " +
                            "$action=index_label_write"})
    @RedirectFilter.RedirectMasterRole
    public Map<String, Id> indexLabelRebuild(@Context GraphManager manager,
                                             @Parameter(description = "The graphspace name")
                                             @PathParam("graphspace")
                                             String graphSpace,
                                             @Parameter(description = "The graph name")
                                             @PathParam("graph") String graph,
                                             @Parameter(description = "The index label name to rebuild")
                                             @PathParam("name") String name) {
        LOG.debug("Graph [{}] rebuild index label: {}", graph, name);

        HugeGraph g = graph(manager, graphSpace, graph);
        return ImmutableMap.of("task_id",
                               g.schema().indexLabel(name).rebuild());
    }
}
