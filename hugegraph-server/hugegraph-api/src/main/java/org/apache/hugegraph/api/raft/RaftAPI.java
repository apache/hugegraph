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

package org.apache.hugegraph.api.raft;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hugegraph.HugeException;
import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.api.API;
import org.apache.hugegraph.api.filter.RedirectFilter;
import org.apache.hugegraph.api.filter.StatusFilter.Status;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.store.raft.RaftAddPeerJob;
import org.apache.hugegraph.backend.store.raft.RaftGroupManager;
import org.apache.hugegraph.backend.store.raft.RaftRemovePeerJob;
import org.apache.hugegraph.core.GraphManager;
import org.apache.hugegraph.job.JobBuilder;
import org.apache.hugegraph.util.DateUtil;
import org.apache.hugegraph.util.JsonUtil;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

import com.codahale.metrics.annotation.Timed;
import com.google.common.collect.ImmutableMap;

import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.annotation.security.RolesAllowed;
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

@Path("graphspaces/{graphspace}/graphs/{graph}/raft")
@Singleton
@Tag(name = "RaftAPI")
public class RaftAPI extends API {

    private static final Logger LOG = Log.logger(RaftAPI.class);

    @GET
    @Timed
    @Path("list_peers")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"space_member"})
    public Map<String, List<String>> listPeers(@Context GraphManager manager,
                                               @Parameter(description = "The graph name")
                                               @PathParam("graph") String graph,
                                               @Parameter(description = "The graph space name")
                                               @PathParam("graphspace") String graphSpace,
                                               @Parameter(description = "The raft group name")
                                               @QueryParam("group")
                                               @DefaultValue("default")
                                               String group) {
        LOG.debug("Graph [{}] prepare to get leader", graph);

        HugeGraph g = graph(manager, graphSpace, graph);
        RaftGroupManager raftManager = raftGroupManager(g, group, "list_peers");
        List<String> peers = raftManager.listPeers();
        return ImmutableMap.of(raftManager.group(), peers);
    }

    @GET
    @Timed
    @Path("get_leader")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"space_member"})
    public Map<String, String> getLeader(@Context GraphManager manager,
                                         @Parameter(description = "The graph name")
                                         @PathParam("graph") String graph,
                                         @Parameter(description = "The graph space name")
                                         @PathParam("graphspace") String graphSpace,
                                         @Parameter(description = "The raft group name")
                                         @QueryParam("group")
                                         @DefaultValue("default")
                                         String group) {
        LOG.debug("Graph [{}] prepare to get leader", graph);

        HugeGraph g = graph(manager, graphSpace, graph);
        RaftGroupManager raftManager = raftGroupManager(g, group, "get_leader");
        String leaderId = raftManager.getLeader();
        return ImmutableMap.of(raftManager.group(), leaderId);
    }

    @POST
    @Timed
    @Status(Status.OK)
    @Path("transfer_leader")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"space_member"})
    public Map<String, String> transferLeader(@Context GraphManager manager,
                                              @Parameter(description = "The graph space name")
                                              @PathParam("graphspace") String graphSpace,
                                              @Parameter(description = "The graph name")
                                              @PathParam("graph") String graph,
                                              @Parameter(description = "The raft group name")
                                              @QueryParam("group")
                                              @DefaultValue("default")
                                              String group,
                                              @Parameter(description = "The endpoint address")
                                              @QueryParam("endpoint")
                                              String endpoint) {
        LOG.debug("Graph [{}] prepare to transfer leader to: {}",
                  graph, endpoint);

        HugeGraph g = graph(manager, graphSpace, graph);
        RaftGroupManager raftManager = raftGroupManager(g, group,
                                                        "transfer_leader");
        String leaderId = raftManager.transferLeaderTo(endpoint);
        return ImmutableMap.of(raftManager.group(), leaderId);
    }

    @POST
    @Timed
    @Status(Status.OK)
    @Path("set_leader")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"space_member"})
    public Map<String, String> setLeader(@Context GraphManager manager,
                                         @Parameter(description = "The graph space name")
                                         @PathParam("graphspace") String graphSpace,
                                         @Parameter(description = "The graph name")
                                         @PathParam("graph") String graph,
                                         @Parameter(description = "The raft group name")
                                         @QueryParam("group")
                                         @DefaultValue("default")
                                         String group,
                                         @Parameter(
                                                 description = "The endpoint address to set as " +
                                                               "leader")
                                         @QueryParam("endpoint")
                                         String endpoint) {
        LOG.debug("Graph [{}] prepare to set leader to: {}",
                  graph, endpoint);

        HugeGraph g = graph(manager, graphSpace, graph);
        RaftGroupManager raftManager = raftGroupManager(g, group, "set_leader");
        String leaderId = raftManager.setLeader(endpoint);
        return ImmutableMap.of(raftManager.group(), leaderId);
    }

    @POST
    @Timed
    @Status(Status.OK)
    @Path("add_peer")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"space_member"})
    @RedirectFilter.RedirectMasterRole
    public Map<String, Id> addPeer(@Context GraphManager manager,
                                   @Parameter(description = "The graph space name")
                                   @PathParam("graphspace") String graphSpace,
                                   @Parameter(description = "The graph name")
                                   @PathParam("graph") String graph,
                                   @Parameter(description = "The raft group name")
                                   @QueryParam("group") @DefaultValue("default")
                                   String group,
                                   @Parameter(
                                           description = "The endpoint address of the peer to add")
                                   @QueryParam("endpoint") String endpoint) {
        LOG.debug("Graph [{}] prepare to add peer: {}", graph, endpoint);

        HugeGraph g = graph(manager, graphSpace, graph);
        RaftGroupManager raftManager = raftGroupManager(g, group, "add_peer");

        JobBuilder<String> builder = JobBuilder.of(g);
        String name = String.format("raft-group-[%s]-add-peer-[%s]-at-[%s]",
                                    raftManager.group(), endpoint,
                                    DateUtil.now());
        Map<String, String> inputs = new HashMap<>();
        inputs.put("endpoint", endpoint);
        builder.name(name)
               .input(JsonUtil.toJson(inputs))
               .job(new RaftAddPeerJob());
        return ImmutableMap.of("task_id", builder.schedule().id());
    }

    @POST
    @Timed
    @Status(Status.OK)
    @Path("remove_peer")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"space_member"})
    @RedirectFilter.RedirectMasterRole
    public Map<String, Id> removePeer(@Context GraphManager manager,
                                      @Parameter(description = "The graph space name")
                                      @PathParam("graphspace") String graphSpace,
                                      @Parameter(description = "The graph name")
                                      @PathParam("graph") String graph,
                                      @Parameter(description = "The raft group name")
                                      @QueryParam("group")
                                      @DefaultValue("default") String group,
                                      @Parameter(
                                              description = "The endpoint address of the peer to " +
                                                            "remove")
                                      @QueryParam("endpoint") String endpoint) {
        LOG.debug("Graph [{}] prepare to remove peer: {}", graph, endpoint);

        HugeGraph g = graph(manager, graphSpace, graph);
        RaftGroupManager raftManager = raftGroupManager(g, group,
                                                        "remove_peer");
        JobBuilder<String> builder = JobBuilder.of(g);
        String name = String.format("raft-group-[%s]-remove-peer-[%s]-at-[%s]",
                                    raftManager.group(), endpoint,
                                    DateUtil.now());
        Map<String, String> inputs = new HashMap<>();
        inputs.put("endpoint", endpoint);
        builder.name(name)
               .input(JsonUtil.toJson(inputs))
               .job(new RaftRemovePeerJob());
        return ImmutableMap.of("task_id", builder.schedule().id());
    }

    private static RaftGroupManager raftGroupManager(HugeGraph graph,
                                                     String group,
                                                     String operation) {
        RaftGroupManager raftManager = graph.raftGroupManager();
        if (raftManager == null) {
            throw new HugeException("Allowed %s operation only when " +
                                    "working on raft mode", operation);
        }
        return raftManager;
    }
}
