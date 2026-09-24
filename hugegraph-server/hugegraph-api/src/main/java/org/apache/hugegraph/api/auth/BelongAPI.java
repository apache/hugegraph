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

package org.apache.hugegraph.api.auth;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.hugegraph.api.API;
import org.apache.hugegraph.api.filter.StatusFilter.Status;
import org.apache.hugegraph.auth.AuthManager;
import org.apache.hugegraph.auth.HugeBelong;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.core.GraphManager;
import org.apache.hugegraph.define.Checkable;
import org.apache.hugegraph.exception.NotFoundException;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.inject.Singleton;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Context;

@Path("graphspaces/{graphspace}/auth/belongs")
@Singleton
@Tag(name = "BelongAPI")
public class BelongAPI extends API {

    private static final Logger LOG = Log.logger(BelongAPI.class);

    @POST
    @Timed
    @Status(Status.CREATED)
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String create(@Context GraphManager manager,
                         @Parameter(description = "The graph space name")
                         @PathParam("graphspace") String graphSpace,
                         JsonBelong jsonBelong) {
        LOG.debug("GraphSpace [{}] create belong: {}", graphSpace, jsonBelong);
        GraphSpaceGroupAPI.ensureAuthManager(manager, graphSpace);
        checkCreatingBody(jsonBelong);

        HugeBelong belong = jsonBelong.build(graphSpace);
        GraphSpaceGroupAPI.checkBelongReferences(manager.authManager(),
                                                 graphSpace, belong);
        belong.id(manager.authManager().createBelong(graphSpace, belong));
        return manager.serializer().writeAuthElement(belong);
    }

    @PUT
    @Timed
    @Path("{id}")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String update(@Context GraphManager manager,
                         @Parameter(description = "The graph space name")
                         @PathParam("graphspace") String graphSpace,
                         @Parameter(description = "The belong id")
                         @PathParam("id") String id,
                         JsonBelong jsonBelong) {
        LOG.debug("GraphSpace [{}] update belong: {}", graphSpace, jsonBelong);
        GraphSpaceGroupAPI.ensureAuthManager(manager, graphSpace);
        checkUpdatingBody(jsonBelong);

        HugeBelong belong;
        try {
            belong = manager.authManager().getBelong(graphSpace,
                                                     UserAPI.parseId(id));
        } catch (NotFoundException e) {
            throw new IllegalArgumentException("Invalid belong id: " + id);
        }
        checkGraphSpace(graphSpace, belong);
        GraphSpaceGroupAPI.checkBelongReferences(manager.authManager(),
                                                 graphSpace, belong);
        belong = jsonBelong.build(belong);
        manager.authManager().updateBelong(graphSpace, belong);
        return manager.serializer().writeAuthElement(belong);
    }

    @GET
    @Timed
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String list(@Context GraphManager manager,
                       @Parameter(description = "The graph space name")
                       @PathParam("graphspace") String graphSpace,
                       @Parameter(description = "The user id to filter by")
                       @QueryParam("user") String user,
                       @Parameter(description = "The group id to filter by")
                       @QueryParam("group") String group,
                       @Parameter(description = "The limit of results to return")
                       @QueryParam("limit") @DefaultValue("100") long limit) {
        LOG.debug("GraphSpace [{}] list belongs by user {} or group {}",
                  graphSpace, user, group);
        GraphSpaceGroupAPI.ensureAuthManager(manager, graphSpace);
        E.checkArgument(user == null || group == null,
                        "Can't pass both user and group at the same time");

        Id userId = user == null ? null : UserAPI.parseId(user);
        Id groupId = group == null ? null : UserAPI.parseId(group);
        List<HugeBelong> belongs = listScopedBelongs(manager.authManager(),
                                                     graphSpace, userId,
                                                     groupId, limit);
        return manager.serializer().writeAuthElements("belongs", belongs);
    }

    static List<HugeBelong> listScopedBelongs(AuthManager authManager,
                                               String graphSpace, Id user,
                                               Id group, long limit) {
        List<HugeBelong> belongs;
        if (user != null) {
            belongs = authManager.listBelongByUser(graphSpace, user, -1L);
        } else if (group != null) {
            belongs = authManager.listBelongByGroup(graphSpace, group, -1L);
        } else {
            belongs = authManager.listAllBelong(graphSpace, -1L);
        }
        belongs = belongs.stream()
                         .filter(belong -> graphSpace.equals(
                                 belong.graphSpace()))
                         .collect(Collectors.toList());
        return GraphSpaceGroupAPI.applyLimit(belongs, limit);
    }

    @GET
    @Timed
    @Path("{id}")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String get(@Context GraphManager manager,
                      @Parameter(description = "The graph space name")
                      @PathParam("graphspace") String graphSpace,
                      @Parameter(description = "The belong id")
                      @PathParam("id") String id) {
        LOG.debug("GraphSpace [{}] get belong: {}", graphSpace, id);
        GraphSpaceGroupAPI.ensureAuthManager(manager, graphSpace);

        HugeBelong belong = manager.authManager().getBelong(
                graphSpace, UserAPI.parseId(id));
        checkGraphSpace(graphSpace, belong);
        return manager.serializer().writeAuthElement(belong);
    }

    @DELETE
    @Timed
    @Path("{id}")
    @Consumes(APPLICATION_JSON)
    public void delete(@Context GraphManager manager,
                       @Parameter(description = "The graph space name")
                       @PathParam("graphspace") String graphSpace,
                       @Parameter(description = "The belong id")
                       @PathParam("id") String id) {
        LOG.debug("GraphSpace [{}] delete belong: {}", graphSpace, id);
        GraphSpaceGroupAPI.ensureAuthManager(manager, graphSpace);

        try {
            HugeBelong belong = manager.authManager().getBelong(
                    graphSpace, UserAPI.parseId(id));
            checkGraphSpace(graphSpace, belong);
            manager.authManager().deleteBelong(graphSpace,
                                               UserAPI.parseId(id));
        } catch (NotFoundException e) {
            throw new IllegalArgumentException("Invalid belong id: " + id);
        }
    }

    static void checkGraphSpace(String graphSpace, HugeBelong belong) {
        E.checkArgumentNotNull(belong, "The belong can't be null");
        if (!graphSpace.equals(belong.graphSpace())) {
            throw new jakarta.ws.rs.ForbiddenException(
                    "Permission denied: belong belongs to another graphspace");
        }
    }

    @JsonIgnoreProperties(value = {"id", "belong_creator",
                                   "belong_create", "belong_update"})
    static class JsonBelong implements Checkable {

        @JsonProperty("user")
        @Schema(description = "The user id", required = true)
        private String user;
        @JsonProperty("group")
        @Schema(description = "The group id", required = true)
        private String group;
        @JsonProperty("belong_description")
        @Schema(description = "The belong description")
        private String description;

        public HugeBelong build(HugeBelong belong) {
            E.checkArgument(this.user == null ||
                            belong.source().equals(UserAPI.parseId(this.user)),
                            "The user of belong can't be updated");
            E.checkArgument(this.group == null ||
                            belong.target().equals(UserAPI.parseId(this.group)),
                            "The group of belong can't be updated");
            if (this.description != null) {
                belong.description(this.description);
            }
            return belong;
        }

        public HugeBelong build(String graphSpace) {
            HugeBelong belong = new HugeBelong(
                    graphSpace, UserAPI.parseId(this.user),
                    UserAPI.parseId(this.group), null, HugeBelong.UG);
            belong.description(this.description);
            return belong;
        }

        @Override
        public void checkCreate(boolean isBatch) {
            E.checkArgumentNotNull(this.user,
                                   "The user of belong can't be null");
            E.checkArgumentNotNull(this.group,
                                   "The group of belong can't be null");
        }

        @Override
        public void checkUpdate() {
            E.checkArgumentNotNull(this.description,
                                   "The description of belong can't be null");
        }
    }
}
