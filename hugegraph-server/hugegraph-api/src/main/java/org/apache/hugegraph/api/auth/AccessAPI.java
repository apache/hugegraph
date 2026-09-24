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
import org.apache.hugegraph.auth.HugeAccess;
import org.apache.hugegraph.auth.HugePermission;
import org.apache.hugegraph.auth.HugeTarget;
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

@Path("graphspaces/{graphspace}/auth/accesses")
@Singleton
@Tag(name = "AccessAPI")
public class AccessAPI extends API {

    private static final Logger LOG = Log.logger(AccessAPI.class);

    @POST
    @Timed
    @Status(Status.CREATED)
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String create(@Context GraphManager manager,
                         @Parameter(description = "The graph space name")
                         @PathParam("graphspace") String graphSpace,
                         JsonAccess jsonAccess) {
        LOG.debug("GraphSpace [{}] create access: {}", graphSpace, jsonAccess);
        GraphSpaceGroupAPI.ensureAuthManager(manager, graphSpace);
        checkCreatingBody(jsonAccess);

        HugeAccess access = jsonAccess.build(graphSpace);
        access.id(createScopedAccess(manager.authManager(), graphSpace,
                                     access));
        return manager.serializer().writeAuthElement(access);
    }

    static Id createScopedAccess(AuthManager authManager, String graphSpace,
                                 HugeAccess access) {
        GraphSpaceGroupAPI.requireScopedGroupReference(
                authManager, graphSpace, access.source());
        HugeTarget target;
        try {
            target = authManager.getTarget(graphSpace, access.target());
        } catch (NotFoundException e) {
            throw new IllegalArgumentException(
                    "Invalid target id: " + access.target());
        }
        E.checkArgument(target != null, "Invalid target id: %s",
                        access.target());
        TargetAPI.checkGraphSpace(graphSpace, target);
        return authManager.createAccess(graphSpace, access);
    }

    @PUT
    @Timed
    @Path("{id}")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String update(@Context GraphManager manager,
                         @Parameter(description = "The graph space name")
                         @PathParam("graphspace") String graphSpace,
                         @Parameter(description = "The access id")
                         @PathParam("id") String id,
                         JsonAccess jsonAccess) {
        LOG.debug("GraphSpace [{}] update access: {}", graphSpace, jsonAccess);
        GraphSpaceGroupAPI.ensureAuthManager(manager, graphSpace);
        checkUpdatingBody(jsonAccess);

        HugeAccess access;
        try {
            access = manager.authManager().getAccess(graphSpace,
                                                     UserAPI.parseId(id));
        } catch (NotFoundException e) {
            throw new IllegalArgumentException("Invalid access id: " + id);
        }
        checkGraphSpace(graphSpace, access);
        GraphSpaceGroupAPI.requireScopedGroupReference(
                manager.authManager(), graphSpace, access.source());
        access = jsonAccess.build(access);
        manager.authManager().updateAccess(graphSpace, access);
        return manager.serializer().writeAuthElement(access);
    }

    @GET
    @Timed
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String list(@Context GraphManager manager,
                       @Parameter(description = "The graph space name")
                       @PathParam("graphspace") String graphSpace,
                       @Parameter(description = "The group id to filter by")
                       @QueryParam("group") String group,
                       @Parameter(description = "The target id to filter by")
                       @QueryParam("target") String target,
                       @Parameter(description = "The limit of results to return")
                       @QueryParam("limit") @DefaultValue("100") long limit) {
        LOG.debug("GraphSpace [{}] list accesses by group {} or target {}",
                  graphSpace, group, target);
        GraphSpaceGroupAPI.ensureAuthManager(manager, graphSpace);
        E.checkArgument(group == null || target == null,
                        "Can't pass both group and target at the same time");

        Id groupId = group == null ? null : UserAPI.parseId(group);
        Id targetId = target == null ? null : UserAPI.parseId(target);
        List<HugeAccess> accesses = listScopedAccesses(manager.authManager(),
                                                       graphSpace, groupId,
                                                       targetId, limit);
        return manager.serializer().writeAuthElements("accesses", accesses);
    }

    static List<HugeAccess> listScopedAccesses(AuthManager authManager,
                                                String graphSpace, Id group,
                                                Id target, long limit) {
        List<HugeAccess> accesses;
        if (group != null) {
            accesses = authManager.listAccessByGroup(graphSpace, group, -1L);
        } else if (target != null) {
            accesses = authManager.listAccessByTarget(graphSpace, target,
                                                       -1L);
        } else {
            accesses = authManager.listAllAccess(graphSpace, -1L);
        }
        accesses = accesses.stream()
                           .filter(access -> graphSpace.equals(
                                   access.graphSpace()))
                           .filter(access ->
                                   GraphSpaceGroupAPI.isScopedGroupReference(
                                           authManager, graphSpace,
                                           access.source()))
                           .collect(Collectors.toList());
        return GraphSpaceGroupAPI.applyLimit(accesses, limit);
    }

    @GET
    @Timed
    @Path("{id}")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String get(@Context GraphManager manager,
                      @Parameter(description = "The graph space name")
                      @PathParam("graphspace") String graphSpace,
                      @Parameter(description = "The access id")
                      @PathParam("id") String id) {
        LOG.debug("GraphSpace [{}] get access: {}", graphSpace, id);
        GraphSpaceGroupAPI.ensureAuthManager(manager, graphSpace);

        HugeAccess access = manager.authManager().getAccess(
                graphSpace, UserAPI.parseId(id));
        checkGraphSpace(graphSpace, access);
        GraphSpaceGroupAPI.requireScopedGroupReference(
                manager.authManager(), graphSpace, access.source());
        return manager.serializer().writeAuthElement(access);
    }

    @DELETE
    @Timed
    @Path("{id}")
    @Consumes(APPLICATION_JSON)
    public void delete(@Context GraphManager manager,
                       @Parameter(description = "The graph space name")
                       @PathParam("graphspace") String graphSpace,
                       @Parameter(description = "The access id")
                       @PathParam("id") String id) {
        LOG.debug("GraphSpace [{}] delete access: {}", graphSpace, id);
        GraphSpaceGroupAPI.ensureAuthManager(manager, graphSpace);

        try {
            HugeAccess access = manager.authManager().getAccess(
                    graphSpace, UserAPI.parseId(id));
            checkGraphSpace(graphSpace, access);
            GraphSpaceGroupAPI.requireScopedGroupReference(
                    manager.authManager(), graphSpace, access.source());
            manager.authManager().deleteAccess(graphSpace,
                                               UserAPI.parseId(id));
        } catch (NotFoundException e) {
            throw new IllegalArgumentException("Invalid access id: " + id);
        }
    }

    static void checkGraphSpace(String graphSpace, HugeAccess access) {
        E.checkArgumentNotNull(access, "The access can't be null");
        if (!graphSpace.equals(access.graphSpace())) {
            throw new jakarta.ws.rs.ForbiddenException(
                    "Permission denied: access belongs to another graphspace");
        }
    }

    @JsonIgnoreProperties(value = {"id", "access_creator",
                                   "access_create", "access_update"})
    static class JsonAccess implements Checkable {

        @JsonProperty("group")
        @Schema(description = "The group id", required = true)
        private String group;
        @JsonProperty("target")
        @Schema(description = "The target id", required = true)
        private String target;
        @JsonProperty("access_permission")
        @Schema(description = "The access permission", required = true)
        private HugePermission permission;
        @JsonProperty("access_description")
        @Schema(description = "The access description")
        private String description;

        public HugeAccess build(HugeAccess access) {
            E.checkArgument(this.group == null ||
                            access.source().equals(UserAPI.parseId(this.group)),
                            "The group of access can't be updated");
            E.checkArgument(this.target == null ||
                            access.target().equals(UserAPI.parseId(this.target)),
                            "The target of access can't be updated");
            E.checkArgument(this.permission == null ||
                            access.permission().equals(this.permission),
                            "The permission of access can't be updated");
            if (this.description != null) {
                access.description(this.description);
            }
            return access;
        }

        public HugeAccess build(String graphSpace) {
            HugeAccess access = new HugeAccess(graphSpace,
                                               UserAPI.parseId(this.group),
                                               UserAPI.parseId(this.target));
            access.permission(this.permission);
            access.description(this.description);
            return access;
        }

        @Override
        public void checkCreate(boolean isBatch) {
            E.checkArgumentNotNull(this.group,
                                   "The group of access can't be null");
            E.checkArgumentNotNull(this.target,
                                   "The target of access can't be null");
            E.checkArgumentNotNull(this.permission,
                                   "The permission of access can't be null");
        }

        @Override
        public void checkUpdate() {
            E.checkArgumentNotNull(this.description,
                                   "The description of access can't be null");
        }
    }
}
