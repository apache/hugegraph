/*
 * Copyright 2017 HugeGraph Authors
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hugegraph.api.auth;

import java.util.ArrayList;
import java.util.List;

import org.apache.hugegraph.api.API;
import org.apache.hugegraph.api.filter.StatusFilter;
import org.apache.hugegraph.auth.AuthManager;
import org.apache.hugegraph.auth.HugeGraphAuthProxy;
import org.apache.hugegraph.auth.HugePermission;
import org.apache.hugegraph.core.GraphManager;
import org.apache.hugegraph.define.Checkable;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.inject.Singleton;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Context;

@Path("graphspaces/{graphspace}/auth/managers")
@Singleton
@Tag(name = "ManagerAPI")
public class ManagerAPI extends API {

    private static final Logger LOG = Log.logger(ManagerAPI.class);

    @POST
    @Timed
    @StatusFilter.Status(StatusFilter.Status.CREATED)
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String createManager(@Context GraphManager manager,
                                @PathParam("graphspace") String graphSpace,
                                JsonManager jsonManager) {
        LOG.debug("Create manager: {}", jsonManager);
        String user = jsonManager.user;
        HugePermission type = jsonManager.type;
        // graphSpace now comes from @PathParam instead of JsonManager

        validType(type);
        AuthManager authManager = manager.authManager();
        validUser(authManager, user);

        String creator = HugeGraphAuthProxy.getContext().user().username();
        switch (type) {
            case SPACE:
                validGraphSpace(manager, graphSpace);
                validPermission(
                        hasAdminOrSpaceManagerPerm(manager, graphSpace, creator),
                        creator, "manager.create");
                if (authManager.isSpaceMember(graphSpace, user)) {
                    authManager.deleteSpaceMember(graphSpace, user);
                }
                authManager.createSpaceManager(graphSpace, user);
                break;
            case SPACE_MEMBER:
                validGraphSpace(manager, graphSpace);
                validPermission(
                        hasAdminOrSpaceManagerPerm(manager, graphSpace, creator),
                        creator, "manager.create");
                if (authManager.isSpaceManager(graphSpace, user)) {
                    authManager.deleteSpaceManager(graphSpace, user);
                }
                authManager.createSpaceMember(graphSpace, user);
                break;
            case ADMIN:
                validPermission(hasAdminPerm(manager, creator),
                                creator, "manager.create");
                authManager.createAdminManager(user);
                break;
            default:
                throw new IllegalArgumentException("Invalid type");
        }

        return manager.serializer()
                      .writeMap(ImmutableMap.of("user", user, "type", type,
                                                "graphspace", graphSpace));
    }

    @DELETE
    @Timed
    @Consumes(APPLICATION_JSON)
    public void delete(@Context GraphManager manager,
                       @PathParam("graphspace") String graphSpace,
                       @QueryParam("user") String user,
                       @QueryParam("type") HugePermission type) {
        LOG.debug("Delete graph manager: {} {} {}", user, type, graphSpace);
        E.checkArgument(!"admin".equals(user) ||
                        type != HugePermission.ADMIN,
                        "User 'admin' can't be removed from ADMIN");

        AuthManager authManager = manager.authManager();
        validType(type);
        validUser(authManager, user);
        String actionUser = HugeGraphAuthProxy.getContext().user().username();

        switch (type) {
            case SPACE:
                // only space manager and admin can delete user permission
                validGraphSpace(manager, graphSpace);
                validPermission(
                        hasAdminOrSpaceManagerPerm(manager, graphSpace, actionUser),
                        actionUser, "manager.delete");
                authManager.deleteSpaceManager(graphSpace, user);
                break;
            case SPACE_MEMBER:
                validGraphSpace(manager, graphSpace);
                validPermission(
                        hasAdminOrSpaceManagerPerm(manager, graphSpace, actionUser),
                        actionUser, "manager.delete");
                authManager.deleteSpaceMember(graphSpace, user);
                break;
            case ADMIN:
                validPermission(
                        hasAdminPerm(manager, actionUser),
                        actionUser, "manager.delete");
                authManager.deleteAdminManager(user);
                break;
            default:
                throw new IllegalArgumentException("Invalid type");
        }
    }

    @GET
    @Timed
    @Consumes(APPLICATION_JSON)
    public String list(@Context GraphManager manager,
                       @PathParam("graphspace") String graphSpace,
                       @QueryParam("type") HugePermission type) {
        LOG.debug("list graph manager: {} {}", type, graphSpace);

        AuthManager authManager = manager.authManager();
        validType(type);
        List<String> adminManagers;
        switch (type) {
            case SPACE:
                validGraphSpace(manager, graphSpace);
                adminManagers = authManager.listSpaceManager(graphSpace);
                break;
            case SPACE_MEMBER:
                validGraphSpace(manager, graphSpace);
                adminManagers = authManager.listSpaceMember(graphSpace);
                break;
            case ADMIN:
                adminManagers = authManager.listAdminManager();
                break;
            default:
                throw new IllegalArgumentException("Invalid type");
        }
        return manager.serializer().writeList("admins", adminManagers);
    }

    @GET
    @Timed
    @Path("check")
    @Consumes(APPLICATION_JSON)
    public String checkRole(@Context GraphManager manager,
                            @PathParam("graphspace") String graphSpace,
                            @QueryParam("type") HugePermission type) {
        LOG.debug("check if current user is graph manager: {} {}", type, graphSpace);

        validType(type);
        AuthManager authManager = manager.authManager();
        String user = HugeGraphAuthProxy.getContext().user().username();

        boolean result;
        switch (type) {
            case SPACE:
                validGraphSpace(manager, graphSpace);
                result = authManager.isSpaceManager(graphSpace, user);
                break;
            case SPACE_MEMBER:
                validGraphSpace(manager, graphSpace);
                result = authManager.isSpaceMember(graphSpace, user);
                break;
            case ADMIN:
                result = authManager.isAdminManager(user);
                break;
            default:
                throw new IllegalArgumentException("Invalid type");
        }
        return manager.serializer().writeMap(ImmutableMap.of("check", result));
    }

    @GET
    @Timed
    @Path("role")
    @Consumes(APPLICATION_JSON)
    public String getRolesInGs(@Context GraphManager manager,
                               @PathParam("graphspace") String graphSpace,
                               @QueryParam("user") String user) {
        LOG.debug("get user [{}]'s role in graph space [{}]", user, graphSpace);
        AuthManager authManager = manager.authManager();
        List<HugePermission> result = new ArrayList<>();
        validGraphSpace(manager, graphSpace);

        if (authManager.isAdminManager(user)) {
            result.add(HugePermission.ADMIN);
        }
        if (authManager.isSpaceManager(graphSpace, user)) {
            result.add(HugePermission.SPACE);
        }
        if (authManager.isSpaceMember(graphSpace, user)) {
            result.add(HugePermission.SPACE_MEMBER);
        }
        if (result.isEmpty()) {
            result.add(HugePermission.NONE);
        }
        return manager.serializer().writeMap(
                ImmutableMap.of("user", user, "graphspace", graphSpace, "roles",
                                result));
    }

    private void validUser(AuthManager authManager, String user) {
        E.checkArgument(authManager.findUser(user) != null ||
                        authManager.findGroup(user) != null,
                        "The user or group is not exist");
    }

    private void validType(HugePermission type) {
        E.checkArgument(type == HugePermission.SPACE ||
                        type == HugePermission.SPACE_MEMBER ||
                        type == HugePermission.ADMIN,
                        "The type must be in [SPACE, SPACE_MEMBER, ADMIN]");
    }

    private void validGraphSpace(GraphManager manager, String graphSpace) {
        E.checkArgument(manager.graphSpace(graphSpace) != null,
                        "The graph space is not exist");
    }

    private static class JsonManager implements Checkable {

        @JsonProperty("user")
        private String user;
        @JsonProperty("type")
        private HugePermission type;

        @Override
        public void checkCreate(boolean isBatch) {
        }

        @Override
        public void checkUpdate() {
        }
    }
}
