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

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.hugegraph.api.API;
import org.apache.hugegraph.api.filter.StatusFilter.Status;
import org.apache.hugegraph.auth.AuthManager;
import org.apache.hugegraph.auth.HugeBelong;
import org.apache.hugegraph.auth.HugeGraphAuthProxy;
import org.apache.hugegraph.auth.HugeGroup;
import org.apache.hugegraph.auth.HugeUser;
import org.apache.hugegraph.auth.StandardAuthManagerV2;
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
import jakarta.ws.rs.ForbiddenException;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Context;

@Path("graphspaces/{graphspace}/auth/groups")
@Singleton
@Tag(name = "GraphSpaceGroupAPI")
public class GraphSpaceGroupAPI extends API {

    private static final Logger LOG = Log.logger(GraphSpaceGroupAPI.class);

    @POST
    @Timed
    @Status(Status.CREATED)
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String create(@Context GraphManager manager,
                         @PathParam("graphspace") String graphSpace,
                         JsonGroup jsonGroup) {
        LOG.debug("GraphSpace [{}] create scoped group", graphSpace);
        ensureManager(manager, graphSpace);
        checkCreatingBody(jsonGroup);
        HugeGroup group = jsonGroup.build(graphSpace);
        checkScopedGroup(graphSpace, group);
        group.id(manager.authManager().createGroup(group));
        return manager.serializer().writeAuthElement(group);
    }

    @PUT
    @Timed
    @Path("{id}")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String update(@Context GraphManager manager,
                         @PathParam("graphspace") String graphSpace,
                         @PathParam("id") String id,
                         JsonGroup jsonGroup) {
        LOG.debug("GraphSpace [{}] update scoped group", graphSpace);
        ensureManager(manager, graphSpace);
        checkUpdatingBody(jsonGroup);
        HugeGroup group = getGroup(manager.authManager(), id);
        checkScopedGroup(graphSpace, group);
        group = jsonGroup.build(group);
        manager.authManager().updateGroup(group);
        return manager.serializer().writeAuthElement(group);
    }

    @GET
    @Timed
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String list(@Context GraphManager manager,
                       @PathParam("graphspace") String graphSpace,
                       @QueryParam("limit") @DefaultValue("100") long limit) {
        LOG.debug("GraphSpace [{}] list scoped groups", graphSpace);
        ensureManager(manager, graphSpace);
        List<HugeGroup> groups = listScopedGroups(manager.authManager(),
                                                  graphSpace, limit);
        return manager.serializer().writeAuthElements("groups", groups);
    }

    static List<HugeGroup> listScopedGroups(AuthManager authManager,
                                             String graphSpace, long limit) {
        List<HugeGroup> groups = authManager.listAllGroups(-1);
        groups = filterScopedGroups(graphSpace, groups);
        return applyLimit(groups, limit);
    }

    static <T> List<T> applyLimit(List<T> values, long limit) {
        E.checkArgument(limit >= -1L,
                        "The limit must be -1 or a non-negative number");
        if (limit >= 0L && values.size() > limit) {
            return new ArrayList<>(values.subList(0, (int) limit));
        }
        return values;
    }

    @GET
    @Timed
    @Path("{id}")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String get(@Context GraphManager manager,
                      @PathParam("graphspace") String graphSpace,
                      @Parameter(description = "The scoped group id")
                      @PathParam("id") String id) {
        LOG.debug("GraphSpace [{}] get scoped group", graphSpace);
        ensureManager(manager, graphSpace);
        HugeGroup group = getGroup(manager.authManager(), id);
        checkScopedGroup(graphSpace, group);
        return manager.serializer().writeAuthElement(group);
    }

    @DELETE
    @Timed
    @Path("{id}")
    @Consumes(APPLICATION_JSON)
    public void delete(@Context GraphManager manager,
                       @PathParam("graphspace") String graphSpace,
                       @Parameter(description = "The scoped group id")
                       @PathParam("id") String id) {
        LOG.debug("GraphSpace [{}] delete scoped group", graphSpace);
        ensureManager(manager, graphSpace);
        HugeGroup group = getGroup(manager.authManager(), id);
        checkScopedGroup(graphSpace, group);
        manager.authManager().deleteGroup(graphSpace, group.id());
    }

    static void checkManagerPermission(AuthManager authManager,
                                       String graphSpace, String username) {
        validPermission(authManager.isAdminManager(username) ||
                        authManager.isSpaceManager(graphSpace, username),
                        username, "graphspace-group.manage");
    }

    static List<HugeGroup> filterScopedGroups(String graphSpace,
                                               List<HugeGroup> groups) {
        List<HugeGroup> scoped = new ArrayList<>();
        for (HugeGroup group : groups) {
            if (isScopedGroup(graphSpace, group)) {
                scoped.add(group);
            }
        }
        return scoped;
    }

    static void checkScopedGroup(String graphSpace, HugeGroup group) {
        if (!isScopedGroup(graphSpace, group)) {
            throw new ForbiddenException(
                    "Permission denied: group belongs to another graphspace");
        }
    }

    static String scopedPrefix(String graphSpace) {
        return StandardAuthManagerV2.scopedGroupPrefix(graphSpace);
    }

    static void ensureManager(GraphManager manager, String graphSpace) {
        ensurePdModeEnabled(manager);
        ensureAuthManager(manager, graphSpace);
    }

    static void ensureAuthManager(GraphManager manager, String graphSpace) {
        E.checkArgument(manager.graphSpace(graphSpace) != null,
                        "The graph space '%s' does not exist", graphSpace);
        if (manager.authManager().supportsGraphSpaceAuth()) {
            checkManagerPermission(manager.authManager(), graphSpace,
                                   HugeGraphAuthProxy.username());
        }
    }

    static void checkBelongReferences(AuthManager authManager,
                                      String graphSpace,
                                      HugeBelong belong) {
        if (!authManager.supportsGraphSpaceAuth()) {
            return;
        }
        HugeUser user = authManager.findUser(belong.source().asString());
        if (user != null) {
            String username = user.name();
            if (!authManager.isAdminManager(username) &&
                !authManager.isSpaceManager(graphSpace, username) &&
                !authManager.isSpaceMember(graphSpace, username)) {
                throw new ForbiddenException(
                        "Permission denied: user is not a graphspace member");
            }
        } else {
            checkScopedGroupReference(authManager, graphSpace,
                                      belong.source());
        }
        checkScopedGroupReference(authManager, graphSpace, belong.target());
    }

    static void checkScopedGroupReference(AuthManager authManager,
                                          String graphSpace, Id groupId) {
        if (!authManager.supportsGraphSpaceAuth()) {
            return;
        }
        HugeGroup group;
        try {
            group = authManager.getGroup(groupId);
        } catch (NotFoundException e) {
            return;
        }
        if (group != null) {
            checkScopedGroup(graphSpace, group);
        }
    }

    static void requireScopedGroupReference(AuthManager authManager,
                                            String graphSpace, Id groupId) {
        if (!authManager.supportsGraphSpaceAuth()) {
            return;
        }
        HugeGroup group;
        try {
            group = authManager.getGroup(groupId);
        } catch (NotFoundException e) {
            throw new ForbiddenException(
                    "Permission denied: access group is not a business group");
        }
        if (group == null) {
            throw new ForbiddenException(
                    "Permission denied: access group is not a business group");
        }
        checkScopedGroup(graphSpace, group);
    }

    static boolean isScopedGroupReference(AuthManager authManager,
                                          String graphSpace, Id groupId) {
        try {
            requireScopedGroupReference(authManager, graphSpace, groupId);
            return true;
        } catch (ForbiddenException e) {
            return false;
        }
    }

    static HugeGroup getGroup(AuthManager authManager, String id) {
        HugeGroup group;
        try {
            group = authManager.getGroup(UserAPI.parseId(id));
        } catch (NotFoundException e) {
            throw new IllegalArgumentException("Invalid group id: " + id);
        }
        E.checkArgument(group != null, "Invalid group id: %s", id);
        return group;
    }

    private static boolean isScopedGroup(String graphSpace,
                                         HugeGroup group) {
        return StandardAuthManagerV2.isScopedGroup(graphSpace, group);
    }

    @JsonIgnoreProperties(value = {"id", "group_creator",
                                   "group_create", "group_update"})
    static class JsonGroup implements Checkable {

        @JsonProperty("group_name")
        @Schema(description = "A required client label; the server generates " +
                              "the persisted scoped group name",
                required = true)
        private String name;
        @JsonProperty("group_description")
        @Schema(description = "The description of group")
        private String description;

        HugeGroup build(HugeGroup group) {
            E.checkArgument(this.name == null ||
                            group.name().equals(this.name),
                            "The name of group can't be updated");
            if (this.description != null) {
                group.description(this.description);
            }
            return group;
        }

        HugeGroup build(String graphSpace) {
            String name = scopedPrefix(graphSpace) +
                          UUID.randomUUID().toString().replace("-", "");
            HugeGroup group = new HugeGroup(name);
            group.description(this.description);
            return group;
        }

        @Override
        public void checkCreate(boolean isBatch) {
            E.checkArgumentNotNull(this.name,
                                   "The group label can't be null");
        }

        @Override
        public void checkUpdate() {
            E.checkArgumentNotNull(this.description,
                                   "The description of group can't be null");
        }
    }
}
