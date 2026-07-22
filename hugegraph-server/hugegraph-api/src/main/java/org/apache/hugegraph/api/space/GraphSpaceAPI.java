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

package org.apache.hugegraph.api.space;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;

import org.apache.hugegraph.api.API;
import org.apache.hugegraph.api.filter.StatusFilter.Status;
import org.apache.hugegraph.auth.AuthManager;
import org.apache.hugegraph.auth.HugeDefaultRole;
import org.apache.hugegraph.auth.HugeGraphAuthProxy;
import org.apache.hugegraph.core.GraphManager;
import org.apache.hugegraph.define.Checkable;
import org.apache.hugegraph.exception.NotFoundException;
import org.apache.hugegraph.space.GraphSpace;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.JsonUtil;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.annotation.security.RolesAllowed;
import jakarta.inject.Singleton;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.ForbiddenException;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.SecurityContext;

@Path("graphspaces")
@Singleton
@Tag(name = "GraphSpaceAPI")
public class GraphSpaceAPI extends API {

    private static final Logger LOG = Log.logger(GraphSpaceAPI.class);

    private static final String GRAPH_SPACE_ACTION = "action";
    private static final String UPDATE = "update";
    private static final String GRAPH_SPACE_ACTION_CLEAR = "clear";

    @GET
    @Timed
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public Object list(@Context GraphManager manager,
                       @Context SecurityContext sc) {
        ensurePdModeEnabled(manager);
        Set<String> spaces = manager.graphSpaces();
        return ImmutableMap.of("graphSpaces", spaces);
    }

    @GET
    @Timed
    @Path("{graphspace}")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public Object get(@Context GraphManager manager,
                      @Parameter(description = "The name of the graph space")
                      @PathParam("graphspace") String graphSpace) {
        ensurePdModeEnabled(manager);
        manager.getSpaceStorage(graphSpace);
        GraphSpace gs = space(manager, graphSpace);

        String json = JsonUtil.toJson(gs);
        Map<Object, Object> gsInfo = JsonUtil.fromJson(json, Map.class);
        // add department user info
        String dpUserName = getDpUserName(graphSpace);
        gsInfo.put("dp_username", dpUserName);
        gsInfo.put("dp_password", getDpPassWord(dpUserName));
        return gsInfo;
    }

    @POST
    @Timed
    @Status(Status.CREATED)
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @Path("{graphspace}/role")
    @RolesAllowed({"admin", "space"})
    public String setDefaultRole(@Context GraphManager manager,
                                 @PathParam("graphspace") String name,
                                 JsonDefaultRole jsonRole) {
        ensurePdModeEnabled(manager);
        E.checkArgumentNotNull(jsonRole, "Request body cannot be null");
        E.checkArgument(StringUtils.isNotEmpty(jsonRole.user),
                        "The 'user' field cannot be null or empty");
        E.checkArgument(StringUtils.isNotEmpty(jsonRole.role),
                        "The 'role' field cannot be null or empty");
        String user = jsonRole.user;
        String graph = jsonRole.graph;
        HugeDefaultRole role;
        try {
            role = HugeDefaultRole.valueOf(jsonRole.role.toUpperCase());
        } catch (IllegalArgumentException e) {
            E.checkArgument(false, "Invalid role value '%s'", jsonRole.role);
            role = null; // unreachable, satisfies compiler
        }
        validGraphSpace(manager, name);
        LOG.debug("Create default role: {} {} {}", user, role,
                              name);
        AuthManager authManager = manager.authManager();
        String operator = HugeGraphAuthProxy.username();
        validPermission(hasAdminOrSpaceManagerPerm(manager, name, operator),
                        operator, "default_role.create");
        E.checkArgument(authManager.findUser(user) != null ||
                        authManager.findGroup(user) != null,
                        "The user or group is not exist");
        // only admin can set space admin
        if (!authManager.isAdminManager(operator) && role.equals(HugeDefaultRole.SPACE)) {
            throw new ForbiddenException("Forbidden to set role " + role.toString());
        }

        boolean hasGraph = role.equals(HugeDefaultRole.OBSERVER);

        E.checkArgument(!hasGraph || StringUtils.isNotEmpty(graph),
                        "Must set a graph for observer");
        if (hasGraph) {
            validGraph(manager, name, graph);
        }

        Map<String, String> result = new HashMap<>();
        result.put("user", user);
        result.put("role", jsonRole.role);
        result.put("graphSpace", name);

        if (hasGraph) {
            authManager.createDefaultRole(name, user, role, graph);
            result.put("graph", graph);
        } else {
            authManager.createSpaceDefaultRole(name, user, role);
        }

        return manager.serializer().writeMap(result);
    }

    @GET
    @Timed
    @Path("{graphspace}/role")
    @RolesAllowed({"admin", "space"})
    public String checkDefaultRole(@Context GraphManager manager,
                                   @PathParam("graphspace") String name,
                                   @QueryParam("user") String user,
                                   @QueryParam("role") String role,
                                   @QueryParam("graph") String graph) {
        ensurePdModeEnabled(manager);
        E.checkArgument(StringUtils.isNotEmpty(user),
                        "The 'user' query param cannot be null or empty");
        E.checkArgument(StringUtils.isNotEmpty(role),
                        "The 'role' query param cannot be null or empty");
        LOG.debug("Check space role: {} {} {}", user, role,
                              name);
        AuthManager authManager = manager.authManager();

        HugeDefaultRole defaultRole;
        try {
            defaultRole = HugeDefaultRole.valueOf(role.toUpperCase());
        } catch (IllegalArgumentException e) {
            E.checkArgument(false, "Invalid role value '%s'", role);
            defaultRole = null; // unreachable, satisfies compiler
        }
        validGraphSpace(manager, name);
        String operator = HugeGraphAuthProxy.username();
        validPermission(hasAdminOrSpaceManagerPerm(manager, name, operator),
                        operator, "default_role.check");
        // Only admin can inspect the space manager default role.
        if (!authManager.isAdminManager(operator) &&
            defaultRole.equals(HugeDefaultRole.SPACE)) {
            throw new ForbiddenException("Forbidden to check role " + role);
        }
        boolean hasGraph = defaultRole.equals(HugeDefaultRole.OBSERVER);
        E.checkArgument(!hasGraph || StringUtils.isNotEmpty(graph),
                        "Must set a graph for observer");
        if (hasGraph) {
            validGraph(manager, name, graph);
        }

        boolean result;
        if (hasGraph) {
            result = authManager.isDefaultRole(name, graph, user,
                                               defaultRole);
        } else {
            result = authManager.isDefaultRole(name, user,
                                               defaultRole);
        }
        return manager.serializer().writeMap(ImmutableMap.of("check", result));
    }

    @DELETE
    @Timed
    @Path("{graphspace}/role")
    @RolesAllowed({"admin", "space"})
    public void deleteDefaultRole(@Context GraphManager manager,
                                  @PathParam("graphspace") String name,
                                  @QueryParam("user") String user,
                                  @QueryParam("role") String role,
                                  @QueryParam("graph") String graph) {
        ensurePdModeEnabled(manager);
        E.checkArgument(StringUtils.isNotEmpty(user),
                        "The 'user' query param cannot be null or empty");
        E.checkArgument(StringUtils.isNotEmpty(role),
                        "The 'role' query param cannot be null or empty");
        LOG.debug("Delete space role: {} {} {}", user, role,
                              name);

        AuthManager authManager = manager.authManager();
        validGraphSpace(manager, name);
        String operator = HugeGraphAuthProxy.username();
        validPermission(hasAdminOrSpaceManagerPerm(manager, name, operator),
                        operator, "default_role.delete");
        E.checkArgument(authManager.findUser(user) != null ||
                        authManager.findGroup(user) != null,
                        "The user or group is not exist");

        if (!authManager.isAdminManager(operator) &&
            role.equalsIgnoreCase(HugeDefaultRole.SPACE.toString())) {
            throw new ForbiddenException("Forbidden to delete role " + role);
        }

        HugeDefaultRole defaultRole;
        try {
            defaultRole = HugeDefaultRole.valueOf(role.toUpperCase());
        } catch (IllegalArgumentException e) {
            E.checkArgument(false, "Invalid role value '%s'", role);
            defaultRole = null; // unreachable, satisfies compiler
        }
        boolean hasGraph = defaultRole.equals(HugeDefaultRole.OBSERVER);
        E.checkArgument(!hasGraph || StringUtils.isNotEmpty(graph),
                        "Must set a graph for observer");
        if (hasGraph) {
            validGraph(manager, name, graph);
        }
        if (hasGraph) {
            authManager.deleteDefaultRole(name, user, defaultRole, graph);
        } else {
            authManager.deleteDefaultRole(name, user, defaultRole);
        }
    }

    @GET
    @Timed
    @Path("profile")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"admin"})
    public Object listProfile(@Context GraphManager manager,
                              @Parameter(description = "Filter graph spaces by " +
                                                        "name or nickname prefix")
                              @QueryParam("prefix") String prefix,
                              @Context SecurityContext sc) {
        ensurePdModeEnabled(manager);
        Set<String> spaces = manager.graphSpaces();
        List<Map<String, Object>> spaceList = new ArrayList<>();
        List<Map<String, Object>> result = new ArrayList<>();
        String user = HugeGraphAuthProxy.username();
        AuthManager authManager = manager.authManager();
        // FIXME: defaultSpace related interface is not implemented
        // String defaultSpace = authManager.getDefaultSpace(user);
        for (String sp : spaces) {
            manager.getSpaceStorage(sp);
            GraphSpace gs = space(manager, sp);
            Map<String, Object> gsProfile = gs.info();
            boolean isManager = verifyPermission(user, authManager, sp);

            // 设置当前用户的是否允许访问该空间
            if (gs.auth() && !isManager) {
                gsProfile.put("authed", false);
            } else {
                gsProfile.put("authed", true);
            }

            gsProfile.put("create_time",
                          DATE_FORMATTER.format(LocalDateTime.ofInstant(
                                  gs.createTime().toInstant(), ZoneId.systemDefault())));
            gsProfile.put("update_time",
                          DATE_FORMATTER.format(LocalDateTime.ofInstant(
                                  gs.updateTime().toInstant(), ZoneId.systemDefault())));
            if (!isPrefix(gsProfile, prefix)) {
                continue;
            }

            gsProfile.put("default", false);
            result.add(gsProfile);
            //boolean defaulted = StringUtils.equals(sp, defaultSpace);
            //gsProfile.put("default", defaulted);
            //if (defaulted) {
            //    result.add(gsProfile);
            //} else {
            //    spaceList.add(gsProfile);
            //}
        }
        result.addAll(spaceList);
        return result;
    }

    @POST
    @Timed
    @Status(Status.CREATED)
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"admin"})
    public String create(@Context GraphManager manager,
                         JsonGraphSpace jsonGraphSpace) {
        ensurePdModeEnabled(manager);
        jsonGraphSpace.checkCreate(false);

        String creator = HugeGraphAuthProxy.username();
        GraphSpace exist = manager.graphSpace(jsonGraphSpace.name);
        E.checkArgument(exist == null, "The graph space '%s' has existed",
                        jsonGraphSpace.name);
        GraphSpace space = manager.createGraphSpace(
                jsonGraphSpace.toGraphSpace(creator));
        return manager.serializer().writeGraphSpace(space);
    }


    @PUT
    @Timed
    @Path("{name}")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"admin"})
    public Map<String, Object> manage(@Context GraphManager manager,
                                      @Parameter(description = "The name of the graph space")
                                      @PathParam("name") String name,
                                      Map<String, Object> actionMap) {
        ensurePdModeEnabled(manager);
        E.checkArgument(actionMap != null && actionMap.size() == 2 &&
                        actionMap.containsKey(GRAPH_SPACE_ACTION),
                        "Invalid request body '%s'", actionMap);
        Object value = actionMap.get(GRAPH_SPACE_ACTION);
        E.checkArgument(value instanceof String,
                        "Invalid action type '%s', must be string",
                        value.getClass());
        String action = (String) value;
        switch (action) {
            case "update":
                E.checkArgument(actionMap.containsKey(UPDATE),
                                "Please pass '%s' for graph space update",
                                UPDATE);
                value = actionMap.get(UPDATE);
                E.checkArgument(value instanceof Map,
                                "The '%s' must be map, but got %s",
                                UPDATE, value.getClass());
                @SuppressWarnings("unchecked")
                Map<String, Object> graphSpaceMap = (Map<String, Object>) value;
                String gsName = (String) graphSpaceMap.get("name");
                E.checkArgument(gsName.equals(name),
                                "Different name in update body with in path");
                GraphSpace exist = manager.graphSpace(name);
                if (exist == null) {
                    throw new NotFoundException(
                            "Can't find graph space with name '%s'", gsName);
                }

                String nickname = (String) graphSpaceMap.get("nickname");
                if (!StringUtils.isEmpty(nickname)) {
                    GraphManager.checkNickname(nickname);
                    exist.nickname(nickname);
                }

                String description = (String) graphSpaceMap.get("description");
                if (!StringUtils.isEmpty(description)) {
                    exist.description(description);
                }

                int maxGraphNumber =
                        (int) graphSpaceMap.get("max_graph_number");
                if (maxGraphNumber != 0) {
                    exist.maxGraphNumber(maxGraphNumber);
                }
                int maxRoleNumber = (int) graphSpaceMap.get("max_role_number");
                if (maxRoleNumber != 0) {
                    exist.maxRoleNumber(maxRoleNumber);
                }

                int cpuLimit = (int) graphSpaceMap.get("cpu_limit");
                if (cpuLimit != 0) {
                    exist.cpuLimit(cpuLimit);
                }
                int memoryLimit = (int) graphSpaceMap.get("memory_limit");
                if (memoryLimit != 0) {
                    exist.memoryLimit(memoryLimit);
                }
                int storageLimit = (int) graphSpaceMap.get("storage_limit");
                if (storageLimit != 0) {
                    exist.storageLimit = storageLimit;
                }

                int computeCpuLimit = (int) graphSpaceMap
                        .getOrDefault("compute_cpu_limit", 0);
                if (computeCpuLimit != 0) {
                    exist.computeCpuLimit(computeCpuLimit);
                }
                int computeMemoryLimit = (int) graphSpaceMap
                        .getOrDefault("compute_memory_limit", 0);
                if (computeMemoryLimit != 0) {
                    exist.computeMemoryLimit(computeMemoryLimit);
                }

                String oltpNamespace =
                        (String) graphSpaceMap.get("oltp_namespace");
                if (oltpNamespace != null &&
                    !StringUtils.isEmpty(oltpNamespace)) {
                    exist.oltpNamespace(oltpNamespace);
                }
                String olapNamespace =
                        (String) graphSpaceMap.get("olap_namespace");
                if (olapNamespace != null &&
                    !StringUtils.isEmpty(olapNamespace)) {
                    exist.olapNamespace(olapNamespace);
                }
                String storageNamespace =
                        (String) graphSpaceMap.get("storage_namespace");
                if (storageNamespace != null &&
                    !StringUtils.isEmpty(storageNamespace)) {
                    exist.storageNamespace(storageNamespace);
                }

                String operatorImagePath = (String) graphSpaceMap
                        .getOrDefault("operator_image_path", "");
                if (!StringUtils.isEmpty(operatorImagePath)) {
                    exist.operatorImagePath(operatorImagePath);
                }

                String internalAlgorithmImageUrl = (String) graphSpaceMap
                        .getOrDefault("internal_algorithm_image_url", "");
                if (!StringUtils.isEmpty(internalAlgorithmImageUrl)) {
                    exist.internalAlgorithmImageUrl(internalAlgorithmImageUrl);
                }

                @SuppressWarnings("unchecked")
                Map<String, Object> configs =
                        (Map<String, Object>) graphSpaceMap.get("configs");
                if (configs != null && !configs.isEmpty()) {
                    exist.configs(configs);
                }
                exist.refreshUpdate();
                GraphSpace space = manager.createGraphSpace(exist);
                return space.info();
            case GRAPH_SPACE_ACTION_CLEAR:
                return ImmutableMap.of(name, "cleared");
            default:
                throw new AssertionError(String.format("Invalid action: '%s'",
                                                       action));
        }
    }

    @DELETE
    @Timed
    @Path("{name}")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"admin"})
    public void delete(@Context GraphManager manager,
                       @Parameter(description = "The name of the graph space")
                       @PathParam("name") String name) {
        ensurePdModeEnabled(manager);
        manager.dropGraphSpace(name);
    }

    private String getDpPassWord(String userName) {
        return DigestUtils.md5Hex("a1p" + DigestUtils.md5Hex(userName).substring(5, 15) + "ck0")
                          .substring(1, 17);
    }

    private String getDpUserName(String graphSpace) {
        return graphSpace.endsWith("gs") ?
               graphSpace.toLowerCase().substring(0, graphSpace.length() - 2) +
               "_dp" : graphSpace.toLowerCase() + "_dp";
    }

    private boolean verifyPermission(String user, AuthManager authManager, String graphSpace) {
        return authManager.isAdminManager(user) ||
               authManager.isSpaceManager(graphSpace, user) ||
               authManager.isSpaceMember(graphSpace, user);
    }

    private static void validGraphSpace(GraphManager manager, String graphSpace) {
        E.checkArgument(manager.graphSpace(graphSpace) != null,
                        "The graph space '%s' does not exist", graphSpace);
    }

    private static void validGraph(GraphManager manager, String graphSpace,
                                   String graph) {
        E.checkArgument(manager.graph(graphSpace, graph) != null,
                        "Graph '%s/%s' does not exist", graphSpace, graph);
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    private static class JsonGraphSpace implements Checkable {

        @JsonProperty("name")
        @Schema(description = "The name of the graph space", required = true)
        public String name;
        @JsonProperty("nickname")
        @Schema(description = "The nickname of the graph space")
        public String nickname;
        @JsonProperty("description")
        @Schema(description = "The description of the graph space")
        public String description;

        @JsonProperty("cpu_limit")
        @Schema(description = "The CPU limit for the graph space", required = true)
        public int cpuLimit;
        @JsonProperty("memory_limit")
        @Schema(description = "The memory limit for the graph space", required = true)
        public int memoryLimit;
        @JsonProperty("storage_limit")
        @Schema(description = "The storage limit for the graph space", required = true)
        public int storageLimit;

        @JsonProperty("compute_cpu_limit")
        @Schema(description = "The compute CPU limit for the graph space")
        public int computeCpuLimit = 0;
        @JsonProperty("compute_memory_limit")
        @Schema(description = "The compute memory limit for the graph space")
        public int computeMemoryLimit = 0;

        @JsonProperty("oltp_namespace")
        @Schema(description = "The OLTP namespace for the graph space")
        public String oltpNamespace = "";
        @JsonProperty("olap_namespace")
        @Schema(description = "The OLAP namespace for the graph space")
        public String olapNamespace = "";
        @JsonProperty("storage_namespace")
        @Schema(description = "The storage namespace for the graph space")
        public String storageNamespace = "";

        @JsonProperty("max_graph_number")
        @Schema(description = "The maximum number of graphs allowed in the space", required = true)
        public int maxGraphNumber;
        @JsonProperty("max_role_number")
        @Schema(description = "The maximum number of roles allowed in the space")
        public int maxRoleNumber;

        @JsonProperty("dp_username")
        @Schema(description = "The data platform username for the graph space")
        public String dpUserName;
        @JsonProperty("dp_password")
        @Schema(description = "The data platform password for the graph space")
        public String dpPassWord;

        @JsonProperty("auth")
        @Schema(description = "Whether authentication is enabled for the graph space")
        public boolean auth = false;

        @JsonProperty("configs")
        @Schema(description = "Additional configurations for the graph space")
        public Map<String, Object> configs;

        @JsonProperty("operator_image_path")
        @Schema(description = "The operator image path for the graph space")
        public String operatorImagePath = "";

        @JsonProperty("internal_algorithm_image_url")
        @Schema(description = "The internal algorithm image URL for the graph space")
        public String internalAlgorithmImageUrl = "";

        @Override
        public void checkCreate(boolean isBatch) {
            E.checkArgument(!StringUtils.isEmpty(this.name),
                            "The name of graph space can't be null or empty");
            E.checkArgument(this.maxGraphNumber > 0,
                            "The max graph number must > 0");

            E.checkArgument(this.cpuLimit > 0,
                            "The cpu limit must be > 0, but got: %s",
                            this.cpuLimit);
            E.checkArgument(this.memoryLimit > 0,
                            "The memory limit must be > 0, but got: %s",
                            this.memoryLimit);
            E.checkArgument(this.storageLimit > 0,
                            "The storage limit must be > 0, but got: %s",
                            this.storageLimit);
            if (this.oltpNamespace == null) {
                this.oltpNamespace = "";
            }
            if (this.olapNamespace == null) {
                this.olapNamespace = "";
            }
            if (this.storageNamespace == null) {
                this.storageNamespace = "";
            }
        }

        public GraphSpace toGraphSpace(String creator) {
            GraphSpace graphSpace = new GraphSpace(this.name,
                                                   this.nickname,
                                                   this.description,
                                                   this.cpuLimit,
                                                   this.memoryLimit,
                                                   this.storageLimit,
                                                   this.maxGraphNumber,
                                                   this.maxRoleNumber,
                                                   this.auth,
                                                   creator,
                                                   this.configs);
            graphSpace.oltpNamespace(this.oltpNamespace);
            graphSpace.olapNamespace(this.olapNamespace);
            graphSpace.storageNamespace(this.storageNamespace);
            graphSpace.computeCpuLimit(this.computeCpuLimit);
            graphSpace.computeMemoryLimit(this.computeMemoryLimit);
            graphSpace.operatorImagePath(this.operatorImagePath);
            graphSpace.internalAlgorithmImageUrl(this.internalAlgorithmImageUrl);
            if (this.configs != null) {
                graphSpace.configs(this.configs);
            }
            return graphSpace;
        }

        public String toString() {
            return String.format("JsonGraphSpace{name=%s, description=%s, " +
                                 "cpuLimit=%s, memoryLimit=%s, " +
                                 "storageLimit=%s, oltpNamespace=%s," +
                                 "olapNamespace=%s, storageNamespace=%s," +
                                 "maxGraphNumber=%s, maxRoleNumber=%s, " +
                                 "configs=%s, operatorImagePath=%s, " +
                                 "internalAlgorithmImageUrl=%s}", this.name,
                                 this.description, this.cpuLimit,
                                 this.memoryLimit, this.storageLimit,
                                 this.oltpNamespace, this.olapNamespace,
                                 this.storageLimit, this.maxGraphNumber,
                                 this.maxRoleNumber, this.configs,
                                 this.operatorImagePath,
                                 this.internalAlgorithmImageUrl);
        }
    }

    private static class JsonDefaultRole implements Checkable {

        @JsonProperty("user")
        @Schema(description = "The username")
        private String user;
        @JsonProperty("role")
        @Schema(description = "The role name")
        private String role;
        @JsonProperty("graph")
        @Schema(description = "The graph name")
        private String graph;

        @Override
        public void checkCreate(boolean isBatch) {
        }

        @Override
        public void checkUpdate() {
        }
    }
}
