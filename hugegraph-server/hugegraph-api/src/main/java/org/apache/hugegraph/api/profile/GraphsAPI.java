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

package org.apache.hugegraph.api.profile;

import java.io.File;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.hugegraph.HugeException;
import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.api.API;
import org.apache.hugegraph.api.filter.StatusFilter;
import org.apache.hugegraph.auth.AuthManager;
import org.apache.hugegraph.auth.HugeAuthenticator.RequiredPerm;
import org.apache.hugegraph.auth.HugeGraphAuthProxy;
import org.apache.hugegraph.auth.HugePermission;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.core.GraphManager;
import org.apache.hugegraph.space.GraphSpace;
import org.apache.hugegraph.type.define.GraphMode;
import org.apache.hugegraph.type.define.GraphReadMode;
import org.apache.hugegraph.util.ConfigUtil;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.JsonUtil;
import org.apache.hugegraph.util.Log;
import org.apache.logging.log4j.util.Strings;
import org.slf4j.Logger;

import com.codahale.metrics.annotation.Timed;
import com.google.common.collect.ImmutableMap;

import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.annotation.security.RolesAllowed;
import jakarta.inject.Singleton;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.ForbiddenException;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.NotSupportedException;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.SecurityContext;

@Path("graphspaces/{graphspace}/graphs")
@Singleton
@Tag(name = "GraphsAPI")
public class GraphsAPI extends API {

    private static final Logger LOG = Log.logger(GraphsAPI.class);

    private static final String CONFIRM_CLEAR = "I'm sure to delete all data";
    private static final String CONFIRM_DROP = "I'm sure to drop the graph";
    private static final String GRAPH_DESCRIPTION = "description";
    private static final String GRAPH_ACTION = "action";
    private static final String UPDATE = "update";
    private static final String GRAPH_ACTION_RELOAD = "reload";

    private static Map<String, Object> convConfig(Map<String, Object> config) {
        Map<String, Object> result = new HashMap<>(config.size());
        for (Map.Entry<String, Object> entry : config.entrySet()) {
            result.put(entry.getKey(), entry.getValue().toString());
        }
        return result;
    }

    @GET
    @Timed
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"space_member", "$dynamic"})
    public Object list(@Context GraphManager manager,
                       @Parameter(description = "The graph space name")
                       @PathParam("graphspace") String graphSpace,
                       @Context SecurityContext sc) {
        LOG.debug("List graphs in graph space {}", graphSpace);
        if (null == manager.graphSpace(graphSpace)) {
            throw new HugeException("Graphspace not exist!");
        }
        Set<String> graphs = manager.graphs(graphSpace);
        LOG.debug("Get graphs list from graph manager with size {}",
                  graphs.size());
        // Filter by user role
        Set<String> filterGraphs = new HashSet<>();
        for (String graph : graphs) {
            LOG.debug("Get graph {} and verify auth", graph);
            String role = RequiredPerm.roleFor(graphSpace, graph,
                                               HugePermission.READ);
            if (sc.isUserInRole(role)) {
                try {
                    graph(manager, graphSpace, graph);
                    filterGraphs.add(graph);
                } catch (ForbiddenException ignored) {
                    // ignore
                }
            } else {
                LOG.debug("The user not in role for graph {}", graph);
            }
        }
        LOG.debug("Finish list graphs with size {}", filterGraphs.size());
        return ImmutableMap.of("graphs", filterGraphs);
    }

    @GET
    @Timed
    @Path("profile")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"space_member", "$dynamic"})
    public Object listProfile(@Context GraphManager manager,
                              @Parameter(description = "The graph space name")
                              @PathParam("graphspace") String graphSpace,
                              @Parameter(description = "Filter graphs by name or nickname prefix")
                              @QueryParam("prefix") String prefix,
                              @Context SecurityContext sc) {
        LOG.debug("List graph profiles in graph space {}", graphSpace);
        if (null == manager.graphSpace(graphSpace)) {
            throw new HugeException("Graphspace not exist!");
        }
        GraphSpace gs = manager.graphSpace(graphSpace);
        // graphSpace.nickname() may be null in non-PD mode (GraphManager returns
        // a placeholder GraphSpace without a nickname set)
        String gsNickname = gs.nickname() != null ? gs.nickname() : graphSpace;

        AuthManager authManager = manager.authManager();
        String user = HugeGraphAuthProxy.username();
        // Default graph concept relies on PD meta storage; in non-PD standalone
        // mode there is no persistent store for this, so we gracefully degrade.
        Map<String, Date> defaultGraphs;
        if (manager.isPDEnabled()) {
            defaultGraphs = authManager.getDefaultGraph(graphSpace, user);
        } else {
            defaultGraphs = new HashMap<>();
        }

        Set<String> graphs = manager.graphs(graphSpace);
        List<Map<String, Object>> profiles = new ArrayList<>();
        List<Map<String, Object>> defaultProfiles = new ArrayList<>();
        for (String graph : graphs) {
            String role = RequiredPerm.roleFor(graphSpace, graph,
                                               HugePermission.READ);
            if (!sc.isUserInRole(role)) {
                continue;
            }
            try {
                HugeGraph hg = graph(manager, graphSpace, graph);
                HugeConfig config = (HugeConfig) hg.configuration();
                String configResp = ConfigUtil.writeConfigToString(config);
                Map<String, Object> profile =
                        JsonUtil.fromJson(configResp, Map.class);
                profile.put("name", graph);
                profile.put("nickname", hg.nickname());
                if (!isPrefix(profile, prefix)) {
                    continue;
                }
                profile.put("graphspace_nickname", gsNickname);
                
                boolean isDefault = defaultGraphs.containsKey(graph);
                profile.put("default", isDefault);
                if (isDefault) {
                    profile.put("default_update_time", defaultGraphs.get(graph));
                }
                
                Date createTime = hg.createTime();
                if (createTime != null) {
                    LocalDateTime ldt = createTime.toInstant()
                            .atZone(ZoneId.systemDefault()).toLocalDateTime();
                    profile.put("create_time", DATE_FORMATTER.format(ldt));
                }
                
                if (isDefault) {
                    defaultProfiles.add(profile);
                } else {
                    profiles.add(profile);
                }
            } catch (ForbiddenException ignored) {
                // ignore graphs the current user has no access to
            }
        }
        defaultProfiles.addAll(profiles);
        return defaultProfiles;
    }

    private static boolean isPrefix(Map<String, Object> profile, String prefix) {
        if (StringUtils.isEmpty(prefix)) {
            return true;
        }
        // graph name or nickname is not empty
        String name = profile.get("name").toString();
        Object nicknameObj = profile.get("nickname");
        String nickname = nicknameObj != null ? nicknameObj.toString() : "";
        return name.startsWith(prefix) || nickname.startsWith(prefix);
    }

    @GET
    @Timed
    @Path("{name}")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"space_member", "$owner=$name"})
    public Object get(@Context GraphManager manager,
                      @Parameter(description = "The graph space name")
                      @PathParam("graphspace") String graphSpace,
                      @Parameter(description = "The graph name")
                      @PathParam("name") String name) {
        LOG.debug("Get graph by name '{}'", name);

        HugeGraph g = graph(manager, graphSpace, name);
        return ImmutableMap.of("name", g.name(), "backend", g.backend());
    }

    @POST
    @Timed
    @Path("{name}/default")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"space_member", "$owner=$name"})
    public Map<String, Object> setDefault(@Context GraphManager manager,
                                          @Parameter(description = "The graph space name")
                                          @PathParam("graphspace") String graphSpace,
                                          @Parameter(description = "The graph name")
                                          @PathParam("name") String name) {
        LOG.debug("Set default graph '{}' in graph space '{}'", name, graphSpace);
        E.checkArgument(manager.graph(graphSpace, name) != null,
                        "Graph '%s/%s' does not exist", graphSpace, name);
        // Default graph persistence requires PD meta storage.
        // In non-PD (standalone RocksDB) mode, gracefully return empty.
        if (!manager.isPDEnabled()) {
            return ImmutableMap.of("default_graph", Collections.emptySet());
        }
        String user = HugeGraphAuthProxy.username();
        AuthManager authManager = manager.authManager();
        authManager.setDefaultGraph(graphSpace, name, user);
        Map<String, Date> defaults = authManager.getDefaultGraph(graphSpace, user);
        return ImmutableMap.of("default_graph", defaults.keySet());
    }

    @DELETE
    @Timed
    @Path("{name}/default")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"space_member", "$owner=$name"})
    public Map<String, Object> unsetDefault(@Context GraphManager manager,
                                            @Parameter(description = "The graph space name")
                                            @PathParam("graphspace") String graphSpace,
                                            @Parameter(description = "The graph name")
                                            @PathParam("name") String name) {
        LOG.debug("Unset default graph '{}' in graph space '{}'", name, graphSpace);
        E.checkArgument(manager.graph(graphSpace, name) != null,
                        "Graph '%s/%s' does not exist", graphSpace, name);
        // Default graph persistence requires PD meta storage.
        // In non-PD (standalone RocksDB) mode, gracefully return empty.
        if (!manager.isPDEnabled()) {
            return ImmutableMap.of("default_graph", Collections.emptySet());
        }
        String user = HugeGraphAuthProxy.username();
        AuthManager authManager = manager.authManager();
        authManager.unsetDefaultGraph(graphSpace, name, user);
        Map<String, Date> defaults = authManager.getDefaultGraph(graphSpace, user);
        return ImmutableMap.of("default_graph", defaults.keySet());
    }

    @GET
    @Timed
    @Path("default")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"space_member", "$dynamic"})
    public Map<String, Object> getDefault(@Context GraphManager manager,
                                          @Parameter(description = "The graph space name")
                                          @PathParam("graphspace") String graphSpace) {
        LOG.debug("Get default graphs in graph space '{}'", graphSpace);
        // Default graph persistence requires PD meta storage.
        // In non-PD (standalone RocksDB) mode, return empty set.
        if (!manager.isPDEnabled()) {
            return ImmutableMap.of("default_graph", Collections.emptySet());
        }
        String user = HugeGraphAuthProxy.username();
        AuthManager authManager = manager.authManager();
        Map<String, Date> defaults = authManager.getDefaultGraph(graphSpace, user);
        return ImmutableMap.of("default_graph", defaults.keySet());
    }

    @DELETE
    @Timed
    @Path("{name}")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"space"})
    public void drop(@Context GraphManager manager,
                     @Parameter(description = "The graph space name")
                     @PathParam("graphspace") String graphSpace,
                     @Parameter(description = "The graph name")
                     @PathParam("name") String name,
                     @Parameter(description = "Confirmation message to drop the graph")
                     @QueryParam("confirm_message") String message) {
        LOG.debug("Drop graph by name '{}'", name);

        E.checkArgument(CONFIRM_DROP.equals(message),
                        "Please take the message: %s", CONFIRM_DROP);
        manager.dropGraph(graphSpace, name, true);
    }

    @PUT
    @Timed
    @Path("{name}")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"space"})
    public Map<String, String> manage(@Context GraphManager manager,
                                      @Parameter(description = "The graph space name")
                                      @PathParam("graphspace") String graphSpace,
                                      @Parameter(description = "The graph name")
                                      @PathParam("name") String name,
                                      @Parameter(description = "Action map: {'action':'update','update':{...}}")
                                      Map<String, Object> actionMap) {
        LOG.debug("Manage graph '{}' with action '{}'", name, actionMap);
        E.checkArgument(actionMap != null && actionMap.containsKey(GRAPH_ACTION),
                        "Invalid request body '%s'", actionMap);
        Object value = actionMap.get(GRAPH_ACTION);
        E.checkArgument(value instanceof String,
                        "Invalid action type '%s', must be string",
                        value.getClass());
        String action = (String) value;
        switch (action) {
            case UPDATE:
                E.checkArgument(actionMap.containsKey(UPDATE),
                                "Please pass '%s' for graph update",
                                UPDATE);
                value = actionMap.get(UPDATE);
                E.checkArgument(value instanceof Map,
                                "The '%s' must be map, but got %s",
                                UPDATE, value.getClass());
                @SuppressWarnings("unchecked")
                Map<String, Object> graphMap = (Map<String, Object>) value;
                String graphName = (String) graphMap.get("name");
                E.checkArgument(graphName != null && graphName.equals(name),
                                "Different name in update body '%s' with path '%s'",
                                graphName, name);
                HugeGraph exist = graph(manager, graphSpace, name);
                String nickname = (String) graphMap.get("nickname");
                if (!Strings.isEmpty(nickname)) {
                    GraphManager.checkNickname(nickname);
                    E.checkArgument(!manager.isExistedGraphNickname(graphSpace, nickname) ||
                                    nickname.equals(exist.nickname()),
                                    "Nickname '%s' has already existed in graphspace '%s'",
                                    nickname, graphSpace);
                    // Delegate to GraphManager: handles both in-memory update and
                    // PD-mode persistence (non-PD mode is in-memory only).
                    manager.updateGraphNickname(graphSpace, name, nickname);
                }
                return ImmutableMap.of(name, "updated");
            default:
                throw new AssertionError(String.format(
                        "Invalid graph action: '%s'", action));
        }
    }

    @PUT
    @Timed
    @Path("manage")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"analyst"})
    public Object reload(@Context GraphManager manager,
                         @Parameter(
                                 description = "The action map containing 'action'='reload'")
                         Map<String, String> actionMap) {

        LOG.info("[SERVER] Manage graph with action map {}", actionMap);
        E.checkArgument(actionMap != null &&
                        actionMap.containsKey(GRAPH_ACTION),
                        "Please pass '%s' for graphs manage", GRAPH_ACTION);
        String action = actionMap.get(GRAPH_ACTION);
        if (action.equals(GRAPH_ACTION_RELOAD)) {
            manager.reload();
            return ImmutableMap.of("graphs", "reloaded");
        }
        throw new AssertionError(String.format(
                "Invalid graphs action: '%s'", action));
    }

    @POST
    @Timed
    @Path("{name}")
    @StatusFilter.Status(StatusFilter.Status.CREATED)
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"space"})
    public Object create(@Context GraphManager manager,
                         @Parameter(description = "The graph space name")
                         @PathParam("graphspace") String graphSpace,
                         @Parameter(description = "The graph name to create")
                         @PathParam("name") String name,
                         @Parameter(description = "The graph name to clone from (optional)")
                         @QueryParam("clone_graph_name") String clone,
                         @Parameter(
                                 description = "The graph configuration options including " +
                                               "'backend', 'serializer', 'store' and optionally " +
                                               "'description'")
                         Map<String, Object> configs) {
        LOG.debug("Create graph {} with config options '{}' in " +
                  "graph space '{}'", name, configs, graphSpace);
        GraphSpace gs = manager.graphSpace(graphSpace);
        HugeGraph graph;
        E.checkArgumentNotNull(gs, "Not existed graph space: '%s'", graphSpace);

        // Check required parameters for creating graph
        if (StringUtils.isEmpty(clone)) {
            // Only check required parameters when creating new graph, not when cloning
            E.checkArgument(configs != null, "Config parameters cannot be null");
            // Always required by TinkerPop's GraphFactory.open()
            configs.putIfAbsent("gremlin.graph",
                                "org.apache.hugegraph.HugeFactory");
            if (manager.isPDEnabled()) {
                // Auto-fill HStore/PD mode defaults only when in distributed mode
                configs.putIfAbsent("backend", "hstore");
            } else {
                // Auto-fill standalone (RocksDB) mode defaults
                configs.putIfAbsent("backend", "rocksdb");
            }
            configs.putIfAbsent("serializer", "binary");
            // 'store' is safe to default to graph name in both PD and non-PD modes
            configs.putIfAbsent("store", name);
            // Map frontend 'schema' field to backend config key
            Object schema = configs.remove("schema");
            if (schema != null && !schema.toString().isEmpty()) {
                configs.put("schema.init_template", schema.toString());
            }
        }

        String creator = HugeGraphAuthProxy.username();

        if (StringUtils.isNotEmpty(clone)) {
            // Clone from existing graph
            LOG.debug("Clone graph '{}' to '{}' in graph space '{}'", clone, name, graphSpace);
            Map<String, Object> cloneConfigs = configs != null ? configs : new HashMap<>();
            graph = manager.cloneGraph(graphSpace, clone, name, convConfig(cloneConfigs));
        } else {
            // Create new graph
            graph = manager.createGraph(graphSpace, name, creator,
                                        convConfig(configs), true);
        }
        String description = (configs != null) ?
                             (String) configs.get(GRAPH_DESCRIPTION) : null;
        if (description == null) {
            description = Strings.EMPTY;
        }
        Object result = ImmutableMap.of("name", graph.name(),
                                        "nickname", graph.nickname(),
                                        "backend", graph.backend(),
                                        "description", description);
        LOG.info("user [{}] create graph [{}] in graph space [{}] with config " +
                 "[{}]", creator, name, graphSpace, configs);
        return result;
    }

    /**
     * Create graph via text/plain (hugegraph-client compatibility).
     * Client sends: POST /graphspaces/{graphspace}/graphs/{name}
     * with Content-Type: text/plain and body containing JSON config string.
     */
    @POST
    @Timed
    @Path("{name}")
    @StatusFilter.Status(StatusFilter.Status.CREATED)
    @Consumes("text/plain")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"space"})
    public Object createByText(@Context GraphManager manager,
                               @Parameter(description = "The graph space name")
                               @PathParam("graphspace") String graphSpace,
                               @Parameter(description = "The graph name to create")
                               @PathParam("name") String name,
                               @Parameter(description = "The graph name to clone from (optional)")
                               @QueryParam("clone_graph_name") String clone,
                               String configText) {
        LOG.debug("Create graph {} with text config in graph space '{}'",
                  name, graphSpace);
        Map<String, Object> configs = null;
        if (configText != null && !configText.isEmpty()) {
            configs = JsonUtil.fromJson(configText, Map.class);
        }
        return create(manager, graphSpace, name, clone, configs);
    }



    @GET
    @Timed
    @Path("{name}/conf")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"space"})
    public File getConf(@Context GraphManager manager,
                        @Parameter(description = "The graph space name")
                        @PathParam("graphspace") String graphSpace,
                        @Parameter(description = "The graph name")
                        @PathParam("name") String name) {
        LOG.debug("Get graph configuration by name '{}'", name);

        HugeGraph g = graph(manager, graphSpace, name);

        HugeConfig config = (HugeConfig) g.configuration();
        File file = config.file();
        if (file == null) {
            throw new NotSupportedException("Can't access the api in " +
                                            "a node which started with non local file config.");
        }
        return file;
    }

    @DELETE
    @Timed
    @Path("{name}/clear")
    @Consumes(APPLICATION_JSON)
    @RolesAllowed({"space"})
    public void clear(@Context GraphManager manager,
                      @Parameter(description = "The graph space name")
                      @PathParam("graphspace") String graphSpace,
                      @Parameter(description = "The graph name")
                      @PathParam("name") String name,
                      @Parameter(description = "Confirmation message to clear all data, must be: " +
                                               CONFIRM_CLEAR)
                      @QueryParam("confirm_message") String message) {
        LOG.debug("Clear graph by name '{}'", name);

        E.checkArgument(CONFIRM_CLEAR.equals(message),
                        "Please take the message: %s", CONFIRM_CLEAR);
        HugeGraph g = graph(manager, graphSpace, name);
        g.truncateBackend();
    }

    @PUT
    @Timed
    @Path("{name}/snapshot_create")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"space", "$owner=$name"})
    public Object createSnapshot(@Context GraphManager manager,
                                 @Parameter(description = "The graph space name")
                                 @PathParam("graphspace") String graphSpace,
                                 @Parameter(description = "The graph name")
                                 @PathParam("name") String name) {
        LOG.debug("Create snapshot for graph '{}'", name);

        HugeGraph g = graph(manager, graphSpace, name);
        g.createSnapshot();
        return ImmutableMap.of(name, "snapshot_created");
    }

    @PUT
    @Timed
    @Path("{name}/snapshot_resume")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"space", "$owner=$name"})
    public Object resumeSnapshot(@Context GraphManager manager,
                                 @Parameter(description = "The graph space name")
                                 @PathParam("graphspace") String graphSpace,
                                 @Parameter(description = "The graph name")
                                 @PathParam("name") String name) {
        LOG.debug("Resume snapshot for graph '{}'", name);

        HugeGraph g = graph(manager, graphSpace, name);
        g.resumeSnapshot();
        return ImmutableMap.of(name, "snapshot_resumed");
    }

    @PUT
    @Timed
    @Path("{name}/compact")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"space"})
    public String compact(@Context GraphManager manager,
                          @Parameter(description = "The graph space name")
                          @PathParam("graphspace") String graphSpace,
                          @Parameter(description = "The graph name")
                          @PathParam("name") String name) {
        LOG.debug("Manually compact graph '{}'", name);

        HugeGraph g = graph(manager, graphSpace, name);
        return JsonUtil.toJson(g.metadata(null, "compact"));
    }

    @PUT
    @Timed
    @Path("{name}/mode")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"space", "$owner=$name"})
    public Map<String, GraphMode> mode(@Context GraphManager manager,
                                       @Parameter(description = "The graph space name")
                                       @PathParam("graphspace") String graphSpace,
                                       @Parameter(description = "The graph name")
                                       @PathParam("name") String name,
                                       GraphMode mode) {
        LOG.debug("Set mode to: '{}' of graph '{}'", mode, name);

        E.checkArgument(mode != null, "Graph mode can't be null");
        HugeGraph g = graph(manager, graphSpace, name);
        g.mode(mode);
        return ImmutableMap.of("mode", mode);
    }

    @GET
    @Timed
    @Path("{name}/mode")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"space_member", "$owner=$name"})
    public Map<String, GraphMode> mode(@Context GraphManager manager,
                                       @PathParam("graphspace") String graphSpace,
                                       @PathParam("name") String name) {
        LOG.debug("Get mode of graph '{}'", name);

        HugeGraph g = graph(manager, graphSpace, name);
        return ImmutableMap.of("mode", g.mode());
    }

    @PUT
    @Timed
    @Path("{name}/graph_read_mode")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"space"})
    public Map<String, GraphReadMode> graphReadMode(
            @Context GraphManager manager,
            @Parameter(description = "The graph space name")
            @PathParam("graphspace") String graphSpace,
            @Parameter(description = "The graph name")
            @PathParam("name") String name,
            GraphReadMode readMode) {
        LOG.debug("Set graph-read-mode to: '{}' of graph '{}'",
                  readMode, name);

        E.checkArgument(readMode != null,
                        "Graph-read-mode can't be null");
        E.checkArgument(readMode == GraphReadMode.ALL ||
                        readMode == GraphReadMode.OLTP_ONLY,
                        "Graph-read-mode could be ALL or OLTP_ONLY");
        HugeGraph g = graph(manager, graphSpace, name);
        manager.graphReadMode(graphSpace, name, readMode);
        g.readMode(readMode);
        return ImmutableMap.of("graph_read_mode", readMode);
    }

    @GET
    @Timed
    @Path("{name}/graph_read_mode")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"space_member", "$owner=$name"})
    public Map<String, GraphReadMode> graphReadMode(
            @Context GraphManager manager,
            @PathParam("graphspace") String graphSpace,
            @PathParam("name") String name) {
        LOG.debug("Get graph-read-mode of graph '{}'", name);

        HugeGraph g = graph(manager, graphSpace, name);
        return ImmutableMap.of("graph_read_mode", g.readMode());
    }
}
