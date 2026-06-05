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

package org.apache.hugegraph.auth;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.security.sasl.AuthenticationException;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hugegraph.HugeException;
import org.apache.hugegraph.HugeGraphParams;
import org.apache.hugegraph.auth.SchemaDefine.AuthElement;
import org.apache.hugegraph.backend.cache.Cache;
import org.apache.hugegraph.backend.cache.CacheManager;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.id.IdGenerator;
import org.apache.hugegraph.config.AuthOptions;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.meta.MetaManager;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Log;
import org.apache.hugegraph.util.StringEncoding;
import org.slf4j.Logger;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import io.jsonwebtoken.Claims;

//only use in pd mode
public class StandardAuthManagerV2 implements AuthManager {

    public static final String ALL_GRAPHS = "*";
    public static final String ALL_GRAPH_SPACES = "*";
    public static final String DEFAULT_SETTER_ROLE_KEY =
            "_DEFAULT_SETTER_ROLE";
    protected static final Logger LOG = Log.logger(StandardAuthManager.class);
    private static final long AUTH_CACHE_EXPIRE = 10 * 60L;
    private static final long AUTH_CACHE_CAPACITY = 1024 * 10L;
    private static final long AUTH_TOKEN_EXPIRE = 3600 * 24L;
    private static final String DEFAULT_ADMIN_ROLE_KEY = "DEFAULT_ADMIN_ROLE";
    private static final String DEFAULT_ADMIN_TARGET_KEY = "DEFAULT_ADMIN_TARGET";
    // Cache <username, HugeUser>
    private final Cache<Id, HugeUser> usersCache;
    // Cache <userId, passwd>
    private final Cache<Id, String> pwdCache;
    // Cache <token, username>
    private final Cache<Id, String> tokenCache;
    private final TokenGenerator tokenGenerator;
    private final long tokenExpire;
    private Set<String> ipWhiteList;
    private Boolean ipWhiteListEnabled;
    private final MetaManager metaManager = MetaManager.instance();
    private final String graphSpace;

    public StandardAuthManagerV2(HugeGraphParams graph) {
        E.checkNotNull(graph, "graph");
        HugeConfig config = graph.configuration();
        long expired = config.get(AuthOptions.AUTH_CACHE_EXPIRE);
        long capacity = config.get(AuthOptions.AUTH_CACHE_CAPACITY);
        this.tokenExpire = config.get(AuthOptions.AUTH_TOKEN_EXPIRE) * 1000;

        this.graphSpace = graph.graph().graphSpace();

        this.usersCache = this.cache("users", capacity, expired);
        this.pwdCache = this.cache("users_pwd", capacity, expired);
        this.tokenCache = this.cache("token", capacity, expired);

        this.tokenGenerator = new TokenGenerator(config);
        LOG.info("Randomly generate a JWT secret key now");

        this.ipWhiteList = new HashSet<>();

        this.ipWhiteListEnabled = false;
    }

    /**
     * Maybe can define an proxy class to choose forward or call local
     */
    public static boolean isLocal(AuthManager authManager) {
        return authManager instanceof StandardAuthManager;
    }

    /**
     * Update creator from current context (from TaskManager ThreadLocal or direct call)
     */
    private AuthElement updateCreator(AuthElement elem) {
        String username = currentUsername();
        if (username != null && elem.creator() == null) {
            elem.creator(username);
        }
        return elem;
    }

    /**
     * Get current username from TaskManager context
     * The context is set by HugeGraphAuthProxy when API calls are made
     */
    private String currentUsername() {
        // Try to get context from TaskManager ThreadLocal
        String taskContext = org.apache.hugegraph.task.TaskManager.getContext();
        if (taskContext != null && !taskContext.isEmpty()) {
            // Parse username from JSON context
            return parseUsernameFromContext(taskContext);
        }
        return null;
    }

    /**
     * Parse username from context string (JSON format)
     * Context format: {"username":"admin","userId":"xxx",...}
     */
    private String parseUsernameFromContext(String context) {
        try {
            // Simple JSON parsing for username field
            if (context.contains("\"username\"")) {
                int start = context.indexOf("\"username\"");
                int valueStart = context.indexOf(":", start) + 1;
                // Skip whitespace and quote
                while (valueStart < context.length() &&
                       (context.charAt(valueStart) == ' ' || context.charAt(valueStart) == '"')) {
                    valueStart++;
                }
                int valueEnd = context.indexOf("\"", valueStart);
                if (valueEnd > valueStart) {
                    return context.substring(valueStart, valueEnd);
                }
            }
        } catch (Exception e) {
            LOG.warn("Failed to parse username from context: {}", context, e);
        }
        return null;
    }

    @Override
    public void init() {
        this.invalidateUserCache();
    }

    private <V> Cache<Id, V> cache(String prefix, long capacity,
                                   long expiredTime) {
        String name = prefix + "-auth";
        Cache<Id, V> cache = CacheManager.instance().cache(name, capacity);
        if (expiredTime > 0L) {
            cache.expire(Duration.ofSeconds(expiredTime).toMillis());
        } else {
            cache.expire(expiredTime);
        }
        return cache;
    }

    @Override
    public boolean close() {
        return true;
    }

    private void invalidateUserCache() {
        this.usersCache.clear();
    }

    private void invalidatePasswordCache(Id id) {
        this.pwdCache.invalidate(id);
        // Clear all tokenCache because can't get userId in it
        this.tokenCache.clear();
    }

    @Override
    public Id createUser(HugeUser user) {
        Id username = IdGenerator.of(user.name());
        HugeUser existed = this.usersCache.get(username);
        if (existed != null) {
            throw new HugeException("The user name '%s' has existed",
                                    user.name());
        }

        try {
            user.create(user.update());
            this.metaManager.createUser(user);

            // Update cache after successful creation
            this.usersCache.update(username, user);
        } catch (IOException e) {
            throw new HugeException("IOException occurs when " +
                                    "serialize user", e);
        }

        return username;
    }

    @Override
    public Id updateUser(HugeUser user) {
        HugeUser result = null;
        try {
            result = this.metaManager.updateUser(user);
            this.invalidateUserCache();
            this.invalidatePasswordCache(user.id());
            return result.id();
        } catch (IOException e) {
            throw new HugeException("IOException occurs when " +
                                    "serialize user", e);
        }
    }

    public List<String> listGraphSpace() {
        return metaManager.listGraphSpace();
    }

    public List<HugeBelong> listBelongBySource(String graphSpace, Id user,
                                               String link, long limit) {
        try {
            return this.metaManager.listBelongBySource(graphSpace, user, link,
                                                       limit);
        } catch (IOException e) {
            throw new HugeException("IOException occurs when " +
                                    "get belong list by user", e);
        } catch (ClassNotFoundException e) {
            throw new HugeException("ClassNotFoundException occurs when " +
                                    "get belong list by user", e);
        }
    }

    protected void deleteBelongsByUserOrGroup(Id id) {
        // delete role belongs
        List<String> spaces = this.listGraphSpace();
        for (String space : spaces) {
            List<HugeBelong> belongs = this.listBelongBySource(space, id,
                                                               HugeBelong.ALL,
                                                               -1);
            for (HugeBelong belong : belongs) {
                this.deleteBelong(belong.id());
            }
        }

        // delete belongs in * space
        List<HugeBelong> belongsAdmin = this.listBelongBySource(ALL_GRAPH_SPACES,
                                                                id,
                                                                HugeBelong.UR,
                                                                -1);
        List<HugeBelong> belongsSource =
                this.listBelongBySource(ALL_GRAPH_SPACES, id, HugeBelong.UG,
                                        -1);
        List<HugeBelong> belongsTarget =
                this.listBelongByTarget(ALL_GRAPH_SPACES, id, HugeBelong.UG,
                                        -1);

        belongsSource.addAll(belongsAdmin);
        belongsSource.addAll(belongsTarget);
        for (HugeBelong belong : belongsSource) {
            this.deleteBelong(ALL_GRAPH_SPACES, belong.id());
        }
    }

    public List<HugeBelong> listBelongByTarget(String graphSpace, Id target,
                                               String link, long limit) {
        try {
            return this.metaManager.listBelongByTarget(graphSpace, target,
                                                       link, limit);
        } catch (IOException e) {
            throw new HugeException("IOException occurs when " +
                                    "get belong list by role", e);
        } catch (ClassNotFoundException e) {
            throw new HugeException("ClassNotFoundException occurs when " +
                                    "get belong list by role", e);
        }
    }

    @Override
    public HugeUser deleteUser(Id id) {
        if (id.asString().equals("admin")) {
            throw new HugeException("admin could not be removed");
        }

        try {
            HugeUser user = this.findUser(id.asString());
            E.checkArgument(user != null,
                            "The user name '%s' is not existed",
                            id.asString());
            E.checkArgument(!"admin".equals(user.name()),
                            "Delete user '%s' is forbidden", user.name());
            this.deleteBelongsByUserOrGroup(id);
            this.invalidateUserCache();
            this.invalidatePasswordCache(id);
            return this.metaManager.deleteUser(id);
        } catch (IOException e) {
            throw new HugeException("IOException occurs when " +
                                    "deserialize user", e);
        } catch (ClassNotFoundException e) {
            throw new HugeException("ClassNotFoundException occurs when " +
                                    "deserialize user", e);
        }
    }

    @Override
    public HugeUser findUser(String name) {
        Id username = IdGenerator.of(name);
        HugeUser user = this.usersCache.get(username);
        if (user == null) {
            try {
                user = this.metaManager.findUser(name);
                if (user != null) {
                    this.usersCache.update(username, user);
                }
            } catch (IOException e) {
                throw new HugeException("IOException occurs when " +
                                        "deserialize user", e);
            } catch (ClassNotFoundException e) {
                throw new HugeException("ClassNotFoundException occurs when " +
                                        "deserialize user", e);
            }
        }

        return user;
    }

    @Override
    public HugeUser getUser(Id id) {
        HugeUser user = this.findUser(id.asString());
        E.checkArgument(user != null, "The user is not existed");
        return user;
    }

    @Override
    public List<HugeUser> listUsers(List<Id> ids) {
        try {
            return this.metaManager.listUsers(ids);
        } catch (IOException e) {
            throw new HugeException("IOException occurs when " +
                                    "deserialize user", e);
        } catch (ClassNotFoundException e) {
            throw new HugeException("ClassNotFoundException occurs when " +
                                    "deserialize user", e);
        }
    }

    @Override
    public List<HugeUser> listAllUsers(long limit) {
        try {
            return this.metaManager.listAllUsers(limit);
        } catch (IOException e) {
            throw new HugeException("IOException occurs when " +
                                    "deserialize user", e);
        } catch (ClassNotFoundException e) {
            throw new HugeException("ClassNotFoundException occurs when " +
                                    "deserialize user", e);
        }
    }

    @Override
    public Id createGroup(HugeGroup group) {
        try {
            group.create(group.update());
            this.metaManager.createGroup(group);
            Id result = IdGenerator.of(group.name());
            group.id(result);
            this.invalidateUserCache();
            return result;
        } catch (IOException e) {
            throw new HugeException("IOException occurs when " +
                                    "serialize group", e);
        }
    }

    @Override
    public Id updateGroup(HugeGroup group) {
        try {
            group.create(group.update());
            HugeGroup result = this.metaManager.updateGroup(group);
            this.invalidateUserCache();
            return result.id();
        } catch (IOException e) {
            throw new HugeException("IOException occurs when " +
                                    "serialize group", e);
        }
    }

    @Override
    public HugeGroup deleteGroup(Id id) {
        try {
            this.deleteBelongsByUserOrGroup(id);
            HugeGroup result = this.metaManager.deleteGroup(id);
            this.invalidateUserCache();
            return result;
        } catch (IOException e) {
            throw new HugeException("IOException occurs when " +
                                    "deserialize group", e);
        } catch (ClassNotFoundException e) {
            throw new HugeException("ClassNotFoundException occurs when " +
                                    "deserialize group", e);
        }
    }

    @Override
    public HugeGroup getGroup(Id id) {
        try {
            HugeGroup result = this.metaManager.findGroup(id.asString());
            this.invalidateUserCache();
            return result;
        } catch (IOException e) {
            throw new HugeException("IOException occurs when " +
                                    "deserialize group", e);
        } catch (ClassNotFoundException e) {
            throw new HugeException("ClassNotFoundException occurs when " +
                                    "deserialize group", e);
        }
    }

    @Override
    public List<HugeGroup> listGroups(List<Id> ids) {
        List<HugeGroup> groups = new ArrayList<>();
        for (Id id : ids) {
            HugeGroup group = this.findGroup(id.asString());
            if (group != null) {
                groups.add(group);
            }
        }
        this.invalidateUserCache();
        return groups;
    }

    @Override
    public List<HugeGroup> listAllGroups(long limit) {
        try {
            List<HugeGroup> result = this.metaManager.listGroups(limit);
            this.invalidateUserCache();
            return result;
        } catch (IOException e) {
            throw new HugeException("IOException occurs when " +
                                    "deserialize group", e);
        } catch (ClassNotFoundException e) {
            throw new HugeException("ClassNotFoundException occurs when " +
                                    "deserialize group", e);
        }
    }

    @Override
    public Id createTarget(HugeTarget target) {
        try {
            target.create(target.update());
            Id result = this.metaManager.createTarget(graphSpace, target);
            this.invalidateUserCache();
            return result;
        } catch (IOException e) {
            throw new HugeException("IOException occurs when " +
                                    "serialize target", e);
        }
    }

    @Override
    public Id updateTarget(HugeTarget target) {
        try {
            HugeTarget result = this.metaManager.updateTarget(graphSpace, target);
            this.invalidateUserCache();
            return result.id();
        } catch (IOException e) {
            throw new HugeException("IOException occurs when " +
                                    "serialize target", e);
        }
    }

    @Override
    public HugeTarget deleteTarget(Id id) {
        try {
            List<HugeAccess> accesses = this.listAccessByTarget(id, -1);
            for (HugeAccess access : accesses) {
                this.deleteAccess(access.id());
            }
            HugeTarget target = this.metaManager.deleteTarget(graphSpace, id);
            this.invalidateUserCache();
            return target;
        } catch (IOException e) {
            throw new HugeException("IOException occurs when " +
                                    "deserialize target", e);
        } catch (ClassNotFoundException e) {
            throw new HugeException("ClassNotFoundException occurs when " +
                                    "deserialize target", e);
        }
    }

    @Override
    public HugeTarget getTarget(Id id) {
        return getTarget(this.graphSpace, id);
    }

    public HugeTarget getTarget(String graphSpace, Id id) {
        try {
            return this.metaManager.getTarget(graphSpace, id);
        } catch (IOException e) {
            throw new HugeException("IOException occurs when " +
                                    "deserialize target", e);
        } catch (ClassNotFoundException e) {
            throw new HugeException("ClassNotFoundException occurs when " +
                                    "deserialize target", e);
        }
    }

    @Override
    public List<HugeTarget> listTargets(List<Id> ids) {
        try {
            return this.metaManager.listTargets(graphSpace, ids);
        } catch (IOException e) {
            throw new HugeException("IOException occurs when " +
                                    "deserialize target", e);
        } catch (ClassNotFoundException e) {
            throw new HugeException("ClassNotFoundException occurs when " +
                                    "deserialize target", e);
        }
    }

    @Override
    public List<HugeTarget> listAllTargets(long limit) {
        try {
            return this.metaManager.listAllTargets(graphSpace, limit);
        } catch (IOException e) {
            throw new HugeException("IOException occurs when " +
                                    "deserialize target", e);
        } catch (ClassNotFoundException e) {
            throw new HugeException("ClassNotFoundException occurs when " +
                                    "deserialize target", e);
        }
    }

    @Override
    public Id createBelong(HugeBelong belong) {
        try {
            belong.create(belong.update());
            this.invalidateUserCache();
            return this.metaManager.createBelong(graphSpace, belong);
        } catch (IOException e) {
            throw new HugeException("IOException occurs when " +
                                    "create belong", e);
        } catch (ClassNotFoundException e) {
            throw new HugeException("ClassNotFoundException occurs when " +
                                    "create belong", e);
        }
    }

    @Override
    public Id updateBelong(HugeBelong belong) {
        try {
            HugeBelong result = this.metaManager.updateBelong(graphSpace, belong);
            this.invalidateUserCache();
            return result.id();
        } catch (IOException e) {
            throw new HugeException("IOException occurs when " +
                                    "update belong", e);
        } catch (ClassNotFoundException e) {
            throw new HugeException("ClassNotFoundException occurs when " +
                                    "update belong", e);
        }
    }

    @Override
    public HugeBelong deleteBelong(Id id) {
        return this.deleteBelong(this.graphSpace, id);
    }

    public HugeBelong deleteBelong(String graphSpace, Id id) {
        try {
            HugeBelong result = this.metaManager.deleteBelong(graphSpace, id);
            this.invalidateUserCache();
            return result;
        } catch (IOException e) {
            throw new HugeException("IOException occurs when " +
                                    "delete belong", e);
        } catch (ClassNotFoundException e) {
            throw new HugeException("ClassNotFoundException occurs when " +
                                    "delete belong", e);
        }
    }

    @Override
    public HugeBelong getBelong(Id id) {
        try {
            return this.metaManager.getBelong(graphSpace, id);
        } catch (IOException e) {
            throw new HugeException("IOException occurs when " +
                                    "get belong", e);
        } catch (ClassNotFoundException e) {
            throw new HugeException("ClassNotFoundException occurs when " +
                                    "get belong", e);
        }
    }

    @Override
    public List<HugeBelong> listBelong(List<Id> ids) {
        try {
            return this.metaManager.listBelong(graphSpace, ids);
        } catch (IOException e) {
            throw new HugeException("IOException occurs when " +
                                    "get belong list by ids", e);
        } catch (ClassNotFoundException e) {
            throw new HugeException("ClassNotFoundException occurs when " +
                                    "get belong list by ids", e);
        }
    }

    @Override
    public List<HugeBelong> listAllBelong(long limit) {
        try {
            return this.metaManager.listAllBelong(graphSpace, limit);
        } catch (IOException e) {
            throw new HugeException("IOException occurs when " +
                                    "get all belong list", e);
        } catch (ClassNotFoundException e) {
            throw new HugeException("ClassNotFoundException occurs when " +
                                    "get all belong list", e);
        }
    }

    @Override
    public List<HugeBelong> listBelongByUser(Id user, long limit) {
        try {
            return this.metaManager.listBelongBySource(this.graphSpace, user, "*", limit);
        } catch (IOException e) {
            throw new HugeException("IOException occurs when " +
                                    "list belong by user", e);
        } catch (ClassNotFoundException e) {
            throw new HugeException("ClassNotFoundException occurs when " +
                                    "list belong by user", e);
        }
    }

    @Override
    public List<HugeBelong> listBelongByGroup(Id role, long limit) {
        try {
            return this.metaManager.listBelongByTarget(this.graphSpace, role, "*", limit);
        } catch (IOException e) {
            throw new HugeException("IOException occurs when " +
                                    "list belong by user", e);
        } catch (ClassNotFoundException e) {
            throw new HugeException("ClassNotFoundException occurs when " +
                                    "list belong by user", e);
        }
    }

    @Override
    public Id createAccess(HugeAccess access) {
        try {
            access.create(access.update());
            Id result = this.metaManager.createAccess(graphSpace, access);
            this.invalidateUserCache();
            return result;
        } catch (IOException e) {
            throw new HugeException("IOException occurs when " +
                                    "create access", e);
        } catch (ClassNotFoundException e) {
            throw new HugeException("ClassNotFoundException occurs when " +
                                    "create access", e);
        }
    }

    @Override
    public Id updateAccess(HugeAccess access) {
        HugeAccess result = null;
        try {
            result = this.metaManager.updateAccess(graphSpace, access);
            this.invalidateUserCache();
            return result.id();
        } catch (IOException e) {
            throw new HugeException("IOException occurs when " +
                                    "update access", e);
        } catch (ClassNotFoundException e) {
            throw new HugeException("ClassNotFoundException occurs when " +
                                    "update access", e);
        }
    }

    @Override
    public HugeAccess deleteAccess(Id id) {

        try {
            HugeAccess result = this.metaManager.deleteAccess(graphSpace, id);
            this.invalidateUserCache();
            return result;
        } catch (IOException e) {
            throw new HugeException("IOException occurs when " +
                                    "delete access", e);
        } catch (ClassNotFoundException e) {
            throw new HugeException("ClassNotFoundException occurs when " +
                                    "delete access", e);
        }
    }

    @Override
    public HugeAccess getAccess(Id id) {
        try {
            return this.metaManager.getAccess(graphSpace, id);
        } catch (IOException e) {
            throw new HugeException("IOException occurs when " +
                                    "get access", e);
        } catch (ClassNotFoundException e) {
            throw new HugeException("ClassNotFoundException occurs when " +
                                    "get access", e);
        }
    }

    @Override
    public List<HugeAccess> listAccess(List<Id> ids) {
        try {
            return this.metaManager.listAccess(graphSpace, ids);
        } catch (IOException e) {
            throw new HugeException("IOException occurs when " +
                                    "get access list", e);
        } catch (ClassNotFoundException e) {
            throw new HugeException("ClassNotFoundException occurs when " +
                                    "get access list", e);
        }
    }

    @Override
    public List<HugeAccess> listAllAccess(long limit) {
        try {
            return this.metaManager.listAllAccess(graphSpace, limit);
        } catch (IOException e) {
            throw new HugeException("IOException occurs when " +
                                    "get all access list", e);
        } catch (ClassNotFoundException e) {
            throw new HugeException("ClassNotFoundException occurs when " +
                                    "get all access list", e);
        }
    }

    @Override
    public List<HugeAccess> listAccessByGroup(Id group, long limit) {
        try {
            return this.metaManager.listAccessByGroup(graphSpace, group, limit);
        } catch (IOException e) {
            throw new HugeException("IOException occurs when " +
                                    "get access list by group", e);
        } catch (ClassNotFoundException e) {
            throw new HugeException("ClassNotFoundException occurs when " +
                                    "get access list by group", e);
        }
    }

    @Override
    public List<HugeAccess> listAccessByTarget(Id target, long limit) {
        try {
            return this.metaManager.listAccessByTarget(this.graphSpace, target, limit);
        } catch (IOException e) {
            throw new HugeException("IOException occurs when " +
                                    "get access list by target", e);
        } catch (ClassNotFoundException e) {
            throw new HugeException("ClassNotFoundException occurs when " +
                                    "get access list by target", e);
        }
    }

    @Override
    public Id createProject(HugeProject project) {
        E.checkArgument(!StringUtils.isEmpty(project.name()),
                        "The name of project can't be null or empty");
        try {
            // Create project admin group
            if (project.adminGroupId() == null) {
                HugeGroup adminGroup = new HugeGroup("admin_" + project.name());
                adminGroup.creator(project.creator());
                Id adminGroupId = this.createGroup(adminGroup);
                project.adminGroupId(adminGroupId);
            }

            // Create project op group
            if (project.opGroupId() == null) {
                HugeGroup opGroup = new HugeGroup("op_" + project.name());
                opGroup.creator(project.creator());
                Id opGroupId = this.createGroup(opGroup);
                project.opGroupId(opGroupId);
            }

            // Create project target to verify permission
            final String targetName = "project_res_" + project.name();
            HugeResource resource = new HugeResource(ResourceType.PROJECT,
                                                     project.name(),
                                                     null);
            Map<String, List<HugeResource>> defaultResources = new LinkedHashMap<>();
            List<HugeResource> resources = new ArrayList<>();
            resources.add(resource);
            defaultResources.put(ALL_GRAPHS, resources);

            HugeTarget target = new HugeTarget(defaultResources,
                                               targetName,
                                               ALL_GRAPHS,
                                               this.graphSpace);

            target.creator(project.creator());
            Id targetId = this.createTarget(target);
            project.targetId(targetId);

            Id adminGroupId = project.adminGroupId();
            Id opGroupId = project.opGroupId();
            HugeAccess adminGroupWriteAccess = new HugeAccess(this.graphSpace,
                                                              adminGroupId, targetId,
                                                              HugePermission.WRITE);
            adminGroupWriteAccess.creator(project.creator());
            HugeAccess adminGroupReadAccess = new HugeAccess(this.graphSpace,
                                                             adminGroupId, targetId,
                                                             HugePermission.READ);
            adminGroupReadAccess.creator(project.creator());
            HugeAccess opGroupReadAccess = new HugeAccess(this.graphSpace,
                                                          opGroupId, targetId,
                                                          HugePermission.READ);
            opGroupReadAccess.creator(project.creator());
            this.createAccess(adminGroupWriteAccess);
            this.createAccess(adminGroupReadAccess);
            this.createAccess(opGroupReadAccess);

            project.create(project.update());
            return this.metaManager.createProject(this.graphSpace, project);
        } catch (Exception e) {
            LOG.error("Exception occurred when trying to create project", e);
            throw new HugeException("Exception occurs when create project", e);
        }
    }

    @Override
    public HugeProject deleteProject(Id id) {
        try {
            HugeProject oldProject = this.metaManager.getProject(this.graphSpace, id);
            // 检查是否有图绑定到此项目
            if (!CollectionUtils.isEmpty(oldProject.graphs())) {
                String errInfo = String.format("Can't delete project '%s' " +
                                               "that contains any graph, " +
                                               "there are graphs bound " +
                                               "to it", id);
                throw new HugeException(errInfo);
            }
            HugeProject project = this.metaManager.deleteProject(this.graphSpace, id);
            this.deleteGroup(project.adminGroupId());
            this.deleteGroup(project.opGroupId());
            this.deleteTarget(project.targetId());
            return project;
        } catch (Exception e) {
            throw new HugeException("Exception occurs when delete project", e);
        }
    }

    @Override
    public Id updateProject(HugeProject project) {
        try {
            HugeProject result = this.metaManager.updateProject(this.graphSpace, project);
            return result.id();
        } catch (Exception e) {
            throw new HugeException("Exception occurs when update project", e);
        }
    }

    @Override
    public Id projectAddGraphs(Id id, Set<String> graphs) {
        E.checkArgument(!CollectionUtils.isEmpty(graphs),
                        "Failed to add graphs to project '%s', the graphs " +
                        "parameter can't be empty", id);
        try {
            HugeProject project = this.metaManager.getProject(this.graphSpace, id);
            Set<String> sourceGraphs = new HashSet<>(project.graphs());
            int oldSize = sourceGraphs.size();
            sourceGraphs.addAll(graphs);
            if (sourceGraphs.size() == oldSize) {
                return id;
            }
            project.graphs(sourceGraphs);
            HugeProject result = this.metaManager.updateProject(this.graphSpace, project);
            return result.id();
        } catch (Exception e) {
            throw new HugeException("Exception occurs when add graphs to project", e);
        }
    }

    @Override
    public Id projectRemoveGraphs(Id id, Set<String> graphs) {
        E.checkArgumentNotNull(id,
                               "Failed to remove graphs, the project id " +
                               "parameter can't be null");
        E.checkArgument(!CollectionUtils.isEmpty(graphs),
                        "Failed to delete graphs from the project '%s', " +
                        "the graphs parameter can't be null or empty", id);
        try {
            HugeProject project = this.metaManager.getProject(this.graphSpace, id);
            Set<String> sourceGraphs = new HashSet<>(project.graphs());
            int oldSize = sourceGraphs.size();
            sourceGraphs.removeAll(graphs);
            if (sourceGraphs.size() == oldSize) {
                return id;
            }
            project.graphs(sourceGraphs);
            HugeProject result = this.metaManager.updateProject(this.graphSpace, project);
            return result.id();
        } catch (Exception e) {
            throw new HugeException("Exception occurs when remove graphs from project", e);
        }
    }

    @Override
    public HugeProject getProject(Id id) {
        try {
            return this.metaManager.getProject(this.graphSpace, id);
        } catch (Exception e) {
            throw new HugeException("Exception occurs when get project", e);
        }
    }

    @Override
    public List<HugeProject> listAllProject(long limit) {
        try {
            return this.metaManager.listAllProjects(this.graphSpace, limit);
        } catch (Exception e) {
            throw new HugeException("Exception occurs when list all projects", e);
        }
    }

    @Override
    public HugeUser matchUser(String name, String password) {
        E.checkArgumentNotNull(name, "User name can't be null");
        E.checkArgumentNotNull(password, "User password can't be null");

        HugeUser user = this.findUser(name);
        if (user == null) {
            return null;
        }

        if (password.equals(this.pwdCache.get(user.id()))) {
            return user;
        }

        if (StringEncoding.checkPassword(password, user.password())) {
            // TODO: rehash password if bcrypt work factor is lower than expected
            this.pwdCache.update(user.id(), password);
            return user;
        }
        return null;
    }

    @Override
    public RolePermission rolePermission(AuthElement element) {
        if (element instanceof HugeUser) {
            return this.rolePermission((HugeUser) element);
        } else if (element instanceof HugeTarget) {
            return this.rolePermission((HugeTarget) element);
        }

        List<HugeAccess> accesses = new ArrayList<>();
        if (element instanceof HugeBelong) {
            HugeBelong belong = (HugeBelong) element;
            accesses.addAll(this.listAccessByGroup(belong.target(), -1));
        } else if (element instanceof HugeGroup) {
            HugeGroup group = (HugeGroup) element;
            accesses.addAll(this.listAccessByGroup(group.id(), -1));
        } else if (element instanceof HugeAccess) {
            HugeAccess access = (HugeAccess) element;
            accesses.add(access);
        } else {
            E.checkArgument(false, "Invalid type for role permission: %s",
                            element);
        }

        return this.rolePermission(accesses);
    }

    private RolePermission rolePermission(HugeUser user) {
        if (user.role() != null && user.role().map() != null &&
            user.role().map().size() != 0) {
            // Return cached role (40ms => 10ms)
            return user.role();
        }

        // Collect accesses by user
        RolePermission role = (isAdminManager(user.name())) ?
                              RolePermission.admin() : new RolePermission();
        // If user is admin, return admin role directly
        if (isAdminManager(user.name())) {
            user.role(role);
            this.usersCache.update(IdGenerator.of(user.name()), user);
            return role;
        }

        // For non-admin users, check if user.id() is null
        if (user.id() == null) {
            // If user id is null, this might be a new user being created
            // Return empty role permission for now
            user.role(role);
            return RolePermission.none();
        }

        List<String> graphSpaces = this.listGraphSpace();
        List<HugeGroup> groups = this.listGroupsByUser(user.name(), -1);
        for (String graphSpace : graphSpaces) {
            List<HugeBelong> belongs = this.listBelongBySource(graphSpace,
                                                               user.id(),
                                                               HugeBelong.ALL,
                                                               -1);
            for (HugeGroup group : groups) {
                List<HugeBelong> belongsG =
                        this.listBelongBySource(graphSpace, group.id(),
                                                HugeBelong.ALL, -1);
                belongs.addAll(belongsG);
            }
            for (HugeBelong belong : belongs) {
                List<HugeAccess> accesses = this.listAccessByRole(graphSpace,
                                                                  belong.target(), -1);
                for (HugeAccess access : accesses) {
                    HugePermission accessPerm = access.permission();
                    HugeTarget target = this.getTarget(graphSpace, access.target());
                    role.add(graphSpace, target.graph(),
                             accessPerm, target.resources());
                }
            }
        }

        user.role(role);
        return role;
    }

    public List<HugeAccess> listAccessByRole(String graphSpace, Id role,
                                             long limit) {
        try {
            return this.metaManager.listAccessByRole(graphSpace, role,
                                                     limit);
        } catch (IOException e) {
            throw new HugeException("IOException occurs when " +
                                    "get access list by role", e);
        } catch (ClassNotFoundException e) {
            throw new HugeException("ClassNotFoundException occurs when " +
                                    "get access list by role", e);
        }
    }

    public List<HugeGroup> listGroupsByUser(String user, long limit) {
        try {
            List<HugeBelong> belongs =
                    this.metaManager.listBelongBySource(ALL_GRAPH_SPACES,
                                                        IdGenerator.of(user),
                                                        HugeBelong.UG, limit);

            List<HugeGroup> result = new ArrayList<>();
            for (HugeBelong belong : belongs) {
                result.add(this.metaManager.findGroup(
                        belong.target().asString()));
            }

            return result;
        } catch (IOException e) {
            throw new HugeException("IOException occurs when " +
                                    "get group list by user", e);
        } catch (ClassNotFoundException e) {
            throw new HugeException("ClassNotFoundException occurs when " +
                                    "get group list by user", e);
        }
    }

    private RolePermission rolePermission(List<HugeAccess> accesses) {
        // Mapping of: graph -> action -> resource
        RolePermission role = new RolePermission();
        for (HugeAccess access : accesses) {
            HugePermission accessPerm = access.permission();
            HugeTarget target = this.getTarget(access.graphSpace(),
                                               access.target());
            role.add(target.graphSpace(), target.graph(),
                     accessPerm, target.resources());
        }
        return role;
    }

    private RolePermission rolePermission(HugeTarget target) {
        RolePermission role = new RolePermission();
        // TODO: improve for the actual meaning
        role.add(target.graphSpace(), target.graph(), HugePermission.READ, target.resources());
        return role;
    }

    @Override
    public String loginUser(String username, String password)
            throws AuthenticationException {
        HugeUser user = this.matchUser(username, password);
        if (user == null) {
            String msg = "Incorrect username or password";
            throw new AuthenticationException(msg);
        }

        Map<String, ?> payload =
                ImmutableMap.of(AuthConstant.TOKEN_USER_NAME,
                                username,
                                AuthConstant.TOKEN_USER_ID,
                                user.id.asString());
        String token = this.tokenGenerator.create(payload, this.tokenExpire);
        this.tokenCache.update(IdGenerator.of(token), username);
        return token;
    }

    // TODO: the expire haven't been implemented yet
    @Override
    public String loginUser(String username, String password, long expire)
            throws AuthenticationException {
        HugeUser user = this.matchUser(username, password);
        if (user == null) {
            String msg = "Incorrect username or password";
            throw new AuthenticationException(msg);
        }

        Map<String, ?> payload = ImmutableMap.of(AuthConstant.TOKEN_USER_NAME,
                                                 username,
                                                 AuthConstant.TOKEN_USER_ID,
                                                 user.id.asString());
        String token = this.tokenGenerator.create(payload, this.tokenExpire);

        this.tokenCache.update(IdGenerator.of(token), username);
        return token;
    }

    @Override
    public void logoutUser(String token) {
        this.tokenCache.invalidate(IdGenerator.of(token));
    }

    @Override
    public UserWithRole validateUser(String username, String password) {
        HugeUser user = this.matchUser(username, password);
        if (user == null) {
            return new UserWithRole(username);
        }
        return new UserWithRole(user.id, username, this.rolePermission(user));
    }

    @Override
    public UserWithRole validateUser(String token) {
        String username = this.tokenCache.get(IdGenerator.of(token));

        Claims payload = null;
        boolean needBuildCache = false;
        if (username == null) {
            try {
                payload = this.tokenGenerator.verify(token);
            } catch (Throwable t) {
                LOG.error(String.format("Failed to verify token:[ %s ], cause:", token), t);
                return new UserWithRole("");
            }
            username = (String) payload.get(AuthConstant.TOKEN_USER_NAME);
            needBuildCache = true;
        }

        HugeUser user = this.findUser(username);
        if (user == null) {
            return new UserWithRole(username);
        } else if (needBuildCache) {
            long expireAt = payload.getExpiration().getTime();
            long bornTime = this.tokenCache.expire() -
                            (expireAt - System.currentTimeMillis());
            this.tokenCache.update(IdGenerator.of(token), username,
                                   Math.negateExact(bornTime));
        }

        return new UserWithRole(user.id(), username, this.rolePermission(user));
    }

    @Override
    public Set<String> listWhiteIPs() {
        return ipWhiteList;
    }

    @Override
    public void setWhiteIPs(Set<String> ipWhiteList) {
        this.ipWhiteList = ipWhiteList;
    }

    @Override
    public boolean getWhiteIpStatus() {
        return this.ipWhiteListEnabled;
    }

    @Override
    public void enabledWhiteIpList(boolean status) {
        this.ipWhiteListEnabled = status;
    }

    @Override
    public Id createSpaceManager(String graphSpace, String user) {
        String role = HugeDefaultRole.SPACE.toString();
        try {
            HugeBelong belong;
            if (HugeGroup.isGroup(user)) {
                belong = new HugeBelong(
                        graphSpace, null, IdGenerator.of(user),
                        IdGenerator.of(role),
                        HugeBelong.GR);
            } else {
                belong = new HugeBelong(
                        graphSpace, IdGenerator.of(user), null,
                        IdGenerator.of(role),
                        HugeBelong.UR);
            }

            this.tryInitDefaultRole(graphSpace,
                                    role,
                                    ALL_GRAPHS);
            // Set creator from current context
            this.updateCreator(belong);
            belong.create(belong.update());
            Id result = this.metaManager.createBelong(graphSpace, belong);
            this.invalidateUserCache();
            return result;
        } catch (Exception e) {
            throw new HugeException("Exception occurs when " +
                                    "create space manager", e);
        }
    }

    @Override
    public void deleteSpaceManager(String graphSpace, String user) {
        try {
            String belongId =
                    this.metaManager.belongId(
                            user, HugeDefaultRole.SPACE.toString());
            this.metaManager.deleteBelong(graphSpace,
                                          IdGenerator.of(belongId));
            this.invalidateUserCache();
        } catch (Exception e) {
            throw new HugeException("Exception occurs when " +
                                    "delete space manager", e);
        }
    }

    @Override
    public List<String> listSpaceManager(String graphSpace) {
        List<String> spaceManagers = new ArrayList<>();
        try {
            List<HugeBelong> belongs =
                    this.metaManager.listBelongByTarget(
                            graphSpace, IdGenerator.of(
                                    HugeDefaultRole.SPACE.toString()),
                            HugeBelong.ALL, -1);
            for (HugeBelong belong : belongs) {
                spaceManagers.add(belong.source().asString());
            }
        } catch (Exception e) {
            throw new HugeException("Exception occurs when " +
                                    "list space manager", e);
        }
        return spaceManagers;
    }

    @Override
    public boolean isSpaceManager(String user) {
        List<String> spaces = this.listGraphSpace();
        for (String space : spaces) {
            if (isSpaceManager(space, user)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean isSpaceManager(String graphSpace, String user) {
        try {
            if (existedInGroup(graphSpace, user, HugeDefaultRole.SPACE)) {
                return true;
            }
            String belongId = this.metaManager.belongId(user, HugeDefaultRole.SPACE.toString());
            return this.metaManager.existBelong(graphSpace, IdGenerator.of(belongId));
        } catch (Exception e) {
            throw new HugeException("Exception occurs when check if is space manager.", e);
        }
    }

    private boolean existedInGroup(String graphSpace, String user,
                                   HugeDefaultRole hugeDefaultRole) {
        List<HugeGroup> groups = this.listGroupsByUser(user, -1);
        for (HugeGroup group : groups) {
            String belongIdG =
                    this.metaManager.belongId(group.name(),
                                              hugeDefaultRole.toString(),
                                              HugeBelong.GR);
            if (this.metaManager.existBelong(graphSpace, IdGenerator.of(belongIdG))) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Id createSpaceMember(String graphSpace, String user) {
        String role = HugeDefaultRole.SPACE_MEMBER.toString();
        try {
            HugeBelong belong;
            if (HugeGroup.isGroup(user)) {
                belong = new HugeBelong(
                        graphSpace, null, IdGenerator.of(user),
                        IdGenerator.of(role),
                        HugeBelong.GR);
            } else {
                belong = new HugeBelong(
                        graphSpace, IdGenerator.of(user), null,
                        IdGenerator.of(role),
                        HugeBelong.UR);
            }

            this.tryInitDefaultRole(graphSpace, role, ALL_GRAPHS);

            // Set creator from current context
            this.updateCreator(belong);
            belong.create(belong.update());
            Id result = this.metaManager.createBelong(graphSpace, belong);
            this.invalidateUserCache();
            return result;
        } catch (Exception e) {
            throw new HugeException("Exception occurs when create space member", e);
        }
    }

    @Override
    public void deleteSpaceMember(String graphSpace, String user) {
        try {
            String belongId =
                    this.metaManager.belongId(user, HugeDefaultRole.SPACE_MEMBER.toString());
            this.metaManager.deleteBelong(graphSpace, IdGenerator.of(belongId));
            this.invalidateUserCache();
        } catch (Exception e) {
            throw new HugeException("Exception occurs when delete space member", e);
        }
    }

    @Override
    public List<String> listSpaceMember(String graphSpace) {
        List<String> spaceManagers = new ArrayList<>();
        try {
            List<HugeBelong> belongs =
                    this.metaManager.listBelongByTarget(graphSpace,
                                                        IdGenerator.of(
                                                                HugeDefaultRole.SPACE_MEMBER.toString()),
                                                        HugeBelong.ALL, -1);
            for (HugeBelong belong : belongs) {
                spaceManagers.add(belong.source().asString());
            }
        } catch (Exception e) {
            throw new HugeException("Exception occurs when list space manager", e);
        }
        return spaceManagers;
    }

    @Override
    public boolean isSpaceMember(String graphSpace, String user) {
        try {
            if (existedInGroup(graphSpace, user,
                               HugeDefaultRole.SPACE_MEMBER)) {
                return true;
            }

            String belongId =
                    this.metaManager.belongId(user, HugeDefaultRole.SPACE_MEMBER.toString());
            return this.metaManager.existBelong(graphSpace, IdGenerator.of(belongId));
        } catch (Exception e) {
            throw new HugeException("Fail call isSpaceMember method", e);
        }
    }

    @Override
    public Id createAdminManager(String user) {
        try {
            HugeBelong belong = new HugeBelong(ALL_GRAPH_SPACES,
                                               IdGenerator.of(user),
                                               IdGenerator.of(DEFAULT_ADMIN_ROLE_KEY));
            this.tryInitAdminRole();
            this.updateCreator(belong);
            belong.create(belong.update());
            Id result = this.metaManager.createBelong(ALL_GRAPH_SPACES, belong);
            this.invalidateUserCache();
            return result;
        } catch (Exception e) {
            throw new HugeException("Exception occurs when " +
                                    "create space op manager", e);
        }
    }

    @Override
    public void deleteAdminManager(String user) {
        try {
            String belongId =
                    this.metaManager.belongId(user,
                                              DEFAULT_ADMIN_ROLE_KEY);
            this.metaManager.deleteBelong(ALL_GRAPH_SPACES,
                                          IdGenerator.of(belongId));
            this.invalidateUserCache();
        } catch (Exception e) {
            throw new HugeException("Exception occurs when " +
                                    "delete space op manager", e);
        }
    }

    @Override
    public List<String> listAdminManager() {
        Set<String> adminManagers = new HashSet<>();
        try {
            List<HugeBelong> belongs =
                    this.metaManager.listBelongByTarget(
                            ALL_GRAPH_SPACES,
                            IdGenerator.of(DEFAULT_ADMIN_ROLE_KEY),
                            HugeBelong.ALL, -1);
            for (HugeBelong belong : belongs) {
                adminManagers.add(belong.source().asString());
            }
        } catch (Exception e) {
            throw new HugeException("Exception occurs when " +
                                    "list admin manager", e);
        }

        // Add DEFAULT admin
        adminManagers.add("admin");

        return new ArrayList<>(adminManagers);
    }

    @Override
    public boolean isAdminManager(String user) {
        if ("admin".equals(user)) {
            return true;
        }

        try {
            String belongId =
                    this.metaManager.belongId(user, DEFAULT_ADMIN_ROLE_KEY);
            return this.metaManager.existBelong(ALL_GRAPH_SPACES,
                                                IdGenerator.of(belongId));
        } catch (Exception e) {
            throw new HugeException("Exception occurs when " +
                                    "check whether is manager", e);
        }
    }

    private void tryInitAdminRole() {
        try {
            HugeRole role = this.metaManager.findRole(ALL_GRAPH_SPACES,
                                                      IdGenerator.of(
                                                              DEFAULT_ADMIN_ROLE_KEY));
            if (role == null) {
                role = new HugeRole(DEFAULT_ADMIN_ROLE_KEY,
                                    ALL_GRAPH_SPACES);
                role.nickname("system-admin");
                this.updateCreator(role);
                role.create(role.update());
                this.metaManager.createRole(ALL_GRAPH_SPACES, role);
            }

            HugeTarget target = this.metaManager.findTarget(ALL_GRAPH_SPACES,
                                                            IdGenerator.of(
                                                                    DEFAULT_ADMIN_TARGET_KEY));
            if (target == null) {
                target = new HugeTarget(DEFAULT_ADMIN_TARGET_KEY,
                                        ALL_GRAPH_SPACES, ALL_GRAPHS);
                this.updateCreator(target);
                target.create(target.update());
                this.metaManager.createTarget(ALL_GRAPH_SPACES, target);
            }

            String accessId =
                    this.metaManager.accessId(DEFAULT_ADMIN_ROLE_KEY,
                                              DEFAULT_ADMIN_TARGET_KEY,
                                              HugePermission.ADMIN);
            HugeAccess access = this.metaManager.findAccess(ALL_GRAPH_SPACES,
                                                            IdGenerator.of(accessId));
            if (access == null) {
                access = new HugeAccess(ALL_GRAPH_SPACES,
                                        IdGenerator.of(DEFAULT_ADMIN_ROLE_KEY),
                                        IdGenerator.of(DEFAULT_ADMIN_TARGET_KEY),
                                        HugePermission.ADMIN);
                this.updateCreator(access);
                access.create(access.update());
                this.metaManager.createAccess(ALL_GRAPH_SPACES, access);
            }
        } catch (Exception e) {
            throw new HugeException("Exception occurs when " +
                                    "init space op manager role", e);
        }
    }

    @Override
    public HugeGroup findGroup(String name) {
        HugeGroup result = null;
        try {
            result = this.metaManager.findGroup(name);
        } catch (IOException e) {
            throw new HugeException("IOException occurs when " +
                                    "deserialize group", e);
        } catch (ClassNotFoundException e) {
            throw new HugeException("ClassNotFoundException occurs when " +
                                    "deserialize group", e);
        }
        return result;
    }

    private void tryInitDefaultRole(String graphSpace,
                                    String roleName,
                                    String graph) {
        try {
            HugeRole role = this.metaManager.findRole(
                    graphSpace, IdGenerator.of(roleName));
            if (role == null) {
                role = new HugeRole(roleName, graphSpace);
                role.nickname(HugeDefaultRole.getNickname(roleName));
                this.updateCreator(role);
                role.create(role.update());
                this.metaManager.createRole(graphSpace, role);
            }

            String targetName = (ALL_GRAPHS.equals(graph)) ?
                                HugeDefaultRole.DEFAULT_SPACE_TARGET_KEY :
                                getGraphTargetName(graph);
            String description = (ALL_GRAPHS.equals(graph)) ?
                                 "All resources in graph space" : graph + "-All resources in graph";
            HugeTarget target = this.metaManager.findTarget(
                    graphSpace, IdGenerator.of(targetName));
            if (target == null) {
                Map<String, List<HugeResource>> spaceResources =
                        new HashMap<>();
                spaceResources.put("ALL", ImmutableList.of(
                        new HugeResource(ResourceType.ALL, null, null)));
                target = new HugeTarget(spaceResources, targetName,
                                        graph, graphSpace
                );
                target.description(description);
                this.updateCreator(target);
                target.create(target.update());
                this.metaManager.createTarget(graphSpace, target);
            }

            createDefaultAccesses(graphSpace, roleName, targetName);
        } catch (Exception e) {
            throw new HugeException("Exception occurs when " +
                                    "init space default role", e);
        }
    }

    public String getGraphTargetName(String graph) {
        return graph + "_" + HugeDefaultRole.DEFAULT_SPACE_TARGET_KEY;
    }

    private void createDefaultAccesses(String graphSpace, String role,
                                       String targetName)
            throws IOException, ClassNotFoundException {
        List<HugePermission> perms;
        if (HugeDefaultRole.SPACE.toString().equals(role)) {
            perms = List.of(HugePermission.SPACE);
        } else if (HugeDefaultRole.SPACE_MEMBER.toString().equals(role)) {
            perms = List.of(HugePermission.SPACE_MEMBER);
        } else if (HugeDefaultRole.ANALYST.toString().equals(role)) {
            perms = Arrays.asList(HugePermission.READ, HugePermission.WRITE,
                                  HugePermission.DELETE, HugePermission.EXECUTE);
        } else if (HugeDefaultRole.isObserver(role)) {
            perms = List.of(HugePermission.READ);
        } else {
            throw new HugeException("Unsupported default role");
        }

        for (HugePermission perm : perms) {
            String accessId = this.metaManager.accessId(role, targetName, perm);
            HugeAccess access =
                    this.metaManager.findAccess(graphSpace, IdGenerator.of(accessId));
            if (access == null) {
                access = new HugeAccess(graphSpace, IdGenerator.of(role),
                                        IdGenerator.of(targetName), perm);
                this.updateCreator(access);
                access.create(access.update());
                this.metaManager.createAccess(graphSpace, access);
            }
        }
    }
}
