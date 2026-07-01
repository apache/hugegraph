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

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

import javax.security.sasl.AuthenticationException;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hugegraph.HugeException;
import org.apache.hugegraph.HugeGraphParams;
import org.apache.hugegraph.auth.HugeUser.P;
import org.apache.hugegraph.auth.SchemaDefine.AuthElement;
import org.apache.hugegraph.backend.cache.Cache;
import org.apache.hugegraph.backend.cache.CacheManager;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.id.IdGenerator;
import org.apache.hugegraph.config.AuthOptions;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.type.define.Directions;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.LockUtil;
import org.apache.hugegraph.util.Log;
import org.apache.hugegraph.util.StringEncoding;
import org.slf4j.Logger;

import com.google.common.collect.ImmutableMap;

import io.jsonwebtoken.Claims;
import jakarta.ws.rs.ForbiddenException;

public class StandardAuthManager implements AuthManager {

    protected static final Logger LOG = Log.logger(StandardAuthManager.class);

    private final HugeGraphParams graph;

    // Cache <username, HugeUser>
    private final Cache<Id, HugeUser> usersCache;
    // Cache <userId, passwd>
    private final Cache<Id, String> pwdCache;
    // Cache <token, username>
    private final Cache<Id, String> tokenCache;

    private final EntityManager<HugeUser> users;
    private final EntityManager<HugeGroup> groups;
    private final EntityManager<HugeTarget> targets;
    private final EntityManager<HugeProject> project;

    private final RelationshipManager<HugeBelong> belong;
    private final RelationshipManager<HugeAccess> access;

    private final TokenGenerator tokenGenerator;
    private final long tokenExpire;

    private final String defaultGraphSpace = "DEFAULT";

    private Set<String> ipWhiteList;

    private Boolean ipWhiteListEnabled;

    public StandardAuthManager(HugeGraphParams graph) {
        E.checkNotNull(graph, "graph");
        HugeConfig config = graph.configuration();
        long expired = config.get(AuthOptions.AUTH_CACHE_EXPIRE);
        long capacity = config.get(AuthOptions.AUTH_CACHE_CAPACITY);
        this.tokenExpire = config.get(AuthOptions.AUTH_TOKEN_EXPIRE) * 1000;

        this.graph = graph;
        this.usersCache = this.cache("users", capacity, expired);
        this.pwdCache = this.cache("users_pwd", capacity, expired);
        this.tokenCache = this.cache("token", capacity, expired);

        this.users = new EntityManager<>(this.graph, HugeUser.P.USER,
                                         HugeUser::fromVertex);
        this.groups = new EntityManager<>(this.graph, HugeGroup.P.GROUP,
                                          HugeGroup::fromVertex);
        this.targets = new EntityManager<>(this.graph, HugeTarget.P.TARGET,
                                           HugeTarget::fromVertex);
        this.project = new EntityManager<>(this.graph, HugeProject.P.PROJECT,
                                           HugeProject::fromVertex);

        this.belong = new RelationshipManager<>(this.graph, HugeBelong.P.BELONG,
                                                HugeBelong::fromEdge);
        this.access = new RelationshipManager<>(this.graph, HugeAccess.P.ACCESS,
                                                HugeAccess::fromEdge);

        this.tokenGenerator = new TokenGenerator(config);
        LOG.info("Randomly generate a JWT secret key now");

        this.ipWhiteList = new HashSet<>();

        this.ipWhiteListEnabled = false;
    }

    /**
     * Maybe can define an proxy class to choose forward or call local
     */
    public static boolean isLocal(AuthManager authManager) {
        return authManager instanceof StandardAuthManager ||
               //FIXME: The judgment of v2 is best placed in the islocal of v2
               authManager instanceof StandardAuthManagerV2;
    }

    private <V> Cache<Id, V> cache(String prefix, long capacity,
                                   long expiredTime) {
        String name = prefix + "-" + this.graph.spaceGraphName();
        Cache<Id, V> cache = CacheManager.instance().cache(name, capacity);
        if (expiredTime > 0L) {
            cache.expire(Duration.ofSeconds(expiredTime).toMillis());
        } else {
            cache.expire(expiredTime);
        }
        return cache;
    }

    @Override
    public void init() {
        this.invalidateUserCache();
        HugeUser.schema(this.graph).initSchemaIfNeeded();
        HugeGroup.schema(this.graph).initSchemaIfNeeded();
        HugeTarget.schema(this.graph).initSchemaIfNeeded();
        HugeBelong.schema(this.graph).initSchemaIfNeeded();
        HugeAccess.schema(this.graph).initSchemaIfNeeded();
        HugeProject.schema(this.graph).initSchemaIfNeeded();
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
        this.invalidateUserCache();
        return this.users.add(user);
    }

    @Override
    public Id updateUser(HugeUser user) {
        this.invalidateUserCache();
        this.invalidatePasswordCache(user.id());
        return this.users.update(user);
    }

    @Override
    public HugeUser deleteUser(Id id) {
        this.invalidateUserCache();
        this.invalidatePasswordCache(id);
        return this.users.delete(id);
    }

    @Override
    public HugeUser findUser(String name) {
        Id username = IdGenerator.of(name);
        HugeUser user = this.usersCache.get(username);
        if (user != null) {
            return user;
        }

        List<HugeUser> users = this.users.query(P.NAME, name, 2L);
        if (!users.isEmpty()) {
            assert users.size() == 1;
            user = users.get(0);
            this.usersCache.update(username, user);
        }
        return user;
    }

    @Override
    public HugeUser getUser(Id id) {
        return this.users.get(id);
    }

    @Override
    public List<HugeUser> listUsers(List<Id> ids) {
        return this.users.list(ids);
    }

    @Override
    public List<HugeUser> listAllUsers(long limit) {
        return this.users.list(limit);
    }

    @Override
    public Id createGroup(HugeGroup group) {
        this.invalidateUserCache();
        return this.groups.add(group);
    }

    @Override
    public Id updateGroup(HugeGroup group) {
        this.invalidateUserCache();
        return this.groups.update(group);
    }

    @Override
    public HugeGroup deleteGroup(Id id) {
        this.invalidateUserCache();
        return this.groups.delete(id);
    }

    @Override
    public HugeGroup getGroup(Id id) {
        return this.groups.get(id);
    }

    @Override
    public List<HugeGroup> listGroups(List<Id> ids) {
        return this.groups.list(ids);
    }

    @Override
    public List<HugeGroup> listAllGroups(long limit) {
        return this.groups.list(limit);
    }

    @Override
    public Id createTarget(HugeTarget target) {
        this.invalidateUserCache();
        return this.targets.add(target);
    }

    @Override
    public Id updateTarget(HugeTarget target) {
        this.invalidateUserCache();
        return this.targets.update(target);
    }

    @Override
    public HugeTarget deleteTarget(Id id) {
        this.invalidateUserCache();
        return this.targets.delete(id);
    }

    @Override
    public HugeTarget getTarget(Id id) {
        return this.targets.get(id);
    }

    @Override
    public List<HugeTarget> listTargets(List<Id> ids) {
        return this.targets.list(ids);
    }

    @Override
    public List<HugeTarget> listAllTargets(long limit) {
        return this.targets.list(limit);
    }

    @Override
    public Id createBelong(HugeBelong belong) {
        this.invalidateUserCache();
        E.checkArgument(this.users.exists(belong.source()),
                        "Not exists user '%s'", belong.source());
        E.checkArgument(this.groups.exists(belong.target()),
                        "Not exists group '%s'", belong.target());
        return this.belong.add(belong);
    }

    @Override
    public Id updateBelong(HugeBelong belong) {
        this.invalidateUserCache();
        return this.belong.update(belong);
    }

    @Override
    public HugeBelong deleteBelong(Id id) {
        this.invalidateUserCache();
        return this.belong.delete(id);
    }

    @Override
    public HugeBelong getBelong(Id id) {
        return this.belong.get(id);
    }

    @Override
    public List<HugeBelong> listBelong(List<Id> ids) {
        return this.belong.list(ids);
    }

    @Override
    public List<HugeBelong> listAllBelong(long limit) {
        return this.belong.list(limit);
    }

    @Override
    public List<HugeBelong> listBelongByUser(Id user, long limit) {
        return this.belong.list(user, Directions.OUT,
                                HugeBelong.P.BELONG, limit);
    }

    @Override
    public List<HugeBelong> listBelongByGroup(Id group, long limit) {
        return this.belong.list(group, Directions.IN,
                                HugeBelong.P.BELONG, limit);
    }

    @Override
    public Id createAccess(HugeAccess access) {
        this.invalidateUserCache();
        E.checkArgument(this.groups.exists(access.source()),
                        "Not exists group '%s'", access.source());
        E.checkArgument(this.targets.exists(access.target()),
                        "Not exists target '%s'", access.target());
        return this.access.add(access);
    }

    @Override
    public Id updateAccess(HugeAccess access) {
        this.invalidateUserCache();
        return this.access.update(access);
    }

    @Override
    public HugeAccess deleteAccess(Id id) {
        this.invalidateUserCache();
        return this.access.delete(id);
    }

    @Override
    public HugeAccess getAccess(Id id) {
        return this.access.get(id);
    }

    @Override
    public List<HugeAccess> listAccess(List<Id> ids) {
        return this.access.list(ids);
    }

    @Override
    public List<HugeAccess> listAllAccess(long limit) {
        return this.access.list(limit);
    }

    @Override
    public List<HugeAccess> listAccessByGroup(Id group, long limit) {
        return this.access.list(group, Directions.OUT,
                                HugeAccess.P.ACCESS, limit);
    }

    @Override
    public List<HugeAccess> listAccessByTarget(Id target, long limit) {
        return this.access.list(target, Directions.IN,
                                HugeAccess.P.ACCESS, limit);
    }

    @Override
    public Id createProject(HugeProject project) {
        E.checkArgument(!StringUtils.isEmpty(project.name()),
                        "The name of project can't be null or empty");
        return commit(() -> {
            // Create project admin group
            if (project.adminGroupId() == null) {
                HugeGroup adminGroup = new HugeGroup("admin_" + project.name());
                /*
                 * "creator" is a necessary parameter, other places are passed
                 * in "AuthManagerProxy", but here is the underlying module, so
                 * pass it directly here
                 */
                adminGroup.creator(project.creator());
                Id adminGroupId = this.createGroup(adminGroup);
                project.adminGroupId(adminGroupId);
            }

            // Create project op group
            if (project.opGroupId() == null) {
                HugeGroup opGroup = new HugeGroup("op_" + project.name());
                // Ditto
                opGroup.creator(project.creator());
                Id opGroupId = this.createGroup(opGroup);
                project.opGroupId(opGroupId);
            }

            // Create project target to verify permission
            final String targetName = "project_res_" + project.name();
            HugeResource resource = new HugeResource(ResourceType.PROJECT,
                                                     project.name(),
                                                     null);
            //FIXME: project api
            Map<String, List<HugeResource>> defaultResources = new LinkedHashMap<>();
            List<HugeResource> resources = new ArrayList<>();
            resources.add(resource);
            defaultResources.put(defaultGraphSpace, resources);

            HugeTarget target = new HugeTarget(targetName,
                                               this.graph.name(),
                                               "localhost:8080",
                                               defaultResources);
            // Ditto
            target.creator(project.creator());
            Id targetId = this.targets.add(target);
            project.targetId(targetId);

            Id adminGroupId = project.adminGroupId();
            Id opGroupId = project.opGroupId();
            HugeAccess adminGroupWriteAccess = new HugeAccess(
                    adminGroupId, targetId,
                    HugePermission.WRITE);
            // Ditto
            adminGroupWriteAccess.creator(project.creator());
            HugeAccess adminGroupReadAccess = new HugeAccess(
                    adminGroupId, targetId,
                    HugePermission.READ);
            // Ditto
            adminGroupReadAccess.creator(project.creator());
            HugeAccess opGroupReadAccess = new HugeAccess(opGroupId, targetId,
                                                          HugePermission.READ);
            // Ditto
            opGroupReadAccess.creator(project.creator());
            this.access.add(adminGroupWriteAccess);
            this.access.add(adminGroupReadAccess);
            this.access.add(opGroupReadAccess);
            return this.project.add(project);
        });
    }

    @Override
    public HugeProject deleteProject(Id id) {
        return this.commit(() -> {
            LockUtil.Locks locks = new LockUtil.Locks(this.graph.spaceGraphName());
            try {
                locks.lockWrites(LockUtil.PROJECT_UPDATE, id);

                HugeProject oldProject = this.project.get(id);
                /*
                 * Check whether there are any graph binding this project,
                 * throw ForbiddenException, if it is
                 */
                if (!CollectionUtils.isEmpty(oldProject.graphs())) {
                    String errInfo = String.format("Can't delete project '%s' " +
                                                   "that contains any graph, " +
                                                   "there are graphs bound " +
                                                   "to it", id);
                    throw new ForbiddenException(errInfo);
                }
                HugeProject project = this.project.delete(id);
                E.checkArgumentNotNull(project,
                                       "Failed to delete the project '%s'",
                                       id);
                E.checkArgumentNotNull(project.adminGroupId(),
                                       "Failed to delete the project '%s'," +
                                       "the admin group of project can't " +
                                       "be null", id);
                E.checkArgumentNotNull(project.opGroupId(),
                                       "Failed to delete the project '%s'," +
                                       "the op group of project can't be null",
                                       id);
                E.checkArgumentNotNull(project.targetId(),
                                       "Failed to delete the project '%s', " +
                                       "the target resource of project " +
                                       "can't be null", id);
                // Delete admin group
                this.groups.delete(project.adminGroupId());
                // Delete op group
                this.groups.delete(project.opGroupId());
                // Delete project_target
                this.targets.delete(project.targetId());
                return project;
            } finally {
                locks.unlock();
            }
        });
    }

    @Override
    public Id updateProject(HugeProject project) {
        return this.project.update(project);
    }

    @Override
    public Id projectAddGraphs(Id id, Set<String> graphs) {
        E.checkArgument(!CollectionUtils.isEmpty(graphs),
                        "Failed to add graphs to project '%s', the graphs " +
                        "parameter can't be empty", id);

        LockUtil.Locks locks = new LockUtil.Locks(this.graph.spaceGraphName());
        try {
            locks.lockWrites(LockUtil.PROJECT_UPDATE, id);

            HugeProject project = this.project.get(id);
            Set<String> sourceGraphs = new HashSet<>(project.graphs());
            int oldSize = sourceGraphs.size();
            sourceGraphs.addAll(graphs);
            // Return if there is none graph been added
            if (sourceGraphs.size() == oldSize) {
                return id;
            }
            project.graphs(sourceGraphs);
            return this.project.update(project);
        } finally {
            locks.unlock();
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

        LockUtil.Locks locks = new LockUtil.Locks(this.graph.spaceGraphName());
        try {
            locks.lockWrites(LockUtil.PROJECT_UPDATE, id);

            HugeProject project = this.project.get(id);
            Set<String> sourceGraphs = new HashSet<>(project.graphs());
            int oldSize = sourceGraphs.size();
            sourceGraphs.removeAll(graphs);
            // Return if there is none graph been removed
            if (sourceGraphs.size() == oldSize) {
                return id;
            }
            project.graphs(sourceGraphs);
            return this.project.update(project);
        } finally {
            locks.unlock();
        }
    }

    @Override
    public HugeProject getProject(Id id) {
        return this.project.get(id);
    }

    @Override
    public List<HugeProject> listAllProject(long limit) {
        return this.project.list(limit);
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
        if (user.role() != null) {
            // Return cached role (40ms => 10ms)
            return user.role();
        }

        // Collect accesses by user
        List<HugeAccess> accesses = new ArrayList<>();
        List<HugeBelong> belongs = this.listBelongByUser(user.id(), -1);

        for (HugeBelong belong : belongs) {
            accesses.addAll(this.listAccessByGroup(belong.target(), -1));
        }

        // Collect permissions by accesses
        RolePermission role = this.rolePermission(accesses);

        user.role(role);
        return role;
    }

    private RolePermission rolePermission(List<HugeAccess> accesses) {
        // Mapping of: graph -> action -> resource
        RolePermission role = new RolePermission();
        for (HugeAccess access : accesses) {
            HugePermission accessPerm = access.permission();
            HugeTarget target = this.getTarget(access.target());
            role.add(target.graph(), accessPerm, target.resources());
        }
        return role;
    }

    private RolePermission rolePermission(HugeTarget target) {
        RolePermission role = new RolePermission();
        // TODO: improve for the actual meaning
        role.add(target.graph(), HugePermission.READ, target.resources());
        return role;
    }

    @Override
    public String loginUser(String username, String password)
            throws AuthenticationException {
        return this.loginUser(username, password, -1L);
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
    public Id createSpaceManager(String graphSpace, String owner) {
        return null;
    }

    @Override
    public void deleteSpaceManager(String graphSpace, String owner) {

    }

    @Override
    public List<String> listSpaceManager(String graphSpace) {
        return List.of();
    }

    @Override
    public boolean isSpaceManager(String owner) {
        return false;
    }

    @Override
    public boolean isSpaceManager(String graphSpace, String owner) {
        return false;
    }

    @Override
    public Id createSpaceMember(String graphSpace, String user) {
        return null;
    }

    @Override
    public void deleteSpaceMember(String graphSpace, String user) {

    }

    @Override
    public List<String> listSpaceMember(String graphSpace) {
        return List.of();
    }

    @Override
    public boolean isSpaceMember(String graphSpace, String user) {
        return false;
    }

    @Override
    public Id createAdminManager(String user) {
        return null;
    }

    @Override
    public void deleteAdminManager(String user) {

    }

    @Override
    public List<String> listAdminManager() {
        return List.of();
    }

    @Override
    public boolean isAdminManager(String user) {
        return false;
    }

    @Override
    public HugeGroup findGroup(String name) {
        return null;
    }

    public <R> R commit(Callable<R> callable) {
        this.groups.autoCommit(false);
        this.access.autoCommit(false);
        this.targets.autoCommit(false);
        this.project.autoCommit(false);
        this.belong.autoCommit(false);
        this.users.autoCommit(false);

        try {
            R result = callable.call();
            this.graph.systemTransaction().commit();
            return result;
        } catch (Throwable e) {
            this.groups.autoCommit(true);
            this.access.autoCommit(true);
            this.targets.autoCommit(true);
            this.project.autoCommit(true);
            this.belong.autoCommit(true);
            this.users.autoCommit(true);
            try {
                this.graph.systemTransaction().rollback();
            } catch (Throwable rollbackException) {
                LOG.error("Failed to rollback transaction: {}",
                          rollbackException.getMessage(), rollbackException);
            }
            if (e instanceof HugeException) {
                throw (HugeException) e;
            } else {
                throw new HugeException("Failed to commit transaction: %s",
                                        e.getMessage(), e);
            }
        }
    }
}
