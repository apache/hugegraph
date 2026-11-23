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

package org.apache.hugegraph.ranger;

import java.util.Date;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

import javax.security.sasl.AuthenticationException;

import org.apache.hugegraph.auth.AuthManager;
import org.apache.hugegraph.auth.HugeAccess;
import org.apache.hugegraph.auth.HugeAuthenticator;
import org.apache.hugegraph.auth.HugeBelong;
import org.apache.hugegraph.auth.HugeDefaultRole;
import org.apache.hugegraph.auth.HugeGroup;
import org.apache.hugegraph.auth.HugePermission;
import org.apache.hugegraph.auth.HugeProject;
import org.apache.hugegraph.auth.HugeTarget;
import org.apache.hugegraph.auth.HugeUser;
import org.apache.hugegraph.auth.ResourceType;
import org.apache.hugegraph.auth.RolePermission;
import org.apache.hugegraph.auth.SchemaDefine.AuthElement;
import org.apache.hugegraph.auth.UserWithRole;
import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.util.JsonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AuthManager implementation that:
 * <ul>
 *   <li>Delegates all identity-management operations (users, groups, tokens) to
 *       the underlying {@link AuthManager} (StandardAuthManager /
 *       StandardAuthManagerV2).</li>
 *   <li>Intercepts {@link #validateUser} / {@link #rolePermission} to merge
 *       Ranger policy decisions into the returned {@link RolePermission}.</li>
 * </ul>
 *
 * <h3>Ranger → RolePermission mapping</h3>
 * <pre>
 *   Ranger access type  │  HugePermission
 *   ────────────────────┼────────────────
 *   read                │  READ
 *   write               │  WRITE
 *   delete              │  DELETE
 *   execute             │  EXECUTE
 *   admin               │  ADMIN
 * </pre>
 */
public class RangerAuthManager implements AuthManager {

    private static final Logger LOG =
            LoggerFactory.getLogger(RangerAuthManager.class);

    /** Permissions probed at login, besides ADMIN which is checked first. */
    private static final HugePermission[] PROBED_PERMS = {
            HugePermission.READ, HugePermission.WRITE,
            HugePermission.DELETE, HugePermission.EXECUTE
    };

    private static final int MAX_CACHED_USERS = 1024;

    private final AuthManager delegate;
    private final RangerHugeGraphPlugin rangerPlugin;
    private final long roleCacheNanos;
    private final LongSupplier nanoClock;
    private final Map<String, CachedGrant> grantCache = new ConcurrentHashMap<>();

    /**
     * @param roleCacheExpireSeconds how long a user's Ranger grants are reused
     *                               between logins; 0 queries Ranger every time
     */
    public RangerAuthManager(AuthManager delegate,
                             String serviceName,
                             String rangerConfigFile,
                             int roleCacheExpireSeconds) {
        this(delegate, createPlugin(serviceName, rangerConfigFile),
             roleCacheExpireSeconds, System::nanoTime);
    }

    RangerAuthManager(AuthManager delegate, RangerHugeGraphPlugin plugin,
                      int roleCacheExpireSeconds, LongSupplier nanoClock) {
        if (roleCacheExpireSeconds < 0) {
            throw new IllegalArgumentException(
                    "The Ranger role cache expire must be >= 0, but got " +
                    roleCacheExpireSeconds);
        }
        this.delegate = delegate;
        this.rangerPlugin = plugin;
        this.roleCacheNanos = TimeUnit.SECONDS.toNanos(roleCacheExpireSeconds);
        this.nanoClock = nanoClock;
    }

    private static RangerHugeGraphPlugin createPlugin(String serviceName,
                                                      String rangerConfigFile) {
        RangerHugeGraphPlugin plugin =
                new RangerHugeGraphPlugin(serviceName, rangerConfigFile);
        plugin.init();
        return plugin;
    }

    /**
     * The underlying Ranger plugin instance, used by
     * {@link RangerHugeGraphAuthenticator#setup} to register real per-request
     * enforcement via {@code HugeGraphAuthProxy.setResourceAuthorizer}.
     */
    public RangerHugeGraphPlugin rangerPlugin() {
        return this.rangerPlugin;
    }

    // ------------------------------------------------------------------
    // Authentication helpers
    // ------------------------------------------------------------------

    /**
     * Authenticate by username+password or token, then enrich the returned
     * role with Ranger policy grants.  Called from
     * {@link RangerHugeGraphAuthenticator#authenticate}.
     */
    public UserWithRole authenticate(String username, String password,
                                     String token) {
        UserWithRole base = (token != null && !token.isEmpty()) ?
                            delegate.validateUser(token) :
                            delegate.validateUser(username, password);
        return enrichWithRanger(base);
    }

    @Override
    public UserWithRole validateUser(String username, String password) {
        return enrichWithRanger(delegate.validateUser(username, password));
    }

    @Override
    public UserWithRole validateUser(String token) {
        return enrichWithRanger(delegate.validateUser(token));
    }

    @Override
    public RolePermission rolePermission(AuthElement element) {
        RolePermission base = delegate.rolePermission(element);
        if (base == null) {
            base = RolePermission.none();
        }
        if (RolePermission.isAdmin(base)) {
            return base;
        }
        String name = (element instanceof HugeUser) ?
                      ((HugeUser) element).name() :
                      element.id().asString();
        // admin is a trusted bootstrap identity (same bypass as
        // HugeGraphAuthProxy.verifyResPermission() and
        // RangerHugeGraphAuthenticator.authenticate()) — no Ranger policy is
        // expected to exist for it, so never probe Ranger on its behalf.
        if (HugeAuthenticator.USER_ADMIN.equals(name)) {
            return RolePermission.admin();
        }
        return mergeRangerRole(base, name);
    }

    // ------------------------------------------------------------------
    // Private: Ranger enrichment
    // ------------------------------------------------------------------

    private UserWithRole enrichWithRanger(UserWithRole base) {
        if (base == null || base.username() == null) {
            return base;
        }
        // userId() is null when authentication failed (wrong password or unknown user).
        // Do not grant Ranger permissions to unauthenticated identities.
        if (base.userId() == null) {
            return base;
        }
        RolePermission role = base.role();
        if (role != null && RolePermission.isAdmin(role)) {
            return base;
        }
        // admin is a trusted bootstrap identity — never probe Ranger for it,
        // otherwise every login logs a spurious Denied admin/*/*/all/* audit
        // entry even though RangerHugeGraphAuthenticator.authenticate()
        // overrides the role with ROLE_ADMIN right after this call returns.
        if (HugeAuthenticator.USER_ADMIN.equals(base.username())) {
            return new UserWithRole(base.userId(), base.username(),
                                    RolePermission.admin());
        }
        RolePermission enriched = mergeRangerRole(
                role != null ? role : RolePermission.none(),
                base.username());
        // No grants from either HugeGraph's own role or Ranger policy: reject the
        // login outright rather than admitting a role with no permissions.
        //
        // This MUST be the HugeAuthenticator.ROLE_NONE instance, not a fresh
        // RolePermission.none(). HugeAuthenticator.verifyRole() tests identity:
        //     return role != ROLE_NONE && role != null;
        // so a newly built none() is not that reference, passes verifyRole() and
        // logs the user in with an empty role.
        if (isEffectivelyNone(enriched)) {
            enriched = HugeAuthenticator.ROLE_NONE;
        }
        return new UserWithRole(base.userId(), base.username(), enriched);
    }

    /**
     * Query Ranger for {@code username} and merge any grants into {@code base}.
     *
     * <p>Login only needs to know which actions the user may attempt at all,
     * so each permission is probed with {@link
     * RangerHugeGraphPlugin#hasAnyAccess}: a policy limited to one graph space
     * or graph still counts. The grant is expressed under the {@code "*"}
     * graph space and graph, which {@code RolePerm} treats as a wildcard, and
     * the real graph space, graph and label of every request are decided by
     * Ranger in {@link org.apache.hugegraph.auth.HugeGraphAuthProxy}.
     *
     * <p>We can't call the protected {@code RolePermission.add()} from outside
     * the package, so we build a JSON role string and deserialise it instead —
     * this goes through the same public factory path used everywhere else.
     */
    private RolePermission mergeRangerRole(RolePermission base,
                                           String username) {
        if (base == null) {
            base = RolePermission.none();
        }
        CachedGrant grant = this.rangerGrant(username);
        if (grant.admin) {
            return RolePermission.admin();
        }
        if (grant.perms.isEmpty()) {
            // Ranger denied everything — return base unchanged
            return base;
        }

        StringBuilder sb = new StringBuilder("{\"roles\":{\"*\":{\"*\":{");
        boolean first = true;
        for (HugePermission perm : grant.perms) {
            if (!first) {
                sb.append(',');
            }
            // JSON format: "READ":{"ALL":[{"type":"ALL","label":"*"}]}
            sb.append('"').append(perm.name()).append('"')
              .append(":{\"ALL\":[{\"type\":\"ALL\",\"label\":\"*\"}]}");
            first = false;
        }
        sb.append("}}}}");

        try {
            RolePermission rangerRole =
                    RolePermission.fromJson(sb.toString());
            // Merge: rangerRole entries win, existing base entries are kept
            return mergeViaJson(base, rangerRole);
        } catch (Exception e) {
            LOG.warn("Failed to parse Ranger-derived role JSON, " +
                     "falling back to local permissions: {}", e.getMessage());
            return base;
        }
    }

    /**
     * The user's Ranger grants, reused for the configured expiry so a burst of
     * logins does not become a burst of policy evaluations. Only Ranger's side
     * is cached, never the local role, so local role changes apply at once.
     * A Ranger policy change therefore reaches new logins after at most the
     * expiry. Per-request enforcement does not use this cache.
     */
    private CachedGrant rangerGrant(String username) {
        long now = this.nanoClock.getAsLong();
        if (this.roleCacheNanos > 0) {
            CachedGrant cached = this.grantCache.get(username);
            if (cached != null && now - cached.loadedAt < this.roleCacheNanos) {
                return cached;
            }
        }

        // Probes are not accesses to a real resource: keep them out of audit
        CachedGrant grant;
        if (this.rangerPlugin.isAllowed(username, "*", "*", ResourceType.ALL,
                                        "*", HugePermission.ADMIN, false)) {
            grant = new CachedGrant(now, true, EnumSet.noneOf(HugePermission.class));
        } else {
            EnumSet<HugePermission> perms = EnumSet.noneOf(HugePermission.class);
            for (HugePermission perm : PROBED_PERMS) {
                if (this.rangerPlugin.hasAnyAccess(username, perm)) {
                    perms.add(perm);
                }
            }
            grant = new CachedGrant(now, false, perms);
        }

        if (this.roleCacheNanos > 0) {
            if (this.grantCache.size() >= MAX_CACHED_USERS) {
                this.grantCache.values().removeIf(
                        g -> now - g.loadedAt >= this.roleCacheNanos);
            }
            if (this.grantCache.size() < MAX_CACHED_USERS) {
                this.grantCache.put(username, grant);
            }
        }
        return grant;
    }

    private static final class CachedGrant {

        private final long loadedAt;
        private final boolean admin;
        private final EnumSet<HugePermission> perms;

        private CachedGrant(long loadedAt, boolean admin,
                            EnumSet<HugePermission> perms) {
            this.loadedAt = loadedAt;
            this.admin = admin;
            this.perms = perms;
        }
    }

    /**
     * Merge two {@link RolePermission} objects using their JSON representation.
     * This avoids calling the package-private {@code add()} method.
     */
    private static RolePermission mergeViaJson(RolePermission a,
                                               RolePermission b) {
        if (a == null) {
            a = RolePermission.none();
        }
        if (b == null) {
            b = RolePermission.none();
        }
        if (RolePermission.isAdmin(a) || RolePermission.isAdmin(b)) {
            return RolePermission.admin();
        }
        // Deserialise both, combine role maps at the JSON level
        String jsonA = a.toJson();
        String jsonB = b.toJson();

        // If either is empty / NONE just return the other
        if (isEffectivelyNone(a)) {
            return b;
        }
        if (isEffectivelyNone(b)) {
            return a;
        }

        // Both are non-trivial: combine by building a merged JSON manually.
        // We rely on the fact that toJson() produces {"roles":{...}} and both
        // have the same outer structure.
        // Simple approach: the role whose JSON is a superset wins.
        // For a production deployment you'd use a richer merge strategy; this
        // is sufficient for the common case where Ranger extends local grants.
        try {
            // Re-serialise 'a' roles map merged with 'b'
            java.util.Map<String, Object> mapA =
                    JsonUtil.fromJson(jsonA, new org.apache.tinkerpop.shaded.jackson.core.type.TypeReference<
                            java.util.Map<String, Object>>() {});
            java.util.Map<String, Object> mapB =
                    JsonUtil.fromJson(jsonB, new org.apache.tinkerpop.shaded.jackson.core.type.TypeReference<
                            java.util.Map<String, Object>>() {});
            deepMerge(mapA, mapB);
            return RolePermission.fromJson(JsonUtil.toJson(mapA));
        } catch (Exception e) {
            LOG.warn("JSON merge failed, returning base role: {}", e.getMessage());
            return a;
        }
    }

    @SuppressWarnings("unchecked")
    private static void deepMerge(java.util.Map<String, Object> target,
                                  java.util.Map<String, Object> source) {
        for (Map.Entry<String, Object> e : source.entrySet()) {
            String key = e.getKey();
            Object srcVal = e.getValue();
            Object tgtVal = target.get(key);
            if (tgtVal instanceof java.util.Map && srcVal instanceof java.util.Map) {
                deepMerge((java.util.Map<String, Object>) tgtVal,
                          (java.util.Map<String, Object>) srcVal);
            } else {
                target.put(key, srcVal);
            }
        }
    }

    private static boolean isEffectivelyNone(RolePermission role) {
        return role == null ||
               role.equals(RolePermission.none()) ||
               role.roles().isEmpty();
    }

    // ------------------------------------------------------------------
    // Pure delegation — identity management
    // ------------------------------------------------------------------

    @Override
    public boolean supportsGraphSpaceAuth() {
        return delegate.supportsGraphSpaceAuth();
    }

    @Override
    public void init() {
        // The delegate (StandardAuthManager) was already initialised by
        // GraphFactory.open() before RangerAuthManager was wired in via
        // HugeGraph.switchAuthManager().  Calling delegate.init() here would
        // create a cycle:
        //   RangerAuthManager.init()
        //     → AuthManagerProxy.init()         (proxy wraps RangerAuthManager)
        //       → RangerAuthManager.init()       (infinite recursion)
        // So we intentionally skip the delegation.
    }

    @Override
    public boolean close() {
        return delegate.close();
    }

    @Override
    public Id createUser(HugeUser user) {
        // The HugeGraphAuthProxy.AuthManagerProxy.updateCreator() path is not
        // reachable here (this manager is not wrapped by that proxy), so we
        // mirror what StandardAuthManagerV2 does: pull the creator from the
        // current auth context, falling back to "system" for bootstrap calls.
        if (user.creator() == null) {
            org.apache.hugegraph.auth.HugeGraphAuthProxy.Context ctx =
                    org.apache.hugegraph.auth.HugeGraphAuthProxy.getContext();
            String username = (ctx != null) ? ctx.user().username() : null;
            user.creator(username != null ? username : "system");
        }
        return delegate.createUser(user);
    }

    @Override
    public Id updateUser(HugeUser user) {
        return delegate.updateUser(user);
    }

    @Override
    public HugeUser deleteUser(Id id) {
        return delegate.deleteUser(id);
    }

    @Override
    public HugeUser findUser(String name) {
        return delegate.findUser(name);
    }

    @Override
    public HugeUser getUser(Id id) {
        return delegate.getUser(id);
    }

    @Override
    public List<HugeUser> listUsers(List<Id> ids) {
        return delegate.listUsers(ids);
    }

    @Override
    public List<HugeUser> listAllUsers(long limit) {
        return delegate.listAllUsers(limit);
    }

    @Override
    public Id createGroup(HugeGroup group) {
        return delegate.createGroup(group);
    }

    @Override
    public Id updateGroup(HugeGroup group) {
        return delegate.updateGroup(group);
    }

    @Override
    public HugeGroup deleteGroup(Id id) {
        return delegate.deleteGroup(id);
    }

    @Override
    public HugeGroup deleteGroup(String graphSpace, Id id) {
        return delegate.deleteGroup(graphSpace, id);
    }

    @Override
    public HugeGroup getGroup(Id id) {
        return delegate.getGroup(id);
    }

    @Override
    public HugeGroup findGroup(String name) {
        return delegate.findGroup(name);
    }

    @Override
    public List<HugeGroup> listGroups(List<Id> ids) {
        return delegate.listGroups(ids);
    }

    @Override
    public List<HugeGroup> listAllGroups(long limit) {
        return delegate.listAllGroups(limit);
    }

    @Override
    public Id createTarget(HugeTarget target) {
        return delegate.createTarget(target);
    }

    @Override
    public Id createTarget(String graphSpace, HugeTarget target) {
        return delegate.createTarget(graphSpace, target);
    }

    @Override
    public Id updateTarget(HugeTarget target) {
        return delegate.updateTarget(target);
    }

    @Override
    public Id updateTarget(String graphSpace, HugeTarget target) {
        return delegate.updateTarget(graphSpace, target);
    }

    @Override
    public HugeTarget deleteTarget(Id id) {
        return delegate.deleteTarget(id);
    }

    @Override
    public HugeTarget deleteTarget(String graphSpace, Id id) {
        return delegate.deleteTarget(graphSpace, id);
    }

    @Override
    public HugeTarget getTarget(Id id) {
        return delegate.getTarget(id);
    }

    @Override
    public HugeTarget getTarget(String graphSpace, Id id) {
        return delegate.getTarget(graphSpace, id);
    }

    @Override
    public List<HugeTarget> listTargets(List<Id> ids) {
        return delegate.listTargets(ids);
    }

    @Override
    public List<HugeTarget> listAllTargets(long limit) {
        return delegate.listAllTargets(limit);
    }

    @Override
    public List<HugeTarget> listAllTargets(String graphSpace, long limit) {
        return delegate.listAllTargets(graphSpace, limit);
    }

    @Override
    public Id createBelong(HugeBelong belong) {
        return delegate.createBelong(belong);
    }

    @Override
    public Id createBelong(String graphSpace, HugeBelong belong) {
        return delegate.createBelong(graphSpace, belong);
    }

    @Override
    public Id updateBelong(HugeBelong belong) {
        return delegate.updateBelong(belong);
    }

    @Override
    public Id updateBelong(String graphSpace, HugeBelong belong) {
        return delegate.updateBelong(graphSpace, belong);
    }

    @Override
    public HugeBelong deleteBelong(Id id) {
        return delegate.deleteBelong(id);
    }

    @Override
    public HugeBelong deleteBelong(String graphSpace, Id id) {
        return delegate.deleteBelong(graphSpace, id);
    }

    @Override
    public HugeBelong getBelong(Id id) {
        return delegate.getBelong(id);
    }

    @Override
    public HugeBelong getBelong(String graphSpace, Id id) {
        return delegate.getBelong(graphSpace, id);
    }

    @Override
    public List<HugeBelong> listBelong(List<Id> ids) {
        return delegate.listBelong(ids);
    }

    @Override
    public List<HugeBelong> listAllBelong(long limit) {
        return delegate.listAllBelong(limit);
    }

    @Override
    public List<HugeBelong> listAllBelong(String graphSpace, long limit) {
        return delegate.listAllBelong(graphSpace, limit);
    }

    @Override
    public List<HugeBelong> listBelongByUser(Id user, long limit) {
        return delegate.listBelongByUser(user, limit);
    }

    @Override
    public List<HugeBelong> listBelongByUser(String graphSpace, Id user,
                                             long limit) {
        return delegate.listBelongByUser(graphSpace, user, limit);
    }

    @Override
    public List<HugeBelong> listBelongByGroup(Id group, long limit) {
        return delegate.listBelongByGroup(group, limit);
    }

    @Override
    public List<HugeBelong> listBelongByGroup(String graphSpace, Id group,
                                              long limit) {
        return delegate.listBelongByGroup(graphSpace, group, limit);
    }

    @Override
    public Id createAccess(HugeAccess access) {
        return delegate.createAccess(access);
    }

    @Override
    public Id createAccess(String graphSpace, HugeAccess access) {
        return delegate.createAccess(graphSpace, access);
    }

    @Override
    public Id updateAccess(HugeAccess access) {
        return delegate.updateAccess(access);
    }

    @Override
    public Id updateAccess(String graphSpace, HugeAccess access) {
        return delegate.updateAccess(graphSpace, access);
    }

    @Override
    public HugeAccess deleteAccess(Id id) {
        return delegate.deleteAccess(id);
    }

    @Override
    public HugeAccess deleteAccess(String graphSpace, Id id) {
        return delegate.deleteAccess(graphSpace, id);
    }

    @Override
    public HugeAccess getAccess(Id id) {
        return delegate.getAccess(id);
    }

    @Override
    public HugeAccess getAccess(String graphSpace, Id id) {
        return delegate.getAccess(graphSpace, id);
    }

    @Override
    public List<HugeAccess> listAccess(List<Id> ids) {
        return delegate.listAccess(ids);
    }

    @Override
    public List<HugeAccess> listAllAccess(long limit) {
        return delegate.listAllAccess(limit);
    }

    @Override
    public List<HugeAccess> listAllAccess(String graphSpace, long limit) {
        return delegate.listAllAccess(graphSpace, limit);
    }

    @Override
    public List<HugeAccess> listAccessByGroup(Id group, long limit) {
        return delegate.listAccessByGroup(group, limit);
    }

    @Override
    public List<HugeAccess> listAccessByGroup(String graphSpace, Id group,
                                              long limit) {
        return delegate.listAccessByGroup(graphSpace, group, limit);
    }

    @Override
    public List<HugeAccess> listAccessByTarget(Id target, long limit) {
        return delegate.listAccessByTarget(target, limit);
    }

    @Override
    public List<HugeAccess> listAccessByTarget(String graphSpace, Id target,
                                               long limit) {
        return delegate.listAccessByTarget(graphSpace, target, limit);
    }

    @Override
    public Id createProject(HugeProject project) {
        return delegate.createProject(project);
    }

    @Override
    public HugeProject deleteProject(Id id) {
        return delegate.deleteProject(id);
    }

    @Override
    public Id updateProject(HugeProject project) {
        return delegate.updateProject(project);
    }

    @Override
    public Id projectAddGraphs(Id id, Set<String> graphs) {
        return delegate.projectAddGraphs(id, graphs);
    }

    @Override
    public Id projectRemoveGraphs(Id id, Set<String> graphs) {
        return delegate.projectRemoveGraphs(id, graphs);
    }

    @Override
    public HugeProject getProject(Id id) {
        return delegate.getProject(id);
    }

    @Override
    public List<HugeProject> listAllProject(long limit) {
        return delegate.listAllProject(limit);
    }

    @Override
    public HugeUser matchUser(String name, String password) {
        return delegate.matchUser(name, password);
    }

    @Override
    public String loginUser(String username, String password)
            throws AuthenticationException {
        return delegate.loginUser(username, password);
    }

    @Override
    public String loginUser(String username, String password, long expire)
            throws AuthenticationException {
        return delegate.loginUser(username, password, expire);
    }

    @Override
    public void logoutUser(String token) {
        delegate.logoutUser(token);
    }

    @Override
    public Set<String> listWhiteIPs() {
        return delegate.listWhiteIPs();
    }

    @Override
    public void setWhiteIPs(Set<String> whiteIpList) {
        delegate.setWhiteIPs(whiteIpList);
    }

    @Override
    public boolean getWhiteIpStatus() {
        return delegate.getWhiteIpStatus();
    }

    @Override
    public void enabledWhiteIpList(boolean status) {
        delegate.enabledWhiteIpList(status);
    }

    @Override
    public Id createSpaceManager(String graphSpace, String owner) {
        return delegate.createSpaceManager(graphSpace, owner);
    }

    @Override
    public void deleteSpaceManager(String graphSpace, String owner) {
        delegate.deleteSpaceManager(graphSpace, owner);
    }

    @Override
    public List<String> listSpaceManager(String graphSpace) {
        return delegate.listSpaceManager(graphSpace);
    }

    @Override
    public boolean isSpaceManager(String owner) {
        return delegate.isSpaceManager(owner);
    }

    @Override
    public boolean isSpaceManager(String graphSpace, String owner) {
        return delegate.isSpaceManager(graphSpace, owner);
    }

    @Override
    public Id createSpaceMember(String graphSpace, String user) {
        return delegate.createSpaceMember(graphSpace, user);
    }

    @Override
    public void deleteSpaceMember(String graphSpace, String user) {
        delegate.deleteSpaceMember(graphSpace, user);
    }

    @Override
    public List<String> listSpaceMember(String graphSpace) {
        return delegate.listSpaceMember(graphSpace);
    }

    @Override
    public boolean isSpaceMember(String graphSpace, String user) {
        return delegate.isSpaceMember(graphSpace, user);
    }

    @Override
    public Id createAdminManager(String user) {
        return delegate.createAdminManager(user);
    }

    @Override
    public void deleteAdminManager(String user) {
        delegate.deleteAdminManager(user);
    }

    @Override
    public List<String> listAdminManager() {
        return delegate.listAdminManager();
    }

    @Override
    public boolean isAdminManager(String user) {
        return delegate.isAdminManager(user);
    }

    @Override
    public void setDefaultGraph(String graphSpace, String graph, String user) {
        delegate.setDefaultGraph(graphSpace, graph, user);
    }

    @Override
    public void unsetDefaultGraph(String graphSpace, String graph,
                                  String user) {
        delegate.unsetDefaultGraph(graphSpace, graph, user);
    }

    @Override
    public Map<String, Date> getDefaultGraph(String graphSpace, String user) {
        return delegate.getDefaultGraph(graphSpace, user);
    }

    @Override
    public Id createDefaultRole(String graphSpace, String owner,
                                HugeDefaultRole role, String graph) {
        return delegate.createDefaultRole(graphSpace, owner, role, graph);
    }

    @Override
    public Id createSpaceDefaultRole(String graphSpace, String owner,
                                     HugeDefaultRole role) {
        return delegate.createSpaceDefaultRole(graphSpace, owner, role);
    }

    @Override
    public boolean isDefaultRole(String graphSpace, String owner,
                                 HugeDefaultRole role) {
        return delegate.isDefaultRole(graphSpace, owner, role);
    }

    @Override
    public boolean isDefaultRole(String graphSpace, String graph,
                                 String owner, HugeDefaultRole role) {
        return delegate.isDefaultRole(graphSpace, graph, owner, role);
    }

    @Override
    public void deleteDefaultRole(String graphSpace, String owner,
                                  HugeDefaultRole role) {
        delegate.deleteDefaultRole(graphSpace, owner, role);
    }

    @Override
    public void deleteDefaultRole(String graphSpace, String owner,
                                  HugeDefaultRole role, String graph) {
        delegate.deleteDefaultRole(graphSpace, owner, role, graph);
    }
}
