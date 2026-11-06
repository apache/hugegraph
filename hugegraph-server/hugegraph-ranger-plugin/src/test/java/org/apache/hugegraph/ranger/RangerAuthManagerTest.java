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

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hugegraph.auth.AuthManager;
import org.apache.hugegraph.auth.HugeAuthenticator;
import org.apache.hugegraph.auth.HugeAuthenticator.RolePerm;
import org.apache.hugegraph.auth.HugeDefaultRole;
import org.apache.hugegraph.auth.HugePermission;
import org.apache.hugegraph.auth.ResourceObject;
import org.apache.hugegraph.auth.ResourceType;
import org.apache.hugegraph.auth.RolePermission;
import org.apache.hugegraph.auth.UserWithRole;
import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.id.IdGenerator;
import org.apache.hugegraph.type.Namifiable;
import org.junit.Assert;
import org.junit.Test;

public class RangerAuthManagerTest {

    private static final long TTL_SECONDS = 30;

    /** Answers from a fixed grant and records how it was asked. */
    private static class StubPlugin extends RangerHugeGraphPlugin {

        private boolean admin;
        private Set<HugePermission> anyAccess =
                EnumSet.noneOf(HugePermission.class);
        private final List<Boolean> auditFlags = new ArrayList<>();
        private int probes;

        StubPlugin() {
            super("hugegraph", null);
        }

        @Override
        public boolean isAllowed(String username, String graphSpace,
                                 String graph, ResourceType resourceType,
                                 String label, HugePermission permission,
                                 boolean audit) {
            this.probes++;
            this.auditFlags.add(audit);
            return this.admin && permission == HugePermission.ADMIN;
        }

        @Override
        public boolean hasAnyAccess(String username,
                                    HugePermission permission) {
            this.probes++;
            return this.anyAccess.contains(permission);
        }
    }

    private final StubPlugin plugin = new StubPlugin();
    private final AtomicLong nanos = new AtomicLong();

    private RangerAuthManager manager(long ttlSeconds) {
        AuthManager delegate = (AuthManager) Proxy.newProxyInstance(
                AuthManager.class.getClassLoader(),
                new Class<?>[]{AuthManager.class},
                (proxy, method, args) -> {
                    if ("validateUser".equals(method.getName())) {
                        return new UserWithRole(IdGenerator.of("u1"), "alice",
                                                RolePermission.none());
                    }
                    throw new UnsupportedOperationException(method.getName());
                });
        return new RangerAuthManager(delegate, this.plugin, (int) ttlSeconds,
                                     this.nanos::get);
    }

    private static ResourceObject<?> vertex(String graphSpace) {
        Namifiable label = () -> "person";
        return ResourceObject.of(graphSpace, "hugegraph",
                                 ResourceType.VERTEX, label);
    }

    private UserWithRole login(RangerAuthManager manager) {
        return manager.validateUser("alice", "secret");
    }

    @Test
    public void testGrantInOneGraphSpaceAdmitsLoginForAnySpace() {
        this.plugin.anyAccess = EnumSet.of(HugePermission.READ);

        UserWithRole user = this.login(this.manager(TTL_SECONDS));

        // The login role is a wildcard; Ranger still decides each request
        Assert.assertNotSame(HugeAuthenticator.ROLE_NONE, user.role());
        Assert.assertFalse(RolePermission.isAdmin(user.role()));
        Assert.assertTrue(RolePerm.match(user.role(), HugePermission.READ,
                                         vertex("gs2")));
        Assert.assertTrue(RolePerm.match(user.role(), HugePermission.READ,
                                         vertex("DEFAULT")));
        Assert.assertFalse(RolePerm.match(user.role(), HugePermission.WRITE,
                                          vertex("gs2")));
    }

    @Test
    public void testNoGrantRejectsLogin() {
        UserWithRole user = this.login(this.manager(TTL_SECONDS));

        Assert.assertSame(HugeAuthenticator.ROLE_NONE, user.role());
    }

    @Test
    public void testAdminGrantProducesAdminRole() {
        this.plugin.admin = true;

        UserWithRole user = this.login(this.manager(TTL_SECONDS));

        Assert.assertTrue(RolePermission.isAdmin(user.role()));
    }

    @Test
    public void testProbesAreNotAudited() {
        this.plugin.anyAccess = EnumSet.of(HugePermission.READ);

        this.login(this.manager(TTL_SECONDS));

        Assert.assertFalse(this.plugin.auditFlags.isEmpty());
        Assert.assertFalse(this.plugin.auditFlags.contains(true));
    }

    @Test
    public void testGrantsAreReusedWithinExpiry() {
        this.plugin.anyAccess = EnumSet.of(HugePermission.READ);
        RangerAuthManager manager = this.manager(TTL_SECONDS);

        this.login(manager);
        int afterFirst = this.plugin.probes;
        Assert.assertTrue(afterFirst > 0);

        this.nanos.addAndGet(secondsToNanos(TTL_SECONDS - 1));
        this.login(manager);
        Assert.assertEquals(afterFirst, this.plugin.probes);

        this.nanos.addAndGet(secondsToNanos(2));
        this.login(manager);
        Assert.assertEquals(afterFirst * 2, this.plugin.probes);
    }

    @Test
    public void testPolicyChangeReachesLoginAfterExpiry() {
        RangerAuthManager manager = this.manager(TTL_SECONDS);
        Assert.assertSame(HugeAuthenticator.ROLE_NONE,
                          this.login(manager).role());

        this.plugin.anyAccess = EnumSet.of(HugePermission.READ);
        Assert.assertSame(HugeAuthenticator.ROLE_NONE,
                          this.login(manager).role());

        this.nanos.addAndGet(secondsToNanos(TTL_SECONDS));
        Assert.assertNotSame(HugeAuthenticator.ROLE_NONE,
                             this.login(manager).role());
    }

    @Test
    public void testZeroExpiryDisablesCache() {
        this.plugin.anyAccess = EnumSet.of(HugePermission.READ);
        RangerAuthManager manager = this.manager(0);

        this.login(manager);
        int afterFirst = this.plugin.probes;
        this.login(manager);

        Assert.assertEquals(afterFirst * 2, this.plugin.probes);
    }

    @Test
    public void testNegativeExpiryIsRejected() {
        Assert.assertThrows(IllegalArgumentException.class,
                            () -> this.manager(-1));
    }

    @Test
    public void testAdminUserIsNeverProbed() {
        AuthManager delegate = (AuthManager) Proxy.newProxyInstance(
                AuthManager.class.getClassLoader(),
                new Class<?>[]{AuthManager.class},
                (proxy, method, args) -> new UserWithRole(
                        IdGenerator.of("a"), HugeAuthenticator.USER_ADMIN,
                        RolePermission.none()));
        RangerAuthManager manager = new RangerAuthManager(
                delegate, this.plugin, (int) TTL_SECONDS, this.nanos::get);

        UserWithRole user = manager.validateUser("admin", "pa");

        Assert.assertTrue(RolePermission.isAdmin(user.role()));
        Assert.assertEquals(0, this.plugin.probes);
    }

    private static long secondsToNanos(long seconds) {
        return seconds * 1_000_000_000L;
    }

    /**
     * The default-graph and default-role methods must reach the delegate
     * directly. The delegate is a different class from the one that declares
     * them, in another package, as AuthManagerProxy is in production, so a
     * reflective lookup on its class would fail where this call does not.
     */
    @Test
    public void testDefaultGraphAndRoleMethodsForwardToDelegate() {
        List<String> calls = new ArrayList<>();
        Id roleId = IdGenerator.of("role");
        Map<String, Date> defaults = new HashMap<>();
        defaults.put("g1", new Date(0L));
        HugeDefaultRole role = HugeDefaultRole.OBSERVER;

        InvocationHandler handler = (proxy, method, args) -> {
            calls.add(method.getName() + List.of(args));
            Class<?> type = method.getReturnType();
            if (type == boolean.class) {
                return true;
            } else if (type == Id.class) {
                return roleId;
            } else if (type == Map.class) {
                return defaults;
            }
            return null;
        };
        AuthManager delegate = (AuthManager) Proxy.newProxyInstance(
                AuthManager.class.getClassLoader(),
                new Class<?>[]{AuthManager.class}, handler);
        RangerAuthManager manager = new RangerAuthManager(
                delegate, this.plugin, 0, this.nanos::get);

        manager.setDefaultGraph("gs", "g", "u");
        manager.unsetDefaultGraph("gs", "g", "u");
        Assert.assertSame(defaults, manager.getDefaultGraph("gs", "u"));
        Assert.assertSame(roleId, manager.createDefaultRole("gs", "o", role, "g"));
        Assert.assertSame(roleId, manager.createSpaceDefaultRole("gs", "o", role));
        Assert.assertTrue(manager.isDefaultRole("gs", "o", role));
        Assert.assertTrue(manager.isDefaultRole("gs", "g", "o", role));
        manager.deleteDefaultRole("gs", "o", role);
        manager.deleteDefaultRole("gs", "o", role, "g");

        Assert.assertEquals(List.of(
                "setDefaultGraph[gs, g, u]",
                "unsetDefaultGraph[gs, g, u]",
                "getDefaultGraph[gs, u]",
                "createDefaultRole[gs, o, " + role + ", g]",
                "createSpaceDefaultRole[gs, o, " + role + "]",
                "isDefaultRole[gs, o, " + role + "]",
                "isDefaultRole[gs, g, o, " + role + "]",
                "deleteDefaultRole[gs, o, " + role + "]",
                "deleteDefaultRole[gs, o, " + role + ", g]"), calls);
    }
}
