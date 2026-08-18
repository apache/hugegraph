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

package org.apache.hugegraph.unit.auth;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.auth.AuthManager;
import org.apache.hugegraph.auth.HugeAuthenticator;
import org.apache.hugegraph.auth.HugeDefaultRole;
import org.apache.hugegraph.auth.HugeGraphAuthProxy;
import org.apache.hugegraph.auth.HugePermission;
import org.apache.hugegraph.auth.HugeUser;
import org.apache.hugegraph.auth.ResourceObject;
import org.apache.hugegraph.auth.RolePermission;
import org.apache.hugegraph.auth.UserWithRole;
import org.apache.hugegraph.backend.cache.Cache;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.id.IdGenerator;
import org.apache.hugegraph.config.AuthOptions;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.task.TaskManager;
import org.apache.hugegraph.task.TaskScheduler;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.testutil.Whitebox;
import org.apache.hugegraph.unit.BaseUnitTest;
import org.apache.hugegraph.util.RateLimiter;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.config.Property;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.junit.After;
import org.junit.Test;
import org.mockito.Mockito;

import jakarta.ws.rs.ForbiddenException;

public class HugeGraphAuthProxyTest extends BaseUnitTest {

    private static HugeGraphAuthProxy.Context setContext(
            HugeGraphAuthProxy.Context context) {
        try {
            Method method = HugeGraphAuthProxy.class.getDeclaredMethod(
                    "setContext",
                    HugeGraphAuthProxy.Context.class);
            method.setAccessible(true);
            return (HugeGraphAuthProxy.Context) method.invoke(null, context);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @After
    public void tearDown() {
        // Clean up contexts after each test
        HugeGraphAuthProxy.resetContext();
        TaskManager.resetContext();
    }

    @Test
    public void testUsernameWithNullContext() {
        // Ensure no context is set
        HugeGraphAuthProxy.resetContext();
        TaskManager.resetContext();

        // When context is null, username() should return "anonymous"
        String username = HugeGraphAuthProxy.username();
        Assert.assertEquals("anonymous", username);
    }

    @Test
    public void testUsernameWithValidContext() {
        // Create a user with a specific username
        HugeAuthenticator.User user = new HugeAuthenticator.User(
                "test_user",
                RolePermission.admin()
        );

        // Set context with this user
        HugeGraphAuthProxy.Context context = new HugeGraphAuthProxy.Context(user);
        setContext(context);

        // username() should return the user's username
        String username = HugeGraphAuthProxy.username();
        Assert.assertEquals("test_user", username);
    }

    @Test
    public void testUsernameWithAdminUser() {
        // Test with ADMIN user
        HugeAuthenticator.User adminUser = HugeAuthenticator.User.ADMIN;
        HugeGraphAuthProxy.Context context = new HugeGraphAuthProxy.Context(
                adminUser);
        setContext(context);

        String username = HugeGraphAuthProxy.username();
        Assert.assertEquals("admin", username);
    }

    @Test
    public void testRunAsAdminRestoresContext() {
        HugeAuthenticator.User user = new HugeAuthenticator.User(
                "test_user",
                RolePermission.admin()
        );
        setContext(new HugeGraphAuthProxy.Context(user));

        HugeGraphAuthProxy.runAsAdmin(() -> {
            Assert.assertEquals(HugeAuthenticator.USER_ADMIN,
                                HugeGraphAuthProxy.username());
        });

        Assert.assertEquals("test_user", HugeGraphAuthProxy.username());
    }

    @Test
    public void testRunAsAdminOverridesTaskContext() {
        HugeAuthenticator.User taskUser = new HugeAuthenticator.User(
                "task_user",
                RolePermission.admin()
        );
        TaskManager.setContext(taskUser.toJson());

        HugeGraphAuthProxy.runAsAdmin(() -> {
            Assert.assertEquals(HugeAuthenticator.USER_ADMIN,
                                HugeGraphAuthProxy.username());
        });

        Assert.assertEquals("task_user", HugeGraphAuthProxy.username());
    }

    @Test
    public void testRunAsAdminRestoresContextAfterException() {
        HugeAuthenticator.User taskUser = new HugeAuthenticator.User(
                "task_user",
                RolePermission.admin()
        );
        TaskManager.setContext(taskUser.toJson());

        Assert.assertThrows(RuntimeException.class, () -> {
            HugeGraphAuthProxy.runAsAdmin(() -> {
                throw new RuntimeException("expected");
            });
        });

        Assert.assertEquals("task_user", HugeGraphAuthProxy.username());
    }

    @Test
    public void testRunAsAdminDoesNotPropagateToChildThread()
            throws InterruptedException {
        AtomicReference<String> username = new AtomicReference<>();

        HugeGraphAuthProxy.runAsAdmin(() -> {
            Thread child = new Thread(() -> {
                username.set(HugeGraphAuthProxy.username());
            });
            child.start();
            try {
                child.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        });

        Assert.assertEquals("anonymous", username.get());
    }

    @Test
    public void testGetContextReturnsNull() {
        // Ensure both TaskManager context and CONTEXTS are null
        HugeGraphAuthProxy.resetContext();
        TaskManager.resetContext();

        HugeGraphAuthProxy.Context context = HugeGraphAuthProxy.getContext();
        Assert.assertNull(context);
    }

    @Test
    public void testGetContextFromThreadLocal() {
        // Set context via setContext (which sets CONTEXTS ThreadLocal)
        HugeAuthenticator.User user = new HugeAuthenticator.User(
                "thread_local_user",
                RolePermission.admin()
        );
        HugeGraphAuthProxy.Context expectedContext = new HugeGraphAuthProxy.Context(
                user);
        setContext(expectedContext);

        // Ensure TaskManager context is null
        TaskManager.resetContext();

        // getContext() should return the context from CONTEXTS ThreadLocal
        HugeGraphAuthProxy.Context context = HugeGraphAuthProxy.getContext();
        Assert.assertNotNull(context);
        Assert.assertEquals("thread_local_user", context.user().username());
    }

    @Test
    public void testGetContextFromTaskManager() {
        // Clear CONTEXTS ThreadLocal
        HugeGraphAuthProxy.resetContext();

        // Create a user and set it in TaskManager context
        HugeAuthenticator.User user = new HugeAuthenticator.User(
                "task_user",
                RolePermission.admin()
        );
        String userJson = user.toJson();
        TaskManager.setContext(userJson);

        // getContext() should return context from TaskManager
        HugeGraphAuthProxy.Context context = HugeGraphAuthProxy.getContext();
        Assert.assertNotNull(context);
        Assert.assertEquals("task_user", context.user().username());
    }

    @Test
    public void testGetContextPrioritizesTaskManager() {
        // Set both TaskManager context and CONTEXTS ThreadLocal
        HugeAuthenticator.User taskUser = new HugeAuthenticator.User(
                "task_user",
                RolePermission.admin()
        );
        String taskUserJson = taskUser.toJson();
        TaskManager.setContext(taskUserJson);

        HugeAuthenticator.User threadUser = new HugeAuthenticator.User(
                "thread_user",
                RolePermission.admin()
        );
        HugeGraphAuthProxy.Context threadContext = new HugeGraphAuthProxy.Context(
                threadUser);
        setContext(threadContext);

        // getContext() should prioritize TaskManager context
        HugeGraphAuthProxy.Context context = HugeGraphAuthProxy.getContext();
        Assert.assertNotNull(context);
        Assert.assertEquals("task_user", context.user().username());
    }

    @Test
    public void testGetContextWithNullTaskManagerJson() {
        // Clear CONTEXTS ThreadLocal
        HugeGraphAuthProxy.resetContext();

        // Set null in TaskManager
        TaskManager.setContext(null);

        // getContext() should return null
        HugeGraphAuthProxy.Context context = HugeGraphAuthProxy.getContext();
        Assert.assertNull(context);
    }

    @Test
    public void testUsernameAfterResetContext() {
        // Set a context first
        HugeAuthenticator.User user = new HugeAuthenticator.User(
                "temp_user",
                RolePermission.admin()
        );
        HugeGraphAuthProxy.Context context = new HugeGraphAuthProxy.Context(user);
        setContext(context);

        // Verify it's set
        Assert.assertEquals("temp_user", HugeGraphAuthProxy.username());

        // Reset context
        HugeGraphAuthProxy.resetContext();

        // username() should now return "anonymous"
        Assert.assertEquals("anonymous", HugeGraphAuthProxy.username());
    }

    @Test
    public void testDefaultRoleMutationInvalidatesUserRoleCache()
            throws Exception {
        HugeGraph graph = Mockito.mock(HugeGraph.class);
        HugeConfig config = Mockito.mock(HugeConfig.class);
        AuthManager authManager = Mockito.mock(AuthManager.class);
        TaskScheduler scheduler = Mockito.mock(TaskScheduler.class);
        Id storedUserId = IdGenerator.of("stored-user-id");

        Mockito.when(graph.spaceGraphName()).thenReturn("hugegraph");
        Mockito.when(graph.configuration()).thenReturn(config);
        Mockito.when(graph.authManager()).thenReturn(authManager);
        Mockito.when(graph.taskScheduler()).thenReturn(scheduler);
        Mockito.when(config.get(AuthOptions.AUTH_CACHE_EXPIRE))
               .thenReturn(3600L);
        Mockito.when(config.get(AuthOptions.AUTH_CACHE_CAPACITY))
               .thenReturn(100L);
        Mockito.when(config.get(AuthOptions.AUTH_AUDIT_LOG_RATE))
               .thenReturn(1000D);
        Mockito.when(authManager.validateUser("cache_user", "pass"))
               .thenReturn(new UserWithRole(
                           storedUserId, "cache_user",
                           RolePermission.all("hugegraph")));
        Mockito.when(authManager.validateUser("invalid", "wrong"))
               .thenReturn(new UserWithRole("invalid"));
        Mockito.when(authManager.createDefaultRole("DEFAULT", "cache_user",
                                                  HugeDefaultRole.ANALYST,
                                                  "hugegraph"))
               .thenReturn(IdGenerator.of("default-role"));

        HugeGraphAuthProxy proxy = new HugeGraphAuthProxy(graph);
        AuthManager proxyAuthManager = proxy.authManager();

        proxyAuthManager.validateUser("invalid", "wrong");
        proxyAuthManager.validateUser("cache_user", "pass");
        Cache<Id, RateLimiter> auditLimiters =
                Whitebox.getInternalState(proxy, "auditLimiters");
        Assert.assertFalse(auditLimiters.containsKey(
                IdGenerator.of("invalid")));
        Assert.assertTrue(auditLimiters.containsKey(
                IdGenerator.of("cache_user")));
        Assert.assertFalse(auditLimiters.containsKey(storedUserId));
        proxyAuthManager.validateUser("cache_user", "pass");
        Mockito.verify(authManager, Mockito.times(1))
               .validateUser("cache_user", "pass");

        proxyAuthManager.createDefaultRole("DEFAULT", "cache_user",
                                           HugeDefaultRole.ANALYST, "hugegraph");
        proxyAuthManager.validateUser("cache_user", "pass");
        Mockito.verify(authManager, Mockito.times(2))
               .validateUser("cache_user", "pass");
    }

    @Test
    public void testLogoutInvalidatesTokenRoleCache() throws Exception {
        HugeGraph graph = Mockito.mock(HugeGraph.class);
        HugeConfig config = Mockito.mock(HugeConfig.class);
        AuthManager authManager = Mockito.mock(AuthManager.class);
        TaskScheduler scheduler = Mockito.mock(TaskScheduler.class);
        String token = "cached-token";
        Id storedUserId = IdGenerator.of("stored-user-id");

        Mockito.when(graph.spaceGraphName()).thenReturn("hugegraph");
        Mockito.when(graph.configuration()).thenReturn(config);
        Mockito.when(graph.authManager()).thenReturn(authManager);
        Mockito.when(graph.taskScheduler()).thenReturn(scheduler);
        Mockito.when(config.get(AuthOptions.AUTH_CACHE_EXPIRE))
               .thenReturn(3600L);
        Mockito.when(config.get(AuthOptions.AUTH_CACHE_CAPACITY))
               .thenReturn(100L);
        Mockito.when(config.get(AuthOptions.AUTH_AUDIT_LOG_RATE))
               .thenReturn(1000D);
        Mockito.when(authManager.validateUser(token))
               .thenReturn(new UserWithRole(
                           storedUserId, "cache_user",
                           RolePermission.all("hugegraph")));
        Mockito.when(authManager.validateUser("invalid-token"))
               .thenReturn(new UserWithRole(""));

        HugeGraphAuthProxy proxy = new HugeGraphAuthProxy(graph);
        AuthManager proxyAuthManager = proxy.authManager();
        proxyAuthManager.validateUser("invalid-token");
        proxyAuthManager.validateUser(token);
        Cache<Id, RateLimiter> auditLimiters =
                Whitebox.getInternalState(proxy, "auditLimiters");
        Assert.assertEquals(1L, auditLimiters.size());
        Assert.assertTrue(auditLimiters.containsKey(
                IdGenerator.of("cache_user")));
        Assert.assertFalse(auditLimiters.containsKey(storedUserId));
        proxyAuthManager.validateUser(token);
        Mockito.verify(authManager, Mockito.times(1)).validateUser(token);

        proxyAuthManager.logoutUser(token);
        proxyAuthManager.validateUser(token);

        Mockito.verify(authManager).logoutUser(token);
        Mockito.verify(authManager, Mockito.times(2)).validateUser(token);
    }

    @Test
    public void testDeleteUserInvalidatesUsernameAuditLimiter() {
        HugeGraph graph = Mockito.mock(HugeGraph.class);
        HugeConfig config = Mockito.mock(HugeConfig.class);
        AuthManager authManager = Mockito.mock(AuthManager.class);
        TaskScheduler scheduler = Mockito.mock(TaskScheduler.class);
        Id storedUserId = IdGenerator.of("stored-user-id");
        HugeUser storedUser = new HugeUser(storedUserId, "cache_user");

        Mockito.when(graph.spaceGraphName()).thenReturn("hugegraph");
        Mockito.when(graph.configuration()).thenReturn(config);
        Mockito.when(graph.authManager()).thenReturn(authManager);
        Mockito.when(graph.taskScheduler()).thenReturn(scheduler);
        Mockito.when(config.get(AuthOptions.AUTH_CACHE_EXPIRE))
               .thenReturn(3600L);
        Mockito.when(config.get(AuthOptions.AUTH_CACHE_CAPACITY))
               .thenReturn(100L);
        Mockito.when(config.get(AuthOptions.AUTH_AUDIT_LOG_RATE))
               .thenReturn(1000D);
        Mockito.when(authManager.validateUser("cache_user", "pass"))
               .thenReturn(new UserWithRole(
                           storedUserId, "cache_user",
                           RolePermission.all("hugegraph")));
        Mockito.when(authManager.getUser(storedUserId)).thenReturn(storedUser);
        Mockito.when(authManager.isAdminManager("custom_admin"))
               .thenReturn(true);

        HugeGraphAuthProxy proxy = new HugeGraphAuthProxy(graph);
        AuthManager proxyAuthManager = proxy.authManager();
        proxyAuthManager.validateUser("cache_user", "pass");
        Cache<Id, RateLimiter> auditLimiters =
                Whitebox.getInternalState(proxy, "auditLimiters");
        Assert.assertTrue(auditLimiters.containsKey(
                IdGenerator.of("cache_user")));

        setContext(new HugeGraphAuthProxy.Context(
                new HugeAuthenticator.User(
                        HugeAuthenticator.USER_ADMIN,
                        RolePermission.admin())));
        proxyAuthManager.updateUser(storedUser);

        setContext(new HugeGraphAuthProxy.Context(
                new HugeAuthenticator.User(
                        "custom_admin",
                        RolePermission.admin())));
        proxyAuthManager.updateUser(storedUser);
        proxyAuthManager.deleteUser(storedUserId);

        Assert.assertFalse(auditLimiters.containsKey(
                IdGenerator.of("cache_user")));
        Mockito.verify(authManager, Mockito.times(2)).updateUser(storedUser);
        Mockito.verify(authManager).deleteUser(storedUserId);
    }

    @Test
    public void testProxyOverridesEveryScopedDefaultMethod() throws Exception {
        HugeGraph graph = Mockito.mock(HugeGraph.class);
        HugeConfig config = Mockito.mock(HugeConfig.class);
        AuthManager origin = Mockito.mock(AuthManager.class);
        TaskScheduler scheduler = Mockito.mock(TaskScheduler.class);

        Mockito.when(graph.spaceGraphName()).thenReturn("hugegraph");
        Mockito.when(graph.configuration()).thenReturn(config);
        Mockito.when(graph.authManager()).thenReturn(origin);
        Mockito.when(graph.taskScheduler()).thenReturn(scheduler);
        Mockito.when(config.get(AuthOptions.AUTH_CACHE_EXPIRE))
               .thenReturn(3600L);
        Mockito.when(config.get(AuthOptions.AUTH_CACHE_CAPACITY))
               .thenReturn(100L);
        Mockito.when(config.get(AuthOptions.AUTH_AUDIT_LOG_RATE))
               .thenReturn(1000D);
        Mockito.when(origin.supportsGraphSpaceAuth()).thenReturn(true);

        AuthManager proxy = new HugeGraphAuthProxy(graph).authManager();
        Assert.assertTrue(proxy.supportsGraphSpaceAuth());
        for (Method method : AuthManager.class.getMethods()) {
            Class<?>[] parameters = method.getParameterTypes();
            if (!method.isDefault() || parameters.length == 0 ||
                parameters[0] != String.class) {
                continue;
            }
            Assert.assertNotNull(proxy.getClass().getDeclaredMethod(
                                 method.getName(), parameters));
        }
    }

    @Test
    public void testValidateUserDoesNotLogBearerToken() {
        String token = "secret-proxy-bearer-token";
        HugeGraph graph = Mockito.mock(HugeGraph.class);
        HugeConfig config = Mockito.mock(HugeConfig.class);
        AuthManager authManager = Mockito.mock(AuthManager.class);
        TaskScheduler scheduler = Mockito.mock(TaskScheduler.class);

        Mockito.when(graph.spaceGraphName()).thenReturn("hugegraph");
        Mockito.when(graph.configuration()).thenReturn(config);
        Mockito.when(graph.authManager()).thenReturn(authManager);
        Mockito.when(graph.taskScheduler()).thenReturn(scheduler);
        Mockito.when(config.get(AuthOptions.AUTH_CACHE_EXPIRE)).thenReturn(3600L);
        Mockito.when(config.get(AuthOptions.AUTH_CACHE_CAPACITY)).thenReturn(100L);
        Mockito.when(config.get(AuthOptions.AUTH_AUDIT_LOG_RATE)).thenReturn(1000D);
        Mockito.when(authManager.validateUser(token))
               .thenThrow(new IllegalArgumentException("invalid token"));

        TestAppender appender = new TestAppender();
        appender.start();
        LoggerContext context = (LoggerContext) LogManager.getContext(false);
        org.apache.logging.log4j.core.config.Configuration configuration =
                context.getConfiguration();
        String loggerName = HugeGraphAuthProxy.class.getName();
        LoggerConfig logger = new LoggerConfig(loggerName, Level.ERROR, false);
        logger.addAppender(appender, Level.ERROR, null);
        configuration.addLogger(loggerName, logger);
        context.updateLoggers();
        try {
            AuthManager proxy = new HugeGraphAuthProxy(graph).authManager();
            Assert.assertThrows(IllegalArgumentException.class,
                                () -> proxy.validateUser(token));
            Assert.assertFalse(appender.events().isEmpty());
            for (LogEvent event : appender.events()) {
                Assert.assertFalse(event.getMessage().getFormattedMessage()
                                        .contains(token));
            }
        } finally {
            configuration.removeLogger(loggerName);
            context.updateLoggers();
            appender.stop();
        }
    }

    @Test
    public void testTraversalPermissions() throws Exception {
        Traversal.Admin<?, ?> read = __.V().asAdmin();
        Assert.assertTrue(traversalPermissions(read).isEmpty());

        Traversal.Admin<?, ?> write =
                __.addV("person").property("name", "marko").asAdmin();
        Assert.assertEquals(Collections.singleton(HugePermission.WRITE),
                            traversalPermissions(write));

        Traversal.Admin<?, ?> delete = __.V().drop().asAdmin();
        Assert.assertEquals(Collections.singleton(HugePermission.DELETE),
                            traversalPermissions(delete));

        Traversal.Admin<?, ?> parent =
                __.V().sideEffect(__.addE("knows")).asAdmin();
        Assert.assertEquals(Collections.singleton(HugePermission.WRITE),
                            traversalPermissions(parent));
    }

    @Test
    public void testExecuteOnlyCannotCreateVertexWithMerge() throws Exception {
        assertExecuteOnlyCannotMerge(false, true);
    }

    @Test
    public void testExecuteOnlyCannotMatchVertexWithMerge() throws Exception {
        assertExecuteOnlyCannotMerge(true, true);
    }

    @Test
    public void testExecuteOnlyCannotCreateEdgeWithMerge() throws Exception {
        assertExecuteOnlyCannotMerge(false, false);
    }

    @Test
    public void testExecuteOnlyCannotMatchEdgeWithMerge() throws Exception {
        assertExecuteOnlyCannotMerge(true, false);
    }

    @Test
    public void testSameSimpleNameOutsideTinkerPopDoesNotRequireWrite()
            throws Exception {
        Traversal.Admin<Object, Object> traversal = __.identity().asAdmin();
        traversal.addStep(new MergeVertexStep(traversal));

        Assert.assertTrue(traversalPermissions(traversal).isEmpty());
    }

    @Test
    public void testMergeRecursesChildTraversals() throws Exception {
        Traversal.Admin<Object, Object> traversal = __.identity().asAdmin();
        AbstractStep<Object, Object> merge = mergeStep(traversal, true);
        addMergeChild(merge, __.V().drop().asAdmin());
        traversal.addStep(merge);

        Set<HugePermission> permissions = traversalPermissions(traversal);
        Assert.assertEquals(2, permissions.size());
        Assert.assertTrue(permissions.contains(HugePermission.WRITE));
        Assert.assertTrue(permissions.contains(HugePermission.DELETE));
    }

    @Test
    public void testTraversalStrategyListKeepsAuthProxy() {
        HugeGraph graph = Mockito.mock(HugeGraph.class);
        HugeConfig config = Mockito.mock(HugeConfig.class);
        AuthManager authManager = Mockito.mock(AuthManager.class);
        TaskScheduler scheduler = Mockito.mock(TaskScheduler.class);

        Mockito.when(graph.spaceGraphName()).thenReturn("hugegraph");
        Mockito.when(graph.configuration()).thenReturn(config);
        Mockito.when(graph.authManager()).thenReturn(authManager);
        Mockito.when(graph.taskScheduler()).thenReturn(scheduler);
        Mockito.when(config.get(AuthOptions.AUTH_CACHE_EXPIRE))
               .thenReturn(3600L);
        Mockito.when(config.get(AuthOptions.AUTH_CACHE_CAPACITY))
               .thenReturn(100L);
        Mockito.when(config.get(AuthOptions.AUTH_AUDIT_LOG_RATE))
               .thenReturn(1000D);

        GraphTraversalSource traversal =
                new HugeGraphAuthProxy(graph).traversal();
        Assert.assertFalse(traversal.getStrategies().toList().isEmpty());
        traversal.getStrategies().toList().forEach(strategy -> {
            Assert.assertEquals("TraversalStrategyProxy",
                                strategy.getClass().getSimpleName());
        });
    }

    @Test
    public void testSpaceMemberDoesNotGrantMutationPermissions() {
        RolePermission role = RolePermission.fromJson(
                "{\"roles\":{\"DEFAULT\":{\"*\":{" +
                "\"READ\":{\"ALL\":[{\"type\":\"ALL\"}]}," +
                "\"SPACE_MEMBER\":{\"ALL\":[{\"type\":\"ALL\"}]}" +
                "}}}}");
        HugeAuthenticator.RequiredPerm read =
                new HugeAuthenticator.RequiredPerm()
                        .graphSpace("DEFAULT")
                        .owner("hugegraph")
                        .action("read");
        HugeAuthenticator.RequiredPerm write =
                new HugeAuthenticator.RequiredPerm()
                        .graphSpace("DEFAULT")
                        .owner("hugegraph")
                        .action("write");
        HugeAuthenticator.RequiredPerm delete =
                new HugeAuthenticator.RequiredPerm()
                        .graphSpace("DEFAULT")
                        .owner("hugegraph")
                        .action("delete");

        Assert.assertTrue(HugeAuthenticator.RolePerm.matchApiRequiredPerm(
                role, read));
        Assert.assertFalse(HugeAuthenticator.RolePerm.matchApiRequiredPerm(
                role, write));
        Assert.assertFalse(HugeAuthenticator.RolePerm.matchApiRequiredPerm(
                role, delete));
    }

    @Test
    public void testSpaceManagerCanManageUserGrantInOwnSpace() {
        RolePermission managerRole = RolePermission.fromJson(
                "{\"roles\":{\"space-a\":{\"*\":{" +
                "\"SPACE\":{\"ALL\":[{\"type\":\"ALL\"}]}" +
                "}}}}");
        RolePermission memberGrant = RolePermission.fromJson(
                "{\"roles\":{\"space-a\":{\"*\":{" +
                "\"READ\":{\"ALL\":[{\"type\":\"ALL\"}]}," +
                "\"WRITE\":{\"ALL\":[{\"type\":\"ALL\"}]}" +
                "}}}}");
        RolePermission otherSpaceGrant = RolePermission.fromJson(
                "{\"roles\":{\"space-b\":{\"*\":{" +
                "\"READ\":{\"ALL\":[{\"type\":\"ALL\"}]}," +
                "\"WRITE\":{\"ALL\":[{\"type\":\"ALL\"}]}" +
                "}}}}");
        RolePermission multiSpaceGrant = RolePermission.fromJson(
                "{\"roles\":{" +
                "\"space-a\":{\"*\":{" +
                "\"READ\":{\"ALL\":[{\"type\":\"ALL\"}]}}}," +
                "\"space-b\":{\"*\":{" +
                "\"READ\":{\"ALL\":[{\"type\":\"ALL\"}]}}}" +
                "}}");
        HugeUser member = new HugeUser("member");
        ResourceObject<?> ownSpace =
                ResourceObject.of("space-a", "hugegraph", member);
        ResourceObject<?> otherSpace =
                ResourceObject.of("space-b", "hugegraph", member);
        ResourceObject<?> admin =
                ResourceObject.of("space-a", "hugegraph",
                                  new HugeUser(HugeAuthenticator.USER_ADMIN));

        Assert.assertTrue(HugeAuthenticator.RolePerm.match(
                managerRole, memberGrant, ownSpace));
        Assert.assertFalse(HugeAuthenticator.RolePerm.match(
                managerRole, memberGrant, otherSpace));
        Assert.assertFalse(HugeAuthenticator.RolePerm.match(
                managerRole, otherSpaceGrant, ownSpace));
        Assert.assertTrue(HugeAuthenticator.RolePerm.match(
                managerRole, multiSpaceGrant, ownSpace));
        Assert.assertFalse(HugeAuthenticator.RolePerm.match(
                managerRole, memberGrant, admin));
        Assert.assertFalse(HugeAuthenticator.RolePerm.match(
                managerRole, RolePermission.admin(), ownSpace));
    }

    @SuppressWarnings("unchecked")
    private static Set<HugePermission> traversalPermissions(
            Traversal.Admin<?, ?> traversal) throws Exception {
        Method method = HugeGraphAuthProxy.class.getDeclaredMethod(
                "traversalPermissions", Traversal.Admin.class);
        method.setAccessible(true);
        return (Set<HugePermission>) method.invoke(null, traversal);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static void assertExecuteOnlyCannotMerge(boolean onMatch,
                                                     boolean vertex)
                                                     throws Exception {
        Traversal.Admin<Object, Object> traversal = __.identity().asAdmin();
        AbstractStep<Object, Object> merge = mergeStep(traversal, vertex);
        if (onMatch) {
            addMergeChild(merge,
                          __.constant(Collections.emptyMap()).asAdmin());
        }
        traversal.addStep(merge);
        Assert.assertEquals(Collections.singleton(HugePermission.WRITE),
                            traversalPermissions(traversal));

        HugeGraph graph = Mockito.mock(HugeGraph.class);
        HugeConfig config = Mockito.mock(HugeConfig.class);
        AuthManager authManager = Mockito.mock(AuthManager.class);
        TaskScheduler scheduler = Mockito.mock(TaskScheduler.class);
        Mockito.when(graph.name()).thenReturn("hugegraph");
        Mockito.when(graph.graphSpace()).thenReturn("DEFAULT");
        Mockito.when(graph.spaceGraphName()).thenReturn("DEFAULT-hugegraph");
        Mockito.when(graph.configuration()).thenReturn(config);
        Mockito.when(graph.authManager()).thenReturn(authManager);
        Mockito.when(graph.taskScheduler()).thenReturn(scheduler);
        Mockito.when(config.get(AuthOptions.AUTH_CACHE_EXPIRE)).thenReturn(3600L);
        Mockito.when(config.get(AuthOptions.AUTH_CACHE_CAPACITY)).thenReturn(100L);
        Mockito.when(config.get(AuthOptions.AUTH_AUDIT_LOG_RATE)).thenReturn(1000D);

        RolePermission executeOnly = RolePermission.fromJson(
                "{\"roles\":{\"DEFAULT\":{\"hugegraph\":{" +
                "\"EXECUTE\":{\"GREMLIN\":[{" +
                "\"type\":\"GREMLIN\",\"label\":\"*\"," +
                "\"properties\":null}]}}}}}");
        setContext(new HugeGraphAuthProxy.Context(
                new HugeAuthenticator.User("execute-only", executeOnly)));

        TraversalStrategy strategy =
                new HugeGraphAuthProxy(graph).traversal()
                                             .getStrategies().toList().get(0);
        Assert.assertThrows(ForbiddenException.class,
                            () -> strategy.apply(traversal));
    }

    @SuppressWarnings("unchecked")
    private static AbstractStep<Object, Object> mergeStep(
            Traversal.Admin<?, ?> traversal, boolean vertex) throws Exception {
        String type = "org.apache.tinkerpop.gremlin.process.traversal.step.map." +
                      (vertex ? "MergeVertexStep" : "MergeEdgeStep");
        Class<?> mergeClass = Class.forName(type);
        try {
            return (AbstractStep<Object, Object>)
                   mergeClass.getConstructor(Traversal.Admin.class,
                                             boolean.class)
                             .newInstance(traversal, true);
        } catch (NoSuchMethodException ignored) {
            return (AbstractStep<Object, Object>)
                   mergeClass.getConstructor(Traversal.Admin.class)
                             .newInstance(traversal);
        }
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static void addMergeChild(AbstractStep<Object, Object> merge,
                                      Traversal.Admin<?, ?> child)
                                      throws Exception {
        try {
            Class<? extends Enum> mergeToken =
                    (Class<? extends Enum>) Class.forName(
                            "org.apache.tinkerpop.gremlin.process.traversal.Merge");
            Enum<?> onMatch = Enum.valueOf(mergeToken, "onMatch");
            merge.getClass().getMethod("addChildOption", mergeToken,
                                       Traversal.Admin.class)
                 .invoke(merge, onMatch, child);
        } catch (ClassNotFoundException ignored) {
            merge.getClass().getMethod("addChild", Traversal.Admin.class)
                 .invoke(merge, child);
        }
    }

    private static class TestTraversalParent
            extends AbstractStep<Object, Object>
            implements TraversalParent {

        private final List<Traversal.Admin<?, ?>> children;

        TestTraversalParent(Traversal.Admin<?, ?> traversal) {
            super(traversal);
            this.children = new ArrayList<>();
        }

        void addChild(Traversal.Admin<?, ?> child) {
            this.children.add(child);
        }

        @SuppressWarnings({"rawtypes", "unchecked"})
        @Override
        public <S, E> List<Traversal.Admin<S, E>> getLocalChildren() {
            return (List) this.children;
        }

        @Override
        protected Traverser.Admin<Object> processNextStart()
                                                      throws NoSuchElementException {
            throw new NoSuchElementException();
        }
    }

    private static class MergeVertexStep extends TestTraversalParent {

        MergeVertexStep(Traversal.Admin<?, ?> traversal) {
            super(traversal);
        }
    }

    private static class TestAppender extends AbstractAppender {

        private final List<LogEvent> events;

        TestAppender() {
            super("HugeGraphAuthProxyTestAppender", (Filter) null,
                  (Layout<?>) null, true, Property.EMPTY_ARRAY);
            this.events = new ArrayList<>();
        }

        @Override
        public void append(LogEvent event) {
            this.events.add(event.toImmutable());
        }

        List<LogEvent> events() {
            return this.events;
        }
    }
}
