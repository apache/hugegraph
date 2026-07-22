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
import java.util.List;

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.auth.AuthManager;
import org.apache.hugegraph.auth.HugeAuthenticator;
import org.apache.hugegraph.auth.HugeDefaultRole;
import org.apache.hugegraph.auth.HugeGraphAuthProxy;
import org.apache.hugegraph.auth.RolePermission;
import org.apache.hugegraph.auth.UserWithRole;
import org.apache.hugegraph.backend.id.IdGenerator;
import org.apache.hugegraph.config.AuthOptions;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.task.TaskManager;
import org.apache.hugegraph.task.TaskScheduler;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.unit.BaseUnitTest;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.config.Property;
import org.junit.After;
import org.junit.Test;
import org.mockito.Mockito;

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
               .thenReturn(new UserWithRole("cache_user"));
        Mockito.when(authManager.createDefaultRole("DEFAULT", "cache_user",
                                                  HugeDefaultRole.ANALYST,
                                                  "hugegraph"))
               .thenReturn(IdGenerator.of("default-role"));

        HugeGraphAuthProxy proxy = new HugeGraphAuthProxy(graph);
        AuthManager proxyAuthManager = proxy.authManager();

        proxyAuthManager.validateUser("cache_user", "pass");
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
               .thenReturn(new UserWithRole("cache_user"));

        AuthManager proxyAuthManager =
                new HugeGraphAuthProxy(graph).authManager();
        proxyAuthManager.validateUser(token);
        proxyAuthManager.validateUser(token);
        Mockito.verify(authManager, Mockito.times(1)).validateUser(token);

        proxyAuthManager.logoutUser(token);
        proxyAuthManager.validateUser(token);

        Mockito.verify(authManager).logoutUser(token);
        Mockito.verify(authManager, Mockito.times(2)).validateUser(token);
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
