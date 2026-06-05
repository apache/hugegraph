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

import org.apache.hugegraph.auth.HugeAuthenticator;
import org.apache.hugegraph.auth.HugeGraphAuthProxy;
import org.apache.hugegraph.auth.RolePermission;
import org.apache.hugegraph.task.TaskManager;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.unit.BaseUnitTest;
import org.junit.After;
import org.junit.Test;

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
}
