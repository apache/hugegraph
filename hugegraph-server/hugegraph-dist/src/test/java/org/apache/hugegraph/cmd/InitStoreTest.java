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

package org.apache.hugegraph.cmd;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.auth.AuthContext;
import org.apache.hugegraph.auth.HugeAuthenticator.User;
import org.apache.hugegraph.auth.HugeGraphAuthProxy;
import org.apache.hugegraph.task.TaskManager;
import org.junit.Assert;
import org.junit.Test;

public class InitStoreTest {

    @Test
    public void testInitBackendFailsFastForPermanentException() throws Exception {
        AtomicInteger invocations = new AtomicInteger();
        IllegalArgumentException exception =
                new IllegalArgumentException("Invalid backend configuration");
        HugeGraph graph = (HugeGraph) Proxy.newProxyInstance(
                HugeGraph.class.getClassLoader(), new Class<?>[]{HugeGraph.class},
                (proxy, invokedMethod, args) -> {
                    if (invokedMethod.getName().equals("initBackend")) {
                        invocations.incrementAndGet();
                        throw exception;
                    }
                    return null;
                });

        Method method = InitStore.class.getDeclaredMethod("initBackend",
                                                          HugeGraph.class);
        method.setAccessible(true);
        try {
            method.invoke(null, graph);
            Assert.fail("Expected initialization to fail");
        } catch (InvocationTargetException e) {
            Assert.assertSame(exception, e.getCause());
        }
        Assert.assertEquals(1, invocations.get());
    }

    @Test
    public void testSystemInfoBootstrapPropagatesAdminToExistingTaskExecutor() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        // Create the worker before the bootstrap command establishes a context.
        executor.submit(HugeGraphAuthProxy::resetContext).get();
        AuthContext.setContext("caller-auth-context");
        TaskManager.setContext("caller-task-context");
        HugeGraphAuthProxy.setRequestGraphSpace("caller_space");
        try {
            AtomicInteger invocations = new AtomicInteger();
            HugeGraph graph = initializationGraph(() -> {
                try {
                    executor.submit(new TaskManager.ContextCallable<>(() -> {
                        Assert.assertEquals(User.ADMIN,
                                            HugeGraphAuthProxy.getContext().user());
                        invocations.incrementAndGet();
                        return null;
                    })).get();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            initializeSystemInfo(graph);
            Assert.assertEquals(1, invocations.get());
            Assert.assertNull(executor.submit(TaskManager::getContext).get());
            assertCallerContextRestored();
        } finally {
            executor.shutdownNow();
            HugeGraphAuthProxy.resetContext();
            TaskManager.resetContext();
        }
    }

    @Test
    public void testSystemInfoBootstrapRestoresContextAfterFailure() throws Exception {
        IllegalStateException failure = new IllegalStateException("Task schema initialization failed");
        AuthContext.setContext("caller-auth-context");
        TaskManager.setContext("caller-task-context");
        HugeGraphAuthProxy.setRequestGraphSpace("caller_space");
        try {
            try {
                initializeSystemInfo(initializationGraph(() -> {
                    throw failure;
                }));
                Assert.fail("Expected initialization failure");
            } catch (InvocationTargetException e) {
                Assert.assertSame(failure, e.getCause());
            }
            assertCallerContextRestored();
        } finally {
            HugeGraphAuthProxy.resetContext();
            TaskManager.resetContext();
        }
    }

    private static HugeGraph initializationGraph(Runnable initialization) {
        return (HugeGraph) Proxy.newProxyInstance(
                HugeGraph.class.getClassLoader(), new Class<?>[]{HugeGraph.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("initSystemInfo")) {
                        initialization.run();
                    }
                    return null;
                });
    }

    private static void initializeSystemInfo(HugeGraph graph) throws Exception {
        Method method = InitStore.class.getDeclaredMethod("initSystemInfo", HugeGraph.class);
        method.setAccessible(true);
        method.invoke(null, graph);
    }

    private static void assertCallerContextRestored() {
        Assert.assertEquals("caller-auth-context", AuthContext.getContext());
        Assert.assertEquals("caller-task-context", TaskManager.getContext());
        Assert.assertEquals("caller_space", HugeGraphAuthProxy.getRequestGraphSpace());
    }
}
