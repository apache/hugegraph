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

package org.apache.hugegraph.unit.core;

import java.io.File;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import javax.script.ScriptException;
import javax.script.SimpleBindings;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.hugegraph.HugeFactory;
import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.HugeGraphParams;
import org.apache.hugegraph.StandardHugeGraph;
import org.apache.hugegraph.auth.AuthContext;
import org.apache.hugegraph.auth.ContextGremlinServer;
import org.apache.hugegraph.auth.HugeAuthenticator;
import org.apache.hugegraph.auth.HugeGraphAuthProxy;
import org.apache.hugegraph.auth.HugePermission;
import org.apache.hugegraph.auth.HugeResource;
import org.apache.hugegraph.auth.RolePermission;
import org.apache.hugegraph.backend.store.BackendFeatures;
import org.apache.hugegraph.backend.store.BackendProviderFactory;
import org.apache.hugegraph.backend.store.BackendStoreProvider;
import org.apache.hugegraph.backend.store.raft.RaftBackendStoreProvider;
import org.apache.hugegraph.backend.store.raft.RaftContext;
import org.apache.hugegraph.backend.store.raft.RaftNode;
import org.apache.hugegraph.backend.store.rocksdb.RocksDBOptions;
import org.apache.hugegraph.backend.store.rocksdb.RocksDBStoreProvider;
import org.apache.hugegraph.config.CoreOptions;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.config.OptionSpace;
import org.apache.hugegraph.config.RpcOptions;
import org.apache.hugegraph.config.ServerOptions;
import org.apache.hugegraph.core.GraphManager;
import org.apache.hugegraph.event.EventHub;
import org.apache.hugegraph.event.EventListener;
import org.apache.hugegraph.exception.HugeException;
import org.apache.hugegraph.rpc.RpcClientProvider;
import org.apache.hugegraph.rpc.RpcServer;
import org.apache.hugegraph.rpc.RpcServiceConfig4Client;
import org.apache.hugegraph.rpc.RpcServiceConfig4Server;
import org.apache.hugegraph.space.GraphSpace;
import org.apache.hugegraph.task.TaskManager;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.testutil.Whitebox;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Events;
import org.apache.hugegraph.meta.MetaManager;
import org.apache.hugegraph.meta.PdMetaDriver;
import org.apache.hugegraph.pd.client.PDClient;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.apache.tinkerpop.gremlin.server.util.DefaultGraphManager;
import org.mockito.Mockito;
import org.apache.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.NodeManager;
import com.alipay.sofa.jraft.core.State;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.option.CliOptions;
import com.alipay.sofa.jraft.rpc.CliRequests;
import com.alipay.sofa.jraft.rpc.impl.BoltRpcServer;
import com.alipay.sofa.jraft.rpc.impl.cli.CliClientServiceImpl;
import com.alipay.sofa.rpc.config.ServerConfig;
import com.alipay.sofa.jraft.rpc.RaftRpcServerFactory;
import com.alipay.sofa.jraft.util.Endpoint;

public class GraphManagerConfigTest {

    @Test
    public void testGraphSpaceDropCleansPdStatisticsBeforeRemovingConfiguration() throws Exception {
        this.checkGraphSpaceStatisticsCleanup(null, false);
    }

    @Test
    public void testGraphSpaceDropKeepsConfigurationWhenPdCleanupFails() throws Exception {
        this.checkGraphSpaceStatisticsCleanup(new PDException(1003, "write failed"), false);
    }

    @Test
    public void testEmptyServerSpaceKeepsRemainingPdGraphDiagnosis() throws Exception {
        this.checkGraphSpaceStatisticsCleanup(new PDException(1003,
                "PD still has 1 graph record in space 'shop': [shop/old/g]. " +
                "Inspect GET /v1/partitionsAndStats"), false);
    }

    @Test
    public void testGraphSpaceDropDoesNotCleanStatisticsWhenGraphRemovalFails() throws Exception {
        this.checkGraphSpaceStatisticsCleanup(null, true);
    }

    @Test
    public void testGraphSpaceDropReportsOldPdRpcAsFailure() throws Exception {
        this.checkGraphSpaceStatisticsCleanup(io.grpc.Status.UNIMPLEMENTED.asRuntimeException(), false);
    }

    @Test
    public void testGraphSpaceDropDoesNotMislabelOtherRpcFailuresAsOldPd() throws Exception {
        this.checkGraphSpaceStatisticsCleanup(io.grpc.Status.UNAVAILABLE.asRuntimeException(), false);
    }

    private void checkGraphSpaceStatisticsCleanup(Exception pdFailure, boolean graphFailure) throws Exception {
        GraphManager manager = Mockito.mock(GraphManager.class, Mockito.CALLS_REAL_METHODS);
        MetaManager meta = Mockito.mock(MetaManager.class);
        PdMetaDriver driver = Mockito.mock(PdMetaDriver.class);
        PDClient client = Mockito.mock(PDClient.class);
        Whitebox.setInternalState(manager, "serviceGraphSpace", "internal");
        Whitebox.setInternalState(manager, "metaManager", meta);
        Whitebox.setInternalState(manager, "graphSpaces", new HashMap<String, GraphSpace>());
        Mockito.doNothing().when(manager).clearGraphSpace("shop");
        Mockito.when(meta.metaDriver()).thenReturn(driver);
        Mockito.when(driver.pdClient()).thenReturn(client);
        if (pdFailure != null) {
            Mockito.doThrow(pdFailure).when(client).deleteGraphSpace("shop");
        }
        if (graphFailure) {
            Mockito.doThrow(new HugeException("graph removal failed")).when(manager).clearGraphSpace("shop");
        }
        if (pdFailure != null || graphFailure) {
            HugeException failure = Assert.assertThrows(HugeException.class,
                    () -> manager.dropGraphSpace("shop"));
            if (pdFailure instanceof PDException) {
                Assert.assertTrue(failure.getMessage().contains(pdFailure.getMessage()));
                Assert.assertSame(pdFailure, failure.getCause());
            }
            if (pdFailure instanceof io.grpc.StatusRuntimeException) {
                io.grpc.StatusRuntimeException rpcFailure = (io.grpc.StatusRuntimeException) pdFailure;
                Assert.assertSame(pdFailure, failure.getCause());
                if (rpcFailure.getStatus().getCode() == io.grpc.Status.Code.UNIMPLEMENTED) {
                    for (String detail : List.of("UNIMPLEMENTED", "Upgrade all PD nodes", "Graphs",
                                                 "authorization", "schema templates", "services",
                                                 "already been cleared", "list entry are retained")) {
                        Assert.assertContains(detail, failure.getMessage());
                    }
                } else {
                    Assert.assertFalse(failure.getMessage().contains("UNIMPLEMENTED"));
                }
            }
            Mockito.verify(meta, Mockito.never()).removeGraphSpaceConfig(Mockito.any());
            Mockito.verify(meta, Mockito.never()).clearGraphSpaceList(Mockito.any());
            Mockito.verify(meta, Mockito.never()).notifyGraphSpaceRemove(Mockito.any());
            if (graphFailure) {
                Mockito.verify(client, Mockito.never()).deleteGraphSpace(Mockito.any());
            }
        } else {
            manager.dropGraphSpace("shop");
            org.mockito.InOrder order = Mockito.inOrder(manager, client, meta);
            order.verify(manager).clearGraphSpace("shop");
            order.verify(client).deleteGraphSpace("shop");
            order.verify(meta).removeGraphSpaceConfig("shop");
            order.verify(meta).clearGraphSpaceList("shop");
            order.verify(meta).notifyGraphSpaceRemove("shop");
        }
    }

    private static ExecutorService previousEventExecutor;
    private static ExecutorService previousTaskDbExecutor;
    private static ExecutorService taskDbExecutor;

    @BeforeClass
    public static void initEventExecutor() {
        // Other suite classes may have destroyed the global EventHub executor.
        ExecutorService previous = Whitebox.getInternalState(EventHub.class, "executor");
        previousEventExecutor = previous != null && !previous.isShutdown() ? previous : null;
        Whitebox.setInternalState(EventHub.class, "executor", null);
        EventHub.init(2);
        previousTaskDbExecutor = Whitebox.getInternalState(TaskManager.instance(), "taskDbExecutor");
        taskDbExecutor = Executors.newSingleThreadExecutor(
                runnable -> new Thread(runnable, "task-db-worker-rpc-config-test"));
        Whitebox.setInternalState(TaskManager.instance(), "taskDbExecutor", taskDbExecutor);
    }

    @AfterClass
    public static void restoreEventExecutor() throws InterruptedException {
        try {
            Assert.assertTrue(EventHub.destroy(5));
        } finally {
            Whitebox.setInternalState(EventHub.class, "executor", previousEventExecutor != null &&
                                      !previousEventExecutor.isShutdown() ? previousEventExecutor : null);
            taskDbExecutor.shutdown();
            try {
                Assert.assertTrue(taskDbExecutor.awaitTermination(5, TimeUnit.SECONDS));
            } finally {
                Whitebox.setInternalState(TaskManager.instance(), "taskDbExecutor", previousTaskDbExecutor);
            }
        }
    }

    @Test
    public void testFixturePreservesLivePreviousEventExecutor() throws Exception {
        this.checkFixtureEventExecutor(false);
    }

    @Test
    public void testFixtureDoesNotRestoreDestroyedEventExecutor() throws Exception {
        this.checkFixtureEventExecutor(true);
    }

    private void checkFixtureEventExecutor(boolean destroyed) throws Exception {
        ExecutorService activeEvent = Whitebox.getInternalState(EventHub.class, "executor");
        ExecutorService activeTask = Whitebox.getInternalState(TaskManager.instance(), "taskDbExecutor");
        ExecutorService savedPreviousEvent = previousEventExecutor;
        ExecutorService savedPreviousTask = previousTaskDbExecutor;
        ExecutorService savedTask = taskDbExecutor;
        ExecutorService prior = Executors.newSingleThreadExecutor();
        try {
            prior.submit(() -> { }).get(5, TimeUnit.SECONDS);
            if (destroyed) {
                prior.shutdown();
                Assert.assertTrue(prior.awaitTermination(5, TimeUnit.SECONDS));
            }
            Whitebox.setInternalState(EventHub.class, "executor", prior);
            initEventExecutor();
            restoreEventExecutor();
            EventHub.init(1);
            EventHub hub = new EventHub("rpc-fixture-executor-control");
            hub.listen("probe", event -> true);
            RuntimeException submission = null;
            try {
                Assert.assertEquals(Integer.valueOf(1), hub.notify("probe").get(5, TimeUnit.SECONDS));
            } catch (RuntimeException failure) {
                submission = failure;
            }
            ExecutorService restored = Whitebox.getInternalState(EventHub.class, "executor");
            Assert.assertNull("Previous destroyed=" + destroyed + ", restoredShutdown=" +
                              restored.isShutdown() + ", notifyFailure=" + submission, submission);
            if (!destroyed) {
                Assert.assertSame(prior, restored);
            } else {
                Assert.assertNotSame(prior, restored);
            }
        } finally {
            ExecutorService current = Whitebox.getInternalState(EventHub.class, "executor");
            if (current != null && current != activeEvent) {
                current.shutdown();
                Assert.assertTrue(current.awaitTermination(5, TimeUnit.SECONDS));
            }
            prior.shutdown();
            Assert.assertTrue(prior.awaitTermination(5, TimeUnit.SECONDS));
            Whitebox.setInternalState(EventHub.class, "executor", activeEvent);
            Whitebox.setInternalState(TaskManager.instance(), "taskDbExecutor", activeTask);
            previousEventExecutor = savedPreviousEvent;
            previousTaskDbExecutor = savedPreviousTask;
            taskDbExecutor = savedTask;
        }
    }

    @Rule
    public TemporaryFolder temporary = new TemporaryFolder();

    @Test
    public void testRpcFeatureCheckPreservesCallbackTransaction() throws Exception {
        for (boolean shared : new boolean[]{false, true}) {
            for (boolean opened : new boolean[]{false, true}) {
                this.checkRpcCallbackTransaction(shared, opened, false);
                if (shared) {
                    this.checkRpcCallbackTransaction(true, opened, true);
                }
            }
        }
    }

    private void checkRpcCallbackTransaction(boolean shared, boolean opened, boolean failing) throws Exception {
        OptionSpace.register("core", CoreOptions.instance());
        PropertiesConfiguration properties = new PropertiesConfiguration();
        properties.setProperty(CoreOptions.BACKEND.name(), "memory");
        properties.setProperty(CoreOptions.STORE.name(), "callback_tx");
        StandardHugeGraph graph = shared ? new SharedMemoryGraph(new HugeConfig(properties)) :
                                  new StandardHugeGraph(new HugeConfig(properties));
        RpcServiceConfig4Server server = Mockito.mock(RpcServiceConfig4Server.class);
        RpcServiceConfig4Client client = Mockito.mock(RpcServiceConfig4Client.class);
        IllegalStateException failure = new IllegalStateException("RPC proxy failed");
        if (failing) {
            Mockito.doThrow(failure).when(client).serviceProxy(Mockito.anyString(), Mockito.any(Class.class));
        }
        Whitebox.invoke(StandardHugeGraph.class, "closeCurrentThreadTransaction", graph);
        ExecutorService callback = Executors.newSingleThreadExecutor();
        try {
            callback.submit(() -> {
                try {
                    if (opened) {
                        graph.tx().open();
                    }
                    if (failing) {
                        Assert.assertThrows(IllegalStateException.class,
                                            () -> graph.registerRpcServices(server, client),
                                            caught -> Assert.assertSame(failure, caught));
                    } else {
                        graph.registerRpcServices(server, client);
                    }
                    Assert.assertEquals(opened, graph.tx().isOpen());
                    AtomicInteger refs = Whitebox.getInternalState(graph.tx(), "refs");
                    Assert.assertEquals(opened ? 1 : 0, refs.get());
                    ThreadLocal<?> transactions = Whitebox.getInternalState(graph.tx(), "transactions");
                    Assert.assertEquals(opened, transactions.get() != null);
                } finally {
                    Whitebox.invoke(StandardHugeGraph.class, "closeCurrentThreadTransaction", graph);
                }
            }).get(5, TimeUnit.SECONDS);
        } finally {
            callback.shutdownNow();
            Assert.assertTrue(callback.awaitTermination(5, TimeUnit.SECONDS));
            graph.close();
        }
    }

    @Test
    public void testRpcFeatureFailurePreservesPrimaryException() throws Exception {
        OptionSpace.register("core", CoreOptions.instance());
        PropertiesConfiguration properties = new PropertiesConfiguration();
        properties.setProperty(CoreOptions.BACKEND.name(), "memory");
        properties.setProperty(CoreOptions.STORE.name(), "feature_failure");
        FeatureFailureGraph graph = new FeatureFailureGraph(new HugeConfig(properties));
        Whitebox.invoke(StandardHugeGraph.class, "closeCurrentThreadTransaction", graph);
        IllegalStateException failure = new IllegalStateException("backend feature read failed");
        IllegalStateException cleanup = new IllegalStateException("transaction listener failed");
        graph.failure = failure;
        graph.tx().addTransactionListener(status -> {
            throw cleanup;
        });
        try {
            Assert.assertThrows(IllegalStateException.class, () -> graph.registerRpcServices(null, null),
                                caught -> Assert.assertSame(failure, caught));
            Assert.assertFalse(graph.tx().isOpen());
            Assert.assertEquals(1, failure.getSuppressed().length);
            Assert.assertSame(cleanup, failure.getSuppressed()[0]);
            ThreadLocal<?> transactions = Whitebox.getInternalState(graph.tx(), "transactions");
            Assert.assertNull(transactions.get());
        } finally {
            graph.failure = null;
            graph.tx().clearTransactionListeners();
            Whitebox.invoke(StandardHugeGraph.class, "closeCurrentThreadTransaction", graph);
            graph.close();
        }
    }

    private static class FeatureFailureGraph extends StandardHugeGraph {

        private RuntimeException failure;

        FeatureFailureGraph(HugeConfig config) {
            super(config);
        }

        @Override
        public BackendFeatures backendStoreFeatures() {
            BackendFeatures features = super.backendStoreFeatures();
            if (this.failure != null) {
                throw this.failure;
            }
            return features;
        }
    }

    @Test
    public void testLocalLoadRollbackUsesAdminScope() throws Exception {
        this.checkLocalLoadRollback(false);
    }

    @Test
    public void testLocalLoadRollbackRetainsOwnerAfterCloseFailure() throws Exception {
        this.checkLocalLoadRollback(true);
    }

    private void checkLocalLoadRollback(boolean foreignTransaction) throws Exception {
        OptionSpace.register("core", CoreOptions.instance());
        OptionSpace.register("rpc", RpcOptions.instance());
        OptionSpace.register("server", ServerOptions.instance());
        String name = foreignTransaction ? "rollback_pending" : "rollback_admin";
        File config = this.temporary.newFile(name + ".properties");
        Files.write(config.toPath(), ("gremlin.graph=" + TrackedAuthMemoryGraph.class.getName() +
                                     "\nbackend=memory\nstore=" + name + "\n").getBytes(StandardCharsets.UTF_8));
        PropertiesConfiguration properties = new PropertiesConfiguration();
        properties.setProperty(ServerOptions.GRAPHS.name(), this.temporary.newFolder(name).getPath());
        properties.setProperty(ServerOptions.USE_PD.name(), false);
        properties.setProperty(RpcOptions.RPC_SERVER_HOST.name(), "127.0.0.1");
        properties.setProperty(RpcOptions.RPC_REMOTE_URL.name(), "127.0.0.1:1");
        GraphManager manager = new GraphManager(new HugeConfig(properties), new EventHub(name));
        RpcServer server = Mockito.mock(RpcServer.class);
        Mockito.when(server.enabled()).thenReturn(true);
        RpcServer originalServer = Whitebox.getInternalState(manager, "rpcServer");
        Mockito.when(server.config()).thenReturn(originalServer.config());
        IllegalStateException failure = new IllegalStateException("RPC publication failed");
        Mockito.doThrow(failure).when(server).exportAll();
        Whitebox.setInternalState(manager, "rpcServer", server);
        Whitebox.setInternalState(manager, "rpcInitialized", true);
        StandardHugeGraph graph = (StandardHugeGraph) HugeFactory.open(new HugeConfig(config.getPath()));
        graph.initBackend();
        Whitebox.invoke(StandardHugeGraph.class, "closeCurrentThreadTransaction", graph);
        CountDownLatch ready = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        ExecutorService worker = Executors.newSingleThreadExecutor();
        Future<?> transaction = foreignTransaction ? worker.submit(() -> {
            graph.tx().open();
            ready.countDown();
            try {
                Assert.assertTrue(release.await(5, TimeUnit.SECONDS));
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(interrupted);
            } finally {
                Whitebox.invoke(StandardHugeGraph.class, "closeCurrentThreadTransaction", graph);
            }
        }) : null;
        String caller = foreignTransaction ? HugeAuthenticator.User.ADMIN.toJson() :
                        new HugeAuthenticator.User("writer", new WriterRole("DEFAULT", name)).toJson();
        AuthContext.setContext(caller);
        TaskManager.setContext(caller);
        HugeGraphAuthProxy.setRequestGraphSpace("DEFAULT");
        try {
            if (foreignTransaction) {
                Assert.assertTrue(ready.await(5, TimeUnit.SECONDS));
            }
            manager.loadGraphsFromLocal(Map.of(name, config.getPath()));
            Assert.assertEquals(caller, AuthContext.getContext());
            Assert.assertEquals(caller, TaskManager.getContext());
            Assert.assertEquals("DEFAULT", HugeGraphAuthProxy.getRequestGraphSpace());
            Map<?, ?> owners = Whitebox.getInternalState(HugeFactory.class, "GRAPHS");
            if (foreignTransaction) {
                Assert.assertFalse(graph.closed());
                Assert.assertSame(graph, owners.get(graph.spaceGraphName()));
                Assert.assertSame(TrackedAuthMemoryGraph.last, manager.graph(graph.spaceGraphName()));
                Assert.assertEquals(1, failure.getSuppressed().length);
            } else {
                Assert.assertTrue(graph.closed());
                Assert.assertFalse(owners.containsKey(graph.spaceGraphName()));
                Assert.assertFalse(manager.graphs().contains(graph.spaceGraphName()));
                Assert.assertEquals(0, failure.getSuppressed().length);
            }
        } finally {
            release.countDown();
            if (transaction != null) {
                transaction.get(5, TimeUnit.SECONDS);
            }
            worker.shutdownNow();
            Assert.assertTrue(worker.awaitTermination(5, TimeUnit.SECONDS));
            HugeGraphAuthProxy.runAsAdmin(() -> {
                manager.close();
                try {
                    graph.close();
                } catch (Exception cleanup) {
                    throw new IllegalStateException(cleanup);
                }
            });
            HugeFactory.remove(graph);
            TrackedAuthMemoryGraph.last = null;
            HugeGraphAuthProxy.resetContext();
            TaskManager.resetContext();
        }
    }

    @Test
    public void testRetainedLoadCandidateIsNotRepublishedAtRpcStartup() throws Exception {
        this.checkRetainedCandidateRpcStartup(false);
    }

    @Test
    public void testRetainedCreateCandidateIsNotRepublishedAtRpcStartup() throws Exception {
        this.checkRetainedCandidateRpcStartup(true);
    }

    private void checkRetainedCandidateRpcStartup(boolean create) throws Exception {
        String name = create ? "failed_create_startup" : "failed_load_startup";
        GraphManager manager = this.dropManager(name);
        RpcServer original = Whitebox.getInternalState(manager, "rpcServer");
        RpcServer server = Mockito.spy(original);
        RpcClientProvider client = Whitebox.getInternalState(manager, "rpcClient");
        Whitebox.setInternalState(manager, "rpcServer", server);
        Whitebox.setInternalState(manager, "rpcInitialized", true);
        ExecutorService worker = Executors.newSingleThreadExecutor();
        CountDownLatch ready = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicReference<Future<?>> transaction = new AtomicReference<>();
        IllegalStateException rejected = new IllegalStateException("candidate RPC export rejected");
        Mockito.doAnswer(invocation -> {
            HugeGraph graph = TrackedSharedMemoryGraph.last;
            Assert.assertNotNull(graph);
            if (!create) {
                graph.initBackend();
                Whitebox.invoke(StandardHugeGraph.class, "closeCurrentThreadTransaction", graph);
            }
            transaction.set(worker.submit(() -> {
                graph.tx().open();
                ready.countDown();
                try {
                    E.checkState(release.await(10, TimeUnit.SECONDS), "Candidate transaction barrier timed out");
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(interrupted);
                } finally {
                    Whitebox.invoke(StandardHugeGraph.class, "closeCurrentThreadTransaction", graph);
                }
            }));
            Assert.assertTrue(ready.await(5, TimeUnit.SECONDS));
            throw rejected;
        }).when(server).exportAll();
        try {
            String configText = "gremlin.graph=" + TrackedSharedMemoryGraph.class.getName() +
                                "\nbackend=memory\nstore=" + name + "\n";
            if (create) {
                Assert.assertThrows(IllegalStateException.class, () -> manager.createGraphLocal(name, configText),
                                    caught -> Assert.assertSame(rejected, caught));
            } else {
                File config = this.temporary.newFile(name + ".properties");
                Files.write(config.toPath(), configText.getBytes(StandardCharsets.UTF_8));
                manager.loadGraphsFromLocal(Map.of(name, config.getPath()));
            }
            HugeGraph candidate = TrackedSharedMemoryGraph.last;
            Assert.assertFalse(candidate.closed());
            Assert.assertSame(candidate, manager.graph(candidate.spaceGraphName()));
            Assert.assertEquals(1, rejected.getSuppressed().length);
            assertRpcResources(server, client, 0);
            Mockito.doCallRealMethod().when(server).exportAll();
            Whitebox.setInternalState(manager, "rpcInitialized", false);
            Whitebox.invoke(GraphManager.class, "startRpcServer", manager);
            Assert.assertSame(candidate, manager.graph(candidate.spaceGraphName()));
            Assert.assertFalse(candidate.closed());
            assertRpcResources(server, client, 0);
        } finally {
            release.countDown();
            if (transaction.get() != null) {
                transaction.get().get(5, TimeUnit.SECONDS);
            }
            worker.shutdown();
            Assert.assertTrue(worker.awaitTermination(5, TimeUnit.SECONDS));
            manager.close();
            if (TrackedSharedMemoryGraph.last != null) {
                TrackedSharedMemoryGraph.last.close();
                HugeFactory.remove(TrackedSharedMemoryGraph.last);
            }
            TrackedSharedMemoryGraph.last = null;
        }
    }

    public static class TrackedSharedMemoryGraph extends SharedMemoryGraph {

        private static TrackedSharedMemoryGraph last;

        public TrackedSharedMemoryGraph(HugeConfig config) {
            super(config);
        }

        public static TrackedSharedMemoryGraph open(Configuration config) {
            last = new TrackedSharedMemoryGraph(new HugeConfig(config));
            return last;
        }
    }

    public static class TrackedAuthMemoryGraph {

        private static HugeGraph last;

        public static HugeGraph open(Configuration config) {
            last = new HugeGraphAuthProxy(HugeFactory.open(config));
            return last;
        }
    }

    @Test
    public void testAuthenticatedRpcCleanupPreservesOwnerUntilAdmin() throws Exception {
        OptionSpace.register("core", CoreOptions.instance());
        OptionSpace.register("rpc", RpcOptions.instance());
        OptionSpace.register("server", ServerOptions.instance());
        PropertiesConfiguration properties = new PropertiesConfiguration();
        properties.setProperty(ServerOptions.GRAPHS.name(), this.temporary.newFolder("auth").getPath());
        properties.setProperty(ServerOptions.USE_PD.name(), false);
        properties.setProperty(ServerOptions.GRAPH_LOAD_FROM_LOCAL_CONFIG.name(), false);
        properties.setProperty(ServerOptions.ENABLE_DYNAMIC_CREATE_DROP.name(), true);
        properties.setProperty(RpcOptions.RPC_SERVER_HOST.name(), "127.0.0.1");
        properties.setProperty(RpcOptions.RPC_SERVER_PORT.name(), 0);
        try (ServerSocket reservation = new ServerSocket(0)) {
            properties.setProperty(RpcOptions.RPC_REMOTE_URL.name(), "127.0.0.1:" + reservation.getLocalPort());
        }
        EventHub hub = new EventHub("authenticated-rpc-cleanup");
        GraphManager manager = new GraphManager(new HugeConfig(properties), hub);
        String originalThreadName = Thread.currentThread().getName();
        AuthContext.setContext(HugeAuthenticator.User.ADMIN.toJson());
        TaskManager.setContext(HugeAuthenticator.User.ADMIN.toJson());
        try {
            String factory = "gremlin.graph=" + AuthSharedMemoryGraph.class.getName() + "\nbackend=memory\nstore=";
            manager.createGraphLocal("auth_startup", factory + "auth_startup\n");
            Whitebox.invoke(GraphManager.class, "startRpcServer", manager);
            HugeGraph owner = manager.createGraphLocal("auth_late", factory + "auth_late\n");
            RpcServer server = Whitebox.getInternalState(manager, "rpcServer");
            RpcClientProvider client = Whitebox.getInternalState(manager, "rpcClient");
            assertRpcResources(server, client, 4);
            EventListener rejection = event -> {
                throw new IllegalStateException("reject authenticated candidate");
            };
            hub.listen(Events.GRAPH_CREATE, rejection);
            try {
                Assert.assertThrows(HugeException.class, () -> manager.createGraphLocal(
                        "auth_rejected", factory + "auth_rejected\n"));
            } finally {
                hub.unlisten(Events.GRAPH_CREATE, rejection);
            }
            assertRpcResources(server, client, 4);
            TaskManager.resetContext();
            RolePermission execute = RolePermission.role(owner.graphSpace(), owner.name(), HugePermission.EXECUTE);
            Assert.assertFalse(RolePermission.isAdmin(execute));
            AuthContext.setContext(new HugeAuthenticator.User("reader", execute).toJson());
            SimpleBindings bindings = new SimpleBindings();
            bindings.put("graph", owner);
            GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine();
            Assert.assertThrows(ScriptException.class, () -> engine.eval("graph.unregisterRpcServices()", bindings),
                                error -> Assert.assertContains("Permission denied", error.getMessage()));
            assertRpcResources(server, client, 4);
            AuthContext.resetContext();
            Assert.assertThrows(IllegalStateException.class, owner::unregisterRpcServices,
                                error -> Assert.assertContains("Missing authentication context", error.getMessage()));
            assertRpcResources(server, client, 4);
            HugeGraphAuthProxy.runAsAdmin(owner::unregisterRpcServices);
            assertRpcResources(server, client, 2);
            AuthContext.setContext(HugeAuthenticator.User.ADMIN.toJson());
            // Use the normal PD-drop branch with explicit admin identity and no placement metadata writes.
            Whitebox.setInternalState(manager, "PDExist", true);
            Map<String, GraphSpace> spaces = Whitebox.getInternalState(manager, "graphSpaces");
            spaces.put("DEFAULT", new GraphSpace("DEFAULT"));
            Thread.currentThread().setName("grpc-auth-cleanup-test");
            manager.dropGraph("DEFAULT", "auth_late", false);
            Assert.assertTrue(owner.closed());
            assertRpcResources(server, client, 2);
        } finally {
            Thread.currentThread().setName(originalThreadName);
            try {
                HugeGraphAuthProxy.runAsAdmin(manager::close);
            } finally {
                HugeGraphAuthProxy.resetContext();
                TaskManager.resetContext();
            }
        }
    }

    @Test
    public void testAuthenticatedPublicationPreservesCallerScope() throws Exception {
        OptionSpace.register("core", CoreOptions.instance());
        OptionSpace.register("rpc", RpcOptions.instance());
        OptionSpace.register("server", ServerOptions.instance());
        PropertiesConfiguration properties = new PropertiesConfiguration();
        properties.setProperty(ServerOptions.GRAPHS.name(), this.temporary.newFolder("lazy").getPath());
        properties.setProperty(ServerOptions.USE_PD.name(), false);
        properties.setProperty(ServerOptions.GRAPH_LOAD_FROM_LOCAL_CONFIG.name(), false);
        properties.setProperty(RpcOptions.RPC_SERVER_HOST.name(), "127.0.0.1");
        properties.setProperty(RpcOptions.RPC_SERVER_PORT.name(), 0);
        try (ServerSocket reservation = new ServerSocket(0)) {
            properties.setProperty(RpcOptions.RPC_REMOTE_URL.name(), "127.0.0.1:" + reservation.getLocalPort());
        }
        GraphManager manager = new GraphManager(new HugeConfig(properties), new EventHub("lazy-auth-rpc"));
        SharedMemoryGraph warm = null;
        try {
            Whitebox.invoke(GraphManager.class, "startRpcServer", manager);
            String space = "rpcscope";
            String name = "lazy_auth";
            PropertiesConfiguration config = new PropertiesConfiguration();
            config.setProperty(CoreOptions.BACKEND.name(), "memory");
            config.setProperty(CoreOptions.STORE.name(), name);
            config.setProperty(CoreOptions.GRAPH_SPACE.name(), space);
            warm = new SharedMemoryGraph(new HugeConfig(config));
            warm.initBackend();
            HugeGraph owner = new HugeGraphAuthProxy(warm);
            RolePermission role = new WriterRole(space, name);
            Assert.assertFalse(RolePermission.isAdmin(role));
            String caller = new HugeAuthenticator.User("writer", role).toJson();
            AuthContext.setContext(caller);
            TaskManager.setContext(caller);
            HugeGraphAuthProxy.setRequestGraphSpace(space);
            Whitebox.invoke(GraphManager.class, new Class<?>[]{String.class, HugeGraph.class},
                            "publishGraph", manager, owner.spaceGraphName(), owner);
            Assert.assertSame(owner, manager.graph(space + "-" + name));
            Assert.assertEquals(caller, AuthContext.getContext());
            Assert.assertEquals(caller, TaskManager.getContext());
            Assert.assertEquals(space, HugeGraphAuthProxy.getRequestGraphSpace());
            Assert.assertEquals("writer", HugeGraphAuthProxy.username());
            RpcServer server = Whitebox.getInternalState(manager, "rpcServer");
            RpcClientProvider client = Whitebox.getInternalState(manager, "rpcClient");
            assertRpcResources(server, client, 2);
            Assert.assertThrows(jakarta.ws.rs.ForbiddenException.class, owner::unregisterRpcServices);
            assertRpcResources(server, client, 2);
        } finally {
            HugeGraph retained = warm;
            HugeGraphAuthProxy.runAsAdmin(() -> {
                manager.close();
                if (retained != null) {
                    try {
                        retained.close();
                    } catch (Exception failure) {
                        throw new IllegalStateException(failure);
                    } finally {
                        HugeFactory.remove(retained);
                    }
                }
            });
            HugeGraphAuthProxy.resetContext();
            TaskManager.resetContext();
        }
    }

    private static class WriterRole extends RolePermission {

        WriterRole(String space, String graph) {
            this.add(space, graph, HugePermission.READ, HugeResource.ALL_RES);
            this.add(space, graph, HugePermission.WRITE, HugeResource.ALL_RES);
        }
    }

    public static class AuthSharedMemoryGraph {

        public static HugeGraph open(Configuration config) {
            return new HugeGraphAuthProxy(new SharedMemoryGraph(new HugeConfig(config)));
        }
    }

    @Test
    public void testLegacyDefaultCleanupDoesNotAbortDropWithoutRpc() throws Exception {
        this.checkLegacyDrop(false, false);
    }

    @Test
    public void testActualCleanupFailureStillPropagates() throws Exception {
        this.checkLegacyDrop(true, false);
    }

    @Test
    public void testClearCleanupFailurePreservesOwnerForRetry() throws Exception {
        this.checkLegacyDrop(true, true);
    }

    @Test
    public void testQuotaUpdateFailurePreservesOwnerAndCountForRetry() throws Exception {
        this.checkLegacyDrop(false, false, true);
    }

    private void checkLegacyDrop(boolean failCleanup, boolean clear) throws Exception {
        this.checkLegacyDrop(failCleanup, clear, false);
    }

    private void checkLegacyDrop(boolean failCleanup, boolean clear, boolean failQuota) throws Exception {
        OptionSpace.register("core", CoreOptions.instance());
        OptionSpace.register("rpc", RpcOptions.instance());
        OptionSpace.register("server", ServerOptions.instance());
        PropertiesConfiguration properties = new PropertiesConfiguration();
        properties.setProperty(ServerOptions.GRAPHS.name(), this.temporary.newFolder("legacy").getPath());
        properties.setProperty(ServerOptions.GRAPH_LOAD_FROM_LOCAL_CONFIG.name(), false);
        properties.setProperty(ServerOptions.USE_PD.name(), false);
        properties.setProperty(ServerOptions.ENABLE_DYNAMIC_CREATE_DROP.name(), true);
        properties.setProperty(RpcOptions.RPC_SERVER_HOST.name(), "");
        properties.setProperty(RpcOptions.RPC_REMOTE_URL.name(), "");
        EventHub hub = new EventHub("legacy-rpc-cleanup-test");
        GraphManager manager = new GraphManager(new HugeConfig(properties), hub);
        HugeGraph delegate = null;
        String originalThreadName = Thread.currentThread().getName();
        try {
            delegate = manager.createGraphLocal("legacy_cleanup", "gremlin.graph=" + HugeFactory.class.getName() +
                                                                  "\nbackend=memory\nstore=legacy_cleanup\n");
            HugeGraph owner = delegate;
            IllegalStateException failure = new IllegalStateException("actual owner cleanup failed");
            AtomicBoolean failing = new AtomicBoolean(failCleanup);
            HugeGraph legacy = (HugeGraph) Proxy.newProxyInstance(HugeGraph.class.getClassLoader(),
                    new Class<?>[]{HugeGraph.class}, (proxy, method, args) -> {
                        if ("unregisterRpcServices".equals(method.getName())) {
                            if (failing.get()) {
                                throw failure;
                            }
                            return InvocationHandler.invokeDefault(proxy, method, args);
                        }
                        try {
                            return method.invoke(owner, args);
                        } catch (InvocationTargetException error) {
                            throw error.getCause();
                        }
                    });
            Map<String, HugeGraph> graphs = Whitebox.getInternalState(manager, "graphs");
            graphs.put(legacy.spaceGraphName(), legacy);
            DefaultGraphManager bindings = new DefaultGraphManager(new Settings());
            bindings.putGraph(legacy.spaceGraphName(), legacy);
            CountDownLatch dropped = new CountDownLatch(1);
            hub.listen(Events.GRAPH_DROP, event -> {
                if (bindings.getGraph(legacy.spaceGraphName()) == legacy) {
                    try {
                        bindings.removeGraph(legacy.spaceGraphName());
                    } catch (Exception failureToUnbind) {
                        throw new IllegalStateException(failureToUnbind);
                    }
                }
                dropped.countDown();
                return true;
            });
            MetaManager meta = Mockito.mock(MetaManager.class);
            Whitebox.setInternalState(manager, "metaManager", meta);
            RpcServer server = Whitebox.getInternalState(manager, "rpcServer");
            RpcClientProvider client = Whitebox.getInternalState(manager, "rpcClient");
            Assert.assertFalse(server.enabled());
            Assert.assertFalse(client.enabled());
            // Reach the normal PD-drop branch without booting PD or writing placement metadata.
            Whitebox.setInternalState(manager, "PDExist", true);
            Map<String, GraphSpace> spaces = Whitebox.getInternalState(manager, "graphSpaces");
            GraphSpace space = new GraphSpace("DEFAULT");
            space.graphNumberUsed(5);
            spaces.put("DEFAULT", space);
            if (failQuota) {
                Mockito.doThrow(failure).doNothing().when(meta).updateGraphSpaceConfig("DEFAULT", space);
            } else {
                Thread.currentThread().setName("grpc-legacy-cleanup-test");
            }
            if (failCleanup || failQuota) {
                Assert.assertThrows(IllegalStateException.class, () -> manager.dropGraph(
                        "DEFAULT", "legacy_cleanup", clear), caught -> Assert.assertSame(failure, caught));
                Assert.assertEquals(!clear || failQuota, owner.closed());
                Assert.assertSame(legacy, manager.graph(legacy.spaceGraphName()));
                Assert.assertSame(legacy, bindings.getGraph(legacy.spaceGraphName()));
                Map<?, ?> factory = Whitebox.getInternalState(HugeFactory.class, "GRAPHS");
                Assert.assertSame(owner, factory.get(legacy.spaceGraphName()));
                Assert.assertEquals(1L, dropped.getCount());
                if (failCleanup) {
                    Mockito.verifyNoInteractions(meta);
                }
                Assert.assertEquals(5, space.graphNumberUsed());
                failing.set(false);
            }
            manager.dropGraph("DEFAULT", "legacy_cleanup", clear);
            Assert.assertTrue(dropped.await(5, TimeUnit.SECONDS));
            Assert.assertTrue(owner.closed());
            Map<?, ?> factory = Whitebox.getInternalState(HugeFactory.class, "GRAPHS");
            Assert.assertFalse(factory.containsKey(legacy.spaceGraphName()));
            Assert.assertNull(bindings.getGraph(legacy.spaceGraphName()));
            Assert.assertFalse(manager.graphs().contains(legacy.spaceGraphName()));
            if (failQuota) {
                Assert.assertEquals(4, space.graphNumberUsed());
                Mockito.verify(meta, Mockito.times(2)).updateGraphSpaceConfig("DEFAULT", space);
            }
        } finally {
            Thread.currentThread().setName(originalThreadName);
            manager.close();
            if (delegate != null) {
                delegate.close();
                HugeFactory.remove(delegate);
            }
        }
    }

    @Test
    public void testPeerRemovalBatchContinuesAfterOpenTransaction() throws Exception {
        GraphManager manager = this.dropManager("peer_batch");
        HugeGraph busy = null;
        HugeGraph next = null;
        ExecutorService worker = Executors.newSingleThreadExecutor();
        CountDownLatch opened = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        String threadName = Thread.currentThread().getName();
        try {
            busy = manager.createGraphLocal("peer_busy", sharedMemoryConfig("peer_busy"));
            next = manager.createGraphLocal("peer_next", sharedMemoryConfig("peer_next"));
            Whitebox.invoke(GraphManager.class, "startRpcServer", manager);
            MetaManager meta = Mockito.mock(MetaManager.class);
            Mockito.when(meta.extractGraphsFromResponse("batch")).thenReturn(
                    List.of(busy.spaceGraphName(), next.spaceGraphName()));
            Whitebox.setInternalState(manager, "metaManager", meta);
            Whitebox.setInternalState(manager, "PDExist", true);
            Thread.currentThread().setName("grpc-peer-removal-test");
            HugeGraph owner = busy;
            Future<?> transaction = worker.submit(() -> {
                try {
                    owner.tx().open();
                    opened.countDown();
                    E.checkState(release.await(10, TimeUnit.SECONDS), "Peer transaction barrier timed out");
                } catch (InterruptedException failure) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(failure);
                } finally {
                    Whitebox.invoke(StandardHugeGraph.class, "closeCurrentThreadTransaction", owner);
                }
            });
            Assert.assertTrue(opened.await(5, TimeUnit.SECONDS));
            Whitebox.invoke(GraphManager.class, new Class<?>[]{Object.class},
                            "graphRemoveHandler", manager, "batch");
            HugeException refusal = Assert.assertThrows(HugeException.class, () -> manager.dropGraph(
                    "DEFAULT", "peer_busy", false));
            Assert.assertTrue(refusal.getCause() instanceof IllegalStateException);
            Assert.assertContains("Ensure scheduler and tx closed", refusal.getCause().getMessage());
            Assert.assertFalse(busy.closed());
            Assert.assertSame(busy, manager.graph(busy.spaceGraphName()));
            Assert.assertTrue(next.closed());
            Assert.assertFalse(manager.graphs().contains(next.spaceGraphName()));
            RpcServer server = Whitebox.getInternalState(manager, "rpcServer");
            RpcClientProvider client = Whitebox.getInternalState(manager, "rpcClient");
            assertRpcResources(server, client, 2);
            release.countDown();
            transaction.get(5, TimeUnit.SECONDS);
        } finally {
            release.countDown();
            worker.shutdownNow();
            Assert.assertTrue(worker.awaitTermination(5, TimeUnit.SECONDS));
            Thread.currentThread().setName(threadName);
            manager.close();
            if (busy != null) {
                HugeFactory.remove(busy);
            }
            if (next != null) {
                HugeFactory.remove(next);
            }
        }
    }

    @Test
    public void testClosedGraphCloseFailureRetainsOwnerUntilRetry() throws Exception {
        this.checkPostCloseDrop(false);
    }

    @Test
    public void testClosedGraphRpcFailureRetriesWithClear() throws Exception {
        this.checkPostCloseDrop(true);
    }

    @Test
    public void testReceivedSameNameAddLoadsLatestAfterOwnerCleanup() throws Exception {
        this.checkReceivedSameNameAddAfterCleanup(true);
    }

    @Test
    public void testSameNameAddAfterOwnerCleanupLoadsLatestConfig() throws Exception {
        this.checkReceivedSameNameAddAfterCleanup(false);
    }

    @Test
    public void testReceivedSameNameAddDoesNotReloadMissingConfig() throws Exception {
        this.checkReceivedSameNameAddAfterCleanup(true, 1);
    }

    @Test
    public void testReplacementLoadFailureDoesNotFailCompletedDelete() throws Exception {
        this.checkReceivedSameNameAddAfterCleanup(true, 2);
    }

    @Test
    public void testPeerRemoveFailureRetriesOnSameNameAdd() throws Exception {
        this.checkReceivedSameNameAddAfterCleanup(true, 0, true);
    }

    @Test
    public void testLateSelfAddRetriesFailedPeerRemoval() throws Exception {
        this.checkReceivedSameNameAddAfterCleanup(false, 0, true, true);
    }

    @Test
    public void testPeerRemoveFailureDoesNotReloadMissingConfig() throws Exception {
        this.checkReceivedSameNameAddAfterCleanup(true, 1, true);
    }

    private void checkReceivedSameNameAddAfterCleanup(boolean addDuringCleanup) throws Exception {
        this.checkReceivedSameNameAddAfterCleanup(addDuringCleanup, 0);
    }

    private void checkReceivedSameNameAddAfterCleanup(boolean addDuringCleanup, int replayFailure) throws Exception {
        this.checkReceivedSameNameAddAfterCleanup(addDuringCleanup, replayFailure, false);
    }

    private void checkReceivedSameNameAddAfterCleanup(boolean addDuringCleanup, int replayFailure,
                                                     boolean peer) throws Exception {
        this.checkReceivedSameNameAddAfterCleanup(addDuringCleanup, replayFailure, peer, false);
    }

    private void checkReceivedSameNameAddAfterCleanup(boolean addDuringCleanup, int replayFailure,
                                                     boolean peer, boolean lateSelfAdd) throws Exception {
        String name = lateSelfAdd ? "late_self_add_peer_retry" :
                      replayFailure == 1 ? "received_add_missing" : replayFailure == 2 ? "received_add_retry" :
                      addDuringCleanup ? "received_add_early" : "received_add_late";
        String key = "DEFAULT-" + name;
        GraphManager manager = this.dropManager(name);
        EventHub hub = Whitebox.getInternalState(manager, "eventHub");
        Settings settings = new Settings();
        settings.gremlinPool = 1;
        settings.threadPoolBoss = 1;
        settings.threadPoolWorker = 1;
        ContextGremlinServer gremlin = new ContextGremlinServer(settings, hub);
        org.apache.tinkerpop.gremlin.server.GraphManager bindings =
                gremlin.getServerGremlinExecutor().getGraphManager();
        HugeGraph owner = null;
        HugeGraph replacement = null;
        AtomicBoolean reject = new AtomicBoolean(true);
        AtomicBoolean rejectReplay = new AtomicBoolean();
        IllegalStateException readFailure = new IllegalStateException("replacement metadata read rejected");
        AtomicInteger cleanupAttempts = new AtomicInteger();
        AtomicInteger singleConfigReads = new AtomicInteger();
        AtomicInteger allConfigReads = new AtomicInteger();
        AtomicReference<Map<String, Object>> latest = new AtomicReference<>();
        try {
            owner = manager.createGraphLocal(name, "gremlin.graph=" + HugeFactory.class.getName() +
                    "\nbackend=memory\nstore=" + name + "\nalias.graph.id=old-config-marker\n");
            HugeGraph graph = owner;
            Map<String, HugeGraph> factories = Whitebox.getInternalState(HugeFactory.class, "GRAPHS");
            Assert.assertSame(graph, factories.get(key));
            Whitebox.invoke(GraphManager.class, "startRpcServer", manager);
            RpcServer server = Whitebox.getInternalState(manager, "rpcServer");
            RpcClientProvider client = Whitebox.getInternalState(manager, "rpcClient");
            // Memory is not a shared backend: actual cache-RPC configuration stays empty.
            assertRpcResources(server, client, 0);
            BackendStoreProvider provider = Mockito.spy(owner.storeProvider());
            Whitebox.setInternalState(owner, "storeProvider", provider);
            IllegalStateException failure = new IllegalStateException("old owner cleanup is retained");
            Mockito.doAnswer(invocation -> {
                invocation.callRealMethod();
                cleanupAttempts.incrementAndGet();
                if (reject.get()) {
                    throw failure;
                }
                return null;
            }).when(provider).close();
            MetaManager meta = Mockito.mock(MetaManager.class);
            GraphSpace space = new GraphSpace("DEFAULT");
            space.graphNumberUsed(5);
            Mockito.when(meta.graphSpace("DEFAULT")).thenReturn(space);
            Mockito.when(meta.extractGraphsFromResponse("new-configuration")).thenReturn(
                    java.util.Collections.singletonList(key));
            Mockito.when(meta.extractGraphsFromResponse("remove-configuration")).thenReturn(
                    java.util.Collections.singletonList(key));
            Mockito.when(meta.getGraphConfig("DEFAULT", name)).thenAnswer(invocation -> {
                singleConfigReads.incrementAndGet();
                if (rejectReplay.get()) {
                    throw readFailure;
                }
                Map<String, Object> current = latest.get();
                return current == null ? null : new HashMap<>(current);
            });
            Mockito.when(meta.graphConfigs("DEFAULT")).thenAnswer(invocation -> {
                allConfigReads.incrementAndGet();
                Map<String, Object> current = latest.get();
                Map<String, Map<String, Object>> configs = new HashMap<>();
                if (current != null) {
                    configs.put(key, new HashMap<>(current));
                }
                return configs;
            });
            Mockito.doAnswer(invocation -> {
                latest.set(null);
                return null;
            }).when(meta).removeGraphConfig("DEFAULT", name);
            Whitebox.setInternalState(manager, "metaManager", meta);
            Whitebox.setInternalState(manager, "PDExist", true);
            Whitebox.setInternalState(manager, "pdPeers", "127.0.0.1:8686");
            Map<String, GraphSpace> spaces = Whitebox.getInternalState(manager, "graphSpaces");
            spaces.put("DEFAULT", space);
            if (lateSelfAdd) {
                // Local creation is still waiting for its own PD acknowledgement.
                java.util.Set<String> creating = Whitebox.getInternalState(manager, "creatingGraphs");
                creating.add(key);
            }
            String threadName = Thread.currentThread().getName();
            if (peer) {
                try {
                    Thread.currentThread().setName("grpc-peer-removal-control");
                    Whitebox.invoke(GraphManager.class, new Class<?>[]{Object.class},
                            "graphRemoveHandler", manager, "remove-configuration");
                } finally {
                    Thread.currentThread().setName(threadName);
                }
                Assert.assertThrows(HugeException.class, () -> new org.apache.hugegraph.api.profile.GraphsAPI().drop(
                        manager, "DEFAULT", name, "I'm sure to drop the graph"));
            } else {
                Assert.assertThrows(HugeException.class, () -> new org.apache.hugegraph.api.profile.GraphsAPI().drop(
                        manager, "DEFAULT", name, "I'm sure to drop the graph"),
                        caught -> Assert.assertSame(failure, caught.getCause()));
            }
            Assert.assertTrue(graph.closed());
            Assert.assertSame(graph, manager.graph(key));
            Assert.assertSame(graph, factories.get(key));
            Assert.assertNull(latest.get());
            Map<String, Object> newConfig = new HashMap<>();
            newConfig.put(CoreOptions.BACKEND.name(), "memory");
            newConfig.put(CoreOptions.STORE.name(), name);
            // This is a latest-config marker; Memory does not implement a physical HStore GraphID.
            newConfig.put(CoreOptions.ALIAS_NAME.name(), "new-config-marker");
            newConfig.put("nickname", "new_generation");
            latest.set(newConfig);
            if (addDuringCleanup) {
                Whitebox.invoke(GraphManager.class, new Class<?>[]{Object.class},
                        "graphAddHandler", manager, "new-configuration");
                Assert.assertSame(graph, manager.graph(key));
                Assert.assertEquals(0, singleConfigReads.get());
            }
            if (replayFailure == 1) {
                latest.set(null);
            } else if (replayFailure == 2) {
                rejectReplay.set(true);
            }
            reject.set(false);
            if (lateSelfAdd) {
                java.util.Set<HugeGraph> failed = Whitebox.getInternalState(manager, "failedPeerRemovals");
                Assert.assertTrue(failed.contains(graph));
            }
            // A replacement read failure must not turn completed old-owner deletion into a failed DELETE.
            if (peer) {
                try {
                    Thread.currentThread().setName("grpc-peer-add-control");
                    Whitebox.invoke(GraphManager.class, new Class<?>[]{Object.class},
                            "graphAddHandler", manager, "new-configuration");
                } finally {
                    Thread.currentThread().setName(threadName);
                }
                Mockito.verify(meta, Mockito.never()).removeGraphConfig("DEFAULT", name);
                Mockito.verify(meta, Mockito.never()).notifyGraphRemove("DEFAULT", name);
                Assert.assertEquals(5, space.graphNumberUsed());
            } else {
                new org.apache.hugegraph.api.profile.GraphsAPI().drop(
                        manager, "DEFAULT", name, "I'm sure to drop the graph");
            }
            Assert.assertEquals(peer && addDuringCleanup ? 3 : 2, cleanupAttempts.get());
            Assert.assertNotSame(graph, factories.get(key));
            java.util.Set<String> pending = Whitebox.getInternalState(manager, "pendingGraphAdds");
            if (replayFailure == 1) {
                Assert.assertNull(manager.graph(key));
                Assert.assertNull(manager.graph("DEFAULT", name));
                Assert.assertFalse(factories.containsKey(key));
                Assert.assertFalse(pending.contains(key));
                return;
            }
            Assert.assertSame(newConfig, latest.get());
            if (replayFailure == 2) {
                Assert.assertNull(manager.graph(key));
                Assert.assertFalse(factories.containsKey(key));
                Assert.assertTrue(pending.contains(key));
                Assert.assertEquals(1, singleConfigReads.get());
                rejectReplay.set(false);
                Whitebox.invoke(GraphManager.class, new Class<?>[]{Object.class},
                        "graphAddHandler", manager, "new-configuration");
            }
            if (!addDuringCleanup && !peer) {
                Whitebox.invoke(GraphManager.class, new Class<?>[]{Object.class},
                        "graphAddHandler", manager, "new-configuration");
            }
            HugeGraph automatic = manager.graph(key);
            boolean listedBeforePublicRead = manager.graphs().contains(key);
            int configReadsBeforePublicRead = singleConfigReads.get();
            // The public two-argument lookup has an existing lazy metadata loading fallback.
            replacement = manager.graph("DEFAULT", name);
            Assert.assertNotNull(replacement);
            Assert.assertNotSame(graph, replacement);
            Assert.assertSame(replacement, manager.graph(key));
            Assert.assertSame(replacement, factories.get(key));
            Assert.assertEquals("new-config-marker", replacement.configuration().get(CoreOptions.ALIAS_NAME));
            Assert.assertEquals("new_generation", replacement.nickname());
            Assert.assertEquals(0L, replacement.traversal().V().count().next().longValue());
            Assert.assertTrue(replacement.started());
            Assert.assertFalse(pending.contains(key));
            Assert.assertSame(replacement, bindings.getGraph(key));
            Assert.assertNotNull(bindings.getTraversalSource("__g_" + key));
            Assert.assertSame(replacement, bindings.getAsBindings().get(key));
            replacement.schema().vertexLabel("replacement_marker").useAutomaticId().create();
            replacement.addVertex(org.apache.tinkerpop.gremlin.structure.T.label, "replacement_marker");
            replacement.tx().commit();
            // The old asynchronous DROP may arrive after the new CREATE; native bindings check identity.
            hub.notifySync(Events.GRAPH_DROP, graph);
            Assert.assertSame(replacement, bindings.getGraph(key));
            Assert.assertNotNull(bindings.getTraversalSource("__g_" + key));
            Assert.assertSame(replacement, bindings.getAsBindings().get(key));
            Whitebox.invoke(GraphManager.class, new Class<?>[]{Object.class},
                    "graphAddHandler", manager, "new-configuration");
            Assert.assertSame(replacement, manager.graph(key));
            Assert.assertEquals(1L, replacement.traversal().V().count().next().longValue());
            assertRpcResources(server, client, 0);
            String facts = String.format("same-name add during cleanup=%s automaticOwner=%s listedBeforeRead=%s " +
                    "getConfigBeforeRead=%s graphConfigsReads=%s publicLazyLoadedLatest=%s replacementStarted=%s " +
                    "rpcProviderConfigs=%s", addDuringCleanup, automatic == null ? "absent" : "new",
                    listedBeforePublicRead, configReadsBeforePublicRead, allConfigReads.get(),
                    replacement != graph, replacement.started(), server.config().configs().size());
            Assert.assertNotNull(facts, automatic);
            Assert.assertTrue(facts, listedBeforePublicRead);
        } finally {
            reject.set(false);
            rejectReplay.set(false);
            manager.close();
            gremlin.stop().get(30, TimeUnit.SECONDS);
            if (owner != null) {
                owner.close();
                HugeFactory.remove(owner);
            }
            if (replacement != null) {
                replacement.close();
                HugeFactory.remove(replacement);
            }
        }
    }

    @Test
    public void testReloadAfterRestCreateWithoutSelfAdd() throws Exception {
        this.checkReloadAfterRestCreate(false, false);
    }

    @Test
    public void testReloadAfterRestCreateAndSelfAdd() throws Exception {
        this.checkReloadAfterRestCreate(true, false);
    }

    @Test
    public void testReloadAfterHealthyDuplicateAdds() throws Exception {
        this.checkReloadAfterRestCreate(true, true);
    }

    private void checkReloadAfterRestCreate(boolean selfAdd, boolean duplicateAdd) throws Exception {
        String name = duplicateAdd ? "duplicate_add_reload" : selfAdd ? "self_add_reload" : "reload_without_add";
        String other = name + "_other";
        String key = "DEFAULT-" + name;
        GraphManager manager = this.dropManager(name);
        EventHub hub = Whitebox.getInternalState(manager, "eventHub");
        Settings settings = new Settings();
        settings.gremlinPool = 1;
        settings.threadPoolBoss = 1;
        settings.threadPoolWorker = 1;
        ContextGremlinServer gremlin = new ContextGremlinServer(settings, hub);
        org.apache.tinkerpop.gremlin.server.GraphManager bindings =
                gremlin.getServerGremlinExecutor().getGraphManager();
        Map<String, Map<String, Object>> configs = new HashMap<>();
        MetaManager meta = Mockito.mock(MetaManager.class);
        GraphSpace space = new GraphSpace("DEFAULT");
        Mockito.when(meta.graphSpace("DEFAULT")).thenReturn(space);
        Mockito.when(meta.extractGraphsFromResponse("self-add")).thenReturn(List.of(key, "DEFAULT-" + other));
        Mockito.when(meta.extractGraphsFromResponse("foreign-add")).thenReturn(List.of(key, "DEFAULT-" + other));
        Mockito.when(meta.graphConfigs("DEFAULT")).thenAnswer(invocation -> new HashMap<>(configs));
        Mockito.when(meta.getGraphConfig(Mockito.eq("DEFAULT"), Mockito.anyString()))
               .thenAnswer(invocation -> {
                   Map<String, Object> config = configs.get("DEFAULT-" + invocation.getArgument(1));
                   return config == null ? null : new HashMap<>(config);
               });
        Mockito.doAnswer(invocation -> {
            Map<String, Object> config = invocation.getArgument(2);
            configs.put("DEFAULT-" + invocation.getArgument(1), new HashMap<>(config));
            return null;
        }).when(meta).addGraphConfig(Mockito.eq("DEFAULT"), Mockito.anyString(), Mockito.anyMap());
        Whitebox.setInternalState(manager, "metaManager", meta);
        Whitebox.setInternalState(manager, "PDExist", true);
        Whitebox.setInternalState(manager, "pdPeers", "127.0.0.1:8686");
        Map<String, GraphSpace> spaces = Whitebox.getInternalState(manager, "graphSpaces");
        spaces.put("DEFAULT", space);
        try {
            org.apache.hugegraph.api.profile.GraphsAPI api = new org.apache.hugegraph.api.profile.GraphsAPI();
            for (String graphName : List.of(name, other)) {
                Map<String, Object> config = new HashMap<>();
                config.put(CoreOptions.BACKEND.name(), "memory");
                config.put(CoreOptions.SERIALIZER.name(), "text");
                api.create(manager, "DEFAULT", graphName, null, config);
            }
            HugeGraph owner = manager.graph(key);
            HugeGraph otherOwner = manager.graph("DEFAULT-" + other);
            Assert.assertSame(owner, bindings.getGraph(key));
            if (selfAdd) {
                Whitebox.invoke(GraphManager.class, new Class<?>[]{Object.class},
                        "graphAddHandler", manager, "self-add");
            }
            java.util.Set<String> pending = Whitebox.getInternalState(manager, "pendingGraphAdds");
            if (duplicateAdd) {
                Whitebox.invoke(GraphManager.class, new Class<?>[]{Object.class},
                        "graphAddHandler", manager, "foreign-add");
                Assert.assertSame(owner, manager.graph(key));
                Assert.assertSame(otherOwner, manager.graph("DEFAULT-" + other));
                Assert.assertTrue(pending.contains(key));
                Assert.assertTrue(pending.contains("DEFAULT-" + other));
            }
            Mockito.clearInvocations(meta);
            long begin = System.nanoTime();
            System.out.println(String.format("self-add reload before: selfAdd=%s pending=%s graphs=%s",
                                             selfAdd, pending.contains(key), manager.graphs().size()));
            try {
                api.reload(manager, Map.of("action", "reload"));
            } finally {
                System.out.println(String.format("self-add reload after: selfAdd=%s durationMs=%s pending=%s graphs=%s",
                                                 selfAdd, TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - begin),
                                                 pending.contains(key), manager.graphs().size()));
            }
            Assert.assertTrue(owner.closed());
            Assert.assertTrue(otherOwner.closed());
            Assert.assertNotNull(manager.graph(key));
            Assert.assertNotNull(manager.graph("DEFAULT-" + other));
            Assert.assertNotSame(owner, manager.graph(key));
            Assert.assertNotSame(otherOwner, manager.graph("DEFAULT-" + other));
            Assert.assertEquals(2, manager.graphs().size());
            Assert.assertFalse(pending.contains(key));
            Assert.assertFalse(pending.contains("DEFAULT-" + other));
            Mockito.verify(meta, Mockito.never()).getGraphConfig(Mockito.anyString(), Mockito.anyString());
            Mockito.verify(meta).graphConfigs("DEFAULT");
            Assert.assertSame(manager.graph(key), bindings.getGraph(key));
            Assert.assertSame(manager.graph("DEFAULT-" + other), bindings.getGraph("DEFAULT-" + other));
            Mockito.verify(meta, Mockito.never()).removeGraphConfig(Mockito.anyString(), Mockito.anyString());
        } finally {
            List<HugeGraph> owners = new ArrayList<>();
            for (String graphName : manager.graphs()) {
                owners.add(manager.graph(graphName));
            }
            manager.close();
            gremlin.stop().get(30, TimeUnit.SECONDS);
            for (HugeGraph owner : owners) {
                HugeFactory.remove(owner);
            }
        }
    }

    @Test
    public void testHealthyOwnerAddBeforePeerRemoveLoadsLatestConfig() throws Exception {
        this.checkAddBeforePeerRemove(false, true);
    }

    @Test
    public void testHealthyOwnerAddBeforePeerRemoveDoesNotReviveMissingConfig() throws Exception {
        this.checkAddBeforePeerRemove(true, true);
    }

    @Test
    public void testDuplicateAddWithoutRemoveKeepsHealthyOwner() throws Exception {
        this.checkAddBeforePeerRemove(false, false);
    }

    private void checkAddBeforePeerRemove(boolean missingConfig, boolean removeReceived) throws Exception {
        String name = missingConfig ? "add_before_remove_missing" :
                      removeReceived ? "add_before_remove" : "healthy_duplicate_add";
        String key = "DEFAULT-" + name;
        GraphManager manager = this.dropManager(name);
        EventHub hub = Whitebox.getInternalState(manager, "eventHub");
        Settings settings = new Settings();
        settings.gremlinPool = 1;
        settings.threadPoolBoss = 1;
        settings.threadPoolWorker = 1;
        ContextGremlinServer gremlin = new ContextGremlinServer(settings, hub);
        org.apache.tinkerpop.gremlin.server.GraphManager bindings =
                gremlin.getServerGremlinExecutor().getGraphManager();
        HugeGraph owner = null;
        HugeGraph replacement = null;
        try {
            owner = manager.createGraphLocal(name, "gremlin.graph=" + HugeFactory.class.getName() +
                    "\nbackend=memory\nstore=" + name + "\nalias.graph.id=old-before-remove\n");
            Map<String, HugeGraph> factories = Whitebox.getInternalState(HugeFactory.class, "GRAPHS");
            owner.schema().vertexLabel("old_marker").useAutomaticId().create();
            owner.addVertex(org.apache.tinkerpop.gremlin.structure.T.label, "old_marker");
            owner.tx().commit();
            Map<String, Object> latest = new HashMap<>();
            latest.put(CoreOptions.BACKEND.name(), "memory");
            latest.put(CoreOptions.STORE.name(), name);
            latest.put(CoreOptions.ALIAS_NAME.name(), "new-before-remove");
            AtomicInteger configReads = new AtomicInteger();
            MetaManager meta = Mockito.mock(MetaManager.class);
            GraphSpace space = new GraphSpace("DEFAULT");
            space.graphNumberUsed(5);
            Mockito.when(meta.graphSpace("DEFAULT")).thenReturn(space);
            Mockito.when(meta.extractGraphsFromResponse("new-add")).thenReturn(List.of(key));
            Mockito.when(meta.extractGraphsFromResponse("old-remove")).thenReturn(List.of(key));
            Mockito.when(meta.getGraphConfig("DEFAULT", name)).thenAnswer(invocation -> {
                configReads.incrementAndGet();
                return missingConfig ? null : new HashMap<>(latest);
            });
            Mockito.when(meta.graphConfigs("DEFAULT")).thenReturn(
                    missingConfig ? java.util.Collections.emptyMap() : Map.of(key, latest));
            Whitebox.setInternalState(manager, "metaManager", meta);
            Whitebox.setInternalState(manager, "PDExist", true);
            Whitebox.setInternalState(manager, "pdPeers", "127.0.0.1:8686");
            Map<String, GraphSpace> spaces = Whitebox.getInternalState(manager, "graphSpaces");
            spaces.put("DEFAULT", space);
            // Independent key watches can deliver the new ADD before the old REMOVE.
            for (int i = 0; i < 2; i++) {
                Whitebox.invoke(GraphManager.class, new Class<?>[]{Object.class},
                        "graphAddHandler", manager, "new-add");
                Assert.assertSame(owner, manager.graph(key));
                Assert.assertSame(owner, factories.get(key));
                Assert.assertFalse(owner.closed());
                Assert.assertEquals(0, configReads.get());
                Assert.assertEquals(1L, owner.traversal().V().count().next().longValue());
                owner.tx().close();
            }
            if (!removeReceived) {
                Assert.assertSame(owner, bindings.getGraph(key));
                return;
            }
            String threadName = Thread.currentThread().getName();
            try {
                Thread.currentThread().setName("grpc-add-before-remove-control");
                Whitebox.invoke(GraphManager.class, new Class<?>[]{Object.class},
                        "graphRemoveHandler", manager, "old-remove");
            } finally {
                Thread.currentThread().setName(threadName);
            }
            Assert.assertTrue(owner.closed());
            Mockito.verify(meta, Mockito.never()).removeGraphConfig("DEFAULT", name);
            Mockito.verify(meta, Mockito.never()).notifyGraphRemove("DEFAULT", name);
            Assert.assertEquals(5, space.graphNumberUsed());
            if (missingConfig) {
                Assert.assertNull(manager.graph(key));
                Assert.assertFalse(factories.containsKey(key));
                hub.notifySync(Events.GRAPH_DROP, owner);
                Assert.assertNull(bindings.getGraph(key));
                return;
            }
            replacement = manager.graph(key);
            Assert.assertNotNull("The received new ADD was lost before the old REMOVE", replacement);
            Assert.assertNotSame(owner, replacement);
            Assert.assertSame(replacement, factories.get(key));
            Assert.assertSame(replacement, bindings.getGraph(key));
            Assert.assertSame(replacement, bindings.getAsBindings().get(key));
            Assert.assertTrue(replacement.started());
            Assert.assertEquals("new-before-remove", replacement.configuration().get(CoreOptions.ALIAS_NAME));
            replacement.schema().vertexLabel("new_marker").useAutomaticId().create();
            replacement.addVertex(org.apache.tinkerpop.gremlin.structure.T.label, "new_marker");
            replacement.tx().commit();
            hub.notifySync(Events.GRAPH_DROP, owner);
            Assert.assertSame(replacement, bindings.getGraph(key));
            Assert.assertNotNull(bindings.getTraversalSource("__g_" + key));
            Assert.assertEquals(1L, replacement.traversal().V().count().next().longValue());
        } finally {
            manager.close();
            gremlin.stop().get(30, TimeUnit.SECONDS);
            if (owner != null) {
                owner.close();
                HugeFactory.remove(owner);
            }
            if (replacement != null) {
                replacement.close();
                HugeFactory.remove(replacement);
            }
        }
    }

    @Test
    public void testRpcStartupDoesNotRepublishDeletingGraph() throws Exception {
        this.checkRpcStartupDuringDeletion(false, false);
    }

    @Test
    public void testRpcStartupSkipsOwnerAfterRemovalTokenConsumed() throws Exception {
        this.checkRpcStartupDuringDeletion(true, false);
    }

    @Test
    public void testRpcStartupPublishesOtherOwnerDuringDeletion() throws Exception {
        this.checkRpcStartupDuringDeletion(true, true);
    }

    private void checkRpcStartupDuringDeletion(boolean consumeToken, boolean keepOther) throws Exception {
        String name = keepOther ? "startup_retains_peer" :
                      consumeToken ? "startup_consumed_removal" : "startup_deleting_graph";
        GraphManager manager = this.dropManager(name);
        HugeGraph owner = null;
        HugeGraph keeper = null;
        ExecutorService deletion = Executors.newSingleThreadExecutor(
                runnable -> new Thread(runnable, "grpc-startup-delete-proof"));
        CountDownLatch metadataEntered = new CountDownLatch(1);
        CountDownLatch metadataRelease = new CountDownLatch(1);
        Future<?> dropped = null;
        try {
            if (keepOther) {
                keeper = manager.createGraphLocal(name + "_peer", sharedMemoryConfig(name + "_peer"));
                Whitebox.invoke(StandardHugeGraph.class, "closeCurrentThreadTransaction", keeper);
            }
            owner = manager.createGraphLocal(name, sharedMemoryConfig(name));
            HugeGraph graph = owner;
            // The existing Memory feature fixture exercises real cache RPC registration, not shared DML.
            Whitebox.invoke(StandardHugeGraph.class, "closeCurrentThreadTransaction", graph);
            MetaManager meta = Mockito.mock(MetaManager.class);
            Mockito.doAnswer(invocation -> {
                metadataEntered.countDown();
                E.checkState(metadataRelease.await(10, TimeUnit.SECONDS), "metadata proof barrier timed out");
                return null;
            }).when(meta).removeGraphConfig("DEFAULT", name);
            Whitebox.setInternalState(manager, "metaManager", meta);
            Whitebox.setInternalState(manager, "PDExist", true);
            Map<String, GraphSpace> spaces = Whitebox.getInternalState(manager, "graphSpaces");
            GraphSpace space = new GraphSpace("DEFAULT");
            space.graphNumberUsed(5);
            spaces.put("DEFAULT", space);
            dropped = deletion.submit(() -> new org.apache.hugegraph.api.profile.GraphsAPI().drop(
                    manager, "DEFAULT", name, "I'm sure to drop the graph"));
            Assert.assertTrue(metadataEntered.await(5, TimeUnit.SECONDS));
            java.util.Set<?> removing = Whitebox.getInternalState(manager, "removingGraphs");
            Assert.assertTrue(removing.contains(graph.spaceGraphName()));
            if (consumeToken) {
                Mockito.when(meta.extractGraphsFromResponse(Mockito.any())).thenReturn(
                        java.util.Collections.singletonList(graph.spaceGraphName()));
                Whitebox.invoke(GraphManager.class, "graphRemoveHandler", manager, new Object());
                Assert.assertFalse(removing.contains(graph.spaceGraphName()));
            }
            Assert.assertFalse(graph.closed());
            Assert.assertSame(graph, manager.graph(graph.spaceGraphName()));
            RpcServer server = Whitebox.getInternalState(manager, "rpcServer");
            RpcClientProvider client = Whitebox.getInternalState(manager, "rpcClient");
            assertRpcResources(server, client, 0);
            // Run actual manager startup while the same owner's placement metadata removal is blocked.
            Whitebox.invoke(GraphManager.class, "startRpcServer", manager);
            Assert.assertFalse(dropped.isDone());
            Assert.assertEquals("RPC startup exported a removing graph during metadata deletion",
                                keepOther ? 2 : 0, server.config().configs().size());
            if (keepOther) {
                Assert.assertFalse(keeper.closed());
                Assert.assertSame(keeper, manager.graph(keeper.spaceGraphName()));
            }
        } finally {
            metadataRelease.countDown();
            try {
                if (dropped != null) {
                    dropped.get(20, TimeUnit.SECONDS);
                }
            } finally {
                deletion.shutdown();
                Assert.assertTrue(deletion.awaitTermination(20, TimeUnit.SECONDS));
                manager.close();
                if (owner != null) {
                    owner.close();
                    HugeFactory.remove(owner);
                }
                if (keeper != null) {
                    HugeFactory.remove(keeper);
                }
            }
        }
    }

    @Test
    public void testNonClearingPeerCloseFailureKeepsRpcAtInitialStartup() throws Exception {
        String name = "peer_close_open_rpc";
        GraphManager manager = this.dropManager(name);
        HugeGraph graph = null;
        ExecutorService worker = Executors.newSingleThreadExecutor();
        CountDownLatch opened = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        try {
            graph = manager.createGraphLocal(name, sharedMemoryConfig(name));
            HugeGraph owner = graph;
            Whitebox.invoke(StandardHugeGraph.class, "closeCurrentThreadTransaction", owner);
            RpcServer server = Whitebox.getInternalState(manager, "rpcServer");
            RpcClientProvider client = Whitebox.getInternalState(manager, "rpcClient");
            assertRpcResources(server, client, 0);
            MetaManager meta = Mockito.mock(MetaManager.class);
            Mockito.when(meta.extractGraphsFromResponse("peer-remove")).thenReturn(List.of(owner.spaceGraphName()));
            Whitebox.setInternalState(manager, "metaManager", meta);
            Whitebox.setInternalState(manager, "PDExist", true);
            Future<?> transaction = worker.submit(() -> {
                try {
                    owner.tx().open();
                    opened.countDown();
                    E.checkState(release.await(15, TimeUnit.SECONDS), "Foreign transaction release timed out");
                } catch (InterruptedException failure) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(failure);
                } finally {
                    Whitebox.invoke(StandardHugeGraph.class, "closeCurrentThreadTransaction", owner);
                }
            });
            Assert.assertTrue(opened.await(5, TimeUnit.SECONDS));
            Whitebox.invoke(GraphManager.class, new Class<?>[]{Object.class},
                    "graphRemoveHandler", manager, "peer-remove");
            Assert.assertFalse(owner.closed());
            Assert.assertSame(owner, manager.graph(owner.spaceGraphName()));
            assertRpcResources(server, client, 0);
            Whitebox.invoke(GraphManager.class, "startRpcServer", manager);
            assertRpcResources(server, client, 2);
            release.countDown();
            transaction.get(5, TimeUnit.SECONDS);
            String threadName = Thread.currentThread().getName();
            try {
                Thread.currentThread().setName("grpc-peer-close-retry");
                Whitebox.invoke(GraphManager.class, new Class<?>[]{Object.class},
                        "graphRemoveHandler", manager, "peer-remove");
            } finally {
                Thread.currentThread().setName(threadName);
            }
            Assert.assertTrue(owner.closed());
            Assert.assertFalse(manager.graphs().contains(owner.spaceGraphName()));
            assertRpcResources(server, client, 0);
            Mockito.verify(meta, Mockito.never()).removeGraphConfig(Mockito.anyString(), Mockito.anyString());
        } finally {
            release.countDown();
            worker.shutdown();
            Assert.assertTrue(worker.awaitTermination(5, TimeUnit.SECONDS));
            manager.close();
            if (graph != null) {
                graph.close();
                HugeFactory.remove(graph);
            }
        }
    }

    @Test
    public void testNonPdOrdinaryClosedOwnerCannotCertifyBackendClear() throws Exception {
        GraphManager manager = this.dropManager("non_pd_uncleared_close");
        HugeGraph owner = null;
        HugeGraph keeper = null;
        try {
            String config = "gremlin.graph=" + HugeFactory.class.getName() + "\nbackend=memory\nstore=";
            keeper = manager.createGraphLocal("uncleared_keeper", config + "uncleared_keeper\n");
            owner = manager.createGraphLocal("uncleared_owner", config + "uncleared_owner\n");
            HugeGraph graph = owner;
            owner.close();
            Assert.assertThrows(IllegalStateException.class, () ->
                    new org.apache.hugegraph.api.profile.GraphsAPI().drop(
                            manager, "DEFAULT", "uncleared_owner", "I'm sure to drop the graph"));
            Map<String, HugeGraph> factories = Whitebox.getInternalState(HugeFactory.class, "GRAPHS");
            Assert.assertSame(owner, factories.get(owner.spaceGraphName()));
            Assert.assertSame(owner, manager.graph(owner.spaceGraphName()));
            Assert.assertTrue(graph.configuration().file().isFile());
        } finally {
            manager.close();
            if (owner != null) {
                owner.close();
                HugeFactory.remove(owner);
            }
            if (keeper != null) {
                HugeFactory.remove(keeper);
            }
        }
    }

    @Test
    public void testRpcStartupSkipsOrdinaryClosedOwner() throws Exception {
        GraphManager manager = this.dropManager("startup_closed_owner");
        HugeGraph owner = null;
        try {
            owner = manager.createGraphLocal("startup_closed", sharedMemoryConfig("startup_closed"));
            owner.close();
            RuntimeException startup = null;
            try {
                Whitebox.invoke(GraphManager.class, "startRpcServer", manager);
            } catch (RuntimeException failure) {
                startup = failure;
            }
            Assert.assertNull("RPC startup must skip a closed owner: " + startup, startup);
            RpcServer server = Whitebox.getInternalState(manager, "rpcServer");
            RpcClientProvider client = Whitebox.getInternalState(manager, "rpcClient");
            assertRpcResources(server, client, 0);
        } finally {
            manager.close();
            if (owner != null) {
                owner.close();
                HugeFactory.remove(owner);
            }
        }
    }

    @Test
    public void testNonPdFailedClearPreservesExistingRpc() throws Exception {
        GraphManager manager = this.dropManager("non_pd_clear_failure");
        HugeGraph owner = null;
        HugeGraph keeper = null;
        BackendStoreProvider provider = null;
        try {
            owner = manager.createGraphLocal("non_pd_clear", sharedMemoryConfig("non_pd_clear"));
            keeper = manager.createGraphLocal("non_pd_clear_peer", sharedMemoryConfig("non_pd_clear_peer"));
            Whitebox.invoke(GraphManager.class, "startRpcServer", manager);
            RpcServer server = Whitebox.getInternalState(manager, "rpcServer");
            RpcClientProvider client = Whitebox.getInternalState(manager, "rpcClient");
            assertRpcResources(server, client, 4);
            provider = Mockito.spy(owner.storeProvider());
            Whitebox.setInternalState(owner, "storeProvider", provider);
            IllegalStateException failure = new IllegalStateException("clear rejected before destructive action");
            Mockito.doThrow(failure).when(provider).clear();
            Assert.assertThrows(IllegalStateException.class, () ->
                    new org.apache.hugegraph.api.profile.GraphsAPI().drop(
                            manager, "DEFAULT", "non_pd_clear", "I'm sure to drop the graph"),
                    caught -> Assert.assertSame(failure, caught));
            Assert.assertFalse(owner.closed());
            Assert.assertSame(owner, manager.graph(owner.spaceGraphName()));
            Assert.assertTrue(owner.configuration().file().isFile());
            java.util.Set<?> pending = Whitebox.getInternalState(manager, "rpcRemovingOwners");
            Assert.assertFalse(pending.contains(owner));
            assertRpcResources(server, client, 4);
        } finally {
            if (provider != null) {
                Mockito.doCallRealMethod().when(provider).clear();
            }
            manager.close();
            if (owner != null) {
                HugeFactory.remove(owner);
            }
            if (keeper != null) {
                HugeFactory.remove(keeper);
            }
        }
    }

    @Test
    public void testNonPdClearFailureDoesNotUnregisterRpcDuringStartup() throws Exception {
        String name = "non_pd_clear_startup";
        GraphManager manager = this.dropManager(name);
        HugeGraph owner = null;
        HugeGraph keeper = null;
        BackendStoreProvider provider = null;
        CountDownLatch clearEntered = new CountDownLatch(1);
        CountDownLatch clearRelease = new CountDownLatch(1);
        ExecutorService deletion = Executors.newSingleThreadExecutor();
        Future<?> dropped = null;
        try {
            owner = manager.createGraphLocal(name, sharedMemoryConfig(name));
            keeper = manager.createGraphLocal(name + "_peer", sharedMemoryConfig(name + "_peer"));
            Whitebox.invoke(GraphManager.class, "startRpcServer", manager);
            Whitebox.invoke(StandardHugeGraph.class, "closeCurrentThreadTransaction", owner);
            Whitebox.invoke(StandardHugeGraph.class, "closeCurrentThreadTransaction", keeper);
            provider = Mockito.spy(owner.storeProvider());
            Whitebox.setInternalState(owner, "storeProvider", provider);
            IllegalStateException failure = new IllegalStateException("clear fails before destructive action");
            Mockito.doAnswer(invocation -> {
                clearEntered.countDown();
                E.checkState(clearRelease.await(10, TimeUnit.SECONDS), "local clear proof barrier timed out");
                throw failure;
            }).when(provider).clear();
            dropped = deletion.submit(() -> new org.apache.hugegraph.api.profile.GraphsAPI().drop(
                    manager, "DEFAULT", name, "I'm sure to drop the graph"));
            Assert.assertTrue(clearEntered.await(5, TimeUnit.SECONDS));
            Whitebox.invoke(GraphManager.class, "startRpcServer", manager);
            clearRelease.countDown();
            Future<?> failedDrop = dropped;
            Assert.assertThrows(ExecutionException.class, () -> failedDrop.get(20, TimeUnit.SECONDS),
                                caught -> Assert.assertSame(failure, caught.getCause()));
            dropped = null;
            Assert.assertFalse(owner.closed());
            Assert.assertSame(owner, manager.graph(owner.spaceGraphName()));
            Assert.assertTrue(owner.configuration().file().isFile());
            RpcServer server = Whitebox.getInternalState(manager, "rpcServer");
            RpcClientProvider client = Whitebox.getInternalState(manager, "rpcClient");
            assertRpcResources(server, client, 4);
        } finally {
            clearRelease.countDown();
            try {
                if (dropped != null) {
                    try {
                        dropped.get(20, TimeUnit.SECONDS);
                    } catch (ExecutionException expected) {
                        // The controlled clear fails before destructive work.
                    }
                }
            } finally {
                if (provider != null) {
                    Mockito.doCallRealMethod().when(provider).clear();
                }
                deletion.shutdown();
                Assert.assertTrue(deletion.awaitTermination(20, TimeUnit.SECONDS));
                manager.close();
                if (owner != null) {
                    HugeFactory.remove(owner);
                }
                if (keeper != null) {
                    HugeFactory.remove(keeper);
                }
            }
        }
    }

    @Test
    public void testMetadataFailureRestoresRpcBeforeClearingRemovalMarker() throws Exception {
        this.checkMetadataRpcRestoration(false);
    }

    @Test
    public void testFailedRpcRestorationRetainsRemovalOwner() throws Exception {
        this.checkMetadataRpcRestoration(true);
    }

    private void checkMetadataRpcRestoration(boolean rejectRestore) throws Exception {
        String name = rejectRestore ? "metadata_restore_failure" : "metadata_restore_success";
        GraphManager manager = this.dropManager(name);
        HugeGraph owner = null;
        RpcServer server = null;
        try {
            owner = manager.createGraphLocal(name, sharedMemoryConfig(name));
            Whitebox.invoke(GraphManager.class, "startRpcServer", manager);
            RpcServer original = Whitebox.getInternalState(manager, "rpcServer");
            server = Mockito.spy(original);
            Whitebox.setInternalState(manager, "rpcServer", server);
            IllegalStateException metadata = new IllegalStateException("placement removal failed");
            IllegalStateException restoration = new IllegalStateException("RPC restoration failed");
            if (rejectRestore) {
                Mockito.doThrow(restoration).when(server).exportAll();
            }
            MetaManager meta = Mockito.mock(MetaManager.class);
            Mockito.doThrow(metadata).when(meta).removeGraphConfig("DEFAULT", name);
            Whitebox.setInternalState(manager, "metaManager", meta);
            Whitebox.setInternalState(manager, "PDExist", true);
            GraphSpace space = new GraphSpace("DEFAULT");
            space.graphNumberUsed(5);
            Map<String, GraphSpace> spaces = Whitebox.getInternalState(manager, "graphSpaces");
            spaces.put("DEFAULT", space);
            Assert.assertThrows(HugeException.class, () ->
                    new org.apache.hugegraph.api.profile.GraphsAPI().drop(
                            manager, "DEFAULT", name, "I'm sure to drop the graph"),
                    caught -> Assert.assertSame(metadata, caught.getCause()));
            java.util.Set<?> pending = Whitebox.getInternalState(manager, "rpcRemovingOwners");
            Assert.assertEquals(rejectRestore, pending.contains(owner));
            Assert.assertFalse(owner.closed());
            Assert.assertSame(owner, manager.graph(owner.spaceGraphName()));
            Assert.assertTrue(owner.configuration().file().isFile());
            RpcClientProvider client = Whitebox.getInternalState(manager, "rpcClient");
            assertRpcResources(server, client, rejectRestore ? 0 : 2);
            if (rejectRestore) {
                Assert.assertEquals(1, metadata.getSuppressed().length);
                Assert.assertSame(restoration, metadata.getSuppressed()[0]);
            }
        } finally {
            if (server != null) {
                Mockito.doCallRealMethod().when(server).exportAll();
            }
            manager.close();
            if (owner != null) {
                HugeFactory.remove(owner);
            }
        }
    }

    @Test
    public void testStartupDoesNotExportPendingOwnerAfterUnexportFailure() throws Exception {
        String name = "startup_unexport_failure";
        GraphManager manager = this.dropManager(name);
        HugeGraph owner = null;
        RpcServer server = null;
        com.alipay.sofa.rpc.config.ProviderConfig<?> retained = null;
        try {
            owner = manager.createGraphLocal(name, sharedMemoryConfig(name));
            RpcServer original = Whitebox.getInternalState(manager, "rpcServer");
            server = Mockito.spy(original);
            Whitebox.setInternalState(manager, "rpcServer", server);
            RpcClientProvider client = Whitebox.getInternalState(manager, "rpcClient");
            owner.registerRpcServices(server.config(), client.config());
            assertRpcResources(server, client, 2);
            Map<String, com.alipay.sofa.rpc.config.ProviderConfig<?>> configs = server.config().configs();
            Map.Entry<String, com.alipay.sofa.rpc.config.ProviderConfig<?>> entry =
                    configs.entrySet().iterator().next();
            retained = Mockito.spy(entry.getValue());
            configs.put(entry.getKey(), retained);
            IllegalStateException failure = new IllegalStateException("provider unexport rejected");
            Mockito.doThrow(failure).when(retained).unExport();
            Whitebox.setInternalState(manager, "PDExist", true);
            MetaManager meta = Mockito.mock(MetaManager.class);
            Whitebox.setInternalState(manager, "metaManager", meta);
            Assert.assertThrows(IllegalStateException.class, () ->
                    new org.apache.hugegraph.api.profile.GraphsAPI().drop(
                            manager, "DEFAULT", name, "I'm sure to drop the graph"));
            Assert.assertEquals(1, server.config().configs().size());
            Mockito.verify(meta, Mockito.never()).removeGraphConfig(Mockito.any(), Mockito.any());
            Assert.assertThrows(IllegalStateException.class, () ->
                    Whitebox.invoke(GraphManager.class, "startRpcServer", manager));
            Mockito.verify(server, Mockito.never()).exportAll();
            Assert.assertFalse(owner.closed());
            Assert.assertSame(owner, manager.graph(owner.spaceGraphName()));
            java.util.Set<?> pending = Whitebox.getInternalState(manager, "rpcRemovingOwners");
            Assert.assertTrue(pending.contains(owner));
        } finally {
            if (retained != null) {
                Mockito.doCallRealMethod().when(retained).unExport();
            }
            manager.close();
            if (owner != null) {
                owner.close();
                HugeFactory.remove(owner);
            }
        }
    }

    @Test
    public void testNonPdHealthyPublicDeleteCompletesCleanup() throws Exception {
        this.checkNonPdPublicDeleteRetry(false, false);
    }

    @Test
    public void testNonPdPublicDeleteRetriesFailedProviderCleanup() throws Exception {
        this.checkNonPdPublicDeleteRetry(true, false);
    }

    @Test
    public void testNonPdPublicDeleteRetriesFailedRpcUnregistration() throws Exception {
        this.checkNonPdPublicDeleteRetry(true, true);
    }

    @Test
    public void testNonPdPublicDeletePreservesFatalCleanupIdentity() throws Exception {
        this.checkNonPdPublicDeleteRetry(true, false, true);
    }

    private void checkNonPdPublicDeleteRetry(boolean failCleanup, boolean rpcCleanup) throws Exception {
        this.checkNonPdPublicDeleteRetry(failCleanup, rpcCleanup, false);
    }

    private void checkNonPdPublicDeleteRetry(boolean failCleanup, boolean rpcCleanup, boolean fatal) throws Exception {
        String name = !failCleanup ? "non_pd_healthy" : rpcCleanup ? "non_pd_rpc_retry" :
                      fatal ? "non_pd_fatal_retry" : "non_pd_provider_retry";
        GraphManager manager = this.dropManager(name);
        HugeGraph owner = null;
        HugeGraph keeper = null;
        AtomicBoolean reject = new AtomicBoolean(failCleanup);
        AtomicInteger attempts = new AtomicInteger();
        try {
            String config = "gremlin.graph=" + HugeFactory.class.getName() + "\nbackend=memory\nstore=";
            keeper = manager.createGraphLocal(name + "_keeper", config + name + "_keeper\n");
            owner = manager.createGraphLocal(name, config + name + "\n");
            HugeGraph graph = owner;
            Map<String, HugeGraph> factories = Whitebox.getInternalState(HugeFactory.class, "GRAPHS");
            Map<String, HugeGraph> managed = Whitebox.getInternalState(manager, "graphs");
            List<?> retained = Whitebox.getInternalState(owner, "closeResources");
            List<Runnable> rpc = Whitebox.getInternalState(owner, "rpcServiceCleanup");
            File file = owner.configuration().file();
            Assert.assertSame(owner, factories.get(owner.spaceGraphName()));
            Assert.assertSame(owner, managed.get(owner.spaceGraphName()));
            Assert.assertTrue(file.isFile());
            Throwable controlled = fatal ? new AssertionError("fatal native cleanup boundary") :
                                           new IllegalStateException("persistent native cleanup boundary");
            BackendStoreProvider provider = Mockito.spy(owner.storeProvider());
            Whitebox.setInternalState(owner, "storeProvider", provider);
            AtomicInteger clears = new AtomicInteger();
            Mockito.doAnswer(invocation -> {
                clears.incrementAndGet();
                invocation.callRealMethod();
                return null;
            }).when(provider).clear();
            if (rpcCleanup) {
                // Only the RPC cleanup callback is controlled; actual graph close/unregister queues run.
                rpc.add(() -> {
                    attempts.incrementAndGet();
                    if (reject.get()) {
                        throw (RuntimeException) controlled;
                    }
                });
            } else {
                Mockito.doAnswer(invocation -> {
                    invocation.callRealMethod();
                    attempts.incrementAndGet();
                    if (reject.get()) {
                        throw controlled;
                    }
                    return null;
                }).when(provider).close();
            }
            org.apache.hugegraph.api.profile.GraphsAPI api = new org.apache.hugegraph.api.profile.GraphsAPI();
            Throwable first = null;
            try {
                api.drop(manager, "DEFAULT", name, "I'm sure to drop the graph");
            } catch (RuntimeException | Error failure) {
                first = failure;
            }
            boolean configRetained = file.isFile();
            boolean factoryRetained = factories.get(owner.spaceGraphName()) == owner;
            boolean managerRetained = managed.get(owner.spaceGraphName()) == owner;
            boolean completed = Whitebox.getInternalState(owner, "closeCompleted");
            int pending = retained.size();
            int rpcPending = rpc.size();
            int firstAttempts = attempts.get();
            reject.set(false);
            Throwable retry = null;
            if (failCleanup) {
                try {
                    api.drop(manager, "DEFAULT", name, "I'm sure to drop the graph");
                } catch (RuntimeException | Error failure) {
                    retry = failure;
                }
            }
            String facts = String.format("native non-PD DELETE: rpc=%s first=%s retry=%s closed=%s " +
                    "closeCompleted=%s config=%s factory=%s manager=%s retained=%s rpcPending=%s " +
                    "attemptsBeforeRetry=%s attemptsAfterRetry=%s", rpcCleanup, first, retry, graph.closed(),
                    completed, configRetained, factoryRetained, managerRetained, pending, rpcPending,
                    firstAttempts, attempts.get());
            if (failCleanup) {
                Assert.assertTrue(facts, first != null);
                if (fatal) {
                    Assert.assertSame(controlled, first);
                }
                Assert.assertTrue(facts, configRetained);
                Assert.assertTrue(facts, factoryRetained);
                Assert.assertTrue(facts, managerRetained);
                Assert.assertFalse(facts, completed);
                Assert.assertTrue(facts, pending > 0);
                Assert.assertNull(facts, retry);
                Assert.assertTrue(facts, attempts.get() > firstAttempts);
            } else {
                Assert.assertNull(facts, first);
                Assert.assertTrue(facts, completed);
                Assert.assertEquals(facts, 0, pending);
            }
            Assert.assertEquals("A successful drop clear must not repeat during cleanup retry", 1, clears.get());
            Assert.assertTrue(Whitebox.getInternalState(owner, "closeCompleted"));
            Assert.assertEquals(0, retained.size());
            Assert.assertFalse(file.exists());
            Assert.assertFalse(factories.containsKey(owner.spaceGraphName()));
            Assert.assertFalse(managed.containsKey(owner.spaceGraphName()));
            Assert.assertSame(keeper, manager.graph(keeper.spaceGraphName()));
        } finally {
            reject.set(false);
            if (owner != null) {
                owner.close();
                HugeFactory.remove(owner);
            }
            manager.close();
            if (keeper != null) {
                HugeFactory.remove(keeper);
            }
        }
    }

    private GraphManager dropManager(String name) throws Exception {
        OptionSpace.register("core", CoreOptions.instance());
        OptionSpace.register("rpc", RpcOptions.instance());
        OptionSpace.register("server", ServerOptions.instance());
        PropertiesConfiguration properties = new PropertiesConfiguration();
        properties.setProperty(ServerOptions.GRAPHS.name(), this.temporary.newFolder(name).getPath());
        properties.setProperty(ServerOptions.USE_PD.name(), false);
        properties.setProperty(ServerOptions.GRAPH_LOAD_FROM_LOCAL_CONFIG.name(), false);
        properties.setProperty(ServerOptions.ENABLE_DYNAMIC_CREATE_DROP.name(), true);
        properties.setProperty(RpcOptions.RPC_SERVER_HOST.name(), "127.0.0.1");
        properties.setProperty(RpcOptions.RPC_SERVER_PORT.name(), 0);
        properties.setProperty(RpcOptions.RPC_REMOTE_URL.name(), "127.0.0.1:1");
        return new GraphManager(new HugeConfig(properties), new EventHub(name));
    }

    private void checkPostCloseDrop(boolean failCleanup) throws Exception {
        String name = failCleanup ? "closed_rpc_retry" : "closed_failure";
        GraphManager manager = this.dropManager(name);
        HugeGraph owner = null;
        try {
            owner = manager.createGraphLocal(name, sharedMemoryConfig(name));
            Whitebox.invoke(GraphManager.class, "startRpcServer", manager);
            HugeGraph delegate = owner;
            Map<String, HugeGraph> factoryOwners = Whitebox.getInternalState(HugeFactory.class, "GRAPHS");
            factoryOwners.put(owner.spaceGraphName(), owner);
            IllegalStateException failure = new IllegalStateException("closed owner cleanup failed");
            AtomicInteger clears = new AtomicInteger();
            AtomicInteger rpcCleanup = new AtomicInteger();
            BackendStoreProvider provider = Mockito.spy(owner.storeProvider());
            if (!failCleanup) {
                Whitebox.setInternalState(owner, "storeProvider", provider);
                Mockito.doAnswer(invocation -> {
                    invocation.callRealMethod();
                    throw failure;
                }).when(provider).close();
            }
            HugeGraph graph = (HugeGraph) Proxy.newProxyInstance(HugeGraph.class.getClassLoader(),
                    new Class<?>[]{HugeGraph.class}, (proxy, method, args) -> {
                        if ("clearBackend".equals(method.getName())) {
                            clears.incrementAndGet();
                        }
                        if ("unregisterRpcServices".equals(method.getName()) &&
                            rpcCleanup.incrementAndGet() == 2 && failCleanup) {
                            throw failure;
                        }
                        try {
                            return method.invoke(delegate, args);
                        } catch (InvocationTargetException error) {
                            throw error.getCause();
                        }
                    });
            Map<String, HugeGraph> graphs = Whitebox.getInternalState(manager, "graphs");
            graphs.put(graph.spaceGraphName(), graph);
            DefaultGraphManager bindings = new DefaultGraphManager(new Settings());
            bindings.putGraph(graph.spaceGraphName(), graph);
            EventHub hub = Whitebox.getInternalState(manager, "eventHub");
            CountDownLatch dropped = new CountDownLatch(1);
            hub.listen(Events.GRAPH_DROP, event -> {
                try {
                    bindings.removeGraph(graph.spaceGraphName());
                } catch (Exception failureToUnbind) {
                    throw new IllegalStateException(failureToUnbind);
                }
                dropped.countDown();
                return true;
            });
            MetaManager meta = Mockito.mock(MetaManager.class);
            Whitebox.setInternalState(manager, "metaManager", meta);
            Whitebox.setInternalState(manager, "PDExist", true);
            Map<String, GraphSpace> spaces = Whitebox.getInternalState(manager, "graphSpaces");
            GraphSpace space = new GraphSpace("DEFAULT");
            space.graphNumberUsed(5);
            spaces.put("DEFAULT", space);
            if (failCleanup) {
                Assert.assertThrows(IllegalStateException.class, () -> manager.dropGraph(
                        "DEFAULT", name, true), caught -> Assert.assertSame(failure, caught));
            } else {
                Assert.assertThrows(HugeException.class, () -> manager.dropGraph(
                        "DEFAULT", name, true), caught -> Assert.assertSame(failure, caught.getCause()));
                Mockito.doCallRealMethod().when(provider).close();
            }
            Assert.assertTrue(owner.closed());
            Assert.assertSame(graph, manager.graph(graph.spaceGraphName()));
            Assert.assertSame(graph, bindings.getGraph(graph.spaceGraphName()));
            Map<?, ?> retainedFactory = Whitebox.getInternalState(HugeFactory.class, "GRAPHS");
            Assert.assertSame(owner, retainedFactory.get(graph.spaceGraphName()));
            Assert.assertEquals(5, space.graphNumberUsed());
            Assert.assertEquals(1L, dropped.getCount());
            manager.dropGraph("DEFAULT", name, true);
            Assert.assertTrue(dropped.await(5, TimeUnit.SECONDS));
            Assert.assertTrue(owner.closed());
            Assert.assertEquals(1, clears.get());
            Mockito.verify(meta).removeGraphConfig("DEFAULT", name);
            Mockito.verify(meta).notifyGraphRemove("DEFAULT", name);
            Mockito.verify(meta).updateGraphSpaceConfig("DEFAULT", space);
            Assert.assertEquals(4, space.graphNumberUsed());
            Assert.assertNull(bindings.getGraph(graph.spaceGraphName()));
            Assert.assertFalse(manager.graphs().contains(graph.spaceGraphName()));
            Map<?, ?> factory = Whitebox.getInternalState(HugeFactory.class, "GRAPHS");
            Assert.assertFalse(factory.containsKey(graph.spaceGraphName()));
            RpcServer server = Whitebox.getInternalState(manager, "rpcServer");
            RpcClientProvider client = Whitebox.getInternalState(manager, "rpcClient");
            assertRpcResources(server, client, 0);
        } finally {
            manager.close();
            if (owner != null) {
                HugeFactory.remove(owner);
            }
        }
    }

    @Test
    public void testQuotaUpdateFailureWithClearRetriesClosedOwner() throws Exception {
        this.checkLegacyDrop(false, true, true);
    }

    @Test
    public void testRaftCloseFailuresKeepOwnerAndPreserveSuppressed() throws Exception {
        this.checkRaftCloseFailures(false);
    }

    @Test
    public void testRaftCloseFailuresDoNotSuppressTheSameException() throws Exception {
        this.checkRaftCloseFailures(true);
    }

    @Test
    public void testRaftClosePrioritizesFatalError() throws Exception {
        this.checkRaftCloseFailures(false, true);
    }

    private void checkRaftCloseFailures(boolean sameFailure) throws Exception {
        this.checkRaftCloseFailures(sameFailure, false);
    }

    private void checkRaftCloseFailures(boolean sameFailure, boolean fatalNode) throws Exception {
        String name = fatalNode ? "raft_fatal_node" : sameFailure ? "raft_same_failure" : "raft_close_failures";
        GraphManager manager = this.dropManager(name);
        HugeGraph graph = null;
        com.alipay.sofa.jraft.rpc.RpcServer nativeRpc = null;
        RaftContext context = null;
        String threadName = Thread.currentThread().getName();
        AtomicBoolean failProvider = new AtomicBoolean(true);
        AtomicBoolean failNode = new AtomicBoolean(true);
        AtomicBoolean failRpc = new AtomicBoolean(true);
        try {
            graph = manager.createGraphLocal(name, sharedMemoryConfig(name));
            Map<String, HugeGraph> factory = Whitebox.getInternalState(HugeFactory.class, "GRAPHS");
            // The shared-memory GraphFactory fixture bypasses HugeFactory.open().
            factory.put(graph.spaceGraphName(), graph);
            nativeRpc = startNativeRaftRpc();
            int port = nativeRpc.boundPort();
            Assert.assertTrue(nativeRpcListening(port));
            graph.configuration().setProperty("raft.group_peers", "127.0.0.1:" + port);
            BackendStoreProvider provider = Mockito.spy(graph.storeProvider());
            IllegalStateException primary = new IllegalStateException("provider failed before release");
            Throwable nodeFailure = fatalNode ? new AssertionError("fatal node close") : sameFailure ? primary :
                                               new IllegalStateException("node failed before release");
            IllegalStateException rpcFailure = sameFailure ? primary :
                                               new IllegalStateException("RPC failed before release");
            AtomicInteger providerCalls = new AtomicInteger();
            AtomicInteger nodeCalls = new AtomicInteger();
            AtomicInteger rpcCalls = new AtomicInteger();
            Mockito.doAnswer(call -> {
                providerCalls.incrementAndGet();
                if (failProvider.get()) {
                    throw primary;
                }
                return call.callRealMethod();
            }).when(provider).close();
            HugeGraphParams params = Whitebox.getInternalState(graph, "params");
            RaftBackendStoreProvider raft = new RaftBackendStoreProvider(params, provider);
            context = Whitebox.getInternalState(raft, "context");
            RaftNode node = Mockito.mock(RaftNode.class);
            Mockito.doAnswer(call -> {
                nodeCalls.incrementAndGet();
                if (failNode.get()) {
                    throw nodeFailure;
                }
                return null;
            }).when(node).shutdown();
            com.alipay.sofa.jraft.rpc.RpcServer rpc = Mockito.spy(nativeRpc);
            Mockito.doAnswer(call -> {
                rpcCalls.incrementAndGet();
                if (failRpc.get()) {
                    throw rpcFailure;
                }
                return call.callRealMethod();
            }).when(rpc).shutdown();
            Whitebox.setInternalState(context, "raftNode", node);
            Whitebox.setInternalState(context, "raftRpcServer", rpc);
            Whitebox.setInternalState(context, "endpoint", new PeerId("127.0.0.1", port));
            Whitebox.setInternalState(graph, "storeProvider", raft);
            MetaManager meta = Mockito.mock(MetaManager.class);
            Whitebox.setInternalState(manager, "metaManager", meta);
            Whitebox.setInternalState(manager, "PDExist", true);
            Thread.currentThread().setName("grpc-raft-close-failures");
            if (fatalNode) {
                AssertionError fatal = Assert.assertThrows(AssertionError.class,
                        () -> manager.dropGraph("DEFAULT", name, false));
                Assert.assertSame(nodeFailure, fatal);
                Assert.assertEquals(2, fatal.getSuppressed().length);
                Assert.assertSame(rpcFailure, fatal.getSuppressed()[0]);
                Assert.assertSame(primary, fatal.getSuppressed()[1]);
            } else {
                HugeException failure = Assert.assertThrows(HugeException.class,
                        () -> manager.dropGraph("DEFAULT", name, false));
                Assert.assertSame(primary, failure.getCause());
                Assert.assertEquals(sameFailure ? 0 : 1, primary.getSuppressed().length);
                if (!sameFailure) {
                    Assert.assertSame(nodeFailure, primary.getSuppressed()[0]);
                    Assert.assertSame(rpcFailure, nodeFailure.getSuppressed()[0]);
                }
            }
            Assert.assertTrue(graph.closed());
            Assert.assertSame(graph, manager.graph(graph.spaceGraphName()));
            Assert.assertSame(graph, factory.get(graph.spaceGraphName()));
            Assert.assertTrue(nativeRpcListening(port));
            Assert.assertFalse(context.snapshotExecutor().isShutdown());
            Assert.assertFalse(context.backendExecutor().isShutdown());
            failProvider.set(false);
            failNode.set(false);
            // A remaining listener failure must retain the logically closed owner too.
            // Retain the original non-clearing removal contract on every retry.
            HugeException failure = Assert.assertThrows(HugeException.class,
                    () -> manager.dropGraph("DEFAULT", name, false));
            Assert.assertSame(rpcFailure, failure.getCause());
            assertRaftExecutorsTerminated(context);
            Assert.assertSame(graph, manager.graph(graph.spaceGraphName()));
            Assert.assertSame(graph, factory.get(graph.spaceGraphName()));
            Assert.assertTrue(nativeRpcListening(port));
            failRpc.set(false);
            manager.dropGraph("DEFAULT", name, false);
            Assert.assertFalse(manager.graphs().contains(graph.spaceGraphName()));
            Assert.assertFalse(factory.containsKey(graph.spaceGraphName()));
            Assert.assertEquals(2, providerCalls.get());
            Assert.assertEquals(2, nodeCalls.get());
            Assert.assertEquals(3, rpcCalls.get());
            Mockito.verify(meta, Mockito.never()).removeGraphConfig("DEFAULT", name);
            Mockito.verify(meta, Mockito.never()).notifyGraphRemove("DEFAULT", name);
            Mockito.verify(meta, Mockito.never()).updateGraphSpaceConfig(Mockito.eq("DEFAULT"),
                                                                       Mockito.any(GraphSpace.class));
            awaitNativeRpcClosed(port);
        } finally {
            failProvider.set(false);
            failNode.set(false);
            failRpc.set(false);
            Thread.currentThread().setName(threadName);
            if (nativeRpc != null && nativeRpcListening(nativeRpc.boundPort())) {
                nativeRpc.shutdown();
            }
            stopRaftFixtureExecutors(context);
            manager.close();
            if (graph != null) {
                HugeFactory.remove(graph);
            }
        }
    }

    @Test
    public void testProviderFailureClosesRealRaftNodeBeforeOwnerRemoval() throws Exception {
        OptionSpace.register("rocksdb", RocksDBOptions.instance());
        BackendProviderFactory.register("rocksdb", RocksDBStoreProvider.class.getName());
        String name = "raft_real_close";
        GraphManager manager = this.dropManager(name);
        HugeGraph graph = null;
        com.alipay.sofa.jraft.rpc.RpcServer rpc = null;
        RaftContext context = null;
        RaftNode raftNode = null;
        String threadName = Thread.currentThread().getName();
        try {
            String data = this.temporary.newFolder("raft_data").getPath();
            String wal = this.temporary.newFolder("raft_wal").getPath();
            graph = manager.createGraphLocal(name, "gremlin.graph=" + HugeFactory.class.getName() +
                                                   "\nbackend=rocksdb\nserializer=binary\nstore=" + name +
                                                   "\nrocksdb.data_path=" + data + "\nrocksdb.wal_path=" + wal + "\n");
            rpc = startNativeRaftRpc();
            int port = rpc.boundPort();
            Assert.assertTrue(nativeRpcListening(port));
            HugeConfig config = graph.configuration();
            config.setProperty("raft.group_peers", "127.0.0.1:" + port);
            config.setProperty(CoreOptions.RAFT_PATH.name(), this.temporary.newFolder("raft_log").getPath());
            config.setProperty(CoreOptions.RAFT_ELECTION_TIMEOUT.name(), 300);
            config.setProperty(CoreOptions.RAFT_SNAPSHOT_INTERVAL.name(), 0);
            config.setProperty(CoreOptions.RAFT_RPC_THREADS.name(), 2);
            config.setProperty(CoreOptions.RAFT_QUEUE_SIZE.name(), 2048);
            config.setProperty(CoreOptions.RAFT_SAFE_READ.name(), true);
            BackendStoreProvider provider = Mockito.spy(graph.storeProvider());
            AtomicInteger providerCalls = new AtomicInteger();
            IllegalStateException primary = new IllegalStateException("provider failed before real node release");
            Mockito.doAnswer(call -> {
                providerCalls.incrementAndGet();
                throw primary;
            }).when(provider).close();
            HugeGraphParams params = Whitebox.getInternalState(graph, "params");
            RaftBackendStoreProvider raft = new RaftBackendStoreProvider(params, provider);
            raft.loadSchemaStore(config);
            raft.loadGraphStore(config);
            raft.loadSystemStore(config);
            context = Whitebox.getInternalState(raft, "context");
            Whitebox.setInternalState(context, "raftRpcServer", rpc);
            Whitebox.setInternalState(context, "endpoint", new PeerId("127.0.0.1", port));
            NodeManager.getInstance().addAddress(new Endpoint("127.0.0.1", port));
            raftNode = new RaftNode(context);
            Whitebox.setInternalState(context, "raftNode", raftNode);
            Node node = Whitebox.invoke(RaftNode.class, "node", raftNode);
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (node.getNodeState() != State.STATE_LEADER && System.nanoTime() < deadline) {
                Thread.sleep(10);
            }
            Assert.assertEquals(State.STATE_LEADER, node.getNodeState());
            Whitebox.setInternalState(graph, "storeProvider", raft);
            Whitebox.setInternalState(manager, "metaManager", Mockito.mock(MetaManager.class));
            Whitebox.setInternalState(manager, "PDExist", true);
            Thread.currentThread().setName("grpc-real-raft-close");
            HugeException failure = Assert.assertThrows(HugeException.class,
                                                       () -> manager.dropGraph("DEFAULT", name, false));
            Assert.assertSame(primary, failure.getCause());
            Assert.assertTrue(graph.closed());
            Assert.assertSame(graph, manager.graph(graph.spaceGraphName()));
            Map<?, ?> factory = Whitebox.getInternalState(HugeFactory.class, "GRAPHS");
            Assert.assertSame(graph, factory.get(graph.spaceGraphName()));
            Assert.assertEquals(State.STATE_SHUTDOWN, node.getNodeState());
            assertRaftExecutorsTerminated(context);
            awaitNativeRpcClosed(port);
            try (ServerSocket reused = new ServerSocket(port)) {
                Assert.assertEquals(port, reused.getLocalPort());
            }
            Mockito.doAnswer(call -> {
                providerCalls.incrementAndGet();
                return call.callRealMethod();
            }).when(provider).close();
            manager.dropGraph("DEFAULT", name, false);
            Assert.assertEquals(2, providerCalls.get());
            Assert.assertFalse(manager.graphs().contains(graph.spaceGraphName()));
            Assert.assertFalse(factory.containsKey(graph.spaceGraphName()));
            Assert.assertEquals(State.STATE_SHUTDOWN, node.getNodeState());
        } finally {
            Thread.currentThread().setName(threadName);
            if (raftNode != null) {
                Node node = Whitebox.invoke(RaftNode.class, "node", raftNode);
                if (node.getNodeState() != State.STATE_SHUTDOWN) {
                    raftNode.shutdown();
                }
            }
            if (rpc != null && nativeRpcListening(rpc.boundPort())) {
                rpc.shutdown();
            }
            stopRaftFixtureExecutors(context);
            manager.close();
            if (graph != null) {
                HugeFactory.remove(graph);
            }
        }
    }

    @Test
    public void testSharedRaftListenerSurvivesGraphDropAndOwnerRestart() throws Exception {
        GraphManager first = this.raftOwnerManager("shared_owner_one", 0);
        GraphManager second = null;
        List<HugeGraph> graphs = new ArrayList<>();
        List<RaftContext> contexts = new ArrayList<>();
        CliClientServiceImpl client = new CliClientServiceImpl();
        client.init(new CliOptions());
        String threadName = Thread.currentThread().getName();
        try {
            com.alipay.remoting.rpc.RpcServer nativeFirst = Whitebox.invoke(GraphManager.class,
                                                                          "remotingRpcServer", first);
            int port = nativeFirst.port();
            Endpoint address = new Endpoint(nativeFirst.ip(), port);
            Endpoint connect = new Endpoint("127.0.0.1", port);
            HugeGraph a = this.attachRaftGraph(first, "shared_real_a", nativeFirst, contexts);
            HugeGraph b = this.attachRaftGraph(first, "shared_real_b", nativeFirst, contexts);
            graphs.add(a);
            graphs.add(b);
            startSharedRaftPair(contexts, nativeFirst);
            Node nodeA = nativeNode(contexts.get(0));
            Node nodeB = nativeNode(contexts.get(1));
            assertRaftLeader(client, connect, b.spaceGraphName());
            Whitebox.setInternalState(first, "metaManager", Mockito.mock(MetaManager.class));
            Whitebox.setInternalState(first, "PDExist", true);
            Thread.currentThread().setName("grpc-shared-real-owner");
            first.dropGraph("DEFAULT", a.name(), false);
            Assert.assertEquals(State.STATE_SHUTDOWN, nodeA.getNodeState());
            assertRaftExecutorsTerminated(contexts.get(0));
            Assert.assertEquals(State.STATE_LEADER, nodeB.getNodeState());
            Assert.assertTrue(nativeRpcListening(port));
            Assert.assertTrue(NodeManager.getInstance().serverExists(address));
            assertRaftLeader(client, connect, b.spaceGraphName());
            client.shutdown();
            client = null;
            RpcServer server = Mockito.spy((RpcServer) Whitebox.getInternalState(first, "rpcServer"));
            IllegalStateException destroyFailure = new IllegalStateException("owner listener release failed");
            Mockito.doThrow(destroyFailure).when(server).destroy();
            Whitebox.setInternalState(first, "rpcServer", server);
            Assert.assertThrows(IllegalStateException.class, first::close,
                                error -> Assert.assertSame(destroyFailure, error));
            Assert.assertEquals(State.STATE_SHUTDOWN, nodeB.getNodeState());
            Assert.assertTrue(nativeRpcListening(port));
            Assert.assertTrue(NodeManager.getInstance().serverExists(address));
            Assert.assertEquals(address, Whitebox.getInternalState(first, "raftRpcEndpoint"));
            Mockito.doCallRealMethod().when(server).destroy();
            first.close();
            Mockito.verify(server, Mockito.times(2)).destroy();
            Assert.assertNull(Whitebox.getInternalState(first, "raftRpcEndpoint"));
            Assert.assertEquals(State.STATE_SHUTDOWN, nodeB.getNodeState());
            awaitNativeRpcClosed(port);
            Assert.assertFalse(NodeManager.getInstance().serverExists(address));
            List<Node> stopped = NodeManager.getInstance().getNodesByGroupId(b.spaceGraphName());
            Assert.assertTrue(stopped == null || stopped.isEmpty());
            try (ServerSocket reused = new ServerSocket(port)) {
                Assert.assertEquals(port, reused.getLocalPort());
            }
            second = this.raftOwnerManager("shared_owner_two", port);
            com.alipay.remoting.rpc.RpcServer nativeSecond = Whitebox.invoke(GraphManager.class,
                                                                           "remotingRpcServer", second);
            Assert.assertNotSame(nativeFirst, nativeSecond);
            Assert.assertEquals(port, nativeSecond.port());
            HugeGraph restarted = this.attachRaftGraph(second, b.name(), nativeSecond, contexts);
            graphs.add(restarted);
            contexts.get(2).initRaftNode(nativeSecond);
            Node fresh = nativeNode(contexts.get(2));
            awaitRaftLeader(fresh);
            Assert.assertNotSame(nodeB, fresh);
            client = new CliClientServiceImpl();
            client.init(new CliOptions());
            assertRaftLeader(client, connect, restarted.spaceGraphName());
            client.shutdown();
            client = null;
            second.close();
            Assert.assertEquals(State.STATE_SHUTDOWN, fresh.getNodeState());
            assertRaftExecutorsTerminated(contexts.get(2));
            awaitNativeRpcClosed(port);
            Assert.assertFalse(NodeManager.getInstance().serverExists(address));
        } finally {
            Thread.currentThread().setName(threadName);
            if (client != null) {
                client.shutdown();
            }
            first.close();
            if (second != null) {
                second.close();
            }
            for (RaftContext context : contexts) {
                stopRaftFixtureExecutors(context);
            }
            for (HugeGraph graph : graphs) {
                HugeFactory.remove(graph);
            }
        }
    }

    @Test
    public void testInterruptedRaftJoinRetainsOwnerUntilRetry() throws Exception {
        GraphManager manager = this.raftOwnerManager("interrupt_shared_owner", 0);
        List<HugeGraph> graphs = new ArrayList<>();
        List<RaftContext> contexts = new ArrayList<>();
        CliClientServiceImpl client = new CliClientServiceImpl();
        client.init(new CliOptions());
        String threadName = Thread.currentThread().getName();
        try {
            com.alipay.remoting.rpc.RpcServer shared = Whitebox.invoke(GraphManager.class,
                                                                     "remotingRpcServer", manager);
            Endpoint address = new Endpoint(shared.ip(), shared.port());
            Endpoint connect = new Endpoint("127.0.0.1", shared.port());
            HugeGraph a = this.attachRaftGraph(manager, "interrupt_real_a", shared, contexts);
            HugeGraph b = this.attachRaftGraph(manager, "interrupt_real_b", shared, contexts);
            graphs.add(a);
            graphs.add(b);
            startSharedRaftPair(contexts, shared);
            RaftContext context = contexts.get(0);
            Node nodeA = nativeNode(context);
            Node nodeB = nativeNode(contexts.get(1));
            RaftNode owner = Mockito.spy(context.node());
            AtomicBoolean interruptJoin = new AtomicBoolean(true);
            AtomicReference<InterruptedException> joinFailure = new AtomicReference<>();
            // Inject an interrupted wait at Node.join(); retry delegates to the same native node.
            // Setting the flag alone is insufficient: native component joins may consume it.
            Node joining = (Node) Proxy.newProxyInstance(Node.class.getClassLoader(),
                    new Class<?>[]{Node.class}, (proxy, method, args) -> {
                        if ("join".equals(method.getName()) && interruptJoin.get()) {
                            Thread.currentThread().interrupt();
                            try {
                                new CountDownLatch(1).await();
                            } catch (InterruptedException interrupted) {
                                joinFailure.set(interrupted);
                                throw interrupted;
                            }
                        }
                        try {
                            return method.invoke(nodeA, args);
                        } catch (InvocationTargetException failure) {
                            throw failure.getCause();
                        }
                    });
            Whitebox.setInternalState(owner, "node", joining);
            // Keep the baseline group-service join on the identical native join boundary too.
            Object group = Whitebox.getInternalState(owner, "raftGroupService");
            Whitebox.setInternalState(group, "node", joining);
            Whitebox.setInternalState(context, "raftNode", owner);
            Whitebox.setInternalState(manager, "metaManager", Mockito.mock(MetaManager.class));
            Whitebox.setInternalState(manager, "PDExist", true);
            Thread.currentThread().setName("grpc-interrupted-raft-join");
            HugeException failure = Assert.assertThrows(HugeException.class,
                    () -> manager.dropGraph("DEFAULT", a.name(), false));
            Assert.assertNotNull(joinFailure.get());
            Assert.assertSame(joinFailure.get(), HugeException.rootCause(failure));
            Assert.assertTrue(Thread.currentThread().isInterrupted());
            Assert.assertTrue(Thread.interrupted());
            Assert.assertTrue(a.closed());
            Assert.assertSame(a, manager.graph(a.spaceGraphName()));
            Map<?, ?> factory = Whitebox.getInternalState(HugeFactory.class, "GRAPHS");
            Assert.assertSame(a, factory.get(a.spaceGraphName()));
            Assert.assertTrue(nativeRpcListening(shared.port()));
            Assert.assertTrue(NodeManager.getInstance().serverExists(address));
            Assert.assertFalse(context.snapshotExecutor().isShutdown());
            Assert.assertFalse(context.backendExecutor().isShutdown());
            assertRaftLeader(client, connect, b.spaceGraphName());
            Mockito.verify(owner).shutdown();
            interruptJoin.set(false);
            manager.dropGraph("DEFAULT", a.name(), false);
            Assert.assertEquals(State.STATE_SHUTDOWN, nodeA.getNodeState());
            assertRaftExecutorsTerminated(context);
            Assert.assertFalse(manager.graphs().contains(a.spaceGraphName()));
            Mockito.verify(owner, Mockito.times(2)).shutdown();
            a.close();
            context.close();
            Mockito.verify(owner, Mockito.times(2)).shutdown();
            Assert.assertEquals(State.STATE_LEADER, nodeB.getNodeState());
            assertRaftLeader(client, connect, b.spaceGraphName());
            client.shutdown();
            client = null;
            manager.close();
            awaitNativeRpcClosed(shared.port());
            Assert.assertFalse(NodeManager.getInstance().serverExists(address));
        } finally {
            Thread.interrupted();
            Thread.currentThread().setName(threadName);
            if (client != null) {
                client.shutdown();
            }
            manager.close();
            for (RaftContext context : contexts) {
                stopRaftFixtureExecutors(context);
            }
            for (HugeGraph graph : graphs) {
                HugeFactory.remove(graph);
            }
        }
    }

    private GraphManager raftOwnerManager(String name, int port) throws Exception {
        OptionSpace.register("rocksdb", RocksDBOptions.instance());
        BackendProviderFactory.register("rocksdb", RocksDBStoreProvider.class.getName());
        GraphManager manager = this.dropManager(name);
        RpcServer rpc = Whitebox.getInternalState(manager, "rpcServer");
        ServerConfig config = Whitebox.getInternalState(rpc, "serverConfig");
        config.setPort(port);
        return manager;
    }

    private HugeGraph attachRaftGraph(GraphManager manager, String name,
                                     com.alipay.remoting.rpc.RpcServer shared,
                                     List<RaftContext> contexts) throws Exception {
        String suffix = name + "_" + contexts.size();
        String data = this.temporary.newFolder(suffix + "_data").getPath();
        String wal = this.temporary.newFolder(suffix + "_wal").getPath();
        HugeGraph graph = manager.createGraphLocal(name, "gremlin.graph=" + HugeFactory.class.getName() +
                "\nbackend=rocksdb\nserializer=binary\nstore=" + name +
                "\nrocksdb.data_path=" + data + "\nrocksdb.wal_path=" + wal + "\n");
        HugeConfig config = graph.configuration();
        config.setProperty("raft.group_peers", shared.ip() + ":" + shared.port());
        config.setProperty(CoreOptions.RAFT_PATH.name(), this.temporary.newFolder(suffix + "_raft").getPath());
        config.setProperty(CoreOptions.RAFT_ELECTION_TIMEOUT.name(), 300);
        config.setProperty(CoreOptions.RAFT_SNAPSHOT_INTERVAL.name(), 0);
        config.setProperty(CoreOptions.RAFT_RPC_THREADS.name(), 2);
        config.setProperty(CoreOptions.RAFT_QUEUE_SIZE.name(), 2048);
        config.setProperty(CoreOptions.RAFT_SAFE_READ.name(), false);
        HugeGraphParams params = Whitebox.getInternalState(graph, "params");
        RaftBackendStoreProvider raft = new RaftBackendStoreProvider(params, graph.storeProvider());
        raft.loadSchemaStore(config);
        raft.loadGraphStore(config);
        raft.loadSystemStore(config);
        contexts.add(Whitebox.getInternalState(raft, "context"));
        Whitebox.setInternalState(graph, "storeProvider", raft);
        return graph;
    }

    private static void startSharedRaftPair(List<RaftContext> contexts,
                                           com.alipay.remoting.rpc.RpcServer shared) throws Exception {
        contexts.get(0).initRaftNode(shared);
        RaftContext second = contexts.get(1);
        // Production second-context init currently repeats processor registration and fails.
        // Bypass only that independent startup defect to exercise real shared-node close ownership.
        Whitebox.setInternalState(second, "raftRpcServer", new BoltRpcServer(shared));
        Whitebox.setInternalState(second, "sharedRpcServer", true);
        Whitebox.setInternalState(second, "endpoint", new PeerId(shared.ip(), shared.port()));
        Whitebox.setInternalState(second, "raftNode", new RaftNode(second));
        awaitRaftLeader(nativeNode(contexts.get(0)));
        awaitRaftLeader(nativeNode(second));
    }

    private static Node nativeNode(RaftContext context) {
        return Whitebox.invoke(RaftNode.class, "node", context.node());
    }

    private static void awaitRaftLeader(Node node) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (node.getNodeState() != State.STATE_LEADER && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }
        Assert.assertEquals(State.STATE_LEADER, node.getNodeState());
    }

    private static void assertRaftLeader(CliClientServiceImpl client, Endpoint endpoint,
                                        String graph) throws Exception {
        com.google.protobuf.Message response = client.getLeader(endpoint,
                CliRequests.GetLeaderRequest.newBuilder().setGroupId(graph).build(), null)
                                                     .get(5, TimeUnit.SECONDS);
        Assert.assertTrue(response instanceof CliRequests.GetLeaderResponse);
        Assert.assertFalse(((CliRequests.GetLeaderResponse) response).getLeaderId().isEmpty());
    }

    @Test
    public void testFatalCloseErrorEscapesPeerRemovalBatch() throws Exception {
        GraphManager manager = this.dropManager("fatal_batch");
        HugeGraph first = null;
        HugeGraph next = null;
        BackendStoreProvider provider = null;
        try {
            first = manager.createGraphLocal("fatal_first", sharedMemoryConfig("fatal_first"));
            next = manager.createGraphLocal("fatal_next", sharedMemoryConfig("fatal_next"));
            Map<String, HugeGraph> factory = Whitebox.getInternalState(HugeFactory.class, "GRAPHS");
            factory.put(first.spaceGraphName(), first);
            factory.put(next.spaceGraphName(), next);
            provider = Mockito.spy(first.storeProvider());
            AssertionError fatal = new AssertionError("fatal provider close");
            Mockito.doThrow(fatal).when(provider).close();
            Whitebox.setInternalState(first, "storeProvider", provider);
            MetaManager meta = Mockito.mock(MetaManager.class);
            Mockito.when(meta.extractGraphsFromResponse("fatal")).thenReturn(
                    List.of(first.spaceGraphName(), next.spaceGraphName()));
            Whitebox.setInternalState(manager, "metaManager", meta);
            Whitebox.setInternalState(manager, "PDExist", true);
            java.lang.reflect.Method method = GraphManager.class.getDeclaredMethod("graphRemoveHandler", Object.class);
            method.setAccessible(true);
            AssertionError caught = Assert.assertThrows(AssertionError.class, () -> {
                try {
                    method.invoke(manager, "fatal");
                } catch (InvocationTargetException failure) {
                    // Whitebox.invoke wraps Error, so preserve the actual handler result here.
                    throw failure.getCause();
                }
            });
            Assert.assertSame(fatal, caught);
            Assert.assertSame(first, manager.graph(first.spaceGraphName()));
            Assert.assertSame(next, manager.graph(next.spaceGraphName()));
            Assert.assertSame(first, factory.get(first.spaceGraphName()));
            Assert.assertFalse(next.closed());
        } finally {
            if (provider != null) {
                Mockito.doCallRealMethod().when(provider).close();
            }
            manager.close();
            if (first != null) {
                HugeFactory.remove(first);
            }
            if (next != null) {
                HugeFactory.remove(next);
            }
        }
    }

    @Test
    public void testEarlierRuntimeFailureDoesNotHideFatalCloseError() throws Exception {
        this.checkFatalClosePriority(false);
    }

    @Test
    public void testRepeatedFatalCloseFailureKeepsIdentity() throws Exception {
        this.checkFatalClosePriority(true);
    }

    private void checkFatalClosePriority(boolean same) throws Exception {
        String name = same ? "fatal_repeated" : "fatal_priority";
        GraphManager manager = this.dropManager(name);
        HugeGraph first = null;
        HugeGraph next = null;
        BackendStoreProvider provider = null;
        org.apache.hugegraph.auth.AuthManager auth = null;
        try {
            first = manager.createGraphLocal(name, sharedMemoryConfig(name));
            next = manager.createGraphLocal(name + "_next", sharedMemoryConfig(name + "_next"));
            Whitebox.invoke(StandardHugeGraph.class, "closeCurrentThreadTransaction", first);
            Whitebox.invoke(StandardHugeGraph.class, "closeCurrentThreadTransaction", next);
            AssertionError fatal = new AssertionError("fatal native release");
            RuntimeException earlier = new IllegalStateException("earlier close failed");
            auth = Mockito.spy(first.authManager());
            Mockito.doThrow(same ? fatal : earlier).when(auth).close();
            first.switchAuthManager(auth);
            provider = Mockito.spy(first.storeProvider());
            Mockito.doThrow(fatal).when(provider).close();
            Whitebox.setInternalState(first, "storeProvider", provider);
            MetaManager meta = Mockito.mock(MetaManager.class);
            Mockito.when(meta.extractGraphsFromResponse("fatal-priority")).thenReturn(
                    List.of(first.spaceGraphName(), next.spaceGraphName()));
            Whitebox.setInternalState(manager, "metaManager", meta);
            Whitebox.setInternalState(manager, "PDExist", true);
            Map<String, GraphSpace> spaces = Whitebox.getInternalState(manager, "graphSpaces");
            GraphSpace space = new GraphSpace("DEFAULT");
            space.graphNumberUsed(5);
            spaces.put("DEFAULT", space);
            java.lang.reflect.Method method = GraphManager.class.getDeclaredMethod("graphRemoveHandler", Object.class);
            method.setAccessible(true);
            AssertionError caught = Assert.assertThrows(AssertionError.class, () -> {
                try {
                    method.invoke(manager, "fatal-priority");
                } catch (InvocationTargetException wrapped) {
                    throw wrapped.getCause();
                }
            });
            Assert.assertSame(fatal, caught);
            Assert.assertEquals(same ? 0 : 1, fatal.getSuppressed().length);
            if (!same) {
                Assert.assertSame(earlier, fatal.getSuppressed()[0]);
            }
            Assert.assertSame(first, manager.graph(first.spaceGraphName()));
            Assert.assertSame(next, manager.graph(next.spaceGraphName()));
            Assert.assertFalse(next.closed());
        } finally {
            if (auth != null) {
                Mockito.doCallRealMethod().when(auth).close();
            }
            if (provider != null) {
                Mockito.doCallRealMethod().when(provider).close();
            }
            manager.close();
            if (first != null) {
                HugeFactory.remove(first);
            }
            if (next != null) {
                HugeFactory.remove(next);
            }
        }
    }

    @Test
    public void testClosedUnclearedOwnerRejectsRestClearRetry() throws Exception {
        String name = "closed_uncleared";
        GraphManager manager = this.dropManager(name);
        HugeGraph graph = null;
        BackendStoreProvider provider = null;
        try {
            OptionSpace.register("rocksdb", RocksDBOptions.instance());
            BackendProviderFactory.register("rocksdb", RocksDBStoreProvider.class.getName());
            String config = "gremlin.graph=" + HugeFactory.class.getName() +
                            "\nbackend=rocksdb\nserializer=binary\nstore=" + name +
                            "\nrocksdb.data_path=" + this.temporary.newFolder("data").getPath() +
                            "\nrocksdb.wal_path=" + this.temporary.newFolder("wal").getPath() + "\n";
            graph = manager.createGraphLocal(name, config);
            graph.schema().vertexLabel("kept").useCustomizeStringId().create();
            graph.addVertex(org.apache.tinkerpop.gremlin.structure.T.label, "kept",
                            org.apache.tinkerpop.gremlin.structure.T.id, "uncleared");
            graph.tx().commit();
            Whitebox.invoke(StandardHugeGraph.class, "closeCurrentThreadTransaction", graph);
            provider = Mockito.spy(graph.storeProvider());
            IllegalStateException first = new IllegalStateException("provider release failed");
            Mockito.doThrow(first).when(provider).close();
            Whitebox.setInternalState(graph, "storeProvider", provider);
            MetaManager meta = Mockito.mock(MetaManager.class);
            Whitebox.setInternalState(manager, "metaManager", meta);
            Whitebox.setInternalState(manager, "PDExist", true);
            HugeException failedClose = Assert.assertThrows(HugeException.class,
                    () -> manager.dropGraph("DEFAULT", name, false));
            Assert.assertSame(first, failedClose.getCause());
            Assert.assertTrue(graph.closed());
            Map<String, HugeGraph> certificates = Whitebox.getInternalState(manager, "clearedGraphs");
            Assert.assertFalse(certificates.containsKey(graph.spaceGraphName()));
            Mockito.doCallRealMethod().when(provider).close();

            HugeException refused = Assert.assertThrows(HugeException.class, () ->
                    new org.apache.hugegraph.api.profile.GraphsAPI().drop(
                            manager, "DEFAULT", name, "I'm sure to drop the graph"));
            Assert.assertContains("without a successful backend clear", refused.getMessage());
            Assert.assertSame(graph, manager.graph(graph.spaceGraphName()));
            Mockito.verifyNoInteractions(meta);
        } finally {
            if (provider != null) {
                Mockito.doCallRealMethod().when(provider).close();
            }
            manager.close();
            if (graph != null) {
                HugeFactory.remove(graph);
            }
        }
    }

    @Test
    public void testClearDeleteRetriesAfterForeignTransactionCloses() throws Exception {
        this.checkClearedOwnerRetry(0);
    }

    @Test
    public void testClearedOwnerPeerRetryDoesNotClearReplacement() throws Exception {
        this.checkClearedOwnerRetry(1);
    }

    @Test
    public void testManagerStopKeepsUnfinishedClearProgress() throws Exception {
        this.checkClearedOwnerRetry(2);
    }

    private void checkClearedOwnerRetry(int retryMode) throws Exception {
        String name = "clear_idle_retry_" + retryMode;
        GraphManager manager = this.dropManager(name);
        HugeGraph graph = null;
        ExecutorService worker = Executors.newSingleThreadExecutor();
        CountDownLatch opened = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger clears = new AtomicInteger();
        try {
            graph = manager.createGraphLocal(name, sharedMemoryConfig(name));
            HugeGraph owner = graph;
            Map<String, HugeGraph> factory = Whitebox.getInternalState(HugeFactory.class, "GRAPHS");
            factory.put(owner.spaceGraphName(), owner);
            HugeGraph proxy = (HugeGraph) Proxy.newProxyInstance(HugeGraph.class.getClassLoader(),
                    new Class<?>[]{HugeGraph.class}, (delegate, method, args) -> {
                        if ("clearBackend".equals(method.getName())) {
                            clears.incrementAndGet();
                        }
                        try {
                            return method.invoke(owner, args);
                        } catch (InvocationTargetException failure) {
                            throw failure.getCause();
                        }
                    });
            Map<String, HugeGraph> graphs = Whitebox.getInternalState(manager, "graphs");
            graphs.put(graph.spaceGraphName(), proxy);
            MetaManager meta = Mockito.mock(MetaManager.class);
            Whitebox.setInternalState(manager, "metaManager", meta);
            Whitebox.setInternalState(manager, "PDExist", true);
            Map<String, GraphSpace> spaces = Whitebox.getInternalState(manager, "graphSpaces");
            GraphSpace space = new GraphSpace("DEFAULT");
            space.graphNumberUsed(5);
            spaces.put("DEFAULT", space);
            Future<?> transaction = worker.submit(() -> {
                try {
                    owner.tx().open();
                    opened.countDown();
                    E.checkState(release.await(15, TimeUnit.SECONDS), "Foreign transaction release timed out");
                } catch (InterruptedException failure) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(failure);
                } finally {
                    Whitebox.invoke(StandardHugeGraph.class, "closeCurrentThreadTransaction", owner);
                }
            });
            Assert.assertTrue(opened.await(5, TimeUnit.SECONDS));
            HugeException refusal = Assert.assertThrows(HugeException.class,
                    () -> manager.dropGraph("DEFAULT", name, true));
            Assert.assertTrue(refusal.getCause() instanceof IllegalStateException);
            Assert.assertFalse(graph.closed());
            Assert.assertEquals(1, clears.get());
            Map<String, HugeGraph> progress = Whitebox.getInternalState(manager, "clearedGraphs");
            Assert.assertSame(proxy, progress.get(graph.spaceGraphName()));
            Assert.assertSame(proxy, manager.graph(graph.spaceGraphName()));
            if (retryMode == 1) {
                Assert.assertThrows(HugeException.class, () -> manager.dropGraph("DEFAULT", name, false));
                Assert.assertSame(proxy, progress.get(graph.spaceGraphName()));
                Whitebox.invoke(GraphManager.class, "startRpcServer", manager);
                RpcServer server = Whitebox.getInternalState(manager, "rpcServer");
                RpcClientProvider client = Whitebox.getInternalState(manager, "rpcClient");
                assertRpcResources(server, client, 0);
            } else if (retryMode == 2) {
                manager.close();
                Assert.assertSame(proxy, progress.get(graph.spaceGraphName()));
            }
            release.countDown();
            transaction.get(5, TimeUnit.SECONDS);
            if (retryMode == 2) {
                manager.close();
            } else {
                // This is the same public path used by REST graph DELETE and by PD peer removal.
                manager.dropGraph("DEFAULT", name, retryMode == 0);
                Assert.assertFalse(manager.graphs().contains(graph.spaceGraphName()));
                Assert.assertFalse(factory.containsKey(graph.spaceGraphName()));
            }
            Assert.assertTrue(graph.closed());
            Assert.assertFalse(progress.containsKey(graph.spaceGraphName()));
            Assert.assertEquals(1, clears.get());
            Mockito.verify(meta).removeGraphConfig("DEFAULT", name);
            Mockito.verify(meta).notifyGraphRemove("DEFAULT", name);
            if (retryMode == 1) {
                HugeGraph replacement = manager.createGraphLocal(name, sharedMemoryConfig(name));
                factory.put(replacement.spaceGraphName(), replacement);
                BackendStoreProvider replacementProvider = Mockito.spy(replacement.storeProvider());
                Whitebox.setInternalState(replacement, "storeProvider", replacementProvider);
                // Even a stale certificate for the same name must not certify this new identity.
                progress.put(replacement.spaceGraphName(), proxy);
                manager.dropGraph("DEFAULT", name, true);
                Mockito.verify(replacementProvider).clear();
                Assert.assertTrue(replacement.closed());
                Assert.assertFalse(progress.containsKey(replacement.spaceGraphName()));
                Mockito.verify(meta, Mockito.times(2)).removeGraphConfig("DEFAULT", name);
                Mockito.verify(meta, Mockito.times(2)).notifyGraphRemove("DEFAULT", name);
            }
        } finally {
            release.countDown();
            worker.shutdownNow();
            Assert.assertTrue(worker.awaitTermination(5, TimeUnit.SECONDS));
            manager.close();
            if (graph != null) {
                HugeFactory.remove(graph);
            }
        }
    }

    @Test
    public void testRaftContextClosesOwnedExecutorsAfterWork() throws Exception {
        this.checkRaftExecutorDrain(0);
    }

    @Test
    public void testRaftExecutorTimeoutRetainsOwnerAndQueuedWork() throws Exception {
        this.checkRaftExecutorDrain(1);
    }

    @Test
    public void testInterruptedRaftExecutorDrainRetainsOwner() throws Exception {
        this.checkRaftExecutorDrain(2);
    }

    private void checkRaftExecutorDrain(int failureMode) throws Exception {
        String name = "raft_executor_drain_" + failureMode;
        GraphManager manager = this.dropManager(name);
        HugeGraph graph = null;
        RaftContext context = null;
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicBoolean interruptedWorker = new AtomicBoolean();
        AtomicBoolean interruptAwait = new AtomicBoolean(true);
        try {
            graph = manager.createGraphLocal(name, sharedMemoryConfig(name));
            Map<String, HugeGraph> factory = Whitebox.getInternalState(HugeFactory.class, "GRAPHS");
            factory.put(graph.spaceGraphName(), graph);
            com.alipay.sofa.jraft.rpc.RpcServer rpc = startNativeRaftRpc();
            graph.configuration().setProperty("raft.group_peers", "127.0.0.1:" + rpc.boundPort());
            graph.configuration().setProperty(CoreOptions.RAFT_SAFE_READ.name(), true);
            graph.configuration().setProperty(CoreOptions.RAFT_BACKEND_THREADS.name(), 1);
            graph.configuration().setProperty(CoreOptions.TASK_WAIT_TIMEOUT.name(), failureMode == 1 ? 0L : 1L);
            RaftBackendStoreProvider raft = new RaftBackendStoreProvider(
                    Whitebox.getInternalState(graph, "params"), graph.storeProvider());
            context = Whitebox.getInternalState(raft, "context");
            Whitebox.setInternalState(context, "raftRpcServer", rpc);
            Whitebox.setInternalState(context, "endpoint", new PeerId("127.0.0.1", rpc.boundPort()));
            Whitebox.setInternalState(graph, "storeProvider", raft);
            for (ExecutorService executor : new ExecutorService[]{context.readIndexExecutor(),
                                                                  context.snapshotExecutor(),
                                                                  context.backendExecutor()}) {
                executor.submit(() -> { }).get(5, TimeUnit.SECONDS);
            }
            Future<?> running = context.backendExecutor().submit(() -> {
                started.countDown();
                try {
                    E.checkState(release.await(10, TimeUnit.SECONDS), "Backend executor release timed out");
                } catch (InterruptedException interrupted) {
                    interruptedWorker.set(true);
                    Thread.currentThread().interrupt();
                }
            });
            Assert.assertTrue(started.await(5, TimeUnit.SECONDS));
            AtomicInteger completed = new AtomicInteger();
            Future<?> queued = context.backendExecutor().submit(completed::incrementAndGet);
            Whitebox.setInternalState(manager, "metaManager", Mockito.mock(MetaManager.class));
            Whitebox.setInternalState(manager, "PDExist", true);
            Map<String, GraphSpace> spaces = Whitebox.getInternalState(manager, "graphSpaces");
            GraphSpace space = new GraphSpace("DEFAULT");
            space.graphNumberUsed(5);
            spaces.put("DEFAULT", space);
            if (failureMode == 0) {
                release.countDown();
                manager.dropGraph("DEFAULT", name, false);
            } else {
                if (failureMode == 2) {
                    // Begin the interrupt only at executor await, after graph's caller-TX cleanup.
                    ExecutorService backend = context.backendExecutor();
                    ExecutorService awaiting = (ExecutorService) Proxy.newProxyInstance(
                            ExecutorService.class.getClassLoader(), new Class<?>[]{ExecutorService.class},
                            (proxy, method, args) -> {
                                if ("awaitTermination".equals(method.getName()) && interruptAwait.get()) {
                                    Thread.currentThread().interrupt();
                                }
                                try {
                                    return method.invoke(backend, args);
                                } catch (InvocationTargetException failure) {
                                    throw failure.getCause();
                                }
                            });
                    Whitebox.setInternalState(context, "backendExecutor", awaiting);
                }
                HugeException failure = Assert.assertThrows(HugeException.class,
                        () -> manager.dropGraph("DEFAULT", name, false));
                if (failureMode == 2) {
                    Assert.assertTrue(HugeException.rootCause(failure) instanceof InterruptedException);
                    Assert.assertTrue(Thread.interrupted());
                    interruptAwait.set(false);
                }
                Assert.assertTrue(graph.closed());
                Assert.assertSame(graph, manager.graph(graph.spaceGraphName()));
                Assert.assertSame(graph, factory.get(graph.spaceGraphName()));
                Assert.assertTrue(context.backendExecutor().isShutdown());
                Assert.assertFalse(context.backendExecutor().isTerminated());
                Assert.assertFalse(queued.isDone());
                Assert.assertFalse(interruptedWorker.get());
                release.countDown();
                running.get(5, TimeUnit.SECONDS);
                queued.get(5, TimeUnit.SECONDS);
                graph.configuration().setProperty(CoreOptions.TASK_WAIT_TIMEOUT.name(), 1L);
                manager.dropGraph("DEFAULT", name, false);
            }
            running.get(5, TimeUnit.SECONDS);
            queued.get(5, TimeUnit.SECONDS);
            Assert.assertEquals(1, completed.get());
            Assert.assertFalse(interruptedWorker.get());
            assertRaftExecutorsTerminated(context);
            Assert.assertFalse(manager.graphs().contains(graph.spaceGraphName()));
            awaitNativeRpcClosed(rpc.boundPort());
        } finally {
            Thread.interrupted();
            release.countDown();
            stopRaftFixtureExecutors(context);
            manager.close();
            if (graph != null) {
                HugeFactory.remove(graph);
            }
        }
    }

    private static void assertRaftExecutorsTerminated(RaftContext context) {
        for (ExecutorService executor : new ExecutorService[]{context.readIndexExecutor(),
                                                              context.snapshotExecutor(),
                                                              context.backendExecutor()}) {
            if (executor != null) {
                Assert.assertTrue(executor.isShutdown());
                Assert.assertTrue(executor.isTerminated());
            }
        }
    }

    private static com.alipay.sofa.jraft.rpc.RpcServer startNativeRaftRpc() throws Exception {
        int port;
        try (ServerSocket reservation = new ServerSocket(0)) {
            port = reservation.getLocalPort();
        }
        return RaftRpcServerFactory.createAndStartRaftRpcServer(new Endpoint("127.0.0.1", port));
    }

    private static boolean nativeRpcListening(int port) {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress("127.0.0.1", port), 200);
            return true;
        } catch (Exception refused) {
            return false;
        }
    }

    private static void awaitNativeRpcClosed(int port) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (nativeRpcListening(port) && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }
        Assert.assertFalse(nativeRpcListening(port));
    }

    private static void stopRaftFixtureExecutors(RaftContext context) throws Exception {
        if (context == null) {
            return;
        }
        for (ExecutorService pool : new ExecutorService[]{context.readIndexExecutor(), context.snapshotExecutor(),
                                                        context.backendExecutor()}) {
            if (pool != null) {
                pool.shutdownNow();
                Assert.assertTrue(pool.awaitTermination(5, TimeUnit.SECONDS));
            }
        }
    }

    @Test
    public void testMetadataDeleteFailureRestoresRetainedOwnerRpc() throws Exception {
        this.checkMetadataDeleteFailure(false, false);
    }

    @Test
    public void testMetadataDeleteFailureDoesNotSkipPeerRemove() throws Exception {
        this.checkMetadataDeleteFailure(false, false, true);
    }

    @Test
    public void testRemoveNotificationFailureRestoresRetainedOwnerRpc() throws Exception {
        this.checkMetadataDeleteFailure(true, false);
    }

    @Test
    public void testMetadataDeleteFailurePreservesRpcRestoreFailure() throws Exception {
        this.checkMetadataDeleteFailure(false, true);
    }

    private void checkMetadataDeleteFailure(boolean notify, boolean failRestore) throws Exception {
        this.checkMetadataDeleteFailure(notify, failRestore, false);
    }

    private void checkMetadataDeleteFailure(boolean notify, boolean failRestore, boolean peerRemove) throws Exception {
        OptionSpace.register("core", CoreOptions.instance());
        OptionSpace.register("rpc", RpcOptions.instance());
        OptionSpace.register("server", ServerOptions.instance());
        String name = notify ? "rpc_notify_fail" : failRestore ? "rpc_restore_fail" : "rpc_metadata_fail";
        PropertiesConfiguration properties = new PropertiesConfiguration();
        properties.setProperty(ServerOptions.GRAPHS.name(), this.temporary.newFolder(name).getPath());
        properties.setProperty(ServerOptions.USE_PD.name(), false);
        properties.setProperty(ServerOptions.GRAPH_LOAD_FROM_LOCAL_CONFIG.name(), false);
        properties.setProperty(ServerOptions.ENABLE_DYNAMIC_CREATE_DROP.name(), true);
        properties.setProperty(RpcOptions.RPC_SERVER_HOST.name(), "127.0.0.1");
        properties.setProperty(RpcOptions.RPC_SERVER_PORT.name(), 0);
        properties.setProperty(RpcOptions.RPC_REMOTE_URL.name(), "127.0.0.1:1");
        EventHub hub = new EventHub(name);
        GraphManager manager = new GraphManager(new HugeConfig(properties), hub);
        HugeGraph graph = null;
        String threadName = Thread.currentThread().getName();
        try {
            graph = manager.createGraphLocal(name, sharedMemoryConfig(name));
            Whitebox.invoke(GraphManager.class, "startRpcServer", manager);
            RpcServer original = Whitebox.getInternalState(manager, "rpcServer");
            RpcServer server = Mockito.spy(original);
            Whitebox.setInternalState(manager, "rpcServer", server);
            RpcClientProvider client = Whitebox.getInternalState(manager, "rpcClient");
            assertRpcResources(server, client, 2);
            IllegalStateException failure = new IllegalStateException("metadata fault before clear");
            IllegalStateException restoration = new IllegalStateException("RPC restoration export failed");
            if (failRestore) {
                Mockito.doThrow(restoration).when(server).exportAll();
            }
            MetaManager meta = Mockito.mock(MetaManager.class);
            if (notify) {
                Mockito.doThrow(failure).doNothing().when(meta).notifyGraphRemove("DEFAULT", name);
            } else {
                Mockito.doThrow(failure).doNothing().when(meta).removeGraphConfig("DEFAULT", name);
            }
            Whitebox.setInternalState(manager, "metaManager", meta);
            Whitebox.setInternalState(manager, "PDExist", true);
            AtomicReference<Throwable> rejected = new AtomicReference<>();
            Assert.assertThrows(HugeException.class, () -> manager.dropGraph("DEFAULT", name, true), rejected::set);
            Mockito.verify(meta).removeGraphConfig("DEFAULT", name);
            if (notify) {
                Mockito.verify(meta).notifyGraphRemove("DEFAULT", name);
            } else {
                Mockito.verify(meta, Mockito.never()).notifyGraphRemove("DEFAULT", name);
            }
            Assert.assertFalse(graph.closed());
            Assert.assertSame(graph, manager.graph(graph.spaceGraphName()));
            assertRpcResources(server, client, failRestore ? 0 : 2);
            Assert.assertSame(failure, rejected.get().getCause());
            Mockito.verify(server).exportAll();
            Assert.assertEquals(failRestore ? 1 : 0, failure.getSuppressed().length);
            if (failRestore) {
                Assert.assertSame(restoration, failure.getSuppressed()[0]);
            }
            Thread.currentThread().setName("grpc-rpc-metadata-retry");
            if (peerRemove) {
                // Another server deleted the config after this local metadata attempt failed.
                Mockito.when(meta.extractGraphsFromResponse(Mockito.any()))
                       .thenReturn(java.util.Collections.singletonList(graph.spaceGraphName()));
                Whitebox.invoke(GraphManager.class, new Class<?>[]{Object.class},
                                "graphRemoveHandler", manager, "peer-remove");
            } else {
                // Retry the fenced deletion; successful clear must not re-export the removed owner.
                if (notify) {
                    java.util.Set<?> removing = Whitebox.getInternalState(manager, "removingGraphs");
                    Assert.assertTrue(removing.contains(graph.spaceGraphName()));
                }
                manager.dropGraph("DEFAULT", name, true);
            }
            Assert.assertTrue(graph.closed());
            Assert.assertFalse(manager.graphs().contains(graph.spaceGraphName()));
            assertRpcResources(server, client, 0);
            Mockito.verify(server).exportAll();
        } finally {
            Thread.currentThread().setName(threadName);
            manager.close();
            if (graph != null) {
                HugeFactory.remove(graph);
            }
        }
    }

    @Test
    public void testLocalGraphGremlinPublicationWaitsForRpc() throws Exception {
        this.checkLocalGraphGremlinPublication(false);
    }

    @Test
    public void testLocalGraphRpcFailureNeverPublishesToGremlin() throws Exception {
        this.checkLocalGraphGremlinPublication(true);
    }

    private void checkLocalGraphGremlinPublication(boolean failExport) throws Exception {
        OptionSpace.register("core", CoreOptions.instance());
        OptionSpace.register("rpc", RpcOptions.instance());
        OptionSpace.register("server", ServerOptions.instance());
        String name = failExport ? "rpc_export_failure" : "rpc_export_success";
        String graphName = "DEFAULT-" + name;
        PropertiesConfiguration properties = new PropertiesConfiguration();
        properties.setProperty(ServerOptions.GRAPHS.name(), this.temporary.newFolder(name).getPath());
        properties.setProperty(ServerOptions.USE_PD.name(), false);
        properties.setProperty(ServerOptions.GRAPH_LOAD_FROM_LOCAL_CONFIG.name(), false);
        properties.setProperty(ServerOptions.ENABLE_DYNAMIC_CREATE_DROP.name(), true);
        properties.setProperty(RpcOptions.RPC_SERVER_HOST.name(), "127.0.0.1");
        properties.setProperty(RpcOptions.RPC_SERVER_PORT.name(), 0);
        properties.setProperty(RpcOptions.RPC_REMOTE_URL.name(), "127.0.0.1:1");
        EventHub hub = new EventHub(name);
        Settings settings = new Settings();
        settings.gremlinPool = 1;
        settings.threadPoolBoss = 1;
        settings.threadPoolWorker = 1;
        // Production constructs Gremlin before REST/GraphManager, registering its listener first.
        ContextGremlinServer gremlin = new ContextGremlinServer(settings, hub);
        GraphManager manager = new GraphManager(new HugeConfig(properties), hub);
        RpcServer original = Whitebox.getInternalState(manager, "rpcServer");
        RpcServer server = Mockito.spy(original);
        RpcClientProvider client = Whitebox.getInternalState(manager, "rpcClient");
        Whitebox.setInternalState(manager, "rpcServer", server);
        Whitebox.setInternalState(manager, "rpcInitialized", true);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        IllegalStateException failure = new IllegalStateException("RPC export rejected");
        Mockito.doAnswer(invocation -> {
            entered.countDown();
            E.checkState(release.await(10, TimeUnit.SECONDS), "RPC export barrier timed out");
            if (failExport) {
                throw failure;
            }
            return invocation.callRealMethod();
        }).when(server).exportAll();
        ExecutorService initializer = Executors.newSingleThreadExecutor();
        AtomicReference<HugeGraph> candidate = new AtomicReference<>();
        org.apache.tinkerpop.gremlin.server.GraphManager bindings =
                gremlin.getServerGremlinExecutor().getGraphManager();
        try {
            Future<HugeGraph> publication = initializer.submit(() -> {
                try {
                    HugeGraph graph = manager.createGraphLocal(name, sharedMemoryConfig(name));
                    candidate.set(graph);
                    return graph;
                } finally {
                    Map<String, HugeGraph> owners = Whitebox.getInternalState(HugeFactory.class, "GRAPHS");
                    HugeGraph graph = owners.get(graphName);
                    if (graph != null) {
                        Whitebox.invoke(StandardHugeGraph.class, "closeCurrentThreadTransaction", graph);
                    }
                }
            });
            Assert.assertTrue(entered.await(5, TimeUnit.SECONDS));
            Assert.assertNull(bindings.getGraph(graphName));
            Assert.assertNull(bindings.getTraversalSource("__g_" + graphName));
            Assert.assertFalse(bindings.getAsBindings().containsKey(graphName));
            Assert.assertFalse(manager.graphs().contains(graphName));
            assertRpcResources(server, client, 2);
            release.countDown();
            if (failExport) {
                Assert.assertThrows(ExecutionException.class, () -> publication.get(5, TimeUnit.SECONDS),
                                    caught -> Assert.assertSame(failure, caught.getCause()));
                Assert.assertNull(bindings.getGraph(graphName));
                Assert.assertNull(bindings.getTraversalSource("__g_" + graphName));
                Assert.assertFalse(manager.graphs().contains(graphName));
                Map<?, ?> owners = Whitebox.getInternalState(HugeFactory.class, "GRAPHS");
                Assert.assertFalse(owners.containsKey(graphName));
                assertRpcResources(server, client, 0);
            } else {
                HugeGraph graph = publication.get(5, TimeUnit.SECONDS);
                Assert.assertSame(graph, bindings.getGraph(graphName));
                Assert.assertNotNull(bindings.getTraversalSource("__g_" + graphName));
                Assert.assertSame(graph, manager.graph(graphName));
                assertRpcResources(server, client, 2);
            }
        } finally {
            release.countDown();
            initializer.shutdown();
            Assert.assertTrue(initializer.awaitTermination(10, TimeUnit.SECONDS));
            manager.close();
            gremlin.stop().get(30, TimeUnit.SECONDS);
            if (candidate.get() != null) {
                HugeFactory.remove(candidate.get());
            }
            HugeGraphAuthProxy.resetContext();
        }
    }

    @Test
    public void testLateGraphRpcPublicationRollbackAndReplacement() throws Exception {
        OptionSpace.register("core", CoreOptions.instance());
        OptionSpace.register("rpc", RpcOptions.instance());
        OptionSpace.register("server", ServerOptions.instance());
        PropertiesConfiguration properties = new PropertiesConfiguration();
        properties.setProperty(ServerOptions.GRAPHS.name(), this.temporary.newFolder("graphs").getPath());
        properties.setProperty(ServerOptions.USE_PD.name(), false);
        properties.setProperty(ServerOptions.GRAPH_LOAD_FROM_LOCAL_CONFIG.name(), false);
        properties.setProperty(ServerOptions.ENABLE_DYNAMIC_CREATE_DROP.name(), true);
        properties.setProperty(RpcOptions.RPC_SERVER_HOST.name(), "127.0.0.1");
        properties.setProperty(RpcOptions.RPC_SERVER_PORT.name(), 0);
        try (ServerSocket reservation = new ServerSocket(0)) {
            properties.setProperty(RpcOptions.RPC_REMOTE_URL.name(), "127.0.0.1:" + reservation.getLocalPort());
        }
        EventHub hub = new EventHub("rpc-publication-test");
        GraphManager manager = new GraphManager(new HugeConfig(properties), hub);
        BarrierRpcServer server = new BarrierRpcServer(new HugeConfig(properties));
        Whitebox.setInternalState(manager, "rpcServer", server);
        RpcClientProvider client = Whitebox.getInternalState(manager, "rpcClient");
        ExecutorService initializer = Executors.newFixedThreadPool(2);
        HugeGraph startup = null;
        HugeGraph replacement = null;
        try {
            startup = manager.createGraphLocal("rpc_startup", sharedMemoryConfig("rpc_startup"));
            Assert.assertEquals(0, server.config().configs().size());
            HugeGraph startupOwner = startup;
            AtomicReference<Thread> initThread = new AtomicReference<>();
            Future<?> initialization = initializer.submit(() -> {
                initThread.set(Thread.currentThread());
                try {
                    Whitebox.invoke(manager.getClass(), "startRpcServer", manager);
                } finally {
                    Whitebox.invoke(StandardHugeGraph.class, "closeCurrentThreadTransaction", startupOwner);
                }
            });
            Assert.assertTrue(server.entered.await(5, TimeUnit.SECONDS));
            Assert.assertEquals(2, server.config().configs().size());
            AtomicReference<Thread> publisher = new AtomicReference<>();
            Future<HugeGraph> publication = initializer.submit(() -> {
                publisher.set(Thread.currentThread());
                HugeGraph graph = manager.createGraphLocal("rpc_late", sharedMemoryConfig("rpc_late"));
                Whitebox.invoke(StandardHugeGraph.class, "closeCurrentThreadTransaction", graph);
                return graph;
            });
            awaitManagerGate(publisher, initThread.get());
            Assert.assertFalse(manager.graphs().contains("DEFAULT-rpc_late"));
            assertRpcResources(server, client, 2);
            server.release.countDown();
            initialization.get(5, TimeUnit.SECONDS);
            HugeGraph original = publication.get(5, TimeUnit.SECONDS);
            Assert.assertEquals(2, server.exports.get());
            assertRpcResources(server, client, 4);

            EventListener failure = event -> {
                throw new IllegalStateException("reject candidate after RPC registration");
            };
            hub.listen(Events.GRAPH_CREATE, failure);
            try {
                Assert.assertThrows(HugeException.class, () -> manager.createGraphLocal(
                        "rpc_rejected", sharedMemoryConfig("rpc_rejected")));
            } finally {
                hub.unlisten(Events.GRAPH_CREATE, failure);
            }
            Assert.assertFalse(manager.graphs().contains("DEFAULT-rpc_rejected"));
            assertRpcResources(server, client, 4);

            manager.dropGraphLocal("rpc_late");
            assertRpcResources(server, client, 2);
            replacement = manager.createGraphLocal("rpc_late", sharedMemoryConfig("rpc_late"));
            hub.notifySync(Events.GRAPH_DROP, original);
            Assert.assertSame(replacement, manager.graph("DEFAULT-rpc_late"));
            assertRpcResources(server, client, 4);
            manager.close();
            List<?> bootstraps = Whitebox.getInternalState(client.config(), "bootstraps");
            Assert.assertEquals(0, server.config().configs().size());
            Assert.assertEquals(0, bootstraps.size());
        } finally {
            server.release.countDown();
            initializer.shutdownNow();
            initializer.awaitTermination(5, TimeUnit.SECONDS);
            manager.close();
            if (startup != null) {
                HugeFactory.remove(startup);
            }
            if (replacement != null) {
                HugeFactory.remove(replacement);
            }
        }
    }

    private static void awaitManagerGate(AtomicReference<Thread> publisher, Thread initializer) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (System.nanoTime() < deadline) {
            Thread thread = publisher.get();
            ThreadInfo info = thread == null ? null : ManagementFactory.getThreadMXBean().getThreadInfo(thread.getId());
            if (info != null && info.getThreadState() == Thread.State.BLOCKED &&
                info.getLockOwnerId() == initializer.getId()) {
                Assert.assertContains(GraphManager.class.getName(), info.getLockName());
                return;
            }
            Thread.sleep(5);
        }
        Assert.fail("Graph publisher did not block on the startup manager gate");
    }

    private static class BarrierRpcServer extends RpcServer {

        private final CountDownLatch entered = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);
        private final AtomicInteger exports = new AtomicInteger();

        BarrierRpcServer(HugeConfig config) {
            super(config);
        }

        @Override
        public void exportAll() {
            if (this.exports.incrementAndGet() == 1) {
                this.entered.countDown();
                try {
                    E.checkState(this.release.await(5, TimeUnit.SECONDS), "RPC export barrier timed out");
                } catch (InterruptedException failure) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(failure);
                }
            }
            super.exportAll();
        }
    }

    private static void assertRpcResources(RpcServer server, RpcClientProvider client, int count) {
        Assert.assertEquals(count, server.config().configs().size());
        Map<?, ?> configs = Whitebox.getInternalState(client.config(), "configs");
        List<?> bootstraps = Whitebox.getInternalState(client.config(), "bootstraps");
        Assert.assertEquals(count, configs.size());
        Assert.assertEquals(count, bootstraps.size());
    }

    private static String sharedMemoryConfig(String name) {
        return "gremlin.graph=" + SharedMemoryGraph.class.getName() + "\nbackend=memory\nstore=" + name + "\n";
    }

    // Exercises cache RPC registration, not shared-backend DML or consistency.
    public static class SharedMemoryGraph extends StandardHugeGraph {

        public SharedMemoryGraph(HugeConfig config) {
            super(config);
        }

        public static SharedMemoryGraph open(Configuration config) {
            return new SharedMemoryGraph(new HugeConfig(config));
        }

        @Override
        public BackendFeatures backendStoreFeatures() {
            BackendFeatures features = super.backendStoreFeatures();
            return (BackendFeatures) Proxy.newProxyInstance(BackendFeatures.class.getClassLoader(),
                    new Class<?>[]{BackendFeatures.class}, (proxy, method, args) ->
                    "supportsSharedStorage".equals(method.getName()) ? true : method.invoke(features, args));
        }
    }

    @Test
    public void testAttachExplicitLocalResourceLimits() {
        PropertiesConfiguration properties = new PropertiesConfiguration();
        properties.setProperty(CoreOptions.SCHEMA_CACHE_CAPACITY.name(), 1000L);
        properties.setProperty(
                CoreOptions.SERIALIZER_BUFFER_MAX_CAPACITY.name(), 8388608);
        GraphManager manager = newManager(properties);

        try {
            Map<String, Object> attached = attach(manager, new HashMap<>());

            Assert.assertEquals(1000L, attached.get(
                    CoreOptions.SCHEMA_CACHE_CAPACITY.name()));
            Assert.assertEquals(8388608, attached.get(
                    CoreOptions.SERIALIZER_BUFFER_MAX_CAPACITY.name()));
        } finally {
            manager.close();
        }
    }

    @Test
    public void testKeepIncomingResourceLimits() {
        PropertiesConfiguration properties = new PropertiesConfiguration();
        properties.setProperty(CoreOptions.SCHEMA_CACHE_CAPACITY.name(), 1000L);
        properties.setProperty(
                CoreOptions.SERIALIZER_BUFFER_MAX_CAPACITY.name(), 8388608);
        GraphManager manager = newManager(properties);
        Map<String, Object> configs = new HashMap<>();
        configs.put(CoreOptions.SCHEMA_CACHE_CAPACITY.name(), 2000L);
        configs.put(CoreOptions.SERIALIZER_BUFFER_MAX_CAPACITY.name(),
                    16777216);

        try {
            Map<String, Object> attached = attach(manager, configs);

            Assert.assertEquals(2000L, attached.get(
                    CoreOptions.SCHEMA_CACHE_CAPACITY.name()));
            Assert.assertEquals(16777216, attached.get(
                    CoreOptions.SERIALIZER_BUFFER_MAX_CAPACITY.name()));
        } finally {
            manager.close();
        }
    }

    @Test
    public void testAttachDoesNotMutateSourceOrCopySensitiveOptions() {
        PropertiesConfiguration properties = new PropertiesConfiguration();
        properties.setProperty(CoreOptions.SCHEMA_CACHE_CAPACITY.name(), 1000L);
        properties.setProperty(
                CoreOptions.SERIALIZER_BUFFER_MAX_CAPACITY.name(), 8388608);
        properties.setProperty(CoreOptions.BACKEND.name(), "memory");
        properties.setProperty(CoreOptions.STORE.name(), "local_store");
        properties.setProperty(ServerOptions.ADMIN_PA.name(), "local_secret");
        GraphManager manager = newManager(properties);
        Map<String, Object> configs = new HashMap<>();
        configs.put("marker", "source");
        Map<String, Object> source = new HashMap<>(configs);

        try {
            Map<String, Object> attached = attach(manager, configs);

            Assert.assertEquals(source, configs);
            Assert.assertEquals("source", attached.get("marker"));
            Assert.assertFalse(attached.containsKey(CoreOptions.BACKEND.name()));
            Assert.assertFalse(attached.containsKey(CoreOptions.STORE.name()));
            Assert.assertFalse(attached.containsKey(ServerOptions.ADMIN_PA.name()));
        } finally {
            manager.close();
        }
    }

    @Test
    public void testDoNotAttachImplicitLocalDefaults() {
        GraphManager manager = newManager(new PropertiesConfiguration());

        try {
            Map<String, Object> attached = attach(manager, new HashMap<>());

            Assert.assertFalse(attached.containsKey(
                    CoreOptions.SCHEMA_CACHE_CAPACITY.name()));
            Assert.assertFalse(attached.containsKey(
                    CoreOptions.SERIALIZER_BUFFER_MAX_CAPACITY.name()));
        } finally {
            manager.close();
        }
    }

    @Test
    public void testDoNotAttachResourceLimitsToAliasGraph() {
        PropertiesConfiguration properties = new PropertiesConfiguration();
        properties.setProperty(CoreOptions.SCHEMA_CACHE_CAPACITY.name(), 1000L);
        properties.setProperty(
                CoreOptions.SERIALIZER_BUFFER_MAX_CAPACITY.name(), 8388608);
        GraphManager manager = newManager(properties);
        Map<String, Object> configs = new HashMap<>();
        configs.put(CoreOptions.ALIAS_NAME.name(), "source_graph");
        Map<String, Object> source = new HashMap<>(configs);

        try {
            Map<String, Object> attached = attach(manager, configs);

            Assert.assertEquals(source, attached);
            Assert.assertEquals(source, configs);
        } finally {
            manager.close();
        }
    }

    private static GraphManager newManager(PropertiesConfiguration properties) {
        return new GraphManager(new HugeConfig(properties),
                                new EventHub("config-test"));
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> attach(GraphManager manager,
                                               Map<String, Object> configs) {
        return Whitebox.invoke(manager.getClass(), new Class<?>[]{Map.class},
                               "attachLocalCacheConfig", manager, configs);
    }
}
