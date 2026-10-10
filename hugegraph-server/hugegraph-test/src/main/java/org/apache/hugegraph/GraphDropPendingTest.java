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

package org.apache.hugegraph;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.TimeUnit;

import org.apache.hugegraph.config.CoreOptions;
import org.apache.hugegraph.backend.store.BackendStoreProvider;
import org.apache.hugegraph.auth.HugeGraphAuthProxy;
import org.apache.hugegraph.auth.HugeAuthenticator;
import org.apache.hugegraph.auth.RolePermission;
import org.apache.hugegraph.auth.HugePermission;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.config.ServerOptions;
import org.apache.hugegraph.core.GraphManager;
import org.apache.hugegraph.dist.RegisterUtil;
import org.apache.hugegraph.event.EventHub;
import org.apache.hugegraph.exception.HugeException;
import org.apache.hugegraph.exception.NotFoundException;
import org.apache.hugegraph.id.IdGenerator;
import org.apache.hugegraph.masterelection.GlobalMasterInfo;
import org.apache.hugegraph.job.EphemeralJob;
import org.apache.hugegraph.meta.MetaManager;
import org.apache.hugegraph.space.GraphSpace;
import org.apache.hugegraph.space.Service;
import org.apache.hugegraph.task.HugeTask;
import org.apache.hugegraph.task.TaskCallable;
import org.apache.hugegraph.task.TaskManager;
import org.apache.hugegraph.task.TaskScheduler;
import org.apache.hugegraph.task.TaskStatus;
import org.apache.hugegraph.task.TaskAndResultSchedulerTest.EmptyCallable;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.testutil.Whitebox;
import org.apache.hugegraph.unit.FakeObjects;
import org.apache.hugegraph.util.Events;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.junit.Test;
import org.mockito.Mockito;

public class GraphDropPendingTest {

    @Test
    public void testDirectDropPreservesActiveTask() throws Exception {
        this.checkDrop(0);
    }

    @Test
    public void testLocalManagerDropPreservesActiveTask() throws Exception {
        this.checkDrop(1);
    }

    @Test
    public void testPdDropPreservesActiveTaskBeforeMetadataRemoval() throws Exception {
        this.checkDrop(2);
    }

    @Test
    public void testPdNotificationRetainsActiveTaskOwners() throws Exception {
        this.checkDrop(3);
    }

    @Test
    public void testClearGraphSpaceRetainsGraphsAndServicesWithSimilarPrefix() throws Exception {
        RegisterUtil.registerBackends();
        HugeConfig config = FakeObjects.newConfig();
        config.setProperty("backend", "memory");
        config.setProperty("serializer", "text");
        config.setProperty("store", "space_prefix_target");
        config.setProperty(CoreOptions.GRAPH_SPACE.name(), "team");
        HugeGraph target = HugeFactory.open(config);
        HugeConfig neighborConfig = FakeObjects.newConfig();
        neighborConfig.setProperty("backend", "memory");
        neighborConfig.setProperty("serializer", "text");
        // The memory provider singleton is keyed by store name, not graph space.
        neighborConfig.setProperty("store", "space_prefix_neighbor");
        neighborConfig.setProperty(CoreOptions.GRAPH_SPACE.name(), "team2");
        HugeGraph neighbor = HugeFactory.open(neighborConfig);
        HugeConfig serverConfig = FakeObjects.newConfig();
        serverConfig.setProperty(ServerOptions.USE_PD.name(), false);
        serverConfig.setProperty(ServerOptions.GRAPH_LOAD_FROM_LOCAL_CONFIG.name(), false);
        GraphManager manager = new GraphManager(serverConfig, new EventHub("space-prefix"));
        Whitebox.setInternalState(manager, "PDExist", true);
        Map<String, Graph> graphs = Whitebox.getInternalState(manager, "graphs");
        graphs.put(target.spaceGraphName(), target);
        graphs.put(neighbor.spaceGraphName(), neighbor);
        Map<String, GraphSpace> spaces = Whitebox.getInternalState(manager, "graphSpaces");
        spaces.put("team", Mockito.mock(GraphSpace.class));
        spaces.put("team2", Mockito.mock(GraphSpace.class));
        Map<String, Service> services = Whitebox.getInternalState(manager, "services");
        Service targetService = Mockito.mock(Service.class);
        Service neighborService = Mockito.mock(Service.class);
        services.put("team-service", targetService);
        services.put("team2-service", neighborService);
        MetaManager meta = Mockito.mock(MetaManager.class);
        Whitebox.setInternalState(manager, "metaManager", meta);
        Mockito.when(meta.service("team", "service")).thenReturn(targetService);
        Mockito.when(meta.service("team2", "service")).thenReturn(neighborService);
        try {
            target.initBackend();
            target.serverStarted(GlobalMasterInfo.master("space-prefix-target"));
            neighbor.initBackend();
            neighbor.serverStarted(GlobalMasterInfo.master("space-prefix-neighbor"));
            neighbor.schema().vertexLabel("person").useCustomizeStringId().create();
            neighbor.addVertex(T.id, "retained", T.label, "person");
            neighbor.tx().commit();
            manager.clearGraphSpace("team");
            Assert.assertTrue(target.closed());
            Assert.assertNull(manager.graph(target.spaceGraphName()));
            Assert.assertFalse(neighbor.closed());
            Assert.assertSame(neighbor, manager.graph(neighbor.spaceGraphName()));
            Assert.assertEquals(IdGenerator.of("retained"), neighbor.vertices("retained").next().id());
            Map<String, HugeGraph> registered = Whitebox.getInternalState(HugeFactory.class, "GRAPHS");
            Assert.assertSame(neighbor, registered.get(neighbor.spaceGraphName()));
            Assert.assertNotNull(TaskManager.instance().getScheduler(neighbor));
            Assert.assertFalse(services.containsKey("team-service"));
            Assert.assertSame(neighborService, services.get("team2-service"));
            Mockito.verify(meta).removeGraphConfig("team", target.name());
            Mockito.verify(meta).removeServiceConfig("team", "service");
            Mockito.verify(meta, Mockito.never()).removeGraphConfig("team2", neighbor.name());
            Mockito.verify(meta, Mockito.never()).removeServiceConfig("team2", "service");
            Mockito.verify(meta, Mockito.never()).clearGraphAuth("team2");
            Mockito.verify(meta, Mockito.never()).clearSchemaTemplate("team2");
        } finally {
            graphs.clear();
            services.clear();
            manager.close();
            if (!target.closed()) {
                target.close();
            }
            if (!neighbor.closed()) {
                neighbor.close();
            }
            HugeFactory.remove(target);
            HugeFactory.remove(neighbor);
        }
    }

    @Test
    public void testZeroPendingCloseFailureRetainsSchedulerAndProviderUntilRetry() throws Exception {
        RegisterUtil.registerBackends();
        HugeConfig config = FakeObjects.newConfig();
        config.setProperty("backend", "memory");
        config.setProperty("serializer", "text");
        config.setProperty("store", "graph_scheduler_close_retry");
        HugeGraph graph = HugeFactory.open(config);
        BackendStoreProvider provider = Mockito.spy(graph.storeProvider());
        Whitebox.setInternalState(graph, "storeProvider", provider);
        HugeGraphParams params = Whitebox.getInternalState(graph, "params");
        Map<HugeGraphParams, TaskScheduler> schedulers =
                Whitebox.getInternalState(TaskManager.instance(), "schedulers");
        TaskScheduler scheduler = Mockito.spy(graph.taskScheduler());
        schedulers.put(params, scheduler);
        RuntimeException failure = new IllegalStateException("scheduler owners not closed");
        Mockito.doThrow(failure).doCallRealMethod().when(scheduler).close();
        try {
            graph.initBackend();
            graph.serverStarted(GlobalMasterInfo.master("scheduler-close-retry-test"));
            Assert.assertEquals(0, scheduler.pendingTasks());
            Assert.assertSame(failure, Assert.assertThrows(IllegalStateException.class, graph::close));
            Assert.assertFalse(graph.closed());
            Assert.assertSame(scheduler, TaskManager.instance().getScheduler(params));
            Mockito.verify(provider, Mockito.never()).close();

            graph.close();
            Assert.assertTrue(graph.closed());
            Assert.assertNull(TaskManager.instance().getScheduler(params));
            Mockito.verify(scheduler, Mockito.times(2)).close();
            Mockito.verify(provider, Mockito.times(1)).close();
        } finally {
            try {
                if (!graph.closed()) {
                    graph.close();
                }
            } finally {
                HugeFactory.remove(graph);
            }
        }
    }

    @Test
    public void testPdNotificationPreservesCloseFailureAndRegistrations() throws Exception {
        HugeConfig config = FakeObjects.newConfig();
        config.setProperty(ServerOptions.USE_PD.name(), false);
        config.setProperty(ServerOptions.GRAPH_LOAD_FROM_LOCAL_CONFIG.name(), false);
        GraphManager manager = new GraphManager(config, new EventHub("drop-close-failure"));
        Whitebox.setInternalState(manager, "PDExist", true);
        StandardHugeGraph graph = Mockito.mock(StandardHugeGraph.class);
        Mockito.when(graph.spaceGraphName()).thenReturn("DEFAULT-close_failure");
        RuntimeException cause = new IllegalStateException("close owner failed");
        Mockito.doThrow(cause).when(graph).close();
        Map<String, Graph> graphs = Whitebox.getInternalState(manager, "graphs");
        graphs.put(graph.spaceGraphName(), graph);
        Map<String, HugeGraph> registered = Whitebox.getInternalState(HugeFactory.class, "GRAPHS");
        registered.put(graph.spaceGraphName(), graph);
        MetaManager meta = Mockito.mock(MetaManager.class);
        Whitebox.setInternalState(manager, "metaManager", meta);
        Map<String, GraphSpace> spaces = Whitebox.getInternalState(manager, "graphSpaces");
        GraphSpace space = Mockito.mock(GraphSpace.class);
        spaces.put("DEFAULT", space);
        try {
            HugeException failure = Assert.assertThrows(HugeException.class,
                                                        () -> manager.dropGraph("DEFAULT", "close_failure", false));
            Assert.assertSame(cause, failure.getCause());
            Assert.assertSame(graph, manager.graph(graph.spaceGraphName()));
            Assert.assertSame(graph, registered.get(graph.spaceGraphName()));
            Mockito.verifyNoInteractions(meta, space);
        } finally {
            graphs.clear();
            manager.close();
            HugeFactory.remove(graph);
        }
    }

    @Test
    public void testPdDropFencesSubmissionAndRetriesMetadataFailure() throws Exception {
        this.checkPdMetadataFailure(false);
    }

    @Test
    public void testPdDropRetriesNotificationWithoutRepeatingMetadataDeletion() throws Exception {
        this.checkPdMetadataFailure(true);
    }

    private void checkPdMetadataFailure(boolean notification) throws Exception {
        RegisterUtil.registerBackends();
        HugeConfig config = FakeObjects.newConfig();
        config.setProperty("backend", "memory");
        config.setProperty("serializer", "text");
        config.setProperty("store", "graph_drop_admission");
        Path file = Files.createTempFile("graph-drop-admission-", ".properties");
        Files.writeString(file, "retained graph config");
        config.file(file.toString());
        HugeGraph graph = HugeFactory.open(config);
        HugeConfig serverConfig = FakeObjects.newConfig();
        serverConfig.setProperty(ServerOptions.USE_PD.name(), false);
        serverConfig.setProperty(ServerOptions.GRAPH_LOAD_FROM_LOCAL_CONFIG.name(), false);
        GraphManager manager = new GraphManager(serverConfig, new EventHub("drop-admission"));
        Whitebox.setInternalState(manager, "PDExist", true);
        Map<String, Graph> graphs = Whitebox.getInternalState(manager, "graphs");
        graphs.put(graph.spaceGraphName(), graph);
        MetaManager meta = Mockito.mock(MetaManager.class);
        Whitebox.setInternalState(manager, "metaManager", meta);
        Map<String, GraphSpace> spaces = Whitebox.getInternalState(manager, "graphSpaces");
        spaces.put(graph.graphSpace(), Mockito.mock(GraphSpace.class));
        CountDownLatch reachedMetadata = new CountDownLatch(1);
        CountDownLatch releaseMetadata = new CountDownLatch(1);
        ExecutorService deleting = Executors.newSingleThreadExecutor();
        TaskScheduler scheduler = graph.taskScheduler();
        RuntimeException transportFailure = new IllegalStateException("metadata unavailable");
        try {
            graph.initBackend();
            graph.serverStarted(GlobalMasterInfo.master("drop-admission-test"));
            graph.schema().propertyKey("marker").asText().create();
            graph.schema().vertexLabel("person").properties("marker").useCustomizeStringId().create();
            graph.addVertex(T.id, "retained", T.label, "person", "marker", "keep");
            graph.tx().commit();
            if (graph.tx().isOpen()) {
                graph.tx().close();
            }
            org.mockito.stubbing.Answer<Object> unavailable = invocation -> {
                reachedMetadata.countDown();
                Assert.assertTrue(releaseMetadata.await(10L, TimeUnit.SECONDS));
                throw transportFailure;
            };
            if (notification) {
                Mockito.doAnswer(unavailable).when(meta).notifyGraphRemove(graph.graphSpace(), graph.name());
            } else {
                Mockito.doAnswer(unavailable).when(meta).removeGraphConfig(graph.graphSpace(), graph.name());
            }
            Future<?> first = deleting.submit(() -> {
                HugeException failure = Assert.assertThrows(HugeException.class,
                                    () -> manager.dropGraph(graph.graphSpace(), graph.name(), true));
                Assert.assertSame(transportFailure, failure.getCause());
            });
            Assert.assertTrue(reachedMetadata.await(10L, TimeUnit.SECONDS));
            AtomicBoolean ran = new AtomicBoolean();
            HugeTask<?> ephemeral = new HugeTask<>(IdGenerator.of(-9999901L), null, new EphemeralJob<Object>() {
                @Override
                public String type() {
                    return "test";
                }

                @Override
                public Object execute() {
                    ran.set(true);
                    return null;
                }
            });
            ephemeral.type("test");
            ephemeral.name("rejected-ephemeral");
            HugeTask<?> persistent = new HugeTask<>(IdGenerator.of(9999901L), null,
                                                    new PendingDropCallable());
            persistent.type("test");
            persistent.name("rejected-persistent");
            for (HugeTask<?> task : new HugeTask<?>[]{ephemeral, persistent}) {
                Assert.assertThrows(IllegalStateException.class, () -> scheduler.schedule(task));
                Assert.assertEquals(TaskStatus.NEW, task.status());
            }
            Assert.assertFalse(ran.get());
            Assert.assertEquals(0, scheduler.pendingTasks());
            releaseMetadata.countDown();
            first.get(10L, TimeUnit.SECONDS);
            Assert.assertFalse(graph.closed());
            Assert.assertSame(graph, manager.graph(graph.spaceGraphName()));
            Assert.assertTrue(Files.exists(file));
            Assert.assertEquals("keep", graph.vertices("retained").next().value("marker"));
            Assert.assertEquals("marker", graph.schema().getPropertyKey("marker").name());
            Assert.assertThrows(NotFoundException.class,
                                () -> scheduler.task(persistent.id()));
            if (notification) {
                Mockito.doNothing().when(meta).notifyGraphRemove(graph.graphSpace(), graph.name());
            } else {
                Mockito.doNothing().when(meta).removeGraphConfig(graph.graphSpace(), graph.name());
            }
            manager.dropGraph(graph.graphSpace(), graph.name(), true);
            Assert.assertTrue(graph.closed());
            Assert.assertNull(manager.graph(graph.spaceGraphName()));
            Mockito.verify(meta, Mockito.times(notification ? 1 : 2))
                   .removeGraphConfig(graph.graphSpace(), graph.name());
            Mockito.verify(meta, Mockito.times(notification ? 2 : 1))
                   .notifyGraphRemove(graph.graphSpace(), graph.name());
        } finally {
            releaseMetadata.countDown();
            deleting.shutdownNow();
            Assert.assertTrue(deleting.awaitTermination(10L, TimeUnit.SECONDS));
            scheduler.waitUntilAllTasksCompleted(10L);
            try {
                if (!graph.closed()) {
                    graph.close();
                }
            } finally {
                graphs.clear();
                manager.close();
                HugeFactory.remove(graph);
                Files.deleteIfExists(file);
            }
        }
    }

    @Test
    public void testPdNotificationDetachesGraphAlreadyClosedDespiteOwnerFailure() throws Exception {
        HugeConfig config = FakeObjects.newConfig();
        config.setProperty(ServerOptions.USE_PD.name(), false);
        config.setProperty(ServerOptions.GRAPH_LOAD_FROM_LOCAL_CONFIG.name(), false);
        GraphManager manager = new GraphManager(config, new EventHub("drop-released-owner"));
        Whitebox.setInternalState(manager, "PDExist", true);
        StandardHugeGraph graph = Mockito.mock(StandardHugeGraph.class);
        Mockito.when(graph.spaceGraphName()).thenReturn("DEFAULT-released_owner");
        Mockito.when(graph.closed()).thenReturn(true);
        Mockito.doThrow(new IllegalStateException("released owner failed")).when(graph).close();
        Map<String, Graph> graphs = Whitebox.getInternalState(manager, "graphs");
        graphs.put(graph.spaceGraphName(), graph);
        Map<String, HugeGraph> registered = Whitebox.getInternalState(HugeFactory.class, "GRAPHS");
        registered.put(graph.spaceGraphName(), graph);
        Whitebox.setInternalState(manager, "metaManager", Mockito.mock(MetaManager.class));
        Map<String, GraphSpace> spaces = Whitebox.getInternalState(manager, "graphSpaces");
        spaces.put("DEFAULT", Mockito.mock(GraphSpace.class));
        try {
            manager.dropGraph("DEFAULT", "released_owner", false);
            Assert.assertNull(manager.graph(graph.spaceGraphName()));
            Assert.assertNull(registered.get(graph.spaceGraphName()));
        } finally {
            graphs.clear();
            manager.close();
            HugeFactory.remove(graph);
        }
    }

    @Test
    public void testSpaceClearRetainsSharedMetadataWhileGraphIsPending() throws Exception {
        this.checkDrop(4);
    }

    @Test
    public void testDirectDropRetainsConfigWhenCloseIsIncomplete() throws Exception {
        this.checkIncompleteClose(0);
    }

    @Test
    public void testLocalDropRetainsRegistrationsWhenCloseIsIncomplete() throws Exception {
        this.checkIncompleteClose(1);
    }

    @Test
    public void testPdDropRetriesIncompleteCloseWithoutRepeatingMetadataDeletion() throws Exception {
        this.checkIncompleteClose(2);
    }

    @Test
    public void testNotificationRetriesIncompleteCloseAfterSchedulerRemoval() throws Exception {
        this.checkIncompleteClose(3);
    }

    @Test
    public void testSpaceRoleCannotRemoveMetadataBeforeAdminCloseCheck() throws Exception {
        RegisterUtil.registerBackends();
        HugeConfig config = FakeObjects.newConfig();
        config.setProperty("backend", "memory");
        config.setProperty("serializer", "text");
        config.setProperty("store", "graph_drop_auth");
        HugeGraph graph = HugeFactory.open(config);
        HugeConfig serverConfig = FakeObjects.newConfig();
        serverConfig.setProperty(ServerOptions.USE_PD.name(), false);
        serverConfig.setProperty(ServerOptions.GRAPH_LOAD_FROM_LOCAL_CONFIG.name(), false);
        GraphManager manager = new GraphManager(serverConfig, new EventHub("drop-auth"));
        Whitebox.setInternalState(manager, "PDExist", true);
        Map<String, Graph> graphs = Whitebox.getInternalState(manager, "graphs");
        MetaManager meta = Mockito.mock(MetaManager.class);
        Whitebox.setInternalState(manager, "metaManager", meta);
        try {
            graph.initBackend();
            graph.serverStarted(GlobalMasterInfo.master("drop-auth-test"));
            HugeGraph proxy = new HugeGraphAuthProxy(graph);
            graphs.put(graph.spaceGraphName(), proxy);
            HugeAuthenticator.User member = new HugeAuthenticator.User("space-member",
                       RolePermission.role(graph.graphSpace(), graph.name(), HugePermission.READ));
            Whitebox.invoke(HugeGraphAuthProxy.class, new Class<?>[]{HugeGraphAuthProxy.Context.class},
                            "setContext", null, new HugeGraphAuthProxy.Context(member));
            Assert.assertThrows(jakarta.ws.rs.ForbiddenException.class,
                                () -> manager.dropGraph(graph.graphSpace(), graph.name(), true));
            Mockito.verifyNoInteractions(meta);
            HugeGraphParams params = Whitebox.getInternalState(graph, "params");
            // Simulate the retry state: the underlying scheduler was drained/removed,
            // but the auth proxy still owns its cached guarded scheduler reference.
            HugeGraphAuthProxy.runAsAdmin(() -> TaskManager.instance().closeScheduler(params));
            Assert.assertNull(TaskManager.instance().getScheduler(params));
            Assert.assertThrows(jakarta.ws.rs.ForbiddenException.class,
                                () -> manager.dropGraph(graph.graphSpace(), graph.name(), true));
            Mockito.verifyNoInteractions(meta);
            Assert.assertSame(proxy, manager.graph(graph.spaceGraphName()));
        } finally {
            HugeGraphAuthProxy.resetContext();
            graphs.clear();
            try {
                HugeGraphAuthProxy.runAsAdmin(() -> {
                    try {
                        graph.close();
                    } catch (Exception e) {
                        throw new AssertionError(e);
                    }
                });
            } finally {
                manager.close();
                HugeFactory.remove(graph);
                HugeGraphAuthProxy.resetContext();
            }
        }
    }

    @Test
    public void testPdQuotaUpdateRetryDoesNotReclearClosedGraphOrRecycleTwice() throws Exception {
        RegisterUtil.registerBackends();
        HugeConfig config = FakeObjects.newConfig();
        config.setProperty("backend", "memory");
        config.setProperty("serializer", "text");
        config.setProperty("store", "graph_drop_quota_retry");
        HugeGraph graph = HugeFactory.open(config);
        HugeConfig serverConfig = FakeObjects.newConfig();
        serverConfig.setProperty(ServerOptions.USE_PD.name(), false);
        serverConfig.setProperty(ServerOptions.GRAPH_LOAD_FROM_LOCAL_CONFIG.name(), false);
        GraphManager manager = new GraphManager(serverConfig, new EventHub("drop-quota-retry"));
        Whitebox.setInternalState(manager, "PDExist", true);
        Map<String, Graph> graphs = Whitebox.getInternalState(manager, "graphs");
        graphs.put(graph.spaceGraphName(), graph);
        MetaManager meta = Mockito.mock(MetaManager.class);
        Whitebox.setInternalState(manager, "metaManager", meta);
        Map<String, GraphSpace> spaces = Whitebox.getInternalState(manager, "graphSpaces");
        GraphSpace space = new GraphSpace(graph.graphSpace());
        space.graphNumberUsed(1);
        spaces.put(graph.graphSpace(), space);
        RuntimeException failure = new IllegalStateException("quota transport unavailable");
        Mockito.doThrow(failure).doNothing().when(meta).updateGraphSpaceConfig(graph.graphSpace(), space);
        try {
            graph.initBackend();
            graph.serverStarted(GlobalMasterInfo.master("drop-quota-retry-test"));
            Assert.assertSame(failure, Assert.assertThrows(IllegalStateException.class,
                               () -> manager.dropGraph(graph.graphSpace(), graph.name(), true)));
            Assert.assertTrue(graph.closed());
            Assert.assertSame(graph, manager.graph(graph.spaceGraphName()));
            Assert.assertEquals(0, space.graphNumberUsed());
            manager.dropGraph(graph.graphSpace(), graph.name(), true);
            Assert.assertEquals(0, space.graphNumberUsed());
            Assert.assertNull(manager.graph(graph.spaceGraphName()));
            Mockito.verify(meta, Mockito.times(1)).removeGraphConfig(graph.graphSpace(), graph.name());
            Mockito.verify(meta, Mockito.times(2)).updateGraphSpaceConfig(graph.graphSpace(), space);
        } finally {
            graphs.clear();
            manager.close();
            HugeFactory.remove(graph);
        }
    }

    @Test
    public void testLocalConfigRemovalRetryDoesNotReclearClosedGraph() throws Exception {
        RegisterUtil.registerBackends();
        HugeConfig config = FakeObjects.newConfig();
        config.setProperty("backend", "memory");
        config.setProperty("serializer", "text");
        config.setProperty("store", "graph_drop_config_retry");
        Path file = Files.createTempFile("graph-drop-config-retry-", ".properties");
        Files.writeString(file, "config removal retry");
        config.file(file.toString());
        HugeGraph graph = HugeFactory.open(config);
        BackendStoreProvider provider = Mockito.spy(graph.storeProvider());
        Whitebox.setInternalState(graph, "storeProvider", provider);
        RuntimeException failure = new IllegalStateException("config removal unavailable");
        Mockito.doThrow(failure).doCallRealMethod().when(provider).onDeleteConfig(config);
        try {
            graph.initBackend();
            graph.serverStarted(GlobalMasterInfo.master("drop-config-retry-test"));
            Assert.assertSame(failure, Assert.assertThrows(IllegalStateException.class, graph::drop));
            Assert.assertTrue(graph.closed());
            Assert.assertTrue(Files.exists(file));
            graph.drop();
            Assert.assertFalse(Files.exists(file));
            Mockito.verify(provider, Mockito.times(1)).clear();
            Mockito.verify(provider, Mockito.times(1)).close();
            Mockito.verify(provider, Mockito.times(2)).onDeleteConfig(config);
        } finally {
            try {
                if (!graph.closed()) {
                    graph.close();
                }
            } finally {
                HugeFactory.remove(graph);
                Files.deleteIfExists(file);
            }
        }
    }

    @Test
    public void testFailedCreateRetainsRegistrationsUntilRollbackCanFinish() throws Exception {
        this.checkFailedCreateRollback(true);
    }

    @Test
    public void testFailedCreateDetachesRegistrationsAfterSuccessfulRollback() throws Exception {
        this.checkFailedCreateRollback(false);
    }

    private void checkFailedCreateRollback(boolean pending) throws Exception {
        RegisterUtil.registerBackends();
        Path directory = Files.createTempDirectory("graph-create-rollback-");
        HugeConfig serverConfig = FakeObjects.newConfig();
        serverConfig.setProperty(ServerOptions.USE_PD.name(), false);
        serverConfig.setProperty(ServerOptions.GRAPH_LOAD_FROM_LOCAL_CONFIG.name(), false);
        serverConfig.setProperty(ServerOptions.GRAPHS.name(), directory.toString());
        serverConfig.setProperty(ServerOptions.ENABLE_DYNAMIC_CREATE_DROP.name(), true);
        EventHub hub = new EventHub("create-rollback-" + pending);
        GraphManager manager = new GraphManager(serverConfig, hub);
        HugeConfig config = FakeObjects.newConfig();
        config.setProperty(Graph.GRAPH, HugeFactory.class.getName());
        config.setProperty("backend", "memory");
        config.setProperty("serializer", "text");
        config.setProperty("store", "create_rollback_" + pending);
        config.setProperty(CoreOptions.TASK_WAIT_TIMEOUT.name(), 0L);
        Map<String, Graph> graphs = Whitebox.getInternalState(manager, "graphs");
        Map<String, HugeGraph> registered = Whitebox.getInternalState(HugeFactory.class, "GRAPHS");
        AtomicReference<HugeGraph> created = new AtomicReference<>();
        AtomicReference<Future<?>> running = new AtomicReference<>();
        AtomicInteger dropped = new AtomicInteger();
        PendingDropCallable callable = new PendingDropCallable();
        hub.listen(Events.GRAPH_CREATE, event -> {
            HugeGraph graph = (HugeGraph) event.args()[0];
            created.set(graph);
            graphs.put(graph.spaceGraphName(), graph);
            if (pending) {
                graph.schema().vertexLabel("person").useCustomizeStringId().create();
                graph.addVertex(T.id, "retained", T.label, "person");
                graph.tx().commit();
                HugeTask<String> task = new HugeTask<>(IdGenerator.of(9999970L), null, callable);
                task.type("test");
                task.name("create-rollback-pending");
                running.set(graph.taskScheduler().schedule(task));
                try {
                    Assert.assertTrue(callable.started.await(10L, TimeUnit.SECONDS));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new HugeException("Interrupted before create rollback", e);
                }
            }
            throw new IllegalStateException("create listener failed");
        });
        hub.listen(Events.GRAPH_DROP, event -> {
            dropped.incrementAndGet();
            HugeGraph graph = (HugeGraph) event.args()[0];
            graphs.remove(graph.spaceGraphName(), graph);
            return null;
        });
        try {
            HugeException failure = Assert.assertThrows(HugeException.class, () ->
                    Whitebox.invoke(GraphManager.class, new Class<?>[]{HugeConfig.class, String.class},
                                    "createGraphLocal", manager, config, config.get(CoreOptions.STORE)));
            Assert.assertContains(Events.GRAPH_CREATE, failure.getMessage());
            HugeGraph graph = created.get();
            Assert.assertNotNull(graph);
            if (pending) {
                Assert.assertEquals(1, failure.getSuppressed().length);
                Assert.assertContains("please retry later", failure.getSuppressed()[0].getMessage());
                Assert.assertSame(graph, manager.graph(graph.spaceGraphName()));
                Assert.assertSame(graph, registered.get(graph.spaceGraphName()));
                Assert.assertFalse(graph.closed());
                Assert.assertEquals(0, dropped.get());
                callable.release.countDown();
                running.get().get(10L, TimeUnit.SECONDS);
                graph.taskScheduler().waitUntilAllTasksCompleted(10L);
                graphs.put("DEFAULT-other", Mockito.mock(HugeGraph.class));
                manager.dropGraphLocal(graph.name());
                Assert.assertTrue(graph.closed());
                Assert.assertNull(manager.graph(graph.spaceGraphName()));
                Assert.assertNull(registered.get(graph.spaceGraphName()));
                Assert.assertEquals(1, dropped.get());
            } else {
                Assert.assertEquals(0, failure.getSuppressed().length);
                Assert.assertTrue(graph.closed());
                Assert.assertNull(manager.graph(graph.spaceGraphName()));
                Assert.assertNull(registered.get(graph.spaceGraphName()));
                Assert.assertEquals(1, dropped.get());
            }
        } finally {
            callable.release.countDown();
            if (running.get() != null) {
                running.get().get(10L, TimeUnit.SECONDS);
            }
            HugeGraph graph = created.get();
            if (graph != null) {
                if (!graph.closed()) {
                    graph.close();
                }
                HugeFactory.remove(graph);
                Files.deleteIfExists(graph.configuration().file().toPath());
            }
            graphs.clear();
            manager.close();
            Files.deleteIfExists(directory);
        }
    }

    private void checkIncompleteClose(int route) throws Exception {
        RegisterUtil.registerBackends();
        HugeConfig config = FakeObjects.newConfig();
        config.setProperty("backend", "memory");
        config.setProperty("serializer", "text");
        config.setProperty("store", "graph_drop_owner_" + route);
        Path file = Files.createTempFile("graph-drop-owner-", ".properties");
        Files.writeString(file, "retained owner config");
        config.file(file.toString());
        HugeGraph graph = HugeFactory.open(config);
        BackendStoreProvider provider = Mockito.spy(graph.storeProvider());
        Whitebox.setInternalState(graph, "storeProvider", provider);
        HugeConfig serverConfig = FakeObjects.newConfig();
        serverConfig.setProperty(ServerOptions.USE_PD.name(), false);
        serverConfig.setProperty(ServerOptions.GRAPH_LOAD_FROM_LOCAL_CONFIG.name(), false);
        serverConfig.setProperty(ServerOptions.ENABLE_DYNAMIC_CREATE_DROP.name(), true);
        GraphManager manager = new GraphManager(serverConfig, new EventHub("drop-owner-" + route));
        Whitebox.setInternalState(manager, "PDExist", route >= 2);
        Map<String, Graph> graphs = Whitebox.getInternalState(manager, "graphs");
        graphs.put(graph.spaceGraphName(), graph);
        if (route == 1) {
            graphs.put("DEFAULT-other", Mockito.mock(HugeGraph.class));
        }
        MetaManager meta = Mockito.mock(MetaManager.class);
        Whitebox.setInternalState(manager, "metaManager", meta);
        Map<String, GraphSpace> spaces = Whitebox.getInternalState(manager, "graphSpaces");
        GraphSpace space = Mockito.mock(GraphSpace.class);
        spaces.put(graph.graphSpace(), space);
        ExecutorService owner = Executors.newSingleThreadExecutor();
        CountDownLatch held = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        Future<?> holding = null;
        try {
            graph.initBackend();
            graph.serverStarted(GlobalMasterInfo.master("drop-owner-test"));
            graph.schema().vertexLabel("person").useCustomizeStringId().create();
            graph.addVertex(org.apache.tinkerpop.gremlin.structure.T.id, "retained",
                            org.apache.tinkerpop.gremlin.structure.T.label, "person");
            graph.tx().commit();
            HugeGraphParams params = Whitebox.getInternalState(graph, "params");
            holding = owner.submit(() -> {
                try {
                    graph.vertices("retained").next();
                    held.countDown();
                    Assert.assertTrue(release.await(10L, TimeUnit.SECONDS));
                } finally {
                    if (graph.tx().isOpen()) {
                        graph.tx().close();
                    }
                }
                return null;
            });
            Assert.assertTrue(held.await(10L, TimeUnit.SECONDS));
            HugeException failure = Assert.assertThrows(HugeException.class, () -> drop(manager, graph, route));
            Assert.assertTrue(HugeException.rootCause(failure) instanceof IllegalStateException);
            Assert.assertFalse(graph.closed());
            Assert.assertTrue(Files.exists(file));
            Assert.assertEquals("retained owner config", Files.readString(file));
            Assert.assertSame(graph, manager.graph(graph.spaceGraphName()));
            Map<String, HugeGraph> registered = Whitebox.getInternalState(HugeFactory.class, "GRAPHS");
            Assert.assertSame(graph, registered.get(graph.spaceGraphName()));
            Assert.assertNull(TaskManager.instance().getScheduler(params));
            Mockito.verify(space, Mockito.never()).recycleGraph();
            if (route == 2 || route == 4) {
                Mockito.verify(meta).removeGraphConfig(graph.graphSpace(), graph.name());
                // A self-notification must not discard the initiator's retry marker.
                Mockito.when(meta.extractGraphsFromResponse("self"))
                       .thenReturn(java.util.Collections.singletonList(graph.spaceGraphName()));
                Whitebox.invoke(GraphManager.class, new Class<?>[]{Object.class},
                                "graphRemoveHandler", manager, "self");
            }
            release.countDown();
            holding.get(10L, TimeUnit.SECONDS);
            drop(manager, graph, route);
            Assert.assertTrue(graph.closed());
            if (route <= 2 || route == 4) {
                Mockito.verify(provider, Mockito.times(1)).clear();
            }
            Assert.assertEquals(route >= 2, Files.exists(file));
            if (route == 0) {
                HugeFactory.remove(graph);
            } else {
                Assert.assertNull(manager.graph(graph.spaceGraphName()));
                Assert.assertNull(registered.get(graph.spaceGraphName()));
            }
            if (route == 2) {
                Mockito.verify(meta, Mockito.times(1)).removeGraphConfig(graph.graphSpace(), graph.name());
                Mockito.verify(space, Mockito.times(1)).recycleGraph();
            }
        } finally {
            release.countDown();
            if (holding != null) {
                holding.get(10L, TimeUnit.SECONDS);
            }
            owner.shutdownNow();
            Assert.assertTrue(owner.awaitTermination(10L, TimeUnit.SECONDS));
            try {
                if (!graph.closed()) {
                    graph.close();
                }
            } finally {
                graphs.clear();
                manager.close();
                HugeFactory.remove(graph);
                Files.deleteIfExists(file);
            }
        }
    }

    private void checkDrop(int route) throws Exception {
        RegisterUtil.registerBackends();
        HugeConfig config = FakeObjects.newConfig();
        config.setProperty("backend", "memory");
        config.setProperty("serializer", "text");
        config.setProperty("store", "graph_drop_pending_" + route);
        config.setProperty(CoreOptions.TASK_WAIT_TIMEOUT.name(), 0L);
        Path file = Files.createTempFile("graph-drop-pending-", ".properties");
        Files.writeString(file, "pending graph config");
        config.file(file.toString());
        HugeGraph graph = HugeFactory.open(config);
        HugeConfig serverConfig = FakeObjects.newConfig();
        serverConfig.setProperty(ServerOptions.USE_PD.name(), false);
        serverConfig.setProperty(ServerOptions.GRAPH_LOAD_FROM_LOCAL_CONFIG.name(), false);
        serverConfig.setProperty(ServerOptions.ENABLE_DYNAMIC_CREATE_DROP.name(), true);
        GraphManager manager = new GraphManager(serverConfig, new EventHub("drop-pending-" + route));
        Whitebox.setInternalState(manager, "PDExist", route >= 2);
        Map<String, Graph> graphs = Whitebox.getInternalState(manager, "graphs");
        graphs.put(graph.spaceGraphName(), graph);
        if (route == 1) {
            graphs.put("DEFAULT-other", Mockito.mock(HugeGraph.class));
        }
        MetaManager meta = Mockito.mock(MetaManager.class);
        Whitebox.setInternalState(manager, "metaManager", meta);
        Map<String, GraphSpace> spaces = Whitebox.getInternalState(manager, "graphSpaces");
        GraphSpace space = Mockito.mock(GraphSpace.class);
        spaces.put(config.get(CoreOptions.GRAPH_SPACE), space);
        PendingDropCallable callable = new PendingDropCallable();
        HugeTask<String> task = new HugeTask<>(IdGenerator.of(9999900L + route), null, callable);
        task.type("test");
        task.name("pending-drop");
        Future<?> running = null;
        TaskScheduler scheduler = graph.taskScheduler();
        try {
            graph.initBackend();
            graph.serverStarted(GlobalMasterInfo.master("pending-drop-test"));
            graph.schema().propertyKey("marker").asText().create();
            graph.schema().vertexLabel("person").properties("marker")
                 .useCustomizeStringId().create();
            graph.addVertex(T.id, "retained", T.label, "person", "marker", "keep");
            graph.tx().commit();
            HugeGraphParams params = Whitebox.getInternalState(graph, "params");
            ThreadLocal<?> owners = Whitebox.getInternalState(graph.tx(), "transactions");
            running = scheduler.schedule(task);
            Assert.assertTrue(callable.started.await(10L, TimeUnit.SECONDS));

            HugeException failure = Assert.assertThrows(HugeException.class,
                                                        () -> drop(manager, graph, route));
            Assert.assertFalse(graph.closed());
            Assert.assertTrue(Files.exists(file));
            Assert.assertEquals("pending graph config", Files.readString(file));
            Assert.assertSame(graph, manager.graph(graph.spaceGraphName()));
            Map<String, HugeGraph> registered = Whitebox.getInternalState(HugeFactory.class, "GRAPHS");
            Assert.assertSame(graph, registered.get(graph.spaceGraphName()));
            Assert.assertSame(scheduler, TaskManager.instance().getScheduler(params));
            Assert.assertEquals("marker", graph.schema().getPropertyKey("marker").name());
            Assert.assertEquals("keep", graph.vertices("retained").next().value("marker"));
            Assert.assertEquals(1, scheduler.pendingTasks());
            Mockito.verifyNoInteractions(meta, space);
            Assert.assertContains("please retry later", failure.getMessage());

            HugeTask<Object> admitted = new HugeTask<>(IdGenerator.of(9999950L + route), null,
                                                       new EmptyCallable());
            admitted.type("test");
            admitted.name("rejected-after-pending-refusal");
            Assert.assertThrows(IllegalStateException.class, () -> scheduler.schedule(admitted));
            Assert.assertThrows(NotFoundException.class, () -> scheduler.task(admitted.id()));
            callable.release.countDown();
            running.get(10L, TimeUnit.SECONDS);
            scheduler.waitUntilAllTasksCompleted(10L);
            Assert.assertEquals(TaskStatus.SUCCESS, scheduler.task(task.id()).status());
            drop(manager, graph, route);
            Assert.assertTrue(graph.closed());
            Assert.assertNull(TaskManager.instance().getScheduler(params));
            Assert.assertNull(owners.get());
            Assert.assertNull(scheduler.call(owners::get));
            if (route == 0) {
                // Direct drop does not own the manager or factory registrations.
                HugeFactory.remove(graph);
            } else {
                Assert.assertNull(manager.graph(graph.spaceGraphName()));
            }
            Assert.assertFalse(registered.containsKey(graph.spaceGraphName()));
            Assert.assertEquals(route >= 2, Files.exists(file));
            if (route == 2 || route == 4) {
                Mockito.verify(meta).removeGraphConfig(graph.graphSpace(), graph.name());
                Mockito.verify(meta).notifyGraphRemove(graph.graphSpace(), graph.name());
            } else {
                Mockito.verify(meta, Mockito.never()).removeGraphConfig(Mockito.anyString(),
                                                                       Mockito.anyString());
            }
        } finally {
            callable.release.countDown();
            try {
                if (running != null) {
                    running.get(10L, TimeUnit.SECONDS);
                    scheduler.waitUntilAllTasksCompleted(10L);
                }
                if (!graph.closed()) {
                    graph.close();
                }
            } finally {
                graphs.clear();
                manager.close();
                HugeFactory.remove(graph);
                Files.deleteIfExists(file);
            }
        }
    }

    private static void drop(GraphManager manager, HugeGraph graph, int route) {
        if (route == 0) {
            graph.drop();
        } else if (route == 1) {
            manager.dropGraphLocal(graph.name());
        } else if (route == 4) {
            manager.clearGraphSpace(graph.graphSpace());
        } else {
            manager.dropGraph(graph.graphSpace(), graph.name(), route == 2);
        }
    }

    public static class PendingDropCallable extends TaskCallable<String> {

        private final CountDownLatch started = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);

        @Override
        protected void done() {
            this.save();
            super.done();
        }

        @Override
        public String call() throws Exception {
            // Open a real graph owner on the task worker before deletion starts.
            this.graph().vertices("retained").next();
            this.started.countDown();
            Assert.assertTrue(this.release.await(10L, TimeUnit.SECONDS));
            return "finished";
        }
    }
}
