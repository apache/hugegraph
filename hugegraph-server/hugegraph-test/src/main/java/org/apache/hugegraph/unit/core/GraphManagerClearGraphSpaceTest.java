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

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.api.space.GraphSpaceAPI;
import org.apache.hugegraph.config.CoreOptions;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.core.GraphManager;
import org.apache.hugegraph.event.EventHub;
import org.apache.hugegraph.event.EventHub.NotifyResult;
import org.apache.hugegraph.exception.HugeException;
import org.apache.hugegraph.meta.MetaDriver;
import org.apache.hugegraph.meta.MetaManager;
import org.apache.hugegraph.meta.managers.SpaceMetaManager;
import org.apache.hugegraph.pd.client.PDClient;
import org.apache.hugegraph.pd.client.PDConfig;
import org.apache.hugegraph.space.GraphSpace;
import org.apache.hugegraph.space.Service;
import org.apache.hugegraph.task.TaskScheduler;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.testutil.Whitebox;
import org.apache.hugegraph.util.JsonUtil;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import com.fasterxml.jackson.databind.ObjectMapper;

public class GraphManagerClearGraphSpaceTest {

    @Test
    public void testStartupLoadsAndRewritesRecordFromOlderServer() throws IOException {
        GraphSpace current = new GraphSpace("DEFAULT");
        current.setStorageUsedKb(512L * 1024L);
        AtomicReference<String> persisted = new AtomicReference<>(
                LegacyGraphSpaceCodec.rewrite(JsonUtil.toJson(current)));
        String key = "HUGEGRAPH/hg/GRAPHSPACE/CONF/DEFAULT";
        MetaDriver driver = Mockito.mock(MetaDriver.class);
        Mockito.when(driver.scanWithPrefix("HUGEGRAPH/hg/GRAPHSPACE/CONF"))
               .thenAnswer(invocation -> Map.of(key, persisted.get()));
        Mockito.when(driver.get(key)).thenAnswer(invocation -> persisted.get());
        Mockito.doAnswer(invocation -> {
            persisted.set(invocation.getArgument(1));
            return null;
        }).when(driver).put(Mockito.eq(key), Mockito.anyString());
        SpaceMetaManager spaces = new SpaceMetaManager(driver, "hg");
        MetaManager meta = Mockito.mock(MetaManager.class, Mockito.CALLS_REAL_METHODS);
        Whitebox.setInternalState(meta, "spaceMetaManager", spaces);
        HugeConfig config = new HugeConfig(new PropertiesConfiguration());
        GraphManager manager = new GraphManager(config, Mockito.mock(EventHub.class));
        try {
            Whitebox.setInternalState(manager, "metaManager", meta);
            Whitebox.setInternalState(manager, "PDExist", true);
            // Exercise both startup readers, with the real metadata decoder.
            Whitebox.invoke(GraphManager.class, "createDefaultGraphSpaceIfNeeded", manager, config);
            Whitebox.invoke(GraphManager.class, "loadGraphSpaces", manager);
            GraphSpace loaded = manager.graphSpace("DEFAULT");
            Assert.assertEquals(512L * 1024L, loaded.info().get("storage_used_kb"));
            meta.updateGraphSpaceConfig("DEFAULT", loaded);
            Assert.assertTrue(JsonUtil.fromJson(persisted.get(), Map.class)
                                      .get("storage_used_kb") instanceof Number);
            Assert.assertEquals(512L * 1024L, meta.graphSpace("DEFAULT").info().get("storage_used_kb"));
            Mockito.verify(driver, Mockito.times(2)).scanWithPrefix("HUGEGRAPH/hg/GRAPHSPACE/CONF");
            Mockito.verify(meta, Mockito.never()).addGraphSpaceConfig(Mockito.anyString(), Mockito.any());
        } finally {
            manager.close();
        }
    }

    /**
     * Graph and service keys are "{graphspace}-{name}", and a graph space
     * name may be a prefix of another one, so clearing "gs" must leave the
     * graphs and services of "gs2" and "gs_x" alone
     */
    @Test
    public void testClearKeepsGraphsOfSpacesNamedLikeIt() {
        HugeGraph cleared = mockGraph("gs-g");
        HugeGraph kept = mockGraph("gs2-g");
        HugeGraph keptUnderscore = mockGraph("gs_x-g");
        MetaManager meta = Mockito.mock(MetaManager.class);

        // A mock hub: the executor of EventHub is static, and a test that ran
        // earlier in the suite may have shut it down (HugeFactory.shutdown)
        GraphManager manager = new GraphManager(
                new HugeConfig(new PropertiesConfiguration()),
                Mockito.mock(EventHub.class));
        try {
            Whitebox.setInternalState(manager, "PDExist", true);
            Whitebox.setInternalState(manager, "metaManager", meta);
            Whitebox.setInternalState(manager, "localGraphs",
                                      Collections.emptySet());
            Map<String, GraphSpace> spaces =
                    Whitebox.getInternalState(manager, "graphSpaces");
            for (String space : new String[]{"gs", "gs2", "gs_x"}) {
                spaces.put(space, new GraphSpace(space));
            }
            Map<String, Graph> graphs =
                    Whitebox.getInternalState(manager, "graphs");
            graphs.put("gs-g", cleared);
            graphs.put("gs2-g", kept);
            graphs.put("gs_x-g", keptUnderscore);
            Map<String, Service> services =
                    Whitebox.getInternalState(manager, "services");
            services.put("gs-s", Mockito.mock(Service.class));
            services.put("gs2-s", Mockito.mock(Service.class));
            services.put("gs_x-s", Mockito.mock(Service.class));

            manager.clearGraphSpace("gs");

            Mockito.verify(cleared).clearBackend();
            Mockito.verify(kept, Mockito.never()).clearBackend();
            Mockito.verify(keptUnderscore, Mockito.never()).clearBackend();
            Assert.assertFalse(graphs.containsKey("gs-g"));
            Assert.assertTrue(graphs.containsKey("gs2-g"));
            Assert.assertTrue(graphs.containsKey("gs_x-g"));

            Mockito.verify(meta).service("gs", "s");
            Mockito.verify(meta, Mockito.never()).service("gs2", "s");
            Mockito.verify(meta, Mockito.never()).service("gs_x", "s");
        } finally {
            manager.close();
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testRemoveEventEvictsOnlyDeletedSpaceOnEveryReplica() {
        MetaManager meta = Mockito.mock(MetaManager.class);
        Object response = new Object();
        Mockito.when(meta.extractGraphSpacesFromResponse(response)).thenReturn(List.of("gs"));
        for (int replica = 0; replica < 2; replica++) {
            GraphManager manager = new GraphManager(
                    new HugeConfig(new PropertiesConfiguration()), Mockito.mock(EventHub.class));
            try {
                Whitebox.setInternalState(manager, "metaManager", meta);
                Map<String, GraphSpace> spaces = Whitebox.getInternalState(manager, "graphSpaces");
                spaces.put("gs", new GraphSpace("gs"));
                spaces.put("gs2", new GraphSpace("gs2"));
                Map<String, Service> services = Whitebox.getInternalState(manager, "services");
                services.put("gs-s", Mockito.mock(Service.class));
                services.put("gs2-s", Mockito.mock(Service.class));
                Whitebox.invoke(GraphManager.class, "listenMetaChanges", manager);
                ArgumentCaptor<Consumer> listener = ArgumentCaptor.forClass(Consumer.class);
                Mockito.verify(meta, Mockito.atLeastOnce()).listenGraphSpaceRemove(listener.capture());
                listener.getValue().accept(response);
                listener.getValue().accept(response);
                Assert.assertFalse(manager.graphSpaces().contains("gs"));
                Assert.assertTrue(manager.graphSpaces().contains("gs2"));
                Assert.assertFalse(services.containsKey("gs-s"));
                Assert.assertTrue(services.containsKey("gs2-s"));
            } finally {
                manager.close();
            }
        }
        Mockito.verify(meta, Mockito.never()).removeGraphSpaceConfig(Mockito.anyString());
        Mockito.verify(meta, Mockito.never()).notifyGraphSpaceRemove(Mockito.anyString());
    }

    @Test
    public void testDelayedRemoveKeepsRecreatedSpace() {
        MetaManager meta = Mockito.mock(MetaManager.class);
        Object response = new Object();
        Mockito.when(meta.extractGraphSpacesFromResponse(response)).thenReturn(List.of("gs"));
        GraphSpace recreated = new GraphSpace("gs");
        recreated.storageLimit = 200;
        Mockito.when(meta.graphSpace("gs")).thenReturn(recreated);
        GraphManager manager = new GraphManager(
                new HugeConfig(new PropertiesConfiguration()), Mockito.mock(EventHub.class));
        try {
            Whitebox.setInternalState(manager, "metaManager", meta);
            Whitebox.setInternalState(manager, "PDExist", true);
            Map<String, GraphSpace> spaces = Whitebox.getInternalState(manager, "graphSpaces");
            GraphSpace deleted = new GraphSpace("gs");
            deleted.storageLimit = 50;
            spaces.put("gs", deleted);
            Map<String, Service> services = Whitebox.getInternalState(manager, "services");
            services.put("gs-s", Mockito.mock(Service.class));
            Whitebox.invoke(GraphManager.class, "graphSpaceRemoveHandler", manager, response);
            Assert.assertSame(recreated, manager.graphSpace("gs"));
            Assert.assertEquals(200, manager.graphSpace("gs").storageLimit());
            Assert.assertFalse(services.containsKey("gs-s"));
        } finally {
            manager.close();
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testAddAndUpdateRefreshQuotasOnEveryReplica() {
        MetaManager meta = Mockito.mock(MetaManager.class);
        Object response = new Object();
        Mockito.when(meta.extractGraphSpacesFromResponse(response)).thenReturn(List.of("gs"));
        AtomicReference<GraphSpace> metadata = new AtomicReference<>();
        Mockito.when(meta.graphSpace("gs")).thenAnswer(call -> metadata.get() == null ? null :
                JsonUtil.fromJson(JsonUtil.toJson(metadata.get()), GraphSpace.class));
        GraphManager first = new GraphManager(
                new HugeConfig(new PropertiesConfiguration()), Mockito.mock(EventHub.class));
        GraphManager second = new GraphManager(
                new HugeConfig(new PropertiesConfiguration()), Mockito.mock(EventHub.class));
        try {
            for (GraphManager manager : List.of(first, second)) {
                Whitebox.setInternalState(manager, "metaManager", meta);
                Whitebox.setInternalState(manager, "PDExist", true);
                Map<String, GraphSpace> spaces = Whitebox.getInternalState(manager, "graphSpaces");
                GraphSpace cached = new GraphSpace("gs");
                cached.storageLimit = 917;
                spaces.put("gs", cached);
                spaces.put("other", new GraphSpace("other"));
                Map<String, Service> services = Whitebox.getInternalState(manager, "services");
                services.put("gs-service", Mockito.mock(Service.class));
                Whitebox.invoke(GraphManager.class, "listenMetaChanges", manager);
            }
            ArgumentCaptor<Consumer> added = ArgumentCaptor.forClass(Consumer.class);
            ArgumentCaptor<Consumer> updated = ArgumentCaptor.forClass(Consumer.class);
            Mockito.verify(meta, Mockito.times(2)).listenGraphSpaceAdd(added.capture());
            Mockito.verify(meta, Mockito.times(2)).listenGraphSpaceUpdate(updated.capture());
            GraphSpace current = new GraphSpace("gs");
            current.storageLimit = 10;
            metadata.set(current);
            // The REST update path also emits ADD, so both event channels must refresh existing caches.
            for (Consumer listener : added.getAllValues()) {
                listener.accept(response);
            }
            Assert.assertEquals(10, first.graphSpace("gs").storageLimit());
            Assert.assertEquals(10, second.graphSpace("gs").storageLimit());
            current.storageLimit = 100;
            for (Consumer listener : updated.getAllValues()) {
                listener.accept(response);
            }
            // An older ADD delivered after UPDATE must reread current metadata, not restore its old value.
            for (Consumer listener : added.getAllValues()) {
                listener.accept(response);
            }
            for (GraphManager manager : List.of(first, second)) {
                Assert.assertEquals(100, manager.graphSpace("gs").storageLimit());
                Assert.assertTrue(manager.graphSpaces().contains("other"));
                Map<String, Service> services = Whitebox.getInternalState(manager, "services");
                Assert.assertTrue(services.containsKey("gs-service"));
            }
            metadata.set(null);
            for (Consumer listener : updated.getAllValues()) {
                listener.accept(response);
            }
            Assert.assertFalse(first.graphSpaces().contains("gs"));
            Assert.assertFalse(second.graphSpaces().contains("gs"));
            Mockito.verify(meta, Mockito.never()).addGraphSpaceConfig(Mockito.anyString(),
                                                                     Mockito.any(GraphSpace.class));
            Mockito.verify(meta, Mockito.never()).notifyGraphSpaceAdd(Mockito.anyString());
            Mockito.verify(meta, Mockito.never()).notifyGraphSpaceUpdate(Mockito.anyString());
        } finally {
            first.close();
            second.close();
        }
    }

    @Test
    public void testDelayedRemoveCannotOverwriteInPlaceApiUpdate() throws Exception {
        MetaManager meta = Mockito.mock(MetaManager.class);
        GraphManager manager = new GraphManager(
                new HugeConfig(new PropertiesConfiguration()), Mockito.mock(EventHub.class));
        Whitebox.setInternalState(manager, "metaManager", meta);
        Whitebox.setInternalState(manager, "PDExist", true);
        GraphSpace cached = new GraphSpace("gs");
        cached.maxGraphNumber(100);
        cached.graphNumberUsed(1);
        Map<String, GraphSpace> spaces = Whitebox.getInternalState(manager, "graphSpaces");
        spaces.put("gs", cached);
        AtomicReference<GraphSpace> metadata = new AtomicReference<>(
                JsonUtil.fromJson(JsonUtil.toJson(cached), GraphSpace.class));
        CountDownLatch snapshotRead = new CountDownLatch(1);
        CountDownLatch releaseSnapshot = new CountDownLatch(1);
        AtomicInteger reads = new AtomicInteger();
        Mockito.when(meta.extractGraphSpacesFromResponse(Mockito.any())).thenReturn(List.of("gs"));
        Mockito.when(meta.graphSpace("gs")).thenAnswer(call -> {
            GraphSpace snapshot = metadata.get();
            if (reads.incrementAndGet() == 1) {
                snapshotRead.countDown();
                Assert.assertTrue(releaseSnapshot.await(5, TimeUnit.SECONDS));
            }
            return snapshot;
        });
        Mockito.doAnswer(call -> {
            metadata.set(JsonUtil.fromJson(JsonUtil.toJson(call.getArgument(1)), GraphSpace.class));
            return null;
        }).when(meta).addGraphSpaceConfig(Mockito.eq("gs"), Mockito.any(GraphSpace.class));
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try (MockedStatic<PDClient> clients = Mockito.mockStatic(PDClient.class)) {
            clients.when(() -> PDClient.create(Mockito.any(PDConfig.class))).thenReturn(Mockito.mock(PDClient.class));
            Future<?> removal = executor.submit(() ->
                    Whitebox.invoke(GraphManager.class, "graphSpaceRemoveHandler", manager, new Object()));
            Assert.assertTrue(snapshotRead.await(5, TimeUnit.SECONDS));
            updateApiGraphSpace(manager, cached, Map.of("max_graph_number", 1, "storage_limit", 200));
            Assert.assertSame(cached, manager.graphSpace("gs"));
            Assert.assertEquals(1, metadata.get().maxGraphNumber());
            releaseSnapshot.countDown();
            removal.get(5, TimeUnit.SECONDS);
            GraphSpace current = manager.graphSpace("gs");
            Assert.assertEquals(1, current.maxGraphNumber());
            Assert.assertEquals(200, current.storageLimit());
            Assert.assertEquals(1, current.graphNumberUsed());
            Assert.assertFalse(current.tryOfferGraph());
            Mockito.when(meta.graphConfigs("gs")).thenReturn(Map.of("gs-existing", Map.of()));
            Assert.assertThrows(HugeException.class, () -> manager.createGraph(
                    "gs", "over_limit", "admin", new HashMap<>(), true),
                    failure -> Assert.assertTrue(failure.getMessage().contains("reach graph limit")));
            Mockito.verify(meta, Mockito.times(2)).graphSpace("gs");
        } finally {
            releaseSnapshot.countDown();
            executor.shutdownNow();
            manager.close();
        }
    }

    @Test
    public void testDelayedRemoveCannotUndoCreatedGraphCount() throws Exception {
        MetaManager meta = Mockito.mock(MetaManager.class);
        EventHub hub = Mockito.mock(EventHub.class);
        NotifyResult notified = Mockito.mock(NotifyResult.class);
        Mockito.when(notified.success()).thenReturn(true);
        Mockito.when(hub.notifySync(Mockito.anyString(), Mockito.any(HugeGraph.class))).thenReturn(notified);
        GraphManager manager = new GraphManager(new HugeConfig(new PropertiesConfiguration()), hub);
        Whitebox.setInternalState(manager, "metaManager", meta);
        Whitebox.setInternalState(manager, "PDExist", true);
        GraphSpace cached = new GraphSpace("gs");
        Map<String, GraphSpace> spaces = Whitebox.getInternalState(manager, "graphSpaces");
        spaces.put("gs", cached);
        AtomicReference<GraphSpace> metadata = new AtomicReference<>(
                JsonUtil.fromJson(JsonUtil.toJson(cached), GraphSpace.class));
        CountDownLatch snapshotRead = new CountDownLatch(1);
        CountDownLatch releaseSnapshot = new CountDownLatch(1);
        AtomicInteger reads = new AtomicInteger();
        Mockito.when(meta.extractGraphSpacesFromResponse(Mockito.any())).thenReturn(List.of("gs"));
        Mockito.when(meta.graphSpace("gs")).thenAnswer(call -> {
            GraphSpace snapshot = metadata.get();
            if (reads.incrementAndGet() == 1) {
                snapshotRead.countDown();
                Assert.assertTrue(releaseSnapshot.await(5, TimeUnit.SECONDS));
            }
            return snapshot;
        });
        Mockito.doAnswer(call -> {
            metadata.set(JsonUtil.fromJson(JsonUtil.toJson(call.getArgument(1)), GraphSpace.class));
            return null;
        }).when(meta).updateGraphSpaceConfig(Mockito.eq("gs"), Mockito.any(GraphSpace.class));
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try (MockedStatic<GraphFactory> factory = Mockito.mockStatic(GraphFactory.class)) {
            HugeGraph graph = mockGraph("gs-created");
            factory.when(() -> GraphFactory.open(Mockito.any(Configuration.class))).thenReturn(graph);
            Future<?> removal = executor.submit(() ->
                    Whitebox.invoke(GraphManager.class, "graphSpaceRemoveHandler", manager, new Object()));
            Assert.assertTrue(snapshotRead.await(5, TimeUnit.SECONDS));
            manager.createGraph("gs", "created", "admin",
                                new HashMap<>(Map.of(CoreOptions.STORE.name(), "created")), true);
            Assert.assertSame(cached, manager.graphSpace("gs"));
            Assert.assertEquals(1, metadata.get().graphNumberUsed());
            releaseSnapshot.countDown();
            removal.get(5, TimeUnit.SECONDS);
            Assert.assertEquals(1, manager.graphSpace("gs").graphNumberUsed());
            Mockito.verify(meta, Mockito.times(3)).graphSpace("gs");
        } finally {
            releaseSnapshot.countDown();
            executor.shutdownNow();
            manager.close();
        }
    }

    @Test
    public void testCacheMissRespectsConcurrentCreationAndDeletion() throws Exception {
        for (boolean deletion : new boolean[]{false, true}) {
            MetaManager meta = Mockito.mock(MetaManager.class);
            GraphManager manager = new GraphManager(
                    new HugeConfig(new PropertiesConfiguration()), Mockito.mock(EventHub.class));
            Whitebox.setInternalState(manager, "metaManager", meta);
            Whitebox.setInternalState(manager, "PDExist", true);
            AtomicReference<GraphSpace> metadata = new AtomicReference<>(new GraphSpace("gs"));
            CountDownLatch snapshotRead = new CountDownLatch(1);
            CountDownLatch releaseSnapshot = new CountDownLatch(1);
            Mockito.when(meta.graphSpace("gs")).thenAnswer(call -> {
                GraphSpace snapshot = metadata.get();
                snapshotRead.countDown();
                Assert.assertTrue(releaseSnapshot.await(5, TimeUnit.SECONDS));
                return snapshot;
            });
            Mockito.doAnswer(call -> {
                metadata.set(null);
                return null;
            }).when(meta).removeGraphSpaceConfig("gs");
            ExecutorService executor = Executors.newSingleThreadExecutor();
            try (MockedStatic<PDClient> clients = Mockito.mockStatic(PDClient.class)) {
                clients.when(() -> PDClient.create(Mockito.any(PDConfig.class)))
                       .thenReturn(Mockito.mock(PDClient.class));
                Future<GraphSpace> lookup = executor.submit(() -> manager.graphSpace("gs"));
                Assert.assertTrue(snapshotRead.await(5, TimeUnit.SECONDS));
                GraphSpace updated = new GraphSpace("gs");
                updated.maxGraphNumber(1);
                if (deletion) {
                    new GraphSpaceAPI().delete(manager, "gs");
                } else {
                    manager.createGraphSpace(updated);
                }
                releaseSnapshot.countDown();
                Assert.assertSame(deletion ? null : updated, lookup.get(5, TimeUnit.SECONDS));
                Assert.assertEquals(!deletion, manager.graphSpaces().contains("gs"));
            } finally {
                releaseSnapshot.countDown();
                executor.shutdownNow();
                manager.close();
            }
        }
    }

    @Test
    public void testDefaultSpaceUsesFiniteDefaultsWithoutMigratingExistingSpace() {
        GraphManager manager = Mockito.mock(GraphManager.class, Mockito.CALLS_REAL_METHODS);
        MetaManager meta = Mockito.mock(MetaManager.class);
        Whitebox.setInternalState(manager, "metaManager", meta);
        Mockito.when(meta.graphSpaceConfigs()).thenReturn(Collections.emptyMap());
        Mockito.doAnswer(invocation -> invocation.getArgument(0))
               .when(manager).createGraphSpace(Mockito.any(GraphSpace.class));
        Whitebox.invoke(GraphManager.class, "createDefaultGraphSpaceIfNeeded", manager,
                        new HugeConfig(new PropertiesConfiguration()));
        ArgumentCaptor<GraphSpace> created = ArgumentCaptor.forClass(GraphSpace.class);
        Mockito.verify(manager).createGraphSpace(created.capture());
        GraphSpace space = created.getValue();
        Assert.assertEquals(128, space.cpuLimit());
        Assert.assertEquals(128, space.memoryLimit());
        Assert.assertEquals(128, space.computeCpuLimit());
        Assert.assertEquals(128, space.computeMemoryLimit());
        Assert.assertEquals(10240, space.storageLimit());
        Assert.assertEquals(100, space.maxGraphNumber());
        Assert.assertEquals(100, space.maxRoleNumber());
        Mockito.when(meta.graphSpaceConfigs()).thenReturn(Map.of("DEFAULT", space));
        Whitebox.invoke(GraphManager.class, "createDefaultGraphSpaceIfNeeded", manager,
                        new HugeConfig(new PropertiesConfiguration()));
        Mockito.verify(manager, Mockito.times(1)).createGraphSpace(Mockito.any(GraphSpace.class));
    }

    @Test
    public void testApiComputeDefaultsInheritRequestedResources() throws Exception {
        for (int cpu : new int[]{4, 128, 256}) {
            GraphSpace space = apiGraphSpace(String.format(
                    "{\"name\":\"gs\",\"cpu_limit\":%d,\"memory_limit\":%d}", cpu, cpu * 2));
            Assert.assertEquals(cpu, space.cpuLimit());
            Assert.assertEquals(cpu * 2, space.memoryLimit());
            Assert.assertEquals(0, space.computeCpuLimit());
            Assert.assertEquals(0, space.computeMemoryLimit());
        }
        GraphSpace defaults = apiGraphSpace("{\"name\":\"gs\"}");
        Assert.assertEquals(128, defaults.cpuLimit());
        Assert.assertEquals(128, defaults.memoryLimit());
        Assert.assertEquals(0, defaults.computeCpuLimit());
        Assert.assertEquals(0, defaults.computeMemoryLimit());
        Assert.assertEquals(10240, defaults.storageLimit());
    }

    @Test
    public void testApiComputeInheritanceSurvivesCpuAndMemoryUpdates() throws Exception {
        GraphSpace space = apiGraphSpace("{\"name\":\"gs\",\"cpu_limit\":4,\"memory_limit\":8}");
        Map<String, Object> result = updateApiGraphSpace(space,
                Map.of("cpu_limit", 8, "memory_limit", 16));
        Assert.assertEquals(8, result.get("cpu_limit"));
        Assert.assertEquals(16, result.get("memory_limit"));
        Assert.assertEquals(0, result.get("compute_cpu_limit"));
        Assert.assertEquals(0, result.get("compute_memory_limit"));
    }

    @Test
    public void testApiComputeUpdatesDistinguishOmissionFromExplicitZero() throws Exception {
        GraphSpace space = apiGraphSpace(
                "{\"name\":\"gs\",\"compute_cpu_limit\":16,\"compute_memory_limit\":32}");
        updateApiGraphSpace(space, Map.of("cpu_limit", 8, "memory_limit", 16));
        Assert.assertEquals(16, space.computeCpuLimit());
        Assert.assertEquals(32, space.computeMemoryLimit());
        updateApiGraphSpace(space, Map.of("compute_cpu_limit", 0, "compute_memory_limit", 0));
        Assert.assertEquals(0, space.computeCpuLimit());
        Assert.assertEquals(0, space.computeMemoryLimit());
        updateApiGraphSpace(space, Map.of("compute_cpu_limit", 5));
        Assert.assertEquals(5, space.computeCpuLimit());
        Assert.assertEquals(0, space.computeMemoryLimit());
        updateApiGraphSpace(space, Map.of("cpu_limit", 16, "memory_limit", 32));
        Assert.assertEquals(5, space.computeCpuLimit());
        Assert.assertEquals(0, space.computeMemoryLimit());
    }

    private static GraphSpace apiGraphSpace(String json) throws Exception {
        Class<?> type = Class.forName("org.apache.hugegraph.api.space.GraphSpaceAPI$JsonGraphSpace");
        Object request = new ObjectMapper().readValue(json, type);
        Whitebox.invoke(type, "checkCreate", request, false);
        return Whitebox.invoke(type, "toGraphSpace", request, "admin");
    }

    private static Map<String, Object> updateApiGraphSpace(GraphSpace space, Map<String, Object> changes) {
        GraphManager manager = Mockito.mock(GraphManager.class);
        Mockito.when(manager.isPDEnabled()).thenReturn(true);
        Mockito.when(manager.graphSpace(space.name())).thenReturn(space);
        Mockito.when(manager.createGraphSpace(Mockito.any(GraphSpace.class)))
               .thenAnswer(call -> call.getArgument(0));
        return updateApiGraphSpace(manager, space, changes);
    }

    private static Map<String, Object> updateApiGraphSpace(GraphManager manager, GraphSpace space,
                                                        Map<String, Object> changes) {
        Map<String, Object> update = new HashMap<>(Map.of(
                "name", space.name(), "max_graph_number", space.maxGraphNumber(),
                "max_role_number", space.maxRoleNumber(), "cpu_limit", space.cpuLimit(),
                "memory_limit", space.memoryLimit(), "storage_limit", space.storageLimit()));
        update.putAll(changes);
        return new GraphSpaceAPI().manage(manager, space.name(), Map.of("action", "update", "update", update));
    }

    private static HugeGraph mockGraph(String spaceGraphName) {
        HugeGraph graph = Mockito.mock(HugeGraph.class);
        // No tasks to drain: the drop closes task admission and goes on
        TaskScheduler scheduler = Mockito.mock(TaskScheduler.class);
        Mockito.when(scheduler.close()).thenReturn(true);
        Mockito.when(graph.taskScheduler()).thenReturn(scheduler);
        Mockito.when(graph.spaceGraphName()).thenReturn(spaceGraphName);
        return graph;
    }
}
