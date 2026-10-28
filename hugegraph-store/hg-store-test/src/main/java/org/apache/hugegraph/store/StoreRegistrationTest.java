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

package org.apache.hugegraph.store;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.hugegraph.pd.client.PDClient;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.store.meta.Partition;
import org.apache.hugegraph.store.meta.PartitionManager;
import org.apache.hugegraph.store.meta.Store;
import org.apache.hugegraph.store.meta.StoreMetadata;
import org.apache.hugegraph.store.options.HgStoreEngineOptions;
import org.apache.hugegraph.store.pd.DefaultPdProvider;
import org.apache.hugegraph.store.pd.PdProvider;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class StoreRegistrationTest {

    @Test
    public void testRestoreCompletesBeforeUpRegistration() throws Exception {
        List<String> events = new ArrayList<>();
        HgStoreEngine engine = Mockito.mock(HgStoreEngine.class);
        PdProvider provider = Mockito.mock(PdProvider.class);
        Mockito.when(provider.registerStore(Mockito.any())).thenAnswer(call -> {
            Store store = call.getArgument(0);
            events.add(store.getState().name());
            return 7L;
        });
        Mockito.doAnswer(call -> {
            events.add("restore");
            return null;
        }).when(engine).stateChanged(Mockito.any(), Mockito.any(), Mockito.eq(Metapb.StoreState.Up));
        HeartbeatService service = service(engine, provider);
        try {
            registerRestored(service);
            Assert.assertEquals(List.of("Offline", "restore", "Up"), events);
        } finally {
            service.shutdown();
        }
    }

    @Test
    public void testFailedRestoreNeverRegistersUp() throws Exception {
        HgStoreEngine engine = Mockito.mock(HgStoreEngine.class);
        PdProvider provider = Mockito.mock(PdProvider.class);
        Mockito.when(provider.registerStore(Mockito.any())).thenReturn(7L);
        Mockito.doThrow(new IllegalStateException("restore failed")).when(engine)
               .stateChanged(Mockito.any(), Mockito.any(), Mockito.eq(Metapb.StoreState.Up));
        HeartbeatService service = service(engine, provider);
        try {
            service.registerStore();
            Assert.assertThrows(ExecutionException.class, () -> restoreTask(service).get(5, TimeUnit.SECONDS));
            service.registerStore();
            Mockito.verify(provider).registerStore(Mockito.any());
            Assert.assertEquals(Metapb.StoreState.Offline, service.getStoreInfo().getState());
        } finally {
            service.shutdown();
        }
    }

    @Test
    public void testReconnectRegistersUpWithoutRestoringAgain() throws Exception {
        List<String> events = new ArrayList<>();
        HgStoreEngine engine = Mockito.mock(HgStoreEngine.class);
        PdProvider provider = Mockito.mock(PdProvider.class);
        Mockito.when(provider.registerStore(Mockito.any())).thenAnswer(call -> {
            events.add(((Store) call.getArgument(0)).getState().name());
            return 7L;
        });
        HeartbeatService service = service(engine, provider);
        try {
            registerRestored(service);
            Assert.assertEquals(List.of("Offline", "Up"), events);
            events.clear();
            CountDownLatch offline = new CountDownLatch(1);
            CountDownLatch up = new CountDownLatch(1);
            service.addStateListener((store, previous, state) -> {
                if (state == Metapb.StoreState.Offline) {
                    offline.countDown();
                } else if (state == Metapb.StoreState.Up) {
                    up.countDown();
                }
            });
            service.onStateChanged(Metapb.StoreState.Offline);
            Assert.assertTrue(offline.await(5, TimeUnit.SECONDS));
            service.registerStore();
            Assert.assertTrue(up.await(5, TimeUnit.SECONDS));
            Assert.assertEquals(List.of("Up"), events);
            Assert.assertEquals(Metapb.StoreState.Up, service.getStoreInfo().getState());
            Mockito.verify(engine).stateChanged(Mockito.any(), Mockito.any(), Mockito.eq(Metapb.StoreState.Up));
        } finally {
            service.shutdown();
        }
    }

    @Test
    public void testFailedUpRegistrationRetriesWithoutRestoringAgain() throws Exception {
        HgStoreEngine engine = Mockito.mock(HgStoreEngine.class);
        PdProvider provider = Mockito.mock(PdProvider.class);
        Mockito.when(provider.registerStore(Mockito.any())).thenReturn(7L)
               .thenThrow(new PDException(1, "unavailable")).thenReturn(7L);
        HeartbeatService service = service(engine, provider);
        try {
            service.registerStore();
            restoreTask(service).get(5, TimeUnit.SECONDS);
            service.registerStore();
            Assert.assertEquals(Metapb.StoreState.Offline, service.getStoreInfo().getState());
            service.registerStore();
            Assert.assertEquals(Metapb.StoreState.Up, service.getStoreInfo().getState());
            Mockito.verify(engine).stateChanged(Mockito.any(), Mockito.any(), Mockito.eq(Metapb.StoreState.Up));
        } finally {
            service.shutdown();
        }
    }

    @Test
    public void testRestoreAttemptsHealthyPartitionAfterFailure() throws Exception {
        HgStoreEngine engine = Mockito.mock(HgStoreEngine.class, Mockito.CALLS_REAL_METHODS);
        HgStoreEngineOptions options = new HgStoreEngineOptions();
        options.setFakePD(true);
        Field optionsField = HgStoreEngine.class.getDeclaredField("options");
        optionsField.setAccessible(true);
        optionsField.set(engine, options);
        PartitionManager manager = Mockito.mock(PartitionManager.class);
        engine.setPartitionManager(manager);
        Partition first = new Partition();
        first.setId(1);
        first.setGraphName("graph");
        Partition second = new Partition();
        second.setId(2);
        second.setGraphName("graph");
        Map<Integer, Partition> partitions = new LinkedHashMap<>();
        partitions.put(1, first);
        partitions.put(2, second);
        Mockito.when(manager.getPartitions()).thenReturn(Map.of("graph", partitions));
        Mockito.doThrow(new IllegalStateException("broken raft group")).when(engine).createPartitionEngine(first);
        PartitionEngine healthy = Mockito.mock(PartitionEngine.class);
        Mockito.doReturn(healthy).when(engine).createPartitionEngine(second);
        try {
            engine.restoreLocalPartitionEngine();
            Assert.fail("partial restore must not publish the Store Up");
        } catch (IllegalStateException e) {
            Assert.assertEquals(1, e.getSuppressed().length);
            Assert.assertTrue(e.getSuppressed()[0].getMessage().contains("graph-1"));
            Assert.assertEquals("broken raft group", e.getSuppressed()[0].getCause().getMessage());
        }
        Mockito.verify(engine).createPartitionEngine(second);
    }

    @Test
    public void testShutdownReportsOfflineAndPreservesMetadata() throws Exception {
        PdProvider provider = Mockito.mock(PdProvider.class);
        PDClient client = Mockito.mock(PDClient.class);
        Mockito.when(provider.getPDClient()).thenReturn(client);
        Metapb.Store remote = Metapb.Store.newBuilder().setId(7).setState(Metapb.StoreState.Up)
                                       .setRaftAddress("127.0.0.1:8510")
                                       .addLabels(Metapb.StoreLabel.newBuilder()
                                                                 .setKey("zone").setValue("a")).build();
        Mockito.when(client.reportStoreShutdown(7)).thenReturn(remote.toBuilder()
                .setState(Metapb.StoreState.Offline).build());
        HeartbeatService service = service(null, provider);
        service.getStoreInfo().setId(7);
        service.shutdown();
        Mockito.verify(client).reportStoreShutdown(7);
        Mockito.verify(client, Mockito.never()).updateStore(Mockito.any());
        Assert.assertEquals(Metapb.StoreState.Offline, service.getStoreInfo().getState());
    }

    @Test
    public void testShutdownContinuesWhenPdIsUnavailable() throws Exception {
        PdProvider provider = Mockito.mock(PdProvider.class);
        PDClient client = Mockito.mock(PDClient.class);
        Mockito.when(provider.getPDClient()).thenReturn(client);
        Mockito.when(client.reportStoreShutdown(7)).thenThrow(new PDException(1, "unavailable"));
        HeartbeatService service = service(null, provider);
        service.getStoreInfo().setId(7);
        service.shutdown();
        Mockito.verify(client, Mockito.never()).updateStore(Mockito.any());
        Assert.assertEquals(Metapb.StoreState.Offline, service.getStoreInfo().getState());
    }

    private static HeartbeatService service(HgStoreEngine engine, PdProvider provider) throws Exception {
        HeartbeatService service = new HeartbeatService(engine);
        HgStoreEngineOptions options = new HgStoreEngineOptions();
        options.setGrpcAddress("127.0.0.1:8500");
        options.setRaftAddress("127.0.0.1:8510");
        Store store = new Store();
        store.setState(Metapb.StoreState.Unknown);
        set(service, "options", options);
        set(service, "storeInfo", store);
        set(service, "pdProvider", provider);
        service.setStoreMetadata(Mockito.mock(StoreMetadata.class));
        Mockito.when(provider.getClusterStats()).thenReturn(Metapb.ClusterStats.newBuilder()
                .setState(Metapb.ClusterState.Cluster_OK).build());
        return service;
    }

    private static FutureTask<?> restoreTask(HeartbeatService service) throws Exception {
        Field field = HeartbeatService.class.getDeclaredField("restoreTask");
        field.setAccessible(true);
        return (FutureTask<?>) field.get(service);
    }

    private static void registerRestored(HeartbeatService service) throws Exception {
        CountDownLatch up = new CountDownLatch(1);
        service.addStateListener((store, previous, state) -> {
            if (state == Metapb.StoreState.Up) {
                up.countDown();
            }
        });
        service.registerStore();
        restoreTask(service).get(5, TimeUnit.SECONDS);
        service.registerStore();
        Assert.assertTrue(up.await(5, TimeUnit.SECONDS));
    }

    @Test
    public void testSlowRestoreKeepsRegisteringOfflineAndSkipsPartitionPulse() throws Exception {
        HgStoreEngine engine = Mockito.mock(HgStoreEngine.class);
        PdProvider provider = Mockito.mock(PdProvider.class);
        Mockito.when(provider.registerStore(Mockito.any())).thenReturn(7L);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        Mockito.doAnswer(call -> {
            entered.countDown();
            Assert.assertTrue(release.await(5, TimeUnit.SECONDS));
            return null;
        }).when(engine).stateChanged(Mockito.any(), Mockito.any(), Mockito.any());
        HeartbeatService service = service(engine, provider);
        Mockito.when(provider.getClusterStats()).thenReturn(Metapb.ClusterStats.newBuilder()
                .setState(Metapb.ClusterState.Cluster_OK).build(), Metapb.ClusterStats.newBuilder()
                .setState(Metapb.ClusterState.Cluster_Not_Ready).build());
        try {
            service.registerStore();
            Assert.assertTrue(entered.await(5, TimeUnit.SECONDS));
            Assert.assertTrue(service.isClusterReady());
            service.registerStore();
            Assert.assertFalse(service.isClusterReady());
            Assert.assertEquals(Metapb.StoreState.Offline, service.getStoreInfo().getState());
            Mockito.verify(provider).registerStore(Mockito.argThat(
                    store -> store.getState() == Metapb.StoreState.Offline));
            service.partitionHeartbeat();
            Mockito.verify(engine, Mockito.never()).getLeaderPartition();
            Mockito.verify(provider, Mockito.never()).partitionHeartbeat(Mockito.anyList());
        } finally {
            release.countDown();
            service.shutdown();
        }
    }

    @Test
    public void testRefusedRegistrationIsRetriedBeforeStartingRestore() throws Exception {
        HgStoreEngine engine = Mockito.mock(HgStoreEngine.class);
        PdProvider provider = Mockito.mock(PdProvider.class);
        Mockito.when(provider.registerStore(Mockito.any())).thenReturn(0L);
        HeartbeatService service = service(engine, provider);
        service.getStoreInfo().setId(7);
        try {
            service.registerStore();
            service.registerStore();
            Assert.assertNull(restoreTask(service));
            Mockito.verify(provider, Mockito.times(2)).registerStore(Mockito.any());
            Mockito.verifyNoInteractions(engine);
        } finally {
            service.shutdown();
        }
    }

    @Test
    public void testRestoreRetryBackoffKeepsLeaseRenewalIndependent() throws Exception {
        HgStoreEngine engine = Mockito.mock(HgStoreEngine.class);
        PdProvider provider = Mockito.mock(PdProvider.class);
        Mockito.when(provider.registerStore(Mockito.any())).thenReturn(7L);
        Mockito.doThrow(new IllegalStateException("broken partition")).when(engine)
               .stateChanged(Mockito.any(), Mockito.any(), Mockito.any());
        HeartbeatService service = Mockito.spy(service(engine, provider));
        Mockito.doReturn(1000L).when(service).monotonicTimeMillis();
        try {
            service.registerStore();
            Assert.assertThrows(ExecutionException.class, () -> restoreTask(service).get(5, TimeUnit.SECONDS));
            service.registerStore();
            Assert.assertNull(restoreTask(service));
            service.registerStore();
            Assert.assertNull(restoreTask(service));
            Mockito.verify(engine).stateChanged(Mockito.any(), Mockito.any(), Mockito.any());

            Mockito.doReturn(2000L).when(service).monotonicTimeMillis();
            service.registerStore();
            Assert.assertThrows(ExecutionException.class, () -> restoreTask(service).get(5, TimeUnit.SECONDS));
            service.registerStore();
            Mockito.doReturn(3000L).when(service).monotonicTimeMillis();
            service.registerStore();
            Assert.assertNull(restoreTask(service));
            Mockito.verify(engine, Mockito.times(2)).stateChanged(Mockito.any(), Mockito.any(), Mockito.any());

            // Renewal remains due after five seconds, independently of the worker's retry deadline.
            Mockito.doReturn(7000L).when(service).monotonicTimeMillis();
            service.registerStore();
            Mockito.verify(provider, Mockito.times(2)).registerStore(Mockito.any());
            Assert.assertEquals(Metapb.StoreState.Offline, service.getStoreInfo().getState());
        } finally {
            service.shutdown();
        }
    }

    private static void set(Object target, String name, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }

    @Test
    public void testOlderPdShutdownDoesNotFallBackToAdministrativeOffline() throws Exception {
        PdProvider provider = Mockito.mock(PdProvider.class);
        PDClient client = Mockito.mock(PDClient.class);
        Mockito.when(provider.getPDClient()).thenReturn(client);
        Mockito.when(client.reportStoreShutdown(7)).thenThrow(io.grpc.Status.UNIMPLEMENTED.asRuntimeException());
        HeartbeatService service = service(null, provider);
        service.getStoreInfo().setId(7);
        service.shutdown();
        Mockito.verify(client, Mockito.never()).updateStore(Mockito.any());
        Assert.assertEquals(Metapb.StoreState.Offline, service.getStoreInfo().getState());
    }

    @Test
    public void testShutdownWaitsForRestoreWorker() throws Exception {
        HgStoreEngine engine = Mockito.mock(HgStoreEngine.class);
        PdProvider provider = Mockito.mock(PdProvider.class);
        Mockito.when(provider.registerStore(Mockito.any())).thenReturn(7L);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        Mockito.doAnswer(call -> {
            entered.countDown();
            Assert.assertTrue(release.await(5, TimeUnit.SECONDS));
            return null;
        }).when(engine).stateChanged(Mockito.any(), Mockito.any(), Mockito.any());
        HeartbeatService service = service(engine, provider);
        ExecutorService closer = Executors.newSingleThreadExecutor();
        try {
            service.registerStore();
            Assert.assertTrue(entered.await(5, TimeUnit.SECONDS));
            Future<?> closed = closer.submit(service::shutdown);
            Assert.assertThrows(TimeoutException.class, () -> closed.get(100, TimeUnit.MILLISECONDS));
            release.countDown();
            closed.get(5, TimeUnit.SECONDS);
            Assert.assertEquals(Metapb.StoreState.Offline, service.getStoreInfo().getState());
        } finally {
            release.countDown();
            service.shutdown();
            closer.shutdownNow();
        }
    }

    @Test
    public void testProviderUsesRegistrationStateWithoutCachedLookup() throws Exception {
        DefaultPdProvider provider = Mockito.mock(DefaultPdProvider.class, Mockito.CALLS_REAL_METHODS);
        PDClient client = Mockito.mock(PDClient.class);
        Field field = DefaultPdProvider.class.getDeclaredField("pdClient");
        field.setAccessible(true);
        field.set(provider, client);
        Metapb.Store offline = Metapb.Store.newBuilder().setId(7).setState(Metapb.StoreState.Offline).build();
        Mockito.when(client.registerStoreWithState(Mockito.any())).thenReturn(offline,
                offline.toBuilder().setState(Metapb.StoreState.Up).build());
        Store store = new Store();
        store.setStoreAddress("127.0.0.1:8500");
        store.setRaftAddress("127.0.0.1:8510");
        store.setDeployPath("");
        store.setDataPath("");
        store.setState(Metapb.StoreState.Offline);
        Assert.assertEquals(7L, provider.registerStore(store));
        store.setState(Metapb.StoreState.Up);
        Assert.assertEquals(7L, provider.registerStore(store));
        Mockito.verify(client, Mockito.never()).getStore(Mockito.anyLong());
    }
}
