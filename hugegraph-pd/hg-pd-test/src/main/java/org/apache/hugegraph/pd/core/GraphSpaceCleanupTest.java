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

package org.apache.hugegraph.pd.core;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hugegraph.pd.ConfigService;
import org.apache.hugegraph.pd.PartitionService;
import org.apache.hugegraph.pd.StoreNodeService;
import org.apache.hugegraph.pd.KvService;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.config.PDConfig;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.pd.grpc.Pdpb;
import org.apache.hugegraph.pd.meta.ConfigMetaStore;
import org.apache.hugegraph.pd.meta.PartitionMeta;
import org.apache.hugegraph.pd.meta.MetadataKeyHelper;
import org.apache.hugegraph.pd.meta.MetadataRocksDBStore;
import org.apache.hugegraph.pd.store.HgKVStore;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class GraphSpaceCleanupTest {

    @Test
    public void testDeletesOnlyExactKeyAndPropagatesWriteFailure() throws Exception {
        ConfigMetaStore meta = Mockito.mock(ConfigMetaStore.class, Mockito.CALLS_REAL_METHODS);
        HgKVStore store = Mockito.mock(HgKVStore.class);
        Field field = MetadataRocksDBStore.class.getDeclaredField("store");
        field.setAccessible(true);
        field.set(meta, store);
        byte[] key = MetadataKeyHelper.getGraphSpaceKey("shop");
        meta.deleteGraphSpace("shop");
        Mockito.verify(store).remove(key);
        Mockito.verify(store, Mockito.never()).removeByPrefix(Mockito.any());
        Mockito.doThrow(new IllegalStateException("write failed")).when(store).remove(key);
        Assert.assertThrows(PDException.class, () -> meta.deleteGraphSpace("shop"));
    }

    @Test
    public void testIdempotentCleanupAndStaleUsageCannotRecreateRecord() throws Exception {
        Fixture fixture = new Fixture();
        fixture.service.deleteGraphSpace("shop", fixture.partitions);
        fixture.service.deleteGraphSpace("shop", fixture.partitions);
        Assert.assertNull(fixture.service.refreshGraphSpaceUsage("shop", 512, fixture.partitions));
        Assert.assertNull(fixture.stored.get());
        Mockito.verify(fixture.meta, Mockito.times(2)).deleteGraphSpace("shop");
        Mockito.verify(fixture.meta, Mockito.never()).setGraphSpace(Mockito.any());
    }

    @Test
    public void testLiveEmptySpaceKeepsLimitAndResetsUsage() throws Exception {
        Fixture fixture = new Fixture();
        Metapb.GraphSpace updated = fixture.service.refreshGraphSpaceUsage("shop", 0,
                                                                          fixture.partitions);
        Assert.assertEquals(100, updated.getStorageLimit());
        Assert.assertEquals(0, updated.getUsedSize());
        Mockito.verify(fixture.partitions, Mockito.never()).getGraphs();
    }

    @Test
    public void testActiveGraphsAllowMissingUsageRecoveryButPreventCleanup() throws Exception {
        Fixture fixture = new Fixture();
        fixture.stored.set(null);
        Mockito.when(fixture.partitions.getGraphs()).thenReturn(List.of(
                Metapb.Graph.newBuilder().setGraphName("shop/g/g").build()));
        Assert.assertEquals(512, fixture.service.refreshGraphSpaceUsage("shop", 512,
                                                                       fixture.partitions).getUsedSize());
        Assert.assertThrows(PDException.class,
                            () -> fixture.service.deleteGraphSpace("shop", fixture.partitions));
        Assert.assertNotNull(fixture.stored.get());
    }

    @Test
    public void testSimilarlyNamedSpaceDoesNotPreventCleanup() throws Exception {
        Fixture fixture = new Fixture();
        Mockito.when(fixture.partitions.getGraphs()).thenReturn(List.of(
                Metapb.Graph.newBuilder().setGraphName("shop-other/g/g").build()));
        fixture.service.deleteGraphSpace("shop", fixture.partitions);
        Assert.assertNull(fixture.service.refreshGraphSpaceUsage("shop", 512, fixture.partitions));
    }

    @Test
    public void testLeftoverPdGraphIsNamedAndRetainedForOperatorRecovery() throws Exception {
        Fixture fixture = new Fixture();
        Mockito.when(fixture.partitions.getGraphs()).thenReturn(List.of(
                Metapb.Graph.newBuilder().setGraphName("shop/old/g").build(),
                Metapb.Graph.newBuilder().setGraphName("shop/old/m").build(),
                Metapb.Graph.newBuilder().setGraphName("shop-other/live/g").build()));
        PDException failure = Assert.assertThrows(PDException.class,
                () -> fixture.service.deleteGraphSpace("shop", fixture.partitions));
        Assert.assertTrue(failure.getMessage().contains("2 graph record(s)"));
        Assert.assertTrue(failure.getMessage().contains("shop/old/g"));
        Assert.assertTrue(failure.getMessage().contains("shop/old/m"));
        Assert.assertTrue(failure.getMessage().contains("GET /v1/partitionsAndStats"));
        Assert.assertFalse(failure.getMessage().contains("shop-other/live/g"));
        Assert.assertNotNull(fixture.stored.get());
        Mockito.verify(fixture.meta, Mockito.never()).deleteGraphSpace(Mockito.any());
        Mockito.verify(fixture.partitions, Mockito.never()).delGraph(Mockito.any());
    }

    @Test
    public void testRemainingGraphDiagnosticBoundsNamesAndPayload() throws Exception {
        Fixture fixture = new Fixture();
        List<Metapb.Graph> graphs = new ArrayList<>();
        for (int i = 0; i < 12; i++) {
            graphs.add(Metapb.Graph.newBuilder().setGraphName(
                    "shop/" + i + "-\n" + "x".repeat(2000) + "/g").build());
        }
        Mockito.when(fixture.partitions.getGraphs()).thenReturn(graphs);
        PDException failure = Assert.assertThrows(PDException.class,
                () -> fixture.service.deleteGraphSpace("shop", fixture.partitions));
        String message = failure.getMessage();
        Assert.assertTrue(message.contains("12 graph record(s)"));
        Assert.assertTrue(message.contains("showing up to 10 names, 128 characters each"));
        Assert.assertTrue(message.contains("shop/9-?"));
        Assert.assertFalse(message.contains("shop/10-"));
        Assert.assertFalse(message.contains("\n"));
        Assert.assertTrue(message.length() < 1800);
        Mockito.verify(fixture.meta, Mockito.never()).deleteGraphSpace(Mockito.any());
        Mockito.verify(fixture.partitions, Mockito.never()).delGraph(Mockito.any());
    }

    @Test
    public void testRejectsEmptyOrNestedNamesAndPropagatesReadFailure() throws Exception {
        Fixture fixture = new Fixture();
        Assert.assertThrows(PDException.class,
                            () -> fixture.service.deleteGraphSpace("", fixture.partitions));
        Assert.assertThrows(PDException.class,
                            () -> fixture.service.deleteGraphSpace("shop/nested", fixture.partitions));
        Mockito.when(fixture.partitions.getGraphs()).thenThrow(
                new PDException(Pdpb.ErrorType.ROCKSDB_READ_ERROR_VALUE, "read failed"));
        Assert.assertThrows(PDException.class,
                            () -> fixture.service.deleteGraphSpace("shop", fixture.partitions));
        Assert.assertNotNull(fixture.stored.get());
        Mockito.verify(fixture.meta, Mockito.never()).deleteGraphSpace(Mockito.any());
    }

    @Test
    public void testCleanupWaitsForOtherFacadeUsageThenRemainsDeleted() throws Exception {
        Fixture fixture = new Fixture();
        ConfigService cleanup = service(fixture.meta);
        CountDownLatch writing = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch deleting = new CountDownLatch(1);
        CountDownLatch deleted = new CountDownLatch(1);
        Mockito.doAnswer(call -> {
            writing.countDown();
            Assert.assertTrue(release.await(5, TimeUnit.SECONDS));
            fixture.stored.set(call.getArgument(0));
            return fixture.stored.get();
        }).when(fixture.meta).setGraphSpace(Mockito.any());
        Mockito.doAnswer(call -> {
            fixture.stored.set(null);
            deleted.countDown();
            return null;
        }).when(fixture.meta).deleteGraphSpace("shop");
        ExecutorService workers = Executors.newFixedThreadPool(2);
        try {
            Future<?> usage = workers.submit(() ->
                    fixture.service.refreshGraphSpaceUsage("shop", 512, fixture.partitions));
            Assert.assertTrue(writing.await(5, TimeUnit.SECONDS));
            Future<?> removal = workers.submit(() -> {
                deleting.countDown();
                cleanup.deleteGraphSpace("shop", fixture.partitions);
                return null;
            });
            Assert.assertTrue(deleting.await(5, TimeUnit.SECONDS));
            Assert.assertFalse(deleted.await(200, TimeUnit.MILLISECONDS));
            release.countDown();
            usage.get(5, TimeUnit.SECONDS);
            removal.get(5, TimeUnit.SECONDS);
            Assert.assertNull(fixture.service.refreshGraphSpaceUsage("shop", 512, fixture.partitions));
            Assert.assertNull(fixture.stored.get());
        } finally {
            release.countDown();
            workers.shutdownNow();
        }
    }

    @Test
    public void testQuotaSnapshotCannotRestoreDeletedSpaceOrGraph() throws Exception {
        Fixture fixture = new Fixture();
        StoreNodeService stores = Mockito.mock(StoreNodeService.class, Mockito.CALLS_REAL_METHODS);
        Field pdConfigField = StoreNodeService.class.getDeclaredField("pdConfig");
        pdConfigField.setAccessible(true);
        pdConfigField.set(stores, new PDConfig());
        for (String fieldName : List.of("configService", "partitionService", "kvService")) {
            Field field = StoreNodeService.class.getDeclaredField(fieldName);
            field.setAccessible(true);
            field.set(stores, fieldName.equals("configService") ? fixture.service :
                              fieldName.equals("partitionService") ? fixture.partitions :
                              Mockito.mock(KvService.class));
        }
        Metapb.Graph oldGraph = Metapb.Graph.newBuilder().setGraphName("shop/g/g")
                .setGraphState(Metapb.GraphState.newBuilder().setMode(Metapb.GraphMode.ReadOnly)).build();
        Mockito.when(fixture.partitions.getGraphs()).thenReturn(List.of(oldGraph), List.of());
        Mockito.when(fixture.meta.getGraphSpace("")).thenReturn(List.of(fixture.stored.get()));
        Mockito.doAnswer(call -> {
            fixture.service.deleteGraphSpace("shop", fixture.partitions);
            return List.of();
        }).when(stores).getStores("shop/g/g");
        Assert.assertTrue(stores.getQuota().isEmpty());
        Assert.assertNull(fixture.stored.get());
        Mockito.verify(fixture.partitions, Mockito.never()).updateGraphIfExists(Mockito.any());
        Mockito.verify(fixture.partitions, Mockito.never()).updateGraph(Mockito.any());
    }

    @Test
    public void testQuotaStateUpdateNeverCreatesMissingGraph() throws Exception {
        PartitionService partitions = Mockito.mock(PartitionService.class, Mockito.CALLS_REAL_METHODS);
        Metapb.Graph graph = Metapb.Graph.newBuilder().setGraphName("shop/g/g").build();
        Mockito.doReturn(null).when(partitions).getGraph("shop/g/g");
        Assert.assertNull(partitions.updateGraphIfExists(graph));
        Mockito.verify(partitions, Mockito.never()).updateGraph(Mockito.any());
        Mockito.doReturn(graph).when(partitions).getGraph("shop/g/g");
        Mockito.doReturn(graph).when(partitions).updateGraph(graph);
        Assert.assertEquals(graph, partitions.updateGraphIfExists(graph));
    }

    @Test
    public void testGraphDeletionWaitsForQuotaUpdateAndPreventsLaterRecreation() throws Exception {
        PartitionService partitions = Mockito.mock(PartitionService.class, Mockito.CALLS_REAL_METHODS);
        PartitionMeta meta = Mockito.mock(PartitionMeta.class);
        Field field = PartitionService.class.getDeclaredField("partitionMeta");
        field.setAccessible(true);
        field.set(partitions, meta);
        Metapb.Graph graph = Metapb.Graph.newBuilder().setGraphName("shop/g/g").build();
        AtomicReference<Metapb.Graph> stored = new AtomicReference<>(graph);
        CountDownLatch updating = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch deleting = new CountDownLatch(1);
        CountDownLatch removed = new CountDownLatch(1);
        Mockito.doAnswer(call -> stored.get()).when(partitions).getGraph("shop/g/g");
        Mockito.doReturn(List.of()).when(partitions).getPartitions("shop/g/g");
        Mockito.doAnswer(call -> {
            updating.countDown();
            Assert.assertTrue(release.await(5, TimeUnit.SECONDS));
            return graph;
        }).when(partitions).updateGraph(graph);
        Mockito.doAnswer(call -> {
            stored.set(null);
            removed.countDown();
            return 1L;
        }).when(meta).removeGraph("shop/g/g");
        ExecutorService workers = Executors.newFixedThreadPool(2);
        try {
            Future<?> update = workers.submit(() -> partitions.updateGraphIfExists(graph));
            Assert.assertTrue(updating.await(5, TimeUnit.SECONDS));
            Future<?> removal = workers.submit(() -> {
                deleting.countDown();
                return partitions.delGraph("shop/g/g");
            });
            Assert.assertTrue(deleting.await(5, TimeUnit.SECONDS));
            Assert.assertFalse(removed.await(200, TimeUnit.MILLISECONDS));
            release.countDown();
            update.get(5, TimeUnit.SECONDS);
            removal.get(5, TimeUnit.SECONDS);
            Assert.assertNull(partitions.updateGraphIfExists(graph));
            Mockito.verify(partitions).updateGraph(graph);
        } finally {
            release.countDown();
            workers.shutdownNow();
        }
    }

    private static ConfigService service(ConfigMetaStore meta) throws Exception {
        ConfigService service = Mockito.mock(ConfigService.class, Mockito.CALLS_REAL_METHODS);
        Field field = ConfigService.class.getDeclaredField("meta");
        field.setAccessible(true);
        field.set(service, meta);
        return service;
    }

    private static class Fixture {

        private final ConfigMetaStore meta = Mockito.mock(ConfigMetaStore.class);
        private final PartitionService partitions = Mockito.mock(PartitionService.class);
        private final AtomicReference<Metapb.GraphSpace> stored = new AtomicReference<>(
                Metapb.GraphSpace.newBuilder().setName("shop").setStorageLimit(100).setUsedSize(512).build());
        private final ConfigService service;

        private Fixture() throws Exception {
            this.service = service(this.meta);
            Mockito.when(this.partitions.getGraphs()).thenReturn(List.of());
            Mockito.when(this.meta.getGraphSpaceExact("shop")).thenAnswer(call -> this.stored.get());
            Mockito.when(this.meta.setGraphSpace(Mockito.any())).thenAnswer(call -> {
                this.stored.set(call.getArgument(0));
                return this.stored.get();
            });
            Mockito.doAnswer(call -> {
                this.stored.set(null);
                return null;
            }).when(this.meta).deleteGraphSpace("shop");
        }
    }
}
