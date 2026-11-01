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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hugegraph.pd.ConfigService;
import org.apache.hugegraph.pd.KvService;
import org.apache.hugegraph.pd.PartitionService;
import org.apache.hugegraph.pd.StoreNodeService;
import org.apache.hugegraph.pd.config.PDConfig;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.pd.meta.ConfigMetaStore;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class GraphSpaceUsageTest {

    private static void setField(Object target, String name, Object value) throws Exception {
        Class<?> type = target instanceof ConfigService ? ConfigService.class : StoreNodeService.class;
        Field field = type.getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }

    @Test
    public void testQuotaUpdatePreservesUsageButStatisticsCanResetToZero() throws Exception {
        ConfigService service = Mockito.mock(ConfigService.class, Mockito.CALLS_REAL_METHODS);
        ConfigMetaStore meta = Mockito.mock(ConfigMetaStore.class);
        setField(service, "meta", meta);
        Metapb.GraphSpace stored = Metapb.GraphSpace.newBuilder().setName("shop")
                                                .setStorageLimit(100).setUsedSize(512).build();
        Mockito.when(meta.getGraphSpaceExact("shop")).thenReturn(stored);
        Mockito.when(meta.setGraphSpace(Mockito.any())).thenAnswer(call -> call.getArgument(0));
        Metapb.GraphSpace update = stored.toBuilder().setStorageLimit(200).setUsedSize(0).build();
        Assert.assertEquals(512, service.setGraphSpaceLimit(update).getUsedSize());
        Assert.assertEquals(200, service.setGraphSpaceLimit(update).getStorageLimit());
        Assert.assertEquals(0, service.setGraphSpaceUsage("shop", 0).getUsedSize());
    }

    @Test
    public void testUsageReturnsToZeroAfterLastGraphIsRemoved() throws Exception {
        StoreNodeService service = Mockito.mock(StoreNodeService.class, Mockito.CALLS_REAL_METHODS);
        PartitionService partitions = Mockito.mock(PartitionService.class);
        ConfigService config = Mockito.mock(ConfigService.class);
        setField(service, "pdConfig", new PDConfig());
        setField(service, "partitionService", partitions);
        setField(service, "configService", config);
        setField(service, "kvService", Mockito.mock(KvService.class));
        Metapb.GraphSpace stored = Metapb.GraphSpace.newBuilder().setName("shop")
                                                .setStorageLimit(100).setUsedSize(512).build();
        Mockito.when(config.getGraphSpace("")).thenReturn(List.of(stored));
        Mockito.when(partitions.getGraphs()).thenReturn(Collections.emptyList());
        service.getQuota();
        Mockito.verify(config).refreshGraphSpaceUsage("shop", 0, partitions);
    }

    @Test
    public void testUsageAdvancesWithoutDefaultClusterConfigAndCountsOnlyLeaders() throws Exception {
        StoreNodeService service = Mockito.mock(StoreNodeService.class, Mockito.CALLS_REAL_METHODS);
        PartitionService partitions = Mockito.mock(PartitionService.class);
        ConfigService config = Mockito.mock(ConfigService.class);
        KvService kv = Mockito.mock(KvService.class);
        setField(service, "pdConfig", new PDConfig());
        setField(service, "partitionService", partitions);
        setField(service, "configService", config);
        setField(service, "kvService", kv);
        Metapb.Graph graph = Metapb.Graph.newBuilder().setGraphName("shop/g/g").build();
        Mockito.when(partitions.getGraphs()).thenReturn(List.of(graph));
        Metapb.GraphStats leader = Metapb.GraphStats.newBuilder().setGraphName("shop/g/g")
                                                  .setRole(Metapb.ShardRole.Leader)
                                                  .setApproximateSize(512).build();
        Metapb.Store store = Metapb.Store.newBuilder().setStats(Metapb.StoreStats.newBuilder()
                .addGraphStats(leader).addGraphStats(leader.toBuilder().setRole(Metapb.ShardRole.Follower)))
                                          .build();
        Mockito.doReturn(List.of(store)).when(service).getStores("shop/g/g");
        service.getQuota();
        Mockito.verify(config).refreshGraphSpaceUsage("shop", 512, partitions);
    }

    @Test
    public void testQuotaCheckKeepsLimitInGb() throws Exception {
        StoreNodeService service = Mockito.mock(StoreNodeService.class, Mockito.CALLS_REAL_METHODS);
        ConfigService config = Mockito.mock(ConfigService.class, Mockito.CALLS_REAL_METHODS);
        ConfigMetaStore meta = Mockito.mock(ConfigMetaStore.class);
        PartitionService partitions = Mockito.mock(PartitionService.class);
        KvService kv = Mockito.mock(KvService.class);
        setField(config, "meta", meta);
        setField(service, "configService", config);
        setField(service, "pdConfig", new PDConfig());
        setField(service, "partitionService", partitions);
        setField(service, "kvService", kv);
        Metapb.GraphSpace stored = Metapb.GraphSpace.newBuilder().setName("shop")
                                                .setStorageLimit(100).setUsedSize(512).build();
        Mockito.when(meta.getGraphSpace("")).thenReturn(List.of(stored));
        Mockito.when(meta.getGraphSpaceExact("shop")).thenReturn(stored);
        Mockito.when(meta.setGraphSpace(Mockito.any())).thenAnswer(call -> call.getArgument(0));
        Mockito.when(kv.get(Mockito.anyString())).thenReturn("{\"storage_limit\":100}");
        Mockito.when(partitions.getGraphs()).thenReturn(Collections.emptyList());
        Assert.assertEquals(false, service.getQuota().get("shop"));
        Mockito.verify(meta, Mockito.never()).getGraphSpace("shop");
        Mockito.verify(meta).setGraphSpace(Mockito.argThat(space ->
                space.getStorageLimit() == 100 && space.getUsedSize() == 0));
    }

    @Test
    public void testQuotaUsesConfiguredServerNamespace() throws Exception {
        StoreNodeService service = Mockito.mock(StoreNodeService.class, Mockito.CALLS_REAL_METHODS);
        PartitionService partitions = Mockito.mock(PartitionService.class, Mockito.CALLS_REAL_METHODS);
        ConfigService config = Mockito.mock(ConfigService.class, Mockito.CALLS_REAL_METHODS);
        ConfigMetaStore meta = Mockito.mock(ConfigMetaStore.class);
        setField(config, "meta", meta);
        Mockito.when(meta.getGraphSpaceExact("shop")).thenReturn(
                Metapb.GraphSpace.newBuilder().setName("shop").setStorageLimit(100).build());
        Mockito.when(meta.setGraphSpace(Mockito.any())).thenAnswer(call -> call.getArgument(0));
        KvService kv = Mockito.mock(KvService.class);
        PDConfig pdConfig = new PDConfig();
        pdConfig.setServerCluster("custom-cluster");
        setField(service, "pdConfig", pdConfig);
        setField(service, "partitionService", partitions);
        setField(service, "configService", config);
        setField(service, "kvService", kv);
        setField(service, "statusListeners", Collections.emptyList());
        Metapb.Graph graph = Metapb.Graph.newBuilder().setGraphName("shop/g/g").build();
        Mockito.doReturn(List.of(graph)).when(partitions).getGraphs();
        Mockito.doReturn(graph).when(partitions).getGraph("shop/g/g");
        Mockito.doReturn(graph).when(partitions).updateGraph(Mockito.any());
        Metapb.Store store = Metapb.Store.newBuilder().setStats(Metapb.StoreStats.newBuilder()
                .addGraphStats(Metapb.GraphStats.newBuilder().setGraphName("shop/g/g")
                        .setRole(Metapb.ShardRole.Leader).setApproximateSize(2L * 1024 * 1024))).build();
        Mockito.doReturn(List.of(store)).when(service).getStores("shop/g/g");
        Mockito.when(kv.get("HUGEGRAPH/custom-cluster/GRAPHSPACE/CONF/shop"))
               .thenReturn("{\"storage_limit\":1}");
        Mockito.when(kv.get("HUGEGRAPH/hg/GRAPHSPACE/CONF/shop"))
               .thenReturn("{\"storage_limit\":100}");

        Assert.assertEquals(true, service.getQuota().get("shop"));
        Mockito.verify(partitions).updateGraph(Mockito.argThat(updated ->
                updated.getGraphState().getMode() == Metapb.GraphMode.ReadOnly));
        Mockito.verify(kv, Mockito.never()).get("HUGEGRAPH/hg/GRAPHSPACE/CONF/shop");
        Mockito.verify(config).setGraphSpaceUsage("shop", 2L * 1024 * 1024);

        pdConfig.setServerCluster(new PDConfig().getServerCluster());
        Assert.assertEquals(false, service.getQuota().get("shop"));
        Mockito.verify(kv).get("HUGEGRAPH/hg/GRAPHSPACE/CONF/shop");
    }

    @Test
    public void testConcurrentFacadesPreserveLimitAndUsage() throws Exception {
        ConfigService usage = Mockito.mock(ConfigService.class, Mockito.CALLS_REAL_METHODS);
        ConfigService quota = Mockito.mock(ConfigService.class, Mockito.CALLS_REAL_METHODS);
        ConfigMetaStore meta = Mockito.mock(ConfigMetaStore.class);
        setField(usage, "meta", meta);
        setField(quota, "meta", meta);
        AtomicReference<Metapb.GraphSpace> stored = new AtomicReference<>(
                Metapb.GraphSpace.newBuilder().setName("shop").setStorageLimit(100).build());
        CountDownLatch writingUsage = new CountDownLatch(1);
        CountDownLatch releaseUsage = new CountDownLatch(1);
        CountDownLatch quotaStarted = new CountDownLatch(1);
        CountDownLatch quotaWrite = new CountDownLatch(1);
        Mockito.when(meta.getGraphSpaceExact("shop")).thenAnswer(call -> stored.get());
        Mockito.doAnswer(call -> {
            quotaWrite.countDown();
            return call.callRealMethod();
        }).when(quota).setGraphSpace(Mockito.any());
        Mockito.when(meta.setGraphSpace(Mockito.any())).thenAnswer(call -> {
            Metapb.GraphSpace value = call.getArgument(0);
            if (value.getStorageLimit() == 100) {
                writingUsage.countDown();
                Assert.assertTrue(releaseUsage.await(5, TimeUnit.SECONDS));
            }
            stored.set(value);
            return value;
        });
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<?> usageResult = executor.submit(() -> usage.setGraphSpaceUsage("shop", 512));
            Assert.assertTrue(writingUsage.await(5, TimeUnit.SECONDS));
            Future<?> quotaResult = executor.submit(() -> {
                quotaStarted.countDown();
                return quota.setGraphSpaceLimit(Metapb.GraphSpace.newBuilder()
                        .setName("shop").setStorageLimit(200).build());
            });
            Assert.assertTrue(quotaStarted.await(5, TimeUnit.SECONDS));
            Assert.assertFalse("Quota must wait for the other facade's read/write transaction",
                               quotaWrite.await(200, TimeUnit.MILLISECONDS));
            releaseUsage.countDown();
            usageResult.get(5, TimeUnit.SECONDS);
            quotaResult.get(5, TimeUnit.SECONDS);
            Assert.assertEquals(200, stored.get().getStorageLimit());
            Assert.assertEquals(512, stored.get().getUsedSize());
            usage.setGraphSpaceUsage("shop", 1024);
            Assert.assertEquals(200, stored.get().getStorageLimit());
            Assert.assertEquals(1024, stored.get().getUsedSize());
        } finally {
            releaseUsage.countDown();
            executor.shutdownNow();
        }
    }

}
