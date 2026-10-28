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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hugegraph.pd.StoreNodeService;
import org.apache.hugegraph.pd.ConfigService;
import org.apache.hugegraph.pd.PartitionService;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.grpc.Pdpb;
import org.apache.hugegraph.pd.config.PDConfig;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.pd.meta.StoreInfoMeta;
import org.apache.hugegraph.testutil.Whitebox;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class StoreRestoreRegistrationTest {

    private StoreNodeService service;
    private StoreInfoMeta metadata;
    private AtomicReference<Metapb.Store> current;
    private PDConfig config;

    @Before
    public void setUp() throws Exception {
        service = Mockito.mock(StoreNodeService.class, Mockito.CALLS_REAL_METHODS);
        metadata = Mockito.mock(StoreInfoMeta.class);
        current = new AtomicReference<>(store(Metapb.StoreState.Up));
        config = Mockito.mock(PDConfig.class);
        Mockito.when(config.getInitialStoreMap()).thenReturn(Map.of());
        Whitebox.setInternalState(service, "pdConfig", config);
        Whitebox.setInternalState(service, "storeInfoMeta", metadata);
        Whitebox.setInternalState(service, "statusListeners", List.of());
        Whitebox.setInternalState(service, "shardGroupStatusListeners", List.of());
        Mockito.doNothing().when(service).checkStoreStatus();
        Mockito.when(metadata.storeExists(7L)).thenReturn(true);
        Mockito.when(metadata.getStore(7L)).thenAnswer(call -> current.get());
        Mockito.doAnswer(call -> {
            current.set(call.getArgument(0));
            return null;
        }).when(metadata).updateStore(Mockito.any());
    }

    @Test
    public void testRestoringStoreRemainsOfflineUntilSecondRegistration() throws Exception {
        Metapb.Store restoring = service.register(store(Metapb.StoreState.Offline));
        Assert.assertEquals(Metapb.StoreState.Offline, restoring.getState());
        Mockito.verify(metadata, Mockito.never()).removeActiveStore(Mockito.any());
        Mockito.verify(metadata).keepStoreAlive(restoring);

        Metapb.Store restored = service.register(store(Metapb.StoreState.Up));
        Assert.assertEquals(Metapb.StoreState.Up, restored.getState());
        Mockito.verify(metadata).keepStoreAlive(restored);
    }

    @Test
    public void testPendingStoreStillRequiresActivation() throws Exception {
        current.set(store(Metapb.StoreState.Pending));
        Assert.assertEquals(Metapb.StoreState.Pending,
                            service.register(store(Metapb.StoreState.Offline)).getState());
        Assert.assertEquals(Metapb.StoreState.Pending,
                            service.register(store(Metapb.StoreState.Up)).getState());
        Mockito.verify(metadata, Mockito.never()).keepStoreAlive(Mockito.any());
    }

    @Test
    public void testLegacyUpRegistrationStillActivatesOfflineStore() throws Exception {
        current.set(store(Metapb.StoreState.Offline));
        Assert.assertEquals(Metapb.StoreState.Up,
                            service.register(store(Metapb.StoreState.Up)).getState());
    }

    @Test
    public void testShutdownOfflinePreservesThreeStoreReadinessUntilExpiry() throws Exception {
        assertTemporaryOfflinePreservesReadiness(false);
    }

    @Test
    public void testRestoreRegistrationPreservesThreeStoreReadinessUntilExpiry() throws Exception {
        assertTemporaryOfflinePreservesReadiness(true);
    }

    private void assertTemporaryOfflinePreservesReadiness(boolean registering) throws Exception {
        Map<Long, Metapb.Store> stores = new HashMap<>();
        for (long id = 7; id < 10; id++) {
            stores.put(id, store(Metapb.StoreState.Up).toBuilder().setId(id).build());
        }
        Map<Long, Metapb.Store> active = new HashMap<>(stores);
        Mockito.when(config.getMinStoreCount()).thenReturn(3);
        Mockito.doCallRealMethod().when(service).checkStoreStatus();
        Mockito.when(metadata.getStore(Mockito.anyLong())).thenAnswer(call -> stores.get(call.getArgument(0)));
        Mockito.when(metadata.getActiveStores()).thenAnswer(call -> new ArrayList<>(active.values()));
        Mockito.when(metadata.getShardGroups()).thenReturn(List.of(Metapb.ShardGroup.newBuilder()
                .setId(1).addShards(Metapb.Shard.newBuilder().setStoreId(7))
                .addShards(Metapb.Shard.newBuilder().setStoreId(8))
                .addShards(Metapb.Shard.newBuilder().setStoreId(9)).build()));
        Mockito.doAnswer(call -> {
            Metapb.Store store = call.getArgument(0);
            stores.put(store.getId(), store);
            return null;
        }).when(metadata).updateStore(Mockito.any());
        Mockito.doAnswer(call -> {
            active.remove(((Metapb.Store) call.getArgument(0)).getId());
            return null;
        }).when(metadata).removeActiveStore(Mockito.any());
        Mockito.doAnswer(call -> {
            Metapb.Store store = call.getArgument(0);
            active.put(store.getId(), store);
            return null;
        }).when(metadata).keepStoreAlive(Mockito.any());

        Metapb.Store offline = store(Metapb.StoreState.Offline);
        if (registering) {
            service.register(offline);
        } else {
            service.reportStoreShutdown(7);
        }
        Assert.assertEquals(Metapb.StoreState.Offline, service.getStore(7).getState());
        Metapb.StoreStats heartbeat = Metapb.StoreStats.newBuilder().setStoreId(8).build();
        Assert.assertEquals(Metapb.ClusterState.Cluster_OK, service.heartBeat(heartbeat).getState());
        Mockito.verify(metadata, Mockito.never()).removeActiveStore(Mockito.any());
        if (!registering) {
            Mockito.verify(metadata, Mockito.never()).keepStoreAlive(Mockito.argThat(store -> store.getId() == 7));
        }

        // Expiry still removes a Store that never comes back; Offline must not renew the lease.
        active.remove(7L);
        Assert.assertEquals(Metapb.ClusterState.Cluster_Not_Ready, service.heartBeat(heartbeat).getState());
        if (registering) {
            service.register(offline);
            Assert.assertEquals(Metapb.ClusterState.Cluster_OK, service.heartBeat(heartbeat).getState());
        }
    }

    @Test
    public void testAdministrativeOfflineStillRemovesActiveEntry() throws Exception {
        service.updateStore(store(Metapb.StoreState.Offline));
        Mockito.verify(metadata).removeActiveStore(Mockito.any());
    }

    @Test
    public void testAdministrativeOfflineRemovesAlreadyOfflineLease() throws Exception {
        current.set(store(Metapb.StoreState.Offline));
        service.updateStore(store(Metapb.StoreState.Offline));
        Mockito.verify(metadata).removeActiveStore(Mockito.any());
    }

    @Test
    public void testMetadataOnlyUpdateDoesNotRemoveRestoringLease() throws Exception {
        current.set(store(Metapb.StoreState.Offline));
        service.updateStore(Metapb.Store.newBuilder().setId(7)
                .addLabels(Metapb.StoreLabel.newBuilder().setKey("zone").setValue("a")).build());
        Mockito.verify(metadata, Mockito.never()).removeActiveStore(Mockito.any());
    }

    @Test
    public void testLiveRestoreLeaseDoesNotPermitPlacementUntilUp() throws Exception {
        Mockito.when(config.getMinStoreCount()).thenReturn(3);
        PDConfig.Partition partition = config.new Partition();
        partition.setShardCount(3);
        Mockito.when(config.getPartition()).thenReturn(partition);
        ConfigService configuration = Mockito.mock(ConfigService.class);
        Mockito.when(config.getConfigService()).thenReturn(configuration);
        Mockito.when(configuration.getPartitionCount()).thenReturn(1);
        PartitionService partitions = Mockito.mock(PartitionService.class);
        Whitebox.setInternalState(service, "partitionService", partitions);
        Metapb.Store second = store(Metapb.StoreState.Up).toBuilder().setId(8).build();
        Metapb.Store third = second.toBuilder().setId(9).build();
        // The retained shutdown lease can still contain the previous Up snapshot.
        AtomicLong clock = new AtomicLong();
        AtomicLong leaseExpiry = new AtomicLong(300_000L);
        Mockito.when(metadata.getActiveStores()).thenAnswer(call -> clock.get() < leaseExpiry.get() ?
                List.of(store(Metapb.StoreState.Up), second, third) : List.of(second, third));
        Mockito.doAnswer(call -> {
            leaseExpiry.set(clock.get() + 300_000L);
            return null;
        }).when(metadata).keepStoreAlive(Mockito.any());
        Mockito.when(metadata.getStore(8L)).thenReturn(second);
        Mockito.when(metadata.getStore(9L)).thenReturn(third);
        AtomicReference<Metapb.ShardGroup> group = new AtomicReference<>();
        Mockito.when(metadata.getShardGroup(0)).thenAnswer(call -> group.get());
        Mockito.doAnswer(call -> {
            group.set(call.getArgument(0));
            return null;
        }).when(metadata).updateShardGroup(Mockito.any());
        Metapb.ShardGroup underReplicated = Metapb.ShardGroup.newBuilder().setId(0)
                .addShards(Metapb.Shard.newBuilder().setStoreId(8).setRole(Metapb.ShardRole.Leader))
                .addShards(Metapb.Shard.newBuilder().setStoreId(9)).build();

        // Controlled metadata clock: recovery lasts longer than the original five-minute lease.
        for (int renewal = 0; renewal < 4; renewal++) {
            clock.addAndGet(200_000L);
            service.register(store(Metapb.StoreState.Offline));
            Assert.assertEquals(3, service.getActiveStores().size());
            Assert.assertEquals(2, service.getPlacementStores().size());
            PDException allocation = Assert.assertThrows(PDException.class, () -> service.allocShards(null, 0));
            Assert.assertEquals(Pdpb.ErrorType.LESS_ACTIVE_STORE_VALUE, allocation.getErrorCode());
            Assert.assertThrows(PDException.class, () -> service.reallocShards(underReplicated));
        }
        Mockito.verify(metadata, Mockito.never()).updateShardGroup(Mockito.any());
        Mockito.verify(metadata, Mockito.times(4)).keepStoreAlive(Mockito.any());

        service.register(store(Metapb.StoreState.Up));
        Assert.assertEquals(3, service.getPlacementStores().size());
        Assert.assertTrue(service.reallocShards(underReplicated).stream().anyMatch(shard -> shard.getStoreId() == 7));
        group.set(null);
        Assert.assertTrue(service.allocShards(null, 0).stream().anyMatch(shard -> shard.getStoreId() == 7));
    }

    @Test
    public void testTemporaryOfflineDoesNotShrinkExistingGroup() throws Exception {
        Mockito.when(config.getMinStoreCount()).thenReturn(1);
        PDConfig.Partition partition = config.new Partition();
        partition.setShardCount(3);
        Mockito.when(config.getPartition()).thenReturn(partition);
        current.set(store(Metapb.StoreState.Offline));
        Metapb.Store second = store(Metapb.StoreState.Up).toBuilder().setId(8).build();
        Mockito.when(metadata.getStore(8L)).thenReturn(second);
        Metapb.Store third = second.toBuilder().setId(9).build();
        Mockito.when(metadata.getStore(9L)).thenReturn(third);
        Mockito.when(metadata.getActiveStores()).thenReturn(List.of(store(Metapb.StoreState.Up), second, third));
        Metapb.ShardGroup group = Metapb.ShardGroup.newBuilder().setId(0)
                .addShards(Metapb.Shard.newBuilder().setStoreId(7))
                .addShards(Metapb.Shard.newBuilder().setStoreId(8))
                .addShards(Metapb.Shard.newBuilder().setStoreId(9).setRole(Metapb.ShardRole.Leader)).build();
        Assert.assertEquals(group.getShardsList(), service.reallocShards(group));
        Mockito.verify(metadata, Mockito.never()).updateShardGroup(Mockito.any());
    }

    @Test
    public void testOnlyRestoringStoreCannotReceiveInitialLeader() throws Exception {
        Mockito.when(config.getMinStoreCount()).thenReturn(1);
        current.set(store(Metapb.StoreState.Offline));
        Mockito.when(metadata.getActiveStores()).thenReturn(List.of(store(Metapb.StoreState.Up)));
        PDException failure = Assert.assertThrows(PDException.class, () -> service.allocShards(null, 0));
        Assert.assertEquals(Pdpb.ErrorType.NO_ACTIVE_STORE_VALUE, failure.getErrorCode());
        Mockito.verify(metadata, Mockito.never()).updateShardGroup(Mockito.any());
    }

    private static Metapb.Store store(Metapb.StoreState state) {
        return Metapb.Store.newBuilder().setId(7).setAddress("127.0.0.1:8500")
                           .setRaftAddress("127.0.0.1:8510").setState(state).build();
    }
}
