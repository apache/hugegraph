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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hugegraph.pd.ConfigService;
import org.apache.hugegraph.pd.PartitionService;
import org.apache.hugegraph.pd.StoreNodeService;
import org.apache.hugegraph.pd.StoreStatusListener;
import org.apache.hugegraph.pd.TaskScheduleService;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.config.PDConfig;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.pd.grpc.Pdpb;
import org.apache.hugegraph.pd.grpc.pulse.ConfChangeType;
import org.apache.hugegraph.pd.meta.StoreInfoMeta;
import org.apache.hugegraph.pd.raft.RaftEngine;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.Invocation;

import com.alipay.sofa.jraft.Node;

public class StoreDrainTest {

    @Test
    public void testExpiredLeaseDoesNotRetireStoreWithLocalGroups() throws Exception {
        Fixture fixture = new Fixture();
        Metapb.Store exiting = fixture.addStore(4, Metapb.StoreState.Exiting);
        // No active entry remains, as when a migration outlasts the heartbeat TTL.
        Mockito.when(fixture.meta.getActiveStores()).thenReturn(List.of());
        fixture.heartbeat(4, 3);
        Assert.assertEquals(Metapb.StoreState.Exiting, fixture.stores.get(4L).getState());
        Mockito.verify(fixture.meta).keepStoreAlive(fixture.stores.get(4L));
        Mockito.verify(fixture.meta, Mockito.never()).removeActiveStore(Mockito.any());
        Assert.assertEquals(exiting.getId(), fixture.stores.get(4L).getStats().getStoreId());
    }

    @Test
    public void testZeroLocalGroupsDoesNotRetireReferencedStore() throws Exception {
        Fixture fixture = new Fixture();
        fixture.addStore(4, Metapb.StoreState.Exiting);
        fixture.groups.put(0, group(0, 1, 2, 4));
        fixture.heartbeat(4, 0);
        Assert.assertEquals(Metapb.StoreState.Exiting, fixture.stores.get(4L).getState());
        Mockito.verify(fixture.meta).keepStoreAlive(fixture.stores.get(4L));
        Mockito.verify(fixture.meta, Mockito.never()).removeActiveStore(Mockito.any());
    }

    @Test
    public void testRetiresOnlyAfterBothViewsAreEmpty() throws Exception {
        Fixture fixture = new Fixture();
        fixture.addStore(4, Metapb.StoreState.Exiting);
        fixture.heartbeat(4, 0);
        Assert.assertEquals(Metapb.StoreState.Tombstone, fixture.stores.get(4L).getState());
        Mockito.verify(fixture.meta).removeActiveStore(fixture.stores.get(4L));
        Mockito.verify(fixture.meta, Mockito.never()).keepStoreAlive(Mockito.any());
    }

    @Test
    public void testPlacementUsesCurrentStateInsteadOfStaleLease() throws Exception {
        Fixture fixture = new Fixture();
        Metapb.Store up = fixture.addStore(1, Metapb.StoreState.Up);
        fixture.addStore(2, Metapb.StoreState.Exiting);
        fixture.addStore(3, Metapb.StoreState.Pending);
        fixture.addStore(4, Metapb.StoreState.Tombstone);
        Metapb.Store staleUp = Metapb.Store.newBuilder().setId(2)
                                         .setState(Metapb.StoreState.Up).build();
        Mockito.when(fixture.meta.getActiveStores()).thenReturn(List.of(
                up, staleUp, fixture.stores.get(3L), fixture.stores.get(4L),
                Metapb.Store.newBuilder().setId(5).setState(Metapb.StoreState.Up).build()));
        Assert.assertEquals(List.of(up), fixture.service.getPlacementStores());
        // Liveness remains available for an exiting source that still serves its groups.
        Assert.assertEquals(5, fixture.service.getActiveStores().size());
    }

    @Test
    public void testDrainCannotPlaceReplicaBackOnSourceAndNotifiesSharedGroupOnce() throws Exception {
        Fixture fixture = new Fixture();
        for (long id = 1; id <= 3; id++) {
            fixture.addStore(id, Metapb.StoreState.Up);
        }
        Metapb.Store source = fixture.addStore(4, Metapb.StoreState.Exiting);
        fixture.groups.put(0, group(0, 1, 2, 4));
        Mockito.when(fixture.meta.getActiveStores()).thenReturn(List.of(
                fixture.stores.get(1L), fixture.stores.get(2L), fixture.stores.get(3L),
                source.toBuilder().setState(Metapb.StoreState.Up).build()));
        List<Metapb.Partition> sharedPartitions = List.of(
                Metapb.Partition.newBuilder().setId(0).setGraphName("graph-a").build(),
                Metapb.Partition.newBuilder().setId(0).setGraphName("graph-b").build());
        Mockito.when(fixture.partitions.getPartitionById(0)).thenReturn(sharedPartitions);

        fixture.service.storeTurnoff(source);

        List<Metapb.Shard> result = fixture.groups.get(0).getShardsList();
        Assert.assertEquals(3, result.size());
        Assert.assertTrue(result.stream().noneMatch(shard -> shard.getStoreId() == 4));
        Assert.assertTrue(result.stream().anyMatch(shard -> shard.getStoreId() == 3));
        List<Invocation> changes = Mockito.mockingDetails(fixture.partitions).getInvocations().stream()
                                          .filter(invocation -> invocation.getMethod().getName()
                                                                          .equals("fireChangeShard"))
                                          .toList();
        Assert.assertEquals(1, changes.size());
        Assert.assertEquals(sharedPartitions.get(0), changes.get(0).getArgument(0));
        Assert.assertEquals(result, changes.get(0).getArgument(1));
        Assert.assertEquals(ConfChangeType.CONF_CHANGE_TYPE_ADJUST, changes.get(0).getArgument(2));
    }

    @Test
    public void testPreSubmittedDrainGroupIsNotDispatchedAgain() throws Exception {
        Fixture fixture = new Fixture();
        for (long id = 1; id <= 4; id++) {
            fixture.addStore(id, Metapb.StoreState.Up);
        }
        Metapb.Store source = fixture.stores.get(4L);
        Mockito.when(fixture.meta.getActiveStores())
               .thenAnswer(call -> new ArrayList<>(fixture.stores.values()));
        Metapb.ShardGroup submitted = group(0, 1, 2, 4);
        fixture.groups.put(0, submitted);
        fixture.groups.put(1, group(1, 1, 2, 4));
        Metapb.Partition fallback = Metapb.Partition.newBuilder()
                                                   .setId(1)
                                                   .setGraphName("graph").build();
        Mockito.when(fixture.partitions.getPartitionById(1))
               .thenReturn(List.of(fallback));
        Metapb.Store drain = source.toBuilder()
                                    .setState(Metapb.StoreState.Exiting).build();

        fixture.service.updateStore(drain, Set.of(0));

        Assert.assertEquals(submitted, fixture.groups.get(0));
        Assert.assertTrue(fixture.groups.get(1).getShardsList().stream()
                                 .noneMatch(shard -> shard.getStoreId() == 4));
        List<Invocation> changes = changeShardInvocations(fixture.partitions);
        Assert.assertEquals(1, changes.size());
        Assert.assertEquals(fallback, changes.get(0).getArgument(0));
    }

    @Test
    public void testInitialAllocationExcludesExitingStoreWithStaleUpLease() throws Exception {
        Fixture fixture = new Fixture();
        for (long id = 1; id <= 3; id++) {
            fixture.addStore(id, Metapb.StoreState.Up);
        }
        Metapb.Store source = fixture.addStore(4, Metapb.StoreState.Exiting);
        Mockito.when(fixture.meta.getActiveStores()).thenReturn(List.of(
                source.toBuilder().setState(Metapb.StoreState.Up).build(),
                fixture.stores.get(1L), fixture.stores.get(2L), fixture.stores.get(3L)));
        List<Metapb.Shard> shards = fixture.service.allocShards(
                Metapb.Graph.newBuilder().setGraphName("new-graph").build(), 0);
        Assert.assertEquals(3, shards.size());
        Assert.assertTrue(shards.stream().noneMatch(shard -> shard.getStoreId() == 4));
    }

    @Test
    public void testDrainingStoreCannotSatisfyPlacementMinimum() throws Exception {
        Fixture fixture = new Fixture();
        fixture.addStore(1, Metapb.StoreState.Up);
        fixture.addStore(2, Metapb.StoreState.Up);
        Metapb.Store source = fixture.addStore(3, Metapb.StoreState.Exiting);
        Mockito.when(fixture.meta.getActiveStores()).thenReturn(new ArrayList<>(fixture.stores.values()));
        Metapb.ShardGroup original = group(0, 1, 2, 3);
        fixture.groups.put(0, original);

        PDException error = Assert.assertThrows(PDException.class, () -> fixture.service.storeTurnoff(source));

        Assert.assertEquals(Pdpb.ErrorType.LESS_ACTIVE_STORE_VALUE, error.getErrorCode());
        Assert.assertEquals(original, fixture.groups.get(0));
        Assert.assertEquals(3, fixture.service.getActiveStores().size());
        Assert.assertEquals(2, fixture.service.getPlacementStores().size());
    }

    @Test
    public void testRepeatedDrainResumesAfterCapacityIsRestored() throws Exception {
        Fixture fixture = new Fixture();
        StoreStatusListener listener = Mockito.mock(StoreStatusListener.class);
        setField(fixture.service, "statusListeners", List.of(listener));
        fixture.addStore(1, Metapb.StoreState.Up);
        fixture.addStore(2, Metapb.StoreState.Up);
        // Retain compatibility with a drain intent persisted before a failed capacity check.
        Metapb.Store source = fixture.addStore(3, Metapb.StoreState.Exiting);
        Mockito.when(fixture.meta.getActiveStores())
               .thenAnswer(call -> new ArrayList<>(fixture.stores.values()));
        Metapb.ShardGroup original = group(0, 1, 2, 3);
        fixture.groups.put(0, original);
        Metapb.Partition partition = Metapb.Partition.newBuilder()
                                                   .setId(0).setGraphName("graph").build();
        Mockito.when(fixture.partitions.getPartitionById(0)).thenReturn(List.of(partition));
        Metapb.Store drain = source.toBuilder().setState(Metapb.StoreState.Exiting).build();

        PDException error = Assert.assertThrows(PDException.class,
                                                () -> fixture.service.updateStore(drain));
        Assert.assertEquals(Pdpb.ErrorType.LESS_ACTIVE_STORE_VALUE, error.getErrorCode());
        Assert.assertEquals(Metapb.StoreState.Exiting, fixture.stores.get(3L).getState());
        Assert.assertEquals(original, fixture.groups.get(0));
        Assert.assertTrue(changeShardInvocations(fixture.partitions).isEmpty());

        // A partial metadata update is not an explicit retry of the drain intent.
        fixture.service.updateStore(Metapb.Store.newBuilder().setId(3)
                                               .setVersion("metadata-only").build());
        Assert.assertEquals(original, fixture.groups.get(0));

        fixture.addStore(4, Metapb.StoreState.Up);
        fixture.service.updateStore(drain);

        List<Metapb.Shard> expected = fixture.groups.get(0).getShardsList();
        Assert.assertEquals(3, expected.size());
        Assert.assertTrue(expected.stream().noneMatch(shard -> shard.getStoreId() == 3));
        Assert.assertTrue(expected.stream().anyMatch(shard -> shard.getStoreId() == 4));
        Assert.assertEquals(Metapb.StoreState.Exiting, fixture.stores.get(3L).getState());
        List<Invocation> changes = changeShardInvocations(fixture.partitions);
        Assert.assertEquals(1, changes.size());
        Assert.assertEquals(partition, changes.get(0).getArgument(0));
        Assert.assertEquals(expected, changes.get(0).getArgument(1));
        Assert.assertEquals(ConfChangeType.CONF_CHANGE_TYPE_ADJUST, changes.get(0).getArgument(2));

        // Planning removed the source from the PD group; another identical request must
        // not resubmit that group or repeat a Store-state notification. This is not a
        // barrier proving that the actual Raft membership has committed the target.
        fixture.service.updateStore(drain);
        Assert.assertEquals(1, changeShardInvocations(fixture.partitions).size());
        Mockito.verifyNoInteractions(listener);
    }

    @Test
    public void testForceTombstoneUpRejectsInsufficientCapacityBeforePersistence() throws Exception {
        assertForceTombstoneRejectsInsufficientCapacity(Metapb.StoreState.Up);
    }

    @Test
    public void testForceTombstoneOfflineRejectsInsufficientCapacityBeforePersistence() throws Exception {
        assertForceTombstoneRejectsInsufficientCapacity(Metapb.StoreState.Offline);
    }

    private static void assertForceTombstoneRejectsInsufficientCapacity(Metapb.StoreState state)
            throws Exception {
        Fixture fixture = new Fixture();
        fixture.config.setMinStoreCount(0);
        fixture.addStore(1, Metapb.StoreState.Up);
        fixture.addStore(2, Metapb.StoreState.Up);
        Metapb.Store source = fixture.addStore(3, state);
        Mockito.when(fixture.meta.getActiveStores())
               .thenAnswer(call -> new ArrayList<>(fixture.stores.values()));
        Metapb.ShardGroup original = group(0, 1, 2, 3);
        fixture.groups.put(0, original);

        PDException error = Assert.assertThrows(PDException.class, () -> fixture.service.updateStore(
                source.toBuilder().setState(Metapb.StoreState.Tombstone).build()));

        Assert.assertEquals(Pdpb.ErrorType.LESS_ACTIVE_STORE_VALUE, error.getErrorCode());
        Assert.assertEquals(source, fixture.stores.get(3L));
        Assert.assertEquals(original, fixture.groups.get(0));
        Mockito.verify(fixture.meta, Mockito.never()).updateStore(Mockito.any());
        Mockito.verify(fixture.meta, Mockito.never()).removeActiveStore(Mockito.any());
        Assert.assertTrue(changeShardInvocations(fixture.partitions).isEmpty());
    }

    @Test
    public void testForceTombstoneWithoutGroupsDoesNotRequirePlacementCapacity() throws Exception {
        for (Metapb.StoreState state : List.of(Metapb.StoreState.Up, Metapb.StoreState.Offline)) {
            Fixture fixture = new Fixture();
            Metapb.Store source = fixture.addStore(3, state);
            Mockito.when(fixture.meta.getActiveStores()).thenReturn(List.of(source));

            Metapb.Store retired = fixture.service.updateStore(
                    source.toBuilder().setState(Metapb.StoreState.Tombstone).build());

            Assert.assertEquals(Metapb.StoreState.Tombstone, retired.getState());
            Assert.assertEquals(retired, fixture.stores.get(3L));
            Assert.assertTrue(changeShardInvocations(fixture.partitions).isEmpty());
        }
    }

    @Test
    public void testTombstoneRetryDoesNotReplanReferencedGroup() throws Exception {
        Fixture fixture = new Fixture();
        Metapb.Store retired = fixture.addStore(3, Metapb.StoreState.Tombstone);
        Metapb.ShardGroup original = group(0, 1, 2, 3);
        fixture.groups.put(0, original);

        Metapb.Store updated = fixture.service.updateStore(retired.toBuilder()
                                                                .setVersion("metadata-only").build());

        Assert.assertEquals(Metapb.StoreState.Tombstone, updated.getState());
        Assert.assertEquals("metadata-only", updated.getVersion());
        Assert.assertEquals(original, fixture.groups.get(0));
        Assert.assertTrue(changeShardInvocations(fixture.partitions).isEmpty());
        Mockito.verify(fixture.meta, Mockito.never()).removeActiveStore(Mockito.any());
    }

    @Test
    public void testManagementCannotReviveTombstoneButAllowsPartialMetadataUpdate() throws Exception {
        Fixture fixture = new Fixture();
        Metapb.Store retired = fixture.addStore(3, Metapb.StoreState.Tombstone);
        for (Metapb.StoreState state : List.of(Metapb.StoreState.Up,
                                               Metapb.StoreState.Exiting,
                                               Metapb.StoreState.Offline,
                                               Metapb.StoreState.Pending)) {
            PDException error = Assert.assertThrows(PDException.class, () ->
                    fixture.service.updateStore(retired.toBuilder().setState(state).build()));
            Assert.assertEquals(Pdpb.ErrorType.STORE_HAS_BEEN_REMOVED_VALUE, error.getErrorCode());
            Assert.assertEquals(retired, fixture.stores.get(3L));
        }
        Metapb.Store partial = Metapb.Store.newBuilder().setId(3)
                                         .setState(Metapb.StoreState.Unknown)
                                         .setVersion("updated").build();
        Metapb.Store updated = fixture.service.updateStore(partial);
        Assert.assertEquals(Metapb.StoreState.Tombstone, updated.getState());
        Assert.assertEquals("updated", updated.getVersion());
        Assert.assertEquals(updated, fixture.service.updateStore(updated));
    }

    private static List<Invocation> changeShardInvocations(PartitionService partitions) {
        return Mockito.mockingDetails(partitions).getInvocations().stream()
                      .filter(invocation -> invocation.getMethod().getName().equals("fireChangeShard"))
                      .toList();
    }

    @Test
    public void testDrainIsBlockedWhenOtherLiveStoresCannotReceiveReplicas() throws Exception {
        Fixture fixture = new Fixture();
        Metapb.Store source = fixture.addStore(1, Metapb.StoreState.Up);
        fixture.addStore(2, Metapb.StoreState.Exiting);
        fixture.addStore(3, Metapb.StoreState.Pending);
        Mockito.when(fixture.meta.getActiveStores()).thenReturn(new ArrayList<>(fixture.stores.values()));
        fixture.groups.put(0, group(0, 1));
        Mockito.when(fixture.partitions.getPartitions()).thenReturn(List.of(
                Metapb.Partition.newBuilder().setId(0).setGraphName("graph-a").build()));
        TaskScheduleService tasks = Mockito.mock(TaskScheduleService.class, Mockito.CALLS_REAL_METHODS);
        Field storesField = TaskScheduleService.class.getDeclaredField("storeService");
        storesField.setAccessible(true);
        storesField.set(tasks, fixture.service);
        Field partitionsField = TaskScheduleService.class.getDeclaredField("partitionService");
        partitionsField.setAccessible(true);
        partitionsField.set(tasks, fixture.partitions);
        RaftEngine raft = RaftEngine.getInstance();
        Field nodeField = RaftEngine.class.getDeclaredField("raftNode");
        nodeField.setAccessible(true);
        Object previous = nodeField.get(raft);
        Node node = Mockito.mock(Node.class);
        Mockito.when(node.isLeader(true)).thenReturn(true);
        nodeField.set(raft, node);
        try {
            Map<String, Object> plan = tasks.canAllPartitionsMovedOut(source);
            Assert.assertEquals(true, plan.get("current_store_is_online"));
            Assert.assertEquals(false, plan.get("flag"));
            Assert.assertNull(plan.get("movedPartitions"));
        } finally {
            nodeField.set(raft, previous);
        }
    }

    @Test
    public void testReregisteringExitingStorePreservesDrainIntent() throws Exception {
        Fixture fixture = new Fixture();
        Metapb.Store source = fixture.addStore(4, Metapb.StoreState.Exiting);
        fixture.config.setInitialStoreList("source:8500");
        source = source.toBuilder().setAddress("source:8500").build();
        fixture.stores.put(4L, source);

        Metapb.Store registered = fixture.service.register(source);

        Assert.assertEquals(Metapb.StoreState.Exiting, registered.getState());
        Mockito.verify(fixture.meta).keepStoreAlive(registered);
        Assert.assertTrue(fixture.service.getPlacementStores().isEmpty());
    }

    @Test
    public void testHeartbeatCannotOverwriteConcurrentDrainIntent() throws Exception {
        assertStateUpdateSerializesWithDrain(false);
    }

    @Test
    public void testRegistrationCannotOverwriteConcurrentDrainIntent() throws Exception {
        assertStateUpdateSerializesWithDrain(true);
    }

    private void assertStateUpdateSerializesWithDrain(boolean registration) throws Exception {
        Fixture fixture = new Fixture();
        for (long id = 1; id <= 3; id++) {
            fixture.addStore(id, Metapb.StoreState.Up);
        }
        Metapb.Store source = fixture.addStore(4, Metapb.StoreState.Up);
        Mockito.when(fixture.meta.getActiveStores()).thenReturn(new ArrayList<>(fixture.stores.values()));
        CountDownLatch readUp = new CountDownLatch(1);
        CountDownLatch continueUpdate = new CountDownLatch(1);
        CountDownLatch drainRequested = new CountDownLatch(1);
        CountDownLatch drainDone = new CountDownLatch(1);
        AtomicBoolean firstRead = new AtomicBoolean(true);
        Mockito.when(fixture.meta.getStore(4L)).thenAnswer(call -> {
            Metapb.Store observed = fixture.stores.get(4L);
            if (firstRead.compareAndSet(true, false)) {
                readUp.countDown();
                Assert.assertTrue(continueUpdate.await(5, TimeUnit.SECONDS));
            }
            return observed;
        });
        ExecutorService workers = Executors.newFixedThreadPool(2);
        try {
            Future<?> update = workers.submit(() -> {
                if (registration) {
                    fixture.service.register(source);
                } else {
                    fixture.heartbeat(4, 3);
                }
                return null;
            });
            Assert.assertTrue(readUp.await(5, TimeUnit.SECONDS));
            Future<?> drain = workers.submit(() -> {
                drainRequested.countDown();
                fixture.service.updateStore(source.toBuilder().setState(Metapb.StoreState.Exiting).build());
                drainDone.countDown();
                return null;
            });
            Assert.assertTrue(drainRequested.await(5, TimeUnit.SECONDS));
            Assert.assertFalse(drainDone.await(200, TimeUnit.MILLISECONDS));
            continueUpdate.countDown();
            update.get(5, TimeUnit.SECONDS);
            drain.get(5, TimeUnit.SECONDS);
            Assert.assertEquals(Metapb.StoreState.Exiting, fixture.stores.get(4L).getState());
        } finally {
            continueUpdate.countDown();
            workers.shutdownNow();
        }
    }

    private static Metapb.ShardGroup group(int id, long... storeIds) {
        Metapb.ShardGroup.Builder group = Metapb.ShardGroup.newBuilder().setId(id);
        for (int i = 0; i < storeIds.length; i++) {
            group.addShards(Metapb.Shard.newBuilder().setStoreId(storeIds[i])
                                  .setRole(i == 0 ? Metapb.ShardRole.Leader :
                                           Metapb.ShardRole.Follower));
        }
        return group.build();
    }

    private static void setField(StoreNodeService service, String name, Object value)
            throws Exception {
        Field field = StoreNodeService.class.getDeclaredField(name);
        field.setAccessible(true);
        field.set(service, value);
    }

    private static class Fixture {
        private final StoreNodeService service =
                Mockito.mock(StoreNodeService.class, Mockito.CALLS_REAL_METHODS);
        private final StoreInfoMeta meta = Mockito.mock(StoreInfoMeta.class);
        private final PartitionService partitions = Mockito.mock(PartitionService.class);
        private final Map<Long, Metapb.Store> stores = new HashMap<>();
        private final Map<Integer, Metapb.ShardGroup> groups = new HashMap<>();
        private final PDConfig config = new PDConfig();

        private Fixture() throws Exception {
            config.setMinStoreCount(3);
            PDConfig.Partition partition = config.new Partition();
            partition.setShardCount(3);
            config.setPartition(partition);
            ConfigService configService = Mockito.mock(ConfigService.class);
            Mockito.when(configService.getPartitionCount()).thenReturn(1);
            config.setConfigService(configService);
            setField(service, "pdConfig", config);
            setField(service, "storeInfoMeta", meta);
            setField(service, "partitionService", partitions);
            setField(service, "shardGroupStatusListeners", List.of());
            setField(service, "statusListeners", List.of());
            setField(service, "clusterStats", Metapb.ClusterStats.getDefaultInstance());
            Mockito.when(meta.getActiveStores()).thenReturn(List.of());
            Mockito.when(meta.getStore(Mockito.anyLong())).thenAnswer(call ->
                    stores.get((Long) call.getArgument(0)));
            Mockito.when(meta.storeExists(Mockito.anyLong())).thenAnswer(call ->
                    stores.containsKey((Long) call.getArgument(0)));
            Mockito.when(meta.getShardGroup(Mockito.anyInt())).thenAnswer(call ->
                    groups.get((Integer) call.getArgument(0)));
            Mockito.when(meta.getShardGroups()).thenAnswer(call -> new ArrayList<>(groups.values()));
            Mockito.doAnswer(call -> {
                Metapb.Store store = call.getArgument(0);
                stores.put(store.getId(), store);
                return null;
            }).when(meta).updateStore(Mockito.any());
            Mockito.doAnswer(call -> {
                Metapb.ShardGroup group = call.getArgument(0);
                groups.put(group.getId(), group);
                return null;
            }).when(meta).updateShardGroup(Mockito.any());
        }

        private Metapb.Store addStore(long id, Metapb.StoreState state) {
            Metapb.Store store = Metapb.Store.newBuilder().setId(id).setState(state).build();
            stores.put(id, store);
            return store;
        }

        private void heartbeat(long id, int groups) throws Exception {
            service.heartBeat(Metapb.StoreStats.newBuilder().setStoreId(id)
                                              .setPartitionCount(groups).build());
        }
    }
}
