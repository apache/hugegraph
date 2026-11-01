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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hugegraph.pd.ConfigService;
import org.apache.hugegraph.pd.PartitionService;
import org.apache.hugegraph.pd.StoreNodeService;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.config.PDConfig;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.pd.grpc.Pdpb;
import org.apache.hugegraph.pd.meta.ConfigMetaStore;
import org.apache.hugegraph.pd.meta.StoreInfoMeta;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class StoreNodeLifecycleTest {

    @Test
    public void testSingleStoreBootstrapAndPlacement() throws Exception {
        Fixture fixture = new Fixture();
        fixture.config.setInitialStoreCount(1);
        fixture.config.getPartition().setShardCount(1);
        fixture.addUp(1);
        fixture.live(1);

        fixture.assertHealth(Metapb.ClusterState.Cluster_OK);
        Assert.assertEquals(1, fixture.allocate(0).size());
        fixture.assertHealth(Metapb.ClusterState.Cluster_OK);
    }

    @Test
    public void testBootstrapWaitsForInitialCountOnlyWithoutGroups() throws Exception {
        Fixture fixture = new Fixture();
        fixture.addUp(1, 2);
        fixture.live(1, 2);
        fixture.config.setInitialStoreCount(3);
        fixture.assertHealth(Metapb.ClusterState.Cluster_Not_Ready);
        fixture.addUp(3);
        fixture.live(1, 2, 3);
        fixture.assertHealth(Metapb.ClusterState.Cluster_OK);
        fixture.live();
        fixture.assertHealth(Metapb.ClusterState.Cluster_Not_Ready);
    }

    @Test
    public void testBootstrapRequiresEligibleStoresAndReplicaCapacity() throws Exception {
        Fixture fixture = new Fixture();
        fixture.config.setInitialStoreCount(1);
        fixture.addUp(1);
        fixture.live(1);
        fixture.assertHealth(Metapb.ClusterState.Cluster_Not_Ready);
        fixture.addUp(2, 3);
        fixture.live(1, 2, 3);
        fixture.stores.put(3L, fixture.stores.get(3L).toBuilder()
                                         .setState(Metapb.StoreState.Pending).build());
        fixture.assertHealth(Metapb.ClusterState.Cluster_Not_Ready);
        Assert.assertThrows(PDException.class, () -> fixture.allocate(0));
        // An operator must explicitly admit Pending capacity before placement can use it.
        fixture.service.updateStore(fixture.stores.get(3L).toBuilder()
                                              .setState(Metapb.StoreState.Up).build());
        fixture.assertHealth(Metapb.ClusterState.Cluster_OK);
        fixture.config.setMinStoreCount(4);
        fixture.assertHealth(Metapb.ClusterState.Cluster_Not_Ready);
        fixture.addUp(4);
        fixture.live(1, 2, 3, 4);
        fixture.assertHealth(Metapb.ClusterState.Cluster_OK);
    }

    @Test
    public void testEstablishedVotingMajorityIgnoresBootstrapAndPlacementFloor() throws Exception {
        Fixture fixture = new Fixture();
        fixture.addUp(1, 2, 3);
        fixture.live(1, 2);
        fixture.groups.put(0, group(0, 1, 2, 3));
        fixture.config.setInitialStoreCount(5);
        fixture.config.setMinStoreCount(5);
        fixture.assertHealth(Metapb.ClusterState.Cluster_OK);
        fixture.live(1);
        fixture.assertHealth(Metapb.ClusterState.Cluster_Not_Ready);
    }

    @Test
    public void testLearnersDoNotReplaceLiveVotersAndExitingVoterStillServes() throws Exception {
        Fixture fixture = new Fixture();
        fixture.addUp(1, 2, 3, 4);
        fixture.groups.put(0, group(0, 1, 2, 3).toBuilder()
                                   .addShards(shard(4, Metapb.ShardRole.Learner)).build());
        fixture.live(1, 4);
        fixture.assertHealth(Metapb.ClusterState.Cluster_Not_Ready);
        fixture.stores.put(1L, fixture.stores.get(1L).toBuilder().setState(Metapb.StoreState.Exiting).build());
        fixture.live(1, 2);
        fixture.assertHealth(Metapb.ClusterState.Cluster_OK);
        Assert.assertEquals(1, fixture.service.getPlacementStores().size());
        Assert.assertThrows(PDException.class, () -> fixture.allocate(1));
    }

    @Test
    public void testTargetNoneDoesNotVoteOrStopRetainedVotingMajority() throws Exception {
        Fixture fixture = new Fixture();
        fixture.addUp(1, 2, 3);
        fixture.groups.put(0, group(0, 1, 2).toBuilder()
                                   .addShards(shard(3, Metapb.ShardRole.None)).build());
        fixture.live(1, 2);
        fixture.assertHealth(Metapb.ClusterState.Cluster_OK);
        fixture.live(1, 3);
        fixture.assertHealth(Metapb.ClusterState.Cluster_Not_Ready);
        fixture.groups.put(0, Metapb.ShardGroup.newBuilder().setId(0)
                                             .addShards(shard(3, Metapb.ShardRole.None)).build());
        fixture.assertHealth(Metapb.ClusterState.Cluster_Not_Ready);
    }

    @Test
    public void testExitingHeartbeatRechecksWarnWithLiveVotingMajority() throws Exception {
        Fixture fixture = exitingSource(1, 2);
        fixture.service.updateClusterStatus(Metapb.PartitionState.PState_Warn);
        Metapb.ClusterStats stats = fixture.service.heartBeat(
                Metapb.StoreStats.newBuilder().setStoreId(1).setPartitionCount(1).build());
        Assert.assertEquals(Metapb.ClusterState.Cluster_OK, stats.getState());
        Assert.assertEquals(Metapb.StoreState.Exiting, fixture.stores.get(1L).getState());
        Assert.assertEquals(Metapb.PartitionState.PState_Warn, fixture.groups.get(0).getState());
    }

    @Test
    public void testExitingHeartbeatDoesNotTreatWarnAsAutomaticPermission() throws Exception {
        Fixture fixture = exitingSource(1);
        fixture.service.updateClusterStatus(Metapb.PartitionState.PState_Warn);
        Metapb.ClusterStats stats = fixture.service.heartBeat(
                Metapb.StoreStats.newBuilder().setStoreId(1).setPartitionCount(1).build());
        Assert.assertEquals(Metapb.ClusterState.Cluster_Not_Ready, stats.getState());
    }

    @Test
    public void testExitingHeartbeatPreservesExplicitFaultAndMaintenanceOffline() throws Exception {
        Fixture fixture = exitingSource(1, 2);
        for (Metapb.ClusterState state : List.of(Metapb.ClusterState.Cluster_Fault,
                                                Metapb.ClusterState.Cluster_Offline)) {
            fixture.service.updateClusterStatus(state);
            Metapb.ClusterStats stats = fixture.service.heartBeat(
                    Metapb.StoreStats.newBuilder().setStoreId(1).setPartitionCount(1).build());
            Assert.assertEquals(state, stats.getState());
        }
    }

    private static Fixture exitingSource(long... live) throws Exception {
        Fixture fixture = new Fixture();
        fixture.addUp(1, 2, 3);
        fixture.stores.put(1L, fixture.stores.get(1L).toBuilder().setState(Metapb.StoreState.Exiting).build());
        fixture.groups.put(0, group(0, 1, 2, 3).toBuilder().setState(Metapb.PartitionState.PState_Warn).build());
        fixture.live(live);
        return fixture;
    }

    @Test
    public void testStrictMajorityAndMalformedOrLearnerOnlyGroups() throws Exception {
        Fixture fixture = new Fixture();
        fixture.addUp(1, 2, 3, 4);
        fixture.live(1, 2);
        fixture.groups.put(0, group(0, 1, 2, 3, 4));
        fixture.assertHealth(Metapb.ClusterState.Cluster_Not_Ready);
        fixture.groups.put(0, Metapb.ShardGroup.newBuilder().setId(0)
                                             .addShards(shard(1, Metapb.ShardRole.Learner))
                                             .addShards(shard(2, Metapb.ShardRole.Learner)).build());
        fixture.assertHealth(Metapb.ClusterState.Cluster_Not_Ready);
        fixture.groups.put(0, group(0, 1, 1, 2));
        fixture.assertHealth(Metapb.ClusterState.Cluster_Not_Ready);
    }

    @Test
    public void testMetadataReadFailureCannotReportHealthy() throws Exception {
        Fixture fixture = new Fixture();
        fixture.addUp(1, 2, 3);
        fixture.live(1, 2, 3);
        Mockito.when(fixture.meta.getShardGroups()).thenThrow(
                new PDException(Pdpb.ErrorType.UNKNOWN_VALUE, "metadata unavailable"));
        fixture.assertHealth(Metapb.ClusterState.Cluster_Not_Ready);
    }

    @Test
    public void testNewGraphReusesExistingGroupWithoutBootstrapOrPlacement() throws Exception {
        Fixture fixture = new Fixture();
        fixture.addUp(1, 2, 3);
        fixture.live(1, 2);
        fixture.config.setInitialStoreCount(5);
        fixture.config.setMinStoreCount(5);
        Metapb.ShardGroup existing = group(0, 1, 2, 3);
        fixture.groups.put(0, existing);
        Assert.assertEquals(existing.getShardsList(), fixture.allocate(0));
        Mockito.verify(fixture.meta, Mockito.never()).updateShardGroup(Mockito.any());
    }

    @Test
    public void testNewGroupAfterBootstrapDoesNotOverwriteExistingMembership() throws Exception {
        Fixture fixture = new Fixture();
        fixture.addUp(1, 2, 3);
        fixture.live(1, 2, 3);
        fixture.config.setInitialStoreCount(5);
        Metapb.ShardGroup existing = group(0, 3, 2, 1).toBuilder().setConfVer(77).build();
        fixture.groups.put(0, existing);
        Assert.assertEquals(3, fixture.allocate(1).size());
        Assert.assertEquals(existing, fixture.groups.get(0));
        Mockito.verify(fixture.meta).updateShardGroup(fixture.groups.get(1));
    }

    @Test
    public void testBootstrapAllocationHonorsInitialCountEvenForOneReplica() throws Exception {
        Fixture fixture = new Fixture();
        fixture.addUp(1, 2, 3);
        fixture.live(1, 2, 3);
        fixture.config.getPartition().setShardCount(1);
        fixture.config.setInitialStoreCount(5);
        Assert.assertThrows(PDException.class, () -> fixture.allocate(0));
        Assert.assertTrue(fixture.groups.isEmpty());
    }

    @Test
    public void testInsufficientStoresNeverClampDesiredReplicaCount() throws Exception {
        Fixture fixture = new Fixture();
        fixture.addUp(1, 2, 3);
        fixture.live(1, 2);
        fixture.config.setInitialStoreCount(1);
        Assert.assertThrows(PDException.class, () -> fixture.allocate(0));
        Metapb.ShardGroup existing = group(0, 1, 2, 3);
        fixture.groups.put(0, existing);
        Assert.assertThrows(PDException.class, () -> fixture.service.reallocShards(existing));
        Assert.assertEquals(existing, fixture.groups.get(0));
        Assert.assertEquals(3, fixture.config.getPartition().getShardCount());
        Mockito.verify(fixture.meta, Mockito.never()).updateShardGroup(Mockito.any());
    }

    @Test
    public void testInvalidDesiredReplicaCountsAreRejectedWithoutMutation() throws Exception {
        Fixture fixture = new Fixture();
        fixture.addUp(1, 2, 3);
        fixture.live(1, 2, 3);
        fixture.config.setInitialStoreCount(1);
        for (int count : List.of(0, -1, 2)) {
            fixture.config.getPartition().setShardCount(count);
            PDException error = Assert.assertThrows(PDException.class, () -> fixture.allocate(0));
            Assert.assertEquals(Pdpb.ErrorType.SET_CONFIG_SHARD_COUNT_ERROR_VALUE, error.getErrorCode());
            Assert.assertTrue(fixture.groups.isEmpty());
        }
    }

    @Test
    public void testDrainStartsWithSourceQuorumAndThenCopiesOneReplicaElsewhere() throws Exception {
        Fixture fixture = new Fixture();
        fixture.addUp(1, 2);
        fixture.live(1, 2);
        fixture.config.getPartition().setShardCount(1);
        fixture.groups.put(0, group(0, 1));
        Assert.assertTrue(fixture.service.checkStoreCanOffline(fixture.stores.get(1L)));
        fixture.service.updateStore(fixture.stores.get(1L).toBuilder()
                                              .setState(Metapb.StoreState.Exiting).build());
        Assert.assertEquals(Metapb.StoreState.Exiting, fixture.stores.get(1L).getState());
        Assert.assertEquals(2, fixture.groups.get(0).getShards(0).getStoreId());
        fixture.live(2);
        fixture.groups.put(0, group(0, 1));
        Assert.assertFalse(fixture.service.checkStoreCanOffline(fixture.stores.get(1L)));
    }

    @Test
    public void testImpossibleDrainIsRejectedBeforePersistingIntent() throws Exception {
        Fixture fixture = new Fixture();
        fixture.addUp(1, 2, 3);
        fixture.live(1, 2, 3);
        Metapb.ShardGroup existing = group(0, 1, 2, 3);
        fixture.groups.put(0, existing);
        Assert.assertFalse(fixture.service.checkStoreCanOffline(fixture.stores.get(3L)));
        Assert.assertThrows(PDException.class, () -> fixture.service.updateStore(
                fixture.stores.get(3L).toBuilder().setState(Metapb.StoreState.Exiting).build()));
        Assert.assertEquals(Metapb.StoreState.Up, fixture.stores.get(3L).getState());
        Assert.assertEquals(existing, fixture.groups.get(0));
        Mockito.verify(fixture.meta, Mockito.never()).updateStore(Mockito.any());
    }

    @Test
    public void testExplicitFloorRestrictsPlacementAndDrainButNotExistingQuorum() throws Exception {
        Fixture fixture = new Fixture();
        fixture.addUp(1, 2, 3);
        fixture.live(1, 2, 3);
        fixture.config.getPartition().setShardCount(1);
        fixture.config.setMinStoreCount(4);
        fixture.groups.put(0, group(0, 1));
        fixture.assertHealth(Metapb.ClusterState.Cluster_OK);
        Assert.assertThrows(PDException.class, () -> fixture.allocate(1));
        Assert.assertFalse(fixture.service.checkStoreCanOffline(fixture.stores.get(3L)));
    }

    @Test
    public void testLegacyZeroFloorIsAutoAndDoesNotInheritBootstrapCount() throws Exception {
        Fixture fixture = new Fixture();
        fixture.config.setInitialStoreCount(5);
        fixture.config.setMinStoreCount(9);
        PolicyFixture policy = new PolicyFixture(fixture.config, policy(0));
        policy.service.loadConfig();
        Assert.assertEquals(0, fixture.config.getMinStoreCount());
        Assert.assertEquals(5, fixture.config.getInitialStoreCount());
        Mockito.verify(policy.meta, Mockito.never()).setPdConfig(Mockito.any());
    }

    @Test
    public void testFreshPolicyPersistsFloorAndLeaderReloadReappliesIt() throws Exception {
        Fixture fixture = new Fixture();
        fixture.config.setInitialStoreCount(5);
        fixture.config.setMinStoreCount(2);
        PolicyFixture policy = new PolicyFixture(fixture.config, null);
        policy.service.loadConfig();
        Assert.assertEquals(2, policy.stored.get().getMinStoreCount());
        policy.stored.set(policy(4));
        policy.service.onRaftLeaderChanged();
        Assert.assertEquals(4, fixture.config.getMinStoreCount());
        Assert.assertEquals(5, fixture.config.getInitialStoreCount());
    }

    @Test
    public void testPartialPolicyUpdatePreservesExistingFloor() throws Exception {
        Fixture fixture = new Fixture();
        PolicyFixture policy = new PolicyFixture(fixture.config, policy(3));
        policy.service.setPDConfig(Metapb.PDConfig.newBuilder().setShardCount(5).build());
        Assert.assertEquals(3, policy.stored.get().getMinStoreCount());
        Assert.assertEquals(3, fixture.config.getMinStoreCount());
        // Proto3 scalar zero lacks presence: preserve old partial-update compatibility.
        policy.service.setPDConfig(policy.stored.get().toBuilder().setMinStoreCount(0).build());
        Assert.assertEquals(3, policy.stored.get().getMinStoreCount());
    }

    private static Metapb.PDConfig policy(int minimum) {
        return Metapb.PDConfig.newBuilder().setMinStoreCount(minimum).setShardCount(3)
                              .setPartitionCount(2).setMaxShardsPerStore(3).setVersion(1).build();
    }

    private static Metapb.Shard shard(long id, Metapb.ShardRole role) {
        return Metapb.Shard.newBuilder().setStoreId(id).setRole(role).build();
    }

    private static Metapb.ShardGroup group(int id, long... storeIds) {
        Metapb.ShardGroup.Builder group = Metapb.ShardGroup.newBuilder().setId(id);
        for (int i = 0; i < storeIds.length; i++) {
            group.addShards(shard(storeIds[i], i == 0 ? Metapb.ShardRole.Leader :
                                 Metapb.ShardRole.Follower));
        }
        return group.build();
    }

    private static void set(Object instance, Class<?> type, String fieldName, Object value) throws Exception {
        Field field = type.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(instance, value);
    }

    private static class PolicyFixture {
        private final ConfigService service = Mockito.mock(ConfigService.class, Mockito.CALLS_REAL_METHODS);
        private final ConfigMetaStore meta = Mockito.mock(ConfigMetaStore.class);
        private final AtomicReference<Metapb.PDConfig> stored;

        private PolicyFixture(PDConfig config, Metapb.PDConfig initial) throws Exception {
            stored = new AtomicReference<>(initial);
            set(service, ConfigService.class, "pdConfig", config);
            set(service, ConfigService.class, "meta", meta);
            config.setConfigService(service);
            Mockito.when(meta.getPdConfig(0)).thenAnswer(call -> stored.get());
            Mockito.when(meta.setPdConfig(Mockito.any())).thenAnswer(call -> {
                stored.set(call.getArgument(0));
                return stored.get();
            });
        }
    }

    private static class Fixture {
        private final StoreNodeService service =
                Mockito.mock(StoreNodeService.class, Mockito.CALLS_REAL_METHODS);
        private final StoreInfoMeta meta = Mockito.mock(StoreInfoMeta.class);
        private final PartitionService partitions = Mockito.mock(PartitionService.class);
        private final PDConfig config = new PDConfig();
        private final Map<Long, Metapb.Store> stores = new HashMap<>();
        private final Map<Integer, Metapb.ShardGroup> groups = new HashMap<>();
        private final Set<Long> live = new HashSet<>();

        private Fixture() throws Exception {
            config.setInitialStoreCount(3);
            config.setMinStoreCount(0);
            config.setInitialStoreList("store-1,store-2,store-3");
            PDConfig.Partition partition = config.new Partition();
            partition.setShardCount(3);
            partition.setTotalCount(2);
            partition.setMaxShardsPerStore(3);
            config.setPartition(partition);
            ConfigService policy = Mockito.mock(ConfigService.class);
            Mockito.when(policy.getPartitionCount()).thenReturn(2);
            config.setConfigService(policy);
            set(service, StoreNodeService.class, "pdConfig", config);
            set(service, StoreNodeService.class, "storeInfoMeta", meta);
            set(service, StoreNodeService.class, "partitionService", partitions);
            set(service, StoreNodeService.class, "statusListeners", List.of());
            set(service, StoreNodeService.class, "shardGroupStatusListeners", List.of());
            set(service, StoreNodeService.class, "clusterStats", Metapb.ClusterStats.newBuilder()
                     .setState(Metapb.ClusterState.Cluster_Not_Ready).build());
            Mockito.when(meta.getActiveStores()).thenAnswer(call ->
                    live.stream().map(stores::get).toList());
            Mockito.when(meta.getStore(Mockito.anyLong())).thenAnswer(call -> stores.get(call.getArgument(0)));
            Mockito.when(meta.getShardGroups()).thenAnswer(call -> new ArrayList<>(groups.values()));
            Mockito.when(meta.getShardGroup(Mockito.anyInt())).thenAnswer(call -> groups.get(call.getArgument(0)));
            Mockito.when(partitions.getPartitionById(Mockito.anyInt())).thenReturn(List.of());
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

        private void addUp(long... ids) {
            for (long id : ids) {
                stores.put(id, Metapb.Store.newBuilder().setId(id).setState(Metapb.StoreState.Up).build());
            }
        }

        private void live(long... ids) {
            live.clear();
            for (long id : ids) {
                live.add(id);
            }
        }

        private void assertHealth(Metapb.ClusterState expected) {
            service.checkStoreStatus();
            Assert.assertEquals(expected, service.getClusterStats().getState());
        }

        private List<Metapb.Shard> allocate(int group) throws PDException {
            return service.allocShards(Metapb.Graph.newBuilder().setGraphName("another-graph").build(), group);
        }
    }
}
