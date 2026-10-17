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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.ToLongBiFunction;
import java.util.function.ToLongFunction;
import java.util.function.UnaryOperator;

import org.apache.hugegraph.pd.StoreNodeService;
import org.apache.hugegraph.pd.TaskScheduleService;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.pd.meta.MetadataFactory;
import org.junit.Before;
import org.junit.Test;

public class TaskScheduleServiceTest extends PDCoreTestBase {

    TaskScheduleService service;

    @Before
    public void init() {
        this.service = getTaskService();
    }

    // TODO
    public void testStoreOffline() {

    }

    // TODO
    public void testPatrolStores() {

    }

    // TODO
    public void testPatrolPartitions() {

    }

    // TODO
    public void testBalancePartitionShard() {

    }

    @Test
    public void testBalancePartitionLeader() throws PDException {
        var list = new ArrayList<Metapb.Partition>();
        for (int i = 0; i < 6; i++) {
            getStoreNodeService().getStoreInfoMeta().updateShardGroup(genShardGroup(i));
            list.add(genPartition(i));
        }

        getPdConfig().getPartition().setShardCount(3);

        getPartitionService().updatePartition(list);
        var rst = this.service.balancePartitionLeader(true);
        assertFalse(rst.isEmpty());
        // recover
        getPdConfig().getPartition().setShardCount(1);
        getStoreNodeService().getStoreInfoMeta().removeAll();
    }

    @Test
    public void testAutoBalancePartitionLeaderWithinSkew() throws PDException {
        // leaders spread 2/2/2 over the three stores
        var rst = autoBalance(new long[]{1, 2, 3, 1, 2, 3}, 1, 2, 3);
        assertTrue(rst.isEmpty());
    }

    @Test
    public void testAutoBalancePartitionLeaderOverSkew() throws PDException {
        // every leader on store 1, as after stores 2 and 3 restarted
        var rst = autoBalance(new long[]{1, 1, 1, 1, 1, 1}, 1, 2, 3);
        assertEquals(4, rst.size());
        assertFalse(rst.containsValue(1L));
    }

    @Test
    public void testAutoBalancePartitionLeaderWithInactiveStore() throws PDException {
        // store 3 holds shards but sends no heartbeat, so it cannot take a leader
        var rst = autoBalance(new long[]{1, 1, 1, 1, 1, 1}, 1, 2);
        assertTrue(rst.isEmpty());
    }

    @Test
    public void testAutoBalancePartitionLeaderWithWarnGroup() throws PDException {
        // store 3 just stopped: still in the active set, but its groups are already Warn
        var rst = autoBalance(new long[]{1, 1, 1, 1, 1, 1},
                              g -> g.toBuilder().setState(Metapb.PartitionState.PState_Warn)
                                    .build(), 0, 1, 2, 3);
        assertTrue(rst.isEmpty());

        // once the group is Normal again, the same imbalance is balanced
        rst = autoBalance(new long[]{1, 1, 1, 1, 1, 1}, g -> g, 0, 1, 2, 3);
        assertEquals(4, rst.size());
    }

    @Test
    public void testAutoBalancePartitionLeaderWithoutQuorum() throws PDException {
        // group 0 lost its quorum: no partition heartbeat for a minute, while its last state
        // is still Normal and every store is still in the active set
        var rst = autoBalance(new long[]{1, 1, 1, 1, 1, 1}, g -> g, 60 * 1000, 1, 2, 3);
        assertTrue(rst.isEmpty());

        // group 0 never reported stats at all
        rst = autoBalance(new long[]{1, 1, 1, 1, 1, 1}, g -> g, -1, 1, 2, 3);
        assertTrue(rst.isEmpty());

        // a heartbeat within the window: the same imbalance is balanced
        rst = autoBalance(new long[]{1, 1, 1, 1, 1, 1}, g -> g, 10 * 1000, 1, 2, 3);
        assertEquals(4, rst.size());
    }

    @Test
    public void testAutoBalancePartitionLeaderWithPendingMember() throws PDException {
        // reallocShards added the shard on store 3 and left its role None
        for (var role : List.of(Metapb.ShardRole.None, Metapb.ShardRole.Learner)) {
            var rst = autoBalance(new long[]{1, 1, 1, 1, 1, 1},
                                  g -> g.toBuilder().setShards(2, g.getShards(2).toBuilder()
                                                                   .setRole(role))
                                        .build(), 0, 1, 2, 3);
            assertTrue(role.name(), rst.isEmpty());
        }
    }

    @Test
    public void testAutoBalancePartitionLeaderWithChangeAfterCheck() throws PDException {
        // reallocShards turns store 3's shards into new None members right after the check
        // read the groups: the plan is made from the checked groups and no transfer is sent
        var rst = new Balance(1, 1, 1, 1, 1, 1)
                .afterRead(g -> g.toBuilder().setShards(2, g.getShards(2).toBuilder()
                                                        .setRole(Metapb.ShardRole.None))
                                 .build())
                .run();
        assertTrue(rst.isEmpty());
    }

    @Test
    public void testAutoBalancePartitionLeaderAfterLosingLeadership() throws PDException {
        // this PD is the leader when the run starts and loses leadership before it sends
        var calls = new AtomicInteger();
        var service = new TaskScheduleService(getPdConfig(), getStoreNodeService(),
                                              getPartitionService()) {
            @Override
            protected boolean isLeader() {
                return calls.getAndIncrement() == 0;
            }
        };
        var rst = new Balance(1, 1, 1, 1, 1, 1).on(service).run();
        assertTrue(rst.isEmpty());
        // the planned transfers were rechecked, not skipped before planning
        assertTrue(calls.get() > 1);
    }

    @Test
    public void testAutoBalancePartitionLeaderWithLaggingFollower() throws PDException {
        // store 3's replicas trail the leaders by 5000 entries or report no index: its share
        // of the leaders stays on store 1 and only store 2 takes leaders
        for (long store3Index : new long[]{5_000, -1}) {
            var rst = new Balance(1, 1, 1, 1, 1, 1)
                    .appliedIndex((storeId, groupId) -> storeId == 3 ? store3Index : 10_000)
                    .run();
            assertEquals(2, rst.size());
            assertTrue(rst.values().stream().allMatch(storeId -> storeId == 2L));
        }

        // 500 entries behind is close enough to take a leader
        var rst = new Balance(1, 1, 1, 1, 1, 1)
                .appliedIndex((storeId, groupId) -> storeId == 3 ? 9_500 : 10_000)
                .run();
        assertEquals(4, rst.size());
        assertTrue(rst.containsValue(3L));
    }

    @Test
    public void testAutoBalancePartitionLeaderWithOlderFollowerReport() throws PDException {
        // The leaders on store 1 apply 100 entries/s. Store 3's last heartbeat reached PD 15 s
        // before the others', so its caught-up replicas report 1500 entries less.
        ToLongFunction<Long> reportAge = storeId -> storeId == 3 ? 15_000 : 0;

        // With no earlier run to tell the write rate, store 3 looks 1500 behind and waits
        var rst = new Balance(1, 1, 1, 1, 1, 1)
                .appliedIndex((storeId, groupId) -> storeId == 3 ? 8_500 : 10_000)
                .reportAge(reportAge)
                .run();
        assertEquals(2, rst.size());
        assertTrue(rst.values().stream().allMatch(storeId -> storeId == 2L));

        // A run 30 s earlier saw the leaders at 7000 and skipped because group 0 was Warn.
        // At the rate that gives, store 3 is caught up and takes its share of the leaders.
        var service = newTaskService();
        rst = new Balance(1, 1, 1, 1, 1, 1).group0(warn(), 0)
                                           .appliedIndex((storeId, groupId) -> 7_000)
                                           .reportAge(storeId -> 30_000)
                                           .on(service).run();
        assertTrue(rst.isEmpty());
        rst = new Balance(1, 1, 1, 1, 1, 1)
                .appliedIndex((storeId, groupId) -> storeId == 3 ? 8_500 : 10_000)
                .reportAge(reportAge)
                .on(service).run();
        assertEquals(4, rst.size());
        assertTrue(rst.containsValue(3L));

        // At the same rate a report of 5000 from store 3 is 3500 behind, so it still waits
        service = newTaskService();
        new Balance(1, 1, 1, 1, 1, 1).group0(warn(), 0)
                                     .appliedIndex((storeId, groupId) -> 7_000)
                                     .reportAge(storeId -> 30_000)
                                     .on(service).run();
        rst = new Balance(1, 1, 1, 1, 1, 1)
                .appliedIndex((storeId, groupId) -> storeId == 3 ? 5_000 : 10_000)
                .reportAge(reportAge)
                .on(service).run();
        assertEquals(2, rst.size());
        assertTrue(rst.values().stream().allMatch(storeId -> storeId == 2L));
    }

    @Test
    public void testAutoBalancePartitionLeaderWithLateBurst() throws PDException {
        // The leaders went from 7000 to 10000 over 300 s, all of it in the last 15 s. Store 3
        // reported 7000 15 s before the others, so it was caught up when it reported. The
        // average rate is 10 entries/s, which puts the leader at about 9850 then: an estimated
        // gap of 2850. The estimate is best effort, so store 3's leaders wait for a later run.
        var service = newTaskService();
        warmUp(service, 7_000, 300_000);
        var rst = new Balance(1, 1, 1, 1, 1, 1)
                .appliedIndex((storeId, groupId) -> storeId == 3 ? 7_000 : 10_000)
                .reportAge(storeId -> storeId == 3 ? 15_000 : 0)
                .on(service).run();
        assertEquals(2, rst.size());
        assertTrue(rst.values().stream().allMatch(storeId -> storeId == 2L));
    }

    @Test
    public void testAutoBalancePartitionLeaderWithEarlyBurst() throws PDException {
        // The leaders went from 7000 to 10000 over 30 s. Store 3 reported 8500 15 s before the
        // others. At the average 100 entries/s that matches the leader then, so store 3 takes
        // its share. Had all the writes come before store 3's report, it was 1500 behind; the
        // estimate cannot tell, and JRaft's transfer timeout bounds what that costs.
        var service = newTaskService();
        warmUp(service, 7_000, 30_000);
        var rst = new Balance(1, 1, 1, 1, 1, 1)
                .appliedIndex((storeId, groupId) -> storeId == 3 ? 8_500 : 10_000)
                .reportAge(storeId -> storeId == 3 ? 15_000 : 0)
                .on(service).run();
        assertEquals(4, rst.size());
        assertTrue(rst.containsValue(3L));
    }

    @Test
    public void testAutoBalancePartitionLeaderWithNewerFollowerReport() throws PDException {
        // Store 3 reported 15 s after the others. The leaders went from 7000 to 10000 in the
        // 30 s before their report, 100 entries/s, so they were at about 11500 when store 3
        // reported.
        ToLongFunction<Long> reportAge = storeId -> storeId == 3 ? 0 : 15_000;

        // At 11400 store 3 is 100 behind the estimate and takes its share
        var service = newTaskService();
        warmUp(service, 7_000, 45_000);
        var rst = new Balance(1, 1, 1, 1, 1, 1)
                .appliedIndex((storeId, groupId) -> storeId == 3 ? 11_400 : 10_000)
                .reportAge(reportAge)
                .on(service).run();
        assertEquals(4, rst.size());
        assertTrue(rst.containsValue(3L));

        // At 10000 it is 1500 behind the estimate and waits
        service = newTaskService();
        warmUp(service, 7_000, 45_000);
        rst = new Balance(1, 1, 1, 1, 1, 1)
                .appliedIndex((storeId, groupId) -> 10_000)
                .reportAge(reportAge)
                .on(service).run();
        assertEquals(2, rst.size());
        assertTrue(rst.values().stream().allMatch(storeId -> storeId == 2L));

        // With no earlier run there is no rate, and the same reports compare as equal
        rst = new Balance(1, 1, 1, 1, 1, 1)
                .appliedIndex((storeId, groupId) -> 10_000)
                .reportAge(reportAge)
                .run();
        assertEquals(4, rst.size());
        assertTrue(rst.containsValue(3L));
    }

    /**
     * A run that skips because group 0 is Warn, so the service only records every store at
     * applied index `index` in a heartbeat received `age` ms before the run
     */
    private void warmUp(TaskScheduleService service, long index, long age) throws PDException {
        var rst = new Balance(1, 1, 1, 1, 1, 1).group0(warn(), 0)
                                               .appliedIndex((storeId, groupId) -> index)
                                               .reportAge(storeId -> age)
                                               .on(service).run();
        assertTrue(rst.isEmpty());
    }

    @Test
    public void testAutoBalancePartitionLeaderWithNegativeSkew() throws PDException {
        // with the default skew this layout moves four leaders; -1 must not run a plan
        getPdConfig().setBalanceLeaderMaxSkew(-1);
        try {
            var rst = autoBalance(new long[]{1, 1, 1, 1, 1, 1}, 1, 2, 3);
            assertTrue(rst.isEmpty());
        } finally {
            getPdConfig().setBalanceLeaderMaxSkew(2);
        }
    }

    @Test
    public void testInitSchedulesLeaderBalance() throws InterruptedException {
        // disabled, or enabled with an interval or skew out of range: no timer
        assertFalse(initSchedulesLeaderBalance(false, 1, 2));
        assertFalse(initSchedulesLeaderBalance(true, 0, 2));
        assertFalse(initSchedulesLeaderBalance(true, -5, 2));
        assertFalse(initSchedulesLeaderBalance(true, 1, -1));
        // enabled with valid settings, including a skew of 0: the timer runs the balance
        assertTrue(initSchedulesLeaderBalance(true, 1, 0));
        assertTrue(initSchedulesLeaderBalance(true, 1, 2));
    }

    /**
     * Runs init() with the given leader balance settings and returns whether it scheduled the
     * periodic leader balance. When it did, the timer must call the balance within 10 s.
     */
    private boolean initSchedulesLeaderBalance(boolean enable, long interval, int maxSkew) throws
                                                                                       InterruptedException {
        var config = getPdConfig();
        config.setBalanceLeaderEnable(enable);
        config.setBalanceLeaderInterval(interval);
        config.setBalanceLeaderMaxSkew(maxSkew);
        var ran = new CountDownLatch(1);
        var service = new TaskScheduleService(config, new StoreNodeService(config),
                                              getPartitionService()) {
            @Override
            public synchronized Map<Integer, Long> autoBalancePartitionLeader() {
                ran.countDown();
                return Map.of();
            }
        };
        try {
            service.init();
            boolean scheduled = service.isLeaderBalanceScheduled();
            if (scheduled) {
                assertTrue(ran.await(10, TimeUnit.SECONDS));
            }
            return scheduled;
        } finally {
            service.shutDown();
            config.setBalanceLeaderEnable(false);
            config.setBalanceLeaderInterval(300);
            config.setBalanceLeaderMaxSkew(2);
        }
    }

    private static UnaryOperator<Metapb.ShardGroup> warn() {
        return g -> g.toBuilder().setState(Metapb.PartitionState.PState_Warn).build();
    }

    // A fresh service, so a balance run by another test does not hit the rate limit
    private TaskScheduleService newTaskService() {
        return new TaskScheduleService(getPdConfig(), getStoreNodeService(),
                                       getPartitionService());
    }

    private Map<Integer, Long> autoBalance(long[] leaderStores, long... activeStores) throws
                                                                                 PDException {
        return new Balance(leaderStores).active(activeStores).run();
    }

    private Map<Integer, Long> autoBalance(long[] leaderStores,
                                           UnaryOperator<Metapb.ShardGroup> group0,
                                           long group0StatsAge,
                                           long... activeStores) throws PDException {
        return new Balance(leaderStores).group0(group0, group0StatsAge).active(activeStores)
                                        .run();
    }

    /**
     * One periodic balance over shard groups 0..n-1 on stores 1, 2 and 3, with the leader of
     * group i on leaderStores[i]. By default every group is Normal with fresh partition stats,
     * stores 1-3 are active and each reports applied index 10000 for every group in a
     * heartbeat PD received at the time of the run.
     */
    private class Balance {

        private final long[] leaderStores;
        private long[] activeStores = {1, 2, 3};
        private UnaryOperator<Metapb.ShardGroup> group0 = g -> g;
        private long group0StatsAge = 0;
        private ToLongBiFunction<Long, Integer> appliedIndex = (storeId, groupId) -> 10_000;
        private ToLongFunction<Long> reportAge = storeId -> 0;
        private UnaryOperator<Metapb.ShardGroup> afterRead;
        private TaskScheduleService service;

        Balance(long... leaderStores) {
            this.leaderStores = leaderStores;
        }

        // Stores marked alive; the others among 1-3 are removed from the active set
        Balance active(long... activeStores) {
            this.activeStores = activeStores;
            return this;
        }

        // Applied to group 0, whose stats are aged by statsAge ms (-1: no stats)
        Balance group0(UnaryOperator<Metapb.ShardGroup> group0, long statsAge) {
            this.group0 = group0;
            this.group0StatsAge = statsAge;
            return this;
        }

        // The applied index each store reports for each group (negative: not reported)
        Balance appliedIndex(ToLongBiFunction<Long, Integer> appliedIndex) {
            this.appliedIndex = appliedIndex;
            return this;
        }

        // How many ms before the run PD received each store's heartbeat
        Balance reportAge(ToLongFunction<Long> reportAge) {
            this.reportAge = reportAge;
            return this;
        }

        // Applied to every stored group right after the balance reads the group list, like a
        // change landing between check and transfer. Runs on a fresh service.
        Balance afterRead(UnaryOperator<Metapb.ShardGroup> afterRead) {
            this.afterRead = afterRead;
            return this;
        }

        // Runs on this service instead of a fresh one
        Balance on(TaskScheduleService service) {
            this.service = service;
            return this;
        }

        Map<Integer, Long> run() throws PDException {
            var storeMeta = getStoreNodeService().getStoreInfoMeta();
            var partitionMeta = MetadataFactory.newPartitionMeta(getPdConfig());
            // Set aside groups other tests in the suite left behind, so only ours are planned
            var otherGroups = storeMeta.getShardGroups();
            for (var group : otherGroups) {
                storeMeta.deleteShardGroup(group.getId());
            }
            var partitions = new ArrayList<Metapb.Partition>();
            long now = System.currentTimeMillis();
            for (int i = 0; i < leaderStores.length; i++) {
                var group = genShardGroup(i, leaderStores[i],
                                          Metapb.PartitionState.PState_Normal);
                long statsAge = 0;
                if (i == 0) {
                    group = group0.apply(group);
                    statsAge = group0StatsAge;
                }
                storeMeta.updateShardGroup(group);
                if (statsAge >= 0) {
                    partitionMeta.updatePartitionStats(Metapb.PartitionStats.newBuilder()
                                                                            .setId(i)
                                                                            .setTimestamp(
                                                                                    now - statsAge)
                                                                            .build());
                }
                partitions.add(genPartition(i));
            }
            for (long storeId = 1; storeId <= 3; storeId++) {
                var stats = Metapb.StoreStats.newBuilder();
                for (int i = 0; i < leaderStores.length; i++) {
                    long index = appliedIndex.applyAsLong(storeId, i);
                    if (index >= 0) {
                        stats.addRaftStats(Metapb.RaftStats.newBuilder().setPartitionId(i)
                                                           .setCommittedIndex(index));
                    }
                }
                var store = Metapb.Store.newBuilder().setId(storeId).setStats(stats)
                                        .setLastHeartbeat(now - reportAge.applyAsLong(storeId))
                                        .build();
                if (Arrays.stream(activeStores).anyMatch(id -> id == store.getId())) {
                    storeMeta.keepStoreAlive(store);
                } else {
                    storeMeta.removeActiveStore(store);
                }
            }
            getPdConfig().getPartition().setShardCount(3);
            getPartitionService().updatePartition(partitions);
            try {
                var balanceService = service;
                if (balanceService == null) {
                    StoreNodeService storeService = getStoreNodeService();
                    if (afterRead != null) {
                        storeService = new StoreNodeService(getPdConfig()) {
                            @Override
                            public List<Metapb.ShardGroup> getShardGroups() throws PDException {
                                var groups = super.getShardGroups();
                                for (var group : groups) {
                                    storeMeta.updateShardGroup(afterRead.apply(group));
                                }
                                return groups;
                            }
                        };
                    }
                    balanceService = new TaskScheduleService(getPdConfig(), storeService,
                                                             getPartitionService());
                }
                return balanceService.autoBalancePartitionLeader();
            } finally {
                getPdConfig().getPartition().setShardCount(1);
                for (long storeId : activeStores) {
                    storeMeta.removeActiveStore(Metapb.Store.newBuilder().setId(storeId)
                                                            .build());
                }
                partitionMeta.removePartitionStats("");
                for (int i = 0; i < leaderStores.length; i++) {
                    storeMeta.deleteShardGroup(i);
                }
                for (var group : otherGroups) {
                    storeMeta.updateShardGroup(group);
                }
            }
        }
    }

    // TODO
    public void testSplitPartition() {

    }

    // TODO
    public void testSplitPartition2() {

    }

    // TODO
    public void testCanAllPartitionsMovedOut() {

    }

    private Metapb.ShardGroup genShardGroup(int groupId) {
        return Metapb.ShardGroup.newBuilder()
                                .setId(groupId)
                                .addAllShards(genShards())
                                .build();
    }

    private Metapb.Partition genPartition(int groupId) {
        return Metapb.Partition.newBuilder()
                               .setId(groupId)
                               .setState(Metapb.PartitionState.PState_Normal)
                               .setGraphName("graph1")
                               .setStartKey(groupId * 10L)
                               .setEndKey(groupId * 10L + 10)
                               .build();
    }

    private Metapb.ShardGroup genShardGroup(int groupId, long leaderStore,
                                            Metapb.PartitionState state) {
        var shards = new ArrayList<Metapb.Shard>();
        for (long storeId = 1; storeId <= 3; storeId++) {
            var role = storeId == leaderStore ? Metapb.ShardRole.Leader :
                       Metapb.ShardRole.Follower;
            shards.add(Metapb.Shard.newBuilder().setStoreId(storeId).setRole(role).build());
        }
        return Metapb.ShardGroup.newBuilder()
                                .setId(groupId)
                                .setState(state)
                                .addAllShards(shards)
                                .build();
    }

    private List<Metapb.Shard> genShards() {
        return List.of(
                Metapb.Shard.newBuilder().setStoreId(1).setRole(Metapb.ShardRole.Leader).build(),
                Metapb.Shard.newBuilder().setStoreId(2).setRole(Metapb.ShardRole.Follower).build(),
                Metapb.Shard.newBuilder().setStoreId(3).setRole(Metapb.ShardRole.Follower).build());
    }

}
