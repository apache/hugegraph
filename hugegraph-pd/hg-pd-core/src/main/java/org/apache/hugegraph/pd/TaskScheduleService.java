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

package org.apache.hugegraph.pd;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.apache.hugegraph.pd.common.KVPair;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.config.PDConfig;
import org.apache.hugegraph.pd.grpc.MetaTask;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.pd.grpc.Pdpb;
import org.apache.hugegraph.pd.meta.TaskInfoMeta;
import org.apache.hugegraph.pd.raft.RaftEngine;

import lombok.extern.slf4j.Slf4j;

/**
 * The task scheduling service checks the status of stores, resources, and partitions on a
 * regular basis, migrates data in a timely manner, and errors are on nodes
 * 1. Monitor whether the store is offline
 * 2. Check whether the replica of the partition is correct
 * 3. Check whether the working mode of the partition is correct
 * 4. Monitor whether the partition needs to be split and whether the split is completed
 */
@Slf4j
public class TaskScheduleService {

    private static final String BALANCE_SHARD_KEY = "BALANCE_SHARD_KEY";
    private static final String KEY_ENABLE_AUTO_BALANCE = "key/ENABLE_AUTO_BALANCE";
    // The dynamic balancing can only be carried out after the machine is offline for 30 minutes
    private final long TurnOffAndBalanceInterval = 30 * 60 * 1000;
    // leader balances the time interval
    private final long BalanceLeaderInterval = 30 * 1000;
    private final PDConfig pdConfig;
    private final long clusterStartTime;
    private StoreNodeService storeService;
    private PartitionService partitionService;
    private ScheduledExecutorService executor;
    private TaskInfoMeta taskInfoMeta;
    private StoreMonitorDataService storeMonitorDataService;
    private KvService kvService;
    private LogService logService;
    private long lastStoreTurnoffTime = 0;
    private long lastBalanceLeaderTime = 0;


    /**
     * Sort by value, then sort by key if values are the same.
     *
     * @param <K>
     * @param <V>
     */
    private static class KvPairComparator<K extends Comparable<K>, V extends Comparable<V>>
            implements Comparator<KVPair<K, V>> {

        private boolean ascend;

        public KvPairComparator(boolean ascend) {
            this.ascend = ascend;
        }

        @Override
        public int compare(KVPair<K, V> o1, KVPair<K, V> o2) {
            if (Objects.equals(o1.getValue(), o2.getValue())) {
                return o1.getKey().compareTo(o2.getKey()) * (ascend ? 1 : -1);
            }
            return (o1.getValue().compareTo(o2.getValue())) * (ascend ? 1 : -1);
        }
    }

    // First sort by value (in reverse order), then sort by key (in ascending order).
    private Comparator<KVPair<Long, Integer>> kvPairComparatorDesc = (o1, o2) -> {
        if (o1.getValue() == o2.getValue()) {
            return o2.getKey().compareTo(o1.getKey());
        }
        return o2.getValue().compareTo(o1.getValue());
    };

    public TaskScheduleService(PDConfig config, StoreNodeService storeService,
                               PartitionService partitionService) {
        this.pdConfig = config;
        this.storeService = storeService;
        this.partitionService = partitionService;
        this.taskInfoMeta = new TaskInfoMeta(config);
        this.logService = new LogService(pdConfig);
        this.storeMonitorDataService = new StoreMonitorDataService(pdConfig);
        this.clusterStartTime = System.currentTimeMillis();
        this.kvService = new KvService(pdConfig);
        this.executor = new ScheduledThreadPoolExecutor(16);
    }

    public void init() {
        executor.scheduleWithFixedDelay(() -> {
            try {
                patrolStores();
            } catch (Throwable e) {
                log.error("patrolStores exception: ", e);
            }

        }, 60, 60, TimeUnit.SECONDS);
        executor.scheduleWithFixedDelay(() -> {
            if (isLeader()) {
                kvService.clearTTLData();
            }
        }, 1000, 1000, TimeUnit.MILLISECONDS);
        executor.scheduleWithFixedDelay(
                () -> {
                    try {
                        if (isLeader()) {
                            storeService.getQuota();
                        }
                    } catch (Exception e) {
                        log.warn("get quota with error:", e);
                    }
                }, 2, 30,
                TimeUnit.SECONDS);
        // clean expired monitor data each 10 minutes, delay 3min.
        if (isLeader() && this.pdConfig.getStore().isMonitorDataEnabled()) {
            executor.scheduleAtFixedRate(() -> {
                Long expTill = System.currentTimeMillis() / 1000 -
                               this.pdConfig.getStore().getRetentionPeriod();
                log.debug("monitor data keys before " + expTill + " will be deleted");
                int records = 0;
                try {
                    for (Metapb.Store store : storeService.getStores()) {
                        int cnt =
                                this.storeMonitorDataService.removeExpiredMonitorData(store.getId(),
                                                                                      expTill);
                        log.debug("store id :{}, records:{}", store.getId(), cnt);
                        records += cnt;
                    }
                } catch (PDException e) {
                    throw new RuntimeException(e);
                }
                log.debug(String.format("%d records has been deleted", records));
            }, 180, 600, TimeUnit.SECONDS);
        }

        storeService.addStatusListener(new StoreStatusListener() {
            @Override
            public void onStoreStatusChanged(Metapb.Store store, Metapb.StoreState old,
                                             Metapb.StoreState status) {
                if (status == Metapb.StoreState.Tombstone) {
                    lastStoreTurnoffTime = System.currentTimeMillis();
                }
            }

            @Override
            public void onGraphChange(Metapb.Graph graph,
                                      Metapb.GraphState stateOld,
                                      Metapb.GraphState stateNew) {

            }

            @Override
            public void onStoreRaftChanged(Metapb.Store store) {

            }
        });
    }

    public void shutDown() {
        executor.shutdownNow();
    }

    private boolean isLeader() {
        return RaftEngine.getInstance().isLeader();
    }

    /**
     * Inspect all stores to see if they are online and have enough storage space
     */
    public List<Metapb.Store> patrolStores() throws PDException {
        if (!isLeader()) {
            return null;
        }

        List<Metapb.Store> changedStores = new ArrayList<>();
        // Check your store online status
        List<Metapb.Store> stores = storeService.getStores("");
        Map<Long, Metapb.Store> activeStores = storeService.getActiveStores("")
                                                           .stream().collect(
                        Collectors.toMap(Metapb.Store::getId, t -> t));
        for (Metapb.Store store : stores) {
            Metapb.Store changeStore = null;
            if ((store.getState() == Metapb.StoreState.Up
                 || store.getState() == Metapb.StoreState.Unknown)
                && !activeStores.containsKey(store.getId())) {
                // If you are not online, the modification status is offline
                changeStore = Metapb.Store.newBuilder(store)
                                          .setState(Metapb.StoreState.Offline)
                                          .build();
            }
            if (changeStore != null) {
                storeService.updateStore(changeStore);
                changedStores.add(changeStore);
            }
        }
        return changedStores;
    }

    /**
     * Inspect all partitions to check whether the number of replicas is correct and the number
     * of replicas in the shard group
     */
    public List<Metapb.Partition> patrolPartitions() throws PDException {
        if (!isLeader()) {
            return null;
        }

        // If the number of replicas is inconsistent, reallocate replicas
        for (Metapb.ShardGroup group : storeService.getShardGroups()) {
            if (group.getShardsCount() != pdConfig.getPartition().getShardCount()) {
                storeService.reallocShards(group);
                kvService.put(BALANCE_SHARD_KEY, "DOING", 180 * 1000);
            }
        }
        // Check if the shard is online.
        Map<Long, Metapb.Store> tombStores = storeService.getTombStores().stream().collect(
                Collectors.toMap(Metapb.Store::getId, t -> t));

        var partIds = new HashSet<Integer>();

        for (var pair : tombStores.entrySet()) {
            for (var partition : partitionService.getPartitionByStore(pair.getValue())) {
                if (partIds.contains(partition.getId())) {
                    continue;
                }
                partIds.add(partition.getId());

                storeService.storeTurnoff(pair.getValue());
                partitionService.shardOffline(partition, pair.getValue().getId());
            }

        }

        return null;
    }

    /**
     * Balance the number of partitions between stores
     * It takes half an hour for the machine to turn to UP before it can be dynamically balanced
     */
    public synchronized Map<Integer, KVPair<Long, Long>> balancePartitionShard() throws
                                                                                 PDException {
        log.info("balancePartitions starting, isleader:{}", isLeader());

        if (!isLeader()) {
            return null;
        }

        if (System.currentTimeMillis() - lastStoreTurnoffTime < TurnOffAndBalanceInterval) {
            return null;
        }

        int activeStores = storeService.getActiveStores().size();
        if (activeStores == 0) {
            log.warn("balancePartitionShard non active stores, skip to balancePartitionShard");
            return null;
        }

        // Avoid frequent calls. (When changing the number of replicas, you need to adjust the shard list, which in turn requires balancing the partitions.)
        // This will send duplicate commands and cause unpredictable results.
        // Serious cases will result in the deletion of the partition.
        if (Objects.equals(kvService.get(BALANCE_SHARD_KEY), "DOING")) {
            return null;
        }

        int totalShards = pdConfig.getConfigService().getPartitionCount() *
                          pdConfig.getPartition().getShardCount();
        int averageCount = totalShards / activeStores;
        int remainder = totalShards % activeStores;

        // Count the partitions on each store, StoreId -> PartitionID, ShardRole
        Map<Long, Map<Integer, Metapb.ShardRole>> partitionMap = new HashMap<>();
        storeService.getActiveStores().forEach(store -> {
            partitionMap.put(store.getId(), new HashMap<>());
        });

        // If it says “leaner,” it means the migration is in progress. Don't submit the task again.
        AtomicReference<Boolean> isLeaner = new AtomicReference<>(false);
        partitionService.getPartitions().forEach(partition -> {

            try {
                storeService.getShardList(partition.getId()).forEach(shard -> {
                    Long storeId = shard.getStoreId();
                    // Determine whether each shard is leaner or in an abnormal state.
                    if (shard.getRole() == Metapb.ShardRole.Learner
                        || partition.getState() != Metapb.PartitionState.PState_Normal) {
                        isLeaner.set(true);
                    }
                    if (partitionMap.containsKey(storeId)) {
                        partitionMap.get(storeId).put(partition.getId(), shard.getRole());
                    }
                });
            } catch (PDException e) {
                log.error("get partition {} shard list error:{}.", partition.getId(),
                          e.getMessage());
            }
        });

        if (isLeaner.get()) {
            log.warn("balancePartitionShard is doing, skip this balancePartitionShard task");
            return null;
        }

        // According to shard sort the quantity from highest to lowest
        List<KVPair<Long, Integer>> sortedList = new ArrayList<>();
        partitionMap.forEach((storeId, shards) -> {
            sortedList.add(new KVPair(storeId, shards.size()));
        });
        sortedList.sort(((o1, o2) -> o2.getValue().compareTo(o1.getValue())));
        // The largest heap, moved in store -> shard count
        PriorityQueue<KVPair<Long, Integer>> maxHeap = new PriorityQueue<>(sortedList.size(),
                                                                           (o1, o2) -> o2.getValue()
                                                                                         .compareTo(
                                                                                                 o1.getValue()));

        // of individual copies committedIndex
        Map<Integer, Map<Long, Long>> committedIndexMap = partitionService.getCommittedIndexStats();
        // Partition ID -->source StoreID, target StoreID
        Map<Integer, KVPair<Long, Long>> movedPartitions = new HashMap<>();
        // Remove redundant shards, traverse the stores in the order of shards from most to
        // least, and the remainder is allocated to the store with more shards first, reducing
        // the probability of migration
        for (int index = 0; index < sortedList.size(); index++) {
            long storeId = sortedList.get(index).getKey();
            if (!partitionMap.containsKey(storeId)) {
                log.error("cannot found storeId {} in partitionMap", storeId);
                return null;
            }
            Map<Integer, Metapb.ShardRole> shards = partitionMap.get(storeId);
            int targetCount = index < remainder ? averageCount + 1 : averageCount;
            //  Remove the redundant shards and add the source StoreID. is not a leader, and the
            //  partition is unique
            if (shards.size() > targetCount) {
                int movedCount = shards.size() - targetCount;
                log.info(
                        "balancePartitionShard storeId {}, shardsSize {}, targetCount {}, " +
                        "moveCount {}",
                        storeId, shards.size(), targetCount, movedCount);
                for (Iterator<Integer> iterator = shards.keySet().iterator();
                     movedCount > 0 && iterator.hasNext(); ) {
                    Integer id = iterator.next();

                    if (!movedPartitions.containsKey(id)) {
                        log.info("store {}, shard of partition {} can be moved", storeId, id);
                        movedPartitions.put(id, new KVPair<>(storeId, 0L));
                        movedCount--;
                    }
                }
            } else if (shards.size() < targetCount) {
                int addCount = targetCount - shards.size();
                log.info(
                        "balancePartitionShard storeId {}, shardsSize {}, targetCount {}, " +
                        "addCount {}",
                        storeId, shards.size(), targetCount, addCount);
                maxHeap.add(new KVPair<>(storeId, addCount));
            }
        }

        if (movedPartitions.size() == 0) {
            log.warn(
                    "movedPartitions is empty, totalShards:{} averageCount:{} remainder:{} " +
                    "sortedList:{}",
                    totalShards, averageCount, remainder, sortedList);
        }
        Iterator<Map.Entry<Integer, KVPair<Long, Long>>> moveIterator =
                movedPartitions.entrySet().iterator();

        while (moveIterator.hasNext()) {
            if (maxHeap.size() == 0) {
                break;
            }
            Map.Entry<Integer, KVPair<Long, Long>> moveEntry = moveIterator.next();
            int partitionId = moveEntry.getKey();
            long sourceStoreId = moveEntry.getValue().getKey();

            List<KVPair<Long, Integer>> tmpList = new ArrayList<>(maxHeap.size());
            while (maxHeap.size() > 0) {
                KVPair<Long, Integer> pair = maxHeap.poll();
                long destStoreId = pair.getKey();
                boolean destContains = false;
                if (partitionMap.containsKey(destStoreId)) {
                    destContains = partitionMap.get(destStoreId).containsKey(partitionId);
                }
                // If the destination store already contains the partition, take the store
                if (!destContains) {
                    moveEntry.getValue().setValue(pair.getKey());
                    log.info(
                            "balancePartitionShard will move partition {} from store {} to store " +
                            "{}",
                            moveEntry.getKey(),
                            moveEntry.getValue().getKey(),
                            moveEntry.getValue().getValue());
                    if (pair.getValue() > 1) {
                        pair.setValue(pair.getValue() - 1);
                        tmpList.add(pair);
                    }
                    break;
                }
                tmpList.add(pair);
            }
            maxHeap.addAll(tmpList);
        }

        kvService.put(BALANCE_SHARD_KEY, "DOING", 180 * 1000);

        // Start the migration
        movedPartitions.forEach((partId, storePair) -> {
            // Neither the source nor destination storeID is 0
            if (storePair.getKey() > 0 && storePair.getValue() > 0) {
                partitionService.movePartitionsShard(partId, storePair.getKey(),
                                                     storePair.getValue());
            } else {
                log.warn("balancePartitionShard key or value is zero, partId:{} storePair:{}",
                         partId, storePair);
            }
        });
        return movedPartitions;
    }

    /**
     * Balance the number of leaders of partitions between stores
     */
    public synchronized Map<Integer, Long> balancePartitionLeader(boolean immediately) throws
                                                                                       PDException {
        Map<Integer, Long> results = new HashMap<>();

        if (!isLeader()) {
            return results;
        }

        if (!immediately &&
            System.currentTimeMillis() - lastBalanceLeaderTime < BalanceLeaderInterval) {
            return results;
        }
        lastBalanceLeaderTime = System.currentTimeMillis();

        List<Metapb.ShardGroup> shardGroups = storeService.getShardGroups();

        // When a task is split or scaled-in, it is exited
        var taskMeta = storeService.getTaskInfoMeta();
        if (taskMeta.hasSplitTaskDoing() || taskMeta.hasMoveTaskDoing()) {
            throw new PDException(1001, "split or combine task is processing, please try later!");
        }

        if (Objects.equals(kvService.get(BALANCE_SHARD_KEY), "DOING")) {
            throw new PDException(1001, "balance shard is processing, please try later!");
        }

        if (shardGroups.size() == 0) {
            return results;
        }

        Map<Long, Integer> storeShardCount = new HashMap<>();

        shardGroups.forEach(group -> {
            group.getShardsList().forEach(shard -> {
                storeShardCount.put(shard.getStoreId(),
                                    storeShardCount.getOrDefault(shard.getStoreId(), 0) + 1);
            });
        });

        log.info("balancePartitionLeader, shard group size: {}, by store: {}", shardGroups.size(),
                 storeShardCount);

        // Calculate the leader count for each store, divided into integer and remainder parts.
        var tmpCountMap = new HashMap<Long, Integer>();
        PriorityQueue<KVPair<Long, Integer>> countReminder =
                new PriorityQueue<>(new KvPairComparator<>(false));
        int sum = 0;

        for (var entry : storeShardCount.entrySet()) {
            var storeId = entry.getKey();
            var count = entry.getValue();
            // First, allocate the integer part.
            int v = count / pdConfig.getPartition().getShardCount();
            sum += v;
            var remainder = count % pdConfig.getPartition().getShardCount();
            tmpCountMap.put(storeId, v);
            if (remainder != 0) {
                countReminder.add(new KVPair<>(storeId, remainder));
            }
        }

        int reminderCount = shardGroups.size() - sum;

        // Then, according to the distribution of reminders
        while (!countReminder.isEmpty() && reminderCount > 0) {
            var pair = countReminder.poll();
            tmpCountMap.put(pair.getKey(), tmpCountMap.getOrDefault(pair.getKey(), 0) + 1);
            reminderCount -= 1;
        }

        PriorityQueue<KVPair<Long, Integer>> targetCount =
                new PriorityQueue<>(new KvPairComparator<>(true));
        targetCount.addAll(tmpCountMap.entrySet().stream()
                                      .map(e -> new KVPair<>(e.getKey(), e.getValue()))
                                      .collect(Collectors.toList()));
        log.info("target count: {}", targetCount);

        for (var group : shardGroups) {
            var map = group.getShardsList().stream()
                           .collect(Collectors.toMap(Metapb.Shard::getStoreId, shard -> shard));
            var tmpList = new ArrayList<KVPair<Long, Integer>>();
            // If there are many stores, they may not contain the corresponding store ID. Save
            // the non-compliant stores to the temporary list until you find a suitable store
            while (!targetCount.isEmpty()) {
                var pair = targetCount.poll();
                var storeId = pair.getKey();
                if (map.containsKey(storeId)) {
                    if (map.get(storeId).getRole() != Metapb.ShardRole.Leader) {
                        log.info("shard group{}, store id:{}, set to leader", group.getId(),
                                 storeId);
                        partitionService.transferLeader(group.getId(), map.get(storeId));
                        results.put(group.getId(), storeId);
                    } else {
                        log.info("shard group {}, store id :{}, is leader, no need change",
                                 group.getId(), storeId);
                    }

                    if (pair.getValue() > 1) {
                        // count -1
                        pair.setValue(pair.getValue() - 1);
                        tmpList.add(pair);
                    }
                    // If it is found, the processing is complete
                    break;
                } else {
                    tmpList.add(pair);
                }
            }
            targetCount.addAll(tmpList);
        }

        return results;
    }

    private long getMaxIndexGap(Map<Integer, Map<Long, Long>> committedIndexMap, int partitionId) {
        long maxGap = Long.MAX_VALUE;
        if (committedIndexMap == null || !committedIndexMap.containsKey(partitionId)) {
            return maxGap;
        }
        Map<Long, Long> shardMap = committedIndexMap.get(partitionId);
        if (shardMap == null || shardMap.size() == 0) {
            return maxGap;
        }
        List<Long> sortedList = new ArrayList<>();
        shardMap.forEach((storeId, committedIndex) -> {
            sortedList.add(committedIndex);
        });
        sortedList.sort(Comparator.reverseOrder());
        maxGap = sortedList.get(0) - sortedList.get(sortedList.size() - 1);
        return maxGap;
    }

    /**
     * Perform partition splitting, which is divided into automatic splitting and manual splitting
     *
     * @return
     * @throws PDException
     */
    public List<Metapb.Partition> splitPartition(
            Pdpb.OperationMode mode, List<Pdpb.SplitDataParam> params) throws PDException {

        if (mode == Pdpb.OperationMode.Auto) {
            return autoSplitPartition();
        }

        var list = params.stream()
                         .map(param -> new KVPair<>(param.getPartitionId(), param.getCount()))
                         .collect(Collectors.toList());

        storeService.splitShardGroups(list);
        return null;
    }

    /**
     * Partition splitting is performed automatically, and each store reaches the maximum number
     * of partitions
     * execution conditions
     * The number of partitions per machine after the split is less than partition
     * .max-partitions-per-store
     *
     * @throws PDException
     */
    public List<Metapb.Partition> autoSplitPartition() throws PDException {
        if (!isLeader()) {
            return null;
        }

        if (Metapb.ClusterState.Cluster_OK != storeService.getClusterStats().getState()) {
            if (Metapb.ClusterState.Cluster_Offline == storeService.getClusterStats().getState()) {
                throw new PDException(Pdpb.ErrorType.Split_Partition_Doing_VALUE,
                                      "The data is splitting");
            } else {
                throw new PDException(Pdpb.ErrorType.Cluster_State_Forbid_Splitting_VALUE,
                                      "The current state of the cluster prohibits splitting data");
            }
        }

        //For TEST
        //   pdConfig.getPartition().setMaxShardsPerStore(pdConfig.getPartition()
        //   .getMaxShardsPerStore()*2);

        // The maximum split count that a compute cluster can support
        int splitCount = pdConfig.getPartition().getMaxShardsPerStore() *
                         storeService.getActiveStores().size() /
                         (storeService.getShardGroups().size() *
                          pdConfig.getPartition().getShardCount());

        if (splitCount < 2) {
            throw new PDException(Pdpb.ErrorType.Too_Many_Partitions_Per_Store_VALUE,
                                  "Too many partitions per store, partition.store-max-shard-count" +
                                  " = "
                                  + pdConfig.getPartition().getMaxShardsPerStore());
        }

        // If the maximum number of partitions per store is not reached, it will be split
        log.info("Start to split partitions..., split count = {}", splitCount);

        // Set the cluster status to Offline
        storeService.updateClusterStatus(Metapb.ClusterState.Cluster_Offline);
        // Modify the default number of partitions
        // pdConfig.getConfigService().setPartitionCount(storeService.getShardGroups().size() *
        // splitCount);

        var list = storeService.getShardGroups().stream()
                               .map(shardGroup -> new KVPair<>(shardGroup.getId(), splitCount))
                               .collect(Collectors.toList());
        storeService.splitShardGroups(list);

        return null;
    }

    /**
     * Store reports the status of the task
     * The state of the partition changes, and the state of the ShardGroup, graph, and the entire
     * cluster where the partition resides
     *
     * @param task
     */
    public void reportTask(MetaTask.Task task) {
        try {
            switch (task.getType()) {
                case Split_Partition:
                    partitionService.handleSplitTask(task);
                    break;
                case Move_Partition:
                    partitionService.handleMoveTask(task);
                    break;
                case Clean_Partition:
                    partitionService.handleCleanPartitionTask(task);
                    break;
                case Build_Index:
                    partitionService.handleBuildIndexTask(task);
                    break;
                default:
                    break;
            }
        } catch (Exception e) {
            log.error("Report task exception {}, {}", e, task);
        }
    }

    /**
     * Compaction on rocksdb
     *
     * @throws PDException
     */
    public Boolean dbCompaction(String tableName) throws PDException {
        if (!isLeader()) {
            return false;
        }

        for (Metapb.ShardGroup shardGroup : storeService.getShardGroups()) {
            storeService.shardGroupsDbCompaction(shardGroup.getId(), tableName);
        }

        //
        return true;
    }

    /**
     * Determine whether all partitions of a store can be migrated out, and give the judgment
     * result and migration plan
     */
    public Map<String, Object> canAllPartitionsMovedOut(Metapb.Store sourceStore) throws
                                                                                  PDException {
        if (!isLeader()) {
            return null;
        }
        // Analyze whether the partition on a store can be completely checked out
        Map<String, Object> resultMap = new HashMap<>();
        // The definition object is used to hold the partition above the source store StoreId
        // ->PartitionID, ShardRole
        Map<Long, Map<Integer, Metapb.ShardRole>> sourcePartitionMap = new HashMap<>();
        sourcePartitionMap.put(sourceStore.getId(), new HashMap<>());
        // The definition object is used to hold the partition above the other active stores
        // StoreId ->PartitionID, ShardRole
        Map<Long, Map<Integer, Metapb.ShardRole>> otherPartitionMap = new HashMap<>();
        // The amount of disk space remaining for each store
        Map<Long, Long> availableDiskSpace = new HashMap<>();
        // Record the amount of data in the partition to be migrated
        Map<Integer, Long> partitionDataSize = new HashMap<>();

        storeService.getActiveStores().forEach(store -> {
            if (store.getId() != sourceStore.getId()) {
                otherPartitionMap.put(store.getId(), new HashMap<>());
                // Records the remaining disk space of other stores, in bytes
                availableDiskSpace.put(store.getId(), store.getStats().getAvailable());
            } else {
                resultMap.put("current_store_is_online", true);
            }
        });
        // Count the size of the partition to be migrated (from storeStats in KB)
        for (Metapb.GraphStats graphStats : sourceStore.getStats().getGraphStatsList()) {
            partitionDataSize.put(graphStats.getPartitionId(),
                                  partitionDataSize.getOrDefault(graphStats.getPartitionId(), 0L)
                                  + graphStats.getApproximateSize());
        }
        // Assign values to sourcePartitionMap and otherPartitionMap
        partitionService.getPartitions().forEach(partition -> {
            try {
                storeService.getShardList(partition.getId()).forEach(shard -> {
                    long storeId = shard.getStoreId();
                    if (storeId == sourceStore.getId()) {
                        sourcePartitionMap.get(storeId).put(partition.getId(), shard.getRole());
                    } else {
                        if (otherPartitionMap.containsKey(storeId)) {
                            otherPartitionMap.get(storeId).put(partition.getId(), shard.getRole());
                        }
                    }

                });
            } catch (PDException e) {
                throw new RuntimeException(e);
            }
        });
        // Count the partitions to be removed: all partitions on the source store
        Map<Integer, KVPair<Long, Long>> movedPartitions = new HashMap<>();
        for (Map.Entry<Integer, Metapb.ShardRole> entry : sourcePartitionMap.get(
                sourceStore.getId()).entrySet()) {
            movedPartitions.put(entry.getKey(), new KVPair<>(sourceStore.getId(), 0L));
        }
        // Count the number of partitions of other stores and save them with a small top heap, so
        // that stores with fewer partitions are always prioritized
        PriorityQueue<KVPair<Long, Integer>> minHeap = new PriorityQueue<>(otherPartitionMap.size(),
                                                                           (o1, o2) -> o1.getValue()
                                                                                         .compareTo(
                                                                                                 o2.getValue()));
        otherPartitionMap.forEach((storeId, shards) -> {
            minHeap.add(new KVPair(storeId, shards.size()));
        });
        // Traverse the partitions to be migrated, and prioritize the migration to the store with
        // fewer partitions
        Iterator<Map.Entry<Integer, KVPair<Long, Long>>> moveIterator =
                movedPartitions.entrySet().iterator();
        while (moveIterator.hasNext()) {
            Map.Entry<Integer, KVPair<Long, Long>> moveEntry = moveIterator.next();
            int partitionId = moveEntry.getKey();
            // Record the elements that have popped up in the priority
            List<KVPair<Long, Integer>> tmpList = new ArrayList<>();
            while (minHeap.size() > 0) {
                KVPair<Long, Integer> pair = minHeap.poll(); // The first element pops up
                long storeId = pair.getKey();
                int partitionCount = pair.getValue();
                Map<Integer, Metapb.ShardRole> shards = otherPartitionMap.get(storeId);
                final int unitRate = 1024; // Balance the feed rate of different storage units
                if ((!shards.containsKey(partitionId)) && (
                        availableDiskSpace.getOrDefault(storeId, 0L) / unitRate >=
                        partitionDataSize.getOrDefault(partitionId, 0L))) {
                    // If the partition is not included on the destination store and the
                    // remaining space of the destination store can accommodate the partition,
                    // the migration is performed
                    moveEntry.getValue().setValue(storeId); // Set the target store for the move
                    log.info("plan to move partition {} to store {}, " +
                             "available disk space {}, current partitionSize:{}",
                             partitionId,
                             storeId,
                             availableDiskSpace.getOrDefault(storeId, 0L) / unitRate,
                             partitionDataSize.getOrDefault(partitionId, 0L)
                    );
                    // Update the expected remaining space for the store
                    availableDiskSpace.put(storeId, availableDiskSpace.getOrDefault(storeId, 0L)
                                                    - partitionDataSize.getOrDefault(partitionId,
                                                                                     0L) *
                                                      unitRate);
                    // Update the number of partitions for that store in the stat variable
                    partitionCount += 1;
                    pair.setValue(partitionCount);
                    tmpList.add(pair);
                    break;
                } else {
                    tmpList.add(pair);
                }
            }
            minHeap.addAll(tmpList);
        }
        // Check that there are no partitions that don't have a target store assigned
        List<Integer> remainPartitions = new ArrayList<>();
        movedPartitions.forEach((partId, storePair) -> {
            if (storePair.getValue() == 0L) {
                remainPartitions.add(partId);
            }
        });

        boolean isExecutingTasks =
                storeService.getStore(sourceStore.getId()).getStats().getExecutingTask();

        if (remainPartitions.size() > 0 || isExecutingTasks) {
            resultMap.put("flag", false);
            resultMap.put("movedPartitions", null);
        } else {
            resultMap.put("flag", true);
            resultMap.put("movedPartitions", movedPartitions);
        }
        return resultMap;

    }

    public Map<Integer, KVPair<Long, Long>> movePartitions(
            Map<Integer, KVPair<Long, Long>> movedPartitions) {
        if (!isLeader()) {
            return null;
        }
        // Start the migration
        log.info("begin move partitions:");
        movedPartitions.forEach((partId, storePair) -> {
            // Neither the source nor destination storeID is 0
            if (storePair.getKey() > 0 && storePair.getValue() > 0) {
                partitionService.movePartitionsShard(partId, storePair.getKey(),
                                                     storePair.getValue());
            }
        });
        return movedPartitions;
    }

}
