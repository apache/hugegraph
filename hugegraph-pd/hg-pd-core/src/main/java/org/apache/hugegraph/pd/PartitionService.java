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
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.commons.collections4.SetUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hugegraph.pd.common.KVPair;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.common.PartitionUtils;
import org.apache.hugegraph.pd.config.PDConfig;
import org.apache.hugegraph.pd.grpc.MetaTask;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.pd.grpc.Pdpb;
import org.apache.hugegraph.pd.grpc.pulse.ChangeShard;
import org.apache.hugegraph.pd.grpc.pulse.CleanPartition;
import org.apache.hugegraph.pd.grpc.pulse.CleanType;
import org.apache.hugegraph.pd.grpc.pulse.ConfChangeType;
import org.apache.hugegraph.pd.grpc.pulse.DbCompaction;
import org.apache.hugegraph.pd.grpc.pulse.MovePartition;
import org.apache.hugegraph.pd.grpc.pulse.PartitionKeyRange;
import org.apache.hugegraph.pd.grpc.pulse.SplitPartition;
import org.apache.hugegraph.pd.grpc.pulse.TransferLeader;
import org.apache.hugegraph.pd.meta.MetadataFactory;
import org.apache.hugegraph.pd.meta.PartitionMeta;
import org.apache.hugegraph.pd.meta.TaskInfoMeta;
import org.apache.hugegraph.pd.raft.RaftStateListener;

import lombok.extern.slf4j.Slf4j;

/**
 * Partition management
 */
@Slf4j
public class PartitionService implements RaftStateListener {

    private final long Partition_Version_Skip = 0x0F;
    private final StoreNodeService storeService;
    private PartitionMeta partitionMeta;
    private PDConfig pdConfig;
    // Partition command listening
    private List<PartitionInstructionListener> instructionListeners;

    // Partition status listeners
    private List<PartitionStatusListener> statusListeners;

    public PartitionService(PDConfig config, StoreNodeService storeService) {
        this.pdConfig = config;
        this.storeService = storeService;
        partitionMeta = MetadataFactory.newPartitionMeta(config);
        instructionListeners =
                Collections.synchronizedList(new ArrayList<PartitionInstructionListener>());
        statusListeners = Collections.synchronizedList(new ArrayList<PartitionStatusListener>());
    }

    public void init() throws PDException {
        partitionMeta.init();
        storeService.addStatusListener(new StoreStatusListener() {
            @Override
            public void onStoreStatusChanged(Metapb.Store store, Metapb.StoreState old,
                                             Metapb.StoreState status) {
                if (status == Metapb.StoreState.Tombstone) {
                    // When the store is stopped, notify all partitions of the store and migrate
                    // the data
                    storeOffline(store);
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

    /**
     * return key partition
     *
     * @param graphName
     * @param key
     * @return
     */
    public Metapb.PartitionShard getPartitionShard(String graphName, byte[] key) throws
                                                                                 PDException {
        long code = PartitionUtils.calcHashcode(key);
        return getPartitionByCode(graphName, code);
    }

    /**
     * Returns the partition to which it belongs based on the hashcode
     *
     * @param graphName
     * @param code
     * @return
     */
    public Metapb.PartitionShard getPartitionByCode(String graphName, long code) throws
                                                                                 PDException {
        if (code < 0 || code >= PartitionUtils.MAX_VALUE) {
            throw new PDException(Pdpb.ErrorType.NOT_FOUND_VALUE, "code error");
        }
        // Find the partition ID based on the code, and if it doesn't find, create a new partition
        Metapb.Partition partition = partitionMeta.getPartitionByCode(graphName, code);

        if (partition == null) {
            synchronized (this) {
                if (partition == null) {
                    partition = newPartition(graphName, code);
                }
            }
        }

        Metapb.PartitionShard partShard = Metapb.PartitionShard.newBuilder()
                                                               .setPartition(partition)
                                                               .setLeader(storeService.getLeader(
                                                                       partition, 0))
                                                               .build();
        log.debug(
                "{} Partition get code = {}, partition id  = {}, start = {}, end = {}, leader = {}",
                graphName, (code), partition.getId(), partition.getStartKey(),
                partition.getEndKey(), partShard.getLeader());

        return partShard;
    }

    /**
     * Returns partition information based on ID
     *
     * @param graphName
     * @param partId
     * @return
     * @throws PDException
     */
    public Metapb.PartitionShard getPartitionShardById(String graphName, int partId) throws
                                                                                     PDException {
        Metapb.Partition partition = partitionMeta.getPartitionById(graphName, partId);
        if (partition == null) {
            return null;
        }

        Metapb.PartitionShard partShard = Metapb.PartitionShard.newBuilder()
                                                               .setPartition(partition)
                                                               .setLeader(storeService.getLeader(
                                                                       partition, 0))
                                                               .build();

        return partShard;
    }

    public Metapb.Partition getPartitionById(String graphName, int partId) throws PDException {
        return partitionMeta.getPartitionById(graphName, partId);
    }

    public List<Metapb.Partition> getPartitionById(int partId) throws PDException {
        return partitionMeta.getPartitionById(partId);
    }

    /**
     * Get all partitions of the graph
     */
    public List<Metapb.Partition> getPartitions() {
        return partitionMeta.getPartitions();
    }

    public List<Metapb.Partition> getPartitions(String graphName) {
        if (StringUtils.isAllEmpty(graphName)) {
            return partitionMeta.getPartitions();
        }
        return partitionMeta.getPartitions(graphName);
    }

    /**
     * Find all the partitions on the store
     *
     * @param store
     * @return
     */
    public List<Metapb.Partition> getPartitionByStore(Metapb.Store store) throws PDException {
        List<Metapb.Partition> partitions = new ArrayList<>();
        getGraphs().forEach(graph -> {
            getPartitions(graph.getGraphName()).forEach(partition -> {
                try {
                    storeService.getShardGroup(partition.getId()).getShardsList().forEach(shard -> {
                        if (shard.getStoreId() == store.getId()) {
                            partitions.add(partition);
                        }
                    });
                } catch (PDException e) {
                    throw new RuntimeException(e);
                }
            });
        });
        return partitions;
    }

    /**
     * Creates a new partition
     *
     * @param graphName
     * @return
     */
    private Metapb.Partition newPartition(String graphName, long code) throws PDException {
        Metapb.Graph graph = partitionMeta.getAndCreateGraph(graphName);
        int partitionSize = PartitionUtils.MAX_VALUE / graph.getPartitionCount();
        if (PartitionUtils.MAX_VALUE % graph.getPartitionCount() != 0) {
            // There is a remainder, and the partition is inexhaustible
            partitionSize++;
        }

        int partitionId = (int) (code / partitionSize);
        long startKey = (long) partitionSize * partitionId;
        long endKey = (long) partitionSize * (partitionId + 1);

        // Check Local
        Metapb.Partition partition = partitionMeta.getPartitionById(graphName, partitionId);
        if (partition == null) {
            storeService.allocShards(null, partitionId);

            // Assign a store
            partition = Metapb.Partition.newBuilder()
                                        .setId(partitionId)
                                        .setVersion(0)
                                        .setState(Metapb.PartitionState.PState_Normal)
                                        .setStartKey(startKey)
                                        .setEndKey(endKey)
                                        .setGraphName(graphName)
                                        .build();

            log.info("Create newPartition {}", partition);
        }

        partitionMeta.updatePartition(partition);

        return partition;
    }

    /**
     * compute graph partition id, partition gap * store group id + offset
     *
     * @param graph  graph
     * @param offset offset
     * @return new partition id
     * @throws PDException
     */
    protected int getPartitionId(String graphName, byte[] key) throws PDException {
        int code = PartitionUtils.calcHashcode(key);
        Metapb.Partition partition = partitionMeta.getPartitionByCode(graphName, code);
        return partition != null ? partition.getId() : -1;
    }

    /**
     * Gets all partitions spanned by the key range
     * For the time being, hashcode is used for calculation, and the normal practice is to query
     * based on the key
     *
     * @param graphName
     * @param startKey
     * @param endKey
     */
    public List<Metapb.PartitionShard> scanPartitions(String graphName, byte[] startKey,
                                                      byte[] endKey)
            throws PDException {
        int startPartId = getPartitionId(graphName, startKey);
        int endPartId = getPartitionId(graphName, endKey);

        List<Metapb.PartitionShard> partShards = new ArrayList<>();
        for (int id = startPartId; id <= endPartId; id++) {
            Metapb.Partition partition = partitionMeta.getPartitionById(graphName, id);
            partShards.add(
                    Metapb.PartitionShard.newBuilder()
                                         .setPartition(partition)
                                         // Here you need to return the correct leader, and
                                         // temporarily default to the first one
                                         .setLeader(storeService.getLeader(partition, 0))
                                         .build()
            );
        }
        return partShards;
    }

    public synchronized long updatePartition(List<Metapb.Partition> partitions) throws PDException {
        for (Metapb.Partition pt : partitions) {
            Metapb.Partition oldPt = getPartitionById(pt.getGraphName(), pt.getId());
            partitionMeta.updatePartition(pt);
            onPartitionChanged(oldPt, pt);
        }
        return partitions.size();
    }

    /**
     * Update the status of partitions and graphs
     *
     * @param graph
     * @param partId
     * @param state
     * @throws PDException
     */
    public synchronized void updatePartitionState(String graph, int partId,
                                                  Metapb.PartitionState state) throws PDException {
        Metapb.Partition partition = getPartitionById(graph, partId);

        if (partition.getState() != state) {
            Metapb.Partition newPartition = partitionMeta.updatePartition(partition.toBuilder()
                                                                                   .setState(state)
                                                                                   .build());

            onPartitionChanged(partition, newPartition);
        }
    }

    public synchronized void updateGraphState(String graphName, Metapb.PartitionState state) throws
                                                                                             PDException {
        Metapb.Graph graph = getGraph(graphName);
        if (graph != null) {
            partitionMeta.updateGraph(graph.toBuilder()
                                           .setState(state).build());
        }
    }

    public synchronized long removePartition(String graphName, int partId) throws PDException {
        log.info("Partition {}-{} removePartition", graphName, partId);
        Metapb.Partition partition = partitionMeta.getPartitionById(graphName, partId);
        var ret = partitionMeta.removePartition(graphName, partId);
        partitionMeta.reload();
        onPartitionRemoved(partition);

        try {
            Metapb.PartitionState state = Metapb.PartitionState.PState_Normal;
            for (Metapb.Partition pt : partitionMeta.getPartitions(partition.getGraphName())) {
                if (pt.getState().getNumber() > state.getNumber()) {
                    state = pt.getState();
                }
            }
            updateGraphState(partition.getGraphName(), state);

            state = Metapb.PartitionState.PState_Normal;
            for (Metapb.ShardGroup group : storeService.getShardGroups()) {
                if (group.getState().getNumber() > state.getNumber()) {
                    state = group.getState();
                }
            }
            storeService.updateClusterStatus(state);

        } catch (PDException e) {
            log.error("onPartitionChanged", e);
        }

        return ret;
    }

    public Metapb.PartitionStats getPartitionStats(String graphName, int partitionId) throws
                                                                                      PDException {
        return partitionMeta.getPartitionStats("", partitionId);
    }

    /**
     * Get the partition status of the graph
     */
    public List<Metapb.PartitionStats> getPartitionStatus(String graphName)
            throws PDException {
        return partitionMeta.getPartitionStats(graphName);
    }

    /**
     * Returns the information of the graph
     */
    public List<Metapb.Graph> getGraphs() throws PDException {
        return partitionMeta.getGraphs();
    }

    public Metapb.Graph getGraph(String graphName) throws PDException {
        return partitionMeta.getGraph(graphName);
    }

    /**
     * Delete the diagram and all partitions of the diagram
     */
    public Metapb.Graph delGraph(String graphName) throws PDException {
        log.info("delGraph {}", graphName);
        Metapb.Graph graph = getGraph(graphName);
        getPartitions(graphName).forEach(partition -> {
            onPartitionRemoved(partition);
        });
        partitionMeta.removeAllPartitions(graphName);
        partitionMeta.removeGraph(graphName);
        if (!StringUtils.isEmpty(graphName)) {
            partitionMeta.removePartitionStats(graphName);
        }
        return graph;
    }

    /**
     * To modify the graph information, you need to notify the store
     */
    public synchronized Metapb.Graph updateGraph(Metapb.Graph graph) throws PDException {
        Metapb.Graph lastGraph = partitionMeta.getAndCreateGraph(graph.getGraphName());
        log.info("updateGraph graph: {}, last: {}", graph, lastGraph);

        int partCount =
                (graph.getGraphName().endsWith("/s") || graph.getGraphName().endsWith("/m")) ?
                1 : pdConfig.getPartition().getTotalCount();

        // set the partition count to specified if legal.
        if (graph.getPartitionCount() <= partCount && graph.getPartitionCount() > 0) {
            partCount = graph.getPartitionCount();
        }

        if (partCount == 0) {
            throw new PDException(10010, "update graph error, partition count = 0");
        }

        graph = lastGraph.toBuilder()
                         .mergeFrom(graph)
                         .setPartitionCount(partCount)
                         .build();
        partitionMeta.updateGraph(graph);

        // The number of partitions has changed
        if (lastGraph.getPartitionCount() != graph.getPartitionCount()) {
            log.info("updateGraph graph: {}, partition count changed from {} to {}",
                     graph.getGraphName(), lastGraph.getPartitionCount(),
                     graph.getPartitionCount());
        }
        return graph;
    }

    // partitionId -> (storeId -> shard committedIndex)
    public Map<Integer, Map<Long, Long>> getCommittedIndexStats() throws PDException {
        Map<Integer, Map<Long, Long>> map = new HashMap<>();
        for (Metapb.Store store : storeService.getActiveStores()) {
            for (Metapb.RaftStats raftStats : store.getStats().getRaftStatsList()) {
                int partitionID = raftStats.getPartitionId();
                if (!map.containsKey(partitionID)) {
                    map.put(partitionID, new HashMap<>());
                }
                Map<Long, Long> storeMap = map.get(partitionID);
                if (!storeMap.containsKey(store.getId())) {
                    storeMap.put(store.getId(), raftStats.getCommittedIndex());
                }
            }
        }
        return map;
    }

    /**
     * The storage is taken offline and the partition data is migrated
     *
     * @param store
     */
    public void storeOffline(Metapb.Store store) {
        try {
            log.info("storeOffline store id: {}, address: {}, state: {}",
                     store.getId(), store.getAddress(), store.getState());
            List<Metapb.Partition> partitions = getPartitionByStore(store);
            var partIds = new HashSet<Integer>();
            for (Metapb.Partition p : partitions) {
                if (partIds.contains(p.getId())) {
                    continue;
                }
                shardOffline(p, store.getId());
                partIds.add(p.getId());
            }
        } catch (PDException e) {
            log.error("storeOffline exception: ", e);
        }
    }

    /**
     * The storage is taken offline and the partition data is migrated
     */
    public synchronized void shardOffline(Metapb.Partition partition, long storeId) {
        try {
            log.info("shardOffline Partition {} - {} shardOffline store : {}",
                     partition.getGraphName(), partition.getId(), storeId);
            // partition = getPartitionById(partition.getGraphName(), partition.getId());
            // Metapb.Partition.Builder builder = Metapb.Partition.newBuilder(partition);
            // builder.clearShards();
            // partition.getShardsList().forEach(shard -> {
            //    if (shard.getStoreId() != storeId)
            //        builder.addShards(shard);
            // });
            // partition = builder.build();
            Metapb.Graph graph = getGraph(partition.getGraphName());
            reallocPartitionShards(graph, partition);

        } catch (PDException e) {
            log.error("storeOffline exception: ", e);
        }
    }

    private boolean isShardListEquals(List<Metapb.Shard> list1, List<Metapb.Shard> list2) {
        if (list1 == list2) {
            return true;
        } else if (list1 != null && list2 != null) {

            var s1 = list1.stream().map(Metapb.Shard::getStoreId).sorted(Long::compare)
                          .collect(Collectors.toList());
            var s2 = list2.stream().map(Metapb.Shard::getStoreId).sorted(Long::compare)
                          .collect(Collectors.toList());

            if (s1.size() == s2.size()) {
                for (int i = 0; i < s1.size(); i++) {
                    if (s1.get(i) != s2.get(i)) {
                        return false;
                    }
                }
                return true;
            }
        }

        return false;
    }

    /**
     * Reassign shards
     *
     * @param graph
     * @param partition
     * @throws PDException
     */
    public void reallocPartitionShards(Metapb.Graph graph, Metapb.Partition partition) throws
                                                                                       PDException {
        if (partition == null) {
            return;
        }
        List<Metapb.Shard> originalShards = storeService.getShardList(partition.getId());

        var shardGroup = storeService.getShardGroup(partition.getId());

        List<Metapb.Shard> shards = storeService.reallocShards(shardGroup);

        if (isShardListEquals(originalShards, shards)) {
            log.info("reallocPartitionShards:{} vs {}", shardGroup, shards);
            //  partition = Metapb.Partition.newBuilder(partition)
            //      .clearShards().addAllShards(shards)
            //      .build();
            // partitionMeta.updatePartition(partition);
            fireChangeShard(partition, shards, ConfChangeType.CONF_CHANGE_TYPE_ADJUST);
        }
    }

    public synchronized void reallocPartitionShards(String graphName, int partitionId) throws
                                                                                       PDException {
        reallocPartitionShards(partitionMeta.getGraph(graphName),
                               partitionMeta.getPartitionById(graphName, partitionId));
    }

    /**
     * Migrate partition copies
     */
    public synchronized void movePartitionsShard(Integer partitionId, long fromStore,
                                                 long toStore) {
        try {
            log.info("movePartitionsShard partitionId {} from store {} to store {}", partitionId,
                     fromStore, toStore);
            for (Metapb.Graph graph : getGraphs()) {
                Metapb.Partition partition =
                        this.getPartitionById(graph.getGraphName(), partitionId);
                if (partition == null) {
                    continue;
                }

                var shardGroup = storeService.getShardGroup(partitionId);
                List<Metapb.Shard> shards = new ArrayList<>();
                shardGroup.getShardsList().forEach(shard -> {
                    if (shard.getStoreId() != fromStore) {
                        shards.add(shard);
                    }
                });

                shards.add(Metapb.Shard.newBuilder().setStoreId(toStore)
                                       .setRole(Metapb.ShardRole.Follower).build());

                // storeService.updateShardGroup(partitionId, shards, -1, -1);
                // storeService.onShardGroupStatusChanged(shardGroup, newShardGroup);
                fireChangeShard(partition, shards, ConfChangeType.CONF_CHANGE_TYPE_ADJUST);
                // Shard groups have nothing to do with Graph, just one is enough
                break;
            }
        } catch (PDException e) {
            log.error("Partition {} movePartitionsShard exception {}", partitionId, e);
        }
    }

    /**
     * Split all partitions in the cluster into splits
     *
     * @param splits Split partitions
     */
    public synchronized void splitPartition(List<KVPair<Integer, Integer>> splits) throws
                                                                                   PDException {
        var tasks = new HashMap<String, List<KVPair<Integer, Integer>>>();

        for (var pair : splits) {
            for (var partition : getPartitionById(pair.getKey())) {
                if (!tasks.containsKey(partition.getGraphName())) {
                    tasks.put(partition.getGraphName(), new ArrayList<>());
                }
                tasks.get(partition.getGraphName()).add(pair);
            }
        }

        for (var entry : tasks.entrySet()) {
            splitPartition(getGraph(entry.getKey()), entry.getValue());
        }
    }

    /**
     * Partition splitting, splitting a graph into N pieces
     *
     * @param graph   graph
     * @param toCount target count
     * @throws PDException
     */
    public synchronized void splitPartition(Metapb.Graph graph, int toCount) throws PDException {

        var partitionCount = getPartitions(graph.getGraphName()).size();
        var maxShardsPerStore = pdConfig.getPartition().getMaxShardsPerStore();
        var shardCount = pdConfig.getPartition().getShardCount();

        if (shardCount * toCount > storeService.getActiveStores().size() * maxShardsPerStore) {
            throw new PDException(Pdpb.ErrorType.Too_Many_Partitions_Per_Store_VALUE,
                                  "can't satisfy target shard group count, reached the upper " +
                                  "limit of the cluster");
        }

        if (toCount % partitionCount != 0 || toCount <= partitionCount) {
            throw new PDException(Pdpb.ErrorType.Invalid_Split_Partition_Count_VALUE,
                                  "invalid split partition count, make sure to count is N time of" +
                                  " current partition count");
        }

        // Since it is an integer multiple,The enrichment factor is toCount / current count
        var splitCount = toCount / partitionCount;
        var list = new ArrayList<KVPair<Integer, Integer>>();
        for (int i = 0; i < partitionCount; i++) {
            list.add(new KVPair<>(i, splitCount));
        }

        splitPartition(graph, list);
    }

    private synchronized void splitPartition(Metapb.Graph graph,
                                             List<KVPair<Integer, Integer>> splits)
            throws PDException {
        var taskInfoMeta = storeService.getTaskInfoMeta();
        if (!taskInfoMeta.scanSplitTask(graph.getGraphName()).isEmpty()) {
            return;
        }

        splits.sort(Comparator.comparing(KVPair::getKey));
        log.info("split partition, graph: {}, splits:{}", graph, splits);

        // Start with the last partition subscript
        var i = getPartitions(graph.getGraphName()).size();

        for (var pair : splits) {
            Metapb.Partition partition =
                    partitionMeta.getPartitionById(graph.getGraphName(), pair.getKey());
            if (partition != null) {
                var splitCount = pair.getValue();
                long splitLen = (partition.getEndKey() - partition.getStartKey()) / splitCount;

                List<Metapb.Partition> newPartitions = new ArrayList<>();
                // The first partition is the original partition
                newPartitions.add(partition.toBuilder()
                                           .setStartKey(partition.getStartKey())
                                           .setEndKey(partition.getStartKey() + splitLen)
                                           .setId(partition.getId())
                                           .setState(Metapb.PartitionState.PState_Offline)
                                           .build());

                int idx = 0;

                for (; idx < splitCount - 2; idx++) {
                    newPartitions.add(partition.toBuilder()
                                               .setStartKey(newPartitions.get(idx).getEndKey())
                                               .setEndKey(newPartitions.get(idx).getEndKey() +
                                                          splitLen)
                                               .setId(i)
                                               .setState(Metapb.PartitionState.PState_Offline)
                                               .build());
                    i += 1;
                }

                newPartitions.add(partition.toBuilder()
                                           .setStartKey(newPartitions.get(idx).getEndKey())
                                           .setEndKey(partition.getEndKey())
                                           .setId(i)
                                           .setState(Metapb.PartitionState.PState_Offline)
                                           .build());
                i += 1;

                // try to save new partitions, and repair shard group
                for (int j = 0; j < newPartitions.size(); j++) {
                    var newPartition = newPartitions.get(j);

                    if (j != 0) {
                        partitionMeta.updatePartition(newPartition);
                    }
                    // Create a shard group, if it is empty, create it according to the shard
                    // group of the partition, and ensure that it is on one machine
                    // If it exists, the number of partitions in each graph is not the same, and
                    // the store side needs to be copied to other machines
                    var shardGroup = storeService.getShardGroup(newPartition.getId());
                    if (shardGroup == null) {
                        shardGroup = storeService.getShardGroup(partition.getId()).toBuilder()
                                                 .setId(newPartition.getId())
                                                 .build();
                        storeService.getStoreInfoMeta().updateShardGroup(shardGroup);
                        updateShardGroupCache(shardGroup);
                    }

                    // check shard list
                    if (shardGroup.getShardsCount() != pdConfig.getPartition().getShardCount()) {
                        storeService.reallocShards(shardGroup);
                    }
                }

                SplitPartition splitPartition = SplitPartition.newBuilder()
                                                              .addAllNewPartition(newPartitions)
                                                              .build();

                fireSplitPartition(partition, splitPartition);
                // Change the partition status to Offline, and resume the partition status to
                // Offline after the task is completed
                updatePartitionState(partition.getGraphName(), partition.getId(),
                                     Metapb.PartitionState.PState_Offline);

                // Record transactions
                var task = MetaTask.Task.newBuilder().setPartition(partition)
                                        .setSplitPartition(splitPartition)
                                        .build();
                taskInfoMeta.addSplitTask(pair.getKey(), task.getPartition(),
                                          task.getSplitPartition());
            }
        }
    }

    /**
     * transfer leader to other shard
     * Just transfer a partition
     */
    public void transferLeader(Integer partId, Metapb.Shard shard) {
        try {
            var partitions = getPartitionById(partId);
            if (partitions.size() > 0) {
                fireTransferLeader(partitions.get(0),
                                   TransferLeader.newBuilder().setShard(shard).build());
            }
//            for (Metapb.Graph graph : getGraphs()) {
//                Metapb.Partition partition = this.getPartitionById(graph.getGraphName(), partId);
//                if (partition != null) {
//                    fireTransferLeader(partition, TransferLeader.newBuilder().setShard(shard)
//                    .build());
//                }
//            }
        } catch (PDException e) {
            log.error("Partition {} transferLeader exception {}", partId, e);
        }
    }

    /**
     * // todo : Check the corresponding store group and check the logic
     * Partition merging: Merges the number of partitions in the entire cluster into toCount
     *
     * @param toCount The number of partitions to be targeted
     * @throws PDException when query errors
     */
    public void combinePartition(int toCount) throws PDException {

        int shardsTotalCount = getShardGroupCount();
        for (var graph : getGraphs()) {
            // All graphs larger than the toCount partition are scaled in
            if (graph.getPartitionCount() > toCount) {
                combineGraphPartition(graph, toCount, shardsTotalCount);
            }
        }
    }

    /**
     * For a single graph, perform partition merging
     *
     * @param graphName the name of the graph
     * @param toCount   the target partition count
     * @throws PDException when query errors
     */
    public void combineGraphPartition(String graphName, int toCount) throws PDException {
        combineGraphPartition(getGraph(graphName), toCount, getShardGroupCount());
    }

    /**
     * Internal implementation of single-graph merging
     *
     * @param graph      the name of the graph
     * @param toCount    the target partition count
     * @param shardCount the shard count of the clusters
     * @throws PDException when query errors
     */
    private synchronized void combineGraphPartition(Metapb.Graph graph, int toCount, int shardCount)
            throws PDException {
        if (graph == null) {
            throw new PDException(1,
                                  "Graph not exists, try to use full graph name, like " +
                                  "/DEFAULT/GRAPH_NAME/g");
        }

        log.info("Combine graph {} partition, from {}, to {}, with shard count:{}",
                 graph.getGraphName(), graph.getPartitionCount(), toCount, shardCount);

        if (!checkTargetCount(graph.getPartitionCount(), toCount, shardCount)) {
            log.error("Combine partition, illegal toCount:{}, graph:{}", toCount,
                      graph.getGraphName());
            throw new PDException(2,
                                  "illegal partition toCount, should between 1 ~ shard group " +
                                  "count and " +
                                  " can be dived by shard group count");
        }

        var taskInfoMeta = storeService.getTaskInfoMeta();
        if (!taskInfoMeta.scanMoveTask(graph.getGraphName()).isEmpty()) {
            throw new PDException(3, "Graph Combine process exists");
        }

        // According to key start sort
        var partitions = getPartitions(graph.getGraphName()).stream()
                                                            .sorted(Comparator.comparing(
                                                                    Metapb.Partition::getStartKey))
                                                            .collect(Collectors.toList());

        // Partition numbers do not have to be sequential
        var sortPartitions = getPartitions(graph.getGraphName())
                .stream()
                .sorted(Comparator.comparing(Metapb.Partition::getId))
                .collect(Collectors.toList());

        var groupSize = partitions.size() / toCount; // merge group size
        // 0~12 to 4 partitions
        // scheme: 0,1,2 => 0, 3,4,5 => 1, 6,7,8 => 2, 9,10,11 => 3
        // Ensure the continuity of partitions
        for (int i = 0; i < toCount; i++) {
            var startKey = partitions.get(i * groupSize).getStartKey();
            var endKey = partitions.get(i * groupSize + groupSize - 1).getEndKey();
            // compose the key range
            // the start key and end key should be changed if combine success.

            var targetPartition = Metapb.Partition.newBuilder(sortPartitions.get(i))
                                                  .setStartKey(startKey)
                                                  .setEndKey(endKey)
                                                  .build();

            for (int j = 0; j < groupSize; j++) {
                var partition = partitions.get(i * groupSize + j);
                // If the partition ID is the same, skip it
                if (i == partition.getId()) {
                    continue;
                }

                log.info("combine partition of graph :{}, from part id {} to {}",
                         partition.getGraphName(),
                         partition.getId(), targetPartition.getId());
                MovePartition movePartition = MovePartition.newBuilder()
                                                           .setTargetPartition(targetPartition)
                                                           .setKeyStart(partition.getStartKey())
                                                           .setKeyEnd(partition.getEndKey())
                                                           .build();
                taskInfoMeta.addMovePartitionTask(partition, movePartition);
                // source is offline
                updatePartitionState(partition.getGraphName(), partition.getId(),
                                     Metapb.PartitionState.PState_Offline);
                fireMovePartition(partition, movePartition);
            }
            // target offline
            updatePartitionState(targetPartition.getGraphName(), targetPartition.getId(),
                                 Metapb.PartitionState.PState_Offline);
        }

        storeService.updateClusterStatus(Metapb.ClusterState.Cluster_Offline);
    }

    /**
     * get raft group count from storeService
     *
     * @return the count of raft groups
     */
    private int getShardGroupCount() {
        try {
            return Optional.ofNullable(storeService.getShardGroups()).orElseGet(ArrayList::new)
                           .size();
        } catch (PDException e) {
            log.error("get shard group failed, error: {}", e);
        }
        return 0;
    }

    /**
     * Determine whether the graph partition can be retrieved from f to t
     *
     * @param fromCount The number of partitions now
     * @param toCount   The number of partitions to be targeted
     * @return true when available , or otherwise
     */
    private boolean checkTargetCount(int fromCount, int toCount, int shardCount) {
        // It should be between 1 ~ N and divisible
        return toCount >= 1 && toCount < fromCount && fromCount % toCount == 0 &&
               toCount < shardCount;
    }

    /**
     * Process partition heartbeats and record leader information
     * Check the term and version to see if it's the latest message
     *
     * @param stats
     */
    public void partitionHeartbeat(Metapb.PartitionStats stats) throws PDException {

        Metapb.ShardGroup shardGroup = storeService.getShardGroup(stats.getId());
        // shard group version changes or leader changes
        // (The shard group is controlled by the PD, and there may be brief inconsistencies after
        // operations such as splitting, subject to PD)
        // store Upload the final one raft group data
        if (shardGroup != null) {
            if (shardGroup.getVersion() < stats.getLeaderTerm() ||
                shardGroup.getConfVer() < stats.getConfVer() ||
                !isShardEquals(shardGroup.getShardsList(), stats.getShardList())) {
                storeService.updateShardGroup(stats.getId(),
                                              stats.getShardList(), stats.getLeaderTerm(),
                                              stats.getConfVer());
            }
        }

        // List<Metapb.Partition> partitions = getPartitionById(stats.getId());
        // for (Metapb.Partition partition : partitions) {
        // partitionMeta.getAndCreateGraph(partition.getGraphName());
        checkShardState(shardGroup, stats);
        // }
        // statistics
        partitionMeta.updatePartitionStats(stats.toBuilder()
                                                .setTimestamp(System.currentTimeMillis()).build());
    }

    private boolean isShardEquals(List<Metapb.Shard> list1, List<Metapb.Shard> list2) {
        return SetUtils.isEqualSet(list1, list2);
    }

    private Long getLeader(Metapb.ShardGroup group) {
        for (var shard : group.getShardsList()) {
            if (shard.getRole() == Metapb.ShardRole.Leader) {
                return shard.getStoreId();
            }
        }
        return null;
    }

    /**
     * Check the shard status, offline shard affects the partition status
     *
     * @param stats
     */
    private void checkShardState(Metapb.ShardGroup shardGroup, Metapb.PartitionStats stats) {

        try {
            Metapb.PartitionState state = Metapb.PartitionState.PState_Normal;

            int offCount = 0;

            for (Metapb.ShardStats shard : stats.getShardStatsList()) {
                if (shard.getState() == Metapb.ShardState.SState_Offline) {
                    offCount++;
                }
            }

            if (offCount > 0 && offCount * 2 < stats.getShardStatsCount()) {
                state = Metapb.PartitionState.PState_Warn;
            }

            if (shardGroup.getState() != state) {
                // update graph state
                for (var graph : getGraphs()) {
                    if (graph.getState() != state) {
                        updateGraphState(graph.getGraphName(), state);
                    }
                }

                storeService.updateShardGroupState(shardGroup.getId(), state);
            }
        } catch (Exception e) {
            log.error("checkShardState {} failed, error: ", shardGroup.getId(), e);
        }
    }

    public void addInstructionListener(PartitionInstructionListener event) {
        instructionListeners.add(event);
    }

    public void addStatusListener(PartitionStatusListener listener) {
        statusListeners.add(listener);
    }

    /**
     * Initiates the Change Shard command
     *
     * @param changeType
     */
    protected void fireChangeShard(Metapb.Partition partition, List<Metapb.Shard> shards,
                                   ConfChangeType changeType) {
        log.info("fireChangeShard partition: {}-{}, changeType: {} {}", partition.getGraphName(),
                 partition.getId(), changeType, shards);
        instructionListeners.forEach(cmd -> {
            try {
                cmd.changeShard(partition, ChangeShard.newBuilder()
                                                      .addAllShard(shards).setChangeType(changeType)
                                                      .build());
            } catch (Exception e) {
                log.error("fireChangeShard", e);
            }
        });
    }

    public void changeShard(int groupId, List<Metapb.Shard> shards) throws PDException {
        var partitions = getPartitionById(groupId);
        if (partitions.isEmpty()) {
            return;
        }
        fireChangeShard(partitions.get(0), shards, ConfChangeType.CONF_CHANGE_TYPE_ADJUST);
    }

    /**
     * Send a partition split message
     *
     * @param partition
     */
    protected void fireSplitPartition(Metapb.Partition partition, SplitPartition splitPartition) {
        log.info("fireSplitPartition partition: {}-{}, split: {}",
                 partition.getGraphName(), partition.getId(), splitPartition);
        instructionListeners.forEach(cmd -> {
            try {
                cmd.splitPartition(partition, splitPartition);
            } catch (Exception e) {
                log.error("fireSplitPartition", e);
            }
        });
    }

    /**
     * Send a Leader Switchover message
     */
    protected void fireTransferLeader(Metapb.Partition partition, TransferLeader transferLeader) {
        log.info("fireTransferLeader partition: {}-{}, leader: {}",
                 partition.getGraphName(), partition.getId(), transferLeader);
        instructionListeners.forEach(cmd -> {
            try {
                cmd.transferLeader(partition, transferLeader);
            } catch (Exception e) {
                log.error("fireSplitPartition", e);
            }
        });
    }

    /**
     * Send a message to the partition to move data
     *
     * @param partition     Original partition
     * @param movePartition Target partition, contains key range
     */
    protected void fireMovePartition(Metapb.Partition partition, MovePartition movePartition) {
        log.info("fireMovePartition partition: {} -> {}",
                 partition, movePartition);

        instructionListeners.forEach(cmd -> {
            try {
                cmd.movePartition(partition, movePartition);
            } catch (Exception e) {
                log.error("fireMovePartition", e);
            }
        });
    }

    protected void fireCleanPartition(Metapb.Partition partition, CleanPartition cleanPartition) {
        log.info("fireCleanPartition partition: {} -> just keep : {}->{}",
                 partition.getId(), cleanPartition.getKeyStart(), cleanPartition.getKeyEnd());

        instructionListeners.forEach(cmd -> {
            try {
                cmd.cleanPartition(partition, cleanPartition);
            } catch (Exception e) {
                log.error("cleanPartition", e);
            }
        });
    }

    protected void fireChangePartitionKeyRange(Metapb.Partition partition,
                                               PartitionKeyRange partitionKeyRange) {
        log.info("fireChangePartitionKeyRange partition: {}-{} -> key range {}",
                 partition.getGraphName(), partition.getId(), partitionKeyRange);

        instructionListeners.forEach(cmd -> {
            try {
                cmd.changePartitionKeyRange(partition, partitionKeyRange);
            } catch (Exception e) {
                log.error("cleanPartition", e);
            }
        });
    }

    /**
     * Handle graph migration tasks
     *
     * @param task
     */
    public synchronized void handleMoveTask(MetaTask.Task task) throws PDException {
        var taskInfoMeta = storeService.getTaskInfoMeta();
        var partition = task.getPartition();
        var movePartition = task.getMovePartition();

        MetaTask.Task pdMetaTask = taskInfoMeta.getMovePartitionTask(partition.getGraphName(),
                                                                     movePartition.getTargetPartition()
                                                                                  .getId(),
                                                                     partition.getId());

        log.info("report move task, graph:{}, pid : {}->{}, state: {}",
                 task.getPartition().getGraphName(),
                 task.getPartition().getId(), task.getMovePartition().getTargetPartition().getId(),
                 task.getState());

        // HAS BEEN PROCESSED(There is it in front)
        if (pdMetaTask != null) {
            var newTask = pdMetaTask.toBuilder().setState(task.getState()).build();
            taskInfoMeta.updateMovePartitionTask(newTask);

            List<MetaTask.Task> subTasks = taskInfoMeta.scanMoveTask(partition.getGraphName());

            var finished = subTasks.stream().allMatch(t ->
                                                              t.getState() ==
                                                              MetaTask.TaskState.Task_Success ||
                                                              t.getState() ==
                                                              MetaTask.TaskState.Task_Failure);

            if (finished) {
                var allSuccess = subTasks.stream().allMatch(
                        t -> t.getState() == MetaTask.TaskState.Task_Success);
                if (allSuccess) {
                    log.info("graph:{} combine task all success!", partition.getGraphName());
                    handleMoveTaskAllSuccess(subTasks, partition.getGraphName(), taskInfoMeta);
                } else {
                    log.info("graph:{} combine task failed!", partition.getGraphName());
                    handleMoveTaskIfFailed(partition.getGraphName(), taskInfoMeta);
                }
            }
        }
    }

    /**
     * When all migration subtasks succeed:
     * 1. Send cleanup source partition directives
     * 2. Set up target online, renewal key range, renewal graph partition count
     * 3. delete move task, mission ended
     *
     * @param subTasks     all move sub tasks
     * @param graphName    graph name
     * @param taskInfoMeta task info meta
     * @throws PDException returns if write db failed
     */
    private void handleMoveTaskAllSuccess(List<MetaTask.Task> subTasks, String graphName,
                                          TaskInfoMeta taskInfoMeta) throws PDException {

        var targetPartitionIds = new HashSet<Integer>();
        var targetPartitions = new ArrayList<Metapb.Partition>();
        var deleteFlags =
                subTasks.stream().map(task -> task.getMovePartition().getTargetPartition().getId())
                        .collect(Collectors.toSet());

        for (MetaTask.Task subTask : subTasks) {
            var source = subTask.getPartition();
            var targetPartition = subTask.getMovePartition().getTargetPartition();
            // Whether it has been dealt with or not
            if (!targetPartitionIds.contains(targetPartition.getId())) {
                // renewal range
                var old = getPartitionById(targetPartition.getGraphName(), targetPartition.getId());
                var newPartition = Metapb.Partition.newBuilder(old)
                                                   .setStartKey(targetPartition.getStartKey())
                                                   .setEndKey(targetPartition.getEndKey())
                                                   .setState(Metapb.PartitionState.PState_Normal)
                                                   .build();
                // Update before the key range to avoid the problem that the store does not have
                // a partition and needs to be queried to the pd
                updatePartition(List.of(newPartition));
                targetPartitions.add(newPartition);

                // Send key range change messages
                PartitionKeyRange partitionKeyRange = PartitionKeyRange.newBuilder()
                                                                       .setPartitionId(old.getId())
                                                                       .setKeyStart(
                                                                               targetPartition.getStartKey())
                                                                       .setKeyEnd(
                                                                               targetPartition.getEndKey())
                                                                       .build();
                // Notice store
                fireChangePartitionKeyRange(
                        old.toBuilder().setState(Metapb.PartitionState.PState_Normal).build(),
                        partitionKeyRange);

                // Set Target to go live. source could theoretically be deleted, so it is not
                // processed
                updatePartitionState(newPartition.getGraphName(), newPartition.getId(),
                                     Metapb.PartitionState.PState_Normal);

                targetPartitionIds.add(targetPartition.getId());
            }

            CleanPartition cleanPartition = CleanPartition.newBuilder()
                                                          .setKeyStart(source.getStartKey())
                                                          .setKeyEnd(source.getEndKey())
                                                          .setCleanType(
                                                                  CleanType.CLEAN_TYPE_EXCLUDE_RANGE)
                                                          // The partition of the target only
                                                          // needs to clean up the data, and does
                                                          // not need to delete the partition
                                                          .setDeletePartition(!deleteFlags.contains(
                                                                  source.getId()))
                                                          .build();

            log.info("pd clean data: {}-{}, key range:{}-{}, type:{}, delete partition:{}",
                     source.getGraphName(),
                     source.getId(),
                     cleanPartition.getKeyStart(),
                     cleanPartition.getKeyEnd(),
                     CleanType.CLEAN_TYPE_EXCLUDE_RANGE,
                     cleanPartition.getDeletePartition());

            // Clean up the data of the partition to be moved
            fireCleanPartition(source, cleanPartition);
        }

        // renewal key range, Local updates, client renewal
        // updatePartition(targetPartitions);

        // renewal target Partition status, source may be deleted, so do not process
        targetPartitions.forEach(p -> {
            try {
                updatePartitionState(p.getGraphName(), p.getId(),
                                     Metapb.PartitionState.PState_Normal);
            } catch (PDException e) {
                throw new RuntimeException(e);
            }
        });

        partitionMeta.reload();

        // renewal graph partition count
        var graph = getGraph(graphName).toBuilder()
                                       .setPartitionCount(targetPartitionIds.size())
                                       .build();
        updateGraph(graph);

        // The transaction is complete
        taskInfoMeta.removeMoveTaskPrefix(graphName);
    }

    /**
     * If the scale-in task fails, roll back the merge operation
     * 1. Clean up the original target partition and delete the migrated data
     * 2. Set the source/target partition to go live
     * 3. Delete the task, and the task ends
     *
     * @param graphName    graph name
     * @param taskInfoMeta task info meta
     * @throws PDException return if write to db failed
     */
    private void handleMoveTaskIfFailed(String graphName, TaskInfoMeta taskInfoMeta) throws
                                                                                     PDException {
        // Send cleanup target partition tasks,rollback target partition
        var targetPartitionIds = new HashSet<Integer>();
        for (var metaTask : taskInfoMeta.scanMoveTask(graphName)) {

            var source = metaTask.getPartition();
            // Set source to upline
            updatePartitionState(source.getGraphName(), source.getId(),
                                 Metapb.PartitionState.PState_Normal);
            var movedPartition = metaTask.getMovePartition().getTargetPartition();

            if (targetPartitionIds.contains(movedPartition.getId())) {
                continue;
            }

            var targetPartition = getPartitionById(graphName, movedPartition.getId());

            CleanPartition cleanPartition = CleanPartition.newBuilder()
                                                          .setKeyStart(
                                                                  targetPartition.getStartKey())
                                                          .setKeyEnd(targetPartition.getEndKey())
                                                          .setCleanType(
                                                                  CleanType.CLEAN_TYPE_KEEP_RANGE)
                                                          .setDeletePartition(false)
                                                          .build();
            fireCleanPartition(targetPartition, cleanPartition);
            targetPartitionIds.add(targetPartition.getId());

            // Set Target online
            updatePartitionState(targetPartition.getGraphName(), targetPartition.getId(),
                                 Metapb.PartitionState.PState_Normal);
        }
        // Clean up the task list
        taskInfoMeta.removeMoveTaskPrefix(graphName);
    }

    /**
     * dispose clean task
     *
     * @param task clean task
     */
    public void handleCleanPartitionTask(MetaTask.Task task) {
        log.info("clean task {} -{}, key range:{}~{}, report: {}",
                 task.getPartition().getGraphName(),
                 task.getPartition().getId(),
                 task.getCleanPartition().getKeyStart(),
                 task.getCleanPartition().getKeyEnd(),
                 task.getState()
        );

        // If it fails, try again?
    }

    public void handleBuildIndexTask(MetaTask.Task task) throws PDException {
        if (task == null) {
            throw new PDException(-1, "Invalid build index task: task is null");
        }

        if (task.getType() != MetaTask.TaskType.Build_Index) {
            throw new PDException(-1, "Task type must be Build_Index");
        }

        if (!task.hasBuildIndex()) {
            throw new PDException(-1, "Task must contain build index data");
        }

        log.info("build index task {} -{} , report state: {}",
                 task.getPartition().getGraphName(),
                 task.getPartition().getId(),
                 task.getState());

        try {
            storeService.getTaskInfoMeta().updateBuildIndexTask(task);
        } catch (Exception e) {
            log.error("Failed to update build index task {}", task.getId(), e);
            throw new PDException(-1, "Failed to update build index task: " + e.getMessage());
        }
    }

    public synchronized void handleSplitTask(MetaTask.Task task) throws PDException {

        var taskInfoMeta = storeService.getTaskInfoMeta();
        var partition = task.getPartition();

        MetaTask.Task pdMetaTask =
                taskInfoMeta.getSplitTask(partition.getGraphName(), partition.getId());

        log.info("report split task, graph:{}, pid : {}, state: {}",
                 task.getPartition().getGraphName(),
                 task.getPartition().getId(), task.getState());

        if (pdMetaTask != null) {
            var newTask = pdMetaTask.toBuilder().setState(task.getState()).build();
            taskInfoMeta.updateSplitTask(newTask);

            List<MetaTask.Task> subTasks = taskInfoMeta.scanSplitTask(partition.getGraphName());

            var finished = subTasks.stream().allMatch(t ->
                                                              t.getState() ==
                                                              MetaTask.TaskState.Task_Success ||
                                                              t.getState() ==
                                                              MetaTask.TaskState.Task_Failure);

            if (finished) {
                var allSuccess = subTasks.stream().allMatch(
                        t -> t.getState() == MetaTask.TaskState.Task_Success);
                if (allSuccess) {
                    log.info("graph:{} split task all success!", partition.getGraphName());
                    handleSplitTaskAllSuccess(subTasks, partition.getGraphName(), taskInfoMeta);
                } else {
                    handleSplitTaskIfFailed(subTasks, partition.getGraphName(), taskInfoMeta);
                }
            }
        }
    }

    private void handleSplitTaskAllSuccess(List<MetaTask.Task> subTasks, String graphName,
                                           TaskInfoMeta taskInfoMeta)
            throws PDException {

        int addedPartitions = 0;
        var partitions = new ArrayList<Metapb.Partition>();
        for (MetaTask.Task subTask : subTasks) {
            var source = subTask.getPartition();
            var newPartition = subTask.getSplitPartition().getNewPartitionList().get(0);

            // Send key range change messages
            PartitionKeyRange partitionKeyRange = PartitionKeyRange.newBuilder()
                                                                   .setPartitionId(source.getId())
                                                                   .setKeyStart(
                                                                           newPartition.getStartKey())
                                                                   .setKeyEnd(
                                                                           newPartition.getEndKey())
                                                                   .build();
            // Notice store
            fireChangePartitionKeyRange(source, partitionKeyRange);
            // Set Target to go live. source could theoretically be deleted, so it is not processed

            CleanPartition cleanPartition = CleanPartition.newBuilder()
                                                          .setKeyStart(newPartition.getStartKey())
                                                          .setKeyEnd(newPartition.getEndKey())
                                                          .setCleanType(
                                                                  CleanType.CLEAN_TYPE_KEEP_RANGE)
                                                          // The partition of the target only
                                                          // needs to clean up the data, and does
                                                          // not need to delete the partition
                                                          .setDeletePartition(false)
                                                          .build();

            log.info("pd clean data: {}-{}, key range:{}-{}, type:{}, delete partition:{}",
                     source.getGraphName(),
                     source.getId(),
                     cleanPartition.getKeyStart(),
                     cleanPartition.getKeyEnd(),
                     CleanType.CLEAN_TYPE_EXCLUDE_RANGE,
                     cleanPartition.getDeletePartition());

            fireCleanPartition(source, cleanPartition);

            // renewal partition state
            for (var sp : subTask.getSplitPartition().getNewPartitionList()) {
                partitions.add(
                        sp.toBuilder().setState(Metapb.PartitionState.PState_Normal).build());
            }

            addedPartitions += subTask.getSplitPartition().getNewPartitionCount() - 1;
        }

        updatePartition(partitions);
        partitionMeta.reload();

        var graph = getGraph(graphName);

        // set partition count
        if (pdConfig.getConfigService().getPartitionCount() !=
            storeService.getShardGroups().size()) {
            pdConfig.getConfigService().setPartitionCount(storeService.getShardGroups().size());
            log.info("set the partition count of config server to {}",
                     storeService.getShardGroups().size());
        }

        // renewal graph partition count
        var newGraph = graph.toBuilder()
                            .setPartitionCount(graph.getPartitionCount() + addedPartitions)
                            .build();
        updateGraph(newGraph);

        // The transaction is complete
        taskInfoMeta.removeSplitTaskPrefix(graphName);
    }

    private void handleSplitTaskIfFailed(List<MetaTask.Task> subTasks, String graphName,
                                         TaskInfoMeta taskInfoMeta)
            throws PDException {
        for (var metaTask : subTasks) {
            var splitPartitions = metaTask.getSplitPartition().getNewPartitionList();
            for (int i = 1; i < splitPartitions.size(); i++) {
                var split = splitPartitions.get(i);
                CleanPartition cleanPartition = CleanPartition.newBuilder()
                                                              .setKeyStart(split.getStartKey())
                                                              .setKeyEnd(split.getEndKey())
                                                              .setCleanType(
                                                                      CleanType.CLEAN_TYPE_EXCLUDE_RANGE)
                                                              .setDeletePartition(true)
                                                              .build();

                fireCleanPartition(split, cleanPartition);
            }

            // set partition state normal
            var partition = metaTask.getPartition();
            updatePartitionState(partition.getGraphName(), partition.getId(),
                                 Metapb.PartitionState.PState_Normal);
        }
        // Clean up the task list
        taskInfoMeta.removeSplitTaskPrefix(graphName);
    }

    /**
     * todo : What is the impact of partition changes??
     * Received a message that the leader has changed
     * Update the status of the graph and trigger a partition change
     */
    protected void onPartitionChanged(Metapb.Partition old, Metapb.Partition partition) {
        log.info("onPartitionChanged partition: {}", partition);
        if (old != null && old.getState() != partition.getState()) {
            Metapb.PartitionState state = Metapb.PartitionState.PState_Normal;
            for (Metapb.Partition pt : partitionMeta.getPartitions(partition.getGraphName())) {
                if (pt.getState().getNumber() > state.getNumber()) {
                    state = pt.getState();
                }
            }
            try {
                updateGraphState(partition.getGraphName(), state);
            } catch (PDException e) {
                log.error("onPartitionChanged", e);
            }

        }

        statusListeners.forEach(e -> {
            e.onPartitionChanged(old, partition);
        });
    }

    protected void onPartitionRemoved(Metapb.Partition partition) {
        log.info("onPartitionRemoved partition: {}", partition);
        statusListeners.forEach(e -> {
            e.onPartitionRemoved(partition);
        });
    }

    /**
     * The leader of the PD has changed and the data needs to be reloaded
     */
    @Override
    public void onRaftLeaderChanged() {
        log.info("Partition service reload cache from rocksdb, due to leader change");
        try {
            partitionMeta.reload();
        } catch (PDException e) {
            log.error("Partition meta reload exception {}", e);
        }
    }

    public void onPartitionStateChanged(String graph, int partId,
                                        Metapb.PartitionState state) throws PDException {
        updatePartitionState(graph, partId, state);
    }

    public void onShardStateChanged(String graph, int partId, Metapb.PartitionState state) {

    }

    /**
     * Send rocksdb compaction message
     *
     * @param partId
     * @param tableName
     */
    public void fireDbCompaction(int partId, String tableName) {

        try {
            for (Metapb.Graph graph : getGraphs()) {
                Metapb.Partition partition =
                        partitionMeta.getPartitionById(graph.getGraphName(), partId);
                // some graphs may doesn't have such partition
                if (partition == null) {
                    continue;
                }

                DbCompaction dbCompaction = DbCompaction.newBuilder()
                                                        .setTableName(tableName)
                                                        .build();
                instructionListeners.forEach(cmd -> {
                    try {
                        cmd.dbCompaction(partition, dbCompaction);
                        log.info("compact partition: {}", partId);
                    } catch (Exception e) {
                        log.error("firedbCompaction", e);
                    }
                });
                break;
            }
        } catch (PDException e) {
            e.printStackTrace();
        }

    }

    public void updateShardGroupCache(Metapb.ShardGroup group) {
        partitionMeta.getPartitionCache().updateShardGroup(group);
    }

    public Map<Integer, Metapb.ShardGroup> getShardGroupCache() {
        return partitionMeta.getPartitionCache().getShardGroups();
    }
}
