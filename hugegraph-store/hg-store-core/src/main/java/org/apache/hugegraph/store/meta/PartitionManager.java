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

package org.apache.hugegraph.store.meta;

import java.io.File;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;

import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.grpc.MetaTask;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.store.HgStoreEngine;
import org.apache.hugegraph.store.business.BusinessHandlerImpl;
import org.apache.hugegraph.store.cmd.HgCmdClient;
import org.apache.hugegraph.store.cmd.request.UpdatePartitionRequest;
import org.apache.hugegraph.store.cmd.response.UpdatePartitionResponse;
import org.apache.hugegraph.store.listener.PartitionChangedListener;
import org.apache.hugegraph.store.meta.base.GlobalMetaStore;
import org.apache.hugegraph.store.options.HgStoreEngineOptions;
import org.apache.hugegraph.store.options.MetadataOptions;
import org.apache.hugegraph.store.pd.PdProvider;
import org.apache.hugegraph.store.util.PartitionMetaStoreWrapper;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

import com.alipay.sofa.jraft.core.ElectionPriority;

import lombok.extern.slf4j.Slf4j;

/**
 * Partition object management strategy, each modification requires cloning a copy, and the
 * version number is incremented.
 */
@Slf4j
public class PartitionManager extends GlobalMetaStore {

    private static final Logger LOG = Log.logger(PartitionManager.class);
    private final PdProvider pdProvider;
    private final GraphManager graphManager;
    private final StoreMetadata storeMetadata;
    private final DeletedFileManager deletedFileManager;
    private final boolean useRaft;
    private final HgStoreEngineOptions options;
    private final List<PartitionChangedListener> partitionChangedListeners;
    // Read-write lock object
    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();

    private final PartitionMetaStoreWrapper wrapper = new PartitionMetaStoreWrapper();

    // Record all partition information of this machine, consistent with rocksdb storage.
    private Map<String, Map<Integer, Partition>> partitions;
    private HgCmdClient cmdClient;

    public PartitionManager(PdProvider pdProvider, HgStoreEngineOptions options) {
        super(new MetadataOptions() {{
            setDataPath(options.getDataPath());
            setRaftPath(options.getRaftPath());
        }});
        this.options = options;
        this.pdProvider = pdProvider;
        partitions = new ConcurrentHashMap<>();

        storeMetadata = new StoreMetadata(getOptions());
        graphManager = new GraphManager(getOptions(), pdProvider);
        deletedFileManager = new DeletedFileManager(getOptions());
        this.useRaft = true;
        partitionChangedListeners = new ArrayList<>();
    }

    public void load() {
        storeMetadata.load();
        graphManager.load();
        deletedFileManager.load();
    }

    public void loadPartition() {
        loadPartitions();
    }

    public DeletedFileManager getDeletedFileManager() {
        return deletedFileManager;
    }

    public PdProvider getPdProvider() {
        return pdProvider;
    }

    public StoreMetadata getStoreMetadata() {
        return storeMetadata;
    }

    public void addPartitionChangedListener(PartitionChangedListener listener) {
        partitionChangedListeners.add(listener);
    }

    /**
     * Judge the storage path as either the partition id or starting with partition id_
     *
     * @param detections  dir list
     * @param partitionId partition id
     * @param checkLogDir :  whether it includes the subdirectory log (raft snapshot and log
     *                    separation, further checks are needed)
     * @return true if contains partition id, otherwise false
     */
    private Boolean checkPathContains(File[] detections, int partitionId, boolean checkLogDir) {
        String partitionDirectory = String.format("%05d", partitionId);
        for (int x = 0; x < detections.length; x++) {
            // Must be in a folder named after the partition id
            if (detections[x].isDirectory()) {
                String tmp = detections[x].getName();
                if (tmp.equals(partitionDirectory) || tmp.startsWith(partitionDirectory + "_")) {
                    if (checkLogDir) {
                        String logDir = detections[x].getAbsolutePath() + "/log";
                        if (new File(logDir).exists()) {
                            return true;
                        }
                    } else {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    /**
     * According to the root directory of the profile, loop through to find the storage path of
     * the partition.
     * According to the agreement, db data is in the dataPath/db/partition_id directory, and raft
     * data is in the dataPath/raft/partition_id directory.
     * Check if the partition storage folder exists
     */
    private Boolean resetPartitionPath(int partitionId) {
        List<String> dataPaths = Arrays.asList(this.options.getDataPath().split(","));
        List<String> raftPaths = Arrays.asList(this.options.getRaftPath().split(","));

        boolean isDataOk = false;
        boolean isRaftOk = false;

        // Check db directory
        for (int i = 0; i < dataPaths.size(); i++) {
            String dbPath = Paths.get(dataPaths.get(i),
                                      HgStoreEngineOptions.DB_Path_Prefix).toAbsolutePath()
                                 .toString();
            File dbFile = new File(dbPath);
            if (dbFile.exists()) {
                File[] dbFiles = dbFile.listFiles();

                if (this.checkPathContains(dbFiles, partitionId, false)) {
                    Metapb.PartitionStore location = storeMetadata.getPartitionStore(partitionId);
                    if (!location.getStoreLocation().equals(dataPaths.get(i))) {
                        Metapb.PartitionStore newLocation = location.toBuilder()
                                                                    .setStoreLocation(
                                                                            dataPaths.get(i))
                                                                    .build();
                        storeMetadata.savePartitionStore(newLocation);
                    }
                    isDataOk = true;
                    break;
                }
            }
        }

        // Check raft directory
        for (int i = 0; i < raftPaths.size(); i++) {
            String raftPath = Paths.get(raftPaths.get(i),
                                        HgStoreEngineOptions.Raft_Path_Prefix).toAbsolutePath()
                                   .toString();

            File raftFile = new File(raftPath);

            if (raftFile.exists()) {
                File[] raftFiles = raftFile.listFiles();
                if (this.checkPathContains(raftFiles, partitionId, true)) {
                    Metapb.PartitionRaft location = storeMetadata.getPartitionRaft(partitionId);
                    // Compatible version upgrade
                    if (location == null ||
                        !Objects.equals(location.getRaftLocation(), raftPaths.get(i))) {
                        Metapb.PartitionRaft newLocation = Metapb.PartitionRaft.newBuilder()
                                                                               .setPartitionId(
                                                                                       partitionId)
                                                                               .setRaftLocation(
                                                                                       raftPaths.get(
                                                                                               i))
                                                                               .build();
                        storeMetadata.savePartitionRaft(newLocation);
                    }
                    isRaftOk = true;
                    break;
                }
            }
        }

        return isDataOk && isRaftOk;
    }

    /**
     * Read the partition from local storage
     */
    private void loadPartitions() {
        byte[] key = MetadataKeyHelper.getPartitionPrefixKey();
        long storeId = getStore().getId();

        // Read partition from data path
        // Record which partitions
        var partIds = new HashSet<Integer>();
        for (String path : this.options.getDataPath().split(",")) {
            File[] dirs = new File(path + "/" + HgStoreEngineOptions.DB_Path_Prefix).listFiles();
            if (dirs == null) {
                continue;
            }

            for (File f : dirs) {
                if (f.isDirectory()) {
                    try {
                        partIds.add(Integer.parseInt(f.getName().split("_")[0]));
                    } catch (Exception e) {
                        log.error("find illegal dir {} in data path, error:{}", f.getName(),
                                  e.getMessage());
                    }
                }
            }
        }

        Set<Integer> normalPartitions = new HashSet<>();

        // Once according to the partition read
        for (int partId : partIds) {
            if (!resetPartitionPath(partId)) {
                log.error("partition " + partId + " Directory not exists,options " +
                          this.options.getDataPath());
                continue;
            }

            var metaParts = wrapper.scan(partId, Metapb.Partition.parser(), key);
            int countOfPartition = 0;

            var shards = pdProvider.getShardGroup(partId).getShardsList();

            for (var metaPart : metaParts) {
                var graph = metaPart.getGraphName();
                var pdPartition = pdProvider.getPartitionByID(graph, metaPart.getId());
                boolean isLegeal = false;

                if (pdPartition != null) {
                    // Check if it contains this store id
                    if (shards.stream().anyMatch(s -> s.getStoreId() == storeId)) {
                        isLegeal = true;
                    }
                } else {
                    continue;
                }

                if (isLegeal) {
                    if (!partitions.containsKey(graph)) {
                        partitions.put(graph, new ConcurrentHashMap<>());
                    }

                    countOfPartition += 1;

                    Partition partition = new Partition(metaPart);
                    partition.setWorkState(
                            Metapb.PartitionState.PState_Normal);     // Start recovery work state
                    partitions.get(graph).put(partition.getId(), partition);
                    log.info("load partition : {} -{}", partition.getGraphName(),
                             partition.getId());
                } else {
                    // Invalid
                    // removePartitionFromLocalDb(graph, partId);
                    // var businessHandler = HgStoreEngine.getInstance().getBusinessHandler();
                    // businessHandler.truncate(graph, partId);
                    // businessHandler.dbCompaction(graph, partId);
                    log.error("partition {}-{} is illegal. store id {} not in valid shard group:{}",
                              graph, partId, getStore().getId(), shards2Peers(shards));
                    System.exit(0);
                }
            }

            if (countOfPartition > 0) {
                // Partition data is normal
                normalPartitions.add(partId);
            }
            wrapper.close(partId);
        }

        // Remove redundant partition storage paths, partitions that have been migrated away may migrate back
        for (var location : storeMetadata.getPartitionStores()) {
            if (!normalPartitions.contains(location.getPartitionId())) {
                storeMetadata.removePartitionStore(location.getPartitionId());
            }
        }
    }

    public List<Metapb.Partition> loadPartitionsFromDb(int partitionId) {
        byte[] key = MetadataKeyHelper.getPartitionPrefixKey();
        return wrapper.scan(partitionId, Metapb.Partition.parser(), key);
    }

    /**
     * Synchronize from PD and delete the extra local partitions.
     * During the synchronization process, new partitions need to be saved locally, and the
     * existing partition information is merged with the local data.
     */
    public void syncPartitionsFromPD(Consumer<Partition> delCallback) throws PDException {
        Lock writeLock = readWriteLock.writeLock();
        writeLock.lock();
        try {

            List<Partition> partListFrPD =
                    pdProvider.getPartitionsByStore(storeMetadata.getStore().getId());

            Map<String, Map<Integer, Partition>> graphPtFrpd = new HashMap<>();
            partListFrPD.forEach(partition -> {
                if (!graphPtFrpd.containsKey(partition.getGraphName())) {
                    graphPtFrpd.put(partition.getGraphName(), new HashMap<>());
                }
                if (isLocalPartition(partition)) {
                    graphPtFrpd.get(partition.getGraphName()).put(partition.getId(), partition);
                }
            });

            // Traverse the local map, delete the local extras, and append the new ones.
            partitions.forEach((graphName, v) -> {
                Map<Integer, Partition> partitionsFrpd = graphPtFrpd.get(graphName);
                v.forEach((id, pt) -> {
                    if (partitionsFrpd == null || !partitionsFrpd.containsKey(id)) {
                        // Local partition, pd no longer exists, needs to be deleted
                        delCallback.accept(pt);
                        removePartition(pt.getGraphName(), pt.getId());
                    } else {
                        // Modify shard information
                        // Partition ptFrpd = partitionsFrpd.get(id);
                        // pt.setShardsList(ptFrpd.getShardsList());
                        savePartition(pt, true, true);

                    }
                });
                if (partitionsFrpd != null) {
                    partitionsFrpd.forEach((id, pt) -> {
                        if (!v.containsKey(id)) {
                            // New partition added
                            savePartition(pt, true);
                        }
                    });
                }
            });
            partitions = graphPtFrpd;
        } finally {
            writeLock.unlock();
        }
    }

    public Partition changeState(Partition partition, Metapb.PartitionState state) {
        Lock writeLock = readWriteLock.writeLock();
        writeLock.lock();
        try {
            partition = findPartition(partition.getGraphName(), partition.getId());
            partition.setWorkState(state);
            savePartition(partition, false);
        } finally {
            writeLock.unlock();
        }
        return partition;
    }

    public Partition changeKeyRange(Partition partition, int startKey, int endKey) {
        Lock writeLock = readWriteLock.writeLock();
        writeLock.lock();
        try {
            partition = findPartition(partition.getGraphName(), partition.getId());
            partition.setStartKey(startKey);
            partition.setEndKey(endKey);
            savePartition(partition, false, true);
        } finally {
            writeLock.unlock();
        }
        return partition;
    }

    public Partition updatePartition(Metapb.Partition partition, boolean updateRange) {
        return updatePartition(new Partition(partition), updateRange);
    }

    /**
     * Add partition object
     *
     * @param partition
     * @return
     */
    public Partition updatePartition(Partition partition, boolean updateRange) {
        Lock writeLock = readWriteLock.writeLock();
        writeLock.lock();
        try {
            savePartition(partition, true, updateRange);
        } finally {
            writeLock.unlock();
        }
        return partition;
    }

    public void updatePartitionRangeOrState(UpdatePartitionRequest req) {
        Lock writeLock = readWriteLock.writeLock();
        writeLock.lock();
        try {
            Partition partition = findPartition(req.getGraphName(), req.getPartitionId());
            if (req.getStartKey() >= 0 && req.getEndKey() > 0
                && partition.getStartKey() != req.getStartKey() &&
                partition.getEndKey() != req.getEndKey()) {
                changeKeyRange(partition, req.getStartKey(), req.getEndKey());
            }
            if (req.getWorkState() != null) {
                changeState(partition, req.getWorkState());
            }
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Force update partition, do not verify version
     *
     * @param partition
     * @return
     */
    public Partition loadPartitionFromSnapshot(Partition partition) {
        Lock writeLock = readWriteLock.writeLock();
        writeLock.lock();
        try {
            savePartition(partition, true, true);
        } finally {
            writeLock.unlock();
        }
        return partition;
    }

    /**
     * Find the Partition belonging to this machine, prioritize searching locally, if not found
     * locally, inquire with pd.
     *
     * @param graph
     * @param partId
     * @return
     */
    public Partition findPartition(String graph, Integer partId) {
        Partition partition = null;
        if (partitions.containsKey(graph)) {
            partition = partitions.get(graph).get(partId);
        }

        if (partition == null) {
            partition = pdProvider.getPartitionByID(graph, partId);
            if (partition != null) {
                if (isLocalPartition(partition)) {

                    // Belong to the local machine's partition, save partition
                    Lock writeLock = readWriteLock.writeLock();
                    writeLock.lock();
                    try {
                        savePartition(partition, true);
                    } finally {
                        writeLock.unlock();
                    }
                } else {
                    LOG.error("Partition {}-{} does not belong to local store! store id{} \n {}",
                              graph, partId,
                              storeMetadata.getStore().getId(), partition.getProtoObj());
                    return null;
                }
            } else {
                LOG.error("Partition {}-{} is not Found! ", graph, partId);
                return null;
            }
        }
        return partitions.get(graph).get(partId);
    }

    public int getPartitionIdByCode(String graph, int code) {
        return pdProvider.getPartitionByCode(graph, code).getId();
    }

    /**
     * Get partition information from pd and merge it with local partition information. Leader
     * and shardList are taken from local.
     */
    public Partition getPartitionFromPD(String graph, int partId) {
        pdProvider.invalidPartitionCache(graph, partId);
        Partition partition = pdProvider.getPartitionByID(graph, partId);
        Lock writeLock = readWriteLock.writeLock();
        writeLock.lock();
        try {
            if (partitions.containsKey(graph)) {
                Partition local = partitions.get(graph).get(partId);
                if (local != null) {
                    // Update the local key range, ensuring consistency between pd and local
                    // partition information
                    local.setStartKey(partition.getStartKey());
                    local.setEndKey(partition.getEndKey());
                    savePartition(local, true, true);
                }
                partition = local;
            }
        } finally {
            writeLock.unlock();
        }
        return partition;
    }

    /**
     * Whether it is a local partition
     * For batch processing storage, only the leader is local.
     *
     * @param partition
     * @return
     */
    public boolean isLocalPartition(Partition partition) {
        boolean isLocal = false;
        var shardGroup = getShardGroup(partition.getId());
        if (shardGroup != null) {
            for (Shard shard : shardGroup.getShards()) {
                if (shard.getStoreId() == storeMetadata.getStore().getId()) {
                    isLocal = true;
                    break;
                }
            }
        }
        return isLocal;
    }

    /**
     * Whether it is a local partition
     * For batch inventory storage, only the leader is local.
     *
     * @return
     */
    public boolean isLocalPartition(int partId) {
        return pdProvider.isLocalPartition(storeMetadata.getStore().getId(), partId);
    }

    /**
     * Store partition information, synchronize saving to memory and rocksdb
     * Not update key range
     */

    private void savePartition(Partition partition, Boolean changeLeader) {
        savePartition(partition, changeLeader, false);
    }

    /**
     * Save partition information
     *
     * @param partition    partition
     * @param changeLeader is change leader
     * @param changeRange  update start and end key if yes.
     *                     using key range in local if no and partition key exists
     */

    private void savePartition(Partition partition, Boolean changeLeader, Boolean changeRange) {
        String graphName = partition.getGraphName();
        Integer partId = partition.getId();
        byte[] key = MetadataKeyHelper.getPartitionKey(graphName, partId);

        if (!changeRange) {
            var local = wrapper.get(partId, key, Metapb.Partition.parser());
            if (local != null) {
                partition.setStartKey(local.getStartKey());
                partition.setEndKey(local.getEndKey());
            }
        }

        if (!partitions.containsKey(graphName)) {
            partitions.put(graphName, new ConcurrentHashMap<>());
        }
        partitions.get(graphName).put(partition.getId(), partition);

        // put(key, partition.getProtoObj().toByteArray());
        wrapper.put(partId, key, partition.getProtoObj().toByteArray());

        Graph graph = new Graph();
        graph.setGraphName(partition.getGraphName());

        graphManager.updateGraph(graph);
        // Update PD cache, subsequent optimization, store does not depend on pdclient cache
        pdProvider.updatePartitionCache(partition, changeLeader);

        partitionChangedListeners.forEach(listener -> {
            listener.onChanged(
                    partition); // Notify raft, synchronize partition information synchronization
        });
    }

    /**
     * Update shard group to db, while updating the shardGroups object
     *
     * @param shardGroup
     */
    public void updateShardGroup(ShardGroup shardGroup) {
        Lock writeLock = readWriteLock.writeLock();
        writeLock.lock();
        wrapper.put(shardGroup.getId(),
                    MetadataKeyHelper.getShardGroupKey(shardGroup.getId()),
                    shardGroup.getProtoObj().toByteArray());
        writeLock.unlock();
    }

    /**
     * Find the shard group corresponding to the partition id.
     * Read in sequence from raft node/local db/ pd.
     *
     * @param partitionId
     * @return
     */
    public ShardGroup getShardGroup(int partitionId) {
        var partitionEngine = HgStoreEngine.getInstance().getPartitionEngine(partitionId);
        if (partitionEngine != null) {
            return partitionEngine.getShardGroup();
        }

        Metapb.ShardGroup shardGroup =
                wrapper.get(partitionId, MetadataKeyHelper.getShardGroupKey(partitionId),
                            Metapb.ShardGroup.parser());

        if (shardGroup == null) {
            shardGroup = pdProvider.getShardGroupDirect(partitionId);

            if (shardGroup != null) {
                // local not found, write back to db from pd
                wrapper.put(partitionId, MetadataKeyHelper.getShardGroupKey(partitionId),
                            shardGroup.toByteArray());
            } else {
                log.error("get shard group {} from pd failed", partitionId);
            }
        }
        return ShardGroup.from(shardGroup);
    }

    public Partition removePartition(String graphName, Integer partId) {
        log.info("partition manager: remove partition : {}-{}", graphName, partId);
        if (partitions.containsKey(graphName)) {
            pdProvider.invalidPartitionCache(graphName, partId);
            removePartitionFromLocalDb(graphName, partId);
            Partition partition = partitions.get(graphName).remove(partId);
            log.info("partition manager: remove partition, partition: {}", partition);
            if (partitions.get(graphName).size() == 0) {
                log.info("remove graph {}", graphName);
                graphManager.removeGraph(graphName);
            }
            return partition;
        }
        return null;
    }

    private void removePartitionFromLocalDb(String graphName, Integer partId) {
        byte[] key = MetadataKeyHelper.getPartitionKey(graphName, partId);
        // delete(key);
        wrapper.delete(partId, key);
    }

    /**
     * Delete graph data, delete local data, and delete partition information on PD.
     */
    public Partition deletePartition(String graphName, Integer partId) {
        removePartition(graphName, partId);
        return pdProvider.delPartition(graphName, partId);
    }

    // Get local Store information
    public Store getStore() {
        return storeMetadata.getStore();
    }

    // Registration will modify StoreId, need to reset
    public void setStore(Store store) {
        Lock writeLock = readWriteLock.writeLock();
        writeLock.lock();
        try {
            storeMetadata.save(store);
        } finally {
            writeLock.unlock();
        }
    }

    public Store getStore(Long storeId) {
        return pdProvider.getStoreByID(storeId);
    }

    public Map<String, Map<Integer, Partition>> getPartitions() {
        return partitions;
    }

    public Map<String, Partition> getPartitions(int partitionId) {
        Map<String, Partition> result = new HashMap<>();
        this.partitions.forEach((k, v) -> {
                                    v.forEach((k1, v1) -> {
                                        if (k1 == partitionId) {
                                            result.put(k, v1);
                                        }
                                    });
                                }
        );
        return result;
    }

    public Partition getPartition(String graphName, int partitionId) {
        return this.partitions.getOrDefault(graphName, new HashMap<>())
                              .getOrDefault(partitionId, null);
    }

    public List<Partition> getPartitionList(int partitionId) {
        List<Partition> pts = new ArrayList<>();
        getPartitions(partitionId).forEach((k, v) -> {
            pts.add(findPartition(k, v.getId()));
        });
        return pts;
    }

    public boolean hasPartition(String graphName, int partitionId) {
        return this.partitions.getOrDefault(graphName, new HashMap<>()).containsKey(partitionId);
    }

    /**
     * Get all Leader partitions in this graph locally.
     *
     * @param graph
     * @return
     */
    public List<Integer> getLeaderPartitionIds(String graph) {
        List<Integer> ids = new ArrayList<>();
        if (partitions.containsKey(graph)) {
            partitions.get(graph).forEach((k, v) -> {
                if (!useRaft || v.isLeader()) {
                    ids.add(k);
                }
            });
        }
        return ids;
    }

    public Set<Integer> getLeaderPartitionIdSet() {
        Set<Integer> ids = new HashSet<>();
        partitions.forEach((key, value) -> {
            value.forEach((k, v) -> {
                if (!useRaft || v.isLeader()) {
                    ids.add(k);
                }
            });
        });
        return ids;
    }

    /**
     * Generate partition peer string, containing priority information *
     *
     * @param shardGroup
     * @return
     */
    public List<String> getPartitionPeers(ShardGroup shardGroup) {
        List<String> peers = new ArrayList<>();
        final int decayPriorityGap = 10;
        int priority = 100;
        if (shardGroup != null) {
            for (Shard shard : shardGroup.getShards()) {
                Store store = getStore(shard.getStoreId());
                if (store != null && !store.getRaftAddress().isEmpty()) {
                    peers.add(store.getRaftAddress() + "::" + priority);
                    final int gap = Math.max(decayPriorityGap, (priority / 5));
                    priority = Math.max(ElectionPriority.MinValue, (priority - gap));
                }
            }
        }

        return peers;
    }

    public List<String> shards2Peers(List<Metapb.Shard> shards) {
        List<String> peers = new ArrayList<>();
        shards.forEach(s -> {
            peers.add(getStore(s.getStoreId()).getRaftAddress());
        });
        return peers;
    }

    /**
     * Whether it is a local store
     *
     * @param store
     * @return
     */
    public boolean isLocalStore(Store store) {
        return storeMetadata.getStore().getId() == store.getId();
    }

    public PartitionRole getLocalRoleFromShard(Partition partition) {
        return partition.isLeader() ? PartitionRole.LEADER : PartitionRole.FOLLOWER;
    }

    /**
     * Modify partition role
     */
    public Partition changeLeader(Partition pt, List<Metapb.Shard> shards, long term) {
        Lock writeLock = readWriteLock.writeLock();
        writeLock.lock();
        try {
            Partition partition = findPartition(pt.getGraphName(), pt.getId());
            if (partition != null) {
                // partition.setShardsList(shards);
                partition.setVersion(term);
                savePartition(partition, true);
            }
            return partition;
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * According to the raft peers list, rebuild shardList
     */
    public Partition changeShards(Partition pt, List<Metapb.Shard> shards) {
        Lock writeLock = readWriteLock.writeLock();
        writeLock.lock();
        try {
            Partition partition = findPartition(pt.getGraphName(), pt.getId());
            if (partition != null) {
                // partition.setShardsList(shards);
                // partition.setConfVer(partition.getConfVer() + 1);
                savePartition(partition, true);
            }
            return partition;
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Split partition object
     */
    public List<Metapb.Partition> updatePartitionToPD(List<Metapb.Partition> partitions) throws
                                                                                         PDException {
        // Update local partition information, as well as cache information
        return pdProvider.updatePartition(partitions);
    }

    /**
     * According to raft address to find Store
     */
    public Store getStoreByRaftEndpoint(ShardGroup group, String endpoint) {
        final Store[] result = {new Store()};
        group.getShards().forEach((shard) -> {
            Store store = getStore(shard.getStoreId());
            if (store != null && store.getRaftAddress().equalsIgnoreCase(endpoint)) {
                result[0] = store;
            }
        });
        return result[0];
    }

    public Shard getShardByEndpoint(ShardGroup group, String endpoint) {
        List<Shard> shards = group.getShards();
        for (Shard shard : shards) {
            Store store = getStore(shard.getStoreId());
            if (store != null && store.getRaftAddress().equalsIgnoreCase(endpoint)) {
                return shard;
            }
        }
        return new Shard();
    }

    /**
     * raft storage path
     *
     * @param groupId
     * @return location/raft/groupId/
     */
    public String getRaftDataPath(int groupId) {
        String location = storeMetadata.getPartitionRaftLocation(groupId);
        location = Paths.get(location,
                             HgStoreEngineOptions.Raft_Path_Prefix,
                             String.format("%05d", groupId)).toAbsolutePath().toString();
        return location;
    }

    /**
     * raft snapshot path should be on the same disk as the db, convenient for hard link
     *
     * @param groupId raft group id
     * @return location/snapshot/0000x/
     */
    public String getRaftSnapShotPath(int groupId) {
        String dbName = BusinessHandlerImpl.getDbName(groupId);
        String location = storeMetadata.getPartitionStoreLocation(groupId, dbName);
        location = Paths.get(location,
                             HgStoreEngineOptions.Raft_Path_Prefix,
                             dbName).toAbsolutePath().toString();
        return location;
    }

    /**
     * db storage path
     *
     * @return location/db
     */
    public String getDbDataPath(int partitionId, String dbName) {
        String location = storeMetadata.getPartitionStoreLocation(partitionId, dbName);
        location = Paths.get(location,
                             HgStoreEngineOptions.DB_Path_Prefix).toAbsolutePath().toString();
        return location;
    }

    /**
     * DB storage path
     *
     * @return location/db
     */
    public String getDbDataPath(int partitionId) {
        String dbName = BusinessHandlerImpl.getDbName(partitionId);
        return getDbDataPath(partitionId, dbName);
    }

    public void reportTask(MetaTask.Task task) {
        try {
            pdProvider.reportTask(task);
        } catch (Exception e) {
            LOG.error("reportTask exception {}, {}", e, task);
        }
    }

    /**
     * Modify the state of the partition state
     */
    public List<Metapb.Partition> changePartitionToOnLine(List<Metapb.Partition> partitions) {
        List<Metapb.Partition> newPartitions = new ArrayList<>();
        partitions.forEach(e -> {
            newPartitions.add(e.toBuilder().setState(Metapb.PartitionState.PState_Normal).build());
        });
        return newPartitions;
    }

    public PartitionMetaStoreWrapper getWrapper() {
        return wrapper;
    }

    public void setCmdClient(HgCmdClient client) {
        this.cmdClient = client;
    }

    public UpdatePartitionResponse updateState(Metapb.Partition partition,
                                               Metapb.PartitionState state) {
        // During partition splitting, actively need to find leader for information synchronization
        UpdatePartitionRequest request = new UpdatePartitionRequest();
        request.setWorkState(state);
        request.setPartitionId(partition.getId());
        request.setGraphName(partition.getGraphName());
        return cmdClient.raftUpdatePartition(request);
    }

    public UpdatePartitionResponse updateRange(Metapb.Partition partition, int startKey,
                                               int endKey) {
        // During partition splitting, actively need to find leader for information synchronization
        UpdatePartitionRequest request = new UpdatePartitionRequest();
        request.setStartKey(startKey);
        request.setEndKey(endKey);
        request.setPartitionId(partition.getId());
        request.setGraphName(partition.getGraphName());
        return cmdClient.raftUpdatePartition(request);
    }

    public List<Integer> getPartitionIds(String graph) {
        List<Integer> ids = new ArrayList<>();
        if (partitions.containsKey(graph)) {
            partitions.get(graph).forEach((k, v) -> {
                ids.add(k);
            });
        }
        return ids;
    }

}
