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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang3.StringUtils;
import org.apache.hugegraph.pd.common.KVPair;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.config.PDConfig;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.pd.grpc.Metapb.GraphMode;
import org.apache.hugegraph.pd.grpc.Metapb.GraphModeReason;
import org.apache.hugegraph.pd.grpc.Metapb.GraphState;
import org.apache.hugegraph.pd.grpc.Pdpb;
import org.apache.hugegraph.pd.grpc.Pdpb.CacheResponse;
import org.apache.hugegraph.pd.grpc.pulse.ConfChangeType;
import org.apache.hugegraph.pd.meta.MetadataFactory;
import org.apache.hugegraph.pd.meta.MetadataKeyHelper;
import org.apache.hugegraph.pd.meta.StoreInfoMeta;
import org.apache.hugegraph.pd.meta.TaskInfoMeta;

import com.google.gson.Gson;

import lombok.extern.slf4j.Slf4j;

/**
 * Hg Store registration and keep-alive management
 */
@Slf4j
public class StoreNodeService {

    private static final Long STORE_HEART_BEAT_INTERVAL = 30000L;
    private static String graphSpaceConfPrefix = "HUGEGRAPH/hg/GRAPHSPACE/CONF/";
    private List<StoreStatusListener> statusListeners;
    private List<ShardGroupStatusListener> shardGroupStatusListeners;
    private StoreInfoMeta storeInfoMeta;
    private TaskInfoMeta taskInfoMeta;
    private Random random = new Random(System.currentTimeMillis());
    private KvService kvService;
    private ConfigService configService;
    private PDConfig pdConfig;
    private PartitionService partitionService;
    private Runnable quotaChecker = () -> {
        try {
            getQuota();
        } catch (Exception e) {
            log.error(
                    "obtaining and sending graph space quota information with error: ",
                    e);
        }
    };
    private volatile Metapb.ClusterStats clusterStats;

    public StoreNodeService(PDConfig config) {
        this.pdConfig = config;
        storeInfoMeta = MetadataFactory.newStoreInfoMeta(pdConfig);
        taskInfoMeta = MetadataFactory.newTaskInfoMeta(pdConfig);
        shardGroupStatusListeners = Collections.synchronizedList(new ArrayList<>());
        statusListeners = Collections.synchronizedList(new ArrayList<StoreStatusListener>());
        clusterStats = Metapb.ClusterStats.newBuilder()
                                          .setState(Metapb.ClusterState.Cluster_Not_Ready)
                                          .setTimestamp(System.currentTimeMillis())
                                          .build();
        kvService = new KvService(pdConfig);
        configService = new ConfigService(pdConfig);
    }

    public void init(PartitionService partitionService) {
        this.partitionService = partitionService;
        partitionService.addStatusListener(new PartitionStatusListener() {
            @Override
            public void onPartitionChanged(Metapb.Partition old, Metapb.Partition partition) {
                if (old != null && old.getState() != partition.getState()) {
                    try {
                        Metapb.PartitionState state = Metapb.PartitionState.PState_Normal;
                        for (Metapb.ShardGroup group : getShardGroups()) {
                            if (group.getState().getNumber() > state.getNumber()) {
                                state = group.getState();
                            }
                        }
                        updateClusterStatus(state);
                    } catch (PDException e) {
                        log.error("onPartitionChanged exception: ", e);
                    }
                }
            }

            @Override
            public void onPartitionRemoved(Metapb.Partition partition) {

            }
        });
    }

    /**
     * Whether the cluster is ready or not
     *
     * @return
     */
    public boolean isOK() {
        return this.clusterStats.getState().getNumber() <
               Metapb.ClusterState.Cluster_Offline.getNumber();
    }

    /**
     * Store registration, record the IP address of the Store, and the first registration needs
     * to generate a store_ID
     *
     * @param store
     */
    public Metapb.Store register(Metapb.Store store) throws PDException {
        if (store.getId() == 0) {
            // Initial registration, generate a new ID, and ensure that the ID is not duplicated.
            store = newStoreNode(store);
        }

        if (!storeInfoMeta.storeExists(store.getId())) {
            log.error("Store id {} does not belong to this PD, address = {}", store.getId(),
                      store.getAddress());
            // storeId does not exist, an exception is thrown
            throw new PDException(Pdpb.ErrorType.STORE_ID_NOT_EXIST_VALUE,
                                  String.format("Store id %d doest not exist.", store.getId()));
        }

        // If the store status is Tombstone, the registration is denied.
        Metapb.Store lastStore = storeInfoMeta.getStore(store.getId());
        if (lastStore.getState() == Metapb.StoreState.Tombstone) {
            log.error("Store id {} has been removed, Please reinitialize, address = {}",
                      store.getId(), store.getAddress());
            // storeId does not exist, an exception is thrown
            throw new PDException(Pdpb.ErrorType.STORE_HAS_BEEN_REMOVED_VALUE,
                                  String.format("Store id %d has been removed. %s", store.getId(),
                                                store.getAddress()));
        }

        // offline or up, or in the initial activation list, go live automatically
        Metapb.StoreState storeState = lastStore.getState();
        if (storeState == Metapb.StoreState.Offline || storeState == Metapb.StoreState.Up
            || inInitialStoreList(store)) {
            storeState = Metapb.StoreState.Up;
        } else {
            storeState = Metapb.StoreState.Pending;
        }

        store = Metapb.Store.newBuilder(lastStore)
                            .setAddress(store.getAddress())
                            .setRaftAddress(store.getRaftAddress())
                            .setDataVersion(store.getDataVersion())
                            .setDeployPath(store.getDeployPath())
                            .setVersion(store.getVersion())
                            .setDataPath(store.getDataPath())
                            .setState(storeState).setCores(store.getCores())
                            .clearLabels().addAllLabels(store.getLabelsList())
                            .setLastHeartbeat(System.currentTimeMillis()).build();

        long current = System.currentTimeMillis();
        boolean raftChanged = false;
        // On-line status Raft Address there has been a change
        if (!Objects.equals(lastStore.getRaftAddress(), store.getRaftAddress()) &&
            storeState == Metapb.StoreState.Up) {
            // If the time interval is too short and the raft changes, it is considered an
            // invalid store
            if (current - lastStore.getLastHeartbeat() < STORE_HEART_BEAT_INTERVAL * 0.8) {
                throw new PDException(Pdpb.ErrorType.STORE_PROHIBIT_DUPLICATE_VALUE,
                                      String.format("Store id %d may be duplicate. addr: %s",
                                                    store.getId(), store.getAddress()));
            } else if (current - lastStore.getLastHeartbeat() > STORE_HEART_BEAT_INTERVAL * 1.2) {
                // It is considered that a change has occurred
                raftChanged = true;
            } else {
                // Wait for the next registration
                return Metapb.Store.newBuilder(store).setId(0L).build();
            }
        }

        // Store information
        storeInfoMeta.updateStore(store);
        if (storeState == Metapb.StoreState.Up) {
            // Update the store active status
            storeInfoMeta.keepStoreAlive(store);
            onStoreStatusChanged(store, Metapb.StoreState.Offline, Metapb.StoreState.Up);
            checkStoreStatus();
        }

        // Wait for the store information to be saved before sending the changes
        if (raftChanged) {
            onStoreRaftAddressChanged(store);
        }

        log.info("Store register, id = {} {}", store.getId(), store);
        return store;
    }

    private boolean inInitialStoreList(Metapb.Store store) {
        return this.pdConfig.getInitialStoreMap().containsKey(store.getAddress());
    }

    /**
     * Creates a new store object
     *
     * @param store
     * @return
     * @throws PDException
     */
    private synchronized Metapb.Store newStoreNode(Metapb.Store store) throws PDException {
        long id = random.nextLong() & Long.MAX_VALUE;
        while (id == 0 || storeInfoMeta.storeExists(id)) {
            id = random.nextLong() & Long.MAX_VALUE;
        }
        store = Metapb.Store.newBuilder(store)
                            .setId(id)
                            .setState(Metapb.StoreState.Pending)
                            .setStartTimestamp(System.currentTimeMillis()).build();
        storeInfoMeta.updateStore(store);
        return store;
    }

    /**
     * Returns Store information based on store_id
     *
     * @param id
     * @return
     * @throws PDException
     */
    public Metapb.Store getStore(long id) throws PDException {
        Metapb.Store store = storeInfoMeta.getStore(id);
        if (store == null) {
            throw new PDException(Pdpb.ErrorType.STORE_ID_NOT_EXIST_VALUE,
                                  String.format("Store id %x doest not exist.", id));
        }
        return store;
    }

    /**
     * Update the store information, detect the change of store status, and notify Hugestore
     */
    public synchronized Metapb.Store updateStore(Metapb.Store store) throws PDException {
        log.info("updateStore storeId: {}, address: {}, state: {}", store.getId(),
                 store.getAddress(), store.getState());
        Metapb.Store lastStore = storeInfoMeta.getStore(store.getId());
        if (lastStore == null) {
            return null;
        }
        Metapb.Store.Builder builder =
                Metapb.Store.newBuilder(lastStore).clearLabels().clearStats();
        store = builder.mergeFrom(store).build();
        if (store.getState() == Metapb.StoreState.Tombstone) {
            List<Metapb.Store> activeStores = getStores();
            if (lastStore.getState() == Metapb.StoreState.Up
                && activeStores.size() - 1 < pdConfig.getMinStoreCount()) {
                throw new PDException(Pdpb.ErrorType.LESS_ACTIVE_STORE_VALUE,
                                      "The number of active stores is less then " +
                                      pdConfig.getMinStoreCount());
            }
        }

        storeInfoMeta.updateStore(store);
        if (store.getState() != Metapb.StoreState.Unknown &&
            store.getState() != lastStore.getState()) {
            // If you want to take the store offline
            if (store.getState() == Metapb.StoreState.Exiting) {
                if (lastStore.getState() == Metapb.StoreState.Exiting) {
                    // If it is already in the offline state, no further processing will be made
                    return lastStore;
                }

                List<Metapb.Store> activeStores = this.getActiveStores();
                Map<Long, Metapb.Store> storeMap = new HashMap<>();
                activeStores.forEach(s -> {
                    storeMap.put(s.getId(), s);
                });
                // If the store is offline, delete it directly from active, and if the store is
                // online, temporarily delete it from active, and then delete it when the status
                // is set to Tombstone
                if (!storeMap.containsKey(store.getId())) {
                    log.info("updateStore removeActiveStores store {}", store.getId());
                    storeInfoMeta.removeActiveStore(store);
                }
                storeTurnoff(store);
            } else if (store.getState() == Metapb.StoreState.Offline) {
                // Monitor that the store has gone offline and is removed from the active
                storeInfoMeta.removeActiveStore(store);
            } else if (store.getState() == Metapb.StoreState.Tombstone) {
                // When the status changes, the store is shut down, the shardGroup is modified,
                // and the replica is migrated
                log.info("updateStore removeActiveStores store {}", store.getId());
                storeInfoMeta.removeActiveStore(store);
                // Storage goes offline
                storeTurnoff(store);
            } else if (store.getState() == Metapb.StoreState.Up) {
                storeInfoMeta.keepStoreAlive(store);
                checkStoreStatus();
            }
            onStoreStatusChanged(lastStore, lastStore.getState(), store.getState());
        }
        return store;
    }

    /**
     * The shard of the shardGroup is reassigned
     *
     * @param store
     * @throws PDException
     */
    public synchronized void storeTurnoff(Metapb.Store store) throws PDException {
        // Traverse ShardGroup,redistribution
        for (Metapb.ShardGroup group : getShardGroupsByStore(store.getId())) {
            Metapb.ShardGroup.Builder builder = Metapb.ShardGroup.newBuilder(group);
            builder.clearShards();
            group.getShardsList().forEach(shard -> {
                if (shard.getStoreId() != store.getId()) {
                    builder.addShards(shard);
                }
            });
            reallocShards(builder.build());
        }
    }

    /**
     * Returns stores information based on the graph name, and if graphName is empty, all store
     * information is returned
     *
     * @throws PDException
     */
    public List<Metapb.Store> getStores() throws PDException {
        return storeInfoMeta.getStores(null);
    }

    public List<Metapb.Store> getStores(String graphName) throws PDException {
        return storeInfoMeta.getStores(graphName);
    }

    public List<Metapb.Store> getStoreStatus(boolean isActive) throws PDException {
        return storeInfoMeta.getStoreStatus(isActive);
    }

    public List<Metapb.ShardGroup> getShardGroups() throws PDException {
        return storeInfoMeta.getShardGroups();
    }

    public Metapb.ShardGroup getShardGroup(int groupId) throws PDException {
        return storeInfoMeta.getShardGroup(groupId);
    }

    public List<Metapb.Shard> getShardList(int groupId) throws PDException {
        var shardGroup = getShardGroup(groupId);
        if (shardGroup != null) {
            return shardGroup.getShardsList();
        }
        return new ArrayList<>();
    }

    public List<Metapb.ShardGroup> getShardGroupsByStore(long storeId) throws PDException {
        List<Metapb.ShardGroup> shardGroups = new ArrayList<>();
        storeInfoMeta.getShardGroups().forEach(shardGroup -> {
            shardGroup.getShardsList().forEach(shard -> {
                if (shard.getStoreId() == storeId) {
                    shardGroups.add(shardGroup);
                }
            });
        });
        return shardGroups;
    }

    /**
     * Returns the active store
     *
     * @param graphName
     * @return
     * @throws PDException
     */
    public List<Metapb.Store> getActiveStores(String graphName) throws PDException {
        return storeInfoMeta.getActiveStores(graphName);
    }

    public List<Metapb.Store> getActiveStores() throws PDException {
        return storeInfoMeta.getActiveStores();
    }

    public List<Metapb.Store> getTombStores() throws PDException {
        List<Metapb.Store> stores = new ArrayList<>();
        for (Metapb.Store store : this.getStores()) {
            if (store.getState() == Metapb.StoreState.Tombstone) {
                stores.add(store);
            }
        }
        return stores;
    }

    public long removeStore(Long storeId) throws PDException {
        return storeInfoMeta.removeStore(storeId);
    }

    /**
     * todo : New logic
     * Assign a store to the partition and decide how many peers to allocate according to the
     * configuration of the graph
     * After allocating all the shards, save the ShardGroup object (store does not change, only
     * executes once)
     */
    public synchronized List<Metapb.Shard> allocShards(Metapb.Graph graph, int partId) throws
                                                                                       PDException {
        // Multiple graphs share raft grouping, so assigning shard only depends on partitionId.
        // The number of partitions can be set based on the size of the data, but the total
        // number cannot exceed the number of raft groups
        if (storeInfoMeta.getShardGroup(partId) == null) {
            // Get active store key
            List<Metapb.Store> stores = storeInfoMeta.getActiveStores();

            if (stores.size() == 0) {
                throw new PDException(Pdpb.ErrorType.NO_ACTIVE_STORE_VALUE,
                                      "There is no any online store");
            }

            if (stores.size() < pdConfig.getMinStoreCount()) {
                throw new PDException(Pdpb.ErrorType.LESS_ACTIVE_STORE_VALUE,
                                      "The number of active stores is less then " +
                                      pdConfig.getMinStoreCount());
            }

            int shardCount = pdConfig.getPartition().getShardCount();
            shardCount = Math.min(shardCount, stores.size());
            // Two shards could not elect a leader
            // It cannot be 0

            if (shardCount == 2 || shardCount < 1) {
                shardCount = 1;
            }

            // All ShardGroups are created at one time to ensure that the initial groupIDs are
            // orderly and easy for humans to read
            for (int groupId = 0; groupId < pdConfig.getConfigService().getPartitionCount();
                 groupId++) {
                int storeIdx = groupId % stores.size();  // Assignment rules, simplified to modulo
                List<Metapb.Shard> shards = new ArrayList<>();
                for (int i = 0; i < shardCount; i++) {
                    Metapb.Shard shard =
                            Metapb.Shard.newBuilder().setStoreId(stores.get(storeIdx).getId())
                                        .setRole(i == 0 ? Metapb.ShardRole.Leader :
                                                 Metapb.ShardRole.Follower) //
                                        .build();
                    shards.add(shard);
                    storeIdx = (storeIdx + 1) >= stores.size() ? 0 : ++storeIdx; // Sequential
                }

                Metapb.ShardGroup group = Metapb.ShardGroup.newBuilder()
                                                           .setId(groupId)
                                                           .setState(
                                                                   Metapb.PartitionState.PState_Normal)
                                                           .addAllShards(shards).build();

                // new group
                storeInfoMeta.updateShardGroup(group);
                partitionService.updateShardGroupCache(group);
                onShardGroupStatusChanged(null, group);
                log.info("alloc shard group: id {}", groupId);
            }
        }
        return storeInfoMeta.getShardGroup(partId).getShardsList();
    }

    /**
     * Based on the shard_count of the graph, reallocate shards
     * Send change shard
     */
    public synchronized List<Metapb.Shard> reallocShards(Metapb.ShardGroup shardGroup) throws
                                                                                       PDException {
        List<Metapb.Store> stores = storeInfoMeta.getActiveStores();

        if (stores.size() == 0) {
            throw new PDException(Pdpb.ErrorType.NO_ACTIVE_STORE_VALUE,
                                  "There is no any online store");
        }

        if (stores.size() < pdConfig.getMinStoreCount()) {
            throw new PDException(Pdpb.ErrorType.LESS_ACTIVE_STORE_VALUE,
                                  "The number of active stores is less then " +
                                  pdConfig.getMinStoreCount());
        }

        int shardCount = pdConfig.getPartition().getShardCount();
        shardCount = Math.min(shardCount, stores.size());
        if (shardCount == 2 || shardCount < 1) {
            // Two shards could not elect a leader
            // It cannot be 0
            shardCount = 1;
        }

        List<Metapb.Shard> shards = new ArrayList<>();
        shards.addAll(shardGroup.getShardsList());

        if (shardCount > shards.size()) {
            // Need to add shards
            log.info("reallocShards ShardGroup {}, add shards from {} to {}",
                     shardGroup.getId(), shards.size(), shardCount);
            int storeIdx = (int) shardGroup.getId() % stores.size();
            for (int addCount = shardCount - shards.size(); addCount > 0; ) {
                // Check if it already exists
                if (!isStoreInShards(shards, stores.get(storeIdx).getId())) {
                    Metapb.Shard shard = Metapb.Shard.newBuilder()
                                                     .setStoreId(stores.get(storeIdx).getId())
                                                     .build();
                    shards.add(shard);
                    addCount--;
                }
                storeIdx = (storeIdx + 1) >= stores.size() ? 0 : ++storeIdx;
            }
        } else if (shardCount < shards.size()) {
            // Need to reduce shard
            log.info("reallocShards ShardGroup {}, remove shards from {} to {}",
                     shardGroup.getId(), shards.size(), shardCount);

            int subCount = shards.size() - shardCount;
            Iterator<Metapb.Shard> iterator = shards.iterator();
            while (iterator.hasNext() && subCount > 0) {
                if (iterator.next().getRole() != Metapb.ShardRole.Leader) {
                    iterator.remove();
                    subCount--;
                }
            }
        } else {
            return shards;
        }

        Metapb.ShardGroup group = Metapb.ShardGroup.newBuilder(shardGroup)
                                                   .clearShards()
                                                   .addAllShards(shards).build();
        storeInfoMeta.updateShardGroup(group);
        partitionService.updateShardGroupCache(group);
        // change shard group
        // onShardGroupStatusChanged(shardGroup, group);

        var partitions = partitionService.getPartitionById(shardGroup.getId());
        if (partitions.size() > 0) {
            // send one message, change shard is regardless with partition/graph
            partitionService.fireChangeShard(partitions.get(0), shards,
                                             ConfChangeType.CONF_CHANGE_TYPE_ADJUST);
        }

        log.info("reallocShards ShardGroup {}, shards: {}", group.getId(), group.getShardsList());
        return shards;
    }

    /**
     * According to the number of partitions, distribute group shard
     *
     * @param groups list of (partition id, count)
     * @return total groups
     */
    public synchronized int splitShardGroups(List<KVPair<Integer, Integer>> groups) throws
                                                                                    PDException {
        int sum = groups.stream().map(pair -> pair.getValue()).reduce(0, Integer::sum);
        // shard group is too big
        if (sum > getActiveStores().size() * pdConfig.getPartition().getMaxShardsPerStore()) {
            throw new PDException(Pdpb.ErrorType.Too_Many_Partitions_Per_Store_VALUE,
                                  "can't satisfy target shard group count");
        }

        partitionService.splitPartition(groups);

        return sum;
    }

    /**
     * Alloc shard group, prepare for the split
     *
     * @param
     * @return true
     * @throws PDException
     */
    private boolean isStoreInShards(List<Metapb.Shard> shards, long storeId) {
        AtomicBoolean exist = new AtomicBoolean(false);
        shards.forEach(s -> {
            if (s.getStoreId() == storeId) {
                exist.set(true);
            }
        });
        return exist.get();
    }

    /**
     * update shard group and cache.
     * send shard group change message.
     *
     * @param groupId     : shard group
     * @param shards      : shard lists
     * @param version:    term version, ignored if less than 0
     * @param confVersion : conf version, ignored if less than 0
     * @return
     */
    public synchronized Metapb.ShardGroup updateShardGroup(int groupId, List<Metapb.Shard> shards,
                                                           long version, long confVersion) throws
                                                                                           PDException {
        Metapb.ShardGroup group = this.storeInfoMeta.getShardGroup(groupId);

        if (group == null) {
            return null;
        }

        var builder = Metapb.ShardGroup.newBuilder(group);
        if (version >= 0) {
            builder.setVersion(version);
        }

        if (confVersion >= 0) {
            builder.setConfVer(confVersion);
        }

        var newGroup = builder.clearShards().addAllShards(shards).build();

        storeInfoMeta.updateShardGroup(newGroup);
        partitionService.updateShardGroupCache(newGroup);
        onShardGroupStatusChanged(group, newGroup);
        log.info("Raft {} updateShardGroup {}", groupId, newGroup);
        return group;
    }

    /**
     * Notify the Store to rebuild the shard group
     *
     * @param groupId raft group id
     * @param shards  shard list: If it is empty, delete the corresponding one partition engine
     */
    public void shardGroupOp(int groupId, List<Metapb.Shard> shards) throws PDException {

        var shardGroup = getShardGroup(groupId);

        if (shardGroup == null) {
            return;
        }

        var newGroup = shardGroup.toBuilder().clearShards().addAllShards(shards).build();
        if (shards.size() == 0) {
            var partitions = partitionService.getPartitionById(groupId);
            for (var partition : partitions) {
                partitionService.removePartition(partition.getGraphName(), groupId);
            }
            deleteShardGroup(groupId);
        }

        onShardGroupOp(newGroup);
    }

    /**
     * Delete shard group
     *
     * @param groupId shard group id
     */
    public synchronized void deleteShardGroup(int groupId) throws PDException {
        Metapb.ShardGroup group = this.storeInfoMeta.getShardGroup(groupId);
        if (group != null) {
            storeInfoMeta.deleteShardGroup(groupId);
        }

        onShardGroupStatusChanged(group, null);

        // Fix the number of partitions for the store. (Result from partition merge)
        var shardGroups = getShardGroups();
        if (shardGroups != null) {
            var count1 = pdConfig.getConfigService().getPDConfig().getPartitionCount();
            var maxGroupId =
                    getShardGroups().stream().map(Metapb.ShardGroup::getId).max(Integer::compareTo);
            if (maxGroupId.get() < count1) {
                pdConfig.getConfigService().setPartitionCount(maxGroupId.get() + 1);
            }
        }
    }

    public synchronized void updateShardGroupState(int groupId, Metapb.PartitionState state) throws
                                                                                             PDException {
        Metapb.ShardGroup shardGroup = storeInfoMeta.getShardGroup(groupId);

        if (state != shardGroup.getState()) {
            var newShardGroup = shardGroup.toBuilder().setState(state).build();
            storeInfoMeta.updateShardGroup(newShardGroup);
            partitionService.updateShardGroupCache(newShardGroup);

            log.debug("update shard group {} state: {}", groupId, state);

            // Check the status of the cluster
            // todo : A clearer definition of cluster status
            Metapb.PartitionState clusterState = state;
            for (Metapb.ShardGroup group : getShardGroups()) {
                if (group.getState().getNumber() > state.getNumber()) {
                    clusterState = group.getState();
                }
            }
            updateClusterStatus(clusterState);
        }
    }

    /**
     * Receive the heartbeat of the Store
     *
     * @param storeStats
     * @throws PDException
     */
    public Metapb.ClusterStats heartBeat(Metapb.StoreStats storeStats) throws PDException {
        this.storeInfoMeta.updateStoreStats(storeStats);
        Metapb.Store lastStore = this.getStore(storeStats.getStoreId());
        if (lastStore == null) {
            // store does not exist
            throw new PDException(Pdpb.ErrorType.STORE_ID_NOT_EXIST_VALUE,
                                  String.format("Store id %d does not exist.",
                                                storeStats.getStoreId()));
        }
        if (lastStore.getState() == Metapb.StoreState.Tombstone) {
            throw new PDException(Pdpb.ErrorType.STORE_HAS_BEEN_REMOVED_VALUE,
                                  String.format(
                                          "Store id %d is useless since it's state is Tombstone",
                                          storeStats.getStoreId()));
        }
        Metapb.Store nowStore;
        // If you are going to take the store offline
        if (lastStore.getState() == Metapb.StoreState.Exiting) {
            List<Metapb.Store> activeStores = this.getActiveStores();
            Map<Long, Metapb.Store> storeMap = new HashMap<>();
            activeStores.forEach(store -> {
                storeMap.put(store.getId(), store);
            });
            // If the partition of the offline store is 0, it means that the migration has been
            // completed and can be taken offline, if it is not 0, the migration is still in
            // progress and you need to wait
            if (storeStats.getPartitionCount() > 0 &&
                storeMap.containsKey(storeStats.getStoreId())) {
                nowStore = Metapb.Store.newBuilder(lastStore)
                                       .setStats(storeStats)
                                       .setLastHeartbeat(System.currentTimeMillis())
                                       .setState(Metapb.StoreState.Exiting).build();
                this.storeInfoMeta.updateStore(nowStore);
                return this.clusterStats;
            } else {
                nowStore = Metapb.Store.newBuilder(lastStore)
                                       .setStats(storeStats)
                                       .setLastHeartbeat(System.currentTimeMillis())
                                       .setState(Metapb.StoreState.Tombstone).build();
                this.storeInfoMeta.updateStore(nowStore);
                storeInfoMeta.removeActiveStore(nowStore);
                return this.clusterStats;
            }
        }

        if (lastStore.getState() == Metapb.StoreState.Pending) {
            nowStore = Metapb.Store.newBuilder(lastStore)
                                   .setStats(storeStats)
                                   .setLastHeartbeat(System.currentTimeMillis())
                                   .setState(Metapb.StoreState.Pending).build();
            this.storeInfoMeta.updateStore(nowStore);
            return this.clusterStats;
        } else {
            if (lastStore.getState() == Metapb.StoreState.Offline) {
                this.updateStore(
                        Metapb.Store.newBuilder(lastStore).setState(Metapb.StoreState.Up).build());
            }
            nowStore = Metapb.Store.newBuilder(lastStore)
                                   .setState(Metapb.StoreState.Up)
                                   .setStats(storeStats)
                                   .setLastHeartbeat(System.currentTimeMillis()).build();
            this.storeInfoMeta.updateStore(nowStore);
            this.storeInfoMeta.keepStoreAlive(nowStore);
            this.checkStoreStatus();
            return this.clusterStats;
        }
    }

    public synchronized Metapb.ClusterStats updateClusterStatus(Metapb.ClusterState state) {
        if (this.clusterStats.getState() != state) {
            log.info("update cluster state: {}", state);
            this.clusterStats = clusterStats.toBuilder().setState(state).build();
        }
        return this.clusterStats;
    }

    public Metapb.ClusterStats updateClusterStatus(Metapb.PartitionState state) {
        Metapb.ClusterState cstate = Metapb.ClusterState.Cluster_OK;
        switch (state) {
            case PState_Normal:
                cstate = Metapb.ClusterState.Cluster_OK;
                break;
            case PState_Warn:
                cstate = Metapb.ClusterState.Cluster_Warn;
                break;
            case PState_Fault:
                cstate = Metapb.ClusterState.Cluster_Fault;
                break;
            case PState_Offline:
                cstate = Metapb.ClusterState.Cluster_Offline;
                break;
        }
        return updateClusterStatus(cstate);
    }

    public Metapb.ClusterStats getClusterStats() {
        return this.clusterStats;
    }

    /**
     * Check the cluster health status
     * Whether the number of active machines is greater than the minimum threshold
     * The number of partition shards online has exceeded half
     */
    public synchronized void checkStoreStatus() {
        Metapb.ClusterStats.Builder builder = Metapb.ClusterStats.newBuilder()
                                                                 .setState(
                                                                         Metapb.ClusterState.Cluster_OK);
        try {
            List<Metapb.Store> activeStores = this.getActiveStores();
            if (activeStores.size() < pdConfig.getMinStoreCount()) {
                builder.setState(Metapb.ClusterState.Cluster_Not_Ready);
                builder.setMessage("The number of active stores is " + activeStores.size()
                                   + ", less than pd.initial-store-count:" +
                                   pdConfig.getMinStoreCount());
            }
            Map<Long, Metapb.Store> storeMap = new HashMap<>();
            activeStores.forEach(store -> {
                storeMap.put(store.getId(), store);
            });

            if (builder.getState() == Metapb.ClusterState.Cluster_OK) {
                // Check whether the number of online shards for each partition is greater than half
                for (Metapb.ShardGroup group : this.getShardGroups()) {
                    int count = 0;
                    for (Metapb.Shard shard : group.getShardsList()) {
                        count += storeMap.containsKey(shard.getStoreId()) ? 1 : 0;
                    }
                    if (count * 2 < group.getShardsList().size()) {
                        builder.setState(Metapb.ClusterState.Cluster_Not_Ready);
                        builder.setMessage(
                                "Less than half of active shard, partitionId is " + group.getId());
                        break;
                    }
                }
            }

        } catch (PDException e) {
            log.error("StoreNodeService updateClusterStatus exception {}", e);
        }
        this.clusterStats = builder.setTimestamp(System.currentTimeMillis()).build();
        if (this.clusterStats.getState() != Metapb.ClusterState.Cluster_OK) {
            log.error("The cluster is not ready, {}", this.clusterStats);
        }
    }

    public void addStatusListener(StoreStatusListener listener) {
        statusListeners.add(listener);
    }

    protected void onStoreRaftAddressChanged(Metapb.Store store) {
        log.info("onStoreRaftAddressChanged storeId = {}, new raft addr:", store.getId(),
                 store.getRaftAddress());
        statusListeners.forEach(e -> {
            e.onStoreRaftChanged(store);
        });
    }

    public void addShardGroupStatusListener(ShardGroupStatusListener listener) {
        shardGroupStatusListeners.add(listener);
    }

    protected void onStoreStatusChanged(Metapb.Store store, Metapb.StoreState old,
                                        Metapb.StoreState stats) {
        log.info("onStoreStatusChanged storeId = {} from {} to {}", store.getId(), old, stats);
        statusListeners.forEach(e -> {
            e.onStoreStatusChanged(store, old, stats);
        });
    }

    protected void onShardGroupStatusChanged(Metapb.ShardGroup group, Metapb.ShardGroup newGroup) {
        if (group == null && newGroup == null) {
            return;
        }

        var id = group == null ? newGroup.getId() : group.getId();
        log.info("onShardGroupStatusChanged, groupId: {}, from {} to {}", id, group, newGroup);
        shardGroupStatusListeners.forEach(e -> e.onShardListChanged(group, newGroup));
    }

    protected void onShardGroupOp(Metapb.ShardGroup shardGroup) {
        log.info("onShardGroupOp, group id: {}, shard group:{}", shardGroup.getId(), shardGroup);
        shardGroupStatusListeners.forEach(e -> e.onShardListOp(shardGroup));
    }

    /**
     * Check whether the current store can be discontinued
     * If the number of active machines is less than or equal to the minimum threshold, they
     * cannot be taken offline
     * If the number of shards in the partition is not more than half, it cannot be offline
     */
    public boolean checkStoreCanOffline(Metapb.Store currentStore) {
        try {
            long currentStoreId = currentStore.getId();
            List<Metapb.Store> activeStores = this.getActiveStores();
            Map<Long, Metapb.Store> storeMap = new HashMap<>();
            activeStores.forEach(store -> {
                if (store.getId() != currentStoreId) {
                    storeMap.put(store.getId(), store);
                }
            });

            if (storeMap.size() < pdConfig.getMinStoreCount()) {
                return false;
            }

            // Check whether the number of online shards for each partition is greater than half
            for (Metapb.ShardGroup group : this.getShardGroups()) {
                int count = 0;
                for (Metapb.Shard shard : group.getShardsList()) {
                    long storeId = shard.getStoreId();
                    count += storeMap.containsKey(storeId) ? 1 : 0;
                }
                if (count * 2 < group.getShardsList().size()) {
                    return false;
                }
            }
        } catch (PDException e) {
            log.error("StoreNodeService checkStoreCanOffline exception {}", e);
            return false;
        }

        return true;
    }

    /**
     * Compaction on rocksdb on the store
     *
     * @param groupId
     * @param tableName
     * @return
     */
    public synchronized void shardGroupsDbCompaction(int groupId, String tableName) throws
                                                                                    PDException {

        // Notify all stores to compaction rocksdb
        partitionService.fireDbCompaction(groupId, tableName);
        // TODO How to deal with exceptions?
    }

    public Map getQuota() throws PDException {
        List<Metapb.Graph> graphs = partitionService.getGraphs();
        String delimiter = String.valueOf(MetadataKeyHelper.DELIMITER);
        HashMap<String, Long> storages = new HashMap<>();
        for (Metapb.Graph g : graphs) {
            String graphName = g.getGraphName();
            String[] splits = graphName.split(delimiter);
            if (splits.length < 2) {
                continue;
            }
            String graphSpace = splits[0];
            storages.putIfAbsent(graphSpace, 0L);
            List<Metapb.Store> stores = getStores(graphName);
            long dataSize = 0;
            for (Metapb.Store store : stores) {
                List<Metapb.GraphStats> gss = store.getStats()
                                                   .getGraphStatsList();
                for (Metapb.GraphStats gs : gss) {
                    boolean nameEqual = graphName.equals(gs.getGraphName());
                    boolean roleEqual = Metapb.ShardRole.Leader.equals(
                            gs.getRole());
                    if (nameEqual && roleEqual) {
                        dataSize += gs.getApproximateSize();
                    }
                }
            }
            Long size = storages.get(graphSpace);
            size += dataSize;
            storages.put(graphSpace, size);

        }
        Metapb.GraphSpace.Builder spaceBuilder = Metapb.GraphSpace.newBuilder();
        HashMap<String, Boolean> limits = new HashMap<>();
        for (Map.Entry<String, Long> item : storages.entrySet()) {
            String spaceName = item.getKey();
            String value = kvService.get(graphSpaceConfPrefix + spaceName);
            if (!StringUtils.isEmpty(value)) {
                HashMap config = new Gson().fromJson(value, HashMap.class);
                Long size = item.getValue();
                int limit = ((Double) config.get("storage_limit")).intValue();
                long limitByLong = limit * 1024L * 1024L;
                try {
                    spaceBuilder.setName(spaceName).setStorageLimit(limitByLong).setUsedSize(size);
                    Metapb.GraphSpace graphSpace = spaceBuilder.build();
                    configService.setGraphSpace(graphSpace);
                } catch (Exception e) {
                    log.error("update graph space with error:", e);
                }
                // KB and GB * 1024L * 1024L
                if (size > limitByLong) {
                    limits.put(spaceName, true);
                    continue;
                }
            }
            limits.put(spaceName, false);

        }
        GraphState.Builder stateBuilder = GraphState.newBuilder()
                                                    .setMode(GraphMode.ReadOnly)
                                                    .setReason(
                                                            GraphModeReason.Quota);
        for (Metapb.Graph g : graphs) {
            String graphName = g.getGraphName();
            String[] splits = graphName.split(delimiter);
            if (splits.length < 2) {
                continue;
            }
            String graphSpace = splits[0];
            Metapb.GraphState gsOld = g.getGraphState();
            GraphMode gmOld = gsOld != null ? gsOld.getMode() : GraphMode.ReadWrite;
            GraphMode gmNew = limits.get(
                    graphSpace) ? GraphMode.ReadOnly : GraphMode.ReadWrite;
            if (gmOld == null || gmOld.getNumber() != gmNew.getNumber()) {
                stateBuilder.setMode(gmNew);
                if (gmNew.getNumber() == GraphMode.ReadOnly.getNumber()) {
                    stateBuilder.setReason(GraphModeReason.Quota);
                }
                GraphState gsNew = stateBuilder.build();
                Metapb.Graph newGraph = g.toBuilder().setGraphState(gsNew)
                                         .build();
                partitionService.updateGraph(newGraph);
                statusListeners.forEach(listener -> {
                    listener.onGraphChange(newGraph, gsOld, gsNew);
                });
            }
        }

        return limits;
    }

    public Runnable getQuotaChecker() {
        return quotaChecker;
    }

    public TaskInfoMeta getTaskInfoMeta() {
        return taskInfoMeta;
    }

    public StoreInfoMeta getStoreInfoMeta() {
        return storeInfoMeta;
    }

    /**
     * Get the leader of the partition
     *
     * @param partition
     * @param initIdx
     * @return
     */
    public Metapb.Shard getLeader(Metapb.Partition partition, int initIdx) {
        Metapb.Shard leader = null;
        try {
            var shardGroup = this.getShardGroup(partition.getId());
            for (Metapb.Shard shard : shardGroup.getShardsList()) {
                if (shard.getRole() == Metapb.ShardRole.Leader) {
                    leader = shard;
                }
            }
        } catch (Exception e) {
            log.error("get leader error: group id:{}, error: {}",
                      partition.getId(), e.getMessage());
        }
        return leader;
    }

    public CacheResponse getCache() throws PDException {

        List<Metapb.Store> stores = getStores();
        List<Metapb.ShardGroup> groups = getShardGroups();
        List<Metapb.Graph> graphs = partitionService.getGraphs();
        CacheResponse cache = CacheResponse.newBuilder().addAllGraphs(graphs)
                                           .addAllShards(groups)
                                           .addAllStores(stores)
                                           .build();
        return cache;
    }
}
