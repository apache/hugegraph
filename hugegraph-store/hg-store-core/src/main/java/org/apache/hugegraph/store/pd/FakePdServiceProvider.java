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

package org.apache.hugegraph.store.pd;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.apache.hugegraph.pd.client.PDClient;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.common.PartitionUtils;
import org.apache.hugegraph.pd.grpc.MetaTask;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.store.meta.GraphManager;
import org.apache.hugegraph.store.meta.Partition;
import org.apache.hugegraph.store.meta.Store;
import org.apache.hugegraph.store.options.HgStoreEngineOptions;
import org.apache.hugegraph.store.processor.Processors;

import lombok.extern.slf4j.Slf4j;

/**
 * Built-in PD service, for standalone deployment or development debugging.
 */
@Slf4j
public class FakePdServiceProvider implements PdProvider {

    private static long specifyStoreId = -1L;
    private final Map<Long, Store> stores;
    private int partitionCount = 0;
    private GraphManager graphManager = null;
    private List<Partition> partitions;
    /**
     * Store for register storage
     */
    private Store registerStore;

    public FakePdServiceProvider(HgStoreEngineOptions.FakePdOptions options) {
        stores = new LinkedHashMap<>();
        if (options != null) {
            String[] storeList = options.getStoreList().split(",");
            String[] peersList = options.getPeersList().split(",");
            for (int i = 0; i < storeList.length; i++) {
                if (!storeList[i].isEmpty()) {
                    addStore(storeList[i], peersList[i]);
                }
            }
        }
        this.partitionCount = options.getPartitionCount();
    }

    public static long makeStoreId(String storeAddress) {
        return specifyStoreId != -1L ? specifyStoreId : storeAddress.hashCode();
    }

    public static void setSpecifyStoreId(long specifyStoreId) {
        FakePdServiceProvider.specifyStoreId = specifyStoreId;
    }

    private void addStore(String storeAddr, String raftAddr) {
        Store store = new Store() {{
            setId(makeStoreId(storeAddr));
            setRaftAddress(raftAddr);
            setStoreAddress(storeAddr);
            setDeployPath("");
            setDataPath("");
        }};
        stores.put(store.getId(), store);
    }

    @Override
    public long registerStore(Store store) throws PDException {
        log.info("registerStore storeId:{}, storeAddress:{}", store.getId(),
                 store.getStoreAddress());

        var storeId = makeStoreId(store.getStoreAddress());
        if (store.getId() == 0) {
            store.setId(storeId);
        }

        if (!stores.containsKey(store.getId())) {
            stores.put(store.getId(), store);
        }

        registerStore = store;

        return store.getId();
    }

    @Override
    public Metapb.ShardGroup getShardGroup(int partitionId) {
        Long storeId;
        if (registerStore != null) {
            storeId = registerStore.getId();
        } else {
            storeId = (Long) stores.keySet().toArray()[0];
        }

        return Metapb.ShardGroup.newBuilder()
                                .setId(partitionId)
                                .setConfVer(0)
                                .setVersion(0)
                                .addAllShards(List.of(Metapb.Shard.newBuilder()
                                                                  .setRole(Metapb.ShardRole.Leader)
                                                                  .setStoreId(storeId).build()))
                                .setState(Metapb.PartitionState.PState_Normal)
                                .build();
    }

    @Override
    public Metapb.ShardGroup getShardGroupDirect(int partitionId) {
        return getShardGroup(partitionId);
    }

    @Override
    public void updateShardGroup(Metapb.ShardGroup shardGroup) throws PDException {
        PdProvider.super.updateShardGroup(shardGroup);
    }

    /**
     * Retrieve partition information for the specified chart and obtain partition object by
     * partition ID
     *
     * @param graph  Graph name
     * @param partId Partition ID
     * @return partition object
     */
    @Override
    public Partition getPartitionByID(String graph, int partId) {
        int partLength = getPartitionLength();
        Metapb.Partition partition = Metapb.Partition.newBuilder()
                                                     .setGraphName(graph)
                                                     .setId(partId)
                                                     .setStartKey((long) partLength * partId)
                                                     .setEndKey((long) partLength * (partId + 1))
                                                     .setState(Metapb.PartitionState.PState_Normal)
                                                     .build();
        return new Partition(partition);
    }

    @Override
    public Metapb.Shard getPartitionLeader(String graph, int partId) {
        return getShardGroup(partId).getShardsList().get(0);
    }

    private int getPartitionLength() {
        return PartitionUtils.MAX_VALUE / (partitionCount == 0 ? stores.size() : partitionCount) +
               1;
    }

    @Override
    public Metapb.Partition getPartitionByCode(String graph, int code) {
        int partId = code / getPartitionLength();
        return getPartitionByID(graph, partId).getProtoObj();
    }

    @Override
    public Partition delPartition(String graph, int partId) {
        return null;
    }

    @Override
    public List<Metapb.Partition> updatePartition(List<Metapb.Partition> partitions) {
        return partitions;
    }

    @Override
    public List<Partition> getPartitionsByStore(long storeId) throws PDException {
        return new ArrayList<>();
    }

    @Override
    public void updatePartitionCache(Partition partition, Boolean changeLeader) {

    }

    @Override
    public void invalidPartitionCache(String graph, int partId) {

    }

    @Override
    public boolean startHeartbeatStream(Consumer<Throwable> onError) {
        return false;
    }

    @Override
    public boolean setCommandProcessors(Processors processors) {
        return true;
    }

    //@Override
    //public boolean addPartitionInstructionListener(PartitionInstructionListener listener) {
    //    return false;
    //}

    @Override
    public boolean partitionHeartbeat(List<Metapb.PartitionStats> statsList) {
        return true;
    }

    @Override
    public boolean partitionHeartbeat(Metapb.PartitionStats stats) {
        return false;
    }

    @Override
    public boolean isLocalPartition(long storeId, int partitionId) {
        return true;
    }

    @Override
    public Metapb.Graph getGraph(String graphName) {
        return Metapb.Graph.newBuilder().setGraphName(graphName)
                           .setPartitionCount(partitionCount)
                           .setState(Metapb.PartitionState.PState_Normal)
                           .build();
    }

    @Override
    public void reportTask(MetaTask.Task task) throws PDException {

    }

    @Override
    public PDClient getPDClient() {
        return null;
    }

    @Override
    public Store getStoreByID(Long storeId) {
        return stores.get(storeId);
    }

    @Override
    public Metapb.ClusterStats getClusterStats() {
        return Metapb.ClusterStats.newBuilder()
                                  .setState(Metapb.ClusterState.Cluster_OK).build();
    }

    @Override
    public Metapb.ClusterStats storeHeartbeat(Store node) {

        return getClusterStats();
    }

    @Override
    public boolean updatePartitionLeader(String graphName, int partId, long leaderStoreId) {
        return false;

    }

    @Override
    public GraphManager getGraphManager() {
        return graphManager;
    }

    @Override
    public void setGraphManager(GraphManager graphManager) {
        this.graphManager = graphManager;
    }

    @Override
    public void deleteShardGroup(int groupId) {

    }

    public List<Store> getStores() {
        return List.copyOf(stores.values());
    }

    public void setPartitionCount(int partitionCount) {
        this.partitionCount = partitionCount;
    }

    @Override
    public String getPdServerAddress() {
        return null;
    }

    @Override
    public void resetPulseClient() {
        PdProvider.super.resetPulseClient();
    }
}
