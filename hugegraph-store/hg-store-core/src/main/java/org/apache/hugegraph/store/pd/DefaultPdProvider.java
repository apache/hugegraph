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
import java.util.List;
import java.util.function.Consumer;

import org.apache.hugegraph.pd.client.PDClient;
import org.apache.hugegraph.pd.client.PDConfig;
import org.apache.hugegraph.pd.client.PDPulse;
import org.apache.hugegraph.pd.client.listener.PDEventListener;
import org.apache.hugegraph.pd.common.KVPair;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.grpc.MetaTask;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.pd.grpc.Metapb.PartitionStats;
import org.apache.hugegraph.pd.grpc.pulse.PartitionHeartbeatRequest;
import org.apache.hugegraph.pd.grpc.pulse.PartitionHeartbeatResponse;
import org.apache.hugegraph.pd.grpc.pulse.PdInstructionType;
import org.apache.hugegraph.pd.grpc.pulse.PulseResponse;
import org.apache.hugegraph.pd.grpc.watch.WatchChangeType;
import org.apache.hugegraph.pd.grpc.watch.WatchGraphResponse;
import org.apache.hugegraph.pd.grpc.watch.WatchResponse;
import org.apache.hugegraph.pd.pulse.PulseServerNotice;
import org.apache.hugegraph.pd.watch.NodeEvent;
import org.apache.hugegraph.pd.watch.PartitionEvent;
import org.apache.hugegraph.store.HgStoreEngine;
import org.apache.hugegraph.store.meta.Graph;
import org.apache.hugegraph.store.meta.GraphManager;
import org.apache.hugegraph.store.meta.Partition;
import org.apache.hugegraph.store.meta.ShardGroup;
import org.apache.hugegraph.store.meta.Store;
import org.apache.hugegraph.store.metric.HgMetricService;
import org.apache.hugegraph.store.processor.Processors;
import org.apache.hugegraph.store.util.Asserts;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DefaultPdProvider implements PdProvider {

    private static final Logger LOG = Log.logger(DefaultPdProvider.class);
    private final PDClient pdClient;
    private final String pdServerAddress;
    private final PDPulse pulseClient;
    private Consumer<Throwable> hbOnError = null;
    private PDPulse.Notifier<PartitionHeartbeatRequest.Builder> pdPulse;
    private Processors processors;
    private GraphManager graphManager = null;

    public static String name = "store";
    public static String authority = "default";

    PDEventListener listener = new PDEventListener() {
        // Listening to pd change information listener
        @Override
        public void onStoreChanged(NodeEvent event) {
            if (event.getEventType() == NodeEvent.EventType.NODE_RAFT_CHANGE) {
                log.info("store raft group changed!, {}", event);
                pdClient.invalidStoreCache(event.getNodeId());
                HgStoreEngine.getInstance().rebuildRaftGroup(event.getNodeId());
            } else if (event.getEventType() == NodeEvent.EventType.NODE_PD_LEADER_CHANGE) {
                log.info("pd leader changed!, {}. restart heart beat", event);
//                if (pulseClient.resetStub(event.getGraph(), pdPulse)) {
//                    startHeartbeatStream(hbOnError);
//                }
            }
        }

        @Override
        public void onPartitionChanged(PartitionEvent event) {

        }

        @Override
        public void onGraphChanged(WatchResponse event) {
            WatchGraphResponse graphResponse = event.getGraphResponse();
            Metapb.Graph graph = graphResponse.getGraph();
            if (graphManager != null) {
                graphManager.updateGraph(new Graph(graph));
            }

        }

        @Override
        public void onShardGroupChanged(WatchResponse event) {
            var response = event.getShardGroupResponse();
            if (response.getType() == WatchChangeType.WATCH_CHANGE_TYPE_SPECIAL1) {
                HgStoreEngine.getInstance().handleShardGroupOp(response.getShardGroupId(),
                                                               response.getShardGroup()
                                                                       .getShardsList());
            } else if (response.getType() == WatchChangeType.WATCH_CHANGE_TYPE_ADD) {
                var shardGroup = response.getShardGroup();
                HgStoreEngine.getInstance().createPartitionEngine(shardGroup.getId(),
                                                                  ShardGroup.from(shardGroup),
                                                                  null);
            }
        }
    };

    public DefaultPdProvider(String pdAddress) {
        PDConfig config = PDConfig.of(pdAddress).setEnableCache(true);
        config.setAuthority(name, authority);
        this.pdClient = PDClient.create(config);
        this.pdClient.addEventListener(listener);
        this.pdServerAddress = pdAddress;
        log.info("pulse client connect to {}", pdClient.getLeaderIp());
        this.pulseClient = this.pdClient.getPulse();
    }

    @Override
    public long registerStore(Store store) throws PDException {
        Asserts.isTrue(this.pdClient != null, "pd client is null");
        LOG.info("registerStore pd={} storeId={}, store={}", this.pdServerAddress, store.getId(),
                 store);

        long storeId = 0;
        Metapb.Store protoObj = store.getProtoObj();
        try {
            storeId = pdClient.registerStore(protoObj);
            store.setId(storeId);
            if (pdClient.getStore(storeId).getState() != Metapb.StoreState.Up) {
                LOG.warn("Store {} is not activated, state is {}", storeId,
                         pdClient.getStore(storeId).getState());
            }
        } catch (PDException e) {
            LOG.error(
                    "Exception in storage registration, StoreID= {} pd= {} exceptCode= {} except=" +
                    " {}.",
                    protoObj.getId(), this.pdServerAddress, e.getErrorCode(), e.getMessage());
            storeId = 0;
            throw e;
        } catch (Exception e) {
            LOG.error(
                    "Exception in storage registration, StoreID= {} pd= {} except= {}, Please " +
                    "check your network settings.",
                    protoObj.getId(), this.pdServerAddress, e.getMessage());
            handleCommonException(e);
            storeId = 0;
        }
        return storeId;
    }

    @Override
    public Partition getPartitionByID(String graph, int partId) {
        try {
            KVPair<Metapb.Partition, Metapb.Shard> pair = pdClient.getPartitionById(
                    graph, partId);
            if (null != pair) {
                return new Partition(pair.getKey());
            }
        } catch (PDException e) {
            log.error("Partition {}-{} getPartitionByID exception {}", graph, partId, e);
        }
        return null;
    }

    @Override
    public Metapb.Shard getPartitionLeader(String graph, int partId) {
        try {
            KVPair<Metapb.Partition, Metapb.Shard> pair = pdClient.getPartitionById(
                    graph, partId);
            if (null != pair) {
                return pair.getValue();
            }
        } catch (PDException e) {
            log.error("Partition {}-{} getPartitionByID exception {}", graph, partId, e);
        }
        return null;
    }

    @Override
    public Metapb.Partition getPartitionByCode(String graph, int code) {
        try {
            KVPair<Metapb.Partition, Metapb.Shard> pair = pdClient.getPartitionByCode(
                    graph, code);
            if (null != pair) {
                return pair.getKey();
            }
        } catch (PDException e) {
            log.error("Partition {} getPartitionByCode {} exception {}", graph, code, e);
        }
        return null;
    }

    @Override
    public Partition delPartition(String graph, int partId) {
        log.info("Partition {}-{} send delPartition to PD", graph, partId);
        try {
            Metapb.Partition partition = pdClient.delPartition(graph, partId);
            if (null != partition) {
                return new Partition(partition);
            }
        } catch (PDException e) {
            log.error("Partition {}-{} remove exception {}", graph, partId, e);
        }
        return null;
    }

    @Override
    public List<Metapb.Partition> updatePartition(List<Metapb.Partition> partitions) throws
                                                                                     PDException {

        try {
            List<Metapb.Partition> results = pdClient.updatePartition(partitions);
            return results;
        } catch (PDException e) {
            throw e;
        }
    }

    @Override
    public List<Partition> getPartitionsByStore(long storeId) throws PDException {
        List<Partition> partitions = new ArrayList<>();
        List<Metapb.Partition> parts = pdClient.getPartitionsByStore(storeId);
        parts.forEach(e -> {
            partitions.add(new Partition(e));
        });
        return partitions;
    }

    @Override
    public void updatePartitionCache(Partition partition, Boolean changeLeader) {
        Metapb.Shard leader = null;

        var shardGroup = getShardGroup(partition.getId());
        if (shardGroup != null) {
            for (Metapb.Shard shard : shardGroup.getShardsList()) {
                if (shard.getRole() == Metapb.ShardRole.Leader) {
                    leader = shard;
                }
            }
        }
        if (!changeLeader) {
            try {
                leader = pdClient.getPartitionById(partition.getGraphName(), partition.getId())
                                 .getValue();
            } catch (PDException e) {
                log.error("find leader error,leader changed to storeId:{}", leader.getStoreId());
            } catch (Exception e1) {
                log.error("exception ", e1);
            }
        }
        pdClient.updatePartitionCache(partition.getProtoObj(), leader);
    }

    @Override
    public void invalidPartitionCache(String graph, int partId) {
        pdClient.invalidPartitionCache(graph, partId);
    }

    /**
     * Start partition heartbeat streaming transmission
     *
     * @return
     */
    @Override
    public boolean startHeartbeatStream(Consumer<Throwable> onError) {
        this.hbOnError = onError;
        pdPulse = pulseClient.connectPartition(new PDPulse.Listener<>() {

            @Override
            public void onNotice(PulseServerNotice<PulseResponse> response) {
                PulseResponse content = response.getContent();

                // Message consumption acknowledgment, if the message can be consumed correctly,
                // call accept to return the status code, otherwise do not call accept.
                Consumer<Integer> consumer = integer -> {
                    LOG.debug("Partition heartbeat accept instruction: {}", content);
                    // LOG.info("accept notice id : {}, ts:{}", response.getNoticeId(), System
                    // .currentTimeMillis());
                    // http2 concurrency issue, need to lock
                    // synchronized (pdPulse) {
                    response.ack();
                    // }
                };

                if (content.hasInstructionResponse()) {
                    var pdInstruction = content.getInstructionResponse();
                    consumer.accept(0);
                    // Current link becomes follower, reconnect
                    if (pdInstruction.getInstructionType() ==
                        PdInstructionType.CHANGE_TO_FOLLOWER) {
                        onCompleted();
                        log.info("got pulse instruction, change leader to {}",
                                 pdInstruction.getLeaderIp());
                        if (pulseClient.resetStub(pdInstruction.getLeaderIp(), pdPulse)) {
                            startHeartbeatStream(hbOnError);
                        }
                    }
                    return;
                }

                PartitionHeartbeatResponse instruct = content.getPartitionHeartbeatResponse();
                processors.process(instruct, consumer);

            }

            @Override
            public void onError(Throwable throwable) {
                LOG.error("Partition heartbeat stream error.", throwable);
            }

            @Override
            public void onCompleted() {
                LOG.info("Partition heartbeat stream complete");
                if (pulseClient.resetStub(pdClient.getLeaderIp(), pdPulse)) {
                    startHeartbeatStream(hbOnError);
                }
            }
        });
        return true;
    }

    @Override
    public boolean setCommandProcessors(Processors processors) {
        this.processors = processors;
        return true;
    }

    @Override
    public boolean partitionHeartbeat(List<Metapb.PartitionStats> statsList) {
        for (Metapb.PartitionStats stats : statsList) {
            PartitionHeartbeatRequest.Builder request = PartitionHeartbeatRequest.newBuilder()
                                                                                 .setStates(stats);
            pdPulse.notifyServer(request);
        }
        return false;
    }

    @Override
    public boolean partitionHeartbeat(PartitionStats stats) {
        PartitionHeartbeatRequest.Builder request = PartitionHeartbeatRequest.newBuilder()
                                                                             .setStates(stats);
        synchronized (pdPulse) {
            pdPulse.notifyServer(request);
        }
        return false;
    }

    @Override
    public boolean isLocalPartition(long storeId, int partitionId) {
        try {
            return !pdClient.queryPartitions(storeId, partitionId).isEmpty();
        } catch (PDException e) {
            log.error("isLocalPartition exception ", e);
        }
        return false;
    }

    @Override
    public Metapb.Graph getGraph(String graphName) throws PDException {
        return pdClient.getGraph(graphName);
    }

    @Override
    public void reportTask(MetaTask.Task task) throws PDException {
        pdClient.reportTask(task);
    }

    @Override
    public PDClient getPDClient() {
        return this.pdClient;
    }

    @Override
    public boolean updatePartitionLeader(String graphName, int partId, long leaderStoreId) {
        this.pdClient.updatePartitionLeader(graphName, partId, leaderStoreId);
        return true;
    }

    @Override
    public Store getStoreByID(Long storeId) {
        try {
            return new Store(pdClient.getStore(storeId));
        } catch (PDException e) {
            log.error("getStoreByID exception {}", e);
        }
        return null;
    }

    @Override
    public Metapb.ClusterStats getClusterStats() {
        try {
            return pdClient.getClusterStats();
        } catch (PDException e) {
            log.error("getClusterStats exception {}", e);
            return Metapb.ClusterStats.newBuilder()
                                      .setState(Metapb.ClusterState.Cluster_Fault).build();
        }
    }

    @Override
    public Metapb.ClusterStats storeHeartbeat(Store node) throws PDException {
        LOG.debug("storeHeartbeat node id: {}", node.getId());

        try {
            Metapb.StoreStats.Builder stats = HgMetricService.getInstance().getMetrics();
            LOG.debug("storeHeartbeat StoreStats: {}", stats);
            stats.setCores(node.getCores());
            var executor = HgStoreEngine.getUninterruptibleJobs();
            stats.setExecutingTask(
                    executor.getActiveCount() != 0 || !executor.getQueue().isEmpty());
            return pdClient.storeHeartbeat(stats.build());

        } catch (PDException e) {
            throw e;
        } catch (Exception e) {
            LOG.warn("Store {} report heartbeat exception: {}", node.getId(), e.toString());
        }

        return Metapb.ClusterStats.newBuilder()
                                  .setState(Metapb.ClusterState.Cluster_Fault).build();
    }

    private void handleCommonException(Exception e) {
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
    public void deleteShardGroup(int groupId) throws PDException {
        pdClient.deleteShardGroup(groupId);
    }

    @Override
    public Metapb.ShardGroup getShardGroup(int partitionId) {
        try {
            return pdClient.getShardGroup(partitionId);
        } catch (PDException e) {
            log.error("get shard group :{} from pd failed: {}", partitionId, e.getMessage());
        }
        return null;
    }

    @Override
    public Metapb.ShardGroup getShardGroupDirect(int partitionId) {
        try {
            return pdClient.getShardGroupDirect(partitionId);
        } catch (PDException e) {
            log.error("get shard group :{} from pd failed: {}", partitionId, e.getMessage());
        }
        return null;
    }

    @Override
    public void updateShardGroup(Metapb.ShardGroup shardGroup) throws PDException {
        pdClient.updateShardGroup(shardGroup);
    }

    @Override
    public String getPdServerAddress() {
        return pdServerAddress;
    }

    @Override
    public void resetPulseClient() {
        pulseClient.resetStub(pdClient.getLeaderIp(), pdPulse);
    }
}
