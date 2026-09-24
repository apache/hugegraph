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

package org.apache.hugegraph.store;

import static org.apache.hugegraph.pd.grpc.Pdpb.ErrorType.PD_UNREACHABLE_VALUE;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.common.PDRuntimeException;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.pd.grpc.Pdpb.ErrorType;
import org.apache.hugegraph.store.consts.PoolNames;
import org.apache.hugegraph.store.listener.PartitionStateListener;
import org.apache.hugegraph.store.listener.StoreStateListener;
import org.apache.hugegraph.store.meta.Partition;
import org.apache.hugegraph.store.meta.PartitionRole;
import org.apache.hugegraph.store.meta.Store;
import org.apache.hugegraph.store.meta.StoreMetadata;
import org.apache.hugegraph.store.options.HgStoreEngineOptions;
import org.apache.hugegraph.store.pd.PdProvider;
import org.apache.hugegraph.store.util.IpUtil;
import org.apache.hugegraph.store.util.Lifecycle;

import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.util.Utils;

import lombok.extern.slf4j.Slf4j;

/**
 * Register and heartbeat, Keep the system online
 */
@Slf4j
public class HeartbeatService implements Lifecycle<HgStoreEngineOptions>, PartitionStateListener {

    private static final int MAX_HEARTBEAT_RETRY_COUNT = 5;
    private static final int REGISTER_RETRY_INTERVAL = 1;
    private static final int processors = Runtime.getRuntime().availableProcessors();
    private final HgStoreEngine storeEngine;
    private HgStoreEngineOptions options;
    private PdProvider pdProvider;
    private Store storeInfo;
    private Metapb.ClusterStats clusterStats;
    private StoreMetadata storeMetadata;
    private final List<StoreStateListener> stateListeners;
    private final Object partitionThreadLock = new Object();
    private final Object storeThreadLock = new Object();
    private int heartbeatFailCount = 0;
    private int reportErrCount = 0;
    // Thread sleep time
    private volatile int timerNextDelay = 1000;
    private volatile boolean terminated = false;

    public HeartbeatService(HgStoreEngine storeEngine) {
        this.storeEngine = storeEngine;
        stateListeners = Collections.synchronizedList(new ArrayList());
    }

    @Override
    public boolean init(HgStoreEngineOptions opts) {
        this.options = opts;
        storeInfo = storeMetadata.getStore();
        if (storeInfo == null) {
            storeInfo = new Store();
        }
        storeInfo.setStoreAddress(options.getGrpcAddress());
        storeInfo.setPdAddress(options.getPdAddress());
        storeInfo.setRaftAddress(options.getRaftAddress());
        storeInfo.setState(Metapb.StoreState.Unknown);
        storeInfo.setLabels(options.getLabels());
        storeInfo.setCores(processors);
        storeInfo.setDeployPath(HeartbeatService.class.getResource("/").getPath());
        storeInfo.setDataPath(options.getDataPath());
        this.pdProvider = options.getPdProvider();
        new Thread(() -> doStoreHeartbeat(), PoolNames.HEARTBEAT).start();
        new Thread(() -> doPartitionHeartbeat(), PoolNames.P_HEARTBEAT).start();
        return true;
    }

    public HeartbeatService addStateListener(StoreStateListener stateListener) {
        stateListeners.add(stateListener);
        return this;
    }

    public Store getStoreInfo() {
        return storeInfo;
    }

    public void setStoreMetadata(StoreMetadata storeMetadata) {
        this.storeMetadata = storeMetadata;
    }

    // Whether the cluster is ready
    public boolean isClusterReady() {
        if (clusterStats == null) {
            clusterStats = pdProvider.getClusterStats();
        }
        return clusterStats != null && clusterStats.getState() == Metapb.ClusterState.Cluster_OK;
    }

    /**
     * Service status has four types
     * Ready, Online, Offline, Dead (excluded from the cluster)
     */
    protected void doStoreHeartbeat() {
        while (!terminated) {
            try {
                switch (storeInfo.getState()) {
                    case Unknown:
                    case Offline:
                        registerStore();
                        break;
                    case Up:
                        storeHeartbeat();
                        monitorMemory();
                        break;
                    case Tombstone:
                        break;

                }
                synchronized (storeThreadLock) {
                    storeThreadLock.wait(timerNextDelay);
                }
            } catch (Throwable e) {
                if (e instanceof PDRuntimeException &&
                    ((PDRuntimeException) e).getErrorCode() == PD_UNREACHABLE_VALUE) {
                    log.error("store heartbeat error: PD UNREACHABLE");
                    synchronized (storeThreadLock) {
                        try {
                            if (timerNextDelay < 10000) {
                                storeThreadLock.wait(timerNextDelay);
                            } else {
                                storeThreadLock.wait(timerNextDelay / 2);
                            }
                        } catch (Exception ie) {
                            log.error("Interrupted while waiting in heartbeat error handling", ie);
                        }
                    }
                } else {
                    log.error("heartbeat error: ", e);
                }
            }
        }
    }

    protected void doPartitionHeartbeat() {
        while (!terminated) {
            try {
                partitionHeartbeat();

            } catch (Exception e) {
                log.error("doPartitionHeartbeat error: ", e);
            }
            try {
                synchronized (partitionThreadLock) {
                    partitionThreadLock.wait(options.getPartitionHBInterval() * 1000L);
                }
            } catch (InterruptedException e) {
                log.error("doPartitionHeartbeat error: ", e);
            }
        }
    }

    protected void registerStore() {
        try {
            // Register store, initial registration of PD generates id, automatically assigns
            // value to storeinfo
            this.storeInfo.setStoreAddress(IpUtil.getNearestAddress(options.getGrpcAddress()));
            this.storeInfo.setRaftAddress(IpUtil.getNearestAddress(options.getRaftAddress()));

            long storeId = pdProvider.registerStore(this.storeInfo);
            if (storeId != 0) {
                storeInfo.setId(storeId);
                storeMetadata.save(storeInfo);
                this.clusterStats = pdProvider.getClusterStats();
                if (clusterStats.getState() == Metapb.ClusterState.Cluster_OK) {
                    timerNextDelay = options.getStoreHBInterval() * 1000;
                } else {
                    timerNextDelay = REGISTER_RETRY_INTERVAL * 1000;
                }
                log.info("Register Store id= {} successfully. store = {}, clusterStats {}",
                         storeInfo.getId(), storeInfo, this.clusterStats);
                // Listen to partition messages
                pdProvider.startHeartbeatStream(error -> {
                    onStateChanged(Metapb.StoreState.Offline);
                    timerNextDelay = REGISTER_RETRY_INTERVAL * 1000;
                    wakeupHeartbeatThread();
                    log.error("Connection closed. The store state changes to {}",
                              Metapb.StoreState.Offline);
                });
                onStateChanged(Metapb.StoreState.Up);
            } else {
                timerNextDelay = REGISTER_RETRY_INTERVAL * 1000 / 2;
            }
        } catch (PDException e) {
            int exceptCode = e.getErrorCode();
            if (exceptCode == ErrorType.STORE_ID_NOT_EXIST_VALUE) {
                log.error("The store ID {} does not match the PD. Check that the correct PD is " +
                          "connected, " + "and then delete the store ID!!!", storeInfo.getId());
                System.exit(-1);
            } else if (exceptCode == ErrorType.STORE_HAS_BEEN_REMOVED_VALUE) {
                log.error("The store ID {} has been removed, please delete all data and restart!",
                          storeInfo.getId());
                System.exit(-1);
            } else if (exceptCode == ErrorType.STORE_PROHIBIT_DUPLICATE_VALUE) {
                log.error("The store ID {} maybe duplicated, please check out store raft address " +
                          "and restart later!", storeInfo.getId());
                System.exit(-1);
            }
        }
    }

    protected void storeHeartbeat() {
        if (log.isDebugEnabled()) {
            log.debug("storeHeartbeat ... ");
        }
        Metapb.ClusterStats clusterStats = null;
        try {
            clusterStats = pdProvider.storeHeartbeat(this.storeInfo);
        } catch (PDException e) {
            int exceptCode = e.getErrorCode();
            if (exceptCode == ErrorType.STORE_ID_NOT_EXIST_VALUE) {
                log.error("The store ID {} does not match the PD. Check that the correct PD is " +
                          "connected, and then delete the store ID!!!", storeInfo.getId());
                System.exit(-1);
            } else if (exceptCode == ErrorType.STORE_HAS_BEEN_REMOVED_VALUE) {
                log.error("The store ID {} has been removed, please delete all data and restart!",
                          storeInfo.getId());
                System.exit(-1);
            }
        }
        if (clusterStats == null || clusterStats.getState() == null) {
            throw new PDRuntimeException(PD_UNREACHABLE_VALUE);
        }
        if (clusterStats.getState().getNumber() >= Metapb.ClusterState.Cluster_Fault.getNumber()) {
            if (reportErrCount == 0) {
                log.info("The cluster is abnormal, {}", clusterStats);
            }
            reportErrCount = (++reportErrCount) % 30;
        }

        if (clusterStats.getState() == Metapb.ClusterState.Cluster_OK) {
            timerNextDelay = options.getStoreHBInterval() * 1000;
        } else {
            timerNextDelay = REGISTER_RETRY_INTERVAL * 1000;
        }

        if (clusterStats.getState() == Metapb.ClusterState.Cluster_Fault) {
            heartbeatFailCount++;
        } else {
            heartbeatFailCount = 0;
            this.clusterStats = clusterStats;
        }
        if (heartbeatFailCount > MAX_HEARTBEAT_RETRY_COUNT) {
            onStateChanged(Metapb.StoreState.Offline);
            timerNextDelay = REGISTER_RETRY_INTERVAL * 1000;
            this.clusterStats = clusterStats;
            log.error("Store heart beat failure. The store state changes to {}",
                      Metapb.StoreState.Offline);
        }
    }

    protected synchronized void onStateChanged(Metapb.StoreState newState) {
        Utils.runInThread(() -> {
            Metapb.StoreState oldState = this.storeInfo.getState();
            this.storeInfo.setState(newState);
            stateListeners.forEach((e) ->
                                           e.stateChanged(this.storeInfo, oldState, newState));
        });
    }

    protected void partitionHeartbeat() {
        if (storeEngine == null) {
            return;
        }

        List<PartitionEngine> partitions = storeEngine.getLeaderPartition();
        final List<Metapb.PartitionStats> statsList = new ArrayList<>(partitions.size());

        Metapb.Shard localLeader = Metapb.Shard.newBuilder()
                                               .setStoreId(storeEngine
                                                                   .getPartitionManager()
                                                                   .getStore().getId())
                                               .setRole(Metapb.ShardRole.Leader)
                                               .build();
        // Get information for each shard.
        for (PartitionEngine partition : partitions) {
            Metapb.PartitionStats.Builder stats = Metapb.PartitionStats.newBuilder();
            stats.setId(partition.getGroupId());
            stats.addAllGraphName(partition.getPartitions().keySet());
            stats.setLeaderTerm(partition.getLeaderTerm());
            stats.setConfVer(partition.getShardGroup().getConfVersion());
            stats.setLeader(localLeader);

            Metapb.PartitionState partitionState = Metapb.PartitionState.PState_Normal;
            for (var entry : storeEngine.getPartitionManager().getPartitions(partition.getGroupId())
                                        .entrySet()) {
                if (entry.getValue().getWorkState() == Metapb.PartitionState.PState_Offline) {
                    partitionState = Metapb.PartitionState.PState_Offline;
                    break;
                }
            }
            // pd will not handle (3.7.2+)
            stats.setState(partitionState);

            stats.addAllShard(partition.getShardGroup().getMetaPbShard());

            // shard status
            List<Metapb.ShardStats> shardStats = new ArrayList<>();
            Map<Long, PeerId> aliveShards = partition.getAlivePeers();
            // Statistics shard status
            partition.getShardGroup().getShards().forEach(shard -> {
                Metapb.ShardState state = Metapb.ShardState.SState_Normal;
                if (!aliveShards.containsKey(shard.getStoreId())) {
                    state = Metapb.ShardState.SState_Offline;
                }

                shardStats.add(Metapb.ShardStats.newBuilder()
                                                .setStoreId(shard.getStoreId())
                                                .setRole(shard.getRole())
                                                .setState(state).build());
            });
            stats.addAllShardStats(shardStats);
            stats.setTimestamp(System.currentTimeMillis());

            statsList.add(stats.build());
        }
        // Send heartbeat
        if (statsList.size() > 0) {
            pdProvider.partitionHeartbeat(statsList);
        }

    }

    public void monitorMemory() {

        // try {
        //    Map<MemoryUsageType, Long> mems =
        //            storeEngine.getBusinessHandler().getApproximateMemoryUsageByType(null);
        //
        //    if (mems.get(MemoryUsageType.kCacheTotal) > RaftRocksdbOptions
        //    .getWriteCacheCapacity() * 0.9 &&
        //            mems.get(MemoryUsageType.kMemTableUnFlushed) > RaftRocksdbOptions
        //            .getWriteCacheCapacity() * 0.1) {
        //       // storeEngine.getBusinessHandler().flushAll();
        //       // log.warn("Less memory, start flush dbs, {}", mems);
        //    }
        // } catch (Exception e) {
        //    log.error("MonitorMemory exception {}", e);
        // }
    }

    @Override
    public void shutdown() {
        log.info("HeartbeatService shutdown");
        terminated = true;
        synchronized (partitionThreadLock) {
            partitionThreadLock.notify();
        }
    }

    @Override
    public void partitionRoleChanged(Partition partition, PartitionRole newRole) {
        if (newRole == PartitionRole.LEADER) {
            // leader changed, activate heartbeat
            synchronized (partitionThreadLock) {
                partitionThreadLock.notifyAll();
            }
        }
    }

    @Override
    public void partitionShardChanged(Partition partition, List<Metapb.Shard> oldShards,
                                      List<Metapb.Shard> newShards) {
        if (partition.isLeader()) {
            synchronized (partitionThreadLock) {
                partitionThreadLock.notifyAll();
            }
        }
    }

    private void wakeupHeartbeatThread() {
        synchronized (storeThreadLock) {
            storeThreadLock.notifyAll();
        }
    }

    /**
     * reconnect pulse
     */
    public void connectNewPulse() {
        pdProvider.getPDClient().forceReconnect();
//        pdProvider.startHeartbeatStream(error->{
//            onStateChanged(Metapb.StoreState.Offline);
//            timerNextDelay = REGISTER_RETRY_INTERVAL * 1000;
//            wakeupHeartbeatThread();
//            log.error("Connection closed. The store state changes to {}", Metapb.StoreState
//            .Offline);
//        });
    }
}
