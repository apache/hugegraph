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

import static org.apache.hugegraph.pd.grpc.MetaTask.TaskType.Clean_Partition;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.collections.ListUtils;
import org.apache.commons.collections.SetUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.grpc.MetaTask;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.pd.raft.RaftReflectionUtil;
import org.apache.hugegraph.store.business.BusinessHandler;
import org.apache.hugegraph.store.business.BusinessHandlerImpl;
import org.apache.hugegraph.store.cmd.HgCmdClient;
import org.apache.hugegraph.store.cmd.request.BatchPutRequest;
import org.apache.hugegraph.store.cmd.request.CleanDataRequest;
import org.apache.hugegraph.store.cmd.request.DbCompactionRequest;
import org.apache.hugegraph.store.cmd.request.UpdatePartitionRequest;
import org.apache.hugegraph.store.listener.PartitionStateListener;
import org.apache.hugegraph.store.meta.Partition;
import org.apache.hugegraph.store.meta.PartitionManager;
import org.apache.hugegraph.store.meta.Shard;
import org.apache.hugegraph.store.meta.ShardGroup;
import org.apache.hugegraph.store.meta.Store;
import org.apache.hugegraph.store.meta.TaskManager;
import org.apache.hugegraph.store.options.HgStoreEngineOptions;
import org.apache.hugegraph.store.options.PartitionEngineOptions;
import org.apache.hugegraph.store.raft.DefaultRaftClosure;
import org.apache.hugegraph.store.raft.PartitionStateMachine;
import org.apache.hugegraph.store.raft.RaftClosure;
import org.apache.hugegraph.store.raft.RaftOperation;
import org.apache.hugegraph.store.raft.RaftStateListener;
import org.apache.hugegraph.store.raft.RaftTaskHandler;
import org.apache.hugegraph.store.raft.util.RaftUtils;
import org.apache.hugegraph.store.snapshot.SnapshotHandler;
import org.apache.hugegraph.store.util.FutureClosure;
import org.apache.hugegraph.store.util.HgRaftError;
import org.apache.hugegraph.store.util.HgStoreException;
import org.apache.hugegraph.store.util.Lifecycle;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.JRaftUtils;
import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.RaftGroupService;
import com.alipay.sofa.jraft.ReplicatorGroup;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.core.DefaultJRaftServiceFactory;
import com.alipay.sofa.jraft.core.NodeMetrics;
import com.alipay.sofa.jraft.core.Replicator;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.entity.Task;
import com.alipay.sofa.jraft.error.RaftException;
import com.alipay.sofa.jraft.option.NodeOptions;
import com.alipay.sofa.jraft.option.RaftOptions;
import com.alipay.sofa.jraft.storage.LogStorage;
import com.alipay.sofa.jraft.storage.impl.RocksDBLogStorage;
import com.alipay.sofa.jraft.storage.log.RocksDBSegmentLogStorage;
import com.alipay.sofa.jraft.util.Endpoint;
import com.alipay.sofa.jraft.util.ThreadId;
import com.alipay.sofa.jraft.util.Utils;
import com.alipay.sofa.jraft.util.internal.ThrowUtil;
import com.google.protobuf.CodedInputStream;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * Partition processing engine
 */
@Slf4j
public class PartitionEngine implements Lifecycle<PartitionEngineOptions>, RaftStateListener {

    private static final ThreadPoolExecutor raftLogWriteExecutor = null;
    public final String raftPrefix = "hg_";

    private final HgStoreEngine storeEngine;
    private final PartitionManager partitionManager;
    private final List<PartitionStateListener> stateListeners;
    private final ShardGroup shardGroup;
    private final AtomicBoolean changingPeer;
    private final AtomicBoolean snapshotFlag;
    private final Object leaderChangedEvent = "leaderChangedEvent";

    private PartitionEngineOptions options;
    private PartitionStateMachine stateMachine;
    @Getter
    private RaftGroupService raftGroupService;
    private TaskManager taskManager;
    private SnapshotHandler snapshotHandler;
    private Node raftNode;
    private volatile boolean started;

    public PartitionEngine(HgStoreEngine storeEngine, ShardGroup shardGroup) {
        this.storeEngine = storeEngine;
        this.shardGroup = shardGroup;
        this.changingPeer = new AtomicBoolean(false);
        this.snapshotFlag = new AtomicBoolean(false);
        partitionManager = storeEngine.getPartitionManager();
        stateListeners = Collections.synchronizedList(new ArrayList());
    }

    /**
     * Record the partition information using this raft.
     */

    public synchronized void loadPartitionFromSnapshot(Partition partition) {
        partitionManager.loadPartitionFromSnapshot(partition);
    }

    public List<Metapb.Partition> loadPartitionsFromLocalDb() {
        return partitionManager.loadPartitionsFromDb(getGroupId());
    }

    public void removePartition(String graphName) {
        partitionManager.removePartition(graphName, options.getGroupId());
    }

    public boolean hasPartition(String graphName) {
        return partitionManager.hasPartition(graphName, getGroupId());
    }

    public Integer getGroupId() {
        return options.getGroupId();
    }

    /**
     * Initialize the raft engine
     *
     * @return
     */
    @Override
    public synchronized boolean init(PartitionEngineOptions opts) {
        this.options = opts;
        if (this.started) {
            log.info("PartitionEngine: {} already started.", this.options.getGroupId());
            return true;
        }

        log.info("PartitionEngine starting: {}", this);
        this.taskManager = new TaskManager(storeEngine.getBusinessHandler(), opts.getGroupId());
        this.snapshotHandler = new SnapshotHandler(this);
        this.stateMachine = new PartitionStateMachine(opts.getGroupId(), snapshotHandler);
        // probably null in test case
        if (opts.getTaskHandler() != null) {
            this.stateMachine.addTaskHandler(opts.getTaskHandler());
        }
        this.stateMachine.addTaskHandler(new TaskHandler());

        // Listen for changes in the group leader
        this.stateMachine.addStateListener(this);

        new File(options.getRaftDataPath()).mkdirs();

        Configuration initConf = opts.getConf();
        if (initConf == null) {
            String peersList = StringUtils.join(options.getPeerList(), ",");
            initConf = new Configuration();
            initConf.parse(peersList);
        }

        // Set Node parameters, including log storage path and state machine instance
        NodeOptions nodeOptions = new NodeOptions();
        nodeOptions.setFsm(stateMachine);
        nodeOptions.setEnableMetrics(true);
        // Log path
        nodeOptions.setLogUri(options.getRaftDataPath() + "/log");
        // raft metadata path
        nodeOptions.setRaftMetaUri(options.getRaftDataPath() + "/meta");
        // Snapshot path
        nodeOptions.setSnapshotUri(options.getRaftSnapShotPath() + "/snapshot");
        nodeOptions.setSharedTimerPool(true);
        nodeOptions.setSharedElectionTimer(true);
        nodeOptions.setSharedSnapshotTimer(true);
        nodeOptions.setSharedStepDownTimer(true);
        nodeOptions.setSharedVoteTimer(true);
        nodeOptions.setFilterBeforeCopyRemote(true);

        HgStoreEngineOptions.RaftOptions raft = options.getRaftOptions();
        nodeOptions.setServiceFactory(new DefaultJRaftServiceFactory() {
            @Override
            public LogStorage createLogStorage(final String uri, final RaftOptions raftOptions) {
                if (options.getRaftOptions().isUseRocksDBSegmentLogStorage()) {
                    return new RocksDBSegmentLogStorage(uri, raftOptions);
                } else {
                    return new RocksDBLogStorage(uri, raftOptions);
                }
            }
        });
        // Initial cluster
        nodeOptions.setInitialConf(initConf);
        // Snapshot time interval
        nodeOptions.setSnapshotIntervalSecs(raft.getSnapshotIntervalSecs());
        //todo soya fix
        // nodeOptions.setSnapShotDownloadingThreads(raft.getSnapshotDownloadingThreads());

        // nodeOptions.setSnapshotLogIndexMargin(options.getRaftOptions()
        // .getSnapshotLogIndexMargin());

        nodeOptions.setRpcConnectTimeoutMs(raft.getRpcConnectTimeoutMs());
        nodeOptions.setRpcDefaultTimeout(raft.getRpcDefaultTimeout());
        nodeOptions.setRpcInstallSnapshotTimeout(raft.getRpcInstallSnapshotTimeout());
        nodeOptions.setElectionTimeoutMs(raft.getElectionTimeoutMs());
        // Set raft configuration
        RaftOptions raftOptions = nodeOptions.getRaftOptions();
        raftOptions.setDisruptorBufferSize(raft.getDisruptorBufferSize());
        raftOptions.setMaxEntriesSize(raft.getMaxEntriesSize());
        raftOptions.setMaxReplicatorInflightMsgs(raft.getMaxReplicatorInflightMsgs());
        raftOptions.setMaxByteCountPerRpc(1024 * 1024);
        nodeOptions.setEnableMetrics(true);
        final PeerId serverId = JRaftUtils.getPeerId(options.getRaftAddress());

        // Build raft group and start raft
        this.raftGroupService = new RaftGroupService(raftPrefix + options.getGroupId(),
                                                     serverId, nodeOptions,
                                                     storeEngine.getRaftRpcServer(), true);
        this.raftNode = raftGroupService.start(false);
        this.raftNode.addReplicatorStateListener(new ReplicatorStateListener());

        // Check if the peers returned by pd are consistent with the local ones, if not, reset
        // the peerlist
        if (this.raftNode != null) {
            // TODO: Check peer list, if peer changes, perform reset
            started = true;
        }

        log.info("PartitionEngine start successfully: id = {}, peers list = {}",
                 options.getGroupId(), nodeOptions.getInitialConf().getPeers());
        return this.started;
    }

    public HgStoreEngine getStoreEngine() {
        return this.storeEngine;
    }

    public ShardGroup getShardGroup() {
        return shardGroup;
    }

    /**
     * 1. Receive the partition migration command sent by PD, add the migration task to the state
     * machine, the state is new.
     * 2, execute state machine messages, add to the task queue, and execute tasks.
     * 3. Compare old and new peers to identify added and removed peers.
     * 4. If there is a new peer added
     * 4.1, For newly added peers, notify the peer to create the raft state machine.
     * 4.2, Join the raft group in learner mode.
     * 4.3, Listen for snapshot synchronization events, and repeat step 3.
     * 5. No new peers exist.
     * 5.1, Remove learner and wait for return
     * 5.2, Modify learner to peer, join raft group
     * 6. Existence of deleted peer
     * 6.1, Notify peer, delete state machine and delete data
     *
     * @param peers
     * @param done
     * @return true means completed, false means not completed
     */
    public Status changePeers(List<String> peers, final Closure done) {
        if (ListUtils.isEqualList(peers, RaftUtils.getPeerEndpoints(raftNode))) {
            return Status.OK();
        }

        Status result = HgRaftError.TASK_CONTINUE.toStatus();
        List<String> oldPeers = RaftUtils.getAllEndpoints(raftNode);
        log.info("Raft {} changePeers start, old peer is {}, new peer is {}",
                 getGroupId(), oldPeers, peers);
        // Check the peer that needs to be added.
        List<String> addPeers = ListUtils.removeAll(peers, oldPeers);
        // learner to be deleted. Possible peer change.
        List<String> removedPeers = ListUtils.removeAll(oldPeers, peers);

        HgCmdClient rpcClient = storeEngine.getHgCmdClient();
        // Generate a new Configuration object

        Configuration oldConf = getCurrentConf();
        Configuration conf = oldConf.copy();

        FutureClosure closure;

        if (!addPeers.isEmpty()) {
            addPeers.forEach(peer -> conf.addLearner(JRaftUtils.getPeerId(peer)));
            doSnapshot(status -> log.info("Raft {} snapshot before add learner, result:{}",
                                          getGroupId(), status));
            // 2.1 learner join in raft group
            for (var peer : addPeers) {
                closure = new FutureClosure();
                rpcClient.createRaftNode(peer, partitionManager.getPartitionList(getGroupId()),
                                         conf, closure);
                var status = closure.get();
                if (!status.isOk()) {
                    log.info("Raft {} createRaftNode, peer:{}, reason:{}", getGroupId(), peer,
                             status.getErrorMsg());
                    return status;
                }
            }

            closure = new FutureClosure();
            raftNode.changePeers(conf, closure);
            var status = closure.get();
            if (!status.isOk()) {
                log.info("Raft {} changePeers failed, reason:{}", getGroupId(),
                         status.getErrorMsg());
                return status;
            }

            // 2.2 Waiting learner to synchronize snapshot (check added learner)
            //todo Each learner will wait for 1s, if another one is not sync.Consider using
            // countdownLatch
            boolean allLearnerSnapshotOk = false;
            long current = System.currentTimeMillis();
            while (!allLearnerSnapshotOk) {
                boolean snapshotOk = true;
                for (var peerId : addPeers) {
                    var state = getReplicatorState(JRaftUtils.getPeerId(peerId));
                    log.info("Raft {}, peer:{}, replicate state:{}", getGroupId(), peerId, state);
                    if (state != Replicator.State.Replicate) {
                        snapshotOk = false;
                    }
                }
                allLearnerSnapshotOk = snapshotOk;

                if (!allLearnerSnapshotOk) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        log.warn("Raft {} sleep when check learner snapshot", getGroupId());
                    }
                }
                if (System.currentTimeMillis() - current > 600 * 1000) {
                    return HgRaftError.TASK_CONTINUE.toStatus();
                }
            }

            log.info("Raft {} replicate status is OK", getGroupId());

            closure = new FutureClosure();
            // 2.3 change learner to follower (first remove, then add follower)
            raftNode.removeLearners(conf.listLearners(), closure);
            if (!closure.get().isOk()) {
                log.error("Raft {} remove learner error, result:{}", getGroupId(), status);
                return HgRaftError.TASK_ERROR.toStatus();
            }

            addPeers.forEach(peer -> {
                conf.removeLearner(JRaftUtils.getPeerId(peer));
                conf.addPeer(JRaftUtils.getPeerId(peer));
            });

            // add follower
            closure = new FutureClosure();
            raftNode.changePeers(conf, closure);
            if (!closure.get().isOk()) {
                log.error("Raft {} changePeers error, result:{}", getGroupId(), status);
                return HgRaftError.TASK_ERROR.toStatus();
            }
        }

        boolean removeSelf = false;
        // case 3:
        if (!removedPeers.isEmpty()) {
            var self = this.getRaftNode().getNodeId().getPeerId().getEndpoint().toString();
            removeSelf = removedPeers.contains(self);
            // 3.1 remove peers
            List<String> toDestroy = new ArrayList<>();
            for (var peer : removedPeers) {
                if (Objects.equals(peer, self)) {
                    continue;
                }
                conf.removeLearner(JRaftUtils.getPeerId(peer));
                conf.removePeer(JRaftUtils.getPeerId(peer));
                toDestroy.add(peer);
            }

            closure = new FutureClosure();
            raftNode.changePeers(conf, closure);
            var status = closure.get();

            if (!status.isOk()) {
                log.error("Raft {} changePeers error after destroy, result:{}", getGroupId(),
                          status);
                return HgRaftError.TASK_ERROR.toStatus();
            } else {
                for (var peer : toDestroy) {
                    closure = new FutureClosure();
                    rpcClient.destroyRaftNode(peer, partitionManager.getPartitionList(getGroupId()),
                                              closure);
                    log.info("Raft {} destroy raft node {}, result:{}", peer, getGroupId(),
                             closure.get());
                }
            }

            // transfer leadership to any peer
            if (removeSelf) {
                raftNode.transferLeadershipTo(PeerId.ANY_PEER);
            }
        }

        return removeSelf ? HgRaftError.TASK_CONTINUE.toStatus() : HgRaftError.OK.toStatus();
    }

    public void addRaftTask(RaftOperation operation, RaftClosure closure) {
        if (!isLeader()) {
            closure.run(new Status(HgRaftError.NOT_LEADER.getNumber(), "Not leader"));
            return;
        }
        final Task task = new Task();
        task.setData(ByteBuffer.wrap(operation.getValues()));
        task.setDone(new DefaultRaftClosure(operation, closure));
        this.raftNode.apply(task);
    }

    @Override
    public void shutdown() {
        if (!this.started) {
            return;
        }
        if (this.raftGroupService != null) {
            this.raftGroupService.shutdown();
            try {
                this.raftGroupService.join();
            } catch (final InterruptedException e) {
                ThrowUtil.throwException(e);
            }
        }
        this.started = false;
        log.info("PartitionEngine shutdown successfully: {}.", this);
    }

    /**
     * Restart raft engine
     */
    public void restartRaftNode() {
        shutdown();
        log.error("Raft {} is restarting !!!", getGroupId());
        this.init(this.options);
    }

    /**
     * Check if it is active, if not, restart it.
     */
    public void checkActivity() {
        Utils.runInThread(() -> {
            if (!this.raftNode.getNodeState().isActive()) {
                log.error("Raft {} is not activity state is {} ",
                          this.getGroupId(), raftNode.getNodeState());
                restartRaftNode();
            }
        });
    }

    /**
     * raft peer is destroyed, deleting logs and data
     */
    public void destroy() {
        shutdown();
        try {
            FileUtils.deleteDirectory(new File(this.options.getRaftDataPath()));
            if (!Objects.equals(this.options.getRaftDataPath(),
                                this.options.getRaftSnapShotPath())) {
                FileUtils.deleteDirectory(new File(this.options.getRaftSnapShotPath()));
            }
        } catch (IOException e) {
            log.error("Raft {} destroy exception {}", this.options.getGroupId(), e);
        }
    }

    public boolean isLeader() {
        return this.raftNode != null && this.raftNode.isLeader(false);
    }

    public Endpoint getLeader() {
        PeerId peerId = this.raftNode.getLeaderId();
        return peerId != null ? peerId.getEndpoint() : null;
    }

    public void addStateListener(PartitionStateListener listener) {
        this.stateListeners.add(listener);
    }

    /**
     * Return all active peers
     *
     * @return
     */
    public Map<Long, PeerId> getAlivePeers() {
        Map<Long, PeerId> peers = new HashMap<>();
        raftNode.listAlivePeers().forEach(peerId -> {
            Shard shard = partitionManager.getShardByEndpoint(shardGroup,
                                                              peerId.getEndpoint().toString());
            if (shard != null) {
                peers.put(shard.getStoreId(), peerId);
            }
        });
        return peers;
    }

    public Node getRaftNode() {
        return raftNode;
    }

    /**
     * Waiting for Leader to be elected
     *
     * @param timeOut
     * @return
     */
    public Endpoint waitForLeader(long timeOut) {
        Endpoint leader = getLeader();
        if (leader != null) {
            return leader;
        }

        synchronized (leaderChangedEvent) {
            leader = getLeader();
            if (leader != null) {
                return leader;
            }

            long start = System.currentTimeMillis();
            while ((System.currentTimeMillis() - start < timeOut) && (leader == null)) {
                try {
                    leaderChangedEvent.wait(timeOut);
                } catch (InterruptedException e) {
                    log.error("Raft {} wait for leader exception", this.options.getGroupId(), e);
                }
                leader = getLeader();
                if (leader == null) {
                    if (partitionManager.isLocalPartition(this.options.getGroupId())) {
                        log.error("Raft {} leader not found, try to repair!",
                                  this.options.getGroupId());
                        // TODO: Check if raft is local, if so, try to fix the Leader, including
                        //  checking if the configuration is correct.
                        storeEngine.createPartitionGroups(
                                partitionManager.getPartitionList(getGroupId()).get(0));
                    }
                } else {
                    log.info("Raft {} wait for leader success, latency time {}",
                             this.options.getGroupId(), System.currentTimeMillis() - start);
                }
            }
            return leader;
        }
    }

    public Map<String, Partition> getPartitions() {
        return partitionManager.getPartitions(getGroupId());
    }

    public Partition getPartition(String graphName) {
        return partitionManager.getPartition(graphName, getGroupId());
    }

    public PartitionEngineOptions getOptions() {
        return options;
    }

    @Override
    public String toString() {
        return "PartitionEngine{" + "groupId =" + this.options.getGroupId() + ", isLeader=" +
               isLeader()
               + ", options=" + this.options + '}';
    }

    /**
     * update partition leader
     *
     * @param newTerm the new term
     */
    @Override
    public void onLeaderStart(long newTerm) {
        log.info("Raft {} onLeaderStart newTerm is {}", getGroupId(), newTerm);
        // Update shard group object
        shardGroup.changeLeader(partitionManager.getStore().getId());

        onConfigurationCommitted(getCurrentConf());
        synchronized (leaderChangedEvent) {
            leaderChangedEvent.notifyAll();
        }

    }

    @Override
    public void onStartFollowing(final PeerId newLeaderId, final long newTerm) {
        onConfigurationCommitted(getCurrentConf());
        synchronized (leaderChangedEvent) {
            leaderChangedEvent.notifyAll();
        }
    }

    /**
     * update partition shardList
     *
     * @param conf committed configuration
     */
    @Override
    public void onConfigurationCommitted(Configuration conf) {
        if (storeEngine.isClosing().get()) {
            return;
        }
        try {
            // Update shardlist
            log.info("Raft {} onConfigurationCommitted, conf is {}", getGroupId(), conf.toString());
            // According to raft endpoint find storeId
            List<Long> peerIds = new ArrayList<>();
            for (String peer : RaftUtils.getPeerEndpoints(conf)) {
                Store store = getStoreByEndpoint(peer);
                if (store != null) {
                    peerIds.add(store.getId());
                } else {
                    log.error("Raft {} GetStoreInfo failure, {}", getGroupId(), peer);
                }
            }
            List<Long> learners = new ArrayList<>();
            for (String learner : RaftUtils.getLearnerEndpoints(conf)) {
                Store store = getStoreByEndpoint(learner);
                if (store != null) {
                    learners.add(store.getId());
                } else {
                    log.error("Raft {} GetStoreInfo failure, {}", getGroupId(), learner);
                }
            }

            shardGroup.changeShardList(peerIds, learners, partitionManager.getStore().getId());
            partitionManager.updateShardGroup(shardGroup);

            if (isLeader()) {
                // partitionManager.getPartitionList(getGroupId()).forEach(partition -> {
                //    partitionManager.changeShards(partition, shardGroup.getMetaPbShard());
                // });
                try {
                    var pdGroup = storeEngine.getPdProvider().getShardGroupDirect(getGroupId());
                    List<String> peers = partitionManager.shards2Peers(pdGroup.getShardsList());

                    Long leaderStoreId = null;
                    for (var shard : pdGroup.getShardsList()) {
                        if (shard.getRole() == Metapb.ShardRole.Leader) {
                            leaderStoreId = shard.getStoreId();
                        }
                    }
                    // Update PD information when leader changes, peers differ, or learners are
                    // different
                    if (!SetUtils.isEqualSet(peers, RaftUtils.getPeerEndpoints(raftNode)) ||
                        !SetUtils.isEqualSet(learners, RaftUtils.getLearnerEndpoints(raftNode)) ||
                        !Objects.equals(leaderStoreId, partitionManager.getStore().getId())) {
                        partitionManager.getPdProvider().updateShardGroup(shardGroup.getProtoObj());
                    }

                } catch (PDException e) {
                    throw new RuntimeException(e);
                }
            }
            log.info("Raft {} onConfigurationCommitted, shardGroup {}", getGroupId(),
                     shardGroup.getMetaPbShard());

        } catch (Exception e) {
            log.error("Raft {} onConfigurationCommitted exception {}", getGroupId(), e);
        }

    }

    private Store getStoreByEndpoint(String endpoint) {
        Store store = partitionManager.getStoreByRaftEndpoint(getShardGroup(), endpoint);
        if (store == null || store.getId() == 0) {
            store = this.storeEngine.getHgCmdClient().getStoreInfo(endpoint);
        }
        return store;
    }

    @Override
    public void onDataCommitted(long index) {

    }

    @Override
    public void onError(RaftException e) {
        this.restartRaftNode();
    }

    public NodeMetrics getNodeMetrics() {
        return this.raftNode.getNodeMetrics();
    }

    public long getCommittedIndex() {
        return stateMachine.getCommittedIndex();
    }

    public long getLeaderTerm() {
        return stateMachine.getLeaderTerm();
    }

    public TaskManager getTaskManager() {
        return this.taskManager;
    }

    /**
     * Received PD's leader transfer command
     *
     * @param graphName
     * @param shard
     * @return
     */
    public Status transferLeader(String graphName, Metapb.Shard shard) {
        if (!isLeader()) {
            return new Status(HgRaftError.NOT_LEADER.getNumber(), "Not leader");
        }
        String address = partitionManager.getStore(shard.getStoreId()).getRaftAddress();
        return raftNode.transferLeadershipTo(JRaftUtils.getPeerId(address));
    }

    /**
     * Received the modification copy instruction sent by pd
     * 1. Compare new and old peers, identify added and removed peers.
     * 2. For new peers, join as a learner.
     * 3. Listen for snapshot synchronization events
     * 4. After the snapshot synchronization is completed, call changePeers, change the learner
     * to follower, and delete the old peer.
     */
    public void doChangeShard(final MetaTask.Task task, Closure done) {
        try {
            if (!isLeader() || !changingPeer.compareAndSet(false, true)) {
                return;
            }

            log.info("Raft {} doChangeShard task is {}", getGroupId(), task);
            Utils.runInThread(() -> {
                List<String> peers =
                        partitionManager.shards2Peers(task.getChangeShard().getShardList());
                HashSet<String> hashSet = new HashSet<>(peers);

                try {
                    // If there are duplicate peers in the task, it indicates the task itself has errors, ignore the task
                    if (peers.size() != hashSet.size()) {
                        log.info("Raft {} doChangeShard peer is repeat, peers:{}", getGroupId(),
                                 peers);
                        return;
                    }
                    Status result = changePeers(peers, null);

                    if (result.getCode() == HgRaftError.TASK_CONTINUE.getNumber()) {
                        // Need to resend a request
                        storeEngine.addRaftTask(task.getPartition().getGraphName(),
                                                task.getPartition().getId(), RaftOperation.create(
                                        RaftOperation.SYNC_PARTITION_TASK, task), status -> {
                                    if (!status.isOk()) {
                                        log.error(
                                                "Raft {} addRaftTask error, " + "status " + "is {}",
                                                task.getPartition().getId(), status);
                                    }
                                });
                    }
                    log.info("Raft {} doChangeShard result is {}", getGroupId(), result);
                } catch (Exception e) {
                    log.error("Raft {} doChangeShard exception {}", getGroupId(), e);
                } finally {
                    changingPeer.set(false);
                }
            });
        } finally {
            if (done != null) {
                done.run(Status.OK());
            }
        }
    }

    /**
     * Received data transfer between partitions sent by PD
     * 1. Notify the target machine to create raft
     * 2. Copy data from the source machine to the target machine
     * 3. After the migration is successful, notify PD to update partition information.
     * 4. Delete source partition
     *
     * @return
     */
    public Status moveData(MetaTask.Task task) {

        synchronized (this) {
            if (taskManager.taskExists(task.getPartition().getId(),
                                       task.getPartition().getGraphName(),
                                       task.getType().name())) {
                log.info("task : {}-{}-{} repeat", task.getPartition().getGraphName(),
                         task.getPartition().getId(),
                         task.getType().name());
                return Status.OK();
            }
            taskManager.updateTask(
                    task.toBuilder().setState(MetaTask.TaskState.Task_Doing).build());
        }

        Status status = Status.OK();

        switch (task.getType()) {
            case Split_Partition:
                status = handleSplitTask(task);
                break;
            case Move_Partition:
                status = handleMoveTask(task);
                break;
            default:
                break;
        }
        log.info("moveData {}-{}-{} result:{}", task.getPartition().getGraphName(),
                 task.getPartition().getId(),
                 task.getType().name(), status);

        if (status.isOk()) {
            // Report task execution results to PD
            partitionManager.reportTask(
                    task.toBuilder().setState(MetaTask.TaskState.Task_Success).build());
            // Update local task status
            taskManager.updateTask(
                    task.toBuilder().setState(MetaTask.TaskState.Task_Success).build());
        } else {
            partitionManager.reportTask(task.toBuilder()
                                            .setState(MetaTask.TaskState.Task_Failure)
                                            .setMessage(status.getErrorMsg()).build());
            // Update local task status
            taskManager.updateTask(task.toBuilder()
                                       .setState(MetaTask.TaskState.Task_Failure)
                                       .setMessage(status.getErrorMsg()).build());
        }

        return status;
    }

    /**
     * Corresponding to the divisional splitting task
     *
     * @param task split partition task
     * @return task execution result
     */
    private Status handleSplitTask(MetaTask.Task task) {

        log.info("Partition {}-{} moveData {}", task.getPartition().getGraphName(),
                 task.getPartition().getId(), task);

        Status status;

        List<Metapb.Partition> targets = task.getSplitPartition().getNewPartitionList();
        List<Metapb.Partition> newPartitions = targets.subList(1, targets.size());
        try {
            for (int i = 0; i < newPartitions.size(); i++) {
                storeEngine.createPartitionGroups(new Partition(newPartitions.get(i)));
            }
            // Copy data from the source machine to the target machine
            status = storeEngine.getDataManager().move(task.getPartition(), newPartitions);

            if (status.isOk()) {
                var source = Metapb.Partition.newBuilder(targets.get(0))
                                             .setState(Metapb.PartitionState.PState_Normal)
                                             .build();
                // Update local key range, and synchronize follower
                partitionManager.updatePartition(source, true);
                partitionManager.updateRange(source,
                                             (int) source.getStartKey(),
                                             (int) source.getEndKey());
            }

            if (!status.isOk()) {
                throw new Exception(status.getErrorMsg());
            }
        } catch (Exception e) {
            log.error("Partition {}-{} moveData exception {}",
                      task.getPartition().getGraphName(), task.getPartition().getId(), e);
            status = new Status(-1, e.getMessage());
        }

        return status;
    }

    /**
     * Corresponding to partition data movement task
     *
     * @param task move partition task
     * @return task execution result
     */
    private Status handleMoveTask(MetaTask.Task task) {
        Status status;
        try {
            log.info("handleMoveTask: start to copy {} data from {} to {}",
                     task.getPartition().getGraphName(),
                     task.getPartition().getId(),
                     task.getMovePartition().getTargetPartition().getId());
            status = storeEngine.getDataManager().move(task.getPartition(),
                                                       task.getMovePartition()
                                                           .getTargetPartition());
        } catch (Exception e) {
            log.error("handleMoveTask got exception: ", e);
            status = new Status(-1, e.getMessage());
        }
        return status;
    }

    /**
     * For the entire graph deletion, clear the deletion partition, if there are no other graphs,
     * destroy the raft group.
     * Need to be placed after the call to move data
     *
     * @param graphName   graph name
     * @param partitionId partition id
     * @param keyStart    key start used for verification
     * @param keyEnd      key end used for verification
     * @param isLeader    Whether leader, to avoid leader drifting, the leader status when moving
     *                    data
     */
    private synchronized void destroyPartitionIfGraphsNull(String graphName, int partitionId,
                                                           long keyStart, long keyEnd,
                                                           boolean isLeader) {
        Partition partition = partitionManager.getPartition(graphName, partitionId);

        // key range validation
        if (partition != null && partition.getEndKey() == keyEnd &&
            partition.getStartKey() == keyStart) {
            log.info("remove partition id :{}, graph:{}", partition.getId(),
                     partition.getGraphName());
            storeEngine.deletePartition(partitionId, graphName);
        }

        // No partition engine present
        if (isLeader && partition == null) {
            partitionManager.deletePartition(graphName, partitionId);
        }

        if (isLeader && partition != null) {
            if (partitionManager.getPartitions(partitionId).size() == 0) {
                log.info("destroyPartitionIfGraphsNull, destroy raft group id:{}",
                         partition.getId());
                storeEngine.destroyPartitionGroups(partition);
                try {
                    // delete shard group from pd
                    partitionManager.getPdProvider().deleteShardGroup(partitionId);
                } catch (PDException e) {
                    log.error("delete shard group failed, status:{}", e.getMessage());
                }
            }
        }
    }

    public void snapshot() {
        log.info("Raft {} send snapshot command. ", this.getGroupId());
        // Null instruction, placeholder
        this.addRaftTask(
                RaftOperation.create(RaftOperation.BLANK_TASK), status -> {
                    // Generate snapshot command
                    this.addRaftTask(
                            RaftOperation.create(RaftOperation.DO_SNAPSHOT), status2 -> {
                            });
                });

    }

    protected void doSnapshot(Closure done) {
        log.info("Raft {} doSnapshot. ", this.getGroupId());

        if (this.snapshotFlag.compareAndSet(false, true)) {

            raftNode.snapshot(status -> {
                log.info("Raft {}  snapshot OK. ", this.getGroupId());
                if (done != null) {
                    done.run(status);
                }
            });
        }

        this.snapshotFlag.set(false);
    }

    public void addBlankRaftTask() {
        // Null instruction, placeholder
        this.addRaftTask(
                RaftOperation.create(RaftOperation.BLANK_TASK), status -> {
                });
    }

    private void handleCleanOp(CleanDataRequest request) {
        // Avoid leader drift during data cleanup process
        boolean isLeader = isLeader();
        var partition =
                partitionManager.getPartition(request.getGraphName(), request.getPartitionId());

        if (partition != null) {
            storeEngine.getDataManager().clean(request);
            storeEngine.getBusinessHandler()
                       .dbCompaction(partition.getGraphName(), partition.getId());

            if (request.isDeletePartition()) {
                destroyPartitionIfGraphsNull(request.getGraphName(), request.getPartitionId(),
                                             request.getKeyStart(), request.getKeyEnd(), isLeader);
            }
        } else {
            log.info("handleCleanOp, partition is null. {}-{}", request.getGraphName(),
                     request.getPartitionId());
            partition = new Partition() {{
                setId(request.getPartitionId());
                setGraphName(request.getGraphName());
                setVersion(0);
                setStartKey(0);
                setEndKey(0);
                setWorkState(Metapb.PartitionState.PState_Normal);
            }};
        }

        // report task
        if (isLeader) {
            MetaTask.Task task = MetaTask.Task.newBuilder()
                                              .setType(Clean_Partition)
                                              .setId(request.getTaskId())
                                              .setPartition(partition.getProtoObj())
                                              .setCleanPartition(
                                                      CleanDataRequest.toCleanPartitionTask(
                                                              request))
                                              .setState(MetaTask.TaskState.Task_Success)
                                              .build();
            partitionManager.reportTask(task);
        }
    }

    public void buildIndex(MetaTask.Task task) {

        var state = MetaTask.TaskState.Task_Failure;
        String message = "SUCCESS";
        try {
            var status = storeEngine.getDataManager().doBuildIndex(task.getBuildIndex().getParam(),
                                                                   task.getPartition());
            if (status.isOk()) {
                state = MetaTask.TaskState.Task_Success;
            } else {
                message = status.getErrorMsg();
            }

        } catch (Exception e) {
            message = e.getMessage() == null ? "UNKNOWN" : e.getMessage();
            log.error("build index error:", e);
        }

        try {
            partitionManager.reportTask(
                    task.toBuilder().setState(state).setMessage(message).build());
        } catch (Exception e) {
            log.error("report task failed: error :", e);
        }

    }

    public void doSnapshotSync(Closure done) {
        long lastIndex = raftNode.getLastAppliedLogIndex();
        BusinessHandler handler = storeEngine.getBusinessHandler();
        Integer groupId = getGroupId();
        String lockPath = handler.getLockPath(groupId);
        AtomicInteger state = handler.getState(groupId);
        if (state != null && state.get() == BusinessHandler.compactionDone) {
            log.info("Partition {},path:{} prepare to doSnapshotSync", this.getGroupId(), lockPath);
            BusinessHandlerImpl.getCompactionPool().execute(() -> {
                try {
                    long start = System.currentTimeMillis();
                    while ((System.currentTimeMillis() - start) < 5000 &&
                           raftNode.getLastAppliedLogIndex() == lastIndex) {
                        synchronized (state) {
                            state.wait(200);
                        }
                    }
                    log.info("Partition {},path:{}  begin to doSnapshotSync", this.getGroupId(),
                             lockPath);
                    //todo soya may have problem
                    //raftNode.getRaftOptions().setTruncateLog(true);
                    CountDownLatch latch = new CountDownLatch(1);
                    AtomicReference<Status> result = new AtomicReference<>();
                    raftNode.snapshot(status -> {
                        result.set(status);
                        try {
                            //todo soya may have problem
                            //raftNode.getRaftOptions().setTruncateLog(false);
                            latch.countDown();
                            log.info("Partition {},path: {} doSnapshotSync result : {}. ", groupId,
                                     lockPath, status);
                        } catch (Exception e) {
                            log.error("wait doSnapshotSync with error:", e);
                        } finally {
                            handler.setAndNotifyState(groupId, BusinessHandler.compactionCanStart);
                            handler.unlock(lockPath);
                            log.info("Partition {},path: {} release dbCompaction lock", groupId,
                                     lockPath);
                        }
                    });
                    latch.await();
                } catch (Exception e) {
                    log.error("doSnapshotSync with error:", e);
                    handler.setAndNotifyState(groupId, BusinessHandler.compactionCanStart);
                    handler.unlock(lockPath);
                }
            });
        }
        if (done != null) {
            done.run(Status.OK());
        }
    }

    public void doBlankTaskSync(Closure done) {
        try {
            doSnapshotSync(done);
        } catch (Exception e) {
            Integer groupId = getGroupId();
            // String msg = String.format("Partition %s blank task done with error：", groupId);
            // log.error(msg, e);
            if (done != null) {
                done.run(new Status(-1, e.getMessage()));
            }
        }
    }

    public Configuration getCurrentConf() {
        return new Configuration(this.raftNode.listPeers(), this.raftNode.listLearners());
    }

    private Replicator.State getReplicatorState(PeerId peerId) {
        return RaftReflectionUtil.getReplicatorState(this.raftNode, peerId);
    }

    class ReplicatorStateListener implements Replicator.ReplicatorStateListener {

        @Override
        public void onCreated(PeerId peer) {
            log.info("Raft {} Replicator onCreated {}", getGroupId(), peer);
        }

        @Override
        public void onError(PeerId peer, Status status) {
            // log.info("Raft {} Replicator onError {} {}", getGroupId(), peer, status);
        }

        @Override
        public void onDestroyed(PeerId peer) {

        }

        /**
         * Listen for changes in replicator status to determine if the snapshot is fully
         * synchronized.
         * Check if there is a changeShard task, if it exists, call changeShard.
         */
        @Override
        public void stateChanged(final PeerId peer, final ReplicatorState newState) {
            log.info("Raft {} Replicator stateChanged {} {}", getGroupId(), peer, newState);
            if (newState == ReplicatorState.ONLINE) {
                MetaTask.Task task = taskManager.getOneTask(getGroupId(),
                                                            MetaTask.TaskType.Change_Shard.name());
                if (task != null) {
                    doChangeShard(task, status -> {
                        log.info("Raft {} (replicator state changed) changeShard task {}, " +
                                 "status is {}", getGroupId(), task, status);
                    });
                }
            }
        }
    }

    class TaskHandler implements RaftTaskHandler {

        @Override
        public boolean invoke(final int groupId, byte[] request,
                              RaftClosure response) throws HgStoreException {
            try {
                CodedInputStream input = CodedInputStream.newInstance(request);
                byte methodId = input.readRawByte();
                switch (methodId) {
                    case RaftOperation.SYNC_PARTITION_TASK:
                        invoke(groupId, methodId, MetaTask.Task.parseFrom(input), response);
                        break;
                    case RaftOperation.SYNC_PARTITION:
                        invoke(groupId, methodId, Metapb.Partition.parseFrom(input), response);
                        break;
                    case RaftOperation.DO_SNAPSHOT:
                    case RaftOperation.DO_SYNC_SNAPSHOT:
                    case RaftOperation.BLANK_TASK:
                    case RaftOperation.SYNC_BLANK_TASK:
                        invoke(groupId, methodId, null, response);
                        break;
                    case RaftOperation.IN_WRITE_OP:
                    case RaftOperation.RAFT_UPDATE_PARTITION:
                    case RaftOperation.IN_CLEAN_OP:
                    case RaftOperation.DB_COMPACTION:
                        invoke(groupId, methodId, RaftOperation.toObject(request, 0), response);
                        break;
                    default:
                        return false;
                }
            } catch (IOException e) {
                log.error("raft task exception ", e);
            }
            return true;
        }

        @Override
        public boolean invoke(final int groupId, byte methodId, Object req,
                              RaftClosure response) throws HgStoreException {
            switch (methodId) {
                case RaftOperation.SYNC_PARTITION_TASK: {
                    MetaTask.Task task = (MetaTask.Task) req;
                    taskManager.updateTask(task);
                    if (task.getType() == MetaTask.TaskType.Change_Shard) {
                        log.info("change shard task: id {}, change shard:{} ",
                                 task.getId(), task.getChangeShard());
                        doChangeShard(task, response);
                    }
                }
                break;
                case RaftOperation.SYNC_PARTITION:
                    log.info("receive sync partition {}", req);
                    if (!isLeader()) {
                        partitionManager.updatePartition((Metapb.Partition) req, true);
                    }
                    break;
                case RaftOperation.BLANK_TASK:
                    break;
                case RaftOperation.DO_SNAPSHOT:
                    doSnapshot(response);
                    break;
                case RaftOperation.IN_WRITE_OP:
                    storeEngine.getDataManager().write((BatchPutRequest) (req));
                    break;
                case RaftOperation.IN_CLEAN_OP:
                    handleCleanOp((CleanDataRequest) req);
                    break;
                case RaftOperation.RAFT_UPDATE_PARTITION:
                    log.info("Raft {}, receive raft updatePartitionRangeOrState {}",
                             getGroupId(), req);
                    partitionManager.updatePartitionRangeOrState((UpdatePartitionRequest) (req));
                    break;
                case RaftOperation.DB_COMPACTION:
                    DbCompactionRequest dbCompactionRequest = (DbCompactionRequest) (req);
                    storeEngine.getBusinessHandler()
                               .dbCompaction(dbCompactionRequest.getGraphName(),
                                             dbCompactionRequest.getPartitionId(),
                                             dbCompactionRequest.getTableName());
                    break;
                case RaftOperation.DO_SYNC_SNAPSHOT:
                    doSnapshotSync(response);
                    break;
                case RaftOperation.SYNC_BLANK_TASK:
                    doBlankTaskSync(response);
                    break;
                default:
                    return false;
            }
            return true;
        }
    }

}
