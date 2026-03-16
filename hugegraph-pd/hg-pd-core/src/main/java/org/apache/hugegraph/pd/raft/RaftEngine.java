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

package org.apache.hugegraph.pd.raft;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.config.PDConfig;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.pd.grpc.Pdpb;
import org.apache.hugegraph.pd.raft.auth.IpAuthHandler;

import com.alipay.remoting.ExtendedNettyChannelHandler;
import com.alipay.remoting.config.BoltServerOption;
import com.alipay.sofa.jraft.JRaftUtils;
import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.RaftGroupService;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.core.Replicator;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.entity.Task;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.option.NodeOptions;
import com.alipay.sofa.jraft.option.RaftOptions;
import com.alipay.sofa.jraft.option.RpcOptions;
import com.alipay.sofa.jraft.rpc.RaftRpcServerFactory;
import com.alipay.sofa.jraft.rpc.RpcServer;
import com.alipay.sofa.jraft.rpc.impl.BoltRpcServer;
import com.alipay.sofa.jraft.util.Endpoint;
import com.alipay.sofa.jraft.util.internal.ThrowUtil;

import io.netty.channel.ChannelHandler;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RaftEngine {

    private volatile static RaftEngine instance = new RaftEngine();
    private RaftStateMachine stateMachine;
    private String groupId = "pd_raft";
    private PDConfig.Raft config;
    private RaftGroupService raftGroupService;
    private RpcServer rpcServer;
    private Node raftNode;
    private RaftRpcClient raftRpcClient;

    public RaftEngine() {
        this.stateMachine = new RaftStateMachine();
    }

    public static RaftEngine getInstance() {
        return instance;
    }

    public synchronized boolean init(PDConfig.Raft config) {
        if (this.raftNode != null) {
            return false;
        }
        this.config = config;

        raftRpcClient = new RaftRpcClient();
        raftRpcClient.init(new RpcOptions());

        String raftPath = config.getDataPath() + "/" + groupId;
        new File(raftPath).mkdirs();

        new File(config.getDataPath()).mkdirs();
        Configuration initConf = new Configuration();
        initConf.parse(config.getPeersList());
        if (config.isEnable() && config.getPeersList().length() < 3) {
            log.error(
                    "The RaftEngine parameter is incorrect." +
                    " When RAFT is enabled, the number of peers " +
                    "cannot be less than 3");
        }
        // Set node parameters, including the log storage path and state machine instance
        NodeOptions nodeOptions = new NodeOptions();
        nodeOptions.setFsm(stateMachine);
        nodeOptions.setEnableMetrics(true);
        // Log path
        nodeOptions.setLogUri(raftPath + "/log");
        // raft metadata path
        nodeOptions.setRaftMetaUri(raftPath + "/meta");
        // Snapshot path
        nodeOptions.setSnapshotUri(raftPath + "/snapshot");
        // Initial cluster
        nodeOptions.setInitialConf(initConf);
        // Snapshot interval
        nodeOptions.setSnapshotIntervalSecs(config.getSnapshotInterval());

        nodeOptions.setRpcConnectTimeoutMs(config.getRpcTimeout());
        nodeOptions.setRpcDefaultTimeout(config.getRpcTimeout());
        nodeOptions.setRpcInstallSnapshotTimeout(config.getRpcTimeout());
        // Set the raft configuration
        RaftOptions raftOptions = nodeOptions.getRaftOptions();

        nodeOptions.setEnableMetrics(true);

        final PeerId serverId = JRaftUtils.getPeerId(config.getAddress());

        rpcServer = createRaftRpcServer(config.getAddress(), initConf.getPeers());
        // construct raft group and start raft
        this.raftGroupService =
                new RaftGroupService(groupId, serverId, nodeOptions, rpcServer, true);
        this.raftNode = raftGroupService.start(false);
        log.info("RaftEngine start successfully: id = {}, peers list = {}", groupId,
                 nodeOptions.getInitialConf().getPeers());
        return this.raftNode != null;
    }

    /**
     * Create a Raft RPC Server for communication between PDs
     */
    private RpcServer createRaftRpcServer(String raftAddr, List<PeerId> peers) {
        Endpoint endpoint = JRaftUtils.getEndPoint(raftAddr);
        RpcServer rpcServer = RaftRpcServerFactory.createRaftRpcServer(endpoint);
        configureRaftServerIpWhitelist(peers, rpcServer);
        RaftRpcProcessor.registerProcessor(rpcServer, this);
        rpcServer.init(null);
        return rpcServer;
    }

    private static void configureRaftServerIpWhitelist(List<PeerId> peers, RpcServer rpcServer) {
        if (rpcServer instanceof BoltRpcServer) {
            ((BoltRpcServer) rpcServer).getServer().option(
                    BoltServerOption.EXTENDED_NETTY_CHANNEL_HANDLER,
                    new ExtendedNettyChannelHandler() {
                        @Override
                        public List<ChannelHandler> frontChannelHandlers() {
                            return Collections.singletonList(
                                    IpAuthHandler.getInstance(
                                            peers.stream()
                                                 .map(PeerId::getIp)
                                                 .collect(Collectors.toSet())
                                    )
                            );
                        }

                        @Override
                        public List<ChannelHandler> backChannelHandlers() {
                            return Collections.emptyList();
                        }
                    }
            );
        }
    }

    public void shutDown() {
        if (this.raftGroupService != null) {
            this.raftGroupService.shutdown();
            try {
                this.raftGroupService.join();
            } catch (final InterruptedException e) {
                this.raftNode = null;
                ThrowUtil.throwException(e);
            }
            this.raftGroupService = null;
        }
        if (this.rpcServer != null) {
            this.rpcServer.shutdown();
            this.rpcServer = null;
        }
        if (this.raftNode != null) {
            this.raftNode.shutdown();
        }
        this.raftNode = null;
    }

    public boolean isLeader() {
        return this.raftNode.isLeader(true);
    }

    /**
     * Add a raft task, and grpc sends data to raft through this interface
     */
    public void addTask(Task task) {
        if (!isLeader()) {
            KVStoreClosure closure = (KVStoreClosure) task.getDone();
            closure.setError(Pdpb.Error.newBuilder().setType(Pdpb.ErrorType.NOT_LEADER).build());
            closure.run(new Status(RaftError.EPERM, "Not leader"));
            return;
        }
        this.raftNode.apply(task);
    }

    public void addStateListener(RaftStateListener listener) {
        this.stateMachine.addStateListener(listener);
    }

    public void addTaskHandler(RaftTaskHandler handler) {
        this.stateMachine.addTaskHandler(handler);
    }

    public PDConfig.Raft getConfig() {
        return this.config;
    }

    public PeerId getLeader() {
        return raftNode.getLeaderId();
    }

    /**
     * Send a message to the leader to get the grpc address;
     */
    public String getLeaderGrpcAddress() throws ExecutionException, InterruptedException {
        if (isLeader()) {
            return config.getGrpcAddress();
        }

        if (raftNode.getLeaderId() == null) {
            waitingForLeader(10000);
        }

        return raftRpcClient.getGrpcAddress(raftNode.getLeaderId().getEndpoint().toString()).get()
                            .getGrpcAddress();
    }

    /**
     * Obtain local member information
     *
     * @return Constructor for local member information object {@link Metapb.Member}
     */
    public Metapb.Member getLocalMember() {
        Metapb.Member.Builder builder = Metapb.Member.newBuilder();
        builder.setClusterId(config.getClusterId());
        builder.setRaftUrl(config.getAddress());
        builder.setDataPath(config.getDataPath());
        builder.setGrpcUrl(config.getGrpcAddress());
        builder.setRestUrl(config.getHost() + ":" + config.getPort());
        builder.setState(Metapb.StoreState.Up);
        return builder.build();
    }

    public List<Metapb.Member> getMembers() throws ExecutionException, InterruptedException {
        List<Metapb.Member> members = new ArrayList<>();

        List<PeerId> peers = raftNode.listPeers();
        peers.addAll(raftNode.listLearners());
        var learners = new HashSet<>(raftNode.listLearners());

        for (PeerId peerId : peers) {
            Metapb.Member.Builder builder = Metapb.Member.newBuilder();
            builder.setClusterId(config.getClusterId());
            CompletableFuture<RaftRpcProcessor.GetMemberResponse> future =
                    raftRpcClient.getGrpcAddress(peerId.getEndpoint().toString());

            Metapb.ShardRole role = Metapb.ShardRole.Follower;
            if (PeerUtil.isPeerEquals(peerId, raftNode.getLeaderId())) {
                role = Metapb.ShardRole.Leader;
            } else if (learners.contains(peerId)) {
                role = Metapb.ShardRole.Learner;
                var state = getReplicatorState(peerId);
                if (state != null) {
                    builder.setReplicatorState(state.name());
                }
            }

            builder.setRole(role);

            try {
                if (future.isCompletedExceptionally()) {
                    log.error("failed to getGrpcAddress of {}", peerId.getEndpoint().toString());
                    builder.setState(Metapb.StoreState.Offline);
                    builder.setRaftUrl(peerId.getEndpoint().toString());
                    members.add(builder.build());
                } else {
                    RaftRpcProcessor.GetMemberResponse response = future.get();
                    builder.setState(Metapb.StoreState.Up);
                    builder.setRaftUrl(response.getRaftAddress());
                    builder.setDataPath(response.getDatePath());
                    builder.setGrpcUrl(response.getGrpcAddress());
                    builder.setRestUrl(response.getRestAddress());
                    members.add(builder.build());
                }
            } catch (Exception e) {
                log.error("failed to getGrpcAddress of {}.", peerId.getEndpoint().toString(), e);
                builder.setState(Metapb.StoreState.Offline);
                builder.setRaftUrl(peerId.getEndpoint().toString());
                members.add(builder.build());
            }

        }
        return members;
    }

    public Status changePeerList(String peerList) {
        AtomicReference<Status> result = new AtomicReference<>();
        Configuration newPeers = new Configuration();
        try {
            String[] peers = peerList.split(",", -1);
            if ((peers.length & 1) != 1) {
                throw new PDException(-1, "the number of peer list must be odd.");
            }
            newPeers.parse(peerList);
            CountDownLatch latch = new CountDownLatch(1);
            this.raftNode.changePeers(newPeers, status -> {
                // Use compareAndSet so a late callback does not overwrite a timeout status
                result.compareAndSet(null, status);
                // Refresh inside callback so it fires even if caller already timed out
                // Note: changePeerList() uses Configuration.parse() which only supports
                // plain comma-separated peer addresses with no learner syntax.
                // getLearners() will always be empty here. Learner support is handled
                // in PDService.updatePdRaft() which uses PeerUtil.parseConfig()
                // and supports the /learner suffix.
                if (status != null && status.isOk()) {
                    IpAuthHandler handler = IpAuthHandler.getInstance();
                    if (handler != null) {
                        Set<String> newIps = newPeers.getPeers()
                                                     .stream()
                                                     .map(PeerId::getIp)
                                                     .collect(Collectors.toSet());
                        handler.refresh(newIps);
                        log.info("IpAuthHandler refreshed after peer list change to: {}",
                                 peerList);
                    } else {
                        log.warn("IpAuthHandler not initialized, skipping refresh for "
                                 + "peer list: {}", peerList);
                    }
                }
                latch.countDown();
            });
            // Use 3x configured RPC timeout — bare await() would block forever if
            // the callback is never invoked (e.g. node not started / RPC failure)
            boolean completed = latch.await(3L * config.getRpcTimeout(),
                                            TimeUnit.MILLISECONDS);
            if (!completed && result.get() == null) {
                Status timeoutStatus = new Status(RaftError.EINTERNAL,
                                                  "changePeerList timed out after %d ms",
                                                  3L * config.getRpcTimeout());
                if (!result.compareAndSet(null, timeoutStatus)) {
                    // Callback arrived just before us — keep its result
                    timeoutStatus = null;
                }
                if (timeoutStatus != null) {
                    log.error("changePeerList to {} timed out after {} ms",
                              peerList, 3L * config.getRpcTimeout());
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            result.set(new Status(RaftError.EINTERNAL, "changePeerList interrupted"));
            log.error("changePeerList to {} was interrupted", peerList, e);
        } catch (Exception e) {
            log.error("failed to changePeerList to {},{}", peerList, e);
            result.set(new Status(-1, e.getMessage()));
        }

        return result.get();
    }

    public PeerId waitingForLeader(long timeOut) {
        PeerId leader = getLeader();
        if (leader != null) {
            return leader;
        }

        synchronized (this) {
            leader = getLeader();
            long start = System.currentTimeMillis();
            while ((System.currentTimeMillis() - start < timeOut) && (leader == null)) {
                try {
                    this.wait(1000);
                } catch (InterruptedException e) {
                    log.error("Raft wait for leader exception", e);
                }
                leader = getLeader();
            }
            return leader;
        }

    }

    public Node getRaftNode() {
        return raftNode;
    }

    private boolean peerEquals(PeerId p1, PeerId p2) {
        if (p1 == null && p2 == null) {
            return true;
        }
        if (p1 == null || p2 == null) {
            return false;
        }
        return Objects.equals(p1.getIp(), p2.getIp()) &&
               Objects.equals(p1.getPort(), p2.getPort());
    }

    private Replicator.State getReplicatorState(PeerId peerId) {
        return RaftReflectionUtil.getReplicatorState(this.raftNode, peerId);
    }
}
