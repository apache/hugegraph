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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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
import com.alipay.sofa.jraft.core.State;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.entity.Task;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.option.NodeOptions;
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

    /**
     * Refresh period of the alive peer count behind {@code hg_raft_alive_peers}. jraft's own
     * step-down timer walks the same peer set every half election timeout, so a one second
     * poll adds nothing next to the work the node already does, and it keeps the gauge fresh
     * well inside any scrape interval.
     */
    private static final long ALIVE_PEERS_REFRESH_MS = 1000L;

    private volatile static RaftEngine instance = new RaftEngine();
    private RaftStateMachine stateMachine;
    private String groupId = "pd_raft";
    private PDConfig.Raft config;
    private RaftGroupService raftGroupService;
    private RpcServer rpcServer;
    private volatile Node raftNode;
    private RaftRpcClient raftRpcClient;
    private volatile int alivePeerCount = -1;
    private ScheduledExecutorService alivePeersRefresher;

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

        // Wire configured rpc timeout into RaftRpcClient so the Bolt transport
        // timeout and the future.get() caller timeout in getLeaderGrpcAddress() are consistent.
        raftRpcClient = new RaftRpcClient();
        RpcOptions rpcOptions = new RpcOptions();
        rpcOptions.setRpcDefaultTimeout(config.getRpcTimeout());
        raftRpcClient.init(rpcOptions);

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
        // TODO: tune RaftOptions for PD (see hugegraph-store PartitionEngine for reference)

        final PeerId serverId = JRaftUtils.getPeerId(config.getAddress());

        rpcServer = createRaftRpcServer(config.getAddress(), initConf.getPeers(),
                                        config.isIpWhitelistEnabled());
        // construct raft group and start raft
        this.raftGroupService =
                new RaftGroupService(groupId, serverId, nodeOptions, rpcServer, true);
        this.raftNode = raftGroupService.start(false);
        startAlivePeersRefresher();
        log.info("RaftEngine start successfully: id = {}, peers list = {}", groupId,
                 nodeOptions.getInitialConf().getPeers());
        return this.raftNode != null;
    }

    /**
     * Create a Raft RPC Server for communication between PDs
     */
    private RpcServer createRaftRpcServer(String raftAddr, List<PeerId> peers,
                                          boolean ipWhitelistEnabled) {
        Endpoint endpoint = JRaftUtils.getEndPoint(raftAddr);
        RpcServer rpcServer = RaftRpcServerFactory.createRaftRpcServer(endpoint);
        configureRaftServerIpWhitelist(peers, rpcServer, ipWhitelistEnabled);
        RaftRpcProcessor.registerProcessor(rpcServer, this);
        rpcServer.init(null);
        return rpcServer;
    }

    private static void configureRaftServerIpWhitelist(List<PeerId> peers, RpcServer rpcServer,
                                                       boolean enabled) {
        if (!enabled) {
            log.info("PD Raft IP whitelist is disabled");
            return;
        }
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
        if (this.alivePeersRefresher != null) {
            this.alivePeersRefresher.shutdownNow();
            try {
                // Best effort: shutdownNow only interrupts, and a refresh parked in
                // listAlivePeers waits on a lock acquire the interrupt does not break,
                // for up to the raft rpc connect timeout per unreachable peer. A refresh
                // that outlives this wait may publish one stale count over the reset
                // below; acceptable while shutDown has no production caller.
                if (!this.alivePeersRefresher.awaitTermination(1, TimeUnit.SECONDS)) {
                    log.warn("Raft alive-peers refresher still running after shutdown; " +
                             "hg_raft_alive_peers may briefly report a stale value");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            this.alivePeersRefresher = null;
        }
        this.alivePeerCount = -1;
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
        Node node = this.raftNode;
        return node != null && node.isLeader(true);
    }

    /**
     * Whether this PD is ready in the sense {@code GET /v1/ready} answers: the raft node
     * is active and sees a leader.
     * <p>
     * A follower only keeps its leader while heartbeats keep arriving inside the election
     * timeout, and a leader only keeps its role while it can reach a quorum. Seeing a leader
     * therefore means this node is part of a quorum from its own point of view, which is the
     * signal a readiness probe needs. Served from the state machine callbacks, not from the
     * raft node, so it never waits on the node lock.
     * <p>
     * Derived from {@link #getRaftStatus()} so {@code hg_raft_has_leader} cannot drift from
     * the endpoint it is documented to mirror.
     */
    public boolean hasLeader() {
        return getRaftStatus().isReady();
    }

    /**
     * Take a view of the local raft state from the volatile copies the state machine
     * callbacks maintain, never from the raft node itself. During an election jraft holds
     * the node lock while it reconnects to peers, so a probe that read the node stalled for
     * the connect timeout instead of answering its 503 promptly. All fields derive from one
     * read of one immutable view, swapped in whole per callback, so they cannot contradict
     * each other.
     * <p>
     * The state reported is the last one a callback announced: leader, follower, error or
     * shutdown, and {@code STATE_UNINITIALIZED} until the first callback runs. jraft emits
     * no callback for candidacy or leadership transfer, so a candidate reads as a follower
     * that sees no leader, which yields the same not-ready answer. The view trails the node
     * by whatever sits in the FSM queue ahead of the announcement, which is the price of
     * never waiting on the node lock.
     * <p>
     * A missing raft node needs no separate branch: before {@link #init} the view still
     * holds its initial {@code STATE_UNINITIALIZED}, and {@code shutDown()} drops the node
     * only after {@code join()} has let the shutdown callback announce
     * {@code STATE_SHUTDOWN}. Neither state is active, so neither reads as ready, and a
     * branch on the node would report an uninitialized PD where the callback said error or
     * shutdown.
     */
    public RaftStatus getRaftStatus() {
        RaftStateMachine.ProbeView view = this.stateMachine.getProbeView();
        return new RaftStatus(view.state.isActive() && view.seesLeader,
                              view.state.name(), State.STATE_LEADER == view.state);
    }

    /**
     * Immutable view of the raft state behind {@code GET /v1/ready}. It carries no cluster
     * addresses: the endpoint is unauthenticated, and the leader's address stays on the
     * authenticated {@code /v1/members}.
     */
    public static final class RaftStatus {

        private final boolean ready;
        private final String state;
        private final boolean localLeader;

        RaftStatus(boolean ready, String state, boolean localLeader) {
            this.ready = ready;
            this.state = state;
            this.localLeader = localLeader;
        }

        public boolean isReady() {
            return this.ready;
        }

        /**
         * @return the jraft node state name, never null
         */
        public String getState() {
            return this.state;
        }

        public boolean isLocalLeader() {
            return this.localLeader;
        }
    }

    /**
     * Number of raft peers, this node included, that the leader has heard from within the
     * leader lease timeout, which jraft derives as 90% of the election timeout by default.
     * Only the leader tracks replication state, so any other node reports -1.
     * <p>
     * This is the value the refresher last published, at most one refresh period old, and -1
     * until the first refresh runs. The count cannot be read here: {@code listAlivePeers}
     * takes the node read lock before it checks for leadership, so a caller that read the
     * node would wait out whoever holds the write lock, which during an election is a
     * per-peer reconnect bounded only by the rpc connect timeout.
     */
    public int getAlivePeerCount() {
        return this.alivePeerCount;
    }

    private void startAlivePeersRefresher() {
        ScheduledExecutorService refresher =
                Executors.newSingleThreadScheduledExecutor(runnable -> {
                    Thread thread = new Thread(runnable, "pd-raft-alive-peers");
                    thread.setDaemon(true);
                    return thread;
                });
        refresher.scheduleWithFixedDelay(this::refreshAlivePeerCount, 0,
                                         ALIVE_PEERS_REFRESH_MS, TimeUnit.MILLISECONDS);
        this.alivePeersRefresher = refresher;
    }

    /**
     * Read the alive peer count from the raft node and publish it for the gauge. Runs on the
     * refresher thread, never on a request thread, because the read can block for as long as
     * an election holds the node write lock. A fixed delay schedule means a blocked refresh
     * only delays the next one, and the gauge keeps reporting the last value meanwhile.
     */
    void refreshAlivePeerCount() {
        Node node = this.raftNode;
        if (node == null || !this.stateMachine.isLeader()) {
            this.alivePeerCount = -1;
            return;
        }
        try {
            this.alivePeerCount = node.listAlivePeers().size();
        } catch (IllegalStateException e) {
            // Lost leadership between the check and the call
            this.alivePeerCount = -1;
        } catch (Throwable e) {
            // scheduleWithFixedDelay cancels every later run if the task throws,
            // and an Error escaping here would leave the gauge serving its last
            // value forever, so this catch has to be wider than Exception.
            log.warn("Failed to refresh the raft alive peer count", e);
            this.alivePeerCount = -1;
        }
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
        Node node = this.raftNode;
        return node == null ? null : node.getLeaderId();
    }

    /**
     * Send a message to the leader to get the grpc address.
     */
    public String getLeaderGrpcAddress() throws ExecutionException, InterruptedException {
        if (isLeader()) {
            return config.getGrpcAddress();
        }

        if (raftNode.getLeaderId() == null) {
            waitingForLeader(config.getRpcTimeout());
        }

        // Cache leader to avoid repeated getLeaderId() calls and guard against
        // waitingForLeader() returning without a leader being elected.
        PeerId leader = raftNode.getLeaderId();
        if (leader == null) {
            throw new ExecutionException(new IllegalStateException("Leader is not ready"));
        }

        RaftRpcProcessor.GetMemberResponse response = null;
        try {
            // TODO: a more complete fix would need a source of truth for the leader's
            // actual grpcAddress rather than deriving it from the local node's port config.
            response = raftRpcClient
                    .getGrpcAddress(leader.getEndpoint().toString())
                    .get(config.getRpcTimeout(), TimeUnit.MILLISECONDS);
            if (response != null && response.getGrpcAddress() != null) {
                return response.getGrpcAddress();
            }
            if (response == null) {
                log.warn("Leader RPC response is null for {}, falling back to derived address",
                         leader);
            } else {
                log.warn("Leader gRPC address field is null in RPC response for {}, "
                         + "falling back to derived address", leader);
            }
        } catch (TimeoutException e) {
            log.warn("Timed out resolving leader gRPC address for {}, falling back to derived "
                     + "address", leader);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            log.warn("Failed to resolve leader gRPC address for {}, falling back to derived "
                     + "address", leader, cause);
        }

        // Best-effort fallback: derive from leader raft endpoint IP + local gRPC port.
        // WARNING: this may be incorrect in clusters where PD nodes use different grpc.port
        // values, a proper fix requires a cluster-wide source of truth for gRPC addresses.
        String derived = leader.getEndpoint().getIp() + ":" + config.getGrpcPort();
        log.info("Using derived leader gRPC address {} - may be incorrect if nodes use different ports",
                 derived);
        return derived;
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
                result.compareAndSet(null, status);
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
            boolean completed = latch.await(3L * config.getRpcTimeout(), TimeUnit.MILLISECONDS);
            if (!completed && result.get() == null) {
                Status timeoutStatus = new Status(RaftError.EINTERNAL,
                                                  "changePeerList timed out after %d ms",
                                                  3L * config.getRpcTimeout());
                if (!result.compareAndSet(null, timeoutStatus)) {
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
                    long remaining = timeOut - (System.currentTimeMillis() - start);
                    this.wait(Math.min(1000, Math.max(0, remaining)));
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
