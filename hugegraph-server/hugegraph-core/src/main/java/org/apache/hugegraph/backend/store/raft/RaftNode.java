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

package org.apache.hugegraph.backend.store.raft;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hugegraph.exception.BackendException;
import org.apache.hugegraph.util.LZ4Util;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.RaftGroupService;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.closure.ReadIndexClosure;
import com.alipay.sofa.jraft.core.Replicator.ReplicatorStateListener;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.entity.Task;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.option.NodeOptions;
import com.alipay.sofa.jraft.rpc.RpcServer;
import com.alipay.sofa.jraft.util.BytesUtil;

public final class RaftNode {

    private static final Logger LOG = Log.logger(RaftNode.class);

    private final RaftContext context;
    private RaftGroupService raftGroupService;
    private final Node node;
    private final StoreStateMachine stateMachine;
    private final AtomicReference<LeaderInfo> leaderInfo;
    private final AtomicBoolean started;
    private final AtomicInteger busyCounter;

    public RaftNode(RaftContext context) {
        this.context = context;
        this.stateMachine = new StoreStateMachine(context);
        try {
            // Start raft node
            this.node = this.initRaftNode();
            LOG.info("Start raft node: {}", this);
        } catch (IOException e) {
            throw new BackendException("Failed to init raft node", e);
        }
        this.node.addReplicatorStateListener(new RaftStateListener());
        this.leaderInfo = new AtomicReference<>(LeaderInfo.NO_LEADER);
        this.started = new AtomicBoolean(false);
        this.busyCounter = new AtomicInteger();
    }

    private RaftContext context() {
        return this.context;
    }

    Node node() {
        assert this.node != null;
        return this.node;
    }

    public PeerId nodeId() {
        return this.node.getNodeId().getPeerId();
    }

    public PeerId leaderId() {
        return this.leaderInfo.get().leaderId;
    }

    public boolean selfIsLeader() {
        return this.leaderInfo.get().selfIsLeader;
    }

    public void onLeaderInfoChange(PeerId leaderId, boolean selfIsLeader) {
        leaderId = leaderId != null ? leaderId.copy() : null;
        this.leaderInfo.set(new LeaderInfo(leaderId, selfIsLeader));
    }

    public void shutdown() {
        LOG.info("Shutdown raft node: {}", this);
        this.node.shutdown();

        if (this.raftGroupService != null) {
            this.raftGroupService.shutdown();
            try {
                this.raftGroupService.join();
            } catch (final InterruptedException e) {
                throw new RaftException(
                        "Interrupted while shutdown raftGroupService");
            }
        }
    }

    public RaftClosure<?> snapshot() {
        RaftClosure<?> future = new RaftClosure<>();
        try {
            this.node().snapshot(future);
            return future;
        } catch (Throwable e) {
            throw new BackendException("Failed to create snapshot", e);
        }
    }

    public void readIndex(byte[] reqCtx, ReadIndexClosure done) {
        this.node.readIndex(reqCtx, done);
    }

    public <T> T submitAndWait(StoreCommand command, RaftStoreClosure future) {
        // Submit command to raft node
        this.submitCommand(command, future);

        try {
            /*
             * Here wait for the command to complete:
             * 1.If on the leader, wait for the logs has been committed.
             * 2.If on the follower, request command will be forwarded to the
             *   leader, actually it has waited in forwardToLeader().
             */
            @SuppressWarnings("unchecked")
            T result = (T) future.waitFinished();
            return result;
        } catch (Throwable e) {
            throw new BackendException("Failed to wait store command %s",
                                       e, command);
        }
    }

    private void submitCommand(StoreCommand command, RaftStoreClosure future) {
        // Wait leader elected
        LeaderInfo leaderInfo = this.waitLeaderElected(
                RaftContext.WAIT_LEADER_TIMEOUT);
        // If myself is not leader, forward to the leader
        if (!leaderInfo.selfIsLeader) {
            this.context.rpcForwarder().forwardToLeader(leaderInfo.leaderId,
                                                        command, future);
            return;
        }

        // Sleep a while when raft node is busy
        this.waitIfBusy();

        Task task = new Task();
        // Compress data, note compress() will return a BytesBuffer
        ByteBuffer buffer = LZ4Util.compress(command.data(),
                                             RaftContext.BLOCK_SIZE)
                                   .forReadWritten()
                                   .asByteBuffer();
        LOG.debug("Submit to raft node '{}', the compressed bytes of command " +
                  "{} is {}", this.node, command.action(), buffer.limit());
        task.setData(buffer);
        task.setDone(future);
        this.node.apply(task);
    }

    LeaderInfo waitLeaderElected(int timeout) {
        String group = this.context.group();
        LeaderInfo leaderInfo = this.leaderInfo.get();
        if (leaderInfo.leaderId != null) {
            return leaderInfo;
        }
        LOG.info("Waiting for raft group '{}' leader elected", group);
        long beginTime = System.currentTimeMillis();
        while (leaderInfo.leaderId == null) {
            try {
                Thread.sleep(RaftContext.POLL_INTERVAL);
            } catch (InterruptedException e) {
                throw new BackendException("Interrupted while waiting for " +
                                           "raft group '%s' election", group, e);
            }
            long consumedTime = System.currentTimeMillis() - beginTime;
            if (timeout > 0 && consumedTime >= timeout) {
                throw new BackendException(
                        "Waiting for raft group '%s' election timeout(%sms)",
                        group, consumedTime);
            }
            leaderInfo = this.leaderInfo.get();
            assert leaderInfo != null;
        }
        LOG.info("Waited for raft group '{}' leader elected successfully", group);
        return leaderInfo;
    }

    void waitRaftLogSynced(int timeout) {
        String group = this.context.group();
        LOG.info("Waiting for raft group '{}' log synced", group);
        long beginTime = System.currentTimeMillis();
        while (!this.started.get()) {
            this.node.readIndex(BytesUtil.EMPTY_BYTES, new ReadIndexClosure() {
                @Override
                public void run(Status status, long index, byte[] reqCtx) {
                    RaftNode.this.started.set(status.isOk());
                }
            });
            try {
                Thread.sleep(RaftContext.POLL_INTERVAL);
            } catch (InterruptedException e) {
                throw new BackendException("Interrupted while waiting for " +
                                           "raft group '%s' log synced", group, e);
            }
            long consumedTime = System.currentTimeMillis() - beginTime;
            if (timeout > 0 && consumedTime >= timeout) {
                throw new BackendException(
                        "Waiting for raft group '%s' log synced timeout(%sms)",
                        group, consumedTime);
            }
        }
        LOG.info("Waited for raft group '{}' log synced successfully", group);
    }

    private void waitIfBusy() {
        int counter = this.busyCounter.get();
        if (counter <= 0) {
            return;
        }
        // It may lead many thread sleep, but this is exactly what I want
        int delta = RaftContext.BUSY_MAX_SLEEP_FACTOR -
                    RaftContext.BUSY_MIN_SLEEP_FACTOR;
        Random random = new Random();
        int timeout = random.nextInt(delta) +
                      RaftContext.BUSY_MIN_SLEEP_FACTOR;
        int time = counter * timeout;
        try {
            Thread.sleep(time);
        } catch (InterruptedException e) {
            // Throw busy exception if the request is timeout
            throw new BackendException("The raft backend store is busy", e);
        } finally {
            if (this.busyCounter.get() > 0) {
                synchronized (this) {
                    if (this.busyCounter.get() > 0) {
                        counter = this.busyCounter.decrementAndGet();
                        LOG.info("Decrease busy counter: [{}]", counter);
                    }
                }
            }
        }
    }

    private Node initRaftNode() throws IOException {
        NodeOptions nodeOptions = this.context.nodeOptions();
        nodeOptions.setFsm(this.stateMachine);
        /*
         * TODO: the groupId is same as graph name now, when support sharding,
         *       groupId needs to be bound to shard Id
         */
        String groupId = this.context.group();
        PeerId endpoint = this.context.endpoint();

        /*
         * Create RaftGroupService with shared rpc-server, then start raft node
         * TODO: don't create + hold RaftGroupService and just share rpc-server
         *       and create Node by RaftServiceFactory.createAndInitRaftNode()
         */
        RpcServer rpcServer = this.context.rpcServer();
        LOG.debug("Start raft node with endpoint '{}', initial conf [{}]",
                  endpoint, nodeOptions.getInitialConf());
        this.raftGroupService = new RaftGroupService(groupId, endpoint,
                                                     nodeOptions,
                                                     rpcServer, true);
        return this.raftGroupService.start(false);
    }

    @Override
    public String toString() {
        return String.format("[%s-%s]", this.context.group(), this.nodeId());
    }

    protected final class RaftStateListener implements ReplicatorStateListener {

        private volatile long lastPrintTime;

        public RaftStateListener() {
            this.lastPrintTime = 0L;
        }

        @Override
        public void onCreated(PeerId peer) {
            LOG.info("The node {} replicator has created", peer);
        }

        @Override
        public void onDestroyed(PeerId peer) {
            LOG.warn("Replicator '{}' is ready to go offline", peer);
        }

        @Override
        public void onError(PeerId peer, Status status) {
            long now = System.currentTimeMillis();
            long interval = now - this.lastPrintTime;
            if (interval >= RaftContext.LOG_WARN_INTERVAL) {
                LOG.warn("Replicator meet error: {}", status);
                this.lastPrintTime = now;
            }
            if (this.isWriteBufferOverflow(status)) {
                // Increment busy counter
                int count = RaftNode.this.busyCounter.incrementAndGet();
                LOG.info("Increase busy counter due to overflow: [{}]", count);
            }
        }

        // NOTE: Jraft itself doesn't have this callback, it's added by us
        public void onBusy(PeerId peer, Status status) {
            /*
             * If follower is busy then increase busy counter,
             * it will lead to submit thread wait more time
             */
            int count = RaftNode.this.busyCounter.incrementAndGet();
            LOG.info("Increase busy counter: [{}]", count);
        }

        private boolean isWriteBufferOverflow(Status status) {
            String expectMsg = "maybe write overflow";
            return RaftError.EINTERNAL == status.getRaftError() &&
                   status.getErrorMsg() != null &&
                   status.getErrorMsg().contains(expectMsg);
        }

        /**
         * Maybe useful in the future
         */
        @SuppressWarnings("unused")
        private boolean isRpcTimeout(Status status) {
            String expectMsg = "Invoke timeout";
            return RaftError.EINTERNAL == status.getRaftError() &&
                   status.getErrorMsg() != null &&
                   status.getErrorMsg().contains(expectMsg);
        }
    }

    /**
     * Jraft Node.getLeaderId() and Node.isLeader() is not always consistent,
     * We define this class to manage leader info by ourselves
     */
    private static class LeaderInfo {

        private static final LeaderInfo NO_LEADER = new LeaderInfo(null, false);

        private final PeerId leaderId;
        private final boolean selfIsLeader;

        public LeaderInfo(PeerId leaderId, boolean selfIsLeader) {
            this.leaderId = leaderId;
            this.selfIsLeader = selfIsLeader;
        }
    }
}
