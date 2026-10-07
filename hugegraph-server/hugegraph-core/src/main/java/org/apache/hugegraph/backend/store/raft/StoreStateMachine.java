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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

import org.apache.hugegraph.exception.BackendException;
import org.apache.hugegraph.serializer.BytesBuffer;
import org.apache.hugegraph.backend.store.BackendMutation;
import org.apache.hugegraph.backend.store.BackendStore;
import org.apache.hugegraph.backend.store.raft.RaftBackendStore.IncrCounter;
import org.apache.hugegraph.backend.store.raft.rpc.RaftRequests.StoreAction;
import org.apache.hugegraph.backend.store.raft.rpc.RaftRequests.StoreType;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.LZ4Util;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Iterator;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.core.StateMachineAdapter;
import com.alipay.sofa.jraft.entity.LeaderChangeContext;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.error.RaftException;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotReader;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotWriter;

public final class StoreStateMachine extends StateMachineAdapter {

    private static final Logger LOG = Log.logger(StoreStateMachine.class);

    private final RaftContext context;
    private final StoreSnapshotFile snapshotFile;

    public StoreStateMachine(RaftContext context) {
        this.context = context;
        this.snapshotFile = new StoreSnapshotFile(context.stores());
    }

    private BackendStore store(StoreType type) {
        return this.context.originStore(type);
    }

    private RaftNode node() {
        return this.context.node();
    }

    @Override
    public void onApply(Iterator iter) {
        LOG.debug("Node role: {}", this.node().selfIsLeader() ?
                                   "leader" : "follower");
        List<Future<?>> futures = new ArrayList<>(64);
        try {
            // Apply all the logs
            while (iter.hasNext()) {
                RaftStoreClosure closure = (RaftStoreClosure) iter.done();
                if (closure != null) {
                    futures.add(this.onApplyLeader(closure));
                } else {
                    futures.add(this.onApplyFollower(iter.getData()));
                }
                iter.next();
            }

            // Wait for all tasks finished
            for (Future<?> future : futures) {
                future.get();
            }
        } catch (Throwable e) {
            String title = "StateMachine occurred critical error";
            LOG.error("{}", title, e);
            Status status = new Status(RaftError.ESTATEMACHINE,
                                       "%s: %s", title, e.getMessage());
            // Will cause current node inactive
            // TODO: rollback to correct index
            iter.setErrorAndRollback(1L, status);
        }
    }

    private Future<?> onApplyLeader(RaftStoreClosure closure) {
        // Leader just take the command out from the closure
        StoreCommand command = closure.command();
        BytesBuffer buffer = BytesBuffer.wrap(command.data());
        // The first two bytes are StoreType and StoreAction
        StoreType type = StoreType.valueOf(buffer.read());
        StoreAction action = StoreAction.valueOf(buffer.read());
        boolean forwarded = command.forwarded();
        // Let the producer thread to handle it, and wait for it
        CompletableFuture<Object> future = new CompletableFuture<>();
        closure.complete(Status.OK(), () -> {
            Object result;
            try {
                result = this.applyCommand(type, action, buffer, forwarded);
            } catch (Throwable e) {
                future.completeExceptionally(e);
                throw e;
            }
            future.complete(result);
            return result;
        });
        return future;
    }

    private Future<?> onApplyFollower(ByteBuffer data) {
        // Follower need to read mutation data
        byte[] bytes = data.array();
        // Let the backend thread do it directly
        return this.context.backendExecutor().submit(() -> {
            BytesBuffer buffer = LZ4Util.decompress(bytes,
                                                    RaftContext.BLOCK_SIZE);
            buffer.forReadWritten();
            StoreType type = StoreType.valueOf(buffer.read());
            StoreAction action = StoreAction.valueOf(buffer.read());
            try {
                return this.applyCommand(type, action, buffer, false);
            } catch (Throwable e) {
                String title = "Failed to execute backend command";
                LOG.error("{}: {}", title, action, e);
                throw new BackendException(title, e);
            }
        });
    }

    private Object applyCommand(StoreType type, StoreAction action,
                                BytesBuffer buffer, boolean forwarded) {
        E.checkState(type != StoreType.ALL,
                     "Can't apply command for all store at one time");
        BackendStore store = this.store(type);
        switch (action) {
            case CLEAR:
                boolean clearSpace = buffer.read() > 0;
                store.clear(clearSpace);
                this.context.clearCache();
                break;
            case TRUNCATE:
                store.truncate();
                this.context.clearCache();
                break;
            case SNAPSHOT:
                assert store == null;
                return this.node().snapshot();
            case BEGIN_TX:
                store.beginTx();
                break;
            case COMMIT_TX:
                List<BackendMutation> mutations = StoreSerializer.readMutations(
                        buffer);
                // RaftBackendStore doesn't write raft log for beginTx
                store.beginTx();
                for (BackendMutation mutation : mutations) {
                    store.mutate(mutation);
                    this.context.updateCacheIfNeeded(mutation, forwarded);
                }
                store.commitTx();
                break;
            case ROLLBACK_TX:
                store.rollbackTx();
                break;
            case INCR_COUNTER:
                // Do increase counter
                IncrCounter counter = StoreSerializer.readIncrCounter(buffer);
                store.increaseCounter(counter.type(), counter.increment());
                break;
            default:
                throw new IllegalArgumentException("Invalid action " + action);
        }
        return null;
    }

    @Override
    public void onSnapshotSave(SnapshotWriter writer, Closure done) {
        LOG.info("The node {} start snapshot saving", this.node().nodeId());
        this.snapshotFile.save(writer, done, this.context.snapshotExecutor());
    }

    @Override
    public boolean onSnapshotLoad(SnapshotReader reader) {
        if (this.node() != null && this.node().selfIsLeader()) {
            LOG.warn("Leader is not supposed to load snapshot.");
            return false;
        }
        /*
         * Snapshot load occurred in RaftNode constructor, specifically at step
         * `this.node = this.initRaftNode()`, at this time the NodeImpl is null
         * in RaftNode so we can't call `this.node().nodeId()`
         */
        LOG.info("The node {} start snapshot loading", this.context.endpoint());
        return this.snapshotFile.load(reader);
    }

    @Override
    public void onLeaderStart(long term) {
        LOG.info("The node {} become to leader", this.context.endpoint());
        this.node().onLeaderInfoChange(this.context.endpoint(), true);
        super.onLeaderStart(term);
    }

    @Override
    public void onLeaderStop(Status status) {
        LOG.info("The node {} abdicated from leader", this.node().nodeId());
        this.node().onLeaderInfoChange(null, false);
        super.onLeaderStop(status);
    }

    @Override
    public void onStartFollowing(LeaderChangeContext ctx) {
        LOG.info("The node {} become to follower", this.node().nodeId());
        this.node().onLeaderInfoChange(ctx.getLeaderId(), false);
        super.onStartFollowing(ctx);
    }

    @Override
    public void onStopFollowing(LeaderChangeContext ctx) {
        LOG.info("The node {} abdicated from follower", this.node().nodeId());
        this.node().onLeaderInfoChange(null, false);
        super.onStopFollowing(ctx);
    }

    @Override
    public void onConfigurationCommitted(Configuration conf) {
        super.onConfigurationCommitted(conf);
    }

    @Override
    public void onError(final RaftException e) {
        LOG.error("Raft error: {}", e.getMessage(), e);
    }
}
