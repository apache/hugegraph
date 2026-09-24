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

package org.apache.hugegraph.store.raft;

import java.util.Base64;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hugegraph.store.snapshot.HgSnapshotHandler;
import org.apache.hugegraph.store.util.HgStoreException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Iterator;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.core.StateMachineAdapter;
import com.alipay.sofa.jraft.entity.LeaderChangeContext;
import com.alipay.sofa.jraft.entity.RaftOutter;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.error.RaftException;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotReader;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotWriter;
import com.alipay.sofa.jraft.util.Utils;

/**
 * Raft State Machine
 */

public class HgStoreStateMachine extends StateMachineAdapter {

    private static final Logger LOG = LoggerFactory.getLogger(HgStoreStateMachine.class);
    private final AtomicLong leaderTerm = new AtomicLong(-1);
    private final HgSnapshotHandler snapshotHandler;
    private final List<RaftTaskHandler> taskHandlers;
    private final List<RaftStateListener> stateListeners;
    private final Integer groupId;
    private long committedIndex;

    public HgStoreStateMachine(Integer groupId, HgSnapshotHandler snapshotHandler) {
        this.groupId = groupId;
        this.snapshotHandler = snapshotHandler;
        this.stateListeners = new CopyOnWriteArrayList<>();
        this.taskHandlers = new CopyOnWriteArrayList<>();
    }

    public void addTaskHandler(RaftTaskHandler handler) {
        taskHandlers.add(handler);
    }

    public void addStateListener(RaftStateListener listener) {
        stateListeners.add(listener);
    }

    public boolean isLeader() {
        return this.leaderTerm.get() > 0;
    }

    @Override
    public void onApply(Iterator inter) {

        while (inter.hasNext()) {
            final RaftClosureAdapter done = (RaftClosureAdapter) inter.done();
            try {
                for (RaftTaskHandler taskHandler : taskHandlers) {
                    if (done != null) {
                        // Leader branch, local call
                        if (taskHandler.invoke(groupId, done.op.getOp(), done.op.getReq(),
                                               done.closure)) {
                            done.run(Status.OK());
                            break;
                        }
                    } else {
                        if (taskHandler.invoke(groupId, inter.getData().array(), null)) {
                            break;
                        }
                    }
                }
            } catch (Throwable t) {
                LOG.info("{}", Base64.getEncoder().encode(inter.getData().array()));
                LOG.error("StateMachine{} meet critical error: .", groupId, t);
                if (done != null) {
                    LOG.error("StateMachine meet critical error: op = {} {}.", done.op.getOp(),
                              done.op.getReq());
                    //    done.run(new Status(RaftError.EINTERNAL, t.getMessage()));
                }
            }
            committedIndex = inter.getIndex();

            stateListeners.forEach(listener -> {
                listener.onDataCommitted(committedIndex);
            });
            // Clean up data
            if (done != null) {
                done.clear();
            }
            // Traverse the next item
            inter.next();
        }
    }

    public long getCommittedIndex() {
        return committedIndex;
    }

    public long getLeaderTerm() {
        return leaderTerm.get();
    }

    @Override
    public void onError(final RaftException e) {
        LOG.error("Raft {} StateMachine on error {}", groupId, e);
        Utils.runInThread(() -> {
            stateListeners.forEach(listener -> {
                listener.onError(e);
            });
        });
    }

    @Override
    public void onShutdown() {
        super.onShutdown();
    }

    @Override
    public void onLeaderStart(final long term) {
        this.leaderTerm.set(term);
        super.onLeaderStart(term);
        Utils.runInThread(() -> {
            stateListeners.forEach(listener -> {
                listener.onLeaderStart(term);
            });
        });
        LOG.info("Raft {} becomes leader ", groupId);
    }

    @Override
    public void onLeaderStop(final Status status) {
        Utils.runInThread(() -> {
            stateListeners.forEach(listener -> {
                listener.onLeaderStop(this.leaderTerm.get());
            });
        });
        this.leaderTerm.set(-1);
        super.onLeaderStop(status);
        LOG.info("Raft {} lost leader ", groupId);
    }

    @Override
    public void onStartFollowing(final LeaderChangeContext ctx) {
        super.onStartFollowing(ctx);
        Utils.runInThread(() -> {
            stateListeners.forEach(listener -> {
                listener.onStartFollowing(ctx.getLeaderId(), ctx.getTerm());
            });
        });
        LOG.info("Raft {} start following: {}.", groupId, ctx);
    }

    @Override
    public void onStopFollowing(final LeaderChangeContext ctx) {
        super.onStopFollowing(ctx);
        Utils.runInThread(() -> {
            stateListeners.forEach(listener -> {
                listener.onStopFollowing(ctx.getLeaderId(), ctx.getTerm());
            });
        });
        LOG.info("Raft {} stop following: {}.", groupId, ctx);
    }

    @Override
    public void onConfigurationCommitted(final Configuration conf) {
        stateListeners.forEach(listener -> {
            Utils.runInThread(() -> {
                try {
                    listener.onConfigurationCommitted(conf);
                } catch (Exception e) {
                    LOG.error("Raft {} onConfigurationCommitted {}", groupId, e);
                }
            });
        });
        LOG.info("Raft {} onConfigurationCommitted {}", groupId, conf);
    }

    @Override
    public void onSnapshotSave(final SnapshotWriter writer, final Closure done) {
        Utils.runInThread(() -> {
            try {
                snapshotHandler.onSnapshotSave(writer);
                LOG.info("Raft {} onSnapshotSave success", groupId);
                done.run(Status.OK());
            } catch (HgStoreException e) {
                LOG.error("Raft {} onSnapshotSave failed. {}", groupId, e.toString());
                done.run(new Status(RaftError.EIO, e.toString()));
            }
        });
    }

    @Override
    public boolean onSnapshotLoad(final SnapshotReader reader) {
        try {
            RaftOutter.SnapshotMeta meta = reader.load();
            if (meta != null) {
                this.committedIndex = meta.getLastIncludedIndex();
                LOG.info("onSnapshotLoad committedIndex = {}", this.committedIndex);
            } else {
                LOG.error("onSnapshotLoad failed to get SnapshotMeta");
                return false;
            }
        } catch (Exception e) {
            LOG.error("onSnapshotLoad failed to get SnapshotMeta. {}", e.toString());
            return false;
        }

        if (isLeader()) {
            LOG.warn("Leader is not supposed to load snapshot");
            return false;
        }
        try {
            snapshotHandler.onSnapshotLoad(reader, this.committedIndex);
            LOG.info("Raft {} onSnapshotLoad success", groupId);
            return true;
        } catch (HgStoreException e) {
            LOG.error("Raft {} onSnapshotLoad failed. {}", groupId, e.toString());
            return false;
        }
    }

    public static class RaftClosureAdapter implements RaftClosure {

        private final RaftClosure closure;
        private RaftOperation op;

        public RaftClosureAdapter(RaftOperation op, RaftClosure closure) {
            this.op = op;
            this.closure = closure;
        }

        @Override
        public void run(Status status) {
            closure.run(status);
        }

        public RaftClosure getClosure() {
            return closure;
        }

        public void clear() {
            op = null;
        }
    }

}
