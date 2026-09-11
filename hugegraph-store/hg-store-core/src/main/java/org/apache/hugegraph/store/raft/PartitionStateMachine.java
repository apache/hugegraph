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
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hugegraph.store.HgStoreEngine;
import org.apache.hugegraph.store.snapshot.SnapshotHandler;
import org.apache.hugegraph.store.util.HgStoreException;

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

import lombok.extern.slf4j.Slf4j;

/**
 * Raft state machine
 */
@Slf4j
public class PartitionStateMachine extends StateMachineAdapter {

    private final AtomicLong leaderTerm = new AtomicLong(-1);
    private final SnapshotHandler snapshotHandler;
    private final Integer groupId;
    private final List<RaftTaskHandler> taskHandlers;
    private final List<RaftStateListener> stateListeners;

    private final Lock lock = new ReentrantLock();
    private long committedIndex;

    public PartitionStateMachine(Integer groupId, SnapshotHandler snapshotHandler) {
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
    public void onApply(Iterator iter) {
        while (iter.hasNext()) {
            final DefaultRaftClosure done = (DefaultRaftClosure) iter.done();
            try {
                for (RaftTaskHandler handler : taskHandlers) {
                    if (done != null) {
                        // Leader branch, call locally
                        RaftOperation operation = done.getOperation();
                        if (handler.invoke(groupId, operation.getOp(), operation.getReq(),
                                           done.getClosure())) {
                            done.run(Status.OK());
                            break;
                        }
                    } else {
                        if (handler.invoke(groupId, iter.getData().array(), null)) {
                            break;
                        }
                    }
                }
            } catch (Throwable t) {
                log.info("{}", Base64.getEncoder().encode(iter.getData().array()));
                log.error(String.format("StateMachine %s meet critical error:", groupId), t);
                if (done != null) {
                    log.error("StateMachine meet critical error: op = {} {}.",
                              done.getOperation().getOp(),
                              done.getOperation().getReq());
                }
            }
            committedIndex = iter.getIndex();
            stateListeners.forEach(listener -> listener.onDataCommitted(committedIndex));
            // clear data
            if (done != null) {
                done.clear();
            }
            // next entry
            iter.next();
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
        log.error(String.format("Raft %s StateMachine on error {}", groupId), e);
        Utils.runInThread(() -> {
            stateListeners.forEach(listener -> listener.onError(e));
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
        Utils.runInThread(() -> stateListeners.forEach(l -> l.onLeaderStart(term)));
        log.info("Raft {} becomes leader ", groupId);
    }

    @Override
    public void onLeaderStop(final Status status) {
        Utils.runInThread(() -> stateListeners.forEach(l -> l.onLeaderStop(this.leaderTerm.get())));
        this.leaderTerm.set(-1);
        super.onLeaderStop(status);
        log.info("Raft {} lost leader ", groupId);
    }

    @Override
    public void onStartFollowing(final LeaderChangeContext ctx) {
        super.onStartFollowing(ctx);
        Utils.runInThread(
                () -> stateListeners.forEach(
                        l -> l.onStartFollowing(ctx.getLeaderId(), ctx.getTerm())));
        log.info("Raft {} start following: {}.", groupId, ctx);
    }

    @Override
    public void onStopFollowing(final LeaderChangeContext ctx) {
        super.onStopFollowing(ctx);
        Utils.runInThread(
                () -> stateListeners.forEach(
                        l -> l.onStopFollowing(ctx.getLeaderId(), ctx.getTerm())));
        if (!ctx.getStatus().getRaftError().equals(RaftError.ESHUTDOWN)) {
            log.info("Raft {} stop following: {}.", groupId, ctx);
        }
    }

    @Override
    public void onConfigurationCommitted(final Configuration conf) {
        stateListeners.forEach(listener -> {
            Utils.runInThread(() -> {
                try {
                    listener.onConfigurationCommitted(conf);
                } catch (Exception e) {
                    log.error("Raft {} onConfigurationCommitted {}", groupId, e);
                }
            });
        });
        log.info("Raft {} onConfigurationCommitted {}", groupId, conf);
    }

    @Override
    public void onSnapshotSave(final SnapshotWriter writer, final Closure done) {
        HgStoreEngine.getUninterruptibleJobs().execute(() -> {
            try {
                lock.lock();
                snapshotHandler.onSnapshotSave(writer);
                log.info("Raft {} onSnapshotSave success", groupId);
                done.run(Status.OK());
            } catch (HgStoreException e) {
                log.error(String.format("Raft %s onSnapshotSave failed. {}", groupId), e);
                // A busy compaction-range lock is transient: jRaft's snapshot scheduler
                // retries independently, so report EBUSY rather than EIO to avoid
                // escalating to reportError()/restartRaftNode() (see SnapshotExecutorImpl
                // #onSnapshotSaveDone, which only escalates on EIO).
                RaftError raftError = e.getCode() == HgStoreException.EC_RKDB_SNAPSHOT_SAVE_BUSY_FAIL ?
                                       RaftError.EBUSY : RaftError.EIO;
                done.run(new Status(raftError, e.toString()));
            } finally {
                lock.unlock();
            }
        });
    }

    @Override
    public boolean onSnapshotLoad(final SnapshotReader reader) {
        try {
            RaftOutter.SnapshotMeta meta = reader.load();
            if (meta != null) {
                this.committedIndex = meta.getLastIncludedIndex();
                log.info("onSnapshotLoad committedIndex = {}", this.committedIndex);
            } else {
                log.error("onSnapshotLoad failed to get SnapshotMeta");
                return false;
            }
        } catch (Exception e) {
            log.error("onSnapshotLoad failed to get SnapshotMeta.", e);
            return false;
        }

        if (isLeader()) {
            log.warn("Leader is not supposed to load snapshot");
            return false;
        }
        try {
            snapshotHandler.onSnapshotLoad(reader, this.committedIndex);
            log.info("Raft {} onSnapshotLoad success", groupId);
            return true;
        } catch (HgStoreException e) {
            log.error(String.format("Raft %s  onSnapshotLoad failed. ", groupId), e);
            return false;
        }
    }

}
