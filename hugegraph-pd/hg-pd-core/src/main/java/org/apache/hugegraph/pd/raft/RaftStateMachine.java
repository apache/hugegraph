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
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.Checksum;

import org.apache.commons.io.FileUtils;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.grpc.Pdpb;
import org.springframework.util.CollectionUtils;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Iterator;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.core.StateMachineAdapter;
import com.alipay.sofa.jraft.entity.LeaderChangeContext;
import com.alipay.sofa.jraft.entity.LocalFileMetaOutter;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.error.RaftException;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotReader;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotWriter;
import com.alipay.sofa.jraft.util.CRC64;
import com.alipay.sofa.jraft.util.Utils;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RaftStateMachine extends StateMachineAdapter {

    private static final String SNAPSHOT_DIR_NAME = "snapshot";
    private static final String SNAPSHOT_ARCHIVE_NAME = "snapshot.zip";
    private final AtomicLong leaderTerm = new AtomicLong(-1);
    private final List<RaftTaskHandler> taskHandlers;
    private final List<RaftStateListener> stateListeners;

    public RaftStateMachine() {
        this.taskHandlers = new CopyOnWriteArrayList<>();
        this.stateListeners = new CopyOnWriteArrayList<>();
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
            final RaftClosureAdapter done = (RaftClosureAdapter) iter.done();
            try {
                KVOperation kvOp;
                if (done != null) {
                    kvOp = done.op;
                } else {
                    kvOp = KVOperation.fromByteArray(iter.getData().array());
                }
                for (RaftTaskHandler taskHandler : taskHandlers) {
                    taskHandler.invoke(kvOp, done);
                }
                if (done != null) {
                    done.run(Status.OK());
                }
            } catch (Throwable t) {
                log.error("StateMachine encountered critical error", t);
                // JRaft completes the failed and remaining closures with the state machine error.
                iter.setErrorAndRollback(1, new Status(RaftError.ESTATEMACHINE, t.getMessage()));
                return;
            }
            iter.next();
        }
    }

    @Override
    public void onError(final RaftException e) {
        log.error("Raft StateMachine encountered an error", e);
    }

    @Override
    public void onShutdown() {
        super.onShutdown();
    }

    @Override
    public void onLeaderStart(final long term) {
        this.leaderTerm.set(term);
        super.onLeaderStart(term);

        log.info("Raft becomes leader");
        Utils.runInThread(() -> {
            if (!CollectionUtils.isEmpty(stateListeners)) {
                stateListeners.forEach(RaftStateListener::onRaftLeaderChanged);
            }
        });
    }

    @Override
    public void onLeaderStop(final Status status) {
        this.leaderTerm.set(-1);
        super.onLeaderStop(status);
        log.info("Raft  lost leader ");
    }

    @Override
    public void onStartFollowing(final LeaderChangeContext ctx) {
        super.onStartFollowing(ctx);
        Utils.runInThread(() -> {
            if (!CollectionUtils.isEmpty(stateListeners)) {
                stateListeners.forEach(RaftStateListener::onRaftLeaderChanged);
            }
        });
    }

    @Override
    public void onStopFollowing(final LeaderChangeContext ctx) {
        super.onStopFollowing(ctx);
    }

    @Override
    public void onConfigurationCommitted(final Configuration conf) {
        log.info("Raft  onConfigurationCommitted {}", conf);
    }

    @Override
    public void onSnapshotSave(final SnapshotWriter writer, final Closure done) {

        String snapshotDir = writer.getPath() + File.separator + SNAPSHOT_DIR_NAME;
        try {
            FileUtils.deleteDirectory(new File(snapshotDir));
            FileUtils.forceMkdir(new File(snapshotDir));
        } catch (IOException e) {
            log.error("Failed to create snapshot directory {}", snapshotDir);
            done.run(new Status(RaftError.EIO, e.toString()));
            return;
        }

        CountDownLatch latch = new CountDownLatch(taskHandlers.size());
        for (RaftTaskHandler taskHandler : taskHandlers) {
            Utils.runInThread(() -> {
                try {
                    KVOperation op = KVOperation.createSaveSnapshot(snapshotDir);
                    taskHandler.invoke(op, null);
                    log.info("Raft onSnapshotSave success");
                    latch.countDown();
                } catch (PDException e) {
                    log.error("Raft onSnapshotSave failed. {}", e.toString());
                    done.run(new Status(RaftError.EIO, e.toString()));
                }
            });
        }
        try {
            latch.await();
        } catch (InterruptedException e) {
            log.error("Raft onSnapshotSave failed. {}", e.toString());
            done.run(new Status(RaftError.EIO, e.toString()));
            return;
        }

        // compress
        try {
            compressSnapshot(writer);
            FileUtils.deleteDirectory(new File(snapshotDir));
        } catch (Exception e) {
            log.error("Failed to delete snapshot directory {}, {}", snapshotDir, e.toString());
            done.run(new Status(RaftError.EIO, e.toString()));
            return;
        }
        done.run(Status.OK());
    }

    @Override
    public boolean onSnapshotLoad(final SnapshotReader reader) {
        if (isLeader()) {
            log.warn("Leader is not supposed to load snapshot");
            return false;
        }
        String snapshotDir = reader.getPath() + File.separator + SNAPSHOT_DIR_NAME;
        String snapshotArchive = reader.getPath() + File.separator + SNAPSHOT_ARCHIVE_NAME;
        // 2. decompress snapshot archive
        try {
            decompressSnapshot(reader);
        } catch (PDException e) {
            log.error("Failed to delete snapshot directory {}, {}", snapshotDir, e.toString());
            return true;
        }

        CountDownLatch latch = new CountDownLatch(taskHandlers.size());
        for (RaftTaskHandler taskHandler : taskHandlers) {
            try {
                KVOperation op = KVOperation.createLoadSnapshot(snapshotDir);
                taskHandler.invoke(op, null);
                log.info("Raft onSnapshotLoad success");
                latch.countDown();
            } catch (PDException e) {
                log.error("Raft onSnapshotLoad failed. {}", e.toString());
                return false;
            }
        }
        try {
            latch.await();
        } catch (InterruptedException e) {
            log.error("Raft onSnapshotSave failed. {}", e.toString());
            return false;
        }

        try {
            // TODO: remove file from meta
            FileUtils.deleteDirectory(new File(snapshotDir));
            File file = new File(snapshotArchive);
            if (file.exists()) {
                FileUtils.forceDelete(file);
            }
        } catch (IOException e) {
            log.error("Failed to delete snapshot directory {} and file {}", snapshotDir,
                      snapshotArchive);
            return false;
        }

        return true;
    }

    private void compressSnapshot(final SnapshotWriter writer) throws PDException {
        final Checksum checksum = new CRC64();
        final String snapshotArchive = writer.getPath() + File.separator + SNAPSHOT_ARCHIVE_NAME;
        try {
            ZipUtils.compress(writer.getPath(), SNAPSHOT_DIR_NAME, snapshotArchive, checksum);
            LocalFileMetaOutter.LocalFileMeta.Builder metaBuild =
                    LocalFileMetaOutter.LocalFileMeta.newBuilder();
            metaBuild.setChecksum(Long.toHexString(checksum.getValue()));
            if (!writer.addFile(SNAPSHOT_ARCHIVE_NAME, metaBuild.build())) {
                throw new PDException(Pdpb.ErrorType.ROCKSDB_SAVE_SNAPSHOT_ERROR_VALUE,
                                      "failed to add file to LocalFileMeta");
            }
        } catch (IOException e) {
            throw new PDException(Pdpb.ErrorType.ROCKSDB_SAVE_SNAPSHOT_ERROR_VALUE, e);
        }
    }

    private void decompressSnapshot(final SnapshotReader reader) throws PDException {
        final LocalFileMetaOutter.LocalFileMeta meta =
                (LocalFileMetaOutter.LocalFileMeta) reader.getFileMeta(SNAPSHOT_ARCHIVE_NAME);
        final Checksum checksum = new CRC64();
        final String snapshotArchive = reader.getPath() + File.separator + SNAPSHOT_ARCHIVE_NAME;
        try {
            ZipUtils.decompress(snapshotArchive, new File(reader.getPath()), checksum);
            if (meta.hasChecksum()) {
                if (!meta.getChecksum().equals(Long.toHexString(checksum.getValue()))) {
                    throw new PDException(Pdpb.ErrorType.ROCKSDB_LOAD_SNAPSHOT_ERROR_VALUE,
                                          "Snapshot checksum failed");
                }
            }
        } catch (IOException e) {
            throw new PDException(Pdpb.ErrorType.ROCKSDB_LOAD_SNAPSHOT_ERROR_VALUE, e);
        }
    }

    public static class RaftClosureAdapter implements KVStoreClosure {

        private final KVOperation op;
        private final KVStoreClosure closure;

        public RaftClosureAdapter(KVOperation op, KVStoreClosure closure) {
            this.op = op;
            this.closure = closure;
        }

        public KVStoreClosure getClosure() {
            return closure;
        }

        @Override
        public void run(Status status) {
            closure.run(status);
        }

        @Override
        public Pdpb.Error getError() {
            return null;
        }

        @Override
        public void setError(Pdpb.Error error) {

        }

        @Override
        public Object getData() {
            return null;
        }

        @Override
        public void setData(Object data) {

        }
    }
}
