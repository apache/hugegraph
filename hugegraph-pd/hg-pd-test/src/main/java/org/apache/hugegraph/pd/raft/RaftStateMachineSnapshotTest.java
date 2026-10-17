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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.io.FileUtils;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.grpc.Pdpb;
import org.apache.hugegraph.pd.service.MetadataService;
import org.apache.hugegraph.testutil.Whitebox;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.entity.LocalFileMetaOutter.LocalFileMeta;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotReader;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotWriter;
import com.google.protobuf.Message;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * jraft calls {@code onSnapshotSave} on the state machine thread with the snapshot index
 * set to the last applied index, and applies the next entry once it returns. The checkpoint
 * must therefore be taken before the call returns: a checkpoint taken later on a job thread
 * holds entries past the snapshot index, which a node installing it applies a second time.
 * <p>
 * {@code onSnapshotLoad} must return false for an archive it did not install: jraft moves
 * its applied index to the snapshot index when the load returns true.
 */
public class RaftStateMachineSnapshotTest {

    private ThreadPoolExecutor originalJobs;
    private ThreadPoolExecutor jobs;
    private File snapshotPath;
    private SnapshotWriter writer;

    @Before
    public void setUp() throws IOException {
        this.originalJobs = MetadataService.getUninterruptibleJobs();
        this.jobs = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
                                           new LinkedBlockingQueue<>());
        Whitebox.setInternalState(MetadataService.class, "uninterruptibleJobs", this.jobs);

        this.snapshotPath = Files.createTempDirectory("pd-snapshot-save").toFile();
        this.writer = mock(SnapshotWriter.class);
        when(this.writer.getPath()).thenReturn(this.snapshotPath.getAbsolutePath());
        when(this.writer.addFile(anyString(), any(Message.class))).thenReturn(true);
    }

    @After
    public void tearDown() throws IOException {
        Whitebox.setInternalState(MetadataService.class, "uninterruptibleJobs",
                                  this.originalJobs);
        this.jobs.shutdownNow();
        FileUtils.deleteDirectory(this.snapshotPath);
    }

    @Test
    public void testCheckpointIsTakenBeforeTheNextEntryIsApplied() throws Exception {
        AtomicLong applied = new AtomicLong(10L);
        AtomicReference<Thread> checkpointThread = new AtomicReference<>();
        AtomicLong checkpointed = new AtomicLong(-1L);
        RaftStateMachine machine = new RaftStateMachine();
        machine.addTaskHandler((op, response) -> {
            if (op.getOp() == KVOperation.SAVE_SNAPSHOT) {
                checkpointThread.set(Thread.currentThread());
                checkpointed.set(applied.get());
                writeCheckpoint((String) op.getAttach(), applied.get());
            }
            return false;
        });
        RecordingClosure done = new RecordingClosure();

        machine.onSnapshotSave(this.writer, done);
        // What the state machine thread does next: apply the entry after the snapshot
        applied.incrementAndGet();

        Assert.assertSame(Thread.currentThread(), checkpointThread.get());
        Assert.assertEquals(10L, checkpointed.get());
        Assert.assertTrue(done.await());
        Assert.assertEquals(1, done.statuses.size());
        Assert.assertTrue(done.statuses.get(0).isOk());
        Assert.assertTrue(new File(this.snapshotPath, "snapshot.zip").isFile());
    }

    @Test
    public void testFailedCheckpointCompletesTheSnapshotOnceWithAnError() throws Exception {
        RaftStateMachine machine = new RaftStateMachine();
        machine.addTaskHandler((op, response) -> {
            if (op.getOp() == KVOperation.SAVE_SNAPSHOT) {
                throw new PDException(Pdpb.ErrorType.ROCKSDB_SAVE_SNAPSHOT_ERROR_VALUE,
                                      "checkpoint failed");
            }
            return false;
        });
        RecordingClosure done = new RecordingClosure();

        machine.onSnapshotSave(this.writer, done);

        Assert.assertTrue(done.await());
        // Drain the job thread so a second completion, if any, has run
        this.jobs.submit(() -> { }).get(10, TimeUnit.SECONDS);
        Assert.assertEquals(1, done.statuses.size());
        Assert.assertEquals(RaftError.EIO, done.statuses.get(0).getRaftError());
        Assert.assertFalse(new File(this.snapshotPath, "snapshot.zip").exists());
    }

    @Test
    public void testSnapshotWithAWrongChecksumIsNotReportedAsInstalled() throws Exception {
        AtomicLong loads = new AtomicLong();
        RaftStateMachine machine = this.machineThatCountsLoads(loads);
        LocalFileMeta meta = this.saveSnapshot(machine);
        LocalFileMeta wrong = LocalFileMeta.newBuilder().setChecksum("0").build();
        Assert.assertNotEquals(wrong.getChecksum(), meta.getChecksum());

        Assert.assertFalse(machine.onSnapshotLoad(this.readerWith(wrong)));

        Assert.assertEquals(0L, loads.get());
    }

    @Test
    public void testSnapshotThatIsNotAnArchiveIsNotReportedAsInstalled() throws Exception {
        AtomicLong loads = new AtomicLong();
        RaftStateMachine machine = this.machineThatCountsLoads(loads);

        // No snapshot.zip in the snapshot directory
        Assert.assertFalse(machine.onSnapshotLoad(
                this.readerWith(LocalFileMeta.newBuilder().build())));

        Assert.assertEquals(0L, loads.get());
    }

    @Test
    public void testSnapshotWithItsChecksumIsInstalled() throws Exception {
        AtomicLong loads = new AtomicLong();
        RaftStateMachine machine = this.machineThatCountsLoads(loads);
        LocalFileMeta meta = this.saveSnapshot(machine);

        Assert.assertTrue(machine.onSnapshotLoad(this.readerWith(meta)));

        Assert.assertEquals(1L, loads.get());
    }

    private RaftStateMachine machineThatCountsLoads(AtomicLong loads) {
        RaftStateMachine machine = new RaftStateMachine();
        machine.addTaskHandler((op, response) -> {
            if (op.getOp() == KVOperation.SAVE_SNAPSHOT) {
                writeCheckpoint((String) op.getAttach(), 10L);
            } else if (op.getOp() == KVOperation.LOAD_SNAPSHOT) {
                loads.incrementAndGet();
            }
            return false;
        });
        return machine;
    }

    /**
     * Saves a snapshot into the snapshot directory and returns the file meta, with the
     * checksum of the archive, that the state machine handed to jraft
     */
    private LocalFileMeta saveSnapshot(RaftStateMachine machine) throws Exception {
        RecordingClosure done = new RecordingClosure();
        machine.onSnapshotSave(this.writer, done);
        Assert.assertTrue(done.await());
        Assert.assertTrue(done.statuses.get(0).isOk());
        ArgumentCaptor<Message> meta = ArgumentCaptor.forClass(Message.class);
        verify(this.writer).addFile(eq("snapshot.zip"), meta.capture());
        return (LocalFileMeta) meta.getValue();
    }

    private SnapshotReader readerWith(LocalFileMeta meta) {
        SnapshotReader reader = mock(SnapshotReader.class);
        when(reader.getPath()).thenReturn(this.snapshotPath.getAbsolutePath());
        when(reader.getFileMeta("snapshot.zip")).thenReturn(meta);
        return reader;
    }

    private static void writeCheckpoint(String dir, long index) {
        try {
            FileUtils.forceMkdir(new File(dir));
            Files.write(new File(dir, "CURRENT").toPath(),
                        Long.toString(index).getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private static final class RecordingClosure implements Closure {

        private final List<Status> statuses = new CopyOnWriteArrayList<>();
        private final CountDownLatch latch = new CountDownLatch(1);

        @Override
        public void run(Status status) {
            this.statuses.add(status);
            this.latch.countDown();
        }

        private boolean await() throws InterruptedException {
            return this.latch.await(10, TimeUnit.SECONDS);
        }
    }
}
