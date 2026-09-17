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

package org.apache.hugegraph.store.core.raft;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

import java.lang.reflect.Field;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;

import org.apache.hugegraph.store.HgStoreEngine;
import org.apache.hugegraph.store.raft.PartitionStateMachine;
import org.apache.hugegraph.store.snapshot.SnapshotHandler;
import org.apache.hugegraph.store.util.ExecutorUtil;
import org.apache.hugegraph.store.util.HgStoreException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotWriter;

/**
 * Covers the EC_RKDB_SNAPSHOT_SAVE_BUSY_FAIL -> RaftError.EBUSY mapping at the
 * PartitionStateMachine.onSnapshotSave(SnapshotWriter, Closure) boundary, per the PR review
 * comment on #3164. Existing coverage (SnapshotHandlerTest) only asserts the exception thrown
 * by SnapshotHandler directly - nothing invoked PartitionStateMachine.onSnapshotSave() itself
 * and captured the resulting Closure status. A regression that mapped busy failures to EIO
 * instead of EBUSY would still pass there, while in production it would cause jRaft to
 * incorrectly escalate to restartRaftNode() instead of simply retrying.
 */
public class PartitionStateMachineTest {

    private static boolean createdTestExecutor;

    /**
     * onSnapshotSave() submits its work to HgStoreEngine's static uninterruptibleJobs executor.
     * Install a lightweight one via reflection instead of going through the full
     * HgStoreEngine.init() bootstrap (rpc server, raft rpc, PD registration, etc.), which would
     * conflict with the singleton lifecycle other suites rely on. Guarded so a real init() that
     * runs first (e.g. if this ever shares a fork with a StoreEngineTestBase-derived suite) is
     * left untouched.
     */
    @BeforeClass
    public static void ensureUninterruptibleJobsExecutor() throws Exception {
        if (HgStoreEngine.getUninterruptibleJobs() == null) {
            Field field = HgStoreEngine.class.getDeclaredField("uninterruptibleJobs");
            field.setAccessible(true);
            field.set(null, ExecutorUtil.createExecutor("test-psm-u-job", 2, 4, 16, true));
            createdTestExecutor = true;
        }
    }

    @AfterClass
    public static void shutDownTestExecutor() {
        if (createdTestExecutor) {
            ((ThreadPoolExecutor) HgStoreEngine.getUninterruptibleJobs()).shutdownNow();
        }
    }

    @Test
    public void testOnSnapshotSaveMapsBusyFailureToEbusy() throws Exception {
        SnapshotHandler mockSnapshotHandler = mock(SnapshotHandler.class);
        doThrow(new HgStoreException(HgStoreException.EC_RKDB_SNAPSHOT_SAVE_BUSY_FAIL,
                                      "Partition 0 snapshot save failed: compaction in progress"))
                .when(mockSnapshotHandler).onSnapshotSave(any());

        PartitionStateMachine stateMachine = new PartitionStateMachine(0, mockSnapshotHandler);
        Status status = runOnSnapshotSave(stateMachine);

        assertEquals("a busy compaction-range lock must map to EBUSY, not EIO, so jRaft's " +
                     "snapshot scheduler retries instead of escalating to restartRaftNode()",
                     RaftError.EBUSY, status.getRaftError());
    }

    @Test
    public void testOnSnapshotSaveMapsOrdinaryFailureToEio() throws Exception {
        SnapshotHandler mockSnapshotHandler = mock(SnapshotHandler.class);
        doThrow(new HgStoreException(HgStoreException.EC_RKDB_EXPORT_SNAPSHOT_FAIL, "disk full"))
                .when(mockSnapshotHandler).onSnapshotSave(any());

        PartitionStateMachine stateMachine = new PartitionStateMachine(0, mockSnapshotHandler);
        Status status = runOnSnapshotSave(stateMachine);

        assertEquals("a non-busy save failure must still escalate as EIO",
                     RaftError.EIO, status.getRaftError());
    }

    /**
     * Confirms onSnapshotSave()'s internal lock is released via its finally block even when
     * snapshotHandler.onSnapshotSave() throws - the same unconditional-unlock class of bug
     * fixed for dbCompaction()'s rangeLock (see HgSnapshotHandlerTest). If the lock were left
     * held, every subsequent onSnapshotSave() call on this state machine would hang forever.
     */
    @Test
    public void testOnSnapshotSaveReleasesLockOnFailure() throws Exception {
        SnapshotHandler mockSnapshotHandler = mock(SnapshotHandler.class);
        doThrow(new HgStoreException(HgStoreException.EC_RKDB_SNAPSHOT_SAVE_BUSY_FAIL,
                                      "Partition 0 snapshot save failed: compaction in progress"))
                .when(mockSnapshotHandler).onSnapshotSave(any());

        PartitionStateMachine stateMachine = new PartitionStateMachine(0, mockSnapshotHandler);
        runOnSnapshotSave(stateMachine);

        Lock internalLock = getInternalLock(stateMachine);
        // The done callback runs before the worker's finally block releases the lock.
        assertTrue("onSnapshotSave's finally block must release its lock even when " +
                   "snapshotHandler.onSnapshotSave() throws, or every later snapshot save " +
                   "attempt on this partition would hang forever",
                   internalLock.tryLock(5, TimeUnit.SECONDS));
        internalLock.unlock();
    }

    private static Status runOnSnapshotSave(PartitionStateMachine stateMachine)
            throws InterruptedException {
        SnapshotWriter stubWriter = mock(SnapshotWriter.class);
        AtomicReference<Status> result = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);

        stateMachine.onSnapshotSave(stubWriter, status -> {
            result.set(status);
            latch.countDown();
        });

        assertTrue("onSnapshotSave's done closure must be invoked",
                   latch.await(5, TimeUnit.SECONDS));
        return result.get();
    }

    private static Lock getInternalLock(PartitionStateMachine stateMachine) throws Exception {
        Field field = PartitionStateMachine.class.getDeclaredField("lock");
        field.setAccessible(true);
        return (Lock) field.get(stateMachine);
    }
}
