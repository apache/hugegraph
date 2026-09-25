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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;

import org.apache.hugegraph.store.HgStoreEngine;
import org.apache.hugegraph.store.raft.DefaultRaftClosure;
import org.apache.hugegraph.store.raft.PartitionStateMachine;
import org.apache.hugegraph.store.raft.RaftClosure;
import org.apache.hugegraph.store.raft.RaftOperation;
import org.apache.hugegraph.store.raft.RaftStateListener;
import org.apache.hugegraph.store.raft.RaftTaskHandler;
import org.apache.hugegraph.store.snapshot.SnapshotHandler;
import org.apache.hugegraph.store.util.ExecutorUtil;
import org.apache.hugegraph.store.util.HgStoreException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.closure.ClosureQueueImpl;
import com.alipay.sofa.jraft.closure.SaveSnapshotClosure;
import com.alipay.sofa.jraft.core.FSMCallerImpl;
import com.alipay.sofa.jraft.core.NodeImpl;
import com.alipay.sofa.jraft.core.NodeMetrics;
import com.alipay.sofa.jraft.entity.EnumOutter;
import com.alipay.sofa.jraft.entity.LogEntry;
import com.alipay.sofa.jraft.entity.LogId;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.error.RaftException;
import com.alipay.sofa.jraft.option.FSMCallerOptions;
import com.alipay.sofa.jraft.storage.LogManager;
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

    @Test
    public void testLeaderApplySuccess() throws Exception {
        assertApply(false, true);
    }

    @Test
    public void testFollowerApplySuccess() throws Exception {
        assertApply(false, false);
    }

    @Test
    public void testLeaderApplyFailure() throws Exception {
        assertApply(true, true);
    }

    @Test
    public void testFollowerApplyFailure() throws Exception {
        assertApply(true, false);
    }

    @Test
    public void testStateMachineErrorNotifiesListenerSynchronously() {
        PartitionStateMachine stateMachine = new PartitionStateMachine(0, mock(SnapshotHandler.class));
        RaftStateListener listener = mock(RaftStateListener.class);
        AtomicReference<Thread> notifiedOn = new AtomicReference<>();
        doAnswer(invocation -> {
            notifiedOn.set(Thread.currentThread());
            return null;
        }).when(listener).onError(any(RaftException.class));
        stateMachine.addStateListener(listener);

        stateMachine.onError(new RaftException(EnumOutter.ErrorType.ERROR_TYPE_STATE_MACHINE));

        assertEquals(Thread.currentThread(), notifiedOn.get());
    }

    @Test
    public void testCompletionFailureDoesNotStopApply() throws Exception {
        assertApply(false, true, true, false);
    }

    @Test
    public void testHandlerCallbackFailureDoesNotStopApply() throws Exception {
        assertApply(false, true, true, true);
    }

    private void assertApply(boolean fail, boolean leader) throws Exception {
        assertApply(fail, leader, false, false);
    }

    private void assertApply(boolean fail, boolean leader, boolean throwFromCallback,
                             boolean completeInHandler) throws Exception {
        PartitionStateMachine stateMachine = new PartitionStateMachine(0, mock(SnapshotHandler.class));
        List<Integer> invoked = new ArrayList<>();
        List<Long> notified = new ArrayList<>();
        stateMachine.addTaskHandler(new RaftTaskHandler() {
            @Override
            public boolean invoke(int groupId, byte[] request, RaftClosure response) {
                return apply(request[0]);
            }

            @Override
            public boolean invoke(int groupId, byte methodId, Object req, RaftClosure response) {
                boolean handled = apply(methodId);
                if (completeInHandler) {
                    response.run(Status.OK());
                    return false;
                }
                return handled;
            }

            private boolean apply(int value) {
                invoked.add(value);
                if (fail && value == 2) {
                    throw new IllegalStateException("injected apply failure: %s, 100%");
                }
                return true;
            }
        });
        stateMachine.addStateListener(new RaftStateListener() {
            @Override
            public void onLeaderStart(long term) {
            }

            @Override
            public void onError(RaftException error) {
            }

            @Override
            public void onDataCommitted(long index) {
                notified.add(index);
            }
        });

        LogManager logs = mock(LogManager.class);
        when(logs.getEntry(anyLong())).thenAnswer(invocation -> {
            long index = invocation.getArgument(0);
            LogEntry entry = new LogEntry(EnumOutter.EntryType.ENTRY_TYPE_DATA);
            entry.setId(new LogId(index, 1));
            entry.setData(ByteBuffer.wrap(new byte[]{(byte) index}));
            return entry;
        });
        ClosureQueueImpl closures = new ClosureQueueImpl("store-apply-test");
        closures.resetFirstIndex(1);
        AtomicIntegerArray calls = new AtomicIntegerArray(3);
        AtomicIntegerArray codes = new AtomicIntegerArray(3);
        CountDownLatch completed = new CountDownLatch(leader ? 3 : 0);
        for (int i = 0; i < 3; i++) {
            final int slot = i;
            closures.appendPendingClosure(leader ? new DefaultRaftClosure(
                    RaftOperation.create((byte) (i + 1)), status -> {
                        codes.set(slot, status.getCode());
                        calls.incrementAndGet(slot);
                        completed.countDown();
                        if (throwFromCallback && slot == 1 && status.isOk()) {
                            throw new IllegalStateException("injected callback failure");
                        }
                    }) : null);
        }
        when(logs.getTerm(anyLong())).thenReturn(1L);
        NodeImpl node = mock(NodeImpl.class);
        when(node.getNodeMetrics()).thenReturn(new NodeMetrics(false));
        when(node.getGroupId()).thenReturn("store-apply-test");
        FSMCallerOptions options = new FSMCallerOptions();
        options.setNode(node);
        options.setFsm(stateMachine);
        options.setLogManager(logs);
        options.setClosureQueue(closures);
        options.setBootstrapId(new LogId(0, 0));
        options.setDisruptorBufferSize(16);
        FSMCallerImpl caller = new FSMCallerImpl();
        assertTrue(caller.init(options));
        CountDownLatch batchApplied = new CountDownLatch(1);
        caller.addLastAppliedLogIndexListener(index -> batchApplied.countDown());
        try {
            assertTrue(caller.onCommitted(3));
            assertTrue(batchApplied.await(5, TimeUnit.SECONDS));
            assertEquals(fail ? 1L : 3L, caller.getLastAppliedIndex());
            assertEquals(3L, caller.getLastCommittedIndex());
            if (fail) {
                assertTrue(caller.onCommitted(4));
                CountDownLatch snapshotCompleted = new CountDownLatch(1);
                AtomicReference<Status> snapshotStatus = new AtomicReference<>();
                SaveSnapshotClosure snapshot = mock(SaveSnapshotClosure.class);
                doAnswer(invocation -> {
                    snapshotStatus.set(invocation.getArgument(0));
                    snapshotCompleted.countDown();
                    return null;
                }).when(snapshot).run(any(Status.class));
                assertTrue(caller.onSnapshotSave(snapshot));
                assertTrue(snapshotCompleted.await(5, TimeUnit.SECONDS));
                assertFalse(snapshotStatus.get().isOk());
                assertTrue(snapshotStatus.get().getErrorMsg()
                                         .startsWith("FSMCaller is in bad status"));
                verify(logs, never()).getConfiguration(anyLong());
                verify(snapshot, never()).start(any());
            }
        } finally {
            caller.shutdown();
            caller.join();
        }
        assertEquals(fail ? Arrays.asList(1, 2) : Arrays.asList(1, 2, 3), invoked);
        assertEquals(fail ? Arrays.asList(1L) : Arrays.asList(1L, 2L, 3L), notified);
        assertEquals(fail ? 1L : 3L, stateMachine.getCommittedIndex());
        assertEquals(fail ? 1L : 3L, caller.getLastAppliedIndex());
        verify(logs).setAppliedId(new LogId(fail ? 1 : 3, 1));
        if (!fail) {
            verify(node, never()).onError(any(RaftException.class));
        }
        assertTrue(completed.await(5, TimeUnit.SECONDS));
        for (int i = 0; i < 3; i++) {
            assertEquals(leader ? 1 : 0, calls.get(i));
            assertEquals(leader && fail && i > 0 ? RaftError.ESTATEMACHINE.getNumber() : 0,
                         codes.get(i));
        }
    }

}
