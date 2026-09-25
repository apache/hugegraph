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

package org.apache.hugegraph.pd.core;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hugegraph.pd.raft.KVOperation;
import org.apache.hugegraph.pd.raft.KVStoreClosure;
import org.apache.hugegraph.pd.raft.RaftStateMachine;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

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

public class RaftStateMachineTest {

    @Test
    public void testSuccessfulLeaderApply() throws Exception {
        assertApply(true, false);
    }

    @Test
    public void testLeaderFailureStopsApply() throws Exception {
        assertApply(true, true);
    }

    @Test
    public void testFollowerFailureStopsReplay() throws Exception {
        assertApply(false, true);
    }

    @Test
    public void testCompletionFailureDoesNotStopApply() throws Exception {
        assertApply(true, false, true);
    }

    private void assertApply(boolean leader, boolean fail) throws Exception {
        assertApply(leader, fail, false);
    }

    private void assertApply(boolean leader, boolean fail, boolean throwFromCallback)
            throws Exception {
        RaftStateMachine stateMachine = new RaftStateMachine();
        List<Integer> applied = new ArrayList<>();
        stateMachine.addTaskHandler((operation, done) -> {
            int key = operation.getKey()[0];
            if (fail && key == 2) {
                throw new IllegalStateException("injected apply failure: %s, 100%");
            }
            applied.add(key);
            return true;
        });

        LogManager logManager = Mockito.mock(LogManager.class);
        ClosureQueueImpl closures = new ClosureQueueImpl("pd-apply-test");
        closures.resetFirstIndex(1);
        List<KVStoreClosure> callbacks = new ArrayList<>();
        CountDownLatch completed = new CountDownLatch(leader ? 3 : 0);
        for (int index = 1; index <= 4; index++) {
            KVOperation operation = KVOperation.createPut(new byte[]{(byte) index}, new byte[]{1});
            LogEntry entry = new LogEntry(EnumOutter.EntryType.ENTRY_TYPE_DATA);
            entry.setId(new LogId(index, 1));
            entry.setData(ByteBuffer.wrap(operation.toByteArray()));
            Mockito.when(logManager.getEntry(index)).thenReturn(entry);
            Mockito.when(logManager.getTerm(index)).thenReturn(1L);
            if (leader && index <= 3) {
                KVStoreClosure callback = Mockito.mock(KVStoreClosure.class);
                boolean failCallback = throwFromCallback && index == 2;
                Mockito.doAnswer(invocation -> {
                    completed.countDown();
                    if (failCallback && ((Status) invocation.getArgument(0)).isOk()) {
                        throw new IllegalStateException("injected callback failure");
                    }
                    return null;
                }).when(callback).run(Mockito.any(Status.class));
                callbacks.add(callback);
                closures.appendPendingClosure(new RaftStateMachine.RaftClosureAdapter(operation,
                                                                                     callback));
            }
        }

        NodeImpl node = Mockito.mock(NodeImpl.class);
        Mockito.when(node.getNodeMetrics()).thenReturn(new NodeMetrics(false));
        Mockito.when(node.getGroupId()).thenReturn("pd-apply-test");
        FSMCallerOptions options = new FSMCallerOptions();
        options.setNode(node);
        options.setFsm(stateMachine);
        options.setLogManager(logManager);
        options.setClosureQueue(closures);
        options.setBootstrapId(new LogId(0, 0));
        options.setDisruptorBufferSize(16);
        FSMCallerImpl caller = new FSMCallerImpl();
        Assert.assertTrue(caller.init(options));
        CountDownLatch batchApplied = new CountDownLatch(1);
        caller.addLastAppliedLogIndexListener(index -> batchApplied.countDown());
        try {
            Assert.assertTrue(caller.onCommitted(3));
            Assert.assertTrue(batchApplied.await(10, TimeUnit.SECONDS));
            Assert.assertEquals(fail ? 1 : 3, caller.getLastAppliedIndex());
            Assert.assertEquals(3, caller.getLastCommittedIndex());
            Assert.assertTrue(completed.await(10, TimeUnit.SECONDS));
            if (fail) {
                // The terminal error must also reject future batches and snapshot saves.
                Assert.assertTrue(caller.onCommitted(4));
                CountDownLatch snapshotCompleted = new CountDownLatch(1);
                AtomicReference<Status> snapshotStatus = new AtomicReference<>();
                SaveSnapshotClosure snapshot = Mockito.mock(SaveSnapshotClosure.class);
                Mockito.doAnswer(invocation -> {
                    snapshotStatus.set(invocation.getArgument(0));
                    snapshotCompleted.countDown();
                    return null;
                }).when(snapshot).run(Mockito.any(Status.class));
                caller.onSnapshotSave(snapshot);
                Assert.assertTrue(snapshotCompleted.await(10, TimeUnit.SECONDS));
                Assert.assertFalse(snapshotStatus.get().isOk());
                Assert.assertTrue(snapshotStatus.get().getErrorMsg()
                                                .startsWith("FSMCaller is in bad status"));
                Mockito.verify(logManager, Mockito.never()).getConfiguration(Mockito.anyLong());
                Mockito.verify(snapshot, Mockito.never()).start(Mockito.any());
            }
        } finally {
            caller.shutdown();
            caller.join();
        }
        Assert.assertEquals(fail ? Arrays.asList(1) : Arrays.asList(1, 2, 3), applied);
        Assert.assertEquals(fail ? 1 : 3, caller.getLastAppliedIndex());
        Mockito.verify(logManager).setAppliedId(new LogId(fail ? 1 : 3, 1));
        if (!fail) {
            Mockito.verify(node, Mockito.never())
                   .onError(Mockito.any(RaftException.class));
        }
        if (leader) {
            for (int index = 0; index < callbacks.size(); index++) {
                int expectedCode = fail && index > 0 ? RaftError.ESTATEMACHINE.getNumber() : 0;
                Mockito.verify(callbacks.get(index), Mockito.times(1))
                       .run(Mockito.argThat(status -> status.getCode() == expectedCode));
                Mockito.verifyNoMoreInteractions(callbacks.get(index));
            }
        }
    }
}
