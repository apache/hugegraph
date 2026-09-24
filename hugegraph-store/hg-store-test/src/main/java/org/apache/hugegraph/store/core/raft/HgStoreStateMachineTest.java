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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hugegraph.store.raft.HgStoreStateMachine;
import org.apache.hugegraph.store.raft.RaftClosure;
import org.apache.hugegraph.store.raft.RaftOperation;
import org.apache.hugegraph.store.raft.RaftStateListener;
import org.apache.hugegraph.store.raft.RaftTaskHandler;
import org.apache.hugegraph.store.snapshot.HgSnapshotHandler;
import org.apache.hugegraph.store.util.HgStoreException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.closure.ClosureQueueImpl;
import com.alipay.sofa.jraft.closure.SaveSnapshotClosure;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.core.FSMCallerImpl;
import com.alipay.sofa.jraft.core.NodeImpl;
import com.alipay.sofa.jraft.core.NodeMetrics;
import com.alipay.sofa.jraft.entity.EnumOutter;
import com.alipay.sofa.jraft.entity.LogEntry;
import com.alipay.sofa.jraft.entity.LogId;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.error.RaftException;
import com.alipay.sofa.jraft.option.FSMCallerOptions;
import com.alipay.sofa.jraft.storage.LogManager;

@RunWith(MockitoJUnitRunner.class)
public class HgStoreStateMachineTest {

    @Mock
    private HgSnapshotHandler mockSnapshotHandler;

    private HgStoreStateMachine hgStoreStateMachineUnderTest;

    @Before
    public void setUp() {
        hgStoreStateMachineUnderTest = new HgStoreStateMachine(0, mockSnapshotHandler);
    }

    @Test
    public void testAddTaskHandler() {
        // Setup
        final RaftTaskHandler handler = new RaftTaskHandler() {
            @Override
            public boolean invoke(int groupId, byte[] request, RaftClosure response) throws
                                                                                     HgStoreException {
                return false;
            }

            @Override
            public boolean invoke(int groupId, byte methodId, Object req, RaftClosure response)
                    throws HgStoreException {
                return false;
            }
        };

        // Run the test
        hgStoreStateMachineUnderTest.addTaskHandler(handler);

        // Verify the results
    }

    @Test
    public void testAddStateListener() {
        // Setup
        final RaftStateListener mockListener = new RaftStateListener() {
            @Override
            public void onLeaderStart(long newTerm) {

            }

            @Override
            public void onError(RaftException e) {

            }
        };

        // Run the test
        hgStoreStateMachineUnderTest.addStateListener(mockListener);

        // Verify the results
    }

    @Test
    public void testIsLeader() {
        // Setup
        // Run the test
        final boolean result = hgStoreStateMachineUnderTest.isLeader();

        // Verify the results
        assertFalse(result);
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

    private void assertApply(boolean fail, boolean leader) throws Exception {
        List<Integer> invoked = new ArrayList<>();
        List<Long> notified = new ArrayList<>();
        hgStoreStateMachineUnderTest.addTaskHandler(new RaftTaskHandler() {
            @Override
            public boolean invoke(int groupId, byte[] request, RaftClosure response) {
                return apply(request[0]);
            }

            @Override
            public boolean invoke(int groupId, byte methodId, Object req, RaftClosure response) {
                return apply(methodId);
            }

            private boolean apply(int value) {
                invoked.add(value);
                if (fail && value == 2) {
                    throw new IllegalStateException("injected apply failure");
                }
                return true;
            }
        });
        hgStoreStateMachineUnderTest.addStateListener(new RaftStateListener() {
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
            closures.appendPendingClosure(leader ? new HgStoreStateMachine.RaftClosureAdapter(
                    RaftOperation.create((byte) (i + 1)), status -> {
                        codes.set(slot, status.getCode());
                        calls.incrementAndGet(slot);
                        completed.countDown();
                    }) : null);
        }
        when(logs.getTerm(anyLong())).thenReturn(1L);
        NodeImpl node = mock(NodeImpl.class);
        when(node.getNodeMetrics()).thenReturn(new NodeMetrics(false));
        when(node.getGroupId()).thenReturn("store-apply-test");
        FSMCallerOptions options = new FSMCallerOptions();
        options.setNode(node);
        options.setFsm(hgStoreStateMachineUnderTest);
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
        assertEquals(fail ? 1L : 3L, hgStoreStateMachineUnderTest.getCommittedIndex());
        assertEquals(fail ? 1L : 3L, caller.getLastAppliedIndex());
        verify(logs).setAppliedId(new LogId(fail ? 1 : 3, 1));
        assertTrue(completed.await(5, TimeUnit.SECONDS));
        for (int i = 0; i < 3; i++) {
            assertEquals(leader ? 1 : 0, calls.get(i));
            assertEquals(leader && fail && i > 0 ? RaftError.ESTATEMACHINE.getNumber() : 0,
                         codes.get(i));
        }
    }

    @Test
    public void testGetLeaderTerm() {
        // Setup
        // Run the test
        final long result = hgStoreStateMachineUnderTest.getLeaderTerm();

        // Verify the results
        assertEquals(-1L, result);
    }


    @Test
    public void testOnLeaderStart() {
        // Setup
        // Run the test
        hgStoreStateMachineUnderTest.onLeaderStart(0L);

        // Verify the results
    }

    @Test
    public void testOnLeaderStop() {
        // Setup
        final Status status = new Status(RaftError.UNKNOWN, "fmt", "args");

        // Run the test
        hgStoreStateMachineUnderTest.onLeaderStop(status);

        // Verify the results
    }

    @Test
    public void testOnStartFollowing() {
        // TODO: uncomment later (jraft)
//        // Setup
//        final LeaderChangeContext ctx =
//                new LeaderChangeContext(new PeerId("ip", 0, 0, 0), "groupId", 0L,
//                                        new Status(RaftError.UNKNOWN, "fmt", "args"));
//
//        // Run the test
//        hgStoreStateMachineUnderTest.onStartFollowing(ctx);

        // Verify the results
    }

    @Test
    public void testOnStopFollowing() {
        // TODO: uncomment later (jraft)
//        // Setup
//        final LeaderChangeContext ctx =
//                new LeaderChangeContext(new PeerId("ip", 0, 0, 0), "groupId", 0L,
//                                        new Status(RaftError.UNKNOWN, "fmt", "args"));
//
//        // Run the test
//        hgStoreStateMachineUnderTest.onStopFollowing(ctx);

        // Verify the results
    }

    @Test
    public void testOnConfigurationCommitted() {
        // Setup
        final Configuration conf = new Configuration(List.of(new PeerId("ip", 0, 0, 0)),
                                                     List.of(new PeerId("ip", 0, 0, 0)));

        // Run the test
        hgStoreStateMachineUnderTest.onConfigurationCommitted(conf);

        // Verify the results
    }

}
