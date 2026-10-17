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

package org.apache.hugegraph.pd.core.meta;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.config.PDConfig;
import org.apache.hugegraph.pd.grpc.Pdpb;
import org.apache.hugegraph.pd.meta.IdMetaStore;
import org.apache.hugegraph.pd.meta.MetadataFactory;
import org.apache.hugegraph.pd.raft.KVOperation;
import org.apache.hugegraph.pd.raft.RaftEngine;
import org.apache.hugegraph.pd.store.HgKVStore;
import org.apache.hugegraph.pd.store.KV;
import org.apache.hugegraph.pd.store.RaftKVStore;
import org.apache.hugegraph.testutil.Whitebox;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;

import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.entity.Task;
import com.alipay.sofa.jraft.error.RaftError;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * {@link IdMetaStore#getId} and both {@code getCId} overloads read local state and then write
 * through raft as two steps, so the read must follow a raft ReadIndex: a leader elected a
 * moment ago may not have applied its predecessor's last write yet, and would hand out the
 * same id twice. The write must also carry the leader term of the read: a request that pauses
 * between the two could otherwise write its stale result after the node lost the leadership
 * and won it back.
 */
public class IdMetaStoreReadIndexTest {

    private HgKVStore originalStore;
    private HgKVStore store;
    private IdMetaStore idMetaStore;

    @Before
    public void setUp() {
        // The metadata stores share one factory-held store; swap in a mock for this test
        this.originalStore = Whitebox.getInternalState(MetadataFactory.class, "store");
        this.store = mock(HgKVStore.class);
        Whitebox.setInternalState(MetadataFactory.class, "store", this.store);
        this.idMetaStore = new IdMetaStore(new PDConfig());
    }

    @After
    public void tearDown() {
        Whitebox.setInternalState(MetadataFactory.class, "store", this.originalStore);
    }

    @Test
    public void testGetIdReadsOnlyAfterTheReadIndex() throws PDException {
        when(this.store.waitReadIndexInTerm()).thenReturn(7L);
        when(this.store.get(any())).thenReturn(IdMetaStore.longToBytes(100L));

        Assert.assertEquals(100L, this.idMetaStore.getId("read-index", 10));

        InOrder order = inOrder(this.store);
        order.verify(this.store).waitReadIndexInTerm();
        order.verify(this.store).get(any());
        // The write carries the term of the read
        order.verify(this.store).put(any(), any(), eq(7L));
        verify(this.store, never()).put(any(), any());
    }

    @Test
    public void testGetIdLeavesTheCounterAloneWhenTheReadIndexFails() throws PDException {
        doThrow(new PDException(Pdpb.ErrorType.NOT_LEADER_VALUE, "not leader"))
                .when(this.store).waitReadIndexInTerm();

        PDException e = Assert.assertThrows(PDException.class, () -> {
            this.idMetaStore.getId("read-index", 10);
        });

        Assert.assertEquals(Pdpb.ErrorType.NOT_LEADER_VALUE, e.getErrorCode());
        verify(this.store, times(1)).waitReadIndexInTerm();
        verify(this.store, never()).get(any());
        verify(this.store, never()).put(any(), any());
        verify(this.store, never()).put(any(), any(), anyLong());
    }

    @Test
    public void testGetCIdReadsOnlyAfterTheReadIndex() throws PDException {
        when(this.store.waitReadIndexInTerm()).thenReturn(7L);
        when(this.store.get(any())).thenReturn(IdMetaStore.longToBytes(5L));

        Assert.assertEquals(5L, this.idMetaStore.getCId("read-index", 16));

        InOrder order = inOrder(this.store);
        order.verify(this.store).waitReadIndexInTerm();
        order.verify(this.store).get(any());
        order.verify(this.store).scanRange(any(), any());
        order.verify(this.store, times(2)).put(any(), any(), eq(7L));
        verify(this.store, never()).put(any(), any());
    }

    @Test
    public void testGetCIdByNameReadsTheDelayedSlotOnlyAfterTheReadIndex()
            throws PDException {
        // A minimal in-memory store, so the delayed slot is written the way PD writes it
        Map<ByteBuffer, byte[]> data = new HashMap<>();
        doAnswer(inv -> data.put(ByteBuffer.wrap(inv.getArgument(0)), inv.getArgument(1)))
                .when(this.store).put(any(), any());
        when(this.store.get(any())).thenAnswer(inv -> {
            return data.get(ByteBuffer.wrap(inv.getArgument(0)));
        });
        this.idMetaStore.delCIdDelay("read-index", "graph", 7L);
        clearInvocations(this.store);
        when(this.store.waitReadIndexInTerm()).thenReturn(7L);

        Assert.assertEquals(7L, this.idMetaStore.getCId("read-index", "graph", 16));

        InOrder order = inOrder(this.store);
        order.verify(this.store).waitReadIndexInTerm();
        order.verify(this.store).scanPrefix(any());
        order.verify(this.store).get(any());
        order.verify(this.store).remove(any(), eq(7L));
        verify(this.store, times(1)).waitReadIndexInTerm();
        verify(this.store, never()).remove(any());
        verify(this.store, never()).put(any(), any());
        verify(this.store, never()).put(any(), any(), anyLong());
    }

    @Test
    public void testGetCIdByNameWithoutADelayedSlotWaitsOnce() throws PDException {
        // A second wait would return the second term
        when(this.store.waitReadIndexInTerm()).thenReturn(7L, 8L);

        Assert.assertEquals(0L, this.idMetaStore.getCId("read-index", "graph", 16));

        // The new cid is written in the term of the wait that covered the delayed slot
        verify(this.store, times(1)).waitReadIndexInTerm();
        verify(this.store, times(2)).put(any(), any(), eq(7L));
        verify(this.store, never()).put(any(), any(), eq(8L));
        verify(this.store, never()).put(any(), any());
    }

    @Test
    public void testGetCIdLeavesTheSlotsAloneWhenTheReadIndexFails() throws PDException {
        doThrow(new PDException(Pdpb.ErrorType.NOT_LEADER_VALUE, "not leader"))
                .when(this.store).waitReadIndexInTerm();

        PDException e = Assert.assertThrows(PDException.class, () -> {
            this.idMetaStore.getCId("read-index", 16);
        });
        Assert.assertEquals(Pdpb.ErrorType.NOT_LEADER_VALUE, e.getErrorCode());
        e = Assert.assertThrows(PDException.class, () -> {
            this.idMetaStore.getCId("read-index", "graph", 16);
        });
        Assert.assertEquals(Pdpb.ErrorType.NOT_LEADER_VALUE, e.getErrorCode());

        verify(this.store, never()).get(any());
        verify(this.store, never()).scanPrefix(any());
        verify(this.store, never()).scanRange(any(), any());
        verify(this.store, never()).put(any(), any());
        verify(this.store, never()).put(any(), any(), anyLong());
        verify(this.store, never()).remove(any());
        verify(this.store, never()).remove(any(), anyLong());
    }

    @Test
    public void testRaftStoreWaitsOnTheRaftEngine() throws PDException {
        RaftEngine engine = mock(RaftEngine.class);
        HgKVStore local = mock(HgKVStore.class);

        new RaftKVStore(engine, local).waitReadIndex();

        verify(engine).waitReadIndex();
    }

    @Test
    public void testRaftStoreBindsOnlyATermWriteToItsTerm() throws PDException {
        RaftEngine engine = mock(RaftEngine.class);
        when(engine.waitReadIndexInTerm()).thenReturn(7L);
        List<Long> expectedTerms = new ArrayList<>();
        doAnswer(inv -> {
            Task task = inv.getArgument(0);
            expectedTerms.add(task.getExpectedTerm());
            task.getDone().run(Status.OK());
            return null;
        }).when(engine).addTask(any());
        RaftKVStore raftStore = new RaftKVStore(engine, mock(HgKVStore.class));

        Assert.assertEquals(7L, raftStore.waitReadIndexInTerm());
        raftStore.put(new byte[]{1}, new byte[]{1}, 7L);
        raftStore.remove(new byte[]{1}, 7L);
        // -1 is jraft's "any term": the writes that follow no read stay as they were
        raftStore.put(new byte[]{1}, new byte[]{1});
        raftStore.remove(new byte[]{1});

        Assert.assertEquals(List.of(7L, 7L, -1L, -1L), expectedTerms);
    }

    @Test
    public void testCounterDoesNotRollBackAcrossALeaderChangeAndBack() throws PDException {
        FencedNode nodeA = new FencedNode();
        IdMetaStore allocator = nodeA.allocator();
        // The request on A reads counter 0 in term 1 and pauses before its put. B leads
        // term 2 and allocates [0,10) and [10,20). A applies both and leads again in term 3.
        nodeA.pauseBeforeFirstWrite(() -> {
            nodeA.term.set(2L);
            Assert.assertEquals(0L, allocator.getId("aba", 10));
            Assert.assertEquals(10L, allocator.getId("aba", 10));
            nodeA.term.set(3L);
        });

        // Written in term 3 from the read of term 1, the put would set the counter back
        // from 20 to 10 and return [0,10) a second time
        Assert.assertEquals(20L, allocator.getId("aba", 10));

        // The put of term 1 was rejected and the allocation read again in term 3
        Assert.assertEquals(List.of(1L), nodeA.rejectedTerms);
        Assert.assertEquals(List.of(2L, 2L, 3L), nodeA.acceptedTerms);
        // A new request does not get [10,20) or [20,30) a second time
        Assert.assertEquals(30L, allocator.getId("aba", 10));
    }

    @Test
    public void testCyclicIdIsNotHandedOutTwiceAcrossALeaderChangeAndBack()
            throws PDException {
        FencedNode nodeA = new FencedNode();
        IdMetaStore allocator = nodeA.allocator();
        // A reads counter 0 and no slot in term 1 and pauses. B leads term 2 and hands out
        // the cids 0 and 1. A applies that and leads again in term 3.
        nodeA.pauseBeforeFirstWrite(() -> {
            nodeA.term.set(2L);
            Assert.assertEquals(0L, allocator.getCId("aba", 16));
            Assert.assertEquals(1L, allocator.getCId("aba", 16));
            nodeA.term.set(3L);
        });

        // Not 0, which B handed out in term 2
        Assert.assertEquals(2L, allocator.getCId("aba", 16));

        Assert.assertEquals(List.of(1L), nodeA.rejectedTerms);
        Assert.assertEquals(3L, allocator.getCId("aba", 16));
    }

    @Test
    public void testAllocationGivesUpWhenItsWritesKeepBeingRejected() throws PDException {
        when(this.store.waitReadIndexInTerm()).thenReturn(7L);
        doThrow(new PDException(Pdpb.ErrorType.NOT_LEADER_VALUE, "another term"))
                .when(this.store).put(any(), any(), anyLong());

        PDException e = Assert.assertThrows(PDException.class, () -> {
            this.idMetaStore.getId("read-index", 10);
        });

        Assert.assertEquals(Pdpb.ErrorType.NOT_LEADER_VALUE, e.getErrorCode());
        verify(this.store, times(3)).waitReadIndexInTerm();
        verify(this.store, times(3)).put(any(), any(), eq(7L));
    }

    @Test
    public void testAllocationDoesNotRepeatAWriteThatFailedForAnotherReason()
            throws PDException {
        doThrow(new PDException(Pdpb.ErrorType.UNKNOWN_VALUE, "apply failed"))
                .when(this.store).put(any(), any(), anyLong());

        PDException e = Assert.assertThrows(PDException.class, () -> {
            this.idMetaStore.getId("read-index", 10);
        });

        Assert.assertEquals(Pdpb.ErrorType.UNKNOWN_VALUE, e.getErrorCode());
        verify(this.store, times(1)).put(any(), any(), anyLong());
    }

    /**
     * PD node A as the allocator sees it: the production {@link IdMetaStore} and
     * {@link RaftKVStore} over a raft engine that accepts a task the way jraft's
     * {@code NodeImpl.executeApplyingTasks} does, only when the task's expected term is -1
     * or the current term, and applies an accepted task to the local state at once.
     */
    private static final class FencedNode {

        private final TreeMap<byte[], byte[]> applied = new TreeMap<>(Arrays::compareUnsigned);
        private final AtomicLong term = new AtomicLong(1L);
        private final List<Long> acceptedTerms = new ArrayList<>();
        private final List<Long> rejectedTerms = new ArrayList<>();
        private final HgKVStore local = mock(HgKVStore.class);
        private final RaftEngine engine = mock(RaftEngine.class);
        private final RaftKVStore raftStore = new RaftKVStore(this.engine, this.local);
        private Pause beforeFirstWrite;

        private FencedNode() throws PDException {
            when(this.local.get(any())).thenAnswer(inv -> {
                return this.applied.get(inv.<byte[]>getArgument(0));
            });
            when(this.local.scanRange(any(), any())).thenAnswer(inv -> {
                List<KV> kvs = new ArrayList<>();
                this.applied.subMap(inv.getArgument(0), inv.getArgument(1))
                            .forEach((key, value) -> kvs.add(new KV(key, value)));
                return kvs;
            });
            doAnswer(inv -> this.applied.put(inv.getArgument(0), inv.getArgument(1)))
                    .when(this.local).put(any(), any());
            when(this.engine.waitReadIndexInTerm()).thenAnswer(inv -> this.term.get());
            doAnswer(inv -> {
                Pause pause = this.beforeFirstWrite;
                this.beforeFirstWrite = null;
                if (pause != null) {
                    pause.run();
                }
                Task task = inv.getArgument(0);
                long expected = task.getExpectedTerm();
                if (expected != -1L && expected != this.term.get()) {
                    this.rejectedTerms.add(expected);
                    task.getDone().run(new Status(RaftError.EPERM,
                                                  "expected_term=%d doesn't match " +
                                                  "current_term=%d", expected,
                                                  this.term.get()));
                    return null;
                }
                this.acceptedTerms.add(expected);
                this.raftStore.invoke(KVOperation.fromByteArray(task.getData().array()),
                                      null);
                task.getDone().run(Status.OK());
                return null;
            }).when(this.engine).addTask(any());
        }

        private IdMetaStore allocator() {
            Whitebox.setInternalState(MetadataFactory.class, "store", this.raftStore);
            return new IdMetaStore(new PDConfig());
        }

        /**
         * Runs {@code then} where the first request has read and is about to write. The
         * lock of an id key is reentrant, so {@code then} can allocate as another leader.
         */
        private void pauseBeforeFirstWrite(Pause then) {
            this.beforeFirstWrite = then;
        }
    }

    private interface Pause {

        void run() throws PDException;
    }
}
