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

package org.apache.hugegraph.pd.core.store;

import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.config.PDConfig;
import org.apache.hugegraph.pd.grpc.Pdpb;
import org.apache.hugegraph.pd.raft.KVOperation;
import org.apache.hugegraph.pd.raft.RaftEngine;
import org.apache.hugegraph.pd.raft.RaftStateMachine;
import org.apache.hugegraph.pd.store.HgKVStore;
import org.apache.hugegraph.pd.store.KV;
import org.apache.hugegraph.pd.store.RaftKVStore;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.alipay.sofa.jraft.Iterator;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.entity.Task;
import com.alipay.sofa.jraft.error.RaftError;

/**
 * Covers RaftKVStore as both sides of a raft round trip: writes become serialized raft tasks,
 * and the decoded operations are dispatched back onto the local store by invoke().
 */
public class RaftKVStoreTest {

    private static final byte[] KEY = "key".getBytes();
    private static final byte[] VALUE = "value".getBytes();

    private HgKVStore store;
    private RaftEngine engine;
    private RaftKVStore raftStore;

    @Before
    public void setUp() {
        this.store = Mockito.mock(HgKVStore.class);
        this.engine = Mockito.mock(RaftEngine.class);
        this.raftStore = new RaftKVStore(this.engine, this.store);
    }

    /**
     * Makes the mocked engine behave like a single-node leader: the task payload is decoded
     * exactly as a follower would decode the log entry, applied, then completed with status.
     */
    private void applyTasksAs(Status status) {
        Mockito.doAnswer(invocation -> {
            Task task = invocation.getArgument(0);
            if (status.isOk()) {
                ByteBuffer data = task.getData();
                byte[] bytes = new byte[data.remaining()];
                data.get(bytes);
                this.raftStore.invoke(KVOperation.fromByteArray(bytes), null);
            }
            task.getDone().run(status);
            return null;
        }).when(this.engine).addTask(Mockito.any(Task.class));
    }

    @Test
    public void testInitRegistersAsRaftTaskHandler() {
        PDConfig config = new PDConfig();

        this.raftStore.init(config);

        Mockito.verify(this.store).init(config);
        Mockito.verify(this.engine).addTaskHandler(this.raftStore);
    }

    @Test
    public void testReadsAreServedLocallyWithoutRaft() throws PDException {
        List<KV> kvs = Collections.singletonList(new KV(KEY, VALUE));
        Mockito.when(this.store.get(KEY)).thenReturn(VALUE);
        Mockito.when(this.store.getWithTTL(KEY)).thenReturn(VALUE);
        Mockito.when(this.store.getListWithTTL(KEY)).thenReturn(Collections.singletonList(VALUE));
        Mockito.when(this.store.scanPrefix(KEY)).thenReturn(kvs);
        Mockito.when(this.store.scanRange(KEY, VALUE)).thenReturn(kvs);

        Assert.assertArrayEquals(VALUE, this.raftStore.get(KEY));
        Assert.assertArrayEquals(VALUE, this.raftStore.getWithTTL(KEY));
        Assert.assertEquals(1, this.raftStore.getListWithTTL(KEY).size());
        Assert.assertSame(kvs, this.raftStore.scanPrefix(KEY));
        Assert.assertSame(kvs, this.raftStore.scanRange(KEY, VALUE));

        Mockito.verify(this.engine, Mockito.never()).addTask(Mockito.any());
    }

    @Test
    public void testWritesReachStoreOnlyThroughRaft() throws PDException {
        applyTasksAs(Status.OK());

        this.raftStore.put(KEY, VALUE);
        this.raftStore.putWithTTL(KEY, VALUE, 60L);
        this.raftStore.putWithTTL(KEY, VALUE, 5L, TimeUnit.MINUTES);
        this.raftStore.remove(KEY);
        this.raftStore.removeByPrefix(KEY);
        this.raftStore.removeWithTTL(KEY);
        this.raftStore.clear();

        Mockito.verify(this.engine, Mockito.times(7)).addTask(Mockito.any(Task.class));
        Mockito.verify(this.store).put(KEY, VALUE);
        Mockito.verify(this.store).putWithTTL(KEY, VALUE, 60L);
        Mockito.verify(this.store).putWithTTL(KEY, VALUE, 5L, TimeUnit.MINUTES);
        Mockito.verify(this.store).remove(KEY);
        Mockito.verify(this.store).removeByPrefix(KEY);
        Mockito.verify(this.store).removeWithTTL(KEY);
        Mockito.verify(this.store).clear();
        Mockito.verifyNoMoreInteractions(this.store);
    }

    @Test
    public void testWriteOnNonLeaderIsRejectedAndNotApplied() throws Exception {
        // A RaftEngine that has not joined a group is never leader
        RaftKVStore notLeader = new RaftKVStore(newEngine(), this.store);

        PDException e = Assert.assertThrows(PDException.class, () -> notLeader.put(KEY, VALUE));

        Assert.assertTrue(e.getMessage(),
                          e.getMessage().contains("Error code = " +
                                                  Pdpb.ErrorType.NOT_LEADER_VALUE));
        Mockito.verifyNoInteractions(this.store);
    }

    @Test
    public void testFailedRaftStatusFailsEveryWrite() {
        applyTasksAs(new Status(RaftError.EINTERNAL, "injected raft failure"));

        assertWriteFails(() -> this.raftStore.put(KEY, VALUE));
        assertWriteFails(() -> this.raftStore.putWithTTL(KEY, VALUE, 1L));
        assertWriteFails(() -> this.raftStore.putWithTTL(KEY, VALUE, 1L, TimeUnit.SECONDS));
        assertWriteFails(() -> this.raftStore.remove(KEY));
        assertWriteFails(() -> this.raftStore.removeByPrefix(KEY));
        assertWriteFails(() -> this.raftStore.removeWithTTL(KEY));
        assertWriteFails(() -> this.raftStore.clear());
        Mockito.verifyNoInteractions(this.store);
    }

    @Test
    public void testEngineExceptionFailsWrite() {
        Mockito.doThrow(new IllegalStateException("engine closed"))
               .when(this.engine).addTask(Mockito.any(Task.class));

        PDException e = Assert.assertThrows(PDException.class,
                                            () -> this.raftStore.put(KEY, VALUE));
        Assert.assertTrue(e.getMessage(), e.getMessage().contains("engine closed"));
        Mockito.verifyNoInteractions(this.store);
    }

    @Test
    public void testInvokeDispatchesSnapshotPathFromAttach() throws PDException {
        this.raftStore.invoke(KVOperation.createSaveSnapshot("/snap/save"), null);
        this.raftStore.invoke(KVOperation.createLoadSnapshot("/snap/load"), null);

        Mockito.verify(this.store).saveSnapshot("/snap/save");
        Mockito.verify(this.store).loadSnapshot("/snap/load");
    }

    @Test
    public void testInvokeIgnoresGetAndUnknownOperations() throws PDException {
        Assert.assertFalse(this.raftStore.invoke(KVOperation.createGet(KEY), null));
        Assert.assertFalse(this.raftStore.invoke(new KVOperation(KEY, VALUE, null, (byte) 0x7F),
                                                 null));

        Mockito.verifyNoInteractions(this.store);
    }

    @Test
    public void testFollowerReplayAppliesTtlWithUnit() throws Exception {
        RaftStateMachine stateMachine = new RaftStateMachine();
        stateMachine.addTaskHandler(this.raftStore);
        byte[] entry = KVOperation.createPutWithTTL(KEY, VALUE, 3L, TimeUnit.HOURS)
                                  .toByteArray();
        Iterator iter = Mockito.mock(Iterator.class);
        Mockito.when(iter.hasNext()).thenReturn(true, false);
        // A follower has no closure for the entry, so it must decode the log data
        Mockito.when(iter.done()).thenReturn(null);
        Mockito.when(iter.getData()).thenReturn(ByteBuffer.wrap(entry));

        stateMachine.onApply(iter);

        Mockito.verify(this.store).putWithTTL(KEY, VALUE, 3L, TimeUnit.HOURS);
        Mockito.verify(iter).next();
        Mockito.verify(iter, Mockito.never()).setErrorAndRollback(Mockito.anyLong(),
                                                                  Mockito.any());
    }

    @Test
    public void testFollowerReplayStopsOnStoreFailure() throws Exception {
        RaftStateMachine stateMachine = new RaftStateMachine();
        stateMachine.addTaskHandler(this.raftStore);
        Mockito.doThrow(new PDException(Pdpb.ErrorType.ROCKSDB_WRITE_ERROR_VALUE, "disk full"))
               .when(this.store).put(KEY, VALUE);
        Iterator iter = Mockito.mock(Iterator.class);
        Mockito.when(iter.hasNext()).thenReturn(true, false);
        Mockito.when(iter.getData())
               .thenReturn(ByteBuffer.wrap(KVOperation.createPut(KEY, VALUE).toByteArray()));

        stateMachine.onApply(iter);

        Mockito.verify(iter).setErrorAndRollback(Mockito.eq(1L), Mockito.argThat(
                status -> status.getRaftError() == RaftError.ESTATEMACHINE &&
                          "disk full".equals(status.getErrorMsg())));
        Mockito.verify(iter, Mockito.never()).next();
    }

    private interface Write {

        void run() throws PDException;
    }

    private static void assertWriteFails(Write write) {
        PDException e = Assert.assertThrows(PDException.class, write::run);
        Assert.assertEquals(Pdpb.ErrorType.UNKNOWN_VALUE, e.getErrorCode());
        Assert.assertTrue(e.getMessage(),
                          e.getMessage().contains("Error code = " + Pdpb.ErrorType.UNKNOWN_VALUE));
    }

    private static RaftEngine newEngine() throws Exception {
        Constructor<RaftEngine> constructor = RaftEngine.class.getDeclaredConstructor();
        constructor.setAccessible(true);
        return constructor.newInstance();
    }
}
