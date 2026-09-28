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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

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

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Iterator;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.entity.LeaderChangeContext;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.entity.Task;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.error.RaftException;

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
    public void testOnApply() {
        RaftOperation op = RaftOperation.create((byte) 0b0);
        final Task task = new Task();
        task.setData(ByteBuffer.wrap(op.getValues()));
        task.setDone(new HgStoreStateMachine.RaftClosureAdapter(op, closure -> {

        }));

        List<Task> tasks = new ArrayList<>();
        tasks.add(task);
        // Setup
        final Iterator inter = new Iterator() {
            final java.util.Iterator<Task> iterator = tasks.iterator();
            Task task;

            @Override
            public ByteBuffer getData() {
                return task.getData();
            }

            @Override
            public long getIndex() {
                return 0;
            }

            @Override
            public long getTerm() {
                return 0;
            }

            @Override
            public Closure done() {
                return null;
            }

            @Override
            public void setErrorAndRollback(long ntail, Status st) {

            }

            @Override
            public boolean commit() {
                throw new UnsupportedOperationException();
            }

            @Override
            public void commitAndSnapshotSync(Closure done) {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public ByteBuffer next() {
                task = iterator.next();
                return task.getData();
            }
        };

        // Run the test
        hgStoreStateMachineUnderTest.onApply(inter);

        // Verify the results
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
