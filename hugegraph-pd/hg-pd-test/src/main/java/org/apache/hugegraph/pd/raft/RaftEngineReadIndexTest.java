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

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;

import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.grpc.Pdpb;
import org.apache.hugegraph.testutil.Whitebox;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.closure.ReadIndexClosure;
import com.alipay.sofa.jraft.error.RaftError;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Covers {@link RaftEngine#waitReadIndex()}, the barrier a leader runs before a local
 * read-then-write such as the id counter. The raft node is a mock that answers each
 * ReadIndex call the way jraft does: it sets the result and runs the closure.
 */
public class RaftEngineReadIndexTest {

    // Queued in place of a status: the closure is never run, as when no quorum answers
    private static final Status NO_ANSWER = new Status(RaftError.UNKNOWN, "no answer");

    private Node originalRaftNode;
    private Node mockNode;
    // One status per readIndex call, OK once the queue is empty
    private final Deque<Status> answers = new ArrayDeque<>();

    @Before
    public void setUp() {
        RaftEngine engine = RaftEngine.getInstance();
        this.originalRaftNode = engine.getRaftNode();
        this.mockNode = mock(Node.class);
        doAnswer(invocation -> {
            ReadIndexClosure closure = invocation.getArgument(1);
            Status status = this.answers.isEmpty() ? Status.OK() : this.answers.poll();
            if (status != NO_ANSWER) {
                closure.setResult(status.isOk() ? 42L : ReadIndexClosure.INVALID_LOG_INDEX,
                                  invocation.getArgument(0));
                closure.run(status);
            }
            return null;
        }).when(this.mockNode).readIndex(any(byte[].class), any(ReadIndexClosure.class));
        Whitebox.setInternalState(engine, "raftNode", this.mockNode);
    }

    @After
    public void tearDown() {
        Whitebox.setInternalState(RaftEngine.getInstance(), "raftNode", this.originalRaftNode);
    }

    @Test
    public void testReturnsOnceTheReadIndexIsApplied() throws PDException {
        RaftEngine.getInstance().waitReadIndex(1000L);

        verify(this.mockNode, times(1)).readIndex(any(byte[].class),
                                                  any(ReadIndexClosure.class));
    }

    @Test
    public void testRetriesWhileTheNewLeaderHasNoCommitInItsTerm() throws PDException {
        this.answers.addAll(Arrays.asList(new Status(RaftError.EAGAIN, "no commit yet"),
                                          new Status(RaftError.EBUSY, "transferring"),
                                          Status.OK()));

        RaftEngine.getInstance().waitReadIndex(1000L);

        verify(this.mockNode, times(3)).readIndex(any(byte[].class),
                                                  any(ReadIndexClosure.class));
    }

    @Test
    public void testRetriesAreBounded() {
        for (int i = 0; i < 10; i++) {
            this.answers.add(new Status(RaftError.EAGAIN, "no commit yet"));
        }

        PDException e = Assert.assertThrows(PDException.class, () -> {
            RaftEngine.getInstance().waitReadIndex(5000L);
        });

        Assert.assertEquals(Pdpb.ErrorType.UNKNOWN_VALUE, e.getErrorCode());
        // The first call and five retries
        verify(this.mockNode, times(6)).readIndex(any(byte[].class),
                                                  any(ReadIndexClosure.class));
    }

    @Test
    public void testNodeThatLostLeadershipFailsAsNotLeader() {
        this.answers.add(new Status(RaftError.EPERM, "not leader"));

        PDException e = Assert.assertThrows(PDException.class, () -> {
            RaftEngine.getInstance().waitReadIndex(1000L);
        });

        Assert.assertEquals(Pdpb.ErrorType.NOT_LEADER_VALUE, e.getErrorCode());
        verify(this.mockNode, times(1)).readIndex(any(byte[].class),
                                                  any(ReadIndexClosure.class));
    }

    @Test
    public void testWaitIsBoundedWhenNoQuorumAnswers() {
        this.answers.add(NO_ANSWER);

        long start = System.nanoTime();
        Assert.assertThrows(PDException.class, () -> {
            RaftEngine.getInstance().waitReadIndex(100L);
        });
        Assert.assertTrue(System.nanoTime() - start < 1_000_000_000L);
    }

    @Test
    public void testMissingRaftNodeFailsAsNotLeader() {
        Whitebox.setInternalState(RaftEngine.getInstance(), "raftNode", null);

        PDException e = Assert.assertThrows(PDException.class, () -> {
            RaftEngine.getInstance().waitReadIndex(1000L);
        });

        Assert.assertEquals(Pdpb.ErrorType.NOT_LEADER_VALUE, e.getErrorCode());
    }
}
