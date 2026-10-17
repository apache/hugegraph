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

import java.util.Arrays;
import java.util.Deque;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.config.PDConfig;
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
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
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
    private PDConfig.Raft originalConfig;
    private long originalLeaderTerm;
    // The term jraft announced this node as the leader of, -1 while it is not the leader
    private AtomicLong leaderTerm;
    private Node mockNode;
    // One status per readIndex call, OK once the queue is empty
    private final Deque<Status> answers = new ConcurrentLinkedDeque<>();

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

        this.originalConfig = engine.getConfig();
        PDConfig.Raft config = new PDConfig().new Raft();
        config.setRpcTimeout(1000);
        Whitebox.setInternalState(engine, "config", config);
        RaftStateMachine machine = Whitebox.getInternalState(engine, "stateMachine");
        this.leaderTerm = Whitebox.getInternalState(machine, "leaderTerm");
        this.originalLeaderTerm = this.leaderTerm.get();
    }

    @After
    public void tearDown() {
        RaftEngine engine = RaftEngine.getInstance();
        Whitebox.setInternalState(engine, "raftNode", this.originalRaftNode);
        Whitebox.setInternalState(engine, "config", this.originalConfig);
        this.leaderTerm.set(this.originalLeaderTerm);
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
    public void testRetriesPastTheFirstHundredMillisecondsUntilTheLeaderCommits()
            throws PDException {
        // 15 answers 20 ms apart: the leader needs about 300 ms to commit in its term
        for (int i = 0; i < 15; i++) {
            this.answers.add(new Status(RaftError.EAGAIN, "no commit yet"));
        }

        RaftEngine.getInstance().waitReadIndex(5000L);

        verify(this.mockNode, times(16)).readIndex(any(byte[].class),
                                                   any(ReadIndexClosure.class));
    }

    @Test
    public void testRetriesStopAtTheDeadline() throws InterruptedException {
        for (int i = 0; i < 1000; i++) {
            this.answers.add(new Status(RaftError.EAGAIN, "no commit yet"));
        }

        long start = System.nanoTime();
        PDException e = Assert.assertThrows(PDException.class, () -> {
            RaftEngine.getInstance().waitReadIndex(200L);
        });
        Assert.assertTrue(System.nanoTime() - start < 1_000_000_000L);
        Assert.assertEquals(Pdpb.ErrorType.UNKNOWN_VALUE, e.getErrorCode());

        // No retry is left running once the wait has given up
        int calls = 1000 - this.answers.size();
        Thread.sleep(100L);
        Assert.assertEquals(calls, 1000 - this.answers.size());
    }

    @Test
    public void testBusyNodeIsRetriedLessAndLessOften() throws InterruptedException {
        // EBUSY is also jraft's answer while its read index queue is full. A retry every
        // 20 ms would be about 25 calls in 500 ms; doubled delays give calls at 0, 20, 60,
        // 140 and 300 ms.
        for (int i = 0; i < 1000; i++) {
            this.answers.add(new Status(RaftError.EBUSY, "read index queue is full"));
        }

        PDException e = Assert.assertThrows(PDException.class, () -> {
            RaftEngine.getInstance().waitReadIndex(500L);
        });

        Assert.assertEquals(Pdpb.ErrorType.UNKNOWN_VALUE, e.getErrorCode());
        int calls = 1000 - this.answers.size();
        Assert.assertTrue("ReadIndex calls: " + calls, calls >= 3 && calls <= 6);
        Thread.sleep(100L);
        Assert.assertEquals(calls, 1000 - this.answers.size());
    }

    @Test
    public void testBackoffStartsOverAfterAnAnswerThatIsNotBusy() throws PDException {
        // 20 + 40 + 80 + 160 ms for four EBUSY answers, 20 ms for EAGAIN, then 20 ms again
        // for the next EBUSY: 340 ms. It would wait 320 ms, 640 ms in all, had it kept
        // the count
        for (int i = 0; i < 4; i++) {
            this.answers.add(new Status(RaftError.EBUSY, "transferring"));
        }
        this.answers.addAll(Arrays.asList(new Status(RaftError.EAGAIN, "no commit yet"),
                                          new Status(RaftError.EBUSY, "transferring")));

        long start = System.nanoTime();
        RaftEngine.getInstance().waitReadIndex(5000L);
        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

        Assert.assertTrue("Returned after " + elapsedMs + " ms", elapsedMs < 500L);
        verify(this.mockNode, times(7)).readIndex(any(byte[].class),
                                                  any(ReadIndexClosure.class));
    }

    @Test
    public void testReadIndexInTermReturnsTheLeaderTerm() throws PDException {
        this.leaderTerm.set(5L);

        Assert.assertEquals(5L, RaftEngine.getInstance().waitReadIndexInTerm());

        verify(this.mockNode, times(1)).readIndex(any(byte[].class),
                                                  any(ReadIndexClosure.class));
    }

    @Test
    public void testReadIndexInTermKeepsTheTermTakenBeforeTheWait() throws PDException {
        // The node loses the leadership of term 5 and wins term 7 during the wait. The
        // caller must get 5: its write is then rejected, as the wait began in another term
        this.leaderTerm.set(5L);
        doAnswer(invocation -> {
            this.leaderTerm.set(7L);
            ReadIndexClosure closure = invocation.getArgument(1);
            closure.setResult(42L, invocation.getArgument(0));
            closure.run(Status.OK());
            return null;
        }).when(this.mockNode).readIndex(any(byte[].class), any(ReadIndexClosure.class));

        Assert.assertEquals(5L, RaftEngine.getInstance().waitReadIndexInTerm());
    }

    @Test
    public void testReadIndexInTermWaitsAgainOnceANewLeaderIsAnnounced() throws PDException {
        // Elected, but not announced before the first entry of the term is applied: the
        // first wait ends after that, the second one runs inside the announced term
        this.leaderTerm.set(-1L);
        doAnswer(invocation -> {
            this.leaderTerm.set(4L);
            ReadIndexClosure closure = invocation.getArgument(1);
            closure.setResult(42L, invocation.getArgument(0));
            closure.run(Status.OK());
            return null;
        }).when(this.mockNode).readIndex(any(byte[].class), any(ReadIndexClosure.class));

        Assert.assertEquals(4L, RaftEngine.getInstance().waitReadIndexInTerm());

        verify(this.mockNode, times(2)).readIndex(any(byte[].class),
                                                  any(ReadIndexClosure.class));
    }

    @Test
    public void testReadIndexInTermFailsAsNotLeaderOnAFollower() {
        // A follower's ReadIndex succeeds through the leader, but it has no term to write in
        this.leaderTerm.set(-1L);

        PDException e = Assert.assertThrows(PDException.class, () -> {
            RaftEngine.getInstance().waitReadIndexInTerm();
        });

        Assert.assertEquals(Pdpb.ErrorType.NOT_LEADER_VALUE, e.getErrorCode());
        verify(this.mockNode, times(1)).readIndex(any(byte[].class),
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
    public void testEveryTimeoutReportsHowLongTheWaitWas() {
        // No time left when the ReadIndex would be sent
        PDException e = Assert.assertThrows(PDException.class, () -> {
            RaftEngine.getInstance().waitReadIndex(0L);
        });
        Assert.assertEquals("Raft read index timed out after 0 ms", e.getMessage());
        verify(this.mockNode, never()).readIndex(any(byte[].class),
                                                 any(ReadIndexClosure.class));

        // No answer: the closure's own timer runs it with ETIMEDOUT about when the wait ends
        this.answers.add(NO_ANSWER);
        e = Assert.assertThrows(PDException.class, () -> {
            RaftEngine.getInstance().waitReadIndex(100L);
        });
        Assert.assertEquals("Raft read index timed out after 100 ms", e.getMessage());
    }

    @Test
    public void testOneReadIndexMayWaitPastTheJraftDefaultTimeout() {
        // No answer at all: the wait lasts until its own deadline. A closure created without
        // a timeout passes this too, since its ETIMEDOUT at 2 s is left to the waiter; the
        // next test tells the two apart
        this.answers.add(NO_ANSWER);

        long start = System.nanoTime();
        Assert.assertThrows(PDException.class, () -> {
            RaftEngine.getInstance().waitReadIndex(2600L);
        });
        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        Assert.assertTrue("Failed after " + elapsedMs + " ms", elapsedMs >= 2400L);
    }

    @Test
    public void testAnswerAfterTheJraftDefaultTimeoutStillCompletesTheWait()
            throws PDException {
        // The leader answers at 2.5 s, after the 2 s default timer of a ReadIndexClosure
        // created without a timeout and well before the 5 s deadline of this wait. A closure
        // that timed out drops the answer, and the wait would fail at its deadline.
        ScheduledExecutorService leader = Executors.newSingleThreadScheduledExecutor();
        try {
            doAnswer(invocation -> {
                ReadIndexClosure closure = invocation.getArgument(1);
                byte[] context = invocation.getArgument(0);
                leader.schedule(() -> {
                    closure.setResult(42L, context);
                    closure.run(Status.OK());
                }, 2500L, TimeUnit.MILLISECONDS);
                return null;
            }).when(this.mockNode).readIndex(any(byte[].class), any(ReadIndexClosure.class));

            long start = System.nanoTime();
            RaftEngine.getInstance().waitReadIndex(5000L);
            long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            Assert.assertTrue("Returned after " + elapsedMs + " ms", elapsedMs >= 2400L);
        } finally {
            leader.shutdownNow();
        }
    }

    @Test
    public void testNodeShuttingDownFailsAsNotLeader() {
        doThrow(new IllegalStateException("Node is shutting down"))
                .when(this.mockNode).readIndex(any(byte[].class), any(ReadIndexClosure.class));

        PDException e = Assert.assertThrows(PDException.class, () -> {
            RaftEngine.getInstance().waitReadIndex(1000L);
        });

        Assert.assertEquals(Pdpb.ErrorType.NOT_LEADER_VALUE, e.getErrorCode());
    }

    @Test
    public void testShutdownClosureThatRunsFirstFailsAsNotLeader() {
        // jraft runs the closure with ENODESHUTDOWN on another thread and then throws;
        // here the closure always completes the wait before the exception is caught
        this.answerShutdown(RaftError.ENODESHUTDOWN, "Node is shutting down");

        PDException e = Assert.assertThrows(PDException.class, () -> {
            RaftEngine.getInstance().waitReadIndex(1000L);
        });

        Assert.assertEquals(Pdpb.ErrorType.NOT_LEADER_VALUE, e.getErrorCode());
    }

    @Test
    public void testStoppedReadServiceClosureThatRunsFirstFailsAsNotLeader() {
        // The read service fails the closure with EHOSTDOWN before it throws
        this.answerShutdown(RaftError.EHOSTDOWN, "Service already shutdown.");

        PDException e = Assert.assertThrows(PDException.class, () -> {
            RaftEngine.getInstance().waitReadIndex(1000L);
        });

        Assert.assertEquals(Pdpb.ErrorType.NOT_LEADER_VALUE, e.getErrorCode());
    }

    private void answerShutdown(RaftError error, String message) {
        doAnswer(invocation -> {
            ReadIndexClosure closure = invocation.getArgument(1);
            closure.setResult(ReadIndexClosure.INVALID_LOG_INDEX, invocation.getArgument(0));
            closure.run(new Status(error, message));
            throw new IllegalStateException(message);
        }).when(this.mockNode).readIndex(any(byte[].class), any(ReadIndexClosure.class));
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
