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

package org.apache.hugegraph.store.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hugegraph.store.HgStoreSession;
import org.apache.hugegraph.store.client.type.HgStoreClientException;
import org.junit.Test;

import io.grpc.Status;

public class NodeTxExecutorTest {

    @Test
    public void testRetryReplacesSessionFromEvictedNode() {
        long nodeId = 1L;
        HgStoreNode oldNode = mock(HgStoreNode.class);
        HgStoreNode currentNode = mock(HgStoreNode.class);
        HgStoreNodeSession oldSession = mock(HgStoreNodeSession.class);
        HgStoreNodeSession currentSession = mock(HgStoreNodeSession.class);

        when(oldNode.getNodeId()).thenReturn(nodeId);
        when(currentNode.getNodeId()).thenReturn(nodeId);
        when(oldNode.openSession("graph")).thenReturn(oldSession);
        when(currentNode.openSession("graph")).thenReturn(currentSession);
        when(oldSession.getStoreNode()).thenReturn(oldNode);
        when(currentSession.getStoreNode()).thenReturn(currentNode);

        NodeTxExecutor executor = NodeTxExecutor.graphOf("graph", null);
        executor.setTx(true);
        AtomicInteger attempts = new AtomicInteger();
        Optional<HgStoreSession> result = executor.retryingInvoke(() -> {
            HgStoreNode node = attempts.getAndIncrement() == 0 ? oldNode : currentNode;
            HgStoreSession session = executor.openNodeSession(node);
            if (node == oldNode) {
                throw new RuntimeException("simulated transport failure");
            }
            return session;
        });

        assertSame(currentSession, result.get());
        verify(oldSession).beginTx();
        verify(currentSession).beginTx();
    }

    @Test
    public void testParallelReplacementUsesOneCurrentSession() throws Exception {
        long nodeId = 2L;
        HgStoreNode oldNode = mock(HgStoreNode.class);
        HgStoreNode currentNode = mock(HgStoreNode.class);
        HgStoreNodeSession oldSession = mock(HgStoreNodeSession.class);
        HgStoreNodeSession firstCurrentSession = mock(HgStoreNodeSession.class);
        HgStoreNodeSession secondCurrentSession = mock(HgStoreNodeSession.class);

        when(oldNode.getNodeId()).thenReturn(nodeId);
        when(currentNode.getNodeId()).thenReturn(nodeId);
        when(oldNode.openSession("graph")).thenReturn(oldSession);
        when(oldSession.getStoreNode()).thenReturn(oldNode);
        when(firstCurrentSession.getStoreNode()).thenReturn(currentNode);
        when(secondCurrentSession.getStoreNode()).thenReturn(currentNode);

        CountDownLatch concurrentCreations = new CountDownLatch(2);
        AtomicInteger creations = new AtomicInteger();
        when(currentNode.openSession("graph")).thenAnswer(invocation -> {
            int creation = creations.getAndIncrement();
            concurrentCreations.countDown();
            concurrentCreations.await(1, TimeUnit.SECONDS);
            return creation == 0 ? firstCurrentSession : secondCurrentSession;
        });

        NodeTxExecutor executor = NodeTxExecutor.graphOf("graph", null);
        executor.openNodeSession(oldNode);

        ExecutorService workers = Executors.newFixedThreadPool(2);
        CountDownLatch start = new CountDownLatch(1);
        Future<HgStoreSession> first = workers.submit(() -> {
            start.await();
            return executor.openNodeSession(currentNode);
        });
        Future<HgStoreSession> second = workers.submit(() -> {
            start.await();
            return executor.openNodeSession(currentNode);
        });
        try {
            start.countDown();
            assertSame(first.get(3, TimeUnit.SECONDS),
                       second.get(3, TimeUnit.SECONDS));
            verify(currentNode, times(1)).openSession("graph");
        } finally {
            workers.shutdownNow();
        }
    }

    @Test
    public void testClassifyFailures() {
        assertEquals(NodeTxExecutor.Failure.DEADLINE,
                     NodeTxExecutor.classify(Status.DEADLINE_EXCEEDED.asRuntimeException()));
        assertEquals(NodeTxExecutor.Failure.FATAL,
                     NodeTxExecutor.classify(Status.CANCELLED.asRuntimeException()));
        assertEquals(NodeTxExecutor.Failure.FATAL,
                     NodeTxExecutor.classify(new InterruptedException("interrupted")));
        // The status is usually wrapped by the time it reaches the retry loop
        assertEquals(NodeTxExecutor.Failure.DEADLINE, NodeTxExecutor.classify(
                HgStoreClientException.of("commit", new RuntimeException(
                        Status.DEADLINE_EXCEEDED.asRuntimeException()))));
        // The most severe class among the suppressed failures of a parallel commit wins
        HgStoreClientException mixed = HgStoreClientException.of(
                Status.UNAVAILABLE.asRuntimeException());
        mixed.addSuppressed(Status.DEADLINE_EXCEEDED.asRuntimeException());
        assertEquals(NodeTxExecutor.Failure.DEADLINE, NodeTxExecutor.classify(mixed));
        mixed.addSuppressed(Status.CANCELLED.asRuntimeException());
        assertEquals(NodeTxExecutor.Failure.FATAL, NodeTxExecutor.classify(mixed));
        assertEquals(NodeTxExecutor.Failure.RETRYABLE,
                     NodeTxExecutor.classify(Status.UNAVAILABLE.asRuntimeException()));
        assertEquals(NodeTxExecutor.Failure.RETRYABLE,
                     NodeTxExecutor.classify(new RuntimeException("simulated transport failure")));
    }

    @Test
    public void testDeadlineExceededIsRetriedExactlyOnce() {
        NodeTxExecutor executor = NodeTxExecutor.graphOf("graph", null);
        AtomicInteger attempts = new AtomicInteger();
        HgStoreClientException e = assertThrows(HgStoreClientException.class, () ->
                executor.retryingInvoke(() -> {
                    attempts.incrementAndGet();
                    throw Status.DEADLINE_EXCEEDED.withDescription("deadline exceeded after 20s")
                                                  .asRuntimeException();
                }));
        assertEquals(2, attempts.get());
        assertTrue(e.getMessage(), e.getMessage().contains("DEADLINE_EXCEEDED"));
    }

    @Test
    public void testDeadlineThenNewLeaderSucceeds() {
        // the failed RPC invalidates the partition cache; the single retry reaches the new leader
        NodeTxExecutor executor = NodeTxExecutor.graphOf("graph", null);
        AtomicInteger attempts = new AtomicInteger();
        Optional<String> result = executor.retryingInvoke(() -> {
            if (attempts.getAndIncrement() == 0) {
                throw Status.DEADLINE_EXCEEDED.asRuntimeException();
            }
            return "ok";
        });
        assertEquals("ok", result.get());
        assertEquals(2, attempts.get());
    }

    @Test
    public void testInterruptStopsRetrying() {
        // A REST worker hitting restserver.request_timeout (or a Gremlin evaluationTimeout)
        // interrupts the calling thread while the store call is failing; the loop must
        // stop instead of sleeping and retrying with the interrupt swallowed.
        NodeTxExecutor executor = NodeTxExecutor.graphOf("graph", null);
        AtomicInteger attempts = new AtomicInteger();
        try {
            HgStoreClientException e = assertThrows(HgStoreClientException.class, () ->
                    executor.retryingInvoke(() -> {
                        attempts.incrementAndGet();
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("simulated transport failure");
                    }));
            assertEquals(1, attempts.get());
            assertTrue("interrupt flag must be restored for the caller",
                       Thread.currentThread().isInterrupted());
            // HugeException.isInterrupted() looks at the root cause
            assertTrue(rootCause(e) instanceof InterruptedException);
            assertEquals("simulated transport failure",
                         rootCause(e).getSuppressed()[0].getMessage());
        } finally {
            // clear the flag so the test runner thread is not left interrupted
            Thread.interrupted();
        }
    }

    private static Throwable rootCause(Throwable t) {
        while (t.getCause() != null && t.getCause() != t) {
            t = t.getCause();
        }
        return t;
    }

    @Test
    public void testTransientFailureIsStillRetried() {
        NodeTxExecutor executor = NodeTxExecutor.graphOf("graph", null);
        AtomicInteger attempts = new AtomicInteger();
        Optional<String> result = executor.retryingInvoke(() -> {
            if (attempts.getAndIncrement() == 0) {
                throw Status.UNAVAILABLE.withDescription("store replaced").asRuntimeException();
            }
            return "ok";
        });
        assertEquals("ok", result.get());
        assertEquals(2, attempts.get());
    }

    @Test
    public void testInterruptBeforeCallSkipsTheAttempt() {
        // restserver.request_timeout expired between two store calls of one request
        NodeTxExecutor executor = NodeTxExecutor.graphOf("graph", null);
        AtomicInteger attempts = new AtomicInteger();
        try {
            Thread.currentThread().interrupt();
            HgStoreClientException e = assertThrows(HgStoreClientException.class, () ->
                    executor.retryingInvoke(() -> {
                        attempts.incrementAndGet();
                        return "ok";
                    }));
            assertEquals(0, attempts.get());
            assertTrue(Thread.currentThread().isInterrupted());
            assertTrue(rootCause(e) instanceof InterruptedException);
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    public void testMixedCommitFailuresAreRetriedExactlyOnce() {
        // One partition on a store being replaced (UNAVAILABLE, retryable), one on a stalled
        // store (DEADLINE_EXCEEDED). Whichever finishes first, the deadline decides: one retry.
        HgStoreSession unavailable = mock(HgStoreSession.class);
        HgStoreSession stalled = mock(HgStoreSession.class);
        when(unavailable.isTx()).thenReturn(true);
        when(stalled.isTx()).thenReturn(true);
        // fresh exceptions on every attempt, as the real gRPC calls produce them
        org.mockito.Mockito.doAnswer(i -> {
            throw new RuntimeException(HgStoreClientException.of(
                    "commit", Status.UNAVAILABLE.asRuntimeException()));
        }).when(unavailable).commit();
        org.mockito.Mockito.doAnswer(i -> {
            throw new RuntimeException(HgStoreClientException.of(
                    "commit", Status.DEADLINE_EXCEEDED.asRuntimeException()));
        }).when(stalled).commit();
        List<HgStoreSession> sessions = Arrays.asList(unavailable, stalled);

        NodeTxExecutor executor = NodeTxExecutor.graphOf("graph", null);
        executor.setTx(true);
        HgStoreClientException e = assertThrows(HgStoreClientException.class,
                                                () -> executor.commitSessions(sessions));
        assertEquals(1, e.getSuppressed().length);
        assertEquals(NodeTxExecutor.Failure.DEADLINE, NodeTxExecutor.classify(e));
        verify(unavailable).rollback();
        verify(stalled).rollback();

        AtomicInteger attempts = new AtomicInteger();
        assertThrows(HgStoreClientException.class, () ->
                executor.retryingInvoke(() -> {
                    attempts.incrementAndGet();
                    executor.commitSessions(sessions);
                    return true;
                }));
        assertEquals(2, attempts.get());
    }

    @Test
    public void testAllRetryableCommitFailuresAreRetried() {
        HgStoreSession a = mock(HgStoreSession.class);
        HgStoreSession b = mock(HgStoreSession.class);
        when(a.isTx()).thenReturn(true);
        when(b.isTx()).thenReturn(true);
        org.mockito.Mockito.doThrow(new RuntimeException(Status.UNAVAILABLE.asRuntimeException()))
                .when(a).commit();
        org.mockito.Mockito.doThrow(new RuntimeException(Status.UNAVAILABLE.asRuntimeException()))
                .when(b).commit();
        NodeTxExecutor executor = NodeTxExecutor.graphOf("graph", null);
        HgStoreClientException e = assertThrows(HgStoreClientException.class,
                                                () -> executor.commitSessions(Arrays.asList(a, b)));
        assertEquals(1, e.getSuppressed().length);
        assertEquals(NodeTxExecutor.Failure.RETRYABLE, NodeTxExecutor.classify(e));
    }

    @Test
    public void testDeadlineBudgetResetsAfterAnotherFailure() {
        // deadline, then a transport error (leaders reloaded again), then a deadline:
        // the two deadlines are not "in a row", so the second one is retried as well
        NodeTxExecutor executor = NodeTxExecutor.graphOf("graph", null);
        AtomicInteger attempts = new AtomicInteger();
        Optional<String> result = executor.retryingInvoke(() -> {
            switch (attempts.getAndIncrement()) {
                case 0:
                case 2:
                    throw Status.DEADLINE_EXCEEDED.asRuntimeException();
                case 1:
                    throw Status.UNAVAILABLE.asRuntimeException();
                default:
                    return "ok";
            }
        });
        assertEquals("ok", result.get());
        assertEquals(4, attempts.get());
    }
}
