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

package org.apache.hugegraph.unit.util;

import java.util.Arrays;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hugegraph.exception.HugeException;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.util.Consumers;
import org.junit.Test;

public class ConsumersTest {

    @Test(timeout = 1000)
    public void testStartProvideAwaitNormal() throws Throwable {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            AtomicInteger processed = new AtomicInteger();

            Consumers<Integer> consumers = new Consumers<>(executor, v -> {
                processed.incrementAndGet();
            });

            consumers.start("test");
            for (int i = 0; i < 50; i++) {
                consumers.provide(i);
            }
            consumers.await();

            Assert.assertEquals("Should process all provided elements",
                                50, processed.get());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test(timeout = 1000)
    public void testAwaitPreservesFatalConsumerError() throws Throwable {
        ExecutorService executor = Executors.newFixedThreadPool(1);
        CountDownLatch release = new CountDownLatch(1);
        AssertionError failure = new AssertionError("fatal consumer failure");
        try {
            Consumers<Integer> consumers = new Consumers<>(executor, v -> {
                try {
                    if (!release.await(5L, TimeUnit.SECONDS)) {
                        throw new AssertionError("Consumer was not released");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(e);
                }
                throw failure;
            });
            consumers.start("test-fatal-error");
            consumers.provide(1);
            release.countDown();
            Assert.assertSame(failure, Assert.assertThrows(AssertionError.class, consumers::await));
        } finally {
            release.countDown();
            executor.shutdownNow();
            Assert.assertTrue(executor.awaitTermination(5L, TimeUnit.SECONDS));
        }
    }

    @Test(timeout = 1000)
    public void testAwaitThrowsWhenConsumerThrows() throws Throwable {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch release = new CountDownLatch(1);
        try {
            final String msg = "Injected exception for test";

            Consumers<Integer> consumers = new Consumers<>(executor, v -> {
                awaitGate(release);
                throw new RuntimeException(msg);
            });

            consumers.start("test");
            consumers.provide(1);
            release.countDown();

            try {
                consumers.await();
                Assert.fail("Expected await() to throw when consumer throws");
            } catch (Throwable t) {
                Throwable root = t.getCause() != null ? t.getCause() : t;
                Assert.assertTrue("Expected RuntimeException, but got: " + root,
                                  root instanceof RuntimeException);
                Assert.assertTrue("Exception message should contain injected message",
                                  root.getMessage() != null &&
                                  root.getMessage().contains(msg));
            }
        } finally {
            release.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    public void testStopWithFullQueueAllowsAwait() throws Exception {
        this.checkStopWithFullQueue(false, false);
    }

    @Test
    public void testStopWithFullQueueReleasesProducer() throws Exception {
        this.checkStopWithFullQueue(true, false);
    }

    @Test
    public void testStopCleanupFailureWithFullQueueAllowsAwait() throws Exception {
        this.checkStopWithFullQueue(false, true);
    }

    @Test
    public void testStopCleanupFailureWithFullQueueReleasesProducer() throws Exception {
        this.checkStopWithFullQueue(true, true);
    }

    @Test
    public void testBusinessFailureWithFullQueueReleasesProducer() throws Exception {
        this.checkTerminalWithFullQueue(true, false, new IllegalStateException("business failure"));
    }

    @Test
    public void testFatalErrorWithFullQueueReleasesProducer() throws Exception {
        this.checkTerminalWithFullQueue(true, false, new AssertionError("fatal failure"));
    }

    private void checkStopWithFullQueue(boolean blockedProducer, boolean cleanupFailure) throws Exception {
        this.checkTerminalWithFullQueue(blockedProducer, cleanupFailure, null);
    }

    private void checkTerminalWithFullQueue(boolean blockedProducer, boolean cleanupFailure,
                                            Throwable terminalFailure) throws Exception {
        ExecutorService worker = Executors.newFixedThreadPool(1);
        ExecutorService caller = Executors.newSingleThreadExecutor();
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger processed = new AtomicInteger();
        AtomicInteger done = new AtomicInteger();
        CountDownLatch finished = new CountDownLatch(1);
        RuntimeException closeFailure = new IllegalStateException("cleanup failure");
        try {
            Consumers<Integer> consumers = new Consumers<>(worker, value -> {
                processed.incrementAndGet();
                entered.countDown();
                try {
                    if (!release.await(5L, TimeUnit.SECONDS)) {
                        throw new AssertionError("Consumer was not released");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(e);
                }
                if (terminalFailure instanceof Error matchedError) {
                    throw matchedError;
                } else if (terminalFailure != null) {
                    throw (RuntimeException) terminalFailure;
                }
                Consumers.StopExecution stop = new Consumers.StopExecution("capacity");
                if (cleanupFailure) {
                    stop.addSuppressed(closeFailure);
                }
                throw stop;
            }, () -> {
                done.incrementAndGet();
                finished.countDown();
            }, 1);
            consumers.start("full-queue-stop");
            try {
                consumers.provide(1);
                Assert.assertTrue(entered.await(5L, TimeUnit.SECONDS));
                consumers.provide(2);
                consumers.provide(3);
            } catch (Throwable failure) {
                throw new AssertionError(failure);
            }
            CountDownLatch calling = new CountDownLatch(1);
            Future<Throwable> result = caller.submit(() -> {
                calling.countDown();
                try {
                    if (blockedProducer) {
                        consumers.provide(4);
                    }
                    consumers.await();
                    return null;
                } catch (Throwable failure) {
                    return failure;
                }
            });
            Assert.assertTrue(calling.await(5L, TimeUnit.SECONDS));
            Assert.assertThrows(TimeoutException.class, () -> result.get(100L, TimeUnit.MILLISECONDS));
            release.countDown();
            Throwable returned = result.get(3L, TimeUnit.SECONDS);
            if (blockedProducer && !cleanupFailure && terminalFailure == null) {
                Assert.assertTrue(returned instanceof Consumers.StopExecution);
            } else {
                Assert.assertSame(cleanupFailure ? closeFailure : terminalFailure, returned);
            }
            Assert.assertTrue(finished.await(5L, TimeUnit.SECONDS));
            if (blockedProducer && (cleanupFailure || terminalFailure != null)) {
                // A failure reported by provide() must not leave await() blocked.
                Future<Throwable> joined = caller.submit(() -> {
                    try {
                        consumers.await();
                        return null;
                    } catch (Throwable failure) {
                        return failure;
                    }
                });
                Assert.assertSame(cleanupFailure ? closeFailure : terminalFailure,
                                  joined.get(3L, TimeUnit.SECONDS));
            }
            Assert.assertEquals(1, processed.get());
            Assert.assertEquals(1, done.get());
        } finally {
            release.countDown();
            caller.shutdownNow();
            worker.shutdownNow();
            for (int i = 0; i < 50 && !caller.awaitTermination(100L, TimeUnit.MILLISECONDS); i++) {
                caller.shutdownNow();
            }
            Assert.assertTrue(caller.isTerminated());
            Assert.assertTrue(worker.awaitTermination(5L, TimeUnit.SECONDS));
        }
    }
    @Test
    public void testProducerFailureRemainsPrimaryAfterCleanupFailure() throws Exception {
        this.checkProducerFailureAfterCleanup(1);
    }

    @Test
    public void testConcurrentCleanupFailuresRemainSuppressed() throws Exception {
        this.checkProducerFailureAfterCleanup(2);
    }

    private void checkProducerFailureAfterCleanup(int workers) throws Exception {
        ExecutorService worker = Executors.newFixedThreadPool(workers);
        ExecutorService caller = Executors.newSingleThreadExecutor();
        CountDownLatch entered = new CountDownLatch(workers);
        CountDownLatch releaseFailure = new CountDownLatch(1);
        CountDownLatch observed = new CountDownLatch(1);
        AtomicInteger done = new AtomicInteger();
        RuntimeException business = new IllegalStateException("business failure");
        RuntimeException[] cleanup = new RuntimeException[workers];
        for (int i = 0; i < workers; i++) {
            cleanup[i] = new IllegalStateException("cleanup failure " + i);
        }
        try {
            Consumers<Integer> consumers = new Consumers<>(worker, value -> {
                entered.countDown();
                awaitGate(releaseFailure);
                throw business;
            }, () -> {
                int index = done.getAndIncrement();
                // Cleanup fails strictly after the blocked producer observes business.
                awaitGate(observed);
                throw cleanup[index];
            }, 1);
            consumers.start("primary-cleanup-race");
            try {
                for (int i = 0; i < workers; i++) {
                    consumers.provide(i);
                }
                Assert.assertTrue(entered.await(5L, TimeUnit.SECONDS));
                for (int i = 0; i < workers + 1; i++) {
                    consumers.provide(workers + i);
                }
            } catch (Throwable failure) {
                throw new AssertionError(failure);
            }
            CountDownLatch calling = new CountDownLatch(1);
            Future<Throwable> provided = caller.submit(() -> {
                calling.countDown();
                try {
                    consumers.provide(2 * workers + 1);
                    return null;
                } catch (Throwable failure) {
                    return failure;
                }
            });
            Assert.assertTrue(calling.await(5L, TimeUnit.SECONDS));
            Assert.assertThrows(TimeoutException.class, () -> provided.get(100L, TimeUnit.MILLISECONDS));
            releaseFailure.countDown();
            Assert.assertSame(business, provided.get(3L, TimeUnit.SECONDS));
            observed.countDown();
            Future<Throwable> awaited = caller.submit(() -> {
                try {
                    consumers.await();
                    return null;
                } catch (Throwable failure) {
                    return failure;
                }
            });
            Assert.assertSame(business, awaited.get(3L, TimeUnit.SECONDS));
            Assert.assertEquals(workers, business.getSuppressed().length);
            for (RuntimeException failure : cleanup) {
                Assert.assertTrue(Arrays.asList(business.getSuppressed()).contains(failure));
            }
            Assert.assertEquals(workers, done.get());
        } finally {
            releaseFailure.countDown();
            observed.countDown();
            caller.shutdownNow();
            worker.shutdownNow();
            Assert.assertTrue(caller.awaitTermination(5L, TimeUnit.SECONDS));
            Assert.assertTrue(worker.awaitTermination(5L, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testCleanStopEndsLazyProducer() throws Exception {
        ExecutorService worker = Executors.newFixedThreadPool(1);
        CountDownLatch stopped = new CountDownLatch(1);
        AtomicInteger reads = new AtomicInteger();
        AtomicInteger processed = new AtomicInteger();
        try {
            Consumers<Integer> consumers = new Consumers<>(worker, value -> {
                processed.incrementAndGet();
                throw new Consumers.StopExecution("capacity");
            }, stopped::countDown, 1);
            consumers.start("lazy-producer-stop");
            try {
                for (int i = 0; i < 100; i++) {
                    if (i == 1) {
                        Assert.assertTrue(stopped.await(5L, TimeUnit.SECONDS));
                    }
                    reads.incrementAndGet();
                    consumers.provide(i);
                }
            } catch (Consumers.StopExecution ignored) {
                // Same successful producer termination contract as Oltp traversal.
            } catch (Throwable failure) {
                throw new AssertionError(failure);
            } finally {
                try {
                    consumers.await();
                } catch (Throwable failure) {
                    throw new AssertionError(failure);
                }
            }
            Assert.assertEquals(2, reads.get());
            Assert.assertEquals(1, processed.get());
        } finally {
            worker.shutdownNow();
            Assert.assertTrue(worker.awaitTermination(5L, TimeUnit.SECONDS));
        }
    }

    @Test(timeout = 15000)
    public void testAwaitPreservesCallerInterruptAndCancelsWorker() throws Throwable {
        this.checkAwaitInterrupt(false);
    }

    @Test(timeout = 15000)
    public void testAwaitInterruptedWhileQueueingEndCancelsWorker() throws Throwable {
        this.checkAwaitInterrupt(true);
    }

    private void checkAwaitInterrupt(boolean fullQueue) throws Throwable {
        ExecutorService worker = Executors.newFixedThreadPool(1);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch completed = new CountDownLatch(1);
        AtomicBoolean workerInterrupted = new AtomicBoolean();
        AtomicBoolean callerInterrupted = new AtomicBoolean();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Consumers<Integer> consumers = new Consumers<>(worker, value -> {
            entered.countDown();
            try {
                release.await();
            } catch (InterruptedException e) {
                workerInterrupted.set(true);
                Thread.currentThread().interrupt();
            }
        }, completed::countDown, 1);
        Thread caller = new Thread(() -> {
            try {
                consumers.await();
            } catch (Throwable e) {
                failure.set(e);
                callerInterrupted.set(Thread.currentThread().isInterrupted());
            }
        }, "consumer-await-interrupt-test");
        try {
            consumers.start("await-interrupt");
            consumers.provide(1);
            Assert.assertTrue(entered.await(5L, TimeUnit.SECONDS));
            if (fullQueue) {
                consumers.provide(2);
                consumers.provide(3);
            }
            caller.start();
            // Keep the worker blocked until the caller reaches the selected await phase.
            awaitCall(caller, fullQueue ? ArrayBlockingQueue.class : CountDownLatch.class,
                      fullQueue ? "offer" : "await");
            caller.interrupt();
            caller.join(5000L);
            Assert.assertFalse(caller.isAlive());
            Assert.assertTrue(failure.get() instanceof HugeException);
            Assert.assertTrue(failure.get().getCause() instanceof InterruptedException);
            Assert.assertTrue(callerInterrupted.get());
            Assert.assertTrue(completed.await(5L, TimeUnit.SECONDS));
            Assert.assertTrue(workerInterrupted.get());
            worker.shutdown();
            Assert.assertTrue(worker.awaitTermination(5L, TimeUnit.SECONDS));
        } finally {
            release.countDown();
            caller.interrupt();
            caller.join(5000L);
            worker.shutdownNow();
            Assert.assertTrue(worker.awaitTermination(5L, TimeUnit.SECONDS));
        }
    }

    @Test(timeout = 15000)
    public void testInterruptedProducerReportsUnacceptedInputAndCancelsOnAwait() throws Throwable {
        ExecutorService worker = Executors.newFixedThreadPool(1);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch completed = new CountDownLatch(1);
        AtomicInteger processed = new AtomicInteger();
        AtomicBoolean producerInterrupted = new AtomicBoolean();
        AtomicReference<Throwable> provideFailure = new AtomicReference<>();
        AtomicReference<Throwable> awaitFailure = new AtomicReference<>();
        Consumers<Integer> consumers = new Consumers<>(worker, value -> {
            processed.incrementAndGet();
            entered.countDown();
            try {
                release.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, completed::countDown, 1);
        Thread producer = new Thread(() -> {
            try {
                consumers.provide(4);
            } catch (Throwable e) {
                provideFailure.set(e);
                producerInterrupted.set(Thread.currentThread().isInterrupted());
            } finally {
                try {
                    consumers.await();
                } catch (Throwable e) {
                    awaitFailure.set(e);
                }
            }
        }, "consumer-provide-interrupt-test");
        try {
            consumers.start("provide-interrupt");
            consumers.provide(1);
            Assert.assertTrue(entered.await(5L, TimeUnit.SECONDS));
            consumers.provide(2);
            consumers.provide(3);
            producer.start();
            awaitCall(producer, ArrayBlockingQueue.class, "offer");
            producer.interrupt();
            producer.join(5000L);
            Assert.assertFalse(producer.isAlive());
            Assert.assertTrue(provideFailure.get() instanceof InterruptedException);
            Assert.assertTrue(producerInterrupted.get());
            Assert.assertTrue(awaitFailure.get() instanceof HugeException);
            Assert.assertTrue(completed.await(5L, TimeUnit.SECONDS));
            Assert.assertEquals(1, processed.get());
            worker.shutdown();
            Assert.assertTrue(worker.awaitTermination(5L, TimeUnit.SECONDS));
        } finally {
            release.countDown();
            producer.interrupt();
            producer.join(5000L);
            worker.shutdownNow();
            Assert.assertTrue(worker.awaitTermination(5L, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testWrapCheckedExceptionPreservesCause() {
        Exception original = new Exception("outer failure", new Exception("root failure"));
        HugeException wrapped = Assert.assertThrows(HugeException.class, () -> Consumers.wrapException(original));
        Assert.assertSame(original, wrapped.getCause());
        Assert.assertEquals("Error when running task: root failure", wrapped.getMessage());
    }

    @Test
    public void testWrapCheckedExceptionWithoutMessagePreservesCause() {
        InterruptedException original = new InterruptedException();
        HugeException wrapped = Assert.assertThrows(HugeException.class, () -> Consumers.wrapException(original));
        Assert.assertSame(original, wrapped.getCause());
        Assert.assertEquals("Error when running task: null", wrapped.getMessage());
    }

    @Test
    public void testWrapRuntimeExceptionPreservesIdentity() {
        for (IllegalStateException original : new IllegalStateException[]{
                new IllegalStateException("task failed"), new IllegalStateException()}) {
            Assert.assertSame(original, Assert.assertThrows(IllegalStateException.class,
                    () -> Consumers.wrapException(original)));
        }
    }

    private static void awaitCall(Thread thread, Class<?> type, String method) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5L);
        while (System.nanoTime() < deadline) {
            if (Arrays.stream(thread.getStackTrace()).anyMatch(frame ->
                    frame.getClassName().equals(type.getName()) && frame.getMethodName().equals(method))) {
                return;
            }
            Thread.sleep(1L);
        }
        Assert.fail("Thread did not reach " + type.getSimpleName() + "." + method);
    }

    private static void awaitGate(CountDownLatch gate) {
        try {
            if (!gate.await(5L, TimeUnit.SECONDS)) {
                throw new AssertionError("Gate was not released");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
    }

}
