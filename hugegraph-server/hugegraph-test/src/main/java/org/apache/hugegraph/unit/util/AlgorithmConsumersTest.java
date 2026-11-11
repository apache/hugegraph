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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hugegraph.job.algorithm.Consumers;
import org.apache.hugegraph.testutil.Assert;
import org.junit.Test;

public class AlgorithmConsumersTest {

    @Test(timeout = 10000)
    public void testProcessesAllItemsAndRunsDoneOncePerWorker() throws Throwable {
        for (int workers : new int[]{0, 2}) {
            ExecutorService executor = workers == 0 ? null : Executors.newFixedThreadPool(workers);
            AtomicInteger processed = new AtomicInteger();
            AtomicInteger completed = new AtomicInteger();
            try {
                Consumers<Integer> consumers = new Consumers<>(executor, processed::addAndGet,
                                                               completed::incrementAndGet);
                consumers.start("algorithm-normal");
                for (int i = 0; i < 50; i++) {
                    consumers.provide(1);
                }
                consumers.await();
                Assert.assertEquals(50, processed.get());
                Assert.assertEquals(Math.max(1, workers), completed.get());
            } finally {
                if (executor != null) {
                    executor.shutdownNow();
                    Assert.assertTrue(executor.awaitTermination(5L, TimeUnit.SECONDS));
                }
            }
        }
    }

    @Test(timeout = 10000)
    public void testAwaitReportsLastWorkerFailure() throws Throwable {
        ExecutorService worker = Executors.newFixedThreadPool(1);
        CountDownLatch release = new CountDownLatch(1);
        RuntimeException failure = new IllegalStateException("last item failed");
        AtomicInteger completed = new AtomicInteger();
        try {
            Consumers<Integer> consumers = new Consumers<>(worker, value -> {
                awaitGate(release);
                throw failure;
            }, completed::incrementAndGet);
            consumers.start("algorithm-last-item-failure");
            consumers.provide(1);
            release.countDown();
            Assert.assertSame(failure, Assert.assertThrows(IllegalStateException.class, consumers::await));
            Assert.assertEquals(1, completed.get());
        } finally {
            release.countDown();
            worker.shutdownNow();
            Assert.assertTrue(worker.awaitTermination(5L, TimeUnit.SECONDS));
        }
    }

    @Test(timeout = 10000)
    public void testCommitFailureIsReportedWithoutRetryingDone() throws Exception {
        ExecutorService worker = Executors.newFixedThreadPool(1);
        AtomicInteger completed = new AtomicInteger();
        RuntimeException failure = new IllegalStateException("commit failed");
        try {
            Consumers<Integer> consumers = new Consumers<>(worker, value -> { }, () -> {
                completed.incrementAndGet();
                throw failure;
            });
            consumers.start("algorithm-commit-failure");
            Assert.assertSame(failure, Assert.assertThrows(IllegalStateException.class, consumers::await));
            Assert.assertEquals(1, completed.get());
        } finally {
            worker.shutdownNow();
            Assert.assertTrue(worker.awaitTermination(5L, TimeUnit.SECONDS));
        }
    }

    @Test(timeout = 10000)
    public void testStoppedWorkerReleasesBlockedProducerWithAlgorithmStopType() throws Throwable {
        this.checkBlockedProducer(true);
    }

    @Test(timeout = 10000)
    public void testFailedWorkerReleasesBlockedProducerWithOriginalFailure() throws Throwable {
        this.checkBlockedProducer(false);
    }

    private void checkBlockedProducer(boolean stop) throws Throwable {
        ExecutorService worker = Executors.newFixedThreadPool(1);
        ExecutorService caller = Executors.newSingleThreadExecutor();
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        RuntimeException failure = stop ? new Consumers.StopExecution("limit") :
                                         new IllegalStateException("worker failed");
        AtomicInteger processed = new AtomicInteger();
        try {
            Consumers<Integer> consumers = new Consumers<>(worker, value -> {
                processed.incrementAndGet();
                entered.countDown();
                awaitGate(release);
                throw failure;
            });
            consumers.start("algorithm-full-queue");
            consumers.provide(1);
            Assert.assertTrue(entered.await(5L, TimeUnit.SECONDS));
            for (int i = 0; i <= Consumers.QUEUE_WORKER_SIZE; i++) {
                consumers.provide(2);
            }
            Future<Throwable> blocked = caller.submit(() -> {
                try {
                    consumers.provide(3);
                    return null;
                } catch (Throwable e) {
                    return e;
                }
            });
            Assert.assertThrows(TimeoutException.class, () -> blocked.get(100L, TimeUnit.MILLISECONDS));
            release.countDown();
            Throwable result = blocked.get(5L, TimeUnit.SECONDS);
            if (stop) {
                Assert.assertTrue(result instanceof Consumers.StopExecution);
                consumers.await();
            } else {
                Assert.assertSame(failure, result);
                Assert.assertSame(failure, Assert.assertThrows(IllegalStateException.class, consumers::await));
            }
            Assert.assertEquals(1, processed.get());
        } finally {
            release.countDown();
            worker.shutdownNow();
            caller.shutdownNow();
            Assert.assertTrue(worker.awaitTermination(5L, TimeUnit.SECONDS));
            Assert.assertTrue(caller.awaitTermination(5L, TimeUnit.SECONDS));
        }
    }

    private static void awaitGate(CountDownLatch gate) {
        try {
            Assert.assertTrue(gate.await(5L, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
    }
}
