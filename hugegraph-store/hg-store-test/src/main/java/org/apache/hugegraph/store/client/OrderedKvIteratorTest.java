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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hugegraph.store.HgKvEntry;
import org.apache.hugegraph.store.HgKvIterator;
import org.junit.Assert;
import org.junit.Test;

public class OrderedKvIteratorTest {

    @Test
    public void testMergeInterleavedSourcesByUnsignedKey() {
        TestIterator first = new TestIterator(1, 4);
        TestIterator second = new TestIterator(2, 3);
        OrderedKvIterator iterator = new OrderedKvIterator(
                Arrays.asList(first, second), 0L);

        Assert.assertEquals(Arrays.asList(1, 2, 3, 4), keys(iterator));
        Assert.assertArrayEquals(keyBytes(4), iterator.position());
        Assert.assertTrue(first.closed);
        Assert.assertTrue(second.closed);
    }

    @Test
    public void testMergeIsLazyAndStopsAtRawLimit() {
        TestIterator first = new TestIterator(1, 4, 5);
        TestIterator second = new TestIterator(2, 3, 6);
        OrderedKvIterator iterator = new OrderedKvIterator(
                Arrays.asList(first, second), 3L);

        Assert.assertEquals(0, first.nextCalls);
        Assert.assertEquals(0, second.nextCalls);

        Assert.assertEquals(Arrays.asList(1, 2, 3), keys(iterator));
        Assert.assertEquals(4, first.nextCalls + second.nextCalls);
        Assert.assertTrue(first.closed);
        Assert.assertTrue(second.closed);
    }

    @Test
    public void testMergeComparesKeysAsUnsignedBytes() {
        TestIterator first = new TestIterator(0x80);
        TestIterator second = new TestIterator(0x7f);
        OrderedKvIterator iterator = new OrderedKvIterator(
                Arrays.asList(first, second), 0L);

        Assert.assertEquals(Arrays.asList(0x7f, 0x80), keys(iterator));
    }

    @Test
    public void testMergeUsesStableSourceOrderForEqualKeys() {
        TestIterator first = new TestIterator(1);
        TestIterator second = new TestIterator(1);
        OrderedKvIterator iterator = new OrderedKvIterator(
                Arrays.asList(first, second), 0L);

        Assert.assertSame(first.entries.get(0), iterator.next());
        Assert.assertSame(second.entries.get(0), iterator.next());
        Assert.assertFalse(iterator.hasNext());
    }

    @Test
    public void testMergeClosesAllSourcesWhenAdvanceFails() {
        TestIterator first = new TestIterator(1, 3);
        TestIterator second = new TestIterator(2, 4);
        first.failOnHasNextAfter(1);
        OrderedKvIterator iterator = new OrderedKvIterator(
                Arrays.asList(first, second), 0L);

        Assert.assertThrows(IllegalStateException.class, iterator::next);
        Assert.assertTrue(first.closed);
        Assert.assertTrue(second.closed);
    }

    @Test
    public void testMergePrimesSourcesConcurrently() {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch bothStarted = new CountDownLatch(2);
        TestIterator first = new TestIterator(1);
        TestIterator second = new TestIterator(2);
        first.blockFirstHasNext(bothStarted);
        second.blockFirstHasNext(bothStarted);
        OrderedKvIterator iterator = new OrderedKvIterator(
                Arrays.asList(first, second), 0L, executor);
        try {
            Assert.assertTrue(iterator.hasNext());
            Assert.assertEquals(0L, bothStarted.getCount());
        } finally {
            iterator.close();
            executor.shutdownNow();
        }
    }

    @Test
    public void testMergeClosesAllSourcesWhenConcurrentInitializeFails() {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        TestIterator first = new TestIterator(1);
        TestIterator second = new TestIterator(2);
        first.failOnHasNextAfter(0);
        OrderedKvIterator iterator = new OrderedKvIterator(
                Arrays.asList(first, second), 0L, executor);
        try {
            Assert.assertThrows(IllegalStateException.class,
                                iterator::hasNext);
            Assert.assertTrue(first.closed);
            Assert.assertTrue(second.closed);
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testConcurrentInitializeFailsWithoutWaitingForSlowSource()
            throws Exception {
        ExecutorService initializer = Executors.newFixedThreadPool(2);
        ExecutorService caller = Executors.newSingleThreadExecutor();
        CountDownLatch slowStarted = new CountDownLatch(1);
        CountDownLatch releaseSlow = new CountDownLatch(1);
        TestIterator slow = new TestIterator(1);
        TestIterator failed = new TestIterator(2);
        slow.blockFirstHasNext(slowStarted, releaseSlow);
        failed.failOnHasNextAfter(0);

        Future<Boolean> result = caller.submit(() -> {
            OrderedKvIterator iterator = new OrderedKvIterator(
                    Arrays.asList(slow, failed), 0L, initializer);
            return iterator.hasNext();
        });
        try {
            Assert.assertTrue(slowStarted.await(3L, TimeUnit.SECONDS));
            try {
                result.get(3L, TimeUnit.SECONDS);
                Assert.fail("Expected initialization failure");
            } catch (ExecutionException e) {
                Assert.assertTrue(e.getCause() instanceof
                                  IllegalStateException);
            }
            Assert.assertTrue(slow.closed);
            Assert.assertTrue(failed.closed);
        } finally {
            releaseSlow.countDown();
            result.cancel(true);
            caller.shutdownNow();
            initializer.shutdownNow();
        }
    }

    @Test
    public void testInitializeDrainsFailureBeforeSubmittingNinthSource()
            throws Exception {
        int workers = 8;
        CountDownLatch firstEightStarted = new CountDownLatch(workers);
        CountDownLatch releaseFailure = new CountDownLatch(1);
        CountDownLatch releaseSlow = new CountDownLatch(1);
        CountDownLatch ninthStarted = new CountDownLatch(1);
        ExecutorService initializer = new ThreadPoolExecutor(
                0, workers, 60L, TimeUnit.SECONDS,
                new SynchronousQueue<>(),
                new ThreadPoolExecutor.CallerRunsPolicy());
        ExecutorService caller = Executors.newSingleThreadExecutor();
        List<TestIterator> sources = new ArrayList<>();
        TestIterator failed = new TestIterator(0);
        failed.blockAndFailFirstHasNext(firstEightStarted, releaseFailure);
        sources.add(failed);
        for (int i = 1; i < workers; i++) {
            TestIterator slow = new TestIterator(i);
            slow.blockFirstHasNext(firstEightStarted, releaseSlow);
            sources.add(slow);
        }
        TestIterator ninth = new TestIterator(workers);
        ninth.blockFirstHasNext(ninthStarted, releaseSlow);
        sources.add(ninth);

        Future<Boolean> result = caller.submit(() -> {
            OrderedKvIterator iterator = new OrderedKvIterator(
                    sources, 0L, initializer);
            return iterator.hasNext();
        });
        try {
            Assert.assertTrue(firstEightStarted.await(3L,
                                                       TimeUnit.SECONDS));
            Assert.assertFalse(ninthStarted.await(1L, TimeUnit.SECONDS));
            releaseFailure.countDown();
            try {
                result.get(3L, TimeUnit.SECONDS);
                Assert.fail("Expected initialization failure");
            } catch (ExecutionException e) {
                Assert.assertTrue(e.getCause() instanceof
                                  IllegalStateException);
            }
            Assert.assertEquals(1L, ninthStarted.getCount());
            for (TestIterator source : sources) {
                Assert.assertTrue(source.closed);
            }
        } finally {
            releaseFailure.countDown();
            releaseSlow.countDown();
            result.cancel(true);
            caller.shutdownNow();
            initializer.shutdownNow();
        }
    }

    @Test
    public void testInitializeDrainsCompletedFailureBeforeRefillingWindow() {
        int workers = 8;
        ExecutorService initializer = new DirectExecutorService();
        List<TestIterator> sources = new ArrayList<>();
        sources.add(new TestIterator(0));
        TestIterator failed = new TestIterator(1);
        failed.failOnHasNextAfter(0);
        sources.add(failed);
        for (int i = 2; i < workers; i++) {
            sources.add(new TestIterator(i));
        }
        CountDownLatch ninthStarted = new CountDownLatch(1);
        TestIterator ninth = new TestIterator(workers);
        ninth.blockFirstHasNext(ninthStarted, new CountDownLatch(0));
        sources.add(ninth);

        OrderedKvIterator iterator = new OrderedKvIterator(
                sources, 0L, initializer);
        try {
            Assert.assertThrows(IllegalStateException.class,
                                iterator::hasNext);
            Assert.assertEquals(1L, ninthStarted.getCount());
            for (TestIterator source : sources) {
                Assert.assertTrue(source.closed);
            }
        } finally {
            initializer.shutdownNow();
        }
    }

    @Test
    public void testSingleSourceInitializationDoesNotUseExecutor() {
        DirectExecutorService initializer = new DirectExecutorService();
        OrderedKvIterator iterator = new OrderedKvIterator(
                Collections.singletonList(new TestIterator(1)), 0L,
                initializer);
        try {
            Assert.assertTrue(iterator.hasNext());
            Assert.assertEquals(0, initializer.executions());
            Assert.assertEquals(1, iterator.next().key()[0] & 0xff);
            Assert.assertFalse(iterator.hasNext());
        } finally {
            iterator.close();
            initializer.shutdownNow();
        }
    }

    @Test
    public void testSingleSourceInitializationPreservesInterrupt()
            throws Exception {
        ExecutorService initializer = Executors.newSingleThreadExecutor();
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        TestIterator source = new TestIterator(1);
        source.blockFirstHasNextWithoutRestoringInterrupt(started, release);
        OrderedKvIterator iterator = new OrderedKvIterator(
                Collections.singletonList(source), 0L, initializer);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        AtomicBoolean interrupted = new AtomicBoolean();
        Thread caller = new Thread(() -> {
            try {
                iterator.hasNext();
            } catch (Throwable e) {
                failure.set(e);
            } finally {
                interrupted.set(Thread.currentThread().isInterrupted());
            }
        });
        caller.start();
        try {
            Assert.assertTrue(started.await(3L, TimeUnit.SECONDS));
            caller.interrupt();
            caller.join(3_000L);

            Assert.assertFalse(caller.isAlive());
            Assert.assertTrue(failure.get() instanceof IllegalStateException);
            Assert.assertEquals("Interrupted while initializing ordered scan",
                                failure.get().getMessage());
            Assert.assertTrue(interrupted.get());
            Assert.assertTrue(source.closed);
        } finally {
            release.countDown();
            caller.interrupt();
            caller.join(3_000L);
            iterator.close();
            initializer.shutdownNow();
        }
    }

    @Test
    public void testInitializeDrainsInFlightTaskBeforeRetryingRejectedSource()
            throws Exception {
        ExecutorService initializer = new ThreadPoolExecutor(
                0, 2, 60L, TimeUnit.SECONDS,
                new SynchronousQueue<>(),
                new ThreadPoolExecutor.AbortPolicy());
        ExecutorService caller = Executors.newSingleThreadExecutor();
        CountDownLatch occupiedStarted = new CountDownLatch(1);
        CountDownLatch releaseOccupied = new CountDownLatch(1);
        CountDownLatch firstStarted = new CountDownLatch(1);
        CountDownLatch releaseFirst = new CountDownLatch(1);
        CountDownLatch secondStarted = new CountDownLatch(1);
        Future<?> occupied = initializer.submit(
                () -> await(occupiedStarted, releaseOccupied));
        TestIterator first = new TestIterator(1);
        first.blockFirstHasNext(firstStarted, releaseFirst);
        TestIterator second = new TestIterator(2);
        second.blockFirstHasNext(secondStarted, new CountDownLatch(0));

        Future<Boolean> result = caller.submit(() -> {
            OrderedKvIterator iterator = new OrderedKvIterator(
                    Arrays.asList(first, second), 0L, initializer);
            try {
                return iterator.hasNext();
            } finally {
                iterator.close();
            }
        });
        try {
            Assert.assertTrue(occupiedStarted.await(3L, TimeUnit.SECONDS));
            Assert.assertTrue(firstStarted.await(3L, TimeUnit.SECONDS));
            Assert.assertFalse(secondStarted.await(1L, TimeUnit.SECONDS));
            releaseFirst.countDown();
            Assert.assertTrue(result.get(3L, TimeUnit.SECONDS));
            Assert.assertEquals(0L, secondStarted.getCount());
        } finally {
            releaseFirst.countDown();
            releaseOccupied.countDown();
            result.cancel(true);
            occupied.cancel(true);
            caller.shutdownNow();
            initializer.shutdownNow();
        }
    }

    @Test
    public void testSlowQueriesUseBoundedWorkersAndDoNotBlockIndependentQuery()
            throws Exception {
        int concurrentQueries = 16;
        ExecutorService callers = Executors.newFixedThreadPool(
                concurrentQueries + 1);
        CountDownLatch slowQueriesStarted = new CountDownLatch(
                concurrentQueries);
        CountDownLatch releaseSlowQueries = new CountDownLatch(1);
        List<Future<Boolean>> slowResults = new ArrayList<>();
        try {
            for (int i = 0; i < concurrentQueries; i++) {
                TestIterator slow = new TestIterator(i);
                slow.blockFirstHasNext(slowQueriesStarted,
                                       releaseSlowQueries);
                slowResults.add(callers.submit(() -> {
                    OrderedKvIterator iterator = new OrderedKvIterator(
                            Collections.singletonList(slow), 0L);
                    try {
                        return iterator.hasNext();
                    } finally {
                        iterator.close();
                    }
                }));
            }
            Assert.assertTrue(slowQueriesStarted.await(3L,
                                                       TimeUnit.SECONDS));
            long workers = initializerThreads();
            Assert.assertTrue("Expected at most 8 ordered scan initializer " +
                              "threads, but found " + workers,
                              workers <= 8L);

            Future<Boolean> fastResult = callers.submit(() -> {
                OrderedKvIterator iterator = new OrderedKvIterator(
                        Collections.singletonList(new TestIterator(100)),
                        0L);
                try {
                    return iterator.hasNext();
                } finally {
                    iterator.close();
                }
            });
            Assert.assertTrue(fastResult.get(3L, TimeUnit.SECONDS));
        } catch (TimeoutException e) {
            Assert.fail("An independent ordered scan was starved by slow " +
                        "queries");
        } finally {
            releaseSlowQueries.countDown();
            for (Future<Boolean> result : slowResults) {
                result.cancel(true);
            }
            callers.shutdownNow();
        }
    }

    private static long initializerThreads() {
        return Thread.getAllStackTraces().keySet().stream()
                     .filter(Thread::isAlive)
                     .filter(thread -> thread.getName().startsWith(
                             "ordered-scan-init-"))
                     .count();
    }

    private static void await(CountDownLatch started,
                              CountDownLatch release) {
        started.countDown();
        try {
            if (!release.await(5L, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Timed out waiting for release");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    private static final class DirectExecutorService
            extends AbstractExecutorService {

        private boolean shutdown;
        private int executions;

        private int executions() {
            return this.executions;
        }

        @Override
        public void shutdown() {
            this.shutdown = true;
        }

        @Override
        public List<Runnable> shutdownNow() {
            this.shutdown = true;
            return Collections.emptyList();
        }

        @Override
        public boolean isShutdown() {
            return this.shutdown;
        }

        @Override
        public boolean isTerminated() {
            return this.shutdown;
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) {
            return this.shutdown;
        }

        @Override
        public void execute(Runnable command) {
            if (this.shutdown) {
                throw new RejectedExecutionException();
            }
            this.executions++;
            command.run();
        }
    }

    private static List<Integer> keys(HgKvIterator<HgKvEntry> iterator) {
        List<Integer> keys = new ArrayList<>();
        while (iterator.hasNext()) {
            keys.add(iterator.next().key()[0] & 0xff);
        }
        return keys;
    }

    private static byte[] keyBytes(int key) {
        return new byte[]{(byte) key};
    }

    private static final class TestIterator implements HgKvIterator<HgKvEntry> {

        private final List<HgKvEntry> entries;
        private int offset;
        private int nextCalls;
        private HgKvEntry current;
        private boolean closed;
        private int failOnHasNextAfter;
        private CountDownLatch firstHasNextBarrier;
        private CountDownLatch firstHasNextStarted;
        private CountDownLatch firstHasNextRelease;
        private boolean firstHasNextBlocked;
        private boolean failAfterFirstHasNextRelease;
        private boolean restoreInterrupt;

        private TestIterator(Integer... keys) {
            this.entries = new ArrayList<>(keys.length);
            for (int key : keys) {
                this.entries.add(new TestEntry(keyBytes(key)));
            }
            this.offset = 0;
            this.nextCalls = 0;
            this.current = null;
            this.closed = false;
            this.failOnHasNextAfter = -1;
            this.firstHasNextBarrier = null;
            this.firstHasNextStarted = null;
            this.firstHasNextRelease = null;
            this.firstHasNextBlocked = false;
            this.failAfterFirstHasNextRelease = false;
            this.restoreInterrupt = true;
        }

        private void failOnHasNextAfter(int nextCalls) {
            this.failOnHasNextAfter = nextCalls;
        }

        private void blockFirstHasNext(CountDownLatch barrier) {
            this.firstHasNextBarrier = barrier;
        }

        private void blockFirstHasNext(CountDownLatch started,
                                       CountDownLatch release) {
            this.firstHasNextStarted = started;
            this.firstHasNextRelease = release;
        }

        private void blockAndFailFirstHasNext(CountDownLatch started,
                                              CountDownLatch release) {
            this.blockFirstHasNext(started, release);
            this.failAfterFirstHasNextRelease = true;
        }

        private void blockFirstHasNextWithoutRestoringInterrupt(
                CountDownLatch started, CountDownLatch release) {
            this.blockFirstHasNext(started, release);
            this.restoreInterrupt = false;
        }

        @Override
        public boolean hasNext() {
            if (this.nextCalls == this.failOnHasNextAfter) {
                throw new IllegalStateException("injected failure");
            }
            if (this.firstHasNextBarrier != null &&
                !this.firstHasNextBlocked) {
                this.firstHasNextBlocked = true;
                this.firstHasNextBarrier.countDown();
                try {
                    if (!this.firstHasNextBarrier.await(5L,
                                                        TimeUnit.SECONDS)) {
                        throw new IllegalStateException(
                                "Timed out waiting for concurrent source");
                    }
                } catch (InterruptedException e) {
                    if (this.restoreInterrupt) {
                        Thread.currentThread().interrupt();
                    }
                    throw new IllegalStateException(e);
                }
            }
            if (this.firstHasNextRelease != null &&
                !this.firstHasNextBlocked) {
                this.firstHasNextBlocked = true;
                this.firstHasNextStarted.countDown();
                try {
                    if (!this.firstHasNextRelease.await(5L,
                                                        TimeUnit.SECONDS)) {
                        throw new IllegalStateException(
                                "Timed out waiting for source release");
                    }
                } catch (InterruptedException e) {
                    if (this.restoreInterrupt) {
                        Thread.currentThread().interrupt();
                    }
                    throw new IllegalStateException(e);
                }
                if (this.failAfterFirstHasNextRelease) {
                    throw new IllegalStateException("injected failure");
                }
            }
            return this.offset < this.entries.size();
        }

        @Override
        public HgKvEntry next() {
            if (!this.hasNext()) {
                throw new NoSuchElementException();
            }
            this.nextCalls++;
            this.current = this.entries.get(this.offset++);
            return this.current;
        }

        @Override
        public byte[] key() {
            return this.current == null ? null : this.current.key();
        }

        @Override
        public byte[] value() {
            return this.current == null ? null : this.current.value();
        }

        @Override
        public void close() {
            this.closed = true;
        }
    }

    private static final class TestEntry implements HgKvEntry {

        private final byte[] key;

        private TestEntry(byte[] key) {
            this.key = key;
        }

        @Override
        public byte[] key() {
            return this.key;
        }

        @Override
        public byte[] value() {
            return this.key;
        }
    }
}
