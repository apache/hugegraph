/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hugegraph.unit.cache;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hugegraph.HugeException;
import org.apache.hugegraph.backend.cache.SchemaVersionReconciler;
import org.apache.hugegraph.backend.cache.SchemaVersionReconciler.VersionStore;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.testutil.Whitebox;
import org.apache.hugegraph.unit.BaseUnitTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SchemaVersionReconcilerTest extends BaseUnitTest {

    private static final String SPACE = "DEFAULT";
    private static final String GRAPH = "g";

    private MemoryStore store;
    private AtomicInteger clears;
    private AtomicBoolean closed;
    private SchemaVersionReconciler reconciler;

    @Before
    public void setup() {
        this.store = new MemoryStore();
        this.clears = new AtomicInteger();
        this.closed = new AtomicBoolean(false);
        this.reconciler = this.newReconciler(this.store);
    }

    @After
    public void teardown() {
        this.reconciler.stop();
    }

    private SchemaVersionReconciler newReconciler(VersionStore store) {
        return new SchemaVersionReconciler(SPACE, GRAPH, "src", store,
                                           this.clears::incrementAndGet,
                                           this.closed::get);
    }

    @Test
    public void testFirstTickClearsAndAdoptsAbsentVersion() {
        this.reconciler.run();
        Assert.assertEquals(1, this.clears.get());
        Assert.assertEquals("", this.reconciler.applied());
    }

    @Test
    public void testTickDoesNothingWhenVersionUnchanged() {
        this.store.put("v1");
        this.reconciler.run();
        this.reconciler.run();
        this.reconciler.run();
        Assert.assertEquals(1, this.clears.get());
        Assert.assertEquals("v1", this.reconciler.applied());
    }

    @Test
    public void testTickClearsOnChangeFromAnotherServer() {
        this.reconciler.run();
        this.store.put("v2");
        this.reconciler.run();
        Assert.assertEquals(2, this.clears.get());
        Assert.assertEquals("v2", this.reconciler.applied());
    }

    @Test
    public void testManyChangesBetweenTicksClearOnce() {
        this.reconciler.run();
        for (int i = 0; i < 1000; i++) {
            this.store.put("v" + i);
        }
        this.reconciler.run();
        Assert.assertEquals(2, this.clears.get());
        Assert.assertEquals("v999", this.reconciler.applied());
    }

    @Test
    public void testNewVersionIsUnique() {
        Set<String> versions = new HashSet<>();
        for (int i = 0; i < 1000; i++) {
            String version = Whitebox.invokeStatic(
                             SchemaVersionReconciler.class,
                             new Class<?>[]{String.class}, "newVersion", "s");
            Assert.assertContains("-s-", version);
            versions.add(version);
        }
        Assert.assertEquals(1000, versions.size());
    }

    @Test
    public void testBumpWritesVersionAndOwnChangeIsNotSkipped() {
        this.reconciler.run();
        this.reconciler.bump();
        String written = this.store.get();
        Assert.assertFalse(written.isEmpty());
        // bump() never adopts its own version
        Assert.assertEquals("", this.reconciler.applied());

        this.reconciler.run();
        Assert.assertEquals(2, this.clears.get());
        Assert.assertEquals(written, this.reconciler.applied());
    }

    @Test
    public void testBumpFailureIsRetriedByNextTick() {
        this.store.failWrites(1, new HugeException("pd down"));
        this.reconciler.bump();
        Assert.assertTrue(this.reconciler.pendingWrite());
        Assert.assertEquals("", this.store.get());

        this.reconciler.run();
        Assert.assertFalse(this.reconciler.pendingWrite());
        Assert.assertFalse(this.store.get().isEmpty());
        Assert.assertEquals(this.store.get(), this.reconciler.applied());
    }

    @Test
    public void testBumpFailureWithNullPointerIsRetried() {
        // PdMetaDriver.put() fails this way when every PD is unreachable
        this.store.failWrites(1, new NullPointerException());
        this.reconciler.bump();
        Assert.assertTrue(this.reconciler.pendingWrite());
        this.reconciler.run();
        Assert.assertFalse(this.reconciler.pendingWrite());
    }

    @Test
    public void testBumpFailureDuringRetryIsNotLost() {
        this.store.failWrites(1, new HugeException("pd down"));
        this.reconciler.bump();
        // The retry of the tick writes, then a new change fails to write
        this.store.onWrite(() -> {
            this.store.failWrites(1, new HugeException("pd down"));
            this.reconciler.bump();
        });
        this.reconciler.run();
        Assert.assertTrue(this.reconciler.pendingWrite());
    }

    @Test
    public void testBumpDoesNotSwallowErrors() {
        this.store.failWrites(1, new AssertionError("fatal"));
        Assert.assertThrows(AssertionError.class, () -> {
            this.reconciler.bump();
        });
        Assert.assertFalse(this.reconciler.pendingWrite());
    }

    @Test
    public void testTickSurvivesReadFailures() {
        this.store.put("v1");
        this.reconciler.run();

        this.store.put("v2");
        this.store.failReads(2);
        this.reconciler.run();
        this.reconciler.run();
        Assert.assertEquals(1, this.clears.get());
        Assert.assertEquals("v1", this.reconciler.applied());

        this.reconciler.run();
        Assert.assertEquals(2, this.clears.get());
        Assert.assertEquals("v2", this.reconciler.applied());
    }

    @Test
    public void testAdoptsVersionReadBeforeClear() {
        // A change landing while the cache is being cleared must not be
        // marked as covered by that clear
        AtomicReference<String> next = new AtomicReference<>("v2");
        SchemaVersionReconciler reconciler = new SchemaVersionReconciler(
                SPACE, GRAPH, "src", this.store, () -> {
            this.clears.incrementAndGet();
            String value = next.getAndSet(null);
            if (value != null) {
                this.store.put(value);
            }
        }, this.closed::get);
        this.store.put("v1");

        reconciler.run();
        Assert.assertEquals("v1", reconciler.applied());
        reconciler.run();
        Assert.assertEquals(2, this.clears.get());
        Assert.assertEquals("v2", reconciler.applied());
    }

    @Test
    public void testClosedGraphStopsReconciler() {
        this.closed.set(true);
        this.reconciler.run();
        Assert.assertTrue(this.reconciler.stopped());
        Assert.assertEquals(0, this.clears.get());
        Assert.assertEquals(0, this.store.reads.get());
    }

    @Test
    public void testScheduledTicksStopAfterStop() throws Exception {
        this.reconciler.start(20L);
        waitFor(() -> this.store.reads.get() >= 2);
        Assert.assertTrue(this.store.readThread.get().isDaemon());
        Assert.assertEquals("schema-version-reconciler",
                            this.store.readThread.get().getName());

        this.reconciler.stop();
        Thread.sleep(50L);
        int reads = this.store.reads.get();
        Thread.sleep(100L);
        Assert.assertEquals(reads, this.store.reads.get());
    }

    @Test
    public void testEnsureScheduledRestartsTaskKilledByError()
                throws Exception {
        this.store.failReads(1, new AssertionError("fatal"));
        this.reconciler.start(20L);
        ScheduledFuture<?> first = Whitebox.invoke(
                                   SchemaVersionReconciler.class, "future",
                                   this.reconciler);
        waitFor(first::isDone);

        this.reconciler.ensureScheduled(20L);
        ScheduledFuture<?> second = Whitebox.invoke(
                                    SchemaVersionReconciler.class, "future",
                                    this.reconciler);
        Assert.assertNotSame(first, second);
        waitFor(() -> this.clears.get() >= 1);
        this.reconciler.stop();
    }

    @Test
    public void testEnsureScheduledKeepsRunningOrStoppedTask() {
        this.reconciler.start(60_000L);
        ScheduledFuture<?> first = Whitebox.invoke(
                                   SchemaVersionReconciler.class, "future",
                                   this.reconciler);
        this.reconciler.ensureScheduled(60_000L);
        Assert.assertSame(first, Whitebox.invoke(SchemaVersionReconciler.class,
                                                 "future", this.reconciler));

        this.reconciler.stop();
        this.reconciler.ensureScheduled(60_000L);
        Assert.assertSame(first, Whitebox.invoke(SchemaVersionReconciler.class,
                                                 "future", this.reconciler));
        Assert.assertTrue(first.isCancelled());
    }

    private static void waitFor(java.util.function.BooleanSupplier condition)
                                throws InterruptedException {
        long deadline = System.currentTimeMillis() + 5000L;
        while (!condition.getAsBoolean()) {
            if (System.currentTimeMillis() > deadline) {
                Assert.fail("Timed out waiting for the condition");
            }
            Thread.sleep(5L);
        }
    }

    private static class MemoryStore implements VersionStore {

        private final Map<String, String> versions = new ConcurrentHashMap<>();
        private final AtomicInteger reads = new AtomicInteger();
        private final AtomicReference<Thread> readThread =
                new AtomicReference<>();
        private final AtomicInteger readFailures = new AtomicInteger();
        private final AtomicInteger writeFailures = new AtomicInteger();
        private volatile Throwable readFailure;
        private volatile Throwable writeFailure;
        private volatile Runnable onWrite;

        void put(String version) {
            this.versions.put(SPACE + "/" + GRAPH, version);
        }

        String get() {
            return this.versions.getOrDefault(SPACE + "/" + GRAPH, "");
        }

        void failReads(int times) {
            this.failReads(times, new HugeException("pd down"));
        }

        void failReads(int times, Throwable failure) {
            this.readFailure = failure;
            this.readFailures.set(times);
        }

        void failWrites(int times, Throwable failure) {
            this.writeFailure = failure;
            this.writeFailures.set(times);
        }

        void onWrite(Runnable action) {
            this.onWrite = action;
        }

        @Override
        public String read(String graphSpace, String graph) {
            this.reads.incrementAndGet();
            this.readThread.set(Thread.currentThread());
            if (this.readFailures.getAndDecrement() > 0) {
                throwUnchecked(this.readFailure);
            }
            return this.versions.getOrDefault(graphSpace + "/" + graph, "");
        }

        @Override
        public void write(String graphSpace, String graph, String version) {
            if (this.writeFailures.getAndDecrement() > 0) {
                throwUnchecked(this.writeFailure);
            }
            this.versions.put(graphSpace + "/" + graph, version);
            Runnable action = this.onWrite;
            if (action != null) {
                this.onWrite = null;
                action.run();
            }
        }

        private static void throwUnchecked(Throwable failure) {
            if (failure instanceof Error) {
                throw (Error) failure;
            }
            throw (RuntimeException) failure;
        }
    }
}
