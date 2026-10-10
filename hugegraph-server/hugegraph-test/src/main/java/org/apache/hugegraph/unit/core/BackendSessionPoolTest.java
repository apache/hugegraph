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

package org.apache.hugegraph.unit.core;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

import org.apache.hugegraph.backend.store.BackendSession;
import org.apache.hugegraph.backend.store.BackendSessionPool;
import org.apache.hugegraph.backend.store.BackendStoreProvider;
import org.apache.hugegraph.backend.store.rocksdb.RocksDBSessions;
import org.apache.hugegraph.backend.store.rocksdb.RocksDBStore.RocksDBGraphStore;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.testutil.Whitebox;
import org.apache.hugegraph.unit.FakeObjects;
import org.junit.Test;
import org.mockito.Mockito;

public class BackendSessionPoolTest {

    private static final long TIMEOUT_SECONDS = 10L;
    private static final long CLOSE_HOLD_SECONDS = 30L;

    @Test
    public void testConcurrentAcquireDoesNotLeaveSessionOnClosedBackend()
            throws Exception {
        TestSessionPool pool = new TestSessionPool();
        pool.open();

        AtomicReference<Throwable> closeFailure = new AtomicReference<>();
        CountDownLatch acquireAttempted = new CountDownLatch(1);
        FutureTask<BackendSession> acquisition =
                new FutureTask<>(() -> {
                    acquireAttempted.countDown();
                    return pool.getOrNewSession();
                });

        Thread closer = new Thread(() -> {
            try {
                pool.getOrNewSession();
                pool.close();
            } catch (Throwable e) {
                closeFailure.set(e);
            }
        }, "backend-session-closer");
        Thread acquirer = new Thread(acquisition, "backend-session-acquirer");

        closer.start();
        try {
            Assert.assertTrue("last-session close did not enter doClose",
                              pool.closeEntered.await(TIMEOUT_SECONDS,
                                                      TimeUnit.SECONDS));

            acquirer.start();
            Assert.assertTrue("acquirer did not start",
                              acquireAttempted.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            boolean acquiredWhileClosePaused =
                    awaitBorrowerInCloseWindow(pool, closer, acquirer);
            BackendSession activeSession = acquiredWhileClosePaused ?
                                           acquisition.get(TIMEOUT_SECONDS,
                                                           TimeUnit.SECONDS) : null;
            pool.allowClose.countDown();

            closer.join(TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS));
            acquirer.join(TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS));
            Assert.assertFalse("closer did not finish", closer.isAlive());
            Assert.assertFalse("acquirer did not finish", acquirer.isAlive());
            Assert.assertNull(closeFailure.get());
            if (acquiredWhileClosePaused) {
                Assert.assertNotNull(activeSession);
                Assert.assertTrue("new session should still be open",
                                  activeSession.opened());
                Assert.assertFalse("pool should still count the new active session",
                                   pool.closed());

                Assert.assertTrue("doClose closed the backend while a newly " +
                                  "acquired session remained active",
                                  pool.opened());
            } else {
                try {
                    acquisition.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                    Assert.fail("late acquire should be rejected after backend close");
                } catch (ExecutionException e) {
                    Assert.assertTrue("late acquire must fail because backend is closed",
                                      e.getCause() instanceof IllegalStateException);
                    Assert.assertEquals("Backend session pool is closed", e.getCause().getMessage());
                    Assert.assertFalse("backend should be closed before rejecting late acquire",
                                       pool.opened());
                    Assert.assertTrue("pool should count no active session after rejection",
                                      pool.closed());
                }
            }
        } finally {
            pool.allowClose.countDown();
            closer.join(TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS));
            if (acquirer.getState() != Thread.State.NEW) {
                acquirer.join(TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS));
            }
        }
    }

    @Test
    public void testAcquireAfterLastSessionCloseIsRejectedByPool() {
        TestSessionPool pool = new TestSessionPool();
        pool.open();
        BackendSession original = pool.getOrNewSession();
        pool.allowClose.countDown();
        Assert.assertTrue(pool.close());
        Assert.assertFalse(pool.opened());

        Throwable failure = Assert.assertThrows(IllegalStateException.class, pool::getOrNewSession);
        Assert.assertEquals("Backend session pool is closed", failure.getMessage());
        Assert.assertTrue(pool.closed());
        Assert.assertSame("rejected acquire must never call newSession", original, pool.session());
    }

    @Test
    public void testStoreOpenKeepsExistingPoolAliveUntilBorrowCompletes() throws Exception {
        RocksDBGraphStore store = new RocksDBGraphStore(
                Mockito.mock(BackendStoreProvider.class), "db", "test");
        RocksDBSessions pool = Mockito.mock(RocksDBSessions.class);
        AtomicInteger leases = new AtomicInteger(1);
        AtomicBoolean nativeOpened = new AtomicBoolean(true);
        CountDownLatch borrowing = new CountDownLatch(1);
        CountDownLatch finishBorrow = new CountDownLatch(1);
        CountDownLatch closing = new CountDownLatch(1);
        CountDownLatch closeFinished = new CountDownLatch(1);
        Mockito.doAnswer(invocation -> leases.get() == 0).when(pool).closed();
        Mockito.doAnswer(invocation -> {
            borrowing.countDown();
            Assert.assertTrue(finishBorrow.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            Assert.assertTrue("last close released the backend before the new store borrow",
                              nativeOpened.get());
            leases.incrementAndGet();
            return new TestSession();
        }).when(pool).useSession();
        Mockito.doAnswer(invocation -> {
            boolean last = leases.decrementAndGet() == 0;
            if (last) {
                nativeOpened.set(false);
            }
            return last;
        }).when(pool).close();
        Whitebox.setInternalState(store, "sessions", pool);
        ConcurrentHashMap<String, RocksDBSessions> databases = new ConcurrentHashMap<>();
        databases.put("db", pool);
        Whitebox.setInternalState(store, "dbs", databases);

        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread opener = new Thread(() -> {
            try {
                store.open(FakeObjects.newConfig());
            } catch (Throwable error) {
                failure.compareAndSet(null, error);
            }
        }, "store-session-opener");
        Thread closer = new Thread(() -> {
            closing.countDown();
            try {
                store.close();
            } catch (Throwable error) {
                failure.compareAndSet(null, error);
            } finally {
                closeFinished.countDown();
            }
        }, "store-session-closer");
        opener.start();
        try {
            Assert.assertTrue(borrowing.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            closer.start();
            Assert.assertTrue(closing.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(TIMEOUT_SECONDS);
            // Wait for close to either reach the held store monitor or finish.
            while (closeFinished.getCount() != 0L) {
                ThreadInfo info = ManagementFactory.getThreadMXBean().getThreadInfo(closer.getId());
                if (info != null && info.getThreadState() == Thread.State.BLOCKED &&
                    info.getLockOwnerId() == opener.getId()) {
                    break;
                }
                Assert.assertTrue("close did not reach the store open window", System.nanoTime() < deadline);
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1L));
            }
        } finally {
            finishBorrow.countDown();
            opener.join(TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS));
            if (closer.getState() != Thread.State.NEW) {
                closer.join(TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS));
            }
        }
        Assert.assertFalse(opener.isAlive());
        Assert.assertFalse(closer.isAlive());
        Assert.assertNull(failure.get());
        Assert.assertTrue(nativeOpened.get());
        Assert.assertEquals(1, leases.get());
        Mockito.verify(pool).useSession();
        Mockito.verify(pool).close();
    }

    private static boolean awaitBorrowerInCloseWindow(TestSessionPool pool,
                                                      Thread closer,
                                                      Thread acquirer) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(TIMEOUT_SECONDS);
        while (System.nanoTime() < deadline) {
            if (pool.acquirerEnteredNewSession.getCount() == 0L) {
                return true;
            }
            ThreadInfo info = ManagementFactory.getThreadMXBean()
                                               .getThreadInfo(acquirer.getId());
            if (info != null && info.getThreadState() == Thread.State.BLOCKED &&
                info.getLockOwnerId() == closer.getId() &&
                info.getLockInfo() != null &&
                info.getLockInfo().getClassName().equals(TestSessionPool.class.getName())) {
                return false;
            }
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1L));
        }
        throw new AssertionError("acquirer neither entered newSession nor blocked on close");
    }

    private static final class TestSessionPool extends BackendSessionPool {

        private final AtomicBoolean opened;
        private final AtomicReference<BackendSession> session;
        private final CountDownLatch closeEntered;
        private final CountDownLatch allowClose;
        private final CountDownLatch acquirerEnteredNewSession;

        private TestSessionPool() {
            super(FakeObjects.newConfig(), "test");
            this.opened = new AtomicBoolean();
            this.session = new AtomicReference<>();
            this.closeEntered = new CountDownLatch(1);
            this.allowClose = new CountDownLatch(1);
            this.acquirerEnteredNewSession = new CountDownLatch(1);
        }

        @Override
        public void open() {
            this.opened.set(true);
        }

        @Override
        protected boolean opened() {
            return this.opened.get();
        }

        @Override
        public BackendSession session() {
            return this.session.get();
        }

        @Override
        protected BackendSession newSession() {
            if (Thread.currentThread().getName().equals("backend-session-acquirer")) {
                this.acquirerEnteredNewSession.countDown();
            }
            BackendSession newSession = new TestSession();
            this.session.set(newSession);
            return newSession;
        }

        @Override
        protected void doClose() {
            this.closeEntered.countDown();
            try {
                if (!this.allowClose.await(CLOSE_HOLD_SECONDS, TimeUnit.SECONDS)) {
                    throw new AssertionError("timed out waiting to finish doClose");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError("interrupted while waiting to finish doClose", e);
            }
            this.opened.set(false);
        }
    }

    private static final class TestSession extends BackendSession.AbstractBackendSession {

        @Override
        public void open() {
            this.opened = true;
        }

        @Override
        public void close() {
            this.opened = false;
        }

        @Override
        public Object commit() {
            return null;
        }

        @Override
        public void rollback() {
            // No transaction state in this test session.
        }

        @Override
        public boolean hasChanges() {
            return false;
        }
    }

}
