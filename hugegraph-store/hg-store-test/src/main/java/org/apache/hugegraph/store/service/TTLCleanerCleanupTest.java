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

package org.apache.hugegraph.store.service;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;

import org.apache.commons.configuration2.MapConfiguration;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.config.OptionSpace;
import org.apache.hugegraph.rocksdb.access.RocksDBOptions;
import org.apache.hugegraph.rocksdb.access.RocksDBSession;
import org.apache.hugegraph.rocksdb.access.ScanIterator;
import org.apache.hugegraph.rocksdb.access.SessionOperator;
import org.apache.hugegraph.store.business.BusinessHandlerImpl;
import org.apache.hugegraph.store.business.InnerKeyCreator;
import org.apache.hugegraph.store.constant.HugeServerTables;
import org.apache.hugegraph.store.node.AppConfig;
import org.apache.hugegraph.store.node.grpc.GrpcShutdownBarrier;
import org.apache.hugegraph.store.node.grpc.HgStoreStreamImpl;
import org.apache.hugegraph.store.node.grpc.query.AggregativeQueryService;
import org.apache.hugegraph.store.node.listener.ContextClosedListener;
import org.apache.hugegraph.store.node.task.TTLCleaner;
import org.apache.hugegraph.store.node.task.ttl.TaskInfo;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.InOrder;
import org.rocksdb.RocksDB;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

public class TTLCleanerCleanupTest {

    @Rule
    public TemporaryFolder directory = new TemporaryFolder();

    @Test(timeout = 5000)
    public void testCompletionWaitsForNativeClose() throws Exception {
        TTLCleaner cleaner = newCleaner();
        CountDownLatch completed = new CountDownLatch(1);
        CountDownLatch closing = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        ScanIterator scan = mock(ScanIterator.class);
        doAnswer(invocation -> {
            closing.countDown();
            release.await();
            return null;
        }).when(scan).close();
        RocksDBSession session = mock(RocksDBSession.class);
        FutureTask<Void> task = new FutureTask<>(task(cleaner, scan, session, completed), null);
        Thread worker = new Thread(task, "test-ttl-close");
        worker.setDaemon(true);
        try {
            worker.start();
            assertTrue(closing.await(1, TimeUnit.SECONDS));
            assertEquals("TTL must not compact before native close finishes", 1L,
                         completed.getCount());
            release.countDown();
            task.get(1, TimeUnit.SECONDS);
            assertEquals(0L, completed.getCount());
            InOrder order = inOrder(scan, session);
            order.verify(scan).close();
            order.verify(session).close();
            cleaner.awaitCleanup();
        } finally {
            release.countDown();
            cleaner.getScheduler().shutdownNow();
        }
    }

    @Test(timeout = 5000)
    public void testFailedCloseStaysBlockedAfterInterruption() throws Exception {
        assertFailedCloseStaysBlocked(true, false);
        assertFailedCloseStaysBlocked(false, true);
        assertFailedCloseStaysBlocked(true, true);
    }

    private static void assertFailedCloseStaysBlocked(boolean scanFails, boolean sessionFails) throws Exception {
        TTLCleaner cleaner = newCleaner();
        RuntimeException failure = new IllegalStateException("native release failed");
        ScanIterator scan = mock(ScanIterator.class);
        RocksDBSession session = mock(RocksDBSession.class);
        RuntimeException sessionFailure = scanFails ? new IllegalStateException("session release failed") : failure;
        if (scanFails) {
            doAnswer(invocation -> { throw failure; }).when(scan).close();
        }
        if (sessionFails) {
            doAnswer(invocation -> { throw sessionFailure; }).when(session).close();
        }
        CountDownLatch completed = new CountDownLatch(1);
        CountDownLatch awaiting = new CountDownLatch(1);
        FutureTask<Boolean> cleanup = new FutureTask<>(() -> {
            awaiting.countDown();
            cleaner.awaitCleanup();
            return Thread.currentThread().isInterrupted();
        });
        Thread shutdown = new Thread(cleanup, "test-ttl-cleanup-failure");
        shutdown.setDaemon(true);
        Field field = TTLCleaner.class.getDeclaredField("cleanupFailure");
        field.setAccessible(true);
        @SuppressWarnings("unchecked")
        AtomicReference<Throwable> retained = (AtomicReference<Throwable>) field.get(cleaner);
        try {
            task(cleaner, scan, session, completed).run();
            InOrder order = inOrder(scan, session);
            order.verify(scan).close();
            order.verify(session).close();
            if (scanFails && sessionFails) {
                assertEquals(1, failure.getSuppressed().length);
                assertSame(sessionFailure, failure.getSuppressed()[0]);
            }
            assertEquals(0L, completed.getCount());
            assertSame(failure, retained.get());
            shutdown.start();
            assertTrue(awaiting.await(1, TimeUnit.SECONDS));
            shutdown.interrupt();
            assertFalse("interruption cannot authorize native storage destruction",
                        cleanup.isDone());
            assertSame(failure, retained.get());
        } finally {
            // Production never clears this failure. Release only this test's daemon waiter.
            synchronized (retained) {
                retained.set(null);
                retained.notifyAll();
            }
            shutdown.interrupt();
            assertTrue(cleanup.get(1, TimeUnit.SECONDS));
            cleaner.getScheduler().shutdownNow();
        }
    }

    @Test(timeout = 5000)
    public void testNativeSessionCloneAndScanReleaseAllReferences() throws Exception {
        RocksDB.loadLibrary();
        OptionSpace.register("rocksdb", "org.apache.hugegraph.rocksdb.access.RocksDBOptions");
        RocksDBOptions.instance();
        HugeConfig config = new HugeConfig(new MapConfiguration(
                Collections.singletonMap("rocksdb.write_buffer_size", "1048576")));
        TTLCleaner cleaner = newCleaner();
        RocksDBSession owner = new RocksDBSession(config, directory.newFolder("ttl-native").getAbsolutePath(),
                                                 "ttl-native", 0L);
        RocksDBSession lease = owner.clone();
        try {
            owner.checkTable(HugeServerTables.VERTEX_TABLE);
            assertEquals(2, owner.getRefCount());
            BusinessHandlerImpl handler = mock(BusinessHandlerImpl.class);
            when(handler.getSession(1)).thenReturn(lease);
            InnerKeyCreator keys = mock(InnerKeyCreator.class);
            when(keys.getStartKey(1, "graph")).thenReturn(new byte[]{0});
            when(keys.getEndKey(1, "graph")).thenReturn(new byte[]{1});
            when(handler.getKeyCreator()).thenReturn(keys);
            CountDownLatch completed = new CountDownLatch(1);
            task(cleaner, handler, completed).run();
            assertEquals(0L, completed.getCount());
            cleaner.awaitCleanup();
            assertEquals("TTL must release both the iterator lease and cloned session", 1, owner.getRefCount());
            RocksDB database = owner.getDB();
            assertTrue(database.isOwningHandle());
            owner.close();
            assertEquals(0, owner.getRefCount());
            assertNull(owner.getDB());
            assertFalse(database.isOwningHandle());
        } finally {
            lease.close();
            owner.close();
            cleaner.getScheduler().shutdownNow();
        }
    }

    @Test(timeout = 30000)
    public void testActiveNativeTtlScanDrainsBeforeInterruptedSpringClose() throws Exception {
        RocksDB.loadLibrary();
        OptionSpace.register("rocksdb", "org.apache.hugegraph.rocksdb.access.RocksDBOptions");
        RocksDBOptions.instance();
        HugeConfig config = new HugeConfig(new MapConfiguration(
                Collections.singletonMap("rocksdb.write_buffer_size", "1048576")));
        RocksDBSession owner = new RocksDBSession(config, directory.newFolder("ttl-spring").getAbsolutePath(),
                                                 "ttl-spring", 0L);
        RocksDBSession lease = owner.clone();
        RocksDB database = owner.getDB();
        TTLCleaner cleaner = spy(newCleaner());
        CountDownLatch judging = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch workerInterrupted = new CountDownLatch(1);
        CountDownLatch closeInterrupted = new CountDownLatch(1);
        ThreadPoolExecutor workers = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
                                                           new LinkedBlockingQueue<>()) {
            @Override
            public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
                try {
                    return super.awaitTermination(timeout, unit);
                } catch (InterruptedException e) {
                    closeInterrupted.countDown();
                    throw e;
                }
            }
        };
        Field executor = TTLCleaner.class.getDeclaredField("executor");
        executor.setAccessible(true);
        executor.set(cleaner, workers);
        Field registry = RocksDBSession.class.getDeclaredField("iteratorMap");
        registry.setAccessible(true);
        Map<?, ?> iterators = (Map<?, ?>) registry.get(owner);
        BiFunction<byte[], byte[], Boolean> judge = (key, value) -> {
            assertArrayEquals(new byte[]{10}, key);
            assertArrayEquals(new byte[]{42}, value);
            judging.countDown();
            boolean interrupted = false;
            while (release.getCount() != 0) {
                try {
                    release.await();
                } catch (InterruptedException e) {
                    interrupted = true;
                    workerInterrupted.countDown();
                }
            }
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
            return false;
        };
        doReturn(judge).when(cleaner).getJudge("graph", HugeServerTables.VERTEX_TABLE);
        BusinessHandlerImpl handler = mock(BusinessHandlerImpl.class);
        when(handler.getSession(1)).thenReturn(lease);
        InnerKeyCreator keys = mock(InnerKeyCreator.class);
        when(keys.getStartKey(1, "graph")).thenReturn(new byte[]{0});
        when(keys.getEndKey(1, "graph")).thenReturn(new byte[]{1});
        when(handler.getKeyCreator()).thenReturn(keys);
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
        CountDownLatch completed = new CountDownLatch(1);
        Future<?> worker = null;
        FutureTask<Void> destruction = new FutureTask<>(() -> {
            assertEquals(0L, completed.getCount());
            assertTrue(iterators.isEmpty());
            assertEquals(1, owner.getRefCount());
            assertTrue(database.isOwningHandle());
            owner.close();
            assertEquals(0, owner.getRefCount());
            assertFalse(database.isOwningHandle());
            return null;
        });
        FutureTask<Boolean> closing = new FutureTask<>(() -> {
            context.close();
            return Thread.currentThread().isInterrupted();
        });
        try {
            owner.checkTable(HugeServerTables.VERTEX_TABLE);
            SessionOperator operator = owner.sessionOp();
            operator.prepare();
            operator.put(HugeServerTables.VERTEX_TABLE, new byte[]{0, 0, 10, 0, 1}, new byte[]{42});
            operator.put(HugeServerTables.VERTEX_TABLE, new byte[]{0, 0, 11, 0, 1}, new byte[]{43});
            operator.commit();
            context.getBeanFactory().registerSingleton("storeStream", mock(HgStoreStreamImpl.class));
            context.getBeanFactory().registerSingleton("queryService", mock(AggregativeQueryService.class));
            context.getBeanFactory().registerSingleton("cleaner", cleaner);
            context.getDefaultListableBeanFactory().registerDisposableBean("database", destruction::run);
            context.register(ContextClosedListener.class, GrpcShutdownBarrier.class);
            context.refresh();
            worker = workers.submit(task(cleaner, handler, completed));
            assertTrue(judging.await(2, TimeUnit.SECONDS));
            assertFalse("the nonempty scan must still own its native iterator", iterators.isEmpty());
            Thread closeThread = new Thread(closing, "test-active-ttl-spring-close");
            closeThread.start();
            assertTrue(workerInterrupted.await(2, TimeUnit.SECONDS));
            closeThread.interrupt();
            assertTrue(closeInterrupted.await(2, TimeUnit.SECONDS));
            assertFalse(closing.isDone());
            assertFalse(destruction.isDone());
            assertEquals(1L, completed.getCount());
            assertFalse(iterators.isEmpty());
            assertTrue(database.isOwningHandle());
            release.countDown();
            worker.get(2, TimeUnit.SECONDS);
            assertTrue("Spring close must preserve interruption", closing.get(2, TimeUnit.SECONDS));
            destruction.get(2, TimeUnit.SECONDS);
            assertTrue(workers.isTerminated());
            assertNull(owner.getDB());
        } finally {
            release.countDown();
            workers.shutdownNow();
            try {
                if (worker != null) {
                    worker.get(2, TimeUnit.SECONDS);
                }
            } finally {
                try {
                    context.close();
                } finally {
                    lease.close();
                    owner.close();
                    cleaner.getScheduler().shutdownNow();
                }
            }
        }
    }

    private static TTLCleaner newCleaner() {
        AppConfig config = mock(AppConfig.class);
        AppConfig.JobConfig job = mock(AppConfig.JobConfig.class);
        when(config.getJobConfig()).thenReturn(job);
        when(job.getStartTime()).thenReturn(19);
        when(job.getBatchSize()).thenReturn(128);
        return new TTLCleaner(config);
    }

    private static Runnable task(TTLCleaner cleaner, ScanIterator scan, RocksDBSession session,
                                 CountDownLatch completed) throws Exception {
        BusinessHandlerImpl handler = mock(BusinessHandlerImpl.class);
        SessionOperator operator = mock(SessionOperator.class);
        when(handler.getSession(1)).thenReturn(session);
        when(handler.getKeyCreator()).thenReturn(mock(InnerKeyCreator.class));
        when(session.sessionOp()).thenReturn(operator);
        when(operator.scan(anyString(), any(), any(), anyInt())).thenReturn(scan);
        return task(cleaner, handler, completed);
    }

    private static Runnable task(TTLCleaner cleaner, BusinessHandlerImpl handler,
                                 CountDownLatch completed) throws Exception {
        TaskInfo info = mock(TaskInfo.class);
        when(info.getTableCounter()).thenReturn(new ConcurrentHashMap<>(
                Collections.singletonMap(HugeServerTables.VERTEX_TABLE, new AtomicLong())));
        Method method = TTLCleaner.class.getDeclaredMethod("getTask", BusinessHandlerImpl.class,
                CountDownLatch.class, Triple.class, Map.class, Map.class);
        method.setAccessible(true);
        return (Runnable) method.invoke(cleaner, handler, completed,
                Triple.of(1, "graph", HugeServerTables.VERTEX_TABLE),
                Collections.singletonMap("graph", info),
                Collections.singletonMap(1, new AtomicLong()));
    }
}
