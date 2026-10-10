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

package org.apache.hugegraph.store.node.grpc.query;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hugegraph.rocksdb.access.RocksDBScanIterator;
import org.apache.hugegraph.rocksdb.access.ScanIterator;
import org.apache.hugegraph.store.business.InnerKeyFilter;
import org.apache.hugegraph.store.business.MultiPartitionIterator;
import org.apache.hugegraph.store.grpc.query.QueryRequest;
import org.apache.hugegraph.store.grpc.query.QueryResponse;
import org.apache.hugegraph.store.node.grpc.GrpcShutdownBarrier;
import org.apache.hugegraph.store.node.grpc.HgStoreStreamImpl;
import org.apache.hugegraph.store.node.grpc.query.model.PipelineResult;
import org.apache.hugegraph.store.node.grpc.query.model.QueryPlan;
import org.apache.hugegraph.store.node.listener.ContextClosedListener;
import org.apache.hugegraph.store.node.task.TTLCleaner;
import org.junit.Test;
import org.rocksdb.RocksIterator;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

public class AggregativeQueryShutdownTest extends AggregativeQueryTestSupport {

    @Test(timeout = 5000)
    public void testContextCloseContinuesAfterCancellationResponseFailure() throws Exception {
        assertContextCloseContinuesAfterCancellationFailure(
                new IllegalStateException("shutdown response failed"));
    }

    @Test(timeout = 5000)
    public void testContextCloseContinuesAfterCancellationResponseError() throws Exception {
        assertContextCloseContinuesAfterCancellationFailure(new AssertionError("shutdown response failed"));
    }

    @Test(timeout = 5000)
    public void testContextWaitsForIteratorFinallyAfterRpcCancellation() throws Exception {
        ThreadPoolExecutor pool = pool(1);
        CountDownLatch reading = new CountDownLatch(1);
        CountDownLatch closingIterator = new CountDownLatch(1);
        CountDownLatch allowClose = new CountDownLatch(1);
        AtomicBoolean databaseClosed = new AtomicBoolean();
        ScanIterator iterator = mock(ScanIterator.class);
        when(iterator.hasNext()).thenAnswer(invocation -> {
            reading.countDown();
            try {
                new CountDownLatch(1).await();
            } catch (InterruptedException ignored) {
                return false;
            }
            return false;
        });
        doAnswer(invocation -> {
            closingIterator.countDown();
            awaitUninterruptibly(allowClose);
            return null;
        }).when(iterator).close();
        AggregativeQueryService service = service(pool, iterator, new QueryPlan(), 500);
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
        try {
            context.getBeanFactory().registerSingleton("queryService", service);
            context.getBeanFactory().registerSingleton("storeStream", mock(HgStoreStreamImpl.class));
            context.getBeanFactory().registerSingleton("cleaner", mock(TTLCleaner.class));
            context.getDefaultListableBeanFactory().registerDisposableBean(
                    "database", () -> databaseClosed.set(true));
            context.register(ContextClosedListener.class, GrpcShutdownBarrier.class);
            context.refresh();
            StreamObserver<QueryRequest> request = service.query(sender());
            request.onNext(QueryRequest.getDefaultInstance());
            assertTrue(reading.await(1, TimeUnit.SECONDS));
            request.onError(Status.CANCELLED.asRuntimeException());
            assertTrue(closingIterator.await(1, TimeUnit.SECONDS));
            FutureTask<Boolean> closing = new FutureTask<>(() -> {
                context.close();
                return Thread.currentThread().isInterrupted();
            });
            Thread closer = start(closing);
            awaitShutdownWait(closer);
            closer.interrupt();
            awaitShutdownWait(closer);
            assertFalse("RPC cancellation must not release a still-closing iterator", closing.isDone());
            assertFalse(databaseClosed.get());
            allowClose.countDown();
            assertTrue("context close must preserve interruption", closing.get(2, TimeUnit.SECONDS));
            assertTrue(databaseClosed.get());
            verify(iterator).close();
        } finally {
            allowClose.countDown();
            context.close();
            pool.shutdownNow();
        }
    }

    @Test(timeout = 5000)
    public void testQueuedParentsAreDrainedAndNewQueriesAreRejected() throws Exception {
        ThreadPoolExecutor pool = pool(1);
        CountDownLatch blockerStarted = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        ScanIterator iterator = mock(ScanIterator.class);
        AtomicInteger closed = new AtomicInteger();
        doAnswer(invocation -> {
            closed.incrementAndGet();
            return null;
        }).when(iterator).close();
        AggregativeQueryService service = service(pool, iterator, new QueryPlan(), 500);
        pool.execute(() -> {
            blockerStarted.countDown();
            awaitUninterruptibly(release);
        });
        try {
            assertTrue(blockerStarted.await(1, TimeUnit.SECONDS));
            service.query(sender()).onNext(QueryRequest.getDefaultInstance());
            service.query(sender()).onNext(QueryRequest.getDefaultInstance());
            service.stopAcceptingQueries();
            try {
                service.query(sender());
                fail("new queries must be rejected after admission closes");
            } catch (StatusRuntimeException expected) {
                assertEquals(Status.Code.UNAVAILABLE, expected.getStatus().getCode());
            }
            FutureTask<Void> closing = close(service);
            awaitShutdownWait(start(closing));
            assertFalse(closing.isDone());
            release.countDown();
            closing.get(2, TimeUnit.SECONDS);
            assertEquals(2, closed.get());
            verify(iterator, never()).hasNext();
            assertTrue(pool.awaitTermination(1, TimeUnit.SECONDS));
        } finally {
            release.countDown();
            service.shutdownQueries();
            pool.shutdownNow();
        }
    }

    @Test(timeout = 5000)
    public void testQueuedPartitionRunsCleanupAfterParentTimeout() throws Exception {
        CountDownLatch childQueued = new CountDownLatch(1);
        CountDownLatch releaseSubmission = new CountDownLatch(1);
        AtomicInteger submissions = new AtomicInteger();
        ThreadPoolExecutor pool = new ThreadPoolExecutor(1, 1, 0, TimeUnit.SECONDS,
                                                        new LinkedBlockingQueue<>()) {
            @Override
            public void execute(Runnable task) {
                super.execute(task);
                if (submissions.incrementAndGet() == 2) {
                    childQueued.countDown();
                    awaitUninterruptibly(releaseSubmission);
                }
            }
        };
        ScanIterator child = mock(ScanIterator.class);
        MultiPartitionIterator iterator = mock(MultiPartitionIterator.class);
        when(iterator.getIterators()).thenReturn(Arrays.asList(child));
        QueryPlan plan = mock(QueryPlan.class);
        when(plan.hasIteratorResult()).thenReturn(true);
        AggregativeQueryService service = service(pool, iterator, plan, 50);
        try {
            StreamObserver<QueryRequest> request = service.query(sender());
            request.onNext(QueryRequest.getDefaultInstance());
            assertTrue(childQueued.await(1, TimeUnit.SECONDS));
            request.onError(Status.CANCELLED.asRuntimeException());
            FutureTask<Void> closing = close(service);
            start(closing);
            releaseSubmission.countDown();
            closing.get(2, TimeUnit.SECONDS);
            verify(child).close();
            verify(child, never()).hasNext();
            verify(plan).clear();
            verify(iterator).close();
        } finally {
            releaseSubmission.countDown();
            service.shutdownQueries();
            pool.shutdownNow();
        }
    }

    @Test(timeout = 5000)
    public void testIdleRpcAndInitializationFailureDoNotBlockShutdown() throws Exception {
        ThreadPoolExecutor pool = pool(1);
        ScanIterator iterator = mock(ScanIterator.class);
        AggregativeQueryService service = new AggregativeQueryService(pool, 500, 10) {
            @Override
            AggregativeQueryObserver newObserver(StreamObserver<QueryResponse> sender) {
                AggregativeQueryObserver observer = spy(super.newObserver(sender));
                doReturn(iterator).when(observer).getIterator(org.mockito.ArgumentMatchers.any());
                org.mockito.Mockito.doThrow(new IllegalArgumentException("invalid query plan"))
                                   .when(observer).buildPlan(org.mockito.ArgumentMatchers.any());
                return observer;
            }
        };
        ResponseRecorder failedResponse = new ResponseRecorder();
        try {
            service.query(sender());
            try {
                service.query(failedResponse).onNext(QueryRequest.getDefaultInstance());
                fail("invalid query must fail initialization");
            } catch (IllegalArgumentException expected) {
                assertEquals("invalid query plan", expected.getMessage());
            }
            assertEquals("framework must retain the synchronous error", 0, failedResponse.completed.get());
            assertTrue(failedResponse.responses.isEmpty());
            service.shutdownQueries();
            verify(iterator).close();
        } finally {
            pool.shutdownNow();
        }
    }

    @Test(timeout = 5000)
    public void testParentCleanupFailuresReportErrorAndKeepShutdownBlocked() throws Exception {
        ThreadPoolExecutor pool = pool(1);
        ScanIterator iterator = mock(ScanIterator.class);
        QueryPlan plan = mock(QueryPlan.class);
        when(plan.onlyStopStage()).thenReturn(true);
        IllegalStateException planFailure = new IllegalStateException("plan cleanup failed");
        IllegalStateException iteratorFailure = new IllegalStateException("iterator cleanup failed");
        doThrow(planFailure).when(plan).clear();
        doThrow(iteratorFailure).when(iterator).close();
        ResponseRecorder response = new ResponseRecorder();
        AggregativeQueryService service = service(pool, iterator, plan, 500);
        FutureTask<Void> closing = close(service);
        try {
            service.query(response).onNext(QueryRequest.newBuilder().setQueryId("cleanup-query").build());
            assertTrue(response.finished.await(1, TimeUnit.SECONDS));
            assertError(response, "cleanup-query");
            assertEquals("plan cleanup failed", response.responses.get(0).getMessage());
            assertEquals(1, planFailure.getSuppressed().length);
            org.junit.Assert.assertSame(iteratorFailure, planFailure.getSuppressed()[0]);
            verify(plan).clear();
            verify(iterator).close();
            awaitShutdownWait(start(closing));
            assertFalse(closing.isDone());
            assertFalse("failed cleanup cannot release the executor", pool.isShutdown());
        } finally {
            releaseMockQueries(service);
            closing.run();
            closing.get(1, TimeUnit.SECONDS);
            pool.shutdownNow();
        }
    }

    @Test(timeout = 5000)
    public void testSpringDoesNotDestroyDatabaseWhenIteratorCloseFails() throws Exception {
        ThreadPoolExecutor pool = pool(1);
        ScanIterator iterator = mock(ScanIterator.class);
        doThrow(new IllegalStateException("native iterator close failed")).when(iterator).close();
        AggregativeQueryService service = service(pool, iterator, new QueryPlan(), 500);
        AtomicBoolean databaseClosed = new AtomicBoolean();
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
        context.getBeanFactory().registerSingleton("queryService", service);
        context.getBeanFactory().registerSingleton("storeStream", mock(HgStoreStreamImpl.class));
        context.getBeanFactory().registerSingleton("cleaner", mock(TTLCleaner.class));
        context.getDefaultListableBeanFactory().registerDisposableBean(
                "database", () -> databaseClosed.set(true));
        context.register(ContextClosedListener.class, GrpcShutdownBarrier.class);
        context.refresh();
        ResponseRecorder response = new ResponseRecorder();
        FutureTask<Void> closing = new FutureTask<>(() -> {
            context.close();
            return null;
        });
        try {
            service.query(response).onNext(QueryRequest.newBuilder().setQueryId("spring-query").build());
            assertTrue(response.finished.await(1, TimeUnit.SECONDS));
            assertError(response, "spring-query");
            Thread closer = start(closing);
            awaitShutdownWait(closer);
            closer.interrupt();
            awaitShutdownWait(closer);
            assertFalse(closing.isDone());
            assertFalse("Spring must retain the database after cleanup failure", databaseClosed.get());
        } finally {
            releaseMockQueries(service);
            closing.run();
            closing.get(1, TimeUnit.SECONDS);
            context.close();
            pool.shutdownNow();
        }
    }

    @Test(timeout = 5000)
    public void testSynchronousFailureRetainsCleanupErrorAndShutdownBarrier() throws Exception {
        ThreadPoolExecutor pool = pool(1);
        ScanIterator iterator = mock(ScanIterator.class);
        IllegalStateException original = new IllegalStateException("plan initialization failed");
        IllegalStateException cleanup = new IllegalStateException("iterator cleanup failed");
        doThrow(cleanup).when(iterator).close();
        AggregativeQueryService service = new AggregativeQueryService(pool, 500, 10) {
            @Override
            AggregativeQueryObserver newObserver(StreamObserver<QueryResponse> sender) {
                AggregativeQueryObserver observer = spy(super.newObserver(sender));
                doReturn(iterator).when(observer).getIterator(org.mockito.ArgumentMatchers.any());
                doThrow(original).when(observer).buildPlan(org.mockito.ArgumentMatchers.any());
                return observer;
            }
        };
        ResponseRecorder response = new ResponseRecorder();
        FutureTask<Void> closing = close(service);
        try {
            IllegalStateException thrown = org.junit.Assert.assertThrows(IllegalStateException.class,
                    () -> service.query(response).onNext(QueryRequest.getDefaultInstance()));
            org.junit.Assert.assertSame(original, thrown);
            assertEquals(1, thrown.getSuppressed().length);
            org.junit.Assert.assertSame(cleanup, thrown.getSuppressed()[0]);
            assertEquals(0, response.completed.get());
            assertTrue(response.responses.isEmpty());
            awaitShutdownWait(start(closing));
            assertFalse(closing.isDone());
        } finally {
            releaseMockQueries(service);
            closing.run();
            closing.get(1, TimeUnit.SECONDS);
            pool.shutdownNow();
        }
    }

    @Test(timeout = 5000)
    public void testPartialPartitionInitializationCleanupFailureRetainsQueryBarrier() throws Exception {
        ThreadPoolExecutor pool = pool(1);
        ScanIterator first = mock(ScanIterator.class);
        when(first.hasNext()).thenReturn(true);
        doThrow(new IllegalStateException("opened partition cleanup failed")).when(first).close();
        MultiPartitionIterator iterator = MultiPartitionIterator.of(Arrays.asList(1, 2), (id, key) -> {
            if (id == 1) {
                return first;
            }
            throw new IllegalStateException("second partition initialization failed");
        });
        QueryPlan plan = mock(QueryPlan.class);
        when(plan.hasIteratorResult()).thenReturn(true);
        ResponseRecorder response = new ResponseRecorder();
        AggregativeQueryService service = service(pool, iterator, plan, 500);
        FutureTask<Void> closing = close(service);
        try {
            service.query(response).onNext(QueryRequest.newBuilder().setQueryId("init-query").build());
            assertTrue(response.finished.await(1, TimeUnit.SECONDS));
            assertError(response, "init-query");
            verify(first).close();
            verify(plan).clear();
            awaitShutdownWait(start(closing));
            assertFalse(closing.isDone());
        } finally {
            releaseMockQueries(service);
            closing.run();
            closing.get(1, TimeUnit.SECONDS);
            pool.shutdownNow();
        }
    }

    @Test(timeout = 5000)
    public void testSequentialEmptyPartitionCleanupFailureRetainsQueryBarrier() throws Exception {
        ThreadPoolExecutor pool = pool(1);
        ScanIterator child = mock(ScanIterator.class);
        doThrow(new IllegalStateException("empty partition cleanup failed")).when(child).close();
        MultiPartitionIterator iterator = MultiPartitionIterator.of(Arrays.asList(1), (id, key) -> child);
        AggregativeQueryService service = service(pool, iterator, new QueryPlan(), 500);
        ResponseRecorder response = new ResponseRecorder();
        FutureTask<Void> closing = close(service);
        try {
            service.query(response).onNext(QueryRequest.newBuilder().setQueryId("empty-query").build());
            assertTrue(response.finished.await(1, TimeUnit.SECONDS));
            assertError(response, "empty-query");
            verify(child).close();
            awaitShutdownWait(start(closing));
            assertFalse(closing.isDone());
            assertFalse("failed sequential cleanup cannot release the executor", pool.isShutdown());
        } finally {
            releaseMockQueries(service);
            closing.run();
            closing.get(1, TimeUnit.SECONDS);
            pool.shutdownNow();
        }
    }

    @Test(timeout = 5000)
    public void testNativeAutomaticCloseFailureRetainsSequentialQueryBarrier() throws Exception {
        ThreadPoolExecutor pool = pool(1);
        RocksIterator raw = mock(RocksIterator.class);
        when(raw.isOwningHandle()).thenReturn(true);
        IllegalStateException failure = new IllegalStateException("native close failed once");
        doThrow(failure).doNothing().when(raw).close();
        // Native close fails before the callback or reference release can be reached.
        RocksDBScanIterator<?> child = new RocksDBScanIterator<>(raw, null, null,
                ScanIterator.Trait.SCAN_ANY, null, ignored -> { });
        MultiPartitionIterator iterator = MultiPartitionIterator.of(Arrays.asList(1), (id, key) -> child);
        AggregativeQueryService service = service(pool, iterator, new QueryPlan(), 500);
        ResponseRecorder response = new ResponseRecorder();
        FutureTask<Void> closing = close(service);
        try {
            service.query(response).onNext(QueryRequest.newBuilder().setQueryId("native-close-query").build());
            assertTrue(response.finished.await(1, TimeUnit.SECONDS));
            assertError(response, "native-close-query");
            org.junit.Assert.assertSame(failure,
                    org.junit.Assert.assertThrows(IllegalStateException.class, child::close));
            org.junit.Assert.assertThrows(IllegalStateException.class, iterator::close);
            verify(raw).close();
            awaitShutdownWait(start(closing));
            assertFalse("automatic native-close failure must retain the query registry", closing.isDone());
            assertFalse(pool.isShutdown());
        } finally {
            releaseMockQueries(service);
            closing.run();
            closing.get(1, TimeUnit.SECONDS);
            pool.shutdownNow();
        }
    }

    @Test(timeout = 10000)
    public void testSupplierInnerKeyFilterCloseFailureRetainsQueryBarrier() throws Exception {
        for (int constructor = 0; constructor < 3; constructor++) {
            assertSupplierFilterCloseFailureRetainsQueryBarrier(constructor);
        }
    }

    @Test(timeout = 5000)
    public void testPartitionCloseFailurePreventsSuccessfulAggregation() throws Exception {
        ThreadPoolExecutor pool = pool(2);
        ScanIterator child = mock(ScanIterator.class);
        doThrow(new IllegalStateException("child cleanup failed")).when(child).close();
        MultiPartitionIterator iterator = mock(MultiPartitionIterator.class);
        when(iterator.getIterators()).thenReturn(Arrays.asList(child));
        QueryPlan plan = mock(QueryPlan.class);
        when(plan.hasIteratorResult()).thenReturn(true);
        ResponseRecorder response = new ResponseRecorder();
        AggregativeQueryService service = service(pool, iterator, plan, 500);
        FutureTask<Void> closing = close(service);
        try {
            service.query(response).onNext(QueryRequest.newBuilder().setQueryId("child-query").build());
            assertTrue(response.finished.await(1, TimeUnit.SECONDS));
            assertError(response, "child-query");
            verify(plan, never()).execute(PipelineResult.EMPTY);
            verify(plan).clear();
            verify(iterator).close();
            verify(child).close();
            awaitShutdownWait(start(closing));
            assertFalse(closing.isDone());
        } finally {
            releaseMockQueries(service);
            closing.run();
            closing.get(1, TimeUnit.SECONDS);
            pool.shutdownNow();
        }
    }

    @Test(timeout = 5000)
    public void testRejectedPartitionCloseFailureStillClosesRemainingChildren() throws Exception {
        AtomicInteger submissions = new AtomicInteger();
        ThreadPoolExecutor pool = new ThreadPoolExecutor(1, 1, 0, TimeUnit.SECONDS,
                                                        new LinkedBlockingQueue<>()) {
            @Override
            public void execute(Runnable task) {
                if (submissions.incrementAndGet() > 1) {
                    throw new RejectedExecutionException("partition rejected");
                }
                super.execute(task);
            }
        };
        ScanIterator failed = mock(ScanIterator.class);
        doThrow(new IllegalStateException("rejected child cleanup failed")).when(failed).close();
        ScanIterator remaining = mock(ScanIterator.class);
        MultiPartitionIterator iterator = mock(MultiPartitionIterator.class);
        when(iterator.getIterators()).thenReturn(Arrays.asList(failed, remaining));
        QueryPlan plan = mock(QueryPlan.class);
        when(plan.hasIteratorResult()).thenReturn(true);
        ResponseRecorder response = new ResponseRecorder();
        AggregativeQueryService service = service(pool, iterator, plan, 500);
        FutureTask<Void> closing = close(service);
        try {
            service.query(response).onNext(QueryRequest.newBuilder().setQueryId("reject-query").build());
            assertTrue(response.finished.await(1, TimeUnit.SECONDS));
            assertError(response, "reject-query");
            verify(failed).close();
            verify(remaining).close();
            verify(plan).clear();
            verify(iterator).close();
            awaitShutdownWait(start(closing));
            assertFalse(closing.isDone());
        } finally {
            releaseMockQueries(service);
            closing.run();
            closing.get(1, TimeUnit.SECONDS);
            pool.shutdownNow();
        }
    }

    private static void assertSupplierFilterCloseFailureRetainsQueryBarrier(int constructor) throws Exception {
        ThreadPoolExecutor pool = pool(1);
        RocksIterator raw = mock(RocksIterator.class);
        when(raw.isOwningHandle()).thenReturn(true);
        IllegalStateException failure = new IllegalStateException("supplier iterator close failed once");
        doThrow(failure).doNothing().when(raw).close();
        RocksDBScanIterator<?> child = new RocksDBScanIterator<>(raw, null, null,
                ScanIterator.Trait.SCAN_ANY, null, ignored -> { });
        AtomicInteger supplierReturned = new AtomicInteger();
        MultiPartitionIterator iterator = MultiPartitionIterator.of(Arrays.asList(1), (id, key) -> {
            InnerKeyFilter<?> filter;
            if (constructor == 0) {
                filter = new InnerKeyFilter<>(child);
            } else if (constructor == 1) {
                filter = new InnerKeyFilter<>(child, true);
            } else {
                filter = new InnerKeyFilter<>(child, 10, 20);
            }
            supplierReturned.incrementAndGet();
            return filter;
        });
        AggregativeQueryService service = service(pool, iterator, new QueryPlan(), 500);
        ResponseRecorder response = new ResponseRecorder();
        FutureTask<Void> closing = close(service);
        try {
            service.query(response).onNext(QueryRequest.newBuilder().setQueryId("supplier-close-query").build());
            assertTrue(response.finished.await(1, TimeUnit.SECONDS));
            assertError(response, "supplier-close-query");
            assertEquals("factory must transfer ownership before any failing prefetch", 1, supplierReturned.get());
            org.junit.Assert.assertThrows(IllegalStateException.class, iterator::close);
            verify(raw).close();
            awaitShutdownWait(start(closing));
            assertFalse("factory prefetch failure must retain the aggregate query barrier", closing.isDone());
            assertFalse(pool.isShutdown());
        } finally {
            releaseMockQueries(service);
            closing.run();
            closing.get(1, TimeUnit.SECONDS);
            pool.shutdownNow();
        }
    }

    private static void assertContextCloseContinuesAfterCancellationFailure(Throwable failure) throws Exception {
        ThreadPoolExecutor pool = pool(1);
        CountDownLatch reading = new CountDownLatch(1);
        CountDownLatch closingIterator = new CountDownLatch(1);
        CountDownLatch allowClose = new CountDownLatch(1);
        AtomicBoolean iteratorClosed = new AtomicBoolean();
        AtomicBoolean databaseClosed = new AtomicBoolean();
        AtomicBoolean prematureDestruction = new AtomicBoolean();
        ScanIterator iterator = mock(ScanIterator.class);
        when(iterator.hasNext()).thenAnswer(invocation -> {
            reading.countDown();
            try {
                new CountDownLatch(1).await();
            } catch (InterruptedException ignored) {
                return false;
            }
            return false;
        });
        doAnswer(invocation -> {
            closingIterator.countDown();
            awaitUninterruptibly(allowClose);
            iteratorClosed.set(true);
            return null;
        }).when(iterator).close();
        AggregativeQueryService service = service(pool, iterator, new QueryPlan(), 500);
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
        try {
            context.getBeanFactory().registerSingleton("queryService", service);
            context.getBeanFactory().registerSingleton("storeStream", mock(HgStoreStreamImpl.class));
            context.getBeanFactory().registerSingleton("cleaner", mock(TTLCleaner.class));
            context.getDefaultListableBeanFactory().registerDisposableBean("database", () -> {
                prematureDestruction.set(!iteratorClosed.get() || !pool.isTerminated());
                databaseClosed.set(true);
            });
            context.register(ContextClosedListener.class, GrpcShutdownBarrier.class);
            context.refresh();
            StreamObserver<QueryResponse> firstSender = sender();
            StreamObserver<QueryResponse> secondSender = sender();
            StreamObserver<QueryRequest> first = service.query(firstSender);
            StreamObserver<QueryRequest> second = service.query(secondSender);
            Field field = AggregativeQueryService.class.getDeclaredField("queries");
            field.setAccessible(true);
            // Fail the first cancellation in the actual HashSet snapshot, regardless of identity hashes.
            boolean firstCancelsFirst = ((Set<?>) field.get(service)).iterator().next() == first;
            StreamObserver<QueryResponse> broken = firstCancelsFirst ? firstSender : secondSender;
            StreamObserver<QueryRequest> active = firstCancelsFirst ? second : first;
            doThrow(failure).when(broken).onError(org.mockito.ArgumentMatchers.any(Throwable.class));
            active.onNext(QueryRequest.getDefaultInstance());
            assertTrue(reading.await(1, TimeUnit.SECONDS));
            FutureTask<Void> closing = new FutureTask<>(() -> {
                context.close();
                return null;
            });
            Thread closer = start(closing);
            assertTrue("other queries must still be cancelled", closingIterator.await(1, TimeUnit.SECONDS));
            awaitShutdownWait(closer);
            assertFalse(closing.isDone());
            assertFalse(databaseClosed.get());
            assertFalse(pool.isShutdown());
            allowClose.countDown();
            closing.get(2, TimeUnit.SECONDS);
            assertTrue(databaseClosed.get());
            assertFalse("Spring must destroy databases only after query cleanup and worker termination",
                        prematureDestruction.get());
            verify(broken).onError(org.mockito.ArgumentMatchers.any(Throwable.class));
            verify(broken, never()).onCompleted();
            verify(iterator).close();
        } finally {
            allowClose.countDown();
            service.shutdownQueries();
            context.close();
            pool.shutdownNow();
        }
    }

    private static void releaseMockQueries(AggregativeQueryService service) throws Exception {
        // Only test mocks are discarded here; production deliberately has no bypass for this barrier.
        Field field = AggregativeQueryService.class.getDeclaredField("queries");
        field.setAccessible(true);
        synchronized (service) {
            ((Set<?>) field.get(service)).clear();
            service.notifyAll();
        }
    }
}
