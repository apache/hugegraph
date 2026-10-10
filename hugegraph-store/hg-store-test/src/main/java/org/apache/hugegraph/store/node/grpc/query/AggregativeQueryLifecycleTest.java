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
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hugegraph.rocksdb.access.RocksDBSession;
import org.apache.hugegraph.rocksdb.access.ScanIterator;
import org.apache.hugegraph.store.business.MultiPartitionIterator;
import org.apache.hugegraph.store.grpc.query.QueryRequest;
import org.apache.hugegraph.store.grpc.query.QueryResponse;
import org.apache.hugegraph.store.node.grpc.query.model.PipelineResult;
import org.apache.hugegraph.store.node.grpc.query.model.QueryPlan;
import org.junit.Test;

public class AggregativeQueryLifecycleTest extends AggregativeQueryTestSupport {

    @Test(timeout = 5000)
    public void testPartitionTimeoutDoesNotClearPlanOrFinishBeforeChildFinally() throws Exception {
        ThreadPoolExecutor pool = pool(2);
        CountDownLatch reading = new CountDownLatch(1);
        CountDownLatch releaseChild = new CountDownLatch(1);
        CountDownLatch parentReturned = new CountDownLatch(1);
        CountDownLatch childClosed = new CountDownLatch(1);
        ScanIterator child = mock(ScanIterator.class);
        when(child.hasNext()).thenAnswer(invocation -> {
            reading.countDown();
            awaitUninterruptibly(releaseChild);
            return false;
        });
        doAnswer(invocation -> {
            childClosed.countDown();
            return null;
        }).when(child).close();
        MultiPartitionIterator iterator = mock(MultiPartitionIterator.class);
        when(iterator.getIterators()).thenReturn(Arrays.asList(child));
        QueryPlan plan = mock(QueryPlan.class);
        when(plan.hasIteratorResult()).thenReturn(true);
        AggregativeQueryService service = new AggregativeQueryService(pool, 50, 10) {
            @Override
            AggregativeQueryObserver newObserver(StreamObserver<QueryResponse> sender) {
                AggregativeQueryObserver observer = super.newObserver(sender);
                return fixture(observer, iterator, plan, parentReturned);
            }
        };
        ResponseRecorder response = new ResponseRecorder();
        try {
            service.query(response).onNext(QueryRequest.newBuilder().setQueryId("timeout-query").build());
            assertTrue(reading.await(1, TimeUnit.SECONDS));
            assertTrue("parent timeout must not wait on its own executor's queued children",
                       parentReturned.await(1, TimeUnit.SECONDS));
            assertTrue("a live client must receive the timeout error", response.received.await(1, TimeUnit.SECONDS));
            assertError(response, "timeout-query");
            assertEquals("response must not complete before child finally", 0, response.completed.get());
            verify(plan, never()).clear();
            verify(iterator, never()).close();
            FutureTask<Void> closing = close(service);
            awaitShutdownWait(start(closing));
            assertFalse(closing.isDone());
            releaseChild.countDown();
            closing.get(2, TimeUnit.SECONDS);
            assertTrue(childClosed.await(1, TimeUnit.SECONDS));
            verify(plan).clear();
            verify(iterator).close();
            assertTrue(pool.awaitTermination(1, TimeUnit.SECONDS));
            assertEquals(0, response.completed.get());
            assertEquals(1, response.errors.get());
        } finally {
            releaseChild.countDown();
            service.shutdownQueries();
            pool.shutdownNow();
        }
    }

    @Test(timeout = 5000)
    public void testRejectedPartitionsReleaseAllOpenedIterators() throws Exception {
        AtomicInteger submissions = new AtomicInteger();
        ThreadPoolExecutor pool = new ThreadPoolExecutor(1, 1, 0, TimeUnit.SECONDS,
                                                        new LinkedBlockingQueue<>()) {
            @Override
            public void execute(Runnable task) {
                if (submissions.incrementAndGet() > 1) {
                    throw new RejectedExecutionException("partition queue is full");
                }
                super.execute(task);
            }
        };
        ScanIterator first = mock(ScanIterator.class);
        ScanIterator second = mock(ScanIterator.class);
        MultiPartitionIterator iterator = mock(MultiPartitionIterator.class);
        when(iterator.getIterators()).thenReturn(Arrays.asList(first, second));
        QueryPlan plan = mock(QueryPlan.class);
        when(plan.hasIteratorResult()).thenReturn(true);
        CountDownLatch parentReturned = new CountDownLatch(1);
        AggregativeQueryService service = new AggregativeQueryService(pool, 500, 10) {
            @Override
            AggregativeQueryObserver newObserver(StreamObserver<QueryResponse> sender) {
                return fixture(super.newObserver(sender), iterator, plan, parentReturned);
            }
        };
        try {
            service.query(sender()).onNext(QueryRequest.getDefaultInstance());
            assertTrue(parentReturned.await(1, TimeUnit.SECONDS));
            FutureTask<Void> closing = close(service);
            start(closing);
            closing.get(2, TimeUnit.SECONDS);
            verify(first).close();
            verify(second).close();
            verify(iterator).close();
            verify(plan).clear();
        } finally {
            service.shutdownQueries();
            pool.shutdownNow();
        }
    }

    @Test(timeout = 5000)
    public void testHasNextFailureSendsErrorBeforeSingleCompletion() throws Exception {
        ThreadPoolExecutor pool = pool(1);
        ScanIterator iterator = mock(ScanIterator.class);
        when(iterator.hasNext()).thenThrow(new IllegalStateException("iterator read failed"));
        QueryPlan plan = mock(QueryPlan.class);
        when(plan.isEmpty()).thenReturn(true);
        ResponseRecorder response = new ResponseRecorder();
        AggregativeQueryService service = service(pool, iterator, plan, 500);
        try {
            service.query(response).onNext(QueryRequest.newBuilder().setQueryId("read-query").build());
            assertTrue(response.finished.await(1, TimeUnit.SECONDS));
            assertError(response, "read-query");
            assertEquals(1, response.completed.get());
            assertEquals(0, response.errors.get());
            service.shutdownQueries();
            verify(iterator).close();
            verify(plan).clear();
        } finally {
            service.shutdownQueries();
            pool.shutdownNow();
        }
    }

    @Test(timeout = 5000)
    public void testPartitionFailureSendsErrorBeforeSingleCompletion() throws Exception {
        ThreadPoolExecutor pool = pool(2);
        ScanIterator child = mock(ScanIterator.class);
        when(child.hasNext()).thenThrow(new IllegalStateException("partition read failed"));
        MultiPartitionIterator iterator = mock(MultiPartitionIterator.class);
        when(iterator.getIterators()).thenReturn(Arrays.asList(child));
        QueryPlan plan = mock(QueryPlan.class);
        when(plan.hasIteratorResult()).thenReturn(true);
        ResponseRecorder response = new ResponseRecorder();
        AggregativeQueryService service = service(pool, iterator, plan, 500);
        try {
            service.query(response).onNext(QueryRequest.newBuilder().setQueryId("partition-query").build());
            assertTrue(response.finished.await(1, TimeUnit.SECONDS));
            assertError(response, "partition-query");
            assertEquals(1, response.completed.get());
            service.shutdownQueries();
            verify(child).close();
            verify(iterator).close();
            verify(plan).clear();
        } finally {
            service.shutdownQueries();
            pool.shutdownNow();
        }
    }

    @Test(timeout = 5000)
    public void testAcceptedChildAndRejectedRemainingChildrenRetainCleanupOwnership() throws Exception {
        CountDownLatch childStarted = new CountDownLatch(1);
        CountDownLatch releaseChild = new CountDownLatch(1);
        CountDownLatch parentReturned = new CountDownLatch(1);
        AtomicInteger submissions = new AtomicInteger();
        ThreadPoolExecutor pool = new ThreadPoolExecutor(2, 2, 0, TimeUnit.SECONDS,
                                                        new LinkedBlockingQueue<>()) {
            @Override
            public void execute(Runnable task) {
                int submitted = submissions.incrementAndGet();
                if (submitted >= 3) {
                    throw new RejectedExecutionException("second partition rejected");
                }
                super.execute(task);
                if (submitted == 2) {
                    awaitUninterruptibly(childStarted);
                }
            }
        };
        ScanIterator accepted = mock(ScanIterator.class);
        when(accepted.hasNext()).thenAnswer(invocation -> {
            childStarted.countDown();
            awaitUninterruptibly(releaseChild);
            return false;
        });
        ScanIterator rejected = mock(ScanIterator.class);
        ScanIterator notSubmitted = mock(ScanIterator.class);
        MultiPartitionIterator iterator = mock(MultiPartitionIterator.class);
        when(iterator.getIterators()).thenReturn(Arrays.asList(accepted, rejected, notSubmitted));
        QueryPlan plan = mock(QueryPlan.class);
        when(plan.hasIteratorResult()).thenReturn(true);
        AggregativeQueryService service = new AggregativeQueryService(pool, 500, 10) {
            @Override
            AggregativeQueryObserver newObserver(StreamObserver<QueryResponse> sender) {
                return fixture(super.newObserver(sender), iterator, plan, parentReturned);
            }
        };
        ResponseRecorder response = new ResponseRecorder();
        try {
            service.query(response).onNext(QueryRequest.newBuilder().setQueryId("mixed-query").build());
            assertTrue(childStarted.await(1, TimeUnit.SECONDS));
            assertTrue(parentReturned.await(1, TimeUnit.SECONDS));
            assertError(response, "mixed-query");
            assertEquals(0, response.completed.get());
            verify(rejected).close();
            verify(notSubmitted).close();
            verify(accepted, never()).close();
            verify(iterator, never()).close();
            verify(plan, never()).clear();
            FutureTask<Void> closing = close(service);
            awaitShutdownWait(start(closing));
            assertFalse("accepted child's finally still owns the query", closing.isDone());
            releaseChild.countDown();
            closing.get(2, TimeUnit.SECONDS);
            assertEquals(0, response.completed.get());
            assertEquals(1, response.errors.get());
            verify(accepted).close();
            verify(iterator).close();
            verify(plan).clear();
        } finally {
            releaseChild.countDown();
            service.shutdownQueries();
            pool.shutdownNow();
        }
    }

    @Test(timeout = 5000)
    public void testShutdownReportsUnavailableWithoutReleasingActiveIteratorEarly() throws Exception {
        ThreadPoolExecutor pool = pool(1);
        ScanIterator iterator = mock(ScanIterator.class);
        CountDownLatch reading = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        when(iterator.hasNext()).thenAnswer(invocation -> {
            reading.countDown();
            awaitUninterruptibly(release);
            return false;
        });
        QueryPlan plan = new QueryPlan();
        ResponseRecorder response = new ResponseRecorder() {
            @Override
            public void onError(Throwable error) {
                assertEquals(Status.Code.UNAVAILABLE, Status.fromThrowable(error).getCode());
                super.onError(error);
            }
        };
        AggregativeQueryService service = service(pool, iterator, plan, 5000);
        try {
            StreamObserver<QueryRequest> request = service.query(response);
            request.onNext(QueryRequest.newBuilder().setQueryId("shutdown-query").build());
            assertTrue(reading.await(1, TimeUnit.SECONDS));
            FutureTask<Void> closing = close(service);
            start(closing);
            assertTrue(response.finished.await(1, TimeUnit.SECONDS));
            assertFalse("termination must retain active iterator ownership", closing.isDone());
            verify(iterator, never()).close();
            release.countDown();
            closing.get(2, TimeUnit.SECONDS);
            request.onCompleted();
            request.onError(Status.CANCELLED.asRuntimeException());
            assertTrue(response.responses.isEmpty());
            assertEquals(0, response.completed.get());
            assertEquals(1, response.errors.get());
            verify(iterator).close();
            assertTrue(pool.awaitTermination(1, TimeUnit.SECONDS));
        } finally {
            release.countDown();
            service.shutdownQueries();
            pool.shutdownNow();
        }
    }

    @Test(timeout = 5000)
    public void testShutdownReportsUnavailableForIdleQueryOnce() {
        ThreadPoolExecutor pool = pool(1);
        AggregativeQueryService service = service(pool, mock(ScanIterator.class), new QueryPlan(), 500);
        ResponseRecorder response = new ResponseRecorder();
        try {
            StreamObserver<QueryRequest> request = service.query(response);
            service.shutdownQueries();
            request.onCompleted();
            service.shutdownQueries();
            assertEquals(0, response.completed.get());
            assertEquals(1, response.errors.get());
            assertTrue(response.responses.isEmpty());
        } finally {
            pool.shutdownNow();
        }
    }

    @Test(timeout = 5000)
    public void testNormalIdleHalfCloseCompletesOnceAndExternalErrorNeverCompletesNormally() {
        ThreadPoolExecutor pool = pool(1);
        AggregativeQueryService service = service(pool, mock(ScanIterator.class), new QueryPlan(), 500);
        try {
            ResponseRecorder normal = new ResponseRecorder();
            StreamObserver<QueryRequest> normalRequest = service.query(normal);
            normalRequest.onCompleted();
            normalRequest.onCompleted();
            assertEquals(1, normal.completed.get());
            ResponseRecorder failed = new ResponseRecorder();
            StreamObserver<QueryRequest> failedRequest = service.query(failed);
            failedRequest.onError(Status.CANCELLED.asRuntimeException());
            failedRequest.onCompleted();
            assertEquals(0, failed.completed.get());
            service.shutdownQueries();
            assertEquals(1, normal.completed.get());
            assertEquals(0, failed.completed.get());
        } finally {
            pool.shutdownNow();
        }
    }

    @Test(timeout = 5000)
    public void testActiveNormalHalfCloseCompletesAfterWorkerCleanupOnce() throws Exception {
        ThreadPoolExecutor pool = pool(1);
        CountDownLatch reading = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        ScanIterator iterator = mock(ScanIterator.class);
        AtomicInteger reads = new AtomicInteger();
        when(iterator.next()).thenReturn(RocksDBSession.BackendColumn.of(new byte[]{1}, new byte[]{2}));
        when(iterator.hasNext()).thenAnswer(invocation -> {
            reading.countDown();
            assertTrue(release.await(2, TimeUnit.SECONDS));
            return reads.incrementAndGet() == 1;
        });
        ResponseRecorder response = new ResponseRecorder();
        AggregativeQueryService service = service(pool, iterator, new QueryPlan(), 500);
        try {
            StreamObserver<QueryRequest> request = service.query(response);
            request.onNext(QueryRequest.newBuilder().setQueryId("half-close-query").build());
            assertTrue(reading.await(1, TimeUnit.SECONDS));
            request.onCompleted();
            assertFalse(response.finished.await(50, TimeUnit.MILLISECONDS));
            release.countDown();
            assertTrue("normal half-close must complete after cleanup", response.finished.await(1, TimeUnit.SECONDS));
            request.onCompleted();
            service.shutdownQueries();
            assertEquals(1, response.completed.get());
            assertEquals(0, response.errors.get());
            assertEquals(1, response.responses.size());
            assertTrue(response.responses.get(0).getIsFinished());
            assertTrue(response.responses.get(0).getIsOk());
            assertEquals(1, response.responses.get(0).getDataCount());
            assertEquals(com.google.protobuf.ByteString.copyFrom(new byte[]{2}),
                         response.responses.get(0).getData(0).getValue());
            verify(iterator).close();
        } finally {
            release.countDown();
            service.shutdownQueries();
            pool.shutdownNow();
        }
    }

    @Test(timeout = 5000)
    public void testHalfCloseWithoutEnoughFeedbackReportsError() throws Exception {
        ThreadPoolExecutor pool = pool(1);
        ScanIterator iterator = mock(ScanIterator.class);
        when(iterator.hasNext()).thenReturn(true);
        when(iterator.next()).thenReturn(RocksDBSession.BackendColumn.of(new byte[]{1}, new byte[]{2}));
        ResponseRecorder response = new ResponseRecorder();
        AggregativeQueryService service = service(pool, iterator, new QueryPlan(), 5000);
        try {
            StreamObserver<QueryRequest> request = service.query(response);
            request.onNext(QueryRequest.newBuilder().setQueryId("feedback-half-close").build());
            request.onCompleted();
            assertTrue(response.finished.await(2, TimeUnit.SECONDS));
            assertEquals(17, response.responses.size());
            QueryResponse error = response.responses.get(16);
            assertFalse(error.getIsOk());
            assertTrue(error.getMessage().contains("without enough feedback"));
            assertEquals(1, response.completed.get());
            verify(iterator).close();
        } finally {
            service.shutdownQueries();
            pool.shutdownNow();
        }
    }

    @Test(timeout = 15000)
    public void testTransportCancellationAfterHalfCloseReleasesWorker() throws Exception {
        assertTransportCancellationAfterHalfClose(false);
    }

    @Test(timeout = 15000)
    public void testDeadlineAfterHalfCloseReleasesWorker() throws Exception {
        assertTransportCancellationAfterHalfClose(true);
    }

    private static void assertTransportCancellationAfterHalfClose(boolean deadline) throws Exception {
        ThreadPoolExecutor pool = pool(1);
        ScanIterator source = mock(ScanIterator.class);
        CountDownLatch reading = new CountDownLatch(1);
        CountDownLatch interrupted = new CountDownLatch(1);
        CountDownLatch released = new CountDownLatch(1);
        CountDownLatch fallbackRelease = new CountDownLatch(1);
        when(source.hasNext()).thenAnswer(invocation -> {
            reading.countDown();
            try {
                fallbackRelease.await();
            } catch (InterruptedException expected) {
                interrupted.countDown();
                Thread.currentThread().interrupt();
            }
            return false;
        });
        doAnswer(invocation -> {
            released.countDown();
            return null;
        }).when(source).close();
        AggregativeQueryService service = new AggregativeQueryService(pool, 5000, 10) {
            @Override
            AggregativeQueryObserver newObserver(StreamObserver<QueryResponse> sender) {
                return fixture(super.newObserver(sender), source, new QueryPlan(), null);
            }
        };
        String name = io.grpc.inprocess.InProcessServerBuilder.generateName();
        io.grpc.Server server = io.grpc.inprocess.InProcessServerBuilder.forName(name)
                .directExecutor().addService(service).build().start();
        io.grpc.ManagedChannel channel = io.grpc.inprocess.InProcessChannelBuilder.forName(name)
                .directExecutor().build();
        io.grpc.CallOptions options = deadline ? io.grpc.CallOptions.DEFAULT.withDeadlineAfter(2, TimeUnit.SECONDS) :
                                     io.grpc.CallOptions.DEFAULT;
        io.grpc.ClientCall<QueryRequest, QueryResponse> call = channel.newCall(
                org.apache.hugegraph.store.grpc.query.QueryServiceGrpc.getQueryMethod(), options);
        CountDownLatch terminated = new CountDownLatch(1);
        java.util.concurrent.atomic.AtomicReference<Status> terminal =
                new java.util.concurrent.atomic.AtomicReference<>();
        try {
            call.start(new io.grpc.ClientCall.Listener<QueryResponse>() {
                @Override
                public void onClose(Status status, io.grpc.Metadata trailers) {
                    terminal.set(status);
                    terminated.countDown();
                }
            }, new io.grpc.Metadata());
            call.request(1);
            call.sendMessage(QueryRequest.newBuilder().setQueryId("half-close-cancel").build());
            assertTrue(reading.await(1, TimeUnit.SECONDS));
            call.halfClose();
            if (!deadline) {
                call.cancel("cancel after half-close", null);
            }
            assertTrue(terminated.await(3, TimeUnit.SECONDS));
            assertEquals(deadline ? Status.Code.DEADLINE_EXCEEDED : Status.Code.CANCELLED,
                         terminal.get().getCode());
            assertTrue("transport cancellation must interrupt the worker after half-close",
                       interrupted.await(1, TimeUnit.SECONDS));
            assertTrue("transport cancellation must release the source without manual progress",
                       released.await(1, TimeUnit.SECONDS));
            verify(source).close();
        } finally {
            fallbackRelease.countDown();
            call.cancel("test cleanup", null);
            channel.shutdownNow();
            server.shutdownNow();
            service.shutdownQueries();
            pool.shutdownNow();
        }
    }

    @Test(timeout = 10000)
    public void testRealQueryClientCloseCancelsEarlyAndPreservesNormalCompletion() throws Exception {
        assertClientIteratorClose(true);
        assertClientIteratorClose(false);
    }

    private static void assertClientIteratorClose(boolean early) throws Exception {
        ThreadPoolExecutor pool = pool(1);
        ScanIterator source = mock(ScanIterator.class);
        CountDownLatch reading = new CountDownLatch(1);
        CountDownLatch released = new CountDownLatch(1);
        CountDownLatch completed = new CountDownLatch(1);
        if (early) {
            when(source.hasNext()).thenAnswer(invocation -> {
                reading.countDown();
                try {
                    new CountDownLatch(1).await();
                } catch (InterruptedException expected) {
                    Thread.currentThread().interrupt();
                }
                return false;
            });
        }
        doAnswer(invocation -> {
            released.countDown();
            return null;
        }).when(source).close();
        java.util.concurrent.atomic.AtomicReference<AggregativeQueryObserver> observed =
                new java.util.concurrent.atomic.AtomicReference<>();
        AggregativeQueryService service = new AggregativeQueryService(pool, 5000, 10) {
            @Override
            AggregativeQueryObserver newObserver(StreamObserver<QueryResponse> sender) {
                StreamObserver<QueryResponse> tracked = new StreamObserver<QueryResponse>() {
                    @Override
                    public void onNext(QueryResponse value) {
                        sender.onNext(value);
                    }

                    @Override
                    public void onError(Throwable failure) {
                        sender.onError(failure);
                    }

                    @Override
                    public void onCompleted() {
                        sender.onCompleted();
                        completed.countDown();
                    }
                };
                AggregativeQueryObserver observer = fixture(super.newObserver(tracked), source, new QueryPlan(), null);
                observed.set(observer);
                return observer;
            }
        };
        String name = io.grpc.inprocess.InProcessServerBuilder.generateName();
        io.grpc.Server server = io.grpc.inprocess.InProcessServerBuilder.forName(name)
                .directExecutor().addService(service).build().start();
        io.grpc.ManagedChannel channel = io.grpc.inprocess.InProcessChannelBuilder.forName(name)
                .directExecutor().build();
        org.apache.hugegraph.store.client.query.QueryV2Client.setTestChannel(channel);
        try {
            org.apache.hugegraph.store.client.query.QueryExecutor executor =
                    new org.apache.hugegraph.store.client.query.QueryExecutor(null, null, 5000L);
            java.lang.reflect.Method getIterator = executor.getClass().getDeclaredMethod(
                    "getIterator", String.class, QueryRequest.class);
            getIterator.setAccessible(true);
            org.apache.hugegraph.store.HgKvIterator<?> iterator =
                    (org.apache.hugegraph.store.HgKvIterator<?>) getIterator.invoke(
                            executor, "in-process-query-close", QueryRequest.newBuilder()
                                    .setQueryId("client-close").setTable("test").build());
            if (early) {
                assertTrue(reading.await(2, TimeUnit.SECONDS));
                FutureTask<Void> first = new FutureTask<>(() -> {
                    iterator.close();
                    return null;
                });
                Thread closer = start(first);
                iterator.close();
                first.get(2, TimeUnit.SECONDS);
                closer.join(1000);
            } else {
                assertFalse(iterator.hasNext());
                iterator.close();
                // A finished batch arrives before transport completion; don't cancel that RPC in teardown.
                assertTrue("normal RPC must finish before channel teardown", completed.await(2, TimeUnit.SECONDS));
            }
            assertTrue("iterator close must release resources before service shutdown",
                       released.await(2, TimeUnit.SECONDS));
            if (early) {
                org.mockito.ArgumentCaptor<Throwable> failure =
                        org.mockito.ArgumentCaptor.forClass(Throwable.class);
                verify(observed.get()).onError(failure.capture());
                verify(observed.get()).cancel();
                assertEquals(Status.Code.CANCELLED, Status.fromThrowable(failure.getValue()).getCode());
            } else {
                verify(observed.get(), never()).onError(org.mockito.ArgumentMatchers.any());
                verify(observed.get(), never()).cancel();
            }
            verify(observed.get()).onNext(org.mockito.ArgumentMatchers.any());
            verify(source).close();
        } finally {
            channel.shutdownNow();
            server.shutdownNow();
            service.shutdownQueries();
            pool.shutdownNow();
            org.apache.hugegraph.store.client.query.QueryV2Client.setTestChannel(null);
        }
    }

    @Test(timeout = 5000)
    public void testIteratorInitializationFailureKeepsOriginalThrowWithoutNormalCompletion() {
        ThreadPoolExecutor pool = pool(1);
        IllegalStateException failure = new IllegalStateException("iterator initialization failed");
        AggregativeQueryService service = new AggregativeQueryService(pool, 500, 10) {
            @Override
            AggregativeQueryObserver newObserver(StreamObserver<QueryResponse> sender) {
                AggregativeQueryObserver observer = spy(super.newObserver(sender));
                doThrow(failure).when(observer).getIterator(org.mockito.ArgumentMatchers.any());
                return observer;
            }
        };
        ResponseRecorder response = new ResponseRecorder();
        try {
            try {
                service.query(response).onNext(QueryRequest.getDefaultInstance());
                fail("synchronous iterator initialization must fail");
            } catch (IllegalStateException expected) {
                org.junit.Assert.assertSame(failure, expected);
            }
            assertEquals(0, response.completed.get());
            assertTrue(response.responses.isEmpty());
            service.shutdownQueries();
        } finally {
            pool.shutdownNow();
        }
    }

    @Test(timeout = 5000)
    public void testParentSubmissionRejectionKeepsOriginalThrowWithoutNormalCompletion() {
        ThreadPoolExecutor pool = pool(1);
        pool.shutdown();
        ScanIterator iterator = mock(ScanIterator.class);
        QueryPlan plan = mock(QueryPlan.class);
        AggregativeQueryService service = service(pool, iterator, plan, 500);
        ResponseRecorder response = new ResponseRecorder();
        try {
            service.query(response).onNext(QueryRequest.getDefaultInstance());
            fail("synchronous parent submission must be rejected");
        } catch (RejectedExecutionException expected) {
            assertEquals(0, response.completed.get());
            assertTrue(response.responses.isEmpty());
            verify(iterator).close();
            verify(plan).clear();
        } finally {
            service.shutdownQueries();
            pool.shutdownNow();
        }
    }

    @Test(timeout = 5000)
    public void testFinalSuccessIsSentOnlyAfterPlanAndIteratorCleanup() throws Exception {
        ThreadPoolExecutor pool = pool(2);
        ScanIterator child = mock(ScanIterator.class);
        MultiPartitionIterator iterator = mock(MultiPartitionIterator.class);
        when(iterator.getIterators()).thenReturn(Arrays.asList(child));
        ScanIterator result = mock(ScanIterator.class);
        when(result.hasNext()).thenReturn(true, false);
        when(result.next()).thenReturn(PipelineResult.EMPTY);
        QueryPlan plan = mock(QueryPlan.class);
        when(plan.hasIteratorResult()).thenReturn(true);
        when(plan.execute(PipelineResult.EMPTY)).thenReturn(result);
        AtomicBoolean cleared = new AtomicBoolean();
        AtomicBoolean closed = new AtomicBoolean();
        doAnswer(invocation -> {
            cleared.set(true);
            return null;
        }).when(plan).clear();
        doAnswer(invocation -> {
            closed.set(true);
            return null;
        }).when(iterator).close();
        ResponseRecorder response = new ResponseRecorder() {
            @Override
            public void onNext(QueryResponse batch) {
                assertTrue("success cannot precede plan cleanup", cleared.get());
                assertTrue("success cannot precede iterator cleanup", closed.get());
                super.onNext(batch);
            }
        };
        AggregativeQueryService service = service(pool, iterator, plan, 500);
        try {
            service.query(response).onNext(QueryRequest.newBuilder().setQueryId("success-query").build());
            assertTrue(response.finished.await(1, TimeUnit.SECONDS));
            assertEquals(1, response.responses.size());
            assertTrue(response.responses.get(0).getIsOk());
            assertTrue(response.responses.get(0).getIsFinished());
            service.shutdownQueries();
            verify(child).close();
        } finally {
            service.shutdownQueries();
            pool.shutdownNow();
        }
    }

    @Test
    public void testPartitionInitializationFailureClosesEveryOpenedChild() {
        ScanIterator first = mock(ScanIterator.class);
        ScanIterator second = mock(ScanIterator.class);
        when(first.hasNext()).thenReturn(true);
        when(second.hasNext()).thenThrow(new IllegalStateException("child initialization failed"));
        MultiPartitionIterator iterator = MultiPartitionIterator.of(Arrays.asList(1, 2),
                                                                     (id, key) -> id == 1 ? first : second);
        org.junit.Assert.assertThrows(IllegalStateException.class, iterator::getIterators);
        verify(first).close();
        verify(second).close();
        iterator.close();
    }

    @Test
    public void testPartitionInitializationCleanupFailureIsRetainedByParent() {
        ScanIterator first = mock(ScanIterator.class);
        ScanIterator second = mock(ScanIterator.class);
        when(first.hasNext()).thenReturn(true);
        when(second.hasNext()).thenThrow(new IllegalStateException("child initialization failed"));
        doThrow(new IllegalStateException("first cleanup failed")).when(first).close();
        MultiPartitionIterator iterator = MultiPartitionIterator.of(Arrays.asList(1, 2),
                                                                     (id, key) -> id == 1 ? first : second);
        org.junit.Assert.assertThrows(IllegalStateException.class, iterator::getIterators);
        verify(first).close();
        verify(second).close();
        org.junit.Assert.assertThrows(IllegalStateException.class, iterator::close);
        verify(first).close();
    }

    @Test
    public void testEmptyPartitionIteratorsAreReleasedBeforeOwnershipTransfer() {
        ScanIterator empty = mock(ScanIterator.class);
        ScanIterator nonempty = mock(ScanIterator.class);
        when(nonempty.hasNext()).thenReturn(true);
        MultiPartitionIterator iterator = MultiPartitionIterator.of(Arrays.asList(1, 2, 3),
                                                                     (id, key) -> id == 1 ? empty :
                                                                                  id == 2 ? null : nonempty);
        assertEquals(Arrays.asList(nonempty), iterator.getIterators());
        verify(empty).close();
        verify(nonempty, never()).close();
        nonempty.close();
    }

    @Test
    public void testSequentialScanReleasesEveryEmptyPartitionBeforeAdvancing() {
        ScanIterator first = mock(ScanIterator.class);
        ScanIterator second = mock(ScanIterator.class);
        ScanIterator nonempty = mock(ScanIterator.class);
        when(nonempty.hasNext()).thenReturn(true);
        MultiPartitionIterator iterator = MultiPartitionIterator.of(Arrays.asList(1, 2, 3, 4),
                (id, key) -> id == 1 ? first : id == 2 ? second : id == 3 ? null : nonempty);

        assertTrue(iterator.hasNext());
        verify(first).close();
        verify(second).close();
        verify(nonempty, never()).close();
        iterator.close();
        iterator.close();
        verify(nonempty).close();
    }

    @Test
    public void testClosedSequentialIteratorDoesNotOpenRemainingPartitions() {
        for (boolean initialized : new boolean[]{false, true}) {
            ScanIterator first = mock(ScanIterator.class);
            ScanIterator second = mock(ScanIterator.class);
            when(first.hasNext()).thenReturn(true);
            when(second.hasNext()).thenReturn(true);
            AtomicInteger opened = new AtomicInteger();
            MultiPartitionIterator iterator = MultiPartitionIterator.of(Arrays.asList(1, 2), (id, key) -> {
                opened.incrementAndGet();
                return id == 1 ? first : second;
            });
            if (initialized) {
                assertTrue(iterator.hasNext());
            }
            iterator.close();
            iterator.close();
            assertFalse(iterator.hasNext());
            assertFalse(iterator.isValid());
            assertEquals(0L, iterator.count());
            assertTrue(iterator.getIterators().isEmpty());
            org.junit.Assert.assertThrows(java.util.NoSuchElementException.class, iterator::next);
            assertEquals(initialized ? 1 : 0, opened.get());
            if (initialized) {
                verify(first).close();
            } else {
                verify(first, never()).close();
            }
            verify(second, never()).close();
        }
    }

    @Test
    public void testSequentialCloseFailureDoesNotReopenRemainingPartitions() {
        ScanIterator first = mock(ScanIterator.class);
        ScanIterator second = mock(ScanIterator.class);
        when(first.hasNext()).thenReturn(true);
        when(second.hasNext()).thenReturn(true);
        IllegalStateException cleanup = new IllegalStateException("partition cleanup failed");
        doThrow(cleanup).when(first).close();
        AtomicInteger opened = new AtomicInteger();
        MultiPartitionIterator iterator = MultiPartitionIterator.of(Arrays.asList(1, 2), (id, key) -> {
            opened.incrementAndGet();
            return id == 1 ? first : second;
        });
        assertTrue(iterator.hasNext());
        IllegalStateException retained = org.junit.Assert.assertThrows(IllegalStateException.class, iterator::close);
        org.junit.Assert.assertSame(cleanup, retained.getCause());
        assertFalse(iterator.hasNext());
        assertFalse(iterator.isValid());
        assertEquals(0L, iterator.count());
        assertTrue(iterator.getIterators().isEmpty());
        org.junit.Assert.assertThrows(java.util.NoSuchElementException.class, iterator::next);
        org.junit.Assert.assertSame(retained,
                org.junit.Assert.assertThrows(IllegalStateException.class, iterator::close));
        assertEquals(1, opened.get());
        verify(first).close();
        verify(second, never()).close();
    }

    @Test
    public void testSequentialInitializationFailureReleasesItsCreatedIterator() {
        ScanIterator child = mock(ScanIterator.class);
        IllegalStateException original = new IllegalStateException("partition initialization failed");
        when(child.hasNext()).thenThrow(original);
        MultiPartitionIterator iterator = MultiPartitionIterator.of(Arrays.asList(1), (id, key) -> child);

        org.junit.Assert.assertSame(original,
                org.junit.Assert.assertThrows(IllegalStateException.class, iterator::hasNext));
        verify(child).close();
        iterator.close();
    }

    @Test
    public void testSequentialInitializationCleanupFailureRetainsOriginalErrorAndBarrier() {
        ScanIterator child = mock(ScanIterator.class);
        IllegalStateException original = new IllegalStateException("partition initialization failed");
        IllegalStateException cleanup = new IllegalStateException("partition cleanup failed");
        when(child.hasNext()).thenThrow(original);
        doThrow(cleanup).when(child).close();
        MultiPartitionIterator iterator = MultiPartitionIterator.of(Arrays.asList(1), (id, key) -> child);

        org.junit.Assert.assertSame(original,
                org.junit.Assert.assertThrows(IllegalStateException.class, iterator::hasNext));
        assertEquals(1, original.getSuppressed().length);
        org.junit.Assert.assertSame(cleanup, original.getSuppressed()[0]);
        IllegalStateException retained = org.junit.Assert.assertThrows(IllegalStateException.class, iterator::close);
        org.junit.Assert.assertSame(cleanup, retained.getCause());
        verify(child).close();
    }

    @Test
    public void testSequentialCountIncludesAlreadyOpenedPartitionAndClosesAllSources() {
        ScanIterator first = mock(ScanIterator.class);
        ScanIterator second = mock(ScanIterator.class);
        when(first.hasNext()).thenReturn(true);
        when(second.hasNext()).thenReturn(true);
        when(first.count()).thenReturn(7L);
        when(second.count()).thenReturn(11L);
        MultiPartitionIterator iterator = MultiPartitionIterator.of(Arrays.asList(1, 2),
                (id, key) -> id == 1 ? first : second);

        assertTrue(iterator.hasNext());
        assertEquals(18L, iterator.count());
        assertFalse(iterator.hasNext());
        iterator.close();
        verify(first).close();
        verify(second).close();
    }

    @Test
    public void testSequentialCountFailureReleasesCurrentSource() {
        ScanIterator child = mock(ScanIterator.class);
        IllegalStateException original = new IllegalStateException("partition count failed");
        when(child.hasNext()).thenReturn(true);
        when(child.count()).thenThrow(original);
        MultiPartitionIterator iterator = MultiPartitionIterator.of(Arrays.asList(1), (id, key) -> child);

        org.junit.Assert.assertSame(original,
                org.junit.Assert.assertThrows(IllegalStateException.class, iterator::count));
        iterator.close();
        verify(child).close();
    }

    @Test
    public void testSequentialCountCleanupFailureRetainsOriginalErrorAndBarrier() {
        ScanIterator child = mock(ScanIterator.class);
        IllegalStateException original = new IllegalStateException("partition count failed");
        IllegalStateException cleanup = new IllegalStateException("partition cleanup failed");
        when(child.hasNext()).thenReturn(true);
        when(child.count()).thenThrow(original);
        doThrow(cleanup).when(child).close();
        MultiPartitionIterator iterator = MultiPartitionIterator.of(Arrays.asList(1), (id, key) -> child);

        org.junit.Assert.assertSame(original,
                org.junit.Assert.assertThrows(IllegalStateException.class, iterator::count));
        assertEquals(1, original.getSuppressed().length);
        org.junit.Assert.assertSame(cleanup, original.getSuppressed()[0]);
        org.junit.Assert.assertThrows(IllegalStateException.class, iterator::close);
        verify(child).close();
    }

    @Test
    public void testSequentialExhaustionCleanupFailureRemainsVisibleToParent() {
        ScanIterator child = mock(ScanIterator.class);
        when(child.hasNext()).thenReturn(true, false);
        IllegalStateException cleanup = new IllegalStateException("exhausted partition cleanup failed");
        doThrow(cleanup).when(child).close();
        MultiPartitionIterator iterator = MultiPartitionIterator.of(Arrays.asList(1), (id, key) -> child);

        assertTrue(iterator.hasNext());
        org.junit.Assert.assertSame(cleanup,
                org.junit.Assert.assertThrows(IllegalStateException.class, iterator::next));
        org.junit.Assert.assertThrows(IllegalStateException.class, iterator::close);
        verify(child).close();
    }

    @Test
    public void testPlanCleanupContinuesAfterStageFailure() {
        QueryStage failed = mock(QueryStage.class);
        QueryStage remaining = mock(QueryStage.class);
        doThrow(new IllegalStateException("stage cleanup failed")).when(failed).close();
        QueryPlan plan = new QueryPlan();
        plan.addStage(failed);
        plan.addStage(remaining);
        org.junit.Assert.assertThrows(IllegalStateException.class, plan::clear);
        verify(failed).close();
        verify(remaining).close();
        assertTrue(plan.isEmpty());
    }
}
