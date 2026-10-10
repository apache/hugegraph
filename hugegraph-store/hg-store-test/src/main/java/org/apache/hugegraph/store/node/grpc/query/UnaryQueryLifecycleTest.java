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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.grpc.Context;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hugegraph.rocksdb.access.ScanIterator;
import org.apache.hugegraph.store.business.MultiPartitionIterator;
import org.apache.hugegraph.store.grpc.query.AggregateFunc;
import org.apache.hugegraph.store.grpc.query.QueryRequest;
import org.apache.hugegraph.store.grpc.query.QueryResponse;
import org.apache.hugegraph.store.query.KvSerializer;
import org.junit.Test;

public class UnaryQueryLifecycleTest extends AggregativeQueryTestSupport {

    private static AggregativeQueryService unaryService(ThreadPoolExecutor pool, ScanIterator iterator,
                                                        RuntimeException initializationFailure) {
        return new AggregativeQueryService(pool, 1000, 10) {
            @Override
            AggregativeQueryObserver newObserver(StreamObserver<QueryResponse> sender) {
                AggregativeQueryObserver owner = spy(super.newObserver(sender));
                if (initializationFailure == null) {
                    doReturn(iterator).when(owner).getIterator(org.mockito.ArgumentMatchers.any());
                    doReturn(iterator).when(owner).getCountIterator(org.mockito.ArgumentMatchers.any());
                } else {
                    doThrow(initializationFailure).when(owner).getIterator(org.mockito.ArgumentMatchers.any());
                    doThrow(initializationFailure).when(owner).getCountIterator(org.mockito.ArgumentMatchers.any());
                }
                return owner;
            }
        };
    }

    private static void call(AggregativeQueryService service, boolean count,
                             StreamObserver<QueryResponse> response) {
        QueryRequest request = QueryRequest.newBuilder().setQueryId("unary-query").build();
        if (count) {
            service.count(request, response);
        } else {
            service.query0(request, response);
        }
    }

    @Test(timeout = 5000)
    public void testSuccessFollowsIteratorReleaseAndCompletesOnce() {
        for (boolean count : new boolean[]{false, true}) {
            ThreadPoolExecutor pool = pool(1);
            ScanIterator iterator = mock(ScanIterator.class);
            AtomicBoolean closed = new AtomicBoolean();
            doAnswer(invocation -> { closed.set(true); return null; }).when(iterator).close();
            ResponseRecorder response = new ResponseRecorder() {
                @Override
                public void onNext(QueryResponse value) {
                    assertTrue(closed.get());
                    super.onNext(value);
                }
            };
            AggregativeQueryService service = unaryService(pool, iterator, null);
            try {
                call(service, count, response);
                assertEquals(1, response.responses.size());
                assertTrue(response.responses.get(0).getIsOk());
                assertTrue(response.responses.get(0).getIsFinished());
                assertEquals(count ? 1 : 0, response.responses.get(0).getDataCount());
                assertEquals(1, response.completed.get());
                verify(iterator).close();
                service.shutdownQueries();
            } finally {
                pool.shutdownNow();
            }
        }
    }

    @Test(timeout = 5000)
    public void testCountPreservesFunctionPayload() {
        ThreadPoolExecutor pool = pool(1);
        ScanIterator iterator = mock(ScanIterator.class);
        when(iterator.hasNext()).thenReturn(true, true, false);
        AggregativeQueryService service = unaryService(pool, iterator, null);
        ResponseRecorder response = new ResponseRecorder();
        try {
            QueryRequest request = QueryRequest.newBuilder().setQueryId("unary-query")
                                               .addFunctions(AggregateFunc.getDefaultInstance())
                                               .addFunctions(AggregateFunc.getDefaultInstance()).build();
            service.count(request, response);
            assertArrayEquals(KvSerializer.toBytes(List.of()),
                              response.responses.get(0).getData(0).getKey().toByteArray());
            assertArrayEquals(KvSerializer.toBytes(List.of(new AtomicLong(2), new AtomicLong(2))),
                              response.responses.get(0).getData(0).getValue().toByteArray());
            service.shutdownQueries();
        } finally {
            pool.shutdownNow();
        }
    }

    @Test(timeout = 5000)
    public void testCountPartitionsRunInParallelAndCloseBeforeResponse() throws Exception {
        ThreadPoolExecutor pool = pool(2);
        CountDownLatch entered = new CountDownLatch(2);
        CountDownLatch release = new CountDownLatch(1);
        MultiPartitionIterator iterator = mock(MultiPartitionIterator.class);
        ScanIterator first = mock(ScanIterator.class);
        ScanIterator second = mock(ScanIterator.class);
        when(iterator.getIterators()).thenReturn(List.of(first, second));
        for (ScanIterator child : List.of(first, second)) {
            when(child.hasNext()).thenAnswer(invocation -> {
                entered.countDown();
                awaitUninterruptibly(release);
                return false;
            });
        }
        AggregativeQueryService service = unaryService(pool, iterator, null);
        ResponseRecorder response = new ResponseRecorder();
        FutureTask<Void> request = new FutureTask<>(() -> { call(service, true, response); return null; });
        try {
            start(request);
            assertTrue(entered.await(1, TimeUnit.SECONDS));
            assertTrue(response.responses.isEmpty());
            release.countDown();
            request.get(1, TimeUnit.SECONDS);
            verify(first).close();
            verify(second).close();
            verify(iterator).close();
            assertTrue(response.responses.get(0).getIsOk());
            assertEquals(1, response.completed.get());
            service.shutdownQueries();
        } finally {
            release.countDown();
            pool.shutdownNow();
        }
    }

    @Test(timeout = 5000)
    public void testRejectedCountChildWaitsForAcceptedChildCleanup() throws Exception {
        AtomicInteger submissions = new AtomicInteger();
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch rejected = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        ThreadPoolExecutor pool = new ThreadPoolExecutor(1, 1, 0, TimeUnit.MILLISECONDS,
                                                       new LinkedBlockingQueue<>()) {
            @Override
            public void execute(Runnable task) {
                if (submissions.incrementAndGet() == 2) {
                    rejected.countDown();
                    throw new RejectedExecutionException("rejected child");
                }
                super.execute(task);
            }
        };
        MultiPartitionIterator iterator = mock(MultiPartitionIterator.class);
        ScanIterator first = mock(ScanIterator.class);
        ScanIterator second = mock(ScanIterator.class);
        when(iterator.getIterators()).thenReturn(List.of(first, second));
        when(first.hasNext()).thenAnswer(invocation -> {
            entered.countDown();
            awaitUninterruptibly(release);
            return false;
        });
        AggregativeQueryService service = unaryService(pool, iterator, null);
        ResponseRecorder response = new ResponseRecorder();
        FutureTask<Void> request = new FutureTask<>(() -> { call(service, true, response); return null; });
        try {
            start(request);
            assertTrue(entered.await(1, TimeUnit.SECONDS));
            assertTrue(rejected.await(1, TimeUnit.SECONDS));
            assertTrue(response.responses.isEmpty());
            assertFalse(request.isDone());
            release.countDown();
            request.get(1, TimeUnit.SECONDS);
            verify(first).close();
            verify(second).close();
            verify(iterator).close();
            assertError(response, "unary-query");
            assertEquals("rejected child", response.responses.get(0).getMessage());
            assertEquals(1, response.completed.get());
            service.shutdownQueries();
        } finally {
            release.countDown();
            pool.shutdownNow();
        }
    }

    @Test(timeout = 5000)
    public void testInitializationFailureReportsSingleErrorAndReleasesOwner() {
        for (boolean count : new boolean[]{false, true}) {
            ThreadPoolExecutor pool = pool(1);
            AggregativeQueryService service = unaryService(pool, null, new IllegalStateException("init"));
            ResponseRecorder response = new ResponseRecorder();
            try {
                call(service, count, response);
                assertError(response, "unary-query");
                assertEquals("init", response.responses.get(0).getMessage());
                assertEquals(1, response.completed.get());
                service.shutdownQueries();
            } finally {
                pool.shutdownNow();
            }
        }
    }

    @Test(timeout = 5000)
    public void testReadFailureClosesIteratorAndPreservesPrimaryError() {
        for (boolean count : new boolean[]{false, true}) {
            ThreadPoolExecutor pool = pool(1);
            ScanIterator iterator = mock(ScanIterator.class);
            when(iterator.hasNext()).thenThrow(new IllegalStateException("read"));
            AggregativeQueryService service = unaryService(pool, iterator, null);
            ResponseRecorder response = new ResponseRecorder();
            try {
                call(service, count, response);
                assertError(response, "unary-query");
                assertEquals("read", response.responses.get(0).getMessage());
                assertEquals(1, response.completed.get());
                verify(iterator).close();
                service.shutdownQueries();
            } finally {
                pool.shutdownNow();
            }
        }
    }

    @Test(timeout = 5000)
    public void testCloseFailureRetainsOwnerAndSuppressesFailureOnReadError() throws Exception {
        for (boolean count : new boolean[]{false, true}) {
            for (boolean readFailure : new boolean[]{false, true}) {
                ThreadPoolExecutor pool = pool(1);
                ScanIterator iterator = mock(ScanIterator.class);
                IllegalStateException primary = new IllegalStateException("read");
                IllegalStateException cleanup = new IllegalStateException("close");
                if (readFailure) {
                    when(iterator.hasNext()).thenThrow(primary);
                }
                doThrow(cleanup).when(iterator).close();
                AggregativeQueryService service = unaryService(pool, iterator, null);
                ResponseRecorder response = new ResponseRecorder();
                try {
                    call(service, count, response);
                    assertError(response, "unary-query");
                    assertEquals(readFailure ? "read" : "close", response.responses.get(0).getMessage());
                    assertEquals(1, response.completed.get());
                    if (readFailure) {
                        assertEquals(1, primary.getSuppressed().length);
                        assertSame(cleanup, primary.getSuppressed()[0]);
                    }
                    Field field = AggregativeQueryService.class.getDeclaredField("queries");
                    field.setAccessible(true);
                    assertEquals(1, ((Set<?>) field.get(service)).size());
                } finally {
                    pool.shutdownNow();
                }
            }
        }
    }

    @Test(timeout = 5000)
    public void testCancellationDuringReadWaitsForIteratorCleanup() throws Exception {
        for (boolean count : new boolean[]{false, true}) {
            ThreadPoolExecutor pool = pool(1);
            ScanIterator iterator = mock(ScanIterator.class);
            CountDownLatch entered = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            AtomicBoolean closed = new AtomicBoolean();
            when(iterator.hasNext()).thenAnswer(invocation -> {
                entered.countDown();
                awaitUninterruptibly(release);
                return false;
            });
            doAnswer(invocation -> { closed.set(true); return null; }).when(iterator).close();
            AggregativeQueryService service = unaryService(pool, iterator, null);
            ResponseRecorder response = new ResponseRecorder();
            FutureTask<Void> request = new FutureTask<>(() -> { call(service, count, response); return null; });
            FutureTask<Void> closing = close(service);
            try {
                start(request);
                assertTrue(entered.await(1, TimeUnit.SECONDS));
                awaitShutdownWait(start(closing));
                assertFalse(closing.isDone());
                assertFalse(closed.get());
                release.countDown();
                request.get(1, TimeUnit.SECONDS);
                closing.get(1, TimeUnit.SECONDS);
                assertTrue(closed.get());
                assertTrue(response.responses.isEmpty());
                assertEquals(0, response.completed.get());
                assertEquals(1, response.errors.get());
                verify(iterator).close();
            } finally {
                release.countDown();
                pool.shutdownNow();
            }
        }
    }

    @Test(timeout = 5000)
    @SuppressWarnings("unchecked")
    public void testTransportCancellationClosesWithoutTerminalCallback() throws Exception {
        for (boolean count : new boolean[]{false, true}) {
            ThreadPoolExecutor pool = pool(1);
            ScanIterator iterator = mock(ScanIterator.class);
            CountDownLatch entered = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            when(iterator.hasNext()).thenAnswer(invocation -> {
                entered.countDown();
                awaitUninterruptibly(release);
                return false;
            });
            AggregativeQueryService service = unaryService(pool, iterator, null);
            ServerCallStreamObserver<QueryResponse> response = mock(ServerCallStreamObserver.class);
            AtomicReference<Runnable> cancellation = new AtomicReference<>();
            doAnswer(invocation -> { cancellation.set(invocation.getArgument(0)); return null; })
                    .when(response).setOnCancelHandler(org.mockito.ArgumentMatchers.any());
            FutureTask<Void> request = new FutureTask<>(() -> { call(service, count, response); return null; });
            try {
                start(request);
                assertTrue(entered.await(1, TimeUnit.SECONDS));
                cancellation.get().run();
                release.countDown();
                request.get(1, TimeUnit.SECONDS);
                service.shutdownQueries();
                verify(iterator).close();
                verify(response, org.mockito.Mockito.never()).onCompleted();
                verify(response, org.mockito.Mockito.never()).onNext(org.mockito.ArgumentMatchers.any());
                verify(response, org.mockito.Mockito.never()).onError(org.mockito.ArgumentMatchers.any());
            } finally {
                release.countDown();
                pool.shutdownNow();
            }
        }
    }

    @Test(timeout = 5000)
    public void testContextCancellationInterruptsBothUnaryReads() throws Exception {
        for (boolean count : new boolean[]{false, true}) {
            ThreadPoolExecutor pool = pool(1);
            ScanIterator iterator = mock(ScanIterator.class);
            CountDownLatch entered = new CountDownLatch(1);
            CountDownLatch interrupted = new CountDownLatch(1);
            CountDownLatch fallback = new CountDownLatch(1);
            when(iterator.hasNext()).thenAnswer(invocation -> {
                entered.countDown();
                try {
                    fallback.await();
                } catch (InterruptedException expected) {
                    interrupted.countDown();
                    Thread.currentThread().interrupt();
                }
                return false;
            });
            AggregativeQueryService service = unaryService(pool, iterator, null);
            ResponseRecorder response = new ResponseRecorder();
            Context.CancellableContext context = Context.current().withCancellation();
            FutureTask<Void> request = new FutureTask<>(() -> {
                context.run(() -> call(service, count, response));
                return null;
            });
            try {
                start(request);
                assertTrue(entered.await(1, TimeUnit.SECONDS));
                context.cancel(new IllegalStateException("client canceled"));
                assertTrue("context must interrupt the unary read", interrupted.await(1, TimeUnit.SECONDS));
                request.get(1, TimeUnit.SECONDS);
                verify(iterator).close();
                assertTrue(response.responses.isEmpty());
                assertEquals(0, response.completed.get());
                assertEquals(0, response.errors.get());
                assertNoUnaryOwners(service);
            } finally {
                fallback.countDown();
                context.cancel(null);
                service.shutdownQueries();
                pool.shutdownNow();
            }
        }
    }

    @Test(timeout = 5000)
    public void testAlreadyCanceledContextNeverOpensUnaryIterator() throws Exception {
        for (boolean count : new boolean[]{false, true}) {
            ThreadPoolExecutor pool = pool(1);
            ScanIterator iterator = mock(ScanIterator.class);
            AggregativeQueryService service = unaryService(pool, iterator, null);
            ResponseRecorder response = new ResponseRecorder();
            Context.CancellableContext context = Context.current().withCancellation();
            context.cancel(null);
            try {
                context.run(() -> call(service, count, response));
                verify(iterator, org.mockito.Mockito.never()).hasNext();
                verify(iterator, org.mockito.Mockito.never()).close();
                assertTrue(response.responses.isEmpty());
                assertEquals(0, response.completed.get());
                assertNoUnaryOwners(service);
            } finally {
                Thread.interrupted();
                service.shutdownQueries();
                pool.shutdownNow();
            }
        }
    }

    @Test(timeout = 15000)
    public void testRealNettyCancellationInterruptsBothUnaryReads() throws Exception {
        for (boolean count : new boolean[]{false, true}) {
            ThreadPoolExecutor pool = pool(2);
            ExecutorService callbacks = Executors.newFixedThreadPool(2);
            ScanIterator iterator = mock(ScanIterator.class);
            CountDownLatch entered = new CountDownLatch(1);
            CountDownLatch interrupted = new CountDownLatch(1);
            CountDownLatch closed = new CountDownLatch(1);
            CountDownLatch fallback = new CountDownLatch(1);
            when(iterator.hasNext()).thenAnswer(invocation -> {
                entered.countDown();
                try {
                    fallback.await();
                } catch (InterruptedException expected) {
                    interrupted.countDown();
                    Thread.currentThread().interrupt();
                }
                return false;
            });
            doAnswer(invocation -> { closed.countDown(); return null; }).when(iterator).close();
            AggregativeQueryService service = unaryService(pool, iterator, null);
            io.grpc.Server server = io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder.forPort(0)
                    .executor(callbacks).addService(service).build().start();
            io.grpc.ManagedChannel channel = io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder
                    .forAddress("localhost", server.getPort()).usePlaintext().build();
            io.grpc.ClientCall<QueryRequest, QueryResponse> call = channel.newCall(count ?
                    org.apache.hugegraph.store.grpc.query.QueryServiceGrpc.getCountMethod() :
                    org.apache.hugegraph.store.grpc.query.QueryServiceGrpc.getQuery0Method(),
                    io.grpc.CallOptions.DEFAULT);
            CountDownLatch terminated = new CountDownLatch(1);
            AtomicReference<Status> terminal = new AtomicReference<>();
            AtomicInteger responses = new AtomicInteger();
            try {
                call.start(new io.grpc.ClientCall.Listener<QueryResponse>() {
                    @Override
                    public void onMessage(QueryResponse response) {
                        responses.incrementAndGet();
                    }

                    @Override
                    public void onClose(Status status, io.grpc.Metadata trailers) {
                        terminal.set(status);
                        terminated.countDown();
                    }
                }, new io.grpc.Metadata());
                call.request(1);
                call.sendMessage(QueryRequest.newBuilder().setQueryId("netty-unary").build());
                call.halfClose();
                assertTrue(entered.await(2, TimeUnit.SECONDS));
                call.cancel("cancel blocked unary read", null);
                assertTrue(terminated.await(2, TimeUnit.SECONDS));
                assertEquals(Status.Code.CANCELLED, terminal.get().getCode());
                assertTrue("transport context must interrupt unary read", interrupted.await(2, TimeUnit.SECONDS));
                assertTrue("cancellation must release iterator", closed.await(2, TimeUnit.SECONDS));
                service.shutdownQueries();
                verify(iterator).close();
                assertEquals(0, responses.get());
                assertNoUnaryOwners(service);
            } finally {
                fallback.countDown();
                call.cancel("test cleanup", null);
                channel.shutdownNow();
                server.shutdownNow();
                service.shutdownQueries();
                callbacks.shutdownNow();
                pool.shutdownNow();
            }
        }
    }

    private static void assertNoUnaryOwners(AggregativeQueryService service) throws Exception {
        Field field = AggregativeQueryService.class.getDeclaredField("queries");
        field.setAccessible(true);
        synchronized (service) {
            assertTrue(((Set<?>) field.get(service)).isEmpty());
        }
    }

    @Test(timeout = 5000)
    public void testBothUnaryEndpointsRejectRequestsAfterAdmissionCloses() {
        ThreadPoolExecutor pool = pool(1);
        AggregativeQueryService service = unaryService(pool, mock(ScanIterator.class), null);
        try {
            service.stopAcceptingQueries();
            for (boolean count : new boolean[]{false, true}) {
                try {
                    call(service, count, new ResponseRecorder());
                    org.junit.Assert.fail("closed service must reject unary RPCs");
                } catch (StatusRuntimeException e) {
                    assertEquals(Status.Code.UNAVAILABLE, e.getStatus().getCode());
                }
            }
        } finally {
            pool.shutdownNow();
        }
    }
}
