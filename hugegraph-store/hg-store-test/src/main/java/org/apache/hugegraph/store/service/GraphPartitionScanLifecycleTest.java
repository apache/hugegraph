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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hugegraph.rocksdb.access.ScanIterator;
import org.apache.hugegraph.store.business.BusinessHandler;
import org.apache.hugegraph.store.business.GraphStoreIterator;
import org.apache.hugegraph.store.grpc.Graphpb;
import org.apache.hugegraph.store.grpc.Graphpb.ScanPartitionRequest;
import org.apache.hugegraph.store.grpc.Graphpb.ScanResponse;
import org.apache.hugegraph.store.node.grpc.HgStoreStreamImpl;
import org.apache.hugegraph.store.node.grpc.scan.ScanResponseObserver;
import org.junit.Test;

import io.grpc.Context;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

public class GraphPartitionScanLifecycleTest extends GraphPartitionScanTestSupport {

    @Test
    public void testNaturalEndAndLimitCloseIteratorBeforeCompleting() throws Exception {
        for (int limit : new int[]{0, 1}) {
            ThreadPoolExecutor executor = executor();
            try {
                HgStoreStreamImpl service = service(executor);
                GraphStoreIterator<Graphpb.Vertex> iterator = mock(GraphStoreIterator.class);
                AtomicInteger consumed = new AtomicInteger();
                when(iterator.hasNext()).thenAnswer(call -> consumed.get() < 2);
                when(iterator.next()).thenAnswer(call -> {
                    consumed.incrementAndGet();
                    return Graphpb.Vertex.getDefaultInstance();
                });
                BusinessHandler handler = mock(BusinessHandler.class);
                doReturn(iterator).when(handler).scan(any());
                StreamObserver<ScanResponse> response = mock(StreamObserver.class);
                AtomicInteger delivered = new AtomicInteger();
                doAnswer(call -> {
                    delivered.addAndGet(((ScanResponse) call.getArgument(0)).getVertexCount());
                    return null;
                }).when(response).onNext(any());
                StreamObserver<ScanPartitionRequest> request = service.scanGraphPartition(response, handler);
                request.onNext(request(limit));
                verify(response, timeout(2000)).onCompleted();
                service.awaitScanCleanup();
                verify(iterator).close();
                assertEquals(limit == 0 ? 2 : 1, consumed.get());
                assertEquals(consumed.get(), delivered.get());
                assertTrue(registry(service).isEmpty());
                request.onError(new IllegalStateException("late cancellation"));
                verify(iterator).close();
            } finally {
                executor.shutdown();
                assertTrue(executor.awaitTermination(2, TimeUnit.SECONDS));
            }
        }
    }

    @Test(timeout = 30000)
    public void testFinalBatchPublishedAfterEmptyPollIsDelivered() throws Exception {
        ThreadPoolExecutor executor = executor();
        CountDownLatch finalRead = new CountDownLatch(1);
        CountDownLatch releaseRead = new CountDownLatch(1);
        CountDownLatch emptyPoll = new CountDownLatch(1);
        CountDownLatch releasePoll = new CountDownLatch(1);
        AtomicInteger consumed = new AtomicInteger();
        GraphStoreIterator<Graphpb.Vertex> iterator = mock(GraphStoreIterator.class);
        when(iterator.hasNext()).thenAnswer(call -> consumed.get() < 100001);
        when(iterator.next()).thenAnswer(call -> {
            if (consumed.get() == 100000) {
                finalRead.countDown();
                assertTrue(releaseRead.await(10, TimeUnit.SECONDS));
            }
            consumed.incrementAndGet();
            return Graphpb.Vertex.getDefaultInstance();
        });
        BusinessHandler handler = mock(BusinessHandler.class);
        doReturn(iterator).when(handler).scan(any());
        StreamObserver<ScanResponse> response = mock(StreamObserver.class);
        AtomicInteger delivered = new AtomicInteger();
        doAnswer(call -> {
            delivered.addAndGet(((ScanResponse) call.getArgument(0)).getVertexCount());
            return null;
        }).when(response).onNext(any());
        ScanResponseObserver<Graphpb.Vertex> request =
                new ScanResponseObserver<>(response, handler, executor);
        LinkedBlockingQueue<ScanResponse> packages = new LinkedBlockingQueue<ScanResponse>(16) {
            @Override
            public ScanResponse poll(long timeout, TimeUnit unit) throws InterruptedException {
                ScanResponse value = super.poll(timeout, unit);
                if (value == null && emptyPoll.getCount() != 0) {
                    emptyPoll.countDown();
                    assertTrue(releasePoll.await(10, TimeUnit.SECONDS));
                }
                return value;
            }
        };
        Field field = ScanResponseObserver.class.getDeclaredField("packages");
        field.setAccessible(true);
        field.set(request, packages);
        try {
            request.onNext(request(0));
            assertTrue(finalRead.await(10, TimeUnit.SECONDS));
            assertTrue("sender must have actually polled an empty queue",
                       emptyPoll.await(10, TimeUnit.SECONDS));
            assertEquals(100000, delivered.get());
            releaseRead.countDown();
            // The sender occupies one worker; this probe runs only after the real reader
            // has published its final batch, closed its iterator and set readOver.
            executor.submit(() -> { }).get(10, TimeUnit.SECONDS);
            verify(iterator).close();
            assertEquals(1, packages.size());
            releasePoll.countDown();
            verify(response, timeout(5000)).onCompleted();
            assertEquals(100001, consumed.get());
            assertEquals(consumed.get(), delivered.get());
            assertTrue(packages.isEmpty());
            request.onCompleted();
            request.onError(new IllegalStateException("late cancellation"));
            verify(response).onCompleted();
            verify(response, org.mockito.Mockito.never()).onError(any());
            verify(iterator).close();
        } finally {
            releaseRead.countDown();
            releasePoll.countDown();
            executor.shutdown();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testEmptyScanClosesOnceAndReleasesBarrier() throws Exception {
        ThreadPoolExecutor executor = executor();
        try {
            HgStoreStreamImpl service = service(executor);
            GraphStoreIterator<?> iterator = mock(GraphStoreIterator.class);
            BusinessHandler handler = mock(BusinessHandler.class);
            doReturn(iterator).when(handler).scan(any());
            StreamObserver<ScanResponse> response = mock(StreamObserver.class);
            service.scanGraphPartition(response, handler).onNext(request(0));
            verify(response, timeout(2000)).onCompleted();
            service.awaitScanCleanup();
            verify(iterator).close();
            assertTrue(registry(service).isEmpty());
        } finally {
            executor.shutdown();
            assertTrue(executor.awaitTermination(2, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testRejectedWorkerClosesIteratorAndTerminatesResponse() throws Exception {
        ThreadPoolExecutor executor = executor();
        executor.shutdown();
        HgStoreStreamImpl service = service(executor);
        GraphStoreIterator<?> iterator = mock(GraphStoreIterator.class);
        BusinessHandler handler = mock(BusinessHandler.class);
        doReturn(iterator).when(handler).scan(any());
        StreamObserver<ScanResponse> response = mock(StreamObserver.class);
        service.scanGraphPartition(response, handler).onNext(request(0));
        verify(response).onError(any());
        verify(iterator).close();
        assertTrue(registry(service).isEmpty());
    }

    @Test
    public void testReadFailureClosesIteratorAndPreservesPrimaryError() throws Exception {
        ThreadPoolExecutor executor = executor();
        try {
            HgStoreStreamImpl service = service(executor);
            GraphStoreIterator<?> iterator = mock(GraphStoreIterator.class);
            RuntimeException failure = new IllegalStateException("native read failed");
            when(iterator.hasNext()).thenThrow(failure);
            BusinessHandler handler = mock(BusinessHandler.class);
            doReturn(iterator).when(handler).scan(any());
            StreamObserver<ScanResponse> response = mock(StreamObserver.class);
            service.scanGraphPartition(response, handler).onNext(request(0));
            verify(response, timeout(2000)).onError(failure);
            service.awaitScanCleanup();
            verify(iterator).close();
        } finally {
            executor.shutdown();
            assertTrue(executor.awaitTermination(2, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testClientCompletionBeforeInitializationReleasesBarrier() throws Exception {
        ThreadPoolExecutor executor = executor();
        try {
            HgStoreStreamImpl service = service(executor);
            StreamObserver<ScanResponse> response = mock(StreamObserver.class);
            StreamObserver<ScanPartitionRequest> request =
                    service.scanGraphPartition(response, mock(BusinessHandler.class));
            request.onCompleted();
            request.onCompleted();
            verify(response).onCompleted();
            assertTrue(registry(service).isEmpty());
        } finally {
            executor.shutdown();
            assertTrue(executor.awaitTermination(2, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testReadFailureKeepsCloseFailureSuppressedAndSticky() throws Exception {
        ThreadPoolExecutor executor = executor();
        try {
            HgStoreStreamImpl service = service(executor);
            GraphStoreIterator<?> iterator = mock(GraphStoreIterator.class);
            RuntimeException primary = new IllegalStateException("read");
            RuntimeException cleanup = new IllegalArgumentException("close");
            when(iterator.hasNext()).thenThrow(primary);
            doThrow(cleanup).when(iterator).close();
            BusinessHandler handler = mock(BusinessHandler.class);
            doReturn(iterator).when(handler).scan(any());
            StreamObserver<ScanResponse> response = mock(StreamObserver.class);
            service.scanGraphPartition(response, handler).onNext(request(0));
            verify(response, timeout(2000)).onError(primary);
            assertEquals(1, primary.getSuppressed().length);
            assertSame(cleanup, primary.getSuppressed()[0]);
            verify(iterator).close();
            assertEquals(1, registry(service).size());
        } finally {
            executor.shutdown();
            assertTrue(executor.awaitTermination(2, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testFullCreditWindowResumesWithOrderedPagesAndKeepsFinalPartialBatch() throws Exception {
        checkCreditWindow(false, 7);
    }

    @Test(timeout = 5000)
    public void testHalfCloseBeforeWorkerStartsKeepsFinalPartialBatch() throws Exception {
        ThreadPoolExecutor executor = executor();
        CountDownLatch workersBlocked = new CountDownLatch(2);
        CountDownLatch release = new CountDownLatch(1);
        List<Future<?>> blockers = new ArrayList<>();
        try {
            // Finish service and mock initialization before establishing the worker gate.
            HgStoreStreamImpl service = service(executor);
            GraphStoreIterator<Graphpb.Vertex> iterator = mock(GraphStoreIterator.class);
            AtomicInteger consumed = new AtomicInteger();
            AtomicInteger reads = new AtomicInteger();
            when(iterator.hasNext()).thenAnswer(call -> {
                reads.incrementAndGet();
                return consumed.get() < 2;
            });
            when(iterator.next()).thenAnswer(call -> {
                consumed.incrementAndGet();
                return Graphpb.Vertex.getDefaultInstance();
            });
            BusinessHandler handler = mock(BusinessHandler.class);
            doReturn(iterator).when(handler).scan(any());
            StreamObserver<ScanResponse> response = mock(StreamObserver.class);
            AtomicInteger delivered = new AtomicInteger();
            doAnswer(call -> {
                delivered.addAndGet(((ScanResponse) call.getArgument(0)).getVertexCount());
                return null;
            }).when(response).onNext(any());
            StreamObserver<ScanPartitionRequest> request = service.scanGraphPartition(response, handler);
            for (int i = 0; i < 2; i++) {
                blockers.add(executor.submit(() -> {
                    workersBlocked.countDown();
                    try {
                        release.await();
                    } catch (InterruptedException failure) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException(failure);
                    }
                }));
            }
            assertTrue(workersBlocked.await(2, TimeUnit.SECONDS));
            request.onNext(request(0));
            assertEquals("reading must not begin before half-close", 0, reads.get());
            request.onCompleted();
            assertEquals("queued reader must remain gated through half-close", 0, reads.get());
            release.countDown();
            for (Future<?> blocker : blockers) {
                blocker.get(2, TimeUnit.SECONDS);
            }
            verify(response, timeout(2000)).onCompleted();
            service.awaitScanCleanup();
            assertEquals(2, delivered.get());
            verify(iterator).close();
            request.onCompleted();
            verify(response).onCompleted();
            assertTrue(registry(service).isEmpty());
        } finally {
            release.countDown();
            executor.shutdown();
            assertTrue(executor.awaitTermination(2, TimeUnit.SECONDS));
        }
    }

    @Test(timeout = 5000)
    public void testContextCancellationInterruptsBlockedReaderBeforeCleanup() throws Exception {
        checkBlockedReaderCancellation(false);
    }

    @Test(timeout = 5000)
    public void testContextDeadlineInterruptsBlockedReaderBeforeCleanup() throws Exception {
        checkBlockedReaderCancellation(true);
    }

    private static void checkBlockedReaderCancellation(boolean deadline) throws Exception {
        ThreadPoolExecutor executor = new ThreadPoolExecutor(1, 1, 0, TimeUnit.MILLISECONDS,
                                                             new LinkedBlockingQueue<>());
        ScheduledExecutorService timer = Executors.newSingleThreadScheduledExecutor();
        HgStoreStreamImpl service = service(executor);
        GraphStoreIterator<?> iterator = mock(GraphStoreIterator.class);
        CountDownLatch reading = new CountDownLatch(1);
        CountDownLatch readExited = new CountDownLatch(1);
        AtomicBoolean interrupted = new AtomicBoolean();
        when(iterator.hasNext()).thenAnswer(call -> {
            reading.countDown();
            try {
                new CountDownLatch(1).await();
                throw new AssertionError("blocked reader must exit through interruption");
            } catch (InterruptedException expected) {
                interrupted.set(true);
                Thread.currentThread().interrupt();
                return false;
            } finally {
                readExited.countDown();
            }
        });
        AtomicBoolean closedAfterRead = new AtomicBoolean();
        doAnswer(call -> {
            closedAfterRead.set(readExited.getCount() == 0);
            return null;
        }).when(iterator).close();
        BusinessHandler handler = mock(BusinessHandler.class);
        doReturn(iterator).when(handler).scan(any());
        StreamObserver<ScanResponse> response = mock(StreamObserver.class);
        Context.CancellableContext context = deadline ?
                Context.current().withDeadlineAfter(1, TimeUnit.SECONDS, timer) :
                Context.current().withCancellation();
        Thread cancellation = null;
        try {
            StreamObserver<ScanPartitionRequest> request = context.call(() ->
                    service.scanGraphPartition(response, handler));
            request.onNext(request(0));
            assertTrue("real worker must enter its interruptible read", reading.await(500, TimeUnit.MILLISECONDS));
            if (!deadline) {
                FutureTask<Void> cancel = new FutureTask<>(() -> {
                    context.cancel(null);
                    return null;
                });
                cancellation = new Thread(cancel, "test-partition-context-cancel");
                cancellation.setDaemon(true);
                cancellation.start();
                cancel.get(2, TimeUnit.SECONDS);
            }
            assertTrue("context cancellation must interrupt the owned reader", readExited.await(2, TimeUnit.SECONDS));
            assertTrue(interrupted.get());
            verify(response, timeout(2000)).onError(any());
            service.awaitScanCleanup();
            verify(iterator).close();
            assertTrue(closedAfterRead.get());
            assertTrue(registry(service).isEmpty());
            // A released worker must remain usable and free from a cancellation interrupt.
            assertFalse(executor.submit(() -> Thread.currentThread().isInterrupted()).get(1, TimeUnit.SECONDS));
        } finally {
            // Also release an old implementation's blocked reader when this regression fails.
            executor.shutdownNow();
            context.cancel(null);
            timer.shutdownNow();
            assertTrue(executor.awaitTermination(2, TimeUnit.SECONDS));
            if (cancellation != null) {
                cancellation.join(1000);
            }
        }
    }

    @Test
    public void testHalfCloseDeliversEntirePermittedWindowAndCompletes() throws Exception {
        checkCreditWindow(true, 0);
    }

    @Test
    public void testHalfCloseReportsInsufficientCreditAfterDeliveringPermittedPages() throws Exception {
        checkCreditWindow(true, 7);
    }

    private void checkCreditWindow(boolean halfClose, int extraRows) throws Exception {
        ThreadPoolExecutor executor = executor();
        HgStoreStreamImpl service = service(executor);
        final int batchSize = 100000;
        final int window = 8;
        final int total = batchSize * window + extraRows;
        AtomicInteger closes = new AtomicInteger();
        Field suppliers = org.apache.hugegraph.store.business.BusinessHandlerImpl.class
                .getDeclaredField("GRAPH_SUPPLIER_CACHE");
        suppliers.setAccessible(true);
        @SuppressWarnings("unchecked")
        Map<String, org.apache.hugegraph.HugeGraphSupplier> graphCache =
                (Map<String, org.apache.hugegraph.HugeGraphSupplier>) suppliers.get(null);
        String graphName = "TEST/credit-window";
        org.apache.hugegraph.HugeGraphSupplier previous =
                graphCache.put(graphName, mock(org.apache.hugegraph.HugeGraphSupplier.class));
        GraphStoreIterator<Graphpb.Vertex> iterator;
        try {
            iterator = new GraphStoreIterator<Graphpb.Vertex>(
                    mock(ScanIterator.class), request(0)) {
                private int row;

                @Override
                public boolean hasNext() {
                    return this.row < total;
                }

                @Override
                public Graphpb.Vertex next() {
                    return Graphpb.Vertex.newBuilder().setId(Graphpb.Variant.newBuilder()
                            .setType(Graphpb.VariantType.VT_LONG).setValueInt64(this.row++)).build();
                }

                @Override
                public void close() {
                    closes.incrementAndGet();
                }
            };
        } finally {
            if (previous == null) {
                graphCache.remove(graphName);
            } else {
                graphCache.put(graphName, previous);
            }
        }
        LinkedBlockingQueue<ScanResponse> responses = new LinkedBlockingQueue<>();
        AtomicInteger terminals = new AtomicInteger();
        AtomicReference<Throwable> error = new AtomicReference<>();
        CountDownLatch finished = new CountDownLatch(1);
        StreamObserver<ScanResponse> response = new StreamObserver<ScanResponse>() {
            @Override
            public void onNext(ScanResponse value) {
                responses.add(value);
            }

            @Override
            public void onError(Throwable failure) {
                error.set(failure);
                terminals.incrementAndGet();
                finished.countDown();
            }

            @Override
            public void onCompleted() {
                terminals.incrementAndGet();
                finished.countDown();
            }
        };
        try {
            BusinessHandler handler = mock(BusinessHandler.class);
            doReturn(iterator).when(handler).scan(any());
            StreamObserver<ScanPartitionRequest> request = service.scanGraphPartition(response, handler);
            request.onNext(request(0));
            if (halfClose) {
                request.onCompleted();
            }
            long expectedId = 0;
            for (int page = 0; page < window; page++) {
                ScanResponse value = responses.poll(20, TimeUnit.SECONDS);
                assertTrue("Expected page " + page, value != null);
                assertEquals(page, value.getSeqNo());
                assertEquals(batchSize, value.getVertexCount());
                for (Graphpb.Vertex vertex : value.getVertexList()) {
                    assertEquals(expectedId++, vertex.getId().getValueInt64());
                }
            }
            assertNull("No ninth page is allowed without credit", responses.poll(100, TimeUnit.MILLISECONDS));
            if (halfClose) {
                assertTrue(finished.await(20, TimeUnit.SECONDS));
                if (extraRows == 0) {
                    assertNull(error.get());
                } else {
                    assertEquals(Status.Code.FAILED_PRECONDITION, Status.fromThrowable(error.get()).getCode());
                }
                service.awaitScanCleanup();
                request.onCompleted();
                assertEquals(1, terminals.get());
                assertEquals(1, closes.get());
                assertTrue(registry(service).isEmpty());
                return;
            }
            assertEquals(0, terminals.get());
            for (int page = 0; page < window; page++) {
                request.onNext(ScanPartitionRequest.newBuilder().setReplyRequest(
                        ScanPartitionRequest.Reply.newBuilder().setSeqNo(page)).build());
            }
            ScanResponse last = responses.poll(20, TimeUnit.SECONDS);
            assertTrue("The last partial batch must be delivered", last != null);
            assertEquals(window, last.getSeqNo());
            assertEquals(7, last.getVertexCount());
            for (Graphpb.Vertex vertex : last.getVertexList()) {
                assertEquals(expectedId++, vertex.getId().getValueInt64());
            }
            assertEquals(total, expectedId);
            assertTrue(finished.await(20, TimeUnit.SECONDS));
            assertNull(error.get());
            service.awaitScanCleanup();
            request.onCompleted();
            assertEquals(1, terminals.get());
            assertEquals(1, closes.get());
            assertTrue(responses.isEmpty());
            assertTrue(registry(service).isEmpty());
        } finally {
            service.shutdownScans();
            executor.shutdown();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }
}
