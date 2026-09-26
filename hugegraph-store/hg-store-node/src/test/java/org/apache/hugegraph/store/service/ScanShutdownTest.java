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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hugegraph.pd.common.KVPair;
import org.apache.hugegraph.rocksdb.access.RocksDBSession;
import org.apache.hugegraph.rocksdb.access.ScanIterator;
import org.apache.hugegraph.store.grpc.common.ScanMethod;
import org.apache.hugegraph.store.grpc.common.ScanOrderType;
import org.apache.hugegraph.store.grpc.stream.KvPageRes;
import org.apache.hugegraph.store.grpc.stream.KvStream;
import org.apache.hugegraph.store.grpc.stream.ScanStreamReq;
import org.apache.hugegraph.store.grpc.stream.ScanStreamBatchReq;
import org.apache.hugegraph.store.grpc.stream.ScanQueryRequest;
import org.apache.hugegraph.store.node.AppConfig;
import org.apache.hugegraph.store.node.grpc.HgStoreStreamImpl;
import org.apache.hugegraph.store.node.grpc.HgStoreWrapperEx;
import org.apache.hugegraph.store.node.grpc.ScanBatchResponse;
import org.apache.hugegraph.store.node.grpc.ParallelScanIterator;
import org.apache.hugegraph.store.node.grpc.QueryCondition;
import org.apache.hugegraph.store.node.grpc.ScanOneShotResponse;
import org.apache.hugegraph.store.node.grpc.ScanStreamResponse;
import org.junit.Test;

import io.grpc.Context;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

public class ScanShutdownTest {

    @Test
    public void testClosingPreventsLazyExecutorAndStateCreation() {
        HgStoreStreamImpl service = new HgStoreStreamImpl();
        service.stopAcceptingScans();
        assertUnavailable(service::getExecutor);
        assertUnavailable(service::getState);
        assertUnavailable(() -> service.scan(mock(StreamObserver.class)));
        service.shutdownScans();
        assertNull(service.getRealExecutor());
    }

    @Test(timeout = 5000)
    public void testQueuedStreamCancellationDrainsWithoutOpeningIterator() throws Exception {
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(1);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch occupied = new CountDownLatch(1);
        executor.execute(() -> {
            occupied.countDown();
            try {
                release.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        HgStoreWrapperEx wrapper = mock(HgStoreWrapperEx.class);
        AppConfig config = mock(AppConfig.class);
        when(config.getServerWaitTime()).thenReturn(60);
        StreamObserver<KvPageRes> output = mock(StreamObserver.class);
        ScanStreamResponse response = ScanStreamResponse.of(output, wrapper, executor, config);
        FutureTask<Void> request = new FutureTask<>(() -> {
            response.onNext(ScanStreamReq.newBuilder().setMethod(ScanMethod.ALL)
                                         .setPageSize(1).setLimit(10).build());
            return null;
        });
        Thread caller = new Thread(request, "scan-shutdown-test");
        try {
            assertTrue(occupied.await(1, TimeUnit.SECONDS));
            caller.start();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
            while (executor.getQueue().isEmpty() && System.nanoTime() < deadline) {
                Thread.yield();
            }
            assertEquals(1, executor.getQueue().size());
            response.onError(Status.CANCELLED.asRuntimeException());
            executor.shutdown();
            release.countDown();
            request.get(1, TimeUnit.SECONDS);
            assertTrue(executor.awaitTermination(1, TimeUnit.SECONDS));
            verifyNoInteractions(wrapper);
            assertEquals(2L, executor.getCompletedTaskCount());
        } finally {
            response.onCompleted();
            release.countDown();
            executor.shutdownNow();
            caller.join(1000);
        }
    }

    @Test(timeout = 5000)
    public void testOneShotObservesContextCancellationAndClosesIterator() {
        HgStoreWrapperEx wrapper = mock(HgStoreWrapperEx.class);
        ScanIterator iterator = mock(ScanIterator.class);
        StreamObserver<KvPageRes> output = mock(StreamObserver.class);
        Context.CancellableContext context = Context.current().withCancellation();
        when(wrapper.scanAll(anyString(), anyString(), any(byte[].class))).thenReturn(iterator);
        when(iterator.hasNext()).thenAnswer(invocation -> {
            context.cancel(null);
            return true;
        });
        when(iterator.next()).thenReturn(RocksDBSession.BackendColumn.of(new byte[4], new byte[0]));
        when(iterator.position()).thenReturn(new byte[4]);
        try {
            context.run(() -> ScanOneShotResponse.scanOneShot(
                    ScanStreamReq.newBuilder().setMethod(ScanMethod.ALL).setLimit(100).build(),
                    output, wrapper));
            verify(iterator).close();
            verifyNoInteractions(output);
        } finally {
            context.cancel(null);
        }
    }

    @Test
    public void testPausedBatchClosesIteratorAndRejectsLaterQuery() throws Exception {
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(1);
        HgStoreWrapperEx wrapper = mock(HgStoreWrapperEx.class);
        ScanIterator iterator = mock(ScanIterator.class);
        StreamObserver<KvStream> output = mock(StreamObserver.class);
        ScanBatchResponse response = new ScanBatchResponse(output, wrapper, executor);
        Field field = ScanBatchResponse.class.getDeclaredField("iterator");
        field.setAccessible(true);
        field.set(response, iterator);
        try {
            response.onCompleted();
            response.onNext(ScanStreamBatchReq.newBuilder()
                                            .setQueryRequest(ScanQueryRequest.getDefaultInstance())
                                            .build());
            response.onCompleted();
            verify(iterator).close();
            verifyNoInteractions(wrapper);
            assertNull(field.get(response));
            assertFalse(executor.isShutdown());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testRejectedBatchReportsFailureWithoutSuccessfulCompletion() {
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(1);
        executor.shutdown();
        StreamObserver<KvStream> output = mock(StreamObserver.class);
        ScanBatchResponse response = new ScanBatchResponse(output, mock(HgStoreWrapperEx.class), executor);
        response.onNext(ScanStreamBatchReq.newBuilder()
                                        .setQueryRequest(ScanQueryRequest.getDefaultInstance()).build());
        verify(output).onError(any(Throwable.class));
        verify(output, never()).onCompleted();
    }

    @Test
    public void testBatchIteratorFailureClosesResourcesWithoutSuccessfulCompletion() throws Exception {
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(1);
        StreamObserver<KvStream> output = mock(StreamObserver.class);
        ScanIterator iterator = mock(ScanIterator.class);
        IllegalStateException failure = new IllegalStateException("iterator failed");
        when(iterator.hasNext()).thenThrow(failure);
        ScanBatchResponse response = new ScanBatchResponse(output, mock(HgStoreWrapperEx.class), executor);
        Field field = ScanBatchResponse.class.getDeclaredField("iterator");
        field.setAccessible(true);
        field.set(response, iterator);
        Method send = ScanBatchResponse.class.getDeclaredMethod("sendEntries");
        send.setAccessible(true);
        try {
            send.invoke(response);
            verify(iterator).close();
            verify(output).onError(failure);
            verify(output, never()).onCompleted();
        } finally {
            response.onCompleted();
            executor.shutdownNow();
        }
    }

    @Test(timeout = 5000)
    public void testActiveStreamCancellationReleasesBlockedProducer() throws Exception {
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(1);
        HgStoreWrapperEx wrapper = mock(HgStoreWrapperEx.class);
        ScanIterator iterator = mock(ScanIterator.class);
        when(wrapper.scanAll(anyString(), anyString(), any(byte[].class))).thenReturn(iterator);
        when(iterator.hasNext()).thenReturn(true);
        when(iterator.next()).thenReturn(RocksDBSession.BackendColumn.of(new byte[4], new byte[0]));
        when(iterator.position()).thenReturn(new byte[4]);
        AppConfig config = mock(AppConfig.class);
        when(config.getServerWaitTime()).thenReturn(60);
        StreamObserver<KvPageRes> output = mock(StreamObserver.class);
        ScanStreamResponse response = ScanStreamResponse.of(output, wrapper, executor, config);
        try {
            // Consume just one page, then leave the producer waiting for its consumer.
            response.onNext(ScanStreamReq.newBuilder().setMethod(ScanMethod.ALL)
                                         .setPageSize(1).setLimit(1000).build());
            response.onError(Status.CANCELLED.asRuntimeException());
            executor.shutdown();
            assertTrue("cancel must release a producer without its 60-second timeout",
                       executor.awaitTermination(1, TimeUnit.SECONDS));
            verify(iterator).close();
        } finally {
            response.onCompleted();
            executor.shutdownNow();
        }
    }

    @Test(timeout = 5000)
    public void testParallelPausedScannerReleasesIterator() throws Exception {
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(1);
        ScanIterator source = mock(ScanIterator.class);
        when(source.hasNext()).thenReturn(true);
        when(source.next()).thenReturn(RocksDBSession.BackendColumn.of(new byte[4], new byte[0]));
        ParallelScanIterator scan = ParallelScanIterator.of(
                () -> new KVPair<>(mock(QueryCondition.class), source), () -> Long.MAX_VALUE,
                ScanQueryRequest.getDefaultInstance(), executor);
        try {
            // One worker fills its output allowance and pauses while retaining its iterator.
            executor.submit(() -> { }).get(2, TimeUnit.SECONDS);
            verify(source, atLeastOnce()).next();
            verify(source, never()).close();
            scan.close();
            scan.close();
            verify(source).close();
            executor.shutdown();
            assertTrue(executor.awaitTermination(1, TimeUnit.SECONDS));
        } finally {
            scan.close();
            executor.shutdownNow();
        }
    }

    @Test(timeout = 5000)
    public void testParallelOrderedScannerReleasesQueueLockOnEmptyIterator() throws Exception {
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(1);
        ScanIterator first = mock(ScanIterator.class);
        ScanIterator empty = mock(ScanIterator.class);
        when(first.hasNext()).thenReturn(true);
        when(first.next()).thenReturn(RocksDBSession.BackendColumn.of(new byte[4], new byte[0]));
        when(empty.hasNext()).thenReturn(false);

        AtomicInteger supplies = new AtomicInteger();
        Field bodySize = ParallelScanIterator.class.getDeclaredField("maxBodySize");
        bodySize.setAccessible(true);
        int originalBodySize = bodySize.getInt(null);
        bodySize.setInt(null, 2);
        ParallelScanIterator scan = null;
        try {
            scan = ParallelScanIterator.of(
                    () -> {
                        int supply = supplies.incrementAndGet();
                        if (supply == 1) {
                            return new KVPair<>(mock(QueryCondition.class), first);
                        }
                        if (supply == 2) {
                            return new KVPair<>(mock(QueryCondition.class), empty);
                        }
                        return new KVPair<>(mock(QueryCondition.class), null);
                    },
                    () -> 1L,
                    ScanQueryRequest.newBuilder().setOrderType(ScanOrderType.ORDER_WITHIN_VERTEX).build(),
                    executor);

            // Wait for the scanner task to finish before checking the lock owner.
            executor.submit(() -> { }).get(2, TimeUnit.SECONDS);
            Field queueLock = ParallelScanIterator.class.getDeclaredField("queueLock");
            queueLock.setAccessible(true);
            assertFalse(((ReentrantLock) queueLock.get(scan)).isLocked());
        } finally {
            if (scan != null) {
                scan.close();
            }
            bodySize.setInt(null, originalBodySize);
            executor.shutdownNow();
        }
    }

    @Test(timeout = 5000)
    public void testParallelOrderedProducerCancelsWithFullOutputQueue() throws Exception {
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(1);
        ScanIterator source = mock(ScanIterator.class);
        Field bodySize = ParallelScanIterator.class.getDeclaredField("maxBodySize");
        bodySize.setAccessible(true);
        byte[] value = new byte[bodySize.getInt(null)];
        CountDownLatch full = new CountDownLatch(1);
        AtomicInteger rows = new AtomicInteger();
        when(source.hasNext()).thenReturn(true);
        when(source.next()).thenAnswer(invocation -> {
            if (rows.incrementAndGet() == 5) {
                full.countDown();
            }
            return RocksDBSession.BackendColumn.of(new byte[4], value);
        });
        ParallelScanIterator scan = ParallelScanIterator.of(
                () -> new KVPair<>(mock(QueryCondition.class), source), () -> Long.MAX_VALUE,
                ScanQueryRequest.newBuilder().setOrderType(ScanOrderType.ORDER_WITHIN_VERTEX).build(),
                executor);
        try {
            assertTrue("four queued batches must fill the single-scanner queue",
                       full.await(2, TimeUnit.SECONDS));
            scan.close();
            executor.shutdown();
            assertTrue("cancellation must release the blocked ordered producer",
                       executor.awaitTermination(1, TimeUnit.SECONDS));
            verify(source).close();
        } finally {
            scan.close();
            executor.shutdownNow();
        }
    }

    private static void assertUnavailable(Runnable action) {
        try {
            action.run();
            fail("Scan admission must be closed");
        } catch (StatusRuntimeException e) {
            assertEquals(Status.Code.UNAVAILABLE, e.getStatus().getCode());
        }
    }
}
