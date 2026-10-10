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
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hugegraph.pd.common.KVPair;
import org.apache.hugegraph.rocksdb.access.RocksDBSession;
import org.apache.hugegraph.rocksdb.access.ScanIterator;
import org.apache.hugegraph.store.grpc.common.Header;
import org.apache.hugegraph.store.grpc.common.ScanMethod;
import org.apache.hugegraph.store.grpc.common.ScanOrderType;
import org.apache.hugegraph.store.grpc.stream.HgStoreStreamGrpc;
import org.apache.hugegraph.store.grpc.stream.KvPageRes;
import org.apache.hugegraph.store.grpc.stream.KvStream;
import org.apache.hugegraph.store.grpc.stream.ScanQueryRequest;
import org.apache.hugegraph.store.grpc.stream.ScanStreamBatchReq;
import org.apache.hugegraph.store.grpc.stream.ScanStreamReq;
import org.apache.hugegraph.store.node.AppConfig;
import org.apache.hugegraph.store.node.grpc.HgStoreStreamImpl;
import org.apache.hugegraph.store.node.grpc.HgStoreWrapperEx;
import org.apache.hugegraph.store.node.grpc.ParallelScanIterator;
import org.apache.hugegraph.store.node.grpc.QueryCondition;
import org.apache.hugegraph.store.node.grpc.ScanBatchOneShotResponse;
import org.apache.hugegraph.store.node.grpc.ScanBatchResponse;
import org.apache.hugegraph.store.node.grpc.ScanBatchResponseFactory;
import org.apache.hugegraph.store.node.grpc.ScanBatchResponse3;
import org.apache.hugegraph.store.node.grpc.ScanOneShotResponse;
import org.apache.hugegraph.store.node.grpc.ScanStreamResponse;
import org.apache.hugegraph.store.node.util.HgChannel;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import io.grpc.Context;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;

public class ScanLifecycleTest extends ScanTestSupport {

    @Test
    public void testFailedSelectionCleanupBlocksLifecycleCompletion() throws Exception {
        ScanIterator iterator = mock(ScanIterator.class);
        IllegalStateException closing = new IllegalStateException("native release failed");
        doThrow(closing).when(iterator).close();
        HgStoreWrapperEx wrapper = mock(HgStoreWrapperEx.class);
        when(wrapper.scanAll(anyString(), anyString(), any(byte[].class))).thenReturn(iterator);
        Class<?> lifecycleType = Class.forName("org.apache.hugegraph.store.node.grpc.ScanLifecycle");
        java.lang.reflect.Constructor<?> constructor = lifecycleType.getDeclaredConstructor();
        constructor.setAccessible(true);
        Object lifecycle = constructor.newInstance();
        Method failedCleanup = lifecycleType.getDeclaredMethod("failedCleanup", Throwable.class);
        failedCleanup.setAccessible(true);
        AtomicBoolean finished = new AtomicBoolean();
        Method onFinished = lifecycleType.getDeclaredMethod("onFinished", Runnable.class);
        onFinished.setAccessible(true);
        onFinished.invoke(lifecycle, (Runnable) () -> finished.set(true));
        java.util.function.Consumer<Throwable> cleanup = failure -> {
            try {
                failedCleanup.invoke(lifecycle, failure);
            } catch (ReflectiveOperationException error) {
                throw new AssertionError(error);
            }
        };
        Class<?> selector = Class.forName("org.apache.hugegraph.store.node.grpc.ScanUtil");
        Method getIterator = selector.getDeclaredMethod("getIterator", ScanStreamReq.class,
                                                       HgStoreWrapperEx.class,
                                                       java.util.function.Consumer.class);
        getIterator.setAccessible(true);
        ScanStreamReq request = ScanStreamReq.newBuilder().setMethod(ScanMethod.ALL)
                .setHeader(Header.newBuilder().setGraph("selection-cleanup"))
                .setTable("unsupported-property-table")
                .setSelects(org.apache.hugegraph.store.grpc.stream.SelectParam.newBuilder().addProperties(1)).build();
        try {
            getIterator.invoke(null, request, wrapper, cleanup);
            fail("Invalid property selection must fail");
        } catch (java.lang.reflect.InvocationTargetException error) {
            assertTrue(error.getCause() instanceof IllegalArgumentException);
            assertEquals(Collections.singletonList(closing), Arrays.asList(error.getCause().getSuppressed()));
        }
        Method finish = lifecycleType.getDeclaredMethod("finishWithoutResponse");
        finish.setAccessible(true);
        finish.invoke(lifecycle);
        assertFalse("Failed native release must remain a shutdown blocker", finished.get());
        verify(iterator).close();
    }

    @Test(timeout = 5000)
    public void testInFlightReceiptDoesNotPublishOverAfterErrorCancellation() throws Exception {
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(1);
        StreamObserver<KvStream> output = mock(StreamObserver.class);
        ScanBatchResponse response = new ScanBatchResponse(output, mock(HgStoreWrapperEx.class), executor);
        Field lockField = ScanBatchResponse.class.getDeclaredField("stateLock");
        lockField.setAccessible(true);
        Field state = ScanBatchResponse.class.getDeclaredField("state");
        state.setAccessible(true);
        Field cancelled = ScanBatchResponse.class.getDeclaredField("cancelled");
        cancelled.setAccessible(true);
        AtomicBoolean cancelling = (AtomicBoolean) cancelled.get(response);
        Object done = Arrays.stream(state.getType().getEnumConstants())
                            .filter(value -> value.toString().equals("DONE")).findFirst().get();
        FutureTask<Void> receipt = new FutureTask<>(() -> {
            response.onNext(ScanStreamBatchReq.newBuilder().setReceiptRequest(
                    org.apache.hugegraph.store.grpc.stream.ScanReceiptRequest.newBuilder().setTimes(1)).build());
            return null;
        });
        FutureTask<Void> failing = new FutureTask<>(() -> {
            response.onError(Status.INTERNAL.asRuntimeException());
            return null;
        });
        Thread reader = new Thread(receipt, "receipt-at-terminal-transition");
        Thread closer = new Thread(failing, "error-at-terminal-transition");
        try {
            synchronized (lockField.get(response)) {
                reader.start();
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
                while (reader.getState() != Thread.State.BLOCKED && System.nanoTime() < deadline) {
                    Thread.yield();
                }
                assertEquals(Thread.State.BLOCKED, reader.getState());
                closer.start();
                deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
                while (!cancelling.get() && System.nanoTime() < deadline) {
                    Thread.yield();
                }
                assertTrue(cancelling.get());
                state.set(response, done);
            }
            receipt.get(2, TimeUnit.SECONDS);
            failing.get(2, TimeUnit.SECONDS);
            verify(output, never()).onNext(any(KvStream.class));
            verify(output).onError(any(Throwable.class));
        } finally {
            executor.shutdownNow();
            reader.join(1000);
            closer.join(1000);
        }
    }

    @Test(timeout = 10000)
    public void testOrderedVertexRemainsConsecutiveAcrossBackpressureWithTwoScanners() throws Exception {
        AtomicBoolean defer = new AtomicBoolean(true);
        List<Runnable> initial = new ArrayList<>();
        ThreadPoolExecutor executor = new ThreadPoolExecutor(2, 2, 0, TimeUnit.SECONDS,
                new java.util.concurrent.LinkedBlockingQueue<>()) {
            @Override
            public void execute(Runnable command) {
                if (defer.get()) {
                    initial.add(command);
                } else {
                    super.execute(command);
                }
            }
        };
        ScanIterator first = mock(ScanIterator.class);
        ScanIterator second = mock(ScanIterator.class);
        AtomicInteger firstRows = new AtomicInteger();
        AtomicInteger secondRows = new AtomicInteger();
        CountDownLatch firstBackpressure = new CountDownLatch(1);
        CountDownLatch secondReading = new CountDownLatch(1);
        when(first.hasNext()).thenAnswer(ignored -> firstRows.get() < 8);
        when(first.next()).thenAnswer(ignored -> {
            if (firstRows.incrementAndGet() == 5) {
                firstBackpressure.countDown();
            }
            return RocksDBSession.BackendColumn.of(new byte[]{1, 0, 0, 0}, new byte[16]);
        });
        when(second.hasNext()).thenAnswer(ignored -> secondRows.get() < 2);
        when(second.next()).thenAnswer(ignored -> {
            secondRows.incrementAndGet();
            secondReading.countDown();
            return RocksDBSession.BackendColumn.of(new byte[]{2, 0, 0, 0}, new byte[16]);
        });
        AtomicInteger supplies = new AtomicInteger();
        Field bodySize = ParallelScanIterator.class.getDeclaredField("maxBodySize");
        bodySize.setAccessible(true);
        int previous = bodySize.getInt(null);
        bodySize.setInt(null, 16);
        ParallelScanIterator scan = null;
        try {
            scan = ParallelScanIterator.of(() -> {
                int index = supplies.incrementAndGet();
                return new KVPair<>(mock(QueryCondition.class), index == 1 ? first : index == 2 ? second : null);
            }, () -> Long.MAX_VALUE,
                    ScanQueryRequest.newBuilder().setOrderType(ScanOrderType.ORDER_WITHIN_VERTEX).build(), executor);
            // Force two owned scanners independently of the container's CPU-derived default.
            Field registry = ParallelScanIterator.class.getDeclaredField("scanners");
            registry.setAccessible(true);
            java.util.Queue<Object> scanners = (java.util.Queue<Object>) registry.get(scan);
            Class<?> scannerType = scanners.peek().getClass();
            java.lang.reflect.Constructor<?> constructor =
                    scannerType.getDeclaredConstructor(ParallelScanIterator.class);
            constructor.setAccessible(true);
            Object other = constructor.newInstance(scan);
            scanners.add(other);
            Method run = scannerType.getDeclaredMethod("scanKV");
            run.setAccessible(true);
            defer.set(false);
            executor.execute(initial.get(0));
            assertTrue(firstBackpressure.await(2, TimeUnit.SECONDS));
            executor.execute(() -> {
                try {
                    run.invoke(other);
                } catch (ReflectiveOperationException e) {
                    throw new AssertionError(e);
                }
            });
            assertTrue(secondReading.await(2, TimeUnit.SECONDS));
            List<Integer> vertices = new ArrayList<>();
            while (scan.hasNext()) {
                for (Object row : scan.next()) {
                    Field key = row.getClass().getField("key");
                    key.setAccessible(true);
                    vertices.add((int) ((byte[]) key.get(row))[0]);
                }
            }
            assertEquals(Arrays.asList(1, 1, 1, 1, 1, 1, 1, 1, 2, 2), vertices);
            verify(first).close();
            verify(second).close();
        } finally {
            if (scan != null) {
                scan.close();
            }
            executor.shutdownNow();
            bodySize.setInt(null, previous);
        }
    }

    @Test(timeout = 5000)
    public void testUnsupportedPausedBatchClosesOwnedIteratorAndRetainsFailure() throws Exception {
        for (boolean fails : new boolean[]{false, true}) {
            ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(1);
            HgStoreStreamImpl service = scanService(executor, mock(HgStoreWrapperEx.class));
            StreamObserver<KvStream> output = mock(StreamObserver.class);
            doThrow(new AssertionError("injected response callback failure"))
                    .when(output).onError(any(Throwable.class));
            StreamObserver<ScanStreamBatchReq> input = service.scanBatch2(output);
            ScanBatchResponse response = null;
            for (Field field : input.getClass().getDeclaredFields()) {
                field.setAccessible(true);
                Object captured = field.get(input);
                if (captured instanceof ScanBatchResponse) {
                    response = (ScanBatchResponse) captured;
                }
            }
            org.junit.Assert.assertNotNull(response);
            ScanIterator iterator = mock(ScanIterator.class);
            if (fails) {
                doThrow(new IllegalStateException("paused iterator close failed")).when(iterator).close();
            }
            Field resource = ScanBatchResponse.class.getDeclaredField("iterator");
            resource.setAccessible(true);
            resource.set(response, iterator);
            try {
                // An idle response can retain a native iterator while waiting for receipts.
                input.onNext(ScanStreamBatchReq.getDefaultInstance());
                verify(iterator).close();
                verify(output).onError(any(Throwable.class));
                verify(output, never()).onCompleted();
                assertEquals(fails ? 1 : 0, scanRegistry(service).size());
                service.shutdownScans();
                input.onCompleted();
                verify(iterator).close();
            } finally {
                scanRegistry(service).clear();
                executor.shutdownNow();
            }
        }
    }

    @Test(timeout = 5000)
    public void testFailedCleanupAndResponseCallbackStillClearWorkerOwnership() throws Exception {
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(1, task -> {
            Thread thread = new Thread(task, "scan-close-callback-failure");
            thread.setUncaughtExceptionHandler((ignored, failure) -> { });
            return thread;
        });
        HgStoreWrapperEx wrapper = mock(HgStoreWrapperEx.class);
        ScanIterator iterator = mock(ScanIterator.class);
        when(wrapper.scanAll(anyString(), anyString(), any(byte[].class))).thenReturn(iterator);
        StreamObserver<KvPageRes> output = mock(StreamObserver.class);
        CountDownLatch sending = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        doAnswer(invocation -> {
            // Exercise cleanup failure while the response callback is already in flight.
            assertTrue(sending.await(2, TimeUnit.SECONDS));
            throw new IllegalStateException("native close failed");
        }).when(iterator).close();
        doAnswer(invocation -> {
            sending.countDown();
            assertTrue(release.await(2, TimeUnit.SECONDS));
            return null;
        }).when(output).onNext(any(KvPageRes.class));
        doThrow(new AssertionError("error callback failed")).when(output).onError(any(Throwable.class));
        AppConfig config = mock(AppConfig.class);
        when(config.getServerWaitTime()).thenReturn(5);
        ScanStreamResponse response = ScanStreamResponse.of(output, wrapper, executor, config);
        FutureTask<Void> request = new FutureTask<>(() -> {
            response.onNext(ScanStreamReq.newBuilder().setMethod(ScanMethod.ALL)
                                         .setPageSize(1).setLimit(10).build());
            return null;
        });
        Thread caller = new Thread(request, "scan-close-callback-request");
        try {
            caller.start();
            assertTrue(sending.await(1, TimeUnit.SECONDS));
            Field finished = ScanStreamResponse.class.getDeclaredField("finishFlag");
            finished.setAccessible(true);
            AtomicBoolean terminal = (AtomicBoolean) finished.get(response);
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
            while (!terminal.get() && System.nanoTime() < deadline) {
                Thread.yield();
            }
            assertTrue("cleanup failure must claim error before normal completion", terminal.get());
            release.countDown();
            request.get(2, TimeUnit.SECONDS);
            executor.shutdown();
            assertTrue(executor.awaitTermination(2, TimeUnit.SECONDS));
            Field worker = ScanStreamResponse.class.getDeclaredField("worker");
            worker.setAccessible(true);
            assertNull(worker.get(response));
            verify(iterator).close();
            verify(output).onError(any(Throwable.class));
            verify(output, never()).onCompleted();
        } finally {
            release.countDown();
            executor.shutdownNow();
            caller.join(1000);
        }
    }

    @Test(timeout = 5000)
    public void testTerminalAdmissionRejectsRequestWaitingForLifecycleLock() throws Exception {
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(1);
        HgStoreWrapperEx wrapper = mock(HgStoreWrapperEx.class);
        HgStoreStreamImpl service = scanService(executor, wrapper);
        StreamObserver<ScanStreamBatchReq> input = service.scanBatch(mock(StreamObserver.class));
        Object lifecycle = scanRegistry(service).keySet().iterator().next();
        Method finished = lifecycle.getClass().getDeclaredMethod("finishWithoutResponse");
        finished.setAccessible(true);
        FutureTask<Void> request = new FutureTask<>(() -> {
            input.onNext(batchRequest());
            return null;
        });
        Thread late = new Thread(request, "late-scan-admission");
        try {
            synchronized (lifecycle) {
                late.start();
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
                while (late.getState() != Thread.State.BLOCKED && System.nanoTime() < deadline) {
                    Thread.yield();
                }
                assertEquals(Thread.State.BLOCKED, late.getState());
                // Exercise the atomic admission invariant directly, without claiming a native fault.
                finished.invoke(lifecycle);
                assertTrue(scanRegistry(service).isEmpty());
            }
            request.get(1, TimeUnit.SECONDS);
            service.shutdownScans();
            assertTrue(executor.awaitTermination(1, TimeUnit.SECONDS));
            verifyNoInteractions(wrapper);
            assertEquals(0, executor.getTaskCount());
        } finally {
            executor.shutdownNow();
            late.join(1000);
        }
    }

    @Test(timeout = 10000)
    public void testInProcessNormalCompletionDoesNotCancelCompletedScan() throws Exception {
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(2);
        HgStoreWrapperEx wrapper = mock(HgStoreWrapperEx.class);
        ScanIterator iterator = mock(ScanIterator.class);
        when(wrapper.scanAll(anyString(), anyString(), any(byte[].class))).thenReturn(iterator);
        HgStoreStreamImpl service = scanService(executor, wrapper);
        String name = InProcessServerBuilder.generateName();
        Server server = InProcessServerBuilder.forName(name)
                .directExecutor().addService(service).build().start();
        ManagedChannel channel = InProcessChannelBuilder.forName(name)
                .directExecutor().build();
        org.apache.logging.log4j.core.Logger logger = (org.apache.logging.log4j.core.Logger)
                org.apache.logging.log4j.LogManager.getLogger(ScanStreamResponse.class);
        List<String> messages = new java.util.concurrent.CopyOnWriteArrayList<>();
        org.apache.logging.log4j.core.appender.AbstractAppender logs =
                new org.apache.logging.log4j.core.appender.AbstractAppender(
                        "scan-normal-completion", null, null, false,
                        org.apache.logging.log4j.core.config.Property.EMPTY_ARRAY) {
                    @Override
                    public void append(org.apache.logging.log4j.core.LogEvent event) {
                        messages.add(event.getMessage().getFormattedMessage());
                    }
                };
        logs.start();
        logger.addAppender(logs);
        CountDownLatch completed = new CountDownLatch(1);
        AtomicInteger errors = new AtomicInteger();
        try {
            StreamObserver<ScanStreamReq> request =
                    HgStoreStreamGrpc.newStub(channel)
                    .scan(new StreamObserver<KvPageRes>() {
                        @Override
                        public void onNext(KvPageRes value) { }

                        @Override
                        public void onError(Throwable failure) {
                            errors.incrementAndGet();
                            completed.countDown();
                        }

                        @Override
                        public void onCompleted() {
                            completed.countDown();
                        }
                    });
            request.onNext(ScanStreamReq.newBuilder().setMethod(ScanMethod.ALL)
                                       .setPageSize(1).setLimit(10).build());
            assertTrue(completed.await(2, TimeUnit.SECONDS));
            service.shutdownScans();
            assertTrue(executor.awaitTermination(2, TimeUnit.SECONDS));
            service.awaitScanCleanup();
            assertEquals(0, errors.get());
            assertTrue(messages.stream().noneMatch(message -> message.contains("onError from client")));
            verify(iterator).close();
            assertTrue(scanRegistry(service).isEmpty());
        } finally {
            logger.removeAppender(logs);
            logs.stop();
            channel.shutdownNow();
            server.shutdownNow();
            executor.shutdownNow();
        }
    }

    @Test(timeout = 10000)
    public void testInProcessClientCancellationDrainsIterator() throws Exception {
        assertInProcessCancellation(false);
        assertInProcessCancellation(true);
    }

    private static void assertInProcessCancellation(boolean completionRace) throws Exception {
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(2);
        HgStoreWrapperEx wrapper = mock(HgStoreWrapperEx.class);
        ScanIterator iterator = mock(ScanIterator.class);
        CountDownLatch reading = new CountDownLatch(1);
        CountDownLatch releaseRead = new CountDownLatch(1);
        when(wrapper.scanAll(anyString(), anyString(), any(byte[].class))).thenReturn(iterator);
        when(iterator.hasNext()).thenAnswer(invocation -> {
            reading.countDown();
            try {
                releaseRead.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return false;
        });
        HgStoreStreamImpl service = scanService(executor, wrapper);
        // gRPC 1.39 dispatches context cancellation through the application executor too.
        // Keep a slot for cancellation while scan.onNext waits for its worker's first page.
        ExecutorService callbacks = Executors.newFixedThreadPool(2);
        String name = InProcessServerBuilder.generateName();
        Server server = InProcessServerBuilder.forName(name)
                .executor(callbacks).addService(service).build().start();
        ManagedChannel channel = InProcessChannelBuilder.forName(name)
                .directExecutor().build();
        Context.CancellableContext context = Context.current().withCancellation();
        CountDownLatch ended = new CountDownLatch(1);
        AtomicInteger terminals = new AtomicInteger();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        try {
            context.run(() -> {
                StreamObserver<ScanStreamReq> request =
                        HgStoreStreamGrpc.newStub(channel)
                        .scan(new StreamObserver<KvPageRes>() {
                            @Override
                            public void onNext(KvPageRes value) { }

                            @Override
                            public void onError(Throwable error) {
                                failure.set(error);
                                terminals.incrementAndGet();
                                ended.countDown();
                            }

                            @Override
                            public void onCompleted() {
                                terminals.incrementAndGet();
                                ended.countDown();
                            }
                        });
                request.onNext(ScanStreamReq.newBuilder().setMethod(ScanMethod.ALL)
                                           .setPageSize(1).setLimit(10).build());
            });
            assertTrue(reading.await(2, TimeUnit.SECONDS));
            if (completionRace) {
                CountDownLatch start = new CountDownLatch(1);
                Thread cancel = new Thread(() -> {
                    try {
                        start.await();
                    } catch (InterruptedException e) {
                        throw new AssertionError(e);
                    }
                    context.cancel(null);
                }, "scan-completion-cancel-race");
                cancel.start();
                start.countDown();
                releaseRead.countDown();
                cancel.join(2000);
                assertFalse(cancel.isAlive());
            } else {
                context.cancel(null);
            }
            assertTrue(ended.await(2, TimeUnit.SECONDS));
            verify(iterator, timeout(2000)).close();
            service.shutdownScans();
            assertTrue(executor.awaitTermination(2, TimeUnit.SECONDS));
            service.awaitScanCleanup();
            assertEquals(1, terminals.get());
            if (!completionRace || failure.get() != null) {
                assertEquals(Status.Code.CANCELLED, Status.fromThrowable(failure.get()).getCode());
            }
            verify(iterator).close();
            assertTrue(scanRegistry(service).isEmpty());
        } finally {
            releaseRead.countDown();
            context.cancel(null);
            channel.shutdownNow();
            server.shutdownNow();
            executor.shutdownNow();
            callbacks.shutdownNow();
        }
    }

    @Test(timeout = 5000)
    public void testBatchRejectionReportsStatusAndClosesUnstartedIterator() throws Exception {
        for (boolean stopped : new boolean[]{false, true}) {
            ThreadPoolExecutor executor = new ThreadPoolExecutor(1, 1, 1, TimeUnit.SECONDS,
                    new SynchronousQueue<>());
            CountDownLatch release = new CountDownLatch(1);
            if (stopped) {
                executor.shutdown();
            } else {
                executor.execute(() -> {
                    try {
                        release.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
            }
            HgStoreWrapperEx wrapper = mock(HgStoreWrapperEx.class);
            HgStoreStreamImpl service = scanService(executor, wrapper);
            StreamObserver<KvPageRes> output = mock(StreamObserver.class);
            try {
                service.scanBatch(output).onNext(batchRequest());
                ArgumentCaptor<Throwable> error = ArgumentCaptor.forClass(Throwable.class);
                verify(output).onError(error.capture());
                assertEquals(stopped ? Status.Code.UNAVAILABLE : Status.Code.RESOURCE_EXHAUSTED,
                             Status.fromThrowable(error.getValue()).getCode());
                verify(output, never()).onCompleted();
                assertTrue(scanRegistry(service).isEmpty());
                verifyNoInteractions(wrapper);
            } finally {
                release.countDown();
                executor.shutdownNow();
            }
        }
    }

    @Test(timeout = 5000)
    public void testBatchTimeoutCleanupDoesNotHoldAdmissionMonitor() throws Exception {
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(1);
        HgStoreStreamImpl service = scanService(executor, mock(HgStoreWrapperEx.class));
        ScanBatchResponse expired = mock(ScanBatchResponse.class);
        CountDownLatch checking = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        ScanBatchResponseFactory factory = ScanBatchResponseFactory.getInstance();
        doAnswer(invocation -> {
            assertFalse(Thread.holdsLock(service));
            checking.countDown();
            assertTrue(release.await(2, TimeUnit.SECONDS));
            return null;
        }).when(expired).checkActiveTimeout();
        FutureTask<StreamObserver<ScanStreamBatchReq>> opening = new FutureTask<>(() ->
                service.scanBatch2(mock(StreamObserver.class)));
        Thread caller = new Thread(opening, "test-batch-timeout-cleanup");
        try {
            factory.addStreamObserver(expired);
            caller.start();
            assertTrue(checking.await(1, TimeUnit.SECONDS));
            FutureTask<Void> stopping = new FutureTask<>(() -> {
                service.stopAcceptingScans();
                return null;
            });
            Thread stopper = new Thread(stopping, "test-batch-stop-admission");
            stopper.start();
            stopping.get(1, TimeUnit.SECONDS);
            assertUnavailable(() -> service.scan(mock(StreamObserver.class)));
            assertFalse(opening.isDone());
            release.countDown();
            opening.get(1, TimeUnit.SECONDS).onError(Status.CANCELLED.asRuntimeException());
            assertTrue(scanRegistry(service).isEmpty());
        } finally {
            release.countDown();
            factory.removeStreamObserver(expired);
            caller.join(2000);
            service.shutdownScans();
            executor.shutdownNow();
        }
    }

    @Test(timeout = 10000)
    public void testOneShotCancellationInterruptsCallerAndClosesIterator() throws Exception {
        for (boolean batch : new boolean[]{false, true}) {
            for (boolean transport : new boolean[]{false, true}) {
                ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(1);
                HgStoreWrapperEx wrapper = mock(HgStoreWrapperEx.class);
                ScanIterator iterator = mock(ScanIterator.class);
                StreamObserver<KvPageRes> output = mock(StreamObserver.class);
                when(wrapper.scanAll(anyString(), anyString(), any(byte[].class))).thenReturn(iterator);
                CountDownLatch reading = new CountDownLatch(1);
                CountDownLatch interrupted = new CountDownLatch(1);
                CountDownLatch release = new CountDownLatch(1);
                when(iterator.hasNext()).thenAnswer(invocation -> {
                    reading.countDown();
                    try {
                        assertTrue(release.await(5, TimeUnit.SECONDS));
                    } catch (InterruptedException expected) {
                        interrupted.countDown();
                        Thread.currentThread().interrupt();
                    }
                    return false;
                });
                HgStoreStreamImpl service = scanService(executor, wrapper);
                Context.CancellableContext context = Context.current().withCancellation();
                FutureTask<Void> scanning = new FutureTask<>(() -> {
                    context.run(() -> {
                        if (batch) {
                            service.scanBatchOneShot(batchRequest(), output);
                        } else {
                            service.scanOneShot(ScanStreamReq.newBuilder().setMethod(ScanMethod.ALL)
                                                           .setLimit(10).build(), output);
                        }
                    });
                    assertFalse("a cancelled call must not interrupt the next call",
                                Thread.currentThread().isInterrupted());
                    doReturn(false).when(iterator).hasNext();
                    StreamObserver<KvPageRes> nextOutput = mock(StreamObserver.class);
                    HgStoreStreamImpl nextService = scanService(executor, wrapper);
                    if (batch) {
                        nextService.scanBatchOneShot(batchRequest(), nextOutput);
                    } else {
                        nextService.scanOneShot(ScanStreamReq.newBuilder().setMethod(ScanMethod.ALL)
                                                           .setLimit(10).build(), nextOutput);
                    }
                    verify(nextOutput).onNext(any(KvPageRes.class));
                    verify(nextOutput).onCompleted();
                    verify(nextOutput, never()).onError(any(Throwable.class));
                    return null;
                });
                Thread caller = new Thread(scanning, "test-one-shot-blocked-read");
                try {
                    caller.start();
                    assertTrue(reading.await(1, TimeUnit.SECONDS));
                    if (transport) {
                        context.cancel(new IllegalStateException("client canceled"));
                    } else {
                        service.shutdownScans();
                    }
                    assertTrue(interrupted.await(1, TimeUnit.SECONDS));
                    scanning.get(1, TimeUnit.SECONDS);
                    assertCancelled(output);
                    verify(output, never()).onNext(any(KvPageRes.class));
                    verify(output, never()).onCompleted();
                    verify(iterator, times(2)).close();
                    assertTrue(scanRegistry(service).isEmpty());
                } finally {
                    release.countDown();
                    context.cancel(null);
                    caller.join(2000);
                    service.shutdownScans();
                    executor.shutdownNow();
                    assertFalse(caller.isAlive());
                }
            }
        }
    }

    private static void awaitCloseRelease(CountDownLatch release) {
        boolean interrupted = false;
        try {
            for (;;) {
                try {
                    assertTrue(release.await(5, TimeUnit.SECONDS));
                    return;
                } catch (InterruptedException ignored) {
                    interrupted = true;
                }
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Test(timeout = 10000)
    public void testInProcessOneShotCancellationDuringCloseReportsCancelled() throws Exception {
        for (boolean batch : new boolean[]{false, true}) {
            ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(1);
            HgStoreWrapperEx wrapper = mock(HgStoreWrapperEx.class);
            ScanIterator iterator = mock(ScanIterator.class);
            when(wrapper.scanAll(anyString(), anyString(), any(byte[].class))).thenReturn(iterator);
            when(iterator.hasNext()).thenReturn(false);
            CountDownLatch closing = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            doAnswer(invocation -> {
                closing.countDown();
                awaitCloseRelease(release);
                return null;
            }).when(iterator).close();
            HgStoreStreamImpl service = scanService(executor, wrapper);
            String name = InProcessServerBuilder.generateName();
            Server server = InProcessServerBuilder.forName(name).directExecutor()
                    .addService(service).build().start();
            ManagedChannel channel = InProcessChannelBuilder.forName(name).directExecutor().build();
            FutureTask<Status.Code> call = new FutureTask<>(() -> {
                try {
                    HgStoreStreamGrpc.HgStoreStreamBlockingStub stub =
                            HgStoreStreamGrpc.newBlockingStub(channel).withDeadlineAfter(3, TimeUnit.SECONDS);
                    if (batch) {
                        stub.scanBatchOneShot(batchRequest());
                    } else {
                        stub.scanOneShot(ScanStreamReq.newBuilder().setMethod(ScanMethod.ALL)
                                                      .setLimit(10).build());
                    }
                    fail("cancelled unary scan reported success");
                    return Status.Code.OK;
                } catch (StatusRuntimeException error) {
                    return error.getStatus().getCode();
                }
            });
            Thread caller = new Thread(call, "one-shot-cancellation-client");
            try {
                caller.start();
                assertTrue(closing.await(2, TimeUnit.SECONDS));
                service.shutdownScans();
                assertEquals(1, scanRegistry(service).size());
                assertFalse(call.isDone());
                release.countDown();
                assertEquals(Status.Code.CANCELLED, call.get(2, TimeUnit.SECONDS));
                service.awaitScanCleanup();
                assertTrue(scanRegistry(service).isEmpty());
                verify(iterator).close();
            } finally {
                release.countDown();
                channel.shutdownNow();
                server.shutdownNow();
                executor.shutdownNow();
                caller.join(2000);
                assertFalse(caller.isAlive());
                assertTrue(channel.awaitTermination(2, TimeUnit.SECONDS));
                assertTrue(server.awaitTermination(2, TimeUnit.SECONDS));
            }
        }
    }

    @Test(timeout = 10000)
    public void testOneShotCancelledDuringCloseDoesNotCompleteSuccessfully() throws Exception {
        for (boolean batch : new boolean[]{false, true}) {
            ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(1);
            HgStoreWrapperEx wrapper = mock(HgStoreWrapperEx.class);
            ScanIterator iterator = mock(ScanIterator.class);
            StreamObserver<KvPageRes> output = mock(StreamObserver.class);
            when(wrapper.scanAll(anyString(), anyString(), any(byte[].class))).thenReturn(iterator);
            when(iterator.hasNext()).thenReturn(false);
            CountDownLatch closing = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            doAnswer(invocation -> {
                closing.countDown();
                awaitCloseRelease(release);
                return null;
            }).when(iterator).close();
            HgStoreStreamImpl service = scanService(executor, wrapper);
            FutureTask<Void> scanning = new FutureTask<>(() -> {
                if (batch) {
                    service.scanBatchOneShot(batchRequest(), output);
                } else {
                    service.scanOneShot(ScanStreamReq.newBuilder().setMethod(ScanMethod.ALL)
                                                       .setLimit(10).build(), output);
                }
                return null;
            });
            Thread caller = new Thread(scanning, "one-shot-close-cancellation");
            try {
                caller.start();
                assertTrue(closing.await(2, TimeUnit.SECONDS));
                service.shutdownScans();
                assertEquals(1, scanRegistry(service).size());
                assertFalse(scanning.isDone());
                verifyNoInteractions(output);
                release.countDown();
                scanning.get(2, TimeUnit.SECONDS);
                ArgumentCaptor<Throwable> error = ArgumentCaptor.forClass(Throwable.class);
                verify(output).onError(error.capture());
                assertEquals(Status.Code.CANCELLED, Status.fromThrowable(error.getValue()).getCode());
                verify(output, never()).onNext(any(KvPageRes.class));
                verify(output, never()).onCompleted();
                verify(iterator).close();
                assertTrue(scanRegistry(service).isEmpty());
                assertTrue(executor.awaitTermination(1, TimeUnit.SECONDS));
            } finally {
                release.countDown();
                caller.join(2000);
                executor.shutdownNow();
                assertFalse(caller.isAlive());
            }
        }
    }

    @Test(timeout = 5000)
    public void testBatchShutdownInterruptsBlockedReaderBeforeIteratorClose() throws Exception {
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(1);
        HgStoreWrapperEx wrapper = mock(HgStoreWrapperEx.class);
        ScanIterator iterator = mock(ScanIterator.class);
        when(wrapper.scanAll(anyString(), anyString(), any(byte[].class))).thenReturn(iterator);
        CountDownLatch reading = new CountDownLatch(1);
        CountDownLatch interrupted = new CountDownLatch(1);
        CountDownLatch readExited = new CountDownLatch(1);
        AtomicBoolean closedAfterRead = new AtomicBoolean();
        when(iterator.hasNext()).thenAnswer(invocation -> {
            reading.countDown();
            try {
                new CountDownLatch(1).await();
                return true;
            } catch (InterruptedException expected) {
                interrupted.countDown();
                return false;
            } finally {
                readExited.countDown();
            }
        });
        doAnswer(invocation -> {
            closedAfterRead.set(readExited.getCount() == 0);
            return null;
        }).when(iterator).close();

        HgStoreStreamImpl service = scanService(executor, wrapper);
        StreamObserver<KvPageRes> output = mock(StreamObserver.class);
        StreamObserver<ScanStreamBatchReq> input = service.scanBatch(output);
        FutureTask<Void> shutdown = new FutureTask<>(() -> {
            service.shutdownScans();
            return null;
        });
        Thread shutdownThread = new Thread(shutdown, "batch-scan-shutdown-test");
        try {
            input.onNext(batchRequest());
            assertTrue("batch reader must enter its blocking iterator read",
                       reading.await(1, TimeUnit.SECONDS));
            shutdownThread.start();
            shutdown.get(2, TimeUnit.SECONDS);
            assertTrue(interrupted.await(1, TimeUnit.SECONDS));
            service.awaitScanCleanup();
            assertTrue(closedAfterRead.get());
            assertTrue(scanRegistry(service).isEmpty());
            assertCancelled(output);
            verify(output, never()).onCompleted();
            verify(iterator).close();
            assertTrue(executor.awaitTermination(1, TimeUnit.SECONDS));
        } finally {
            executor.shutdownNow();
            shutdownThread.interrupt();
            shutdownThread.join(1000);
        }
    }

    @Test(timeout = 5000)
    public void testEncodedBatchShutdownInterruptsBlockedScannerBeforeIteratorClose() throws Exception {
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(1);
        HgStoreWrapperEx wrapper = mock(HgStoreWrapperEx.class);
        ScanIterator iterator = mock(ScanIterator.class);
        when(wrapper.scanAll(anyString(), anyString(), any(byte[].class))).thenReturn(iterator);
        CountDownLatch reading = new CountDownLatch(1);
        CountDownLatch interrupted = new CountDownLatch(1);
        CountDownLatch readExited = new CountDownLatch(1);
        AtomicBoolean closedAfterRead = new AtomicBoolean();
        when(iterator.hasNext()).thenAnswer(invocation -> {
            reading.countDown();
            try {
                new CountDownLatch(1).await();
                return true;
            } catch (InterruptedException expected) {
                interrupted.countDown();
                return false;
            } finally {
                readExited.countDown();
            }
        });
        doAnswer(invocation -> {
            closedAfterRead.set(readExited.getCount() == 0);
            return null;
        }).when(iterator).close();

        HgStoreStreamImpl service = scanService(executor, wrapper);
        StreamObserver<KvStream> output = mock(StreamObserver.class);
        StreamObserver<ScanStreamBatchReq> input = service.scanBatch2(output);
        FutureTask<Void> shutdown = new FutureTask<>(() -> {
            service.shutdownScans();
            return null;
        });
        Thread shutdownThread = new Thread(shutdown, "encoded-batch-scan-shutdown-test");
        try {
            input.onNext(batchRequest());
            assertTrue("parallel scanner must enter its blocking iterator read",
                       reading.await(1, TimeUnit.SECONDS));
            shutdownThread.start();
            shutdown.get(2, TimeUnit.SECONDS);
            assertTrue(interrupted.await(1, TimeUnit.SECONDS));
            service.awaitScanCleanup();
            assertTrue(closedAfterRead.get());
            assertTrue(scanRegistry(service).isEmpty());
            ArgumentCaptor<Throwable> failure = ArgumentCaptor.forClass(Throwable.class);
            verify(output).onError(failure.capture());
            assertEquals(Status.Code.CANCELLED, Status.fromThrowable(failure.getValue()).getCode());
            verify(output, never()).onCompleted();
            verify(iterator).close();
            assertTrue(executor.awaitTermination(1, TimeUnit.SECONDS));
        } finally {
            executor.shutdownNow();
            shutdownThread.interrupt();
            shutdownThread.join(1000);
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

    @Test(timeout = 5000)
    public void testBatchDoneRejectsSecondQueryBeforeAllocatingIterator() throws Exception {
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(1);
        HgStoreWrapperEx wrapper = mock(HgStoreWrapperEx.class);
        ScanIterator source = mock(ScanIterator.class);
        when(wrapper.scanAll(anyString(), anyString(), any(byte[].class))).thenReturn(source);
        StreamObserver<KvStream> output = mock(StreamObserver.class);
        ScanBatchResponse response = new ScanBatchResponse(output, wrapper, executor);
        Field state = ScanBatchResponse.class.getDeclaredField("state");
        state.setAccessible(true);
        Field iterator = ScanBatchResponse.class.getDeclaredField("iterator");
        iterator.setAccessible(true);
        try {
            response.onNext(batchRequest());
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
            while (!"DONE".equals(state.get(response).toString()) && System.nanoTime() < deadline) {
                Thread.yield();
            }
            assertEquals("DONE", state.get(response).toString());
            verify(source).close();
            assertNull(iterator.get(response));
            response.onNext(batchRequest());
            // The single worker processes the queued request before this barrier.
            executor.submit(() -> { }).get(2, TimeUnit.SECONDS);
            assertNull("a completed query must reject another request before allocating", iterator.get(response));
            response.onCompleted();
            response.onCompleted();
            verify(wrapper).scanAll(anyString(), anyString(), any(byte[].class));
            verify(source).close();
            verify(output).onCompleted();
            verify(output, never()).onError(any(Throwable.class));
        } finally {
            // Preserve cleanup even when the old implementation allocated a second producer.
            Object remaining = iterator.get(response);
            response.onCompleted();
            if (remaining instanceof ScanIterator) {
                ((ScanIterator) remaining).close();
            }
            executor.shutdownNow();
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

    @Test(timeout = 5000)
    public void testRejectedStreamReportsOnlyFailure() {
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(1);
        executor.shutdown();
        HgStoreWrapperEx wrapper = mock(HgStoreWrapperEx.class);
        AppConfig config = mock(AppConfig.class);
        when(config.getServerWaitTime()).thenReturn(60);
        StreamObserver<KvPageRes> output = mock(StreamObserver.class);
        ScanStreamResponse response = ScanStreamResponse.of(output, wrapper, executor, config);
        ScanStreamReq request = ScanStreamReq.newBuilder().setMethod(ScanMethod.ALL)
                                             .setPageSize(1).setLimit(10).build();

        response.onNext(request);
        response.onNext(request);
        response.onCompleted();

        verify(output).onError(any(Throwable.class));
        verify(output, never()).onNext(any(KvPageRes.class));
        verify(output, never()).onCompleted();
        verifyNoInteractions(wrapper);
    }

    @Test(timeout = 5000)
    public void testStreamIteratorFailureReportsOnlyFailure() throws Exception {
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(1);
        HgStoreWrapperEx wrapper = mock(HgStoreWrapperEx.class);
        when(wrapper.scanAll(anyString(), anyString(), any(byte[].class)))
                .thenThrow(new IllegalStateException("iterator failed"));
        AppConfig config = mock(AppConfig.class);
        when(config.getServerWaitTime()).thenReturn(60);
        StreamObserver<KvPageRes> output = mock(StreamObserver.class);
        ScanStreamResponse response = ScanStreamResponse.of(output, wrapper, executor, config);
        FutureTask<Void> request = new FutureTask<>(() -> {
            response.onNext(ScanStreamReq.newBuilder().setMethod(ScanMethod.ALL)
                                         .setPageSize(1).setLimit(10).build());
            return null;
        });
        Thread caller = new Thread(request, "scan-failure-test");
        try {
            caller.start();
            request.get(1, TimeUnit.SECONDS);
            executor.shutdown();
            assertTrue(executor.awaitTermination(1, TimeUnit.SECONDS));
            response.onCompleted();
            verify(output).onError(any(Throwable.class));
            verify(output, never()).onNext(any(KvPageRes.class));
            verify(output, never()).onCompleted();
        } finally {
            response.onCompleted();
            executor.shutdownNow();
            caller.join(1000);
        }
    }

    @Test(timeout = 5000)
    public void testReceiveFailureDoesNotInterruptItsCallbackThread() throws Exception {
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(1);
        CountDownLatch occupied = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
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
        when(config.getServerWaitTime()).thenReturn(0);
        StreamObserver<KvPageRes> output = mock(StreamObserver.class);
        ScanStreamResponse response = ScanStreamResponse.of(output, wrapper, executor, config);
        FutureTask<Boolean> request = new FutureTask<>(() -> {
            response.onNext(ScanStreamReq.newBuilder().setMethod(ScanMethod.ALL)
                                         .setPageSize(1).setLimit(10).build());
            return Thread.currentThread().isInterrupted();
        });
        Thread caller = new Thread(request, "scan-receive-failure-test");
        try {
            assertTrue(occupied.await(1, TimeUnit.SECONDS));
            caller.start();
            assertFalse(request.get(1, TimeUnit.SECONDS));
            release.countDown();
            executor.shutdown();
            assertTrue(executor.awaitTermination(1, TimeUnit.SECONDS));
            verify(output).onError(any(Throwable.class));
            verify(output, never()).onNext(any(KvPageRes.class));
            verify(output, never()).onCompleted();
            verifyNoInteractions(wrapper);
        } finally {
            release.countDown();
            response.onCompleted();
            executor.shutdownNow();
            caller.join(1000);
        }
    }

    @Test(timeout = 10000)
    public void testStreamFailureWaitsForPageCallback() throws Exception {
        AtomicReference<Thread> scanWorker = new AtomicReference<>();
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(1, task -> {
            Thread thread = new Thread(task, "scan-page-failure-worker");
            scanWorker.set(thread);
            return thread;
        });
        HgStoreWrapperEx wrapper = mock(HgStoreWrapperEx.class);
        ScanIterator iterator = mock(ScanIterator.class);
        when(wrapper.scanAll(anyString(), anyString(), any(byte[].class))).thenReturn(iterator);
        when(iterator.next()).thenReturn(RocksDBSession.BackendColumn.of(new byte[4], new byte[0]));
        when(iterator.position()).thenReturn(new byte[4]);
        CountDownLatch pageEntered = new CountDownLatch(1);
        CountDownLatch releasePage = new CountDownLatch(1);
        CountDownLatch iteratorFailed = new CountDownLatch(1);
        CountDownLatch errorReceived = new CountDownLatch(1);
        AtomicInteger advances = new AtomicInteger();
        when(iterator.hasNext()).thenAnswer(invocation -> {
            if (advances.incrementAndGet() <= 2) {
                return true;
            }
            assertTrue(pageEntered.await(1, TimeUnit.SECONDS));
            iteratorFailed.countDown();
            throw new IllegalStateException("failure after first page");
        });
        StreamObserver<KvPageRes> output = mock(StreamObserver.class);
        List<String> signals = Collections.synchronizedList(new ArrayList<>());
        doAnswer(invocation -> {
            signals.add("page-start");
            pageEntered.countDown();
            assertTrue(releasePage.await(2, TimeUnit.SECONDS));
            signals.add("page-end");
            return null;
        }).when(output).onNext(any(KvPageRes.class));
        doAnswer(invocation -> {
            signals.add("error");
            errorReceived.countDown();
            return null;
        }).when(output).onError(any(Throwable.class));
        AppConfig config = mock(AppConfig.class);
        when(config.getServerWaitTime()).thenReturn(60);
        ScanStreamResponse response = ScanStreamResponse.of(output, wrapper, executor, config);
        FutureTask<Void> request = new FutureTask<>(() -> {
            response.onNext(ScanStreamReq.newBuilder().setMethod(ScanMethod.ALL)
                                         .setPageSize(1).setLimit(10).build());
            return null;
        });
        Thread caller = new Thread(request, "scan-page-failure-test");
        try {
            caller.start();
            assertTrue(pageEntered.await(1, TimeUnit.SECONDS));
            assertTrue(iteratorFailed.await(1, TimeUnit.SECONDS));
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
            while (scanWorker.get().getState() != Thread.State.BLOCKED &&
                   errorReceived.getCount() != 0 && System.nanoTime() < deadline) {
                Thread.yield();
            }
            assertEquals("worker must reach the response serialization point",
                         Thread.State.BLOCKED, scanWorker.get().getState());
            assertEquals(1L, errorReceived.getCount());
            releasePage.countDown();
            request.get(1, TimeUnit.SECONDS);
            executor.shutdown();
            assertTrue(executor.awaitTermination(1, TimeUnit.SECONDS));
            assertEquals(Arrays.asList("page-start", "page-end", "error"), signals);
            verify(output).onError(any(Throwable.class));
            verify(output, never()).onCompleted();
            verify(iterator).close();
        } finally {
            releasePage.countDown();
            response.onCompleted();
            executor.shutdownNow();
            caller.join(1000);
        }
    }

    @Test(timeout = 10000)
    public void testNormalStreamCancellationClaimsTerminalBeforeInterruptingWorker() throws Exception {
        for (boolean closeRequest : new boolean[]{false, true}) {
            CountDownLatch workerEntered = new CountDownLatch(1);
            CountDownLatch iteratorClosed = new CountDownLatch(1);
            ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(1);
            HgStoreWrapperEx wrapper = mock(HgStoreWrapperEx.class);
            ScanIterator iterator = mock(ScanIterator.class);
            when(wrapper.scanAll(anyString(), anyString(), any(byte[].class))).thenReturn(iterator);
            when(iterator.next()).thenReturn(RocksDBSession.BackendColumn.of(new byte[4], new byte[0]));
            when(iterator.position()).thenReturn(new byte[4]);
            AtomicInteger advances = new AtomicInteger();
            when(iterator.hasNext()).thenAnswer(invocation -> {
                if (advances.incrementAndGet() <= 2) {
                    return true;
                }
                workerEntered.countDown();
                new CountDownLatch(1).await();
                return false;
            });
            doAnswer(invocation -> {
                iteratorClosed.countDown();
                return null;
            }).when(iterator).close();
            AppConfig config = mock(AppConfig.class);
            when(config.getServerWaitTime()).thenReturn(60);
            StreamObserver<KvPageRes> output = mock(StreamObserver.class);
            ScanStreamResponse response = ScanStreamResponse.of(output, wrapper, executor, config);
            try {
                response.onNext(ScanStreamReq.newBuilder().setMethod(ScanMethod.ALL)
                                             .setPageSize(1).setLimit(10).build());
                assertTrue(workerEntered.await(1, TimeUnit.SECONDS));
                if (closeRequest) {
                    response.onNext(ScanStreamReq.newBuilder().setCloseFlag(1).build());
                } else {
                    response.onCompleted();
                }
                executor.shutdown();
                assertTrue(executor.awaitTermination(1, TimeUnit.SECONDS));
                verify(output, never()).onError(any(Throwable.class));
                verify(output).onCompleted();
                verify(iterator).close();
            } finally {
                response.onCompleted();
                executor.shutdownNow();
            }
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
        Field bodySize = ParallelScanIterator.class.getDeclaredField("maxBodySize");
        bodySize.setAccessible(true);
        int originalBodySize = bodySize.getInt(null);
        bodySize.setInt(null, 2);
        ParallelScanIterator scan = null;
        try {
            scan = ParallelScanIterator.of(
                    () -> new KVPair<>(mock(QueryCondition.class), source), () -> Long.MAX_VALUE,
                    ScanQueryRequest.getDefaultInstance(), executor);
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
            if (scan != null) {
                scan.close();
            }
            bodySize.setInt(null, originalBodySize);
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

    @Test
    public void testInterruptedOneShotDoesNotSendPartialSuccess() throws Exception {
        for (boolean batch : new boolean[]{false, true}) {
            ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(1);
            HgStoreWrapperEx wrapper = mock(HgStoreWrapperEx.class);
            ScanIterator iterator = mock(ScanIterator.class);
            StreamObserver<KvPageRes> output = mock(StreamObserver.class);
            when(wrapper.scanAll(anyString(), anyString(), any(byte[].class))).thenReturn(iterator);
            when(iterator.hasNext()).thenReturn(true);
            when(iterator.position()).thenReturn(new byte[4]);
            when(iterator.next()).thenAnswer(invocation -> {
                Thread.currentThread().interrupt();
                return RocksDBSession.BackendColumn.of(new byte[4], new byte[0]);
            });
            try {
                HgStoreStreamImpl service = scanService(executor, wrapper);
                if (batch) {
                    service.scanBatchOneShot(batchRequest(), output);
                } else {
                    service.scanOneShot(ScanStreamReq.newBuilder().setMethod(ScanMethod.ALL).setLimit(10).build(),
                                        output);
                }
                verify(iterator).next();
                assertTrue("scan must preserve an external interrupt",
                           Thread.currentThread().isInterrupted());
                verify(output).onError(any(Throwable.class));
                verify(output, never()).onNext(any(KvPageRes.class));
                verify(output, never()).onCompleted();
                verify(iterator).close();
                StreamObserver<KvPageRes> nextOutput = mock(StreamObserver.class);
                if (batch) {
                    service.scanBatchOneShot(batchRequest(), nextOutput);
                } else {
                    service.scanOneShot(ScanStreamReq.newBuilder().setMethod(ScanMethod.ALL).setLimit(10).build(),
                                        nextOutput);
                }
                assertTrue("an interrupt present at entry must be preserved", Thread.currentThread().isInterrupted());
                verify(nextOutput).onError(any(Throwable.class));
                verify(nextOutput, never()).onNext(any(KvPageRes.class));
                verify(nextOutput, never()).onCompleted();
            } finally {
                Thread.interrupted();
                executor.shutdownNow();
            }
        }
    }

    @Test(timeout = 5000)
    public void testInterruptedBatchWorkerDoesNotCompletePartialPage() throws Exception {
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(1);
        HgStoreWrapperEx wrapper = mock(HgStoreWrapperEx.class);
        ScanIterator iterator = mock(ScanIterator.class);
        StreamObserver<KvPageRes> output = mock(StreamObserver.class);
        when(wrapper.scanAll(anyString(), anyString(), any(byte[].class))).thenReturn(iterator);
        when(iterator.hasNext()).thenReturn(true);
        when(iterator.position()).thenReturn(new byte[4]);
        when(iterator.next()).thenAnswer(invocation -> {
            Thread.currentThread().interrupt();
            return RocksDBSession.BackendColumn.of(new byte[4], new byte[0]);
        });
        StreamObserver<ScanStreamBatchReq> response = ScanBatchResponse3.of(output, wrapper, executor);
        try {
            response.onNext(batchRequest());
            executor.shutdown();
            assertTrue(executor.awaitTermination(1, TimeUnit.SECONDS));
            verify(iterator).next();
            assertCancelled(output);
            verify(output, never()).onNext(any(KvPageRes.class));
            verify(output, never()).onCompleted();
            verify(iterator).close();
        } finally {
            response.onCompleted();
            executor.shutdownNow();
        }
    }

    @Test(timeout = 10000)
    public void testCancellationDoesNotInterruptReceiverAfterHandoff() throws Exception {
        assertReceiverInterruptAfterHandoff(false);
    }

    @Test(timeout = 10000)
    public void testCancellationPreservesExternalReceiverInterruptAfterHandoff() throws Exception {
        assertReceiverInterruptAfterHandoff(true);
    }

    @Test(timeout = 5000)
    public void testReceivePreservesExternalInterrupt() throws Exception {
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(1);
        HgStoreWrapperEx wrapper = mock(HgStoreWrapperEx.class);
        AppConfig config = mock(AppConfig.class);
        when(config.getServerWaitTime()).thenReturn(60);
        StreamObserver<KvPageRes> output = mock(StreamObserver.class);
        ScanStreamResponse response = ScanStreamResponse.of(output, wrapper, executor, config);
        FutureTask<Boolean> request = new FutureTask<>(() -> {
            Thread.currentThread().interrupt();
            response.onNext(ScanStreamReq.newBuilder().setMethod(ScanMethod.ALL)
                                         .setPageSize(1).setLimit(10).build());
            return Thread.currentThread().isInterrupted();
        });
        Thread caller = new Thread(request, "scan-external-interrupt-test");
        try {
            caller.start();
            assertTrue(request.get(1, TimeUnit.SECONDS));
            assertCancelled(output);
            verify(output, never()).onCompleted();
            executor.shutdown();
            assertTrue(executor.awaitTermination(1, TimeUnit.SECONDS));
        } finally {
            response.onCompleted();
            executor.shutdownNow();
            caller.join(1000);
        }
    }

    private static void assertReceiverInterruptAfterHandoff(boolean externalInterrupt) throws Exception {
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(1);
        HgStoreWrapperEx wrapper = mock(HgStoreWrapperEx.class);
        ScanIterator iterator = mock(ScanIterator.class);
        when(wrapper.scanAll(anyString(), anyString(), any(byte[].class))).thenReturn(iterator);
        when(iterator.next()).thenReturn(RocksDBSession.BackendColumn.of(new byte[4], new byte[0]));
        when(iterator.position()).thenReturn(new byte[4]);
        CountDownLatch beforePage = new CountDownLatch(1);
        CountDownLatch releasePage = new CountDownLatch(1);
        AtomicInteger advances = new AtomicInteger();
        when(iterator.hasNext()).thenAnswer(invocation -> {
            int advance = advances.incrementAndGet();
            if (advance == 2) {
                beforePage.countDown();
                assertTrue(releasePage.await(2, TimeUnit.SECONDS));
            }
            return advance <= 2;
        });
        AppConfig config = mock(AppConfig.class);
        when(config.getServerWaitTime()).thenReturn(60);
        StreamObserver<KvPageRes> output = mock(StreamObserver.class);
        ScanStreamResponse response = ScanStreamResponse.of(output, wrapper, executor, config);
        Field responseLock = ScanStreamResponse.class.getDeclaredField("responseLock");
        responseLock.setAccessible(true);
        FutureTask<Boolean> request = new FutureTask<>(() -> {
            response.onNext(ScanStreamReq.newBuilder().setMethod(ScanMethod.ALL)
                                         .setPageSize(1).setLimit(10).build());
            return Thread.currentThread().isInterrupted();
        });
        Thread caller = new Thread(request, "scan-late-cancellation-test");
        try {
            caller.start();
            assertTrue(beforePage.await(1, TimeUnit.SECONDS));
            synchronized (responseLock.get(response)) {
                // The real channel hands off a page before its callback reaches responseLock.
                releasePage.countDown();
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
                while (caller.getState() != Thread.State.BLOCKED && System.nanoTime() < deadline) {
                    Thread.yield();
                }
                assertEquals(Thread.State.BLOCKED, caller.getState());
                if (externalInterrupt) {
                    caller.interrupt();
                }
                response.onCompleted();
            }
            assertEquals(externalInterrupt, request.get(1, TimeUnit.SECONDS));
            executor.shutdown();
            assertTrue(executor.awaitTermination(1, TimeUnit.SECONDS));
            verify(output).onCompleted();
            verify(output, never()).onError(any(Throwable.class));
            verify(output, never()).onNext(any(KvPageRes.class));
            verify(iterator).close();
        } finally {
            releasePage.countDown();
            response.onCompleted();
            executor.shutdownNow();
            caller.join(1000);
        }
    }

    @Test(timeout = 5000)
    public void testInterruptedStreamWorkerDoesNotCompletePartialPage() throws Exception {
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(1);
        HgStoreWrapperEx wrapper = mock(HgStoreWrapperEx.class);
        ScanIterator iterator = mock(ScanIterator.class);
        when(wrapper.scanAll(anyString(), anyString(), any(byte[].class))).thenReturn(iterator);
        when(iterator.hasNext()).thenReturn(true);
        when(iterator.position()).thenReturn(new byte[4]);
        when(iterator.next()).thenAnswer(invocation -> {
            Thread.currentThread().interrupt();
            return RocksDBSession.BackendColumn.of(new byte[4], new byte[0]);
        });
        AppConfig config = mock(AppConfig.class);
        when(config.getServerWaitTime()).thenReturn(60);
        StreamObserver<KvPageRes> output = mock(StreamObserver.class);
        ScanStreamResponse response = ScanStreamResponse.of(output, wrapper, executor, config);
        try {
            response.onNext(ScanStreamReq.newBuilder().setMethod(ScanMethod.ALL)
                                         .setPageSize(1).setLimit(10).build());
            executor.shutdown();
            assertTrue(executor.awaitTermination(1, TimeUnit.SECONDS));
            verify(iterator).next();
            assertCancelled(output);
            verify(output, never()).onNext(any(KvPageRes.class));
            verify(output, never()).onCompleted();
            verify(iterator).close();
        } finally {
            response.onCompleted();
            executor.shutdownNow();
        }
    }

    @Test(timeout = 5000)
    public void testInterruptedStreamSenderReportsCancellation() throws Exception {
        AtomicReference<Thread> worker = new AtomicReference<>();
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(1, task -> {
            Thread thread = new Thread(task, "scan-interrupted-sender-test");
            worker.set(thread);
            return thread;
        });
        HgStoreWrapperEx wrapper = mock(HgStoreWrapperEx.class);
        ScanIterator iterator = mock(ScanIterator.class);
        when(wrapper.scanAll(anyString(), anyString(), any(byte[].class))).thenReturn(iterator);
        when(iterator.hasNext()).thenReturn(true);
        when(iterator.next()).thenReturn(RocksDBSession.BackendColumn.of(new byte[4], new byte[0]));
        when(iterator.position()).thenReturn(new byte[4]);
        AppConfig config = mock(AppConfig.class);
        when(config.getServerWaitTime()).thenReturn(60);
        StreamObserver<KvPageRes> output = mock(StreamObserver.class);
        CountDownLatch errorReceived = new CountDownLatch(1);
        doAnswer(invocation -> {
            errorReceived.countDown();
            return null;
        }).when(output).onError(any(Throwable.class));
        ScanStreamResponse response = ScanStreamResponse.of(output, wrapper, executor, config);
        try {
            // Consume one page, then interrupt the actual worker waiting to hand off its next page.
            response.onNext(ScanStreamReq.newBuilder().setMethod(ScanMethod.ALL)
                                         .setPageSize(1).setLimit(10).build());
            awaitTimedWaiting(worker.get());
            worker.get().interrupt();
            assertTrue(errorReceived.await(1, TimeUnit.SECONDS));
            executor.shutdown();
            assertTrue(executor.awaitTermination(1, TimeUnit.SECONDS));
            assertCancelled(output);
            verify(output).onNext(any(KvPageRes.class));
            verify(output, never()).onCompleted();
            verify(iterator).close();
        } finally {
            response.onCompleted();
            executor.shutdownNow();
        }
    }

    @Test(timeout = 5000)
    public void testChannelCloseReleasesWaitingReceiverWithoutInterrupt() throws Exception {
        HgChannel<String> channel = HgChannel.of(60);
        FutureTask<Boolean> receive = new FutureTask<>(() -> {
            assertNull(channel.receive());
            return Thread.currentThread().isInterrupted();
        });
        Thread caller = new Thread(receive, "channel-close-receiver-test");
        try {
            caller.start();
            awaitTimedWaiting(caller);
            channel.close();
            assertFalse(receive.get(1, TimeUnit.SECONDS));
            assertNull(channel.receive());
            assertFalse(channel.send("after-close"));
        } finally {
            channel.close();
            caller.join(1000);
        }
    }

    @Test(timeout = 5000)
    public void testChannelCloseReleasesWaitingProducer() throws Exception {
        HgChannel<String> channel = HgChannel.of(60);
        FutureTask<Boolean> send = new FutureTask<>(() -> channel.send("page"));
        Thread producer = new Thread(send, "channel-close-producer-test");
        try {
            producer.start();
            awaitTimedWaiting(producer);
            channel.close();
            assertFalse(send.get(1, TimeUnit.SECONDS));
        } finally {
            channel.close();
            producer.join(1000);
        }
    }

    @Test(timeout = 5000)
    public void testChannelRetainsTimeoutAndNormalHandoff() throws Exception {
        HgChannel<String> channel = HgChannel.of(1);
        AtomicInteger timeouts = new AtomicInteger();
        long started = System.nanoTime();
        assertNull(channel.receive(timeout -> {
            assertEquals(Long.valueOf(1L), timeout);
            timeouts.incrementAndGet();
        }));
        assertEquals(1, timeouts.get());
        assertTrue("short close polling must not shorten the configured timeout",
                   System.nanoTime() - started >= TimeUnit.MILLISECONDS.toNanos(900));
        FutureTask<Boolean> send = new FutureTask<>(() -> channel.send("page"));
        Thread producer = new Thread(send, "channel-handoff-test");
        try {
            producer.start();
            assertEquals("page", channel.receive());
            assertTrue(send.get(1, TimeUnit.SECONDS));
        } finally {
            channel.close();
            producer.join(1000);
        }
    }

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
}
