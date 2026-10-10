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

package org.apache.hugegraph.store.node.grpc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hugegraph.rocksdb.access.RocksDBSession;
import org.apache.hugegraph.rocksdb.access.ScanIterator;
import org.apache.hugegraph.store.HgScanQuery;
import org.apache.hugegraph.store.buffer.KVByteBuffer;
import org.apache.hugegraph.store.client.grpc.KvBatchScanner;
import org.apache.hugegraph.store.client.grpc.KvBatchScannerMerger;
import org.apache.hugegraph.store.grpc.common.Header;
import org.apache.hugegraph.store.grpc.common.ScanMethod;
import org.apache.hugegraph.store.grpc.stream.HgStoreStreamGrpc;
import org.apache.hugegraph.store.grpc.stream.KvPageRes;
import org.apache.hugegraph.store.grpc.stream.KvStream;
import org.apache.hugegraph.store.grpc.stream.ScanCancelRequest;
import org.apache.hugegraph.store.grpc.stream.ScanQueryRequest;
import org.apache.hugegraph.store.grpc.stream.ScanStreamBatchReq;
import org.junit.Test;

import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;

public class BatchScanHalfCloseTest {

    @Test(timeout = 5000)
    public void testQueuedQueryHalfCloseDeliversFinalPartialBatch() throws Exception {
        for (boolean encoded : new boolean[]{false, true}) {
            DeferredExecutor executor = new DeferredExecutor();
            ScanIterator source = rows(3);
            HgStoreWrapperEx wrapper = wrapper(source);
            Output output = new Output();
            StreamObserver<ScanStreamBatchReq> input = input(encoded, output, wrapper, executor);
            try {
                input.onNext(request(2));
                input.onCompleted();
                assertEquals("half-close must preserve the queued query", 0, output.completions);
                executor.runAll();
                assertEquals(3, output.rows);
                assertTrue(output.over);
                assertEquals(1, output.completions);
                assertEquals(null, output.error);
                verify(source).close();
                input.onCompleted();
                assertEquals(1, output.completions);
            } finally {
                input.onError(Status.CANCELLED.asRuntimeException());
                executor.runAll();
                executor.shutdownNow();
            }
        }
    }

    @Test(timeout = 5000)
    public void testHalfCloseWithExhaustedCreditFailsInsteadOfTruncating() throws Exception {
        for (boolean encoded : new boolean[]{false, true}) {
            DeferredExecutor executor = new DeferredExecutor();
            Output output = new Output();
            ScanIterator iterator;
            StreamObserver<ScanStreamBatchReq> input;
            if (encoded) {
                ScanBatchResponse response = new ScanBatchResponse(output.streams(), mock(HgStoreWrapperEx.class),
                                                                   executor);
                iterator = batches(20);
                set(response, "iterator", iterator);
                set(response, "limit", Long.MAX_VALUE);
                set(response, "queryReceived", true);
                input = response;
            } else {
                iterator = rows(21);
                input = input(false, output, wrapper(iterator), executor);
                input.onNext(request(1));
            }
            try {
                input.onCompleted();
                executor.runAll();
                assertTrue("granted batches must precede the credit failure", output.rows > 0);
                assertTrue("the full result exceeds granted credit", output.rows < 20);
                assertFalse(output.over);
                assertEquals(0, output.completions);
                assertEquals(Status.Code.FAILED_PRECONDITION, Status.fromThrowable(output.error).getCode());
                verify(iterator).close();
                input.onCompleted();
                assertEquals(1, output.errors);
            } finally {
                input.onError(Status.CANCELLED.asRuntimeException());
                executor.runAll();
                executor.shutdownNow();
            }
        }
    }

    @Test(timeout = 5000)
    public void testExplicitAndTransportCancellationWinBeforeFinalDelivery() throws Exception {
        for (boolean encoded : new boolean[]{false, true}) {
            for (boolean transport : new boolean[]{false, true}) {
                DeferredExecutor executor = new DeferredExecutor();
                Output output = new Output();
                AtomicReference<StreamObserver<ScanStreamBatchReq>> request = new AtomicReference<>();
                AtomicInteger checks = new AtomicInteger();
                ScanIterator source = rows(2);
                when(source.hasNext()).thenAnswer(ignored -> {
                    if (checks.getAndIncrement() == 0) {
                        if (transport) {
                            request.get().onError(Status.CANCELLED.asRuntimeException());
                        } else {
                            request.get().onNext(ScanStreamBatchReq.newBuilder().setCancelRequest(
                                    ScanCancelRequest.getDefaultInstance()).build());
                        }
                    }
                    return false;
                });
                StreamObserver<ScanStreamBatchReq> input;
                if (encoded) {
                    ScanBatchResponse response = new ScanBatchResponse(output.streams(), mock(HgStoreWrapperEx.class),
                                                                       executor);
                    set(response, "iterator", source);
                    set(response, "limit", Long.MAX_VALUE);
                    set(response, "queryReceived", true);
                    input = response;
                } else {
                    input = input(false, output, wrapper(source), executor);
                    input.onNext(request(1));
                }
                request.set(input);
                try {
                    input.onCompleted();
                    executor.runAll();
                    assertEquals(0, output.rows);
                    assertFalse(output.over);
                    assertEquals(0, output.completions);
                    assertEquals(1, output.errors);
                    assertEquals(Status.Code.CANCELLED, Status.fromThrowable(output.error).getCode());
                    input.onCompleted();
                    input.onError(Status.CANCELLED.asRuntimeException());
                    verify(source).close();
                    assertEquals(1, output.errors);
                } finally {
                    executor.shutdownNow();
                }
            }
        }
    }

    @Test(timeout = 5000)
    public void testClientEarlyCloseSendsCancelButNormalFinishOnlyHalfCloses() throws Exception {
        List<ScanStreamBatchReq.QueryCase> commands = new ArrayList<>();
        AtomicInteger completions = new AtomicInteger();
        String name = InProcessServerBuilder.generateName();
        Server server = InProcessServerBuilder.forName(name).directExecutor().addService(
                new HgStoreStreamGrpc.HgStoreStreamImplBase() {
                    @Override
                    public StreamObserver<ScanStreamBatchReq> scanBatch2(StreamObserver<KvStream> output) {
                        return new StreamObserver<ScanStreamBatchReq>() {
                            @Override
                            public void onNext(ScanStreamBatchReq value) {
                                commands.add(value.getQueryCase());
                            }

                            @Override
                            public void onError(Throwable error) {
                                throw new AssertionError(error);
                            }

                            @Override
                            public void onCompleted() {
                                completions.incrementAndGet();
                                output.onCompleted();
                            }
                        };
                    }
                }).build().start();
        ManagedChannel channel = InProcessChannelBuilder.forName(name).directExecutor().build();
        try {
            KvBatchScanner early = new KvBatchScanner(HgStoreStreamGrpc.newStub(channel), "g", HgScanQuery.tableOf("t"),
                                                      mock(KvBatchScannerMerger.class));
            early.close();
            early.close();
            assertEquals(java.util.Arrays.asList(ScanStreamBatchReq.QueryCase.QUERY_REQUEST,
                                               ScanStreamBatchReq.QueryCase.CANCEL_REQUEST), commands);
            assertEquals(1, completions.get());
            commands.clear();
            KvBatchScanner normal = new KvBatchScanner(HgStoreStreamGrpc.newStub(channel), "g", HgScanQuery.tableOf("t"),
                                                       mock(KvBatchScannerMerger.class));
            normal.dataComplete();
            assertEquals(Collections.singletonList(ScanStreamBatchReq.QueryCase.QUERY_REQUEST), commands);
            assertEquals(2, completions.get());
        } finally {
            channel.shutdownNow();
            server.shutdownNow();
        }
    }

    private static StreamObserver<ScanStreamBatchReq> input(boolean encoded, Output output,
                                                            HgStoreWrapperEx wrapper, DeferredExecutor executor) {
        return encoded ? new ScanBatchResponse(output.streams(), wrapper, executor) :
               ScanBatchResponse3.of(output.pages(), wrapper, executor);
    }

    private static ScanStreamBatchReq request(int pageSize) {
        return ScanStreamBatchReq.newBuilder().setHeader(Header.newBuilder().setGraph("g"))
                .setQueryRequest(ScanQueryRequest.newBuilder().setMethod(ScanMethod.ALL).setTable("t")
                    .setLimit(Long.MAX_VALUE).setPerKeyMax(Long.MAX_VALUE).setPageSize(pageSize)).build();
    }

    private static HgStoreWrapperEx wrapper(ScanIterator source) {
        HgStoreWrapperEx wrapper = mock(HgStoreWrapperEx.class);
        when(wrapper.scanAll(anyString(), anyString(), any(byte[].class))).thenReturn(source);
        return wrapper;
    }

    private static ScanIterator rows(int size) {
        ScanIterator source = mock(ScanIterator.class);
        AtomicInteger read = new AtomicInteger();
        when(source.hasNext()).thenAnswer(ignored -> read.get() < size);
        when(source.next()).thenAnswer(ignored -> {
            read.incrementAndGet();
            return RocksDBSession.BackendColumn.of(new byte[4], new byte[4]);
        });
        when(source.position()).thenReturn(new byte[4]);
        return source;
    }

    private static ScanIterator batches(int size) {
        ScanIterator source = mock(ScanIterator.class);
        AtomicInteger read = new AtomicInteger();
        when(source.hasNext()).thenAnswer(ignored -> read.get() < size);
        when(source.next()).thenAnswer(ignored -> {
            read.incrementAndGet();
            return Collections.singletonList(ParallelScanIterator.KV.of(
                    RocksDBSession.BackendColumn.of(new byte[4], new byte[4])));
        });
        return source;
    }

    private static void set(Object target, String name, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }

    private static final class DeferredExecutor extends ThreadPoolExecutor {

        private final Queue<Runnable> tasks = new ArrayDeque<>();

        DeferredExecutor() {
            super(1, 1, 0, TimeUnit.SECONDS, new LinkedBlockingQueue<>());
        }

        @Override
        public void execute(Runnable task) {
            this.tasks.add(task);
        }

        void runAll() {
            Runnable task;
            while ((task = this.tasks.poll()) != null) {
                task.run();
            }
        }
    }

    private static final class Output {

        int rows;
        int completions;
        int errors;
        boolean over;
        Throwable error;

        StreamObserver<KvPageRes> pages() {
            return new StreamObserver<KvPageRes>() {
                @Override
                public void onNext(KvPageRes value) {
                    rows += value.getDataCount();
                    over |= value.getOver();
                }

                @Override
                public void onError(Throwable failure) {
                    error = failure;
                    errors++;
                }

                @Override
                public void onCompleted() {
                    completions++;
                }
            };
        }

        StreamObserver<KvStream> streams() {
            return new StreamObserver<KvStream>() {
                @Override
                public void onNext(KvStream value) {
                    over |= value.getOver();
                    if (value.getStream() != null) {
                        KVByteBuffer buffer = new KVByteBuffer(value.getStream().duplicate());
                        while (buffer.hasRemaining()) {
                            buffer.getBytes();
                            buffer.getBytes();
                            rows++;
                        }
                    }
                }

                @Override
                public void onError(Throwable failure) {
                    error = failure;
                    errors++;
                }

                @Override
                public void onCompleted() {
                    completions++;
                }
            };
        }
    }
}
