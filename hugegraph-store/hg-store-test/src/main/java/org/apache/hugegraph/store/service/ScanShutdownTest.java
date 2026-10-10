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
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hugegraph.rocksdb.access.ScanIterator;
import org.apache.hugegraph.store.grpc.common.ScanMethod;
import org.apache.hugegraph.store.grpc.stream.KvPageRes;
import org.apache.hugegraph.store.grpc.stream.ScanStreamReq;
import org.apache.hugegraph.store.node.AppConfig;
import org.apache.hugegraph.store.node.grpc.GrpcShutdownBarrier;
import org.apache.hugegraph.store.node.grpc.HgStoreStreamImpl;
import org.apache.hugegraph.store.node.grpc.HgStoreWrapperEx;
import org.apache.hugegraph.store.node.grpc.ScanStreamResponse;
import org.apache.hugegraph.store.node.grpc.query.AggregativeQueryService;
import org.apache.hugegraph.store.node.listener.ContextClosedListener;
import org.apache.hugegraph.store.node.task.TTLCleaner;
import org.junit.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

public class ScanShutdownTest extends ScanTestSupport {

    @Test(timeout = 20000)
    public void testOrdinaryScanCleanupFailureBlocksSpringDestruction() throws Exception {
        for (int mode = 0; mode < 5; mode++) {
            assertCleanupBlocksDestruction(mode);
        }
    }

    private static void assertCleanupBlocksDestruction(int mode) throws Exception {
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(4);
        HgStoreWrapperEx wrapper = mock(HgStoreWrapperEx.class);
        ScanIterator broken = mock(ScanIterator.class);
        ScanIterator healthy = mock(ScanIterator.class);
        IllegalStateException failure = new IllegalStateException("injected native release failure");
        doThrow(failure).when(broken).close();
        when(wrapper.scanAll(anyString(), anyString(), any(byte[].class))).thenReturn(broken, healthy);
        HgStoreStreamImpl service = scanService(executor, wrapper);
        AnnotationConfigApplicationContext context =
                new AnnotationConfigApplicationContext();
        AtomicBoolean destroyed = new AtomicBoolean();
        FutureTask<Void> closing = new FutureTask<>(() -> {
            context.close();
            return null;
        });
        Thread closer = new Thread(closing, "scan-failed-cleanup-context-close");
        try {
            context.getBeanFactory().registerSingleton("storeStream", service);
            context.getBeanFactory().registerSingleton("queryService",
                    mock(AggregativeQueryService.class));
            context.getBeanFactory().registerSingleton("cleaner",
                    mock(TTLCleaner.class));
            context.getDefaultListableBeanFactory().registerDisposableBean("database", () -> destroyed.set(true));
            context.register(ContextClosedListener.class,
                             GrpcShutdownBarrier.class);
            context.refresh();
            for (int i = 0; i < 2; i++) {
                ScanStreamReq request = ScanStreamReq.newBuilder().setMethod(ScanMethod.ALL)
                                                     .setPageSize(1).setLimit(10).build();
                switch (mode) {
                    case 0:
                        service.scan(mock(StreamObserver.class)).onNext(request);
                        break;
                    case 1:
                        service.scanBatch2(mock(StreamObserver.class)).onNext(batchRequest());
                        break;
                    case 2:
                        service.scanBatch(mock(StreamObserver.class)).onNext(batchRequest());
                        break;
                    case 3:
                        service.scanOneShot(request, mock(StreamObserver.class));
                        break;
                    default:
                        service.scanBatchOneShot(batchRequest(), mock(StreamObserver.class));
                }
            }
            verify(broken, timeout(2000)).close();
            verify(healthy, timeout(2000)).close();
            closer.start();
            assertTrue(executor.awaitTermination(2, TimeUnit.SECONDS));
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
            Thread.State observed = closer.getState();
            while (observed != Thread.State.TIMED_WAITING && System.nanoTime() < deadline) {
                Thread.yield();
                observed = closer.getState();
            }
            // Assert the captured wait observation: a second state read can see a
            // notification wakeup even though database destruction remains blocked.
            assertEquals(Thread.State.TIMED_WAITING, observed);
            assertFalse("terminated workers do not confirm native cleanup", closing.isDone());
            assertFalse(destroyed.get());
            verify(broken).close();
            verify(healthy).close();
            Map<?, ?> pending = scanRegistry(service);
            assertEquals(1, pending.size());
            Object retained = pending.keySet().iterator().next();
            Field cleanupFailure = retained.getClass().getDeclaredField("cleanupFailure");
            cleanupFailure.setAccessible(true);
            assertSame(failure, cleanupFailure.get(retained));
        } finally {
            // Test-only teardown: production never clears a failed native release.
            Map<?, ?> pending = scanRegistry(service);
            synchronized (pending) {
                pending.clear();
                pending.notifyAll();
            }
            executor.shutdownNow();
            if (closer.isAlive()) {
                closing.get(2, TimeUnit.SECONDS);
            }
            context.close();
        }
    }




}
