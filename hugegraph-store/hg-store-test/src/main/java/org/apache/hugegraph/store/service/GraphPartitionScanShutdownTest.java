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
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.hugegraph.store.business.BusinessHandler;
import org.apache.hugegraph.store.business.GraphStoreIterator;
import org.apache.hugegraph.store.grpc.Graphpb.ScanResponse;
import org.apache.hugegraph.store.node.grpc.HgStoreStreamImpl;
import org.junit.Test;

import io.grpc.stub.StreamObserver;

public class GraphPartitionScanShutdownTest extends GraphPartitionScanTestSupport {

    @Test
    public void testFailedNativeCloseRemainsInShutdownBarrier() throws Exception {
        ThreadPoolExecutor executor = executor();
        try {
            HgStoreStreamImpl service = service(executor);
            GraphStoreIterator<?> iterator = mock(GraphStoreIterator.class);
            RuntimeException failure = new IllegalStateException("native close failed");
            doThrow(failure).when(iterator).close();
            BusinessHandler handler = mock(BusinessHandler.class);
            doReturn(iterator).when(handler).scan(any());
            StreamObserver<ScanResponse> response = mock(StreamObserver.class);
            service.scanGraphPartition(response, handler).onNext(request(0));
            verify(response, timeout(2000)).onError(failure);
            assertEquals(1, registry(service).size());
            Object lifecycle = registry(service).keySet().iterator().next();
            java.lang.reflect.Method cleanup = lifecycle.getClass().getDeclaredMethod("cleanupFailure");
            cleanup.setAccessible(true);
            assertSame(failure, cleanup.invoke(lifecycle));
            verify(iterator).close();
        } finally {
            executor.shutdown();
            assertTrue(executor.awaitTermination(2, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testShutdownWaitsForActiveReadBeforeClosingNativeIterator() throws Exception {
        ThreadPoolExecutor executor = executor();
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch stopped = new CountDownLatch(1);
        CountDownLatch stoppingStarted = new CountDownLatch(1);
        try {
            HgStoreStreamImpl service = service(executor);
            GraphStoreIterator<?> iterator = mock(GraphStoreIterator.class);
            when(iterator.hasNext()).thenAnswer(call -> {
                entered.countDown();
                boolean interrupted = false;
                try {
                    // Model native work that cannot exit until the read itself finishes.
                    while (release.getCount() != 0) {
                        try {
                            release.await();
                        } catch (InterruptedException cancellation) {
                            interrupted = true;
                        }
                    }
                } finally {
                    if (interrupted) {
                        Thread.currentThread().interrupt();
                    }
                }
                return false;
            });
            BusinessHandler handler = mock(BusinessHandler.class);
            doReturn(iterator).when(handler).scan(any());
            StreamObserver<ScanResponse> response = mock(StreamObserver.class);
            service.scanGraphPartition(response, handler).onNext(request(0));
            assertTrue(entered.await(2, TimeUnit.SECONDS));
            Thread stopping = new Thread(() -> {
                service.stopAcceptingScans();
                stoppingStarted.countDown();
                service.shutdownScans();
                service.awaitScanCleanup();
                stopped.countDown();
            });
            stopping.start();
            assertTrue(stoppingStarted.await(2, TimeUnit.SECONDS));
            assertFalse(stopped.await(100, TimeUnit.MILLISECONDS));
            verify(iterator, never()).close();
            release.countDown();
            assertTrue(stopped.await(2, TimeUnit.SECONDS));
            stopping.join(2000);
            verify(iterator).close();
            verify(response).onError(any());
            assertTrue(registry(service).isEmpty());
        } finally {
            release.countDown();
            executor.shutdown();
            assertTrue(executor.awaitTermination(2, TimeUnit.SECONDS));
        }
    }
}
