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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hugegraph.store.node.grpc.GrpcShutdownBarrier;
import org.apache.hugegraph.store.node.grpc.HgStoreStreamImpl;
import org.apache.hugegraph.store.node.listener.ContextClosedListener;
import org.apache.hugegraph.store.node.task.TTLCleaner;
import org.junit.Test;
import org.lognet.springboot.grpc.context.GRpcServerInitializedEvent;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import io.grpc.Server;

public class ContextClosedListenerTest {

    @Test(timeout = 5000)
    public void testContextCloseStopsActiveWorkersWithoutPartitionDelay() throws Exception {
        ThreadPoolExecutor scan = (ThreadPoolExecutor) Executors.newFixedThreadPool(1);
        ThreadPoolExecutor ttl = (ThreadPoolExecutor) Executors.newFixedThreadPool(1);
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        CountDownLatch started = new CountDownLatch(3);
        CountDownLatch interrupted = new CountDownLatch(3);
        CountDownLatch cleanupStarted = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch cleanupAllowed = new CountDownLatch(1);
        Runnable work = () -> {
            started.countDown();
            try {
                release.await();
            } catch (InterruptedException e) {
                interrupted.countDown();
                cleanupStarted.countDown();
                boolean done = false;
                while (!done) {
                    try {
                        cleanupAllowed.await();
                        done = true;
                    } catch (InterruptedException ignored) {
                        // Model resource cleanup which must finish before DB destruction.
                    }
                }
                Thread.currentThread().interrupt();
            }
        };
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
        try {
            HgStoreStreamImpl stream = mock(HgStoreStreamImpl.class);
            TTLCleaner cleaner = mock(TTLCleaner.class);
            when(stream.getRealExecutor()).thenReturn(scan);
            doAnswer(invocation -> {
                scan.shutdownNow();
                return null;
            }).when(stream).shutdownScans();
            when(cleaner.getExecutor()).thenReturn(ttl);
            when(cleaner.getScheduler()).thenReturn(scheduler);
            context.getBeanFactory().registerSingleton("storeStream", stream);
            context.getBeanFactory().registerSingleton("cleaner", cleaner);
            context.register(ContextClosedListener.class, GrpcShutdownBarrier.class);
            context.refresh();
            scan.execute(work);
            ttl.execute(work);
            scheduler.execute(work);
            assertTrue("all shutdown participants must be active", started.await(1, TimeUnit.SECONDS));

            FutureTask<Void> closing = new FutureTask<>(() -> {
                context.close();
                return null;
            });
            Thread closeThread = new Thread(closing, "test-context-close");
            closeThread.setDaemon(true);
            closeThread.start();
            // Scheduler shutdown must not let DB destruction overtake resource cleanup.
            assertTrue(cleanupStarted.await(1, TimeUnit.SECONDS));
            assertFalse("context close must wait for worker cleanup", closing.isDone());
            cleanupAllowed.countDown();
            closing.get(2, TimeUnit.SECONDS);
            assertTrue("close must interrupt active workers", interrupted.await(1, TimeUnit.SECONDS));
            assertTrue(scan.awaitTermination(1, TimeUnit.SECONDS));
            assertTrue(ttl.awaitTermination(1, TimeUnit.SECONDS));
            assertTrue(scheduler.awaitTermination(1, TimeUnit.SECONDS));
        } finally {
            cleanupAllowed.countDown();
            release.countDown();
            scan.shutdownNow();
            ttl.shutdownNow();
            scheduler.shutdownNow();
            context.close();
        }
    }

    @Test(timeout = 5000)
    public void testGrpcCallbacksFinishBeforeBeanDestructionEvenWhenInterrupted() throws Exception {
        CountDownLatch awaitingCallbacks = new CountDownLatch(1);
        CountDownLatch releaseCallbacks = new CountDownLatch(1);
        AtomicBoolean terminated = new AtomicBoolean();
        AtomicBoolean databaseClosed = new AtomicBoolean();
        Server server = mock(Server.class);
        when(server.shutdownNow()).thenReturn(server);
        when(server.isTerminated()).thenAnswer(invocation -> terminated.get());
        when(server.awaitTermination(5, TimeUnit.SECONDS)).thenAnswer(invocation -> {
            awaitingCallbacks.countDown();
            releaseCallbacks.await();
            terminated.set(true);
            return true;
        });
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
        try {
            context.getBeanFactory().registerSingleton("storeStream", mock(HgStoreStreamImpl.class));
            context.getBeanFactory().registerSingleton("cleaner", mock(TTLCleaner.class));
            context.getDefaultListableBeanFactory().registerDisposableBean("database", () -> databaseClosed.set(true));
            context.register(ContextClosedListener.class, GrpcShutdownBarrier.class);
            context.refresh();
            context.publishEvent(new GRpcServerInitializedEvent(context, server));
            FutureTask<Boolean> closing = new FutureTask<>(() -> {
                context.close();
                return Thread.currentThread().isInterrupted();
            });
            Thread closeThread = new Thread(closing, "test-grpc-context-close");
            closeThread.setDaemon(true);
            closeThread.start();
            assertTrue(awaitingCallbacks.await(1, TimeUnit.SECONDS));
            closeThread.interrupt();
            assertFalse("DB must remain open while callbacks own its resources", databaseClosed.get());
            releaseCallbacks.countDown();
            assertTrue("shutdown must preserve interruption", closing.get(2, TimeUnit.SECONDS));
            assertTrue(terminated.get());
            assertTrue(databaseClosed.get());
        } finally {
            releaseCallbacks.countDown();
            context.close();
        }
    }
}
