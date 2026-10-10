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
import org.apache.hugegraph.store.node.grpc.query.AggregativeQueryService;
import org.apache.hugegraph.store.node.listener.ContextClosedListener;
import org.apache.hugegraph.store.node.task.TTLCleaner;
import org.junit.Test;
import org.lognet.springboot.grpc.context.GRpcServerInitializedEvent;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import io.grpc.Server;

public class ContextClosedListenerTest {


    @Test(timeout = 5000)
    public void testTtlCleanupBlocksBeanDestructionAfterWorkersTerminate() throws Exception {
        CountDownLatch awaitingCleanup = new CountDownLatch(1);
        CountDownLatch interruptedCleanup = new CountDownLatch(1);
        CountDownLatch releaseCleanup = new CountDownLatch(1);
        AtomicBoolean databaseClosed = new AtomicBoolean();
        TTLCleaner cleaner = mock(TTLCleaner.class);
        doAnswer(invocation -> {
            boolean interrupted = false;
            awaitingCleanup.countDown();
            boolean released = false;
            while (!released) {
                try {
                    releaseCleanup.await();
                    released = true;
                } catch (InterruptedException e) {
                    interrupted = true;
                    interruptedCleanup.countDown();
                }
            }
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
            return null;
        }).when(cleaner).awaitCleanup();
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
        try {
            context.getBeanFactory().registerSingleton("storeStream", mock(HgStoreStreamImpl.class));
            context.getBeanFactory().registerSingleton("queryService", mock(AggregativeQueryService.class));
            context.getBeanFactory().registerSingleton("cleaner", cleaner);
            context.getDefaultListableBeanFactory().registerDisposableBean("database", () -> databaseClosed.set(true));
            context.register(ContextClosedListener.class, GrpcShutdownBarrier.class);
            context.refresh();
            FutureTask<Boolean> closing = new FutureTask<>(() -> {
                context.close();
                return Thread.currentThread().isInterrupted();
            });
            Thread closeThread = new Thread(closing, "test-ttl-context-close");
            closeThread.setDaemon(true);
            closeThread.start();
            assertTrue(awaitingCleanup.await(1, TimeUnit.SECONDS));
            closeThread.interrupt();
            assertTrue(interruptedCleanup.await(1, TimeUnit.SECONDS));
            assertFalse("TTL native cleanup must finish before DB destruction", databaseClosed.get());
            assertFalse(closing.isDone());
            releaseCleanup.countDown();
            assertTrue("shutdown must preserve interruption", closing.get(2, TimeUnit.SECONDS));
            assertTrue(databaseClosed.get());
        } finally {
            releaseCleanup.countDown();
            context.close();
        }
    }

}
