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

package org.apache.hugegraph.pd.client;

import java.lang.reflect.Field;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.grpc.Pdpb;
import org.apache.hugegraph.pd.grpc.kv.LockRequest;
import org.apache.hugegraph.pd.grpc.kv.LockResponse;
import org.apache.hugegraph.pd.grpc.kv.WatchResponse;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

import io.grpc.MethodDescriptor;

@RunWith(Parameterized.class)
public class KvClientLockTest {

    private final boolean reentrant;

    public KvClientLockTest(boolean reentrant) {
        this.reentrant = reentrant;
    }

    @Parameterized.Parameters(name = "reentrant={0}")
    public static Object[][] lockMethods() {
        return new Object[][]{{true}, {false}};
    }

    @Test(timeout = 10000)
    public void testPreInterruptedLockDoesNotSendRequest() throws Exception {
        try (ControlledKvClient client = new ControlledKvClient(false)) {
            AtomicReference<Throwable> failure = new AtomicReference<>();
            AtomicBoolean interrupted = new AtomicBoolean();
            Thread caller = new Thread(() -> {
                Thread.currentThread().interrupt();
                callAndCapture(client, failure, interrupted);
            });
            try {
                caller.start();
                caller.join(5000);
                Assert.assertFalse(caller.isAlive());
                assertInterrupted(failure.get(), interrupted.get());
                Assert.assertTrue(client.requests.isEmpty());

                Assert.assertTrue(callLock(client).getSucceed());
                Assert.assertEquals(0L, client.requests.get(0).getClientId());
                Assert.assertTrue(callLock(client).getSucceed());
                Assert.assertEquals(42L, client.requests.get(1).getClientId());
            } finally {
                caller.interrupt();
                caller.join(5000);
            }
        }
    }

    @Test(timeout = 15000)
    public void testInterruptedWaitDoesNotReleaseAnotherCallersPermit() throws Exception {
        try (ControlledKvClient client = new ControlledKvClient(true)) {
            ExecutorService executor = Executors.newSingleThreadExecutor();
            AtomicReference<Throwable> failure = new AtomicReference<>();
            AtomicBoolean interrupted = new AtomicBoolean();
            Thread waiter = new Thread(() -> callAndCapture(client, failure, interrupted));
            try {
                Future<LockResponse> first = executor.submit(() -> callLock(client));
                Assert.assertTrue(client.firstRequest.await(5, TimeUnit.SECONDS));
                Semaphore semaphore = lockSemaphore(client);
                waiter.start();
                awaitQueuedWaiter(semaphore);

                waiter.interrupt();
                waiter.join(5000);
                Assert.assertFalse(waiter.isAlive());
                assertInterrupted(failure.get(), interrupted.get());
                Assert.assertEquals(1, client.requests.size());
                Assert.assertEquals("The first caller must still own the permit", 0,
                                    semaphore.availablePermits());

                client.releaseFirst.countDown();
                Assert.assertEquals(42L, first.get(5, TimeUnit.SECONDS).getClientId());
                Assert.assertEquals(1, semaphore.availablePermits());
                Assert.assertTrue(callLock(client).getSucceed());
                Assert.assertEquals(42L, client.requests.get(1).getClientId());
            } finally {
                client.releaseFirst.countDown();
                waiter.interrupt();
                waiter.join(5000);
                executor.shutdownNow();
                Assert.assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
            }
        }
    }

    private LockResponse callLock(KvClient<?> client) throws PDException {
        return this.reentrant ? client.lock("key", 1000L) :
               client.lockWithoutReentrant("key", 1000L);
    }

    private void callAndCapture(KvClient<?> client, AtomicReference<Throwable> failure,
                                AtomicBoolean interrupted) {
        try {
            callLock(client);
        } catch (Throwable e) {
            failure.set(e);
            interrupted.set(Thread.currentThread().isInterrupted());
        }
    }

    private static void assertInterrupted(Throwable failure, boolean interrupted) {
        Assert.assertTrue(failure instanceof PDException);
        Assert.assertEquals(Pdpb.ErrorType.UNKNOWN_VALUE, ((PDException) failure).getErrorCode());
        Assert.assertTrue(failure.getCause() instanceof InterruptedException);
        Assert.assertTrue(interrupted);
    }

    private static Semaphore lockSemaphore(KvClient<?> client) throws Exception {
        Field field = KvClient.class.getDeclaredField("lockSemaphore");
        field.setAccessible(true);
        return (Semaphore) field.get(client);
    }

    private static void awaitQueuedWaiter(Semaphore semaphore) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!semaphore.hasQueuedThreads() && System.nanoTime() < deadline) {
            Thread.sleep(1);
        }
        Assert.assertTrue("The second caller must be waiting for initialization",
                          semaphore.hasQueuedThreads());
    }

    private static class ControlledKvClient extends KvClient<WatchResponse> {

        private final boolean holdFirst;
        private final CountDownLatch firstRequest = new CountDownLatch(1);
        private final CountDownLatch releaseFirst = new CountDownLatch(1);
        private final List<LockRequest> requests = new CopyOnWriteArrayList<>();

        ControlledKvClient(boolean holdFirst) {
            super(PDConfig.of("unused:8686"), Mockito.mock(ScheduledExecutorService.class));
            this.holdFirst = holdFirst;
        }

        @SuppressWarnings("unchecked")
        @Override
        protected <ReqT, RespT> RespT blockingUnaryCall(
                MethodDescriptor<ReqT, RespT> method, ReqT request) throws PDException {
            this.requests.add((LockRequest) request);
            if (this.holdFirst && this.requests.size() == 1) {
                this.firstRequest.countDown();
                try {
                    Assert.assertTrue(this.releaseFirst.await(10, TimeUnit.SECONDS));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new PDException(Pdpb.ErrorType.UNKNOWN_VALUE, e);
                }
            }
            return (RespT) LockResponse.newBuilder().setHeader(AbstractClient.okHeader)
                                      .setClientId(42L).setSucceed(true).build();
        }
    }
}
