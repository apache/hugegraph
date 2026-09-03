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

package org.apache.hugegraph;

import java.lang.reflect.Field;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.apache.hugegraph.exception.HugeException;
import org.apache.hugegraph.exception.NotAllowException;
import org.apache.hugegraph.pd.client.KvClient;
import org.apache.hugegraph.pd.client.PDConfig;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.grpc.kv.WatchResponse;
import org.junit.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SchemaDriverTest {

    private AtomicReference<SchemaDriver> instance;
    private SchemaDriver previousInstance;

    @Before
    public void setUp() throws Exception {
        this.instance = instanceReference();
        this.previousInstance = this.instance.getAndSet(null);
    }

    @After
    public void tearDown() {
        this.instance.set(this.previousInstance);
    }

    @Test
    public void testDestroyClosesOwnedKvClient() {
        TrackingKvClient client = new TrackingKvClient();
        SchemaDriver driver = new SchemaDriver(client, 10, 60_000L);
        this.instance.set(driver);

        try {
            SchemaDriver.destroy();

            Assert.assertTrue(client.closed);
            Assert.assertNull(SchemaDriver.getInstance());
        } finally {
            client.close();
        }
    }

    @Test
    public void testDestroyKeepsInstanceUntilResourcesAreClosed() throws Exception {
        BlockingCloseKvClient client = new BlockingCloseKvClient();
        SchemaDriver driver = new SchemaDriver(client, 10, 60_000L);
        this.instance.set(driver);
        Thread destroyThread = new Thread(SchemaDriver::destroy, "schema-driver-destroy");
        try {
            destroyThread.start();
            client.awaitCloseStarted();

            Assert.assertSame(driver, SchemaDriver.getInstance());
        } finally {
            client.allowClose.countDown();
            destroyThread.join(5000L);
            client.close();
        }

        Assert.assertFalse(destroyThread.isAlive());
        Assert.assertTrue(client.closed);
    }

    @Test
    public void testInitDoesNotWaitForDestroyCleanup() throws Exception {
        BlockingCloseKvClient client = new BlockingCloseKvClient();
        SchemaDriver driver = new SchemaDriver(client, 10, 60_000L);
        this.instance.set(driver);
        AtomicReference<Throwable> initFailure = new AtomicReference<>();
        CountDownLatch initFinished = new CountDownLatch(1);
        Thread destroyThread = new Thread(SchemaDriver::destroy, "schema-driver-destroy");
        Thread initThread = new Thread(() -> {
            try {
                SchemaDriver.init(PDConfig.of("127.0.0.1:8686"), 10, 60_000L);
            } catch (Throwable throwable) {
                initFailure.set(throwable);
            } finally {
                initFinished.countDown();
            }
        }, "schema-driver-init");
        boolean initCompletedDuringCleanup;
        try {
            destroyThread.start();
            client.awaitCloseStarted();
            initThread.start();
            initCompletedDuringCleanup = initFinished.await(1L, TimeUnit.SECONDS);
        } finally {
            client.allowClose.countDown();
            destroyThread.join(5000L);
            initThread.join(5000L);
            client.close();
        }

        Assert.assertTrue(initCompletedDuringCleanup);
        Assert.assertTrue(initFailure.get() instanceof NotAllowException);
        Assert.assertFalse(destroyThread.isAlive());
        Assert.assertFalse(initThread.isAlive());
    }

    @Test
    public void testConcurrentDestroyWaitsForCleanup() throws Exception {
        BlockingCloseKvClient client = new BlockingCloseKvClient();
        SchemaDriver driver = new SchemaDriver(client, 10, 60_000L);
        this.instance.set(driver);
        CountDownLatch secondDestroyStarted = new CountDownLatch(1);
        CountDownLatch secondDestroyReturned = new CountDownLatch(1);
        Thread firstDestroy = new Thread(SchemaDriver::destroy, "schema-driver-destroy-first");
        Thread secondDestroy = new Thread(() -> {
            secondDestroyStarted.countDown();
            SchemaDriver.destroy();
            secondDestroyReturned.countDown();
        }, "schema-driver-destroy-second");
        boolean returnedBeforeCleanup;
        try {
            firstDestroy.start();
            client.awaitCloseStarted();
            secondDestroy.start();
            Assert.assertTrue(secondDestroyStarted.await(5L, TimeUnit.SECONDS));
            returnedBeforeCleanup = secondDestroyReturned.await(500L, TimeUnit.MILLISECONDS);
        } finally {
            client.allowClose.countDown();
            firstDestroy.join(5000L);
            secondDestroy.join(5000L);
            client.close();
        }

        Assert.assertFalse(returnedBeforeCleanup);
        Assert.assertFalse(firstDestroy.isAlive());
        Assert.assertFalse(secondDestroy.isAlive());
    }

    @Test
    public void testConstructorFailureClosesOwnedKvClient() {
        FailingListenKvClient client = new FailingListenKvClient();

        try {
            new SchemaDriver(client, 10, 60_000L);
            Assert.fail("SchemaDriver construction should fail");
        } catch (HugeException expected) {
            Assert.assertEquals(2, client.listenCalls);
            Assert.assertTrue(client.closed);
        } finally {
            client.close();
        }
    }

    @SuppressWarnings("unchecked")
    private static AtomicReference<SchemaDriver> instanceReference() throws Exception {
        Field field = SchemaDriver.class.getDeclaredField("INSTANCE");
        field.setAccessible(true);
        return (AtomicReference<SchemaDriver>) field.get(null);
    }

    private static class TrackingKvClient extends KvClient<WatchResponse> {

        protected volatile boolean closed;

        TrackingKvClient() {
            super(PDConfig.of("127.0.0.1:8686"));
        }

        @Override
        public void listen(String key, Consumer<WatchResponse> consumer) throws PDException {
            // Avoid opening a real PD stream while constructing SchemaDriver.
        }

        @Override
        public void close() {
            this.closed = true;
            super.close();
        }
    }

    private static class BlockingCloseKvClient extends TrackingKvClient {

        private final CountDownLatch closeStarted = new CountDownLatch(1);
        private final CountDownLatch allowClose = new CountDownLatch(1);

        @Override
        public void close() {
            this.closeStarted.countDown();
            try {
                this.allowClose.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Client close was interrupted", e);
            }
            super.close();
        }

        private void awaitCloseStarted() throws InterruptedException {
            Assert.assertTrue(this.closeStarted.await(5L, TimeUnit.SECONDS));
        }
    }

    private static class FailingListenKvClient extends TrackingKvClient {

        private int listenCalls;

        @Override
        public void listen(String key, Consumer<WatchResponse> consumer) throws PDException {
            if (++this.listenCalls == 2) {
                throw new PDException(-1, "listener startup failed");
            }
        }
    }
}
