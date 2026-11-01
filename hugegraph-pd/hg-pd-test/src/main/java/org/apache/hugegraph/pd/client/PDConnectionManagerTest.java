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

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Assert;
import org.junit.Test;

public class PDConnectionManagerTest {

    @Test
    public void testInterruptedWaitCancelsTaskAndPreservesInterrupt() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        CountDownLatch taskStarted = new CountDownLatch(1);
        CountDownLatch taskCancelled = new CountDownLatch(1);
        CountDownLatch releaseTask = new CountDownLatch(1);
        AtomicBoolean interrupted = new AtomicBoolean();
        AtomicReference<String> result = new AtomicReference<>("pending");
        Thread waiter = new Thread(() -> {
            result.set(PDConnectionManager.tryTask(executor, () -> {
                taskStarted.countDown();
                try {
                    releaseTask.await();
                } catch (InterruptedException e) {
                    taskCancelled.countDown();
                    throw e;
                }
                return "completed";
            }, "interrupted leader query"));
            interrupted.set(Thread.currentThread().isInterrupted());
        });

        try {
            waiter.start();
            Assert.assertTrue(taskStarted.await(5, TimeUnit.SECONDS));
            waiter.interrupt();
            waiter.join(5000);

            Assert.assertFalse("The interrupted caller must stop waiting", waiter.isAlive());
            Assert.assertNull(result.get());
            Assert.assertTrue("The caller's interrupt must be preserved", interrupted.get());
            Assert.assertTrue("The abandoned task must be cancelled",
                              taskCancelled.await(5, TimeUnit.SECONDS));
            Assert.assertEquals("next query", executor.submit(() -> "next query")
                                                      .get(5, TimeUnit.SECONDS));
        } finally {
            releaseTask.countDown();
            waiter.interrupt();
            executor.shutdownNow();
            waiter.join(5000);
            Assert.assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testSuccessfulTaskReturnsResult() throws InterruptedException {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Assert.assertEquals("leader", PDConnectionManager.tryTask(executor, () -> "leader",
                                                                      "leader query"));
        } finally {
            executor.shutdownNow();
            Assert.assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testFailedTaskReturnsNull() throws InterruptedException {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Callable<String> task = () -> {
                throw new IllegalStateException("query failed");
            };
            Assert.assertNull(PDConnectionManager.tryTask(executor, task, "failed leader query"));
        } finally {
            executor.shutdownNow();
            Assert.assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }
}
