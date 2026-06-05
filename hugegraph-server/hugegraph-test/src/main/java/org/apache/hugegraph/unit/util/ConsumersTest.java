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

package org.apache.hugegraph.unit.util;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.util.Consumers;
import org.junit.Test;

public class ConsumersTest {

    @Test(timeout = 1000)
    public void testStartProvideAwaitNormal() throws Throwable {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            AtomicInteger processed = new AtomicInteger();

            Consumers<Integer> consumers = new Consumers<>(executor, v -> {
                processed.incrementAndGet();
            });

            consumers.start("test");
            for (int i = 0; i < 50; i++) {
                consumers.provide(i);
            }
            consumers.await();

            Assert.assertEquals("Should process all provided elements",
                                50, processed.get());
        } finally {
            executor.shutdownNow();
        }
    }

    /**
     * Regression test for deadlock:
     *
     * ContextCallable fails before entering runAndDone().
     * await() must still return because latch is decremented in safeRun().
     */
    @Test(timeout = 1000)
    public void testAwaitDoesNotHangWhenContextCallableFails() throws Throwable {
        ExecutorService executor = Executors.newFixedThreadPool(1);
        try {
            // Use AssertionError to bypass the inner catch(Exception) loop in runAndDone()
            // This simulates a scenario where an exception escapes the task logic
            // (similar to how a ContextCallable failure would behave from safeRun's perspective)
            Consumers<Integer> consumers = new Consumers<>(executor, v -> {
                throw new AssertionError("Simulated fatal error (OOM/StackOverflow/etc)");
            });
            consumers.start("test-fatal-error");
            consumers.provide(1);
            // Verification:
            // Without the fix, the latch would never be decremented (because runAndDone crashes), causing await() to hang.
            // With the fix (safeRun wrapper), the finally block ensures latch.countDown() is called.
            consumers.await();

            // Note: consumer.exception will be null because safeRun only catches Exception, not Error.
            // This is acceptable behavior for fatal errors, as long as it doesn't deadlock.
        } finally {
            executor.shutdownNow();
        }
    }

    @Test(timeout = 1000)
    public void testAwaitThrowsWhenConsumerThrows() throws Throwable {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            final String msg = "Injected exception for test";

            Consumers<Integer> consumers = new Consumers<>(executor, v -> {
                throw new RuntimeException(msg);
            });

            consumers.start("test");
            consumers.provide(1);

            try {
                consumers.await();
                Assert.fail("Expected await() to throw when consumer throws");
            } catch (Throwable t) {
                Throwable root = t.getCause() != null ? t.getCause() : t;
                Assert.assertTrue("Expected RuntimeException, but got: " + root,
                                  root instanceof RuntimeException);
                Assert.assertTrue("Exception message should contain injected message",
                                  root.getMessage() != null &&
                                  root.getMessage().contains(msg));
            }
        } finally {
            executor.shutdownNow();
        }
    }
}
