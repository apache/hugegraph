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

import java.lang.reflect.Constructor;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.LongAdder;

import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.testutil.Whitebox;
import org.apache.hugegraph.util.FixedTimerWindowRateLimiter;
import org.apache.hugegraph.util.FixedWatchWindowRateLimiter;
import org.apache.hugegraph.util.RateLimiter;
import org.junit.Before;
import org.junit.Test;

public abstract class RateLimiterTest {

    @Before
    public void setup() {
        // pass
    }

    protected abstract RateLimiter newRateLimiter(int rate);

    @Test
    public void testDefaultRateLimiterCreate() {
        int rateLimit = 500;
        RateLimiter limiter = RateLimiter.create(rateLimit);
        Assert.assertInstanceOf(FixedTimerWindowRateLimiter.class, limiter);

        Object limit = Whitebox.getInternalState(limiter, "limit");
        Assert.assertEquals(rateLimit, limit);
    }

    @Test
    public void testTryAcquire() {
        int rateLimit = 400;
        RateLimiter limiter = this.newRateLimiter(rateLimit);

        int limit = Whitebox.getInternalState(limiter, "limit");
        Assert.assertEquals(rateLimit, limit);

        LongAdder count = Whitebox.getInternalState(limiter, "count");
        Assert.assertEquals(0, count.intValue());

        for (int i = 0; i < rateLimit; i++) {
            Assert.assertTrue(limiter.tryAcquire());
        }

        count = Whitebox.getInternalState(limiter, "count");
        Assert.assertEquals(rateLimit, count.intValue());
        Assert.assertFalse(limiter.tryAcquire());
    }

    public static class FixedTimerWindowRateLimiterTest extends
                                                        RateLimiterTest {

        @Override
        public RateLimiter newRateLimiter(int rate) {
            return new FixedTimerWindowRateLimiter(rate);
        }

        @Test
        public void testInitialTimerWindow() throws Exception {
            try (RecordingTimer timer = new RecordingTimer()) {
                Constructor<FixedTimerWindowRateLimiter> constructor =
                        FixedTimerWindowRateLimiter.class.getDeclaredConstructor(int.class, Timer.class);
                constructor.setAccessible(true);
                RateLimiter limiter = constructor.newInstance(400, timer);

                // The initial reset must wait for a complete window, even if the timer thread starts late.
                Assert.assertEquals(RateLimiter.RESET_PERIOD, timer.delay);
                Assert.assertEquals(RateLimiter.RESET_PERIOD, timer.period);
                for (int i = 0; i < 400; i++) {
                    Assert.assertTrue(limiter.tryAcquire());
                }
                LongAdder count = Whitebox.getInternalState(limiter, "count");
                Assert.assertEquals(400, count.intValue());
                Assert.assertFalse(limiter.tryAcquire());

                timer.task.run();
                Assert.assertEquals(0, count.intValue());
                Assert.assertTrue(limiter.tryAcquire());
                timer.task.run();
                Assert.assertEquals(1, count.intValue());
            }
        }

        private static class RecordingTimer extends Timer implements AutoCloseable {

            private TimerTask task;
            private long delay;
            private long period;

            private RecordingTimer() {
                super("RateLimiterTest", true);
            }

            @Override
            public void schedule(TimerTask task, long delay, long period) {
                this.task = task;
                this.delay = delay;
                this.period = period;
            }

            @Override
            public void close() {
                this.cancel();
            }
        }

        @Test
        public void testTryAcquireAfterPeriod() throws Exception {
            int rateLimit = 400;
            RateLimiter limiter = this.newRateLimiter(rateLimit);

            for (int i = 0; i < rateLimit; i++) {
                Assert.assertTrue(limiter.tryAcquire());
            }

            Thread.sleep(RateLimiter.RESET_PERIOD * 2);
            Assert.assertTrue(limiter.tryAcquire());
            LongAdder count = Whitebox.getInternalState(limiter, "count");
            Assert.assertEquals(1, count.intValue());
        }

        @Test
        public void testTimerRateLimiterWithIdle() throws Exception {
            int rateLimit = 200;
            RateLimiter limiter = this.newRateLimiter(rateLimit);

            LongAdder count = Whitebox.getInternalState(limiter, "count");
            Assert.assertEquals(0, count.intValue());

            for (int i = 0; i < rateLimit; i++) {
                Assert.assertTrue(limiter.tryAcquire());
            }
            Thread.sleep(RateLimiter.RESET_PERIOD * 2);
            count = Whitebox.getInternalState(limiter, "count");
            Assert.assertEquals(0, count.intValue());

            // Assert count doesn't reset after period if not hit limit
            for (int i = 0; i < rateLimit / 2; i++) {
                Assert.assertTrue(limiter.tryAcquire());
            }
            Thread.sleep(RateLimiter.RESET_PERIOD);

            count = Whitebox.getInternalState(limiter, "count");
            Assert.assertEquals(rateLimit / 2, count.intValue());
            Assert.assertTrue(limiter.tryAcquire());

            Thread.sleep(RateLimiter.RESET_PERIOD);
            Assert.assertTrue(limiter.tryAcquire());
        }
    }

    public static class FixedWatchWindowRateLimiterTest extends
                                                        RateLimiterTest {

        @Override
        public RateLimiter newRateLimiter(int rate) {
            return new FixedWatchWindowRateLimiter(rate);
        }

        @Test
        public void testTryAcquireAfterPeriod() throws Exception {
            int rateLimit = 300;
            RateLimiter limiter = this.newRateLimiter(rateLimit);

            for (int i = 0; i < rateLimit; i++) {
                Assert.assertTrue(limiter.tryAcquire());
            }

            // Reset count after period
            Thread.sleep(RateLimiter.RESET_PERIOD);
            Assert.assertTrue(limiter.tryAcquire());
            LongAdder count = Whitebox.getInternalState(limiter, "count");
            Assert.assertEquals(1, count.intValue());
        }

        @Test
        public void testStopWatchRateLimiterWithIdle() throws Exception {
            int rateLimit = 100;
            RateLimiter limiter = this.newRateLimiter(rateLimit);

            LongAdder count = Whitebox.getInternalState(limiter, "count");
            Assert.assertEquals(0, count.intValue());

            for (int i = 0; i < rateLimit; i++) {
                Assert.assertTrue(limiter.tryAcquire());
            }
            count = Whitebox.getInternalState(limiter, "count");
            Assert.assertEquals(rateLimit, count.intValue());

            // Count will not be reset if tryAcquire() is not called
            Thread.sleep(RateLimiter.RESET_PERIOD);
            count = Whitebox.getInternalState(limiter, "count");
            Assert.assertEquals(rateLimit, count.intValue());

            // Reset when method call
            Assert.assertTrue(limiter.tryAcquire());
            count = Whitebox.getInternalState(limiter, "count");
            Assert.assertEquals(1, count.intValue());
        }
    }
}
