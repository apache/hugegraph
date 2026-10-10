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

package org.apache.hugegraph.util;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.LongAdder;

/**
 * This class is used for fixed window to limit request per second
 * The different with stopwatch is to use timer for reducing count times
 * <p>
 * TODO: Move to common module
 */
public class FixedTimerWindowRateLimiter implements RateLimiter {

    private final Timer timer;
    private final LongAdder count;
    private final int limit;

    public FixedTimerWindowRateLimiter(int limitPerSecond) {
        this(limitPerSecond, new Timer("RateAuditLog", true));
    }

    FixedTimerWindowRateLimiter(int limitPerSecond, Timer timer) {
        this.timer = timer;
        this.count = new LongAdder();
        this.limit = limitPerSecond;
        // Count will be reset if hit limit (run once per 1000ms)
        this.timer.schedule(new TimerTask() {
            @Override
            public void run() {
                if (count.intValue() >= limit) {
                    count.reset();
                }
            }
        }, RESET_PERIOD, RESET_PERIOD);
    }

    @Override
    public boolean tryAcquire() {
        if (count.intValue() >= limit) {
            return false;
        }

        count.increment();
        return true;
    }
}
