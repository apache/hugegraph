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

package org.apache.hugegraph.dist;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.config.ServerOptions;
import org.apache.hugegraph.util.ExecutorUtil;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

import com.sun.management.ThreadMXBean;

public class MemoryMonitor {

    private static final Logger LOG = Log.logger(MemoryMonitor.class);
    private final double MEMORY_MONITOR_THRESHOLD;
    private final int MEMORY_MONITOR_DETECT_PERIOD;
    private final ScheduledExecutorService scheduler;

    public MemoryMonitor(String restServerConf) {
        HugeConfig restServerConfig = new HugeConfig(restServerConf);
        MEMORY_MONITOR_THRESHOLD =
                restServerConfig.get(ServerOptions.JVM_MEMORY_MONITOR_THRESHOLD);
        MEMORY_MONITOR_DETECT_PERIOD =
                restServerConfig.get(ServerOptions.JVM_MEMORY_MONITOR_DETECT_PERIOD);
        this.scheduler = ExecutorUtil.newScheduledThreadPool("memory-monitor-thread-%d");
    }

    private void runMemoryDetect() {
        double memoryUsagePercentage = getMemoryUsageRatio();

        if (memoryUsagePercentage > MEMORY_MONITOR_THRESHOLD) {
            LOG.warn("JVM memory usage is '{}', exceeding the threshold of '{}'.",
                     memoryUsagePercentage, MEMORY_MONITOR_THRESHOLD);
            System.gc();
            LOG.warn("Trigger System.gc()");

            double doubleCheckUsage = getMemoryUsageRatio();
            if (doubleCheckUsage > MEMORY_MONITOR_THRESHOLD) {
                LOG.warn("JVM memory usage is '{}', exceeding the threshold of '{}'.",
                         doubleCheckUsage, MEMORY_MONITOR_THRESHOLD);
                interruptHighestMemoryThread();
            }
        }
    }

    private double getMemoryUsageRatio() {
        MemoryUsage heapMemoryUsage = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
        return (double) heapMemoryUsage.getUsed() / heapMemoryUsage.getMax();
    }

    private Thread getHighestMemoryThread() {
        long highestMemory = 0;
        Thread highestThread = null;

        ThreadMXBean threadMXBean = (ThreadMXBean) ManagementFactory.getThreadMXBean();

        Thread[] threads = new Thread[Thread.activeCount()];
        Thread.enumerate(threads);
        for (Thread thread : threads) {
            if (thread.getState() != Thread.State.RUNNABLE || thread.getName() == null ||
                !thread.getName().startsWith("grizzly-http-server-")) {
                continue;
            }

            long threadMemory = threadMXBean.getThreadAllocatedBytes(thread.getId());
            if (threadMemory > highestMemory) {
                highestMemory = threadMemory;
                highestThread = thread;
            }
        }
        return highestThread;
    }

    private void interruptHighestMemoryThread() {
        Thread targetThread = getHighestMemoryThread();
        if (targetThread != null) {
            targetThread.interrupt();
            LOG.warn("Send interrupt to '{}' thread", targetThread.getName());
        }
    }

    public void start() {
        if (MEMORY_MONITOR_THRESHOLD >= 1.0) {
            LOG.info("Memory monitoring is disabled by threshold setting.");
            return;
        }
        this.scheduler.scheduleAtFixedRate(this::runMemoryDetect, 0, MEMORY_MONITOR_DETECT_PERIOD,
                                           TimeUnit.MILLISECONDS);
        LOG.info("Memory monitoring started.");
    }

    public void stop() {
        if (MEMORY_MONITOR_THRESHOLD >= 1.0) {
            return;
        }
        this.scheduler.shutdownNow();
        LOG.info("Memory monitoring stopped.");
    }
}
