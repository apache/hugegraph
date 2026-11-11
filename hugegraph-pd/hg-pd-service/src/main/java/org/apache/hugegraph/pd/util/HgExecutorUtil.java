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

package org.apache.hugegraph.pd.util;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hugegraph.pd.common.HgAssert;

public final class HgExecutorUtil {

    private static final Map<String, ThreadPoolExecutor> EXECUTOR_MAP = new ConcurrentHashMap<>();
    private static final Executor COMMON_EXECUTOR
            = new ThreadPoolExecutor(0, Integer.MAX_VALUE,
                                     60L, TimeUnit.SECONDS,
                                     new SynchronousQueue<>(),
                                     newThreadFactory("pd-common"));

    public static void execute(Runnable command) {
        if (command == null) {
            return;
        }
        COMMON_EXECUTOR.execute(command);
    }

    public static ThreadFactory newThreadFactory(String namePrefix, int priority) {
        HgAssert.isArgumentNotNull(namePrefix, "namePrefix");
        return new HgThreadFactory(namePrefix, priority);
    }

    public static ThreadFactory newThreadFactory(String namePrefix) {
        HgAssert.isArgumentNotNull(namePrefix, "namePrefix");
        return new HgDefaultThreadFactory(namePrefix);
    }

    public static ThreadPoolExecutor getThreadPoolExecutor(String name) {
        if (name == null) {
            return null;
        }
        return EXECUTOR_MAP.get(name);
    }

    /**
     * @see #createExecutor(String, int, int, int)
     */
    @Deprecated
    public static Executor createExecutor(String name, int coreThreads, int maxThreads) {
        return createExecutor(name, coreThreads, maxThreads, Integer.MAX_VALUE);
    }

    public static ThreadPoolExecutor createExecutor(String name, int coreThreads, int maxThreads,
                                                    int queueSize) {
        ThreadPoolExecutor res = EXECUTOR_MAP.get(name);

        if (res != null) {
            return res;
        }

        synchronized (EXECUTOR_MAP) {
            res = EXECUTOR_MAP.get(name);
            if (res != null) {
                return res;
            }

            BlockingQueue<Runnable> queue;

            if (queueSize <= 0) {
                queue = new SynchronousQueue<>();
            } else {
                queue = new LinkedBlockingQueue<>(queueSize);
            }

            res = new ThreadPoolExecutor(
                    coreThreads,
                    maxThreads,
                    60L, TimeUnit.SECONDS,
                    queue,
                    newThreadFactory(name)
            );
            EXECUTOR_MAP.put(name, res);
        }

        return res;
    }

    /**
     * The default thread factory
     */
    static class HgThreadFactory implements ThreadFactory {

        private final AtomicInteger threadNumber = new AtomicInteger(1);
        private final String namePrefix;
        private final int priority;

        HgThreadFactory(String namePrefix, int priority) {
            this.namePrefix = namePrefix;
            this.priority = priority;
        }

        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(null, r,
                                  namePrefix + "-" + threadNumber.getAndIncrement(),
                                  0);
            if (t.isDaemon()) {
                t.setDaemon(false);
            }
            if (t.getPriority() != priority) {
                t.setPriority(priority);
            }
            return t;
        }
    }

    /**
     * The default thread factory, which added threadNamePrefix in construction method.
     */
    static class HgDefaultThreadFactory implements ThreadFactory {

        private static final AtomicInteger POOL_NUMBER = new AtomicInteger(1);
        private final AtomicInteger threadNumber = new AtomicInteger(1);
        private final String namePrefix;

        HgDefaultThreadFactory(String threadNamePrefix) {
            this.namePrefix = threadNamePrefix + "-" +
                              POOL_NUMBER.getAndIncrement() +
                              "-thread-";
        }

        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(null, r,
                                  namePrefix + threadNumber.getAndIncrement(),
                                  0);
            if (t.isDaemon()) {
                t.setDaemon(false);
            }
            if (t.getPriority() != Thread.NORM_PRIORITY) {
                t.setPriority(Thread.NORM_PRIORITY);
            }
            return t;
        }
    }
}
