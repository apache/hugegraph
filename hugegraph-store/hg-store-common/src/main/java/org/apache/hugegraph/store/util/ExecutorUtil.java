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

package org.apache.hugegraph.store.util;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

//FIXME Using Guava Cache
public final class ExecutorUtil {

    private static final Map<String, ThreadPoolExecutor> pools = new ConcurrentHashMap<>();

    public static ThreadPoolExecutor getThreadPoolExecutor(String name) {
        if (name == null) {
            return null;
        }
        return pools.get(name);
    }

    public static ThreadPoolExecutor createExecutor(String name, int coreThreads, int maxThreads,
                                                    int queueSize) {

        return createExecutor(name, coreThreads, maxThreads, queueSize, true);
    }

    public static ThreadPoolExecutor createExecutor(String name, int coreThreads, int maxThreads,
                                                    int queueSize, boolean daemon) {
        //Argument check
        if (coreThreads <= 0 || maxThreads <= 0) {
            throw new IllegalArgumentException("coreThreads and maxThreads must be positive");
        }

        if (coreThreads > maxThreads) {
            throw new IllegalArgumentException("coreThreads cannot be greater than maxThreads");
        }

        ThreadPoolExecutor res = pools.get(name);
        if (res != null) {
            return res;
        }
        synchronized (pools) {
            res = pools.get(name);
            if (res != null) {
                return res;
            }
            BlockingQueue queue;
            if (queueSize <= 0) {
                queue = new SynchronousQueue();
            } else {
                queue = new LinkedBlockingQueue<>(queueSize);
            }
            res = new ThreadPoolExecutor(coreThreads, maxThreads, 60L, TimeUnit.SECONDS, queue,
                                         new DefaultThreadFactory(name, daemon));
            pools.put(name, res);
        }
        return res;
    }

    /**
     * Shutdown name-specific thread pool
     *
     * @param name
     * @param now
     */
    public static void shutdown(String name, boolean now) {
        if (name == null) {
            return;
        }
        ThreadPoolExecutor executor = pools.remove(name);
        if (executor != null) {
            if (now) {
                executor.shutdownNow();
            } else {
                executor.shutdown();
            }
        }
    }

    public static void shutDownAll(boolean now) {
        for (Map.Entry<String, ThreadPoolExecutor> entry : pools.entrySet()) {
            ThreadPoolExecutor executor = entry.getValue();
            if (now) {
                executor.shutdownNow();
            } else {
                executor.shutdown();
            }
            pools.clear();
        }
    }
}
