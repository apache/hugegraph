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

package org.apache.hugegraph.store.client.util;

import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class ExecutorPool {

    public static ThreadFactory newThreadFactory(String namePrefix) {
        HgAssert.isArgumentNotNull(namePrefix, "namePrefix");
        return new DefaultThreadFactory(namePrefix);
    }

    public static ThreadPoolExecutor createExecutor(String name, long keepAliveTime,
                                                    int coreThreads, int maxThreads) {
        return createExecutor(name, keepAliveTime, coreThreads, maxThreads,
                              new ThreadPoolExecutor.CallerRunsPolicy());
    }

    public static ThreadPoolExecutor createExecutor(String name, long keepAliveTime,
                                                    int coreThreads, int maxThreads,
                                                    RejectedExecutionHandler handler) {
        return new ThreadPoolExecutor(coreThreads, maxThreads, keepAliveTime, TimeUnit.SECONDS,
                                      new SynchronousQueue<>(),
                                      newThreadFactory(name),
                                      handler
        );
    }

    public static class DefaultThreadFactory implements ThreadFactory {

        private final AtomicInteger threadNumber = new AtomicInteger(1);
        private final String namePrefix;

        public DefaultThreadFactory(String threadNamePrefix) {
            this.namePrefix = threadNamePrefix + "-";
        }

        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(null, r, namePrefix + threadNumber.getAndIncrement(), 0);
            t.setDaemon(true);
            t.setPriority(Thread.NORM_PRIORITY);
            return t;
        }
    }
}
