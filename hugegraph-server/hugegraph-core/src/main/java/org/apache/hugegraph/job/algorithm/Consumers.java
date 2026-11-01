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

package org.apache.hugegraph.job.algorithm;

import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

public class Consumers<V> {

    public static final int CPUS = Runtime.getRuntime().availableProcessors();
    public static final int THREADS = org.apache.hugegraph.util.Consumers.THREADS;
    public static final int QUEUE_WORKER_SIZE = org.apache.hugegraph.util.Consumers.QUEUE_WORKER_SIZE;

    private final org.apache.hugegraph.util.Consumers<V> delegate;

    public Consumers(ExecutorService executor, Consumer<V> consumer) {
        this(executor, consumer, null);
    }

    public Consumers(ExecutorService executor,
                     Consumer<V> consumer, Runnable done) {
        this.delegate = new org.apache.hugegraph.util.Consumers<>(executor, consumer, done);
    }

    public void start(String name) {
        this.delegate.start(name);
    }

    public void provide(V value) throws Throwable {
        try {
            this.delegate.provide(value);
        } catch (org.apache.hugegraph.util.Consumers.StopExecution e) {
            // Preserve the algorithm API's control-flow exception type.
            if (e instanceof StopExecution stop) {
                throw stop;
            }
            throw new StopExecution(e.getMessage());
        }
    }

    public void await() {
        try {
            this.delegate.await();
        } catch (Error error) {
            throw error;
        } catch (Throwable e) {
            throw wrapException(e);
        }
    }

    public static ExecutorService newThreadPool(String prefix, int workers) {
        return org.apache.hugegraph.util.Consumers.newThreadPool(prefix, workers);
    }

    public static RuntimeException wrapException(Throwable e) {
        return org.apache.hugegraph.util.Consumers.wrapException(e);
    }

    public static class StopExecution extends org.apache.hugegraph.util.Consumers.StopExecution {

        private static final long serialVersionUID = -371829356182454517L;

        public StopExecution(String message) {
            super(message);
        }

        public StopExecution(String message, Object... args) {
            super(message, args);
        }
    }
}
