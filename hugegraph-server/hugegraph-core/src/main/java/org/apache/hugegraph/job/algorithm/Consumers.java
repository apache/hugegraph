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

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.apache.hugegraph.exception.HugeException;
import org.apache.hugegraph.task.TaskManager.ContextCallable;
import org.apache.hugegraph.util.ExecutorUtil;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

public class Consumers<V> {

    public static final int CPUS = Runtime.getRuntime().availableProcessors();
    public static final int THREADS = 4 + CPUS / 4;
    public static final int QUEUE_WORKER_SIZE = 1000;

    private static final Logger LOG = Log.logger(Consumers.class);

    private final ExecutorService executor;
    private final Consumer<V> consumer;
    private final Runnable done;

    private final int workers;
    private final int queueSize;
    private final CountDownLatch latch;
    private final BlockingQueue<V> queue;

    private volatile boolean ending = false;
    private volatile Throwable exception = null;

    public Consumers(ExecutorService executor, Consumer<V> consumer) {
        this(executor, consumer, null);
    }

    public Consumers(ExecutorService executor,
                     Consumer<V> consumer, Runnable done) {
        this.executor = executor;
        this.consumer = consumer;
        this.done = done;

        int workers = THREADS;
        if (this.executor instanceof ThreadPoolExecutor) {
            workers = ((ThreadPoolExecutor) this.executor).getCorePoolSize();
        }
        this.workers = workers;
        this.queueSize = QUEUE_WORKER_SIZE * workers;
        this.latch = new CountDownLatch(workers);
        this.queue = new ArrayBlockingQueue<>(this.queueSize);
    }

    public void start(String name) {
        this.ending = false;
        this.exception = null;
        if (this.executor == null) {
            return;
        }
        LOG.info("Starting {} workers[{}] with queue size {}...",
                 this.workers, name, this.queueSize);
        for (int i = 0; i < this.workers; i++) {
            this.executor.submit(new ContextCallable<>(this::runAndDone));
        }
    }

    private Void runAndDone() {
        try {
            this.run();
            this.done();
        } catch (Throwable e) {
            // Only the first exception to one thread can be stored
            this.exception = e;
            if (!(e instanceof StopExecution)) {
                LOG.error("Error when running task", e);
            }
            this.done();
        } finally {
            this.latch.countDown();
        }
        return null;
    }

    private void run() {
        LOG.debug("Start to work...");
        while (!this.ending) {
            this.consume();
        }
        assert this.ending;
        while (this.consume()) {
        }

        LOG.debug("Worker finished");
    }

    private boolean consume() {
        V elem;
        try {
            elem = this.queue.poll(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            // ignore
            return true;
        }
        if (elem == null) {
            return false;
        }
        // do job
        this.consumer.accept(elem);
        return true;
    }

    private void done() {
        if (this.done != null) {
            this.done.run();
        }
    }

    public void provide(V v) throws Throwable {
        if (this.executor == null) {
            assert this.exception == null;
            // do job directly if without thread pool
            this.consumer.accept(v);
        } else if (this.exception != null) {
            throw this.exception;
        } else {
            try {
                this.queue.put(v);
            } catch (InterruptedException e) {
                LOG.warn("Interrupted", e);
            }
        }
    }

    public void await() {
        this.ending = true;
        if (this.executor == null) {
            // call done() directly if without thread pool
            this.done();
        } else {
            try {
                this.latch.await();
            } catch (InterruptedException e) {
                LOG.warn("Interrupted", e);
            }
        }
    }

    public static ExecutorService newThreadPool(String prefix, int workers) {
        if (workers == 0) {
            return null;
        } else {
            if (workers < 0) {
                assert workers == -1;
                workers = Consumers.THREADS;
            } else if (workers > Consumers.CPUS * 2) {
                workers = Consumers.CPUS * 2;
            }
            String name = prefix + "-worker-%d";
            return ExecutorUtil.newFixedThreadPool(workers, name);
        }
    }

    public static RuntimeException wrapException(Throwable e) {
        if (e instanceof RuntimeException) {
            throw (RuntimeException) e;
        }
        throw new HugeException("Error when running task: %s",
                                HugeException.rootCause(e).getMessage(), e);
    }

    public static class StopExecution extends HugeException {

        private static final long serialVersionUID = -371829356182454517L;

        public StopExecution(String message) {
            super(message);
        }

        public StopExecution(String message, Object... args) {
            super(message, args);
        }
    }
}
