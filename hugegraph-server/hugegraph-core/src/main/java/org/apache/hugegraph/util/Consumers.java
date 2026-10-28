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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.apache.hugegraph.exception.HugeException;
import org.apache.hugegraph.config.CoreOptions;
import org.apache.hugegraph.task.TaskManager.ContextCallable;
import org.slf4j.Logger;

public final class Consumers<V> {

    public static final int THREADS = 4 + CoreOptions.CPUS / 4;
    public static final int QUEUE_WORKER_SIZE = 1000;
    public static final long CONSUMER_WAKE_PERIOD = 1;

    private static final Logger LOG = Log.logger(Consumers.class);

    private final V QUEUE_END = (V) new Object();
    private final ExecutorService executor;
    private final Consumer<V> consumer;
    private final Runnable doneHandle;
    private final Consumer<Throwable> exceptionHandle;
    private final int workers;
    private final List<Future> runningFutures;
    private final int queueSize;
    private final CountDownLatch latch;
    private final BlockingQueue<V> queue;
    private volatile Throwable exception = null;
    private volatile boolean stopped;

    public Consumers(ExecutorService executor, Consumer<V> consumer) {
        this(executor, consumer, null);
    }

    public Consumers(ExecutorService executor,
                     Consumer<V> consumer, Runnable doneHandle) {
        this(executor, consumer, doneHandle, QUEUE_WORKER_SIZE);
    }

    public Consumers(ExecutorService executor,
                     Consumer<V> consumer,
                     Runnable doneHandle,
                     int queueSizePerWorker) {
        this(executor, consumer, doneHandle, null, queueSizePerWorker);
    }

    public Consumers(ExecutorService executor,
                     Consumer<V> consumer,
                     Runnable doneHandle,
                     Consumer<Throwable> exceptionHandle,
                     int queueSizePerWorker) {
        this.executor = executor;
        this.consumer = consumer;
        this.doneHandle = doneHandle;
        this.exceptionHandle = exceptionHandle;

        int workers = THREADS;
        if (this.executor instanceof ThreadPoolExecutor) {
            workers = ((ThreadPoolExecutor) this.executor).getCorePoolSize();
        }
        this.workers = workers;

        this.runningFutures = new ArrayList<>(workers);
        this.queueSize = queueSizePerWorker * workers + 1;
        this.latch = new CountDownLatch(workers);
        this.queue = new ArrayBlockingQueue<>(this.queueSize);
    }

    public void start(String name) {
        this.exception = null;
        this.stopped = false;
        if (this.executor == null) {
            return;
        }

        LOG.info("Starting {} workers[{}] with queue size {}...",
                 this.workers, name, this.queueSize);

        for (int i = 0; i < this.workers; i++) {
            // capture submission thread context HERE
            ContextCallable<Void> task = new ContextCallable<>(this::runAndDone);

            // wrapper ensures latch always decremented even if ContextCallable fails
            this.runningFutures.add(this.executor.submit(() -> this.safeRun(task)));
        }
    }

    private Void safeRun(ContextCallable<Void> task) {
        try {
            return task.call(); // may fail before/after runAndDone()
        } catch (Throwable e) {
            // This exception is from ContextCallable wrapper (setContext/resetContext/delegate dispatch),
            // not from runAndDone() business logic (that one is handled inside runAndDone()).
            if (this.recordException(e)) {
                LOG.error("Consumer worker failed in ContextCallable wrapper", e);
            } else {
                LOG.warn("Additional worker failure in ContextCallable wrapper; first exception already recorded", e);
            }
            this.stop();
            this.exceptionHandle(e);
        } finally {
            this.latch.countDown();
        }
        return null;
    }

    private Void runAndDone() {
        try {
            this.run();
        } catch (Throwable e) {
            if (e instanceof StopExecution) {
                Throwable[] suppressed = e.getSuppressed();
                // Stop stays control flow, but failed cleanup must reach the caller.
                for (Throwable failure : suppressed) {
                    this.recordException(failure);
                }
            } else {
                if (this.recordException(e)) {
                    LOG.error("Unhandled exception in consumer task", e);
                } else {
                    LOG.warn("Additional exception in consumer task; first exception already recorded", e);
                }
            }
            this.stop();
            this.exceptionHandle(e);
        } finally {
            this.done();
        }
        return null;
    }

    private void stop() {
        this.stopped = true;
        this.queue.clear();
    }

    private void run() {
        LOG.debug("Start to work...");
        while (this.consume()) {
            // ignore
        }

        LOG.debug("Worker finished");
    }

    private boolean consume() {
        V elem = null;
        while (elem == null) {
            if (this.stopped) {
                return false;
            }
            try {
                elem = this.queue.poll(CONSUMER_WAKE_PERIOD, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                // ignore
                return false;
            }
        }

        if (this.stopped) {
            return false;
        }
        if (elem == QUEUE_END) {
            putQueueEnd();
            return false;
        }
        // do job
        this.consumer.accept(elem);
        return true;
    }

    private void exceptionHandle(Throwable e) {
        if (this.exceptionHandle == null) {
            return;
        }

        try {
            this.exceptionHandle.accept(e);
        } catch (Throwable ex) {
            if (!this.recordException(ex)) {
                LOG.warn("Error while calling exceptionHandle()", ex);
            }
        }
    }

    private void done() {
        if (this.doneHandle == null) {
            return;
        }

        try {
            this.doneHandle.run();
        } catch (Throwable e) {
            if (!this.recordException(e)) {
                LOG.warn("Error while calling done()", e);
            }
            this.stop();
        }
    }

    private synchronized boolean recordException(Throwable failure) {
        if (this.exception == null) {
            this.exception = failure;
            return true;
        }
        if (this.exception != failure) {
            for (Throwable suppressed : this.exception.getSuppressed()) {
                if (suppressed == failure) {
                    return false;
                }
            }
            this.exception.addSuppressed(failure);
        }
        return false;
    }

    private Throwable throwException() {
        assert this.exception != null;
        return this.exception;
    }

    public void provide(V v) throws Throwable {
        if (this.executor == null) {
            assert this.exception == null;
            // do job directly if without thread pool
            this.consumer.accept(v);
        } else if (this.exception != null) {
            throw this.throwException();
        } else {
            try {
                while (!this.stopped &&
                       !this.queue.offer(v, CONSUMER_WAKE_PERIOD, TimeUnit.MILLISECONDS)) {
                    // A stopped worker must not leave a producer blocked on a full queue.
                }
                if (this.stopped) {
                    this.queue.clear();
                    if (this.exception != null) {
                        throw this.throwException();
                    }
                    throw new StopExecution("Consumer stopped");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw e;
            }
        }
    }

    private void putQueueEnd() {
        if (this.executor != null) {
            try {
                while (!this.stopped &&
                       !this.queue.offer(QUEUE_END, CONSUMER_WAKE_PERIOD, TimeUnit.MILLISECONDS)) {
                    // Stop wakes consumers through its state, without another END token.
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOG.warn("Interrupted while enqueue", e);
            }
        }
    }

    public void await() throws Throwable {
        if (this.executor == null) {
            // call done() directly if without thread pool
            this.done();
        } else {
            try {
                putQueueEnd();
                this.latch.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                String error = "Interrupted while waiting for consumers";
                for (Future f : this.runningFutures) {
                    f.cancel(true);
                }
                this.recordException(new HugeException(error, e));
                this.stop();
                LOG.warn(error, e);
            }
        }

        if (this.exception != null) {
            throw this.throwException();
        }
    }

    public ExecutorService executor() {
        return this.executor;
    }

    public static void executeOncePerThread(ExecutorService executor,
                                            int totalThreads,
                                            Runnable callback,
                                            long invokeTimeout)
            throws InterruptedException {
        // Ensure callback execute at least once for every thread
        final Map<Thread, Integer> threadsTimes = new ConcurrentHashMap<>();
        final List<Callable<Void>> tasks = new ArrayList<>();
        final Callable<Void> task = () -> {
            Thread current = Thread.currentThread();
            threadsTimes.putIfAbsent(current, 0);
            int times = threadsTimes.get(current);
            if (times == 0) {
                callback.run();
                // Let other threads run
                Thread.yield();
            } else {
                assert times < totalThreads;
                assert threadsTimes.size() < totalThreads;
                E.checkState(tasks.size() == totalThreads,
                             "Bad tasks size: %s", tasks.size());
                // Let another thread run and wait for it
                executor.submit(tasks.get(0)).get();
            }
            threadsTimes.put(current, ++times);
            return null;
        };

        // NOTE: expect each task thread to perform a close operation
        for (int i = 0; i < totalThreads; i++) {
            tasks.add(task);
        }
        executor.invokeAll(tasks, invokeTimeout, TimeUnit.SECONDS);
    }

    public static ExecutorService newThreadPool(String prefix, int workers) {
        if (workers == 0) {
            return null;
        } else {
            if (workers < 0) {
                assert workers == -1;
                workers = Consumers.THREADS;
            } else if (workers > CoreOptions.CPUS * 2) {
                workers = CoreOptions.CPUS * 2;
            }
            String name = prefix + "-worker-%d";
            return ExecutorUtil.newFixedThreadPool(workers, name);
        }
    }

    public static ExecutorPool newExecutorPool(String prefix, int workers) {
        return new ExecutorPool(prefix, workers);
    }

    public static RuntimeException wrapException(Throwable e) {
        if (e instanceof RuntimeException runtimeException) {
            throw runtimeException;
        }
        throw new HugeException("Error when running task: %s", e,
                                HugeException.rootCause(e).getMessage());
    }

    public static class ExecutorPool {

        private static final int POOL_CAPACITY = 2 * CoreOptions.CPUS;

        private final String threadNamePrefix;
        private final int executorWorkers;
        private final AtomicInteger count;

        private final Queue<ExecutorService> executors;

        public ExecutorPool(String prefix, int workers) {
            this.threadNamePrefix = prefix;
            this.executorWorkers = workers;
            this.count = new AtomicInteger();
            this.executors = new ArrayBlockingQueue<>(POOL_CAPACITY);
        }

        public synchronized ExecutorService getExecutor() {
            ExecutorService executor = this.executors.poll();
            if (executor == null) {
                int count = this.count.incrementAndGet();
                String prefix = this.threadNamePrefix + "-" + count;
                executor = newThreadPool(prefix, this.executorWorkers);
            }
            return executor;
        }

        public synchronized void returnExecutor(ExecutorService executor) {
            E.checkNotNull(executor, "executor");
            if (!this.executors.offer(executor)) {
                try {
                    executor.shutdown();
                } catch (Exception e) {
                    LOG.warn("close ExecutorService with error:", e);
                }
            }
        }

        public synchronized void destroy() {
            for (ExecutorService executor : this.executors) {
                try {
                    executor.shutdownNow();
                } catch (Exception e) {
                    LOG.warn("close ExecutorService with error:", e);
                }
            }
            this.executors.clear();
        }
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
