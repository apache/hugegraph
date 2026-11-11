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

package org.apache.hugegraph.task;

import java.util.ArrayList;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.hugegraph.exception.HugeException;
import org.apache.hugegraph.HugeGraphParams;
import org.apache.hugegraph.concurrent.PausableScheduledThreadPool;
import org.apache.hugegraph.util.Consumers;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.ExecutorUtil;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

/**
 * Central task management system that coordinates task scheduling and execution.
 * Manages the task schedulers of each graph and the executors they share.
 */
public final class TaskManager {

    private static final Logger LOG = Log.logger(TaskManager.class);

    public static final String TASK_WORKER_PREFIX = "task-worker";
    public static final String TASK_WORKER = TASK_WORKER_PREFIX + "-%d";
    public static final String TASK_DB_WORKER = "task-db-worker-%d";

    public static final String OLAP_TASK_WORKER = "olap-task-worker-%d";
    public static final String SCHEMA_TASK_WORKER = "schema-task-worker-%d";
    public static final String EPHEMERAL_TASK_WORKER = "ephemeral-task-worker-%d";
    public static final String DISTRIBUTED_TASK_SCHEDULER = "distributed-scheduler-%d";

    static final long SCHEDULE_PERIOD = 1000L; // unit ms
    private static final long TX_CLOSE_TIMEOUT = 30L; // unit s
    private static final int THREADS = 4;
    private static final TaskManager MANAGER = new TaskManager(THREADS);

    private final Map<HugeGraphParams, TaskScheduler> schedulers;

    private final ExecutorService taskExecutor;
    private final ExecutorService taskDbExecutor;

    private final ExecutorService schemaTaskExecutor;
    private final ExecutorService olapTaskExecutor;
    private final ExecutorService ephemeralTaskExecutor;
    private final PausableScheduledThreadPool distributedSchedulerExecutor;

    public static TaskManager instance() {
        return MANAGER;
    }

    private TaskManager(int pool) {
        this.schedulers = new ConcurrentHashMap<>();

        // For execute tasks
        this.taskExecutor = ExecutorUtil.newFixedThreadPool(pool, TASK_WORKER);
        // For save/query task state, just one thread is ok
        this.taskDbExecutor = ExecutorUtil.newFixedThreadPool(
                1, TASK_DB_WORKER);

        this.schemaTaskExecutor = ExecutorUtil.newFixedThreadPool(pool, SCHEMA_TASK_WORKER);
        this.olapTaskExecutor = ExecutorUtil.newFixedThreadPool(pool, OLAP_TASK_WORKER);
        this.ephemeralTaskExecutor = ExecutorUtil.newFixedThreadPool(pool, EPHEMERAL_TASK_WORKER);
        this.distributedSchedulerExecutor =
                ExecutorUtil.newPausableScheduledThreadPool(1, DISTRIBUTED_TASK_SCHEDULER);
    }

    public void addScheduler(HugeGraphParams graph) {
        E.checkArgumentNotNull(graph, "The graph can't be null");
        LOG.info("Use {} as the scheduler of graph ({})",
                 graph.schedulerType(), graph.name());
        // TODO: If the current service is bound to a specified non-DEFAULT graph space, the
        //  graph outside of the current graph space will no longer create task schedulers (graph
        //  space)
        switch (graph.schedulerType()) {
            case "distributed": {
                TaskScheduler scheduler =
                        new DistributedTaskScheduler(
                                graph,
                                distributedSchedulerExecutor,
                                taskDbExecutor,
                                schemaTaskExecutor,
                                olapTaskExecutor,
                                taskExecutor, /* gremlinTaskExecutor */
                                ephemeralTaskExecutor);
                this.schedulers.put(graph, scheduler);
                break;
            }
            case "local":
            default: {
                TaskScheduler scheduler =
                        new StandardTaskScheduler(
                                graph,
                                this.taskExecutor,
                                this.taskDbExecutor);
                this.schedulers.put(graph, scheduler);
                break;
            }
        }
    }

    public void closeScheduler(HugeGraphParams graph) {
        TaskScheduler scheduler = this.schedulers.get(graph);
        Throwable failure = null;
        boolean drained = scheduler == null;
        if (scheduler != null) {
            try {
                // Distributed close waits for cron without holding its monitor.
                if (scheduler.close() && scheduler.pendingTasks() == 0) {
                    this.schedulers.remove(graph, scheduler);
                    drained = true;
                }
            } catch (RuntimeException | Error error) {
                failure = error;
            }
        }
        if (!drained && (failure == null || scheduler.pendingTasks() != 0)) {
            // A running cron/job must finish before owner callbacks are queued
            // on its executor. In particular, do not rejoin a timed-out cron.
            if (failure instanceof Error matchedError) {
                throw matchedError;
            }
            if (failure != null) {
                throw (RuntimeException) failure;
            }
            return;
        }
        // An owner-close failure retains the scheduler, but idle workers can still clean up.
        for (Runnable close : new Runnable[]{() -> {
            if (!this.taskExecutor.isTerminated()) {
                this.closeTaskTx(graph);
            }
        }, () -> {
            if (!this.distributedSchedulerExecutor.isTerminated()) {
                this.closeDistributedSchedulerTx(graph);
            }
        }}) {
            try {
                close.run();
            } catch (RuntimeException | Error error) {
                if (failure == null) {
                    failure = error;
                } else if (failure != error) {
                    failure.addSuppressed(error);
                }
            }
        }
        if (failure instanceof Error matchedError) {
            throw matchedError;
        }
        if (failure != null) {
            throw (RuntimeException) failure;
        }
    }

    public void forceRemoveScheduler(HugeGraphParams params) {
        this.schedulers.remove(params);
    }

    private void closeTaskTx(HugeGraphParams graph) {
        final boolean selfIsTaskWorker = Thread.currentThread().getName()
                                               .startsWith(TASK_WORKER_PREFIX);
        final int totalThreads = selfIsTaskWorker ? THREADS - 1 : THREADS;
        Queue<Throwable> failures = new ConcurrentLinkedQueue<>();
        Runnable close = () -> {
            try {
                graph.closeTx();
            } catch (RuntimeException | Error error) {
                // invokeAll() does not inspect worker Futures. Capture failures
                // here so each worker still completes its execution accounting.
                failures.add(error);
                LOG.error("Failed to close task tx in thread '{}'", Thread.currentThread().getName(), error);
            }
        };
        try {
            if (selfIsTaskWorker) {
                // Call closeTx directly if myself is task thread(ignore others)
                close.run();
            } else {
                Consumers.executeOncePerThread(this.taskExecutor, totalThreads,
                                               close, TX_CLOSE_TIMEOUT);
            }
        } catch (Exception error) {
            failures.add(error);
            if (error instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
        }
        Throwable failure = failures.poll();
        if (failure != null) {
            for (Throwable error : failures) {
                if (error != failure) {
                    failure.addSuppressed(error);
                }
            }
            if (failure instanceof Error matchedError) {
                throw matchedError;
            }
            throw new HugeException("Exception when closing task tx", failure);
        }
    }

    private void closeDistributedSchedulerTx(HugeGraphParams graph) {
        final Callable<Void> closeTx = () -> {
            // Do close-tx for the current thread
            graph.closeTx();
            // Let other threads run
            Thread.yield();
            return null;
        };
        try {
            this.distributedSchedulerExecutor.submit(closeTx).get();
        } catch (Exception e) {
            throw new HugeException("Exception when closing scheduler tx", e);
        }
    }

    public TaskScheduler getScheduler(HugeGraphParams graph) {
        return this.schedulers.get(graph);
    }

    public ServerInfoManager getServerInfoManager(HugeGraphParams graph) {
        TaskScheduler scheduler = this.getScheduler(graph);
        if (scheduler == null) {
            return null;
        }
        return scheduler.serverManager();
    }

    public void shutdown(long timeout) {
        if (!this.schedulers.isEmpty()) {
            LOG.warn("Stopping task executors with {} schedulers still registered", this.schedulers.size());
        }

        try {
            shutdownExecutor(this.distributedSchedulerExecutor, timeout);
            shutdownExecutor(this.taskExecutor, timeout);
            // Early REST/Gremlin closes can retain graphs while tasks finish.
            // Their DB owners must close before the owner executor stops.
            this.closeRetainedGraphs();
            shutdownExecutor(this.taskDbExecutor, timeout);
            shutdownExecutor(this.ephemeralTaskExecutor, timeout);
            shutdownExecutor(this.schemaTaskExecutor, timeout);
            shutdownExecutor(this.olapTaskExecutor, timeout);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new HugeException("Failed to wait for TaskScheduler", e);
        } catch (TimeoutException e) {
            throw new HugeException("Failed to wait for TaskScheduler", e);
        }
    }

    private static void shutdownExecutor(ExecutorService executor, long timeout)
                                         throws InterruptedException, TimeoutException {
        if (!executor.isShutdown()) {
            executor.shutdown();
        }
        if (!executor.isTerminated() && !executor.awaitTermination(timeout, TimeUnit.SECONDS)) {
            throw new TimeoutException(timeout + "s");
        }
    }

    private void closeRetainedGraphs() {
        Throwable failure = null;
        for (HugeGraphParams graph : new ArrayList<>(this.schedulers.keySet())) {
            try {
                graph.graph().close();
            } catch (Exception | Error error) {
                if (failure == null) {
                    failure = error;
                } else if (failure != error) {
                    failure.addSuppressed(error);
                }
            }
        }
        if (failure != null) {
            // Keep the registry and DB executor available for an explicit retry.
            throw new HugeException("Failed to close retained graphs before task DB shutdown", failure);
        }
    }

    public int workerPoolSize() {
        return ((ThreadPoolExecutor) this.taskExecutor).getCorePoolSize();
    }

    public int pendingTasks() {
        int size = 0;
        for (TaskScheduler scheduler : this.schedulers.values()) {
            size += scheduler.pendingTasks();
        }
        return size;
    }

    private static final ThreadLocal<String> CONTEXTS = new ThreadLocal<>();

    public static void setContext(String context) {
        CONTEXTS.set(context);
    }

    public static void resetContext() {
        CONTEXTS.remove();
    }

    public static String getContext() {
        return CONTEXTS.get();
    }

    public static class ContextCallable<V> implements Callable<V> {

        private final Callable<V> callable;
        private final String context;

        public ContextCallable(Callable<V> callable) {
            E.checkNotNull(callable, "callable");
            this.context = getContext();
            this.callable = callable;
        }

        @Override
        public V call() throws Exception {
            setContext(this.context);
            try {
                return this.callable.call();
            } finally {
                resetContext();
            }
        }
    }
}
