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

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.hugegraph.HugeException;
import org.apache.hugegraph.HugeGraphParams;
import org.apache.hugegraph.concurrent.PausableScheduledThreadPool;
import org.apache.hugegraph.util.Consumers;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.ExecutorUtil;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

/**
 * Central task management system that coordinates task scheduling and execution.
 * Manages task schedulers for different graphs and handles role-based execution.
 * <p>
 * Note: The local master-worker mechanism will be deprecated in version 1.7
 * (configuration has been removed from config files).
 */
public final class TaskManager {

    private static final Logger LOG = Log.logger(TaskManager.class);

    public static final String TASK_WORKER_PREFIX = "task-worker";
    public static final String TASK_WORKER = TASK_WORKER_PREFIX + "-%d";
    public static final String TASK_DB_WORKER = "task-db-worker-%d";
    public static final String SERVER_INFO_DB_WORKER = "server-info-db-worker-%d";
    public static final String TASK_SCHEDULER = "task-scheduler-%d";

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
    private final ExecutorService serverInfoDbExecutor;
    private final PausableScheduledThreadPool schedulerExecutor;

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
        this.serverInfoDbExecutor = ExecutorUtil.newFixedThreadPool(
                1, SERVER_INFO_DB_WORKER);

        this.schemaTaskExecutor = ExecutorUtil.newFixedThreadPool(pool, SCHEMA_TASK_WORKER);
        this.olapTaskExecutor = ExecutorUtil.newFixedThreadPool(pool, OLAP_TASK_WORKER);
        this.ephemeralTaskExecutor = ExecutorUtil.newFixedThreadPool(pool, EPHEMERAL_TASK_WORKER);
        this.distributedSchedulerExecutor =
                ExecutorUtil.newPausableScheduledThreadPool(1, DISTRIBUTED_TASK_SCHEDULER);

        // For a schedule task to run, just one thread is ok
        this.schedulerExecutor = ExecutorUtil.newPausableScheduledThreadPool(
                1, TASK_SCHEDULER);
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
                                ephemeralTaskExecutor,
                                serverInfoDbExecutor);
                this.schedulers.put(graph, scheduler);
                break;
            }
            case "local":
            default: {
                TaskScheduler scheduler =
                        new StandardTaskScheduler(
                                graph,
                                this.taskExecutor,
                                this.taskDbExecutor,
                                this.serverInfoDbExecutor);
                this.schedulers.put(graph, scheduler);
                break;
            }
        }
    }

    public void closeScheduler(HugeGraphParams graph) {
        TaskScheduler scheduler = this.schedulers.get(graph);
        if (scheduler != null) {
            /*
             * Synch close+remove scheduler and iterate scheduler, details:
             * 'closeScheduler' should sync with 'scheduleOrExecuteJob'.
             * Because 'closeScheduler' will be called by 'graph.close()' in
             * main thread and there is gap between 'scheduler.close()'
             * (will close graph tx) and 'this.schedulers.remove(graph)'.
             * In this gap 'scheduleOrExecuteJob' may be run in
             * scheduler-db-thread and 'scheduleOrExecuteJob' will reopen
             * graph tx. As a result, graph tx will mistakenly not be closed
             * after 'graph.close()'.
             */
            synchronized (scheduler) {
                if (scheduler.close()) {
                    this.schedulers.remove(graph);
                }
            }
        }

        if (!this.taskExecutor.isTerminated()) {
            this.closeTaskTx(graph);
        }

        if (!this.schedulerExecutor.isTerminated()) {
            this.closeSchedulerTx(graph);
        }

        if (!this.distributedSchedulerExecutor.isTerminated()) {
            this.closeDistributedSchedulerTx(graph);
        }
    }

    public void forceRemoveScheduler(HugeGraphParams params) {
        this.schedulers.remove(params);
    }

    private void closeTaskTx(HugeGraphParams graph) {
        final boolean selfIsTaskWorker = Thread.currentThread().getName()
                                               .startsWith(TASK_WORKER_PREFIX);
        final int totalThreads = selfIsTaskWorker ? THREADS - 1 : THREADS;
        try {
            if (selfIsTaskWorker) {
                // Call closeTx directly if myself is task thread(ignore others)
                graph.closeTx();
            } else {
                Consumers.executeOncePerThread(this.taskExecutor, totalThreads,
                                               graph::closeTx, TX_CLOSE_TIMEOUT);
            }
        } catch (Exception e) {
            throw new HugeException("Exception when closing task tx", e);
        }
    }

    private void closeSchedulerTx(HugeGraphParams graph) {
        final Callable<Void> closeTx = () -> {
            // Do close-tx for the current thread
            graph.closeTx();
            // Let other threads run
            Thread.yield();
            return null;
        };
        try {
            this.schedulerExecutor.submit(closeTx).get();
        } catch (Exception e) {
            throw new HugeException("Exception when closing scheduler tx", e);
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
        assert this.schedulers.isEmpty() : this.schedulers.size();

        Throwable ex = null;
        boolean terminated = this.schedulerExecutor.isTerminated();
        final TimeUnit unit = TimeUnit.SECONDS;

        if (!this.schedulerExecutor.isShutdown()) {
            this.schedulerExecutor.shutdown();
            try {
                terminated = this.schedulerExecutor.awaitTermination(timeout, unit);
            } catch (Throwable e) {
                ex = e;
            }
        }

        if (terminated && !this.distributedSchedulerExecutor.isShutdown()) {
            this.distributedSchedulerExecutor.shutdown();
            try {
                terminated = this.distributedSchedulerExecutor.awaitTermination(timeout, unit);
            } catch (Throwable e) {
                ex = e;
            }
        }

        if (terminated && !this.taskExecutor.isShutdown()) {
            this.taskExecutor.shutdown();
            try {
                terminated = this.taskExecutor.awaitTermination(timeout, unit);
            } catch (Throwable e) {
                ex = e;
            }
        }

        if (terminated && !this.serverInfoDbExecutor.isShutdown()) {
            this.serverInfoDbExecutor.shutdown();
            try {
                terminated = this.serverInfoDbExecutor.awaitTermination(timeout, unit);
            } catch (Throwable e) {
                ex = e;
            }
        }

        if (terminated && !this.taskDbExecutor.isShutdown()) {
            this.taskDbExecutor.shutdown();
            try {
                terminated = this.taskDbExecutor.awaitTermination(timeout, unit);
            } catch (Throwable e) {
                ex = e;
            }
        }

        if (terminated && !this.ephemeralTaskExecutor.isShutdown()) {
            this.ephemeralTaskExecutor.shutdown();
            try {
                terminated = this.ephemeralTaskExecutor.awaitTermination(timeout, unit);
            } catch (Throwable e) {
                ex = e;
            }
        }

        if (terminated && !this.schemaTaskExecutor.isShutdown()) {
            this.schemaTaskExecutor.shutdown();
            try {
                terminated = this.schemaTaskExecutor.awaitTermination(timeout, unit);
            } catch (Throwable e) {
                ex = e;
            }
        }

        if (terminated && !this.olapTaskExecutor.isShutdown()) {
            this.olapTaskExecutor.shutdown();
            try {
                terminated = this.olapTaskExecutor.awaitTermination(timeout, unit);
            } catch (Throwable e) {
                ex = e;
            }
        }

        if (!terminated) {
            ex = new TimeoutException(timeout + "s");
        }
        if (ex != null) {
            throw new HugeException("Failed to wait for TaskScheduler", ex);
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

    public void onAsRoleMaster() {
        // ServerInfo based role propagation is deprecated.
    }

    public void onAsRoleWorker() {
        // ServerInfo based role propagation is deprecated.
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
