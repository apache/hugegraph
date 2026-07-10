/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hugegraph.task;

import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hugegraph.HugeException;
import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.HugeGraphParams;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.query.QueryResults;
import org.apache.hugegraph.config.CoreOptions;
import org.apache.hugegraph.exception.ConnectionException;
import org.apache.hugegraph.exception.NotFoundException;
import org.apache.hugegraph.meta.MetaManager;
import org.apache.hugegraph.meta.lock.LockResult;
import org.apache.hugegraph.structure.HugeVertex;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.LockUtil;
import org.apache.hugegraph.util.Log;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;

public class DistributedTaskScheduler extends TaskAndResultScheduler {

    private static final Logger LOG = Log.logger(DistributedTaskScheduler.class);
    private final long schedulePeriod;
    private final ExecutorService taskDbExecutor;
    private final ExecutorService schemaTaskExecutor;
    private final ExecutorService olapTaskExecutor;
    private final ExecutorService ephemeralTaskExecutor;
    private final ExecutorService gremlinTaskExecutor;
    private final ScheduledThreadPoolExecutor schedulerExecutor;
    private final ScheduledFuture<?> cronFuture;

    /**
     * the status of scheduler
     */
    private final AtomicBoolean closed = new AtomicBoolean(true);

    private final ConcurrentHashMap<Id, HugeTask<?>> runningTasks = new ConcurrentHashMap<>();
    private final Set<Id> deletingTasks = ConcurrentHashMap.newKeySet();

    public DistributedTaskScheduler(HugeGraphParams graph,
                                    ScheduledThreadPoolExecutor schedulerExecutor,
                                    ExecutorService taskDbExecutor,
                                    ExecutorService schemaTaskExecutor,
                                    ExecutorService olapTaskExecutor,
                                    ExecutorService gremlinTaskExecutor,
                                    ExecutorService ephemeralTaskExecutor,
                                    ExecutorService serverInfoDbExecutor) {
        super(graph, serverInfoDbExecutor);

        this.taskDbExecutor = taskDbExecutor;
        this.schemaTaskExecutor = schemaTaskExecutor;
        this.olapTaskExecutor = olapTaskExecutor;
        this.gremlinTaskExecutor = gremlinTaskExecutor;
        this.ephemeralTaskExecutor = ephemeralTaskExecutor;

        this.schedulerExecutor = schedulerExecutor;

        this.closed.set(false);

        this.schedulePeriod = this.graph.configuration()
                                        .get(CoreOptions.TASK_SCHEDULE_PERIOD);

        this.cronFuture = this.schedulerExecutor.scheduleWithFixedDelay(
                () -> {
                    LockUtil.lock(this.graph().spaceGraphName(), LockUtil.GRAPH_LOCK);
                    try {
                        // TODO: Use super administrator privileges to query tasks.
                        // TaskManager.useAdmin();
                        this.cronSchedule();
                    } catch (Throwable t) {
                        LOG.info("cronScheduler exception graph: {}", this.spaceGraphName(), t);
                    } finally {
                        LockUtil.unlock(this.graph().spaceGraphName(), LockUtil.GRAPH_LOCK);
                    }
                },
                10L, schedulePeriod,
                TimeUnit.SECONDS);
    }

    private static boolean sleep(long ms) {
        try {
            Thread.sleep(ms);
            return true;
        } catch (InterruptedException ignored) {
            // Ignore InterruptedException
            return false;
        }
    }

    public void cronSchedule() {
        // Perform periodic scheduling tasks

        // Check closed flag first to exit early
        if (this.closed.get()) {
            return;
        }

        if (!this.graph.started() || this.graph.closed()) {
            return;
        }

        // Handle tasks in NEW status
        Iterator<HugeTask<Object>> news = queryTaskWithoutResultByStatus(
                TaskStatus.NEW);

        while (!this.closed.get() && news.hasNext()) {
            HugeTask<?> newTask = news.next();
            LOG.info("Try to start task({})@({}/{})", newTask.id(),
                     this.graphSpace, this.graphName);
            if (!tryStartHugeTask(newTask)) {
                // Task submission failed when the thread pool is full.
                break;
            }
        }

        // Handling tasks in RUNNING state
        Iterator<HugeTask<Object>> runnings =
                queryTaskWithoutResultByStatus(TaskStatus.RUNNING);

        while (!this.closed.get() && runnings.hasNext()) {
            HugeTask<?> running = runnings.next();
            initTaskParams(running);
            if (!isLockedTask(running.id().toString())) {
                LOG.info("Try to update task({})@({}/{}) status" +
                         "(RUNNING->FAILED)", running.id(), this.graphSpace,
                         this.graphName);
                if (updateStatusWithLock(running.id(), TaskStatus.RUNNING,
                                         TaskStatus.FAILED)) {
                    runningTasks.remove(running.id());
                } else {
                    LOG.warn("Update task({})@({}/{}) status" +
                             "(RUNNING->FAILED) failed",
                             running.id(), this.graphSpace, this.graphName);
                }
            }
        }

        // Handle tasks in FAILED/HANGING state
        Iterator<HugeTask<Object>> faileds =
                queryTaskWithoutResultByStatus(TaskStatus.FAILED);

        while (!this.closed.get() && faileds.hasNext()) {
            HugeTask<?> failed = faileds.next();
            initTaskParams(failed);
            if (failed.retries() < this.graph().option(CoreOptions.TASK_RETRY)) {
                LOG.info("Try to update task({})@({}/{}) status(FAILED->NEW)",
                         failed.id(), this.graphSpace, this.graphName);
                updateStatusWithLock(failed.id(), TaskStatus.FAILED,
                                     TaskStatus.NEW);
            }
        }

        // Handling tasks in CANCELLING state
        Iterator<HugeTask<Object>> cancellings = queryTaskWithoutResultByStatus(
                TaskStatus.CANCELLING);

        while (!this.closed.get() && cancellings.hasNext()) {
            Id cancellingId = cancellings.next().id();
            HugeTask<?> cancelling = runningTasks.get(cancellingId);
            if (cancelling != null) {
                initTaskParams(cancelling);
                LOG.info("Try to cancel task({})@({}/{})",
                         cancelling.id(), this.graphSpace, this.graphName);
                if (!cancelling.cancel(true)) {
                    // Task already completed normally; force CANCELLED so
                    // it doesn't stay stuck in CANCELLING forever.
                    updateStatusWithLock(cancellingId, TaskStatus.CANCELLING,
                                         TaskStatus.CANCELLED);
                }
            } else {
                // Local no execution task, but the current task has no nodes executing.
                if (!isLockedTask(cancellingId.toString())) {
                    updateStatusWithLock(cancellingId, TaskStatus.CANCELLING,
                                         TaskStatus.CANCELLED);
                }
            }
        }

        // Handling tasks in DELETING status
        Iterator<HugeTask<Object>> deletings = queryTaskWithoutResultByStatus(
                TaskStatus.DELETING);

        while (!this.closed.get() && deletings.hasNext()) {
            Id deletingId = deletings.next().id();
            HugeTask<?> deleting = runningTasks.get(deletingId);
            if (deleting != null) {
                this.markTaskDeleting(deleting);
                deleting.cancel(true);
            } else {
                // Local has no task execution, but the current task has no nodes executing anymore.
                if (!isLockedTask(deletingId.toString())) {
                    deleteFromDB(deletingId);
                }
            }
        }
    }

    protected <V> Iterator<HugeTask<V>> queryTaskWithoutResultByStatus(TaskStatus status) {
        if (this.closed.get()) {
            return QueryResults.emptyIterator();
        }
        return queryTaskWithoutResult(HugeTask.P.STATUS, status.code(), NO_LIMIT, null);
    }

    @Override
    public HugeGraph graph() {
        return this.graph.graph();
    }

    @Override
    public int pendingTasks() {
        return this.runningTasks.size();
    }

    @Override
    public <V> void restoreTasks() {
        // DO Nothing!
    }

    @Override
    public <V> Future<?> schedule(HugeTask<V> task) {
        E.checkArgumentNotNull(task, "Task can't be null");

        initTaskParams(task);

        if (task.ephemeralTask()) {
            // Handle ephemeral tasks, no scheduling needed, execute directly
            return this.ephemeralTaskExecutor.submit(task);
        }

        // Validate task state before saving to ensure correct exception type
        E.checkState(task.type() != null, "Task type can't be null");
        E.checkState(task.name() != null, "Task name can't be null");

        // Process schema task
        // Handle gremlin task
        // Handle OLAP calculation tasks
        // Add task to DB, current task status is NEW
        // TODO: save server id for task
        this.save(task);

        if (!this.closed.get()) {
            LOG.info("Try to start task({})@({}/{}) immediately", task.id(),
                     this.graphSpace, this.graphName);
            tryStartHugeTask(task);
        } else {
            LOG.info("TaskScheduler has closed");
        }

        return null;
    }

    protected <V> void initTaskParams(HugeTask<V> task) {
        // Bind the environment variables required for the current task execution
        // Before task deserialization and execution, this method needs to be called.
        task.scheduler(this);
        TaskCallable<V> callable = task.callable();
        callable.task(task);
        callable.graph(this.graph());

        if (callable instanceof TaskCallable.SysTaskCallable) {
            ((TaskCallable.SysTaskCallable<?>) callable).params(this.graph);
        }
    }

    /**
     * Note: This method will update the status of the input task.
     *
     * @param task
     * @param <V>
     */
    @Override
    public <V> void cancel(HugeTask<V> task) {
        E.checkArgumentNotNull(task, "Task can't be null");

        if (task.completed() || task.cancelling()) {
            return;
        }

        LOG.info("Cancel task '{}' in status {}", task.id(), task.status());

        // Check if task is running locally, cancel it directly if so
        HugeTask<?> runningTask = this.runningTasks.get(task.id());
        if (runningTask != null) {
            boolean cancelled = runningTask.cancel(true);
            if (cancelled) {
                task.overwriteStatus(TaskStatus.CANCELLED);
                this.save(runningTask);
            }
            LOG.info("Cancel local running task '{}' result: {}", task.id(), cancelled);
            return;
        }

        // Task not running locally, update status to CANCELLING
        // for cronSchedule() or other nodes to handle
        TaskStatus currentStatus = task.status();
        if (this.updateStatus(task.id(), currentStatus, TaskStatus.CANCELLING)) {
            task.overwriteStatus(TaskStatus.CANCELLING);
        } else {
            // Status race: re-read from DB and retry with fresh status
            HugeTask<Object> reloaded;
            try {
                reloaded = this.taskWithoutResult(task.id());
            } catch (NotFoundException e) {
                LOG.info("Task '{}' already deleted, skip cancel", task.id());
                return;
            }
            TaskStatus stored = reloaded.status();
            if (stored != TaskStatus.CANCELLING &&
                !TaskStatus.COMPLETED_STATUSES.contains(stored)) {
                if (this.updateStatus(task.id(), stored, TaskStatus.CANCELLING)) {
                    task.overwriteStatus(TaskStatus.CANCELLING);
                    LOG.info("Retry cancel task '{}' succeeded (stored was {})",
                             task.id(), stored);
                } else {
                    LOG.warn("Failed to cancel task '{}', re-read status {} changed again",
                             task.id(), stored);
                }
            } else {
                LOG.info("Task '{}' already {}/terminal, skip cancel",
                         task.id(), stored);
            }
        }
    }

    @Override
    public void init() {
        this.call(() -> this.tx().initSchema());
    }

    protected <V> HugeTask<V> deleteFromDB(Id id) {
        // Delete Task from DB, without checking task status
        try {
            return this.call(() -> {
                Iterator<Vertex> vertices = this.tx().queryTaskInfos(id);
                HugeVertex vertex = (HugeVertex) QueryResults.one(vertices);
                if (vertex == null) {
                    this.deleteTaskResultFromTx(id);
                    return null;
                }
                HugeTask<V> result = HugeTask.fromVertex(vertex, false);
                // Keep the task vertex as a retryable tombstone until its result
                // vertex is removed; cronSchedule() can rediscover DELETING tasks.
                this.deleteTaskResultFromTx(id);
                this.tx().removeTaskVertex(vertex);
                return result;
            });
        } finally {
            if (!this.runningTasks.containsKey(id)) {
                this.deletingTasks.remove(id);
            }
        }
    }

    @Override
    public <V> void save(HugeTask<V> task) {
        E.checkArgumentNotNull(task, "Task can't be null");
        if (this.deletingTasks.contains(task.id())) {
            LOG.info("Skip saving task({})@({}/{}) because it is deleting",
                     task.id(), this.graphSpace, this.graphName);
            return;
        }

        String rawResult = task.result();
        Boolean saved = this.call(() -> {
            if (task.status() != TaskStatus.DELETING &&
                this.storedTaskDeleting(task.id())) {
                LOG.info("Skip saving task({})@({}/{}) because stored status " +
                         "is DELETING", task.id(), this.graphSpace,
                         this.graphName);
                return false;
            }
            HugeVertex vertex = this.tx().constructTaskVertex(task);
            this.tx().deleteIndex(vertex);
            this.tx().addVertex(vertex);
            return true;
        });

        if (!saved || rawResult == null) {
            return;
        }

        this.call(() -> {
            if (this.deletingTasks.contains(task.id()) ||
                !this.storedTaskAllowsResultSave(task.id())) {
                LOG.info("Skip saving task({}) result@({}/{}) because it is " +
                         "deleting or missing", task.id(), this.graphSpace,
                         this.graphName);
                return null;
            }
            HugeTaskResult result =
                    new HugeTaskResult(HugeTaskResult.genId(task.id()));
            result.result(rawResult);

            HugeVertex vertex = this.tx().constructTaskResultVertex(result);
            return this.tx().addVertex(vertex);
        });
    }

    private void markTaskDeleting(HugeTask<?> task) {
        this.deletingTasks.add(task.id());
        initTaskParams(task);
        HugeTask<?> deleting;
        synchronized (task) {
            task.overwriteStatus(TaskStatus.DELETING);
            deleting = task.copyWithoutResult();
        }
        this.saveTaskWithoutResult(deleting);
    }

    private boolean storedTaskDeleting(Id id) {
        Iterator<Vertex> vertices = this.tx().queryTaskInfos(id);
        Vertex vertex = QueryResults.one(vertices);
        if (vertex == null) {
            return false;
        }
        HugeTask<?> task = HugeTask.fromVertex(vertex, false);
        return task.status() == TaskStatus.DELETING;
    }

    private boolean storedTaskAllowsResultSave(Id id) {
        Iterator<Vertex> vertices = this.tx().queryTaskInfos(id);
        Vertex vertex = QueryResults.one(vertices);
        if (vertex == null) {
            return false;
        }
        HugeTask<?> task = HugeTask.fromVertex(vertex, false);
        return task.status() != TaskStatus.DELETING;
    }

    private void saveTaskWithoutResult(HugeTask<?> task) {
        this.call(() -> {
            HugeVertex vertex = this.tx().constructTaskVertex(task);
            this.tx().deleteIndex(vertex);
            return this.tx().addVertex(vertex);
        });
    }

    @Override
    public <V> HugeTask<V> delete(Id id, boolean force) {
        HugeTask<?> task = this.taskWithoutResult(id);
        HugeTask<?> running = this.runningTasks.get(id);

        if (running != null) {
            this.markTaskDeleting(running);
            running.cancel(true);
            @SuppressWarnings("unchecked")
            HugeTask<V> result = (HugeTask<V>) running;
            return result;
        }

        if (!force && !task.completed()) {
            // Can't safely mark a remotely running task without owning its
            // lock; the owner may otherwise overwrite DELETING on final save.
            LockResult lockResult = tryLockTask(id.asString());
            checkDeleteLock(id, lockResult);
            try {
                this.markTaskDeleting(task);
            } finally {
                unlockTask(id.asString(), lockResult);
            }
            @SuppressWarnings("unchecked")
            HugeTask<V> result = (HugeTask<V>) task;
            return result;
        }

        if (!task.completed()) {
            LockResult lockResult = tryLockTask(id.asString());
            checkDeleteLock(id, lockResult);
            try {
                if (task.status() != TaskStatus.DELETING) {
                    this.markTaskDeleting(task);
                }
                return this.deleteFromDB(id);
            } finally {
                unlockTask(id.asString(), lockResult);
            }
        }

        // Write DELETING status before attempting physical delete so that a
        // failed result deletion is recoverable via cronSchedule().
        if (task.status() != TaskStatus.DELETING) {
            this.markTaskDeleting(task);
        }

        return this.deleteFromDB(id);
    }

    private static void checkDeleteLock(Id id, LockResult lockResult) {
        E.checkState(lockResult.lockSuccess(),
                     "Can't delete task '%s' because it is locked by another " +
                     "server, please retry later", id);
    }

    @Override
    public boolean close() {
        if (this.closed.get()) {
            return true;
        }

        // set closed
        this.closed.set(true);

        // cancel cron thread
        if (!cronFuture.isDone() && !cronFuture.isCancelled()) {
            cronFuture.cancel(false);
        }

        // Wait behind the scheduler thread to ensure any running cron task is completed
        try {
            Future<?> barrier = this.schedulerExecutor.submit(() -> {
                // pass
            });
            barrier.get(schedulePeriod + 5, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            LOG.warn("Cron task did not complete in time when closing scheduler");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.warn("Interrupted while waiting for cron task to complete", e);
        } catch (ExecutionException e) {
            LOG.warn("Exception while waiting for cron task to complete", e);
        }

        // cancel all running tasks
        for (HugeTask<?> task : this.runningTasks.values()) {
            LOG.info("cancel task({}) @({}/{}) when closing scheduler",
                     task.id(), graphSpace, graphName);
            this.cancel(task);
        }

        try {
            this.waitUntilAllTasksCompleted(10);
        } catch (TimeoutException e) {
            LOG.warn("Tasks not completed when close distributed task scheduler", e);
            return false;
        }

        if (!this.taskDbExecutor.isShutdown()) {
            this.call(() -> {
                try {
                    this.tx().close();
                } catch (ConnectionException ignored) {
                    // ConnectionException means no connection established
                }
                this.graph.closeTx();
            });
        }

        // TODO: serverInfoManager section should be removed in the future.
        return this.serverManager().close();
    }

    @Override
    public <V> HugeTask<V> waitUntilTaskCompleted(Id id, long seconds)
            throws TimeoutException {
        return this.waitUntilTaskCompleted(id, seconds, QUERY_INTERVAL);
    }

    @Override
    public <V> HugeTask<V> waitUntilTaskCompleted(Id id)
            throws TimeoutException {
        // This method is just used by tests
        long timeout = this.graph.configuration()
                                 .get(CoreOptions.TASK_WAIT_TIMEOUT);
        return this.waitUntilTaskCompleted(id, timeout, 1L);
    }

    private <V> HugeTask<V> waitUntilTaskCompleted(Id id, long seconds,
                                                   long intervalMs)
            throws TimeoutException {
        long passes = seconds * 1000 / intervalMs;
        HugeTask<V> task = null;
        for (long pass = 0; ; pass++) {
            try {
                task = this.taskWithoutResult(id);
            } catch (NotFoundException e) {
                if (task != null && task.completed()) {
                    assert task.id().asLong() < 0L : task.id();
                    sleep(intervalMs);
                    return task;
                }
                throw e;
            }
            if (task.completed()) {
                // Wait for task result being set after status is completed
                sleep(intervalMs);
                // Query task information with results
                task = this.task(id);
                return task;
            }
            if (pass >= passes) {
                break;
            }
            sleep(intervalMs);
        }
        throw new TimeoutException(String.format(
                "Task '%s' was not completed in %s seconds", id, seconds));
    }

    @Override
    public void waitUntilAllTasksCompleted(long seconds)
            throws TimeoutException {
        long passes = seconds * 1000 / QUERY_INTERVAL;
        int taskSize = 0;
        for (long pass = 0; ; pass++) {
            taskSize = this.pendingTasks();
            if (taskSize == 0) {
                sleep(QUERY_INTERVAL);
                return;
            }
            if (pass >= passes) {
                break;
            }
            sleep(QUERY_INTERVAL);
        }
        throw new TimeoutException(String.format(
                "There are still %s incomplete tasks after %s seconds",
                taskSize, seconds));

    }

    @Override
    public void checkRequirement(String op) {
        // Distributed scheduler uses task locks to coordinate workers.
    }

    @Override
    public <V> V call(Callable<V> callable) {
        return this.call(callable, this.taskDbExecutor);
    }

    @Override
    public <V> V call(Runnable runnable) {
        return this.call(Executors.callable(runnable, null));
    }

    private <V> V call(Callable<V> callable, ExecutorService executor) {
        try {
            callable = new TaskManager.ContextCallable<>(callable);
            return executor.submit(callable).get();
        } catch (Exception e) {
            throw new HugeException("Failed to update/query TaskStore for " +
                                    "graph(%s/%s): %s", e, this.graphSpace,
                                    this.graph.spaceGraphName(), e.toString());
        }
    }

    protected boolean updateStatus(Id id, TaskStatus prestatus,
                                   TaskStatus status) {
        HugeTask<Object> task = this.taskWithoutResult(id);
        initTaskParams(task);
        if (prestatus == null || task.status() == prestatus) {
            task.overwriteStatus(status);
            // If the status is updated to FAILED -> NEW, then increase the retry count.
            if (prestatus == TaskStatus.FAILED && status == TaskStatus.NEW) {
                task.retry();
            }
            this.save(task);
            LOG.info("Update task({}) success: pre({}), status({})",
                     id, prestatus, status);

            return true;
        } else {
            LOG.warn("Update task({}) status conflict: current({}), " +
                     "pre({}), status({})", id, task.status(),
                     prestatus, status);
            return false;
        }
    }

    protected boolean updateStatusWithLock(Id id, TaskStatus prestatus,
                                           TaskStatus status) {

        LockResult lockResult = tryLockTask(id.asString());

        if (lockResult.lockSuccess()) {
            try {
                return updateStatus(id, prestatus, status);
            } finally {
                unlockTask(id.asString(), lockResult);
            }
        }

        return false;
    }

    /**
     * try to start task;
     *
     * @param task
     * @return true if the task have start
     */
    private boolean tryStartHugeTask(HugeTask<?> task) {
        // Print Scheduler status
        logCurrentState();

        initTaskParams(task);

        ExecutorService chosenExecutor = gremlinTaskExecutor;

        if (task.computer()) {
            chosenExecutor = this.olapTaskExecutor;
        }

        // TODO: uncomment later - vermeer job
        //if (task.vermeer()) {
        //    chosenExecutor = this.olapTaskExecutor;
        //}

        if (task.gremlinTask()) {
            chosenExecutor = this.gremlinTaskExecutor;
        }

        if (task.schemaTask()) {
            chosenExecutor = schemaTaskExecutor;
        }

        ThreadPoolExecutor executor = (ThreadPoolExecutor) chosenExecutor;
        if (executor.getActiveCount() < executor.getMaximumPoolSize()) {
            TaskRunner<?> runner = new TaskRunner<>(task);
            chosenExecutor.submit(runner);
            LOG.info("Submit task({})@({}/{})", task.id(),
                     this.graphSpace, this.graphName);

            return true;
        }

        return false;
    }

    protected void logCurrentState() {
        int gremlinActive =
                ((ThreadPoolExecutor) gremlinTaskExecutor).getActiveCount();
        int schemaActive =
                ((ThreadPoolExecutor) schemaTaskExecutor).getActiveCount();
        int ephemeralActive =
                ((ThreadPoolExecutor) ephemeralTaskExecutor).getActiveCount();
        int olapActive =
                ((ThreadPoolExecutor) olapTaskExecutor).getActiveCount();

        LOG.info("Current State: gremlinTaskExecutor({}), schemaTaskExecutor" +
                 "({}), ephemeralTaskExecutor({}), olapTaskExecutor({})",
                 gremlinActive, schemaActive, ephemeralActive, olapActive);
    }

    private LockResult tryLockTask(String taskId) {

        LockResult lockResult = new LockResult();

        try {
            lockResult =
                    MetaManager.instance().tryLockTask(graphSpace, graphName,
                                                       taskId);
        } catch (Throwable t) {
            LOG.warn(String.format("try to lock task(%s) error", taskId), t);
        }

        return lockResult;
    }

    private void unlockTask(String taskId, LockResult lockResult) {

        try {
            MetaManager.instance().unlockTask(graphSpace, graphName, taskId,
                                              lockResult);
        } catch (Throwable t) {
            LOG.warn(String.format("try to unlock task(%s) error",
                                   taskId), t);
        }
    }

    protected boolean isLockedTask(String taskId) {
        return MetaManager.instance().isLockedTask(graphSpace,
                                                   graphName, taskId);
    }

    @Override
    public String graphName() {
        return this.graph.name();
    }

    @Override
    public String spaceGraphName() {
        return this.graphSpace + "-" + this.graphName;
    }

    @Override
    public void taskDone(HugeTask<?> task) {
        // DO Nothing
    }

    private class TaskRunner<V> implements Runnable {

        private final HugeTask<V> task;

        public TaskRunner(HugeTask<V> task) {
            this.task = task;
        }

        @Override
        public void run() {
            LockResult lockResult = tryLockTask(task.id().asString());

            initTaskParams(task);
            if (lockResult.lockSuccess() && !task.completed()) {

                LOG.info("Start task({})", task.id());

                TaskManager.setContext(task.context());
                try {
                    // 1. start task can be from schedule() & cronSchedule()
                    // 2. recheck the status of task, in case one same task
                    // called by both methods at same time;
                    HugeTask<Object> queryTask = task(this.task.id(), false);
                    if (queryTask != null &&
                        !TaskStatus.NEW.equals(queryTask.status())) {
                        return;
                    }

                    runningTasks.put(task.id(), task);

                    // Task execution will not throw exceptions, HugeTask will catch exceptions
                    // during execution and store them in the DB.
                    task.run();
                } catch (Throwable t) {
                    LOG.warn("exception when execute task", t);
                } finally {
                    runningTasks.remove(task.id());
                    deletingTasks.remove(task.id());
                    unlockTask(task.id().asString(), lockResult);

                    LOG.info("task({}) finished.", task.id().toString());
                }
            }
        }
    }
}
