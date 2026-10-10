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

package org.apache.hugegraph;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.dist.RegisterUtil;
import org.apache.hugegraph.exception.HugeException;
import org.apache.hugegraph.id.IdGenerator;
import org.apache.hugegraph.job.SysJob;
import org.apache.hugegraph.masterelection.GlobalMasterInfo;
import org.apache.hugegraph.task.HugeTask;
import org.apache.hugegraph.task.StandardTaskScheduler;
import org.apache.hugegraph.task.TaskStatus;
import org.apache.hugegraph.task.TaskAndResultSchedulerTest.EmptyCallable;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.testutil.Whitebox;
import org.apache.hugegraph.unit.FakeObjects;
import org.apache.tinkerpop.gremlin.structure.T;
import org.junit.Test;
import org.mockito.Mockito;

public class StandardTaskAdmissionTest {

    @Test
    public void testCancelledRunningTaskRetainsWorkerBeforeDrop() throws Exception {
        RegisterUtil.registerBackends();
        HugeConfig config = FakeObjects.newConfig();
        config.setProperty("backend", "memory");
        config.setProperty("serializer", "text");
        config.setProperty("store", "standard_cancelled_owner");
        Path file = Files.createTempFile("standard-cancelled-owner-", ".properties");
        Files.writeString(file, "retained graph config");
        config.file(file.toString());
        HugeGraph graph = HugeFactory.open(config);
        StandardTaskScheduler scheduler = (StandardTaskScheduler) graph.taskScheduler();
        BlockingJob job = new BlockingJob();
        HugeTask<Object> task = new HugeTask<>(IdGenerator.of(9999981L), null, job);
        task.type("test");
        task.name("noninterruptible-standard-owner");
        Future<?> running = null;
        try {
            graph.initBackend();
            graph.serverStarted(GlobalMasterInfo.master("standard-owner-test"));
            graph.schema().propertyKey("marker").asText().create();
            graph.schema().vertexLabel("person").properties("marker")
                 .useCustomizeStringId().create();
            graph.addVertex(T.id, "retained", T.label, "person", "marker", "keep");
            graph.tx().commit();
            running = scheduler.schedule(task);
            Assert.assertTrue(job.started.await(10L, TimeUnit.SECONDS));
            HugeTask<Object> stored = scheduler.findTask(task.id());
            Assert.assertEquals(TaskStatus.RUNNING, stored.status());
            scheduler.cancel(stored);
            Assert.assertTrue(task.isCancelled());
            Assert.assertEquals(1, scheduler.pendingTasks());
            Assert.assertThrows(HugeException.class, graph::drop);
            Assert.assertFalse(graph.closed());
            Assert.assertTrue(Files.exists(file));
            Assert.assertEquals("keep", graph.vertices("retained").next().value("marker"));
            job.release.countDown();
            running.get(10L, TimeUnit.SECONDS);
            scheduler.waitUntilAllTasksCompleted(10L);
            Assert.assertEquals(0, scheduler.pendingTasks());
            ThreadLocal<?> owners = Whitebox.getInternalState(graph.tx(), "transactions");
            ThreadPoolExecutor workers = Whitebox.getInternalState(scheduler, "taskExecutor");
            CountDownLatch checked = new CountDownLatch(workers.getCorePoolSize());
            List<Future<?>> inspections = new ArrayList<>();
            for (int i = 0; i < workers.getCorePoolSize(); i++) {
                inspections.add(workers.submit(() -> {
                    Object owner = owners.get();
                    checked.countDown();
                    Assert.assertTrue(checked.await(10L, TimeUnit.SECONDS));
                    Assert.assertNull(owner);
                    return null;
                }));
            }
            for (Future<?> inspection : inspections) {
                inspection.get(10L, TimeUnit.SECONDS);
            }
            graph.drop();
            Assert.assertTrue(graph.closed());
        } finally {
            job.release.countDown();
            if (running != null) {
                running.get(10L, TimeUnit.SECONDS);
                scheduler.waitUntilAllTasksCompleted(10L);
            }
            if (!graph.closed()) {
                graph.close();
            }
            HugeFactory.remove(graph);
            Files.deleteIfExists(file);
        }
    }

    @Test
    public void testQueuedCancellationAndRejectedSubmissionReleaseReservations() throws Exception {
        RegisterUtil.registerBackends();
        HugeConfig config = FakeObjects.newConfig();
        config.setProperty("backend", "memory");
        config.setProperty("serializer", "text");
        config.setProperty("store", "standard_queued_owner");
        HugeGraph graph = HugeFactory.open(config);
        ExecutorService worker = Executors.newSingleThreadExecutor();
        ExecutorService database = Executors.newSingleThreadExecutor();
        CountDownLatch occupied = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        StandardTaskScheduler scheduler = new StandardTaskScheduler(
                Whitebox.getInternalState(graph, "params"), worker, database);
        try {
            graph.initBackend();
            graph.serverStarted(GlobalMasterInfo.master("standard-queued-test"));
            worker.submit(() -> {
                occupied.countDown();
                release.await();
                return null;
            });
            Assert.assertTrue(occupied.await(10L, TimeUnit.SECONDS));
            AtomicBoolean ran = new AtomicBoolean();
            HugeTask<Object> task = new HugeTask<>(IdGenerator.of(9999982L), null,
                                                  new SysJob<Object>() {
                @Override
                public String type() {
                    return "test";
                }

                @Override
                public Object execute() {
                    ran.set(true);
                    return null;
                }
            });
            task.type("test");
            task.name("queued-standard-owner");
            scheduler.schedule(task);
            Assert.assertEquals(1, scheduler.pendingTasks());
            scheduler.cancel(task);
            Assert.assertEquals(0, scheduler.pendingTasks());
            release.countDown();
            worker.submit(() -> null).get(10L, TimeUnit.SECONDS);
            Assert.assertFalse(ran.get());
            worker.shutdown();
            Assert.assertTrue(worker.awaitTermination(10L, TimeUnit.SECONDS));
            HugeTask<Object> rejected = new HugeTask<>(IdGenerator.of(9999983L), null, new BlockingJob());
            rejected.type("test");
            rejected.name("rejected-standard-owner");
            Assert.assertThrows(RejectedExecutionException.class, () -> scheduler.schedule(rejected));
            Assert.assertEquals(0, scheduler.pendingTasks());
        } finally {
            release.countDown();
            worker.shutdownNow();
            Assert.assertTrue(worker.awaitTermination(10L, TimeUnit.SECONDS));
            scheduler.close();
            database.shutdownNow();
            Assert.assertTrue(database.awaitTermination(10L, TimeUnit.SECONDS));
            graph.close();
            HugeFactory.remove(graph);
        }
    }

    @Test
    public void testCancelledDependencyRetryRetainsEarlierWorkerCleanup() throws Exception {
        RegisterUtil.registerBackends();
        HugeConfig config = FakeObjects.newConfig();
        config.setProperty("backend", "memory");
        config.setProperty("serializer", "text");
        config.setProperty("store", "standard_dependency_owner");
        HugeGraph graph = HugeFactory.open(config);
        ExecutorService worker = Executors.newSingleThreadExecutor();
        ExecutorService database = Executors.newSingleThreadExecutor();
        CountDownLatch closing = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicBoolean armed = new AtomicBoolean(true);
        HugeGraphParams params = Mockito.spy((HugeGraphParams) Whitebox.getInternalState(graph, "params"));
        Mockito.doAnswer(invocation -> {
            if (armed.compareAndSet(true, false)) {
                closing.countDown();
                release.await();
            }
            return invocation.callRealMethod();
        }).when(params).closeTx();
        StandardTaskScheduler scheduler = new StandardTaskScheduler(params, worker, database);
        Future<?> first = null;
        try {
            graph.initBackend();
            graph.serverStarted(GlobalMasterInfo.master("standard-dependency-test"));
            HugeTask<Object> dependency = new HugeTask<>(IdGenerator.of(9999984L), null, new EmptyCallable());
            dependency.type("test");
            dependency.name("unfinished-dependency");
            scheduler.save(dependency);
            HugeTask<Object> retry = new HugeTask<>(IdGenerator.of(9999985L), null, new EmptyCallable());
            retry.type("test");
            retry.name("dependency-retry-owner");
            retry.depends(dependency.id());
            first = scheduler.schedule(retry);
            Assert.assertTrue(closing.await(10L, TimeUnit.SECONDS));
            scheduler.cancel(retry);
            Assert.assertTrue(retry.isCancelled());
            Assert.assertEquals(1, scheduler.pendingTasks());
            Assert.assertFalse(scheduler.close());
            release.countDown();
            first.get(10L, TimeUnit.SECONDS);
            scheduler.waitUntilAllTasksCompleted(10L);
            Assert.assertEquals(0, scheduler.pendingTasks());
        } finally {
            release.countDown();
            if (first != null) {
                first.get(10L, TimeUnit.SECONDS);
            }
            worker.shutdownNow();
            Assert.assertTrue(worker.awaitTermination(10L, TimeUnit.SECONDS));
            scheduler.close();
            database.shutdownNow();
            Assert.assertTrue(database.awaitTermination(10L, TimeUnit.SECONDS));
            graph.close();
            HugeFactory.remove(graph);
        }
    }

    @Test
    public void testCompletedWrapperCannotCancelDependencyRetry() throws Exception {
        RegisterUtil.registerBackends();
        HugeConfig config = FakeObjects.newConfig();
        config.setProperty("backend", "memory");
        config.setProperty("serializer", "text");
        config.setProperty("store", "standard_completed_future");
        HugeGraph graph = HugeFactory.open(config);
        CountDownLatch queued = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger executions = new AtomicInteger();
        ThreadPoolExecutor worker = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.SECONDS,
                                                           new LinkedBlockingQueue<>()) {
            @Override
            protected void beforeExecute(Thread thread, Runnable runnable) {
                if (executions.incrementAndGet() == 2) {
                    queued.countDown();
                    try {
                        Assert.assertTrue(release.await(10L, TimeUnit.SECONDS));
                    } catch (InterruptedException error) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException(error);
                    }
                }
            }
        };
        ExecutorService database = Executors.newSingleThreadExecutor();
        StandardTaskScheduler scheduler = new StandardTaskScheduler(
                Whitebox.getInternalState(graph, "params"), worker, database);
        try {
            graph.initBackend();
            graph.serverStarted(GlobalMasterInfo.master("standard-completed-future-test"));
            HugeTask<Object> dependency = new HugeTask<>(IdGenerator.of(9999986L), null, new EmptyCallable());
            dependency.type("test");
            dependency.name("unfinished-future-dependency");
            scheduler.save(dependency);
            HugeTask<Object> retry = new HugeTask<>(IdGenerator.of(9999987L), null, new EmptyCallable());
            retry.type("test");
            retry.name("completed-wrapper-retry");
            retry.depends(dependency.id());
            Future<?> completed = scheduler.schedule(retry);
            completed.get(10L, TimeUnit.SECONDS);
            Assert.assertTrue(queued.await(10L, TimeUnit.SECONDS));
            Assert.assertFalse(completed.cancel(true));
            Assert.assertFalse(retry.isCancelled());
            Assert.assertEquals(TaskStatus.QUEUED, retry.status());
            Assert.assertEquals(1, scheduler.pendingTasks());
            dependency.overwriteStatus(TaskStatus.SUCCESS);
            scheduler.save(dependency);
            release.countDown();
            scheduler.waitUntilAllTasksCompleted(10L);
            Assert.assertEquals(TaskStatus.SUCCESS, retry.status());
        } finally {
            release.countDown();
            worker.shutdownNow();
            Assert.assertTrue(worker.awaitTermination(10L, TimeUnit.SECONDS));
            scheduler.close();
            database.shutdownNow();
            Assert.assertTrue(database.awaitTermination(10L, TimeUnit.SECONDS));
            graph.close();
            HugeFactory.remove(graph);
        }
    }

    @Test
    public void testFutureCancellationWithoutInterruptRetainsActiveWorker() throws Exception {
        RegisterUtil.registerBackends();
        HugeConfig config = FakeObjects.newConfig();
        config.setProperty("backend", "memory");
        config.setProperty("serializer", "text");
        config.setProperty("store", "standard_future_no_interrupt");
        HugeGraph graph = HugeFactory.open(config);
        ExecutorService worker = Executors.newSingleThreadExecutor();
        ExecutorService database = Executors.newSingleThreadExecutor();
        StandardTaskScheduler scheduler = new StandardTaskScheduler(
                Whitebox.getInternalState(graph, "params"), worker, database);
        BlockingJob job = new BlockingJob();
        HugeTask<Object> task = new HugeTask<>(IdGenerator.of(9999988L), null, job);
        task.type("test");
        task.name("cancel-without-worker-interrupt");
        try {
            graph.initBackend();
            graph.serverStarted(GlobalMasterInfo.master("standard-no-interrupt-test"));
            Future<?> running = scheduler.schedule(task);
            Assert.assertTrue(job.started.await(10L, TimeUnit.SECONDS));
            Assert.assertTrue(running.cancel(false));
            Assert.assertFalse(task.isCancelled());
            Assert.assertEquals(TaskStatus.RUNNING, task.status());
            Assert.assertEquals(1, scheduler.pendingTasks());
            Assert.assertFalse(scheduler.close());
            HugeTask<Object> late = new HugeTask<>(IdGenerator.of(9999990L), null, new EmptyCallable());
            late.type("test");
            late.name("rejected-during-worker-drain");
            Assert.assertThrows(IllegalStateException.class, () -> scheduler.schedule(late));
            Assert.assertEquals(TaskStatus.NEW, late.status());
            Assert.assertEquals(1, scheduler.pendingTasks());
            job.release.countDown();
            scheduler.waitUntilAllTasksCompleted(10L);
            Assert.assertEquals(0, job.interrupts.get());
            Assert.assertEquals(TaskStatus.SUCCESS, task.status());
        } finally {
            job.release.countDown();
            scheduler.waitUntilAllTasksCompleted(10L);
            worker.shutdownNow();
            Assert.assertTrue(worker.awaitTermination(10L, TimeUnit.SECONDS));
            scheduler.close();
            database.shutdownNow();
            Assert.assertTrue(database.awaitTermination(10L, TimeUnit.SECONDS));
            graph.close();
            HugeFactory.remove(graph);
        }
    }

    @Test
    public void testDispatchCancellationPreservesCallerTransaction() throws Exception {
        for (boolean queued : new boolean[]{true, false}) {
            for (boolean interrupt : new boolean[]{true, false}) {
                this.checkDispatchCancellation(queued, interrupt);
            }
        }
    }

    private void checkDispatchCancellation(boolean queued, boolean interrupt) throws Exception {
        RegisterUtil.registerBackends();
        HugeConfig config = FakeObjects.newConfig();
        config.setProperty("backend", "memory");
        config.setProperty("serializer", "text");
        config.setProperty("store", "standard_dispatch_tx_" + queued + "_" + interrupt);
        HugeGraph graph = HugeFactory.open(config);
        ExecutorService worker = Executors.newSingleThreadExecutor();
        ExecutorService database = Executors.newSingleThreadExecutor();
        StandardTaskScheduler scheduler = new StandardTaskScheduler(
                Whitebox.getInternalState(graph, "params"), worker, database);
        CountDownLatch occupied = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        BlockingJob job = new BlockingJob();
        HugeTask<Object> task = new HugeTask<>(IdGenerator.of(9999989L), null, job);
        task.type("test");
        task.name("dispatch-caller-transaction");
        try {
            graph.initBackend();
            graph.serverStarted(GlobalMasterInfo.master("standard-dispatch-tx-test"));
            graph.schema().vertexLabel("person").useCustomizeStringId().create();
            if (queued) {
                worker.submit(() -> {
                    occupied.countDown();
                    release.await();
                    return null;
                });
                Assert.assertTrue(occupied.await(10L, TimeUnit.SECONDS));
            }
            Future<?> dispatch = scheduler.schedule(task);
            if (!queued) {
                Assert.assertTrue(job.started.await(10L, TimeUnit.SECONDS));
            }
            graph.addVertex(T.id, "caller-write", T.label, "person");
            Assert.assertTrue(graph.tx().isOpen());
            Assert.assertTrue(dispatch.cancel(interrupt));
            Assert.assertTrue(graph.tx().isOpen());
            Assert.assertFalse(task.isCancelled());
            Assert.assertEquals(queued ? TaskStatus.QUEUED : TaskStatus.RUNNING, task.status());
            Assert.assertEquals(queued ? 0 : 1, scheduler.pendingTasks());
            graph.tx().commit();
            Assert.assertTrue(graph.vertices("caller-write").hasNext());
            graph.tx().close();
            release.countDown();
            job.release.countDown();
            scheduler.waitUntilAllTasksCompleted(10L);
            if (queued) {
                Assert.assertEquals(1L, job.started.getCount());
            } else {
                Assert.assertEquals(TaskStatus.SUCCESS, task.status());
                if (!interrupt) {
                    Assert.assertEquals(0, job.interrupts.get());
                }
            }
        } finally {
            release.countDown();
            job.release.countDown();
            scheduler.waitUntilAllTasksCompleted(10L);
            worker.shutdownNow();
            Assert.assertTrue(worker.awaitTermination(10L, TimeUnit.SECONDS));
            scheduler.close();
            database.shutdownNow();
            Assert.assertTrue(database.awaitTermination(10L, TimeUnit.SECONDS));
            graph.close();
            HugeFactory.remove(graph);
        }
    }

    @Test
    public void testCloseFencesUnfinishedDependencyRetriesBeforeDrain() throws Exception {
        RegisterUtil.registerBackends();
        HugeConfig config = FakeObjects.newConfig();
        config.setProperty("backend", "memory");
        config.setProperty("serializer", "text");
        config.setProperty("store", "standard_close_dependency");
        HugeGraph graph = HugeFactory.open(config);
        ExecutorService worker = Executors.newSingleThreadExecutor();
        ExecutorService database = Executors.newSingleThreadExecutor();
        CountDownLatch occupied = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        StandardTaskScheduler scheduler = new StandardTaskScheduler(
                Whitebox.getInternalState(graph, "params"), worker, database);
        HugeTask<Object> retry = null;
        try {
            graph.initBackend();
            graph.serverStarted(GlobalMasterInfo.master("standard-close-dependency-test"));
            worker.submit(() -> {
                occupied.countDown();
                release.await();
                return null;
            });
            Assert.assertTrue(occupied.await(10L, TimeUnit.SECONDS));
            HugeTask<Object> dependency = new HugeTask<>(IdGenerator.of(9999991L), null, new EmptyCallable());
            dependency.type("test");
            dependency.name("unfinished-close-dependency");
            scheduler.save(dependency);
            retry = new HugeTask<>(IdGenerator.of(9999992L), null, new EmptyCallable());
            retry.type("test");
            retry.name("closing-dependency-retry");
            retry.depends(dependency.id());
            Future<?> dispatch = scheduler.schedule(retry);
            Assert.assertFalse(scheduler.close());
            Assert.assertEquals(1, scheduler.pendingTasks());
            HugeTask<Object> late = new HugeTask<>(IdGenerator.of(9999993L), null, new EmptyCallable());
            late.type("test");
            late.name("rejected-during-dependency-drain");
            Assert.assertThrows(IllegalStateException.class, () -> scheduler.schedule(late));
            Assert.assertEquals(TaskStatus.NEW, late.status());
            release.countDown();
            dispatch.get(10L, TimeUnit.SECONDS);
            scheduler.waitUntilAllTasksCompleted(10L);
            Assert.assertEquals(TaskStatus.FAILED, retry.status());
            Assert.assertContains("is closing", retry.result());
            Assert.assertEquals(0, scheduler.pendingTasks());
            Assert.assertTrue(scheduler.close());
        } finally {
            release.countDown();
            if (retry != null && !retry.completed()) {
                scheduler.cancel(retry);
            }
            scheduler.waitUntilAllTasksCompleted(10L);
            worker.shutdownNow();
            Assert.assertTrue(worker.awaitTermination(10L, TimeUnit.SECONDS));
            scheduler.close();
            database.shutdownNow();
            Assert.assertTrue(database.awaitTermination(10L, TimeUnit.SECONDS));
            graph.close();
            HugeFactory.remove(graph);
        }
    }

    public static class BlockingJob extends SysJob<Object> {

        private final CountDownLatch started = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);
        private final AtomicInteger interrupts = new AtomicInteger();

        @Override
        public String type() {
            return "test";
        }

        @Override
        public Object execute() {
            this.graph().vertices().hasNext();
            this.started.countDown();
            while (true) {
                try {
                    this.release.await();
                    return null;
                } catch (InterruptedException ignored) {
                    this.interrupts.incrementAndGet();
                    // Reproduce a worker which keeps owning its transaction after cancellation.
                }
            }
        }
    }
}
