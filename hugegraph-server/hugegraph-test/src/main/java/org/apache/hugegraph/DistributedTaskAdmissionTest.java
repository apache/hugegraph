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

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.dist.RegisterUtil;
import org.apache.hugegraph.exception.HugeException;
import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.id.IdGenerator;
import org.apache.hugegraph.meta.MetaManager;
import org.apache.hugegraph.meta.lock.LockResult;
import org.apache.hugegraph.meta.managers.TaskMetaManager;
import org.apache.hugegraph.job.EphemeralJob;
import org.apache.hugegraph.masterelection.GlobalMasterInfo;
import org.apache.hugegraph.task.DistributedTaskScheduler;
import org.apache.hugegraph.task.HugeTask;
import org.apache.hugegraph.task.TaskAndResultSchedulerTest.EmptyCallable;
import org.apache.hugegraph.task.TaskCallable;
import org.apache.hugegraph.task.TaskManager;
import org.apache.hugegraph.task.TaskScheduler;
import org.apache.hugegraph.task.TaskStatus;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.testutil.Whitebox;
import org.apache.hugegraph.unit.FakeObjects;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class DistributedTaskAdmissionTest {

    private TaskMetaManager originalLocks;

    @Before
    public void prepareLocks() {
        this.originalLocks = Whitebox.getInternalState(MetaManager.instance(), "taskMetaManager");
        TaskMetaManager locks = Mockito.mock(TaskMetaManager.class);
        Mockito.when(locks.tryLockTask(Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
               .thenAnswer(invocation -> {
                   LockResult acquired = new LockResult();
                   acquired.lockSuccess(true);
                   return acquired;
               });
        Whitebox.setInternalState(MetaManager.instance(), "taskMetaManager", locks);
    }

    @After
    public void restoreLocks() {
        Whitebox.setInternalState(MetaManager.instance(), "taskMetaManager", this.originalLocks);
    }

    @Test
    public void testCloseDuringPersistentRegistrationPreservesNewTask() throws Exception {
        try (Fixture fixture = new Fixture("register_new")) {
            CountDownLatch reading = new CountDownLatch(1);
            CountDownLatch finishRead = new CountDownLatch(1);
            fixture.scheduler.beforeTaskRead = () -> {
                reading.countDown();
                awaitUninterruptibly(finishRead);
            };
            HugeTask<?> task = task(false, new AtomicBoolean(), 9999917L);
            ExecutorService requests = Executors.newSingleThreadExecutor();
            try {
                fixture.scheduler.schedule(task);
                Assert.assertTrue(reading.await(10L, TimeUnit.SECONDS));
                Future<Boolean> close = requests.submit(fixture.scheduler::close);
                AtomicBoolean closed = Whitebox.getInternalState(fixture.scheduler, "closed");
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10L);
                while (!closed.get() && System.nanoTime() < deadline) {
                    Thread.yield();
                }
                Assert.assertTrue(closed.get());
                Assert.assertTrue(fixture.scheduler.pendingTasks() > 0);
                Assert.assertFalse(close.isDone());
                finishRead.countDown();
                Assert.assertTrue(close.get(10L, TimeUnit.SECONDS));
                fixture.scheduler.beforeTaskRead = () -> { };
                Assert.assertEquals(TaskStatus.NEW, fixture.scheduler.task(task.id(), false).status());
                Assert.assertEquals(TaskStatus.NEW, task.status());
                fixture.assertWorkerClean();
            } finally {
                finishRead.countDown();
                stop(requests);
            }
        }
    }

    @Test
    public void testAcceptedRemoteCancellationPreventsEarlyDrain() throws Exception {
        try (Fixture fixture = new Fixture("cancel_gate")) {
            CountDownLatch busy = new CountDownLatch(1);
            CountDownLatch finishWorker = new CountDownLatch(1);
            Future<?> occupied = fixture.worker.submit(() -> {
                busy.countDown();
                awaitUninterruptibly(finishWorker);
            });
            ExecutorService requests = Executors.newFixedThreadPool(2);
            GatedTask task = new GatedTask(9999918L, new EmptyCallable());
            try {
                Assert.assertTrue(busy.await(10L, TimeUnit.SECONDS));
                fixture.scheduler.schedule(task);
                Assert.assertEquals(0, fixture.scheduler.pendingTasks());
                task.armed.set(true);
                Future<?> cancel = requests.submit(() -> fixture.scheduler.cancel(task));
                Assert.assertTrue(task.reached.await(10L, TimeUnit.SECONDS));
                Future<Boolean> close = requests.submit(fixture.scheduler::close);
                Assert.assertTrue(fixture.scheduler.pendingTasks() > 0);
                Assert.assertFalse(close.isDone());
                task.release.countDown();
                cancel.get(10L, TimeUnit.SECONDS);
                Assert.assertTrue(close.get(10L, TimeUnit.SECONDS));
                Assert.assertEquals(TaskStatus.CANCELLING, fixture.scheduler.task(task.id(), false).status());
            } finally {
                task.release.countDown();
                finishWorker.countDown();
                occupied.get(10L, TimeUnit.SECONDS);
                stop(requests);
            }
        }
    }

    @Test
    public void testQueuedPersistentClosePreservesNewTask() throws Exception {
        try (Fixture fixture = new Fixture("queued_new")) {
            fixture.worker.armed.set(true);
            HugeTask<?> task = task(false, new AtomicBoolean(), 9999910L);
            fixture.scheduler.schedule(task);
            Assert.assertTrue(fixture.worker.reached.await(10L, TimeUnit.SECONDS));
            Assert.assertEquals(1, fixture.scheduler.pendingTasks());
            Assert.assertTrue(fixture.scheduler.close());
            Assert.assertFalse(task.isCancelled());
            Assert.assertEquals(TaskStatus.NEW, task.status());
            Assert.assertEquals(TaskStatus.NEW, fixture.scheduler.task(task.id(), false).status());
            Assert.assertEquals(0, fixture.scheduler.pendingTasks());
            for (boolean ephemeral : new boolean[]{true, false}) {
                Assert.assertThrows(IllegalStateException.class,
                                    () -> fixture.scheduler.schedule(task(ephemeral, new AtomicBoolean(), 9999911L)));
            }
        }
    }

    @Test
    public void testBusyWorkerLeavesPersistentBacklogInDatabase() throws Exception {
        try (Fixture fixture = new Fixture("busy_new")) {
            CountDownLatch busy = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            Future<?> occupied = fixture.worker.submit(() -> {
                busy.countDown();
                Assert.assertTrue(release.await(10L, TimeUnit.SECONDS));
                return null;
            });
            try {
                Assert.assertTrue(busy.await(10L, TimeUnit.SECONDS));
                HugeTask<?> task = task(false, new AtomicBoolean(), 9999912L);
                fixture.scheduler.schedule(task);
                Assert.assertEquals(0, fixture.scheduler.pendingTasks());
                Assert.assertTrue(fixture.worker.getQueue().isEmpty());
                Assert.assertEquals(TaskStatus.NEW, fixture.scheduler.task(task.id(), false).status());
                Assert.assertTrue(fixture.scheduler.close());
            } finally {
                release.countDown();
                occupied.get(10L, TimeUnit.SECONDS);
            }
        }
    }

    @Test
    public void testCancellationFailurePreservesCallerTransaction() throws Exception {
        try (Fixture fixture = new Fixture("failed_caller_tx")) {
            fixture.worker.armed.set(true);
            AtomicBoolean reject = new AtomicBoolean(true);
            IllegalStateException failure = new IllegalStateException("Cancellation rejected");
            HugeTask<?> task = new HugeTask<>(IdGenerator.of(-9999951L), null, new EphemeralJob<Object>() {
                @Override
                public String type() {
                    return "test";
                }

                @Override
                public Object execute() {
                    return null;
                }
            }) {
                @Override
                public boolean cancel(boolean interrupt) {
                    if (reject.get()) {
                        throw failure;
                    }
                    return super.cancel(interrupt);
                }
            };
            task.type("test");
            task.name("failed-cancellation-caller-tx");
            fixture.scheduler.schedule(task);
            Assert.assertTrue(fixture.worker.reached.await(10L, TimeUnit.SECONDS));
            try {
                this.openCallerTransaction(fixture.graph);
                Assert.assertThrows(HugeException.class, () -> fixture.scheduler.cancel(task),
                                    caught -> Assert.assertSame(failure, caught.getCause()));
                Assert.assertFalse(task.isCancelled());
                this.assertCallerTransactionCommits(fixture);
                Assert.assertEquals(1, fixture.scheduler.pendingTasks());
            } finally {
                reject.set(false);
                fixture.worker.release.countDown();
            }
            fixture.assertWorkerClean();
        }
    }

    @Test
    public void testNormalWorkerTransactionCleanupControl() throws Exception {
        try (Fixture fixture = new Fixture("normal_worker_control")) {
            AtomicBoolean opened = new AtomicBoolean();
            HugeTask<?> task = new HugeTask<>(IdGenerator.of(-9999950L), null, new EphemeralJob<Object>() {
                @Override
                public String type() {
                    return "test";
                }

                @Override
                public Object execute() {
                    this.graph().vertices().hasNext();
                    opened.set(this.graph().tx().isOpen());
                    return null;
                }
            });
            task.type("test");
            task.name("normal-worker-cleanup");
            fixture.scheduler.schedule(task).get(10L, TimeUnit.SECONDS);
            Assert.assertTrue(opened.get());
            Assert.assertFalse(task.isCancelled());
            Assert.assertEquals(TaskStatus.SUCCESS, task.status());
            fixture.assertWorkerClean();
            Assert.assertFalse(fixture.worker.submit(() -> fixture.graph.tx().isOpen()).get(10L, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testQueuedDefaultCancellationPreservesCallerTransaction() throws Exception {
        this.assertDefaultCancellationPreservesCallerTransaction(false);
    }

    @Test
    public void testRunningDefaultCancellationPreservesCallerTransaction() throws Exception {
        this.assertDefaultCancellationPreservesCallerTransaction(true);
    }

    private void assertDefaultCancellationPreservesCallerTransaction(boolean running) throws Exception {
        try (Fixture fixture = new Fixture("caller_tx_" + running)) {
            CountDownLatch started = new CountDownLatch(1);
            CountDownLatch finish = new CountDownLatch(1);
            if (!running) {
                fixture.worker.armed.set(true);
            }
            HugeTask<?> task = new HugeTask<>(IdGenerator.of(running ? -9999921L : -9999922L), null,
                    new EphemeralJob<Object>() {
                        @Override
                        public String type() {
                            return "test";
                        }

                        @Override
                        public Object execute() {
                            this.graph().vertices().hasNext();
                            started.countDown();
                            awaitUninterruptibly(finish);
                            return null;
                        }
                    });
            task.type("test");
            task.name("default-callback-caller-tx");
            try {
                fixture.scheduler.schedule(task);
                Assert.assertTrue((running ? started : fixture.worker.reached)
                                  .await(10L, TimeUnit.SECONDS));
                this.openCallerTransaction(fixture.graph);
                fixture.scheduler.cancel(task);
                Assert.assertTrue(task.isCancelled());
                this.assertCallerTransactionCommits(fixture);
                if (running) {
                    Assert.assertEquals(1, fixture.scheduler.pendingTasks());
                }
            } finally {
                finish.countDown();
                fixture.worker.release.countDown();
            }
            fixture.assertWorkerClean();
            Assert.assertEquals(running ? 0L : 1L, started.getCount());
        }
    }

    @Test
    public void testCompletedCancellationPreservesCallerTransaction() throws Exception {
        try (Fixture fixture = new Fixture("completed_caller_tx")) {
            HugeTask<?> task = task(true, new AtomicBoolean(), 9999923L);
            fixture.scheduler.schedule(task).get(10L, TimeUnit.SECONDS);
            fixture.assertWorkerClean();
            this.openCallerTransaction(fixture.graph);
            fixture.scheduler.cancel(task);
            Assert.assertFalse(task.isCancelled());
            this.assertCallerTransactionCommits(fixture);
            Assert.assertEquals(0, fixture.scheduler.pendingTasks());
        }
    }

    private void openCallerTransaction(HugeGraph graph) {
        graph.schema().vertexLabel("caller").useCustomizeStringId().create();
        graph.tx().commit();
        graph.addVertex(org.apache.tinkerpop.gremlin.structure.T.id, "pending",
                        org.apache.tinkerpop.gremlin.structure.T.label, "caller");
        Assert.assertTrue(graph.tx().isOpen());
    }

    private void assertCallerTransactionCommits(Fixture fixture) throws Exception {
        Assert.assertTrue(fixture.graph.tx().isOpen());
        Assert.assertTrue(fixture.graph.vertices("pending").hasNext());
        fixture.graph.tx().commit();
        Assert.assertFalse(fixture.graph.tx().isOpen());
        Assert.assertTrue(fixture.database.submit(() -> {
            try {
                return fixture.graph.vertices("pending").hasNext();
            } finally {
                fixture.params.closeTx();
            }
        }).get(10L, TimeUnit.SECONDS));
    }

    @Test
    public void testQueuedOuterCancellationPreservesCallerTransaction() throws Exception {
        try (Fixture fixture = new Fixture("queued_future")) {
            fixture.worker.armed.set(true);
            AtomicBoolean ran = new AtomicBoolean();
            HugeTask<?> task = task(true, ran, 9999913L);
            Future<?> dispatch = fixture.scheduler.schedule(task);
            Assert.assertTrue(fixture.worker.reached.await(10L, TimeUnit.SECONDS));
            fixture.graph.schema().vertexLabel("caller").useCustomizeStringId().create();
            fixture.graph.addVertex(org.apache.tinkerpop.gremlin.structure.T.id, "pending",
                                    org.apache.tinkerpop.gremlin.structure.T.label, "caller");
            Assert.assertTrue(dispatch.cancel(false));
            Assert.assertTrue(fixture.graph.tx().isOpen());
            Assert.assertTrue(fixture.graph.vertices("pending").hasNext());
            fixture.graph.tx().rollback();
            Assert.assertFalse(task.isCancelled());
            Assert.assertEquals(0, fixture.scheduler.pendingTasks());
            Assert.assertTrue(fixture.scheduler.close());
            fixture.worker.release.countDown();
            fixture.worker.submit(() -> null).get(10L, TimeUnit.SECONDS);
            Assert.assertFalse(ran.get());
        }
    }

    @Test
    public void testActiveOuterCancellationRetainsActualWorkerOwner() throws Exception {
        try (Fixture fixture = new Fixture("active_future")) {
            CountDownLatch started = new CountDownLatch(1);
            CountDownLatch finish = new CountDownLatch(1);
            HugeTask<?> task = new HugeTask<>(IdGenerator.of(-9999914L), null, new EphemeralJob<Object>() {
                @Override
                public String type() {
                    return "test";
                }

                @Override
                public Object execute() {
                    this.graph().vertices().hasNext();
                    started.countDown();
                    awaitUninterruptibly(finish);
                    return null;
                }
            });
            task.type("test");
            task.name("active-owner");
            Future<?> dispatch = fixture.scheduler.schedule(task);
            ExecutorService requests = Executors.newSingleThreadExecutor();
            try {
                Assert.assertTrue(started.await(10L, TimeUnit.SECONDS));
                Assert.assertTrue(dispatch.cancel(true));
                Future<Boolean> close = requests.submit(fixture.scheduler::close);
                Assert.assertTrue(fixture.scheduler.pendingTasks() > 0);
                Assert.assertFalse(close.isDone());
                finish.countDown();
                Assert.assertTrue(close.get(10L, TimeUnit.SECONDS));
                fixture.assertWorkerClean();
            } finally {
                finish.countDown();
                stop(requests);
            }
        }
    }

    @Test
    public void testAcceptedLocalCancellationRetainsCallbackAndSupportsNestedCancel() throws Exception {
        try (Fixture fixture = new Fixture("nested_callback")) {
            fixture.worker.armed.set(true);
            HugeTask<?> other = task(true, new AtomicBoolean(), 9999919L);
            GatedTask task = new GatedTask(-9999915L, new EphemeralJob<Object>() {
                @Override
                public String type() {
                    return "test";
                }

                @Override
                public Object execute() {
                    Assert.fail("Cancelled queued work must not execute");
                    return null;
                }

                @Override
                protected void cancelled() {
                    fixture.scheduler.cancel(other);
                }
            });
            Future<?> dispatch = fixture.scheduler.schedule(task);
            Assert.assertTrue(fixture.worker.reached.await(10L, TimeUnit.SECONDS));
            fixture.scheduler.schedule(other);
            task.armed.set(true);
            ExecutorService requests = Executors.newSingleThreadExecutor();
            try {
                Future<?> cancel = requests.submit(() -> {
                    TaskManager.setContext("nested-cancel");
                    try {
                        fixture.scheduler.cancel(task);
                        Assert.assertEquals("nested-cancel", TaskManager.getContext());
                    } finally {
                        TaskManager.resetContext();
                    }
                });
                Assert.assertTrue(task.reached.await(10L, TimeUnit.SECONDS));
                Assert.assertTrue(dispatch.cancel(false));
                Assert.assertTrue(fixture.scheduler.pendingTasks() > 0);
                task.release.countDown();
                cancel.get(10L, TimeUnit.SECONDS);
                Assert.assertTrue(task.isCancelled());
                Assert.assertTrue(other.isCancelled());
                Assert.assertEquals(0, fixture.scheduler.pendingTasks());
            } finally {
                task.release.countDown();
                stop(requests);
            }
        }
    }

    @Test
    public void testCallbackCleanupCompletesBeforeGraphDrain() throws Exception {
        try (Fixture fixture = new Fixture("callback_owner")) {
            fixture.worker.armed.set(true);
            CountDownLatch callback = new CountDownLatch(1);
            CountDownLatch finish = new CountDownLatch(1);
            HugeTask<?> task = new HugeTask<>(IdGenerator.of(-9999916L), null, new EphemeralJob<Object>() {
                @Override
                public String type() {
                    return "test";
                }

                @Override
                public Object execute() {
                    Assert.fail("Cancelled queued work must not execute");
                    return null;
                }

                @Override
                protected void done() {
                    this.graph().vertices().hasNext();
                    callback.countDown();
                    awaitUninterruptibly(finish);
                    super.done();
                }
            });
            task.type("test");
            task.name("callback-owner");
            Future<?> dispatch = fixture.scheduler.schedule(task);
            Assert.assertTrue(fixture.worker.reached.await(10L, TimeUnit.SECONDS));
            ExecutorService requests = Executors.newSingleThreadExecutor();
            ExecutorService callbacks = Executors.newSingleThreadExecutor();
            try {
                Future<?> cancel = callbacks.submit(() -> fixture.scheduler.cancel(task));
                Assert.assertTrue(callback.await(10L, TimeUnit.SECONDS));
                Assert.assertTrue(dispatch.cancel(true));
                Future<Boolean> close = requests.submit(fixture.scheduler::close);
                Assert.assertTrue(fixture.scheduler.pendingTasks() > 0);
                Assert.assertFalse(close.isDone());
                finish.countDown();
                cancel.get(10L, TimeUnit.SECONDS);
                Assert.assertTrue(close.get(10L, TimeUnit.SECONDS));
                ThreadLocal<?> owners = Whitebox.getInternalState(fixture.graph.tx(), "transactions");
                Assert.assertNull(callbacks.submit(owners::get).get(10L, TimeUnit.SECONDS));
                Assert.assertNull(callbacks.submit(TaskManager::getContext).get(10L, TimeUnit.SECONDS));
            } finally {
                finish.countDown();
                stop(requests);
                stop(callbacks);
            }
        }
    }

    @Test
    public void testBlockedCronStillFencesAdmissionAndTimesOutClose() throws Exception {
        try (Fixture fixture = new Fixture("blocked_cron", 1L)) {
            CountDownLatch querying = new CountDownLatch(1);
            CountDownLatch finishQuery = new CountDownLatch(1);
            fixture.scheduler.beforeTaskQuery = () -> {
                querying.countDown();
                awaitUninterruptibly(finishQuery);
            };
            ExecutorService requests = Executors.newSingleThreadExecutor();
            Future<?> cron = fixture.cron.submit(fixture.scheduler::cronSchedule);
            try {
                Assert.assertTrue(querying.await(10L, TimeUnit.SECONDS));
                Future<Boolean> close = requests.submit(fixture.scheduler::close);
                AtomicBoolean closed = Whitebox.getInternalState(fixture.scheduler, "closed");
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2L);
                while (!closed.get() && System.nanoTime() < deadline) {
                    Thread.yield();
                }
                Assert.assertTrue(closed.get());
                Assert.assertThrows(IllegalStateException.class,
                        () -> fixture.scheduler.schedule(task(true, new AtomicBoolean(), 9999919L)));
                // This exercises the real cron barrier, not a mocked drain method.
                Assert.assertFalse(close.get(10L, TimeUnit.SECONDS));
                Assert.assertFalse(cron.isDone());
                finishQuery.countDown();
                cron.get(10L, TimeUnit.SECONDS);
                Assert.assertTrue(fixture.scheduler.close());
                fixture.assertWorkerClean();
            } finally {
                finishQuery.countDown();
                cron.get(10L, TimeUnit.SECONDS);
                stop(requests);
            }
        }
    }

    @Test
    public void testDeleteAfterLocalOwnerFinishesStillRemovesRecord() throws Exception {
        try (Fixture fixture = new Fixture("delete_finished_owner")) {
            DeleteCompletionCallable.started = new CountDownLatch(1);
            DeleteCompletionCallable.finish = new CountDownLatch(1);
            HugeTask<?> task = new HugeTask<>(IdGenerator.of(9999952L), null, new DeleteCompletionCallable());
            task.type("test");
            task.name("delete-finished-owner");
            Map<Id, HugeTask<?>> running = Whitebox.getInternalState(fixture.scheduler, "runningTasks");
            Map<Id, HugeTask<?>> observed = Mockito.spy(running);
            AtomicBoolean intercept = new AtomicBoolean();
            Mockito.doAnswer(invocation -> {
                HugeTask<?> value = (HugeTask<?>) invocation.callRealMethod();
                if (intercept.compareAndSet(true, false)) {
                    Assert.assertSame(task, value);
                    DeleteCompletionCallable.finish.countDown();
                    // The actual worker completes persistence and releases ownership before delete continues.
                    fixture.worker.submit(() -> null).get(10L, TimeUnit.SECONDS);
                    Assert.assertEquals(TaskStatus.SUCCESS, task.status());
                    Assert.assertEquals(TaskStatus.SUCCESS, fixture.scheduler.task(task.id(), false).status());
                    Assert.assertEquals(0, fixture.scheduler.pendingTasks());
                }
                return value;
            }).when(observed).get(task.id());
            Whitebox.setInternalState(fixture.scheduler, "runningTasks", observed);
            try {
                fixture.scheduler.schedule(task);
                Assert.assertTrue(DeleteCompletionCallable.started.await(10L, TimeUnit.SECONDS));
                intercept.set(true);
                Assert.assertNotNull(fixture.scheduler.delete(task.id(), false));
                Assert.assertFalse(intercept.get());
                Assert.assertThrows(org.apache.hugegraph.exception.NotFoundException.class,
                                    () -> fixture.scheduler.task(task.id(), false));
                fixture.assertWorkerClean();
            } finally {
                DeleteCompletionCallable.finish.countDown();
            }
        }
    }

    public static class DeleteCompletionCallable extends org.apache.hugegraph.job.UserJob<Object> {

        private static CountDownLatch started;
        private static CountDownLatch finish;

        @Override
        public String type() {
            return "test";
        }

        @Override
        public Object execute() {
            started.countDown();
            awaitUninterruptibly(finish);
            return null;
        }
    }

    private static void awaitUninterruptibly(CountDownLatch release) {
        while (release.getCount() != 0L) {
            try {
                Assert.assertTrue(release.await(10L, TimeUnit.SECONDS));
            } catch (InterruptedException ignored) {
                // Keep the real owner active after logical cancellation.
            }
        }
    }

    private static void stop(ExecutorService executor) throws InterruptedException {
        executor.shutdownNow();
        Assert.assertTrue(executor.awaitTermination(10L, TimeUnit.SECONDS));
    }

    private static HugeTask<?> task(boolean ephemeral, AtomicBoolean ran, long id) {
        HugeTask<?> task;
        if (ephemeral) {
            task = new HugeTask<>(IdGenerator.of(-id), null, new EphemeralJob<Object>() {
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
        } else {
            task = new HugeTask<>(IdGenerator.of(id), null, new EmptyCallable());
        }
        task.type("test");
        task.name("admission-test");
        return task;
    }

    private static class Fixture implements AutoCloseable {

        private final HugeGraph graph;
        private final ScheduledThreadPoolExecutor cron = new ScheduledThreadPoolExecutor(1);
        private final ExecutorService database = Executors.newSingleThreadExecutor();
        private final GatedExecutor worker = new GatedExecutor();
        private final TestScheduler scheduler;
        private final HugeGraphParams params;
        private final TaskScheduler originalScheduler;
        private final Map<HugeGraphParams, TaskScheduler> registry;

        Fixture(String suffix) {
            this(suffix, 600L);
        }

        Fixture(String suffix, long schedulePeriod) {
            RegisterUtil.registerBackends();
            HugeConfig config = FakeObjects.newConfig();
            config.setProperty("backend", "memory");
            config.setProperty("serializer", "text");
            config.setProperty("store", "distributed_admission_" + suffix);
            config.setProperty("task.schedule_period", schedulePeriod);
            config.setProperty("task.retry", 0);
            this.graph = HugeFactory.open(config);
            this.graph.initBackend();
            this.graph.serverStarted(GlobalMasterInfo.master("distributed-admission-test"));
            this.params = Whitebox.getInternalState(this.graph, "params");
            this.registry = Whitebox.getInternalState(TaskManager.instance(), "schedulers");
            this.originalScheduler = this.registry.get(this.params);
            Assert.assertTrue(this.originalScheduler.close());
            this.scheduler = new TestScheduler(this.params, this.cron, this.database, this.worker);
            this.scheduler.init();
            Assert.assertTrue(this.registry.replace(this.params, this.originalScheduler, this.scheduler));
        }

        void assertWorkerClean() throws Exception {
            this.worker.submit(() -> null).get(10L, TimeUnit.SECONDS);
            Assert.assertEquals(0, this.scheduler.pendingTasks());
            ThreadLocal<?> owners = Whitebox.getInternalState(this.graph.tx(), "transactions");
            Assert.assertNull(this.worker.submit(owners::get).get(10L, TimeUnit.SECONDS));
            Assert.assertNull(this.worker.submit(TaskManager::getContext).get(10L, TimeUnit.SECONDS));
        }

        @Override
        public void close() throws Exception {
            this.worker.release.countDown();
            try {
                Assert.assertTrue(this.scheduler.close());
                this.scheduler.closeDatabaseOwner();
            } finally {
                Assert.assertTrue(this.registry.replace(this.params, this.scheduler, this.originalScheduler));
                for (ExecutorService executor : new ExecutorService[]{this.worker, this.database, this.cron}) {
                    stop(executor);
                }
                try {
                    this.graph.close();
                } finally {
                    HugeFactory.remove(this.graph);
                }
            }
        }
    }

    private static class GatedTask extends HugeTask<Object> {

        private final AtomicBoolean armed = new AtomicBoolean();
        private final CountDownLatch reached = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);

        GatedTask(long id, TaskCallable<Object> callable) {
            super(IdGenerator.of(id), null, callable);
            this.type("test");
            this.name("accepted-cancel");
        }

        @Override
        public boolean completed() {
            if (this.armed.compareAndSet(true, false)) {
                this.reached.countDown();
                awaitUninterruptibly(this.release);
            }
            return super.completed();
        }
    }

    private static class GatedExecutor extends ThreadPoolExecutor {

        private final AtomicBoolean armed = new AtomicBoolean();
        private final CountDownLatch reached = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);

        GatedExecutor() {
            super(1, 1, 0L, TimeUnit.SECONDS, new LinkedBlockingQueue<>());
        }

        @Override
        protected void beforeExecute(Thread thread, Runnable work) {
            if (this.armed.compareAndSet(true, false)) {
                this.reached.countDown();
                awaitUninterruptibly(this.release);
            }
        }
    }

    private static class TestScheduler extends DistributedTaskScheduler {

        private volatile Runnable beforeTaskRead = () -> { };
        private volatile Runnable beforeTaskQuery = () -> { };

        @Override
        protected <V> Iterator<HugeTask<V>> queryTaskWithoutResultByStatus(TaskStatus status) {
            this.beforeTaskQuery.run();
            return super.queryTaskWithoutResultByStatus(status);
        }

        @Override
        public <V> HugeTask<V> task(Id id, boolean withResult) {
            this.beforeTaskRead.run();
            return super.task(id, withResult);
        }

        TestScheduler(HugeGraphParams graph, ScheduledThreadPoolExecutor cron,
                      ExecutorService database, ExecutorService worker) {
            super(graph, cron, database, worker, worker, worker, worker);
        }

        void closeDatabaseOwner() {
            this.call(() -> {
                this.tx().close();
                this.graph.closeTx();
                return null;
            });
        }
    }
}
