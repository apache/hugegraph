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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hugegraph.backend.store.BackendStoreProvider;
import org.apache.hugegraph.backend.tx.GraphTransaction;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.dist.RegisterUtil;
import org.apache.hugegraph.exception.HugeException;
import org.apache.hugegraph.masterelection.GlobalMasterInfo;
import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.id.IdGenerator;
import org.apache.hugegraph.task.DistributedTaskScheduler;
import org.apache.hugegraph.task.HugeTask;
import org.apache.hugegraph.task.TaskAndResultSchedulerTest;
import org.apache.hugegraph.task.TaskStatus;
import org.apache.hugegraph.task.TaskManager;
import org.apache.hugegraph.task.TaskScheduler;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.testutil.Whitebox;
import org.apache.hugegraph.unit.FakeObjects;
import org.apache.hugegraph.util.Consumers;
import org.junit.Test;
import org.mockito.AdditionalAnswers;
import org.mockito.Mockito;

public class HugeFactoryTest {

    @Test
    public void testCleanupContinuesAfterGraphFailure() {
        StandardHugeGraph failed = Mockito.mock(StandardHugeGraph.class);
        StandardHugeGraph cleaned = Mockito.mock(StandardHugeGraph.class);
        Mockito.doThrow(new HugeException("test"))
               .when(failed).closeCurrentThreadTransaction();

        Assert.assertThrows(HugeException.class, () -> {
            HugeFactory.closeCurrentThreadTransactions(
                    Arrays.asList(failed, cleaned));
        });

        Mockito.verify(failed).closeCurrentThreadTransaction();
        Mockito.verify(cleaned).closeCurrentThreadTransaction();
    }

    @Test
    public void testGraphCloseRetriesAfterReleasedWorkerOwnerFails() throws Exception {
        this.checkGraphCloseAfterOwnerFailure(false);
    }

    @Test
    public void testGraphCloseRetriesAfterWorkerOwnerFailsBeforeRelease() throws Exception {
        this.checkGraphCloseAfterOwnerFailure(true);
    }

    private void checkGraphCloseAfterOwnerFailure(boolean beforeRelease) throws Exception {
        RegisterUtil.registerBackends();
        HugeConfig config = FakeObjects.newConfig();
        config.setProperty("backend", "memory");
        config.setProperty("serializer", "text");
        config.setProperty("store", "graph_close_owner_failure_" + (beforeRelease ? "before" : "after"));
        HugeGraph graph = HugeFactory.open(config);
        TaskScheduler scheduler = graph.taskScheduler();
        AtomicReference<GraphTransaction> workerOwner = new AtomicReference<>();
        try {
            graph.initBackend();
            graph.serverStarted(GlobalMasterInfo.master("close-owner-test"));
            HugeGraphParams params = Whitebox.getInternalState(graph, "params");
            TaskManager manager = TaskManager.instance();
            ExecutorService workers = Whitebox.getInternalState(manager, "taskExecutor");
            ThreadLocal<?> owners = Whitebox.getInternalState(graph.tx(), "transactions");
            RuntimeException first = new IllegalStateException("released task DB owner failed");
            RuntimeException second = new IllegalArgumentException("released caller owner failed");
            AtomicInteger workerClosed = new AtomicInteger();
            AtomicInteger callerClosed = new AtomicInteger();
            AtomicInteger otherWorkerClosed = new AtomicInteger();
            BackendStoreProvider provider = Mockito.spy(graph.storeProvider());
            Whitebox.setInternalState(graph, "storeProvider", provider);

            scheduler.call(() -> {
                workerOwner.set(failOwnerClose(graph, params, first, workerClosed, beforeRelease));
                return null;
            });
            workers.submit(() -> {
                GraphTransaction realOwner = params.graphTransaction();
                GraphTransaction owner = Mockito.mock(GraphTransaction.class,
                                                      AdditionalAnswers.delegatesTo(realOwner));
                Mockito.doAnswer(invocation -> {
                    realOwner.close();
                    otherWorkerClosed.incrementAndGet();
                    return null;
                }).when(owner).close();
                Whitebox.setInternalState(owners.get(), "graphTx", owner);
            }).get(10L, TimeUnit.SECONDS);
            failOwnerClose(graph, params, second, callerClosed, false);

            Throwable failure = Assert.assertThrows(HugeException.class, graph::close);
            Assert.assertSame(first, HugeException.rootCause(failure));
            Assert.assertTrue(Arrays.asList(failure.getSuppressed()).contains(second));
            Assert.assertFalse(graph.closed());
            Assert.assertSame(scheduler, manager.getScheduler(params));
            Assert.assertTrue((boolean) Whitebox.getInternalState(scheduler.serverManager(), "closed"));
            Assert.assertNull(scheduler.call(owners::get));
            Assert.assertNull(owners.get());
            Assert.assertEquals(1, workerClosed.get());
            Assert.assertEquals(1, callerClosed.get());
            // Task-worker cleanup waits for an acknowledged scheduler close.
            Assert.assertEquals(0, otherWorkerClosed.get());
            Mockito.verify(provider, Mockito.never()).close();

            if (beforeRelease) {
                // The fixture owns the native owner whose failing callback did not release it.
                scheduler.call(() -> {
                    workerOwner.getAndSet(null).close();
                    return null;
                });
            }
            graph.close();
            Assert.assertTrue(graph.closed());
            Assert.assertNull(manager.getScheduler(params));
            Assert.assertNull(scheduler.call(owners::get));
            Assert.assertNull(owners.get());
            Assert.assertEquals(1, workerClosed.get());
            Assert.assertEquals(1, callerClosed.get());
            Assert.assertEquals(1, otherWorkerClosed.get());
            Mockito.verify(provider, Mockito.times(1)).close();
        } finally {
            try {
                // Only the memory fixture owns this retained failed transaction.
                // A failed close does not claim the native owner was released.
                GraphTransaction retained = workerOwner.get();
                if (beforeRelease && retained != null) {
                    scheduler.call(() -> {
                        retained.close();
                        return null;
                    });
                }
                if (!graph.closed()) {
                    graph.close();
                }
            } finally {
                HugeFactory.remove(graph);
            }
        }
    }

    private static GraphTransaction failOwnerClose(HugeGraph graph, HugeGraphParams params,
                                                   RuntimeException failure, AtomicInteger closed,
                                                   boolean beforeRelease) {
        GraphTransaction realOwner = params.graphTransaction();
        GraphTransaction owner = Mockito.mock(GraphTransaction.class,
                                              AdditionalAnswers.delegatesTo(realOwner));
        Mockito.doAnswer(invocation -> {
            if (!beforeRelease) {
                realOwner.close();
            }
            closed.incrementAndGet();
            throw failure;
        }).when(owner).close();
        ThreadLocal<?> owners = Whitebox.getInternalState(graph.tx(), "transactions");
        Whitebox.setInternalState(owners.get(), "graphTx", owner);
        return realOwner;
    }

    @Test
    public void testGraphCloseReportsEveryTaskWorkerOwnerFailure() throws Exception {
        RegisterUtil.registerBackends();
        HugeConfig config = FakeObjects.newConfig();
        config.setProperty("backend", "memory");
        config.setProperty("serializer", "text");
        config.setProperty("store", "graph_close_task_worker_failures");
        HugeGraph graph = HugeFactory.open(config);
        CountDownLatch ready = new CountDownLatch(4);
        CountDownLatch release = new CountDownLatch(1);
        List<Future<?>> initialized = new ArrayList<>();
        try {
            graph.initBackend();
            graph.serverStarted(GlobalMasterInfo.master("close-task-worker-test"));
            HugeGraphParams params = Whitebox.getInternalState(graph, "params");
            TaskManager manager = TaskManager.instance();
            ExecutorService workers = Whitebox.getInternalState(manager, "taskExecutor");
            ThreadLocal<?> owners = Whitebox.getInternalState(graph.tx(), "transactions");
            Queue<RuntimeException> failures = new ConcurrentLinkedQueue<>();
            AtomicInteger closed = new AtomicInteger();
            for (int i = 0; i < 4; i++) {
                initialized.add(workers.submit(() -> {
                    try {
                        RuntimeException failure = new IllegalStateException(
                                "Task owner failed: " + Thread.currentThread().getName());
                        failures.add(failure);
                        failOwnerClose(graph, params, failure, closed, false);
                    } finally {
                        ready.countDown();
                    }
                    Assert.assertTrue(release.await(10L, TimeUnit.SECONDS));
                    return null;
                }));
            }
            Assert.assertTrue(ready.await(10L, TimeUnit.SECONDS));
            release.countDown();
            for (Future<?> future : initialized) {
                future.get(10L, TimeUnit.SECONDS);
            }

            Throwable failure = Assert.assertThrows(HugeException.class, graph::close);
            Throwable first = HugeException.rootCause(failure);
            Assert.assertTrue(failures.contains(first));
            Assert.assertEquals(3, first.getSuppressed().length);
            for (RuntimeException workerFailure : failures) {
                Assert.assertTrue(workerFailure == first ||
                                  Arrays.asList(first.getSuppressed()).contains(workerFailure));
            }
            Assert.assertEquals(4, closed.get());
            Assert.assertTrue(graph.closed());
            Assert.assertNull(manager.getScheduler(params));
            AtomicInteger cleared = new AtomicInteger();
            Consumers.executeOncePerThread(workers, 4, () -> {
                if (owners.get() == null) {
                    cleared.incrementAndGet();
                }
            }, 10L);
            Assert.assertEquals(4, cleared.get());
        } finally {
            release.countDown();
            try {
                if (!graph.closed()) {
                    graph.close();
                }
            } finally {
                HugeFactory.remove(graph);
            }
        }
    }

    @Test
    public void testGraphClosePreservesRollbackFailureWhenOwnerCloseFails() throws Exception {
        RegisterUtil.registerBackends();
        HugeConfig config = FakeObjects.newConfig();
        config.setProperty("backend", "memory");
        config.setProperty("serializer", "text");
        config.setProperty("store", "graph_close_rollback_and_owner_failure");
        HugeGraph graph = HugeFactory.open(config);
        try {
            graph.initBackend();
            graph.serverStarted(GlobalMasterInfo.master("close-rollback-test"));
            HugeGraphParams params = Whitebox.getInternalState(graph, "params");
            graph.tx().open();
            GraphTransaction realOwner = params.graphTransaction();
            GraphTransaction owner = Mockito.mock(GraphTransaction.class,
                                                  AdditionalAnswers.delegatesTo(realOwner));
            RuntimeException first = new IllegalStateException("Rollback failed");
            RuntimeException second = new IllegalArgumentException("Owner close failed");
            Mockito.doThrow(first).when(owner).rollback();
            Mockito.doAnswer(invocation -> {
                realOwner.close();
                throw second;
            }).when(owner).close();
            ThreadLocal<?> owners = Whitebox.getInternalState(graph.tx(), "transactions");
            Whitebox.setInternalState(owners.get(), "graphTx", owner);

            Throwable failure = Assert.assertThrows(IllegalStateException.class, graph::close);
            Assert.assertSame(first, failure);
            Assert.assertTrue(Arrays.asList(first.getSuppressed()).contains(second));
            Assert.assertTrue(graph.closed());
            Assert.assertNull(owners.get());
            Assert.assertNull(TaskManager.instance().getScheduler(params));
            Mockito.verify(owner).rollback();
            Mockito.verify(owner).close();
        } finally {
            try {
                if (!graph.closed()) {
                    graph.close();
                }
            } finally {
                HugeFactory.remove(graph);
            }
        }
    }

    @Test
    public void testLocalCloseKeepsPendingTaskPersistenceUsable() throws Exception {
        RegisterUtil.registerBackends();
        HugeConfig config = FakeObjects.newConfig();
        config.setProperty("backend", "memory");
        config.setProperty("serializer", "text");
        config.setProperty("store", "graph_close_pending_local_task");
        HugeGraph graph = HugeFactory.open(config);
        Map<Id, HugeTask<?>> pending = null;
        HugeTask<Object> task = new HugeTask<>(IdGenerator.of(9999998L), null,
                                              new TaskAndResultSchedulerTest.EmptyCallable());
        task.type("test");
        task.name("pending-task-close");
        try {
            graph.initBackend();
            graph.serverStarted(GlobalMasterInfo.master("pending-task-close-test"));
            TaskScheduler scheduler = graph.taskScheduler();
            pending = Whitebox.getInternalState(scheduler, "tasks");
            task.overwriteStatus(TaskStatus.RUNNING);
            scheduler.save(task);
            pending.put(task.id(), task);

            Assert.assertThrows(IllegalStateException.class, graph::close);
            Assert.assertFalse(graph.closed());
            HugeGraphParams params = Whitebox.getInternalState(graph, "params");
            Assert.assertSame(scheduler, TaskManager.instance().getScheduler(params));
            Assert.assertFalse((boolean) Whitebox.getInternalState(scheduler.serverManager(), "closed"));

            // This is the same persistence path a running job uses from done().
            task.overwriteStatus(TaskStatus.SUCCESS);
            scheduler.save(task);
            Assert.assertEquals(TaskStatus.SUCCESS, scheduler.task(task.id()).status());
            pending.remove(task.id());
            graph.close();
            Assert.assertTrue(graph.closed());
            Assert.assertNull(TaskManager.instance().getScheduler(params));
        } finally {
            if (pending != null) {
                pending.remove(task.id());
            }
            try {
                if (!graph.closed()) {
                    graph.close();
                }
            } finally {
                HugeFactory.remove(graph);
            }
        }
    }

    @Test
    public void testIncompleteSchedulerCloseDoesNotQueueOwnerCleanup() {
        HugeGraphParams params = Mockito.mock(HugeGraphParams.class);
        TaskScheduler scheduler = Mockito.mock(TaskScheduler.class);
        TaskManager manager = TaskManager.instance();
        Map<HugeGraphParams, TaskScheduler> schedulers = Whitebox.getInternalState(manager, "schedulers");
        schedulers.put(params, scheduler);
        try {
            Mockito.when(scheduler.close()).thenReturn(false);
            manager.closeScheduler(params);
            Assert.assertSame(scheduler, manager.getScheduler(params));
            Mockito.verify(params, Mockito.never()).closeTx();

            Mockito.when(scheduler.close()).thenReturn(true);
            manager.closeScheduler(params);
            Assert.assertNull(manager.getScheduler(params));
            Mockito.verify(params, Mockito.atLeastOnce()).closeTx();
        } finally {
            schedulers.remove(params);
        }
    }

    @Test
    public void testDistributedCloseRetriesDrainAfterTimeout() throws Exception {
        HugeGraphParams params = Mockito.mock(HugeGraphParams.class);
        Mockito.when(params.graph()).thenReturn(Mockito.mock(HugeGraph.class));
        Mockito.when(params.configuration()).thenReturn(FakeObjects.newConfig());
        ScheduledThreadPoolExecutor cron = Mockito.mock(ScheduledThreadPoolExecutor.class);
        Mockito.doReturn(Mockito.mock(ScheduledFuture.class)).when(cron).scheduleWithFixedDelay(
                Mockito.any(Runnable.class), Mockito.anyLong(), Mockito.anyLong(), Mockito.eq(TimeUnit.SECONDS));
        Mockito.doReturn(CompletableFuture.completedFuture(null)).when(cron).submit(Mockito.any(Runnable.class));
        ExecutorService executor = Mockito.mock(ExecutorService.class);
        Mockito.when(executor.isShutdown()).thenReturn(true);
        DistributedTaskScheduler scheduler = Mockito.spy(new DistributedTaskScheduler(
                params, cron, executor, executor, executor, executor, executor));
        AtomicBoolean active = new AtomicBoolean(true);
        AtomicInteger drainAttempts = new AtomicInteger();
        Mockito.doAnswer(invocation -> {
            drainAttempts.incrementAndGet();
            if (active.get()) {
                throw new TimeoutException("Task still running");
            }
            return null;
        }).when(scheduler).waitUntilAllTasksCompleted(Mockito.anyLong());

        Assert.assertFalse(scheduler.close());
        Assert.assertFalse(scheduler.close());
        Assert.assertEquals(2, drainAttempts.get());
        Assert.assertFalse((boolean) Whitebox.getInternalState(scheduler.serverManager(), "closed"));
        active.set(false);
        Assert.assertTrue(scheduler.close());
        Assert.assertEquals(3, drainAttempts.get());
        Assert.assertTrue((boolean) Whitebox.getInternalState(scheduler.serverManager(), "closed"));
        Assert.assertTrue(scheduler.close());
        Assert.assertEquals(3, drainAttempts.get());
    }
}
