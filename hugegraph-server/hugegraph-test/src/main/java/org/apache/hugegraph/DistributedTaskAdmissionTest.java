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
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.dist.RegisterUtil;
import org.apache.hugegraph.id.IdGenerator;
import org.apache.hugegraph.job.EphemeralJob;
import org.apache.hugegraph.job.SysJob;
import org.apache.hugegraph.masterelection.GlobalMasterInfo;
import org.apache.hugegraph.task.DistributedTaskScheduler;
import org.apache.hugegraph.task.HugeTask;
import org.apache.hugegraph.task.TaskAndResultSchedulerTest.EmptyCallable;
import org.apache.hugegraph.task.TaskStatus;
import org.apache.hugegraph.task.TaskScheduler;
import org.apache.hugegraph.task.TaskCallable;
import org.apache.hugegraph.task.TaskManager;
import org.apache.hugegraph.task.HugeTaskResult;
import org.apache.hugegraph.meta.MetaManager;
import org.apache.hugegraph.meta.managers.TaskMetaManager;
import org.apache.hugegraph.meta.lock.LockResult;
import org.mockito.Mockito;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.testutil.Whitebox;
import org.apache.hugegraph.unit.FakeObjects;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class DistributedTaskAdmissionTest {

    private TaskMetaManager originalLocks;
    private TaskMetaManager locks;

    @Before
    public void prepareLocks() {
        MetaManager meta = MetaManager.instance();
        this.originalLocks = Whitebox.getInternalState(meta, "taskMetaManager");
        this.locks = Mockito.mock(TaskMetaManager.class);
        Mockito.when(this.locks.tryLockTask(Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
               .thenAnswer(invocation -> {
                   LockResult acquired = new LockResult();
                   acquired.lockSuccess(true);
                   return acquired;
               });
        Whitebox.setInternalState(meta, "taskMetaManager", this.locks);
    }

    @After
    public void restoreLocks() {
        Whitebox.setInternalState(MetaManager.instance(), "taskMetaManager", this.originalLocks);
    }

    @Test
    public void testQueuedEphemeralAdmissionDrainsBeforeClose() throws Exception {
        this.checkQueuedAdmission(true);
    }

    @Test
    public void testQueuedPersistentAdmissionDrainsBeforeClose() throws Exception {
        this.checkQueuedAdmission(false);
    }

    private void checkQueuedAdmission(boolean ephemeral) throws Exception {
        try (Fixture fixture = new Fixture("queued_" + ephemeral)) {
            CountDownLatch occupied = fixture.worker.reached;
            CountDownLatch release = fixture.worker.release;
            fixture.worker.armed.set(true);
            ExecutorService closing = Executors.newSingleThreadExecutor();
            try {
                AtomicBoolean ran = new AtomicBoolean();
                HugeTask<?> admitted = task(ephemeral, ran, 9999910L);
                fixture.scheduler.schedule(admitted);
                Assert.assertTrue(occupied.await(10L, TimeUnit.SECONDS));
                Assert.assertEquals(1, fixture.scheduler.pendingTasks());
                Future<Boolean> close = closing.submit(fixture.scheduler::close);
                AtomicBoolean closed = Whitebox.getInternalState(fixture.scheduler, "closed");
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10L);
                while (!closed.get() && System.nanoTime() < deadline) {
                    Thread.yield();
                }
                Assert.assertTrue(closed.get());
                Assert.assertEquals(0, fixture.scheduler.lateSaved.get());
                for (boolean lateEphemeral : new boolean[]{true, false}) {
                    HugeTask<?> late = task(lateEphemeral, ran, 9999911L);
                    Assert.assertThrows(IllegalStateException.class, () -> fixture.scheduler.schedule(late));
                    Assert.assertEquals(TaskStatus.NEW, late.status());
                }
                Assert.assertEquals(0, fixture.scheduler.lateSaved.get());
                Assert.assertFalse(close.isDone());
                // Wait for actual cancellation, not just the earlier admission transition.
                deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10L);
                while (!admitted.isCancelled() && System.nanoTime() < deadline) {
                    Thread.yield();
                }
                Assert.assertTrue(admitted.isCancelled());
                release.countDown();
                Assert.assertTrue(close.get(10L, TimeUnit.SECONDS));
                Assert.assertFalse(ran.get());
                Assert.assertTrue(admitted.isCancelled());
                Assert.assertEquals(0, fixture.scheduler.pendingTasks());
                ThreadLocal<?> owners = Whitebox.getInternalState(fixture.graph.tx(), "transactions");
                Assert.assertNull(fixture.worker.submit(owners::get).get(10L, TimeUnit.SECONDS));
                Assert.assertNull(fixture.worker.submit(org.apache.hugegraph.task.TaskManager::getContext)
                                               .get(10L, TimeUnit.SECONDS));
                Assert.assertTrue(fixture.scheduler.close());
            } finally {
                release.countDown();
                closing.shutdownNow();
                Assert.assertTrue(closing.awaitTermination(10L, TimeUnit.SECONDS));
            }
        }
    }

    @Test
    public void testReturnedFutureQueuedCancellationReleasesAdmission() throws Exception {
        try (Fixture fixture = new Fixture("future_queued_cancel")) {
            fixture.worker.armed.set(true);
            AtomicBoolean ran = new AtomicBoolean();
            HugeTask<?> task = task(true, ran, 9999921L);
            Future<?> returned = fixture.scheduler.schedule(task);
            try {
                Assert.assertTrue(fixture.worker.reached.await(10L, TimeUnit.SECONDS));
                Assert.assertEquals(1, fixture.scheduler.pendingTasks());
                fixture.graph.schema().vertexLabel("caller").useCustomizeStringId().create();
                fixture.graph.addVertex(org.apache.tinkerpop.gremlin.structure.T.id, "uncommitted",
                                        org.apache.tinkerpop.gremlin.structure.T.label, "caller");
                Assert.assertTrue(fixture.graph.tx().isOpen());
                Assert.assertTrue(returned.cancel(true));
                Assert.assertTrue(fixture.graph.tx().isOpen());
                Assert.assertTrue(fixture.graph.vertices("uncommitted").hasNext());
                fixture.graph.tx().rollback();
                Assert.assertFalse(task.isCancelled());
                Assert.assertEquals(TaskStatus.NEW, task.status());
                Assert.assertEquals(0, fixture.scheduler.pendingTasks());
                Assert.assertTrue(fixture.scheduler.close());
                Assert.assertFalse(ran.get());
            } finally {
                fixture.worker.release.countDown();
            }
            fixture.worker.submit(() -> null).get(10L, TimeUnit.SECONDS);
            Assert.assertFalse(ran.get());
            Assert.assertEquals(0, fixture.scheduler.pendingTasks());
        }
    }

    @Test
    public void testQueuedClosePreservesCallerTransactionAndCancellationCallbacks() throws Exception {
        this.checkQueuedOwnedCancellation(true);
    }

    @Test
    public void testQueuedCancelPreservesCallerTransactionAndCancellationCallbacks() throws Exception {
        this.checkQueuedOwnedCancellation(false);
    }

    private void checkQueuedOwnedCancellation(boolean close) throws Exception {
        try (Fixture fixture = new Fixture("owned_cancel_" + close)) {
            fixture.worker.armed.set(true);
            CancellationJob callable = new CancellationJob();
            HugeTask<?> task = new HugeTask<>(IdGenerator.of(9999930L), null, callable);
            task.type("test");
            task.name("owned-cancel");
            fixture.scheduler.schedule(task);
            Assert.assertTrue(fixture.worker.reached.await(10L, TimeUnit.SECONDS));
            AtomicBoolean nestedRan = new AtomicBoolean();
            HugeTask<?> nested = task(true, nestedRan, 9999931L);
            fixture.scheduler.schedule(nested);
            // Reentrant cancellation on the same scheduler worker must not wait on itself.
            callable.onCancelled = () -> fixture.scheduler.cancel(nested);
            fixture.graph.schema().vertexLabel("caller").useCustomizeStringId().create();
            fixture.graph.addVertex(org.apache.tinkerpop.gremlin.structure.T.id, "uncommitted",
                                    org.apache.tinkerpop.gremlin.structure.T.label, "caller");
            Assert.assertTrue(fixture.graph.tx().isOpen());
            ExecutorService release = Executors.newSingleThreadExecutor();
            try {
                Future<?> gate = release.submit(() -> {
                    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10L);
                    while (callable.cancelled.get() == 0 && System.nanoTime() < deadline) {
                        Thread.yield();
                    }
                    if (close) {
                        fixture.worker.release.countDown();
                    }
                });
                if (close) {
                    Assert.assertTrue(fixture.scheduler.close());
                } else {
                    fixture.scheduler.cancel(task);
                    fixture.worker.release.countDown();
                }
                Assert.assertTrue(fixture.graph.tx().isOpen());
                Assert.assertTrue(fixture.graph.vertices("uncommitted").hasNext());
                fixture.graph.tx().rollback();
                Assert.assertEquals(1, callable.done.get());
                Assert.assertEquals(1, callable.cancelled.get());
                Assert.assertNotEquals(Thread.currentThread(), callable.cancellationThread);
                Assert.assertTrue(task.isCancelled());
                Assert.assertTrue(nested.isCancelled());
                Assert.assertFalse(callable.ran.get());
                Assert.assertFalse(nestedRan.get());
                // Query the real persisted SysJob status even after scheduler.close cleanup.
                Assert.assertEquals(TaskStatus.CANCELLED, fixture.scheduler.task(task.id(), false).status());
                fixture.scheduler.close();
                gate.get(10L, TimeUnit.SECONDS);
                Assert.assertEquals(0, fixture.scheduler.pendingTasks());
                ThreadLocal<?> owners = Whitebox.getInternalState(fixture.graph.tx(), "transactions");
                Assert.assertNull(fixture.cron.submit(owners::get).get(10L, TimeUnit.SECONDS));
                Assert.assertNull(fixture.cron.submit(org.apache.hugegraph.task.TaskManager::getContext)
                                             .get(10L, TimeUnit.SECONDS));
            } finally {
                fixture.worker.release.countDown();
                release.shutdownNow();
                Assert.assertTrue(release.awaitTermination(10L, TimeUnit.SECONDS));
            }
        }
    }

    public static class CancellationJob extends SysJob<Object> {

        protected final AtomicInteger done = new AtomicInteger();
        protected final AtomicInteger cancelled = new AtomicInteger();
        private final AtomicBoolean ran = new AtomicBoolean();
        private final AtomicInteger callbackDepth = new AtomicInteger();
        private volatile Thread cancellationThread;
        protected Runnable onCancelled = () -> { };

        @Override
        public String type() {
            return "test";
        }

        @Override
        public Object execute() throws Exception {
            this.ran.set(true);
            return null;
        }

        @Override
        protected void done() {
            this.callbackDepth.incrementAndGet();
            try {
                this.done.incrementAndGet();
                super.done();
            } finally {
                this.callbackDepth.decrementAndGet();
            }
        }

        @Override
        protected void cancelled() {
            this.callbackDepth.incrementAndGet();
            try {
                this.cancellationThread = Thread.currentThread();
                this.cancelled.incrementAndGet();
                this.onCancelled.run();
                super.cancelled();
            } finally {
                this.callbackDepth.decrementAndGet();
            }
        }
    }

    @Test
    public void testReturnedFutureActiveCancellationRetainsOwnerUntilCleanup() throws Exception {
        try (Fixture fixture = new Fixture("future_active_cancel")) {
            CountDownLatch started = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            CountDownLatch interrupted = new CountDownLatch(1);
            HugeTask<?> task = new HugeTask<>(IdGenerator.of(-9999922L), null, new EphemeralJob<Object>() {
                @Override
                public String type() {
                    return "test";
                }

                @Override
                public Object execute() {
                    this.graph().vertices().hasNext();
                    started.countDown();
                    while (release.getCount() != 0L) {
                        try {
                            Assert.assertTrue(release.await(10L, TimeUnit.SECONDS));
                        } catch (InterruptedException e) {
                            interrupted.countDown();
                        }
                    }
                    return null;
                }
            });
            task.type("test");
            task.name("active-future-cancellation");
            Future<?> returned = fixture.scheduler.schedule(task);
            try {
                Assert.assertTrue(started.await(10L, TimeUnit.SECONDS));
                Assert.assertTrue(returned.cancel(true));
                Assert.assertTrue(interrupted.await(10L, TimeUnit.SECONDS));
                Assert.assertEquals(1, fixture.scheduler.pendingTasks());
            } finally {
                release.countDown();
            }
            fixture.worker.submit(() -> null).get(10L, TimeUnit.SECONDS);
            Assert.assertEquals(0, fixture.scheduler.pendingTasks());
            ThreadLocal<?> owners = Whitebox.getInternalState(fixture.graph.tx(), "transactions");
            Assert.assertNull(fixture.worker.submit(owners::get).get(10L, TimeUnit.SECONDS));
            Assert.assertNull(fixture.worker.submit(org.apache.hugegraph.task.TaskManager::getContext)
                                           .get(10L, TimeUnit.SECONDS));
            Assert.assertTrue(fixture.scheduler.close());
        }
    }

    @Test
    public void testLostLeaseKeepsLocalExecutionReservedUntilOwnerCleanup() throws Exception {
        MetaManager meta = MetaManager.instance();
        TaskMetaManager original = Whitebox.getInternalState(meta, "taskMetaManager");
        TaskMetaManager locks = Mockito.mock(TaskMetaManager.class);
        LockResult acquired = new LockResult();
        acquired.lockSuccess(true);
        Mockito.when(locks.tryLockTask(Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
               .thenReturn(acquired);
        Mockito.when(locks.isLockedTask(Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
               .thenReturn(false);
        Whitebox.setInternalState(meta, "taskMetaManager", locks);
        try (Fixture fixture = new Fixture("lost_lease")) {
            LeaseCallable callable = new LeaseCallable();
            callable.scheduler = fixture.scheduler;
            HugeTask<Object> task = new HugeTask<>(IdGenerator.of(9999923L), null, callable);
            task.type("test");
            task.name("lost-lease-owner");
            fixture.scheduler.schedule(task);
            ExecutorService closing = Executors.newSingleThreadExecutor();
            try {
                Assert.assertTrue(callable.started.await(10L, TimeUnit.SECONDS));
                Assert.assertEquals(TaskStatus.RUNNING, fixture.scheduler.task(task.id(), false).status());
                fixture.scheduler.cronSchedule();
                Assert.assertTrue(fixture.scheduler.leaseRecovered.get());
                Assert.assertEquals(TaskStatus.FAILED, fixture.scheduler.task(task.id(), false).status());
                Assert.assertEquals(1, fixture.scheduler.pendingTasks());
                Future<Boolean> close = closing.submit(fixture.scheduler::close);
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10L);
                AtomicBoolean closingAdmission = Whitebox.getInternalState(fixture.scheduler, "closed");
                while (!closingAdmission.get() && System.nanoTime() < deadline) {
                    Thread.yield();
                }
                Assert.assertTrue(closingAdmission.get());
                Assert.assertFalse(task.isCancelled());
                Assert.assertEquals(TaskStatus.FAILED, fixture.scheduler.task(task.id(), false).status());
                Assert.assertFalse(close.isDone());
                Assert.assertEquals(1, fixture.scheduler.pendingTasks());
                callable.release.countDown();
                Assert.assertTrue(close.get(10L, TimeUnit.SECONDS));
                Assert.assertEquals(0, fixture.scheduler.pendingTasks());
                ThreadLocal<?> owners = Whitebox.getInternalState(fixture.graph.tx(), "transactions");
                Assert.assertNull(fixture.worker.submit(owners::get).get(10L, TimeUnit.SECONDS));
            } finally {
                callable.release.countDown();
                closing.shutdownNow();
                Assert.assertTrue(closing.awaitTermination(10L, TimeUnit.SECONDS));
            }
        } finally {
            Whitebox.setInternalState(meta, "taskMetaManager", original);
        }
    }

    public static class LeaseCallable extends TaskCallable<Object> {

        private final CountDownLatch started = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);
        private CountingScheduler scheduler;

        @Override
        public Object call() {
            this.graph().vertices().hasNext();
            this.scheduler.save(this.task());
            this.started.countDown();
            while (this.release.getCount() != 0L) {
                try {
                    Assert.assertTrue(this.release.await(10L, TimeUnit.SECONDS));
                } catch (InterruptedException ignored) {
                    // Deliberately retain the real owner until the test releases the callable.
                }
            }
            return null;
        }
    }

    @Test
    public void testSharedWorkerSaturationAccountsForAcceptedPersistentTask() throws Exception {
        try (Fixture other = new Fixture("shared_other");
             Fixture target = new Fixture("shared_target", other.worker)) {
            other.worker.armed.set(true);
            AtomicBoolean otherRan = new AtomicBoolean();
            HugeTask<?> otherTask = task(true, otherRan, 9999940L);
            other.scheduler.schedule(otherTask);
            Assert.assertTrue(other.worker.reached.await(10L, TimeUnit.SECONDS));
            HugeTask<?> admitted = task(false, new AtomicBoolean(), 9999941L);
            target.scheduler.schedule(admitted);
            Assert.assertEquals(TaskStatus.NEW, target.scheduler.task(admitted.id(), false).status());
            Assert.assertEquals(1, target.scheduler.pendingTasks());
            Assert.assertEquals(1, other.worker.getQueue().size());
            Assert.assertEquals(1, other.scheduler.pendingTasks());
            // Another node's durable NEW backlog must not be drained into our busy pool.
            HugeTask<?> backlog = task(false, new AtomicBoolean(), 9999942L);
            target.scheduler.save(backlog);
            target.scheduler.cronSchedule();
            Assert.assertEquals(TaskStatus.NEW, target.scheduler.task(backlog.id(), false).status());
            Assert.assertEquals(1, target.scheduler.pendingTasks());
            Assert.assertEquals(1, other.worker.getQueue().size());
            ExecutorService closing = Executors.newSingleThreadExecutor();
            try {
                Future<Boolean> close = closing.submit(target.scheduler::close);
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10L);
                while (!admitted.isCancelled() && System.nanoTime() < deadline) {
                    Thread.yield();
                }
                Assert.assertTrue(admitted.isCancelled());
                // The target's inactive wrapper has no owner: its callbacks/save are
                // finished, so closing it need not wait for another graph's worker.
                Assert.assertTrue(close.get(10L, TimeUnit.SECONDS));
                Assert.assertEquals(0, target.scheduler.pendingTasks());
                Assert.assertTrue(other.worker.getQueue().isEmpty());
                Assert.assertFalse(otherTask.isCancelled());
                other.worker.release.countDown();
                other.worker.submit(() -> null).get(10L, TimeUnit.SECONDS);
                Assert.assertEquals(0, target.scheduler.pendingTasks());
                Assert.assertEquals(0, other.scheduler.pendingTasks());
                Assert.assertTrue(otherRan.get());
                Assert.assertEquals(TaskStatus.CANCELLED,
                                    target.scheduler.task(admitted.id(), false).status());
            } finally {
                other.worker.release.countDown();
                closing.shutdownNow();
                Assert.assertTrue(closing.awaitTermination(10L, TimeUnit.SECONDS));
            }
        }
    }

    @Test
    public void testQueuedPersistentCancellationReleasesAfterFinalSave() throws Exception {
        try (Fixture other = new Fixture("cancel_other");
             Fixture target = new Fixture("cancel_target", other.worker)) {
            other.worker.armed.set(true);
            AtomicBoolean otherRan = new AtomicBoolean();
            HugeTask<?> otherTask = task(true, otherRan, 9999950L);
            other.scheduler.schedule(otherTask);
            Assert.assertTrue(other.worker.reached.await(10L, TimeUnit.SECONDS));
            CancellationJob callable = new CancellationJob();
            HugeTask<?> admitted = new HugeTask<>(IdGenerator.of(9999951L), null, callable);
            admitted.type("test");
            admitted.name("queued-persistent-cancel");
            target.scheduler.schedule(admitted);
            HugeTask<?> stale = target.scheduler.task(admitted.id(), false);
            AtomicBoolean nestedReturned = new AtomicBoolean();
            callable.onCancelled = () -> {
                target.scheduler.cancel(stale);
                Assert.assertEquals(1, target.scheduler.pendingTasks());
                Assert.assertEquals(1, other.worker.getQueue().size());
                nestedReturned.set(true);
            };
            CountDownLatch finalSave = new CountDownLatch(1);
            CountDownLatch finishSave = new CountDownLatch(1);
            AtomicBoolean finalSaved = new AtomicBoolean();
            target.scheduler.beforeSave = () -> {
                if (admitted.status() == TaskStatus.CANCELLED &&
                    Thread.currentThread() == callable.cancellationThread &&
                    callable.callbackDepth.get() == 0 && finalSaved.compareAndSet(false, true)) {
                    finalSave.countDown();
                    try {
                        Assert.assertTrue(finishSave.await(10L, TimeUnit.SECONDS));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException(e);
                    }
                }
            };
            ExecutorService cancelling = Executors.newFixedThreadPool(2);
            try {
                Future<?> cancel = cancelling.submit(() -> target.scheduler.cancel(admitted));
                Assert.assertTrue(finalSave.await(10L, TimeUnit.SECONDS));
                Assert.assertEquals(1, callable.done.get());
                Assert.assertEquals(1, callable.cancelled.get());
                Assert.assertTrue(nestedReturned.get());
                Assert.assertEquals(TaskStatus.CANCELLED, target.scheduler.task(admitted.id(), false).status());
                // Callbacks have saved, but the request's final DB operation still owns admission.
                Assert.assertEquals(1, target.scheduler.pendingTasks());
                Assert.assertEquals(1, other.worker.getQueue().size());
                int queuedBefore = target.cron.getQueue().size();
                Future<?> repeated = cancelling.submit(() -> target.scheduler.cancel(stale));
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10L);
                while (target.cron.getQueue().size() <= queuedBefore && !repeated.isDone() &&
                       System.nanoTime() < deadline) {
                    Thread.yield();
                }
                Assert.assertFalse(repeated.isDone());
                Assert.assertTrue(target.cron.getQueue().size() > queuedBefore);
                Assert.assertEquals(1, target.scheduler.pendingTasks());
                Assert.assertEquals(1, other.worker.getQueue().size());
                finishSave.countDown();
                cancel.get(10L, TimeUnit.SECONDS);
                repeated.get(10L, TimeUnit.SECONDS);
                Assert.assertEquals(0, target.scheduler.pendingTasks());
                Assert.assertTrue(other.worker.getQueue().isEmpty());
                Assert.assertEquals(TaskStatus.CANCELLED, target.scheduler.task(admitted.id(), false).status());
                Assert.assertFalse(callable.ran.get());
                Assert.assertFalse(otherTask.isCancelled());
                // A fresh same-id retry has a new ticket; old cancellation cannot remove it.
                HugeTask<?> replacement = task(false, new AtomicBoolean(), 9999951L);
                target.scheduler.schedule(replacement);
                Assert.assertEquals(TaskStatus.NEW, target.scheduler.task(replacement.id(), false).status());
                Assert.assertEquals(1, target.scheduler.pendingTasks());
                Assert.assertEquals(1, other.worker.getQueue().size());
                target.scheduler.cancel(replacement);
                Assert.assertEquals(0, target.scheduler.pendingTasks());
                Assert.assertTrue(other.worker.getQueue().isEmpty());
                other.worker.release.countDown();
                other.worker.submit(() -> null).get(10L, TimeUnit.SECONDS);
                Assert.assertTrue(otherRan.get());
                Assert.assertEquals(0, other.scheduler.pendingTasks());
            } finally {
                finishSave.countDown();
                other.worker.release.countDown();
                cancelling.shutdownNow();
                Assert.assertTrue(cancelling.awaitTermination(10L, TimeUnit.SECONDS));
            }
        }
    }

    @Test
    public void testCancelledQueuedTaskReleasesAfterFinalPersistenceFailure() throws Exception {
        try (Fixture other = new Fixture("failed_cancel_other");
             Fixture target = new Fixture("failed_cancel_target", other.worker)) {
            other.worker.armed.set(true);
            HugeTask<?> otherTask = task(true, new AtomicBoolean(), 9999960L);
            other.scheduler.schedule(otherTask);
            Assert.assertTrue(other.worker.reached.await(10L, TimeUnit.SECONDS));
            CancellationJob callable = new CancellationJob();
            HugeTask<?> admitted = new HugeTask<>(IdGenerator.of(9999961L), null, callable);
            admitted.type("test");
            admitted.name("cancel-persistence-failure");
            target.scheduler.schedule(admitted);
            target.graph.schema().vertexLabel("caller").useCustomizeStringId().create();
            target.graph.addVertex(org.apache.tinkerpop.gremlin.structure.T.id, "uncommitted",
                                   org.apache.tinkerpop.gremlin.structure.T.label, "caller");
            ScheduledThreadPoolExecutor rejecting = Mockito.mock(ScheduledThreadPoolExecutor.class);
            RejectedExecutionException dispatchFailure = new RejectedExecutionException("cancel owner rejected");
            Mockito.doThrow(dispatchFailure).when(rejecting)
                   .submit(Mockito.<java.util.concurrent.Callable<Object>>any());
            Whitebox.setInternalState(target.scheduler, "schedulerExecutor", rejecting);
            try {
                org.apache.hugegraph.exception.HugeException failure = Assert.assertThrows(
                        org.apache.hugegraph.exception.HugeException.class,
                        () -> target.scheduler.cancel(admitted));
                Assert.assertSame(dispatchFailure, org.apache.hugegraph.exception.HugeException.rootCause(failure));
                Assert.assertFalse(admitted.isCancelled());
                Assert.assertEquals(TaskStatus.CANCELLING, admitted.status());
                Assert.assertEquals(1, target.scheduler.pendingTasks());
                Assert.assertEquals(1, other.worker.getQueue().size());
            } finally {
                Whitebox.setInternalState(target.scheduler, "schedulerExecutor", target.cron);
            }
            IllegalStateException persistenceFailure = new IllegalStateException("final cancel save failed");
            AtomicBoolean failed = new AtomicBoolean();
            target.scheduler.beforeSave = () -> {
                if (admitted.isCancelled() && callable.callbackDepth.get() == 0 &&
                    Thread.currentThread() == callable.cancellationThread && failed.compareAndSet(false, true)) {
                    throw persistenceFailure;
                }
            };
            org.apache.hugegraph.exception.HugeException failure = Assert.assertThrows(
                    org.apache.hugegraph.exception.HugeException.class,
                    () -> target.scheduler.cancel(admitted));
            Assert.assertSame(persistenceFailure, org.apache.hugegraph.exception.HugeException.rootCause(failure));
            Assert.assertTrue(admitted.isCancelled());
            Assert.assertEquals(1, callable.done.get());
            Assert.assertEquals(1, callable.cancelled.get());
            Assert.assertEquals(TaskStatus.CANCELLED, target.scheduler.task(admitted.id(), false).status());
            Assert.assertEquals(0, target.scheduler.pendingTasks());
            Assert.assertTrue(other.worker.getQueue().isEmpty());
            Assert.assertTrue(target.graph.tx().isOpen());
            Assert.assertTrue(target.graph.vertices("uncommitted").hasNext());
            target.graph.tx().rollback();
            target.scheduler.beforeSave = () -> { };
            HugeTask<?> replacement = task(false, new AtomicBoolean(), 9999961L);
            target.scheduler.schedule(replacement);
            Assert.assertEquals(TaskStatus.NEW, target.scheduler.task(replacement.id(), false).status());
            Assert.assertEquals(1, target.scheduler.pendingTasks());
            target.scheduler.cancel(replacement);
            Assert.assertEquals(0, target.scheduler.pendingTasks());
            Assert.assertTrue(other.worker.getQueue().isEmpty());
        }
    }

    @Test
    public void testQueuedRemoteOwnerCancellationRetainsCoordinationUntilCloseRetry() throws Exception {
        AtomicBoolean available = new AtomicBoolean();
        Mockito.when(this.locks.tryLockTask(Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
               .thenAnswer(invocation -> {
                   LockResult acquired = new LockResult();
                   acquired.lockSuccess(available.get());
                   return acquired;
               });
        Mockito.when(this.locks.isLockedTask(Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
               .thenAnswer(invocation -> !available.get());
        try (Fixture other = new Fixture("remote_other");
             Fixture target = new Fixture("remote_target", other.worker)) {
            other.worker.armed.set(true);
            other.scheduler.schedule(task(true, new AtomicBoolean(), 9999970L));
            Assert.assertTrue(other.worker.reached.await(10L, TimeUnit.SECONDS));
            CancellationJob callable = new CancellationJob();
            HugeTask<?> admitted = new HugeTask<>(IdGenerator.of(9999971L), null, callable);
            admitted.type("test");
            admitted.name("remote-owner-cancellation");
            target.scheduler.schedule(admitted);
            HugeTask<?> remote = target.scheduler.task(admitted.id(), false);
            remote.overwriteStatus(TaskStatus.RUNNING);
            target.scheduler.save(remote);
            HugeTask<?> cancellation = target.scheduler.task(admitted.id(), false);
            target.scheduler.cancel(cancellation);
            Assert.assertEquals(TaskStatus.NEW, admitted.status());
            target.scheduler.cronSchedule();
            Assert.assertEquals(TaskStatus.CANCELLING, target.scheduler.task(admitted.id(), false).status());
            Assert.assertFalse(admitted.isCancelled());
            Assert.assertEquals(0, callable.cancelled.get());
            Assert.assertEquals(1, target.scheduler.pendingTasks());
            Assert.assertEquals(1, other.worker.getQueue().size());
            Assert.assertThrows(IllegalStateException.class,
                                () -> target.scheduler.delete(admitted.id(), true));
            Assert.assertEquals(TaskStatus.CANCELLING, target.scheduler.task(admitted.id(), false).status());
            other.worker.release.countDown();
            other.worker.submit(() -> null).get(10L, TimeUnit.SECONDS);
            // A failed local lock attempt must not drop the parked coordination ticket.
            Assert.assertEquals(1, target.scheduler.pendingTasks());
            Assert.assertFalse(target.scheduler.close());
            Assert.assertEquals(1, target.scheduler.pendingTasks());
            Assert.assertEquals(0, callable.cancelled.get());
            available.set(true);
            Assert.assertTrue(target.scheduler.close());
            Assert.assertEquals(TaskStatus.CANCELLED, target.scheduler.task(admitted.id(), false).status());
            Assert.assertEquals(1, callable.done.get());
            Assert.assertEquals(1, callable.cancelled.get());
            Assert.assertFalse(callable.ran.get());
            Assert.assertEquals(0, target.scheduler.pendingTasks());
        }
    }

    @Test
    public void testRemoteTerminalCoordinationReleasesParkedTicketOnCron() throws Exception {
        this.checkRemoteCoordinationCompletion(false);
    }

    @Test
    public void testMissingRemoteRecordReleasesParkedTicketOnCron() throws Exception {
        this.checkRemoteCoordinationCompletion(true);
    }

    private void checkRemoteCoordinationCompletion(boolean deleted) throws Exception {
        AtomicBoolean available = new AtomicBoolean();
        Mockito.when(this.locks.tryLockTask(Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
               .thenAnswer(invocation -> {
                   LockResult acquired = new LockResult();
                   acquired.lockSuccess(available.get());
                   return acquired;
               });
        try (Fixture other = new Fixture("terminal_other_" + deleted);
             Fixture target = new Fixture("terminal_target_" + deleted, other.worker)) {
            other.worker.armed.set(true);
            other.scheduler.schedule(task(true, new AtomicBoolean(), 9999975L));
            Assert.assertTrue(other.worker.reached.await(10L, TimeUnit.SECONDS));
            CancellationJob callable = new CancellationJob();
            HugeTask<?> admitted = new HugeTask<>(IdGenerator.of(9999976L), null, callable);
            admitted.type("test");
            admitted.name("remote-terminal-coordination");
            target.scheduler.schedule(admitted);
            target.scheduler.cancel(admitted);
            other.worker.release.countDown();
            other.worker.submit(() -> null).get(10L, TimeUnit.SECONDS);
            Assert.assertEquals(1, target.scheduler.pendingTasks());
            // Simulate the actual remote owner's durable completion while our transport lock is held.
            HugeTask<?> remote = target.scheduler.task(admitted.id(), false);
            remote.overwriteStatus(TaskStatus.CANCELLED);
            target.scheduler.save(remote);
            if (deleted) {
                target.scheduler.deleteRemoteRecord(admitted.id());
            }
            available.set(true);
            target.scheduler.cronSchedule();
            Assert.assertEquals(0, target.scheduler.pendingTasks());
            Assert.assertEquals(0, callable.done.get());
            Assert.assertEquals(0, callable.cancelled.get());
            Assert.assertFalse(admitted.isCancelled());
            if (deleted) {
                Assert.assertThrows(org.apache.hugegraph.exception.NotFoundException.class,
                                    () -> target.scheduler.task(admitted.id(), false));
            } else {
                Assert.assertEquals(TaskStatus.CANCELLED, target.scheduler.task(admitted.id(), false).status());
            }
        }
    }

    @Test
    public void testCloseRetriesCompletedParkedTaskWithoutCron() throws Exception {
        try (Fixture fixture = new Fixture("completed_parked_close")) {
            FailingJob callable = new FailingJob();
            HugeTask<?> admitted = new HugeTask<>(IdGenerator.of(9999977L), null, callable);
            admitted.type("test");
            admitted.name("completed-parked-close");
            fixture.scheduler.schedule(admitted);
            Assert.assertTrue(callable.entered.await(10L, TimeUnit.SECONDS));
            ScheduledThreadPoolExecutor rejecting = Mockito.mock(ScheduledThreadPoolExecutor.class);
            Mockito.doThrow(new RejectedExecutionException("cancel owner rejected")).when(rejecting)
                   .submit(Mockito.<java.util.concurrent.Callable<Object>>any());
            Whitebox.setInternalState(fixture.scheduler, "schedulerExecutor", rejecting);
            try {
                Assert.assertThrows(org.apache.hugegraph.exception.HugeException.class,
                                    () -> fixture.scheduler.cancel(admitted));
            } finally {
                Whitebox.setInternalState(fixture.scheduler, "schedulerExecutor", fixture.cron);
                callable.release.countDown();
            }
            fixture.worker.submit(() -> null).get(10L, TimeUnit.SECONDS);
            Assert.assertTrue(admitted.completed());
            Assert.assertEquals(TaskStatus.FAILED, admitted.status());
            Assert.assertEquals(1, fixture.scheduler.pendingTasks());
            Assert.assertEquals(1, callable.done.get());
            Assert.assertSame(fixture.scheduler, fixture.graph.taskScheduler());
            String outcome = fixture.scheduler.separateResult(admitted.id());
            Assert.assertNotNull(outcome);
            Assert.assertContains("terminal failure", outcome);
            Assert.assertEquals(outcome, fixture.scheduler.task(admitted.id(), true).result());
            Assert.assertNull(fixture.scheduler.task(admitted.id(), false).result());
            LockResult heldElsewhere = new LockResult();
            Mockito.when(this.locks.tryLockTask(Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
                   .thenReturn(heldElsewhere);
            Assert.assertFalse(fixture.scheduler.close());
            Assert.assertEquals(1, fixture.scheduler.pendingTasks());
            Assert.assertTrue(fixture.cron.getQueue().isEmpty());
            LockResult available = new LockResult();
            available.lockSuccess(true);
            Mockito.when(this.locks.tryLockTask(Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
                   .thenReturn(available);
            Assert.assertTrue(fixture.scheduler.close());
            Assert.assertEquals(0, fixture.scheduler.pendingTasks());
            Assert.assertEquals(0, callable.cancelled.get());
            Assert.assertEquals(TaskStatus.FAILED, fixture.scheduler.task(admitted.id(), false).status());
            Assert.assertEquals(outcome, fixture.scheduler.separateResult(admitted.id()));
        }
    }

    public static class FailingJob extends CancellationJob {

        private final CountDownLatch entered = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);

        @Override
        public Object execute() throws InterruptedException {
            this.entered.countDown();
            Assert.assertTrue(this.release.await(10L, TimeUnit.SECONDS));
            throw new IllegalStateException("terminal failure");
        }
    }

    @Test
    public void testAdoptedLocalLockChecksDurableTerminalBeforeCallbacks() throws Exception {
        try (Fixture fixture = new Fixture("adopt_terminal")) {
            CountDownLatch query = new CountDownLatch(1);
            CountDownLatch finishQuery = new CountDownLatch(1);
            AtomicBoolean once = new AtomicBoolean();
            fixture.scheduler.beforeTask = () -> {
                if (Thread.currentThread() == fixture.worker.owner && once.compareAndSet(false, true)) {
                    query.countDown();
                    try {
                        Assert.assertTrue(finishQuery.await(10L, TimeUnit.SECONDS));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException(e);
                    }
                }
            };
            CancellationJob callable = new CancellationJob();
            HugeTask<?> admitted = new HugeTask<>(IdGenerator.of(9999978L), null, callable);
            admitted.type("test");
            admitted.name("adopted-lock-terminal");
            fixture.scheduler.schedule(admitted);
            try {
                Assert.assertTrue(query.await(10L, TimeUnit.SECONDS));
                HugeTask<?> remote = fixture.scheduler.task(admitted.id(), false);
                remote.overwriteStatus(TaskStatus.SUCCESS);
                fixture.scheduler.save(remote);
                fixture.scheduler.cancel(admitted);
                Assert.assertEquals(TaskStatus.SUCCESS, fixture.scheduler.task(admitted.id(), false).status());
                Assert.assertEquals(0, callable.done.get());
                Assert.assertEquals(0, callable.cancelled.get());
                Assert.assertFalse(admitted.isCancelled());
                Assert.assertEquals(1, fixture.scheduler.pendingTasks());
            } finally {
                finishQuery.countDown();
            }
            fixture.worker.submit(() -> null).get(10L, TimeUnit.SECONDS);
            Assert.assertEquals(0, fixture.scheduler.pendingTasks());
            Assert.assertFalse(callable.ran.get());
        }
    }

    @Test
    public void testFailedUnmarkedDeleteAllowsRecoveredCronDispatch() throws Exception {
        AtomicBoolean available = new AtomicBoolean();
        Mockito.when(this.locks.tryLockTask(Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
               .thenAnswer(invocation -> {
                   LockResult acquired = new LockResult();
                   acquired.lockSuccess(available.get());
                   return acquired;
               });
        try (Fixture fixture = new Fixture("failed_delete_new")) {
            fixture.worker.armed.set(true);
            HugeTask<?> admitted = new HugeTask<>(IdGenerator.of(9999990L), null, new RecoveryJob());
            admitted.type("test");
            admitted.name("failed-unmarked-delete");
            fixture.scheduler.schedule(admitted);
            Assert.assertTrue(fixture.worker.reached.await(10L, TimeUnit.SECONDS));
            Assert.assertThrows(IllegalStateException.class,
                                () -> fixture.scheduler.delete(admitted.id(), false));
            fixture.worker.release.countDown();
            fixture.worker.submit(() -> null).get(10L, TimeUnit.SECONDS);
            Assert.assertEquals(TaskStatus.NEW, fixture.scheduler.task(admitted.id(), false).status());
            available.set(true);
            fixture.scheduler.cronSchedule();
            fixture.worker.submit(() -> null).get(10L, TimeUnit.SECONDS);
            Assert.assertEquals(TaskStatus.SUCCESS, fixture.scheduler.task(admitted.id(), false).status());
            Assert.assertEquals("\"recovered\"", fixture.scheduler.task(admitted.id(), true).result());
            Assert.assertEquals("\"recovered\"", fixture.scheduler.separateResult(admitted.id()));
            Assert.assertEquals(0, fixture.scheduler.pendingTasks());
        }
    }

    @Test
    public void testDeleteRechecksFinishedTaskBeforeAsynchronousOwnerClaim() throws Exception {
        try (Fixture fixture = new Fixture("delete_finished_claim")) {
            fixture.worker.armed.set(true);
            HugeTask<?> admitted = new HugeTask<>(IdGenerator.of(9999991L), null, new RecoveryJob());
            admitted.type("test");
            admitted.name("delete-finished-before-claim");
            fixture.scheduler.schedule(admitted);
            Assert.assertTrue(fixture.worker.reached.await(10L, TimeUnit.SECONDS));
            CountDownLatch occupied = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            Future<?> gate = fixture.cron.submit(() -> {
                occupied.countDown();
                Assert.assertTrue(release.await(10L, TimeUnit.SECONDS));
                return null;
            });
            Assert.assertTrue(occupied.await(10L, TimeUnit.SECONDS));
            int before = fixture.cron.getQueue().size();
            ExecutorService deleting = Executors.newSingleThreadExecutor();
            try {
                Future<?> delete = deleting.submit(() -> fixture.scheduler.delete(admitted.id(), false));
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10L);
                while (fixture.cron.getQueue().size() <= before && System.nanoTime() < deadline) {
                    Thread.yield();
                }
                Assert.assertTrue(fixture.cron.getQueue().size() > before);
                fixture.worker.release.countDown();
                fixture.worker.submit(() -> null).get(10L, TimeUnit.SECONDS);
                Assert.assertEquals(TaskStatus.SUCCESS, fixture.scheduler.task(admitted.id(), false).status());
                release.countDown();
                gate.get(10L, TimeUnit.SECONDS);
                Assert.assertNotNull(delete.get(10L, TimeUnit.SECONDS));
                Assert.assertThrows(org.apache.hugegraph.exception.NotFoundException.class,
                                    () -> fixture.scheduler.task(admitted.id(), false));
                Assert.assertNull(fixture.scheduler.separateResult(admitted.id()));
                Assert.assertEquals(0, fixture.scheduler.pendingTasks());
            } finally {
                release.countDown();
                fixture.worker.release.countDown();
                deleting.shutdownNow();
                Assert.assertTrue(deleting.awaitTermination(10L, TimeUnit.SECONDS));
            }
        }
    }

    @Test
    public void testFailedDeleteMarkerKeepsExplicitRetry() throws Exception {
        try (Fixture fixture = new Fixture("delete_marker_retry")) {
            CountingScheduler original = fixture.scheduler;
            CountingScheduler scheduler = Mockito.spy(original);
            Assert.assertTrue(fixture.registry.replace(fixture.params, original, scheduler));
            Whitebox.setInternalState(fixture, "scheduler", scheduler);
            fixture.worker.armed.set(true);
            HugeTask<?> admitted = new HugeTask<>(IdGenerator.of(9999992L), null, new RecoveryJob());
            admitted.type("test");
            admitted.name("delete-marker-retry");
            scheduler.schedule(admitted);
            Assert.assertTrue(fixture.worker.reached.await(10L, TimeUnit.SECONDS));
            IllegalStateException markerFailure = new IllegalStateException("marker save failed");
            AtomicBoolean failed = new AtomicBoolean();
            Mockito.doAnswer(invocation -> {
                if (admitted.status() == TaskStatus.DELETING && failed.compareAndSet(false, true)) {
                    throw markerFailure;
                }
                return invocation.callRealMethod();
            }).when(scheduler).call(Mockito.<java.util.concurrent.Callable<Object>>any());
            org.apache.hugegraph.exception.HugeException failure = Assert.assertThrows(
                    org.apache.hugegraph.exception.HugeException.class,
                    () -> scheduler.delete(admitted.id(), false));
            Assert.assertSame(markerFailure, org.apache.hugegraph.exception.HugeException.rootCause(failure));
            Assert.assertEquals(TaskStatus.NEW, scheduler.task(admitted.id(), false).status());
            Assert.assertEquals(TaskStatus.DELETING, admitted.status());
            Assert.assertEquals(1, scheduler.pendingTasks());
            Assert.assertFalse(admitted.isCancelled());
            Assert.assertEquals(TaskStatus.DELETING, scheduler.delete(admitted.id(), false).status());
            Assert.assertEquals(0, scheduler.pendingTasks());
            Assert.assertEquals(TaskStatus.DELETING, scheduler.task(admitted.id(), false).status());
            scheduler.cronSchedule();
            Assert.assertThrows(org.apache.hugegraph.exception.NotFoundException.class,
                                () -> scheduler.task(admitted.id(), false));
        }
    }

    public static class RecoveryJob extends CancellationJob {

        @Override
        public Object execute() throws Exception {
            super.execute();
            return "recovered";
        }
    }

    @Test
    public void testQueuedDeleteReleasesAfterOwnerCloseFailure() throws Exception {
        this.checkDeletingCloseFailure(false);
    }

    @Test
    public void testCronDeletingReleasesAfterOwnerCloseFailure() throws Exception {
        this.checkDeletingCloseFailure(true);
    }

    private void checkDeletingCloseFailure(boolean cron) throws Exception {
        try (Fixture other = new Fixture("delete_close_other_" + cron);
             Fixture target = new Fixture("delete_close_target_" + cron, other.worker)) {
            other.worker.armed.set(true);
            other.scheduler.schedule(task(true, new AtomicBoolean(), 9999980L));
            Assert.assertTrue(other.worker.reached.await(10L, TimeUnit.SECONDS));
            CancellationJob callable = new CancellationJob();
            HugeTask<?> admitted = new HugeTask<>(IdGenerator.of(9999981L), null, callable);
            admitted.type("test");
            admitted.name("delete-close-failure");
            target.scheduler.schedule(admitted);
            HugeGraphParams params = Whitebox.getInternalState(target.scheduler, "graph");
            HugeGraphParams failing = Mockito.mock(HugeGraphParams.class,
                                                  org.mockito.AdditionalAnswers.delegatesTo(params));
            IllegalStateException closeFailure = new IllegalStateException("owner close failed");
            AtomicBoolean once = new AtomicBoolean();
            Mockito.doAnswer(invocation -> {
                params.closeTx();
                if (once.compareAndSet(false, true)) {
                    throw closeFailure;
                }
                return null;
            }).when(failing).closeTx();
            Whitebox.setInternalState(target.scheduler, "graph", failing);
            try {
                if (cron) {
                    HugeTask<?> deleting = target.scheduler.task(admitted.id(), false);
                    deleting.overwriteStatus(TaskStatus.DELETING);
                    target.scheduler.save(deleting);
                }
                org.apache.hugegraph.exception.HugeException failure = Assert.assertThrows(
                        org.apache.hugegraph.exception.HugeException.class, () -> {
                            if (cron) {
                                target.scheduler.cronSchedule();
                            } else {
                                target.scheduler.delete(admitted.id(), true);
                            }
                        });
                Assert.assertSame(closeFailure, org.apache.hugegraph.exception.HugeException.rootCause(failure));
                Assert.assertTrue(admitted.isCancelled());
                Assert.assertEquals(1, callable.done.get());
                Assert.assertEquals(0, callable.cancelled.get());
                Assert.assertEquals(0, target.scheduler.pendingTasks());
                Assert.assertTrue(other.worker.getQueue().isEmpty());
                Assert.assertEquals(TaskStatus.DELETING, target.scheduler.task(admitted.id(), false).status());
                target.scheduler.cronSchedule();
                Assert.assertThrows(org.apache.hugegraph.exception.NotFoundException.class,
                                    () -> target.scheduler.task(admitted.id(), false));
            } finally {
                Whitebox.setInternalState(target.scheduler, "graph", params);
            }
        }
    }

    @Test
    public void testActiveOwnerDefersUnlockUntilCancellationPersistenceCompletes() throws Exception {
        AtomicInteger unlocked = new AtomicInteger();
        Mockito.doAnswer(invocation -> {
            unlocked.incrementAndGet();
            return null;
        }).when(this.locks).unlockTask(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(),
                                      Mockito.any(LockResult.class));
        try (Fixture fixture = new Fixture("owner_unlock")) {
            LockHoldingJob callable = new LockHoldingJob();
            HugeTask<?> task = new HugeTask<>(IdGenerator.of(9999985L), null, callable);
            task.type("test");
            task.name("owner-unlock-cancellation");
            fixture.scheduler.schedule(task);
            Assert.assertTrue(callable.entered.await(10L, TimeUnit.SECONDS));
            CountDownLatch callback = new CountDownLatch(1);
            CountDownLatch finishCallback = new CountDownLatch(1);
            callable.onCancelled = () -> {
                callback.countDown();
                try {
                    Assert.assertTrue(finishCallback.await(10L, TimeUnit.SECONDS));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(e);
                }
            };
            ExecutorService cancelling = Executors.newSingleThreadExecutor();
            try {
                Future<?> cancel = cancelling.submit(() -> fixture.scheduler.cancel(task));
                Assert.assertTrue(callback.await(10L, TimeUnit.SECONDS));
                callable.release.countDown();
                fixture.worker.submit(() -> null).get(10L, TimeUnit.SECONDS);
                Assert.assertEquals(0, unlocked.get());
                Assert.assertEquals(1, fixture.scheduler.pendingTasks());
                finishCallback.countDown();
                cancel.get(10L, TimeUnit.SECONDS);
                Assert.assertEquals(1, unlocked.get());
                Assert.assertEquals(0, fixture.scheduler.pendingTasks());
                Assert.assertEquals(TaskStatus.CANCELLED, fixture.scheduler.task(task.id(), false).status());
            } finally {
                callable.release.countDown();
                finishCallback.countDown();
                cancelling.shutdownNow();
                Assert.assertTrue(cancelling.awaitTermination(10L, TimeUnit.SECONDS));
            }
        }
    }

    public static class LockHoldingJob extends CancellationJob {

        private final CountDownLatch entered = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);

        @Override
        public Object execute() {
            this.entered.countDown();
            while (this.release.getCount() != 0L) {
                try {
                    Assert.assertTrue(this.release.await(10L, TimeUnit.SECONDS));
                } catch (InterruptedException ignored) {
                    // Retain the real execution owner until released by the test.
                }
            }
            return null;
        }
    }

    @Test
    public void testFailedCancellationLockReleasesFinishedRunnerToken() throws Exception {
        CountDownLatch runnerAcquired = new CountDownLatch(1);
        CountDownLatch publishRunner = new CountDownLatch(1);
        CountDownLatch cancellationAcquiring = new CountDownLatch(1);
        CountDownLatch returnFailedLock = new CountDownLatch(1);
        AtomicInteger unlocked = new AtomicInteger();
        Mockito.doAnswer(invocation -> {
            unlocked.incrementAndGet();
            return null;
        }).when(this.locks).unlockTask(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(),
                                      Mockito.any(LockResult.class));
        try (Fixture fixture = new Fixture("failed_acquire_handoff")) {
            Mockito.when(this.locks.tryLockTask(Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
                   .thenAnswer(invocation -> {
                       LockResult acquired = new LockResult();
                       if (Thread.currentThread() == fixture.worker.owner) {
                           runnerAcquired.countDown();
                           Assert.assertTrue(publishRunner.await(10L, TimeUnit.SECONDS));
                           acquired.lockSuccess(true);
                       } else {
                           cancellationAcquiring.countDown();
                           Assert.assertTrue(returnFailedLock.await(10L, TimeUnit.SECONDS));
                       }
                       return acquired;
                   });
            CancellationJob callable = new CancellationJob();
            HugeTask<?> admitted = new HugeTask<>(IdGenerator.of(9999988L), null, callable);
            admitted.type("test");
            admitted.name("failed-lock-token-handoff");
            fixture.scheduler.schedule(admitted);
            ExecutorService cancelling = Executors.newSingleThreadExecutor();
            try {
                Assert.assertTrue(runnerAcquired.await(10L, TimeUnit.SECONDS));
                Future<?> cancel = cancelling.submit(() -> fixture.scheduler.cancel(admitted));
                Assert.assertTrue(cancellationAcquiring.await(10L, TimeUnit.SECONDS));
                publishRunner.countDown();
                fixture.worker.submit(() -> null).get(10L, TimeUnit.SECONDS);
                Assert.assertEquals(0, unlocked.get());
                Assert.assertEquals(1, fixture.scheduler.pendingTasks());
                returnFailedLock.countDown();
                cancel.get(10L, TimeUnit.SECONDS);
                Assert.assertEquals(1, unlocked.get());
                Assert.assertEquals(1, fixture.scheduler.pendingTasks());
                Assert.assertEquals(TaskStatus.CANCELLING, fixture.scheduler.task(admitted.id(), false).status());
                Assert.assertEquals(0, callable.done.get());
                Assert.assertEquals(0, callable.cancelled.get());
                Mockito.when(this.locks.tryLockTask(Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
                       .thenAnswer(invocation -> {
                           LockResult acquired = new LockResult();
                           acquired.lockSuccess(true);
                           return acquired;
                       });
                fixture.scheduler.cancel(admitted);
                Assert.assertEquals(2, unlocked.get());
                Assert.assertEquals(0, fixture.scheduler.pendingTasks());
                Assert.assertEquals(1, callable.cancelled.get());
                Assert.assertFalse(callable.ran.get());
            } finally {
                publishRunner.countDown();
                returnFailedLock.countDown();
                cancelling.shutdownNow();
                Assert.assertTrue(cancelling.awaitTermination(10L, TimeUnit.SECONDS));
            }
        }
    }

    @Test
    public void testAdmissionLimitRejectsBeforeBindingOrPersistentSave() throws Exception {
        try (Fixture fixture = new Fixture("admission_limit")) {
            fixture.worker.armed.set(true);
            List<Future<?>> accepted = new ArrayList<>();
            AtomicBoolean ran = new AtomicBoolean();
            try {
                for (int i = 0; i < TaskScheduler.MAX_PENDING_TASKS; i++) {
                    accepted.add(fixture.scheduler.schedule(task(true, ran, 10000000L + i)));
                    if (i == 0) {
                        Assert.assertTrue(fixture.worker.reached.await(10L, TimeUnit.SECONDS));
                    }
                }
                Assert.assertEquals(TaskScheduler.MAX_PENDING_TASKS, fixture.scheduler.pendingTasks());
                for (boolean ephemeral : new boolean[]{true, false}) {
                    HugeTask<?> rejected = task(ephemeral, ran, 9999911L);
                    Assert.assertThrows(IllegalArgumentException.class,
                                        () -> fixture.scheduler.schedule(rejected));
                    Assert.assertEquals(TaskStatus.NEW, rejected.status());
                    Assert.assertNull(Whitebox.getInternalState(rejected, "scheduler"));
                }
                Assert.assertEquals(0, fixture.scheduler.lateSaved.get());
                Assert.assertThrows(org.apache.hugegraph.exception.NotFoundException.class,
                                    () -> fixture.scheduler.task(IdGenerator.of(9999911L), false));
                Assert.assertFalse(ran.get());
                for (Future<?> queued : accepted) {
                    Assert.assertTrue(queued.cancel(false));
                }
                accepted.clear();
                Assert.assertEquals(0, fixture.scheduler.pendingTasks());
                Assert.assertTrue(fixture.worker.getQueue().isEmpty());
                // Repeated cancellation must not bypass the cap by retaining queue tombstones.
                for (int i = 0; i < 100; i++) {
                    Future<?> queued = fixture.scheduler.schedule(task(true, ran, 11000000L + i));
                    Assert.assertTrue(queued.cancel(false));
                    Assert.assertEquals(0, fixture.scheduler.pendingTasks());
                    Assert.assertTrue(fixture.worker.getQueue().isEmpty());
                }
            } finally {
                for (Future<?> queued : accepted) {
                    queued.cancel(false);
                }
                fixture.worker.release.countDown();
            }
            fixture.worker.submit(() -> null).get(10L, TimeUnit.SECONDS);
            Assert.assertEquals(0, fixture.scheduler.pendingTasks());
            Assert.assertFalse(ran.get());
        }
    }

    @Test
    public void testRejectedSubmissionRollsBackDispatchReservation() throws Exception {
        for (boolean ephemeral : new boolean[]{true, false}) {
            try (Fixture fixture = new Fixture("rejected_" + ephemeral)) {
                fixture.worker.shutdown();
                AtomicBoolean ran = new AtomicBoolean();
                Assert.assertThrows(RejectedExecutionException.class,
                                    () -> fixture.scheduler.schedule(task(ephemeral, ran, 9999920L)));
                Assert.assertEquals(0, fixture.scheduler.pendingTasks());
                Assert.assertFalse(ran.get());
                if (!ephemeral) {
                    // Rejected dispatch is reported to the caller; its saved NEW record
                    // remains available for the existing scheduler's later retry.
                    Assert.assertEquals(TaskStatus.NEW,
                                        fixture.scheduler.task(IdGenerator.of(9999920L), false).status());
                }
                Assert.assertTrue(fixture.scheduler.close());
            }
        }
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
        private final GatedExecutor worker;
        private final boolean ownsWorker;
        private final CountingScheduler scheduler;
        private final HugeGraphParams params;
        private final TaskScheduler originalScheduler;
        private final Map<HugeGraphParams, TaskScheduler> registry;

        Fixture(String suffix) {
            this(suffix, null);
        }

        Fixture(String suffix, GatedExecutor sharedWorker) {
            this.ownsWorker = sharedWorker == null;
            this.worker = this.ownsWorker ? new GatedExecutor() : sharedWorker;
            RegisterUtil.registerBackends();
            HugeConfig config = FakeObjects.newConfig();
            config.setProperty("backend", "memory");
            config.setProperty("serializer", "text");
            config.setProperty("store", "distributed_admission_" + suffix);
            config.setProperty("task.schedule_period", 600L);
            config.setProperty("task.retry", 0);
            this.graph = HugeFactory.open(config);
            this.graph.initBackend();
            this.graph.serverStarted(GlobalMasterInfo.master("distributed-admission-test"));
            this.params = Whitebox.getInternalState(this.graph, "params");
            this.registry = Whitebox.getInternalState(TaskManager.instance(), "schedulers");
            this.originalScheduler = this.registry.get(this.params);
            Assert.assertTrue(this.originalScheduler.close());
            this.scheduler = new CountingScheduler(this.params, this.cron, this.database, this.worker);
            this.scheduler.init();
            Assert.assertTrue(this.registry.replace(this.params, this.originalScheduler, this.scheduler));
            Assert.assertSame(this.scheduler, this.graph.taskScheduler());
        }

        @Override
        public void close() throws Exception {
            try {
                this.worker.release.countDown();
                Assert.assertTrue(this.scheduler.close());
                // Tests may query a closed scheduler again; close its actual owner after those reads.
                this.scheduler.closeDatabaseOwner();
                this.cron.submit(this.params::closeTx).get(10L, TimeUnit.SECONDS);
            } finally {
                Assert.assertTrue(this.registry.replace(this.params, this.scheduler, this.originalScheduler));
                ExecutorService[] owned = this.ownsWorker ?
                                          new ExecutorService[]{this.worker, this.database, this.cron} :
                                          new ExecutorService[]{this.database, this.cron};
                for (ExecutorService executor : owned) {
                    executor.shutdownNow();
                    Assert.assertTrue(executor.awaitTermination(10L, TimeUnit.SECONDS));
                }
                try {
                    this.graph.close();
                } finally {
                    HugeFactory.remove(this.graph);
                    Assert.assertNull(this.registry.get(this.params));
                }
            }
        }
    }

    private static class GatedExecutor extends ThreadPoolExecutor {

        private volatile Thread owner;
        private final AtomicBoolean armed = new AtomicBoolean();
        private final CountDownLatch reached = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);

        GatedExecutor() {
            super(1, 1, 0L, TimeUnit.SECONDS, new LinkedBlockingQueue<>());
        }

        @Override
        protected void beforeExecute(Thread thread, Runnable work) {
            this.owner = thread;
            if (this.armed.compareAndSet(true, false)) {
                this.reached.countDown();
                try {
                    Assert.assertTrue(this.release.await(10L, TimeUnit.SECONDS));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(e);
                }
            }
        }
    }

    private static class CountingScheduler extends DistributedTaskScheduler {

        private final AtomicInteger lateSaved = new AtomicInteger();
        private final AtomicBoolean leaseRecovered = new AtomicBoolean();
        private volatile Runnable beforeSave = () -> { };
        private volatile Runnable beforeTask = () -> { };

        void deleteRemoteRecord(org.apache.hugegraph.id.Id id) {
            this.deleteFromDB(id);
        }

        String separateResult(org.apache.hugegraph.id.Id id) {
            HugeTaskResult result = this.queryTaskResult(id);
            return result == null ? null : result.result();
        }

        void closeDatabaseOwner() {
            this.call(() -> {
                this.tx().close();
                this.graph.closeTx();
                return null;
            });
        }

        @Override
        public <V> HugeTask<V> task(org.apache.hugegraph.id.Id id, boolean withResult) {
            this.beforeTask.run();
            return super.task(id, withResult);
        }

        CountingScheduler(HugeGraphParams graph, ScheduledThreadPoolExecutor cron,
                          ExecutorService database, ExecutorService worker) {
            super(graph, cron, database, worker, worker, worker, worker);
        }

        @Override
        protected boolean updateStatusWithLock(org.apache.hugegraph.id.Id id,
                                               TaskStatus previous, TaskStatus status) {
            boolean updated = super.updateStatusWithLock(id, previous, status);
            if (updated && previous == TaskStatus.RUNNING && status == TaskStatus.FAILED) {
                this.leaseRecovered.set(true);
            }
            return updated;
        }

        @Override
        public <V> void save(HugeTask<V> task) {
            this.beforeSave.run();
            if (Math.abs(task.id().asLong()) == 9999911L) {
                this.lateSaved.incrementAndGet();
            }
            super.save(task);
        }
    }
}
