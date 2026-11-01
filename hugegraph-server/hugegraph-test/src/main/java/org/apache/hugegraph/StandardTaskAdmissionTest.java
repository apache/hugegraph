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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.dist.RegisterUtil;
import org.apache.hugegraph.id.IdGenerator;
import org.apache.hugegraph.job.SysJob;
import org.apache.hugegraph.masterelection.GlobalMasterInfo;
import org.apache.hugegraph.task.HugeTask;
import org.apache.hugegraph.task.StandardTaskScheduler;
import org.apache.hugegraph.task.TaskCallable;
import org.apache.hugegraph.task.TaskStatus;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.testutil.Whitebox;
import org.apache.hugegraph.unit.FakeObjects;
import org.apache.tinkerpop.gremlin.structure.T;
import org.junit.Test;
import org.mockito.Mockito;

public class StandardTaskAdmissionTest {

    @Test
    public void testCancellationFailurePreservesCallerTransaction() throws Exception {
        try (Fixture fixture = new Fixture("standard_failed_caller_tx")) {
            CountDownLatch occupied = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            fixture.worker.submit(() -> {
                occupied.countDown();
                await(release);
            });
            Assert.assertTrue(occupied.await(10L, TimeUnit.SECONDS));
            AtomicBoolean reject = new AtomicBoolean(true);
            IllegalStateException failure = new IllegalStateException("Cancellation rejected");
            HugeTask<Object> task = new HugeTask<>(IdGenerator.of(9999952L), null, new EmptyJob()) {
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
            try {
                fixture.graph.schema().vertexLabel("caller").useCustomizeStringId().create();
                fixture.graph.tx().commit();
                fixture.graph.addVertex(T.id, "pending", T.label, "caller");
                Assert.assertThrows(IllegalStateException.class, () -> fixture.scheduler.cancel(task),
                                    caught -> Assert.assertSame(failure, caught));
                Assert.assertFalse(task.isCancelled());
                Assert.assertTrue(fixture.graph.tx().isOpen());
                fixture.graph.tx().commit();
                Assert.assertTrue(fixture.database.submit(() -> {
                    try {
                        return fixture.graph.vertices("pending").hasNext();
                    } finally {
                        fixture.params.closeTx();
                    }
                }).get(10L, TimeUnit.SECONDS));
                Assert.assertEquals(1, fixture.scheduler.pendingTasks());
            } finally {
                reject.set(false);
                release.countDown();
            }
            fixture.worker.submit(() -> null).get(10L, TimeUnit.SECONDS);
            Assert.assertEquals(0, fixture.scheduler.pendingTasks());
        }
    }

    @Test
    public void testNormalWorkerTransactionCleanupControl() throws Exception {
        try (Fixture fixture = new Fixture("standard_normal_worker_control")) {
            BlockingJob job = new BlockingJob();
            job.release.countDown();
            HugeTask<Object> task = fixture.task(job);
            fixture.scheduler.schedule(task).get(10L, TimeUnit.SECONDS);
            fixture.worker.submit(() -> null).get(10L, TimeUnit.SECONDS);
            Assert.assertFalse(task.isCancelled());
            Assert.assertEquals(TaskStatus.SUCCESS, task.status());
            Assert.assertEquals(0L, job.started.getCount());
            Assert.assertEquals(0, fixture.scheduler.pendingTasks());
            ThreadLocal<?> owners = Whitebox.getInternalState(fixture.graph.tx(), "transactions");
            Assert.assertNull(fixture.worker.submit(owners::get).get(10L, TimeUnit.SECONDS));
            Assert.assertFalse(fixture.worker.submit(() -> fixture.graph.tx().isOpen()).get(10L, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testDirectDefaultDoneCancellationPreservesCallerTransaction() throws Exception {
        try (Fixture fixture = new Fixture("standard_direct_done_control")) {
            CountDownLatch occupied = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            fixture.worker.submit(() -> {
                occupied.countDown();
                await(release);
            });
            Assert.assertTrue(occupied.await(10L, TimeUnit.SECONDS));
            HugeTask<Object> task = fixture.task(new EmptyJob());
            try {
                fixture.scheduler.schedule(task);
                fixture.graph.schema().vertexLabel("caller").useCustomizeStringId().create();
                fixture.graph.tx().commit();
                fixture.graph.addVertex(T.id, "pending", T.label, "caller");
                // Direct FutureTask cancellation invokes the inherited TaskCallable.done before outer guards.
                task.cancel(true);
                Assert.assertTrue(fixture.graph.tx().isOpen());
                Assert.assertTrue(fixture.graph.vertices("pending").hasNext());
                fixture.graph.tx().commit();
                Assert.assertTrue(fixture.database.submit(() -> {
                    try {
                        return fixture.graph.vertices("pending").hasNext();
                    } finally {
                        fixture.params.closeTx();
                    }
                }).get(10L, TimeUnit.SECONDS));
            } finally {
                release.countDown();
            }
            fixture.worker.submit(() -> null).get(10L, TimeUnit.SECONDS);
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
        try (Fixture fixture = new Fixture("standard_caller_tx_" + running)) {
            CountDownLatch occupied = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            if (!running) {
                fixture.worker.submit(() -> {
                    occupied.countDown();
                    await(release);
                });
                Assert.assertTrue(occupied.await(10L, TimeUnit.SECONDS));
            }
            BlockingJob job = new BlockingJob();
            HugeTask<Object> task = fixture.task(job);
            try {
                fixture.scheduler.schedule(task);
                if (running) {
                    Assert.assertTrue(job.started.await(10L, TimeUnit.SECONDS));
                }
                fixture.graph.schema().vertexLabel("caller").useCustomizeStringId().create();
                fixture.graph.tx().commit();
                fixture.graph.addVertex(T.id, "pending", T.label, "caller");
                fixture.scheduler.cancel(task);
                Assert.assertTrue(task.isCancelled());
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
                if (running) {
                    Assert.assertEquals(1, fixture.scheduler.pendingTasks());
                }
            } finally {
                release.countDown();
                job.release.countDown();
            }
            fixture.worker.submit(() -> null).get(10L, TimeUnit.SECONDS);
            Assert.assertEquals(0, fixture.scheduler.pendingTasks());
            ThreadLocal<?> owners = Whitebox.getInternalState(fixture.graph.tx(), "transactions");
            Assert.assertNull(fixture.worker.submit(owners::get).get(10L, TimeUnit.SECONDS));
            Assert.assertEquals(running ? 0L : 1L, job.started.getCount());
        }
    }

    @Test
    public void testCancelledWorkerFencesAdmissionUntilActualExit() throws Exception {
        try (Fixture fixture = new Fixture("standard_cancel_worker")) {
            BlockingJob job = new BlockingJob();
            HugeTask<Object> task = fixture.task(job);
            fixture.scheduler.schedule(task);
            Assert.assertTrue(job.started.await(10L, TimeUnit.SECONDS));
            fixture.scheduler.cancel(task);
            Assert.assertTrue(task.isCancelled());
            Assert.assertEquals(1, fixture.scheduler.pendingTasks());
            Assert.assertFalse(fixture.scheduler.close());
            HugeTask<Object> late = fixture.task(new EmptyJob());
            Assert.assertThrows(IllegalStateException.class, () -> fixture.scheduler.schedule(late));
            Assert.assertThrows(IllegalStateException.class, () -> {
                fixture.scheduler.restoreTasks();
            }, error -> Assert.assertContains("closing", error.getMessage()));
            Assert.assertEquals(TaskStatus.NEW, late.status());
            job.release.countDown();
            fixture.scheduler.waitUntilAllTasksCompleted(10L);
            Assert.assertEquals(TaskStatus.CANCELLED, fixture.scheduler.findTask(task.id()).status());
            Assert.assertTrue(fixture.scheduler.close());
        }
    }

    @Test
    public void testDispatchFutureCancellationKeepsRunningOwner() throws Exception {
        try (Fixture fixture = new Fixture("standard_cancel_future")) {
            BlockingJob job = new BlockingJob();
            HugeTask<Object> task = fixture.task(job);
            Future<?> dispatch = fixture.scheduler.schedule(task);
            Assert.assertTrue(job.started.await(10L, TimeUnit.SECONDS));
            Assert.assertTrue(dispatch.cancel(false));
            Assert.assertFalse(task.isCancelled());
            Assert.assertFalse(fixture.scheduler.close());
            job.release.countDown();
            fixture.scheduler.waitUntilAllTasksCompleted(10L);
            Assert.assertEquals(TaskStatus.SUCCESS, fixture.scheduler.findTask(task.id()).status());
            Assert.assertTrue(fixture.scheduler.close());
        }
    }

    @Test
    public void testCancellationCallbackRetainsItsTransactionAfterWorkerExits() throws Exception {
        try (Fixture fixture = new Fixture("standard_cancel_callback")) {
            fixture.graph.schema().vertexLabel("person").useCustomizeStringId().create();
            CountDownLatch callback = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            BlockingJob job = new BlockingJob() {
                @Override
                protected void cancelled() {
                    this.graph().addVertex(T.id, "callback", T.label, "person");
                    callback.countDown();
                    await(release);
                    this.graph().tx().commit();
                    super.cancelled();
                }
            };
            HugeTask<Object> task = fixture.task(job);
            fixture.scheduler.schedule(task);
            Assert.assertTrue(job.started.await(10L, TimeUnit.SECONDS));
            ExecutorService caller = Executors.newSingleThreadExecutor();
            try {
                Future<?> cancellation = caller.submit(() -> fixture.scheduler.cancel(task));
                Assert.assertTrue(callback.await(10L, TimeUnit.SECONDS));
                job.release.countDown();
                fixture.worker.submit(() -> null).get(10L, TimeUnit.SECONDS);
                Assert.assertEquals(1, fixture.scheduler.pendingTasks());
                Assert.assertFalse(fixture.scheduler.close());
                release.countDown();
                cancellation.get(10L, TimeUnit.SECONDS);
                fixture.scheduler.waitUntilAllTasksCompleted(10L);
                Assert.assertTrue(fixture.graph.vertices("callback").hasNext());
                Assert.assertEquals(TaskStatus.CANCELLED, fixture.scheduler.findTask(task.id()).status());
            } finally {
                release.countDown();
                caller.shutdownNow();
                Assert.assertTrue(caller.awaitTermination(10L, TimeUnit.SECONDS));
            }
        }
    }

    @Test
    public void testQueuedCancellationAndSubmissionRejectionReleaseOwners() throws Exception {
        try (Fixture fixture = new Fixture("standard_cancel_queued")) {
            CountDownLatch occupied = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            fixture.worker.submit(() -> {
                occupied.countDown();
                release.await();
                return null;
            });
            try {
                Assert.assertTrue(occupied.await(10L, TimeUnit.SECONDS));
                BlockingJob job = new BlockingJob();
                Future<?> queued = fixture.scheduler.schedule(fixture.task(job));
                Assert.assertTrue(queued.cancel(false));
                Assert.assertEquals(0, fixture.scheduler.pendingTasks());
                release.countDown();
                fixture.worker.submit(() -> null).get(10L, TimeUnit.SECONDS);
                Assert.assertEquals(1L, job.started.getCount());
                fixture.worker.shutdown();
                Assert.assertTrue(fixture.worker.awaitTermination(10L, TimeUnit.SECONDS));
                Assert.assertThrows(RejectedExecutionException.class,
                                    () -> fixture.scheduler.schedule(fixture.task(new EmptyJob())));
                Assert.assertEquals(0, fixture.scheduler.pendingTasks());
            } finally {
                release.countDown();
            }
        }
    }

    @Test
    public void testDependencyRetryDoesNotReleaseEarlierCleanupOwner() throws Exception {
        try (Fixture fixture = new Fixture("standard_dependency_owner")) {
            CountDownLatch closing = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            AtomicBoolean first = new AtomicBoolean(true);
            Mockito.doAnswer(invocation -> {
                if (Thread.currentThread().getName().equals("standard-test-worker") &&
                    first.compareAndSet(true, false)) {
                    closing.countDown();
                    Assert.assertTrue(release.await(10L, TimeUnit.SECONDS));
                }
                return invocation.callRealMethod();
            }).when(fixture.params).closeTx();
            HugeTask<Object> dependency = fixture.task(new EmptyJob());
            fixture.scheduler.save(dependency);
            HugeTask<Object> task = fixture.task(new EmptyJob());
            task.depends(dependency.id());
            fixture.scheduler.schedule(task);
            try {
                Assert.assertTrue(closing.await(10L, TimeUnit.SECONDS));
                fixture.scheduler.cancel(task);
                Assert.assertEquals(1, fixture.scheduler.pendingTasks());
                Assert.assertFalse(fixture.scheduler.close());
            } finally {
                release.countDown();
            }
            fixture.scheduler.waitUntilAllTasksCompleted(10L);
            Assert.assertEquals(TaskStatus.CANCELLED, fixture.scheduler.findTask(task.id()).status());
        }
    }

    @Test
    public void testCloseFencesDependencyResubmission() throws Exception {
        try (Fixture fixture = new Fixture("standard_dependency_fence")) {
            CountDownLatch occupied = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            fixture.worker.submit(() -> {
                occupied.countDown();
                release.await();
                return null;
            });
            try {
                Assert.assertTrue(occupied.await(10L, TimeUnit.SECONDS));
                HugeTask<Object> dependency = fixture.task(new EmptyJob());
                fixture.scheduler.save(dependency);
                HugeTask<Object> task = fixture.task(new EmptyJob());
                task.depends(dependency.id());
                Future<?> dispatch = fixture.scheduler.schedule(task);
                Assert.assertFalse(fixture.scheduler.close());
                release.countDown();
                dispatch.get(10L, TimeUnit.SECONDS);
                fixture.scheduler.waitUntilAllTasksCompleted(10L);
                Assert.assertEquals(TaskStatus.FAILED, fixture.scheduler.findTask(task.id()).status());
                Assert.assertContains("is closing", task.result());
                Assert.assertTrue(fixture.scheduler.close());
            } finally {
                release.countDown();
            }
        }
    }

    @Test
    public void testCloseFailureRemainsRetryable() throws Exception {
        try (Fixture fixture = new Fixture("standard_close_failure")) {
            Mockito.doThrow(new IllegalStateException("close failed")).doCallRealMethod()
                   .when(fixture.params).closeTx();
            Assert.assertThrows(RuntimeException.class, fixture.scheduler::close);
            HugeTask<Object> late = fixture.task(new EmptyJob());
            Assert.assertThrows(IllegalStateException.class, () -> fixture.scheduler.schedule(late));
            Assert.assertTrue(fixture.scheduler.close());
        }
    }

    @Test
    public void testForceDeleteHidesPublicTaskWhileNativeWorkerRetainsOwnership() throws Exception {
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch interrupted = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        java.util.Map<String, Object> observed = new java.util.LinkedHashMap<>();
        boolean publicPresent;
        boolean listPresent;
        try (Fixture fixture = new Fixture("standard_force_delete_native")) {
            ExecutorService observerWorker = Executors.newSingleThreadExecutor();
            ExecutorService observerDatabase = Executors.newSingleThreadExecutor(runnable ->
                    new Thread(runnable, "standard-independent-db"));
            StandardTaskScheduler observer = new StandardTaskScheduler(fixture.params, observerWorker,
                                                                       observerDatabase);
            BlockingJob job = new BlockingJob() {
                @Override
                public Object execute() {
                    this.graph().vertices().hasNext();
                    started.countDown();
                    boolean wasInterrupted = false;
                    while (release.getCount() != 0L) {
                        try {
                            release.await();
                        } catch (InterruptedException ignored) {
                            wasInterrupted = true;
                            interrupted.countDown();
                        }
                    }
                    if (wasInterrupted) {
                        Thread.currentThread().interrupt();
                    }
                    return null;
                }
            };
            HugeTask<Object> task = fixture.task(job);
            try {
                fixture.scheduler.save(task);
                Assert.assertNotNull(observer.findTask(task.id()));
                fixture.scheduler.schedule(task);
                Assert.assertTrue(started.await(10L, TimeUnit.SECONDS));
                Assert.assertNotNull(observer.findTask(task.id()));
                HugeTask<?> deleted = fixture.scheduler.delete(task.id(), true);
                Assert.assertNotNull(deleted);
                Assert.assertTrue(interrupted.await(10L, TimeUnit.SECONDS));
                Assert.assertEquals(1L, release.getCount());
                try {
                    observed.put("public_task_status_before_release",
                                 fixture.scheduler.task(task.id()).status().name());
                    publicPresent = true;
                } catch (org.apache.hugegraph.exception.NotFoundException missing) {
                    publicPresent = false;
                }
                listPresent = fixture.scheduler.tasks(java.util.Collections.singletonList(task.id())).hasNext();
                try {
                    fixture.scheduler.task(task.id(), false);
                    observed.put("public_task_without_result_present_before_release", true);
                } catch (org.apache.hugegraph.exception.NotFoundException missing) {
                    observed.put("public_task_without_result_present_before_release", false);
                }
                observed.put("public_ids_without_result_present_before_release", fixture.scheduler.tasks(
                        java.util.Collections.singletonList(task.id()), false).hasNext());
                Assert.assertThrows(org.apache.hugegraph.exception.NotFoundException.class,
                                    () -> observer.findTask(task.id()));
                Assert.assertFalse(observer.findAllTask(10L, null, false).hasNext());
                int pending = fixture.scheduler.pendingTasks();
                boolean closed = fixture.scheduler.close();
                observed.put("public_task_present_before_release", publicPresent);
                observed.put("public_task_ids_list_present_before_release", listPresent);
                observed.put("independent_committed_db_present_before_release", false);
                observed.put("native_worker_interrupted_and_still_held", true);
                observed.put("pending_before_release", pending);
                observed.put("close_before_release", closed);
                observed.put("logical_task_cancelled", task.isCancelled());
                observed.put("native_scheduler_origin", StandardTaskScheduler.class.getProtectionDomain()
                            .getCodeSource().getLocation().toString());
                Assert.assertEquals(1, pending);
                Assert.assertFalse(closed);
                release.countDown();
                fixture.worker.submit(() -> null).get(10L, TimeUnit.SECONDS);
                fixture.scheduler.waitUntilAllTasksCompleted(10L);
                Assert.assertThrows(org.apache.hugegraph.exception.NotFoundException.class,
                                    () -> fixture.scheduler.task(task.id()));
                Assert.assertFalse(fixture.scheduler.tasks(java.util.Collections.singletonList(task.id())).hasNext());
                Assert.assertThrows(org.apache.hugegraph.exception.NotFoundException.class,
                                    () -> observer.findTask(task.id()));
                Assert.assertEquals(0, fixture.scheduler.pendingTasks());
                Assert.assertTrue(fixture.scheduler.close());
                observed.put("public_task_present_after_release", false);
                observed.put("public_task_ids_list_present_after_release", false);
                observed.put("independent_committed_db_present_after_release", false);
                observed.put("pending_after_release", 0);
                observed.put("close_after_release", true);
                observed.put("one_persisted_task_one_delete", true);
                String evidence = System.getProperty("hugegraph.task.delete.evidence");
                if (evidence != null) {
                    java.nio.file.Files.writeString(java.nio.file.Paths.get(evidence, "standard-observation.json"),
                                                   org.apache.hugegraph.util.JsonUtil.toJson(observed));
                }
            } finally {
                release.countDown();
                fixture.worker.submit(() -> null).get(10L, TimeUnit.SECONDS);
                Assert.assertTrue(observer.close());
                observerWorker.shutdownNow();
                observerDatabase.shutdownNow();
                Assert.assertTrue(observerWorker.awaitTermination(10L, TimeUnit.SECONDS));
                Assert.assertTrue(observerDatabase.awaitTermination(10L, TimeUnit.SECONDS));
            }
        }
        // Admission accounting must keep the true native owner above, while
        // the established Standard force-delete API contract requires absence.
        Assert.assertFalse("force-deleted task is still publicly visible", publicPresent);
        Assert.assertFalse("force-deleted task is still listed by ID", listPresent);
        Assert.assertEquals(false, observed.get("public_task_without_result_present_before_release"));
        Assert.assertEquals(false, observed.get("public_ids_without_result_present_before_release"));
    }

    @Test
    public void testDeletingTaskStillVisibleWhileItsRecordExists() throws Exception {
        try (Fixture fixture = new Fixture("standard_deleting_db_visible")) {
            BlockingJob job = new BlockingJob();
            HugeTask<Object> task = fixture.task(job);
            fixture.scheduler.schedule(task);
            try {
                Assert.assertTrue(job.started.await(10L, TimeUnit.SECONDS));
                task.overwriteStatus(TaskStatus.DELETING);
                fixture.scheduler.save(task);
                for (boolean withResult : new boolean[]{true, false}) {
                    Assert.assertEquals(TaskStatus.DELETING, fixture.scheduler.task(task.id(), withResult).status());
                    java.util.Iterator<HugeTask<Object>> tasks = fixture.scheduler.tasks(
                            java.util.Collections.singletonList(task.id()), withResult);
                    Assert.assertTrue(tasks.hasNext());
                    Assert.assertEquals(TaskStatus.DELETING, tasks.next().status());
                    Assert.assertFalse(tasks.hasNext());
                }
            } finally {
                job.release.countDown();
                fixture.worker.submit(() -> null).get(10L, TimeUnit.SECONDS);
            }
        }
    }

    @Test
    public void testOrdinaryTaskVisibilityRetainsResultSelection() throws Exception {
        try (Fixture fixture = new Fixture("standard_normal_task_visible")) {
            HugeTask<Object> task = fixture.task(new ResultJob());
            fixture.scheduler.schedule(task).get(10L, TimeUnit.SECONDS);
            for (boolean withResult : new boolean[]{true, false}) {
                HugeTask<Object> loaded = fixture.scheduler.task(task.id(), withResult);
                Assert.assertEquals(TaskStatus.SUCCESS, loaded.status());
                Assert.assertEquals(withResult ? task.result() : null, loaded.result());
                java.util.Iterator<HugeTask<Object>> tasks = fixture.scheduler.tasks(
                        java.util.Collections.singletonList(task.id()), withResult);
                Assert.assertTrue(tasks.hasNext());
                HugeTask<Object> listed = tasks.next();
                Assert.assertEquals(TaskStatus.SUCCESS, listed.status());
                Assert.assertEquals(withResult ? task.result() : null, listed.result());
                Assert.assertFalse(tasks.hasNext());
            }
        }
    }

    @Test
    public void testForceDeleteCompletedOwnerDoesNotResurrectAfterDoneSave() throws Exception {
        java.util.Map<String, Object> observed = new java.util.LinkedHashMap<>();
        boolean publicPresent;
        boolean idsPresent;
        boolean dbResurrected;
        try (Fixture fixture = new Fixture("standard_force_delete_done_save")) {
            HugeGraphParams nativeParams = (HugeGraphParams) Whitebox.getInternalState(fixture.graph, "params");
            @SuppressWarnings("unchecked")
            java.util.Map<HugeGraphParams, org.apache.hugegraph.task.TaskScheduler> schedulers =
                    (java.util.Map<HugeGraphParams, org.apache.hugegraph.task.TaskScheduler>) Whitebox
                            .getInternalState(org.apache.hugegraph.task.TaskManager.instance(), "schedulers");
            org.apache.hugegraph.task.TaskScheduler original = schedulers.put(nativeParams, fixture.scheduler);
            ExecutorService observerWorker = Executors.newSingleThreadExecutor();
            ExecutorService observerDatabase = Executors.newSingleThreadExecutor();
            StandardTaskScheduler observer = new StandardTaskScheduler(fixture.params, observerWorker,
                                                                       observerDatabase);
            CompletionSaveJob job = new CompletionSaveJob();
            HugeTask<Object> task = fixture.task(job);
            try {
                Assert.assertSame(fixture.scheduler, fixture.graph.taskScheduler());
                fixture.scheduler.schedule(task);
                Assert.assertTrue(job.beforeSave.await(10L, TimeUnit.SECONDS));
                Assert.assertEquals(TaskStatus.SUCCESS, task.status());
                Assert.assertTrue(task.completed());
                Assert.assertEquals(1, fixture.scheduler.pendingTasks());
                Assert.assertNotNull(observer.findTask(task.id()));
                HugeTask<?> deleted = fixture.scheduler.delete(task.id(), true);
                Assert.assertNotNull(deleted);
                Assert.assertThrows(org.apache.hugegraph.exception.NotFoundException.class,
                                    () -> observer.findTask(task.id()));
                try {
                    observed.put("public_status_after_delete_before_done_save",
                                 fixture.scheduler.task(task.id()).status().name());
                    publicPresent = true;
                } catch (org.apache.hugegraph.exception.NotFoundException missing) {
                    publicPresent = false;
                }
                idsPresent = fixture.scheduler.tasks(java.util.Collections.singletonList(task.id())).hasNext();
                observed.put("success_owner_retained_before_delete", true);
                observed.put("public_present_after_delete_before_done_save", publicPresent);
                observed.put("ids_present_after_delete_before_done_save", idsPresent);
                observed.put("committed_db_present_after_delete_before_done_save", false);
                observed.put("pending_before_done_save", fixture.scheduler.pendingTasks());
                Assert.assertEquals(1, fixture.scheduler.pendingTasks());
                Assert.assertFalse(fixture.scheduler.close());
                observed.put("close_before_done_save", false);
                job.releaseSave.countDown();
                fixture.worker.submit(() -> null).get(10L, TimeUnit.SECONDS);
                Assert.assertTrue(job.saved.await(10L, TimeUnit.SECONDS));
                try {
                    observed.put("db_status_after_real_done_save", observer.findTask(task.id()).status().name());
                    dbResurrected = true;
                } catch (org.apache.hugegraph.exception.NotFoundException missing) {
                    dbResurrected = false;
                }
                observed.put("committed_db_resurrected_after_real_done_save", dbResurrected);
                observed.put("pending_after_real_done_save", fixture.scheduler.pendingTasks());
                Assert.assertEquals(0, fixture.scheduler.pendingTasks());
                Assert.assertTrue(fixture.scheduler.close());
                observed.put("close_after_real_done_save", true);
                observed.put("real_sysjob_done_save_scheduler_binding", true);
                String evidence = System.getProperty("hugegraph.task.delete.evidence");
                if (evidence != null) {
                    java.nio.file.Files.writeString(
                            java.nio.file.Paths.get(evidence, "completion-save-observation.json"),
                            org.apache.hugegraph.util.JsonUtil.toJson(observed));
                }
            } finally {
                job.releaseSave.countDown();
                fixture.worker.submit(() -> null).get(10L, TimeUnit.SECONDS);
                Assert.assertTrue(schedulers.replace(nativeParams, fixture.scheduler, original));
                Assert.assertTrue(observer.close());
                observerWorker.shutdownNow();
                observerDatabase.shutdownNow();
                Assert.assertTrue(observerWorker.awaitTermination(10L, TimeUnit.SECONDS));
                Assert.assertTrue(observerDatabase.awaitTermination(10L, TimeUnit.SECONDS));
            }
        }
        Assert.assertFalse("completed owner still visible after force deletion", publicPresent);
        Assert.assertFalse("completed owner still listed after force deletion", idsPresent);
        Assert.assertFalse("real done save recreated force-deleted task", dbResurrected);
    }

    @Test
    public void testDoneSaveBeforeDeleteRetainsNativeCleanupOwner() throws Exception {
        this.doneSaveBeforeDelete(true);
    }

    @Test
    public void testNonForceCompletedDeleteRetainsNativeCleanupOwner() throws Exception {
        this.doneSaveBeforeDelete(false);
    }

    @Test
    public void testNonForceDeleteDatabaseFailureCanRetryWithDurableCompletion() throws Exception {
        this.doneSaveBeforeDelete(false, true);
    }

    private void doneSaveBeforeDelete(boolean force) throws Exception {
        this.doneSaveBeforeDelete(force, false);
    }

    private void doneSaveBeforeDelete(boolean force, boolean failure) throws Exception {
        try (Fixture fixture = new Fixture("standard_save_before_delete");
             SchedulerBinding binding = new SchedulerBinding(fixture)) {
            CountDownLatch closing = new CountDownLatch(1);
            CountDownLatch close = new CountDownLatch(1);
            AtomicBoolean first = new AtomicBoolean(true);
            Mockito.doAnswer(invocation -> {
                if (Thread.currentThread().getName().equals("standard-test-worker") &&
                    first.compareAndSet(true, false)) {
                    closing.countDown();
                    await(close);
                }
                return invocation.callRealMethod();
            }).when(fixture.params).closeTx();
            HugeTask<Object> task = fixture.task(new ResultJob());
            try {
                fixture.scheduler.schedule(task);
                Assert.assertTrue(closing.await(10L, TimeUnit.SECONDS));
                Assert.assertEquals(TaskStatus.SUCCESS, fixture.scheduler.findTask(task.id()).status());
                Assert.assertEquals(1, fixture.scheduler.pendingTasks());
                if (failure) {
                    org.apache.hugegraph.task.TaskTransaction transaction = (org.apache.hugegraph.task.TaskTransaction)
                            Whitebox.getInternalState(fixture.scheduler, "taskTx");
                    org.apache.hugegraph.task.TaskTransaction rejecting = Mockito.spy(transaction);
                    Error rejected = new AssertionError("native completed delete rejected before mutation");
                    Mockito.doThrow(rejected).doCallRealMethod().when(rejecting)
                           .removeTaskVertex(Mockito.any(org.apache.hugegraph.structure.HugeVertex.class));
                    Whitebox.setInternalState(fixture.scheduler, "taskTx", rejecting);
                    Throwable failed = Assert.assertThrows(RuntimeException.class,
                                                          () -> fixture.scheduler.delete(task.id(), force));
                    Throwable cause = failed;
                    while (cause.getCause() != null) {
                        cause = cause.getCause();
                    }
                    Assert.assertSame(rejected, cause);
                    Assert.assertEquals(TaskStatus.SUCCESS, fixture.scheduler.findTask(task.id()).status());
                    Assert.assertEquals(TaskStatus.SUCCESS, fixture.scheduler.task(task.id()).status());
                    Assert.assertEquals(TaskStatus.DELETING, task.status());
                    Assert.assertEquals(1, fixture.scheduler.pendingTasks());
                }
                fixture.scheduler.delete(task.id(), force);
                Assert.assertThrows(org.apache.hugegraph.exception.NotFoundException.class,
                                    () -> fixture.scheduler.findTask(task.id()));
                Assert.assertEquals(1, fixture.scheduler.pendingTasks());
                Assert.assertFalse(fixture.scheduler.close());
                assertTaskAbsent(fixture.scheduler, task);
            } finally {
                close.countDown();
                fixture.worker.submit(() -> null).get(10L, TimeUnit.SECONDS);
            }
            assertTaskAbsent(fixture.scheduler, task);
            Assert.assertEquals(0, fixture.scheduler.pendingTasks());
        }
    }

    @Test
    public void testDeleteDatabaseFailureRetainsRecordForRetryAndOriginalError() throws Exception {
        try (Fixture fixture = new Fixture("standard_delete_db_failure");
             SchedulerBinding binding = new SchedulerBinding(fixture)) {
            CompletionSaveJob job = new CompletionSaveJob();
            HugeTask<Object> task = fixture.task(job);
            try {
                fixture.scheduler.schedule(task);
                Assert.assertTrue(job.beforeSave.await(10L, TimeUnit.SECONDS));
                org.apache.hugegraph.task.TaskTransaction transaction = (org.apache.hugegraph.task.TaskTransaction)
                        Whitebox.getInternalState(fixture.scheduler, "taskTx");
                org.apache.hugegraph.task.TaskTransaction rejecting = Mockito.spy(transaction);
                Error rejected = new AssertionError("native delete boundary rejected before mutation");
                Mockito.doThrow(rejected).doCallRealMethod().when(rejecting)
                       .removeTaskVertex(Mockito.any(org.apache.hugegraph.structure.HugeVertex.class));
                Whitebox.setInternalState(fixture.scheduler, "taskTx", rejecting);
                Throwable failed = Assert.assertThrows(RuntimeException.class,
                                                      () -> fixture.scheduler.delete(task.id(), true));
                Throwable cause = failed;
                while (cause.getCause() != null) {
                    cause = cause.getCause();
                }
                Assert.assertSame(rejected, cause);
                Assert.assertNotNull(fixture.scheduler.findTask(task.id()));
                Assert.assertNotNull(fixture.scheduler.task(task.id()));
                Assert.assertEquals(TaskStatus.DELETING, task.status());
                Assert.assertEquals(1, fixture.scheduler.pendingTasks());
                fixture.scheduler.delete(task.id(), true);
                assertTaskAbsent(fixture.scheduler, task);
                job.releaseSave.countDown();
                fixture.worker.submit(() -> null).get(10L, TimeUnit.SECONDS);
                Assert.assertTrue(job.saved.await(10L, TimeUnit.SECONDS));
                assertTaskAbsent(fixture.scheduler, task);
                Assert.assertEquals(0, fixture.scheduler.pendingTasks());
            } finally {
                job.releaseSave.countDown();
                fixture.worker.submit(() -> null).get(10L, TimeUnit.SECONDS);
            }
        }
    }

    @Test
    public void testUnownedExplicitSaveCanRestoreDeletingRecord() throws Exception {
        try (Fixture fixture = new Fixture("standard_unowned_explicit_save")) {
            HugeTask<Object> task = fixture.task(new EmptyJob());
            task.overwriteStatus(TaskStatus.DELETING);
            fixture.scheduler.save(task);
            Assert.assertEquals(0, fixture.scheduler.pendingTasks());
            Assert.assertEquals(TaskStatus.DELETING, fixture.scheduler.task(task.id()).status());
            fixture.scheduler.delete(task.id(), true);
            assertTaskAbsent(fixture.scheduler, task);
            fixture.scheduler.save(task);
            Assert.assertEquals(TaskStatus.DELETING, fixture.scheduler.findTask(task.id()).status());
            Assert.assertEquals(0, fixture.scheduler.pendingTasks());
        }
    }

    @Test
    public void testNonForceDeleteStillRequiresPersistedCompletion() throws Exception {
        try (Fixture fixture = new Fixture("standard_nonforce_durable_check");
             SchedulerBinding binding = new SchedulerBinding(fixture)) {
            CompletionSaveJob job = new CompletionSaveJob();
            HugeTask<Object> task = fixture.task(job);
            try {
                fixture.scheduler.schedule(task);
                Assert.assertTrue(job.beforeSave.await(10L, TimeUnit.SECONDS));
                Assert.assertEquals(TaskStatus.SUCCESS, task.status());
                Assert.assertEquals(TaskStatus.RUNNING, fixture.scheduler.findTask(task.id()).status());
                Assert.assertThrows(RuntimeException.class, () -> fixture.scheduler.delete(task.id(), false));
                Assert.assertEquals(TaskStatus.SUCCESS, task.status());
                Assert.assertEquals(TaskStatus.RUNNING, fixture.scheduler.findTask(task.id()).status());
                Assert.assertEquals(1, fixture.scheduler.pendingTasks());
                Assert.assertFalse(task.isCancelled());
                job.releaseSave.countDown();
                fixture.worker.submit(() -> null).get(10L, TimeUnit.SECONDS);
                Assert.assertEquals(TaskStatus.SUCCESS, fixture.scheduler.findTask(task.id()).status());
                fixture.scheduler.delete(task.id(), false);
                assertTaskAbsent(fixture.scheduler, task);
            } finally {
                job.releaseSave.countDown();
                fixture.worker.submit(() -> null).get(10L, TimeUnit.SECONDS);
            }
        }
    }

    @Test
    public void testNonForceDeleteRejectsActuallyRunningTask() throws Exception {
        try (Fixture fixture = new Fixture("standard_nonforce_running");
             SchedulerBinding binding = new SchedulerBinding(fixture)) {
            BlockingJob job = new BlockingJob();
            HugeTask<Object> task = fixture.task(job);
            try {
                fixture.scheduler.schedule(task);
                Assert.assertTrue(job.started.await(10L, TimeUnit.SECONDS));
                Assert.assertEquals(TaskStatus.RUNNING, task.status());
                Assert.assertThrows(IllegalArgumentException.class, () -> fixture.scheduler.delete(task.id(), false));
                Assert.assertEquals(TaskStatus.RUNNING, task.status());
                Assert.assertNotNull(fixture.scheduler.findTask(task.id()));
                Assert.assertEquals(1, fixture.scheduler.pendingTasks());
                Assert.assertFalse(task.isCancelled());
            } finally {
                job.release.countDown();
                fixture.worker.submit(() -> null).get(10L, TimeUnit.SECONDS);
            }
        }
    }

    private static void assertTaskAbsent(StandardTaskScheduler scheduler, HugeTask<?> task) {
        for (boolean withResult : new boolean[]{true, false}) {
            Assert.assertThrows(org.apache.hugegraph.exception.NotFoundException.class,
                                () -> scheduler.task(task.id(), withResult));
            Assert.assertFalse(scheduler.tasks(java.util.Collections.singletonList(task.id()), withResult).hasNext());
        }
        Assert.assertThrows(org.apache.hugegraph.exception.NotFoundException.class,
                            () -> scheduler.findTask(task.id()));
    }

    private static final class SchedulerBinding implements AutoCloseable {
        private final java.util.Map<HugeGraphParams, org.apache.hugegraph.task.TaskScheduler> schedulers;
        private final HugeGraphParams nativeParams;
        private final org.apache.hugegraph.task.TaskScheduler original;
        private final StandardTaskScheduler owner;
        @SuppressWarnings("unchecked")
        private SchedulerBinding(Fixture fixture) {
            this.schedulers = (java.util.Map<HugeGraphParams, org.apache.hugegraph.task.TaskScheduler>)
                    Whitebox.getInternalState(org.apache.hugegraph.task.TaskManager.instance(), "schedulers");
            this.nativeParams = (HugeGraphParams) Whitebox.getInternalState(fixture.graph, "params");
            this.owner = fixture.scheduler;
            this.original = this.schedulers.put(this.nativeParams, this.owner);
            Assert.assertNotNull(this.original);
            Assert.assertSame(this.owner, fixture.graph.taskScheduler());
        }
        @Override
        public void close() {
            Assert.assertTrue(this.schedulers.replace(this.nativeParams, this.owner, this.original));
        }
    }

    private static void await(CountDownLatch latch) {
        boolean interrupted = false;
        while (true) {
            try {
                latch.await();
                break;
            } catch (InterruptedException ignored) {
                interrupted = true;
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private static class Fixture implements AutoCloseable {

        private final HugeGraph graph;
        private final HugeGraphParams params;
        private final ExecutorService worker;
        private final ExecutorService database;
        private final StandardTaskScheduler scheduler;
        private final List<BlockingJob> jobs = new ArrayList<>();
        private long nextId = 9998000L;

        private Fixture(String name) {
            RegisterUtil.registerBackends();
            HugeConfig config = FakeObjects.newConfig();
            config.setProperty("backend", "memory");
            config.setProperty("serializer", "text");
            config.setProperty("store", name);
            this.graph = HugeFactory.open(config);
            this.graph.initBackend();
            this.graph.serverStarted(GlobalMasterInfo.master("standard-owner-test"));
            this.params = Mockito.spy((HugeGraphParams) Whitebox.getInternalState(this.graph, "params"));
            this.worker = Executors.newSingleThreadExecutor(runnable ->
                    new Thread(runnable, "standard-test-worker"));
            this.database = Executors.newSingleThreadExecutor();
            this.scheduler = new StandardTaskScheduler(this.params, this.worker, this.database);
        }

        private HugeTask<Object> task(TaskCallable<Object> job) {
            if (job instanceof BlockingJob blockingJob) {
                this.jobs.add(blockingJob);
            }
            HugeTask<Object> task = new HugeTask<>(IdGenerator.of(this.nextId++), null, job);
            task.type("test");
            task.name("standard-owner");
            return task;
        }

        @Override
        public void close() throws Exception {
            for (BlockingJob job : this.jobs) {
                job.release.countDown();
            }
            this.scheduler.close();
            this.worker.shutdown();
            Assert.assertTrue(this.worker.awaitTermination(10L, TimeUnit.SECONDS));
            Assert.assertTrue(this.scheduler.close());
            this.database.shutdownNow();
            Assert.assertTrue(this.database.awaitTermination(10L, TimeUnit.SECONDS));
            this.graph.close();
            HugeFactory.remove(this.graph);
        }
    }

    public static class EmptyJob extends SysJob<Object> {

        @Override
        public String type() {
            return "test";
        }

        @Override
        public Object execute() {
            return null;
        }
    }

    public static class CompletionSaveJob extends EmptyJob {
        private final CountDownLatch beforeSave = new CountDownLatch(1);
        private final CountDownLatch releaseSave = new CountDownLatch(1);
        private final CountDownLatch saved = new CountDownLatch(1);
        @Override
        public Object execute() {
            this.graph().vertices().hasNext();
            return "completion-result";
        }
        @Override
        protected void done() {
            this.beforeSave.countDown();
            await(this.releaseSave);
            try {
                super.done();
            } finally {
                this.saved.countDown();
            }
        }
    }

    public static class ResultJob extends EmptyJob {
        @Override
        public Object execute() {
            return "ordinary-result";
        }
    }

    public static class BlockingJob extends SysJob<Object> {

        private final CountDownLatch started = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);

        @Override
        public String type() {
            return "test";
        }

        @Override
        public Object execute() {
            this.graph().vertices().hasNext();
            this.started.countDown();
            await(this.release);
            return null;
        }
    }
}
