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
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hugegraph.HugeException;
import org.apache.hugegraph.HugeGraphParams;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.id.IdGenerator;
import org.apache.hugegraph.backend.query.QueryResults;
import org.apache.hugegraph.core.BaseCoreTest;
import org.apache.hugegraph.structure.HugeVertex;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.testutil.Whitebox;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

public class TaskAndResultSchedulerTest extends BaseCoreTest {

    @Test
    public void testMetadataOnlyReadsSkipLargeCompressedTaskResult() {
        Id id = IdGenerator.of(88901);
        TestDistributedTaskScheduler scheduler = this.newScheduler();

        try {
            scheduler.init();

            String largeResult = largeTaskResult();
            HugeTask<Object> task = newTask(id, "large-result-task",
                                            TaskStatus.SUCCESS, largeResult);
            scheduler.save(task);
            Assert.assertTrue(scheduler.taskResultVertexExists(id));

            HugeTask<?> taskWithResult = scheduler.task(id, true);
            Assert.assertEquals(largeResult, taskWithResult.result());
            Assert.assertGt(0, scheduler.resultReadCount());

            scheduler.resetResultReadCount();
            scheduler.forbidResultRead(true);

            HugeTask<?> taskWithoutResult = scheduler.task(id, false);
            assertTaskWithoutResult(taskWithoutResult, id);
            Assert.assertEquals(0, scheduler.resultReadCount());

            Iterator<HugeTask<Object>> tasks = scheduler.tasks(ImmutableList.of(id),
                                                               false);
            assertIteratorContainsTaskWithoutResult(tasks, id);
            Assert.assertEquals(0, scheduler.resultReadCount());

            tasks = scheduler.tasks(TaskStatus.SUCCESS, 10L, null, false);
            assertIteratorContainsTaskWithoutResult(tasks, id);
            Assert.assertEquals(0, scheduler.resultReadCount());

            tasks = scheduler.tasks(ImmutableList.of(id));
            assertIteratorContainsTaskWithoutResult(tasks, id);
            Assert.assertEquals(0, scheduler.resultReadCount());

            tasks = scheduler.tasks(TaskStatus.SUCCESS, 10L, null);
            assertIteratorContainsTaskWithoutResult(tasks, id);
            Assert.assertEquals(0, scheduler.resultReadCount());

            tasks = scheduler.queryMetadataTasksByStatus(TaskStatus.SUCCESS);
            assertIteratorContainsTaskWithoutResult(tasks, id);
            Assert.assertEquals(0, scheduler.resultReadCount());

            scheduler.forbidResultRead(false);
            scheduler.resetResultReadCount();

            tasks = scheduler.tasks(ImmutableList.of(id), true);
            assertIteratorContainsTaskWithResult(tasks, id, largeResult);
            Assert.assertGt(0, scheduler.resultReadCount());
        } finally {
            scheduler.forbidResultRead(false);
            scheduler.deleteFromDBForTest(id);
            scheduler.closeAndShutdown();
        }
    }

    @Test
    public void testDistributedDeleteKeepsTaskResultRecoverable() {
        Id id = IdGenerator.of(88902);
        TestDistributedTaskScheduler scheduler = this.newScheduler();

        try {
            scheduler.init();

            HugeTask<Object> task = newTask(id, "delete-order-task",
                                            TaskStatus.SUCCESS, "\"result\"");
            scheduler.save(task);
            Assert.assertTrue(scheduler.taskVertexExists(id));
            Assert.assertTrue(scheduler.taskResultVertexExists(id));

            scheduler.failNextTaskResultDelete();
            Assert.assertThrows(HugeException.class, () -> {
                scheduler.delete(id, true);
            });

            Assert.assertTrue(scheduler.taskVertexExists(id));
            Assert.assertEquals(TaskStatus.DELETING, scheduler.taskStatus(id));
            Assert.assertTrue(scheduler.taskResultVertexExists(id));

            scheduler.cronSchedule();
            Assert.assertFalse(scheduler.taskVertexExists(id));
            Assert.assertFalse(scheduler.taskResultVertexExists(id));
        } finally {
            scheduler.allowTaskResultDelete();
            scheduler.deleteFromDBForTest(id);
            scheduler.closeAndShutdown();
        }
    }

    private TestDistributedTaskScheduler newScheduler() {
        return new TestDistributedTaskScheduler(this.params());
    }

    private static HugeTask<Object> newTask(Id id, String name,
                                            TaskStatus status, String result) {
        HugeTask<Object> task = new HugeTask<>(id, null, new EmptyCallable());
        task.type("test");
        task.name(name);
        task.overwriteStatus(status);
        Whitebox.setInternalState(task, "result", result);
        return task;
    }

    private static void assertTaskWithoutResult(HugeTask<?> task, Id id) {
        Assert.assertEquals(id, task.id());
        Assert.assertNull(task.result());
    }

    private static void assertIteratorContainsTaskWithoutResult(
            Iterator<HugeTask<Object>> tasks, Id id) {
        boolean matched = false;
        while (tasks.hasNext()) {
            HugeTask<?> task = tasks.next();
            if (id.equals(task.id())) {
                assertTaskWithoutResult(task, id);
                matched = true;
            }
        }
        Assert.assertTrue(matched);
    }

    private static void assertIteratorContainsTaskWithResult(
            Iterator<HugeTask<Object>> tasks, Id id, String result) {
        boolean matched = false;
        while (tasks.hasNext()) {
            HugeTask<?> task = tasks.next();
            if (id.equals(task.id())) {
                Assert.assertEquals(id, task.id());
                Assert.assertEquals(result, task.result());
                matched = true;
            }
        }
        Assert.assertTrue(matched);
    }

    private static String largeTaskResult() {
        char[] chars = new char[1024 * 1024 + 257];
        int value = 0x12345678;
        for (int i = 0; i < chars.length; i++) {
            value = value * 1103515245 + 12345;
            chars[i] = (char) ('a' + ((value >>> 16) & 0x0F));
        }
        return new String(chars);
    }

    public static class EmptyCallable extends TaskCallable<Object> {

        public EmptyCallable() {
            // pass
        }

        @Override
        public Object call() {
            return null;
        }
    }

    private static class TestDistributedTaskScheduler
            extends DistributedTaskScheduler {

        private final ScheduledThreadPoolExecutor schedulerExecutor;
        private final ExecutorService taskDbExecutor;
        private final ExecutorService schemaTaskExecutor;
        private final ExecutorService olapTaskExecutor;
        private final ExecutorService gremlinTaskExecutor;
        private final ExecutorService ephemeralTaskExecutor;
        private final ExecutorService serverInfoDbExecutor;
        private final AtomicInteger resultReadCount;
        private volatile boolean forbidResultRead;
        private volatile boolean failNextTaskResultDelete;

        TestDistributedTaskScheduler(HugeGraphParams graph) {
            this(graph, new ScheduledThreadPoolExecutor(1),
                 Executors.newSingleThreadExecutor(),
                 Executors.newSingleThreadExecutor(),
                 Executors.newSingleThreadExecutor(),
                 Executors.newSingleThreadExecutor(),
                 Executors.newSingleThreadExecutor(),
                 Executors.newSingleThreadExecutor());
        }

        private TestDistributedTaskScheduler(
                HugeGraphParams graph,
                ScheduledThreadPoolExecutor schedulerExecutor,
                ExecutorService taskDbExecutor,
                ExecutorService schemaTaskExecutor,
                ExecutorService olapTaskExecutor,
                ExecutorService gremlinTaskExecutor,
                ExecutorService ephemeralTaskExecutor,
                ExecutorService serverInfoDbExecutor) {
            super(graph, schedulerExecutor, taskDbExecutor, schemaTaskExecutor,
                  olapTaskExecutor, gremlinTaskExecutor, ephemeralTaskExecutor,
                  serverInfoDbExecutor);
            this.schedulerExecutor = schedulerExecutor;
            this.taskDbExecutor = taskDbExecutor;
            this.schemaTaskExecutor = schemaTaskExecutor;
            this.olapTaskExecutor = olapTaskExecutor;
            this.gremlinTaskExecutor = gremlinTaskExecutor;
            this.ephemeralTaskExecutor = ephemeralTaskExecutor;
            this.serverInfoDbExecutor = serverInfoDbExecutor;
            this.resultReadCount = new AtomicInteger();
            this.forbidResultRead = false;
            this.failNextTaskResultDelete = false;
        }

        @Override
        protected HugeTaskResult queryTaskResult(Id taskid) {
            this.resultReadCount.incrementAndGet();
            this.checkResultReadAllowed();
            return super.queryTaskResult(taskid);
        }

        @Override
        protected Iterator<HugeTaskResult> queryTaskResult(List<Id> taskIds) {
            this.resultReadCount.incrementAndGet();
            this.checkResultReadAllowed();
            return super.queryTaskResult(taskIds);
        }

        @Override
        protected void deleteTaskResultFromTx(Id taskId) {
            if (this.failNextTaskResultDelete) {
                this.failNextTaskResultDelete = false;
                throw new HugeException("Mock task result delete failure");
            }
            super.deleteTaskResultFromTx(taskId);
        }

        @Override
        protected boolean isLockedTask(String taskId) {
            return false;
        }

        public <V> HugeTask<V> deleteFromDBForTest(Id id) {
            return super.deleteFromDB(id);
        }

        public Iterator<HugeTask<Object>> queryMetadataTasksByStatus(
                TaskStatus status) {
            return super.queryTaskWithoutResultByStatus(status);
        }

        public boolean taskVertexExists(Id id) {
            return this.vertexExists(id);
        }

        public boolean taskResultVertexExists(Id id) {
            return this.vertexExists(HugeTaskResult.genId(id));
        }

        public TaskStatus taskStatus(Id id) {
            return this.task(id, false).status();
        }

        public int resultReadCount() {
            return this.resultReadCount.get();
        }

        public void resetResultReadCount() {
            this.resultReadCount.set(0);
        }

        public void forbidResultRead(boolean forbid) {
            this.forbidResultRead = forbid;
        }

        public void failNextTaskResultDelete() {
            this.failNextTaskResultDelete = true;
        }

        public void allowTaskResultDelete() {
            this.failNextTaskResultDelete = false;
        }

        public void closeAndShutdown() {
            try {
                this.close();
            } finally {
                this.schedulerExecutor.shutdownNow();
                this.taskDbExecutor.shutdownNow();
                this.schemaTaskExecutor.shutdownNow();
                this.olapTaskExecutor.shutdownNow();
                this.gremlinTaskExecutor.shutdownNow();
                this.ephemeralTaskExecutor.shutdownNow();
                this.serverInfoDbExecutor.shutdownNow();
            }
        }

        private boolean vertexExists(Object id) {
            return this.call(() -> {
                Iterator<Vertex> vertices = this.tx().queryTaskInfos(id);
                HugeVertex vertex = (HugeVertex) QueryResults.one(vertices);
                return vertex != null;
            });
        }

        private void checkResultReadAllowed() {
            if (this.forbidResultRead) {
                throw new AssertionError("Unexpected task result read");
            }
        }
    }
}
