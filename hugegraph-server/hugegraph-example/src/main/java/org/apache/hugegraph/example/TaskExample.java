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

package org.apache.hugegraph.example;

import java.util.Iterator;

import org.apache.commons.collections.IteratorUtils;
import org.apache.hugegraph.HugeFactory;
import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.id.IdGenerator;
import org.apache.hugegraph.task.HugeTask;
import org.apache.hugegraph.task.TaskCallable;
import org.apache.hugegraph.task.TaskScheduler;
import org.apache.hugegraph.task.TaskStatus;
import org.apache.hugegraph.testutil.Whitebox;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

public class TaskExample {

    private static final Logger LOG = Log.logger(TaskExample.class);

    public static void main(String[] args) throws Exception {
        LOG.info("TaskExample start!");

        HugeGraph graph = ExampleUtil.loadGraph();
        testTask(graph);
        graph.close();

        // Stop daemon thread
        HugeFactory.shutdown(30L);
    }

    public static void testTask(HugeGraph graph) throws InterruptedException {
        Id id = IdGenerator.of(8);
        String callable = "org.apache.hugegraph.example.TaskExample$TestTask";
        HugeTask<?> task = new HugeTask<>(id, null, callable, "test-parameter");
        task.type("type-1");
        task.name("test-task");

        TaskScheduler scheduler = graph.taskScheduler();
        scheduler.schedule(task);
        scheduler.save(task);
        Iterator<HugeTask<Object>> iter;
        iter = scheduler.tasks(TaskStatus.RUNNING, -1, null);
        LOG.info(">>>> running task: {}", IteratorUtils.toList(iter));

        Thread.sleep(TestTask.UNIT * 33);
        task.cancel(true);
        Thread.sleep(TestTask.UNIT * 1);
        scheduler.save(task);

        // Find task not finished(actually it should be RUNNING)
        iter = scheduler.tasks(TaskStatus.CANCELLED, -1, null);
        assert iter.hasNext();
        task = iter.next();

        LOG.info(">>>> task may be interrupted");

        Thread.sleep(TestTask.UNIT * 10);
        LOG.info(">>>> restore task...");
        Whitebox.setInternalState(task, "status", TaskStatus.RUNNING);
        scheduler.restoreTasks();
        Thread.sleep(TestTask.UNIT * 80);
        scheduler.save(task);

        iter = scheduler.tasks(TaskStatus.SUCCESS, -1, null);
        assert iter.hasNext();
        task = iter.next();
        assert task.status() == TaskStatus.SUCCESS;
        assert task.retries() == 1;
    }

    public static class TestTask extends TaskCallable<Integer> {

        public static final int UNIT = 100; // ms

        public volatile boolean run = true;

        @Override
        public Integer call() throws Exception {
            LOG.info(">>>> running task with parameter: {}", this.task().input());
            for (int i = this.task().progress(); i <= 100 && this.run; i++) {
                LOG.info(">>>> progress {}", i);
                this.task().progress(i);
                this.graph().taskScheduler().save(this.task());
                Thread.sleep(UNIT);
            }
            return 18;
        }
    }
}
