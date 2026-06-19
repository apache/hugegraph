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

package org.apache.hugegraph.core;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Random;
import java.util.concurrent.TimeoutException;

import org.apache.hugegraph.HugeException;
import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.api.job.GremlinAPI.GremlinRequest;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.id.IdGenerator;
import org.apache.hugegraph.exception.NotFoundException;
import org.apache.hugegraph.job.EphemeralJob;
import org.apache.hugegraph.job.EphemeralJobBuilder;
import org.apache.hugegraph.job.GremlinJob;
import org.apache.hugegraph.job.JobBuilder;
import org.apache.hugegraph.task.HugeTask;
import org.apache.hugegraph.task.TaskCallable;
import org.apache.hugegraph.task.TaskScheduler;
import org.apache.hugegraph.task.TaskStatus;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.testutil.Whitebox;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class TaskCoreTest extends BaseCoreTest {

    @Before
    @Override
    public void setup() {
        super.setup();

        HugeGraph graph = graph();
        TaskScheduler scheduler = graph.taskScheduler();

        Iterator<HugeTask<Object>> iter = scheduler.tasks(null, -1, null);
        while (iter.hasNext()) {
            scheduler.delete(iter.next().id(), false);
        }
    }

    @Test
    public void testTask() throws TimeoutException {
        HugeGraph graph = graph();
        TaskScheduler scheduler = graph.taskScheduler();

        TaskCallable<Object> callable = new SleepCallable<>();

        Id id = IdGenerator.of(88888);
        HugeTask<?> task = new HugeTask<>(id, null, callable);
        task.type("test");
        task.name("test-task");

        scheduler.schedule(task);
        Assert.assertEquals(id, task.id());
        Assert.assertFalse(task.completed());

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            scheduler.delete(id, false);
        }, e -> {
            Assert.assertContains("Can't delete incomplete task '88888'",
                                  e.getMessage());
        });

        task = scheduler.waitUntilTaskCompleted(task.id(), 10);
        Assert.assertEquals(id, task.id());
        Assert.assertEquals("test-task", task.name());
        Assert.assertEquals(TaskStatus.SUCCESS, task.status());

        Assert.assertEquals("test-task", scheduler.task(id).name());
        Assert.assertEquals("test-task", scheduler.tasks(Arrays.asList(id))
                                                  .next().name());

        Iterator<HugeTask<Object>> iter = scheduler.tasks(ImmutableList.of(id));
        Assert.assertTrue(iter.hasNext());
        Assert.assertEquals("test-task", iter.next().name());
        Assert.assertFalse(iter.hasNext());

        iter = scheduler.tasks(TaskStatus.SUCCESS, 10, null);
        Assert.assertTrue(iter.hasNext());
        Assert.assertEquals("test-task", iter.next().name());
        Assert.assertFalse(iter.hasNext());

        iter = scheduler.tasks(null, 10, null);
        Assert.assertTrue(iter.hasNext());
        Assert.assertEquals("test-task", iter.next().name());
        Assert.assertFalse(iter.hasNext());

        scheduler.delete(id, false);
        iter = scheduler.tasks(null, 10, null);
        Assert.assertFalse(iter.hasNext());
        Assert.assertThrows(NotFoundException.class, () -> {
            scheduler.task(id);
        });
    }

    @Test
    public void testTaskWithoutResult() throws TimeoutException {
        HugeGraph graph = graph();
        TaskScheduler scheduler = graph.taskScheduler();

        Id id = IdGenerator.of(88889);
        HugeTask<?> task = new HugeTask<>(id, null, new SleepCallable<>());
        task.type("test");
        task.name("metadata-task-in-memory");
        scheduler.schedule(task);

        try {
            Whitebox.setInternalState(task, "result", "\"in-memory-result\"");

            HugeTask<?> taskWithoutResult = scheduler.task(id, false);
            Assert.assertEquals("metadata-task-in-memory",
                                taskWithoutResult.name());
            Assert.assertNull(taskWithoutResult.result());

            Iterator<HugeTask<Object>> iter = scheduler.tasks(ImmutableList.of(id),
                                                              false);
            Assert.assertTrue(iter.hasNext());
            taskWithoutResult = iter.next();
            Assert.assertEquals("metadata-task-in-memory",
                                taskWithoutResult.name());
            Assert.assertNull(taskWithoutResult.result());
            Assert.assertFalse(iter.hasNext());
        } finally {
            Whitebox.setInternalState(task, "result", null);
        }

        scheduler.waitUntilTaskCompleted(id, 10);
        scheduler.delete(id, false);

        id = IdGenerator.of(88890);
        task = new HugeTask<>(id, null, new MetadataResultCallable());
        task.type("test");
        task.name("metadata-task");
        scheduler.schedule(task);

        scheduler.waitUntilTaskCompleted(id, 10);

        HugeTask<?> taskWithResult = scheduler.task(id, true);
        Assert.assertEquals("\"metadata-result\"", taskWithResult.result());

        HugeTask<?> taskWithoutResult = scheduler.task(id, false);
        Assert.assertEquals("metadata-task", taskWithoutResult.name());
        Assert.assertNull(taskWithoutResult.result());

        Iterator<HugeTask<Object>> iter = scheduler.tasks(ImmutableList.of(id));
        Assert.assertTrue(iter.hasNext());
        taskWithResult = iter.next();
        Assert.assertEquals("metadata-task", taskWithResult.name());
        Assert.assertEquals("\"metadata-result\"", taskWithResult.result());
        Assert.assertFalse(iter.hasNext());

        iter = scheduler.tasks(ImmutableList.of(id), false);
        Assert.assertTrue(iter.hasNext());
        taskWithoutResult = iter.next();
        Assert.assertEquals("metadata-task", taskWithoutResult.name());
        Assert.assertNull(taskWithoutResult.result());
        Assert.assertFalse(iter.hasNext());

        iter = scheduler.tasks(TaskStatus.SUCCESS, 10, null, false);
        Assert.assertTrue(iter.hasNext());
        taskWithoutResult = iter.next();
        Assert.assertEquals("metadata-task", taskWithoutResult.name());
        Assert.assertNull(taskWithoutResult.result());

        iter = scheduler.tasks(TaskStatus.SUCCESS, 10, null);
        Assert.assertTrue(iter.hasNext());
        taskWithResult = iter.next();
        Assert.assertEquals("metadata-task", taskWithResult.name());
        Assert.assertEquals("\"metadata-result\"", taskWithResult.result());

        Id taskId = id;
        scheduler.delete(taskId, false);
        Assert.assertThrows(NotFoundException.class, () -> {
            scheduler.task(taskId);
        });
    }

    @Test
    public void testTaskWithFailure() throws TimeoutException {
        HugeGraph graph = graph();
        TaskScheduler scheduler = graph.taskScheduler();

        TaskCallable<Integer> callable = new TaskCallable<Integer>() {
            @Override
            public Integer call() throws Exception {
                sleepAWhile();
                return 125;
            }

            @Override
            protected void done() {
                scheduler.save(this.task());
            }
        };

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            new HugeTask<>(null, null, callable);
        }, e -> {
            Assert.assertContains("Task id can't be null", e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            Id id = IdGenerator.of("88888");
            new HugeTask<>(id, null, callable);
        }, e -> {
            Assert.assertContains("Invalid task id type, it must be number",
                                  e.getMessage());
        });

        Assert.assertThrows(NullPointerException.class, () -> {
            Id id = IdGenerator.of(88888);
            new HugeTask<>(id, null, null);
        });

        Assert.assertThrows(IllegalStateException.class, () -> {
            Id id = IdGenerator.of(88888);
            HugeTask<?> task2 = new HugeTask<>(id, null, callable);
            task2.name("test-task");
            scheduler.schedule(task2);
        }, e -> {
            Assert.assertContains("Task type can't be null", e.getMessage());
        });

        Assert.assertThrows(IllegalStateException.class, () -> {
            Id id = IdGenerator.of(88888);
            HugeTask<?> task2 = new HugeTask<>(id, null, callable);
            task2.type("test");
            scheduler.schedule(task2);
        }, e -> {
            Assert.assertContains("Task name can't be null", e.getMessage());
        });
    }

    @Test
    public void testEphemeralJob() throws TimeoutException {
        HugeGraph graph = graph();
        TaskScheduler scheduler = graph.taskScheduler();

        EphemeralJobBuilder<Object> builder = EphemeralJobBuilder.of(graph);
        builder.name("test-job-ephemeral")
               .job(new EphemeralJob<Object>() {
                   @Override
                   public String type() {
                       return "test";
                   }

                   @Override
                   public Object execute() throws Exception {
                       sleepAWhile();
                       return ImmutableMap.of("k1", 13579, "k2", "24680");
                   }
               });

        HugeTask<Object> task = builder.schedule();
        Assert.assertEquals("test-job-ephemeral", task.name());
        Assert.assertEquals("test", task.type());
        Assert.assertFalse(task.completed());

        HugeTask<?> task2 = scheduler.waitUntilTaskCompleted(task.id(), 10);
        Assert.assertEquals(TaskStatus.SUCCESS, task.status());
        Assert.assertEquals("{\"k1\":13579,\"k2\":\"24680\"}", task.result());

        Assert.assertEquals(TaskStatus.SUCCESS, task2.status());
        Assert.assertEquals("{\"k1\":13579,\"k2\":\"24680\"}", task2.result());

        Assert.assertThrows(NotFoundException.class, () -> {
            scheduler.waitUntilTaskCompleted(task.id(), 10);
        });
        Assert.assertThrows(NotFoundException.class, () -> {
            scheduler.task(task.id());
        });
    }

    @Test
    public void testGremlinJob() throws TimeoutException {
        HugeGraph graph = graph();
        TaskScheduler scheduler = graph.taskScheduler();

        GremlinRequest request = new GremlinRequest();
        request.gremlin("sleep(100); 3 + 5");

        JobBuilder<Object> builder = JobBuilder.of(graph);
        builder.name("test-job-gremlin")
               .input(request.toJson())
               .job(new GremlinJob());

        HugeTask<Object> task = builder.schedule();
        Assert.assertEquals("test-job-gremlin", task.name());
        Assert.assertEquals("gremlin", task.type());
        Assert.assertFalse(task.completed());
        Assert.assertNull(task.result());

        task = scheduler.waitUntilTaskCompleted(task.id(), 10);
        Assert.assertEquals("test-job-gremlin", task.name());
        Assert.assertEquals("gremlin", task.type());
        Assert.assertEquals(TaskStatus.SUCCESS, task.status());
        Assert.assertEquals("8", task.result());

        task = scheduler.task(task.id());
        Assert.assertEquals("test-job-gremlin", task.name());
        Assert.assertEquals("gremlin", task.type());
        Assert.assertEquals(TaskStatus.SUCCESS, task.status());
        Assert.assertEquals("8", task.result());
    }

    @Test
    public void testGremlinJobWithScript() throws TimeoutException {
        HugeGraph graph = graph();
        TaskScheduler scheduler = graph.taskScheduler();

        String script = "schema=graph.schema();" +
                        "schema.propertyKey('name').asText().ifNotExist().create();" +
                        "schema.propertyKey('age').asInt().ifNotExist().create();" +
                        "schema.propertyKey('lang').asText().ifNotExist().create();" +
                        "schema.propertyKey('date').asDate().ifNotExist().create();" +
                        "schema.propertyKey('price').asInt().ifNotExist().create();" +
                        "schema.vertexLabel('person1').properties('name','age').ifNotExist()" +
                        ".create();" +
                        "schema.vertexLabel('person2').properties('name','age').ifNotExist()" +
                        ".create();" +
                        "schema.edgeLabel('knows').sourceLabel('person1').targetLabel('person2')." +
                        "properties('date').ifNotExist().create();" +
                        "for(int i = 0; i < 1000; i++) {" +
                        "  p1=graph.addVertex(T.label,'person1','name','p1-'+i,'age',29);" +
                        "  p2=graph.addVertex(T.label,'person2','name','p2-'+i,'age',27);" +
                        "  p1.addEdge('knows',p2,'date','2016-01-10');" +
                        "}";

        HugeTask<Object> task = runGremlinJob(script);
        task = scheduler.waitUntilTaskCompleted(task.id(), 10);
        Assert.assertEquals("test-gremlin-job", task.name());
        Assert.assertEquals("gremlin", task.type());
        Assert.assertEquals(TaskStatus.SUCCESS, task.status());
        Assert.assertEquals("[]", task.result());

        script = "g.V().count()";
        task = runGremlinJob(script);
        task = scheduler.waitUntilTaskCompleted(task.id(), 10);
        Assert.assertEquals(TaskStatus.SUCCESS, task.status());
        Assert.assertEquals("[2000]", task.result());

        script = "g.V().hasLabel('person1').count()";
        task = runGremlinJob(script);
        task = scheduler.waitUntilTaskCompleted(task.id(), 10);
        Assert.assertEquals(TaskStatus.SUCCESS, task.status());
        Assert.assertEquals("[1000]", task.result());

        script = "g.V().hasLabel('person2').count()";
        task = runGremlinJob(script);
        task = scheduler.waitUntilTaskCompleted(task.id(), 10);
        Assert.assertEquals(TaskStatus.SUCCESS, task.status());
        Assert.assertEquals("[1000]", task.result());

        script = "g.E().count()";
        task = runGremlinJob(script);
        task = scheduler.waitUntilTaskCompleted(task.id(), 10);
        Assert.assertEquals(TaskStatus.SUCCESS, task.status());
        Assert.assertEquals("[1000]", task.result());

        script = "g.E().hasLabel('knows').count()";
        task = runGremlinJob(script);
        task = scheduler.waitUntilTaskCompleted(task.id(), 10);
        Assert.assertEquals(TaskStatus.SUCCESS, task.status());
        Assert.assertEquals("[1000]", task.result());
    }

    @Test
    public void testGremlinJobWithSerializedResults() throws TimeoutException {
        HugeGraph graph = graph();
        TaskScheduler scheduler = graph.taskScheduler();

        String script = "schema=graph.schema();" +
                        "schema.propertyKey('name').asText().ifNotExist().create();" +
                        "schema.vertexLabel('char').useCustomizeNumberId()" +
                        "      .properties('name').ifNotExist().create();" +
                        "schema.edgeLabel('next').sourceLabel('char').targetLabel('char')" +
                        "      .properties('name').ifNotExist().create();" +
                        "g.addV('char').property(id,1).property('name','A').as('a')" +
                        " .addV('char').property(id,2).property('name','B').as('b')" +
                        " .addV('char').property(id,3).property('name','C').as('c')" +
                        " .addV('char').property(id,4).property('name','D').as('d')" +
                        " .addV('char').property(id,5).property('name','E').as('e')" +
                        " .addV('char').property(id,6).property('name','F').as('f')" +
                        " .addE('next').from('a').to('b').property('name','ab')" +
                        " .addE('next').from('b').to('c').property('name','bc')" +
                        " .addE('next').from('b').to('d').property('name','bd')" +
                        " .addE('next').from('c').to('d').property('name','cd')" +
                        " .addE('next').from('c').to('e').property('name','ce')" +
                        " .addE('next').from('d').to('e').property('name','de')" +
                        " .addE('next').from('e').to('f').property('name','ef')" +
                        " .addE('next').from('f').to('d').property('name','fd')" +
                        " .iterate();" +
                        "g.tx().commit(); g.E().count();";

        HugeTask<Object> task = runGremlinJob(script);
        task = scheduler.waitUntilTaskCompleted(task.id(), 10);
        Assert.assertEquals("test-gremlin-job", task.name());
        Assert.assertEquals("gremlin", task.type());
        Assert.assertEquals(TaskStatus.SUCCESS, task.status());
        Assert.assertEquals("[8]", task.result());

        Id edgeLabelId = graph.schema().getEdgeLabel("next").id();

        script = "g.V(1).outE().inV().path()";
        task = runGremlinJob(script);
        task = scheduler.waitUntilTaskCompleted(task.id(), 10);
        Assert.assertEquals(TaskStatus.SUCCESS, task.status());
        String expected = String.format("[{\"labels\":[[],[],[]],\"objects\":[" +
                                        "{\"id\":1,\"label\":\"char\",\"type\":\"vertex\"," +
                                        "\"properties\":{\"name\":\"A\"}}," +
                                        "{\"id\":\"L1>%s>%s>>L2\",\"label\":\"next\"," +
                                        "\"type\":\"edge\",\"outV\":1," +
                                        "\"outVLabel\":\"char\",\"inV\":2,\"" +
                                        "inVLabel\":\"char\",\"properties\":{\"name\":\"ab\"}}," +
                                        "{\"id\":2,\"label\":\"char\",\"type\":\"vertex\"," +
                                        "\"properties\":{\"name\":\"B\"}}" +
                                        "]}]", edgeLabelId, edgeLabelId);
        Assert.assertEquals(expected, task.result());

        script = "g.V(1).out().out().path()";
        task = runGremlinJob(script);
        task = scheduler.waitUntilTaskCompleted(task.id(), 10);
        Assert.assertEquals(TaskStatus.SUCCESS, task.status());
        expected = "[{\"labels\":[[],[],[]],\"objects\":[" +
                   "{\"id\":1,\"label\":\"char\",\"type\":\"vertex\"," +
                   "\"properties\":{\"name\":\"A\"}}," +
                   "{\"id\":2,\"label\":\"char\",\"type\":\"vertex\"," +
                   "\"properties\":{\"name\":\"B\"}}," +
                   "{\"id\":3,\"label\":\"char\",\"type\":\"vertex\"," +
                   "\"properties\":{\"name\":\"C\"}}]}," +
                   "{\"labels\":[[],[],[]],\"objects\":[" +
                   "{\"id\":1,\"label\":\"char\",\"type\":\"vertex\"," +
                   "\"properties\":{\"name\":\"A\"}}," +
                   "{\"id\":2,\"label\":\"char\",\"type\":\"vertex\"," +
                   "\"properties\":{\"name\":\"B\"}}," +
                   "{\"id\":4,\"label\":\"char\",\"type\":\"vertex\"," +
                   "\"properties\":{\"name\":\"D\"}}]}]";
        Assert.assertEquals(expected, task.result());

        script = "g.V(1).outE().inV().tree()";
        task = runGremlinJob(script);
        task = scheduler.waitUntilTaskCompleted(task.id(), 10);
        Assert.assertEquals(TaskStatus.SUCCESS, task.status());
        expected = String.format("[[{\"key\":{\"id\":1,\"label\":\"char\",\"type\":\"vertex\"," +
                                 "\"properties\":{\"name\":\"A\"}}," +
                                 "\"value\":[" +
                                 "{\"key\":{\"id\":\"L1>%s>%s>>L2\",\"label\":\"next\"," +
                                 "\"type\":\"edge\",\"outV\":1," +
                                 "\"outVLabel\":\"char\",\"inV\":2,\"inVLabel\":\"char\"," +
                                 "\"properties\":{\"name\":\"ab\"}}," +
                                 "\"value\":[{\"key\":{\"id\":2,\"label\":\"char\"," +
                                 "\"type\":\"vertex\"," +
                                 "\"properties\":{\"name\":\"B\"}},\"value\":[]}]}]}]]",
                                 edgeLabelId, edgeLabelId);
        Assert.assertEquals(expected, task.result());

        script = "g.V(1).out().out().tree()";
        task = runGremlinJob(script);
        task = scheduler.waitUntilTaskCompleted(task.id(), 10);
        Assert.assertEquals(TaskStatus.SUCCESS, task.status());
        expected = "[[{\"key\":{\"id\":1,\"label\":\"char\",\"type\":\"vertex\"," +
                   "\"properties\":{\"name\":\"A\"}}," +
                   "\"value\":[{\"key\":{\"id\":2,\"label\":\"char\",\"type\":\"vertex\"," +
                   "\"properties\":{\"name\":\"B\"}}," +
                   "\"value\":[" +
                   "{\"key\":{\"id\":3,\"label\":\"char\",\"type\":\"vertex\",\"properties\":" +
                   "{\"name\":\"C\"}},\"value\":[]}," +
                   "{\"key\":{\"id\":4,\"label\":\"char\",\"type\":\"vertex\",\"properties\":" +
                   "{\"name\":\"D\"}},\"value\":[]}]}]}]]";
        Assert.assertEquals(expected, task.result());
    }

    @Test
    public void testGremlinJobWithFailure() throws TimeoutException {
        HugeGraph graph = graph();
        TaskScheduler scheduler = graph.taskScheduler();

        JobBuilder<Object> builder = JobBuilder.of(graph);
        builder.name("test-job-gremlin")
               .input("")
               .job(new GremlinJob());
        HugeTask<Object> task = builder.schedule();
        task = scheduler.waitUntilTaskCompleted(task.id(), 10);
        Assert.assertEquals(TaskStatus.FAILED, task.status());
        Assert.assertContains("Can't read json", task.result());

        builder = JobBuilder.of(graph);
        builder.name("test-job-gremlin")
               .job(new GremlinJob());
        task = builder.schedule();
        scheduler.waitUntilTaskCompleted(task.id(), 10);
        task = scheduler.task(task.id());
        Assert.assertEquals(TaskStatus.FAILED, task.status());
        Assert.assertContains("The input can't be null", task.result());

        builder = JobBuilder.of(graph);
        builder.name("test-job-gremlin")
               .input("{}")
               .job(new GremlinJob());
        task = builder.schedule();
        task = scheduler.waitUntilTaskCompleted(task.id(), 10);
        Assert.assertEquals(TaskStatus.FAILED, task.status());
        Assert.assertContains("Invalid gremlin value 'null'", task.result());

        builder = JobBuilder.of(graph);
        builder.name("test-job-gremlin")
               .input("{\"gremlin\":8}")
               .job(new GremlinJob());
        task = builder.schedule();
        task = scheduler.waitUntilTaskCompleted(task.id(), 10);
        Assert.assertEquals(TaskStatus.FAILED, task.status());
        Assert.assertContains("Invalid gremlin value '8'", task.result());

        builder = JobBuilder.of(graph);
        builder.name("test-job-gremlin")
               .input("{\"gremlin\":\"\"}")
               .job(new GremlinJob());
        task = builder.schedule();
        task = scheduler.waitUntilTaskCompleted(task.id(), 10);
        Assert.assertEquals(TaskStatus.FAILED, task.status());
        Assert.assertContains("Invalid bindings value 'null'", task.result());

        builder = JobBuilder.of(graph);
        builder.name("test-job-gremlin")
               .input("{\"gremlin\":\"\", \"bindings\":\"\"}")
               .job(new GremlinJob());
        task = builder.schedule();
        task = scheduler.waitUntilTaskCompleted(task.id(), 10);
        Assert.assertEquals(TaskStatus.FAILED, task.status());
        Assert.assertContains("Invalid bindings value ''", task.result());

        builder = JobBuilder.of(graph);
        builder.name("test-job-gremlin")
               .input("{\"gremlin\":\"\", \"bindings\":{}}")
               .job(new GremlinJob());
        task = builder.schedule();
        task = scheduler.waitUntilTaskCompleted(task.id(), 10);
        Assert.assertEquals(TaskStatus.FAILED, task.status());
        Assert.assertContains("Invalid language value 'null'", task.result());

        builder = JobBuilder.of(graph);
        builder.name("test-job-gremlin")
               .input("{\"gremlin\":\"\", \"bindings\":{}, \"language\":{}}")
               .job(new GremlinJob());
        task = builder.schedule();
        task = scheduler.waitUntilTaskCompleted(task.id(), 10);
        Assert.assertEquals(TaskStatus.FAILED, task.status());
        Assert.assertContains("Invalid language value '{}'", task.result());

        builder = JobBuilder.of(graph);
        builder.name("test-job-gremlin")
               .input("{\"gremlin\":\"\", \"bindings\":{}, \"language\":\"\"}")
               .job(new GremlinJob());
        task = builder.schedule();
        task = scheduler.waitUntilTaskCompleted(task.id(), 10);
        Assert.assertEquals(TaskStatus.FAILED, task.status());
        Assert.assertContains("Invalid aliases value 'null'", task.result());

        builder = JobBuilder.of(graph);
        builder.name("test-job-gremlin")
               .input("{\"gremlin\":\"\", \"bindings\":{}, " +
                      "\"language\":\"test\", \"aliases\":{}}")
               .job(new GremlinJob());
        task = builder.schedule();
        task = scheduler.waitUntilTaskCompleted(task.id(), 10);
        Assert.assertEquals(TaskStatus.FAILED, task.status());
        Assert.assertContains("test is not an available GremlinScriptEngine",
                              task.result());
    }

    @Test
    public void testGremlinJobWithError() throws TimeoutException {
        HugeGraph graph = graph();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            JobBuilder.of(graph)
                      .job(new GremlinJob())
                      .schedule();
        }, e -> {
            Assert.assertContains("Job name can't be null", e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            JobBuilder.of(graph)
                      .name("test-job-gremlin")
                      .schedule();
        }, e -> {
            Assert.assertContains("Job callable can't be null", e.getMessage());
        });

        // Test failure task with big input
        int length = 8 * 1024 * 1024;
        Random random = new Random();
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            sb.append("node:").append(random.nextInt(1000));
        }
        String bigInput = sb.toString();
        Assert.assertThrows(HugeException.class, () -> {
            runGremlinJob(bigInput);
        }, e -> {
            Assert.assertContains("Task input size", e.getMessage());
            Assert.assertContains("exceeded limit 16777216 bytes",
                                  e.getMessage());
        });
    }

    @Test
    public void testGremlinJobAndCancel() throws TimeoutException {
        HugeGraph graph = graph();
        TaskScheduler scheduler = graph.taskScheduler();

        HugeTask<Object> task = runGremlinJob("Thread.sleep(1000 * 10);");

        sleepAWhile(200 * 6);
        task = scheduler.task(task.id());
        scheduler.cancel(task);

        task = scheduler.task(task.id());
        Assert.assertEquals(TaskStatus.CANCELLING, task.status());

        task = scheduler.waitUntilTaskCompleted(task.id(), 10);
        Assert.assertEquals(TaskStatus.CANCELLED, task.status());
        Assert.assertEquals("test-gremlin-job", task.name());
        Assert.assertTrue(task.result(), task.result() == null ||
                                         task.result().endsWith("InterruptedException"));

        // Cancel success task
        HugeTask<Object> task2 = runGremlinJob("1+2");
        task2 = scheduler.waitUntilTaskCompleted(task2.id(), 10);
        Assert.assertEquals(TaskStatus.SUCCESS, task2.status());
        scheduler.cancel(task2);
        task2 = scheduler.task(task2.id());
        Assert.assertEquals(TaskStatus.SUCCESS, task2.status());
        Assert.assertEquals("3", task2.result());

        // Cancel failure task with big results (job size exceeded limit)
        String bigList = "def l=[]; for (i in 1..800001) l.add(i); l;";
        HugeTask<Object> task3 = runGremlinJob(bigList);
        task3 = scheduler.waitUntilTaskCompleted(task3.id(), 12);
        Assert.assertEquals(TaskStatus.FAILED, task3.status());
        scheduler.cancel(task3);
        task3 = scheduler.task(task3.id());
        Assert.assertEquals(TaskStatus.FAILED, task3.status());
        Assert.assertContains("LimitExceedException: Job results size 800001 " +
                              "has exceeded the max limit 800000",
                              task3.result());

        // Cancel failure task with big results (task exceeded limit 16M)
        String bigResults = "def random = new Random(); def rs=[];" +
                            "for (i in 0..4) {" +
                            "  def len = 1024 * 1024;" +
                            "  def item = new StringBuilder(len);" +
                            "  for (j in 0..len) { " +
                            "    item.append(\"node:\"); " +
                            "    item.append((char) random.nextInt(256)); " +
                            "    item.append(\",\"); " +
                            "  };" +
                            "  rs.add(item);" +
                            "};" +
                            "rs;";
        HugeTask<Object> task4 = runGremlinJob(bigResults);
        task4 = scheduler.waitUntilTaskCompleted(task4.id(), 10);
        Assert.assertEquals(TaskStatus.FAILED, task4.status());
        scheduler.cancel(task4);
        task4 = scheduler.task(task4.id());
        Assert.assertEquals(TaskStatus.FAILED, task4.status());
        Assert.assertContains("LimitExceedException: Task result size",
                              task4.result());
        Assert.assertContains("exceeded limit 16777216 bytes",
                              task4.result());
    }

    @Test
    public void testGremlinJobAndRestore() throws Exception {
        HugeGraph graph = graph();
        TaskScheduler scheduler = graph.taskScheduler();

        String gremlin = "println('task start');" +
                         "for(int i=gremlinJob.progress(); i<=10; i++) {" +
                         "  gremlinJob.updateProgress(i);" +
                         "  Thread.sleep(200); " +
                         "  println('sleep=>'+i);" +
                         "}; 100;";
        HugeTask<Object> task = runGremlinJob(gremlin);

        sleepAWhile(200 * 6);
        task = scheduler.task(task.id());
        scheduler.cancel(task);

        task = scheduler.task(task.id());
        Assert.assertEquals(TaskStatus.CANCELLING, task.status());

        task = scheduler.waitUntilTaskCompleted(task.id(), 10);
        Assert.assertEquals(TaskStatus.CANCELLED, task.status());
        Assert.assertTrue("progress=" + task.progress(),
                          0 < task.progress() && task.progress() < 10);
        Assert.assertEquals(0, task.retries());
        Assert.assertEquals(null, task.result());

        HugeTask<Object> finalTask = task;
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            Whitebox.invoke(scheduler.getClass(), "restore", scheduler,
                            finalTask);
        }, e -> {
            Assert.assertContains("No need to restore completed task",
                                  e.getMessage());
        });

        HugeTask<Object> task2 = scheduler.task(task.id());
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            Whitebox.invoke(scheduler.getClass(), "restore", scheduler, task2);
        }, e -> {
            Assert.assertContains("No need to restore completed task",
                                  e.getMessage());
        });

        Whitebox.setInternalState(task2, "status", TaskStatus.RUNNING);
        Whitebox.invoke(scheduler.getClass(), "restore", scheduler, task2);

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            Whitebox.invoke(scheduler.getClass(), "restore", scheduler, task2);
        }, e -> {
            Assert.assertContains("is already in the queue", e.getMessage());
        });

        scheduler.waitUntilTaskCompleted(task2.id(), 10);
        sleepAWhile(500);
        Assert.assertEquals(10, task2.progress());
        Assert.assertEquals(1, task2.retries());
        Assert.assertEquals("100", task2.result());
    }

    private HugeTask<Object> runGremlinJob(String gremlin) {
        HugeGraph graph = graph();

        GremlinRequest request = new GremlinRequest();
        request.gremlin(gremlin);

        JobBuilder<Object> builder = JobBuilder.of(graph);
        builder.name("test-gremlin-job")
               .input(request.toJson())
               .job(new GremlinJob());

        return builder.schedule();
    }

    private static void sleepAWhile() {
        sleepAWhile(100);
    }

    private static void sleepAWhile(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            // ignore
        }
    }

    public static class SleepCallable<V> extends TaskCallable<V> {

        public SleepCallable() {
            // pass
        }

        @Override
        public V call() throws Exception {
            Thread.sleep(1000);
            return null;
        }

        @Override
        public void done() {
            this.graph().taskScheduler().save(this.task());
        }
    }

    public static class MetadataResultCallable extends TaskCallable<String> {

        public MetadataResultCallable() {
            // pass
        }

        @Override
        public String call() {
            return "metadata-result";
        }

        @Override
        public void done() {
            this.graph().taskScheduler().save(this.task());
        }
    }
}
