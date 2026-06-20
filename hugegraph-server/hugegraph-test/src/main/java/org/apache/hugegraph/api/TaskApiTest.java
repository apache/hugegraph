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

package org.apache.hugegraph.api;

import java.util.List;
import java.util.Map;

import org.apache.hugegraph.testutil.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

import jakarta.ws.rs.core.Response;

public class TaskApiTest extends BaseApiTest {

    private static final String PATH = "/graphspaces/DEFAULT/graphs/hugegraph/tasks/";

    @Before
    public void prepareSchema() {
        BaseApiTest.initPropertyKey();
        BaseApiTest.initVertexLabel();
        BaseApiTest.initIndexLabel();
    }

    @Test
    public void testList() {
        // create a task
        int taskId = this.rebuild();

        Response r = client().get(PATH, ImmutableMap.of("limit", -1));
        String content = assertResponseStatus(200, r);
        List<Map<?, ?>> tasks = assertJsonContains(content, "tasks");
        assertArrayContains(tasks, "id", taskId);

        waitTaskSuccess(taskId);

        r = client().get(PATH, String.valueOf(taskId));
        content = assertResponseStatus(200, r);
        String status = assertJsonContains(content, "task_status");
        Assert.assertEquals("success", status);

        /*
         * FIXME: sometimes may get results of RUNNING tasks after the task
         *        status is SUCCESS, which is stored in DB if there are worker
         *        nodes in raft-api test.
         * NOTE: seems the master node won't store task status in memory,
         *       because only worker nodes store task status in memory.
         */
        r = client().get(PATH, ImmutableMap.of("status", "RUNNING"));
        content = assertResponseStatus(200, r);
        tasks = assertJsonContains(content, "tasks");
        String message = String.format("Expect none RUNNING tasks(%d), but got %s", taskId, tasks);
        Assert.assertTrue(message, tasks.isEmpty());
    }

    @Test
    public void testGet() {
        // create a task
        int taskId = this.rebuild();

        Response r = client().get(PATH, String.valueOf(taskId));
        String content = assertResponseStatus(200, r);
        assertJsonContains(content, "id");

        waitTaskSuccess(taskId);

        r = client().get(PATH, String.valueOf(taskId));
        content = assertResponseStatus(200, r);
        String status = assertJsonContains(content, "task_status");
        Assert.assertEquals("success", status);
    }

    @Test
    public void testGetWithoutResult() {
        int taskId = this.gremlinJob("1 + 2");

        waitTaskSuccess(taskId);

        Response r = client().get(PATH, ImmutableMap.of("limit", -1));
        String content = assertResponseStatus(200, r);
        Assert.assertFalse(content, content.contains("task_result"));

        r = client().get(PATH, String.valueOf(taskId));
        content = assertResponseStatus(200, r);
        assertJsonContains(content, "task_result");

        r = client().get(PATH + taskId,
                         ImmutableMap.of("with_result", false));
        content = assertResponseStatus(200, r);
        assertJsonContains(content, "id");
        assertJsonContains(content, "task_callable");
        Assert.assertFalse(content, content.contains("task_result"));
    }

    @Test
    public void testCancel() {
        // create a task
        int taskId = this.gremlinJob();

        sleepAWhile();

        // cancel task
        Map<String, Object> params = ImmutableMap.of("action", "cancel");
        Response r = client().put(PATH, String.valueOf(taskId), "", params);
        String content = r.readEntity(String.class);
        Assert.assertTrue(content,
                          r.getStatus() == 202 || r.getStatus() == 400);
        if (r.getStatus() == 202) {
            String status = assertJsonContains(content, "task_status");
            Assert.assertTrue(status, "cancelling".equals(status) || "cancelled".equals(status));
            /*
             * NOTE: should be waitTaskStatus(taskId, "cancelled"), but worker
             * node may ignore the CANCELLING status due to now we can't atomically
             * update task status, and then the task is running to SUCCESS.
             */
            waitTaskCompleted(taskId);
        } else {
            assert r.getStatus() == 400;
            String error = String.format("Can't cancel task '%s' which is completed", taskId);
            Assert.assertContains(error, content);

            r = client().get(PATH, String.valueOf(taskId));
            content = assertResponseStatus(200, r);
            String status = assertJsonContains(content, "task_status");
            Assert.assertEquals("success", status);
        }
    }

    @Test
    public void testDelete() {
        // create a task
        int taskId = this.rebuild();

        waitTaskSuccess(taskId);
        // delete task
        Response r = client().delete(PATH, String.valueOf(taskId));
        assertResponseStatus(204, r);
    }

    private int rebuild() {
        // create a rebuild_index task
        String rebuildPath = "/graphspaces/DEFAULT/graphs/hugegraph/jobs/rebuild/indexlabels";
        String personByCity = "personByCity";
        Map<String, Object> params = ImmutableMap.of();
        Response r = client().put(rebuildPath, personByCity, "", params);
        String content = assertResponseStatus(202, r);
        return assertJsonContains(content, "task_id");
    }

    private int gremlinJob() {
        return this.gremlinJob("Thread.sleep(1000L)");
    }

    private int gremlinJob(String gremlin) {
        String body = "{" +
                      "\"gremlin\":\"" + gremlin + "\"," +
                      "\"bindings\":{}," +
                      "\"language\":\"gremlin-groovy\"," +
                      "\"aliases\":{}}";
        String path = "/graphspaces/DEFAULT/graphs/hugegraph/jobs/gremlin";
        String content = assertResponseStatus(201, client().post(path, body));
        return assertJsonContains(content, "task_id");
    }

    private void sleepAWhile() {
        try {
            Thread.sleep(200L);
        } catch (InterruptedException e) {
            // ignore
        }
    }
}
