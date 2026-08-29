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
import org.apache.hugegraph.util.JsonUtil;
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
    public void testGetCompleteResult() {
        int taskId = this.gremlinJob("[1, 2, 3]");
        waitTaskSuccess(taskId);

        Response response = client().get(resultPath(taskId));

        Assert.assertEquals("no-store",
                            response.getHeaderString("Cache-Control"));
        Assert.assertEquals("[1,2,3]", assertResponseStatus(200, response));
    }

    @Test
    public void testGetCompleteResultWithCompression() {
        int taskId = this.gremlinJob("[1, 2, 3]");
        waitTaskSuccess(taskId);

        Response response = client().target().path(resultPath(taskId))
                                    .request()
                                    .header("Accept-Encoding", "gzip")
                                    .get();

        Assert.assertEquals("gzip",
                            response.getHeaderString("Content-Encoding"));
        Assert.assertEquals("[1,2,3]", assertResponseStatus(200, response));
    }

    @Test
    public void testHeadResultUsesMetadataOnlyResponse() {
        int taskId = this.gremlinJob("[1, 2, 3]");
        waitTaskSuccess(taskId);

        Response response = client().target().path(resultPath(taskId))
                                    .request()
                                    .header("Accept-Encoding", "gzip")
                                    .head();

        Assert.assertEquals(200, response.getStatus());
        Assert.assertEquals("no-store",
                            response.getHeaderString("Cache-Control"));
        Assert.assertEquals("gzip",
                            response.getHeaderString("Content-Encoding"));
        Assert.assertFalse(response.hasEntity());
    }

    @Test
    public void testGetArrayResultByPage() {
        int taskId = this.gremlinJob("[1, 2, 3, 4]");
        waitTaskSuccess(taskId);

        Response response = client().get(resultPath(taskId),
                                         ImmutableMap.of("limit", 2));
        Map<?, ?> first = JsonUtil.fromJson(
                assertResponseStatus(200, response), Map.class);
        Assert.assertEquals("array", first.get("root_type"));
        Assert.assertEquals(List.of(1, 2), first.get("items"));
        String nextPage = (String) first.get("page");
        Assert.assertNotNull(nextPage);

        response = client().get(resultPath(taskId),
                                ImmutableMap.of("page", nextPage));
        Map<?, ?> second = JsonUtil.fromJson(
                assertResponseStatus(200, response), Map.class);
        Assert.assertEquals("array", second.get("root_type"));
        Assert.assertEquals(List.of(3, 4), second.get("items"));
        Assert.assertNull(second.get("page"));
    }

    @Test
    public void testGetObjectResultByPage() {
        int taskId = this.gremlinJob("['a': 1, 'b': 2]");
        waitTaskSuccess(taskId);

        Response response = client().get(resultPath(taskId),
                                         ImmutableMap.of("limit", 1));
        Map<?, ?> first = JsonUtil.fromJson(
                assertResponseStatus(200, response), Map.class);
        List<?> items = (List<?>) first.get("items");
        Assert.assertEquals(1, items.size());
        Assert.assertEquals(ImmutableMap.of("key", "a", "value", 1),
                            items.get(0));
        Assert.assertNotNull(first.get("page"));
    }

    @Test
    public void testRejectScalarPaginationAndConflictingParameters() {
        int taskId = this.gremlinJob("1 + 2");
        waitTaskSuccess(taskId);

        Response response = client().get(resultPath(taskId),
                                         ImmutableMap.of("limit", 1));
        String content = assertResponseStatus(400, response);
        assertTaskResultError(content, "TaskResultNotPageableException");

        response = client().get(resultPath(taskId),
                                ImmutableMap.of("limit", 1,
                                                 "page", "invalid"));
        assertResponseStatus(400, response);
    }

    @Test
    public void testRejectResultUntilTaskSucceeds() {
        int taskId = this.gremlinJob("Thread.sleep(2000L); 3");

        Response response = client().get(resultPath(taskId));

        String content = assertResponseStatus(409, response);
        assertTaskResultError(content, "TaskResultNotReadyException");
        waitTaskSuccess(taskId);
    }

    @Test
    public void testFailedResultRemainsOnTaskDetailsEndpoint() {
        int taskId = this.gremlinJob(
                "throw new RuntimeException('expected failure')");
        waitTaskCompleted(taskId);

        Response response = client().get(resultPath(taskId));
        String content = assertResponseStatus(409, response);
        assertTaskResultError(content, "TaskResultNotReadableException");

        response = client().get(PATH, String.valueOf(taskId));
        content = assertResponseStatus(200, response);
        Assert.assertEquals("failed",
                            assertJsonContains(content, "task_status"));
        Assert.assertContains("expected failure",
                              assertJsonContains(content, "task_result"));
    }

    @Test
    public void testRejectTamperedPageToken() {
        int taskId = this.gremlinJob("[1, 2, 3]");
        waitTaskSuccess(taskId);
        Response response = client().get(resultPath(taskId),
                                         ImmutableMap.of("limit", 1));
        Map<?, ?> page = JsonUtil.fromJson(
                assertResponseStatus(200, response), Map.class);
        String token = (String) page.get("page");
        int index = token.indexOf('.') + 2;
        char replacement = token.charAt(index) == 'A' ? 'B' : 'A';
        String tampered = token.substring(0, index) + replacement +
                          token.substring(index + 1);

        response = client().get(resultPath(taskId),
                                ImmutableMap.of("page", tampered));

        String content = assertResponseStatus(400, response);
        assertTaskResultError(content, "InvalidTaskResultPageTokenException");
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

    private static String resultPath(int taskId) {
        return PATH + taskId + "/result";
    }

    private static void assertTaskResultError(String content,
                                              String exception) {
        Map<?, ?> error = JsonUtil.fromJson(content, Map.class);
        Assert.assertContains(exception, (String) error.get("exception"));
        Assert.assertFalse(error.containsKey("reason"));
    }

    private void sleepAWhile() {
        try {
            Thread.sleep(200L);
        } catch (InterruptedException e) {
            // ignore
        }
    }
}
