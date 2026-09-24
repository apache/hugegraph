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

package org.apache.hugegraph.api;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hugegraph.util.JsonUtil;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.core.Response;

public class GraphsApiTest extends BaseApiTest {

    private static final String TEMP_SPACE = "DEFAULT";
    private static final String PATH = "graphspaces/DEFAULT/graphs";

    @Test
    public void testListGraphs() {
        try {
            // Create multiple graphs
            Response r1 = createGraphInRocksDB(TEMP_SPACE, "listtest1");
            assertResponseStatus(201, r1);

            Response r2 = createGraphInRocksDB(TEMP_SPACE, "listtest2");
            assertResponseStatus(201, r2);

            // List all graphs
            Response r = client().get(PATH);
            String content = assertResponseStatus(200, r);

            Map<String, Object> result = JsonUtil.fromJson(content, Map.class);
            Assert.assertTrue(result.containsKey("graphs"));

            @SuppressWarnings("unchecked")
            List<String> graphs = (List<String>) result.get("graphs");
            Assert.assertTrue(graphs.contains("listtest1"));
            Assert.assertTrue(graphs.contains("listtest2"));
        } finally {
            // Clean up
            Map<String, Object> params = ImmutableMap.of(
                    "confirm_message", "I'm sure to drop the graph");
            client().delete(PATH + "/listtest1", params);
            client().delete(PATH + "/listtest2", params);
        }
    }

    @Test
    public void testGetGraph() {
        try {
            // Create a graph
            Response r = createGraphInRocksDB(TEMP_SPACE, "get_test", "GetTestGraph");
            assertResponseStatus(201, r);

            // Get the graph
            Response getResponse = client().get(PATH + "/get_test");
            String content = assertResponseStatus(200, getResponse);

            Map<String, Object> result = JsonUtil.fromJson(content, Map.class);
            Assert.assertTrue(result.containsKey("name"));
            Assert.assertTrue(result.containsKey("backend"));
            Assert.assertEquals("get_test", result.get("name"));
        } finally {
            // Clean up
            Map<String, Object> params = ImmutableMap.of(
                    "confirm_message", "I'm sure to drop the graph");
            client().delete(PATH + "/get_test", params);
        }
    }

    @Test
    public void testcreateGraphInRocksDB() {
        try {
            String config = "{\n" +
                            "  \"gremlin.graph\": \"org.apache.hugegraph.HugeFactory\",\n" +
                            "  \"backend\": \"rocksdb\",\n" +
                            "  \"serializer\": \"binary\",\n" +
                            "  \"store\": \"create_test\",\n" +
                            "  \"nickname\": \"CreateTestGraph\",\n" +
                            "  \"description\": \"Test graph creation\",\n" +
                            "  \"rocksdb.data_path\": \"rocksdbtest-data-create_test\",\n" +
                            "  \"rocksdb.wal_path\": \"rocksdbtest-data-create_test\"\n" +
                            "}";

            Response r = client().post(PATH + "/create_test",
                                       Entity.json(config));
            String content = assertResponseStatus(201, r);

            Map<String, Object> result = JsonUtil.fromJson(content, Map.class);
            Assert.assertEquals("create_test", result.get("name"));
            Assert.assertEquals("CreateTestGraph", result.get("nickname"));
            Assert.assertEquals("rocksdb", result.get("backend"));
            Assert.assertEquals("Test graph creation", result.get("description"));
        } finally {
            // Clean up
            Map<String, Object> params = ImmutableMap.of(
                    "confirm_message", "I'm sure to drop the graph");
            client().delete(PATH + "/create_test", params);
        }
    }

    @Test
    public void testcreateGraphInRocksDBWithMissingRequiredParams() {
        // Missing 'backend' parameter
        String config = "{\n" +
                        "  \"serializer\": \"binary\",\n" +
                        "  \"store\": \"invalid_test\"\n" +
                        "}";

        Response r = client().post(PATH + "/invalid_test",
                                   Entity.json(config));
        Assert.assertTrue(r.getStatus() >= 400);
    }

    @Test
    public void testCloneGraph() {
        try {
            // Create source graph
            Response r1 = createGraphInRocksDB(TEMP_SPACE, "clone_source", "SourceGraph");
            assertResponseStatus(201, r1);

            // Clone the graph
            String config = "{\n" +
                            "  \"gremlin.graph\": \"org.apache.hugegraph.HugeFactory\",\n" +
                            "  \"backend\": \"rocksdb\",\n" +
                            "  \"serializer\": \"binary\",\n" +
                            "  \"store\": \"clone_target\",\n" +
                            "  \"nickname\": \"ClonedGraph\",\n" +
                            "  \"rocksdb.data_path\": \"rocksdbtest-data-clone_target\",\n" +
                            "  \"rocksdb.wal_path\": \"rocksdbtest-data-clone_target\"\n" +
                            "}";

            Map<String, Object> params = ImmutableMap.of(
                    "clone_graph_name", "clone_source");

            String path = PATH + "/clone_target";
            Response r = client().target(baseUrl())
                                 .path(path)
                                 .queryParam("clone_graph_name", "clone_source")
                                 .request()
                                 .post(Entity.json(config));

            String content = assertResponseStatus(201, r);
            Map<String, Object> result = JsonUtil.fromJson(content, Map.class);
            Assert.assertEquals("clone_target", result.get("name"));
        } finally {
            // Clean up
            Map<String, Object> deleteParams = ImmutableMap.of(
                    "confirm_message", "I'm sure to drop the graph");
            client().delete(PATH + "/clone_source", deleteParams);
            client().delete(PATH + "/clone_target", deleteParams);
        }
    }

    @Test
    public void testDeleteGraph() {
        Response r = createGraphInRocksDB(TEMP_SPACE, "delete_test");
        assertResponseStatus(201, r);

        Map<String, Object> params = new HashMap<>();
        params.put("confirm_message", "I'm sure to drop the graph");

        r = client().delete(PATH + "/delete_test", params);
        assertResponseStatus(204, r);

        // Verify graph is deleted
        Response getResponse = client().get(PATH + "/delete_test");
        Assert.assertTrue(getResponse.getStatus() >= 400);
    }

    @Test
    public void testDeleteGraphWithoutConfirmMessage() {
        try {
            Response r = createGraphInRocksDB(TEMP_SPACE, "delete_no_confirm");
            assertResponseStatus(201, r);

            // Try to delete without confirmation
            Response deleteResponse = client().delete(PATH + "/delete_no_confirm",
                                                      new HashMap<>());
            Assert.assertTrue(deleteResponse.getStatus() >= 400);
        } finally {
            // Clean up properly
            Map<String, Object> params = ImmutableMap.of(
                    "confirm_message", "I'm sure to drop the graph");
            client().delete(PATH + "/delete_no_confirm", params);
        }
    }

    @Test
    public void testClearGraph() {
        try {
            Response r = createGraphInRocksDB(TEMP_SPACE, "clear_test");
            assertResponseStatus(201, r);

            Map<String, Object> params = ImmutableMap.of(
                    "confirm_message", "I'm sure to delete all data");

            Response clearResponse = client().delete(PATH + "/clear_test/clear",
                                                     params);
            assertResponseStatus(204, clearResponse);
        } finally {
            // Clean up
            Map<String, Object> deleteParams = ImmutableMap.of(
                    "confirm_message", "I'm sure to drop the graph");
            client().delete(PATH + "/clear_test", deleteParams);
        }
    }

    @Test
    public void testClearGraphWithoutConfirmMessage() {
        try {
            Response r = createGraphInRocksDB(TEMP_SPACE, "clear_no_confirm");
            assertResponseStatus(201, r);

            // Try to clear without confirmation
            Response clearResponse = client().delete(PATH + "/clear_no_confirm/clear",
                                                     new HashMap<>());
            Assert.assertTrue(clearResponse.getStatus() >= 400);
        } finally {
            // Clean up
            Map<String, Object> params = ImmutableMap.of(
                    "confirm_message", "I'm sure to drop the graph");
            client().delete(PATH + "/clear_no_confirm", params);
        }
    }

    @Test
    public void testSetGraphMode() {
        try {
            Response r = createGraphInRocksDB(TEMP_SPACE, "mode_test");
            assertResponseStatus(201, r);

            // Set mode to RESTORING
            String modeJson = "\"RESTORING\"";
            Response modeResponse = client().target(baseUrl())
                                            .path(PATH + "/mode_test/mode")
                                            .request()
                                            .put(Entity.json(modeJson));

            String content = assertResponseStatus(200, modeResponse);
            Map<String, Object> result = JsonUtil.fromJson(content, Map.class);
            Assert.assertEquals("RESTORING", result.get("mode"));
        } finally {
            // Clean up
            Map<String, Object> params = ImmutableMap.of(
                    "confirm_message", "I'm sure to drop the graph");
            client().delete(PATH + "/mode_test", params);
        }
    }

    @Test
    public void testGetGraphMode() {
        try {
            Response r = createGraphInRocksDB(TEMP_SPACE, "get_mode_test");
            assertResponseStatus(201, r);

            Response modeResponse = client().get(PATH + "/get_mode_test/mode");
            String content = assertResponseStatus(200, modeResponse);

            Map<String, Object> result = JsonUtil.fromJson(content, Map.class);
            Assert.assertTrue(result.containsKey("mode"));
        } finally {
            // Clean up
            Map<String, Object> params = ImmutableMap.of(
                    "confirm_message", "I'm sure to drop the graph");
            client().delete(PATH + "/get_mode_test", params);
        }
    }

    @Test
    public void testSetGraphReadMode() {
        try {
            Response r = createGraphInRocksDB(TEMP_SPACE, "read_mode_test");
            assertResponseStatus(201, r);

            // Set read mode to OLTP_ONLY
            String readModeJson = "\"OLTP_ONLY\"";
            Response readModeResponse = client().target(baseUrl())
                                                .path(PATH + "/read_mode_test/graph_read_mode")
                                                .request()
                                                .put(Entity.json(readModeJson));

            String content = assertResponseStatus(200, readModeResponse);
            Map<String, Object> result = JsonUtil.fromJson(content, Map.class);
            Assert.assertEquals("OLTP_ONLY", result.get("graph_read_mode"));
        } finally {
            // Clean up
            Map<String, Object> params = ImmutableMap.of(
                    "confirm_message", "I'm sure to drop the graph");
            client().delete(PATH + "/read_mode_test", params);
        }
    }

    @Test
    public void testGetGraphReadMode() {
        try {
            Response r = createGraphInRocksDB(TEMP_SPACE, "get_read_mode_test");
            assertResponseStatus(201, r);

            Response readModeResponse = client().get(PATH + "/get_read_mode_test/graph_read_mode");
            String content = assertResponseStatus(200, readModeResponse);

            Map<String, Object> result = JsonUtil.fromJson(content, Map.class);
            Assert.assertTrue(result.containsKey("graph_read_mode"));
        } finally {
            // Clean up
            Map<String, Object> params = ImmutableMap.of(
                    "confirm_message", "I'm sure to drop the graph");
            client().delete(PATH + "/get_read_mode_test", params);
        }
    }

    @Test
    public void testReloadGraphsWithInvalidAction() {
        String actionJson = "{\n" +
                            "  \"action\": \"invalid_action\"\n" +
                            "}";

        Response r = client().target(baseUrl())
                             .path(PATH + "/manage")
                             .request()
                             .put(Entity.json(actionJson));

        Assert.assertTrue(r.getStatus() >= 400);
    }

    @Test
    public void testGraphNotExist() {
        Response r = client().get(PATH + "/non_existent_graph");
        Assert.assertTrue(r.getStatus() >= 400);
    }
}
