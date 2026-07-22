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

import org.apache.hugegraph.util.JsonUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

import jakarta.ws.rs.core.Response;

/**
 * Tests for Hubble-compatible graph profile and default graph APIs
 * in standalone (RocksDB) mode:
 *  - GET  /graphspaces/{gs}/graphs/profile         — works with default graph
 *  - POST /graphspaces/{gs}/graphs/{name}/default  — sets default graph
 *  - DELETE /graphspaces/{gs}/graphs/{name}/default — unsets default graph
 *  - GET  /graphspaces/{gs}/graphs/default          — gets default graph
 *  - PUT  /graphspaces/{gs}/graphs/{name}           — update nickname
 */
public class GraphsApiStandaloneTest extends BaseApiTest {

    private static final String SPACE = "DEFAULT";
    private static final String GRAPHS_PATH = "graphspaces/DEFAULT/graphs";
    private static final String PROFILE_PATH = GRAPHS_PATH + "/profile";
    private static final String DEFAULT_PATH = GRAPHS_PATH + "/default";

    /** Graph names created per test — cleaned up in teardown */
    private static final String TEST_GRAPH = "gst_test_graph";
    private static final String TEST_GRAPH2 = "gst_test_graph2";

    @Before
    public void assumeStandalone() {
        assumeStandaloneMode();
    }

    @After
    @Override
    public void teardown() throws Exception {
        client().delete(GRAPHS_PATH + "/" + TEST_GRAPH + "/default",
                        ImmutableMap.of());
        client().delete(GRAPHS_PATH + "/" + TEST_GRAPH2 + "/default",
                        ImmutableMap.of());
        Map<String, Object> drop = ImmutableMap.of(
                "confirm_message", "I'm sure to drop the graph");
        client().delete(GRAPHS_PATH + "/" + TEST_GRAPH, drop);
        client().delete(GRAPHS_PATH + "/" + TEST_GRAPH2, drop);
        super.teardown();
    }

    // ------------------------------------------------------------------ //
    //  GET /graphspaces/{gs}/graphs/profile
    // ------------------------------------------------------------------ //

    @Test
    public void testListProfileReturnsGraphEntries() {
        Response r = client().get(PROFILE_PATH);
        String content = assertResponseStatus(200, r);

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> profiles = JsonUtil.fromJson(content, List.class);
        Assert.assertFalse("profile list should not be empty", profiles.isEmpty());

        for (Map<String, Object> p : profiles) {
            Assert.assertTrue("profile entry must have 'name'", p.containsKey("name"));
            Assert.assertTrue("profile entry must have 'default'", p.containsKey("default"));
        }
    }

    @Test
    public void testListProfileContainsCreatedGraph() {
        Response r = createGraphInRocksDB(SPACE, TEST_GRAPH, "ProfileTestNick");
        assertResponseStatus(201, r);

        Response profile = client().get(PROFILE_PATH);
        String content = assertResponseStatus(200, profile);

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> profiles = JsonUtil.fromJson(content, List.class);
        boolean found = profiles.stream()
                                .anyMatch(p -> TEST_GRAPH.equals(p.get("name")));
        Assert.assertTrue("Newly created graph should appear in profile list", found);
    }

    @Test
    public void testListProfileWithPrefixFilter() {
        Response r1 = createGraphInRocksDB(SPACE, TEST_GRAPH, "PrefixNick");
        assertResponseStatus(201, r1);
        Response r2 = createGraphInRocksDB(SPACE, TEST_GRAPH2, "OtherNick");
        assertResponseStatus(201, r2);

        Response profile = client().get(PROFILE_PATH,
                                        ImmutableMap.of("prefix", TEST_GRAPH));
        String content = assertResponseStatus(200, profile);

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> profiles = JsonUtil.fromJson(content, List.class);
        for (Map<String, Object> p : profiles) {
            String name = String.valueOf(p.get("name"));
            Object nickObj = p.get("nickname");
            String nick = nickObj != null ? String.valueOf(nickObj) : "";
            boolean matches = name.startsWith(TEST_GRAPH) ||
                              nick.startsWith(TEST_GRAPH);
            Assert.assertTrue("All returned profiles should match prefix: " + name,
                              matches);
        }
    }

    @Test
    public void testListProfileNicknameField() {
        Response r = createGraphInRocksDB(SPACE, TEST_GRAPH, "MyNickname");
        assertResponseStatus(201, r);

        Response profile = client().get(PROFILE_PATH);
        String content = assertResponseStatus(200, profile);

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> profiles = JsonUtil.fromJson(content, List.class);
        Map<String, Object> entry = profiles.stream()
                                            .filter(p -> TEST_GRAPH.equals(p.get("name")))
                                            .findFirst()
                                            .orElse(null);
        Assert.assertNotNull("Graph should be present in profile list", entry);
        Assert.assertTrue("Profile entry should contain 'nickname'",
                          entry.containsKey("nickname"));
        Assert.assertEquals("MyNickname", entry.get("nickname"));
    }

    @Test
    public void testListProfileContainsDefaultGraph() {
        Response r = createGraphInRocksDB(SPACE, TEST_GRAPH);
        assertResponseStatus(201, r);
        Response setR = client().post(GRAPHS_PATH + "/" + TEST_GRAPH + "/default", "");
        assertResponseStatus(200, setR);

        Response profile = client().get(PROFILE_PATH);
        String content = assertResponseStatus(200, profile);

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> profiles = JsonUtil.fromJson(content, List.class);
        Map<String, Object> entry = profiles.stream()
                                            .filter(p -> TEST_GRAPH.equals(p.get("name")))
                                            .findFirst()
                                            .orElse(null);
        Assert.assertNotNull(entry);
        Assert.assertEquals("default graph should be marked in standalone mode",
                            Boolean.TRUE, entry.get("default"));
    }

    // ------------------------------------------------------------------ //
    //  Canonical default graph operations work in standalone mode
    // ------------------------------------------------------------------ //

    @Test
    public void testSetDefaultGraph() {
        Response r = createGraphInRocksDB(SPACE, TEST_GRAPH);
        assertResponseStatus(201, r);

        Response setR = client().post(GRAPHS_PATH + "/" + TEST_GRAPH + "/default", "");
        String content = assertResponseStatus(200, setR);
        Assert.assertTrue(content.contains(TEST_GRAPH));

        Response repeatedSetR = client().post(GRAPHS_PATH + "/" + TEST_GRAPH +
                                              "/default", "");
        content = assertResponseStatus(200, repeatedSetR);
        Assert.assertTrue(content.contains(TEST_GRAPH));
    }

    @Test
    public void testUnsetDefaultGraph() {
        Response r = createGraphInRocksDB(SPACE, TEST_GRAPH);
        assertResponseStatus(201, r);

        Response setR = client().post(GRAPHS_PATH + "/" + TEST_GRAPH + "/default", "");
        assertResponseStatus(200, setR);

        Response unsetR = client().delete(GRAPHS_PATH + "/" + TEST_GRAPH + "/default",
                                          ImmutableMap.of());
        String content = assertResponseStatus(200, unsetR);
        Assert.assertFalse(content.contains(TEST_GRAPH));
    }

    @Test
    public void testSetDefaultGraphWithGetIsNotAllowed() {
        Response r = createGraphInRocksDB(SPACE, TEST_GRAPH);
        assertResponseStatus(201, r);

        Response setR = client().get(GRAPHS_PATH + "/" + TEST_GRAPH + "/default");
        assertResponseStatus(405, setR);
    }

    @Test
    public void testUnsetDefaultGraphWithGetIsNotFound() {
        Response r = createGraphInRocksDB(SPACE, TEST_GRAPH);
        assertResponseStatus(201, r);

        Response unsetR = client().get(GRAPHS_PATH + "/" + TEST_GRAPH + "/undefault");
        assertResponseStatus(404, unsetR);
    }

    @Test
    public void testGetDefaultGraph() {
        Response r = createGraphInRocksDB(SPACE, TEST_GRAPH);
        assertResponseStatus(201, r);
        Response setR = client().post(GRAPHS_PATH + "/" + TEST_GRAPH + "/default", "");
        assertResponseStatus(200, setR);

        Response getR = client().get(DEFAULT_PATH);
        String content = assertResponseStatus(200, getR);
        Assert.assertTrue(content.contains(TEST_GRAPH));
    }

    // ------------------------------------------------------------------ //
    //  PUT /graphspaces/{gs}/graphs/{name}  — update nickname
    // ------------------------------------------------------------------ //

    @Test
    public void testUpdateGraphNickname() {
        Response r = createGraphInRocksDB(SPACE, TEST_GRAPH, "OldNickname");
        assertResponseStatus(201, r);

        Response updateR = updateGraph("update", SPACE, TEST_GRAPH, "NewNickname");
        String content = assertResponseStatus(200, updateR);

        @SuppressWarnings("unchecked")
        Map<String, String> result = JsonUtil.fromJson(content, Map.class);
        Assert.assertTrue("Response should contain graph name key",
                          result.containsKey(TEST_GRAPH));
        Assert.assertEquals("updated", result.get(TEST_GRAPH));
    }

    @Test
    public void testUpdateGraphWithInvalidAction() {
        Response r = createGraphInRocksDB(SPACE, TEST_GRAPH);
        assertResponseStatus(201, r);

        String badBody = "{\"action\": \"invalid_action\", \"update\": {\"name\": \"" +
                         TEST_GRAPH + "\", \"nickname\": \"X\"}}";
        Response updateR = client().target(baseUrl())
                                   .path(GRAPHS_PATH + "/" + TEST_GRAPH)
                                   .request()
                                   .put(jakarta.ws.rs.client.Entity.json(badBody));
        assertResponseStatus(400, updateR);
    }

    @Test
    public void testUpdateGraphWithInvalidFieldTypeReturnsBadRequest() {
        Response r = createGraphInRocksDB(SPACE, TEST_GRAPH);
        assertResponseStatus(201, r);

        String badNameBody = "{\"action\": \"update\", \"update\": {\"name\": 123, " +
                             "\"nickname\": \"X\"}}";
        Response updateR = client().target(baseUrl())
                                   .path(GRAPHS_PATH + "/" + TEST_GRAPH)
                                   .request()
                                   .put(jakarta.ws.rs.client.Entity.json(badNameBody));
        assertResponseStatus(400, updateR);

        String badNicknameBody = "{\"action\": \"update\", \"update\": {\"name\": \"" +
                                 TEST_GRAPH + "\", \"nickname\": 123}}";
        updateR = client().target(baseUrl())
                          .path(GRAPHS_PATH + "/" + TEST_GRAPH)
                          .request()
                          .put(jakarta.ws.rs.client.Entity.json(badNicknameBody));
        assertResponseStatus(400, updateR);
    }

    @Test
    public void testCreateGraphWithInvalidConfigValueReturnsBadRequest() {
        String nullValueBody = "{\"nickname\":\"NullConfig\",\"search.text_analyzer\":null}";
        Response createR = client().target(baseUrl())
                                   .path(GRAPHS_PATH + "/" + TEST_GRAPH)
                                   .request()
                                   .post(jakarta.ws.rs.client.Entity.json(nullValueBody));
        assertResponseStatus(400, createR);

        String objectValueBody = "{\"nickname\":\"ObjectConfig\"," +
                                 "\"schema\":{\"name\":\"template\"}}";
        createR = client().target(baseUrl())
                          .path(GRAPHS_PATH + "/" + TEST_GRAPH2)
                          .request()
                          .post(jakarta.ws.rs.client.Entity.json(objectValueBody));
        assertResponseStatus(400, createR);
    }
}
