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

import jakarta.ws.rs.client.Entity;
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

    @Test
    public void testCloneSchemaWithoutGraphData() {
        String source = GRAPHS_PATH + "/schema_source";
        String target = GRAPHS_PATH + "/schema_target";
        try {
            assertResponseStatus(201, createGraphInRocksDB(SPACE, "schema_source"));
            createAndAssert(source + "/schema/propertykeys",
                            "{\"name\":\"name\",\"data_type\":\"TEXT\"," +
                            "\"cardinality\":\"SINGLE\"}", 202);
            createAndAssert(source + "/schema/vertexlabels",
                            "{\"name\":\"person\",\"id_strategy\":\"AUTOMATIC\"," +
                            "\"properties\":[\"name\"],\"nullable_keys\":[]}");
            createAndAssert(source + "/schema/indexlabels",
                            "{\"name\":\"personByName\",\"base_type\":\"VERTEX_LABEL\"," +
                            "\"base_value\":\"person\",\"index_type\":\"SECONDARY\"," +
                            "\"fields\":[\"name\"],\"rebuild\":false}", 202);
            createAndAssert(source + "/graph/vertices",
                            "{\"label\":\"person\",\"properties\":{\"name\":\"alice\"}}");

            Response cloned = client().target(baseUrl()).path(target + "/schema-clone")
                                      .queryParam("source", "schema_source").request()
                                      .post(Entity.json("{\"nickname\":\"SchemaCopy\"}"));
            String result = assertResponseStatus(201, cloned);
            Assert.assertEquals("schema_target", JsonUtil.fromJson(result, Map.class).get("name"));
            Assert.assertEquals("SchemaCopy", JsonUtil.fromJson(result, Map.class).get("nickname"));
            Assert.assertEquals("name", assertJsonContains(assertResponseStatus(200,
                    client().get(target + "/schema/propertykeys/name")), "name"));
            Assert.assertEquals("person", assertJsonContains(assertResponseStatus(200,
                    client().get(target + "/schema/vertexlabels/person")), "name"));
            Assert.assertEquals("person", assertJsonContains(assertResponseStatus(200,
                    client().get(target + "/schema/indexlabels/personByName")), "base_value"));
            String vertices = assertResponseStatus(200, client().get(target + "/graph/vertices"));
            Assert.assertTrue(readList(vertices, "vertices", Map.class).isEmpty());
        } finally {
            Map<String, Object> confirm = ImmutableMap.of(
                    "confirm_message", "I'm sure to drop the graph");
            client().delete(target, confirm);
            client().delete(source, confirm);
        }
    }

    @Test
    public void testSchemaCloneRejectsOrdinaryEdgeBeforeCreatingTarget() {
        String source = GRAPHS_PATH + "/schema_edge_source";
        String target = GRAPHS_PATH + "/schema_edge_target";
        try {
            assertResponseStatus(201, createGraphInRocksDB(SPACE, "schema_edge_source"));
            createAndAssert(source + "/schema/vertexlabels",
                            "{\"name\":\"person\",\"id_strategy\":\"AUTOMATIC\"}");
            createAndAssert(source + "/schema/edgelabels",
                            "{\"name\":\"knows\",\"source_label\":\"person\"," +
                            "\"target_label\":\"person\",\"frequency\":\"SINGLE\"}");
            Response response = client().target(baseUrl()).path(target + "/schema-clone")
                                        .queryParam("source", "schema_edge_source").request()
                                        .post(Entity.json("{}"));
            String error = assertResponseStatus(400, response);
            Assert.assertTrue(error.contains("edge labels"));
            Assert.assertTrue(client().get(target).getStatus() >= 400);
        } finally {
            Map<String, Object> confirm = ImmutableMap.of(
                    "confirm_message", "I'm sure to drop the graph");
            client().delete(target, confirm);
            client().delete(source, confirm);
        }
    }

    @Test
    public void testSchemaCloneRejectsMissingSourceBeforeCreatingTarget() {
        String target = GRAPHS_PATH + "/invalid_schema_target";
        Response response = client().target(baseUrl()).path(target + "/schema-clone")
                                    .queryParam("source", "missing_source").request()
                                    .post(Entity.json("{}"));
        Assert.assertTrue(response.getStatus() >= 400);
        Assert.assertTrue(client().get(target).getStatus() >= 400);
    }

    @Test
    public void testSchemaCloneRejectsDataOption() {
        String target = GRAPHS_PATH + "/invalid_data_target";
        Response response = client().target(baseUrl()).path(target + "/schema-clone")
                                    .queryParam("source", "hugegraph").request()
                                    .post(Entity.json("{\"load_data\":true}"));
        Assert.assertTrue(response.getStatus() >= 400);
        Assert.assertTrue(client().get(target).getStatus() >= 400);
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
