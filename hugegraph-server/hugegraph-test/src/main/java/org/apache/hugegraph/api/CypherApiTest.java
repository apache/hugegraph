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

import static org.apache.hugegraph.testutil.Assert.assertContains;

import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Map;

import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.util.JsonUtil;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.MultivaluedHashMap;
import jakarta.ws.rs.core.MultivaluedMap;
import jakarta.ws.rs.core.Response;

public class CypherApiTest extends BaseApiTest {

    private static final String PATH = URL_PREFIX + "/cypher";
    private static final String QUERY = "MATCH (n:cypher_person) WHERE n.city = 'Beijing' " +
                                        "RETURN n.name AS name ORDER BY name";

    @Before
    public void prepareSchema() {
        String schema = URL_PREFIX + "/schema/";
        for (String[] property : new String[][]{{"name", "TEXT"}, {"age", "INT"},
                                                {"city", "TEXT"}, {"active", "BOOLEAN"},
                                                {"note", "TEXT"}, {"birthday", "DATE"}}) {
            createAndAssert(schema + "propertykeys", JsonUtil.toJson(ImmutableMap.of(
                    "name", property[0], "data_type", property[1],
                    "cardinality", "SINGLE")), 202);
        }
        createAndAssert(schema + "vertexlabels", "{\"name\":\"cypher_person\", " +
                        "\"id_strategy\":\"PRIMARY_KEY\",\"primary_keys\":[\"name\"], " +
                        "\"properties\":[\"name\",\"age\",\"city\",\"active\",\"note\"," +
                        "\"birthday\"],\"nullable_keys\":[\"active\",\"note\",\"birthday\"]}");
        createAndAssert(schema + "edgelabels", "{\"name\":\"cypher_knows\", " +
                        "\"source_label\":\"cypher_person\",\"target_label\":\"cypher_person\"," +
                        "\"frequency\":\"SINGLE\",\"properties\":[],\"nullable_keys\":[]}");
        // Install indexes before inserting data; no existing data needs rebuilding.
        for (String[] index : new String[][]{{"city", "SECONDARY"}, {"age", "RANGE"}}) {
            createAndAssert(schema + "indexlabels", JsonUtil.toJson(ImmutableMap.builder()
                    .put("name", "cypher_person_by_" + index[0])
                    .put("base_type", "VERTEX_LABEL")
                    .put("base_value", "cypher_person")
                    .put("index_type", index[1])
                    .put("fields", Collections.singletonList(index[0]))
                    .put("rebuild", false).build()), 202);
        }
        addVertex("marko", 20, "Beijing", true, "2000-01-01");
        addVertex("peter", 30, "Beijing", false);
        addVertex("alice", 40, "Shanghai", true);
        addVertex("dave", 30, "Shanghai", false);
        addEdge("marko", "peter");
        addEdge("peter", "alice");
    }

    @Test
    public void testGet() {
        Map<String, Object> params = ImmutableMap.of("cypher", QUERY);
        Response r = client().get(PATH, params);

        assertColumn(assertCypherSuccessData(assertResponseStatus(200, r)), "name", "marko", "peter");
    }

    @Test
    public void testPost() {
        assertColumn(query(QUERY), "name", "marko", "peter");
    }

    @Test
    public void testRejectPlainTextPost() {
        Response response = client().post(PATH, Entity.entity(QUERY, MediaType.TEXT_PLAIN_TYPE));
        assertResponseStatus(415, response);
    }

    @Test
    public void testComputedRegexExecutesExtensionPredicate() {
        String query = "MATCH (n:cypher_person) WHERE (n.name + '') =~ 'mar.*' " +
                       "RETURN n.name AS name";
        String content = this.testCypherQueryAndContains(query, "marko");
        Assert.assertEquals(List.of(Map.of("name", "marko")), assertCypherSuccessData(content));
    }

    @Test
    public void testComputedRegexRequiresWholeStringMatch() {
        String query = "MATCH (n:cypher_person) WHERE (n.name + '') =~ 'ark' " +
                       "RETURN n.name AS name";
        String content = this.testCypherQueryAndContains(query, "data");
        Assert.assertTrue(assertCypherSuccessData(content).isEmpty());
    }

    @Test
    public void testDateFunctionMatchesDateProperty() {
        assertColumn(query("MATCH (n:cypher_person) WHERE n.birthday = date('2000-01-01') " +
                           "RETURN n.name AS name"), "name", "marko");
    }

    @Test
    public void testDateFunctionYieldsNoMatchOnTextProperty() {
        Map<?, ?> row = assertSingleMapRow(query(
                "MATCH (n:cypher_person {note: date('2024-02-05')}) RETURN count(n) AS count"));
        Assert.assertEquals(0L, ((Number) row.get("count")).longValue());
    }

    @Test
    public void testCreate() {
        assertColumn(query("CREATE (n:cypher_person {name:'test', age:20, city:'Hefei'}) " +
                           "RETURN n.name AS name"), "name", "test");
        assertNativeVertex("test", 20, "Hefei");
    }

    @Test
    public void testRelationQuery() {
        String cypher = "MATCH (n:cypher_person)-[r:cypher_knows]->(friend:cypher_person)\n" +
                        "WHERE n.name = 'marko'\n" +
                        "RETURN friend.name AS friend";
        assertColumn(query(cypher), "friend", "peter");
    }

    @Test
    public void testReturnNodeIdAsPrimitiveValue() {
        String cypher = "MATCH (n:cypher_person) WHERE n.name = 'marko' " +
                        "RETURN id(n) AS nodeId";

        String content = this.testCypherQueryAndContains(cypher, "nodeId");
        List<?> data = assertCypherSuccessData(content);
        Map<?, ?> row = assertSingleMapRow(data);
        Object nodeId = row.get("nodeId");

        Assert.assertEquals(vertexId("marko"), nodeId);
        assertPrimitiveValue(nodeId);
        assertNoHugeGraphIdLeak(content);
    }

    @Test
    public void testReturnNodeDoesNotLeakInternalIdTypes() {
        String cypher = "MATCH (n:cypher_person) WHERE n.name = 'marko' RETURN n";

        String content = this.testCypherQueryAndContains(cypher, "marko");
        Map<?, ?> node = assertMapValue(assertSingleMapRow(assertCypherSuccessData(content)), "n");
        Assert.assertEquals("cypher_person", node.get("_label"));
        Assert.assertEquals("marko", node.get("name"));
        Assert.assertEquals(20, ((Number) node.get("age")).intValue());
        Assert.assertEquals("Beijing", node.get("city"));
        assertNoHugeGraphIdLeak(content);
    }

    @Test
    public void testReturnNestedIdDoesNotLeakInternalIdTypes() {
        String cypher = "MATCH (n:cypher_person) WHERE n.name = 'marko' " +
                        "RETURN {nodeId: id(n), values: [id(n), n.name]} " +
                        "AS payload";

        String content = this.testCypherQueryAndContains(cypher, "payload");
        List<?> data = assertCypherSuccessData(content);
        Map<?, ?> row = assertSingleMapRow(data);
        Map<?, ?> payload = assertMapValue(row, "payload");
        List<?> values = assertListValue(payload, "values");
        Object nodeId = payload.get("nodeId");

        Assert.assertEquals(vertexId("marko"), nodeId);
        assertPrimitiveValue(nodeId);
        Assert.assertEquals(2, values.size());
        Assert.assertEquals(nodeId, values.get(0));
        Assert.assertEquals("marko", values.get(1));
        assertNoHugeGraphIdLeak(content);
    }

    @Test
    public void testReturnRelationIdDoesNotLeakInternalIdTypes() {
        String cypher = "MATCH (n:cypher_person)-[r:cypher_knows]->(friend:cypher_person) " +
                        "WHERE n.name = 'marko' RETURN id(r) AS relationId";

        String content = this.testCypherQueryAndContains(cypher, "relationId");
        List<?> data = assertCypherSuccessData(content);
        Map<?, ?> row = assertSingleMapRow(data);
        Object relationId = row.get("relationId");

        Assert.assertNotNull(relationId);
        assertPrimitiveValue(relationId);
        assertNoHugeGraphIdLeak(content);
    }

    @Test
    public void testReturnPathShape() {
        String cypher = "MATCH p=(n:cypher_person)-[r:cypher_knows]->(friend:cypher_person) " +
                        "WHERE n.name = 'marko' RETURN p AS path";

        String content = this.testCypherQueryAndContains(cypher, "path");
        List<?> data = assertCypherSuccessData(content);
        Map<?, ?> row = assertSingleMapRow(data);
        List<?> path = assertListValue(row, "path");

        Assert.assertEquals(3, path.size());
        Map<?, ?> source = assertMapValue(path, 0);
        Map<?, ?> relation = assertMapValue(path, 1);
        Map<?, ?> target = assertMapValue(path, 2);

        Assert.assertEquals("node", source.get("_type"));
        Assert.assertEquals("cypher_person", source.get("_label"));
        Assert.assertEquals("marko", source.get("name"));
        Assert.assertEquals("cypher_knows", relation.get("_label"));
        Assert.assertEquals("node", target.get("_type"));
        Assert.assertEquals("cypher_person", target.get("_label"));
        Assert.assertEquals("peter", target.get("name"));
        assertContains("marko", content);
        assertContains("peter", content);
        assertNoHugeGraphIdLeak(content);
    }

    @Test
    public void testExactReadsAndPredicates() {
        assertColumn(query("MATCH (n:cypher_person) RETURN n.name AS name ORDER BY name"),
                     "name", "alice", "dave", "marko", "peter");
        assertColumn(query("MATCH (n:cypher_person) WHERE n.age >= 30 AND " +
                           "(n.active = true OR n.name = 'dave') " +
                           "RETURN n.name AS name ORDER BY name"), "name", "alice", "dave");
        assertColumn(query("MATCH (n:cypher_person)-[:cypher_knows]->(m:cypher_person) " +
                           "RETURN n.name AS name ORDER BY name"), "name", "marko", "peter");
        assertColumn(query("MATCH (n:cypher_person)-[:cypher_knows]->()" +
                           "-[:cypher_knows]->(m:cypher_person) RETURN m.name AS name"),
                     "name", "alice");
        Assert.assertTrue(query("MATCH (n:cypher_person) WHERE n.name = 'absent' RETURN n").isEmpty());
    }

    @Test
    public void testDuplicatesDistinctAndPagination() {
        assertColumn(query("MATCH (n:cypher_person) RETURN n.city AS city ORDER BY city"),
                     "city", "Beijing", "Beijing", "Shanghai", "Shanghai");
        assertColumn(query("MATCH (n:cypher_person) RETURN DISTINCT n.city AS city ORDER BY city"),
                     "city", "Beijing", "Shanghai");
        assertColumn(query("MATCH (n:cypher_person) RETURN n.name AS name " +
                           "ORDER BY n.age, name SKIP 1 LIMIT 2"), "name", "dave", "peter");
    }

    @Test
    public void testAggregates() {
        Map<?, ?> row = assertSingleMapRow(query("MATCH (n:cypher_person) " +
                "RETURN count(n) AS count, sum(n.age) AS sum, min(n.age) AS min, " +
                "max(n.age) AS max, avg(n.age) AS avg"));
        Assert.assertEquals(5, row.size());
        for (String key : Arrays.asList("count", "sum", "min", "max", "avg")) {
            Assert.assertInstanceOf(Number.class, row.get(key));
        }
        Assert.assertEquals(4L, ((Number) row.get("count")).longValue());
        Assert.assertEquals(120L, ((Number) row.get("sum")).longValue());
        Assert.assertEquals(20L, ((Number) row.get("min")).longValue());
        Assert.assertEquals(40L, ((Number) row.get("max")).longValue());
        Assert.assertEquals(30.0D, ((Number) row.get("avg")).doubleValue(), 0.0D);
    }

    @Test
    public void testOrderByNullableBooleanGroupingKey() {
        // #3319: sorting a Boolean-or-null grouping key once crashed with a
        // ClassCastException when the translator null marker met ordered values
        createAndAssert(URL_PREFIX + "/graph/vertices", JsonUtil.toJson(ImmutableMap.of(
                "label", "cypher_person",
                "properties", ImmutableMap.of("name", "noactive", "age", 50,
                                              "city", "Beijing"))));
        String cypher = "MATCH (n:cypher_person) " +
                        "OPTIONAL MATCH (n)-[r0:cypher_knows]->(n1 {city: 'Nowhere'}) " +
                        "WITH n.active AS g, count(n1.name) AS c " +
                        "RETURN g, c ORDER BY g ";
        List<?> rows = query(cypher + "DESC");
        Assert.assertEquals(3, rows.size());
        assertGroupRow(rows.get(0), null);
        assertGroupRow(rows.get(1), true);
        assertGroupRow(rows.get(2), false);

        rows = query(cypher + "ASC");
        Assert.assertEquals(3, rows.size());
        assertGroupRow(rows.get(0), false);
        assertGroupRow(rows.get(1), true);
        assertGroupRow(rows.get(2), null);

        query("MATCH (n:cypher_person {name: 'noactive'}) DETACH DELETE n");
        Assert.assertEquals(4, nativeVertices().size());
    }

    private static void assertGroupRow(Object row, Object groupKey) {
        Map<?, ?> map = assertMapValue(Collections.singletonList(row), 0);
        Assert.assertEquals(2, map.size());
        Assert.assertEquals(groupKey, map.get("g"));
        Assert.assertEquals(0L, ((Number) map.get("c")).longValue());
    }

    @Test
    public void testParameters() {
        assertColumn(bound("MATCH (n:cypher_person) WHERE n.name = $name AND " +
                           "n.age >= $age AND n.active = $active RETURN n.name AS name",
                           ImmutableMap.of("name", "marko", "age", 20, "active", true)),
                     "name", "marko");
        String text = "quote ' and \" newline\n MATCH (n) DELETE n //";
        assertColumn(bound("RETURN $text AS text", ImmutableMap.of("text", text)), "text", text);
        assertColumn(bound("MATCH (n:cypher_person) RETURN n.name AS name ORDER BY name",
                           Collections.emptyMap()), "name", "alice", "dave", "marko", "peter");
        assertColumn(assertCypherSuccessData(assertResponseStatus(200, client().post(PATH,
                     "{\"cypher\":\"RETURN 1 AS value\"}"))), "value", 1);
        assertExecutionError(client().post(PATH, JsonUtil.toJson(ImmutableMap.of(
                "cypher", "RETURN $missing AS value", "parameters", Collections.emptyMap()))));
        assertColumn(bound("RETURN $value AS value", Collections.singletonMap("value", null)),
                     "value", (Object) null);
        Map<?, ?> reservedNames = assertSingleMapRow(bound("RETURN $id AS id, $label AS label",
                ImmutableMap.of("id", "bound-id", "label", "bound-label")));
        Assert.assertEquals(ImmutableMap.of("id", "bound-id", "label", "bound-label"), reservedNames);
        assertColumn(bound("RETURN '$not_a_binding' AS value // $ignored",
                           Collections.emptyMap()), "value", "$not_a_binding");
        assertExecutionError(client().post(PATH, JsonUtil.toJson(ImmutableMap.of(
                "cypher", "RETURN {nested: $missing} AS value",
                "parameters", Collections.emptyMap()))));
        Assert.assertEquals(4, nativeVertices().size());
    }

    @Test
    public void testRejectTranslatorNullMarker() {
        String marker = "  cypher.null";
        for (Object value : Arrays.asList(marker, List.of(marker), Map.of("nested", marker))) {
            assertExecutionError(client().post(PATH, JsonUtil.toJson(ImmutableMap.of(
                    "cypher", "RETURN $value AS value", "parameters", Map.of("value", value)))));
        }
        assertColumn(bound("RETURN $value AS value", Map.of("value", "cypher.null")),
                     "value", "cypher.null");
    }

    @Test
    public void testNullAndMissingProperty() {
        List<?> rows = query("MATCH (n:cypher_person) RETURN n.note AS note");
        Assert.assertEquals(4, rows.size());
        for (Object value : rows) {
            Map<?, ?> row = (Map<?, ?>) value;
            Assert.assertTrue(row.containsKey("note"));
            Assert.assertNull(row.get("note"));
        }
        Assert.assertEquals(4, query("MATCH (n:cypher_person) WHERE n.note IS NULL RETURN n.name").size());
        Assert.assertTrue(query("MATCH (n:cypher_person) WHERE n.note IS NOT NULL RETURN n.name").isEmpty());
    }

    @Test
    public void testCreateSetAndDeleteWithNativeReadback() {
        assertColumn(query("CREATE (n:cypher_person {name:'created', age:50, city:'Paris'}) " +
                           "RETURN n.name AS name"), "name", "created");
        assertNativeVertex("created", 50, "Paris");
        assertColumn(query("MATCH (n:cypher_person) WHERE n.name = 'created' " +
                           "SET n.age = 51, n.city = 'London' RETURN n.name AS name"), "name", "created");
        assertNativeVertex("created", 51, "London");
        query("MATCH (a:cypher_person), (b:cypher_person) " +
              "WHERE a.name = 'created' AND b.name = 'marko' CREATE (a)-[:cypher_knows]->(b)");
        List<?> edges = nativeEdges();
        Assert.assertEquals(3, edges.size());
        boolean found = false;
        for (Object value : edges) {
            Map<?, ?> edge = (Map<?, ?>) value;
            if (vertexId("created").equals(edge.get("outV"))) {
                Assert.assertEquals(vertexId("marko"), edge.get("inV"));
                Assert.assertEquals("cypher_knows", edge.get("label"));
                found = true;
            }
        }
        Assert.assertTrue(found);
        query("MATCH (a:cypher_person)-[r:cypher_knows]->() WHERE a.name = 'created' DELETE r");
        Assert.assertEquals(2, nativeEdges().size());
        query("MATCH (n:cypher_person) WHERE n.name = 'created' DELETE n");
        Assert.assertEquals(4, nativeVertices().size());
        Assert.assertNull(nativeVertex("created"));
    }

    @Test
    public void testFailedWriteDoesNotLeakIntoLaterRequests() {
        assertExecutionError(client().post(PATH,
                "CREATE (good:cypher_person {name:'rollback', age:33, city:'Beijing'}), " +
                "(bad:cypher_person {name:'invalid', age:34, city:'Shanghai', rogue:'x'}) " +
                "RETURN good.name"));
        Assert.assertNull(nativeVertex("rollback"));
        // Failed-write residue became visible after later reads in the regression reproducer.
        // Immediate REST readback alone therefore does not establish rollback.
        for (int i = 0; i < 32; i++) {
            Map<?, ?> row = assertSingleMapRow(query("MATCH (n:cypher_person) RETURN count(n) AS count"));
            Assert.assertEquals(4L, ((Number) row.get("count")).longValue());
            Assert.assertEquals(4, nativeVertices().size());
            Assert.assertNull(nativeVertex("rollback"));
            Assert.assertNull(nativeVertex("invalid"));
        }
    }

    @Test
    public void testInvalidRequests() {
        assertExecutionError(client().post(PATH, "MATCH ( RETURN broken"));
        assertExecutionError(client().post(PATH,
                "CREATE (:cypher_person {name:'wrongtype', age:'not an integer', city:'Paris'})"));
        Assert.assertNull(nativeVertex("wrongtype"));
        assertResponseStatus(400, client().get(PATH, ImmutableMap.of("cypher", "")));
        for (String body : Arrays.asList(" \n", "{", "{}", "{\"cypher\":\" \"}",
                                        "{\"cypher\":1}", "[]", "[1, 2]", "\"RETURN 1\"",
                                        " null ", "1", "-1.5", "true", "false",
                                        "{\"cypher\":\"RETURN 1\"} garbage",
                                        "{\"cypher\":\"RETURN 1\"} {}",
                                        "{\"cypher\":\"RETURN 1\",\"parameters\":false}",
                                        "{\"cypher\":\"RETURN 1\",\"parameters\":null}",
                                        "{\"cypher\":\"RETURN 1\",\"parameters\":[]}",
                                        "{\"cypher\":\"RETURN 1\",\"parameters\":3}")) {
            assertResponseStatus(400, client().post(PATH, body));
        }
    }

    @Test
    public void testAuthenticationAndGraphRouting() {
        RestClient invalid = new RestClient(BASE_URL, "admin", "invalid-cypher-password");
        try {
            assertResponseStatus(401, invalid.post(PATH, "RETURN 1"));
        } finally {
            invalid.close();
        }
        Response response = client().post("graphspaces/DEFAULT/graphs/cypher_missing_graph/cypher",
                                          "MATCH (n) RETURN n");
        assertExecutionError(response);
        assertColumn(query("MATCH (n:cypher_person) RETURN n.name AS name ORDER BY name"),
                     "name", "alice", "dave", "marko", "peter");
    }

    @Test
    public void testRejectMissingAuthorization() {
        RestClient anonymous = new RestClient(baseUrl(), false);
        try {
            assertResponseStatus(401, anonymous.get(PATH, ImmutableMap.of("cypher", "RETURN 1")));
            assertResponseStatus(401, anonymous.post(PATH, "RETURN 1"));
        } finally {
            anonymous.close();
        }
    }

    @Test
    public void testReadOnlyUserCannotWriteOrReadAnotherGraph() {
        assumeStandaloneMode();
        String graph = "cypher_reader_other";
        String graphPath = "graphspaces/DEFAULT/graphs/" + graph;
        Deque<String[]> created = new ArrayDeque<>();
        RestClient reader = null;
        boolean graphCreated = false;
        try {
            String config = JsonUtil.toJson(ImmutableMap.of(
                    "gremlin.graph", "org.apache.hugegraph.auth.HugeFactoryAuthProxy",
                    "backend", "memory", "serializer", "text", "store", graph));
            Response response = client().post(graphPath, Entity.json(config));
            graphCreated = response.getStatus() == 201;
            assertResponseStatus(201, response);
            reader = new RestClient(baseUrl(), "cypher_reader", "cypher-reader-password");
            String group = this.createAuth(created, "auth/groups",
                                           "{\"group_name\":\"cypher_read_only_group\"}");
            String target = this.createAuth(created, "graphspaces/DEFAULT/auth/targets",
                                            "{\"target_name\":\"cypher_read_only_target\"," +
                                            "\"target_graph\":\"hugegraph\"," +
                                            "\"target_resources\":[{\"type\":\"ALL\"}]}");
            for (String permission : ImmutableList.of("READ", "EXECUTE")) {
                this.createAuth(created, "graphspaces/DEFAULT/auth/accesses", String.format(
                        "{\"group\":\"%s\",\"target\":\"%s\",\"access_permission\":\"%s\"}",
                        group, target, permission));
            }
            String user = this.createAuth(created, "graphspaces/DEFAULT/auth/users",
                                          "{\"user_name\":\"cypher_reader\"," +
                                          "\"user_password\":\"cypher-reader-password\"}");
            this.createAuth(created, "graphspaces/DEFAULT/auth/belongs", String.format(
                    "{\"user\":\"%s\",\"group\":\"%s\"}", user, group));

            assertColumn(assertCypherSuccessData(assertResponseStatus(200, reader.post(PATH, QUERY))),
                         "name", "marko", "peter");
            assertPermissionDenied(reader.post(PATH,
                    "CREATE (n:cypher_person {name:'cypher_denied',age:1,city:'Beijing'}) RETURN n"),
                    "write", "hugegraph");
            Assert.assertNull(this.nativeVertex("cypher_denied"));
            // This existing graph is accessible to the administrator but not to the reader.
            assertCypherSuccessData(assertResponseStatus(200, client().post(
                    graphPath + "/cypher", "MATCH (n) RETURN n")));
            assertPermissionDenied(reader.post(graphPath + "/cypher", "MATCH (n) RETURN n"),
                                   "execute", graph);
        } finally {
            if (reader != null) {
                reader.close();
            }
            try {
                this.deleteAuth(created);
            } finally {
                if (graphCreated) {
                    assertResponseStatus(204, client().delete(graphPath, ImmutableMap.of(
                            "confirm_message", "I'm sure to drop the graph")));
                }
            }
        }
    }

    @Test
    public void testBasicAuthWithColonPassword() {
        // The group, target and access setup below uses the standalone auth API; in PD mode
        // groups are graphspace-scoped. The header parsing under test is backend-independent.
        assumeStandaloneMode();
        // CypherAPI parses the Basic header itself, apart from AuthenticationFilter
        String user = "cypher_colon_user";
        // With this user name these encode to standard Base64 containing '+' and '/'
        Assert.assertTrue(basicCredential(user, "a:b:c~").contains("+"));
        Assert.assertTrue(basicCredential(user, "a:b:c?").contains("/"));
        Deque<String[]> created = new ArrayDeque<>();
        RestClient noAuthClient = new RestClient(baseUrl(), false);
        try {
            String groupId = this.createAuth(created, "auth/groups",
                                             "{\"group_name\":\"cypher_colon_group\"}");
            String targetId = this.createAuth(created, "graphspaces/DEFAULT/auth/targets",
                                              "{\"target_name\":\"cypher_colon_target\"," +
                                              "\"target_graph\":\"hugegraph\"," +
                                              "\"target_url\":\"127.0.0.1:8080\"," +
                                              "\"target_resources\":[{\"type\":\"ALL\"}]}");
            for (String permission : ImmutableList.of("READ", "EXECUTE")) {
                this.createAuth(created, "graphspaces/DEFAULT/auth/accesses", String.format(
                        "{\"group\":\"%s\",\"target\":\"%s\",\"access_permission\":\"%s\"}",
                        groupId, targetId, permission));
            }

            for (String password : ImmutableList.of("a:b:c", "a:b:c~", "a:b:c?")) {
                Deque<String[]> member = new ArrayDeque<>();
                try {
                    String userId = this.createAuth(member, "graphspaces/DEFAULT/auth/users",
                                                    String.format("{\"user_name\":\"%s\"," +
                                                                  "\"user_password\":\"%s\"}",
                                                                  user, password));
                    this.createAuth(member, "graphspaces/DEFAULT/auth/belongs", String.format(
                            "{\"user\":\"%s\",\"group\":\"%s\"}", userId, groupId));

                    Response r = basicAuthCypher(noAuthClient, user, password);
                    assertColumn(assertCypherSuccessData(assertResponseStatus(200, r)),
                                 "name", "marko", "peter");

                    r = basicAuthCypher(noAuthClient, user, "wrong" + password);
                    assertResponseStatus(401, r);
                } finally {
                    this.deleteAuth(member);
                }
            }
        } finally {
            noAuthClient.close();
            this.deleteAuth(created);
        }
    }

    @Test
    public void testSpecifiedGraphRouting() {
        assumeStandaloneMode();
        String graph = "cypher_route_test";
        String path = "graphspaces/DEFAULT/graphs/" + graph;
        assertResponseStatus(201, createGraphInRocksDB("DEFAULT", graph));
        try {
            List<?> rows = assertCypherSuccessData(assertResponseStatus(200,
                    client().post(path + "/cypher", "MATCH (n) RETURN count(n) AS count")));
            Map<?, ?> emptyGraph = assertSingleMapRow(rows);
            Assert.assertEquals(0L, ((Number) emptyGraph.get("count")).longValue());
            Map<?, ?> defaultGraph = assertSingleMapRow(query("MATCH (n) RETURN count(n) AS count"));
            Assert.assertEquals(4L, ((Number) defaultGraph.get("count")).longValue());
        } finally {
            assertResponseStatus(204, client().delete(path, ImmutableMap.of(
                    "confirm_message", "I'm sure to drop the graph")));
        }
    }

    private String createAuth(Deque<String[]> created, String path, String body) {
        String content = assertResponseStatus(201, client().post(path, body));
        String id = assertJsonContains(content, "id");
        created.push(new String[]{path, id});
        return id;
    }

    private void deleteAuth(Deque<String[]> created) {
        // Attempt every deletion first, then fail on any record that was left behind
        List<String> failures = new ArrayList<>();
        while (!created.isEmpty()) {
            String[] entry = created.pop();
            Response r = client().delete(entry[0], entry[1]);
            if (r.getStatus() != 204) {
                failures.add(String.format("%s/%s: %s %s", entry[0], entry[1],
                                           r.getStatus(), r.readEntity(String.class)));
            }
        }
        Assert.assertTrue("Failed to delete auth records: " + failures, failures.isEmpty());
    }

    private static String basicCredential(String user, String password) {
        byte[] credential = (user + ":" + password).getBytes(StandardCharsets.UTF_8);
        return Base64.getEncoder().encodeToString(credential);
    }

    private static Response basicAuthCypher(RestClient client, String user, String password) {
        // Built by hand: Jersey's HttpAuthenticationFeature encodes the credential as ISO-8859-1
        MultivaluedMap<String, Object> headers = new MultivaluedHashMap<>();
        headers.add(HttpHeaders.AUTHORIZATION, "Basic " + basicCredential(user, password));
        return client.target().path(PATH).queryParam("cypher", QUERY)
                     .request().headers(headers).get();
    }

    private List<?> query(String cypher) {
        return assertCypherSuccessData(assertResponseStatus(200, client().post(PATH, cypher)));
    }

    private List<?> bound(String cypher, Map<String, Object> parameters) {
        return query(JsonUtil.toJson(ImmutableMap.of("cypher", cypher, "parameters", parameters)));
    }

    private static void assertColumn(List<?> rows, String key, Object... expected) {
        List<Object> actual = new ArrayList<>();
        for (Object value : rows) {
            Assert.assertInstanceOf(Map.class, value);
            Map<?, ?> row = (Map<?, ?>) value;
            Assert.assertEquals(1, row.size());
            Assert.assertTrue(row.containsKey(key));
            actual.add(row.get(key));
        }
        Assert.assertEquals(Arrays.asList(expected), actual);
    }

    private static void assertExecutionError(Response response) {
        String content = assertResponseStatus(200, response);
        Map<?, ?> body = JsonUtil.fromJson(content, Map.class);
        Map<?, ?> status = assertMapValue(body, "status");
        Assert.assertTrue(content, ((Number) status.get("code")).intValue() >= 400);
        Assert.assertFalse(String.valueOf(status.get("message")).isEmpty());
        Assert.assertNull(assertMapValue(body, "result").get("data"));
    }

    private static void assertPermissionDenied(Response response, String operation, String graph) {
        String content = assertResponseStatus(200, response);
        Map<?, ?> body = JsonUtil.fromJson(content, Map.class);
        Map<?, ?> status = assertMapValue(body, "status");
        Assert.assertEquals(400, ((Number) status.get("code")).intValue());
        String message = String.valueOf(status.get("message"));
        assertContains("Permission denied: " + operation, message);
        assertContains("graph=" + graph + ",", message);
        Assert.assertNull(assertMapValue(body, "result").get("data"));
    }

    private void addVertex(String name, int age, String city, boolean active) {
        createAndAssert(URL_PREFIX + "/graph/vertices", JsonUtil.toJson(ImmutableMap.of(
                "label", "cypher_person", "properties", ImmutableMap.of(
                        "name", name, "age", age, "city", city, "active", active))));
    }

    private void addVertex(String name, int age, String city, boolean active, String birthday) {
        createAndAssert(URL_PREFIX + "/graph/vertices", JsonUtil.toJson(ImmutableMap.of(
                "label", "cypher_person", "properties", ImmutableMap.of(
                        "name", name, "age", age, "city", city, "active", active,
                        "birthday", birthday))));
    }

    private void addEdge(String source, String target) {
        createAndAssert(URL_PREFIX + "/graph/edges", JsonUtil.toJson(
                ImmutableMap.<String, Object>builder()
                            .put("label", "cypher_knows")
                            .put("outV", vertexId(source))
                            .put("inV", vertexId(target))
                            .put("outVLabel", "cypher_person")
                            .put("inVLabel", "cypher_person")
                            .put("properties", Collections.emptyMap())
                            .build()));
    }

    private List<?> nativeVertices() {
        return nativeElements("vertices");
    }

    private List<?> nativeEdges() {
        return nativeElements("edges");
    }

    private List<?> nativeElements(String type) {
        String content = assertResponseStatus(200, client().get(URL_PREFIX + "/graph/" + type,
                                                              ImmutableMap.of("limit", -1)));
        return assertListValue(JsonUtil.fromJson(content, Map.class), type);
    }

    private Map<?, ?> nativeVertex(String name) {
        for (Object value : nativeVertices()) {
            Map<?, ?> vertex = (Map<?, ?>) value;
            if (name.equals(assertMapValue(vertex, "properties").get("name"))) {
                return vertex;
            }
        }
        return null;
    }

    private Object vertexId(String name) {
        Map<?, ?> vertex = nativeVertex(name);
        Assert.assertNotNull(vertex);
        return vertex.get("id");
    }

    private void assertNativeVertex(String name, int age, String city) {
        Map<?, ?> vertex = nativeVertex(name);
        Assert.assertNotNull(vertex);
        Assert.assertEquals("cypher_person", vertex.get("label"));
        Map<?, ?> properties = assertMapValue(vertex, "properties");
        Assert.assertEquals(name, properties.get("name"));
        Assert.assertEquals(age, ((Number) properties.get("age")).intValue());
        Assert.assertEquals(city, properties.get("city"));
    }

    @Test
    public void testSyntaxErrorHasStructuredErrorWithHint() throws Exception {
        this.assertError("MATCH (n:cypher_person) RETURN not_defined_var",
                         "HugeGraph.Cypher.SyntaxError",
                         "Declare the variable in a MATCH, UNWIND or WITH clause " +
                         "before referencing it");
    }

    @Test
    public void testParserErrorHasStructuredErrorWithHint() throws Exception {
        this.assertError("MATCH (n:cypher_person RETURN n", "HugeGraph.Cypher.SyntaxError",
                         "Check the query syntax and function names supported by " +
                         "the built-in Cypher translator");
    }

    @Test
    public void testMultipleStatementsHaveStructuredError() throws Exception {
        this.assertError("MATCH (n) RETURN n; MATCH (m) RETURN m",
                         "HugeGraph.Cypher.SyntaxError",
                         "Submit exactly one Cypher statement per request");
    }

    @Test
    public void testUndefinedLabelHasSchemaHint() throws Exception {
        this.assertError("MATCH (n:robot) RETURN n", "HugeGraph.Cypher.ExecutionError",
                         "Create the schema element via the schema API before running the query");
    }

    @Test
    public void testMissingParameterHasFallbackError() throws Exception {
        this.assertError("RETURN $missing AS value",
                         "HugeGraph.Cypher.ExecutionError", "");
    }

    private void assertError(String query, String code, String hint) throws Exception {
        Response r = client().post(PATH, query);
        JsonNode response = new ObjectMapper().readTree(assertResponseStatus(200, r));
        Assert.assertEquals(3, response.size());
        Assert.assertFalse(response.has("errors"));
        Assert.assertEquals(400, response.at("/status/code").asInt());
        Assert.assertFalse(response.at("/status/message").asText().isEmpty());
        Assert.assertEquals(1, response.at("/status/attributes/errors").size());
        JsonNode error = response.at("/status/attributes/errors/0");
        Assert.assertEquals(code, error.get("code").asText());
        Assert.assertEquals(hint, error.get("hint").asText());
        Assert.assertFalse(error.get("message").asText().isEmpty());
    }

    private String testCypherQueryAndContains(String cypher,
                                             String containsText) {
        Response r = client().post(PATH, cypher);
        return this.validStatusAndTextContains(containsText, r);
    }

    private String validStatusAndTextContains(String value, Response r) {
        String content = assertResponseStatus(200, r);
        assertContains(value, content);
        assertCypherSuccessData(content);
        return content;
    }

    private static void assertNoHugeGraphIdLeak(String content) {
        Assert.assertFalse(content.contains("org.apache.hugegraph.backend.id"));
        Assert.assertFalse(content.contains("StringId"));
        Assert.assertFalse(content.contains("LongId"));
        Assert.assertFalse(content.contains("UuidId"));
        Assert.assertFalse(content.contains("EdgeId"));
    }

    @SuppressWarnings("unchecked")
    private static List<?> assertCypherSuccessData(String content) {
        Map<?, ?> response = JsonUtil.fromJson(content, Map.class);
        Assert.assertTrue(response.containsKey("requestId"));

        Map<?, ?> status = assertMapValue(response, "status");
        Assert.assertEquals(content, 200, ((Number) status.get("code")).intValue());
        Assert.assertEquals("", status.get("message"));

        Map<?, ?> result = assertMapValue(response, "result");
        Assert.assertInstanceOf(List.class, result.get("data"));
        Assert.assertInstanceOf(Map.class, result.get("meta"));
        return (List<?>) result.get("data");
    }

    private static Map<?, ?> assertSingleMapRow(List<?> data) {
        Assert.assertEquals(1, data.size());
        Assert.assertInstanceOf(Map.class, data.get(0));
        return (Map<?, ?>) data.get(0);
    }

    private static Map<?, ?> assertMapValue(Map<?, ?> map, String key) {
        Assert.assertTrue(map.containsKey(key));
        Assert.assertInstanceOf(Map.class, map.get(key));
        return (Map<?, ?>) map.get(key);
    }

    private static Map<?, ?> assertMapValue(List<?> list, int index) {
        Assert.assertTrue(list.size() > index);
        Assert.assertInstanceOf(Map.class, list.get(index));
        return (Map<?, ?>) list.get(index);
    }

    private static List<?> assertListValue(Map<?, ?> map, String key) {
        Assert.assertTrue(map.containsKey(key));
        Assert.assertInstanceOf(List.class, map.get(key));
        return (List<?>) map.get(key);
    }

    private static void assertPrimitiveValue(Object value) {
        Assert.assertFalse(value instanceof Map);
        Assert.assertFalse(value instanceof List);
    }
}
