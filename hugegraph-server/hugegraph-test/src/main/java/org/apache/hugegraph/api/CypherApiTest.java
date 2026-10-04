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

import java.util.List;
import java.util.Map;

import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.util.JsonUtil;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

import jakarta.ws.rs.core.Response;

public class CypherApiTest extends BaseApiTest {

    private static final String PATH = URL_PREFIX + "/cypher";
    private static final String QUERY = "MATCH (n:person) where n.city ='Beijing' return n";
    private static final String QUERY_RESULT = "Beijing";

    @Before
    public void prepareSchema() {
        BaseApiTest.initPropertyKey();
        BaseApiTest.initVertexLabel();
        BaseApiTest.initEdgeLabel();
        BaseApiTest.initIndexLabel();
        BaseApiTest.initVertex();
        BaseApiTest.initEdge();
    }

    @Test
    public void testGet() {
        Map<String, Object> params = ImmutableMap.of("cypher", QUERY);
        Response r = client().get(PATH, params);

        this.validStatusAndTextContains(QUERY_RESULT, r);
    }

    @Test
    public void testPost() {
        this.testCypherQueryAndContains(QUERY, QUERY_RESULT);
    }

    @Test
    public void testCreate() {
        this.testCypherQueryAndContains("CREATE (n:person { name : 'test', " +
                                        "age: 20, city: 'Hefei' }) return n",
                                        "Hefei");
    }

    @Test
    public void testRelationQuery() {
        String cypher = "MATCH (n:person)-[r:knows]->(friend:person)\n" +
                        "WHERE n.name = 'marko'\n" +
                        "RETURN n, friend.name AS friend";
        this.testCypherQueryAndContains(cypher, "friend");
    }

    @Test
    public void testReturnNodeIdAsPrimitiveValue() {
        String cypher = "MATCH (n:person) WHERE n.name = 'marko' " +
                        "RETURN id(n) AS nodeId";

        String content = this.testCypherQueryAndContains(cypher, "nodeId");
        List<?> data = assertCypherSuccessData(content);
        Map<?, ?> row = assertSingleMapRow(data);
        Object nodeId = row.get("nodeId");

        Assert.assertNotNull(nodeId);
        assertPrimitiveValue(nodeId);
        assertNoHugeGraphIdLeak(content);
    }

    @Test
    public void testReturnNodeDoesNotLeakInternalIdTypes() {
        String cypher = "MATCH (n:person) WHERE n.name = 'marko' RETURN n";

        String content = this.testCypherQueryAndContains(cypher, "marko");
        assertNoHugeGraphIdLeak(content);
    }

    @Test
    public void testReturnNestedIdDoesNotLeakInternalIdTypes() {
        String cypher = "MATCH (n:person) WHERE n.name = 'marko' " +
                        "RETURN {nodeId: id(n), values: [id(n), n.name]} " +
                        "AS payload";

        String content = this.testCypherQueryAndContains(cypher, "payload");
        List<?> data = assertCypherSuccessData(content);
        Map<?, ?> row = assertSingleMapRow(data);
        Map<?, ?> payload = assertMapValue(row, "payload");
        List<?> values = assertListValue(payload, "values");
        Object nodeId = payload.get("nodeId");

        Assert.assertNotNull(nodeId);
        assertPrimitiveValue(nodeId);
        Assert.assertEquals(2, values.size());
        Assert.assertEquals(nodeId, values.get(0));
        Assert.assertEquals("marko", values.get(1));
        assertNoHugeGraphIdLeak(content);
    }

    @Test
    public void testReturnRelationIdDoesNotLeakInternalIdTypes() {
        String cypher = "MATCH (n:person)-[r:knows]->(friend:person) " +
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
        String cypher = "MATCH p=(n:person)-[r:knows]->(friend:person) " +
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
        Assert.assertEquals("person", source.get("_label"));
        Assert.assertEquals("marko", source.get("name"));
        Assert.assertEquals("knows", relation.get("_label"));
        Assert.assertEquals("node", target.get("_type"));
        Assert.assertEquals("person", target.get("_label"));
        Assert.assertEquals("peter", target.get("name"));
        assertContains("marko", content);
        assertContains("peter", content);
        assertNoHugeGraphIdLeak(content);
    }

    private String testCypherQueryAndContains(String cypher,
                                             String containsText) {
        Response r = client().post(PATH, cypher);
        return this.validStatusAndTextContains(containsText, r);
    }

    private String validStatusAndTextContains(String value, Response r) {
        String content = assertResponseStatus(200, r);
        assertContains(value, content);
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
        Assert.assertEquals(200, ((Number) status.get("code")).intValue());
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
