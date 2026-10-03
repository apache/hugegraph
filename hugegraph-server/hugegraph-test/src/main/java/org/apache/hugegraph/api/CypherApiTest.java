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

import java.util.Map;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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
    public void testSyntaxErrorHasStructuredErrorWithHint() throws Exception {
        this.assertError("MATCH (n:person) RETURN not_defined_var",
                         "HugeGraph.Cypher.SyntaxError",
                         "Declare the variable in a MATCH, UNWIND or WITH clause " +
                         "before referencing it");
    }

    @Test
    public void testParserErrorHasStructuredErrorWithHint() throws Exception {
        this.assertError("MATCH (n:person RETURN n", "HugeGraph.Cypher.SyntaxError",
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

    private void testCypherQueryAndContains(String cypher, String containsText) {
        Response r = client().post(PATH, cypher);
        this.validStatusAndTextContains(containsText, r);
    }

    private void validStatusAndTextContains(String value, Response r) {
        String content = assertResponseStatus(200, r);
        assertContains(value, content);
    }
}
