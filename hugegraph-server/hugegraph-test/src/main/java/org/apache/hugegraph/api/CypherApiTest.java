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
    public void testSyntaxErrorHasStructuredErrorWithHint() {
        Response r = client().post(PATH,
                                   "MATCH (n:person) RETURN not_defined_var");
        String content = assertResponseStatus(200, r);
        assertContains("\"errors\"", content);
        assertContains("SyntaxError", content);
        assertContains("Declare the variable", content);
    }

    @Test
    public void testUndefinedLabelHasSchemaHint() {
        Response r = client().post(PATH, "MATCH (n:robot) RETURN n");
        String content = assertResponseStatus(200, r);
        assertContains("ExecutionError", content);
        assertContains("Create the schema element", content);
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
