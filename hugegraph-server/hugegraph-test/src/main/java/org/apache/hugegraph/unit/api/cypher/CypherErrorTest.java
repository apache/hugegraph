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

package org.apache.hugegraph.unit.api.cypher;

import java.util.Collections;
import java.util.Locale;
import java.util.concurrent.ExecutionException;

import org.apache.hugegraph.api.cypher.CypherErrorMapper;
import org.apache.hugegraph.api.cypher.CypherModel;
import org.apache.hugegraph.traversal.optimize.TraversalUtil;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.driver.exception.ResponseException;
import org.apache.tinkerpop.gremlin.util.message.ResponseStatusCode;
import org.junit.Assert;
import org.junit.Test;
import org.opencypher.gremlin.translation.CypherAst;
import org.opencypher.gremlin.translation.translator.Translator;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class CypherErrorTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    @Test
    public void testSuccessEnvelope() throws Exception {
        JsonNode response = JSON.readTree(JSON.writeValueAsString(
                CypherModel.dataOf("request", Collections.singletonList("ok"))));
        Assert.assertEquals(3, response.size());
        Assert.assertFalse(response.has("errors"));
        Assert.assertEquals("request", response.get("requestId").asText());
        Assert.assertEquals(200, response.at("/status/code").asInt());
        Assert.assertEquals(0, response.at("/status/attributes").size());
        Assert.assertEquals("ok", response.at("/result/data/0").asText());
    }

    @Test
    public void testFailureEnvelope() throws Exception {
        CypherModel.CypherError error = CypherErrorMapper.map(
                new IllegalArgumentException("Invalid input 'R'"));
        JsonNode response = JSON.readTree(JSON.writeValueAsString(
                CypherModel.failOf("request", "original message", error)));
        Assert.assertEquals(3, response.size());
        Assert.assertFalse(response.has("errors"));
        Assert.assertEquals(400, response.at("/status/code").asInt());
        Assert.assertEquals("original message", response.at("/status/message").asText());
        Assert.assertTrue(response.at("/result/data").isNull());
        JsonNode metadata = response.at("/status/attributes/errors/0");
        Assert.assertEquals("HugeGraph.Cypher.SyntaxError", metadata.get("code").asText());
        Assert.assertEquals(error.message, metadata.get("message").asText());
        Assert.assertEquals(error.hint, metadata.get("hint").asText());
        Assert.assertFalse(error.hint.isEmpty());
    }

    @Test
    public void testFailureWithoutMetadata() throws Exception {
        JsonNode response = JSON.readTree(JSON.writeValueAsString(
                CypherModel.failOf("request", "failure", null)));
        Assert.assertEquals(3, response.size());
        Assert.assertEquals(0, response.at("/status/attributes").size());
        Assert.assertEquals(400, response.at("/status/code").asInt());
    }

    @Test
    public void testLegacyFailureFactory() throws Exception {
        JsonNode response = JSON.readTree(JSON.writeValueAsString(
                CypherModel.failOf("legacy-request", "legacy failure")));
        Assert.assertEquals(3, response.size());
        Assert.assertFalse(response.has("errors"));
        Assert.assertEquals("legacy-request", response.get("requestId").asText());
        Assert.assertEquals(400, response.at("/status/code").asInt());
        Assert.assertEquals("legacy failure", response.at("/status/message").asText());
        Assert.assertEquals(0, response.at("/status/attributes").size());
        Assert.assertTrue(response.at("/result/data").isNull());
        Assert.assertEquals(0, response.at("/result/meta").size());
    }

    @Test
    public void testRealParserFailures() {
        String[] queries = {
                "MATCH (n:person RETURN n",
                "MATCH (n:person) RETRUN n",
                "MATCH (n) RETURN n; MATCH (m) RETURN m",
                "RETURN duration('P1Y')",
                "MATCH (n:person) RETURN not_defined_var",
                "MATCH (n) RETURN n.age + 'a' * true",
                "CREATE (n) CREATE (n) RETURN n",
                "MATCH (n) WITH n.name RETURN n",
                "MATCH (n) RETURN 1 AS a, 2 AS a"
        };
        for (String query : queries) {
            Exception failure = Assert.assertThrows(Exception.class, () -> translate(query));
            // Gremlin's response exception loses the parser's exception type.
            CypherModel.CypherError error = CypherErrorMapper.map(
                    new ExecutionException(new ResponseException(
                            ResponseStatusCode.SERVER_ERROR, failure.getMessage())));
            Assert.assertEquals(query, "HugeGraph.Cypher.SyntaxError", error.code);
            Assert.assertEquals(query, failure.getMessage(), error.message);
            Assert.assertFalse(query, error.hint.isEmpty());
            Assert.assertEquals(query, query.contains("not_defined_var"),
                                error.hint.startsWith("Declare the variable"));
        }
    }

    @Test
    public void testSchemaFailuresAreNotVariableErrors() {
        for (String type : new String[]{"vertex label", "edge label", "property key",
                                       "index label"}) {
            CypherModel.CypherError error = CypherErrorMapper.map(
                    new IllegalArgumentException("Undefined " + type + ": 'x'"));
            Assert.assertEquals("HugeGraph.Cypher.ExecutionError", error.code);
            Assert.assertTrue(error.hint.startsWith("Create the schema element"));
        }
        Assert.assertEquals("HugeGraph.Cypher.ExecutionError", CypherErrorMapper.map(
                new IllegalArgumentException("Resource not defined")).code);
    }

    @Test
    public void testUnsupportedAndFallback() {
        for (String message : new String[]{"Feature not supported", "Unsupported construct"}) {
            CypherModel.CypherError error = CypherErrorMapper.map(new UnsupportedOperationException(message));
            Assert.assertEquals("HugeGraph.Cypher.ExecutionError", error.code);
            Assert.assertEquals(message, error.message);
            Assert.assertEquals("", error.hint);
        }
        CypherModel.CypherError fallback = CypherErrorMapper.map(new RuntimeException());
        Assert.assertEquals("HugeGraph.Cypher.ExecutionError", fallback.code);
        Assert.assertEquals("", fallback.hint);
        Assert.assertFalse(fallback.message.isEmpty());
    }

    @Test
    public void testTranslatorWithoutRequestValidationLeavesParametersUnbound() {
        String gremlin = translate("MATCH (n:person) WHERE n.name = $name RETURN n");
        Assert.assertTrue(gremlin, gremlin.contains("cypher.null"));
    }

    @Test
    public void testClassificationIsLocaleIndependent() {
        Locale previous = Locale.getDefault();
        try {
            Locale.setDefault(Locale.forLanguageTag("tr-TR"));
            Assert.assertEquals("HugeGraph.Cypher.SyntaxError", CypherErrorMapper.map(
                    new IllegalArgumentException("INVALID INPUT 'R'")).code);
        } finally {
            Locale.setDefault(previous);
        }
    }

    @Test
    public void testRealUnsupportedMatchPattern() {
        Exception failure = Assert.assertThrows(Exception.class, () -> translate(
                "MATCH p=shortestPath((a)-[*]->(b)) RETURN p"));
        Assert.assertTrue(failure.getMessage(), failure.getMessage().startsWith("Unsupported match pattern:"));
        CypherModel.CypherError error = transportError(failure);
        Assert.assertEquals("HugeGraph.Cypher.UnsupportedFeature", error.code);
        Assert.assertEquals(failure.getMessage(), error.message);
        Assert.assertTrue(error.hint.startsWith("This construct is not covered"));
    }

    @Test
    public void testRuntimeUnsupportedErrorsAreNotTranslatorGaps() {
        Exception contains = Assert.assertThrows(IllegalArgumentException.class,
                () -> TraversalUtil.convContains2Relation(null, new HasContainer("key", P.neq("value"))));
        for (Exception failure : new Exception[]{TraversalUtil.newUnsupportedPredicate(P.eq("value")), contains}) {
            CypherModel.CypherError error = transportError(failure);
            Assert.assertEquals("HugeGraph.Cypher.ExecutionError", error.code);
            Assert.assertEquals(failure.getMessage(), error.message);
            Assert.assertEquals("", error.hint);
        }
    }

    @Test
    public void testMissingParameterRemainsExecutionFallback() {
        CypherModel.CypherError error = transportError(
                new IllegalArgumentException("Missing Cypher parameter: missing"));
        Assert.assertEquals("HugeGraph.Cypher.ExecutionError", error.code);
        Assert.assertEquals("Missing Cypher parameter: missing", error.message);
        Assert.assertEquals("", error.hint);
    }

    private static CypherModel.CypherError transportError(Exception failure) {
        return CypherErrorMapper.map(new ExecutionException(
                new ResponseException(ResponseStatusCode.SERVER_ERROR, failure.getMessage())));
    }

    private static String translate(String query) {
        return CypherAst.parse(query, Collections.emptyMap()).buildTranslation(
                Translator.builder().gremlinGroovy()
                          .build("gremlin+cfog_server_extensions+inline_parameters"));
    }
}
