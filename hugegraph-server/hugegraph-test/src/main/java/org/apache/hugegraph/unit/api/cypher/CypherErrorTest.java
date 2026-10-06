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
import org.apache.tinkerpop.gremlin.driver.exception.ResponseException;
import org.apache.tinkerpop.gremlin.driver.message.ResponseStatusCode;
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
    public void testRealParserFailures() {
        String[] queries = {
                "MATCH (n:person RETURN n",
                "MATCH (n:person) RETRUN n",
                "MATCH (n) RETURN n; MATCH (m) RETURN m",
                "RETURN date()",
                "MATCH (n:person) RETURN not_defined_var"
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
        Assert.assertEquals("HugeGraph.Cypher.UnsupportedFeature", CypherErrorMapper.map(
                new UnsupportedOperationException("Feature not supported")).code);
        Assert.assertEquals("HugeGraph.Cypher.UnsupportedFeature", CypherErrorMapper.map(
                new UnsupportedOperationException("Unsupported construct")).code);
        CypherModel.CypherError fallback = CypherErrorMapper.map(new RuntimeException());
        Assert.assertEquals("HugeGraph.Cypher.ExecutionError", fallback.code);
        Assert.assertEquals("", fallback.hint);
        Assert.assertFalse(fallback.message.isEmpty());
    }

    @Test
    public void testParametersRemainUnbound() {
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

    private static String translate(String query) {
        return CypherAst.parse(query, Collections.emptyMap()).buildTranslation(
                Translator.builder().gremlinGroovy()
                          .build("gremlin+cfog_server_extensions+inline_parameters"));
    }
}
