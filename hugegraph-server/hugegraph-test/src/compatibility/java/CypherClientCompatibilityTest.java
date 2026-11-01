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

import java.util.Collections;

import org.apache.hugegraph.api.cypher.CypherErrorMapper;
import org.apache.hugegraph.api.cypher.CypherModel;
import org.apache.hugegraph.rest.RestResult;
import org.apache.hugegraph.structure.gremlin.Response;
import org.junit.Assert;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Run with an existing hugegraph-client jar (see docs/cypher-api.md).
 * Kept outside the Server reactor to avoid a Server -> Client dependency cycle.
 */
public class CypherClientCompatibilityTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    @Test
    public void testLegacySuccessAndFailure() {
        for (int code : new int[]{200, 400}) {
            String body = "{\"requestId\":\"request\",\"status\":{\"code\":" + code +
                          ",\"message\":\"original\",\"attributes\":{}}," +
                          "\"result\":{\"data\":" + (code == 200 ? "[]" : "null") +
                          ",\"meta\":{}}}";
            Response response = read(body);
            Assert.assertEquals(code, response.status().code());
            Assert.assertEquals("original", response.status().message());
            Assert.assertTrue(response.status().attributes().isEmpty());
        }
    }

    @Test
    public void testCurrentSuccess() throws Exception {
        Response response = read(JSON.writeValueAsString(CypherModel.dataOf(
                "request", Collections.singletonList(Collections.singletonMap("value", 1)))));
        Assert.assertEquals(200, response.status().code());
        Assert.assertEquals("request", response.requestId());
        Assert.assertTrue(response.status().attributes().isEmpty());
        Assert.assertEquals(1, response.result().size());
    }

    @Test
    public void testCurrentFailure() throws Exception {
        CypherModel.CypherError error = CypherErrorMapper.map(
                new IllegalArgumentException("Invalid input 'R'"));
        Response response = read(JSON.writeValueAsString(
                CypherModel.failOf("request", "original failure", error)));
        // CypherManager checks this status before handing the result to Hubble.
        Assert.assertEquals(400, response.status().code());
        Assert.assertEquals("original failure", response.status().message());
        Assert.assertEquals("HugeGraph.Cypher.SyntaxError", JSON.valueToTree(
                response.status().attributes()).at("/errors/0/code").asText());
    }

    @Test
    public void testTopLevelErrorsNegativeControl() throws Exception {
        ObjectNode body = (ObjectNode) JSON.readTree(JSON.writeValueAsString(
                CypherModel.dataOf("request", Collections.emptyList())));
        body.putArray("errors");
        RuntimeException failure = Assert.assertThrows(RuntimeException.class,
                                                       () -> read(body.toString()));
        Throwable cause = failure;
        while (cause.getCause() != null) {
            cause = cause.getCause();
        }
        Assert.assertEquals("UnrecognizedPropertyException", cause.getClass().getSimpleName());
        Assert.assertTrue(cause.getMessage().contains("errors"));
    }

    private static Response read(String body) {
        return new RestResult(200, body, null).readObject(Response.class);
    }
}
