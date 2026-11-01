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

import java.io.IOException;
import java.util.Map;

import org.apache.hugegraph.api.BaseApiTest.RestClient;
import org.junit.Assert;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import jakarta.ws.rs.core.Response;

public class CypherAnonymousApiTest {

    private static final String PATH = BaseApiTest.URL_PREFIX + "/cypher";
    private static final String QUERY = "RETURN 1 AS value";
    private static final ObjectMapper JSON = new ObjectMapper();

    @Test
    public void testAnonymousGet() throws IOException {
        RestClient client = new RestClient(BaseApiTest.baseUrl(), false);
        try {
            assertQueryResult(client.get(PATH, Map.of("cypher", QUERY)));
        } finally {
            client.close();
        }
    }

    @Test
    public void testAnonymousPost() throws IOException {
        RestClient client = new RestClient(BaseApiTest.baseUrl(), false);
        try {
            assertQueryResult(client.post(PATH, QUERY));
        } finally {
            client.close();
        }
    }

    private static void assertQueryResult(Response response) throws IOException {
        JsonNode result = JSON.readTree(BaseApiTest.assertResponseStatus(200, response));
        Assert.assertEquals(200, result.path("status").path("code").asInt());
        JsonNode data = result.path("result").path("data");
        Assert.assertTrue(data.isArray());
        Assert.assertEquals(1, data.size());
        Assert.assertEquals(1, data.get(0).path("value").asInt());
    }
}
