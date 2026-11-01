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

package org.apache.hugegraph.api.traversers;

import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.hugegraph.api.BaseApiTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import jakarta.ws.rs.core.Response;

public class SameNeighborsBatchApiTest extends BaseApiTest {

    private static final String PATH = TRAVERSERS_API + "/sameneighborsbatch";

    @Before
    public void prepareSchema() {
        BaseApiTest.initPropertyKey();
        BaseApiTest.initVertexLabel();
        BaseApiTest.initEdgeLabel();
        BaseApiTest.initVertex();
        BaseApiTest.initEdge();
    }

    @Test
    public void testPairOrderAndMeasures() {
        Map<String, String> ids = listAllVertexName2Ids();
        String body = String.format("{\"vertex_list\":[[\"%s\",\"%s\"],[\"%s\",\"%s\"]]}",
                                    ids.get("marko"), ids.get("josh"),
                                    ids.get("marko"), ids.get("peter"));
        try (Response response = client().post(PATH, body)) {
            String content = assertResponseStatus(200, response);
            List<List<String>> results = assertJsonContains(content, "same_neighbors");
            Assert.assertEquals(2, results.size());
            Assert.assertEquals(1, results.get(0).size());
            Assert.assertTrue(results.get(0).contains(ids.get("peter")));
            Assert.assertEquals(1, results.get(1).size());
            Assert.assertTrue(results.get(1).contains(ids.get("ripple")));
            Map<String, Object> measures = assertJsonContains(content, "measure");
            Assert.assertEquals(4L, ((Number) measures.get("vertice_iterations")).longValue());
            Assert.assertEquals(9L, ((Number) measures.get("edge_iterations")).longValue());
        }
    }

    @Test
    public void testPerPairLimitAndDuplicates() {
        String marko = listAllVertexName2Ids().get("marko");
        for (int limit : new int[]{1, 2, -1}) {
            String body = String.format("{\"vertex_list\":[[\"%s\",\"%s\"],[\"%s\",\"%s\"]]," +
                                        "\"limit\":%s}", marko, marko, marko, marko, limit);
            try (Response response = client().post(PATH, body)) {
                String content = assertResponseStatus(200, response);
                List<List<String>> results = assertJsonContains(content, "same_neighbors");
                Assert.assertEquals(2, results.size());
                Assert.assertEquals(limit == 1 ? 1 : 2, results.get(0).size());
                Assert.assertEquals(new HashSet<>(results.get(0)), new HashSet<>(results.get(1)));
            }
        }
    }

    @Test
    public void testLabelAndDirection() {
        Map<String, String> ids = listAllVertexName2Ids();
        String body = String.format("{\"vertex_list\":[[\"%s\",\"%s\"]]," +
                                    "\"direction\":\"OUT\",\"label\":\"created\"}",
                                    ids.get("marko"), ids.get("peter"));
        try (Response response = client().post(PATH, body)) {
            String content = assertResponseStatus(200, response);
            List<List<String>> results = assertJsonContains(content, "same_neighbors");
            Assert.assertEquals(1, results.size());
            Assert.assertEquals(1, results.get(0).size());
            Assert.assertTrue(results.get(0).contains(ids.get("ripple")));
        }
        body = String.format("{\"vertex_list\":[[\"%s\",\"%s\"]]," +
                             "\"direction\":\"IN\",\"label\":\"created\"}",
                             ids.get("marko"), ids.get("peter"));
        try (Response response = client().post(PATH, body)) {
            String content = assertResponseStatus(200, response);
            List<List<String>> results = assertJsonContains(content, "same_neighbors");
            Assert.assertEquals(1, results.size());
            Assert.assertTrue(results.get(0).isEmpty());
        }
    }

    @Test
    public void testMaximumLimit() {
        String marko = listAllVertexName2Ids().get("marko");
        String body = String.format("{\"vertex_list\":[[\"%s\",\"%s\"]],\"limit\":2147483647}",
                                    marko, marko);
        try (Response response = client().post(PATH, body)) {
            String content = assertResponseStatus(200, response);
            List<?> results = assertJsonContains(content, "same_neighbors");
            Assert.assertEquals(1, results.size());
        }
    }

    @Test
    public void testInvalidNumericIds() {
        for (String id : new String[]{"1.0", "1.5", "9007199254740993.0",
                                      "9223372036854775808", "-9223372036854775809"}) {
            String body = "{\"vertex_list\":[[" + id + ",1]]}";
            try (Response response = client().post(PATH, body)) {
                String content = assertResponseStatus(400, response);
                Assert.assertTrue(content, content.contains(
                        "Numeric vertex ids must be whole numbers in the long range"));
            }
        }
    }

    @Test
    public void testInvalidInput() {
        String marko = listAllVertexName2Ids().get("marko");
        String validPair = String.format("[[\"%s\",\"%s\"]]", marko, marko);
        String[] bodies = {
                "null", "{}", "{\"vertex_list\":null}", "{\"vertex_list\":[]}",
                "{\"vertex_list\":[null]}", "{\"vertex_list\":[[]]}",
                "{\"vertex_list\":[[1]]}", "{\"vertex_list\":[[1,2,3]]}",
                "{\"vertex_list\":[[null,1]]}", "{\"vertex_list\":[[true,1]]}",
                "{\"vertex_list\":[[{},1]]}", "{\"vertex_list\":[[[],1]]}",
                "{\"vertex_list\":[[\"missing-a\",\"missing-b\"]]}",
                "{\"vertex_list\":" + validPair + ",\"limit\":0}",
                "{\"vertex_list\":" + validPair + ",\"limit\":-2}",
                "{\"vertex_list\":" + validPair + ",\"limit\":2147483648}",
                "{\"vertex_list\":" + validPair + ",\"max_degree\":0}",
                "{\"vertex_list\":" + validPair + ",\"max_degree\":-2}",
                "{\"vertex_list\":" + validPair + ",\"direction\":\"invalid\"}"
        };
        for (String body : bodies) {
            try (Response response = client().post(PATH, body)) {
                assertResponseStatus(400, response);
            }
        }
    }
}
