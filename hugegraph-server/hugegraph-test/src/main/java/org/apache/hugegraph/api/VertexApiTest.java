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

import org.apache.hugegraph.testutil.Assert;
import org.junit.Before;
import org.junit.Test;

import jakarta.ws.rs.core.Response;

import com.google.common.collect.ImmutableMap;

public class VertexApiTest extends BaseApiTest {

    private static final String PATH = "/graphspaces/DEFAULT/graphs/hugegraph/graph/vertices/";

    @Before
    public void prepareSchema() {
        initPropertyKey();
        initVertexLabel();
    }

    @Test
    public void testCreate() {
        String vertex = "{" +
                        "\"label\":\"person\"," +
                        "\"properties\":{" +
                        "\"name\":\"James\"," +
                        "\"city\":\"Beijing\"," +
                        "\"age\":19}" +
                        "}";
        Response r = client().post(PATH, vertex);
        assertResponseStatus(201, r);
    }

    @Test
    public void testGet() throws IOException {
        String vertex = "{" +
                        "\"label\":\"person\"," +
                        "\"properties\":{" +
                        "\"name\":\"James\"," +
                        "\"city\":\"Beijing\"," +
                        "\"age\":19}" +
                        "}";
        Response r = client().post(PATH, vertex);
        String content = assertResponseStatus(201, r);

        String id = parseId(content);
        id = String.format("\"%s\"", id);
        r = client().get(PATH, id);
        assertResponseStatus(200, r);
    }

    @Test
    public void testList() {
        String vertex = "{" +
                        "\"label\":\"person\"," +
                        "\"properties\":{" +
                        "\"name\":\"James\"," +
                        "\"city\":\"Beijing\"," +
                        "\"age\":19}" +
                        "}";
        Response r = client().post(PATH, vertex);
        assertResponseStatus(201, r);

        r = client().get(PATH);
        assertResponseStatus(200, r);
    }

    @Test
    public void testDelete() throws IOException {
        String vertex = "{" +
                        "\"label\":\"person\"," +
                        "\"properties\":{" +
                        "\"name\":\"James\"," +
                        "\"city\":\"Beijing\"," +
                        "\"age\":19}" +
                        "}";
        Response r = client().post(PATH, vertex);
        String content = assertResponseStatus(201, r);

        String id = parseId(content);
        id = String.format("\"%s\"", id);
        r = client().delete(PATH, id);
        assertResponseStatus(204, r);
    }

    @Test
    public void testBatchUpdateDecimalWithSumStrategy() throws IOException {
        // schema: a decimal balance on an account keyed by name
        createAndAssert(URL_PREFIX + "/schema/propertykeys",
                        "{" +
                        "\"name\": \"balance\"," +
                        "\"data_type\": \"DECIMAL\"," +
                        "\"cardinality\": \"SINGLE\"," +
                        "\"check_exist\": false," +
                        "\"properties\":[]" +
                        "}", 202);
        createAndAssert(URL_PREFIX + "/schema/vertexlabels",
                        "{" +
                        "\"primary_keys\":[\"name\"]," +
                        "\"id_strategy\": \"PRIMARY_KEY\"," +
                        "\"name\": \"account\"," +
                        "\"properties\":[\"name\", \"balance\"]," +
                        "\"check_exist\": false," +
                        "\"nullable_keys\":[\"balance\"]" +
                        "}");

        // 2^256 - 2, as a string
        String almostMax = "115792089237316195423570985008687907853" +
                           "269984665640564039457584007913129639934";
        String max = "115792089237316195423570985008687907853" +
                     "269984665640564039457584007913129639935";
        String vertex = "{" +
                        "\"label\":\"account\"," +
                        "\"properties\":{" +
                        "\"name\":\"alice\"," +
                        "\"balance\":\"" + almostMax + "\"}" +
                        "}";
        Response r = client().post(PATH, vertex);
        String content = assertResponseStatus(201, r);
        String id = parseId(content);
        Assert.assertContains("\"balance\":\"" + almostMax + "\"", content);

        // SUM through the batch update: the server adds exactly; an integral
        // JSON number literal is accepted as the increment
        String batch = "{" +
                       "\"vertices\":[{" +
                       "\"label\":\"account\"," +
                       "\"properties\":{" +
                       "\"name\":\"alice\"," +
                       "\"balance\":1}" +
                       "}]," +
                       "\"update_strategies\":{\"balance\":\"SUM\"}," +
                       "\"create_if_not_exist\":true" +
                       "}";
        r = client().put(PATH, "batch", batch, ImmutableMap.of());
        content = assertResponseStatus(200, r);
        Assert.assertContains("\"balance\":\"" + max + "\"", content);

        // a fraction is sent as a string (a JSON fraction literal would be a
        // double to the parser); two entries for the same vertex in one
        // request are combined first, then added to the stored value
        batch = "{" +
                "\"vertices\":[{" +
                "\"label\":\"account\"," +
                "\"properties\":{" +
                "\"name\":\"alice\"," +
                "\"balance\":\"0.000000000000000000\"}" +
                "},{" +
                "\"label\":\"account\"," +
                "\"properties\":{" +
                "\"name\":\"alice\"," +
                "\"balance\":\"0.000000000000000001\"}" +
                "}]," +
                "\"update_strategies\":{\"balance\":\"SUM\"}," +
                "\"create_if_not_exist\":true" +
                "}";
        r = client().put(PATH, "batch", batch, ImmutableMap.of());
        content = assertResponseStatus(200, r);
        String expected = max + ".000000000000000001";
        Assert.assertContains("\"balance\":\"" + expected + "\"", content);

        // read back through GET
        r = client().get(PATH, String.format("\"%s\"", id));
        content = assertResponseStatus(200, r);
        Assert.assertContains("\"balance\":\"" + expected + "\"", content);

        // BIGGER keeps the larger of the two, compared as decimals
        batch = "{" +
                "\"vertices\":[{" +
                "\"label\":\"account\"," +
                "\"properties\":{" +
                "\"name\":\"alice\"," +
                "\"balance\":\"" + almostMax + "\"}" +
                "}]," +
                "\"update_strategies\":{\"balance\":\"BIGGER\"}," +
                "\"create_if_not_exist\":true" +
                "}";
        r = client().put(PATH, "batch", batch, ImmutableMap.of());
        content = assertResponseStatus(200, r);
        Assert.assertContains("\"balance\":\"" + expected + "\"", content);
    }
}
