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
    public void testDecimalJsonNumberLiteralIsExact() throws IOException {
        createAndAssert(URL_PREFIX + "/schema/propertykeys",
                        "{" +
                        "\"name\": \"amount\"," +
                        "\"data_type\": \"DECIMAL\"," +
                        "\"cardinality\": \"SINGLE\"," +
                        "\"check_exist\": false," +
                        "\"properties\":[]" +
                        "}", 202);
        createAndAssert(URL_PREFIX + "/schema/propertykeys",
                        "{" +
                        "\"name\": \"weight\"," +
                        "\"data_type\": \"DOUBLE\"," +
                        "\"cardinality\": \"SINGLE\"," +
                        "\"check_exist\": false," +
                        "\"properties\":[]" +
                        "}", 202);
        createAndAssert(URL_PREFIX + "/schema/vertexlabels",
                        "{" +
                        "\"primary_keys\":[\"name\"]," +
                        "\"id_strategy\": \"PRIMARY_KEY\"," +
                        "\"name\": \"transfer\"," +
                        "\"properties\":[\"name\", \"amount\", \"weight\"]," +
                        "\"check_exist\": false," +
                        "\"nullable_keys\":[\"amount\", \"weight\"]" +
                        "}");

        // 39 significant digits as a JSON number literal: a double parser
        // would keep 17 of them; the value is stored and echoed exactly
        String literal = "12345678901234567890.123456789012345678";
        String vertex = "{" +
                        "\"label\":\"transfer\"," +
                        "\"properties\":{" +
                        "\"name\":\"t1\"," +
                        "\"amount\":" + literal + "," +
                        "\"weight\":" + literal + "}" +
                        "}";
        Response r = client().post(PATH, vertex);
        String content = assertResponseStatus(201, r);
        Assert.assertContains("\"amount\":\"" + literal + "\"", content);
        // the DOUBLE key narrows the same literal to a double, as before
        Assert.assertContains("\"weight\":1.2345678901234567E19", content);

        r = client().get(PATH, String.format("\"%s\"", parseId(content)));
        content = assertResponseStatus(200, r);
        Assert.assertContains("\"amount\":\"" + literal + "\"", content);

        // exponent literals are accepted and stored in plain form
        vertex = "{" +
                 "\"label\":\"transfer\"," +
                 "\"properties\":{" +
                 "\"name\":\"t2\"," +
                 "\"amount\":1E-18," +
                 "\"weight\":2.5}" +
                 "}";
        r = client().post(PATH, vertex);
        content = assertResponseStatus(201, r);
        Assert.assertContains("\"amount\":\"0.000000000000000001\"", content);
        Assert.assertContains("\"weight\":2.5", content);

        // integer keys still reject a fraction, with the usual message
        vertex = "{" +
                 "\"label\":\"person\"," +
                 "\"properties\":{" +
                 "\"name\":\"t3\"," +
                 "\"age\":29.5," +
                 "\"city\":\"Beijing\"}" +
                 "}";
        r = client().post(PATH, vertex);
        content = assertResponseStatus(400, r);
        Assert.assertContains("Invalid property value", content);
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

        // a fraction as a string and as a JSON number literal (read as
        // BigDecimal, see ObjectMapperResolver); two entries for the same
        // vertex in one request are combined first, then added to the
        // stored value
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
                "\"balance\":0.000000000000000001}" +
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
