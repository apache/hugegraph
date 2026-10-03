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
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

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

        // Filtering by the decimal value is exact on every backend. A vertex
        // filter without an index is refused, so the case that reaches the
        // store is an edge query by vertex + label + property: on HStore
        // the condition is pushed down and must arrive as a BigDecimal
        createAndAssert(URL_PREFIX + "/schema/edgelabels",
                        "{" +
                        "\"name\": \"pay\"," +
                        "\"source_label\": \"person\"," +
                        "\"target_label\": \"person\"," +
                        "\"frequency\": \"SINGLE\"," +
                        "\"properties\":[\"amount\"]," +
                        "\"nullable_keys\":[\"amount\"]," +
                        "\"check_exist\": false" +
                        "}");
        String payer = parseId(assertResponseStatus(201, client().post(PATH,
                "{\"label\":\"person\",\"properties\":{\"name\":\"payer\"," +
                "\"age\":30,\"city\":\"Beijing\"}}")));
        String payee = parseId(assertResponseStatus(201, client().post(PATH,
                "{\"label\":\"person\",\"properties\":{\"name\":\"payee\"," +
                "\"age\":31,\"city\":\"Beijing\"}}")));
        String edge = "{\"label\":\"pay\",\"outVLabel\":\"person\"," +
                      "\"inVLabel\":\"person\",\"outV\":\"" + payer + "\"," +
                      "\"inV\":\"" + payee + "\"," +
                      "\"properties\":{\"amount\":" + literal + "}}";
        content = assertResponseStatus(201, client().post(
                URL_PREFIX + "/graph/edges/", edge));
        Assert.assertContains("\"amount\":\"" + literal + "\"", content);

        String edges = URL_PREFIX + "/graph/edges/";
        r = client().get(edges, ImmutableMap.of(
                "vertex_id", id2Json(payer), "direction", "OUT", "label", "pay",
                "properties", URLEncoder.encode("{\"amount\":\"" + literal + "\"}",
                                                StandardCharsets.UTF_8)));
        content = assertResponseStatus(200, r);
        Assert.assertContains("\"amount\":\"" + literal + "\"", content);
        String near = literal.substring(0, literal.length() - 1) + "9";
        r = client().get(edges, ImmutableMap.of(
                "vertex_id", id2Json(payer), "direction", "OUT", "label", "pay",
                "properties", URLEncoder.encode("{\"amount\":\"" + near + "\"}",
                                                StandardCharsets.UTF_8)));
        content = assertResponseStatus(200, r);
        Assert.assertEquals("{\"edges\":[]}", content);
        // the same filter as JSON number literals: parsed exactly, not as a
        // double, so the exact value hits and the one-digit change misses
        r = client().get(edges, ImmutableMap.of(
                "vertex_id", id2Json(payer), "direction", "OUT", "label", "pay",
                "properties", URLEncoder.encode("{\"amount\":" + literal + "}",
                                                StandardCharsets.UTF_8)));
        content = assertResponseStatus(200, r);
        Assert.assertContains("\"amount\":\"" + literal + "\"", content);
        r = client().get(edges, ImmutableMap.of(
                "vertex_id", id2Json(payer), "direction", "OUT", "label", "pay",
                "properties", URLEncoder.encode("{\"amount\":" + near + "}",
                                                StandardCharsets.UTF_8)));
        content = assertResponseStatus(200, r);
        Assert.assertEquals("{\"edges\":[]}", content);

        // a predicate with a fractional operand is exact as well
        r = client().get(edges, ImmutableMap.of(
                "vertex_id", id2Json(payer), "direction", "OUT", "label", "pay",
                "properties", URLEncoder.encode("{\"amount\":\"P.eq(" + literal + ")\"}",
                                                StandardCharsets.UTF_8)));
        content = assertResponseStatus(200, r);
        Assert.assertContains("\"amount\":\"" + literal + "\"", content);
        r = client().get(edges, ImmutableMap.of(
                "vertex_id", id2Json(payer), "direction", "OUT", "label", "pay",
                "properties", URLEncoder.encode("{\"amount\":\"P.eq(" + near + ")\"}",
                                                StandardCharsets.UTF_8)));
        content = assertResponseStatus(200, r);
        Assert.assertEquals("{\"edges\":[]}", content);
        r = client().get(edges, ImmutableMap.of(
                "vertex_id", id2Json(payer), "direction", "OUT", "label", "pay",
                "properties", URLEncoder.encode("{\"amount\":\"P.within(" + near + "," +
                                                literal + ")\"}", StandardCharsets.UTF_8)));
        content = assertResponseStatus(200, r);
        Assert.assertContains("\"amount\":\"" + literal + "\"", content);

        // a LIST key: a list filter converts every member, a scalar filter
        // keeps membership semantics
        createAndAssert(URL_PREFIX + "/schema/propertykeys",
                        "{" +
                        "\"name\": \"amounts\"," +
                        "\"data_type\": \"DECIMAL\"," +
                        "\"cardinality\": \"LIST\"," +
                        "\"check_exist\": false," +
                        "\"properties\":[]" +
                        "}", 202);
        createAndAssert(URL_PREFIX + "/schema/edgelabels",
                        "{" +
                        "\"name\": \"pays\"," +
                        "\"source_label\": \"person\"," +
                        "\"target_label\": \"person\"," +
                        "\"frequency\": \"SINGLE\"," +
                        "\"properties\":[\"amounts\"]," +
                        "\"nullable_keys\":[\"amounts\"]," +
                        "\"check_exist\": false" +
                        "}");
        content = assertResponseStatus(201, client().post(URL_PREFIX + "/graph/edges/",
                "{\"label\":\"pays\",\"outVLabel\":\"person\"," +
                "\"inVLabel\":\"person\",\"outV\":\"" + payer + "\"," +
                "\"inV\":\"" + payee + "\"," +
                "\"properties\":{\"amounts\":[" + literal + ", 1.0]}}"));
        Assert.assertContains("\"amounts\":[\"" + literal + "\",\"1.0\"]", content);
        r = client().get(edges, ImmutableMap.of(
                "vertex_id", id2Json(payer), "direction", "OUT", "label", "pays",
                "properties", URLEncoder.encode("{\"amounts\":[" + literal + ",1.0]}",
                                                StandardCharsets.UTF_8)));
        content = assertResponseStatus(200, r);
        Assert.assertContains("\"amounts\":[\"" + literal + "\",\"1.0\"]", content);
        r = client().get(edges, ImmutableMap.of(
                "vertex_id", id2Json(payer), "direction", "OUT", "label", "pays",
                "properties", URLEncoder.encode("{\"amounts\":" + near + "}",
                                                StandardCharsets.UTF_8)));
        content = assertResponseStatus(200, r);
        Assert.assertEquals("{\"edges\":[]}", content);

        // a DECIMAL default value given as a JSON number keeps every digit,
        // on create and after the schema is read back from the backend
        String fee = "0.1234567890123456789";
        createAndAssert(URL_PREFIX + "/schema/propertykeys",
                        "{" +
                        "\"name\": \"fee\"," +
                        "\"data_type\": \"DECIMAL\"," +
                        "\"cardinality\": \"SINGLE\"," +
                        "\"check_exist\": false," +
                        "\"user_data\": {\"~default_value\": " + fee + "}," +
                        "\"properties\":[]" +
                        "}", 202);
        r = client().get(URL_PREFIX + "/schema/propertykeys/", "fee");
        content = assertResponseStatus(200, r);
        Assert.assertContains("\"~default_value\":\"" + fee + "\"", content);
        createAndAssert(URL_PREFIX + "/schema/vertexlabels",
                        "{" +
                        "\"primary_keys\":[\"name\"]," +
                        "\"id_strategy\": \"PRIMARY_KEY\"," +
                        "\"name\": \"fees\"," +
                        "\"properties\":[\"name\", \"fee\"]," +
                        "\"check_exist\": false," +
                        "\"nullable_keys\":[\"fee\"]" +
                        "}");
        r = client().post(PATH, "{\"label\":\"fees\",\"properties\":{\"name\":\"f1\"}}");
        content = assertResponseStatus(201, r);
        Assert.assertContains("\"fee\":\"" + fee + "\"", content);

        // a fraction elsewhere in a body keeps its usual type: a DOUBLE key
        // with a fractional default value round-trips as a JSON number
        createAndAssert(URL_PREFIX + "/schema/propertykeys",
                        "{" +
                        "\"name\": \"ratio\"," +
                        "\"data_type\": \"DOUBLE\"," +
                        "\"cardinality\": \"SINGLE\"," +
                        "\"check_exist\": false," +
                        "\"user_data\": {\"~default_value\": 1.5, \"rate\": 0.85}," +
                        "\"properties\":[]" +
                        "}", 202);
        r = client().get(URL_PREFIX + "/schema/propertykeys/", "ratio");
        content = assertResponseStatus(200, r);
        Assert.assertContains("\"~default_value\":1.5", content);
        // custom metadata keeps its type: a JSON number, not a string
        Assert.assertContains("\"rate\":0.85", content);
        // an OBJECT property keeps the numbers inside it as numbers
        createAndAssert(URL_PREFIX + "/schema/propertykeys",
                        "{" +
                        "\"name\": \"meta\"," +
                        "\"data_type\": \"OBJECT\"," +
                        "\"cardinality\": \"SINGLE\"," +
                        "\"check_exist\": false," +
                        "\"properties\":[]" +
                        "}", 202);
        createAndAssert(URL_PREFIX + "/schema/vertexlabels",
                        "{" +
                        "\"primary_keys\":[\"name\"]," +
                        "\"id_strategy\": \"PRIMARY_KEY\"," +
                        "\"name\": \"metas\"," +
                        "\"properties\":[\"name\", \"meta\"]," +
                        "\"check_exist\": false," +
                        "\"nullable_keys\":[\"meta\"]" +
                        "}");
        r = client().post(PATH, "{\"label\":\"metas\",\"properties\":{\"name\":\"m1\"," +
                                "\"meta\":{\"ratio\":0.25,\"n\":3}}}");
        content = assertResponseStatus(201, r);
        Assert.assertContains("\"ratio\":0.25", content);
        r = client().get(PATH, String.format("\"%s\"", parseId(content)));
        content = assertResponseStatus(200, r);
        Assert.assertContains("\"ratio\":0.25", content);

        // a default of another type keeps the form the user sent (a DATE
        // default is converted only when it is applied, as on master)
        createAndAssert(URL_PREFIX + "/schema/propertykeys",
                        "{" +
                        "\"name\": \"day\"," +
                        "\"data_type\": \"DATE\"," +
                        "\"cardinality\": \"SINGLE\"," +
                        "\"check_exist\": false," +
                        "\"user_data\": {\"~default_value\": \"2020-01-01\"}," +
                        "\"properties\":[]" +
                        "}", 202);
        r = client().get(URL_PREFIX + "/schema/propertykeys/", "day");
        content = assertResponseStatus(200, r);
        Assert.assertContains("\"~default_value\":\"2020-01-01\"", content);

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
