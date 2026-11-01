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


package org.apache.hugegraph.SimpleClusterTest;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;

import jakarta.ws.rs.core.Response;

/**
 * Writes graph data through the server and reads it back, so the request path
 * crosses Server, the HStore backend and the Store node of the simple cluster.
 */
public class SimpleClusterGraphTest extends BaseSimpleTest {

    private static final String SCHEMA = URL_PREFIX + "/schema";
    private static final String VERTICES = URL_PREFIX + "/graph/vertices";
    private static final String EDGES = URL_PREFIX + "/graph/edges";
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @BeforeClass
    public static void initSchema() {
        createPropertyKey("ct_name", "TEXT");
        createPropertyKey("ct_age", "INT");
        createPropertyKey("ct_weight", "DOUBLE");
        createAndAssert(SCHEMA + "/vertexlabels", "{"
                                                  + "\"name\": \"ct_person\","
                                                  + "\"id_strategy\": \"PRIMARY_KEY\","
                                                  + "\"properties\": [\"ct_name\", \"ct_age\"],"
                                                  + "\"primary_keys\": [\"ct_name\"],"
                                                  + "\"check_exist\": false"
                                                  + "}", 201);
        createAndAssert(SCHEMA + "/edgelabels", "{"
                                                + "\"name\": \"ct_knows\","
                                                + "\"source_label\": \"ct_person\","
                                                + "\"target_label\": \"ct_person\","
                                                + "\"properties\": [\"ct_weight\"],"
                                                + "\"check_exist\": false"
                                                + "}", 201);
    }

    @Test
    public void testVertexAndEdgeRoundTripThroughStore() throws IOException {
        String alice = createPerson("ct-alice", 30);
        String bob = createPerson("ct-bob", 31);

        Map<?, ?> vertex = read(assertResponseStatus(200, client.get(VERTICES, json(alice))));
        Assert.assertEquals(alice, vertex.get("id"));
        Assert.assertEquals("ct_person", vertex.get("label"));
        Map<?, ?> props = (Map<?, ?>) vertex.get("properties");
        Assert.assertEquals("ct-alice", props.get("ct_name"));
        Assert.assertEquals(30, props.get("ct_age"));

        Response r = client.post(EDGES, "{"
                                        + "\"label\": \"ct_knows\","
                                        + "\"outV\": " + json(alice) + ","
                                        + "\"inV\": " + json(bob) + ","
                                        + "\"outVLabel\": \"ct_person\","
                                        + "\"inVLabel\": \"ct_person\","
                                        + "\"properties\": {\"ct_weight\": 0.5}"
                                        + "}");
        String edgeId = (String) read(assertResponseStatus(201, r)).get("id");

        Map<?, ?> edge = read(assertResponseStatus(200, client.get(EDGES, edgeId)));
        Assert.assertEquals(alice, edge.get("outV"));
        Assert.assertEquals(bob, edge.get("inV"));
        Assert.assertEquals(0.5, ((Map<?, ?>) edge.get("properties")).get("ct_weight"));

        // Adjacency is served from the Store side, so query it by direction
        List<?> out = edges(alice, "OUT");
        Assert.assertEquals(1, out.size());
        Assert.assertEquals(edgeId, ((Map<?, ?>) out.get(0)).get("id"));
        Assert.assertEquals(1, edges(bob, "IN").size());
        Assert.assertEquals(0, edges(bob, "OUT").size());
    }

    @Test
    public void testInvalidGraphRequestsAreRejected() {
        Response r = client.post(VERTICES, "{"
                                           + "\"label\": \"ct_undefined_label\","
                                           + "\"properties\": {\"ct_name\": \"x\"}"
                                           + "}");
        assertResponseStatus(400, r);

        r = client.post(VERTICES, "{"
                                  + "\"label\": \"ct_person\","
                                  + "\"properties\": {\"ct_name\": \"x\", \"ct_age\": \"old\"}"
                                  + "}");
        assertResponseStatus(400, r);

        // A primary key vertex can't be created without its primary key value
        r = client.post(VERTICES, "{"
                                  + "\"label\": \"ct_person\","
                                  + "\"properties\": {\"ct_age\": 1}"
                                  + "}");
        assertResponseStatus(400, r);

        r = client.get(VERTICES, json("ct-person-never-written"));
        assertResponseStatus(404, r);
    }

    private static void createPropertyKey(String name, String dataType) {
        createAndAssert(SCHEMA + "/propertykeys", "{"
                                                  + "\"name\": \"" + name + "\","
                                                  + "\"data_type\": \"" + dataType + "\","
                                                  + "\"cardinality\": \"SINGLE\","
                                                  + "\"check_exist\": false,"
                                                  + "\"properties\": []"
                                                  + "}", 202);
    }

    private static String createPerson(String name, int age) throws IOException {
        Response r = client.post(VERTICES, "{"
                                           + "\"label\": \"ct_person\","
                                           + "\"properties\": {"
                                           + "\"ct_name\": \"" + name + "\","
                                           + "\"ct_age\": " + age
                                           + "}}");
        return (String) read(assertResponseStatus(201, r)).get("id");
    }

    private static List<?> edges(String vertexId, String direction) {
        Response r = client.get(EDGES, ImmutableMap.<String, Object>of("vertex_id", json(vertexId),
                                                                       "direction", direction));
        try {
            return (List<?>) read(assertResponseStatus(200, r)).get("edges");
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    private static Map<?, ?> read(String content) throws IOException {
        return MAPPER.readValue(content, Map.class);
    }

    private static String json(String id) {
        return "\"" + id + "\"";
    }
}
