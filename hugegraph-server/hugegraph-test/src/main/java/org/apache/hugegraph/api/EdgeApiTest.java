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

import com.google.common.collect.ImmutableMap;

import jakarta.ws.rs.core.Response;

public class EdgeApiTest extends BaseApiTest {

    private static final String PATH = "/graphspaces/DEFAULT/graphs/hugegraph/graph/edges/";

    @Before
    public void prepareSchema() {
        initPropertyKey();
        initVertexLabel();
        initEdgeLabel();
        initVertex();
    }

    @Test
    public void testCreate() throws IOException {
        String outVId = getVertexId("person", "name", "peter");
        String inVId = getVertexId("software", "name", "lop");

        String edge = String.format("{" +
                                    "\"label\": \"created\"," +
                                    "\"outVLabel\": \"person\"," +
                                    "\"inVLabel\": \"software\"," +
                                    "\"outV\": \"%s\"," +
                                    "\"inV\": \"%s\"," +
                                    "\"properties\":{" +
                                    "\"date\": \"20170324\"," +
                                    "\"weight\": 0.5}" +
                                    "}", outVId, inVId);
        Response r = client().post(PATH, edge);
        assertResponseStatus(201, r);
    }

    @Test
    public void testBatchUpdate() throws IOException {
        String outVId = getVertexId("person", "name", "marko");
        String inVId = getVertexId("person", "name", "josh");
        // Create
        String edge = String.format("{" +
                                    "\"label\": \"knows\"," +
                                    "\"outVLabel\": \"person\"," +
                                    "\"inVLabel\": \"person\"," +
                                    "\"outV\": \"%s\"," +
                                    "\"inV\": \"%s\"," +
                                    "\"properties\":{" +
                                    "\"date\": \"2013-02-20\"," +
                                    "\"weight\": 1.0}" +
                                    "}", outVId, inVId);
        Response r = client().post(PATH, edge);
        // The edge id is 'S1:marko>1>7JooBil0>S1:josh'
        String content = assertResponseStatus(201, r);
        String edgeId = parseId(content);

        // Update edge with edgeId
        edge = String.format("{" +
                             "\"edges\":[" +
                             "{" +
                             "\"id\":\"%s\"," +
                             "\"label\":\"knows\"," +
                             "\"outV\":\"%s\"," +
                             "\"outVLabel\":\"person\"," +
                             "\"inV\":\"%s\"," +
                             "\"inVLabel\":\"person\"," +
                             "\"properties\":{" +
                             "\"weight\":0.2," +
                             "\"date\":\"2014-02-20\"" +
                             "}" +
                             "}" +
                             "]," +
                             "\"update_strategies\":{" +
                             "\"weight\":\"SUM\"," +
                             "\"date\":\"BIGGER\"" +
                             "}," +
                             "\"check_vertex\":false," +
                             "\"create_if_not_exist\":true" +
                             "}", edgeId, outVId, inVId);
        r = client().put(PATH, "batch", edge, ImmutableMap.of());
        // Now allowed to modify sortkey values, the property 'date' has changed
        content = assertResponseStatus(400, r);
        Assert.assertTrue(content.contains(
                "either be null or equal to the origin value when " +
                "specified edge id"));

        // Update edge without edgeId
        edge = String.format("{" +
                             "\"edges\":[" +
                             "{" +
                             "\"label\":\"knows\"," +
                             "\"outV\":\"%s\"," +
                             "\"outVLabel\":\"person\"," +
                             "\"inV\":\"%s\"," +
                             "\"inVLabel\":\"person\"," +
                             "\"properties\":{" +
                             "\"weight\":0.2," +
                             "\"date\":\"2014-02-20\"" +
                             "}" +
                             "}" +
                             "]," +
                             "\"update_strategies\":{" +
                             "\"weight\":\"SUM\"," +
                             "\"date\":\"BIGGER\"" +
                             "}," +
                             "\"check_vertex\":false," +
                             "\"create_if_not_exist\":true" +
                             "}", outVId, inVId);
        r = client().put(PATH, "batch", edge, ImmutableMap.of());
        // Add a new edge when sortkey value has changed
        content = assertResponseStatus(200, r);
        String newEdgeId = parseId(content);
        Assert.assertNotEquals(newEdgeId, edgeId);
    }

    @Test
    public void testGet() throws IOException {
        String outVId = getVertexId("person", "name", "peter");
        String inVId = getVertexId("software", "name", "lop");

        String edge = String.format("{" +
                                    "\"label\": \"created\"," +
                                    "\"outVLabel\": \"person\"," +
                                    "\"inVLabel\": \"software\"," +
                                    "\"outV\": \"%s\"," +
                                    "\"inV\": \"%s\"," +
                                    "\"properties\":{" +
                                    "\"date\": \"20170324\"," +
                                    "\"weight\": 0.5}" +
                                    "}", outVId, inVId);
        Response r = client().post(PATH, edge);
        String content = assertResponseStatus(201, r);

        String id = parseId(content);
        r = client().get(PATH, id);
        assertResponseStatus(200, r);
    }

    @Test
    public void testList() throws IOException {
        String outVId = getVertexId("person", "name", "peter");
        String inVId = getVertexId("software", "name", "lop");

        String edge = String.format("{" +
                                    "\"label\": \"created\"," +
                                    "\"outVLabel\": \"person\"," +
                                    "\"inVLabel\": \"software\"," +
                                    "\"outV\": \"%s\"," +
                                    "\"inV\": \"%s\"," +
                                    "\"properties\":{" +
                                    "\"date\": \"20170324\"," +
                                    "\"weight\": 0.5}" +
                                    "}", outVId, inVId);
        Response r = client().post(PATH, edge);
        assertResponseStatus(201, r);

        r = client().get(PATH);
        assertResponseStatus(200, r);
    }

    @Test
    public void testDelete() throws IOException {
        String outVId = getVertexId("person", "name", "peter");
        String inVId = getVertexId("software", "name", "lop");

        String edge = String.format("{" +
                                    "\"label\": \"created\"," +
                                    "\"outVLabel\": \"person\"," +
                                    "\"inVLabel\": \"software\"," +
                                    "\"outV\": \"%s\"," +
                                    "\"inV\": \"%s\"," +
                                    "\"properties\":{" +
                                    "\"date\": \"20170324\"," +
                                    "\"weight\": 0.5}" +
                                    "}", outVId, inVId);
        Response r = client().post(PATH, edge);
        String content = assertResponseStatus(201, r);

        String id = parseId(content);
        r = client().delete(PATH, id);
        assertResponseStatus(204, r);
    }
}
