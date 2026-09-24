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

import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

import jakarta.ws.rs.core.Response;

public class VertexLabelApiTest extends BaseApiTest {

    private static final String PATH =
            "/graphspaces/DEFAULT/graphs/hugegraph/schema/vertexlabels/";

    @Before
    public void prepareSchema() {
        initPropertyKey();
    }

    @Test
    public void testCreate() {
        String vertexLabel = "{" +
                             "\"name\": \"person\"," +
                             "\"id_strategy\": \"PRIMARY_KEY\"," +
                             "\"properties\": [\"name\", \"age\", \"city\"]," +
                             "\"primary_keys\":[\"name\"]," +
                             "\"nullable_keys\":[\"city\"]" +
                             "}";
        Response r = client().post(PATH, vertexLabel);
        assertResponseStatus(201, r);
    }

    @Test
    public void testAppend() {
        String vertexLabel = "{" +
                             "\"name\": \"person\"," +
                             "\"id_strategy\": \"PRIMARY_KEY\"," +
                             "\"properties\": [\"name\", \"age\", \"city\"]," +
                             "\"primary_keys\":[\"name\"]," +
                             "\"nullable_keys\":[\"city\"]" +
                             "}";
        Response r = client().post(PATH, vertexLabel);
        assertResponseStatus(201, r);

        vertexLabel = "{" +
                      "\"name\": \"person\"," +
                      "\"id_strategy\":\"DEFAULT\"," +
                      "\"properties\":[\"lang\"]," +
                      "\"primary_keys\":[]," +
                      "\"nullable_keys\":[\"lang\"]" +
                      "}";
        Map<String, Object> params = ImmutableMap.of("action", "append");
        r = client().put(PATH, "person", vertexLabel, params);
        assertResponseStatus(200, r);
    }

    @Test
    public void testGet() {
        String vertexLabel = "{" +
                             "\"name\": \"person\"," +
                             "\"id_strategy\": \"PRIMARY_KEY\"," +
                             "\"properties\": [\"name\", \"age\", \"city\"]," +
                             "\"primary_keys\":[\"name\"]," +
                             "\"nullable_keys\":[\"city\"]" +
                             "}";
        Response r = client().post(PATH, vertexLabel);
        assertResponseStatus(201, r);

        String name = "person";
        r = client().get(PATH, name);
        assertResponseStatus(200, r);
    }

    @Test
    public void testList() {
        String vertexLabel = "{" +
                             "\"name\": \"person\"," +
                             "\"id_strategy\": \"PRIMARY_KEY\"," +
                             "\"properties\": [\"name\", \"age\", \"city\"]," +
                             "\"primary_keys\":[\"name\"]," +
                             "\"nullable_keys\":[\"city\"]" +
                             "}";
        Response r = client().post(PATH, vertexLabel);
        assertResponseStatus(201, r);

        r = client().get(PATH);
        assertResponseStatus(200, r);
    }

    @Test
    public void testDelete() {
        String vertexLabel = "{" +
                             "\"name\": \"person\"," +
                             "\"id_strategy\": \"PRIMARY_KEY\"," +
                             "\"properties\": [\"name\", \"age\", \"city\"]," +
                             "\"primary_keys\":[\"name\"]," +
                             "\"nullable_keys\":[\"city\"]" +
                             "}";
        Response r = client().post(PATH, vertexLabel);
        assertResponseStatus(201, r);

        String name = "person";
        r = client().delete(PATH, name);
        String content = assertResponseStatus(202, r);
        int task = assertJsonContains(content, "task_id");
        waitTaskSuccess(task);
    }
}
