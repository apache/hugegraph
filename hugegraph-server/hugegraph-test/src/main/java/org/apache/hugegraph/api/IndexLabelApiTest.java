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

public class IndexLabelApiTest extends BaseApiTest {

    private static final String PATH =
            "/graphspaces/DEFAULT/graphs/hugegraph/schema/indexlabels/";

    @Before
    public void prepareSchema() {
        initPropertyKey();
        initVertexLabel();
        initEdgeLabel();
    }

    @Test
    public void testCreate() {
        String indexLabel = "{" +
                            "\"name\": \"personByAge\"," +
                            "\"base_type\": \"VERTEX_LABEL\"," +
                            "\"base_value\": \"person\"," +
                            "\"index_type\": \"RANGE\"," +
                            "\"fields\":[\"age\"]" +
                            "}";
        Response r = client().post(PATH, indexLabel);
        assertResponseStatus(202, r);
    }

    @Test
    public void testAppend() {
        String indexLabel = "{" +
                            "\"name\": \"personByAge\"," +
                            "\"base_type\": \"VERTEX_LABEL\"," +
                            "\"base_value\": \"person\"," +
                            "\"index_type\": \"RANGE\"," +
                            "\"fields\":[\"age\"]" +
                            "}";

        Response r = client().post(PATH, indexLabel);
        assertResponseStatus(202, r);

        indexLabel = "{" +
                     "\"name\": \"personByAge\"," +
                     "\"user_data\": {" +
                     "\"min\": 0," +
                     "\"max\": 100" +
                     "}" +
                     "}";
        Map<String, Object> params = ImmutableMap.of("action", "append");
        r = client().put(PATH, "personByAge", indexLabel, params);
        assertResponseStatus(200, r);
    }

    @Test
    public void testEliminate() {
        String indexLabel = "{" +
                            "\"name\": \"personByAge\"," +
                            "\"base_type\": \"VERTEX_LABEL\"," +
                            "\"base_value\": \"person\"," +
                            "\"index_type\": \"RANGE\"," +
                            "\"fields\":[\"age\"]," +
                            "\"user_data\": {" +
                            "\"min\": 0," +
                            "\"max\": 100" +
                            "}" +
                            "}";
        Response r = client().post(PATH, indexLabel);
        assertResponseStatus(202, r);

        indexLabel = "{" +
                     "\"name\": \"personByAge\"," +
                     "\"user_data\": {" +
                     "\"min\": 0" +
                     "}" +
                     "}";
        Map<String, Object> params = ImmutableMap.of("action", "eliminate");
        r = client().put(PATH, "personByAge", indexLabel, params);
        assertResponseStatus(200, r);
    }

    @Test
    public void testGet() {
        String indexLabel = "{" +
                            "\"name\": \"personByAge\"," +
                            "\"base_type\": \"VERTEX_LABEL\"," +
                            "\"base_value\": \"person\"," +
                            "\"index_type\": \"RANGE\"," +
                            "\"fields\":[\"age\"]" +
                            "}";
        Response r = client().post(PATH, indexLabel);
        assertResponseStatus(202, r);

        String name = "personByAge";
        r = client().get(PATH, name);
        assertResponseStatus(200, r);
    }

    @Test
    public void testList() {
        String indexLabel = "{" +
                            "\"name\": \"personByAge\"," +
                            "\"base_type\": \"VERTEX_LABEL\"," +
                            "\"base_value\": \"person\"," +
                            "\"index_type\": \"RANGE\"," +
                            "\"fields\":[\"age\"]" +
                            "}";
        Response r = client().post(PATH, indexLabel);
        assertResponseStatus(202, r);

        r = client().get(PATH);
        assertResponseStatus(200, r);
    }

    @Test
    public void testDelete() {
        String indexLabel = "{" +
                            "\"name\": \"personByAge\"," +
                            "\"base_type\": \"VERTEX_LABEL\"," +
                            "\"base_value\": \"person\"," +
                            "\"index_type\": \"RANGE\"," +
                            "\"fields\":[\"age\"]" +
                            "}";
        Response r = client().post(PATH, indexLabel);
        assertResponseStatus(202, r);

        String name = "personByAge";
        r = client().delete(PATH, name);
        String content = assertResponseStatus(202, r);
        int task = assertJsonContains(content, "task_id");
        waitTaskSuccess(task);
    }
}
