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

import org.junit.Before;
import org.junit.Test;

import jakarta.ws.rs.core.Response;

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
}
