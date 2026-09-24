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

import org.apache.hugegraph.testutil.Assert;
import org.junit.Test;

import jakarta.ws.rs.core.Response;

public class PropertyKeyApiTest extends BaseApiTest {

    private static final String PATH =
            "/graphspaces/DEFAULT/graphs/hugegraph/schema/propertykeys/";

    @Test
    public void testCreate() {
        String propertyKey = "{" +
                             "\"name\": \"id\"," +
                             "\"data_type\": \"TEXT\"," +
                             "\"cardinality\": \"SINGLE\"," +
                             "\"properties\":[]" +
                             "}";
        Response r = client().post(PATH, propertyKey);
        assertResponseStatus(202, r);
    }

    @Test
    public void testGet() {
        String propertyKey = "{" +
                             "\"name\": \"id\"," +
                             "\"data_type\": \"TEXT\"," +
                             "\"cardinality\": \"SINGLE\"," +
                             "\"properties\":[]" +
                             "}";
        Response r = client().post(PATH, propertyKey);
        assertResponseStatus(202, r);

        String name = "id";
        r = client().get(PATH, name);
        assertResponseStatus(200, r);
    }

    @Test
    public void testList() {
        String propertyKey = "{" +
                             "\"name\": \"id\"," +
                             "\"data_type\": \"TEXT\"," +
                             "\"cardinality\": \"SINGLE\"," +
                             "\"properties\":[]" +
                             "}";
        Response r = client().post(PATH, propertyKey);
        assertResponseStatus(202, r);

        r = client().get(PATH);
        assertResponseStatus(200, r);
    }

    @Test
    public void testDelete() {
        String propertyKey = "{" +
                             "\"name\": \"id\"," +
                             "\"data_type\": \"TEXT\"," +
                             "\"cardinality\": \"SINGLE\"," +
                             "\"properties\":[]" +
                             "}";
        Response r = client().post(PATH, propertyKey);
        assertResponseStatus(202, r);

        String name = "id";
        r = client().delete(PATH, name);
        String content = assertResponseStatus(202, r);
        int task = assertJsonContains(content, "task_id");
        Assert.assertEquals(0, task);
    }
}
