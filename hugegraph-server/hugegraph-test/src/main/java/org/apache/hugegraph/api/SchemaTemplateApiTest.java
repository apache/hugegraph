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

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import jakarta.ws.rs.core.Response;

/**
 * Tests for Schema Template APIs in standalone (RocksDB) mode.
 *
 * SchemaTemplate management requires the PD/hstore backend (it relies on
 * the distributed meta manager). In standalone mode every endpoint must
 * return HTTP 400 with a friendly "not supported" error message, matching
 * the behaviour of other PD-only APIs (ManagerAPI, GraphSpaceAPI roles).
 *
 * Covered endpoints:
 *  GET    /graphspaces/{gs}/schematemplates
 *  GET    /graphspaces/{gs}/schematemplates/{name}
 *  POST   /graphspaces/{gs}/schematemplates
 *  PUT    /graphspaces/{gs}/schematemplates/{name}
 *  DELETE /graphspaces/{gs}/schematemplates/{name}
 */
public class SchemaTemplateApiTest extends BaseApiTest {

    private static final String PATH = "graphspaces/DEFAULT/schematemplates";
    private static final String TEMPLATE_NAME = "st_test_template";
    private static final String TEMPLATE_SCHEMA =
            "schema.propertyKey('name').asText().ifNotExist().create();";

    @Before
    public void assumeStandalone() {
        assumeStandaloneMode();
    }

    // ------------------------------------------------------------------ //
    //  GET /graphspaces/{gs}/schematemplates
    // ------------------------------------------------------------------ //

    @Test
    public void testListSchemaTemplatesReturnsFriendlyError() {
        Response r = client().get(PATH);
        String content = assertResponseStatus(400, r);
        Assert.assertTrue(content.contains(STANDALONE_ERROR));
    }

    // ------------------------------------------------------------------ //
    //  GET /graphspaces/{gs}/schematemplates/{name}
    // ------------------------------------------------------------------ //

    @Test
    public void testGetSchemaTemplateReturnsFriendlyError() {
        Response r = client().get(PATH, TEMPLATE_NAME);
        String content = assertResponseStatus(400, r);
        Assert.assertTrue(content.contains(STANDALONE_ERROR));
    }

    // ------------------------------------------------------------------ //
    //  POST /graphspaces/{gs}/schematemplates
    // ------------------------------------------------------------------ //

    @Test
    public void testCreateSchemaTemplateReturnsFriendlyError() {
        String body = String.format("{\"name\":\"%s\",\"schema\":\"%s\"}",
                                   TEMPLATE_NAME, TEMPLATE_SCHEMA);
        Response r = client().post(PATH, body);
        String content = assertResponseStatus(400, r);
        Assert.assertTrue(content.contains(STANDALONE_ERROR));
    }

    // ------------------------------------------------------------------ //
    //  PUT /graphspaces/{gs}/schematemplates/{name}
    // ------------------------------------------------------------------ //

    @Test
    public void testUpdateSchemaTemplateReturnsFriendlyError() {
        String body = String.format("{\"schema\":\"%s\"}", TEMPLATE_SCHEMA);
        Response r = client().target(baseUrl())
                             .path(PATH + "/" + TEMPLATE_NAME)
                             .request()
                             .put(jakarta.ws.rs.client.Entity.json(body));
        String content = assertResponseStatus(400, r);
        Assert.assertTrue(content.contains(STANDALONE_ERROR));
    }

    // ------------------------------------------------------------------ //
    //  DELETE /graphspaces/{gs}/schematemplates/{name}
    // ------------------------------------------------------------------ //

    @Test
    public void testDeleteSchemaTemplateReturnsFriendlyError() {
        Response r = client().delete(PATH, TEMPLATE_NAME);
        String content = assertResponseStatus(400, r);
        Assert.assertTrue(content.contains(STANDALONE_ERROR));
    }

    // ------------------------------------------------------------------ //
    //  Non-existent graphspace — also returns 400 in standalone
    // ------------------------------------------------------------------ //

    @Test
    public void testListSchemaTemplatesNonExistentSpaceReturnsFriendlyError() {
        Response r = client().get("graphspaces/nonexistent/schematemplates");
        String content = assertResponseStatus(400, r);
        Assert.assertTrue(content.contains(STANDALONE_ERROR));
    }

    @Test
    public void testCreateSchemaTemplateNonExistentSpaceReturnsFriendlyError() {
        String body = String.format("{\"name\":\"%s\",\"schema\":\"%s\"}",
                                   TEMPLATE_NAME, TEMPLATE_SCHEMA);
        Response r = client().post("graphspaces/nonexistent/schematemplates", body);
        String content = assertResponseStatus(400, r);
        Assert.assertTrue(content.contains(STANDALONE_ERROR));
    }
}
