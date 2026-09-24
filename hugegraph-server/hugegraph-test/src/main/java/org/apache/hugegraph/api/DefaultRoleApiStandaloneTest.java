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

import com.google.common.collect.ImmutableMap;

import jakarta.ws.rs.core.Response;

/**
 * Tests for the Hubble 2.0 default-role APIs added to GraphSpaceAPI and ManagerAPI
 * in standalone (RocksDB) mode.
 *
 * In standalone mode these endpoints should return HTTP 400 with a
 * friendly "not supported" error, because role/graphspace management
 * requires the PD/hstore backend.
 *
 * Covered endpoints:
 *  POST   /graphspaces/{gs}/role
 *  GET    /graphspaces/{gs}/role?user=&role=
 *  DELETE /graphspaces/{gs}/role?user=&role=
 *  GET    /graphspaces/{gs}/auth/managers/default?role=
 */
public class
DefaultRoleApiStandaloneTest extends BaseApiTest {

    private static final String SPACE = "DEFAULT";

    private static String rolePath(String graphspace) {
        return String.format("graphspaces/%s/role", graphspace);
    }

    private static String managerDefaultPath(String graphspace) {
        return String.format("graphspaces/%s/auth/managers/default", graphspace);
    }

    @Before
    public void assumeStandalone() {
        assumeStandaloneMode();
    }

    // ------------------------------------------------------------------ //
    //  POST /graphspaces/{gs}/role
    // ------------------------------------------------------------------ //

    @Test
    public void testCreateAnalystRoleReturnsFriendlyError() {
        String body = "{\"user\":\"admin\",\"role\":\"ANALYST\"}";
        Response r = client().post(rolePath(SPACE), body);
        String content = assertResponseStatus(400, r);
        Assert.assertTrue("should contain standalone error message",
                          content.contains(STANDALONE_ERROR));
    }

    @Test
    public void testCreateObserverRoleReturnsFriendlyError() {
        // OBSERVER requires a 'graph' field
        String body = "{\"user\":\"admin\",\"role\":\"OBSERVER\",\"graph\":\"hugegraph\"}";
        Response r = client().post(rolePath(SPACE), body);
        String content = assertResponseStatus(400, r);
        Assert.assertTrue(content.contains(STANDALONE_ERROR));
    }

    @Test
    public void testCreateSpaceRoleReturnsFriendlyError() {
        String body = "{\"user\":\"admin\",\"role\":\"SPACE\"}";
        Response r = client().post(rolePath(SPACE), body);
        String content = assertResponseStatus(400, r);
        Assert.assertTrue(content.contains(STANDALONE_ERROR));
    }

    // ------------------------------------------------------------------ //
    //  GET /graphspaces/{gs}/role?user=&role=
    // ------------------------------------------------------------------ //

    @Test
    public void testCheckAnalystRoleReturnsFriendlyError() {
        Response r = client().get(rolePath(SPACE),
                                  ImmutableMap.of("user", "admin", "role", "ANALYST"));
        String content = assertResponseStatus(400, r);
        Assert.assertTrue(content.contains(STANDALONE_ERROR));
    }

    @Test
    public void testCheckObserverRoleReturnsFriendlyError() {
        Response r = client().get(rolePath(SPACE),
                                  ImmutableMap.of("user", "admin",
                                                  "role", "OBSERVER",
                                                  "graph", "hugegraph"));
        String content = assertResponseStatus(400, r);
        Assert.assertTrue(content.contains(STANDALONE_ERROR));
    }

    // ------------------------------------------------------------------ //
    //  DELETE /graphspaces/{gs}/role?user=&role=
    // ------------------------------------------------------------------ //

    @Test
    public void testDeleteAnalystRoleReturnsFriendlyError() {
        Response r = client().delete(rolePath(SPACE),
                                     ImmutableMap.of("user", "admin", "role", "ANALYST"));
        String content = assertResponseStatus(400, r);
        Assert.assertTrue(content.contains(STANDALONE_ERROR));
    }

    @Test
    public void testDeleteObserverRoleReturnsFriendlyError() {
        Response r = client().delete(rolePath(SPACE),
                                     ImmutableMap.of("user", "admin",
                                                     "role", "OBSERVER",
                                                     "graph", "hugegraph"));
        String content = assertResponseStatus(400, r);
        Assert.assertTrue(content.contains(STANDALONE_ERROR));
    }

    // ------------------------------------------------------------------ //
    //  GET /graphspaces/{gs}/auth/managers/default?role=
    // ------------------------------------------------------------------ //

    @Test
    public void testCheckDefaultAnalystRoleReturnsFriendlyError() {
        Response r = client().get(managerDefaultPath(SPACE),
                                  ImmutableMap.of("role", "ANALYST"));
        String content = assertResponseStatus(400, r);
        Assert.assertTrue(content.contains(STANDALONE_ERROR));
    }

    @Test
    public void testCheckDefaultObserverRoleReturnsFriendlyError() {
        Response r = client().get(managerDefaultPath(SPACE),
                                  ImmutableMap.of("role", "OBSERVER",
                                                  "graph", "hugegraph"));
        String content = assertResponseStatus(400, r);
        Assert.assertTrue(content.contains(STANDALONE_ERROR));
    }

    // ------------------------------------------------------------------ //
    //  Non-existent graphspace — should also return 400 in standalone
    // ------------------------------------------------------------------ //

    @Test
    public void testCreateRoleNonExistentSpaceReturnsFriendlyError() {
        String body = "{\"user\":\"admin\",\"role\":\"ANALYST\"}";
        Response r = client().post(rolePath("nonexistent"), body);
        String content = assertResponseStatus(400, r);
        Assert.assertTrue(content.contains(STANDALONE_ERROR));
    }

    @Test
    public void testCheckRoleNonExistentSpaceReturnsFriendlyError() {
        Response r = client().get(rolePath("nonexistent"),
                                  ImmutableMap.of("user", "admin", "role", "ANALYST"));
        String content = assertResponseStatus(400, r);
        Assert.assertTrue(content.contains(STANDALONE_ERROR));
    }

    @Test
    public void testDeleteRoleNonExistentSpaceReturnsFriendlyError() {
        Response r = client().delete(rolePath("nonexistent"),
                                     ImmutableMap.of("user", "admin", "role", "ANALYST"));
        String content = assertResponseStatus(400, r);
        Assert.assertTrue(content.contains(STANDALONE_ERROR));
    }
}
