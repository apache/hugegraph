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

import org.apache.hugegraph.auth.HugePermission;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import jakarta.ws.rs.core.Response;

/**
 * Tests that ManagerAPI returns a friendly HTTP 400 error in standalone mode
 * (i.e. when the server is started without PD / hstore backend).
 * <p>
 * This class intentionally does NOT have a class-level Assume guard so that
 * the tests are actually executed in non-hstore CI runs.
 */
public class ManagerApiStandaloneTest extends BaseApiTest {

    private static String managerPath(String graphSpace) {
        return String.format("graphspaces/%s/auth/managers", graphSpace);
    }

    @Before
    public void skipForPdMode() {
        assumeStandaloneMode();
    }

    @Test
    public void testCreateManagerReturnsFriendlyError() {
        String body = "{\"user\":\"admin\",\"type\":\"ADMIN\"}";
        Response r = this.client().post(managerPath("DEFAULT"), body);
        String content = assertResponseStatus(400, r);
        Assert.assertTrue(content.contains(STANDALONE_ERROR));
    }

    @Test
    public void testDeleteManagerReturnsFriendlyError() {
        Response r = this.client().delete(managerPath("DEFAULT"),
                                          Map.of("user", "admin",
                                                 "type", HugePermission.ADMIN));
        String content = assertResponseStatus(400, r);
        Assert.assertTrue(content.contains(STANDALONE_ERROR));
    }

    @Test
    public void testListManagerReturnsFriendlyError() {
        Response r = this.client().get(managerPath("DEFAULT"),
                                       Map.of("type", (Object)HugePermission.ADMIN));
        String content = assertResponseStatus(400, r);
        Assert.assertTrue(content.contains(STANDALONE_ERROR));
    }

    @Test
    public void testCheckRoleReturnsFriendlyError() {
        Response r = this.client().get(managerPath("DEFAULT") + "/check",
                                       Map.of("type", (Object)HugePermission.ADMIN));
        String content = assertResponseStatus(400, r);
        Assert.assertTrue(content.contains(STANDALONE_ERROR));
    }

    @Test
    public void testGetRolesInGsReturnsFriendlyError() {
        Response r = this.client().get(managerPath("DEFAULT") + "/role",
                                       Map.of("user", (Object)"admin"));
        String content = assertResponseStatus(400, r);
        Assert.assertTrue(content.contains(STANDALONE_ERROR));
    }

    @Test
    public void testCreateSpaceManagerReturnsFriendlyError() {
        String body = "{\"user\":\"admin\",\"type\":\"SPACE\"}";
        Response r = this.client().post(managerPath("nonexistent"), body);
        String content = assertResponseStatus(400, r);
        Assert.assertTrue(content.contains(STANDALONE_ERROR));
    }

    @Test
    public void testDeleteSpaceManagerReturnsFriendlyError() {
        Response r = this.client().delete(managerPath("nonexistent"),
                                          Map.of("user", "admin",
                                                 "type", HugePermission.SPACE));
        String content = assertResponseStatus(400, r);
        Assert.assertTrue(content.contains(STANDALONE_ERROR));
    }

    @Test
    public void testListSpaceManagerReturnsFriendlyError() {
        Response r = this.client().get(managerPath("nonexistent"),
                                       Map.of("type", (Object) HugePermission.SPACE));
        String content = assertResponseStatus(400, r);
        Assert.assertTrue(content.contains(STANDALONE_ERROR));
    }

    @Test
    public void testCheckRoleSpaceReturnsFriendlyError() {
        Response r = this.client().get(managerPath("nonexistent") + "/check",
                                       Map.of("type", (Object) HugePermission.SPACE));
        String content = assertResponseStatus(400, r);
        Assert.assertTrue(content.contains(STANDALONE_ERROR));
    }

    @Test
    public void testGetRolesInGsNonExistentReturnsFriendlyError() {
        Response r = this.client().get(managerPath("nonexistent") + "/role",
                                       Map.of("user", (Object) "admin"));
        String content = assertResponseStatus(400, r);
        Assert.assertTrue(content.contains(STANDALONE_ERROR));
    }
}
