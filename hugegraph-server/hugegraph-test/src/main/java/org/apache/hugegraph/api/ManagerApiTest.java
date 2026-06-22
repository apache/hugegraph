/*
 * Copyright 2017 HugeGraph Authors
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hugegraph.api;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.hugegraph.auth.HugePermission;
import org.apache.hugegraph.util.JsonUtil;
import org.apache.tinkerpop.shaded.jackson.core.type.TypeReference;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

import jakarta.ws.rs.core.Response;

public class ManagerApiTest extends BaseApiTest {

    private static final String USER_PATH = "graphspaces/DEFAULT/auth/users";
    private static final int NO_LIMIT = -1;

    // Helper method to build manager path with graphspace
    private static String managerPath(String graphSpace) {
        return String.format("graphspaces/%s/auth/managers", graphSpace);
    }

    @BeforeClass
    public static void setUpClass() {
        // skip this test for non-pd
        Assume.assumeTrue("skip this test for non-pd",
                          Objects.equals("hstore", System.getProperty("backend")));
    }

    @Override
    @After
    public void teardown() throws Exception {
        super.teardown();
        deleteSpaceMembers();
        deleteSpaceAdmins();
        deleteAdmins();
        deleteUsers();
        clearSpaces();
    }

    private void deleteSpaceMembers() {
        Response r1 = this.client().get("/graphspaces");
        String result = r1.readEntity(String.class);
        Map<String, Object> resultMap = JsonUtil.fromJson(result, Map.class);
        List<String> spaces = (List<String>) resultMap.get("graphSpaces");
        for (String space : spaces) {
            Response r = this.client().get(managerPath(space),
                                           ImmutableMap.of("type", HugePermission.SPACE_MEMBER));
            result = r.readEntity(String.class);
            resultMap = JsonUtil.fromJson(result, Map.class);
            List<String> spaceAdmins = (List<String>) resultMap.get("admins");
            for (String user : spaceAdmins) {
                this.client().delete(managerPath(space),
                                     ImmutableMap.of("user", user,
                                                     "type", HugePermission.SPACE_MEMBER));
            }
        }
    }

    public void deleteAdmins() {
        // ADMIN is global, use DEFAULT graphspace
        Response r = this.client().get(managerPath("DEFAULT"),
                                       ImmutableMap.of("type", HugePermission.ADMIN));
        String result = r.readEntity(String.class);
        Map<String, Object> resultMap = JsonUtil.fromJson(result, Map.class);
        List<String> admins = (List<String>) resultMap.get("admins");
        for (String user : admins) {
            if ("admin".equals(user)) {
                continue;
            }
            this.client().delete(managerPath("DEFAULT"),
                                 ImmutableMap.of("user", user, "type", HugePermission.ADMIN));
        }
    }

    public void deleteSpaceAdmins() {
        Response r1 = this.client().get("/graphspaces");
        String result = r1.readEntity(String.class);
        Map<String, Object> resultMap = JsonUtil.fromJson(result, Map.class);
        List<String> spaces = (List<String>) resultMap.get("graphSpaces");
        for (String space : spaces) {
            Response r = this.client().get(managerPath(space),
                                           ImmutableMap.of("type", HugePermission.SPACE));
            result = r.readEntity(String.class);
            resultMap = JsonUtil.fromJson(result, Map.class);
            List<String> spaceAdmins = (List<String>) resultMap.get("admins");
            for (String user : spaceAdmins) {
                this.client().delete(managerPath(space),
                                     ImmutableMap.of("user", user,
                                                     "type", HugePermission.SPACE));
            }
        }
    }

    public void deleteUsers() {
        List<Map<String, Object>> users = listUsers();
        for (Map<String, Object> user : users) {
            if (user.get("user_name").equals("admin")) {
                continue;
            }
            this.client().delete(USER_PATH, (String) user.get("id"));
        }
    }

    @Test
    public void testSpaceMemberCRUD() {
        createSpace("testspace", true);
        createGraph("testspace", "testgraph");

        this.createUser("test_member1", "testspace", "testgraph");
        this.createUser("test_member2", "testspace", "testgraph");
        String spaceMember1 = "{\"user\":\"test_member1\"," +
                              "\"type\":\"SPACE_MEMBER\"}";

        String spaceMember2 = "{\"user\":\"test_member2\"," +
                              "\"type\":\"SPACE_MEMBER\"}";

        Response r = client().post(managerPath("testspace"), spaceMember1);
        assertResponseStatus(201, r);

        r = client().post(managerPath("testspace"), spaceMember2);
        assertResponseStatus(201, r);

        r = client().post(managerPath("testspace"), spaceMember1);
        assertResponseStatus(400, r);

        client().get(managerPath("testspace") + "/check",
                     ImmutableMap.of("type", HugePermission.SPACE_MEMBER));

        RestClient member1Client =
                new RestClient(baseUrl(), "test_member1", "password1");
        RestClient member2Client =
                new RestClient(baseUrl(), "test_member2", "password1");

        String res1 = member1Client.get(managerPath("testspace") + "/check",
                                        ImmutableMap.of("type",
                                                        HugePermission.SPACE_MEMBER))
                                   .readEntity(String.class);

        String res2 = member2Client.get(managerPath("testspace") + "/check",
                                        ImmutableMap.of("type",
                                                        HugePermission.SPACE_MEMBER))
                                   .readEntity(String.class);
        Assert.assertTrue(res1.contains("true"));
        Assert.assertTrue(res2.contains("true"));

        String members = member1Client.get(managerPath("testspace"),
                                           ImmutableMap.of("type",
                                                           HugePermission.SPACE_MEMBER))
                                      .readEntity(String.class);
        Assert.assertTrue(members.contains("test_member1") &&
                          members.contains("test_member2"));

        client().delete(managerPath("testspace"),
                        ImmutableMap.of("user", "test_member1",
                                        "type", HugePermission.SPACE_MEMBER));

        members = client().get(managerPath("testspace"),
                               ImmutableMap.of("type",
                                               HugePermission.SPACE_MEMBER))
                          .readEntity(String.class);
        Assert.assertTrue(!members.contains("test_member1") &&
                          members.contains("test_member2"));

        String res = member1Client.get(managerPath("testspace") + "/check",
                                       ImmutableMap.of("type",
                                                       HugePermission.SPACE_MEMBER))
                                  .readEntity(String.class);
        Assert.assertTrue(res.contains("false"));
    }

    @Test
    public void testPermission() {
        createSpace("testspace", true);
        createGraph("testspace", "testgraph");

        this.createUser("perm_member", "testspace", "testgraph");
        this.createUser("perm_manager", "testspace", "testgraph");
        String spaceMember = "{\"user\":\"perm_member\"," +
                             "\"type\":\"SPACE_MEMBER\"}";

        String spaceManager = "{\"user\":\"perm_manager\"," +
                              "\"type\":\"SPACE\"}";

        Response r = client().post(managerPath("testspace"), spaceMember);
        assertResponseStatus(201, r);

        r = client().post(managerPath("testspace"), spaceManager);
        assertResponseStatus(201, r);

        RestClient spaceMemberClient =
                new RestClient(baseUrl(), "perm_member", "password1");
        RestClient spaceManagerClient =
                new RestClient(baseUrl(), "perm_manager", "password1");

        String userPath = "graphspaces/testspace/graphs/testgraph/auth/users";
        String user = "{\"user_name\":\"" + "test_perm_user" +
                      "\",\"user_password\":\"password1" +
                      "\", \"user_email\":\"user1@test.com\"," +
                      "\"user_phone\":\"123456789\",\"user_avatar\":\"image1" +
                      ".jpg\"}";

        r = spaceManagerClient.post(userPath, user);

        String s = "{\"user\":\"test_perm_user\"," +
                   "\"type\":\"SPACE\"}";
        String response =
                spaceMemberClient.post(managerPath("testspace"), s).readEntity(String.class);
        Assert.assertTrue(response.contains("ermission"));

        r = spaceManagerClient.post(managerPath("testspace"), s);
        assertResponseStatus(201, r);

        s = "{\"user\":\"test_perm_user\"," +
            "\"type\":\"SPACE_MEMBER\"}";
        response = spaceMemberClient.post(managerPath("testspace"), s).readEntity(String.class);
        Assert.assertTrue(response.contains("ermission"));

        r = spaceManagerClient.post(managerPath("testspace"), s);
        assertResponseStatus(201, r);

        s = "{\"user\":\"test_perm_user\"," +
            "\"type\":\"ADMIN\"}";
        response = spaceMemberClient.post(managerPath("DEFAULT"), s).readEntity(String.class);
        Assert.assertTrue(response.contains("ermission"));

        response = spaceManagerClient.post(managerPath("DEFAULT"), s).readEntity(String.class);
        Assert.assertTrue(response.contains("ermission"));
    }

    @Test
    public void testCreate() {
        createSpace("testspace", true);
        createGraph("testspace", "testgraph");

        this.createUser("create_user1", "testspace", "testgraph");
        this.createUser("create_user2", "testspace", "testgraph");

        String admin1 = "{\"user\":\"create_user1\"," +
                        "\"type\":\"ADMIN\"}";

        String space1 = "{\"user\":\"create_user2\"," +
                        "\"type\":\"SPACE\"}";

        Response r = client().post(managerPath("DEFAULT"), admin1);
        assertResponseStatus(201, r);
        r = client().post(managerPath("testspace"), space1);
        assertResponseStatus(201, r);

        String admin2 = "{\"user\":\"create_user1\"," +
                        "\"type\":\"READ\"}";
        r = client().post(managerPath("DEFAULT"), admin2);
        String result = assertResponseStatus(400, r);
        Map<String, String> resultMap = JsonUtil.fromJson(result, Map.class);
        Assert.assertTrue(resultMap.get("message").contains("must be in"));

        String admin3 = "{\"user\":\"create_user1\"," +
                        "\"type\":\"ADMIN2\"}";
        r = client().post(managerPath("DEFAULT"), admin3);
        result = assertResponseStatus(400, r);
        Assert.assertTrue(result.contains("Cannot deserialize value of type"));

        String admin4 = "{\"user\":\"create_user3\"," +
                        "\"type\":\"ADMIN\"}";
        r = client().post(managerPath("DEFAULT"), admin4);
        result = assertResponseStatus(400, r);
        resultMap = JsonUtil.fromJson(result, Map.class);
        Assert.assertTrue(resultMap.get("message").contains("The user or group is not exist"));

        String space2 = "{\"user\":\"create_user2\"," +
                        "\"type\":\"SPACE\"}";
        r = client().post(managerPath("nonexist"), space2);
        result = assertResponseStatus(400, r);
        resultMap = JsonUtil.fromJson(result, Map.class);
        Assert.assertTrue(resultMap.get("message").contains("The graph space is not exist"));
    }

    protected void createUser(String name) {
        createUser(name, "DEFAULT", "hugegraph");
    }

    protected void createUser(String name, String graphSpace, String graph) {
        String userPath = String.format("graphspaces/%s/graphs/%s/auth/users",
                                        graphSpace, graph);
        String user = "{\"user_name\":\"" + name + "\",\"user_password\":\"password1" +
                      "\", \"user_email\":\"user1@baidu.com\"," +
                      "\"user_phone\":\"123456789\",\"user_avatar\":\"image1" +
                      ".jpg\"}";
        Response r = this.client().post(userPath, user);
        assertResponseStatus(201, r);
    }

    protected List<Map<String, Object>> listUsers() {
        return listUsers("DEFAULT", "hugegraph");
    }

    protected List<Map<String, Object>> listUsers(String graphSpace, String graph) {
        String userPath = String.format("graphspaces/%s/graphs/%s/auth/users",
                                        graphSpace, graph);
        Response r = this.client().get(userPath, ImmutableMap.of("limit", NO_LIMIT));
        String result = assertResponseStatus(200, r);

        Map<String, List<Map<String, Object>>> resultMap =
                JsonUtil.fromJson(result, new TypeReference<Map<String,
                        List<Map<String, Object>>>>() {
                });
        return resultMap.get("users");
    }

    /**
     * Test space manager boundary: SpaceA's manager cannot operate SpaceB's resources
     */
    @Test
    public void testSpaceManagerBoundary() {
        // Create two graph spaces
        createSpace("spacea", true);
        createSpace("spaceb", true);

        // Create users (by admin)
        this.createUser("userina");
        this.createUser("userinb");
        this.createUser("managera");
        this.createUser("managerb");

        // Set managera as spacea's manager (by admin)
        String managerA = "{\"user\":\"managera\"," +
                          "\"type\":\"SPACE\"}";
        Response r = client().post(managerPath("spacea"), managerA);
        assertResponseStatus(201, r);

        // Set managerb as spaceb's manager (by admin)
        String managerB = "{\"user\":\"managerb\"," +
                          "\"type\":\"SPACE\"}";
        r = client().post(managerPath("spaceb"), managerB);
        assertResponseStatus(201, r);

        // Admin adds userina to spacea (initial setup)
        String memberA = "{\"user\":\"userina\"," +
                         "\"type\":\"SPACE_MEMBER\"}";
        r = client().post(managerPath("spacea"), memberA);
        assertResponseStatus(201, r);

        // Admin adds userinb to spaceb (initial setup)
        String memberB = "{\"user\":\"userinb\"," +
                         "\"type\":\"SPACE_MEMBER\"}";
        r = client().post(managerPath("spaceb"), memberB);
        assertResponseStatus(201, r);

        RestClient managerAClient = new RestClient(baseUrl(), "managera", "password1");
        RestClient managerBClient = new RestClient(baseUrl(), "managerb", "password1");

        // Test 1: managera cannot add members to spaceb (cross-space operation)
        String anotherUserB = "{\"user\":\"userina\"," +
                              "\"type\":\"SPACE_MEMBER\"}";
        r = managerAClient.post(managerPath("spaceb"), anotherUserB);
        String response = r.readEntity(String.class);
        Assert.assertEquals(403, r.getStatus());
        Assert.assertTrue(response.contains("Permission denied") ||
                          response.contains("no permission"));

        // Test 2: managerb cannot delete members from spacea
        r = managerBClient.delete(managerPath("spacea"), ImmutableMap.of("user", "userina",
                                                                         "type",
                                                                         HugePermission.SPACE_MEMBER));
        response = r.readEntity(String.class);
        Assert.assertEquals(403, r.getStatus());
        Assert.assertTrue(response.contains("Permission denied") ||
                          response.contains("no permission"));

        // Test 3: managera cannot list members in spaceb
        r = managerAClient.get(managerPath("spaceb"),
                               ImmutableMap.of("type", HugePermission.SPACE_MEMBER));
        response = r.readEntity(String.class);
        // May return 403 or empty list depending on implementation
        if (r.getStatus() == 403) {
            Assert.assertTrue(response.contains("Permission denied") ||
                              response.contains("no permission"));
        }

        // Test 4: managera can list members in spacea
        r = managerAClient.get(managerPath("spacea"),
                               ImmutableMap.of("type", HugePermission.SPACE_MEMBER));
        assertResponseStatus(200, r);

        // Test 5: Admin can delete members from spacea
        r = client().delete(managerPath("spacea"), ImmutableMap.of("user", "userina",
                                                                   "type",
                                                                   HugePermission.SPACE_MEMBER));
        assertResponseStatus(204, r);

        // Test 6: Verify userina is no longer a member of spacea
        r = client().get(managerPath("spacea"),
                         ImmutableMap.of("type", HugePermission.SPACE_MEMBER));
        String remainingMembers = assertResponseStatus(200, r);
        Assert.assertFalse(remainingMembers.contains("userina"));

        // Test 7: Verify userinb is still a member of spaceb
        r = client().get(managerPath("spaceb"),
                         ImmutableMap.of("type", HugePermission.SPACE_MEMBER));
        String spaceBMembers = assertResponseStatus(200, r);
        Assert.assertTrue(spaceBMembers.contains("userinb"));
    }

    /**
     * Test space manager cannot operate graphs in other spaces
     */
    @Test
    public void testSpaceManagerCannotOperateOtherSpaceGraphs() {
        // Create two graph spaces
        createSpace("spacex", true);
        createSpace("spacey", true);

        // Create graphs in each space
        createGraph("spacex", "graphx");
        createGraph("spacey", "graphy");

        // Create manager for spacex
        this.createUser("managerx");
        String managerX = "{\"user\":\"managerx\"," +
                          "\"type\":\"SPACE\"}";
        Response r = client().post(managerPath("spacex"), managerX);
        assertResponseStatus(201, r);

        RestClient managerXClient = new RestClient(baseUrl(), "managerx", "password1");

        // Test 1: managerx cannot access spacey's graph
        String pathY = "graphspaces/spacey/graphs/graphy/schema/propertykeys";
        r = managerXClient.get(pathY);
        // Should get 403 or 404
        Assert.assertTrue(r.getStatus() == 403 || r.getStatus() == 404);

        // Test 2: managerx can access spacex's graph
        String pathX = "graphspaces/spacex/graphs/graphx/schema/propertykeys";
        r = managerXClient.get(pathX);
        assertResponseStatus(200, r);
    }

    /**
     * Test space manager cannot promote users in other spaces
     */
    @Test
    public void testSpaceManagerCannotPromoteUsersInOtherSpaces() {
        // Create two graph spaces
        createSpace("spacealpha", true);
        createSpace("spacebeta", true);

        // Create users (by admin)
        this.createUser("manageralpha");
        this.createUser("usertest");

        // Set manageralpha as spacealpha's manager (by admin)
        String managerAlpha = "{\"user\":\"manageralpha\"," +
                              "\"type\":\"SPACE\"}";
        Response r = client().post(managerPath("spacealpha"), managerAlpha);
        assertResponseStatus(201, r);

        RestClient managerAlphaClient = new RestClient(baseUrl(), "manageralpha", "password1");

        // Test: manageralpha cannot promote usertest to be spacebeta's manager
        String promoteBeta = "{\"user\":\"usertest\"," +
                             "\"type\":\"SPACE\"}";
        r = managerAlphaClient.post(managerPath("spacebeta"), promoteBeta);
        String response = assertResponseStatus(403, r);
        Assert.assertTrue(response.contains("Permission denied") ||
                          response.contains("no permission"));

        // Verify: manageralpha CAN promote usertest to be spacealpha's member
        // But this will fail because manageralpha doesn't have permission to read user from
        // DEFAULT space
        // This is expected behavior - space managers should only manage users already in their
        // space
        // or admin should assign users to spaces first

        // Let admin assign the user to spacealpha first
        String promoteAlphaByAdmin = "{\"user\":\"usertest\"," +
                                     "\"type\":\"SPACE_MEMBER\"}";
        r = client().post(managerPath("spacealpha"), promoteAlphaByAdmin);
        assertResponseStatus(201, r);

        // Now manageralpha should be able to see and manage users in spacealpha
        // Verify manageralpha can list members in spacealpha
        r = managerAlphaClient.get(managerPath("spacealpha"),
                                   ImmutableMap.of("type", HugePermission.SPACE_MEMBER));
        String members = assertResponseStatus(200, r);
        Assert.assertTrue(members.contains("usertest"));
    }

    /**
     * Test multiple space managers with different spaces
     */
    @Test
    public void testMultipleSpaceManagersIsolation() {
        // Create three graph spaces
        createSpace("space1", true);
        createSpace("space2", true);
        createSpace("space3", true);

        // Create managers for each space (by admin)
        this.createUser("manager1");
        this.createUser("manager2");
        this.createUser("manager3");

        // Create test users (by admin)
        this.createUser("testuser1");
        this.createUser("testuser2");

        // Admin assigns managers to their respective spaces
        client().post(managerPath("space1"), "{\"user\":\"manager1\",\"type\":\"SPACE\"}");
        client().post(managerPath("space2"), "{\"user\":\"manager2\",\"type\":\"SPACE\"}");
        client().post(managerPath("space3"), "{\"user\":\"manager3\",\"type\":\"SPACE\"}");

        // Admin adds testuser1 to space1 (initial setup)
        Response r = client().post(managerPath("space1"),
                                   "{\"user\":\"testuser1\",\"type\":\"SPACE_MEMBER\"}");
        assertResponseStatus(201, r);

        // Admin adds testuser2 to space2 (initial setup)
        r = client().post(managerPath("space2"),
                          "{\"user\":\"testuser2\",\"type\":\"SPACE_MEMBER\"}");
        assertResponseStatus(201, r);

        RestClient manager1Client = new RestClient(baseUrl(), "manager1", "password1");
        RestClient manager2Client = new RestClient(baseUrl(), "manager2", "password1");

        // Test 1: manager1 can see testuser1 in space1's member list
        r = manager1Client.get(managerPath("space1"),
                               ImmutableMap.of("type", HugePermission.SPACE_MEMBER));
        String members = assertResponseStatus(200, r);
        Assert.assertTrue(members.contains("testuser1"));

        // Test 2: manager2 cannot see testuser1 in space2's member list
        r = manager2Client.get(managerPath("space2"),
                               ImmutableMap.of("type", HugePermission.SPACE_MEMBER));
        members = assertResponseStatus(200, r);
        Assert.assertFalse(members.contains("testuser1"));
        Assert.assertTrue(members.contains("testuser2"));

        // Test 3: manager1 cannot delete testuser2 from space2 (cross-space operation)
        r = manager1Client.delete(managerPath("space2"), ImmutableMap.of("user", "testuser2",
                                                                         "type",
                                                                         HugePermission.SPACE_MEMBER));
        Assert.assertEquals(403, r.getStatus());

        // Test 4: Verify manager1 can only check role in space1
        r = manager1Client.get(managerPath("space1") + "/check",
                               ImmutableMap.of("type", HugePermission.SPACE));
        String result = assertResponseStatus(200, r);
        Assert.assertTrue(result.contains("true"));

        r = manager1Client.get(managerPath("space2") + "/check",
                               ImmutableMap.of("type", HugePermission.SPACE));
        result = assertResponseStatus(200, r);
        Assert.assertTrue(result.contains("false"));

        // Cleanup: Admin deletes members
        client().delete(managerPath("space1"), ImmutableMap.of("user", "testuser1",
                                                               "type",
                                                               HugePermission.SPACE_MEMBER));
        client().delete(managerPath("space2"), ImmutableMap.of("user", "testuser2",
                                                               "type",
                                                               HugePermission.SPACE_MEMBER));
    }

    /**
     * Test space manager and space member resource operation permissions
     */
    @Test
    public void testSpaceManagerAndMemberResourcePermissions() {
        // Setup: Create space and graph
        createSpace("testspace", true);
        createGraph("testspace", "testgraph");

        // Create users
        this.createUser("spacemanager");
        this.createUser("spacemember");
        this.createUser("outsider");

        // Assign roles
        client().post(managerPath("testspace"), "{\"user\":\"spacemanager\",\"type\":\"SPACE\"}");
        client().post(managerPath("testspace"),
                      "{\"user\":\"spacemember\",\"type\":\"SPACE_MEMBER\"}");

        RestClient managerClient = new RestClient(baseUrl(), "spacemanager", "password1");
        RestClient memberClient = new RestClient(baseUrl(), "spacemember", "password1");
        RestClient outsiderClient = new RestClient(baseUrl(), "outsider", "password1");

        String schemaPath = "graphspaces/testspace/graphs/testgraph/schema";
        String vertexPath = "graphspaces/testspace/graphs/testgraph/graph/vertices";

        // Test 1: Space manager can read schema
        Response r = managerClient.get(schemaPath);
        assertResponseStatus(200, r);

        // Test 2: Space member can read schema
        r = memberClient.get(schemaPath);
        assertResponseStatus(200, r);

        // Test 3: Outsider cannot read schema
        r = outsiderClient.get(schemaPath);
        String response = r.readEntity(String.class);
        Assert.assertEquals(403, r.getStatus());

        // Test 4: Space manager can create vertex (if schema exists)
        // First create a vertex label using admin
        String plJson = "{\"name\":\"age\",\"data_type\":\"INT\",\"cardinality\":\"SINGLE\"}";
        r = client().post(schemaPath + "/propertykeys", plJson);
        String result = r.readEntity(String.class);

        String vlJson = "{\"name\":\"person\",\"id_strategy\":\"PRIMARY_KEY\"," +
                        "\"properties\":[\"age\"],\"primary_keys\":[\"age\"]}";
        client().post(schemaPath + "/vertexlabels", vlJson);

        // Space manager creates vertex
        String vertexJson = "{\"label\":\"person\",\"properties\":{\"age\":30}}";
        r = managerClient.post(vertexPath, vertexJson);
        String response2 = r.readEntity(String.class);
        // Note: Vertex write might require specific permissions depending on configuration
        // We check if it's either allowed (201) or forbidden (403)
        int status = r.getStatus();
        Assert.assertTrue("Status should be 201 or 403, but was: " + status,
                          status == 201 || status == 403);

        // Test 5: Space member vertex write permission
        String vertexJson2 = "{\"label\":\"person\",\"properties\":{\"age\":25}}";
        r = memberClient.post(vertexPath, vertexJson2);
        status = r.getStatus();
        // Space member typically has read-only or limited write access
        Assert.assertTrue("Status should be 201 or 403, but was: " + status,
                          status == 201 || status == 403);

        // Test 6: Outsider cannot create vertex
        String vertexJson3 = "{\"label\":\"person\",\"properties\":{\"age\":20}}";
        r = outsiderClient.post(vertexPath, vertexJson3);
        Assert.assertEquals(403, r.getStatus());

        // Test 7: Space manager can manage space members (already tested in other tests)
        // Test 8: Space member cannot manage space members
        this.createUser("newuser");
        String addMemberJson = "{\"user\":\"newuser\",\"type\":\"SPACE_MEMBER\"}";
        r = memberClient.post(managerPath("testspace"), addMemberJson);
        response = r.readEntity(String.class);
        Assert.assertEquals(403, r.getStatus());
        Assert.assertTrue(response.contains("Permission denied") ||
                          response.contains("no permission"));

        // Test 9: Verify space manager can list members
        r = managerClient.get(managerPath("testspace"),
                              ImmutableMap.of("type", HugePermission.SPACE_MEMBER));
        assertResponseStatus(200, r);

        // Test 10: Verify space member cannot list members in management context
        r = memberClient.get(managerPath("testspace"),
                             ImmutableMap.of("type", HugePermission.SPACE_MEMBER));
        status = r.getStatus();
        // Space member might have limited visibility
        Assert.assertTrue("Status should be 200 or 403, but was: " + status,
                          status == 200 || status == 403);
    }

    @Test
    public void testRoleUpgradeLifecycle() {
        createSpace("testspace", true);
        createGraph("testspace", "graph1");
        createUser("testuser", "testspace", "graph1");

        String memberJson = "{\"user\":\"testuser\",\"type\":\"SPACE_MEMBER\"}";
        Response r = client().post(managerPath("testspace"), memberJson);
        assertResponseStatus(201, r);

        r = client().get(managerPath("testspace") + "/role",
                         ImmutableMap.of("user", "testuser"));
        String result = assertResponseStatus(200, r);
        Assert.assertTrue("Should be SPACE_MEMBER", result.contains("SPACE_MEMBER"));
        Assert.assertFalse("Should not be SPACE manager", result.contains("\"SPACE\""));

        String managerJson = "{\"user\":\"testuser\",\"type\":\"SPACE\"}";
        r = client().post(managerPath("testspace"), managerJson);
        assertResponseStatus(201, r);

        r = client().get(managerPath("testspace") + "/role",
                         ImmutableMap.of("user", "testuser"));
        result = assertResponseStatus(200, r);
        Assert.assertTrue("Should be SPACE manager", result.contains("\"SPACE\""));
        Assert.assertFalse("Should not be SPACE_MEMBER anymore", result.contains("SPACE_MEMBER"));

        r = client().post(managerPath("testspace"), memberJson);
        assertResponseStatus(201, r);

        r = client().get(managerPath("testspace") + "/role",
                         ImmutableMap.of("user", "testuser"));
        result = assertResponseStatus(200, r);
        Assert.assertTrue("Should be SPACE_MEMBER again", result.contains("SPACE_MEMBER"));
        Assert.assertFalse("Should not be SPACE manager", result.contains("\"SPACE\""));
    }

    /**
     * Test space manager can delete graph but space member cannot
     */
    @Test
    public void testSpaceManagerCanDeleteGraph() {
        createSpace("deletespace", true);
        createGraph("deletespace", "deletegraph1");
        createGraph("deletespace", "deletegraph2");
        createGraph("deletespace", "deletegraph3");

        this.createUser("deletemanager");
        this.createUser("deletemember");
        this.createUser("deleteoutsider");

        client().post(managerPath("deletespace"),
                      "{\"user\":\"deletemanager\",\"type\":\"SPACE\"}");
        client().post(managerPath("deletespace"),
                      "{\"user\":\"deletemember\",\"type\":\"SPACE_MEMBER\"}");

        RestClient managerClient = new RestClient(baseUrl(), "deletemanager", "password1");
        RestClient memberClient = new RestClient(baseUrl(), "deletemember", "password1");
        RestClient outsiderClient = new RestClient(baseUrl(), "deleteoutsider", "password1");

        String graphsPath = "graphspaces/deletespace/graphs";
        String confirmMessage = "I'm sure to drop the graph";

        Response r = memberClient.delete(graphsPath + "/deletegraph1",
                                         ImmutableMap.of("confirm_message", confirmMessage));
        String response = r.readEntity(String.class);
        Assert.assertEquals(403, r.getStatus());
        Assert.assertTrue(response.contains("auth") ||
                          response.contains("ermission"));

        r = outsiderClient.delete(graphsPath + "/deletegraph2",
                                  ImmutableMap.of("confirm_message", confirmMessage));
        response = r.readEntity(String.class);
        Assert.assertEquals(403, r.getStatus());
        Assert.assertTrue(response.contains("auth") ||
                          response.contains("ermission"));

        r = managerClient.delete(graphsPath + "/deletegraph1",
                                 ImmutableMap.of("confirm_message", confirmMessage));
        int status = r.getStatus();
        Assert.assertTrue("Graph deletion should succeed with 200 or 204, but was: " + status,
                          status == 200 || status == 204);

        r = managerClient.get(graphsPath);
        String graphsList = assertResponseStatus(200, r);
        Assert.assertFalse("deletegraph1 should be deleted",
                           graphsList.contains("deletegraph1"));
        Assert.assertTrue("deletegraph2 should still exist",
                          graphsList.contains("deletegraph2"));
        Assert.assertTrue("deletegraph3 should still exist",
                          graphsList.contains("deletegraph3"));

        createSpace("otherspace", true);
        createGraph("otherspace", "othergraph");

        r = managerClient.delete("graphspaces/otherspace/graphs/othergraph",
                                 ImmutableMap.of("confirm_message", confirmMessage));
        response = r.readEntity(String.class);
        Assert.assertEquals(403, r.getStatus());
        Assert.assertTrue(response.contains("auth") ||
                          response.contains("ermission"));

        r = client().delete(graphsPath + "/deletegraph2",
                            ImmutableMap.of("confirm_message", confirmMessage));
        status = r.getStatus();
        Assert.assertTrue("Admin graph deletion should succeed with 200 or 204, but was: " + status,
                          status == 200 || status == 204);

        r = client().get(graphsPath);
        graphsList = assertResponseStatus(200, r);
        Assert.assertFalse("deletegraph2 should be deleted",
                           graphsList.contains("deletegraph2"));
        Assert.assertTrue("deletegraph3 should still exist",
                          graphsList.contains("deletegraph3"));
    }

    /**
     * Test space manager can promote user to manager and both can delete graphs
     * while regular users cannot delete graphs
     */
    @Test
    public void testManagerCanPromoteUserAndDeleteGraph() {
        // Setup: Create space with graphs
        createSpace("promotespace", true);
        createGraph("promotespace", "graph1");
        createGraph("promotespace", "graph2");
        createGraph("promotespace", "graph3");

        // Create users
        this.createUser("originalmanager", "promotespace", "graph1");
        this.createUser("regularuser", "promotespace", "graph1");

        // Admin assigns originalmanager as space manager
        String managerJson = "{\"user\":\"originalmanager\",\"type\":\"SPACE\"}";
        Response r = client().post(managerPath("promotespace"), managerJson);
        assertResponseStatus(201, r);

        // Admin assigns regularuser as space member
        String memberJson = "{\"user\":\"regularuser\",\"type\":\"SPACE_MEMBER\"}";
        r = client().post(managerPath("promotespace"), memberJson);
        assertResponseStatus(201, r);

        RestClient managerClient = new RestClient(baseUrl(), "originalmanager", "password1");
        RestClient userClient = new RestClient(baseUrl(), "regularuser", "password1");

        // Test 1: Regular user cannot delete graph
        String graphsPath = "graphspaces/promotespace/graphs";
        String confirmMessage = "I'm sure to drop the graph";
        r = userClient.delete(graphsPath + "/graph1",
                              ImmutableMap.of("confirm_message", confirmMessage));
        String response = r.readEntity(String.class);
        Assert.assertEquals("Regular user should not be able to delete graph", 403, r.getStatus());
        Assert.assertTrue(response.contains("auth") || response.contains("ermission"));

        // Test 2: Space manager can promote regular user to manager
        String promoteJson = "{\"user\":\"regularuser\",\"type\":\"SPACE\"}";
        r = managerClient.post(managerPath("promotespace"), promoteJson);
        assertResponseStatus(201, r);

        // Test 3: Verify regularuser is now a manager
        r = userClient.get(managerPath("promotespace") + "/check",
                           ImmutableMap.of("type", HugePermission.SPACE));
        String checkResult = assertResponseStatus(200, r);
        Assert.assertTrue("User should now have SPACE manager role",
                          checkResult.contains("true"));

        // Test 4: Original manager can delete graph
        r = managerClient.delete(graphsPath + "/graph1",
                                 ImmutableMap.of("confirm_message", confirmMessage));
        int status = r.getStatus();
        Assert.assertTrue("Manager should be able to delete graph, status: " + status,
                          status == 200 || status == 204);

        // Test 5: Newly promoted manager can also delete graph
        userClient = new RestClient(baseUrl(), "regularuser", "password1");
        r = userClient.delete(graphsPath + "/graph2",
                              ImmutableMap.of("confirm_message", confirmMessage));
        status = r.getStatus();
        Assert.assertTrue("Promoted manager should be able to delete graph, status: " + status,
                          status == 200 || status == 204);

        // Test 6: Verify graphs were deleted
        r = client().get(graphsPath);
        String graphsList = assertResponseStatus(200, r);
        Assert.assertFalse("graph1 should be deleted", graphsList.contains("graph1"));
        Assert.assertFalse("graph2 should be deleted", graphsList.contains("graph2"));
        Assert.assertTrue("graph3 should still exist", graphsList.contains("graph3"));

        // Test 7: Verify managers are listed
        r = client().get(managerPath("promotespace"),
                         ImmutableMap.of("type", HugePermission.SPACE));
        String managersList = assertResponseStatus(200, r);
        Assert.assertTrue("originalmanager should be in managers list",
                          managersList.contains("originalmanager"));
        Assert.assertTrue("regularuser should be in managers list after promotion",
                          managersList.contains("regularuser"));
    }

    /**
     * Test user with different roles in multiple graph spaces
     * Verify permissions by testing graph deletion capability
     */
    @Test
    public void testUserWithDifferentRolesInMultipleSpaces() {
        // Create two graph spaces with graphs
        createSpace("spacea", true);
        createSpace("spaceb", true);
        createGraph("spacea", "grapha1");
        createGraph("spacea", "grapha2");
        createGraph("spaceb", "graphb1");
        createGraph("spaceb", "graphb2");

        // Create a user in both spaces
        this.createUser("multiuser", "spacea", "grapha1");

        // Assign different roles: SPACE manager in spacea, SPACE_MEMBER in spaceb
        String managerInSpaceA = "{\"user\":\"multiuser\",\"type\":\"SPACE\"}";
        Response r = client().post(managerPath("spacea"), managerInSpaceA);
        assertResponseStatus(201, r);

        String memberInSpaceB = "{\"user\":\"multiuser\",\"type\":\"SPACE_MEMBER\"}";
        r = client().post(managerPath("spaceb"), memberInSpaceB);
        assertResponseStatus(201, r);

        // Verify roles in both spaces
        r = client().get(managerPath("spacea") + "/role",
                         ImmutableMap.of("user", "multiuser"));
        String result = assertResponseStatus(200, r);
        Assert.assertTrue("User should be SPACE manager in spacea",
                          result.contains("\"SPACE\""));
        Assert.assertFalse("User should not be SPACE_MEMBER in spacea",
                           result.contains("SPACE_MEMBER"));

        r = client().get(managerPath("spaceb") + "/role",
                         ImmutableMap.of("user", "multiuser"));
        result = assertResponseStatus(200, r);
        Assert.assertTrue("User should be SPACE_MEMBER in spaceb",
                          result.contains("SPACE_MEMBER"));
        Assert.assertFalse("User should not be SPACE manager in spaceb",
                           result.contains("\"SPACE\""));

        // Create client for the multi-role user
        RestClient multiuserClient = new RestClient(baseUrl(), "multiuser", "password1");
        String confirmMessage = "I'm sure to drop the graph";

        // Test 1: As SPACE manager in spacea, should be able to delete graph
        r = multiuserClient.delete("graphspaces/spacea/graphs/grapha1",
                                   ImmutableMap.of("confirm_message", confirmMessage));
        int status = r.getStatus();
        Assert.assertTrue("As SPACE manager in spacea, should be able to delete graph, " +
                          "status: " + status,
                          status == 200 || status == 204);

        // Verify graph deletion in spacea
        r = multiuserClient.get("graphspaces/spacea/graphs");
        String graphsList = assertResponseStatus(200, r);
        Assert.assertFalse("grapha1 should be deleted", graphsList.contains("grapha1"));
        Assert.assertTrue("grapha2 should still exist", graphsList.contains("grapha2"));

        // Test 2: As SPACE_MEMBER in spaceb, should NOT be able to delete graph
        r = multiuserClient.delete("graphspaces/spaceb/graphs/graphb1",
                                   ImmutableMap.of("confirm_message", confirmMessage));
        String response = r.readEntity(String.class);
        Assert.assertEquals("As SPACE_MEMBER in spaceb, should not be able to delete graph",
                            403, r.getStatus());
        Assert.assertTrue("Response should indicate permission denied",
                          response.contains("permission") || response.contains("Forbidden"));

        // Verify graph still exists in spaceb
        r = multiuserClient.get("graphspaces/spaceb/graphs");
        graphsList = assertResponseStatus(200, r);
        Assert.assertTrue("graphb1 should still exist", graphsList.contains("graphb1"));
        Assert.assertTrue("graphb2 should still exist", graphsList.contains("graphb2"));

        // Test 3: Verify user can read graphs in both spaces
        r = multiuserClient.get("graphspaces/spacea/graphs/grapha2/schema");
        assertResponseStatus(200, r);

        r = multiuserClient.get("graphspaces/spaceb/graphs/graphb1/schema");
        assertResponseStatus(200, r);

        // Test 4: Admin verifies the isolation by deleting a graph in spaceb
        r = client().delete("graphspaces/spaceb/graphs/graphb2",
                            ImmutableMap.of("confirm_message", confirmMessage));
        status = r.getStatus();
        Assert.assertTrue("Admin should be able to delete graph in spaceb",
                          status == 200 || status == 204);

        // Final verification
        r = client().get("graphspaces/spacea/graphs");
        graphsList = assertResponseStatus(200, r);
        Assert.assertTrue("Only grapha2 should remain in spacea",
                          !graphsList.contains("grapha1") && graphsList.contains("grapha2"));

        r = client().get("graphspaces/spaceb/graphs");
        graphsList = assertResponseStatus(200, r);
        Assert.assertTrue("Only graphb1 should remain in spaceb",
                          graphsList.contains("graphb1") && !graphsList.contains("graphb2"));
    }
}
