/*
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

import org.apache.hugegraph.util.JsonUtil;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

import jakarta.ws.rs.core.Response;

public class GraphSpaceApiTest extends BaseApiTest {

    private static final String PATH = "graphspaces";

    @Before
    public void removeSpaces() {
        Assume.assumeTrue("skip this test for non-hstore",
                          Objects.equals("hstore", System.getProperty("backend")));
        Response r = this.client().get(PATH);
        String result = r.readEntity(String.class);
        Assert.assertEquals(result, 200, r.getStatus());
        Map<String, Object> resultMap = JsonUtil.fromJson(result, Map.class);
        List<String> spaces = (List<String>)resultMap.get("graphSpaces");
        Assert.assertNotNull("graphspace API should be available in hstore",
                             spaces);
        for (String space : spaces) {
            if (!"DEFAULT".equals(space)) {
                this.client().delete(PATH, space);
            }
        }
    }

    @Test
    public void testAddSpaceNamespace() {
        String body = "{\n" +
                      "  \"name\": \"test_add_no_ns\",\n" +
                      "  \"nickname\":\"TestNoNamespace\",\n" +
                      "  \"description\": \"nonamespace\",\n" +
                      "  \"cpu_limit\": 1000,\n" +
                      "  \"memory_limit\": 1024,\n" +
                      "  \"storage_limit\": 1000,\n" +
                      "  \"compute_cpu_limit\": 0,\n" +
                      "  \"compute_memory_limit\": 0,\n" +
                      "  \"oltp_namespace\": null,\n" +
                      "  \"olap_namespace\": null,\n" +
                      "  \"storage_namespace\": null,\n" +
                      "  \"operator_image_path\": \"aaa\",\n" +
                      "  \"internal_algorithm_image_url\": \"aaa\",\n" +
                      "  \"max_graph_number\": 100,\n" +
                      "  \"max_role_number\": 100,\n" +
                      "  \"auth\": false,\n" +
                      "  \"configs\": {}\n" +
                      "}";
        Response r = this.client().post(PATH, body);
        assertResponseStatus(201, r);

        String body2 = "{\n" +
                       "  \"name\": \"test_add_has_ns\",\n" +
                       "  \"nickname\":\"TestWithNamespace\",\n" +
                       "  \"description\": \"hasnamespace\",\n" +
                       "  \"cpu_limit\": 1000,\n" +
                       "  \"memory_limit\": 1024,\n" +
                       "  \"storage_limit\": 1000,\n" +
                       "  \"compute_cpu_limit\": 0,\n" +
                       "  \"compute_memory_limit\": 0,\n" +
                       "  \"oltp_namespace\": \"oltp5\",\n" +
                       "  \"olap_namespace\": \"olap5\",\n" +
                       "  \"storage_namespace\": \"st5\",\n" +
                       "  \"operator_image_path\": \"aaa\",\n" +
                       "  \"internal_algorithm_image_url\": \"aaa\",\n" +
                       "  \"max_graph_number\": 100,\n" +
                       "  \"max_role_number\": 100,\n" +
                       "  \"auth\": false,\n" +
                       "  \"configs\": {}\n" +
                       "}";
        r = this.client().post(PATH, body2);
        assertResponseStatus(201, r);
    }

    @Test
    public void testGetSpace() {
        Response r = this.client().get(PATH + "/DEFAULT");
        assertResponseStatus(200, r);
    }

    @Test
    public void testDeleteSpace() {
        String spaceName = "test_delete_space";
        String body = "{"
                      + "\"name\":\"" + spaceName + "\","
                      + "\"nickname\":\"TestDeleteSpace\","
                      + "\"description\":\"Testdeletespace\","
                      + "\"cpu_limit\":1000,"
                      + "\"memory_limit\":1024,"
                      + "\"storage_limit\":1000,"
                      + "\"compute_cpu_limit\":0,"
                      + "\"compute_memory_limit\":0,"
                      + "\"oltp_namespace\":null,"
                      + "\"olap_namespace\":null,"
                      + "\"storage_namespace\":null,"
                      + "\"operator_image_path\":\"test\","
                      + "\"internal_algorithm_image_url\":\"test\","
                      + "\"max_graph_number\":100,"
                      + "\"max_role_number\":100,"
                      + "\"auth\":false,"
                      + "\"configs\":{}"
                      + "}";

        // Create graph space
        Response r = this.client().post(PATH, body);
        assertResponseStatus(201, r);

        // Verify graph space exists
        r = this.client().get(PATH, spaceName);
        assertResponseStatus(200, r);

        // Delete graph space
        r = this.client().delete(PATH, spaceName);
        assertResponseStatus(204, r);

        // Verify graph space has been deleted
        r = this.client().get(PATH, spaceName);
        assertResponseStatus(400, r);
    }

    @Test
    public void testCreateSpaceWithSameName() {
        String spaceName = "duplicate_space";
        String body = "{"
                      + "\"name\":\"" + spaceName + "\","
                      + "\"nickname\":\"DuplicateTestSpace\","
                      + "\"description\":\"Testduplicatespace\","
                      + "\"cpu_limit\":1000,"
                      + "\"memory_limit\":1024,"
                      + "\"storage_limit\":1000,"
                      + "\"compute_cpu_limit\":0,"
                      + "\"compute_memory_limit\":0,"
                      + "\"oltp_namespace\":null,"
                      + "\"olap_namespace\":null,"
                      + "\"storage_namespace\":null,"
                      + "\"operator_image_path\":\"test\","
                      + "\"internal_algorithm_image_url\":\"test\","
                      + "\"max_graph_number\":100,"
                      + "\"max_role_number\":100,"
                      + "\"auth\":false,"
                      + "\"configs\":{}"
                      + "}";

        // First creation should succeed
        Response r = this.client().post(PATH, body);
        assertResponseStatus(201, r);

        // Second creation should fail (duplicate name)
        r = this.client().post(PATH, body);
        assertResponseStatus(400, r);
    }

    @Test
    public void testSpaceResourceLimits() {
        String spaceName = "test_limits_space";

        // Test minimum limits
        String minLimitsBody = "{"
                               + "\"name\":\"" + spaceName + "_min\","
                               + "\"nickname\":\"MinimumLimitsTest\","
                               + "\"description\":\"Testminimumlimits\","
                               + "\"cpu_limit\":1,"
                               + "\"memory_limit\":1,"
                               + "\"storage_limit\":1,"
                               + "\"compute_cpu_limit\":0,"
                               + "\"compute_memory_limit\":0,"
                               + "\"oltp_namespace\":null,"
                               + "\"olap_namespace\":null,"
                               + "\"storage_namespace\":null,"
                               + "\"operator_image_path\":\"test\","
                               + "\"internal_algorithm_image_url\":\"test\","
                               + "\"max_graph_number\":1,"
                               + "\"max_role_number\":1,"
                               + "\"auth\":false,"
                               + "\"configs\":{}"
                               + "}";

        Response r = this.client().post(PATH, minLimitsBody);
        assertResponseStatus(201, r);

        // Test maximum limits
        String maxLimitsBody = "{"
                               + "\"name\":\"" + spaceName + "_max\","
                               + "\"nickname\":\"MaximumLimitsTest\","
                               + "\"description\":\"Testmaximumlimits\","
                               + "\"cpu_limit\":999999,"
                               + "\"memory_limit\":999999,"
                               + "\"storage_limit\":999999,"
                               + "\"compute_cpu_limit\":999999,"
                               + "\"compute_memory_limit\":999999,"
                               + "\"oltp_namespace\":\"large_oltp\","
                               + "\"olap_namespace\":\"large_olap\","
                               + "\"storage_namespace\":\"large_storage\","
                               + "\"operator_image_path\":\"large_path\","
                               + "\"internal_algorithm_image_url\":\"large_url\","
                               + "\"max_graph_number\":999999,"
                               + "\"max_role_number\":999999,"
                               + "\"auth\":true,"
                               + "\"configs\":{\"large_key\":\"large_value\"}"
                               + "}";

        r = this.client().post(PATH, maxLimitsBody);
        assertResponseStatus(201, r);
    }

    @Test
    public void testInvalidSpaceCreation() {
        // Test invalid space name
        String invalidNameBody = "{"
                                 + "\"name\":\"\","
                                 + "\"nickname\":\"Invalid Name Test\","
                                 + "\"description\":\"Test invalid name\","
                                 + "\"cpu_limit\":1000,"
                                 + "\"memory_limit\":1024,"
                                 + "\"storage_limit\":1000,"
                                 + "\"compute_cpu_limit\":0,"
                                 + "\"compute_memory_limit\":0,"
                                 + "\"oltp_namespace\":null,"
                                 + "\"olap_namespace\":null,"
                                 + "\"storage_namespace\":null,"
                                 + "\"operator_image_path\":\"test\","
                                 + "\"internal_algorithm_image_url\":\"test\","
                                 + "\"max_graph_number\":100,"
                                 + "\"max_role_number\":100,"
                                 + "\"auth\":false,"
                                 + "\"configs\":{}"
                                 + "}";

        Response r = this.client().post(PATH, invalidNameBody);
        assertResponseStatus(400, r);

        // Test negative limits
        String negativeLimitsBody = "{"
                                    + "\"name\":\"test_negative\","
                                    + "\"nickname\":\"Negative Limits Test\","
                                    + "\"description\":\"Test negative limits\","
                                    + "\"cpu_limit\":-1,"
                                    + "\"memory_limit\":-1,"
                                    + "\"storage_limit\":-1,"
                                    + "\"compute_cpu_limit\":0,"
                                    + "\"compute_memory_limit\":0,"
                                    + "\"oltp_namespace\":null,"
                                    + "\"olap_namespace\":null,"
                                    + "\"storage_namespace\":null,"
                                    + "\"operator_image_path\":\"test\","
                                    + "\"internal_algorithm_image_url\":\"test\","
                                    + "\"max_graph_number\":-1,"
                                    + "\"max_role_number\":-1,"
                                    + "\"auth\":false,"
                                    + "\"configs\":{}"
                                    + "}";

        r = this.client().post(PATH, negativeLimitsBody);
        assertResponseStatus(400, r);
    }

    @Test
    public void testListProfile() {
        // Get profile list without prefix
        Response r = this.client().get(PATH + "/profile");
        String result = assertResponseStatus(200, r);

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> profiles = JsonUtil.fromJson(result, List.class);

        // Should contain at least the DEFAULT space
        Assert.assertTrue("Expected at least one profile", profiles.size() >= 1);

        // Verify profile structure
        for (Map<String, Object> profile : profiles) {
            Assert.assertTrue("Profile should contain 'name'",
                              profile.containsKey("name"));
            Assert.assertTrue("Profile should contain 'authed'",
                              profile.containsKey("authed"));
            Assert.assertTrue("Profile should contain 'create_time'",
                              profile.containsKey("create_time"));
            Assert.assertTrue("Profile should contain 'update_time'",
                              profile.containsKey("update_time"));
            Assert.assertTrue("Profile should contain 'default'",
                              profile.containsKey("default"));
        }
    }

    @Test
    public void testListProfileWithPrefix() {
        // Create test spaces with different names
        String space1 = "{"
                        + "\"name\":\"test_profile_space1\","
                        + "\"nickname\":\"TestProfileSpace\","
                        + "\"description\":\"Testprofilelisting\","
                        + "\"cpu_limit\":1000,"
                        + "\"memory_limit\":1024,"
                        + "\"storage_limit\":1000,"
                        + "\"compute_cpu_limit\":0,"
                        + "\"compute_memory_limit\":0,"
                        + "\"oltp_namespace\":null,"
                        + "\"olap_namespace\":null,"
                        + "\"storage_namespace\":null,"
                        + "\"operator_image_path\":\"test\","
                        + "\"internal_algorithm_image_url\":\"test\","
                        + "\"max_graph_number\":100,"
                        + "\"max_role_number\":100,"
                        + "\"auth\":false,"
                        + "\"configs\":{}"
                        + "}";

        // Create a space that should NOT match the prefix filter
        String space2 = "{"
                        + "\"name\":\"other_profile_space\","
                        + "\"nickname\":\"OtherProfileSpace\","
                        + "\"description\":\"Other profile listing\","
                        + "\"cpu_limit\":1000,"
                        + "\"memory_limit\":1024,"
                        + "\"storage_limit\":1000,"
                        + "\"compute_cpu_limit\":0,"
                        + "\"compute_memory_limit\":0,"
                        + "\"oltp_namespace\":null,"
                        + "\"olap_namespace\":null,"
                        + "\"storage_namespace\":null,"
                        + "\"operator_image_path\":\"test\","
                        + "\"internal_algorithm_image_url\":\"test\","
                        + "\"max_graph_number\":100,"
                        + "\"max_role_number\":100,"
                        + "\"auth\":false,"
                        + "\"configs\":{}"
                        + "}";

        // Create spaces
        Response r = this.client().post(PATH, space1);
        assertResponseStatus(201, r);
        r = this.client().post(PATH, space2);
        assertResponseStatus(201, r);

        // Test with prefix filter
        r = this.client().get(PATH + "/profile",
                              ImmutableMap.of("prefix", "test"));
        String result = assertResponseStatus(200, r);

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> profiles = JsonUtil.fromJson(result, List.class);
        Assert.assertFalse("Expected non-empty profile list with prefix filter",
                           profiles.isEmpty());

        // Verify all returned profiles match the prefix
        for (Map<String, Object> profile : profiles) {
            String name = Objects.toString(profile.get("name"), "");
            String nickname = Objects.toString(profile.get("nickname"), "");
            boolean matchesPrefix = name.startsWith("test") ||
                                    nickname.startsWith("test") ||
                                    nickname.startsWith("Test");
            Assert.assertTrue(
                    "Profile should match prefix 'test': " + profile,
                    matchesPrefix);

            // Ensure the non-matching space is excluded
            Assert.assertNotEquals("Non-matching space should be filtered out",
                                   "other_profile_space", name);
        }
    }

    @Test
    public void testListProfileWithAuth() {
        // Create a space with auth enabled
        String authSpace = "{"
                           + "\"name\":\"auth_test_space\","
                           + "\"nickname\":\"AuthTestSpace\","
                           + "\"description\":\"Test auth in profile\","
                           + "\"cpu_limit\":1000,"
                           + "\"memory_limit\":1024,"
                           + "\"storage_limit\":1000,"
                           + "\"compute_cpu_limit\":0,"
                           + "\"compute_memory_limit\":0,"
                           + "\"oltp_namespace\":null,"
                           + "\"olap_namespace\":null,"
                           + "\"storage_namespace\":null,"
                           + "\"operator_image_path\":\"test\","
                           + "\"internal_algorithm_image_url\":\"test\","
                           + "\"max_graph_number\":100,"
                           + "\"max_role_number\":100,"
                           + "\"auth\":true,"
                           + "\"configs\":{}"
                           + "}";

        Response r = this.client().post(PATH, authSpace);
        assertResponseStatus(201, r);

        // Get profile list
        r = this.client().get(PATH + "/profile");
        String result = assertResponseStatus(200, r);

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> profiles = JsonUtil.fromJson(result, List.class);

        // Find the auth_test_space and verify authed field
        boolean found = false;
        for (Map<String, Object> profile : profiles) {
            if ("auth_test_space".equals(profile.get("name"))) {
                found = true;
                // Admin user should be authed
                Assert.assertTrue("Profile should contain 'authed' field",
                                  profile.containsKey("authed"));
                Assert.assertEquals("Admin user should be authorized",
                                    true, profile.get("authed"));
                break;
            }
        }
        Assert.assertTrue("auth_test_space not found in profile list", found);
    }

    @Test
    public void testDefaultGraphLifecycle() {
        String space = "default_graph_space";
        String graph = "default_graph_test";
        createSpace(space, true);
        Response r = createGraph(space, graph, "DefaultGraphNick");
        assertResponseStatus(201, r);

        String graphsPath = String.format("graphspaces/%s/graphs", space);
        r = this.client().post(graphsPath + "/" + graph + "/default", "");
        String result = assertResponseStatus(200, r);
        Assert.assertTrue(result.contains(graph));

        r = this.client().get(graphsPath + "/default");
        result = assertResponseStatus(200, r);
        Assert.assertTrue(result.contains(graph));

        r = this.client().get(graphsPath + "/profile");
        result = assertResponseStatus(200, r);
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> profiles = JsonUtil.fromJson(result, List.class);
        Map<String, Object> profile = profiles.stream()
                                              .filter(p -> graph.equals(p.get("name")))
                                              .findFirst()
                                              .orElse(null);
        Assert.assertNotNull("Default graph should appear in profile list", profile);
        Assert.assertEquals(Boolean.TRUE, profile.get("default"));

        r = this.client().delete(graphsPath + "/" + graph + "/default",
                                 ImmutableMap.of());
        result = assertResponseStatus(200, r);
        Assert.assertFalse(result.contains(graph));
    }

    @Test
    public void testDefaultRoleLifecycle() {
        String space = "default_role_space";
        createSpace(space, true);

        String rolePath = String.format("graphspaces/%s/role", space);
        String body = "{\"user\":\"admin\",\"role\":\"ANALYST\"}";
        Response r = this.client().post(rolePath, body);
        assertResponseStatus(201, r);

        r = this.client().get(rolePath,
                              ImmutableMap.of("user", "admin",
                                              "role", "ANALYST"));
        String result = assertResponseStatus(200, r);
        Assert.assertTrue(result.contains("true"));

        r = this.client().delete(rolePath,
                                 ImmutableMap.of("user", "admin",
                                                 "role", "ANALYST"));
        assertResponseStatus(204, r);

        r = this.client().get(rolePath,
                              ImmutableMap.of("user", "admin",
                                              "role", "ANALYST"));
        result = assertResponseStatus(200, r);
        Assert.assertTrue(result.contains("false"));
    }

    @Test
    public void testDefaultRoleMutationRequiresSpaceManager() {
        String space = "default_role_auth_space";
        String analyst = "default_role_analyst";
        String target = "default_role_target";
        String manager = "default_role_manager";
        createSpace(space, true);

        RestClient analystClient = userClient(analyst);
        userClient(target);
        RestClient managerClient = spaceManagerClient(space, manager);

        String rolePath = String.format("graphspaces/%s/role", space);
        String analystBody = String.format("{\"user\":\"%s\",\"role\":\"ANALYST\"}",
                                           analyst);
        Response r = this.client().post(rolePath, analystBody);
        assertResponseStatus(201, r);

        String grantTargetBody = String.format("{\"user\":\"%s\",\"role\":\"ANALYST\"}",
                                               target);
        r = analystClient.post(rolePath, grantTargetBody);
        assertResponseStatus(403, r);

        r = analystClient.get(rolePath,
                              ImmutableMap.of("user", target,
                                              "role", "ANALYST"));
        assertResponseStatus(403, r);

        r = analystClient.delete(rolePath,
                                 ImmutableMap.of("user", analyst,
                                                 "role", "ANALYST"));
        assertResponseStatus(403, r);

        r = managerClient.post(rolePath, grantTargetBody);
        assertResponseStatus(201, r);

        r = managerClient.get(rolePath,
                              ImmutableMap.of("user", target,
                                              "role", "ANALYST"));
        String result = assertResponseStatus(200, r);
        Assert.assertTrue(result.contains("true"));

        String spaceRoleBody = String.format("{\"user\":\"%s\",\"role\":\"SPACE\"}",
                                             target);
        r = managerClient.post(rolePath, spaceRoleBody);
        assertResponseStatus(403, r);

        r = managerClient.delete(rolePath,
                                 ImmutableMap.of("user", target,
                                                 "role", "ANALYST"));
        assertResponseStatus(204, r);
    }

    @Test
    public void testSchemaTemplateLifecycle() {
        String space = "schema_template_space";
        String template = "hubble_schema_template";
        String schema = "schema.propertyKey('name').asText().ifNotExist().create();";
        String path = String.format("graphspaces/%s/schematemplates", space);
        createSpace(space, true);

        String body = String.format("{\"name\":\"%s\",\"schema\":\"%s\"}",
                                    template, schema);
        Response r = this.client().post(path, body);
        String result = assertResponseStatus(201, r);
        Assert.assertTrue(result.contains(template));

        r = this.client().get(path, template);
        result = assertResponseStatus(200, r);
        Assert.assertTrue(result.contains(template));

        r = this.client().get(path);
        result = assertResponseStatus(200, r);
        Assert.assertTrue(result.contains(template));

        String updated = "schema.propertyKey('age').asInt().ifNotExist().create();";
        body = String.format("{\"schema\":\"%s\"}", updated);
        r = this.client().target(baseUrl())
                         .path(path + "/" + template)
                         .request()
                         .put(jakarta.ws.rs.client.Entity.json(body));
        result = assertResponseStatus(200, r);
        Assert.assertTrue(result.contains("age"));

        r = this.client().delete(path, template);
        assertResponseStatus(204, r);

        r = this.client().get(path, template);
        assertResponseStatus(404, r);
    }
}
