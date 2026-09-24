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

import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hugegraph.util.JsonUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.core.Response;

public class ProjectApiTest extends BaseApiTest {

    private static final String PATH = "graphspaces/DEFAULT/auth/projects";

    @BeforeClass
    public static void setUpClass() {
        // FIXME: skip this test for hstore
        Assume.assumeTrue("skip this test for hstore",
                          !Objects.equals("hstore", System.getProperty("backend")));
        Assume.assumeTrue("skip this test for null", !(System.getProperty("backend") == null));
    }

    @Override
    @After
    public void teardown() throws Exception {
        Response resp = client().get(PATH);
        String respBody = assertResponseStatus(200, resp);
        List<?> projects = readList(respBody, "projects", Map.class);
        for (Object project : projects) {
            @SuppressWarnings("unchecked")
            Map<String, Object> projectMap = ((Map<String, Object>) project);
            String projectId = (String) projectMap.get("id");
            // remove graphs from a project if needed
            List<?> projectGraphs = (List<?>) projectMap.get("project_graphs");
            if (projectGraphs != null && !projectGraphs.isEmpty()) {
                Map<String, Object> graphs = ImmutableMap.of("project_graphs", projectGraphs);
                resp = client().target()
                               .path(PATH)
                               .path(projectId)
                               .queryParam("action", "remove_graph")
                               .request()
                               .put(Entity.json(JsonUtil.toJson(graphs)));
                assertResponseStatus(200, resp);
            }
            // delete project
            resp = client().target()
                           .path(PATH)
                           .path(projectId)
                           .request()
                           .delete();
            assertResponseStatus(204, resp);
        }
    }

    @Test
    public void testCreate() {
        String project = "{\"project_name\": \"test_project\"," +
                         "\"project_description\": " +
                         "\"this is a good project\"}";
        Response resp = client().post(PATH, project);
        String respBody = assertResponseStatus(201, resp);
        String projectName = assertJsonContains(respBody, "project_name");
        Assert.assertEquals("test_project", projectName);
        String projectDescription = assertJsonContains(respBody, "project_description");
        Assert.assertEquals("this is a good project", projectDescription);
        String projectTarget = assertJsonContains(respBody, "project_target");
        Assert.assertFalse(StringUtils.isEmpty(projectTarget));
        String projectAdminGroup = assertJsonContains(respBody, "project_admin_group");
        Assert.assertFalse(StringUtils.isEmpty(projectAdminGroup));
        String projectOpGroup = assertJsonContains(respBody, "project_op_group");
        Assert.assertFalse(StringUtils.isEmpty(projectOpGroup));
    }

    @Test
    public void testDelete() {
        String project = this.createProject("test_project1", "this is a good project");
        String projectId = assertJsonContains(project, "id");
        Response resp = client().target()
                                .path(PATH)
                                .path(projectId)
                                .request()
                                .delete();
        assertResponseStatus(204, resp);
    }

    @Test
    public void testGet() {
        String project = this.createProject("test_project46", "this is a good project");
        String projectId = assertJsonContains(project, "id");
        String project2 = this.getProject(projectId);
        Assert.assertEquals(project, project2);
    }

    @Test
    public void testList() {
        createProject("test_project46", null);
        createProject("test_project47", null);
        Response resp = client().get(PATH);
        String respBody = assertResponseStatus(200, resp);
        List<?> projects = readList(respBody, "projects", Map.class);
        Assert.assertNotNull(projects);
        Assert.assertEquals(2, projects.size());
    }

    @Test
    public void testUpdate() {
        String project = "{\"project_description\": \"update desc\"}";
        Response resp = client().target()
                                .path(PATH)
                                .path("no_exist_id")
                                .request()
                                .put(Entity.json(project));
        assertResponseStatus(400, resp);

        String projectId = assertJsonContains(createProject("test_project2", "desc"), "id");
        resp = client().target()
                       .path(PATH)
                       .path(projectId)
                       .request()
                       .put(Entity.json(project));
        String respBody = assertResponseStatus(200, resp);
        String description = assertJsonContains(respBody, "project_description");
        Assert.assertEquals("update desc", description);
    }

    @Test
    public void testAddGraphs() {
        String project = createProject("project_test2", null);
        String projectId = assertJsonContains(project, "id");
        String graphs = "{\"project_graphs\":[\"graph_test\", " +
                        "\"graph_test2\"]}";
        Response resp = client().target()
                                .path(PATH)
                                .path(projectId)
                                .queryParam("action", "add_graph")
                                .request()
                                .put(Entity.json(graphs));
        assertResponseStatus(200, resp);

        project = this.getProject(projectId);
        List<String> graphs1 = assertJsonContains(project, "project_graphs");
        Assert.assertEquals(2, graphs1.size());
        Assert.assertTrue(graphs1.contains("graph_test"));
        Assert.assertTrue(graphs1.contains("graph_test2"));
    }

    @Test
    public void testRemoveGraphs() {
        String projectId = this.createProjectAndAddGraph("project_test", "graph_test");
        String graphs = "{\"project_graphs\":[\"graph_test\"]}";
        Response resp = client().target()
                                .path(PATH)
                                .path(projectId)
                                .queryParam("action", "remove_graph")
                                .request()
                                .put(Entity.json(graphs));
        assertResponseStatus(200, resp);

        String project = this.getProject(projectId);
        Assert.assertFalse(project.contains("project_graphs"));

        this.addGraphsToProject(projectId, "graph_test1", "graph_test2");

        graphs = "{\"project_graphs\":[\"graph_test1\"]}";
        resp = client().target()
                       .path(PATH)
                       .path(projectId)
                       .queryParam("action", "remove_graph")
                       .request()
                       .put(Entity.json(graphs));
        assertResponseStatus(200, resp);

        project = this.getProject(projectId);
        List<String> graphs1 = assertJsonContains(project, "project_graphs");
        Assert.assertEquals(1, graphs1.size());
        Assert.assertTrue(graphs1.contains("graph_test2"));
    }

    private String createProject(String name, String desc) {
        String project = String.format("{\"project_name\": \"%s\"," +
                                       "\"project_description\": " +
                                       "\"%s\"}", name, desc);
        Response resp = client().post(PATH, project);
        String respBody = assertResponseStatus(201, resp);
        String projectName = assertJsonContains(respBody, "project_name");
        Assert.assertEquals(name, projectName);
        if (!StringUtils.isEmpty(desc)) {
            String description = assertJsonContains(respBody,
                                                    "project_description");
            Assert.assertEquals(desc, description);
        }
        Assert.assertFalse(StringUtils.isEmpty(
                assertJsonContains(respBody, "project_target")));
        Assert.assertFalse(StringUtils.isEmpty(
                assertJsonContains(respBody, "project_admin_group")));
        Assert.assertFalse(StringUtils.isEmpty(
                assertJsonContains(respBody, "project_op_group")));
        return respBody;
    }

    private String createProjectAndAddGraph(String projectName, String graph) {
        String projectId = assertJsonContains(createProject(projectName, null), "id");
        this.addGraphsToProject(projectId, graph);
        return projectId;
    }

    private void addGraphsToProject(String projectId, String... graphNames) {
        Assert.assertFalse(ArrayUtils.isEmpty(graphNames));
        StringBuilder graphNamesBuilder = new StringBuilder();
        for (int i = 0; i < graphNames.length - 1; i++) {
            graphNamesBuilder.append(String.format("\"%s\",", graphNames[i]));
        }
        graphNamesBuilder.append(String.format("\"%s\"", graphNames[graphNames.length - 1]));
        String graphs = String.format("{\"project_graphs\":[%s]}",
                                      graphNamesBuilder);
        Response resp = client().target()
                                .path(PATH)
                                .path(projectId)
                                .queryParam("action", "add_graph")
                                .request()
                                .put(Entity.json(graphs));
        assertResponseStatus(200, resp);
    }

    private String getProject(String projectId) {
        Response resp = client().target()
                                .path(PATH)
                                .path(projectId)
                                .request()
                                .get();
        return assertResponseStatus(200, resp);
    }
}
