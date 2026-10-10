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

package org.apache.hugegraph.meta.managers;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hugegraph.auth.HugeProject;
import org.apache.hugegraph.auth.HugeTarget;
import org.apache.hugegraph.id.IdGenerator;
import org.apache.hugegraph.meta.MetaDriver;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.util.JsonUtil;
import org.junit.Test;
import org.mockito.Mockito;

public class AuthMetaManagerTest {

    @Test
    public void testGetTargetRestoresLegacyGraphSpaceFromNamespace()
            throws Exception {
        MetaDriver driver = Mockito.mock(MetaDriver.class);
        Map<String, Object> legacy = target("DEFAULT").asMap();
        legacy.remove("graphspace");
        Mockito.when(driver.get(Mockito.anyString()))
               .thenReturn(JsonUtil.toJson(legacy));
        AuthMetaManager manager = new AuthMetaManager(driver, "cluster");

        HugeTarget restored = manager.getTarget(
                              "SPACE_A", IdGenerator.of("target"));

        Assert.assertEquals("SPACE_A", restored.graphSpace());
    }

    @Test
    public void testGetTargetRejectsExplicitGraphSpaceMismatch() {
        MetaDriver driver = Mockito.mock(MetaDriver.class);
        Mockito.when(driver.get(Mockito.anyString()))
               .thenReturn(JsonUtil.toJson(target("SPACE_B").asMap()));
        AuthMetaManager manager = new AuthMetaManager(driver, "cluster");

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            manager.getTarget("SPACE_A", IdGenerator.of("target"));
        });
    }

    @Test
    public void testDeleteTargetRejectsMismatchBeforeMutation() {
        MetaDriver driver = Mockito.mock(MetaDriver.class);
        Mockito.when(driver.get(Mockito.anyString()))
               .thenReturn(JsonUtil.toJson(target("SPACE_B").asMap()));
        AuthMetaManager manager = new AuthMetaManager(driver, "cluster");

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            manager.deleteTarget("SPACE_A", IdGenerator.of("target"));
        });
        Mockito.verify(driver, Mockito.never()).delete(Mockito.anyString());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testProjectGraphsJsonRoundTrip() {
        HugeProject project = project();
        Map<String, Object> map = JsonUtil.fromJson(
                                  JsonUtil.toJson(project.asMap()), Map.class);

        Assert.assertTrue(map.get("project_graphs") instanceof List);
        HugeProject restored = HugeProject.fromMap(map);
        Assert.assertEquals(project.id(), restored.id());
        Assert.assertEquals(project.graphs(), restored.graphs());
        Assert.assertEquals(project.adminGroupId(), restored.adminGroupId());
        Assert.assertEquals(project.opGroupId(), restored.opGroupId());
        Assert.assertEquals(project.targetId(), restored.targetId());
        Assert.assertEquals(project.description(), restored.description());
    }

    @Test
    public void testProjectGraphsAcceptsSetAndDeduplicatesList() {
        Map<String, Object> map = project().asMap();
        Set<String> graphs = new HashSet<>(Arrays.asList("graph1", "graph2"));
        map.put("project_graphs", graphs);
        Assert.assertEquals(graphs, HugeProject.fromMap(map).graphs());

        map.put("project_graphs", Arrays.asList("graph1", "graph2", "graph1"));
        Assert.assertEquals(graphs, HugeProject.fromMap(map).graphs());
        map.put("project_graphs", Collections.emptyList());
        Assert.assertTrue(HugeProject.fromMap(map).graphs().isEmpty());
        map.put("project_graphs", null);
        Assert.assertTrue(HugeProject.fromMap(map).graphs().isEmpty());
        map.remove("project_graphs");
        Assert.assertTrue(HugeProject.fromMap(map).graphs().isEmpty());
    }

    @Test
    public void testProjectGraphsRejectsInvalidTypes() {
        Map<String, Object> map = project().asMap();
        for (Object value : Arrays.asList("graph1", Collections.emptyMap(),
                                         Arrays.asList("graph1", 1),
                                         Arrays.asList("graph1", null))) {
            map.put("project_graphs", value);
            Assert.assertThrows(IllegalArgumentException.class, () -> {
                HugeProject.fromMap(map);
            });
        }
    }

    @Test
    public void testProjectMetadataReadAndMutationPaths() throws Exception {
        HugeProject project = project();
        String json = JsonUtil.toJson(project.asMap());
        String key = "HUGEGRAPH/cluster/GRAPHSPACE/SPACE_A/AUTH/PROJECT/project";
        MetaDriver driver = Mockito.mock(MetaDriver.class);
        Mockito.when(driver.get(key)).thenReturn(json);
        Mockito.when(driver.scanWithPrefix(Mockito.anyString()))
               .thenReturn(Collections.singletonMap(key, json));
        AuthMetaManager manager = new AuthMetaManager(driver, "cluster");

        Assert.assertEquals(project.graphs(),
                            manager.getProject("SPACE_A", project.id()).graphs());
        Assert.assertEquals(project.graphs(),
                            manager.listAllProjects("SPACE_A", -1).get(0).graphs());

        project.graphs(Collections.singleton("graph3"));
        HugeProject updated = manager.updateProject("SPACE_A", project);
        Assert.assertEquals(project.graphs(), updated.graphs());
        Mockito.verify(driver).put(key, JsonUtil.toJson(updated.asMap()));

        HugeProject deleted = manager.deleteProject("SPACE_A", project.id());
        Assert.assertEquals(new HashSet<>(Arrays.asList("graph1", "graph2")),
                            deleted.graphs());
        Mockito.verify(driver).delete(key);
    }

    private static HugeProject project() {
        HugeProject project = new HugeProject("project", "description");
        project.adminGroupId(IdGenerator.of("admin-group"));
        project.opGroupId(IdGenerator.of("op-group"));
        project.targetId(IdGenerator.of("target"));
        project.creator("admin");
        project.graphs(new HashSet<>(Arrays.asList("graph1", "graph2")));
        return project;
    }

    private static HugeTarget target(String graphSpace) {
        HugeTarget target = new HugeTarget("target", "hugegraph", "url");
        target.graphSpace(graphSpace);
        target.creator("admin");
        return target;
    }
}
