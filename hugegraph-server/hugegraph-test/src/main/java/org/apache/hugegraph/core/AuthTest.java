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

package org.apache.hugegraph.core;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import javax.security.sasl.AuthenticationException;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hugegraph.HugeException;
import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.HugeGraphParams;
import org.apache.hugegraph.auth.AuthManager;
import org.apache.hugegraph.auth.HugeAccess;
import org.apache.hugegraph.auth.HugeBelong;
import org.apache.hugegraph.auth.HugeGroup;
import org.apache.hugegraph.auth.HugePermission;
import org.apache.hugegraph.auth.HugeProject;
import org.apache.hugegraph.auth.HugeResource;
import org.apache.hugegraph.auth.HugeTarget;
import org.apache.hugegraph.auth.HugeUser;
import org.apache.hugegraph.auth.RolePermission;
import org.apache.hugegraph.auth.StandardAuthManager;
import org.apache.hugegraph.auth.UserWithRole;
import org.apache.hugegraph.backend.cache.Cache;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.id.IdGenerator;
import org.apache.hugegraph.schema.VertexLabel;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.testutil.Whitebox;
import org.apache.hugegraph.util.JsonUtil;
import org.apache.hugegraph.util.StringEncoding;
import org.junit.After;
import org.junit.Assume;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

public class AuthTest extends BaseCoreTest {

    private static Id makeProjectAndAddGraph(HugeGraph graph,
                                             String projectName,
                                             String graphName) {
        HugeProject project = makeProject(projectName, "");
        AuthManager authManager = graph.authManager();
        Id projectId = authManager.createProject(project);
        projectId = authManager.projectAddGraphs(projectId,
                                                 ImmutableSet.of(graphName));
        Assert.assertNotNull(projectId);
        return projectId;
    }

    private static HugeProject makeProject(String name, String desc) {
        HugeProject project = new HugeProject(name, desc);
        project.creator("admin");
        return project;
    }

    private static HugeUser makeUser(String name, String password) {
        HugeUser user = new HugeUser(name);
        user.password(password);
        user.creator("admin");
        return user;
    }

    private static HugeGroup makeGroup(String name) {
        HugeGroup group = new HugeGroup(name);
        group.creator("admin");
        return group;
    }

    private static HugeTarget makeTarget(String name, String url) {
        HugeTarget target = new HugeTarget(name, url);
        target.creator("admin");
        return target;
    }

    private static HugeTarget makeTarget(String name, String graph, String url,
                                         Map<String, List<HugeResource>> ress) {
        HugeTarget target = new HugeTarget(name, graph, url, ress);
        target.creator("admin");
        return target;
    }

    private static HugeBelong makeBelong(Id user, Id group) {
        HugeBelong belong = new HugeBelong(user, group);
        belong.creator("admin");
        return belong;
    }

    private static HugeAccess makeAccess(Id group, Id target,
                                         HugePermission permission) {
        HugeAccess access = new HugeAccess(group, target, permission);
        access.creator("admin");
        return access;
    }

    @After
    public void clearAll() {
        HugeGraph graph = graph();
        AuthManager authManager = graph.authManager();

        for (HugeUser user : authManager.listAllUsers(-1)) {
            if (user.name().equals("admin")) {
                continue;
            }
            authManager.deleteUser(user.id());
        }
        for (HugeGroup group : authManager.listAllGroups(-1)) {
            authManager.deleteGroup(group.id());
        }
        for (HugeTarget target : authManager.listAllTargets(-1)) {
            authManager.deleteTarget(target.id());
        }
        for (HugeBelong belong : authManager.listAllBelong(-1)) {
            authManager.deleteBelong(belong.id());
        }

        //FIXME: support project in hstore
        boolean isHstore = Objects.equals("hstore", System.getProperty("backend")) ||
                           (System.getProperty("backend") == null);

        if (!isHstore) {
            for (HugeProject project : authManager.listAllProject(-1)) {
                if (!CollectionUtils.isEmpty(project.graphs())) {
                    authManager.projectRemoveGraphs(project.id(), project.graphs());
                }
                authManager.deleteProject(project.id());
            }
        }

        Assert.assertEquals(0, authManager.listAllAccess(-1).size());
        Assert.assertEquals(0, authManager.listAllBelong(-1).size());
    }

    @Test
    public void testCreateUser() {
        HugeGraph graph = graph();
        AuthManager authManager = graph.authManager();

        Id id = authManager.createUser(makeUser("tom", "pass1"));

        HugeUser user = authManager.getUser(id);
        Assert.assertEquals("tom", user.name());
        Assert.assertEquals("pass1", user.password());
        Assert.assertEquals(user.create(), user.update());
        Assert.assertNull(user.phone());
        Assert.assertNull(user.email());
        Assert.assertNull(user.avatar());

        Map<String, Object> expected = new HashMap<>();
        expected.putAll(ImmutableMap.of("user_name", "tom",
                                        "user_password", "pass1",
                                        "user_creator", "admin"));
        expected.putAll(ImmutableMap.of("user_create", user.create(),
                                        "user_update", user.update(),
                                        "id", user.id()));

        Assert.assertEquals(expected, user.asMap());

        Assert.assertThrows(Exception.class, () -> {
            authManager.createUser(makeUser("tom", "pass1"));
        }, e -> {
            Assert.assertContains("exist", e.getMessage());
        });
    }

    @Test
    public void testCreateUserWithDetailsInfo() {
        HugeGraph graph = graph();
        AuthManager authManager = graph.authManager();

        HugeUser user = new HugeUser("james");
        user.password("pass2");
        user.phone("13812345678");
        user.email("test@hugegraph.com");
        user.avatar("http://image.hugegraph.com/image1");
        user.creator("admin");

        Id id = authManager.createUser(user);

        user = authManager.getUser(id);
        Assert.assertEquals("james", user.name());
        Assert.assertEquals("pass2", user.password());
        Assert.assertEquals(user.create(), user.update());
        Assert.assertEquals("13812345678", user.phone());
        Assert.assertEquals("test@hugegraph.com", user.email());
        Assert.assertEquals("http://image.hugegraph.com/image1", user.avatar());

        Map<String, Object> expected = new HashMap<>();
        expected.put("user_name", "james");
        expected.put("user_password", "pass2");
        expected.put("user_creator", "admin");
        expected.put("user_create", user.create());
        expected.put("user_update", user.update());
        expected.put("user_phone", user.phone());
        expected.put("user_email", user.email());
        expected.put("user_avatar", user.avatar());
        expected.put("id", user.id());
        Assert.assertEquals(expected, user.asMap());
    }

    @Test
    public void testListUsers() {
        HugeGraph graph = graph();
        AuthManager authManager = graph.authManager();

        Id id1 = authManager.createUser(makeUser("tom", "pass1"));
        Id id2 = authManager.createUser(makeUser("james", "pass2"));

        List<HugeUser> users = authManager.listUsers(ImmutableList.of(id1,
                                                                      id2));
        Assert.assertEquals(2, users.size());
        Assert.assertEquals("tom", users.get(0).name());
        Assert.assertEquals("james", users.get(1).name());

        users = authManager.listUsers(ImmutableList.of(id1, id2, id2));
        Assert.assertEquals(3, users.size());
        Assert.assertEquals("tom", users.get(0).name());
        Assert.assertEquals("james", users.get(1).name());
        Assert.assertEquals("james", users.get(2).name());

        users = authManager.listUsers(ImmutableList.of(id1, id2, id1));
        Assert.assertEquals(3, users.size());
        Assert.assertEquals("tom", users.get(0).name());
        Assert.assertEquals("james", users.get(1).name());
        Assert.assertEquals("tom", users.get(2).name());

        users = authManager.listUsers(ImmutableList.of(id1, id2,
                                                       IdGenerator.of("fake")));
        Assert.assertEquals(2, users.size());
    }

    @Test
    public void testListAllUsers() {
        HugeGraph graph = graph();
        AuthManager authManager = graph.authManager();

        authManager.createUser(makeUser("tom", "pass1"));
        authManager.createUser(makeUser("james", "pass2"));

        List<HugeUser> users = new ArrayList<>(authManager.listAllUsers(-1));

        // When hugegraphAuthProxy exists, admin will not be listed
        users.removeIf(u -> u.name().equals("admin"));
        Assert.assertEquals(2, users.size());
        Assert.assertEquals(ImmutableSet.of("tom", "james"),
                            ImmutableSet.of(users.get(0).name(),
                                            users.get(1).name()));

        Assert.assertEquals(0, authManager.listAllUsers(0).size());
        Assert.assertEquals(1, authManager.listAllUsers(1).size());
        Assert.assertEquals(2, authManager.listAllUsers(2).size());
    }

    @Test
    public void testGetUser() {
        HugeGraph graph = graph();
        AuthManager authManager = graph.authManager();

        Id id = authManager.createUser(makeUser("tom", "pass1"));

        HugeUser user = authManager.getUser(id);
        Assert.assertEquals("tom", user.name());
        Assert.assertEquals("pass1", user.password());

        Assert.assertThrows(Exception.class, () -> {
            authManager.getUser(IdGenerator.of("fake"));
        });

        Assert.assertThrows(Exception.class, () -> {
            authManager.getUser(null);
        });
    }

    @Test
    public void testMatchUser() {
        HugeGraph graph = graph();
        AuthManager authManager = graph.authManager();

        String password = StringEncoding.hashPassword("pass1");
        authManager.createUser(makeUser("tom", password));

        Assert.assertNotNull(authManager.matchUser("tom", "pass1"));
        Assert.assertNull(authManager.matchUser("tom", "pass2"));
        Assert.assertNull(authManager.matchUser("Tom", "pass1"));

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            authManager.matchUser("Tom", null);
        });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            authManager.matchUser(null, "pass1");
        });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            authManager.matchUser(null, null);
        });
    }

    @Test
    public void testUpdateUser() throws InterruptedException {
        HugeGraph graph = graph();
        AuthManager authManager = graph.authManager();

        Id id = authManager.createUser(makeUser("tom", "pass1"));
        HugeUser user = authManager.getUser(id);
        Assert.assertEquals("tom", user.name());
        Assert.assertEquals("pass1", user.password());
        Assert.assertEquals(user.create(), user.update());

        Date oldUpdateTime = user.update();
        Thread.sleep(1L);

        user.password("pass2");
        authManager.updateUser(user);

        HugeUser user2 = authManager.getUser(id);
        Assert.assertEquals("tom", user2.name());
        Assert.assertEquals("pass2", user2.password());
        Assert.assertEquals(oldUpdateTime, user2.create());
        Assert.assertNotEquals(oldUpdateTime, user2.update());

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            authManager.updateUser(makeUser("tom2", "pass1"));
        }, e -> {
            Assert.assertContains("exist", e.getMessage());
        });
    }

    @Test
    public void testDeleteUser() {
        HugeGraph graph = graph();
        AuthManager authManager = graph.authManager();

        Id id1 = authManager.createUser(makeUser("tom", "pass1"));
        Id id2 = authManager.createUser(makeUser("james", "pass2"));

        List<HugeUser> users = new ArrayList<>(authManager.listAllUsers(-1));
        users.removeIf(u -> u.name().equals("admin"));
        Assert.assertEquals(2, users.size());

        HugeUser user = authManager.deleteUser(id1);
        Assert.assertEquals("tom", user.name());
        users = new ArrayList<>(authManager.listAllUsers(-1));
        users.removeIf(u -> u.name().equals("admin"));
        Assert.assertEquals(1, users.size());
    }

    @Test
    public void testCreateGroup() {
        HugeGraph graph = graph();
        AuthManager authManager = graph.authManager();

        HugeGroup group = makeGroup("group1");
        Id id = authManager.createGroup(group);

        group = authManager.getGroup(id);
        Assert.assertEquals("group1", group.name());
        Assert.assertNull(group.description());
        Assert.assertEquals(group.create(), group.update());

        Assert.assertEquals(ImmutableMap.of("group_name", "group1",
                                            "group_create", group.create(),
                                            "group_update", group.update(),
                                            "group_creator", "admin",
                                            "id", group.id()),
                            group.asMap());

        group = makeGroup("group2");
        group.description("something");
        id = authManager.createGroup(group);

        group = authManager.getGroup(id);
        Assert.assertEquals("group2", group.name());
        Assert.assertEquals("something", group.description());
        Assert.assertEquals(group.create(), group.update());

        HashMap<String, Object> expected = new HashMap<>();
        expected.putAll(ImmutableMap.of("group_name", "group2",
                                        "group_description", "something",
                                        "group_creator", "admin"));
        expected.putAll(ImmutableMap.of("group_create", group.create(),
                                        "group_update", group.update(),
                                        "id", group.id()));

        Assert.assertEquals(expected, group.asMap());
    }

    @Test
    public void testListGroups() {
        HugeGraph graph = graph();
        AuthManager authManager = graph.authManager();

        Id id1 = authManager.createGroup(makeGroup("group1"));
        Id id2 = authManager.createGroup(makeGroup("group2"));

        List<HugeGroup> groups = authManager.listGroups(ImmutableList.of(id1,
                                                                         id2));
        Assert.assertEquals(2, groups.size());
        Assert.assertEquals("group1", groups.get(0).name());
        Assert.assertEquals("group2", groups.get(1).name());

        groups = authManager.listGroups(ImmutableList.of(id1, id2, id2));
        Assert.assertEquals(3, groups.size());
        Assert.assertEquals("group1", groups.get(0).name());
        Assert.assertEquals("group2", groups.get(1).name());
        Assert.assertEquals("group2", groups.get(2).name());

        groups = authManager.listGroups(ImmutableList.of(
                id1, id2, IdGenerator.of("fake")));
        Assert.assertEquals(2, groups.size());
    }

    @Test
    public void testListAllGroups() {
        HugeGraph graph = graph();
        AuthManager authManager = graph.authManager();

        authManager.createGroup(makeGroup("group1"));
        authManager.createGroup(makeGroup("group2"));

        List<HugeGroup> groups = authManager.listAllGroups(-1);
        Assert.assertEquals(2, groups.size());
        Assert.assertEquals(ImmutableSet.of("group1", "group2"),
                            ImmutableSet.of(groups.get(0).name(),
                                            groups.get(1).name()));

        Assert.assertEquals(0, authManager.listAllGroups(0).size());
        Assert.assertEquals(1, authManager.listAllGroups(1).size());
        Assert.assertEquals(2, authManager.listAllGroups(2).size());
        Assert.assertEquals(2, authManager.listAllGroups(3).size());
    }

    @Test
    public void testGetGroup() {
        HugeGraph graph = graph();
        AuthManager authManager = graph.authManager();

        Id id = authManager.createGroup(makeGroup("group-test"));
        HugeGroup group = authManager.getGroup(id);
        Assert.assertEquals("group-test", group.name());

        //FIXME: There are still many places where standAuthManager will throw exceptions, but
        // version v2 will return null
        //Assert.assertThrows(Exception.class, () -> {
        //    authManager.getGroup(IdGenerator.of("fake"));
        //});
        //
        //Assert.assertThrows(Exception.class, () -> {
        //    authManager.getGroup(null);
        //});
        //
        //Assert.assertThrows(IllegalArgumentException.class, () -> {
        //    Id user = authManager.createUser(makeUser("tom", "pass1"));
        //    authManager.getGroup(user);
        //});
    }

    @Test
    public void testUpdateGroup() throws InterruptedException {
        HugeGraph graph = graph();
        AuthManager authManager = graph.authManager();

        HugeGroup group = makeGroup("group2");
        group.description("description1");
        Id id = authManager.createGroup(group);

        group = authManager.getGroup(id);
        Assert.assertEquals("group2", group.name());
        Assert.assertEquals("description1", group.description());
        Assert.assertEquals(group.create(), group.update());

        Date oldUpdateTime = group.update();

        group.description("description2");
        //FIXME: It will take two seconds to update here in hstore
        Thread.sleep(2000L);
        authManager.updateGroup(group);
        HugeGroup group2 = authManager.getGroup(id);
        Assert.assertEquals("group2", group2.name());
        Assert.assertEquals("description2", group2.description());
        Assert.assertEquals(oldUpdateTime, group2.create());
        Assert.assertNotEquals(oldUpdateTime, group2.update());
    }

    @Test
    public void testDeleteGroup() {
        HugeGraph graph = graph();
        AuthManager authManager = graph.authManager();

        Id id1 = authManager.createGroup(makeGroup("group1"));
        Id id2 = authManager.createGroup(makeGroup("group2"));
        Assert.assertEquals(2, authManager.listAllGroups(-1).size());

        HugeGroup group = authManager.deleteGroup(id1);
        Assert.assertEquals("group1", group.name());
        Assert.assertEquals(1, authManager.listAllGroups(-1).size());

        group = authManager.deleteGroup(id2);
        Assert.assertEquals("group2", group.name());
        Assert.assertEquals(0, authManager.listAllGroups(-1).size());

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            Id user = authManager.createUser(makeUser("tom", "pass1"));
            authManager.deleteGroup(user);
        });
    }

    @Test
    public void testCreateTarget() {
        HugeGraph graph = graph();
        AuthManager authManager = graph.authManager();

        HugeTarget target = makeTarget("graph1", "127.0.0.1:8080");
        target.creator("admin");
        Id id = authManager.createTarget(target);

        target = authManager.getTarget(id);
        Assert.assertEquals("graph1", target.name());
        Assert.assertEquals("127.0.0.1:8080", target.url());
        Assert.assertEquals("DEFAULT", target.graphSpace());
        Assert.assertNull(target.description());
        Assert.assertEquals(target.create(), target.update());

        HashMap<String, Object> expected = new HashMap<>();
        expected.putAll(ImmutableMap.of("target_name", "graph1",
                                        "target_graph", "graph1",
                                        "target_url", "127.0.0.1:8080",
                                        "target_creator", "admin"));
        expected.putAll(ImmutableMap.of("target_create", target.create(),
                                        "target_update", target.update(),
                                        "id", target.id(),
                                        "graphspace", "DEFAULT"));

        Assert.assertEquals(expected, target.asMap());
    }

    @Test
    public void testTargetScopedRoundTripAfterAuthManagerReopen() {
        AuthManager authManager = graph().authManager();
        Assume.assumeTrue(authManager instanceof StandardAuthManager);

        HugeTarget target = makeTarget("target-scoped", "url");
        target.graphSpace("DEFAULT");
        target.description("description");
        Id id = authManager.createTarget("DEFAULT", target);

        StandardAuthManager reopened = new StandardAuthManager(params());
        reopened.init();
        HugeTarget stored = reopened.getTarget("DEFAULT", id);

        Assert.assertEquals("DEFAULT", stored.graphSpace());
        Assert.assertEquals("description", stored.description());
        reopened.close();
    }

    @Test
    public void testTargetSchemaUpgradeAddsNullableScopedProperties() {
        LegacyTargetSchema schema = new LegacyTargetSchema(params());
        schema.initLegacySchema();
        VertexLabel legacy = graph().vertexLabel(LegacyTargetSchema.LABEL);
        List<String> legacyProperties = graph().mapPkId2Name(
                                        legacy.properties());
        Assert.assertFalse(legacyProperties.contains(HugeTarget.P.GRAPHSPACE));
        Assert.assertFalse(legacyProperties.contains(
                HugeTarget.P.DESCRIPTION));

        schema.initSchemaIfNeeded();

        VertexLabel upgraded = graph().vertexLabel(LegacyTargetSchema.LABEL);
        Id graphSpace = graph().propertyKey(HugeTarget.P.GRAPHSPACE).id();
        Id description = graph().propertyKey(HugeTarget.P.DESCRIPTION).id();
        Assert.assertTrue(upgraded.properties().contains(graphSpace));
        Assert.assertTrue(upgraded.properties().contains(description));
        Assert.assertTrue(upgraded.nullableKeys().contains(graphSpace));
        Assert.assertTrue(upgraded.nullableKeys().contains(description));
    }

    @Test
    public void testStandaloneScopedOverloadsRejectForeignGraphSpace() {
        AuthManager authManager = graph().authManager();
        Assume.assumeTrue(authManager instanceof StandardAuthManager);
        authManager.init();

        Id fake = IdGenerator.of("fake");
        HugeTarget target = makeTarget("foreign-target", "url");
        HugeBelong belong = makeBelong(fake, fake);
        HugeAccess access = makeAccess(fake, fake, HugePermission.READ);
        List<Runnable> operations = ImmutableList.of(
                () -> authManager.createTarget("FOREIGN", target),
                () -> authManager.updateTarget("FOREIGN", target),
                () -> authManager.deleteTarget("FOREIGN", fake),
                () -> authManager.getTarget("FOREIGN", fake),
                () -> authManager.listAllTargets("FOREIGN", -1),
                () -> authManager.createBelong("FOREIGN", belong),
                () -> authManager.updateBelong("FOREIGN", belong),
                () -> authManager.deleteBelong("FOREIGN", fake),
                () -> authManager.getBelong("FOREIGN", fake),
                () -> authManager.listAllBelong("FOREIGN", -1),
                () -> authManager.listBelongByUser("FOREIGN", fake, -1),
                () -> authManager.listBelongByGroup("FOREIGN", fake, -1),
                () -> authManager.createAccess("FOREIGN", access),
                () -> authManager.updateAccess("FOREIGN", access),
                () -> authManager.deleteAccess("FOREIGN", fake),
                () -> authManager.getAccess("FOREIGN", fake),
                () -> authManager.listAllAccess("FOREIGN", -1),
                () -> authManager.listAccessByGroup("FOREIGN", fake, -1),
                () -> authManager.listAccessByTarget("FOREIGN", fake, -1));

        for (Runnable operation : operations) {
            Assert.assertThrows(IllegalArgumentException.class,
                                operation::run,
                                e -> Assert.assertContains("DEFAULT",
                                                           e.getMessage()));
        }
        Assert.assertEquals(0, authManager.listAllTargets(-1).size());
        Assert.assertEquals(0, authManager.listAllBelong(-1).size());
        Assert.assertEquals(0, authManager.listAllAccess(-1).size());
    }

    @Test
    public void testStandaloneScopedWritesRejectForeignEntityGraphSpace() {
        AuthManager authManager = graph().authManager();
        Assume.assumeTrue(authManager instanceof StandardAuthManager);
        authManager.init();

        Id fake = IdGenerator.of("fake");
        HugeTarget target = makeTarget("foreign-target", "url");
        target.graphSpace("FOREIGN");
        HugeBelong belong = new HugeBelong("FOREIGN", fake, fake,
                                            null, HugeBelong.UG);
        HugeAccess access = new HugeAccess("FOREIGN", fake, fake,
                                           HugePermission.READ);
        List<Runnable> operations = ImmutableList.of(
                () -> authManager.createTarget("DEFAULT", target),
                () -> authManager.updateTarget("DEFAULT", target),
                () -> authManager.createBelong("DEFAULT", belong),
                () -> authManager.updateBelong("DEFAULT", belong),
                () -> authManager.createAccess("DEFAULT", access),
                () -> authManager.updateAccess("DEFAULT", access));

        for (Runnable operation : operations) {
            Assert.assertThrows(IllegalArgumentException.class,
                                operation::run,
                                e -> Assert.assertContains("DEFAULT",
                                                           e.getMessage()));
        }
        Assert.assertEquals(0, authManager.listAllTargets(-1).size());
        Assert.assertEquals(0, authManager.listAllBelong(-1).size());
        Assert.assertEquals(0, authManager.listAllAccess(-1).size());
    }

    @Test
    public void testStandaloneScopedOverloadsPreserveDefaultCompatibility() {
        AuthManager authManager = graph().authManager();
        Assume.assumeTrue(authManager instanceof StandardAuthManager);
        authManager.init();

        Id user = authManager.createUser(makeUser("default-user", "pass"));
        Id group = authManager.createGroup(makeGroup("default-group"));
        HugeTarget target = makeTarget("default-target", "url");
        Id targetId = authManager.createTarget("DEFAULT", target);
        HugeBelong belong = makeBelong(user, group);
        Id belongId = authManager.createBelong("DEFAULT", belong);
        HugeAccess access = makeAccess(group, targetId, HugePermission.READ);
        Id accessId = authManager.createAccess("DEFAULT", access);

        Assert.assertEquals(targetId,
                            authManager.getTarget("DEFAULT", targetId).id());
        Assert.assertEquals(belongId,
                            authManager.getBelong("DEFAULT", belongId).id());
        Assert.assertEquals(accessId,
                            authManager.getAccess("DEFAULT", accessId).id());
        Assert.assertEquals(1,
                            authManager.listAllTargets("DEFAULT", -1).size());
        Assert.assertEquals(1,
                            authManager.listAllBelong("DEFAULT", -1).size());
        Assert.assertEquals(1,
                            authManager.listAllAccess("DEFAULT", -1).size());
    }

    @Test
    public void testStandaloneScopedReadsHidePreexistingForeignTarget() {
        AuthManager authManager = graph().authManager();
        Assume.assumeTrue(authManager instanceof StandardAuthManager);
        authManager.init();

        HugeTarget target = makeTarget("legacy-foreign-target", "url");
        target.graphSpace("FOREIGN");
        Id id = authManager.createTarget(target);

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            authManager.getTarget("DEFAULT", id);
        });
        Assert.assertEquals(0,
                            authManager.listAllTargets("DEFAULT", -1).size());
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            authManager.deleteTarget("DEFAULT", id);
        });
        Assert.assertNotNull(authManager.getTarget(id));
    }

    private static final class LegacyTargetSchema extends HugeTarget.Schema {

        private static final String LABEL = HugeTarget.P.TARGET +
                                            "_upgrade_test";

        private LegacyTargetSchema(HugeGraphParams graph) {
            super(graph, LABEL);
        }

        private void initLegacySchema() {
            List<String> properties = new ArrayList<>();
            properties.add(createPropertyKey(HugeTarget.P.NAME));
            properties.add(createPropertyKey(HugeTarget.P.GRAPH));
            properties.add(createPropertyKey(HugeTarget.P.URL));
            properties.add(createPropertyKey(HugeTarget.P.RESS));
            String[] all = super.initProperties(properties);

            VertexLabel label = this.schema()
                                    .vertexLabel(LABEL)
                                    .properties(all)
                                    .usePrimaryKeyId()
                                    .primaryKeys(HugeTarget.P.NAME)
                                    .nullableKeys(HugeTarget.P.RESS)
                                    .enableLabelIndex(true)
                                    .build();
            this.graph.schemaTransaction().addVertexLabel(label);
        }
    }

    @Test
    public void testCreateTargetWithRess() {
        HugeGraph graph = graph();
        AuthManager authManager = graph.authManager();

        String ress = "[{\"type\": \"VERTEX\", \"label\": \"person\", " +
                      "\"properties\":{\"city\": \"Beijing\"}}, " +
                      "{\"type\": \"EDGE\", \"label\": \"transfer\"}]";
        HugeTarget target = makeTarget("graph1", "127.0.0.1:8080");
        target.resources(ress);
        Id id = authManager.createTarget(target);

        target = authManager.getTarget(id);
        Assert.assertEquals("graph1", target.name());
        Assert.assertEquals("127.0.0.1:8080", target.url());
        Assert.assertEquals(target.create(), target.update());

        String expect =
                "{\"VERTEX#person\":[{\"type\":\"VERTEX\",\"label\":\"person\"," +
                "\"properties\":{\"city\":\"Beijing\"}}],\"EDGE#transfer\":[{\"type\":\"EDGE\"," +
                "\"label\":\"transfer\",\"properties\":null}]}";
        Assert.assertEquals(expect, JsonUtil.toJson(target.asMap()
                                                          .get("target_resources")));
    }

    @Test
    public void testListTargets() {
        HugeGraph graph = graph();
        AuthManager authManager = graph.authManager();

        Id id1 = authManager.createTarget(makeTarget("target1", "url1"));
        Id id2 = authManager.createTarget(makeTarget("target2", "url2"));

        List<HugeTarget> targets = authManager.listTargets(ImmutableList.of(
                id1, id2));
        Assert.assertEquals(2, targets.size());
        Assert.assertEquals("target1", targets.get(0).name());
        Assert.assertEquals("target2", targets.get(1).name());

        targets = authManager.listTargets(ImmutableList.of(id1, id2, id2));
        Assert.assertEquals(3, targets.size());
        Assert.assertEquals("target1", targets.get(0).name());
        Assert.assertEquals("target2", targets.get(1).name());
        Assert.assertEquals("target2", targets.get(2).name());

        targets = authManager.listTargets(ImmutableList.of(
                id1, id2, IdGenerator.of("fake")));
        Assert.assertEquals(2, targets.size());
    }

    @Test
    public void testListAllTargets() {
        HugeGraph graph = graph();
        AuthManager authManager = graph.authManager();

        authManager.createTarget(makeTarget("target1", "url1"));
        authManager.createTarget(makeTarget("target2", "url1"));

        List<HugeTarget> targets = authManager.listAllTargets(-1);
        Assert.assertEquals(2, targets.size());
        Assert.assertEquals(ImmutableSet.of("target1", "target2"),
                            ImmutableSet.of(targets.get(0).name(),
                                            targets.get(1).name()));

        Assert.assertEquals(0, authManager.listAllTargets(0).size());
        Assert.assertEquals(1, authManager.listAllTargets(1).size());
        Assert.assertEquals(2, authManager.listAllTargets(2).size());
        Assert.assertEquals(2, authManager.listAllTargets(3).size());
    }

    @Test
    public void testGetTarget() {
        HugeGraph graph = graph();
        AuthManager authManager = graph.authManager();

        Id id = authManager.createTarget(makeTarget("target-test", "url1"));
        HugeTarget target = authManager.getTarget(id);
        Assert.assertEquals("target-test", target.name());

        Assert.assertThrows(Exception.class, () -> {
            authManager.getTarget(IdGenerator.of("fake"));
        });

        Assert.assertThrows(Exception.class, () -> {
            authManager.getTarget(null);
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            Id user = authManager.createUser(makeUser("tom", "pass1"));
            authManager.getTarget(user);
        });
    }

    @Test
    public void testUpdateTarget() throws InterruptedException {
        HugeGraph graph = graph();
        AuthManager authManager = graph.authManager();

        HugeTarget target = makeTarget("target1", "url1");
        Id id = authManager.createTarget(target);

        target = authManager.getTarget(id);
        Assert.assertEquals("target1", target.name());
        Assert.assertEquals("url1", target.url());
        Assert.assertEquals(target.create(), target.update());

        Date oldUpdateTime = target.update();
        Thread.sleep(1L);

        target.url("url2");
        authManager.updateTarget(target);

        HugeTarget target2 = authManager.getTarget(id);
        Assert.assertEquals("target1", target2.name());
        Assert.assertEquals("url2", target2.url());
        Assert.assertEquals(oldUpdateTime, target2.create());
        Assert.assertNotEquals(oldUpdateTime, target2.update());
    }

    @Test
    public void testDeleteTarget() {
        HugeGraph graph = graph();
        AuthManager authManager = graph.authManager();

        Id id1 = authManager.createTarget(makeTarget("target1", "url1"));
        Id id2 = authManager.createTarget(makeTarget("target2", "url2"));
        Assert.assertEquals(2, authManager.listAllTargets(-1).size());

        HugeTarget target = authManager.deleteTarget(id1);
        Assert.assertEquals("target1", target.name());
        Assert.assertEquals(1, authManager.listAllTargets(-1).size());

        target = authManager.deleteTarget(id2);
        Assert.assertEquals("target2", target.name());
        Assert.assertEquals(0, authManager.listAllTargets(-1).size());

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            Id user = authManager.createUser(makeUser("tom", "pass1"));
            authManager.deleteTarget(user);
        });
    }

    @Test
    public void testCreateBelong() {
        HugeGraph graph = graph();
        AuthManager authManager = graph.authManager();

        Id user = authManager.createUser(makeUser("tom", "pass1"));
        Id group1 = authManager.createGroup(makeGroup("group1"));
        Id group2 = authManager.createGroup(makeGroup("group2"));

        Id id1 = authManager.createBelong(makeBelong(user, group1));
        Id id2 = authManager.createBelong(makeBelong(user, group2));

        HugeBelong belong = authManager.getBelong(id1);
        Assert.assertEquals(user, belong.source());
        Assert.assertEquals(group1, belong.target());
        Assert.assertNull(belong.description());
        Assert.assertEquals(belong.create(), belong.update());

        Map<String, Object> expected = new HashMap<>();
        expected.putAll(ImmutableMap.of("id", belong.id(),
                                        "user", user,
                                        "graphspace", "DEFAULT",
                                        "group", group1));
        expected.putAll(ImmutableMap.of("belong_creator", "admin",
                                        "belong_create", belong.create(),
                                        "belong_update", belong.update()));
        expected.put("link", "ug");
        Assert.assertEquals(expected, belong.asMap());

        belong = authManager.getBelong(id2);
        Assert.assertEquals(user, belong.source());
        Assert.assertEquals(group2, belong.target());
        Assert.assertNull(belong.description());
        Assert.assertEquals(belong.create(), belong.update());

        expected = new HashMap<>();
        expected.putAll(ImmutableMap.of("id", belong.id(),
                                        "user", user,
                                        "graphspace", "DEFAULT",
                                        "group", group2));
        expected.putAll(ImmutableMap.of("belong_creator", "admin",
                                        "belong_create", belong.create(),
                                        "belong_update", belong.update()));
        expected.put("link", "ug");
        Assert.assertEquals(expected, belong.asMap());

        List<HugeBelong> belongs = authManager.listBelongByUser(user, -1);
        Assert.assertEquals(2, belongs.size());

        belongs = authManager.listBelongByGroup(group1, -1);
        Assert.assertEquals(1, belongs.size());

        belongs = authManager.listBelongByGroup(group2, -1);
        Assert.assertEquals(1, belongs.size());

        // Create belong with description
        Id user1 = authManager.createUser(makeUser("user1", "pass1"));
        belong = makeBelong(user1, group1);
        belong.description("something2");
        Id id3 = authManager.createBelong(belong);
        belong = authManager.getBelong(id3);
        Assert.assertEquals(user1, belong.source());
        Assert.assertEquals(group1, belong.target());
        Assert.assertEquals("something2", belong.description());
        Assert.assertEquals(belong.create(), belong.update());

        expected = new HashMap<>();
        expected.putAll(ImmutableMap.of("id", belong.id(),
                                        "user", user1,
                                        "graphspace", "DEFAULT",
                                        "group", group1));
        expected.putAll(ImmutableMap.of("belong_description", "something2",
                                        "belong_creator", "admin",
                                        "belong_create", belong.create(),
                                        "belong_update", belong.update()));
        expected.put("link", "ug");
        Assert.assertEquals(expected, belong.asMap());

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            authManager.createBelong(makeBelong(user, group1));
        }, e -> {
            String message = e.getMessage();
            boolean containsExpected = message.contains("Can't save") ||
                                       message.contains("exist");
            Assert.assertTrue(containsExpected);
        });
    }

    @Test
    public void testListBelong() {
        HugeGraph graph = graph();
        AuthManager authManager = graph.authManager();

        Id user = authManager.createUser(makeUser("tom", "pass1"));
        Id group1 = authManager.createGroup(makeGroup("group1"));
        Id group2 = authManager.createGroup(makeGroup("group2"));

        Id id1 = authManager.createBelong(makeBelong(user, group1));
        Id id2 = authManager.createBelong(makeBelong(user, group2));

        List<HugeBelong> belongs = authManager.listBelong(ImmutableList.of(
                id1, id2));
        Assert.assertEquals(2, belongs.size());
        Assert.assertEquals(user, belongs.get(0).source());
        Assert.assertEquals(user, belongs.get(1).source());
        Assert.assertEquals(group1, belongs.get(0).target());
        Assert.assertEquals(group2, belongs.get(1).target());

        belongs = authManager.listBelong(ImmutableList.of(id1, id2, id2));
        Assert.assertEquals(3, belongs.size());

        belongs = authManager.listBelong(ImmutableList.of(
                id1, id2, IdGenerator.of("fake")));
        Assert.assertEquals(2, belongs.size());

        belongs = authManager.listBelongByUser(user, -1);
        Assert.assertEquals(2, belongs.size());
        Assert.assertEquals(user, belongs.get(0).source());
        Assert.assertEquals(user, belongs.get(1).source());

        belongs = authManager.listBelongByGroup(group1, -1);
        Assert.assertEquals(1, belongs.size());
        Assert.assertEquals(user, belongs.get(0).source());
        Assert.assertEquals(group1, belongs.get(0).target());

        belongs = authManager.listBelongByGroup(group2, -1);
        Assert.assertEquals(1, belongs.size());
        Assert.assertEquals(user, belongs.get(0).source());
        Assert.assertEquals(group2, belongs.get(0).target());
    }

    @Test
    public void testListAllBelong() {
        HugeGraph graph = graph();
        AuthManager authManager = graph.authManager();

        Id user = authManager.createUser(makeUser("tom", "pass1"));
        Id group1 = authManager.createGroup(makeGroup("group1"));
        Id group2 = authManager.createGroup(makeGroup("group2"));

        authManager.createBelong(makeBelong(user, group1));
        authManager.createBelong(makeBelong(user, group2));

        List<HugeBelong> belongs = authManager.listAllBelong(-1);
        Assert.assertEquals(2, belongs.size());
        Assert.assertEquals(ImmutableSet.of(group1, group2),
                            ImmutableSet.of(belongs.get(0).target(),
                                            belongs.get(1).target()));

        Assert.assertEquals(0, authManager.listAllBelong(0).size());
        Assert.assertEquals(1, authManager.listAllBelong(1).size());
        Assert.assertEquals(2, authManager.listAllBelong(2).size());
        Assert.assertEquals(2, authManager.listAllBelong(3).size());
    }

    @Test
    public void testGetBelong() {
        HugeGraph graph = graph();
        AuthManager authManager = graph.authManager();

        Id user = authManager.createUser(makeUser("tom", "pass1"));
        Id group1 = authManager.createGroup(makeGroup("group1"));
        Id group2 = authManager.createGroup(makeGroup("group2"));

        Id id1 = authManager.createBelong(makeBelong(user, group1));
        Id id2 = authManager.createBelong(makeBelong(user, group2));

        HugeBelong belong1 = authManager.getBelong(id1);
        Assert.assertEquals(group1, belong1.target());

        HugeBelong belong2 = authManager.getBelong(id2);
        Assert.assertEquals(group2, belong2.target());

        Assert.assertThrows(Exception.class, () -> {
            authManager.getBelong(IdGenerator.of("fake"));
        });

        Assert.assertThrows(Exception.class, () -> {
            authManager.getBelong(null);
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            Id target = authManager.createTarget(makeTarget("graph1", ""));
            Id access = authManager.createAccess(makeAccess(group1, target,
                                                            HugePermission.READ));
            authManager.getBelong(access);
        });
    }

    @Test
    public void testUpdateBelong() throws InterruptedException {
        HugeGraph graph = graph();
        AuthManager authManager = graph.authManager();

        Id user = authManager.createUser(makeUser("tom", "pass1"));
        Id group = authManager.createGroup(makeGroup("group1"));

        HugeBelong belong = makeBelong(user, group);
        belong.description("description1");
        Id id = authManager.createBelong(belong);

        belong = authManager.getBelong(id);
        Assert.assertEquals(user, belong.source());
        Assert.assertEquals(group, belong.target());
        Assert.assertEquals("description1", belong.description());
        Assert.assertEquals(belong.create(), belong.update());

        Date oldUpdateTime = belong.update();
        Thread.sleep(1L);

        belong.description("description2");
        authManager.updateBelong(belong);

        HugeBelong belong2 = authManager.getBelong(id);
        Assert.assertEquals(user, belong.source());
        Assert.assertEquals(group, belong.target());
        Assert.assertEquals("description2", belong.description());
        Assert.assertEquals(oldUpdateTime, belong2.create());
        Assert.assertNotEquals(oldUpdateTime, belong2.update());

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            Id group2 = authManager.createGroup(makeGroup("group2"));
            HugeBelong belong3 = makeBelong(user, group2);
            authManager.updateBelong(belong3);
        }, e -> {
            String message = e.getMessage();
            boolean containsExpected = message.contains("Can't save access") ||
                                       message.contains("not exist");
            Assert.assertTrue(containsExpected);
        });
    }

    @Test
    public void testDeleteBelong() {
        HugeGraph graph = graph();
        AuthManager authManager = graph.authManager();

        Id user = authManager.createUser(makeUser("tom", "pass1"));
        Id group1 = authManager.createGroup(makeGroup("group1"));
        Id group2 = authManager.createGroup(makeGroup("group2"));

        Id id1 = authManager.createBelong(makeBelong(user, group1));
        Id id2 = authManager.createBelong(makeBelong(user, group2));

        Assert.assertEquals(2, authManager.listAllBelong(-1).size());

        HugeBelong belong = authManager.deleteBelong(id1);
        Assert.assertEquals(group1, belong.target());
        Assert.assertEquals(1, authManager.listAllBelong(-1).size());
        Assert.assertEquals(1, authManager.listAllBelong(2).size());

        belong = authManager.deleteBelong(id2);
        Assert.assertEquals(group2, belong.target());
        Assert.assertEquals(0, authManager.listAllBelong(-1).size());

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            Id target = authManager.createTarget(makeTarget("graph1", ""));
            Id access = authManager.createAccess(makeAccess(group1, target,
                                                            HugePermission.READ));
            authManager.deleteBelong(access);
        });
    }

    @Test
    public void testCreateAccess() {
        HugeGraph graph = graph();
        AuthManager authManager = graph.authManager();

        Id group1 = authManager.createGroup(makeGroup("group1"));
        Id group2 = authManager.createGroup(makeGroup("group2"));
        Id target1 = authManager.createTarget(makeTarget("graph1", "url1"));
        Id target2 = authManager.createTarget(makeTarget("graph2", "url2"));

        Id id1 = authManager.createAccess(makeAccess(group1, target1,
                                                     HugePermission.READ));
        Id id2 = authManager.createAccess(makeAccess(group1, target1,
                                                     HugePermission.WRITE));
        Id id3 = authManager.createAccess(makeAccess(group1, target2,
                                                     HugePermission.READ));
        Id id4 = authManager.createAccess(makeAccess(group2, target2,
                                                     HugePermission.READ));

        HugeAccess access = authManager.getAccess(id1);
        Assert.assertEquals(group1, access.source());
        Assert.assertEquals(target1, access.target());
        Assert.assertEquals(HugePermission.READ, access.permission());
        Assert.assertEquals(access.create(), access.update());

        Map<String, Object> expected = new HashMap<>();
        expected.putAll(ImmutableMap.of("id", access.id(),
                                        "group", group1,
                                        "target", target1,
                                        "access_permission",
                                        HugePermission.READ,
                                        "access_creator", "admin"));
        expected.putAll(ImmutableMap.of("access_create", access.create(),
                                        "graphspace", "DEFAULT",
                                        "access_update", access.update()));
        Assert.assertEquals(expected, access.asMap());

        access = authManager.getAccess(id2);
        Assert.assertEquals(group1, access.source());
        Assert.assertEquals(target1, access.target());
        Assert.assertEquals(HugePermission.WRITE, access.permission());
        Assert.assertEquals(access.create(), access.update());

        expected = new HashMap<>();
        expected.putAll(ImmutableMap.of("id", access.id(),
                                        "group", group1,
                                        "target", target1,
                                        "access_permission",
                                        HugePermission.WRITE,
                                        "access_creator", "admin"));
        expected.putAll(ImmutableMap.of("access_create", access.create(),
                                        "graphspace", "DEFAULT",
                                        "access_update", access.update()));
        Assert.assertEquals(expected, access.asMap());

        access = authManager.getAccess(id3);
        Assert.assertEquals(group1, access.source());
        Assert.assertEquals(target2, access.target());
        Assert.assertEquals(HugePermission.READ, access.permission());
        Assert.assertEquals(access.create(), access.update());

        expected = new HashMap<>();
        expected.putAll(ImmutableMap.of("id", access.id(),
                                        "group", group1,
                                        "target", target2,
                                        "access_permission",
                                        HugePermission.READ,
                                        "access_creator", "admin"));
        expected.putAll(ImmutableMap.of("access_create", access.create(),
                                        "graphspace", "DEFAULT",
                                        "access_update", access.update()));
        Assert.assertEquals(expected, access.asMap());

        access = authManager.getAccess(id4);
        Assert.assertEquals(group2, access.source());
        Assert.assertEquals(target2, access.target());
        Assert.assertEquals(HugePermission.READ, access.permission());
        Assert.assertEquals(access.create(), access.update());

        expected = new HashMap<>();
        expected.putAll(ImmutableMap.of("id", access.id(),
                                        "group", group2,
                                        "target", target2,
                                        "access_permission",
                                        HugePermission.READ,
                                        "access_creator", "admin"));
        expected.putAll(ImmutableMap.of("access_create", access.create(),
                                        "graphspace", "DEFAULT",
                                        "access_update", access.update()));
        Assert.assertEquals(expected, access.asMap());

        List<HugeAccess> accesses = authManager.listAccessByGroup(group1, -1);
        Assert.assertEquals(3, accesses.size());

        accesses = authManager.listAccessByGroup(group2, -1);
        Assert.assertEquals(1, accesses.size());

        accesses = authManager.listAccessByTarget(target1, -1);
        Assert.assertEquals(2, accesses.size());

        accesses = authManager.listAccessByTarget(target2, -1);
        Assert.assertEquals(2, accesses.size());

        // Create access with description
        access = makeAccess(group2, target2, HugePermission.WRITE);
        access.description("something3");
        Id id5 = authManager.createAccess(access);
        access = authManager.getAccess(id5);
        Assert.assertEquals(group2, access.source());
        Assert.assertEquals(target2, access.target());
        Assert.assertEquals(HugePermission.WRITE, access.permission());
        Assert.assertEquals("something3", access.description());
        Assert.assertEquals(access.create(), access.update());

        expected = new HashMap<>();
        expected.putAll(ImmutableMap.of("id", access.id(),
                                        "group", group2,
                                        "target", target2,
                                        "access_permission",
                                        HugePermission.WRITE,
                                        "access_creator", "admin"));
        expected.putAll(ImmutableMap.of("access_description", "something3",
                                        "graphspace", "DEFAULT",
                                        "access_create", access.create(),
                                        "access_update", access.update()));
        Assert.assertEquals(expected, access.asMap());

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            authManager.createAccess(makeAccess(group1, target1,
                                                HugePermission.READ));
        }, e -> {
            String message = e.getMessage();
            boolean containsExpected = message.contains("Can't save access") ||
                                       message.contains("has exist");
            Assert.assertTrue(containsExpected);
        });
    }

    @Test
    public void testListAccess() {
        HugeGraph graph = graph();
        AuthManager authManager = graph.authManager();

        Id group = authManager.createGroup(makeGroup("group1"));
        Id target1 = authManager.createTarget(makeTarget("graph1", "url1"));
        Id target2 = authManager.createTarget(makeTarget("graph2", "url2"));

        Id id1 = authManager.createAccess(makeAccess(group, target1,
                                                     HugePermission.READ));
        Id id2 = authManager.createAccess(makeAccess(group, target2,
                                                     HugePermission.READ));

        List<HugeAccess> access = authManager.listAccess(ImmutableList.of(
                id1, id2));
        Assert.assertEquals(2, access.size());
        Assert.assertEquals(group, access.get(0).source());
        Assert.assertEquals(group, access.get(1).source());
        Assert.assertEquals(target1, access.get(0).target());
        Assert.assertEquals(target2, access.get(1).target());

        access = authManager.listAccess(ImmutableList.of(id1, id2, id2));
        Assert.assertEquals(3, access.size());

        access = authManager.listAccess(ImmutableList.of(
                id1, id2, IdGenerator.of("fake")));
        Assert.assertEquals(2, access.size());

        access = authManager.listAccessByGroup(group, -1);
        Assert.assertEquals(2, access.size());
        Assert.assertEquals(group, access.get(0).source());
        Assert.assertEquals(group, access.get(1).source());

        access = authManager.listAccessByTarget(target1, -1);
        Assert.assertEquals(1, access.size());
        Assert.assertEquals(group, access.get(0).source());
        Assert.assertEquals(target1, access.get(0).target());

        access = authManager.listAccessByTarget(target2, -1);
        Assert.assertEquals(1, access.size());
        Assert.assertEquals(group, access.get(0).source());
        Assert.assertEquals(target2, access.get(0).target());
    }

    @Test
    public void testListAllAccess() {
        HugeGraph graph = graph();
        AuthManager authManager = graph.authManager();

        Id group = authManager.createGroup(makeGroup("group1"));
        Id target1 = authManager.createTarget(makeTarget("graph1", "url1"));
        Id target2 = authManager.createTarget(makeTarget("graph2", "url2"));

        authManager.createAccess(makeAccess(group, target1,
                                            HugePermission.READ));
        authManager.createAccess(makeAccess(group, target2,
                                            HugePermission.READ));

        List<HugeAccess> access = authManager.listAllAccess(-1);
        Assert.assertEquals(2, access.size());
        Assert.assertEquals(ImmutableSet.of(target1, target2),
                            ImmutableSet.of(access.get(0).target(),
                                            access.get(1).target()));

        Assert.assertEquals(0, authManager.listAllAccess(0).size());
        Assert.assertEquals(1, authManager.listAllAccess(1).size());
        Assert.assertEquals(2, authManager.listAllAccess(2).size());
        Assert.assertEquals(2, authManager.listAllAccess(3).size());
    }

    @Test
    public void testGetAccess() {
        HugeGraph graph = graph();
        AuthManager authManager = graph.authManager();

        Id group = authManager.createGroup(makeGroup("group1"));
        Id target1 = authManager.createTarget(makeTarget("graph1", "url1"));
        Id target2 = authManager.createTarget(makeTarget("graph2", "url2"));

        Id id1 = authManager.createAccess(makeAccess(group, target1,
                                                     HugePermission.READ));
        Id id2 = authManager.createAccess(makeAccess(group, target2,
                                                     HugePermission.READ));

        HugeAccess access1 = authManager.getAccess(id1);
        Assert.assertEquals(target1, access1.target());

        HugeAccess access2 = authManager.getAccess(id2);
        Assert.assertEquals(target2, access2.target());

        Assert.assertThrows(Exception.class, () -> {
            authManager.getAccess(IdGenerator.of("fake"));
        });

        Assert.assertThrows(Exception.class, () -> {
            authManager.getAccess(null);
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            Id user = authManager.createUser(makeUser("tom", "pass1"));
            Id belong = authManager.createBelong(makeBelong(user, group));
            authManager.getAccess(belong);
        });
    }

    @Test
    public void testUpdateAccess() throws InterruptedException {
        HugeGraph graph = graph();
        AuthManager authManager = graph.authManager();

        Id group = authManager.createGroup(makeGroup("group1"));
        Id target = authManager.createTarget(makeTarget("graph1", "url1"));
        Id id = authManager.createAccess(makeAccess(group, target,
                                                    HugePermission.READ));

        HugeAccess access = authManager.getAccess(id);
        Assert.assertEquals(group, access.source());
        Assert.assertEquals(target, access.target());
        Assert.assertEquals(HugePermission.READ, access.permission());
        Assert.assertEquals(access.create(), access.update());

        Date oldUpdateTime = access.update();
        Thread.sleep(1L);

        access.permission(HugePermission.READ);
        authManager.updateAccess(access);

        HugeAccess access2 = authManager.getAccess(id);
        Assert.assertEquals(group, access.source());
        Assert.assertEquals(target, access.target());
        Assert.assertEquals(HugePermission.READ, access.permission());
        Assert.assertEquals(oldUpdateTime, access2.create());
        Assert.assertNotEquals(oldUpdateTime, access2.update());

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            access.permission(HugePermission.WRITE);
            authManager.updateAccess(access);
        }, e -> {
            String message = e.getMessage();
            boolean containsExpected = message.contains("Can't save access") ||
                                       message.contains("not exist");
            Assert.assertTrue(containsExpected);
        });

        access.permission(HugePermission.READ);
        access.description("description updated");
        id = authManager.updateAccess(access);

        HugeAccess access3 = authManager.getAccess(id);
        Assert.assertEquals(group, access3.source());
        Assert.assertEquals(target, access3.target());
        Assert.assertEquals("description updated", access3.description());
        Assert.assertEquals(HugePermission.READ, access3.permission());
        Assert.assertEquals(oldUpdateTime, access3.create());
        Assert.assertNotEquals(access3.create(), access3.update());

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            HugeAccess access4 = makeAccess(group, target,
                                            HugePermission.DELETE);
            authManager.updateAccess(access4);
        }, e -> {
            String message = e.getMessage();
            boolean containsExpected = message.contains("Can't save access") ||
                                       message.contains("not exist");
            Assert.assertTrue(containsExpected);
        });
    }

    @Test
    public void testDeleteAccess() {
        HugeGraph graph = graph();
        AuthManager authManager = graph.authManager();

        Id group = authManager.createGroup(makeGroup("group1"));
        Id target1 = authManager.createTarget(makeTarget("graph1", "url1"));
        Id target2 = authManager.createTarget(makeTarget("graph2", "url2"));

        Id id1 = authManager.createAccess(makeAccess(group, target1,
                                                     HugePermission.READ));
        Id id2 = authManager.createAccess(makeAccess(group, target2,
                                                     HugePermission.READ));

        Assert.assertEquals(2, authManager.listAllAccess(-1).size());

        HugeAccess access = authManager.deleteAccess(id1);
        Assert.assertEquals(target1, access.target());
        Assert.assertEquals(1, authManager.listAllAccess(-1).size());
        Assert.assertEquals(1, authManager.listAllAccess(2).size());

        access = authManager.deleteAccess(id2);
        Assert.assertEquals(target2, access.target());
        Assert.assertEquals(0, authManager.listAllAccess(-1).size());

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            Id user = authManager.createUser(makeUser("tom", "pass1"));
            Id belong = authManager.createBelong(makeBelong(user, group));
            authManager.deleteAccess(belong);
        });
    }

    @Test
    public void testRolePermission() {
        HugeGraph graph = graph();
        AuthManager authManager = graph.authManager();

        Id user0 = authManager.createUser(makeUser("hugegraph", "p0"));
        Id user1 = authManager.createUser(makeUser("hugegraph1", "p1"));

        Id group1 = authManager.createGroup(makeGroup("group1"));
        Id group2 = authManager.createGroup(makeGroup("group2"));

        Id graph1 = authManager.createTarget(makeTarget("hugegraph", "url1"));
        Id graph2 = authManager.createTarget(makeTarget("hugegraph1", "url2"));

        Map<String, List<HugeResource>> rv = HugeResource.parseResources(
                "[{\"type\": \"VERTEX\", \"label\": \"person\", " +
                "\"properties\":{\"city\": \"Beijing\", \"age\": \"P.gte(20)\"}}," +
                " {\"type\": \"VERTEX_LABEL\", \"label\": \"*\"}," +
                " {\"type\": \"PROPERTY_KEY\", \"label\": \"*\"}]");
        Map<String, List<HugeResource>> re = HugeResource.parseResources(
                "[{\"type\": \"EDGE\", \"label\": \"write\"}, " +
                " {\"type\": \"PROPERTY_KEY\"}, {\"type\": \"VERTEX_LABEL\"}, " +
                " {\"type\": \"EDGE_LABEL\"}, {\"type\": \"INDEX_LABEL\"}]");
        Map<String, List<HugeResource>> rg = HugeResource.parseResources(
                "[{\"type\": \"GREMLIN\"}]");
        Id graph1v = authManager.createTarget(makeTarget("hugegraph-v",
                                                         "hugegraph", "url1",
                                                         rv));
        Id graph1e = authManager.createTarget(makeTarget("hugegraph-e",
                                                         "hugegraph", "url1",
                                                         re));
        Id graph1gremlin = authManager.createTarget(makeTarget("hugegraph-g",
                                                               "hugegraph", "url1",
                                                               rg));
        Id belong1 = authManager.createBelong(makeBelong(user0, group1));
        Id belong2 = authManager.createBelong(makeBelong(user1, group2));

        authManager.createAccess(makeAccess(group1, graph1,
                                            HugePermission.READ));
        authManager.createAccess(makeAccess(group1, graph1,
                                            HugePermission.WRITE));
        authManager.createAccess(makeAccess(group1, graph2,
                                            HugePermission.READ));
        authManager.createAccess(makeAccess(group2, graph2,
                                            HugePermission.READ));

        Id access1v = authManager.createAccess(makeAccess(group1, graph1v,
                                                          HugePermission.READ));
        authManager.createAccess(makeAccess(group1, graph1v,
                                            HugePermission.WRITE));
        authManager.createAccess(makeAccess(group1, graph1e,
                                            HugePermission.READ));
        Id access1g = authManager.createAccess(makeAccess(group1, graph1gremlin,
                                                          HugePermission.EXECUTE));

        RolePermission role;
        role = authManager.rolePermission(authManager.getUser(user0));
        String expected =
                "{\"roles\":{\"DEFAULT\":{\"hugegraph\":{\"READ\":{\"EDGE#write\":[{\"type" +
                "\":\"EDGE\",\"label\":\"write\",\"properties\":null}]," +
                "\"PROPERTY_KEY#*\":[{\"type\":\"PROPERTY_KEY\",\"label\":\"*\"," +
                "\"properties\":null},{\"type\":\"PROPERTY_KEY\",\"label\":\"*\"," +
                "\"properties\":null}],\"VERTEX_LABEL#*\":[{\"type\":\"VERTEX_LABEL\"," +
                "\"label\":\"*\",\"properties\":null},{\"type\":\"VERTEX_LABEL\",\"label\":\"*\"," +
                "\"properties\":null}],\"EDGE_LABEL#*\":[{\"type\":\"EDGE_LABEL\"," +
                "\"label\":\"*\",\"properties\":null}]," +
                "\"INDEX_LABEL#*\":[{\"type\":\"INDEX_LABEL\",\"label\":\"*\"," +
                "\"properties\":null}],\"VERTEX#person\":[{\"type\":\"VERTEX\"," +
                "\"label\":\"person\",\"properties\":{\"city\":\"Beijing\",\"age\":\"P.gte(20)" +
                "\"}}]},\"WRITE\":{\"VERTEX#person\":[{\"type\":\"VERTEX\",\"label\":\"person\"," +
                "\"properties\":{\"city\":\"Beijing\",\"age\":\"P.gte(20)\"}}]," +
                "\"VERTEX_LABEL#*\":[{\"type\":\"VERTEX_LABEL\",\"label\":\"*\"," +
                "\"properties\":null}],\"PROPERTY_KEY#*\":[{\"type\":\"PROPERTY_KEY\"," +
                "\"label\":\"*\",\"properties\":null}]}," +
                "\"EXECUTE\":{\"GREMLIN\":[{\"type\":\"GREMLIN\",\"label\":\"*\"," +
                "\"properties\":null}]}}}}}";
        Assert.assertEquals(expected, role.toJson());

        role = authManager.rolePermission(authManager.getBelong(belong1));
        Assert.assertEquals(expected, role.toJson());

        role = authManager.rolePermission(authManager.getGroup(group1));
        Assert.assertEquals(expected, role.toJson());

        role = authManager.rolePermission(authManager.getAccess(access1v));
        expected =
                "{\"roles\":{\"DEFAULT\":{\"hugegraph\":{\"READ\":{\"VERTEX#person\":[{\"type" +
                "\":\"VERTEX\",\"label\":\"person\",\"properties\":{\"city\":\"Beijing\"," +
                "\"age\":\"P.gte(20)\"}}],\"VERTEX_LABEL#*\":[{\"type\":\"VERTEX_LABEL\"," +
                "\"label\":\"*\",\"properties\":null}]," +
                "\"PROPERTY_KEY#*\":[{\"type\":\"PROPERTY_KEY\",\"label\":\"*\"," +
                "\"properties\":null}]}}}}}";
        Assert.assertEquals(expected, role.toJson());

        role = authManager.rolePermission(authManager.getAccess(access1g));
        expected = "{\"roles\":{\"DEFAULT\":{\"hugegraph\":{\"EXECUTE\":{\"GREMLIN\":[" +
                   "{\"type\":\"GREMLIN\",\"label\":\"*\",\"properties\":null}]}}}}}";
        Assert.assertEquals(expected, role.toJson());

        role = authManager.rolePermission(authManager.getUser(user1));
        expected = "{\"roles\":{}}";
        Assert.assertEquals(expected, role.toJson());

        role = authManager.rolePermission(authManager.getBelong(belong2));
        expected = "{\"roles\":{}}";
        Assert.assertEquals(expected, role.toJson());

        role = authManager.rolePermission(authManager.getTarget(graph1v));
        expected = "{\"roles\":{\"DEFAULT\":" +
                   "{\"hugegraph\":" +
                   "{\"READ\":{\"VERTEX#person\":[{\"type\":\"VERTEX\",\"label\":\"person\"," +
                   "\"properties\":" +
                   "{\"city\":\"Beijing\",\"age\":\"P.gte(20)\"}}]," +
                   "\"VERTEX_LABEL#*\":[{\"type\":\"VERTEX_LABEL\",\"label\":\"*\"," +
                   "\"properties\":null}]," +
                   "\"PROPERTY_KEY#*\":[{\"type\":\"PROPERTY_KEY\",\"label\":\"*\"," +
                   "\"properties\":null}]}}}}}";
        Assert.assertEquals(expected, role.toJson());
    }

    @Test
    public void testLogin() throws AuthenticationException {
        AuthManager authManager = graph().authManager();

        HugeUser user = makeUser("test", StringEncoding.hashPassword("pass"));
        authManager.createUser(user);

        // Login
        authManager.loginUser("test", "pass", 10080L);

        // Invalid username or password
        Assert.assertThrows(AuthenticationException.class, () -> {
            authManager.loginUser("huge", "graph", 10080L);
        }, e -> {
            Assert.assertContains("Incorrect username or password", e.getMessage());
        });
    }

    @Test
    public void testValidateUserByToken() throws AuthenticationException {
        AuthManager authManager = graph().authManager();

        HugeUser user = makeUser("test", StringEncoding.hashPassword("pass"));
        Id userId = authManager.createUser(user);

        String token = authManager.loginUser("test", "pass", 10080L);

        UserWithRole userWithRole;
        userWithRole = authManager.validateUser(token);
        Assert.assertEquals(userId, userWithRole.userId());
        Assert.assertEquals("test", userWithRole.username());
        Assert.assertEquals("{\"roles\":{}}", userWithRole.role().toJson());

        // Token cache missed
        Cache<Id, String> tokenCache = Whitebox.getInternalState(authManager,
                                                                 "tokenCache");
        tokenCache.invalidate(IdGenerator.of(token));
        Assert.assertFalse(tokenCache.containsKey(IdGenerator.of(token)));

        userWithRole = authManager.validateUser(token);
        Assert.assertEquals(userId, userWithRole.userId());
        Assert.assertEquals("test", userWithRole.username());
        Assert.assertEquals("{\"roles\":{}}", userWithRole.role().toJson());
        Assert.assertTrue(tokenCache.containsKey(IdGenerator.of(token)));

        // User deleted after login and token not expire
        authManager.deleteUser(userId);
        userWithRole = authManager.validateUser(token);
        Assert.assertNull(null, userWithRole.userId());
        Assert.assertEquals("test", userWithRole.username());
        Assert.assertNull(userWithRole.role());
    }

    @Test
    public void testLogout() throws AuthenticationException {
        AuthManager authManager = graph().authManager();

        HugeUser user = makeUser("test", StringEncoding.hashPassword("pass"));
        @SuppressWarnings("unused")
        Id userId = authManager.createUser(user);

        // Login
        String token = authManager.loginUser("test", "pass", 10080L);

        // Logout
        Cache<Id, String> tokenCache = Whitebox.getInternalState(authManager,
                                                                 "tokenCache");
        Assert.assertTrue(tokenCache.containsKey(IdGenerator.of(token)));
        authManager.logoutUser(token);
        Assert.assertFalse(tokenCache.containsKey(IdGenerator.of(token)));
    }

    @Test
    public void testValidateUserByNameAndPassword() {
        AuthManager authManager = graph().authManager();

        HugeUser user = makeUser("test", StringEncoding.hashPassword("pass"));
        Id userId = authManager.createUser(user);

        UserWithRole userWithRole;
        userWithRole = authManager.validateUser("test", "pass");
        Assert.assertEquals(userId, userWithRole.userId());
        Assert.assertEquals("test", userWithRole.username());
        Assert.assertEquals("{\"roles\":{}}", userWithRole.role().toJson());

        // Error case
        userWithRole = authManager.validateUser("huge", "graph");
        Assert.assertNull(userWithRole.userId());
        Assert.assertEquals("huge", userWithRole.username());
        Assert.assertNull(userWithRole.role());
    }

    @Test
    public void testCreateProject() {
        Assume.assumeTrue("skip this test for hstore",
                          !Objects.equals("hstore", System.getProperty("backend")));
        Assume.assumeTrue("skip this test for null", !(System.getProperty("backend") == null));
        HugeGraph graph = graph();
        HugeProject project = makeProject("test_project",
                                          "this is a test project");
        Id id = graph.authManager().createProject(project);
        Assert.assertNotNull(id);
        project = graph.authManager().getProject(id);
        Assert.assertNotNull(project);
        Assert.assertEquals("this is a test project", project.description());
        Assert.assertEquals("test_project", project.name());
        Assert.assertNotNull(project.adminGroupId());
        Assert.assertNotNull(project.opGroupId());
        Assert.assertNotNull(project.targetId());

        // Check name is unique index
        HugeProject sameNameProject = makeProject("test_project",
                                                  "this is a test " +
                                                  "project another");
        Assert.assertThrows(HugeException.class, () -> {
            graph.authManager().createProject(sameNameProject);
        });
    }

    @Test
    public void testDelProject() {
        Assume.assumeTrue("skip this test for hstore",
                          !Objects.equals("hstore", System.getProperty("backend")));
        Assume.assumeTrue("skip this test for null", !(System.getProperty("backend") == null));
        HugeProject project = makeProject("test_project", null);
        AuthManager authManager = graph().authManager();
        Id projectId = authManager.createProject(project);
        Assert.assertNotNull(projectId);
        HugeProject deletedProject = authManager.deleteProject(projectId);
        Assert.assertThrows(Exception.class, () -> {
            authManager.getProject(projectId);
        });
        Assert.assertThrows(Exception.class, () -> {
            authManager.getGroup(IdGenerator.of(deletedProject.adminGroupId()));
        });
        Assert.assertThrows(Exception.class, () -> {
            authManager.getGroup(IdGenerator.of(deletedProject.opGroupId()));
        });
        Assert.assertThrows(Exception.class, () -> {
            authManager.getTarget(IdGenerator.of(deletedProject.targetId()));
        });
    }

    @Test
    public void testUpdateProject() {
        Assume.assumeTrue("skip this test for hstore",
                          !Objects.equals("hstore", System.getProperty("backend")));
        Assume.assumeTrue("skip this test for null", !(System.getProperty("backend") == null));
        HugeProject project = makeProject("test_project1314",
                                          "this is a desc");
        AuthManager authManager = graph().authManager();
        Id projectId = authManager.createProject(project);
        project = authManager.getProject(projectId);
        project.description("this is a desc another");
        projectId = authManager.updateProject(project);
        HugeProject newProject = authManager.getProject(projectId);
        Assert.assertEquals("this is a desc another", newProject.description());
    }

    @Test
    public void testProjectAddGraph() {
        Assume.assumeTrue("skip this test for hstore",
                          !Objects.equals("hstore", System.getProperty("backend")));
        Assume.assumeTrue("skip this test for null", !(System.getProperty("backend") == null));
        HugeProject project = makeProject("test_project", "");
        AuthManager authManager = graph().authManager();
        Id projectId = authManager.createProject(project);
        projectId = authManager.projectAddGraphs(projectId,
                                                 ImmutableSet.of("graph_test"));
        Assert.assertNotNull(projectId);
        project = authManager.getProject(projectId);
        Assert.assertFalse(project.graphs().isEmpty());
    }

    @Test
    public void testProjectRemoveGraph() {
        Assume.assumeTrue("skip this test for hstore",
                          !Objects.equals("hstore", System.getProperty("backend")));
        Assume.assumeTrue("skip this test for null", !(System.getProperty("backend") == null));
        Id projectId = makeProjectAndAddGraph(graph(), "test_project",
                                              "graph_test");
        AuthManager authManager = graph().authManager();
        Assert.assertNotNull(projectId);
        HugeProject project = authManager.getProject(projectId);
        Assert.assertNotNull(project);
        Assert.assertFalse(project.graphs().isEmpty());
        projectId = authManager.projectRemoveGraphs(
                project.id(), ImmutableSet.of("graph_test"));
        project = authManager.getProject(projectId);
        Assert.assertNotNull(project);
        Assert.assertTrue(project.graphs().isEmpty());
    }

    @Test
    public void testListProject() {
        Assume.assumeTrue("skip this test for hstore",
                          !Objects.equals("hstore", System.getProperty("backend")));
        Assume.assumeTrue("skip this test for null", !(System.getProperty("backend") == null));
        AuthManager authManager = graph().authManager();
        authManager.createProject(makeProject("test_project1", ""));
        authManager.createProject(makeProject("test_project2", ""));
        authManager.createProject(makeProject("test_project3", ""));

        List<HugeProject> projects = authManager.listAllProject(1);
        Assert.assertNotNull(projects);
        Assert.assertEquals(1, projects.size());

        projects = authManager.listAllProject(-1);
        Assert.assertNotNull(projects);
        Assert.assertEquals(3, projects.size());

        projects = authManager.listAllProject(3);
        Assert.assertNotNull(projects);
        Assert.assertEquals(3, projects.size());

        projects = authManager.listAllProject(4);
        Assert.assertNotNull(projects);
        Assert.assertEquals(3, projects.size());

        projects = authManager.listAllProject(2);
        Assert.assertNotNull(projects);
        Assert.assertEquals(2, projects.size());
    }
}
