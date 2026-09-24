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

package org.apache.hugegraph.unit.core;

import org.apache.hugegraph.HugeException;
import org.apache.hugegraph.auth.HugePermission;
import org.apache.hugegraph.auth.HugeProject;
import org.apache.hugegraph.auth.HugeResource;
import org.apache.hugegraph.auth.HugeResource.NameObject;
import org.apache.hugegraph.auth.HugeTarget;
import org.apache.hugegraph.auth.HugeUser;
import org.apache.hugegraph.auth.ResourceObject;
import org.apache.hugegraph.auth.ResourceType;
import org.apache.hugegraph.auth.RolePermission;
import org.apache.hugegraph.backend.id.IdGenerator;
import org.apache.hugegraph.schema.VertexLabel;
import org.apache.hugegraph.structure.HugeEdge;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.testutil.Whitebox;
import org.apache.hugegraph.type.define.IdStrategy;
import org.apache.hugegraph.unit.FakeObjects;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

public class RolePermissionTest {

    @Test
    public void testBuiltinAdmin() {
        String adminAll = "{\"roles\":{\"*\":{\"*\":{\"ADMIN\":{\"ALL" +
                          "\":[{\"type\":\"ALL\",\"label\":\"write\"," +
                          "\"properties\":null}]}}}}}";
        RolePermission admin = RolePermission.admin();
        RolePermission role1 = RolePermission.role("*", "*", HugePermission.ADMIN);
        Assert.assertEquals(admin, role1);
        Assert.assertEquals(admin, RolePermission.builtin(admin));
        Assert.assertEquals(admin, RolePermission.builtin(role1));
        RolePermission role = RolePermission.fromJson(adminAll);
        Assert.assertTrue(roleContains(admin, role));

        RolePermission role2 = RolePermission.all("admin");
        Assert.assertEquals(admin, RolePermission.builtin(role2));
        Assert.assertTrue(roleContains(admin, role2));
        Assert.assertTrue(roleContains(role2, role));

        RolePermission hg = RolePermission.all("hg1");
        RolePermission role3 = RolePermission.fromJson(adminAll);
        Assert.assertEquals(hg, RolePermission.builtin(hg));
        Assert.assertEquals(hg, RolePermission.fromJson(hg));
        Assert.assertTrue(roleContains(hg, role3));

        /*
         * NOTE: admin role not match graph role
         * if want do this, rely on upper-layer special judgment
         */
        Assert.assertTrue(roleContains(admin, hg));
    }

    @Test
    public void testBuiltinNone() {
        RolePermission none = RolePermission.none();
        RolePermission role1 = RolePermission.role("*", "*", HugePermission.NONE);
        Assert.assertEquals(none, role1);
        Assert.assertEquals(none, RolePermission.builtin(none));
        Assert.assertEquals(none, RolePermission.builtin(role1));

        Assert.assertEquals("{\"roles\":{}}", none.toJson());
        RolePermission role = RolePermission.fromJson("{\"roles\":{}}");
        Assert.assertTrue(roleContains(none, role));
    }

    @Test
    public void testContains() {
        String json =
                "{\"roles\":{\"DEFAULT\":{\"hugegraph\":{\"READ\":" +
                "{\"EDGE#write\":[{\"type\":\"EDGE\",\"label\":\"write\"," +
                "\"properties\":null}]," +
                "\"PROPERTY_KEY#*\":[{\"type\":\"PROPERTY_KEY\"," +
                "\"label\":\"*\",\"properties\":null}," +
                "{\"type\":\"PROPERTY_KEY\",\"label\":\"*\",\"properties\":null}]," +
                "\"VERTEX_LABEL#*\":[{\"type\":\"VERTEX_LABEL\",\"label\":\"*\"," +
                "\"properties\":null},{\"type\":\"VERTEX_LABEL\",\"label\":\"*\"," +
                "\"properties\":null}]," +
                "\"EDGE_LABEL#*\":[{\"type\":\"EDGE_LABEL\",\"label\":\"*\"," +
                "\"properties\":null}]," +
                "\"INDEX_LABEL#*\":[{\"type\":\"INDEX_LABEL\",\"label\":\"*\"," +
                "\"properties\":null}]," +
                "\"VERTEX#person\":[{\"type\":\"VERTEX\"," +
                "\"label\":\"person\",\"properties\":{\"city\":\"Beijing\"," +
                "\"age\":\"P.gte(20)\"}}]}," +
                "\"WRITE\":{\"VERTEX#person\":[{\"type\":\"VERTEX\"," +
                "\"label\":\"person\",\"properties\":{\"city\":\"Beijing\"," +
                "\"age\":\"P.gte(20)\"}}]," +
                "\"VERTEX_LABEL#*\":[{\"type\":\"VERTEX_LABEL\"," +
                "\"label\":\"*\",\"properties\":null}]," +
                "\"PROPERTY_KEY#*\":[{\"type\":\"PROPERTY_KEY\",\"label\":\"*\"," +
                "\"properties\":null}]}," +
                "\"EXECUTE\":{\"GREMLIN#*\":[{\"type\":\"GREMLIN\",\"label\":\"*\"," +
                "\"properties\":null}]}},\"hugegraph1\":{\"READ\":{}}}}}}";

        RolePermission role = RolePermission.fromJson(json);

        RolePermission r1 = RolePermission.fromJson(json);
        Assert.assertEquals(role, r1);
        Assert.assertTrue(roleContains(role, r1));

        RolePermission r2 = RolePermission.fromJson(
                "{\"roles\":{\"DEFAULT\":{\"hugegraph\":{\"READ\":{\"EDGE" +
                "#write\":[{\"type\":\"EDGE\",\"label\":\"write\"," +
                "\"properties\":null}]}}}}");
        Assert.assertTrue(roleContains(role, r2));

        RolePermission r3 = RolePermission.fromJson(
                "{\"roles\":{\"DEFAULT\":{\"hugegraph\":{\"READ\":{\"EDGE" +
                "#write\":[{\"type\":\"EDGE\",\"label\":\"write\"," +
                "\"properties\":{\"date\":\"2018-8-8\"}}]}}}}");
        Assert.assertTrue(roleContains(role, r3));

        RolePermission r4 = RolePermission.fromJson(
                "{\"roles\":{\"DEFAULT\":{\"hugegraph\":{\"WRITE\":{\"VERTEX" +
                "#person\":[{\"type\":\"VERTEX\",\"label\":\"person\"," +
                "\"properties\":{\"city\":\"Beijing\",\"age\":\"P.gte(20)" +
                "\"}}]}}}}");
        Assert.assertTrue(roleContains(role, r4));

        RolePermission r5 = RolePermission.fromJson(
                "{\"roles\":{\"DEFAULT\":{\"hugegraph\":{\"WRITE\":{\"VERTEX" +
                "#person\":[{\"type\":\"VERTEX\",\"label\":\"person\"," +
                "\"properties\":{\"city\":\"Beijing\",\"age\":\"P.gte(21)" +
                "\"}}]}}}}");
        Assert.assertFalse(roleContains(role, r5));

        RolePermission r6 = RolePermission.fromJson(
                "{\"roles\":{\"DEFAULT\":{\"hugegraph\":{\"WRITE\":{\"VERTEX" +
                "#person\":[{\"type\":\"VERTEX\",\"label\":\"person\"," +
                "\"properties\":null}]}}}}");
        Assert.assertFalse(roleContains(role, r6));

        RolePermission r7 = RolePermission.fromJson(
                "{\"roles\":{\"DEFAULT\":{\"hugegraph\":{\"WRITE\":{\"VERTEX" +
                "#person2\":[{\"type\":\"VERTEX\",\"label\":\"person2\"," +
                "\"properties\":{\"city\":\"Beijing\",\"age\":\"P.gte(20)" +
                "\"}}]}}}}");
        Assert.assertFalse(roleContains(role, r7));

        RolePermission r8 = RolePermission.fromJson(
                "{\"roles\":{\"DEFAULT\":{\"hugegraph\":{\"WRITE\":{\"EDGE" +
                "#person\":[{\"type\":\"EDGE\",\"label\":\"person\"," +
                "\"properties\":{\"city\":\"Beijing\",\"age\":\"P.gte(20)" +
                "\"}}]}}}}");
        Assert.assertFalse(roleContains(role, r8));

        role = RolePermission.fromJson(
                "{\"roles\":{\"DEFAULT\":{\"hugegraph\":{\"READ\":{\"ALL" +
                "#write\":[{\"type\":\"ALL\",\"label\":\"write\"," +
                "\"properties\":null}]}}}}");
        RolePermission r9 = RolePermission.fromJson(
                "{\"roles\":{\"DEFAULT\":{\"hugegraph\":{\"READ\":{\"ALL" +
                "#write\":[{\"type\":\"ALL\",\"label\":\"write\"," +
                "\"properties\":null}]}}}}");
        Assert.assertTrue(roleContains(role, r9));

        RolePermission r10 = RolePermission.fromJson(
                "{\"roles\":{\"DEFAULT\":{\"hugegraph\":{\"READ\":{\"EDGE" +
                "#write\":[{\"type\":\"EDGE\",\"label\":\"write\"," +
                "\"properties\":null}]}}}}");
        Assert.assertTrue(roleContains(role, r10));

        RolePermission r11 = RolePermission.fromJson(
                "{\"roles\":{\"DEFAULT\":{\"hugegraph\":{\"READ\":{\"VERTEX" +
                "#write\":[{\"type\":\"VERTEX\",\"label\":\"write\"," +
                "\"properties\":null}]}}}}");
        Assert.assertTrue(roleContains(role, r11));

        RolePermission r12 = RolePermission.fromJson(
                "{\"roles\":{\"DEFAULT\":{\"hugegraph\":{\"READ\":{\"VERTEX" +
                "#person\":[{\"type\":\"VERTEX\",\"label\":\"person\"," +
                "\"properties\":null}]}}}}");
        Assert.assertFalse(roleContains(role, r12));

        RolePermission r13 = RolePermission.fromJson(
                "{\"roles\":{\"DEFAULT\":{\"hugegraph\":{\"WRITE\":{\"VERTEX" +
                "#write\":[{\"type\":\"VERTEX\",\"label\":\"write\"," +
                "\"properties\":null}]}}}}");
        Assert.assertFalse(roleContains(role, r13));

        RolePermission r14 = RolePermission.fromJson(
                "{\"roles\":{\"DEFAULT\":{\"hugegraph2\":{\"READ\":{\"VERTEX" +
                "#write\":[{\"type\":\"VERTEX\",\"label\":\"write\"," +
                "\"properties\":null}]}}}}");
        Assert.assertFalse(roleContains(role, r14));
    }

    @Test
    public void testResourceType() {
        // ROOT
        Assert.assertTrue(ResourceType.ROOT.match(ResourceType.NONE));
        Assert.assertTrue(ResourceType.ROOT.match(ResourceType.STATUS));
        Assert.assertTrue(ResourceType.ROOT.match(ResourceType.VERTEX));
        Assert.assertTrue(ResourceType.ROOT.match(ResourceType.EDGE));
        Assert.assertTrue(ResourceType.ROOT.match(ResourceType.VERTEX_LABEL));
        Assert.assertTrue(ResourceType.ROOT.match(ResourceType.META));
        Assert.assertTrue(ResourceType.ROOT.match(ResourceType.ALL));
        Assert.assertTrue(ResourceType.ROOT.match(ResourceType.GRANT));
        Assert.assertTrue(ResourceType.ROOT.match(ResourceType.USER_GROUP));
        Assert.assertTrue(ResourceType.ROOT.match(ResourceType.TARGET));
        Assert.assertTrue(ResourceType.ROOT.match(ResourceType.METRICS));
        Assert.assertTrue(ResourceType.ROOT.match(ResourceType.PROJECT));
        Assert.assertTrue(ResourceType.ROOT.match(ResourceType.ROOT));

        // ALL
        Assert.assertTrue(ResourceType.ALL.match(ResourceType.NONE));
        Assert.assertTrue(ResourceType.ALL.match(ResourceType.STATUS));
        Assert.assertTrue(ResourceType.ALL.match(ResourceType.VERTEX));
        Assert.assertTrue(ResourceType.ALL.match(ResourceType.EDGE));
        Assert.assertTrue(ResourceType.ALL.match(ResourceType.VERTEX_LABEL));
        Assert.assertTrue(ResourceType.ALL.match(ResourceType.META));
        Assert.assertTrue(ResourceType.ALL.match(ResourceType.ALL));

        Assert.assertFalse(ResourceType.ALL.match(ResourceType.GRANT));
        Assert.assertFalse(ResourceType.ALL.match(ResourceType.USER_GROUP));
        Assert.assertFalse(ResourceType.ALL.match(ResourceType.PROJECT));
        Assert.assertFalse(ResourceType.ALL.match(ResourceType.TARGET));
        Assert.assertFalse(ResourceType.ALL.match(ResourceType.METRICS));
        Assert.assertFalse(ResourceType.ALL.match(ResourceType.ROOT));

        // SCHEMA
        Assert.assertTrue(ResourceType.SCHEMA.match(ResourceType.NONE));
        Assert.assertTrue(ResourceType.SCHEMA.match(ResourceType.PROPERTY_KEY));
        Assert.assertTrue(ResourceType.SCHEMA.match(ResourceType.VERTEX_LABEL));
        Assert.assertTrue(ResourceType.SCHEMA.match(ResourceType.EDGE_LABEL));
        Assert.assertTrue(ResourceType.SCHEMA.match(ResourceType.INDEX_LABEL));
        Assert.assertTrue(ResourceType.SCHEMA.match(ResourceType.SCHEMA));

        Assert.assertFalse(ResourceType.SCHEMA.match(ResourceType.STATUS));
        Assert.assertFalse(ResourceType.SCHEMA.match(ResourceType.VERTEX));
        Assert.assertFalse(ResourceType.SCHEMA.match(ResourceType.EDGE));
        Assert.assertFalse(ResourceType.SCHEMA.match(ResourceType.VERTEX_AGGR));
        Assert.assertFalse(ResourceType.SCHEMA.match(ResourceType.EDGE_AGGR));
        Assert.assertFalse(ResourceType.SCHEMA.match(ResourceType.VAR));
        Assert.assertFalse(ResourceType.SCHEMA.match(ResourceType.GREMLIN));
        Assert.assertFalse(ResourceType.SCHEMA.match(ResourceType.TASK));
        Assert.assertFalse(ResourceType.SCHEMA.match(ResourceType.META));
        Assert.assertFalse(ResourceType.SCHEMA.match(ResourceType.ALL));
        Assert.assertFalse(ResourceType.SCHEMA.match(ResourceType.GRANT));
        Assert.assertFalse(ResourceType.SCHEMA.match(ResourceType.USER_GROUP));
        Assert.assertFalse(ResourceType.SCHEMA.match(ResourceType.PROJECT));
        Assert.assertFalse(ResourceType.SCHEMA.match(ResourceType.TARGET));
        Assert.assertFalse(ResourceType.SCHEMA.match(ResourceType.METRICS));
        Assert.assertFalse(ResourceType.SCHEMA.match(ResourceType.ROOT));

        // isRepresentative
        Assert.assertTrue(ResourceType.ROOT.isRepresentative());
        Assert.assertTrue(ResourceType.ALL.isRepresentative());
        Assert.assertTrue(ResourceType.SCHEMA.isRepresentative());

        Assert.assertFalse(ResourceType.NONE.isRepresentative());
        Assert.assertFalse(ResourceType.VERTEX.isRepresentative());
        Assert.assertFalse(ResourceType.META.isRepresentative());
        Assert.assertFalse(ResourceType.METRICS.isRepresentative());

        // isAuth
        Assert.assertTrue(ResourceType.GRANT.isAuth());
        Assert.assertTrue(ResourceType.USER_GROUP.isAuth());
        Assert.assertTrue(ResourceType.PROJECT.isAuth());
        Assert.assertTrue(ResourceType.TARGET.isAuth());

        Assert.assertFalse(ResourceType.ROOT.isAuth());
        Assert.assertFalse(ResourceType.ALL.isAuth());
        Assert.assertFalse(ResourceType.SCHEMA.isAuth());
        Assert.assertFalse(ResourceType.NONE.isAuth());
        Assert.assertFalse(ResourceType.VERTEX.isAuth());
        Assert.assertFalse(ResourceType.META.isAuth());
        Assert.assertFalse(ResourceType.METRICS.isAuth());

        // isGrantOrUser
        Assert.assertTrue(ResourceType.GRANT.isGrantOrUser());
        Assert.assertTrue(ResourceType.USER_GROUP.isGrantOrUser());
        Assert.assertFalse(ResourceType.PROJECT.isGrantOrUser());
        Assert.assertFalse(ResourceType.TARGET.isGrantOrUser());

        Assert.assertFalse(ResourceType.ROOT.isGrantOrUser());
        Assert.assertFalse(ResourceType.ALL.isGrantOrUser());
        Assert.assertFalse(ResourceType.SCHEMA.isGrantOrUser());
        Assert.assertFalse(ResourceType.NONE.isGrantOrUser());
        Assert.assertFalse(ResourceType.VERTEX.isGrantOrUser());
        Assert.assertFalse(ResourceType.META.isGrantOrUser());
        Assert.assertFalse(ResourceType.METRICS.isGrantOrUser());

        // isSchema
        Assert.assertTrue(ResourceType.PROPERTY_KEY.isSchema());
        Assert.assertTrue(ResourceType.VERTEX_LABEL.isSchema());
        Assert.assertTrue(ResourceType.EDGE_LABEL.isSchema());
        Assert.assertTrue(ResourceType.INDEX_LABEL.isSchema());
        Assert.assertTrue(ResourceType.SCHEMA.isSchema());

        Assert.assertFalse(ResourceType.ROOT.isSchema());
        Assert.assertFalse(ResourceType.ALL.isSchema());
        Assert.assertFalse(ResourceType.NONE.isSchema());
        Assert.assertFalse(ResourceType.STATUS.isSchema());
        Assert.assertFalse(ResourceType.VAR.isSchema());
        Assert.assertFalse(ResourceType.GREMLIN.isSchema());
        Assert.assertFalse(ResourceType.TASK.isSchema());
        Assert.assertFalse(ResourceType.META.isSchema());
        Assert.assertFalse(ResourceType.METRICS.isSchema());

        // isGraph
        Assert.assertTrue(ResourceType.VERTEX.isGraph());
        Assert.assertTrue(ResourceType.EDGE.isGraph());

        Assert.assertFalse(ResourceType.ROOT.isGraph());
        Assert.assertFalse(ResourceType.ALL.isGraph());
        Assert.assertFalse(ResourceType.SCHEMA.isGraph());
        Assert.assertFalse(ResourceType.NONE.isGraph());
        Assert.assertFalse(ResourceType.STATUS.isGraph());
        Assert.assertFalse(ResourceType.VERTEX_AGGR.isGraph());
        Assert.assertFalse(ResourceType.EDGE_AGGR.isGraph());
        Assert.assertFalse(ResourceType.VAR.isGraph());
        Assert.assertFalse(ResourceType.GREMLIN.isGraph());
        Assert.assertFalse(ResourceType.TASK.isGraph());
        Assert.assertFalse(ResourceType.META.isGraph());
        Assert.assertFalse(ResourceType.METRICS.isGraph());
    }

    @Test
    public void testHugeResource() {
        HugeResource r = new HugeResource(ResourceType.VERTEX, "person",
                                          ImmutableMap.of("city", "Beijing"));
        String json = "{\"type\":\"VERTEX\",\"label\":\"person\",\"properties\":" +
                      "{\"city\":\"Beijing\"}}";
        Assert.assertEquals(json, r.toString());
        Assert.assertEquals(r, HugeResource.parseResource(json));

        HugeResource r1 = new HugeResource(null, null, null);
        HugeResource r2 = new HugeResource(null, null, null);
        String nullJson = "{\"type\":null,\"label\":null,\"properties\":null}";
        Assert.assertEquals(nullJson, r1.toString());
        Assert.assertEquals(r1, r2);

        HugeResource r3 = HugeResource.parseResource(nullJson);
        Assert.assertEquals(r1, r3);

        Assert.assertThrows(HugeException.class, () -> {
            new HugeResource(ResourceType.VERTEX, "person",
                             ImmutableMap.of("city", "P.(1)"));
        }, e -> {
            Assert.assertContains("Invalid predicate: P.(1)",
                                  e.getMessage());
        });

        Assert.assertThrows(HugeException.class, () -> {
            String resource = "{\"type\":\"VERTEX\",\"label\":\"person\",\"properties\":" +
                              "{\"city\":\"P.(1)\"}}";
            HugeResource.parseResource(resource);
        }, e -> {
            Assert.assertContains("Invalid predicate: P.(1)",
                                  e.getMessage());
        });

        Assert.assertThrows(HugeException.class, () -> {
            String resources = "[{\"type\":\"VERTEX\",\"label\":\"person\",\"properties\":" +
                               "{\"city\":\"P.(1)\"}}]";
            HugeResource.parseResources(resources);
        }, e -> {
            Assert.assertContains("Invalid predicate: P.(1)",
                                  e.getMessage());
        });
    }

    @Test
    public void testHugeResourceFilter() {
        HugeResource all = HugeResource.ALL;

        // common
        ResourceObject<?> r1 = ResourceObject.of("g1", ResourceType.GREMLIN,
                                                 NameObject.ANY);
        Assert.assertTrue(all.filter(r1));

        ResourceObject<?> r2 = ResourceObject.of("g1", ResourceType.META,
                                                 NameObject.of("test"));
        Assert.assertTrue(all.filter(r2));

        HugeResource page = new HugeResource(ResourceType.META,
                                             "page", null);
        Assert.assertFalse(page.filter(r2));

        ResourceObject<?> r3 = ResourceObject.of("g1", ResourceType.META,
                                                 NameObject.of("page"));
        Assert.assertTrue(page.filter(r3));
    }

    @Test
    public void testHugeResourceFilterSchema() {
        HugeResource all = HugeResource.ALL;
        HugeResource schema = new HugeResource(ResourceType.SCHEMA,
                                               HugeResource.ANY, null);

        HugeResource vlPrefix = new HugeResource(ResourceType.VERTEX_LABEL,
                                                 "p-.*", null);

        ResourceObject<?> r3 = ResourceObject.of("DEFAULT", "g1",
                                                 ResourceType.VERTEX_LABEL,
                                                 NameObject.of("test"));
        Assert.assertTrue(all.filter(r3));
        Assert.assertTrue(schema.filter(r3));
        Assert.assertFalse(vlPrefix.filter(r3));

        ResourceObject<?> r4 = ResourceObject.of("DEFAULT", "g1",
                                                 ResourceType.VERTEX_LABEL,
                                                 NameObject.of("p-test"));
        Assert.assertTrue(all.filter(r4));
        Assert.assertTrue(schema.filter(r4));
        Assert.assertTrue(vlPrefix.filter(r4));

        FakeObjects fo = new FakeObjects();

        VertexLabel vl1 = fo.newVertexLabel(IdGenerator.of("id1"), "person",
                                            IdStrategy.PRIMARY_KEY,
                                            IdGenerator.of("1"));
        ResourceObject<?> r5 = ResourceObject.of("DEFAULT", "g1", vl1);
        Assert.assertTrue(all.filter(r5));
        Assert.assertTrue(schema.filter(r5));
        Assert.assertFalse(vlPrefix.filter(r5));

        VertexLabel vl2 = fo.newVertexLabel(IdGenerator.of("id1"), "p-person",
                                            IdStrategy.PRIMARY_KEY,
                                            IdGenerator.of("1"));
        ResourceObject<?> r6 = ResourceObject.of("g1", vl2);
        Assert.assertTrue(all.filter(r6));
        Assert.assertTrue(schema.filter(r6));
        Assert.assertTrue(vlPrefix.filter(r6));
    }

    @Test
    public void testHugeResourceFilterVertexOrEdge() {
        HugeResource all = HugeResource.ALL;

        // vertex & edge
        FakeObjects fo = new FakeObjects();
        HugeEdge edge = fo.newEdge(1, 2);
        ResourceObject<?> r1 = ResourceObject.of("g1", edge.sourceVertex());
        ResourceObject<?> r2 = ResourceObject.of("g1", edge.targetVertex());
        ResourceObject<?> r3 = ResourceObject.of("g1", edge);

        Assert.assertTrue(all.filter(r1));
        Assert.assertTrue(all.filter(r2));
        Assert.assertTrue(all.filter(r3));

        HugeResource vr = new HugeResource(ResourceType.VERTEX,
                                           HugeResource.ANY, null);
        Assert.assertTrue(vr.filter(r1));
        Assert.assertTrue(vr.filter(r2));
        Assert.assertFalse(vr.filter(r3));

        vr = new HugeResource(ResourceType.VERTEX,
                              "person", null);
        Assert.assertTrue(vr.filter(r1));
        Assert.assertTrue(vr.filter(r2));
        Assert.assertFalse(vr.filter(r3));

        vr = new HugeResource(ResourceType.VERTEX,
                              "person", ImmutableMap.of("city", "Beijing"));
        Assert.assertTrue(vr.filter(r1));
        Assert.assertFalse(vr.filter(r2));
        Assert.assertFalse(vr.filter(r3));

        vr = new HugeResource(ResourceType.VERTEX,
                              "person", ImmutableMap.of("city", "Shanghai"));
        Assert.assertFalse(vr.filter(r1));
        Assert.assertTrue(vr.filter(r2));
        Assert.assertFalse(vr.filter(r3));

        vr = new HugeResource(ResourceType.VERTEX, "person", ImmutableMap.of(
                "city", "P.within(\"Beijing\", \"Shanghai\")"));
        Assert.assertTrue(vr.filter(r1));
        Assert.assertTrue(vr.filter(r2));
        Assert.assertFalse(vr.filter(r3));

        vr = new HugeResource(ResourceType.VERTEX, "person",
                              ImmutableMap.of("age", "P.gt(18)"));
        Assert.assertFalse(vr.filter(r1));
        Assert.assertTrue(vr.filter(r2));
        Assert.assertFalse(vr.filter(r3));

        vr = new HugeResource(ResourceType.VERTEX, "person",
                              ImmutableMap.of("age", "P.between(20, 21)"));
        Assert.assertFalse(vr.filter(r1));
        Assert.assertTrue(vr.filter(r2));
        Assert.assertFalse(vr.filter(r3));

        vr = new HugeResource(ResourceType.VERTEX, "person",
                              ImmutableMap.of("age", "P.between(18, 21)"));
        Assert.assertTrue(vr.filter(r1));
        Assert.assertTrue(vr.filter(r2));
        Assert.assertFalse(vr.filter(r3));

        HugeResource er = new HugeResource(ResourceType.EDGE,
                                           "knows", null);
        Assert.assertFalse(er.filter(r1));
        Assert.assertFalse(er.filter(r2));
        Assert.assertTrue(er.filter(r3));

        er = new HugeResource(ResourceType.EDGE,
                              "knows", ImmutableMap.of("weight", "P.gt(0.7)"));
        Assert.assertFalse(er.filter(r1));
        Assert.assertFalse(er.filter(r2));
        Assert.assertTrue(er.filter(r3));

        er = new HugeResource(ResourceType.EDGE,
                              "knows", ImmutableMap.of("weight", "P.gt(0.8)"));
        Assert.assertFalse(er.filter(r1));
        Assert.assertFalse(er.filter(r2));
        Assert.assertFalse(er.filter(r3));

        er = new HugeResource(ResourceType.EDGE,
                              "knows", ImmutableMap.of("weight", "P.lt(0.8)"));
        Assert.assertFalse(er.filter(r1));
        Assert.assertFalse(er.filter(r2));
        Assert.assertTrue(er.filter(r3));
    }

    @Test
    public void testHugeResourceFilterUser() {
        HugeResource all = HugeResource.ALL;

        // user
        ResourceObject<?> r3 = ResourceObject.of("g1", ResourceType.USER_GROUP,
                                                 NameObject.ANY);
        Assert.assertFalse(all.filter(r3));

        HugeResource user = new HugeResource(ResourceType.USER_GROUP,
                                             HugeResource.ANY, null);
        Assert.assertTrue(user.filter(r3));

        ResourceObject<?> r4 = ResourceObject.of("g1", new HugeUser("fake"));
        Assert.assertTrue(user.filter(r4));

        HugeResource user2 = new HugeResource(ResourceType.USER_GROUP,
                                              "bj-.*", null);
        Assert.assertTrue(user2.filter(r3));
        Assert.assertFalse(user2.filter(r4));

        HugeResource user3 = new HugeResource(ResourceType.USER_GROUP,
                                              "fa.*", null);
        Assert.assertTrue(user3.filter(r3));
        Assert.assertTrue(user3.filter(r4));

        ResourceObject<?> r5 = ResourceObject.of("g1", new HugeTarget("g", ""));
        Assert.assertFalse(user.filter(r5));

        HugeResource root = new HugeResource(ResourceType.ROOT,
                                             HugeResource.ANY, null);
        Assert.assertTrue(root.filter(r3));
        Assert.assertTrue(root.filter(r4));
        Assert.assertTrue(root.filter(r5));
    }

    @Test
    public void testHugeResourceFilterProject() {
        HugeResource all = HugeResource.ALL;
        ResourceObject<?> r1 = ResourceObject.of("hugegraph",
                                                 new HugeProject("project1"));
        Assert.assertFalse(all.filter(r1));

        HugeResource project = new HugeResource(ResourceType.PROJECT,
                                                "project1",
                                                null);
        Assert.assertTrue(project.filter(r1));

        HugeResource root = new HugeResource(ResourceType.ROOT,
                                             HugeResource.ANY, null);
        Assert.assertTrue(root.filter(r1));

        ResourceObject<?> r2 = ResourceObject.of("hugegraph",
                                                 new HugeProject("project2"));
        Assert.assertFalse(project.filter(r2));
    }

    private boolean roleContains(RolePermission role, RolePermission other) {
        return Whitebox.invoke(RolePermission.class, "contains", role, other);
    }
}
