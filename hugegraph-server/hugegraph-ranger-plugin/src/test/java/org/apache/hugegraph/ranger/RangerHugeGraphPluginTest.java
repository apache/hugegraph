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

package org.apache.hugegraph.ranger;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.gson.Gson;

import org.apache.hugegraph.auth.HugePermission;
import org.apache.hugegraph.auth.ResourceType;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItem;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemAccess;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest.ResourceMatchingScope;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.util.ServicePolicies;
import org.junit.Assert;
import org.junit.Test;

public class RangerHugeGraphPluginTest {

    @Test
    public void testHugePermToRangerAccess() {
        Assert.assertEquals("read",    RangerHugeGraphPlugin.hugePermToRangerAccess(HugePermission.READ));
        Assert.assertEquals("write",   RangerHugeGraphPlugin.hugePermToRangerAccess(HugePermission.WRITE));
        Assert.assertEquals("delete",  RangerHugeGraphPlugin.hugePermToRangerAccess(HugePermission.DELETE));
        Assert.assertEquals("execute", RangerHugeGraphPlugin.hugePermToRangerAccess(HugePermission.EXECUTE));
        Assert.assertEquals("admin",   RangerHugeGraphPlugin.hugePermToRangerAccess(HugePermission.ADMIN));
    }

    @Test
    public void testRangerAccessToHugePerm() {
        Assert.assertEquals(HugePermission.READ,    RangerHugeGraphPlugin.rangerAccessToHugePerm("read"));
        Assert.assertEquals(HugePermission.WRITE,   RangerHugeGraphPlugin.rangerAccessToHugePerm("write"));
        Assert.assertEquals(HugePermission.DELETE,  RangerHugeGraphPlugin.rangerAccessToHugePerm("delete"));
        Assert.assertEquals(HugePermission.EXECUTE, RangerHugeGraphPlugin.rangerAccessToHugePerm("execute"));
        Assert.assertEquals(HugePermission.ADMIN,   RangerHugeGraphPlugin.rangerAccessToHugePerm("admin"));
        Assert.assertEquals(HugePermission.NONE,    RangerHugeGraphPlugin.rangerAccessToHugePerm("unknown"));
    }

    /**
     * Ranger's RangerDefaultAuditHandler populates the audited "Access Type"
     * column from getAction(), not getAccessType() — regression test for the
     * bug where the Ranger Admin UI audit log always showed Access Type=null
     * because setAction() was never called.
     */
    @Test
    public void testBuildRequestSetsActionForAudit() {
        RangerHugeGraphPlugin plugin = new RangerHugeGraphPlugin("hugegraph", null);
        RangerAccessRequestImpl request = plugin.buildRequest(
                "bob", "DEFAULT", "hugegraph", ResourceType.VERTEX, "person",
                HugePermission.READ);

        Assert.assertEquals("read", request.getAccessType());
        Assert.assertEquals(request.getAccessType(), request.getAction());
    }

    /**
     * The resource path built for each request should reflect the real
     * graphspace/graph/resource-type/label being accessed, not a hardcoded
     * wildcard — regression test for Resource Path always showing the
     * wildcard "*", "*", "all", "*" values instead of real ones.
     */
    @Test
    public void testBuildRequestUsesRealResourcePath() {
        RangerHugeGraphPlugin plugin = new RangerHugeGraphPlugin("hugegraph", null);
        RangerAccessRequestImpl request = plugin.buildRequest(
                "bob", "DEFAULT", "hugegraph", ResourceType.VERTEX, "person",
                HugePermission.READ);

        Assert.assertEquals("DEFAULT",
                            request.getResource().getValue(
                                    RangerHugeGraphPlugin.RES_GRAPHSPACE));
        Assert.assertEquals("hugegraph",
                            request.getResource().getValue(
                                    RangerHugeGraphPlugin.RES_GRAPH));
        Assert.assertEquals("vertex",
                            request.getResource().getValue(
                                    RangerHugeGraphPlugin.RES_RESOURCE_TYPE));
        Assert.assertEquals("person",
                            request.getResource().getValue(
                                    RangerHugeGraphPlugin.RES_LABEL));
    }

    @Test
    public void testBuildAnyAccessRequestHasNoResourceKeys() {
        RangerHugeGraphPlugin plugin = new RangerHugeGraphPlugin("hugegraph", null);
        RangerAccessRequestImpl request = plugin.buildAnyAccessRequest(
                "bob", HugePermission.WRITE);

        Assert.assertEquals(ResourceMatchingScope.SELF_OR_DESCENDANTS,
                            request.getResourceMatchingScope());
        Set<String> keys = request.getResource().getKeys();
        Assert.assertTrue(keys == null || keys.isEmpty());
        Assert.assertEquals("write", request.getAccessType());
        Assert.assertEquals(request.getAccessType(), request.getAction());
        Assert.assertEquals("bob", request.getUser());
    }

    /**
     * Evaluated by a real policy engine: a policy scoped to one graph space
     * and graph must count as "some access", while it must not for another
     * permission or user. A request value of "*" would not match it.
     */
    @Test
    public void testHasAnyAccessMatchesPolicyScopedToOneGraphSpace()
            throws Exception {
        // As written in the Ranger UI (all four levels) and with only the
        // mandatory levels
        for (boolean allLevels : new boolean[]{true, false}) {
            RangerHugeGraphPlugin plugin = pluginWithPolicy(
                    "alice", "gs2", "hugegraph", "read", allLevels);

            Assert.assertTrue(plugin.hasAnyAccess("alice",
                                                  HugePermission.READ));
            Assert.assertFalse(plugin.hasAnyAccess("alice",
                                                   HugePermission.WRITE));
            Assert.assertFalse(plugin.hasAnyAccess("bob",
                                                   HugePermission.READ));
        }
    }

    /** The login probe is only a gate: real requests are still decided. */
    @Test
    public void testScopedPolicyStillDecidesEachResource() throws Exception {
        RangerHugeGraphPlugin plugin = pluginWithPolicy(
                "alice", "gs2", "hugegraph", "read", true);

        Assert.assertTrue(plugin.isAllowed("alice", "gs2", "hugegraph",
                                           ResourceType.VERTEX, "person",
                                           HugePermission.READ, false));
        Assert.assertFalse(plugin.isAllowed("alice", "DEFAULT", "hugegraph",
                                            ResourceType.VERTEX, "person",
                                            HugePermission.READ, false));
        Assert.assertFalse(plugin.isAllowed("alice", "gs2", "hugegraph",
                                            ResourceType.VERTEX, "person",
                                            HugePermission.WRITE, false));
        // Not granted by a policy limited to gs2, whatever the login probe says
        Assert.assertFalse(plugin.isAllowed("alice", "*", "*",
                                            ResourceType.ALL, "*",
                                            HugePermission.READ, false));
    }

    private static RangerHugeGraphPlugin pluginWithPolicy(String user,
                                                          String graphSpace,
                                                          String graph,
                                                          String access,
                                                          boolean allLevels)
            throws Exception {
        RangerServiceDef def;
        try (InputStream in = RangerHugeGraphPluginTest.class.getClassLoader()
                .getResourceAsStream("ranger-servicedef-hugegraph.json")) {
            def = new Gson().fromJson(
                    new InputStreamReader(in, StandardCharsets.UTF_8),
                    RangerServiceDef.class);
        }

        Map<String, RangerPolicyResource> resources = new HashMap<>();
        resources.put(RangerHugeGraphPlugin.RES_GRAPHSPACE,
                      new RangerPolicyResource(graphSpace));
        resources.put(RangerHugeGraphPlugin.RES_GRAPH,
                      new RangerPolicyResource(graph));
        if (allLevels) {
            resources.put(RangerHugeGraphPlugin.RES_RESOURCE_TYPE,
                          new RangerPolicyResource("*"));
            resources.put(RangerHugeGraphPlugin.RES_LABEL,
                          new RangerPolicyResource("*"));
        }
        RangerPolicyItem item = new RangerPolicyItem();
        item.setUsers(Collections.singletonList(user));
        item.setAccesses(Collections.singletonList(
                new RangerPolicyItemAccess(access)));
        RangerPolicy policy = new RangerPolicy();
        policy.setId(1L);
        policy.setName("scoped");
        policy.setService("hugegraph");
        policy.setIsEnabled(true);
        policy.setResources(resources);
        policy.setPolicyItems(new ArrayList<>(List.of(item)));

        ServicePolicies policies = new ServicePolicies();
        policies.setServiceName("hugegraph");
        policies.setServiceId(1L);
        policies.setPolicyVersion(1L);
        policies.setServiceDef(def);
        policies.setPolicies(new ArrayList<>(List.of(policy)));

        RangerHugeGraphPlugin plugin = new RangerHugeGraphPlugin("hugegraph",
                                                                 null);
        plugin.setPolicies(policies);
        return plugin;
    }
}
