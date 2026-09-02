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

import org.apache.hugegraph.auth.HugePermission;
import org.apache.hugegraph.auth.ResourceType;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
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
}
