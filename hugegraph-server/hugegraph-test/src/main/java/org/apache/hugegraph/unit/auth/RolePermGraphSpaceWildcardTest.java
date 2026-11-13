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


package org.apache.hugegraph.unit.auth;

import org.apache.hugegraph.auth.HugeAuthenticator.RolePerm;
import org.apache.hugegraph.auth.HugePermission;
import org.apache.hugegraph.auth.ResourceObject;
import org.apache.hugegraph.auth.ResourceType;
import org.apache.hugegraph.auth.RolePermission;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.type.Namifiable;
import org.junit.Test;

/**
 * Grants under the "*" graph-space key apply to every space, while a role
 * naming a space still applies to that space only.
 */
public class RolePermGraphSpaceWildcardTest {

    private static ResourceObject<?> vertex(String graphSpace) {
        Namifiable label = () -> "person";
        return ResourceObject.of(graphSpace, "hugegraph",
                                 ResourceType.VERTEX, label);
    }

    @Test
    public void testWildcardGraphSpaceMatchesAnySpace() {
        RolePermission role = RolePermission.role("*", "*", HugePermission.READ);

        for (String space : new String[]{"DEFAULT", "gs2", "other"}) {
            Assert.assertTrue(space, RolePerm.match(
                              role, HugePermission.READ, vertex(space)));
        }
    }

    @Test
    public void testWildcardGraphSpaceStillChecksAction() {
        RolePermission role = RolePermission.role("*", "*", HugePermission.READ);

        Assert.assertFalse(RolePerm.match(role, HugePermission.WRITE,
                                          vertex("gs2")));
        Assert.assertFalse(RolePerm.match(role, HugePermission.DELETE,
                                          vertex("gs2")));
    }

    @Test
    public void testWildcardGraphSpaceIsNotAdmin() {
        RolePermission role = RolePermission.role("*", "*", HugePermission.READ);

        Assert.assertFalse(RolePermission.isAdmin(role));
    }

    @Test
    public void testNamedGraphSpaceDoesNotLeakToOthers() {
        RolePermission role = RolePermission.role("gs1", "*", HugePermission.READ);

        Assert.assertTrue(RolePerm.match(role, HugePermission.READ,
                                         vertex("gs1")));
        Assert.assertFalse(RolePerm.match(role, HugePermission.READ,
                                          vertex("gs2")));
    }

    @Test
    public void testNamedGraphSpaceAndWildcardCombine() {
        RolePermission named = RolePermission.role("gs1", "*",
                                                   HugePermission.WRITE);
        RolePermission wildcard = RolePermission.role("*", "*",
                                                      HugePermission.READ);
        RolePermission role = RolePermission.fromJson(
                "{\"roles\":{" + entries(named) + "," + entries(wildcard) + "}}");

        Assert.assertTrue(RolePerm.match(role, HugePermission.WRITE,
                                         vertex("gs1")));
        Assert.assertTrue(RolePerm.match(role, HugePermission.READ,
                                         vertex("gs1")));
        Assert.assertTrue(RolePerm.match(role, HugePermission.READ,
                                         vertex("gs2")));
        Assert.assertFalse(RolePerm.match(role, HugePermission.WRITE,
                                          vertex("gs2")));
    }

    @Test
    public void testWildcardResourceSpaceIsLookedUpOnce() {
        RolePermission wildcard = RolePermission.role("*", "*",
                                                      HugePermission.READ);
        RolePermission named = RolePermission.role("gs1", "*",
                                                   HugePermission.READ);

        // A "*" resource space is the wildcard key itself, not a fallback
        Assert.assertTrue(RolePerm.match(wildcard, HugePermission.READ,
                                         vertex("*")));
        Assert.assertFalse(RolePerm.match(wildcard, HugePermission.WRITE,
                                          vertex("*")));
        Assert.assertFalse(RolePerm.match(named, HugePermission.READ,
                                          vertex("*")));
    }

    /** The inner members of {"roles":{...}} for a single-space role. */
    private static String entries(RolePermission role) {
        String json = role.toJson();
        int start = json.indexOf('{', json.indexOf("roles") + 1) + 1;
        return json.substring(start, json.lastIndexOf('}', json.length() - 2));
    }
}
