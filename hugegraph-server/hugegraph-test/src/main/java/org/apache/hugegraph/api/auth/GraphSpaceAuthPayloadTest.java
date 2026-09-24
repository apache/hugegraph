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

package org.apache.hugegraph.api.auth;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.hugegraph.auth.AuthManager;
import org.apache.hugegraph.auth.HugeAccess;
import org.apache.hugegraph.auth.HugeBelong;
import org.apache.hugegraph.auth.HugeGroup;
import org.apache.hugegraph.auth.HugePermission;
import org.apache.hugegraph.auth.HugeTarget;
import org.apache.hugegraph.backend.id.IdGenerator;
import org.apache.hugegraph.testutil.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.ws.rs.ForbiddenException;

public class GraphSpaceAuthPayloadTest {

    @Test
    public void testTargetPayloadUsesPathGraphSpaceAndOptionalUrl()
            throws Exception {
        TargetAPI.JsonTarget jsonTarget = new ObjectMapper().readValue(
                "{\"target_name\":\"target\"," +
                "\"target_graph\":\"hugegraph\"," +
                "\"target_description\":\"description\"," +
                "\"target_resources\":[]}",
        TargetAPI.JsonTarget.class);

        HugeTarget target = jsonTarget.build("SPACE_A");
        target.creator("manager");
        Map<String, Object> properties = target.asMap();

        Assert.assertEquals("SPACE_A", target.graphSpace());
        Assert.assertEquals("description", target.description());
        Assert.assertEquals("", target.url());
        Assert.assertEquals("SPACE_A", properties.get("graphspace"));
        Assert.assertEquals("description",
                            properties.get("target_description"));
    }

    @Test
    public void testRelationshipPayloadsUsePathGraphSpace() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        AccessAPI.JsonAccess jsonAccess = mapper.readValue(
                "{\"group\":\"group\",\"target\":\"target\"," +
                "\"access_permission\":\"READ\"}",
                AccessAPI.JsonAccess.class);
        HugeAccess access = jsonAccess.build("SPACE_A");

        Assert.assertEquals("SPACE_A", access.graphSpace());
        Assert.assertEquals(HugePermission.READ, access.permission());

        BelongAPI.JsonBelong jsonBelong = mapper.readValue(
                "{\"user\":\"user\",\"group\":\"group\"}",
                BelongAPI.JsonBelong.class);
        HugeBelong belong = jsonBelong.build("SPACE_A");

        Assert.assertEquals("SPACE_A", belong.graphSpace());
        Assert.assertEquals(HugeBelong.UG, belong.link());
    }

    @Test
    public void testTargetRejectsForeignGraphSpace() {
        HugeTarget target = new HugeTarget("target", "hugegraph", "");
        target.graphSpace("SPACE_A");

        TargetAPI.checkGraphSpace("SPACE_A", target);
        Assert.assertThrows(ForbiddenException.class, () ->
                TargetAPI.checkGraphSpace("SPACE_B", target));
    }

    @Test
    public void testAccessRejectsForeignGraphSpace() {
        HugeAccess access = new HugeAccess("SPACE_A",
                                           IdGenerator.of("group"),
                                           IdGenerator.of("target"));

        AccessAPI.checkGraphSpace("SPACE_A", access);
        Assert.assertThrows(ForbiddenException.class, () ->
                AccessAPI.checkGraphSpace("SPACE_B", access));
    }

    @Test
    public void testCreateAccessRejectsForeignTargetWithoutSideEffects()
            throws Exception {
        AuthManager auth = Mockito.mock(AuthManager.class);
        AccessAPI.JsonAccess jsonAccess = new ObjectMapper().readValue(
                "{\"group\":\"group\",\"target\":\"target\"," +
                "\"access_permission\":\"READ\"}",
                AccessAPI.JsonAccess.class);
        HugeAccess access = jsonAccess.build("SPACE_A");
        HugeTarget target = new HugeTarget("target", "hugegraph", "");
        target.graphSpace("SPACE_B");
        Mockito.when(auth.getTarget("SPACE_A", access.target()))
               .thenReturn(target);

        Assert.assertThrows(ForbiddenException.class, () ->
                AccessAPI.createScopedAccess(auth, "SPACE_A", access));

        Mockito.verify(auth).getTarget("SPACE_A", access.target());
        Mockito.verify(auth, Mockito.never())
               .createAccess(Mockito.anyString(),
                             Mockito.any(HugeAccess.class));
    }

    @Test
    public void testCreateAccessRejectsBuiltInRoleGroup() throws Exception {
        AuthManager auth = Mockito.mock(AuthManager.class);
        Mockito.when(auth.supportsGraphSpaceAuth()).thenReturn(true);
        AccessAPI.JsonAccess jsonAccess = new ObjectMapper().readValue(
                "{\"group\":\"builtin\",\"target\":\"target\"," +
                "\"access_permission\":\"READ\"}",
                AccessAPI.JsonAccess.class);
        HugeAccess access = jsonAccess.build("SPACE_A");
        Mockito.when(auth.getGroup(access.source())).thenReturn(null);

        Assert.assertThrows(ForbiddenException.class, () ->
                AccessAPI.createScopedAccess(auth, "SPACE_A", access));

        Mockito.verify(auth, Mockito.never())
               .createAccess(Mockito.anyString(),
                             Mockito.any(HugeAccess.class));
    }

    @Test
    public void testAccessListHidesBuiltInRoleAccesses() {
        AuthManager auth = Mockito.mock(AuthManager.class);
        Mockito.when(auth.supportsGraphSpaceAuth()).thenReturn(true);
        HugeAccess builtIn = access("SPACE_A", "builtin");
        HugeAccess business = access("SPACE_A", "business");
        Mockito.when(auth.listAllAccess("SPACE_A", -1L)).thenReturn(
                Arrays.asList(builtIn, business));
        Mockito.when(auth.getGroup(builtIn.source())).thenReturn(null);
        Mockito.when(auth.getGroup(business.source())).thenReturn(
                new HugeGroup(GraphSpaceGroupAPI.scopedPrefix("SPACE_A") +
                              "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"));

        List<HugeAccess> accesses = AccessAPI.listScopedAccesses(
                                    auth, "SPACE_A", null, null, 100L);

        Assert.assertEquals(Arrays.asList(business), accesses);
    }

    @Test
    public void testBelongRejectsForeignGraphSpace() {
        HugeBelong belong = new HugeBelong("SPACE_A",
                                           IdGenerator.of("user"),
                                           IdGenerator.of("group"),
                                           null, HugeBelong.UG);

        BelongAPI.checkGraphSpace("SPACE_A", belong);
        Assert.assertThrows(ForbiddenException.class, () ->
                BelongAPI.checkGraphSpace("SPACE_B", belong));
    }

    @Test
    public void testScopedChecksRejectMissingEntities() {
        Assert.assertThrows(IllegalArgumentException.class, () ->
                TargetAPI.checkGraphSpace("SPACE_A", null));
        Assert.assertThrows(IllegalArgumentException.class, () ->
                BelongAPI.checkGraphSpace("SPACE_A", null));
        Assert.assertThrows(IllegalArgumentException.class, () ->
                AccessAPI.checkGraphSpace("SPACE_A", null));
    }

    @Test
    public void testScopedListsFilterBeforeApplyingLimit() {
        AuthManager auth = Mockito.mock(AuthManager.class);
        HugeTarget foreignTarget = target("SPACE_B", "foreign");
        HugeTarget ownTarget1 = target("SPACE_A", "own-1");
        HugeTarget ownTarget2 = target("SPACE_A", "own-2");
        Mockito.when(auth.listAllTargets("SPACE_A", -1L)).thenReturn(
                Arrays.asList(foreignTarget, ownTarget1, ownTarget2));

        HugeBelong foreignBelong = belong("SPACE_B", "foreign");
        HugeBelong ownBelong1 = belong("SPACE_A", "own-1");
        HugeBelong ownBelong2 = belong("SPACE_A", "own-2");
        Mockito.when(auth.listAllBelong("SPACE_A", -1L)).thenReturn(
                Arrays.asList(foreignBelong, ownBelong1, ownBelong2));

        HugeAccess foreignAccess = access("SPACE_B", "foreign");
        HugeAccess ownAccess1 = access("SPACE_A", "own-1");
        HugeAccess ownAccess2 = access("SPACE_A", "own-2");
        Mockito.when(auth.listAllAccess("SPACE_A", -1L)).thenReturn(
                Arrays.asList(foreignAccess, ownAccess1, ownAccess2));

        List<HugeTarget> targets = TargetAPI.listScopedTargets(
                                   auth, "SPACE_A", 1L);
        List<HugeBelong> belongs = BelongAPI.listScopedBelongs(
                                   auth, "SPACE_A", null, null, 1L);
        List<HugeAccess> accesses = AccessAPI.listScopedAccesses(
                                   auth, "SPACE_A", null, null, 1L);

        Assert.assertEquals(Arrays.asList(ownTarget1), targets);
        Assert.assertEquals(Arrays.asList(ownBelong1), belongs);
        Assert.assertEquals(Arrays.asList(ownAccess1), accesses);
    }

    private static HugeTarget target(String graphSpace, String name) {
        HugeTarget target = new HugeTarget(name, "hugegraph", "");
        target.graphSpace(graphSpace);
        target.creator("admin");
        return target;
    }

    private static HugeBelong belong(String graphSpace, String suffix) {
        HugeBelong belong = new HugeBelong(
                graphSpace, IdGenerator.of("user-" + suffix),
                IdGenerator.of("group-" + suffix), null, HugeBelong.UG);
        belong.creator("admin");
        return belong;
    }

    private static HugeAccess access(String graphSpace, String suffix) {
        HugeAccess access = new HugeAccess(
                graphSpace, IdGenerator.of("group-" + suffix),
                IdGenerator.of("target-" + suffix));
        access.creator("admin");
        return access;
    }
}
