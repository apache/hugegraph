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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hugegraph.auth.AuthManager;
import org.apache.hugegraph.auth.HugeGroup;
import org.apache.hugegraph.testutil.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.ws.rs.ForbiddenException;

public class GraphSpaceGroupAPITest {

    @Test
    public void testCreatePayloadCannotControlScopedGroupName()
            throws Exception {
        GraphSpaceGroupAPI.JsonGroup jsonGroup = new ObjectMapper().readValue(
                "{\"group_name\":\"client-controlled\"," +
                "\"group_description\":\"description\"}",
                GraphSpaceGroupAPI.JsonGroup.class);

        HugeGroup group = jsonGroup.build("SPACE_A");

        Assert.assertTrue(group.name().matches(
                GraphSpaceGroupAPI.scopedPrefix("SPACE_A") +
                "[0-9a-f]{32}"));
        Assert.assertNotEquals("client-controlled", group.name());
        Assert.assertEquals("description", group.description());
    }

    @Test
    public void testListFiltersBeforeApplyingLimit() {
        AuthManager auth = Mockito.mock(AuthManager.class);

        List<HugeGroup> groups = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            groups.add(group("SPACE_B", (char) ('a' + i % 6)));
        }
        groups.add(group("SPACE_A", 'a'));
        groups.add(group("SPACE_A", 'b'));
        Mockito.when(auth.listAllGroups(Mockito.anyLong())).thenReturn(groups);

        List<HugeGroup> one = GraphSpaceGroupAPI.listScopedGroups(
                              auth, "SPACE_A", 1);
        List<HugeGroup> hundred = GraphSpaceGroupAPI.listScopedGroups(
                                  auth, "SPACE_A", 100);
        List<HugeGroup> unlimited = GraphSpaceGroupAPI.listScopedGroups(
                                    auth, "SPACE_A", -1);

        Assert.assertEquals(1, one.size());
        Assert.assertEquals(2, hundred.size());
        Assert.assertEquals(2, unlimited.size());
        Mockito.verify(auth, Mockito.times(3)).listAllGroups(-1);
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            GraphSpaceGroupAPI.listScopedGroups(auth, "SPACE_A", -2);
        });
    }

    @Test
    public void testSpaceManagerCanOnlyUseOwnedGraphSpaceEndpoint() {
        AuthManager auth = Mockito.mock(AuthManager.class);
        Mockito.when(auth.isAdminManager("manager")).thenReturn(false);
        Mockito.when(auth.isSpaceManager("SPACE_A", "manager"))
               .thenReturn(true);

        GraphSpaceGroupAPI.checkManagerPermission(auth, "SPACE_A",
                                                  "manager");
        Assert.assertThrows(ForbiddenException.class, () -> {
            GraphSpaceGroupAPI.checkManagerPermission(auth, "SPACE_B",
                                                      "manager");
        });
        Assert.assertThrows(ForbiddenException.class, () -> {
            GraphSpaceGroupAPI.checkManagerPermission(auth, "SPACE_A",
                                                      "ordinary");
        });
    }

    @Test
    public void testScopedGroupFilterNeverReturnsForeignOrLegacyGroup() {
        HugeGroup own = group("SPACE_A", 'a');
        HugeGroup foreign = group("SPACE_B", 'b');
        HugeGroup legacy = new HugeGroup("legacy-role");

        List<HugeGroup> filtered = GraphSpaceGroupAPI.filterScopedGroups(
                                   "SPACE_A",
                                   Arrays.asList(own, foreign, legacy));

        Assert.assertEquals(1, filtered.size());
        Assert.assertSame(own, filtered.get(0));
    }

    @Test
    public void testScopedGroupValidationRejectsForeignAndLegacyGroup() {
        HugeGroup own = group("SPACE_A", 'a');
        HugeGroup foreign = group("SPACE_B", 'b');
        HugeGroup legacy = new HugeGroup("legacy-role");

        GraphSpaceGroupAPI.checkScopedGroup("SPACE_A", own);
        Assert.assertThrows(ForbiddenException.class, () -> {
            GraphSpaceGroupAPI.checkScopedGroup("SPACE_A", foreign);
        });
        Assert.assertThrows(ForbiddenException.class, () -> {
            GraphSpaceGroupAPI.checkScopedGroup("SPACE_A", legacy);
        });
    }

    @Test
    public void testGetGroupRejectsMissingV2Result() {
        AuthManager auth = Mockito.mock(AuthManager.class);
        Mockito.when(auth.getGroup(Mockito.any())).thenReturn(null);

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            GraphSpaceGroupAPI.getGroup(auth, "missing");
        });
    }

    private static HugeGroup group(String graphSpace, char suffix) {
        return new HugeGroup(GraphSpaceGroupAPI.scopedPrefix(graphSpace) +
                             repeat(suffix, 32));
    }

    private static String repeat(char value, int count) {
        StringBuilder builder = new StringBuilder(count);
        for (int i = 0; i < count; i++) {
            builder.append(value);
        }
        return builder.toString();
    }
}
