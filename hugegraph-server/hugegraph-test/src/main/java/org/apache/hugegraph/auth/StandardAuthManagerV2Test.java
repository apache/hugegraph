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

package org.apache.hugegraph.auth;

import java.util.Arrays;
import java.util.Collections;

import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.id.IdGenerator;
import org.apache.hugegraph.testutil.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class StandardAuthManagerV2Test {

    @Test
    public void testScopedWriteRejectsMismatchedEntityGraphSpace() {
        StandardAuthManagerV2.checkGraphSpace("SPACE_A", "SPACE_A");
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            StandardAuthManagerV2.checkGraphSpace("SPACE_A", "SPACE_B");
        });
    }

    @Test
    public void testScopedGroupValidationRejectsForeignOrMissingGroup() {
        HugeGroup own = group("SPACE_A", 'a');
        HugeGroup foreign = group("SPACE_B", 'b');

        StandardAuthManagerV2.checkScopedGroup("SPACE_A", own);
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            StandardAuthManagerV2.checkScopedGroup("SPACE_A", foreign);
        });
        try {
            StandardAuthManagerV2.checkScopedGroup("SPACE_A", null);
            Assert.fail("Expected a missing-group error");
        } catch (IllegalArgumentException e) {
            Assert.assertContains("does not exist", e.getMessage());
        }
    }

    @Test
    public void testDeleteGroupRelationsCascadesWithinGraphSpace() {
        AuthManager auth = Mockito.mock(AuthManager.class);
        Id group = IdGenerator.of("group");
        HugeBelong groupRole = groupRole("group-role", group);
        HugeBelong userGroup = userGroup("user-group", group);
        HugeBelong sourceCollision = userGroup(
                "source-collision", group, IdGenerator.of("other-group"));
        HugeBelong targetCollision = userRole("target-collision", group);
        HugeAccess access = access("access", group);

        Mockito.when(auth.listBelongByUser("SPACE_A", group, -1L))
               .thenReturn(Arrays.asList(groupRole, sourceCollision));
        Mockito.when(auth.listBelongByGroup("SPACE_A", group, -1L))
               .thenReturn(Arrays.asList(userGroup, targetCollision));
        Mockito.when(auth.listAccessByGroup("SPACE_A", group, -1L))
               .thenReturn(Collections.singletonList(access));

        StandardAuthManagerV2.deleteGroupRelations(auth, "SPACE_A", group);

        Mockito.verify(auth).listBelongByUser("SPACE_A", group, -1L);
        Mockito.verify(auth).listBelongByGroup("SPACE_A", group, -1L);
        Mockito.verify(auth).listAccessByGroup("SPACE_A", group, -1L);
        Mockito.verify(auth).deleteBelong("SPACE_A", groupRole.id());
        Mockito.verify(auth).deleteBelong("SPACE_A", userGroup.id());
        Mockito.verify(auth, Mockito.never())
               .deleteBelong("SPACE_A", sourceCollision.id());
        Mockito.verify(auth, Mockito.never())
               .deleteBelong("SPACE_A", targetCollision.id());
        Mockito.verify(auth).deleteAccess("SPACE_A", access.id());
        Mockito.verifyNoMoreInteractions(auth);
    }

    private static HugeBelong groupRole(String id, Id group) {
        HugeBelong belong = new HugeBelong(
                "SPACE_A", null, group, IdGenerator.of("role"),
                HugeBelong.GR);
        belong.id(IdGenerator.of(id));
        return belong;
    }

    private static HugeBelong userGroup(String id, Id group) {
        return userGroup(id, IdGenerator.of("user"), group);
    }

    private static HugeBelong userGroup(String id, Id user, Id group) {
        HugeBelong belong = new HugeBelong(
                "SPACE_A", user, group, null, HugeBelong.UG);
        belong.id(IdGenerator.of(id));
        return belong;
    }

    private static HugeBelong userRole(String id, Id role) {
        HugeBelong belong = new HugeBelong(
                "SPACE_A", IdGenerator.of("user"), null, role,
                HugeBelong.UR);
        belong.id(IdGenerator.of(id));
        return belong;
    }

    private static HugeAccess access(String id, Id group) {
        HugeAccess access = new HugeAccess("SPACE_A", group,
                                           IdGenerator.of("target"));
        access.id(IdGenerator.of(id));
        return access;
    }

    private static HugeGroup group(String graphSpace, char suffix) {
        String name = StandardAuthManagerV2.scopedGroupPrefix(graphSpace) +
                      repeat(suffix, 32);
        return new HugeGroup(name);
    }

    private static String repeat(char value, int count) {
        StringBuilder builder = new StringBuilder(count);
        for (int i = 0; i < count; i++) {
            builder.append(value);
        }
        return builder.toString();
    }
}
