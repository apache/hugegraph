/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hugegraph.unit.api.space;

import org.apache.hugegraph.api.space.SchemaTemplateAPI;
import org.apache.hugegraph.auth.AuthManager;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.testutil.Whitebox;
import org.junit.Test;
import org.mockito.Mockito;

public class SchemaTemplateAPITest {

    private static final String GRAPHSPACE = "space";
    private static final String CREATOR = "creator";

    @Test
    public void testCreatorCanManageTemplate() {
        Assert.assertTrue(canManage(authManager(false, false), CREATOR));
    }

    @Test
    public void testGlobalAdminCanManageAnotherUsersTemplate() {
        Assert.assertTrue(canManage(authManager(true, false), "admin"));
    }

    @Test
    public void testSpaceManagerCanManageAnotherUsersTemplate() {
        Assert.assertTrue(canManage(authManager(false, true), "space-admin"));
    }

    @Test
    public void testUnrelatedUserCannotManageTemplate() {
        Assert.assertFalse(canManage(authManager(false, false), "member"));
    }

    private static AuthManager authManager(boolean admin,
                                           boolean spaceManager) {
        AuthManager auth = Mockito.mock(AuthManager.class);
        Mockito.when(auth.isAdminManager(Mockito.anyString()))
               .thenReturn(admin);
        Mockito.when(auth.isSpaceManager(GRAPHSPACE, "space-admin"))
               .thenReturn(spaceManager);
        return auth;
    }

    private static boolean canManage(AuthManager auth, String username) {
        return Whitebox.invokeStatic(
                SchemaTemplateAPI.class,
                new Class<?>[]{AuthManager.class, String.class,
                               String.class, String.class},
                "canManage",
                auth, GRAPHSPACE, CREATOR, username);
    }
}
