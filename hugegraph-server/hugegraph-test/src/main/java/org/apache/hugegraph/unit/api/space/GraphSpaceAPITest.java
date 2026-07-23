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

package org.apache.hugegraph.unit.api.space;

import static org.apache.hugegraph.HugeFactory.SYS_GRAPH;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.api.auth.ManagerAPI;
import org.apache.hugegraph.api.profile.GraphsAPI;
import org.apache.hugegraph.api.space.GraphSpaceAPI;
import org.apache.hugegraph.auth.AuthManager;
import org.apache.hugegraph.auth.HugeAuthenticator;
import org.apache.hugegraph.auth.HugeDefaultRole;
import org.apache.hugegraph.auth.HugeGraphAuthProxy;
import org.apache.hugegraph.auth.HugeUser;
import org.apache.hugegraph.auth.RolePermission;
import org.apache.hugegraph.core.GraphManager;
import org.apache.hugegraph.meta.MetaManager;
import org.apache.hugegraph.space.GraphSpace;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.testutil.Whitebox;
import org.apache.hugegraph.unit.BaseUnitTest;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.junit.After;
import org.junit.Test;
import org.mockito.Mockito;

import jakarta.ws.rs.ForbiddenException;
import jakarta.ws.rs.core.SecurityContext;
import sun.misc.Unsafe;

public class GraphSpaceAPITest extends BaseUnitTest {

    private static final String GRAPHSPACE = "default_role_space";
    private static final String ADMIN = "admin";
    private static final String OPERATOR = "space_manager";
    private static final String TARGET = "target_user";
    private static final String GRAPH = "hugegraph";

    @After
    public void tearDown() {
        HugeGraphAuthProxy.resetContext();
    }

    @Test
    public void testSpaceManagerCannotCheckSpaceDefaultRole() {
        GraphSpaceAPI api = new GraphSpaceAPI();
        GraphManager manager = managerWithDefaultRoleContext(OPERATOR, false);
        setContext(OPERATOR);

        Assert.assertThrows(ForbiddenException.class, () -> {
            api.checkDefaultRole(manager, GRAPHSPACE, TARGET, "SPACE", null);
        });
    }

    @Test
    public void testAdminCanCheckSpaceDefaultRole() {
        GraphSpaceAPI api = new GraphSpaceAPI();
        GraphManager manager = managerWithDefaultRoleContext(ADMIN, true);
        setContext(ADMIN);

        String result = api.checkDefaultRole(manager, GRAPHSPACE, TARGET,
                                             "SPACE", null);

        Assert.assertContains("\"check\":true", result);
    }

    @Test
    public void testManagerDefaultRoleRejectsMissingGraphSpace() {
        ManagerAPI api = new ManagerAPI();
        GraphManager manager = managerWithDefaultRoleContext(TARGET, false);
        Whitebox.setInternalState(manager, "graphSpaces",
                                  new ConcurrentHashMap<>());
        setContext(TARGET);

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            api.checkDefaultRole(manager, GRAPHSPACE, "SPACE", null);
        }, e -> {
            Assert.assertContains("graph space", e.getMessage());
            Assert.assertContains("does not exist", e.getMessage());
        });
    }

    @Test
    public void testManagerDefaultRoleRejectsMissingObserverGraph() {
        ManagerAPI api = new ManagerAPI();
        GraphManager manager = managerWithDefaultRoleContext(TARGET, false);
        setContext(TARGET);

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            api.checkDefaultRole(manager, GRAPHSPACE, "OBSERVER", "missing");
        }, e -> {
            Assert.assertContains("Graph", e.getMessage());
            Assert.assertContains("does not exist", e.getMessage());
        });
    }

    @Test
    public void testManagerDefaultRoleAcceptsExistingResources() {
        ManagerAPI api = new ManagerAPI();
        GraphManager manager = managerWithDefaultRoleContext(TARGET, false);
        setContext(TARGET);

        String spaceResult = api.checkDefaultRole(manager, GRAPHSPACE,
                                                  "SPACE", null);
        String observerResult = api.checkDefaultRole(manager, GRAPHSPACE,
                                                     "OBSERVER", GRAPH);

        Assert.assertContains("\"check\":true", spaceResult);
        Assert.assertContains("\"check\":true", observerResult);
    }

    @Test
    public void testSpaceManagerCannotDeleteSpaceDefaultRole() {
        GraphSpaceAPI api = new GraphSpaceAPI();
        GraphManager manager = managerWithDefaultRoleContext(OPERATOR, false);
        setContext(OPERATOR);

        Assert.assertThrows(ForbiddenException.class, () -> {
            api.deleteDefaultRole(manager, GRAPHSPACE, TARGET, "SPACE", null);
        });
    }

    @Test
    public void testPdGraphsIncludesLoadedLocalConfigGraphs() {
        GraphManager manager = allocateGraphManager();
        Whitebox.setInternalState(manager, "PDExist", true);
        Whitebox.setInternalState(manager, "localGraphs",
                                  Collections.emptySet());
        MetaManager metaManager = Mockito.mock(MetaManager.class);
        Mockito.when(metaManager.graphConfigs("DEFAULT"))
               .thenReturn(Collections.emptyMap());
        Whitebox.setInternalState(manager, "metaManager", metaManager);

        Map<String, Graph> graphs = new ConcurrentHashMap<>();
        graphs.put("DEFAULT-hugegraph", Mockito.mock(Graph.class));
        graphs.put("DEFAULT-" + SYS_GRAPH, Mockito.mock(Graph.class));
        Whitebox.setInternalState(manager, "graphs", graphs);

        Set<String> result = manager.graphs("DEFAULT");

        Assert.assertTrue(result.contains("hugegraph"));
        Assert.assertFalse(result.contains(SYS_GRAPH));
    }

    @Test
    public void testAdminCanListGraphsWithoutGraphReadRole() {
        GraphManager manager = managerWithLoadedDefaultGraph(ADMIN, true);
        setContext(ADMIN);

        SecurityContext securityContext = Mockito.mock(SecurityContext.class);
        Mockito.when(securityContext.isUserInRole(Mockito.anyString()))
               .thenReturn(false);

        @SuppressWarnings("unchecked")
        Map<String, Object> result = (Map<String, Object>) new GraphsAPI()
                .list(manager, "DEFAULT", securityContext);

        Assert.assertEquals(Collections.singleton("hugegraph"),
                            result.get("graphs"));
    }

    private static GraphManager managerWithDefaultRoleContext(String operator,
                                                             boolean admin) {
        AuthManager authManager = Mockito.mock(AuthManager.class);
        Mockito.when(authManager.isAdminManager(operator)).thenReturn(admin);
        Mockito.when(authManager.isSpaceManager(GRAPHSPACE, operator))
               .thenReturn(!admin);
        Mockito.when(authManager.isDefaultRole(GRAPHSPACE, TARGET,
                                               HugeDefaultRole.SPACE))
               .thenReturn(true);
        Mockito.when(authManager.findUser(TARGET))
               .thenReturn(new HugeUser(TARGET));

        HugeAuthenticator authenticator = Mockito.mock(HugeAuthenticator.class);
        Mockito.when(authenticator.authManager()).thenReturn(authManager);

        GraphManager manager = allocateGraphManager();
        Whitebox.setInternalState(manager, "PDExist", true);
        Whitebox.setInternalState(manager, "authenticator", authenticator);
        Map<String, GraphSpace> spaces = new ConcurrentHashMap<>();
        spaces.put(GRAPHSPACE, new GraphSpace(GRAPHSPACE));
        Whitebox.setInternalState(manager, "graphSpaces", spaces);

        MetaManager metaManager = Mockito.mock(MetaManager.class);
        Mockito.when(metaManager.graphConfigs(GRAPHSPACE))
               .thenReturn(Collections.emptyMap());
        Whitebox.setInternalState(manager, "metaManager", metaManager);

        Map<String, Graph> graphs = new ConcurrentHashMap<>();
        graphs.put(GRAPHSPACE + "-" + GRAPH, Mockito.mock(HugeGraph.class));
        Whitebox.setInternalState(manager, "graphs", graphs);

        Mockito.when(authManager.isDefaultRole(GRAPHSPACE, GRAPH, TARGET,
                                               HugeDefaultRole.OBSERVER))
               .thenReturn(true);
        return manager;
    }

    private static GraphManager managerWithLoadedDefaultGraph(String operator,
                                                             boolean admin) {
        GraphManager manager = allocateGraphManager();
        Whitebox.setInternalState(manager, "PDExist", true);
        Whitebox.setInternalState(manager, "localGraphs",
                                  Collections.singleton("hugegraph"));
        MetaManager metaManager = Mockito.mock(MetaManager.class);
        Mockito.when(metaManager.graphConfigs("DEFAULT"))
               .thenReturn(Collections.emptyMap());
        Whitebox.setInternalState(manager, "metaManager", metaManager);

        AuthManager authManager = Mockito.mock(AuthManager.class);
        Mockito.when(authManager.isAdminManager(operator)).thenReturn(admin);
        HugeAuthenticator authenticator = Mockito.mock(HugeAuthenticator.class);
        Mockito.when(authenticator.authManager()).thenReturn(authManager);
        Whitebox.setInternalState(manager, "authenticator", authenticator);

        Map<String, GraphSpace> spaces = new ConcurrentHashMap<>();
        spaces.put("DEFAULT", new GraphSpace("DEFAULT"));
        Whitebox.setInternalState(manager, "graphSpaces", spaces);

        Map<String, Graph> graphs = new ConcurrentHashMap<>();
        graphs.put("DEFAULT-hugegraph", Mockito.mock(HugeGraph.class));
        Whitebox.setInternalState(manager, "graphs", graphs);
        return manager;
    }

    private static GraphManager allocateGraphManager() {
        try {
            Field field = Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            Unsafe unsafe = (Unsafe) field.get(null);
            return (GraphManager) unsafe.allocateInstance(GraphManager.class);
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    private static void setContext(String username) {
        try {
            HugeAuthenticator.User user = new HugeAuthenticator.User(
                    username, RolePermission.admin());
            HugeGraphAuthProxy.Context context =
                    new HugeGraphAuthProxy.Context(user);
            Method method = HugeGraphAuthProxy.class.getDeclaredMethod(
                    "setContext", HugeGraphAuthProxy.Context.class);
            method.setAccessible(true);
            method.invoke(null, context);
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }
}
