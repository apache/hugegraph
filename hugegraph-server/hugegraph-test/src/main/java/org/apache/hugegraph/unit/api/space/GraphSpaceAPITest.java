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

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URI;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.HugeFactory;
import org.apache.hugegraph.LoadingMode;
import org.apache.hugegraph.api.auth.ManagerAPI;
import org.apache.hugegraph.api.filter.AuthenticationFilter.Authorizer;
import org.apache.hugegraph.api.profile.GraphsAPI;
import org.apache.hugegraph.api.space.GraphSpaceAPI;
import org.apache.hugegraph.auth.AuthManager;
import org.apache.hugegraph.auth.HugeAuthenticator;
import org.apache.hugegraph.auth.HugeDefaultRole;
import org.apache.hugegraph.auth.HugeGraphAuthProxy;
import org.apache.hugegraph.auth.HugePermission;
import org.apache.hugegraph.auth.HugeResource;
import org.apache.hugegraph.auth.ResourceType;
import org.apache.hugegraph.auth.HugeUser;
import org.apache.hugegraph.auth.RolePermission;
import org.apache.hugegraph.core.GraphManager;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.config.ServerOptions;
import org.apache.hugegraph.event.EventHub;
import org.apache.hugegraph.meta.MetaManager;
import org.apache.hugegraph.space.GraphSpace;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.testutil.Whitebox;
import org.apache.hugegraph.type.define.GraphMode;
import org.apache.hugegraph.unit.BaseUnitTest;
import org.apache.hugegraph.unit.FakeObjects;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.junit.After;
import org.junit.Test;
import org.glassfish.jersey.internal.MapPropertiesDelegate;
import org.glassfish.jersey.server.ContainerRequest;
import org.glassfish.jersey.server.filter.RolesAllowedDynamicFeature;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import jakarta.ws.rs.ForbiddenException;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.container.ResourceInfo;
import jakarta.ws.rs.core.FeatureContext;
import jakarta.ws.rs.ClientErrorException;
import jakarta.ws.rs.core.SecurityContext;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.core.MultivaluedHashMap;
import jakarta.ws.rs.core.UriInfo;

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
    public void testAdminCanCheckSpaceWideObserverRole() {
        GraphSpaceAPI api = new GraphSpaceAPI();
        GraphManager manager = managerWithDefaultRoleContext(ADMIN, true);
        setContext(ADMIN);

        String result = api.checkDefaultRole(manager, GRAPHSPACE, TARGET,
                                             "OBSERVER", null);

        Assert.assertContains("\"check\":true", result);
    }

    @Test
    public void testCurrentUserCanCheckSpaceWideObserverRole() {
        ManagerAPI api = new ManagerAPI();
        GraphManager manager = managerWithDefaultRoleContext(TARGET, false);
        setContext(TARGET);

        String result = api.checkDefaultRole(manager, GRAPHSPACE,
                                             "OBSERVER", null);

        Assert.assertContains("\"check\":true", result);
    }

    @Test
    public void testCurrentUserConcreteGraphCheckIncludesSpaceObserver() {
        ManagerAPI api = new ManagerAPI();
        GraphManager manager = managerWithDefaultRoleContext(TARGET, false);
        AuthManager auth = manager.authManager();
        Mockito.when(auth.isDefaultRole(GRAPHSPACE, GRAPH, TARGET,
                                        HugeDefaultRole.OBSERVER))
               .thenReturn(false);
        setContext(TARGET);

        String result = api.checkDefaultRole(manager, GRAPHSPACE,
                                             "OBSERVER", GRAPH);

        Assert.assertContains("\"check\":true", result);
        Mockito.verify(auth).isDefaultRole(GRAPHSPACE, TARGET,
                                           HugeDefaultRole.OBSERVER);
    }

    @Test
    public void testConcreteGraphCheckIncludesSpaceObserver() {
        GraphSpaceAPI api = new GraphSpaceAPI();
        GraphManager manager = managerWithDefaultRoleContext(ADMIN, true);
        AuthManager auth = manager.authManager();
        Mockito.when(auth.isDefaultRole(GRAPHSPACE, GRAPH, TARGET,
                                        HugeDefaultRole.OBSERVER))
               .thenReturn(false);
        setContext(ADMIN);

        String result = api.checkDefaultRole(manager, GRAPHSPACE, TARGET,
                                             "OBSERVER", GRAPH);

        Assert.assertContains("\"check\":true", result);
        Mockito.verify(auth).isDefaultRole(GRAPHSPACE, TARGET,
                                           HugeDefaultRole.OBSERVER);
    }

    @Test
    public void testSpaceObserverDeleteDoesNotModifyGraphRoles() {
        GraphSpaceAPI api = new GraphSpaceAPI();
        GraphManager manager = managerWithDefaultRoleContext(ADMIN, true);
        AuthManager auth = manager.authManager();
        setContext(ADMIN);

        api.deleteDefaultRole(manager, GRAPHSPACE, TARGET, "OBSERVER", null);

        Mockito.verify(auth).deleteDefaultRole(
                GRAPHSPACE, TARGET, HugeDefaultRole.OBSERVER);
        Mockito.verify(auth, Mockito.never()).deleteDefaultRole(
                Mockito.eq(GRAPHSPACE), Mockito.eq(TARGET),
                Mockito.eq(HugeDefaultRole.OBSERVER), Mockito.anyString());
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
        GraphManager manager = newGraphManager();
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
    public void testUnproxiedLoadingOwnerUsesAuthenticatedPrincipal() throws Exception {
        HugeConfig config = FakeObjects.newConfig();
        config.setProperty("backend", "memory");
        config.setProperty("serializer", "text");
        config.setProperty("store", "unproxied_loading_owner");
        HugeGraph graph = HugeFactory.open(config);
        try {
            GraphManager manager = Mockito.mock(GraphManager.class);
            Mockito.when(manager.graph("DEFAULT", "hugegraph")).thenReturn(graph);
            GraphsAPI api = new GraphsAPI();
            GraphsAPI.JsonLoadingOwner request = new GraphsAPI.JsonLoadingOwner();
            request.instance = (String) graph.loadingMode().get("instance");
            request.epoch = 0L;
            request.token = java.util.UUID.randomUUID().toString();
            request.action = "acquire";
            setContext("alice");
            Assert.assertEquals("active", api.loadingMode(manager, "DEFAULT", "hugegraph", request, null).get("state"));
            request.action = "release";
            setContext("bob");
            ClientErrorException conflict = org.junit.Assert.assertThrows(ClientErrorException.class,
                    () -> api.loadingMode(manager, "DEFAULT", "hugegraph", request, null));
            Assert.assertEquals(409, conflict.getResponse().getStatus());
            Assert.assertEquals(GraphMode.LOADING, graph.mode());
            setContext("alice");
            Assert.assertEquals("released", api.loadingMode(manager, "DEFAULT", "hugegraph", request, null).get("state"));
            Assert.assertEquals(GraphMode.NONE, graph.mode());
        } finally {
            graph.close();
            HugeFactory.remove(graph);
        }
    }

    @Test
    public void testLoadingOwnerRoutesAllowScopedStatusWriter() throws Exception {
        RolePermission role = RolePermission.role(GRAPHSPACE, GRAPH, HugePermission.WRITE);
        role.roles().get(GRAPHSPACE).get(GRAPH).put(HugePermission.WRITE,
                Collections.singletonMap(ResourceType.STATUS.name(),
                        Collections.singletonList(new HugeResource(ResourceType.STATUS, "*", null))));
        int endpoints = 0;
        for (Method method : GraphsAPI.class.getDeclaredMethods()) {
            if (!method.getName().equals("loadingMode")) {
                continue;
            }
            endpoints++;
            authorizeLoadingOwnerRoute(method, role, GRAPHSPACE, GRAPH);
            authorizeLoadingOwnerRoute(method, RolePermission.admin(), GRAPHSPACE, GRAPH);
            authorizeLoadingOwnerRoute(method, RolePermission.role(GRAPHSPACE, GRAPH, HugePermission.SPACE),
                                       GRAPHSPACE, GRAPH);
            Assert.assertThrows(ForbiddenException.class,
                    () -> authorizeLoadingOwnerRoute(method, role, "other_space", GRAPH));
            Assert.assertThrows(ForbiddenException.class,
                    () -> authorizeLoadingOwnerRoute(method, role, GRAPHSPACE, "other_graph"));
            Assert.assertThrows(ForbiddenException.class,
                    () -> authorizeLoadingOwnerRoute(method, RolePermission.none(), GRAPHSPACE, GRAPH));
        }
        Assert.assertEquals(2, endpoints);
    }

    private static void authorizeLoadingOwnerRoute(Method method, RolePermission role,
                                                   String space, String graph) throws IOException {
        GraphSpace graphSpace = new GraphSpace(space);
        graphSpace.auth(true);
        GraphManager manager = Mockito.mock(GraphManager.class);
        Mockito.when(manager.graphSpace(space)).thenReturn(graphSpace);
        MultivaluedHashMap<String, String> parameters = new MultivaluedHashMap<>();
        parameters.putSingle("graphspace", space);
        parameters.putSingle("name", graph);
        UriInfo uri = Mockito.mock(UriInfo.class);
        Mockito.when(uri.getPathParameters()).thenReturn(parameters);
        Authorizer authorizer = new Authorizer(manager, new HugeAuthenticator.User("writer", role), uri);
        ContainerRequestContext request = Mockito.mock(ContainerRequestContext.class);
        Mockito.when(request.getSecurityContext()).thenReturn(authorizer);
        ResourceInfo resource = Mockito.mock(ResourceInfo.class);
        Mockito.when(resource.getResourceMethod()).thenReturn(method);
        Mockito.doReturn(GraphsAPI.class).when(resource).getResourceClass();
        FeatureContext feature = Mockito.mock(FeatureContext.class);
        new RolesAllowedDynamicFeature().configure(resource, feature);
        ArgumentCaptor<Object> registered = ArgumentCaptor.forClass(Object.class);
        Mockito.verify(feature).register(registered.capture());
        ((ContainerRequestFilter) registered.getValue()).filter(request);
    }

    @Test
    public void testAuthDisabledSpaceRecoveryStillRequiresGlobalAdmin() throws Exception {
        HugeConfig config = FakeObjects.newConfig();
        config.setProperty("backend", "memory");
        config.setProperty("serializer", "text");
        config.setProperty("store", "unproxied_loading_recovery");
        HugeGraph graph = HugeFactory.open(config);
        try {
            Assert.assertFalse(graph instanceof HugeGraphAuthProxy);
            String space = "open_space";
            GraphSpace graphSpace = new GraphSpace(space);
            graphSpace.auth(false);
            GraphManager manager = Mockito.mock(GraphManager.class);
            Mockito.when(manager.requireAuthentication()).thenReturn(true);
            Mockito.when(manager.graphSpace(space)).thenReturn(graphSpace);
            Mockito.when(manager.graph(space, "hugegraph")).thenReturn(graph);
            UriInfo uri = Mockito.mock(UriInfo.class);
            MultivaluedHashMap<String, String> parameters = new MultivaluedHashMap<>();
            parameters.putSingle("graphspace", space);
            Mockito.when(uri.getPathParameters()).thenReturn(parameters);
            Authorizer restricted = new Authorizer(manager,
                    new HugeAuthenticator.User("bob", RolePermission.none()), uri);
            Assert.assertTrue(restricted.isUserInRole("admin"));
            Assert.assertFalse(RolePermission.isAdmin(restricted.role()));
            URI endpoint = URI.create("http://localhost/graphspaces/open_space/graphs/hugegraph/mode/owners");
            ContainerRequest context = new ContainerRequest(endpoint, endpoint, "PUT", restricted,
                                                             new MapPropertiesDelegate());
            GraphsAPI api = new GraphsAPI();
            Map<String, Object> state = graph.loadingMode();
            String instance = (String) state.get("instance");
            String token = java.util.UUID.randomUUID().toString();
            graph.loadingMode("acquire", instance, 0, token, "alice");
            Map<String, Object> before = api.loadingMode(manager, space, "hugegraph");
            GraphsAPI.JsonLoadingOwner request = new GraphsAPI.JsonLoadingOwner();
            request.action = "recover";
            request.instance = instance;
            request.epoch = (Long) before.get("epoch");
            request.revision = (Long) before.get("revision");
            request.confirm = "I'm sure all imports have stopped";
            ForbiddenException denied = org.junit.Assert.assertThrows(ForbiddenException.class,
                    () -> api.loadingMode(manager, space, "hugegraph", request, context));
            Assert.assertEquals(403, denied.getResponse().getStatus());
            Assert.assertEquals(before, graph.loadingMode());
            Assert.assertEquals(GraphMode.LOADING, graph.mode());

            for (HugeAuthenticator.User user : new HugeAuthenticator.User[]{HugeAuthenticator.User.ADMIN,
                                                                           HugeAuthenticator.User.ANONYMOUS}) {
                Mockito.when(manager.requireAuthentication()).thenReturn(user != HugeAuthenticator.User.ANONYMOUS);
                context.setSecurityContext(new Authorizer(manager, user, uri));
                state = graph.loadingMode();
                request.epoch = (Long) state.get("epoch");
                graph.loadingMode("acquire", instance, request.epoch, token, "alice");
                request.revision = (Long) graph.loadingMode().get("revision");
                Assert.assertEquals("recovered", api.loadingMode(manager, space, "hugegraph", request,
                                                                  context).get("state"));
                Assert.assertEquals(GraphMode.NONE, graph.mode());
            }
        } finally {
            graph.close();
            HugeFactory.remove(graph);
        }
    }

    @Test
    public void testLoadingRecoveryConfirmationAndConflictResponse() {
        GraphManager manager = Mockito.mock(GraphManager.class);
        HugeGraph graph = Mockito.mock(HugeGraph.class);
        Mockito.when(manager.graph("DEFAULT", "hugegraph")).thenReturn(graph);
        GraphsAPI api = new GraphsAPI();
        GraphsAPI.JsonLoadingOwner request = new GraphsAPI.JsonLoadingOwner();
        request.action = "recover";
        request.instance = "instance";
        request.epoch = 0L;
        ContainerRequestContext requestContext = Mockito.mock(ContainerRequestContext.class);
        Authorizer authorizer = Mockito.mock(Authorizer.class);
        Mockito.when(requestContext.getSecurityContext()).thenReturn(authorizer);
        Mockito.when(authorizer.role()).thenReturn(RolePermission.none());
        Assert.assertThrows(ForbiddenException.class,
                            () -> api.loadingMode(manager, "DEFAULT", "hugegraph", request, requestContext));
        Mockito.verify(graph, Mockito.never()).recoverLoadingMode(Mockito.anyString(), Mockito.anyLong(),
                                                                Mockito.anyLong());
        Mockito.when(authorizer.role()).thenReturn(RolePermission.admin());
        Assert.assertThrows(IllegalArgumentException.class,
                            () -> api.loadingMode(manager, "DEFAULT", "hugegraph", request, requestContext));
        Mockito.verify(graph, Mockito.never()).loadingMode(Mockito.anyString(), Mockito.anyString(),
                                                         Mockito.anyLong(), Mockito.any(), Mockito.anyString());
        request.confirm = "I'm sure all imports have stopped";
        Assert.assertThrows(IllegalArgumentException.class,
                            () -> api.loadingMode(manager, "DEFAULT", "hugegraph", request, requestContext));
        request.revision = 0L;
        Map<String, Object> receipt = Collections.singletonMap("state", "recovered");
        Mockito.when(graph.recoverLoadingMode("instance", 0, 0)).thenReturn(receipt);
        Assert.assertSame(receipt, api.loadingMode(manager, "DEFAULT", "hugegraph", request, requestContext));

        Mockito.when(graph.recoverLoadingMode("instance", 0, 0))
               .thenThrow(new LoadingMode.ConflictException("stale epoch"));
        ClientErrorException ownerConflict = org.junit.Assert.assertThrows(ClientErrorException.class,
                () -> api.loadingMode(manager, "DEFAULT", "hugegraph", request, requestContext));
        Assert.assertEquals(409, ownerConflict.getResponse().getStatus());
        Mockito.doThrow(new LoadingMode.ConflictException("active owners")).when(graph).mode(GraphMode.NONE);
        ClientErrorException modeConflict = org.junit.Assert.assertThrows(ClientErrorException.class,
                () -> api.mode(manager, "DEFAULT", "hugegraph", GraphMode.NONE));
        Assert.assertEquals(409, modeConflict.getResponse().getStatus());
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
        Mockito.when(authManager.isDefaultRole(GRAPHSPACE, TARGET,
                                               HugeDefaultRole.OBSERVER))
               .thenReturn(true);
        Mockito.when(authManager.findUser(TARGET))
               .thenReturn(new HugeUser(TARGET));

        HugeAuthenticator authenticator = Mockito.mock(HugeAuthenticator.class);
        Mockito.when(authenticator.authManager()).thenReturn(authManager);

        GraphManager manager = newGraphManager();
        Whitebox.setInternalState(manager, "PDExist", true);
        Whitebox.setInternalState(manager, "authenticator", authenticator);
        Map<String, GraphSpace> spaces = new ConcurrentHashMap<>();
        spaces.put(GRAPHSPACE, new GraphSpace(GRAPHSPACE));
        Whitebox.setInternalState(manager, "graphSpaces", spaces);

        MetaManager metaManager = Mockito.mock(MetaManager.class);
        Mockito.when(metaManager.graphConfigs(GRAPHSPACE))
               .thenReturn(Collections.singletonMap(
                       GRAPHSPACE + "-" + GRAPH, Collections.emptyMap()));
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
        GraphManager manager = newGraphManager();
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

    private static GraphManager newGraphManager() {
        HugeConfig config = FakeObjects.newConfig();
        config.setProperty(ServerOptions.USE_PD.name(), false);
        config.setProperty(ServerOptions.GRAPH_LOAD_FROM_LOCAL_CONFIG.name(), false);
        return new GraphManager(config, new EventHub("graph-space-api-test"));
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
