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

package org.apache.hugegraph.unit.api.filter;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.hugegraph.api.filter.AuthenticationFilter.Authorizer;
import org.apache.hugegraph.api.graph.EdgeAPI;
import org.apache.hugegraph.api.graph.VertexAPI;
import org.apache.hugegraph.auth.HugeAuthenticator.User;
import org.apache.hugegraph.auth.HugePermission;
import org.apache.hugegraph.auth.RolePermission;
import org.apache.hugegraph.core.GraphManager;
import org.apache.hugegraph.space.GraphSpace;
import org.glassfish.jersey.server.filter.RolesAllowedDynamicFeature;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import jakarta.annotation.security.RolesAllowed;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.ForbiddenException;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.container.ResourceInfo;
import jakarta.ws.rs.core.FeatureContext;
import jakarta.ws.rs.core.MultivaluedHashMap;
import jakarta.ws.rs.core.MultivaluedMap;
import jakarta.ws.rs.core.UriInfo;

public class VertexAuthorizationTest {

    private static final String SPACE = "vertex_scope_space";
    private static final String GRAPH = "shared_graph_name";

    @Test
    public void testScopedVertexPermissions() throws IOException {
        List<String> denied = new ArrayList<>();
        for (Method method : endpoints(VertexAPI.class)) {
            RolePermission role = RolePermission.role(SPACE, GRAPH, action(method));
            try {
                authorize(method, role, SPACE, GRAPH);
            } catch (ForbiddenException failure) {
                denied.add(method.toGenericString());
            }
        }
        Assert.assertTrue("Scoped vertex permissions denied: " + denied, denied.isEmpty());
    }

    @Test
    public void testSameGraphNameInAnotherSpaceIsDenied() {
        for (Method method : endpoints(VertexAPI.class)) {
            RolePermission role = RolePermission.role(SPACE, GRAPH, action(method));
            Assert.assertThrows(ForbiddenException.class,
                                () -> authorize(method, role, "another_space", GRAPH));
        }
    }

    @Test
    public void testAnotherGraphIsDenied() {
        for (Method method : endpoints(VertexAPI.class)) {
            RolePermission role = RolePermission.role(SPACE, GRAPH, action(method));
            Assert.assertThrows(ForbiddenException.class,
                                () -> authorize(method, role, SPACE, "another_graph"));
        }
    }

    @Test
    public void testReadPermissionCannotWriteOrDelete() throws IOException {
        RolePermission role = RolePermission.role(SPACE, GRAPH, HugePermission.READ);
        for (Method method : endpoints(VertexAPI.class)) {
            if (action(method) == HugePermission.READ) {
                authorize(method, role, SPACE, GRAPH);
            } else {
                Assert.assertThrows(ForbiddenException.class,
                                    () -> authorize(method, role, SPACE, GRAPH));
            }
        }
    }

    @Test
    public void testDefaultSpaceWithoutPathParameter() throws IOException {
        for (Method method : endpoints(VertexAPI.class)) {
            RolePermission role = RolePermission.role("DEFAULT", GRAPH, action(method));
            authorize(method, role, null, GRAPH);
        }
    }

    @Test
    public void testAdminAndSpaceMemberRemainAuthorized() throws IOException {
        RolePermission member = RolePermission.role(SPACE, GRAPH, HugePermission.READ);
        member.roles().get(SPACE).get(GRAPH).put(HugePermission.SPACE_MEMBER, Collections.emptyMap());
        for (Method method : endpoints(VertexAPI.class)) {
            authorize(method, RolePermission.admin(), SPACE, GRAPH);
            if (!method.getName().equals("list")) {
                authorize(method, member, SPACE, GRAPH);
            }
        }
    }

    @Test
    public void testEdgeEndpointsUseTheSameScopedPermissions() throws IOException {
        for (Method method : endpoints(EdgeAPI.class)) {
            authorize(method, RolePermission.role(SPACE, GRAPH, action(method)), SPACE, GRAPH);
        }
    }

    private static List<Method> endpoints(Class<?> api) {
        List<Method> methods = Arrays.stream(api.getDeclaredMethods())
                                    .filter(method -> method.isAnnotationPresent(RolesAllowed.class))
                                    .toList();
        Assert.assertEquals(7, methods.size());
        return methods;
    }

    private static HugePermission action(Method method) {
        if (method.isAnnotationPresent(GET.class)) {
            return HugePermission.READ;
        }
        return method.isAnnotationPresent(DELETE.class) ? HugePermission.DELETE : HugePermission.WRITE;
    }

    private static void authorize(Method method, RolePermission role,
                                  String space, String graph) throws IOException {
        GraphSpace graphSpace = new GraphSpace(SPACE);
        graphSpace.auth(true);
        GraphManager manager = Mockito.mock(GraphManager.class);
        Mockito.when(manager.graphSpace(Mockito.anyString())).thenReturn(graphSpace);
        MultivaluedMap<String, String> parameters = new MultivaluedHashMap<>();
        parameters.putSingle("graph", graph);
        if (space != null) {
            parameters.putSingle("graphspace", space);
        }
        UriInfo uri = Mockito.mock(UriInfo.class);
        Mockito.when(uri.getPathParameters()).thenReturn(parameters);
        Authorizer authorizer = new Authorizer(manager, new User("scoped_user", role), uri);
        ContainerRequestContext request = Mockito.mock(ContainerRequestContext.class);
        Mockito.when(request.getSecurityContext()).thenReturn(authorizer);
        ResourceInfo resource = Mockito.mock(ResourceInfo.class);
        Mockito.when(resource.getResourceMethod()).thenReturn(method);
        Mockito.doReturn(method.getDeclaringClass()).when(resource).getResourceClass();
        FeatureContext feature = Mockito.mock(FeatureContext.class);
        new RolesAllowedDynamicFeature().configure(resource, feature);
        ArgumentCaptor<Object> registered = ArgumentCaptor.forClass(Object.class);
        Mockito.verify(feature).register(registered.capture());
        ((ContainerRequestFilter) registered.getValue()).filter(request);
    }
}
