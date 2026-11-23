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

package org.apache.hugegraph.unit.api.profile;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.Set;

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.api.profile.GraphsAPI;
import org.apache.hugegraph.auth.AuthManager;
import org.apache.hugegraph.auth.HugeAuthenticator;
import org.apache.hugegraph.auth.HugeGraphAuthProxy;
import org.apache.hugegraph.auth.HugePermission;
import org.apache.hugegraph.auth.ResourceAuthorizer;
import org.apache.hugegraph.auth.RolePermission;
import org.apache.hugegraph.config.AuthOptions;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.core.GraphManager;
import org.apache.hugegraph.space.GraphSpace;
import org.apache.hugegraph.task.TaskScheduler;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.unit.BaseUnitTest;
import org.junit.After;
import org.junit.Test;
import org.mockito.Mockito;

import jakarta.ws.rs.core.SecurityContext;

public class GraphsAPIListTest extends BaseUnitTest {

    private static final String SPACE = "DEFAULT";

    @After
    public void tearDown() {
        HugeGraphAuthProxy.setResourceAuthorizer(null);
        HugeGraphAuthProxy.resetContext();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testListHidesGraphsDeniedByResourceAuthorizer() {
        // The user holds a wildcard role (as with Ranger), so the local role
        // check passes for every graph; only the authorizer can narrow it
        ResourceAuthorizer authorizer = Mockito.mock(ResourceAuthorizer.class);
        Mockito.when(authorizer.isAllowed(Mockito.anyString(), Mockito.eq(SPACE),
                                          Mockito.eq("allowed"), Mockito.any(),
                                          Mockito.any(), Mockito.any(),
                                          Mockito.anyBoolean()))
               .thenReturn(true);
        Mockito.when(authorizer.isAllowed(Mockito.anyString(), Mockito.eq(SPACE),
                                          Mockito.eq("denied"), Mockito.any(),
                                          Mockito.any(), Mockito.any(),
                                          Mockito.anyBoolean()))
               .thenReturn(false);
        HugeGraphAuthProxy.setResourceAuthorizer(authorizer);
        setContext(new HugeAuthenticator.User(
                "reader", RolePermission.role("*", "*", HugePermission.READ)));

        HugeGraph allowed = newProxy("allowed");
        HugeGraph denied = newProxy("denied");
        HugeGraph plain = Mockito.mock(HugeGraph.class);
        AuthManager authManager = Mockito.mock(AuthManager.class);
        GraphManager manager = Mockito.mock(GraphManager.class);
        Mockito.when(manager.graphSpace(SPACE)).thenReturn(new GraphSpace(SPACE));
        Mockito.when(manager.graphs(SPACE)).thenReturn(Set.of("allowed", "denied", "plain"));
        Mockito.when(manager.authManager()).thenReturn(authManager);
        Mockito.when(manager.graph(SPACE, "allowed")).thenReturn(allowed);
        Mockito.when(manager.graph(SPACE, "denied")).thenReturn(denied);
        // A graph without auth proxy (auth disabled) is listed as before
        Mockito.when(manager.graph(SPACE, "plain")).thenReturn(plain);
        SecurityContext sc = Mockito.mock(SecurityContext.class);
        Mockito.when(sc.isUserInRole(Mockito.anyString())).thenReturn(true);

        Map<String, Object> result = (Map<String, Object>) new GraphsAPI().list(manager, SPACE, sc);

        Assert.assertEquals(Set.of("allowed", "plain"), result.get("graphs"));
    }

    private static HugeGraphAuthProxy newProxy(String name) {
        HugeGraph graph = Mockito.mock(HugeGraph.class);
        HugeConfig config = Mockito.mock(HugeConfig.class);
        Mockito.when(graph.name()).thenReturn(name);
        Mockito.when(graph.graphSpace()).thenReturn(SPACE);
        Mockito.when(graph.spaceGraphName()).thenReturn(name);
        Mockito.when(graph.configuration()).thenReturn(config);
        Mockito.when(graph.authManager()).thenReturn(Mockito.mock(AuthManager.class));
        Mockito.when(graph.taskScheduler()).thenReturn(Mockito.mock(TaskScheduler.class));
        Mockito.when(config.get(AuthOptions.AUTH_CACHE_EXPIRE)).thenReturn(3600L);
        Mockito.when(config.get(AuthOptions.AUTH_CACHE_CAPACITY)).thenReturn(100L);
        Mockito.when(config.get(AuthOptions.AUTH_AUDIT_LOG_RATE)).thenReturn(1000D);
        return new HugeGraphAuthProxy(graph);
    }

    private static void setContext(HugeAuthenticator.User user) {
        try {
            Method method = HugeGraphAuthProxy.class.getDeclaredMethod(
                    "setContext", HugeGraphAuthProxy.Context.class);
            method.setAccessible(true);
            method.invoke(null, new HugeGraphAuthProxy.Context(user));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
