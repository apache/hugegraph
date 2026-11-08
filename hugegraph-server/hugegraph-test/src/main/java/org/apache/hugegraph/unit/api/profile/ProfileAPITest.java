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

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hugegraph.api.gremlin.GremlinAPI;
import org.apache.hugegraph.api.profile.ProfileAPI;
import org.apache.hugegraph.api.profile.VersionAPI;
import org.apache.hugegraph.api.space.GraphSpaceAPI;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.util.JsonUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Application;
import jakarta.ws.rs.core.UriInfo;

public class ProfileAPITest {

    @Before
    @After
    public void clearCachedProfile() throws Exception {
        java.lang.reflect.Field field = ProfileAPI.class.getDeclaredField("API_PROFILES");
        field.setAccessible(true);
        ((Map<?, ?>) field.get(null)).clear();
    }

    @Test
    public void testIndexUsesCallableServerPaths() {
        List<Map<String, Object>> profiles = profiles(Set.of(
                ProfileAPI.class, GremlinAPI.class, VersionAPI.class, GraphSpaceAPI.class));
        List<String> urls = new ArrayList<>();
        for (Map<String, Object> profile : profiles) {
            String url = (String) profile.get("url");
            urls.add(url);
            Assert.assertTrue(url.startsWith("/"));
            Assert.assertFalse(url.startsWith("//"));
            Assert.assertFalse(url.startsWith("/apis/"));
            Assert.assertFalse(url.contains("//"));
        }
        Assert.assertTrue(urls.contains("/gremlin"));
        Assert.assertTrue(urls.contains("/versions"));
        Assert.assertTrue(urls.contains("/graphspaces"));
        Assert.assertTrue(urls.contains("/apis"));
        Assert.assertTrue(urls.contains("/"));
    }

    @Test
    public void testPathsResolveFromIndexWithOrWithoutTrailingSlash() {
        for (Map<String, Object> profile : profiles(Set.of(GremlinAPI.class, VersionAPI.class))) {
            String path = (String) profile.get("url");
            for (String index : List.of("http://localhost:8080/apis", "http://localhost:8080/apis/")) {
                URI resolved = URI.create(index).resolve(path);
                Assert.assertEquals(path, resolved.getPath());
                Assert.assertEquals("localhost", resolved.getHost());
                Assert.assertEquals(8080, resolved.getPort());
            }
        }
    }

    @Test
    public void testIndexPreservesNonRootApplicationPath() {
        Set<Class<?>> classes = Set.of(ProfileAPI.class, GremlinAPI.class, VersionAPI.class,
                                       GraphSpaceAPI.class, ExampleAPI.class);
        Assert.assertTrue(profiles(classes).stream()
                          .anyMatch(p -> "/gremlin".equals(p.get("url"))));

        List<Map<String, Object>> prefixed = profiles(
                classes, URI.create("http://localhost:8080/hg/"));
        List<String> paths = new ArrayList<>();
        for (Map<String, Object> profile : prefixed) {
            String path = (String) profile.get("url");
            Assert.assertTrue(path.startsWith("/hg/"));
            Assert.assertFalse(path.contains("//"));
            paths.add(path);
        }
        for (String path : List.of("/hg/", "/hg/apis", "/hg/gremlin",
                                   "/hg/versions", "/hg/graphspaces", "/hg/example/items")) {
            Assert.assertTrue(paths.contains(path));
        }
        Assert.assertEquals(prefixed, profiles(classes, URI.create("http://localhost:8080/hg")));
        for (String index : List.of("http://localhost:8080/hg/apis",
                                    "http://localhost:8080/hg/apis/")) {
            Assert.assertEquals("/hg/gremlin",
                                URI.create(index).resolve("/hg/gremlin").getPath());
        }
    }

    @Test
    public void testChildMethodsAndParametersRemainAvailable() {
        List<Map<String, Object>> profiles = profiles(Set.of(ExampleAPI.class));
        Assert.assertEquals(2, profiles.size());
        Map<String, Object> child = profiles.stream()
                .filter(profile -> "POST".equals(profile.get("method")))
                .findFirst().orElseThrow();
        Assert.assertEquals("/example/items", child.get("url"));
        List<?> parameters = (List<?>) child.get("parameters");
        Assert.assertEquals(2, parameters.size());
        Map<?, ?> query = (Map<?, ?>) parameters.get(0);
        Assert.assertEquals("limit", query.get("name"));
        Assert.assertEquals("10", query.get("default_value"));
        Assert.assertEquals("int", query.get("type"));
        Assert.assertEquals("body", ((Map<?, ?>) parameters.get(1)).get("name"));
    }

    @Test
    public void testClassesWithoutRoutesAreIgnored() {
        Assert.assertTrue(profiles(Set.of(String.class)).isEmpty());
    }

    private static List<Map<String, Object>> profiles(Set<Class<?>> classes) {
        return profiles(classes, URI.create("http://localhost:8080/"));
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> profiles(Set<Class<?>> classes, URI baseUri) {
        Application application = new Application() {
            @Override
            public Set<Class<?>> getClasses() {
                return classes;
            }
        };
        UriInfo uriInfo = Mockito.mock(UriInfo.class);
        Mockito.when(uriInfo.getBaseUri()).thenReturn(baseUri);
        Map<String, Object> parsed = JsonUtil.fromJson(
                new ProfileAPI().showAllAPIs(application, uriInfo), Map.class);
        Map<String, Map<String, List<Map<String, Object>>>> categories =
                (Map<String, Map<String, List<Map<String, Object>>>>) parsed.get("apis");
        List<Map<String, Object>> result = new ArrayList<>();
        categories.values().forEach(category -> category.values().forEach(result::addAll));
        return result;
    }

    @Path("/example/")
    public static class ExampleAPI {

        @GET
        public String list() {
            return "[]";
        }

        @POST
        @Path("items")
        public String create(@QueryParam("limit") @DefaultValue("10") int limit,
                             String body) {
            return body;
        }
    }
}
