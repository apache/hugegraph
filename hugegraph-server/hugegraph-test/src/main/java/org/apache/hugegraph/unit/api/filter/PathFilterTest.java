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
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.hugegraph.api.filter.PathFilter;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.config.ServerOptions;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.unit.BaseUnitTest;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import jakarta.inject.Provider;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.core.PathSegment;
import jakarta.ws.rs.core.UriBuilder;
import jakarta.ws.rs.core.UriInfo;

/**
 * Unit tests for PathFilter
 * Test scenarios:
 * 1. Whitelist paths are not redirected
 * 2. Normal paths are correctly prefixed with graphspace
 * 3. Query parameters are preserved
 * 4. Special characters and encoding handling
 * 5. Edge cases (empty path, root path, etc.)
 */
public class PathFilterTest extends BaseUnitTest {

    private PathFilter pathFilter;
    private Provider<HugeConfig> configProvider;
    private HugeConfig config;
    private ContainerRequestContext requestContext;
    private UriInfo uriInfo;

    @Before
    public void setup() {
        // Create configuration
        Configuration conf = new PropertiesConfiguration();
        conf.setProperty(ServerOptions.PATH_GRAPH_SPACE.name(), "DEFAULT");
        this.config = new HugeConfig(conf);

        // Create Provider
        this.configProvider = () -> config;

        // Create PathFilter and inject Provider
        this.pathFilter = new PathFilter();
        injectProvider(this.pathFilter, this.configProvider);

        // Mock request context and uriInfo
        this.requestContext = Mockito.mock(ContainerRequestContext.class);
        this.uriInfo = Mockito.mock(UriInfo.class);
        Mockito.when(this.requestContext.getUriInfo()).thenReturn(this.uriInfo);
    }

    /**
     * Inject configProvider using reflection
     */
    private void injectProvider(PathFilter filter, Provider<HugeConfig> provider) {
        try {
            java.lang.reflect.Field field = PathFilter.class.getDeclaredField("configProvider");
            field.setAccessible(true);
            field.set(filter, provider);
        } catch (Exception e) {
            throw new RuntimeException("Failed to inject configProvider", e);
        }
    }

    /**
     * Create PathSegment mock
     */
    private PathSegment createPathSegment(String path) {
        PathSegment segment = Mockito.mock(PathSegment.class);
        Mockito.when(segment.getPath()).thenReturn(path);
        return segment;
    }

    /**
     * Setup URI information
     */
    private void setupUriInfo(String basePath, String requestPath, List<String> segments,
                              String query) {
        URI baseUri = URI.create("http://localhost:8080" + basePath);
        URI requestUri =
                query != null ? URI.create("http://localhost:8080" + requestPath + "?" + query) :
                URI.create("http://localhost:8080" + requestPath);

        Mockito.when(uriInfo.getBaseUri()).thenReturn(baseUri);
        Mockito.when(uriInfo.getRequestUri()).thenReturn(requestUri);

        List<PathSegment> pathSegments = new ArrayList<>();
        for (String segment : segments) {
            pathSegments.add(createPathSegment(segment));
        }
        Mockito.when(uriInfo.getPathSegments()).thenReturn(pathSegments);
        Mockito.when(uriInfo.getPath()).thenReturn(String.join("/", segments));

        // Mock UriBuilder - capture the path passed to uri() method
        final String[] capturedPath = new String[1];
        UriBuilder uriBuilder = Mockito.mock(UriBuilder.class);
        Mockito.when(uriInfo.getRequestUriBuilder()).thenReturn(uriBuilder);
        Mockito.when(uriBuilder.uri(Mockito.anyString())).thenAnswer(invocation -> {
            capturedPath[0] = invocation.getArgument(0);
            return uriBuilder;
        });
        Mockito.when(uriBuilder.build()).thenAnswer(invocation -> {
            // Build URI based on captured path and preserve query parameters
            String path = capturedPath[0] != null ? capturedPath[0] : requestPath;
            return URI.create("http://localhost:8080" + path + (query != null ? "?" + query : ""));
        });
    }

    /**
     * Test whitelist API - empty path
     */
    @Test
    public void testWhiteListApi_EmptyPath() throws IOException {
        setupUriInfo("/", "/", List.of(""), null);

        pathFilter.filter(requestContext);

        // Verify whitelist API does not trigger setRequestUri
        Mockito.verify(requestContext, Mockito.never()).setRequestUri(
                Mockito.any(URI.class), Mockito.any(URI.class));
        // Verify request timestamp is set
        Mockito.verify(requestContext).setProperty(
                Mockito.eq(PathFilter.REQUEST_TIME), Mockito.anyLong());
    }

    /**
     * Test whitelist API - /apis
     */
    @Test
    public void testWhiteListApi_Apis() throws IOException {
        setupUriInfo("/", "/apis", List.of("apis"), null);

        pathFilter.filter(requestContext);

        Mockito.verify(requestContext, Mockito.never()).setRequestUri(
                Mockito.any(URI.class), Mockito.any(URI.class));
    }

    /**
     * Test whitelist API - /gremlin
     */
    @Test
    public void testWhiteListApi_Gremlin() throws IOException {
        setupUriInfo("/", "/gremlin", List.of("gremlin"), null);

        pathFilter.filter(requestContext);

        Mockito.verify(requestContext, Mockito.never()).setRequestUri(
                Mockito.any(URI.class), Mockito.any(URI.class));
    }

    /**
     * Test whitelist API - /auth (single segment)
     */
    @Test
    public void testWhiteListApi_Auth() throws IOException {
        setupUriInfo("/", "/auth", List.of("auth"), null);

        pathFilter.filter(requestContext);

        Mockito.verify(requestContext, Mockito.never())
               .setRequestUri(Mockito.any(URI.class), Mockito.any(URI.class));
    }

    /**
     * Test whitelist API - /auth/users (multi-segment path)
     */
    @Test
    public void testWhiteListApi_AuthUsers_MultiSegment() throws IOException {
        // Test complete /auth/users path with all segments
        setupUriInfo("/", "/auth/users", Arrays.asList("auth", "users"), null);

        pathFilter.filter(requestContext);

        // Should not be redirected (first segment "auth" matches whitelist)
        Mockito.verify(requestContext, Mockito.never()).setRequestUri(
                Mockito.any(URI.class), Mockito.any(URI.class));
    }

    /**
     * Test graphspaces path is not redirected
     */
    @Test
    public void testGraphSpacePath_NotRedirected() throws IOException {
        setupUriInfo("/", "/graphspaces/space1/graphs",
                     Arrays.asList("graphspaces", "space1", "graphs"), null);

        pathFilter.filter(requestContext);

        Mockito.verify(requestContext, Mockito.never()).setRequestUri(
                Mockito.any(URI.class), Mockito.any(URI.class));
    }

    /**
     * Test arthas path is not redirected
     */
    @Test
    public void testArthasPath_NotRedirected() throws IOException {
        setupUriInfo("/", "/arthas/api", Arrays.asList("arthas", "api"), null);

        pathFilter.filter(requestContext);

        Mockito.verify(requestContext, Mockito.never()).setRequestUri(
                Mockito.any(URI.class), Mockito.any(URI.class));
    }

    /**
     * Test normal path is correctly redirected - single segment
     */
    @Test
    public void testNormalPath_SingleSegment() throws IOException {
        setupUriInfo("/", "/graphs", List.of("graphs"), null);

        pathFilter.filter(requestContext);

        // Verify redirect is called with correct path
        ArgumentCaptor<URI> uriCaptor = ArgumentCaptor.forClass(URI.class);
        Mockito.verify(requestContext).setRequestUri(Mockito.any(URI.class), uriCaptor.capture());

        URI capturedUri = uriCaptor.getValue();
        Assert.assertTrue("Redirect URI should contain graphspaces/DEFAULT prefix",
                          capturedUri.getPath().startsWith("/graphspaces/DEFAULT/graphs"));
        Assert.assertEquals("/graphspaces/DEFAULT/graphs", capturedUri.getPath());
    }

    /**
     * Test normal path is correctly redirected - multiple segments
     */
    @Test
    public void testNormalPath_MultipleSegments() throws IOException {
        setupUriInfo("/", "/graphs/hugegraph/vertices",
                     Arrays.asList("graphs", "hugegraph", "vertices"), null);

        pathFilter.filter(requestContext);

        // Verify redirect is called with correct path
        ArgumentCaptor<URI> uriCaptor = ArgumentCaptor.forClass(URI.class);
        Mockito.verify(requestContext).setRequestUri(Mockito.any(URI.class), uriCaptor.capture());

        URI capturedUri = uriCaptor.getValue();
        Assert.assertEquals("/graphspaces/DEFAULT/graphs/hugegraph/vertices",
                            capturedUri.getPath());
    }

    /**
     * Test query parameters are preserved
     */
    @Test
    public void testQueryParameters_Preserved() throws IOException {
        String queryString = "limit=10&offset=20&label=person";
        setupUriInfo("/", "/graphs/hugegraph/vertices",
                     Arrays.asList("graphs", "hugegraph", "vertices"), queryString);

        URI originalRequestUri = uriInfo.getRequestUri();
        Assert.assertTrue("Original URI should contain query string",
                          originalRequestUri.toString().contains(queryString));

        pathFilter.filter(requestContext);

        // Use ArgumentCaptor to capture the actual URI passed to setRequestUri
        ArgumentCaptor<URI> uriCaptor = ArgumentCaptor.forClass(URI.class);
        Mockito.verify(requestContext).setRequestUri(Mockito.any(URI.class), uriCaptor.capture());

        URI capturedUri = uriCaptor.getValue();
        // Verify query parameters are indeed preserved
        Assert.assertNotNull("Query parameters should be preserved", capturedUri.getQuery());
        Assert.assertTrue("Query should contain limit parameter",
                          capturedUri.getQuery().contains("limit=10"));
        Assert.assertTrue("Query should contain offset parameter",
                          capturedUri.getQuery().contains("offset=20"));
        Assert.assertTrue("Query should contain label parameter",
                          capturedUri.getQuery().contains("label=person"));
    }

    /**
     * Test special characters in path handling
     */
    @Test
    public void testSpecialCharacters_InPath() throws IOException {
        setupUriInfo("/", "/schema/vertexlabels/person-label",
                     Arrays.asList("schema", "vertexlabels", "person-label"), null);

        pathFilter.filter(requestContext);

        ArgumentCaptor<URI> uriCaptor = ArgumentCaptor.forClass(URI.class);
        Mockito.verify(requestContext).setRequestUri(Mockito.any(URI.class), uriCaptor.capture());

        URI capturedUri = uriCaptor.getValue();
        Assert.assertEquals("/graphspaces/DEFAULT/schema/vertexlabels/person-label",
                            capturedUri.getPath());
    }

    /**
     * Test URL encoded characters handling
     */
    @Test
    public void testUrlEncoded_Characters() throws IOException {
        // Path contains encoded space %20
        setupUriInfo("/", "/schema/propertykeys/my%20key",
                     Arrays.asList("schema", "propertykeys", "my%20key"), null);

        pathFilter.filter(requestContext);

        ArgumentCaptor<URI> uriCaptor = ArgumentCaptor.forClass(URI.class);
        Mockito.verify(requestContext).setRequestUri(Mockito.any(URI.class), uriCaptor.capture());

        URI capturedUri = uriCaptor.getValue();
        // URI automatically decodes %20 to space
        Assert.assertEquals("/graphspaces/DEFAULT/schema/propertykeys/my key",
                            capturedUri.getPath());
    }

    /**
     * Test custom graph space configuration
     */
    @Test
    public void testCustomGraphSpace_Configuration() throws IOException {
        // Modify configuration to custom graph space
        Configuration customConf = new PropertiesConfiguration();
        customConf.setProperty(ServerOptions.PATH_GRAPH_SPACE.name(), "CUSTOM_SPACE");
        HugeConfig customConfig = new HugeConfig(customConf);

        Provider<HugeConfig> customProvider = () -> customConfig;
        injectProvider(this.pathFilter, customProvider);

        setupUriInfo("/", "/graphs/test", Arrays.asList("graphs", "test"), null);

        pathFilter.filter(requestContext);

        ArgumentCaptor<URI> uriCaptor = ArgumentCaptor.forClass(URI.class);
        Mockito.verify(requestContext).setRequestUri(Mockito.any(URI.class), uriCaptor.capture());

        URI capturedUri = uriCaptor.getValue();
        Assert.assertEquals("/graphspaces/CUSTOM_SPACE/graphs/test", capturedUri.getPath());
    }

    /**
     * Test deeply nested path
     */
    @Test
    public void testDeeplyNested_Path() throws IOException {
        setupUriInfo("/", "/graphs/hugegraph/traversers/shortestpath",
                     Arrays.asList("graphs", "hugegraph", "traversers", "shortestpath"), null);

        pathFilter.filter(requestContext);

        ArgumentCaptor<URI> uriCaptor = ArgumentCaptor.forClass(URI.class);
        Mockito.verify(requestContext).setRequestUri(Mockito.any(URI.class), uriCaptor.capture());

        URI capturedUri = uriCaptor.getValue();
        Assert.assertEquals("/graphspaces/DEFAULT/graphs/hugegraph/traversers/shortestpath",
                            capturedUri.getPath());
    }

    /**
     * Test isWhiteAPI static method - single segment whitelist paths
     * Note: PathFilter.isWhiteAPI() only checks the first segment in actual usage
     */
    @Test
    public void testIsWhiteAPI_AllWhiteListPaths() {
        // Test single-segment whitelist entries (as used in PathFilter.filter())
        Assert.assertTrue(PathFilter.isWhiteAPI(""));
        Assert.assertTrue(PathFilter.isWhiteAPI("apis"));
        Assert.assertTrue(PathFilter.isWhiteAPI("metrics"));
        Assert.assertTrue(PathFilter.isWhiteAPI("versions"));
        Assert.assertTrue(PathFilter.isWhiteAPI("health"));
        Assert.assertTrue(PathFilter.isWhiteAPI("gremlin"));
        Assert.assertTrue(PathFilter.isWhiteAPI("auth"));
        Assert.assertTrue(PathFilter.isWhiteAPI("hstore"));
        Assert.assertTrue(PathFilter.isWhiteAPI("pd"));
        Assert.assertTrue(PathFilter.isWhiteAPI("kafka"));
        Assert.assertTrue(PathFilter.isWhiteAPI("openapi.json"));
    }

    /**
     * Test isWhiteAPI static method - multi-segment strings
     * Note: This tests the static method directly with multi-segment strings,
     * but in actual usage, only the first segment is passed to isWhiteAPI()
     */
    @Test
    public void testIsWhiteAPI_MultiSegmentStrings() {
        // These are how multi-segment entries are stored in the whitelist set
        Assert.assertTrue(PathFilter.isWhiteAPI("auth/users"));
        Assert.assertTrue(PathFilter.isWhiteAPI("graphs/auth"));
        Assert.assertTrue(PathFilter.isWhiteAPI("graphs/auth/users"));
    }

    /**
     * Test isWhiteAPI static method - non-whitelist paths
     */
    @Test
    public void testIsWhiteAPI_NonWhiteListPaths() {
        Assert.assertFalse(PathFilter.isWhiteAPI("graphs"));
        Assert.assertFalse(PathFilter.isWhiteAPI("schema"));
        Assert.assertFalse(PathFilter.isWhiteAPI("vertices"));
        Assert.assertFalse(PathFilter.isWhiteAPI("edges"));
        Assert.assertFalse(PathFilter.isWhiteAPI("traversers"));
        Assert.assertFalse(PathFilter.isWhiteAPI("tasks"));
        Assert.assertFalse(PathFilter.isWhiteAPI("unknown"));
    }
}

