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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.URI;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.hugegraph.api.filter.AccessLogFilter;
import org.apache.hugegraph.api.filter.DecompressInterceptor;
import org.apache.hugegraph.api.filter.DecompressInterceptor.Decompress;
import org.apache.hugegraph.api.filter.PathFilter;
import org.apache.hugegraph.api.filter.RedirectFilter;
import org.apache.hugegraph.api.filter.RedirectFilter.RedirectMasterRole;
import org.apache.hugegraph.api.filter.RedirectFilterDynamicFeature;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.config.ServerOptions;
import org.apache.hugegraph.core.GraphManager;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.testutil.Whitebox;
import org.apache.hugegraph.unit.BaseUnitTest;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.config.Property;
import org.glassfish.grizzly.http.server.Request;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import jakarta.inject.Provider;
import jakarta.ws.rs.Priorities;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerResponseContext;
import jakarta.ws.rs.container.ResourceInfo;
import jakarta.ws.rs.core.FeatureContext;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.MultivaluedHashMap;
import jakarta.ws.rs.core.UriInfo;
import jakarta.ws.rs.ext.ReaderInterceptorContext;
import sun.misc.Unsafe;

/**
 * Unit tests for AccessLogFilter
 * Test scenarios:
 * 1. Which requests qualify for the slow query log
 * 2. Bounded request body capture and full replay to the resource method
 * 3. Body capture is skipped when it can not or should not be recorded
 * 4. The slow query log line carries the client IP and the body preview,
 *    and stays on a single line whatever the client sends
 */
public class AccessLogFilterTest extends BaseUnitTest {

    private static final String TEST_LOGGER_NAME = AccessLogFilter.class.getName();
    private static final String SLOW_QUERY_PREFIX = "[Slow Query]";
    private static final String CLIENT_IP = "10.0.0.9";

    private AccessLogFilter filter;
    private ContainerRequestContext requestContext;
    private ContainerResponseContext responseContext;
    private UriInfo uriInfo;
    private Request request;
    private ResourceInfo resourceInfo;
    private TestAppender testAppender;
    private LoggerContext loggerContext;
    private org.apache.logging.log4j.core.config.Configuration loggerConfiguration;
    private LoggerConfig originalLoggerConfig;

    @Before
    public void setup() {
        this.filter = new AccessLogFilter();
        this.requestContext = Mockito.mock(ContainerRequestContext.class);
        this.responseContext = Mockito.mock(ContainerResponseContext.class);
        this.uriInfo = Mockito.mock(UriInfo.class);
        this.request = Mockito.mock(Request.class);
        this.resourceInfo = Mockito.mock(ResourceInfo.class);

        Mockito.when(this.requestContext.getUriInfo()).thenReturn(this.uriInfo);
        this.mockResourceMethod("plainResource");
        Whitebox.setInternalState(this.filter, "resourceInfo", this.resourceInfo);
        Mockito.when(this.uriInfo.getPathParameters()).thenReturn(new MultivaluedHashMap<>());
        Mockito.when(this.responseContext.getStatus()).thenReturn(200);
        Mockito.when(this.request.getRemoteAddr()).thenReturn(CLIENT_IP);

        this.setConfig(1000L, 512);
        this.setRemoteRequest(this.request);
        Whitebox.setInternalState(this.filter, "managerProvider",
                                  (Provider<GraphManager>) AccessLogFilterTest::managerWithoutAuth);

        this.testAppender = new TestAppender();
        this.testAppender.start();
        this.loggerContext = (LoggerContext) LogManager.getContext(false);
        this.loggerConfiguration = this.loggerContext.getConfiguration();
        /*
         * log4j2.xml of this module already declares an (async) logger with this
         * name and addLogger() only adds absent names, so swap it for a
         * synchronous one during the test and put it back afterwards
         */
        LoggerConfig existing = this.loggerConfiguration.getLoggerConfig(TEST_LOGGER_NAME);
        this.originalLoggerConfig = TEST_LOGGER_NAME.equals(existing.getName()) ? existing : null;
        if (this.originalLoggerConfig != null) {
            this.loggerConfiguration.removeLogger(TEST_LOGGER_NAME);
        }
        LoggerConfig loggerConfig = new LoggerConfig(TEST_LOGGER_NAME, Level.INFO, false);
        loggerConfig.addAppender(this.testAppender, Level.INFO, null);
        this.loggerConfiguration.addLogger(TEST_LOGGER_NAME, loggerConfig);
        this.loggerContext.updateLoggers();
    }

    @After
    public void teardown() {
        this.loggerConfiguration.removeLogger(TEST_LOGGER_NAME);
        if (this.originalLoggerConfig != null) {
            this.loggerConfiguration.addLogger(TEST_LOGGER_NAME, this.originalLoggerConfig);
        }
        this.loggerContext.updateLoggers();
        this.testAppender.stop();
    }

    /**
     * Test which requests are candidates for the slow query log
     */
    @Test
    public void testNeedRecordLog() {
        Assert.assertTrue(this.needRecordLog("POST", "gremlin"));
        Assert.assertTrue(this.needRecordLog("GET", "gremlin"));
        Assert.assertTrue(this.needRecordLog("POST", "graphs/hugegraph/jobs/gremlin"));
        Assert.assertTrue(this.needRecordLog("POST", "graphs/hugegraph/cypher"));
        Assert.assertTrue(this.needRecordLog("GET", "graphs/hugegraph/graph/vertices"));
        // PathFilter redirects requests under graphspaces/, they must stay loggable
        Assert.assertTrue(this.needRecordLog("POST", "graphspaces/DEFAULT/graphs/hugegraph/cypher"));
        Assert.assertTrue(this.needRecordLog("GET", "graphspaces/DEFAULT/graphs/hugegraph/graph/vertices"));

        Assert.assertFalse(this.needRecordLog("POST", "graphs/hugegraph/graph/vertices/batch"));
        Assert.assertFalse(this.needRecordLog("PUT", "graphs/hugegraph/graph/edges/batch"));
        Assert.assertFalse(this.needRecordLog("POST", "graphs/hugegraph/schema/vertexlabels"));
        Assert.assertFalse(this.needRecordLog("POST", "auth/login"));
        Assert.assertFalse(this.needRecordLog("GET", "metrics"));
    }

    /**
     * Test a short body is recorded as is and still readable by the resource
     */
    @Test
    public void testCaptureBody_ShortBody() throws IOException {
        String body = "{\"gremlin\":\"g.V().limit(1)\"}";
        this.mockRequest("POST", "gremlin", body);

        this.filter.filter(this.requestContext);

        Assert.assertEquals(body, this.capturedBody());
        Assert.assertEquals(body, this.replayedEntity());
    }

    /**
     * Test only the configured prefix is recorded while the full body is replayed
     */
    @Test
    public void testCaptureBody_LongBodyIsTruncatedButReplayedInFull() throws IOException {
        this.setConfig(1000L, 16);
        String body = "{\"gremlin\":\"g.V().hasLabel('person').limit(1)\"}";
        this.mockRequest("POST", "gremlin", body);

        this.filter.filter(this.requestContext);

        Assert.assertEquals(body.substring(0, 16) + "...", this.capturedBody());
        Assert.assertEquals(body, this.replayedEntity());
    }

    /**
     * Test a body whose size equals the limit is not marked as truncated
     */
    @Test
    public void testCaptureBody_ExactLimitIsNotMarkedTruncated() throws IOException {
        this.setConfig(1000L, 8);
        String body = "12345678";
        this.mockRequest("PUT", "graphs/hugegraph/cypher", body);

        this.filter.filter(this.requestContext);

        Assert.assertEquals(body, this.capturedBody());
        Assert.assertEquals(body, this.replayedEntity());
    }

    /**
     * Test a multi-byte character cut by the limit is dropped, not replaced
     */
    @Test
    public void testCaptureBody_MultiByteCharacterAtLimitIsDropped() throws IOException {
        // "é" is 2 bytes and "€" is 3 bytes in UTF-8, limit 4 cuts "€"
        this.setConfig(1000L, 4);
        String body = "é€ab";
        this.mockRequest("POST", "gremlin", body);

        this.filter.filter(this.requestContext);

        Assert.assertEquals("é...", this.capturedBody());
        Assert.assertEquals(body, this.replayedEntity());
    }

    @Test
    public void testCaptureBody_UsesRequestCharset() throws IOException {
        String body = "MATCH (张三) RETURN 张三";
        InputStream entity = new ByteArrayInputStream(body.getBytes(StandardCharsets.UTF_16LE));
        this.mockStreamRequest("POST", "graphs/hugegraph/cypher", entity);
        Mockito.when(this.requestContext.getMediaType())
               .thenReturn(MediaType.valueOf("application/json; charset=UTF-16LE"));

        this.filter.filter(this.requestContext);

        Assert.assertEquals(body, this.capturedBody());
        Assert.assertEquals(body, this.replayedEntity(StandardCharsets.UTF_16LE));
    }

    /**
     * Test line breaks in the body are kept in the preview and in the replay
     */
    @Test
    public void testCaptureBody_KeepsLineBreaks() throws IOException {
        String body = "g.V()\n .limit(1)\r\n";
        this.mockRequest("POST", "graphs/hugegraph/cypher", body);

        this.filter.filter(this.requestContext);

        Assert.assertEquals(body, this.capturedBody());
        Assert.assertEquals(body, this.replayedEntity());
    }

    /**
     * Test a Content-Encoding header alone does not stop the capture, since the
     * endpoints that qualify for the slow query log read their entity as is
     */
    @Test
    public void testCaptureBody_ContentEncodingHeaderIsIgnored() throws IOException {
        String body = "{\"gremlin\":\"g.V()\"}";
        this.mockRequest("POST", "gremlin", body);
        Mockito.when(this.requestContext.getHeaderString("Content-Encoding")).thenReturn("gzip");

        this.filter.filter(this.requestContext);

        Assert.assertEquals(body, this.capturedBody());
        Assert.assertEquals(body, this.replayedEntity());
    }

    /**
     * Test an empty body is handled
     */
    @Test
    public void testCaptureBody_EmptyBody() throws IOException {
        this.mockRequest("POST", "gremlin", "");

        this.filter.filter(this.requestContext);

        Assert.assertEquals("", this.capturedBody());
        Assert.assertEquals("", this.replayedEntity());
    }

    /**
     * Test nothing is read when the slow query log is disabled
     */
    @Test
    public void testSkipBody_WhenSlowQueryLogDisabled() throws IOException {
        this.setConfig(0L, 512);
        this.mockRequest("POST", "gremlin", "{\"gremlin\":\"g.V()\"}");

        this.filter.filter(this.requestContext);

        this.verifyNoCapture();
    }

    /**
     * Test nothing is read when the body limit is zero
     */
    @Test
    public void testSkipBody_WhenBodyLimitIsZero() throws IOException {
        this.setConfig(1000L, 0);
        this.mockRequest("POST", "gremlin", "{\"gremlin\":\"g.V()\"}");

        this.filter.filter(this.requestContext);

        this.verifyNoCapture();
    }

    /**
     * Test GET and DELETE requests are never read
     */
    @Test
    public void testSkipBody_ForMethodsWithoutBody() throws IOException {
        this.mockRequest("GET", "gremlin", "ignored");
        this.filter.filter(this.requestContext);
        this.verifyNoCapture();

        this.mockRequest("DELETE", "graphs/hugegraph/cypher", "ignored");
        this.filter.filter(this.requestContext);
        this.verifyNoCapture();
    }

    /**
     * Test loader batch imports are left untouched, even when gzip encoded
     */
    @Test
    public void testSkipBody_ForBatchImport() throws IOException {
        String body = "[{\"id\":1}]";
        InputStream entity = new ByteArrayInputStream(gzip(body));
        this.mockResourceMethod("decompressResource");
        this.mockStreamRequest("POST", "graphs/hugegraph/graph/vertices/batch", entity);
        Mockito.when(this.requestContext.getHeaderString("Content-Encoding")).thenReturn("gzip");
        this.filter.filter(this.requestContext);
        this.verifyNoCapture();
        Assert.assertEquals(body, decompress(entity));

        this.mockRequest("PUT", "graphs/hugegraph/graph/edges/batch", "[{}]");
        this.filter.filter(this.requestContext);
        this.verifyNoCapture();
    }

    /**
     * Test the entity of a resource that decodes it later is not read
     */
    @Test
    public void testSkipBody_ForResourceThatDecodesEntity() throws IOException {
        this.mockResourceMethod("decompressResource");
        this.mockRequest("POST", "gremlin", "compressed bytes");

        this.filter.filter(this.requestContext);

        Assert.assertEquals("<encoded>", this.capturedBody());
        Mockito.verify(this.requestContext, Mockito.never()).getEntityStream();
        Mockito.verify(this.requestContext, Mockito.never()).setEntityStream(Mockito.any(InputStream.class));
    }

    @Test
    public void testRedirectRunsAfterBodyCapture() {
        this.mockResourceMethod("redirectResource");
        FeatureContext context = Mockito.mock(FeatureContext.class);

        new RedirectFilterDynamicFeature().configure(this.resourceInfo, context);

        Mockito.verify(context).register(RedirectFilter.class, Priorities.USER + 1);
    }

    /**
     * Test the slow query log line contains the client IP and the body preview
     */
    @Test
    public void testSlowQueryLog_ContainsClientIpAndBody() throws IOException {
        this.mockRequest("POST", "gremlin", null);
        this.mockElapsed(5000L);
        Mockito.when(this.requestContext.getProperty(AccessLogFilter.REQUEST_BODY))
               .thenReturn("{\"gremlin\":\"g.V()\"}");

        this.filter.filter(this.requestContext, this.responseContext);

        List<String> messages = this.slowQueryMessages();
        Assert.assertEquals(1, messages.size());
        String message = messages.get(0);
        Assert.assertTrue(message, message.contains("ip=" + CLIENT_IP + ","));
        Assert.assertTrue(message, message.contains("method=POST,"));
        Assert.assertTrue(message, message.contains("path=gremlin,"));
        Assert.assertTrue(message, message.endsWith("body={\"gremlin\":\"g.V()\"}"));
    }

    /**
     * Test the query string and a missing body are logged for GET
     */
    @Test
    public void testSlowQueryLog_GetWithQuery() throws IOException {
        this.mockRequest("GET", "graphs/hugegraph/graph/vertices", null);
        Mockito.when(this.uriInfo.getRequestUri()).thenReturn(URI.create(
                "http://localhost:8080/graphs/hugegraph/graph/vertices?label=person&limit=1"));
        this.mockElapsed(5000L);

        this.filter.filter(this.requestContext, this.responseContext);

        List<String> messages = this.slowQueryMessages();
        Assert.assertEquals(1, messages.size());
        String message = messages.get(0);
        Assert.assertTrue(message, message.contains("method=GET,"));
        Assert.assertTrue(message, message.contains("query=label=person&limit=1,"));
        Assert.assertTrue(message, message.endsWith("body=null"));
    }

    /**
     * Test line breaks in path, query and body can not forge a second log entry
     */
    @Test
    public void testSlowQueryLog_StaysOnOneLine() throws IOException {
        this.mockRequest("GET", "graphs/hugegraph/graph/vertices", null);
        Mockito.when(this.uriInfo.getPath()).thenReturn("graphs/hugegraph/graph/vertices\nforged path");
        // URI.getQuery() decodes %0A into a line feed
        Mockito.when(this.uriInfo.getRequestUri()).thenReturn(URI.create(
                "http://localhost:8080/graphs/hugegraph/graph/vertices?x=%0Aforged%20query"));
        Mockito.when(this.requestContext.getProperty(AccessLogFilter.REQUEST_BODY)).thenReturn("a\r\nforged body");
        this.mockElapsed(5000L);

        this.filter.filter(this.requestContext, this.responseContext);

        List<String> messages = this.slowQueryMessages();
        Assert.assertEquals(1, messages.size());
        String message = messages.get(0);
        Assert.assertFalse(message, message.contains("\n") || message.contains("\r"));
        Assert.assertTrue(message, message.contains("path=graphs/hugegraph/graph/vertices" +
                                                    "\\nforged path,"));
        Assert.assertTrue(message, message.contains("query=x=\\nforged query,"));
        Assert.assertTrue(message, message.endsWith("body=a\\r\\nforged body"));
    }

    /**
     * Test the client IP falls back to a placeholder without a peer request
     */
    @Test
    public void testSlowQueryLog_UnknownIpWithoutRequest() throws IOException {
        this.setRemoteRequest(null);
        this.mockRequest("POST", "gremlin", null);
        this.mockElapsed(5000L);

        this.filter.filter(this.requestContext, this.responseContext);

        List<String> messages = this.slowQueryMessages();
        Assert.assertEquals(1, messages.size());
        Assert.assertTrue(messages.get(0), messages.get(0).contains("ip=" + AccessLogFilter.UNKNOWN_IP + ","));
    }

    /**
     * Test fast requests and disabled slow query log produce no log line
     */
    @Test
    public void testSlowQueryLog_SkipsFastRequestAndDisabledLog() throws IOException {
        this.mockRequest("POST", "gremlin", null);
        this.mockElapsed(0L);
        this.filter.filter(this.requestContext, this.responseContext);
        Assert.assertTrue(this.slowQueryMessages().isEmpty());

        this.setConfig(0L, 512);
        this.mockElapsed(5000L);
        this.filter.filter(this.requestContext, this.responseContext);
        Assert.assertTrue(this.slowQueryMessages().isEmpty());
    }

    private boolean needRecordLog(String method, String path) {
        this.mockRequest(method, path, null);
        return AccessLogFilter.needRecordLog(this.requestContext);
    }

    private void mockRequest(String method, String path, String body) {
        Mockito.when(this.requestContext.getMethod()).thenReturn(method);
        Mockito.when(this.uriInfo.getPath()).thenReturn(path);
        Mockito.when(this.uriInfo.getRequestUri()).thenReturn(URI.create("http://localhost:8080/" + path));
        if (body != null) {
            InputStream entity = new ByteArrayInputStream(body.getBytes(StandardCharsets.UTF_8));
            Mockito.when(this.requestContext.getEntityStream()).thenReturn(entity);
        }
    }

    private void mockStreamRequest(String method, String path, InputStream entity) {
        Mockito.when(this.requestContext.getMethod()).thenReturn(method);
        Mockito.when(this.uriInfo.getPath()).thenReturn(path);
        Mockito.when(this.uriInfo.getRequestUri()).thenReturn(URI.create("http://localhost:8080/" + path));
        Mockito.when(this.requestContext.getEntityStream()).thenReturn(entity);
    }

    private static byte[] gzip(String body) throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        try (GZIPOutputStream gzip = new GZIPOutputStream(output)) {
            gzip.write(body.getBytes(StandardCharsets.UTF_8));
        }
        return output.toByteArray();
    }

    private static String decompress(InputStream entity) throws IOException {
        ReaderInterceptorContext context = Mockito.mock(ReaderInterceptorContext.class);
        MultivaluedHashMap<String, String> headers = new MultivaluedHashMap<>();
        headers.putSingle("Content-Encoding", "gzip");
        AtomicReference<InputStream> input = new AtomicReference<>(entity);
        Mockito.when(context.getHeaders()).thenReturn(headers);
        Mockito.when(context.getInputStream()).thenAnswer(invocation -> input.get());
        Mockito.doAnswer(invocation -> {
            input.set(invocation.getArgument(0));
            return null;
        }).when(context).setInputStream(Mockito.any(InputStream.class));
        Mockito.when(context.proceed()).thenAnswer(invocation ->
                new String(input.get().readAllBytes(), StandardCharsets.UTF_8));
        return (String) new DecompressInterceptor().aroundReadFrom(context);
    }

    private void mockResourceMethod(String name) {
        try {
            Method method = AccessLogFilterTest.class.getMethod(name);
            Mockito.when(this.resourceInfo.getResourceMethod()).thenReturn(method);
        } catch (NoSuchMethodException e) {
            throw new AssertionError(e);
        }
    }

    /**
     * Stand-in for a resource method that reads its entity as is
     */
    public void plainResource() {
        // pass
    }

    /**
     * Stand-in for a resource method whose entity is decoded by DecompressInterceptor
     */
    @Decompress
    public void decompressResource() {
        // pass
    }

    @RedirectMasterRole
    public void redirectResource() {
        // pass
    }

    private void mockElapsed(long elapsed) {
        Mockito.when(this.requestContext.getProperty(PathFilter.REQUEST_TIME))
               .thenReturn(System.currentTimeMillis() - elapsed);
    }

    private void setConfig(long threshold, int bodyLimit) {
        Configuration conf = new PropertiesConfiguration();
        conf.setProperty(ServerOptions.SLOW_QUERY_LOG_TIME_THRESHOLD.name(), threshold);
        conf.setProperty(ServerOptions.SLOW_QUERY_LOG_BODY_LIMIT.name(), bodyLimit);
        HugeConfig config = new HugeConfig(conf);
        Whitebox.setInternalState(this.filter, "configProvider", (Provider<HugeConfig>) () -> config);
    }

    private void setRemoteRequest(Request request) {
        Whitebox.setInternalState(this.filter, "requestProvider", (Provider<Request>) () -> request);
    }

    private String capturedBody() {
        ArgumentCaptor<Object> captor = ArgumentCaptor.forClass(Object.class);
        Mockito.verify(this.requestContext).setProperty(Mockito.eq(AccessLogFilter.REQUEST_BODY), captor.capture());
        return (String) captor.getValue();
    }

    private String replayedEntity() throws IOException {
        return this.replayedEntity(StandardCharsets.UTF_8);
    }

    private String replayedEntity(Charset charset) throws IOException {
        ArgumentCaptor<InputStream> captor = ArgumentCaptor.forClass(InputStream.class);
        Mockito.verify(this.requestContext).setEntityStream(captor.capture());
        return new String(captor.getValue().readAllBytes(), charset);
    }

    private void verifyNoCapture() {
        Mockito.verify(this.requestContext, Mockito.never()).getEntityStream();
        Mockito.verify(this.requestContext, Mockito.never()).setEntityStream(Mockito.any(InputStream.class));
        Mockito.verify(this.requestContext, Mockito.never())
               .setProperty(Mockito.eq(AccessLogFilter.REQUEST_BODY), Mockito.any());
    }

    private List<String> slowQueryMessages() {
        return this.testAppender.events().stream()
                   .map(event -> event.getMessage().getFormattedMessage())
                   .filter(message -> message.startsWith(SLOW_QUERY_PREFIX))
                   .collect(Collectors.toList());
    }

    /**
     * GraphManager has no test friendly constructor, allocate one without an
     * authenticator so that requireAuthentication() is false
     */
    private static GraphManager managerWithoutAuth() {
        try {
            Field field = Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            Unsafe unsafe = (Unsafe) field.get(null);
            return (GraphManager) unsafe.allocateInstance(GraphManager.class);
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    private static class TestAppender extends AbstractAppender {

        private final List<LogEvent> events;

        protected TestAppender() {
            super("AccessLogFilterTestAppender", (Filter) null,
                  (Layout<? extends Serializable>) null, false,
                  Property.EMPTY_ARRAY);
            this.events = new ArrayList<>();
        }

        @Override
        public void append(LogEvent event) {
            this.events.add(event.toImmutable());
        }

        public List<LogEvent> events() {
            return this.events;
        }
    }
}
