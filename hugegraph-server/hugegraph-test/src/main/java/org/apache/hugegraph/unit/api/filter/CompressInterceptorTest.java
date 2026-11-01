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
import java.io.OutputStream;
import java.lang.annotation.Annotation;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPInputStream;

import org.apache.hugegraph.api.filter.CompressInterceptor;
import org.apache.hugegraph.api.filter.CompressInterceptor.Compress;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.testutil.Whitebox;
import org.glassfish.jersey.internal.MapPropertiesDelegate;
import org.glassfish.jersey.server.ContainerRequest;
import org.junit.Test;
import org.mockito.Mockito;

import jakarta.inject.Provider;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MultivaluedHashMap;
import jakarta.ws.rs.core.MultivaluedMap;
import jakarta.ws.rs.ext.WriterInterceptorContext;

public class CompressInterceptorTest {

    private static final byte[] ENTITY = "{\"result\":\"hello HugeGraph\"}".getBytes(StandardCharsets.UTF_8);

    @Test
    public void testPlainResponseWhenGzipNotAccepted() throws Exception {
        for (String accept : new String[]{null, "", "identity", "br", "gzip;q=0", "*;q=0",
                                          "identity;q=0", "gzip;q=0, identity;q=0",
                                          "*;q=1, gzip;q=0", "gzip;q=0.5, identity;q=1",
                                          "gzip;q=invalid"}) {
            this.assertResponse(accept, false);
        }
    }

    @Test
    public void testGzipResponseWhenAccepted() throws Exception {
        for (String accept : new String[]{"gzip", "GZIP", "br, gzip", "gzip;q=0.5", "*",
                                          "gzip;q=1, identity;q=0.5", "*;q=0, gzip;q=1"}) {
            this.assertResponse(accept, true);
        }
    }

    @Test
    public void testTrailingDotQualityFactors() throws Exception {
        for (String accept : new String[]{"gzip;q=0.", "*;q=1, gzip;q=0.",
                                          "gzip;q=0.5, identity;q=1.", "*;q=0.",
                                          "*;q=0.5, identity;q=1.",
                                          "*;q=1., GZIP; Q=0. ",
                                          "gzip;q=0.0", "gzip;q=1.001", "gzip;q=1..",
                                          "gzip;q=0.foo", "gzip;q=1. 0", "gzip;q=1.0000",
                                          "gzip;Q =1.", "gzip;Q= 1."}) {
            this.assertResponse(accept, false);
        }
        for (String accept : new String[]{"gzip;q=1.", "*;q=1.",
                                          "gzip;q=1., identity;q=0.5", "*;q=0., gzip;q=1.",
                                          "gzip;q=0.5, identity;q=0.",
                                          "GZIP; Q=1. ", "GZIP;\tQ=1.\t",
                                          "gzip;q=1.000", "gzip;q=0.001"}) {
            this.assertResponse(accept, true);
        }
    }

    @Test
    public void testPreserveVaryFields() throws Exception {
        MultivaluedMap<String, Object> headers = new MultivaluedHashMap<>();
        headers.add(HttpHeaders.VARY, "Origin");
        this.writeResponse("gzip", headers, true);
        Assert.assertEquals(2, headers.get(HttpHeaders.VARY).size());
        Assert.assertEquals("Origin", headers.get(HttpHeaders.VARY).get(0));
        Assert.assertEquals(HttpHeaders.ACCEPT_ENCODING, headers.get(HttpHeaders.VARY).get(1));
    }

    @Test
    public void testDoNotDuplicateVary() throws Exception {
        for (String vary : new String[]{"Origin, accept-encoding", "*"}) {
            MultivaluedMap<String, Object> headers = new MultivaluedHashMap<>();
            headers.add(HttpHeaders.VARY, vary);
            this.writeResponse("identity", headers, true);
            Assert.assertEquals(1, headers.get(HttpHeaders.VARY).size());
            Assert.assertEquals(vary, headers.getFirst(HttpHeaders.VARY));
        }
    }

    @Test
    public void testPreserveExistingEncoding() throws Exception {
        MultivaluedMap<String, Object> headers = new MultivaluedHashMap<>();
        headers.add(HttpHeaders.CONTENT_ENCODING, "br");
        headers.add(HttpHeaders.CONTENT_LENGTH, ENTITY.length);
        Assert.assertArrayEquals(ENTITY, this.writeResponse("gzip", headers, true));
        Assert.assertEquals("br", headers.getFirst(HttpHeaders.CONTENT_ENCODING));
        Assert.assertEquals(ENTITY.length, headers.getFirst(HttpHeaders.CONTENT_LENGTH));
    }

    @Test
    public void testUnannotatedResponseRemainsPlain() throws Exception {
        MultivaluedMap<String, Object> headers = new MultivaluedHashMap<>();
        Assert.assertArrayEquals(ENTITY, this.writeResponse("gzip", headers, false));
        Assert.assertFalse(headers.containsKey(HttpHeaders.CONTENT_ENCODING));
        Assert.assertFalse(headers.containsKey(HttpHeaders.VARY));
    }

    @Compress
    private static void compressedResource() {
    }

    private void assertResponse(String accept, boolean compressed) throws Exception {
        MultivaluedMap<String, Object> headers = new MultivaluedHashMap<>();
        headers.add(HttpHeaders.CONTENT_LENGTH, ENTITY.length);
        byte[] response = this.writeResponse(accept, headers, true);
        Assert.assertEquals(HttpHeaders.ACCEPT_ENCODING, headers.getFirst(HttpHeaders.VARY));
        if (compressed) {
            Assert.assertEquals("gzip", headers.getFirst(HttpHeaders.CONTENT_ENCODING));
            Assert.assertFalse(headers.containsKey(HttpHeaders.CONTENT_LENGTH));
            try (GZIPInputStream input = new GZIPInputStream(new ByteArrayInputStream(response))) {
                Assert.assertArrayEquals(ENTITY, input.readAllBytes());
            }
        } else {
            Assert.assertArrayEquals(ENTITY, response);
            Assert.assertFalse(headers.containsKey(HttpHeaders.CONTENT_ENCODING));
            Assert.assertEquals(ENTITY.length, headers.getFirst(HttpHeaders.CONTENT_LENGTH));
        }
    }

    private byte[] writeResponse(String accept, MultivaluedMap<String, Object> headers,
                                 boolean annotated) throws Exception {
        // Real Jersey headers and parser, without creating an HTTP server or socket.
        URI uri = URI.create("http://localhost/gremlin");
        ContainerRequest request = new ContainerRequest(uri, uri, "POST", null,
                                                        new MapPropertiesDelegate());
        if (accept != null) {
            request.getHeaders().add(HttpHeaders.ACCEPT_ENCODING, accept);
        }
        CompressInterceptor interceptor = new CompressInterceptor();
        Whitebox.setInternalState(interceptor, "requestHeaders", (Provider<HttpHeaders>) () -> request);
        WriterInterceptorContext context = Mockito.mock(WriterInterceptorContext.class);
        Annotation[] annotations = annotated ?
                                   CompressInterceptorTest.class.getDeclaredMethod("compressedResource")
                                                                .getAnnotations() : new Annotation[0];
        Mockito.when(context.getAnnotations()).thenReturn(annotations);
        Mockito.when(context.getHeaders()).thenReturn(headers);
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        OutputStream[] output = new OutputStream[]{bytes};
        Mockito.when(context.getOutputStream()).thenAnswer(invocation -> output[0]);
        Mockito.doAnswer(invocation -> {
            output[0] = invocation.getArgument(0);
            return null;
        }).when(context).setOutputStream(Mockito.any(OutputStream.class));
        Mockito.doAnswer(invocation -> {
            output[0].write(ENTITY);
            output[0].close();
            return null;
        }).when(context).proceed();
        interceptor.aroundWriteTo(context);
        Mockito.verify(context, Mockito.times(1)).proceed();
        return bytes.toByteArray();
    }
}
