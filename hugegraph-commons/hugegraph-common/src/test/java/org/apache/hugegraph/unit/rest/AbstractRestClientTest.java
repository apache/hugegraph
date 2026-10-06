/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hugegraph.unit.rest;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Collections;

import org.apache.hugegraph.rest.AbstractRestClient;
import org.apache.hugegraph.rest.RestHeaders;
import org.apache.hugegraph.testutil.Whitebox;
import org.junit.Assert;
import org.junit.Test;

import okhttp3.RequestBody;
import okio.Buffer;
import okio.GzipSource;

public class AbstractRestClientTest {

    @Test
    public void testExpectedDefaultCharset() {
        // CI repeats this class in a JVM started with US-ASCII as its default.
        String expected = System.getProperty("hugegraph.test.expectedCharset");
        if (expected != null) {
            Assert.assertEquals(Charset.forName(expected), Charset.defaultCharset());
        }
    }

    @Test
    public void testJsonRequestBodyUsesUtf8() throws IOException {
        String json = "{\"id\":\"\u4f60\u597d\ud83d\ude80\"}";
        assertUtf8Body(json, null, json, false);
        assertUtf8Body(Collections.singletonMap("id", "\u4f60\u597d\ud83d\ude80"),
                       new RestHeaders(), json, false);
    }

    @Test
    public void testGzipRequestBodyUsesUtf8() throws IOException {
        String json = "{\"id\":\"\u4f60\u597d\ud83d\ude80\"}";
        RestHeaders headers = new RestHeaders().add(RestHeaders.CONTENT_ENCODING, "gzip");
        assertUtf8Body(json, headers, json, true);
    }

    @Test
    public void testRequestBodyRespectsExplicitCharset() throws IOException {
        RestHeaders headers = new RestHeaders().add(RestHeaders.CONTENT_TYPE,
                                                    "text/plain; charset=iso-8859-1");
        RequestBody body = requestBody("caf\u00e9", headers);
        Buffer buffer = new Buffer();
        body.writeTo(buffer);
        Assert.assertEquals(StandardCharsets.ISO_8859_1, body.contentType().charset());
        Assert.assertArrayEquals("caf\u00e9".getBytes(StandardCharsets.ISO_8859_1),
                                 buffer.readByteArray());
    }

    private static void assertUtf8Body(Object value, RestHeaders headers,
                                      String expected, boolean gzip) throws IOException {
        RequestBody body = requestBody(value, headers);
        Assert.assertEquals(StandardCharsets.UTF_8, body.contentType().charset());
        Buffer buffer = new Buffer();
        body.writeTo(buffer);
        if (gzip) {
            try (GzipSource source = new GzipSource(buffer)) {
                Buffer decoded = new Buffer();
                decoded.writeAll(source);
                Assert.assertArrayEquals(expected.getBytes(StandardCharsets.UTF_8),
                                         decoded.readByteArray());
            }
        } else {
            Assert.assertArrayEquals(expected.getBytes(StandardCharsets.UTF_8),
                                     buffer.readByteArray());
        }
    }

    private static RequestBody requestBody(Object value, RestHeaders headers) {
        return Whitebox.invokeStatic(AbstractRestClient.class,
                                     new Class<?>[]{Object.class, RestHeaders.class},
                                     "buildRequestBody", value, headers);
    }

    @Test
    public void testEncodeWithSpaces() {
        String raw = "hello world";
        String expected = "hello%2Bworld";
        String encoded = AbstractRestClient.encode(raw);
        Assert.assertEquals(expected, encoded);
    }

    @Test
    public void testEncodeWithSpecialCharacters() {
        String raw = "hello@world!";
        String expected = "hello%40world%21";
        String encoded = AbstractRestClient.encode(raw);
        Assert.assertEquals(expected, encoded);
    }

    @Test
    public void testEncodeWithChineseCharacters() {
        String raw = "你好";
        String expected = "%E4%BD%A0%E5%A5%BD";
        String encoded = AbstractRestClient.encode(raw);
        Assert.assertEquals(expected, encoded);
    }

    @Test
    public void testEncodeWithNullInput() {
        String raw = null;
        Assert.assertThrows(NullPointerException.class, () -> {
            AbstractRestClient.encode(raw);
        });
    }
}
