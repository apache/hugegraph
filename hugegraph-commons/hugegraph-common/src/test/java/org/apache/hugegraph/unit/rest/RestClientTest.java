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

import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSessionContext;

import org.apache.hugegraph.rest.AbstractRestClient;
import org.apache.hugegraph.rest.ClientException;
import org.apache.hugegraph.rest.RestClient;
import org.apache.hugegraph.rest.RestClientConfig;
import org.apache.hugegraph.rest.RestHeaders;
import org.apache.hugegraph.rest.RestResult;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.testutil.Whitebox;
import org.apache.hugegraph.unit.BaseUnitTest;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.net.HttpHeaders;

import lombok.SneakyThrows;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

public class RestClientTest {

    private static final String TEST_URL = "http://localhost:8080";

    @Test
    public void testPost() {
        RestClientConfig restClientConfig = RestClientConfig.builder().timeout(1000).build();
        RestClient client = new RestClientImpl(TEST_URL, restClientConfig, 200);
        RestResult restResult = client.post("path", "body");
        Assert.assertEquals(200, restResult.status());
    }

    @Test
    // TODO: How to verify it?
    public void testPostWithMaxConnsAndPerRoute() {
        RestClientConfig restClientConfig =
                RestClientConfig.builder().timeout(1000).maxConns(10).maxConnsPerRoute(5).build();
        RestClient client = new RestClientImpl(TEST_URL, restClientConfig, 200);
        RestResult restResult = client.post("path", "body");
        Assert.assertEquals(200, restResult.status());
    }

    @Test
    public void testPostWithUserAndPassword() {
        RestClientConfig restClientConfig =
                RestClientConfig.builder().user("user").password("").timeout(1000).build();
        RestClient client = new RestClientImpl(TEST_URL, restClientConfig, 200);
        RestResult restResult = client.post("path", "body");
        Assert.assertEquals(200, restResult.status());
    }

    @Test
    public void testPostWithToken() {
        RestClientConfig restClientConfig =
                RestClientConfig.builder().token("token").timeout(1000).build();
        RestClient client = new RestClientImpl(TEST_URL, restClientConfig, 200);
        RestResult restResult = client.post("path", "body");
        Assert.assertEquals(200, restResult.status());
    }

    @Test
    public void testPostWithAllParams() {
        RestClientConfig restClientConfig =
                RestClientConfig.builder().user("user").password("").timeout(1000).maxConns(10)
                                .maxConnsPerRoute(5).build();
        RestClient client = new RestClientImpl(TEST_URL, restClientConfig, 200);
        RestResult restResult = client.post("path", "body");
        Assert.assertEquals(200, restResult.status());
    }

    @Test
    public void testPostWithTokenAndAllParams() {
        RestClientConfig restClientConfig =
                RestClientConfig.builder().token("token").timeout(1000).maxConns(10)
                                .maxConnsPerRoute(5).build();
        RestClient client = new RestClientImpl(TEST_URL, restClientConfig, 200);
        RestResult restResult = client.post("path", "body");
        Assert.assertEquals(200, restResult.status());
    }

    @Test
    public void testPostHttpsWithAllParams() {
        String url = "https://github.com/apache/hugegraph-doc/" +
                     "raw/master/dist/commons/cacerts.jks";
        String trustStoreFile = "src/test/resources/cacerts.jks";
        BaseUnitTest.downloadFileByUrl(url, trustStoreFile);

        String trustStorePassword = "changeit";
        RestClientConfig restClientConfig =
                RestClientConfig.builder().user("user").password("").timeout(1000).maxConns(10)
                                .maxConnsPerRoute(5).trustStoreFile(trustStoreFile)
                                .trustStorePassword(trustStorePassword).build();
        RestClient client = new RestClientImpl(TEST_URL, restClientConfig, 200);
        RestResult restResult = client.post("path", "body");
        Assert.assertEquals(200, restResult.status());
    }

    @Test
    public void testPostHttpsWithTokenAndAllParams() {
        String url = "https://github.com/apache/hugegraph-doc/" +
                     "raw/master/dist/commons/cacerts.jks";
        String trustStoreFile = "src/test/resources/cacerts.jks";
        BaseUnitTest.downloadFileByUrl(url, trustStoreFile);

        String trustStorePassword = "changeit";
        RestClientConfig restClientConfig =
                RestClientConfig.builder().token("token").timeout(1000).maxConns(10)
                                .maxConnsPerRoute(5).trustStoreFile(trustStoreFile)
                                .trustStorePassword(trustStorePassword).build();
        RestClient client = new RestClientImpl(TEST_URL, restClientConfig, 200);
        RestResult restResult = client.post("path", "body");
        Assert.assertEquals(200, restResult.status());
    }

    @Test
    public void testHostNameVerifier() {
        BiFunction<String, String, Boolean> verifer = (url, hostname) -> {
            AbstractRestClient.HostNameVerifier verifier;
            SSLSession session;
            try {
                SSLSessionContext sc = SSLContext.getDefault()
                                                 .getClientSessionContext();
                session = sc.getSession(new byte[]{11});
                verifier = new AbstractRestClient.HostNameVerifier(url);
            } catch (NoSuchAlgorithmException e) {
                throw new RuntimeException(e);
            }
            return verifier.verify(hostname, session);
        };

        Assert.assertTrue(verifer.apply("http://baidu.com", "baidu.com"));
        Assert.assertTrue(verifer.apply("http://test1.baidu.com", "baidu.com"));
        Assert.assertTrue(verifer.apply("http://test2.baidu.com", "baidu.com"));
        Assert.assertFalse(verifer.apply("http://baidu2.com", "baidu.com"));
        Assert.assertTrue(verifer.apply("http://baidu.com", ""));
        Assert.assertTrue(verifer.apply("baidu.com", "baidu.com"));
        Assert.assertTrue(verifer.apply("http://baidu.com/test", "baidu.com"));
        Assert.assertTrue(verifer.apply("baidu.com/test/abc", "baidu.com"));
        Assert.assertFalse(verifer.apply("baidu.com.sina.com", "baidu.com"));
    }

    @Test
    public void testPostWithHeaderAndContent() {
        RestHeaders headers = new RestHeaders().add("key1", "value1-1")
                                               .add("key1", "value1-2")
                                               .add("Content-Encoding", "gzip");
        String content = "{\"names\": [\"marko\", \"josh\", \"lop\"]}";
        RestClientConfig restClientConfig = RestClientConfig.builder().timeout(1000).build();
        RestClient client = new RestClientImpl(TEST_URL, restClientConfig, 200, headers, content);
        RestResult restResult = client.post("path", "body");
        Assert.assertEquals(200, restResult.status());
        Assert.assertEquals(headers, restResult.headers());
        Assert.assertEquals(content, restResult.content());
        Assert.assertEquals(ImmutableList.of("marko", "josh", "lop"),
                            restResult.readList("names", String.class));
    }

    @Test
    public void testPostWithException() {
        RestClientConfig restClientConfig = RestClientConfig.builder().timeout(1000).build();
        RestClient client = new RestClientImpl(TEST_URL, restClientConfig, 400);
        Assert.assertThrows(ClientException.class, () -> {
            client.post("path", "body");
        });
    }

    @Test
    public void testPostWithParams() {
        RestClientConfig restClientConfig = RestClientConfig.builder().timeout(1000).build();
        RestClient client = new RestClientImpl(TEST_URL, restClientConfig, 200);
        RestHeaders headers = new RestHeaders();

        Map<String, Object> params = ImmutableMap.of("param1", "value1");
        RestResult restResult = client.post("path", "body", headers,
                                            params);
        Assert.assertEquals(200, restResult.status());
    }

    @Test
    public void testPut() {
        RestClientConfig restClientConfig = RestClientConfig.builder().timeout(1000).build();
        RestClient client = new RestClientImpl(TEST_URL, restClientConfig, 200);
        RestResult restResult = client.put("path", "id1", "body");
        Assert.assertEquals(200, restResult.status());
    }

    @Test
    public void testPutWithHeaders() {
        RestClientConfig restClientConfig = RestClientConfig.builder().timeout(1000).build();
        RestClient client = new RestClientImpl(TEST_URL, restClientConfig, 200);
        RestHeaders headers = new RestHeaders().add("key1", "value1-1")
                                               .add("key2", "value1-2")
                                               .add("Content-Encoding", "gzip");
        RestResult restResult = client.put("path", "id1", "body", headers);
        Assert.assertEquals(200, restResult.status());
    }

    @Test
    public void testPutWithParams() {
        RestClientConfig restClientConfig = RestClientConfig.builder().timeout(1000).build();
        RestClient client = new RestClientImpl(TEST_URL, restClientConfig, 200);
        Map<String, Object> params = ImmutableMap.of("param1", "value1");
        RestResult restResult = client.put("path", "id1", "body", params);
        Assert.assertEquals(200, restResult.status());
    }

    @Test
    public void testPutWithException() {
        RestClientConfig restClientConfig = RestClientConfig.builder().timeout(1000).build();
        RestClient client = new RestClientImpl(TEST_URL, restClientConfig, 400);
        Assert.assertThrows(ClientException.class, () -> {
            client.put("path", "id1", "body");
        });
    }

    @Test
    public void testGet() {
        RestClientConfig restClientConfig = RestClientConfig.builder().timeout(1000).build();
        RestClient client = new RestClientImpl(TEST_URL, restClientConfig, 200);
        RestResult restResult = client.get("path");
        Assert.assertEquals(200, restResult.status());
    }

    @Test
    public void testGetWithId() {
        RestClientConfig restClientConfig = RestClientConfig.builder().timeout(1000).build();
        RestClient client = new RestClientImpl(TEST_URL, restClientConfig, 200);
        RestResult restResult = client.get("path", "id1");
        Assert.assertEquals(200, restResult.status());
    }

    @Test
    public void testGetWithParams() {
        RestClientConfig restClientConfig = RestClientConfig.builder().timeout(1000).build();
        RestClient client = new RestClientImpl(TEST_URL, restClientConfig, 200);
        Map<String, Object> params = new HashMap<>();
        params.put("key1", ImmutableList.of("value1-1", "value1-2"));
        params.put("key2", "value2");
        RestResult restResult = client.get("path", params);
        Assert.assertEquals(200, restResult.status());
    }

    @Test
    public void testGetWithException() {
        RestClientConfig restClientConfig = RestClientConfig.builder().timeout(1000).build();
        RestClient client = new RestClientImpl(TEST_URL, restClientConfig, 400);
        Assert.assertThrows(ClientException.class, () -> {
            client.get("path", "id1");
        });
    }

    @Test
    public void testDeleteWithId() {
        RestClientConfig restClientConfig = RestClientConfig.builder().timeout(1000).build();
        RestClient client = new RestClientImpl(TEST_URL, restClientConfig, 204);
        RestResult restResult = client.delete("path", "id1");
        Assert.assertEquals(204, restResult.status());
    }

    @Test
    public void testDeleteWithParams() {
        RestClientConfig restClientConfig = RestClientConfig.builder().timeout(1000).build();
        RestClient client = new RestClientImpl(TEST_URL, restClientConfig, 204);
        Map<String, Object> params = ImmutableMap.of("param1", "value1");
        RestResult restResult = client.delete("path", params);
        Assert.assertEquals(204, restResult.status());
    }

    @Test
    public void testDeleteWithException() {
        RestClientConfig restClientConfig = RestClientConfig.builder().timeout(1000).build();
        RestClient client = new RestClientImpl(TEST_URL, restClientConfig, 400);
        Assert.assertThrows(ClientException.class, () -> {
            client.delete("path", "id1");
        });
    }

    @Test
    public void testAuthContext() {
        RestClientConfig restClientConfig = RestClientConfig.builder().timeout(1000).build();
        RestClientImpl client = new RestClientImpl(TEST_URL, restClientConfig, 200);
        Assert.assertNull(client.getAuthContext());

        String token = UUID.randomUUID().toString();
        client.setAuthContext(token);
        Assert.assertEquals(token, client.getAuthContext());

        client.resetAuthContext();
        Assert.assertNull(client.getAuthContext());
    }

    @SneakyThrows
    @Test
    public void testBuilderCallback() {
        // default configs
        MockRestClientImpl restClient = new MockRestClientImpl(TEST_URL,
                                                               RestClientConfig.builder().build());
        OkHttpClient okHttpClient = Whitebox.getInternalState(restClient, "client");
        Assert.assertEquals(okHttpClient.connectTimeoutMillis(), 10000);
        Assert.assertEquals(okHttpClient.readTimeoutMillis(), 10000);

        // set config by (user)builderCallback
        RestClientConfig config = RestClientConfig.builder().builderCallback(
                builder -> builder.connectTimeout(5, TimeUnit.SECONDS)
                                  .readTimeout(30, TimeUnit.SECONDS))
                                  .build();

        restClient = new MockRestClientImpl(TEST_URL, config);
        okHttpClient = Whitebox.getInternalState(restClient, "client");

        Assert.assertEquals(okHttpClient.connectTimeoutMillis(), 5000);
        Assert.assertEquals(okHttpClient.readTimeoutMillis(), 30000);
    }

    @SneakyThrows
    @Test
    public void testRequest() {
        Response response = Mockito.mock(Response.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(response.code()).thenReturn(200);
        Mockito.when(response.headers())
               .thenReturn(new RestHeaders().toOkHttpHeader());
        Mockito.when(response.body().string()).thenReturn("content");

        Request.Builder requestBuilder = Mockito.mock(Request.Builder.class,
                                                      Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(requestBuilder.delete()).thenReturn(requestBuilder);
        Mockito.when(requestBuilder.get()).thenReturn(requestBuilder);
        Mockito.when(requestBuilder.put(Mockito.any())).thenReturn(requestBuilder);
        Mockito.when(requestBuilder.post(Mockito.any())).thenReturn(requestBuilder);
        Mockito.when(requestBuilder.url((HttpUrl) Mockito.any())).thenReturn(requestBuilder);
        MockRestClientImpl client = new MockRestClientImpl(TEST_URL, 1000) {
            @Override
            protected Request.Builder newRequestBuilder() {
                return requestBuilder;
            }
        };

        OkHttpClient okHttpClient = Mockito.mock(OkHttpClient.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(okHttpClient.newCall(Mockito.any()).execute()).thenReturn(response);

        Whitebox.setInternalState(client, "client", okHttpClient);

        RestResult result;

        // Test delete
        client.setAuthContext("token1");
        result = client.delete("test", ImmutableMap.of());
        Assert.assertEquals(200, result.status());
        Mockito.verify(requestBuilder).addHeader(RestHeaders.AUTHORIZATION, "token1");

        client.resetAuthContext();

        client.setAuthContext("token2");
        result = client.delete("test", "id");
        Assert.assertEquals(200, result.status());
        Mockito.verify(requestBuilder).addHeader(HttpHeaders.AUTHORIZATION, "token2");
        client.resetAuthContext();

        // Test get
        client.setAuthContext("token3");
        result = client.get("test");
        Assert.assertEquals(200, result.status());
        Mockito.verify(requestBuilder).addHeader(HttpHeaders.AUTHORIZATION, "token3");
        client.resetAuthContext();

        client.setAuthContext("token4");
        result = client.get("test", ImmutableMap.of());
        Assert.assertEquals(200, result.status());
        Mockito.verify(requestBuilder).addHeader(HttpHeaders.AUTHORIZATION, "token4");
        client.resetAuthContext();

        client.setAuthContext("token5");
        result = client.get("test", "id");
        Assert.assertEquals(200, result.status());
        Mockito.verify(requestBuilder).addHeader(HttpHeaders.AUTHORIZATION, "token5");
        client.resetAuthContext();

        // Test put
        client.setAuthContext("token6");
        result = client.post("test", null);
        Assert.assertEquals(200, result.status());
        Mockito.verify(requestBuilder).addHeader(HttpHeaders.AUTHORIZATION, "token6");
        client.resetAuthContext();

        client.setAuthContext("token7");
        result = client.post("test", null, new RestHeaders());
        Assert.assertEquals(200, result.status());
        Mockito.verify(requestBuilder).addHeader(HttpHeaders.AUTHORIZATION, "token7");
        client.resetAuthContext();

        client.setAuthContext("token8");
        result = client.post("test", null, ImmutableMap.of());
        Assert.assertEquals(200, result.status());
        Mockito.verify(requestBuilder).addHeader(HttpHeaders.AUTHORIZATION, "token8");
        client.resetAuthContext();

        client.setAuthContext("token9");
        result = client.post("test", null, new RestHeaders(),
                             ImmutableMap.of());
        Assert.assertEquals(200, result.status());
        Mockito.verify(requestBuilder).addHeader(HttpHeaders.AUTHORIZATION, "token9");
        client.resetAuthContext();

        // Test post
        client.setAuthContext("token10");
        result = client.post("test", null);
        Assert.assertEquals(200, result.status());
        Mockito.verify(requestBuilder).addHeader(HttpHeaders.AUTHORIZATION, "token10");
        client.resetAuthContext();

        client.setAuthContext("token11");
        result = client.post("test", null, new RestHeaders());
        Assert.assertEquals(200, result.status());
        Mockito.verify(requestBuilder).addHeader(HttpHeaders.AUTHORIZATION, "token11");
        client.resetAuthContext();

        client.setAuthContext("token12");
        result = client.post("test", null, ImmutableMap.of());
        Assert.assertEquals(200, result.status());
        Mockito.verify(requestBuilder).addHeader(HttpHeaders.AUTHORIZATION, "token12");
        client.resetAuthContext();

        client.setAuthContext("token13");
        result = client.post("test", null, new RestHeaders(),
                             ImmutableMap.of());
        Assert.assertEquals(200, result.status());
        Mockito.verify(requestBuilder).addHeader(HttpHeaders.AUTHORIZATION, "token13");
        client.resetAuthContext();
    }

    private static class RestClientImpl extends AbstractRestClient {

        private final int status;
        private final RestHeaders headers;
        private final String content;

        public RestClientImpl(String url, RestClientConfig config, int status) {
            this(url, config, status, new RestHeaders(), "");
        }

        public RestClientImpl(String url, RestClientConfig config, int status, RestHeaders headers,
                              String content) {
            super(url, config);
            this.status = status;
            this.headers = headers;
            this.content = content;
        }

        @SneakyThrows
        @Override
        protected Response request(Request.Builder requestBuilder) {
            Response response = Mockito.mock(Response.class, Mockito.RETURNS_DEEP_STUBS);
            Mockito.when(response.code()).thenReturn(this.status);
            Mockito.when(response.headers()).thenReturn(this.headers.toOkHttpHeader());
            Mockito.when(response.body().string()).thenReturn(this.content);
            return response;
        }

        @Override
        protected void checkStatus(Response response, int... statuses) {
            boolean match = false;
            for (int status : statuses) {
                if (status == response.code()) {
                    match = true;
                    break;
                }
            }
            if (!match) {
                throw new ClientException("Invalid response '%s'", response);
            }
        }
    }

    private static class MockRestClientImpl extends AbstractRestClient {

        public MockRestClientImpl(String url, int timeout) {
            super(url, timeout);
        }

        public MockRestClientImpl(String url, RestClientConfig config) {
            super(url, config);
        }

        @Override
        protected void checkStatus(Response response, int... statuses) {
            // pass
        }
    }
}
