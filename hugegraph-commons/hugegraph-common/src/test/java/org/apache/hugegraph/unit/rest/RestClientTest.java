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

import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;

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
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsServer;

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
    public void testHttpsCertificateIdentity() throws Exception {
        Path directory = Files.createTempDirectory("rest-client-tls-");
        try {
            Path validStore = createServerStore(directory, "valid", "dns:localhost,ip:127.0.0.1");
            Path wrongStore = createServerStore(directory, "wrong", "dns:other.example");
            Path noSanStore = createServerStore(directory, "no-san", null);
            assertHttpsRequest(validStore, validStore, "localhost", null);
            assertHttpsRequest(validStore, validStore, "127.0.0.1", null);
            assertHttpsRequest(wrongStore, wrongStore, "localhost", SSLPeerUnverifiedException.class);
            assertHttpsRequest(noSanStore, noSanStore, "localhost", SSLPeerUnverifiedException.class);
            assertHttpsRequest(validStore, wrongStore, "localhost", SSLHandshakeException.class);
        } finally {
            try (Stream<Path> paths = Files.walk(directory)) {
                for (Path path : (Iterable<Path>) paths.sorted(Comparator.reverseOrder())::iterator) {
                    Files.deleteIfExists(path);
                }
            }
        }
    }

    private static Path createServerStore(Path directory, String name, String san) throws Exception {
        Path store = directory.resolve(name + ".p12");
        List<String> command = new ArrayList<>(Arrays.asList(
                Paths.get(System.getProperty("java.home"), "bin", "keytool").toString(),
                "-genkeypair", "-alias", "server", "-keyalg", "RSA", "-keysize", "2048",
                "-validity", "2", "-dname", "CN=localhost", "-storetype", "PKCS12",
                "-keystore", store.toString(), "-storepass", "test-password",
                "-keypass", "test-password", "-noprompt"));
        if (san != null) {
            command.addAll(Arrays.asList("-ext", "SAN=" + san));
        }
        Path log = directory.resolve(name + ".log");
        Process process = new ProcessBuilder(command).redirectErrorStream(true)
                                                     .redirectOutput(log.toFile()).start();
        try {
            boolean finished = process.waitFor(30, TimeUnit.SECONDS);
            Assert.assertTrue("keytool timed out", finished);
            Assert.assertEquals(Files.readString(log, StandardCharsets.UTF_8), 0, process.exitValue());
        } finally {
            if (process.isAlive()) {
                process.destroyForcibly();
                process.waitFor(5, TimeUnit.SECONDS);
            }
        }
        return store;
    }

    private static KeyStore readStore(Path path) throws Exception {
        KeyStore store = KeyStore.getInstance("PKCS12");
        try (InputStream input = Files.newInputStream(path)) {
            store.load(input, "test-password".toCharArray());
        }
        return store;
    }

    private static void assertHttpsRequest(Path serverStore, Path trustedStore, String hostname,
                                           Class<? extends Throwable> failure) throws Exception {
        KeyStore keys = readStore(serverStore);
        KeyManagerFactory keyManagers = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        keyManagers.init(keys, "test-password".toCharArray());
        SSLContext context = SSLContext.getInstance("TLS");
        context.init(keyManagers.getKeyManagers(), null, null);
        HttpsServer server = HttpsServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.setHttpsConfigurator(new HttpsConfigurator(context));
        server.createContext("/probe", exchange -> {
            byte[] response = "{}".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, response.length);
            try (OutputStream output = exchange.getResponseBody()) {
                output.write(response);
            }
        });
        Path trustStore = Files.createTempFile(serverStore.getParent(), "trust-", ".jks");
        server.start();
        try {
            KeyStore trusted = KeyStore.getInstance("JKS");
            trusted.load(null, null);
            trusted.setCertificateEntry("server", readStore(trustedStore).getCertificate("server"));
            try (OutputStream output = Files.newOutputStream(trustStore)) {
                trusted.store(output, "test-password".toCharArray());
            }
            RestClientConfig config = RestClientConfig.builder().timeout(5000)
                    .trustStoreFile(trustStore.toString()).trustStorePassword("test-password").build();
            String url = "https://" + hostname + ":" + server.getAddress().getPort();
            RestClient client = new AbstractRestClient(url, config) {
                @Override
                protected void checkStatus(Response response, int... statuses) {
                    Assert.assertEquals(200, response.code());
                }
            };
            try {
                if (failure == null) {
                    Assert.assertEquals(200, client.get("probe").status());
                } else {
                    org.junit.Assert.assertThrows(failure, () -> client.get("probe"));
                }
            } finally {
                client.close();
            }
            SSLSession session = Mockito.mock(SSLSession.class);
            Mockito.when(session.getPeerCertificates()).thenReturn(keys.getCertificateChain("server"));
            Assert.assertEquals(failure != SSLPeerUnverifiedException.class,
                                new AbstractRestClient.HostNameVerifier(url).verify(hostname, session));
        } finally {
            server.stop(0);
            Files.deleteIfExists(trustStore);
        }
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
