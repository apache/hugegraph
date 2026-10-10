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

package org.apache.hugegraph.rest;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.security.KeyStore;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

import org.apache.commons.lang3.StringUtils;
import org.apache.hugegraph.util.JsonUtilCommon;
import org.jetbrains.annotations.NotNull;

import com.google.common.collect.ImmutableMap;

import lombok.SneakyThrows;
import okhttp3.ConnectionPool;
import okhttp3.HttpUrl;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okio.BufferedSink;
import okio.GzipSink;
import okio.Okio;

/**
 * This class provides an abstract implementation of the RestClient interface.
 * It provides methods for making HTTP requests (GET, POST, PUT, DELETE) to a REST API.
 * Note: It uses the OkHttp library to make these requests for now.
 */
public abstract class AbstractRestClient implements RestClient {

    private final ThreadLocal<String> authContext;

    private final OkHttpClient client;

    private final String baseUrl;

    public AbstractRestClient(String url, int timeout) {
        this(url, RestClientConfig.builder().timeout(timeout).build());
    }

    public AbstractRestClient(String url, String user, String password, int timeout) {
        this(url, RestClientConfig.builder()
                                  .user(user)
                                  .password(password)
                                  .timeout(timeout)
                                  .build());
    }

    public AbstractRestClient(String url, int timeout, int idleTime,
                              int maxConns, int maxConnsPerRoute) {
        this(url, RestClientConfig.builder()
                                  .idleTime(idleTime)
                                  .timeout(timeout)
                                  .maxConns(maxConns)
                                  .maxConnsPerRoute(maxConnsPerRoute)
                                  .build());
    }

    public AbstractRestClient(String url, String user, String password, int timeout,
                              int maxConns, int maxConnsPerRoute,
                              String trustStoreFile, String trustStorePassword) {
        this(url, RestClientConfig.builder()
                                  .user(user).password(password)
                                  .timeout(timeout)
                                  .maxConns(maxConns)
                                  .maxConnsPerRoute(maxConnsPerRoute)
                                  .trustStoreFile(trustStoreFile)
                                  .trustStorePassword(trustStorePassword)
                                  .build());
    }

    public AbstractRestClient(String url, String token, int timeout,
                              int maxConns, int maxConnsPerRoute,
                              String trustStoreFile, String trustStorePassword) {
        this(url, RestClientConfig.builder()
                                  .token(token)
                                  .timeout(timeout)
                                  .maxConns(maxConns)
                                  .maxConnsPerRoute(maxConnsPerRoute)
                                  .trustStoreFile(trustStoreFile)
                                  .trustStorePassword(trustStorePassword)
                                  .build());
    }

    public AbstractRestClient(String url, RestClientConfig config) {
        this.baseUrl = url;
        this.client = buildOkHttpClient(config);
        this.authContext = new InheritableThreadLocal<>();
    }

    private static RequestBody buildRequestBody(Object body, RestHeaders headers) {
        MediaType contentType = MediaType.parse(parseContentType(headers));
        String bodyContent;
        if (contentType != null && "application".equals(contentType.type()) &&
            "json".equals(contentType.subtype())) {
            if (body == null) {
                bodyContent = "{}";
            } else if (body instanceof String) {
                bodyContent = (String) body;
            } else {
                bodyContent = JsonUtilCommon.toJson(body);
            }
        } else {
            bodyContent = String.valueOf(body);
        }
        RequestBody requestBody = RequestBody.create(bodyContent, contentType);

        if (headers != null &&
            "gzip".equals(headers.get(RestHeaders.CONTENT_ENCODING))) {
            requestBody = gzipBody(requestBody);
        }
        return requestBody;
    }

    private static RequestBody gzipBody(final RequestBody body) {
        return new RequestBody() {
            @Override
            public MediaType contentType() {
                return body.contentType();
            }

            @Override
            public long contentLength() {
                return -1; // We don't know the compressed length in advance!
            }

            @Override
            public void writeTo(@NotNull BufferedSink sink) throws IOException {
                BufferedSink gzipSink = Okio.buffer(new GzipSink(sink));
                body.writeTo(gzipSink);
                gzipSink.close();
            }
        };
    }

    private static String parseContentType(RestHeaders headers) {
        if (headers != null) {
            String contentType = headers.get(RestHeaders.CONTENT_TYPE);
            if (contentType != null) {
                return contentType;
            }
        }
        return RestHeaders.APPLICATION_JSON;
    }

    private OkHttpClient buildOkHttpClient(RestClientConfig config) {
        OkHttpClient.Builder builder = new OkHttpClient.Builder();

        if (config.getTimeout() != null) {
            builder.connectTimeout(config.getTimeout(), TimeUnit.MILLISECONDS)
                   .readTimeout(config.getTimeout(), TimeUnit.MILLISECONDS);
        }
        if (config.getConnectTimeout() != null) {
            builder.connectTimeout(config.getConnectTimeout(), TimeUnit.MILLISECONDS);
        }
        if (config.getReadTimeout() != null) {
            builder.readTimeout(config.getReadTimeout(), TimeUnit.MILLISECONDS);
        }

        if (config.getMaxIdleConns() != null || config.getIdleTime() != null) {
            ConnectionPool connectionPool = new ConnectionPool(config.getMaxIdleConns(),
                                                               config.getIdleTime(),
                                                               TimeUnit.SECONDS);
            builder.connectionPool(connectionPool);
        }

        // auth header interceptor
        if (StringUtils.isNotBlank(config.getUser()) &&
            StringUtils.isNotBlank(config.getPassword())) {
            builder.addInterceptor(new OkHttpBasicAuthInterceptor(config.getUser(),
                                                                  config.getPassword()));
        }
        if (StringUtils.isNotBlank(config.getToken())) {
            builder.addInterceptor(new OkHttpTokenInterceptor(config.getToken()));
        }

        // ssl
        configSsl(builder, config.getTrustStoreFile(),
                  config.getTrustStorePassword());

        // Execute builder callback before builder.build() for user configs
        if (config.getBuilderCallback() != null) {
            config.getBuilderCallback().accept(builder);
        }

        OkHttpClient okHttpClient = builder.build();

        if (config.getMaxConns() != null) {
            okHttpClient.dispatcher().setMaxRequests(config.getMaxConns());
        }

        if (config.getMaxConnsPerRoute() != null) {
            okHttpClient.dispatcher().setMaxRequestsPerHost(config.getMaxConnsPerRoute());
        }

        return okHttpClient;
    }

    @SneakyThrows
    private void configSsl(OkHttpClient.Builder builder, String trustStoreFile,
                           String trustStorePass) {
        if (StringUtils.isBlank(trustStoreFile) || StringUtils.isBlank(trustStorePass)) {
            return;
        }

        X509TrustManager trustManager = trustManagerForCertificates(trustStoreFile, trustStorePass);
        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(null, new TrustManager[]{trustManager}, null);
        SSLSocketFactory sslSocketFactory = sslContext.getSocketFactory();

        builder.sslSocketFactory(sslSocketFactory, trustManager);
    }

    @Override
    public RestResult post(String path, Object object) {
        return this.post(path, object, null, null);
    }

    @Override
    public RestResult post(String path, Object object, RestHeaders headers) {
        return this.post(path, object, headers, null);
    }

    @Override
    public RestResult post(String path, Object object, Map<String, Object> params) {
        return this.post(path, object, null, params);
    }

    private Request.Builder genRequestBuilder(String path, String id, RestHeaders headers,
                                              Map<String, Object> params) {
        HttpUrl.Builder urlBuilder = Objects.requireNonNull(HttpUrl.parse(this.baseUrl))
                                            .newBuilder()
                                            .addPathSegments(path);
        if (id != null) {
            urlBuilder.addPathSegment(id);
        }

        if (params != null) {
            params.forEach((name, value) -> {
                if (value == null) {
                    return;
                }

                if (value instanceof Collection) {
                    for (Object i : (Collection<?>) value) {
                        urlBuilder.addQueryParameter(name, String.valueOf(i));
                    }
                } else {
                    urlBuilder.addQueryParameter(name, String.valueOf(value));
                }
            });
        }

        Request.Builder builder = newRequestBuilder().url(urlBuilder.build());

        if (headers != null) {
            builder.headers(headers.toOkHttpHeader());
        }

        this.attachAuthToRequest(builder);

        return builder;
    }

    /**
     * In order to provide subclasses with overloading opportunities
     */
    protected Request.Builder newRequestBuilder() {
        return new Request.Builder();
    }

    @SneakyThrows
    @Override
    public RestResult post(String path, Object object, RestHeaders headers,
                           Map<String, Object> params) {
        Request.Builder requestBuilder = genRequestBuilder(path, null, headers, params);
        requestBuilder.post(buildRequestBody(object, headers));

        try (Response response = request(requestBuilder)) {
            checkStatus(response, 200, 201, 202);
            return new RestResult(response);
        }
    }

    @Override
    public RestResult put(String path, String id, Object object) {
        return this.put(path, id, object, ImmutableMap.of());
    }

    @Override
    public RestResult put(String path, String id, Object object, RestHeaders headers) {
        return this.put(path, id, object, headers, null);
    }

    @Override
    public RestResult put(String path, String id, Object object, Map<String, Object> params) {
        return this.put(path, id, object, null, params);
    }

    @SneakyThrows
    @Override
    public RestResult put(String path, String id, Object object,
                          RestHeaders headers,
                          Map<String, Object> params) {
        Request.Builder requestBuilder = genRequestBuilder(path, id, headers, params);
        requestBuilder.put(buildRequestBody(object, headers));

        try (Response response = request(requestBuilder)) {
            checkStatus(response, 200, 202);
            return new RestResult(response);
        }
    }

    @Override
    public RestResult get(String path) {
        return this.get(path, null, ImmutableMap.of());
    }

    @Override
    public RestResult get(String path, Map<String, Object> params) {
        return this.get(path, null, params);
    }

    @Override
    public RestResult get(String path, String id) {
        return this.get(path, id, ImmutableMap.of());
    }

    @SneakyThrows
    private RestResult get(String path, String id, Map<String, Object> params) {
        Request.Builder requestBuilder = genRequestBuilder(path, id, null, params);

        try (Response response = request(requestBuilder)) {
            checkStatus(response, 200);
            return new RestResult(response);
        }
    }

    @Override
    public RestResult delete(String path, Map<String, Object> params) {
        return this.delete(path, null, params);
    }

    @Override
    public RestResult delete(String path, String id) {
        return this.delete(path, id, ImmutableMap.of());
    }

    @SneakyThrows
    private RestResult delete(String path, String id,
                              Map<String, Object> params) {
        Request.Builder requestBuilder = genRequestBuilder(path, id, null, params);
        requestBuilder.delete();

        try (Response response = request(requestBuilder)) {
            checkStatus(response, 204, 202);
            return new RestResult(response);
        }
    }

    protected abstract void checkStatus(Response response, int... statuses);

    @SneakyThrows
    protected Response request(Request.Builder requestBuilder) {
        return this.client.newCall(requestBuilder.build()).execute();
    }

    @SneakyThrows
    @Override
    public void close() {
        if (this.client != null) {
            this.client.dispatcher().executorService().shutdown();
            this.client.connectionPool().evictAll();
            if (this.client.cache() != null) {
                this.client.cache().close();
            }
        }
    }

    public static String encode(String raw) {
        try {
            return URLEncoder.encode(raw, StandardCharsets.UTF_8.toString()).replace("+", "%2B");
        } catch (UnsupportedEncodingException e) {
            throw new IllegalArgumentException("Failed to encode string: " + raw, e);
        }
    }

    public void resetAuthContext() {
        this.authContext.remove();
    }

    public String getAuthContext() {
        return this.authContext.get();
    }

    public void setAuthContext(String auth) {
        this.authContext.set(auth);
    }

    private void attachAuthToRequest(Request.Builder builder) {
        // Add auth header
        String auth = this.getAuthContext();
        if (StringUtils.isNotEmpty(auth)) {
            builder.addHeader(RestHeaders.AUTHORIZATION, auth);
        }
    }

    @SneakyThrows
    private X509TrustManager trustManagerForCertificates(String trustStoreFile,
                                                         String trustStorePass) {
        char[] password = trustStorePass.toCharArray();

        // load keyStore
        KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
        try (FileInputStream in = new FileInputStream(trustStoreFile)) {
            keyStore.load(in, password);
        }

        TrustManagerFactory trustManagerFactory =
                TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trustManagerFactory.init(keyStore);

        TrustManager[] trustManagers = trustManagerFactory.getTrustManagers();
        if (trustManagers.length != 1 || !(trustManagers[0] instanceof X509TrustManager)) {
            throw new IllegalStateException("Unexpected default trust managers:" +
                                            Arrays.toString(trustManagers));
        }
        return (X509TrustManager) trustManagers[0];
    }

    /**
     * Compatibility wrapper using the same certificate identity verification as OkHttp.
     */
    public static class HostNameVerifier implements HostnameVerifier {

        private static final HostnameVerifier DEFAULT_VERIFIER = new OkHttpClient().hostnameVerifier();

        public HostNameVerifier(String url) {
            // Keep the public constructor; a URL is not evidence of the peer's identity.
        }

        @Override
        public boolean verify(String hostname, SSLSession session) {
            return DEFAULT_VERIFIER.verify(hostname, session);
        }
    }
}
