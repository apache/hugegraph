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

package org.apache.hugegraph.api.gremlin;

import java.net.SocketTimeoutException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.script.Bindings;
import javax.script.SimpleBindings;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.VersionInfo;
import org.apache.hugegraph.auth.HugeGraphAuthProxy;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.config.ServerOptions;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.JsonUtil;
import org.apache.hugegraph.util.Log;
import org.apache.tinkerpop.gremlin.groovy.engine.GremlinExecutor;
import org.apache.tinkerpop.gremlin.jsr223.GremlinScriptChecker;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.AbstractTraverser;
import org.apache.tinkerpop.gremlin.server.GraphManager;
import org.apache.tinkerpop.gremlin.server.GremlinServer;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.apache.tinkerpop.gremlin.server.auth.AllowAllAuthenticator;
import org.apache.tinkerpop.gremlin.server.handler.HttpHandlerUtil;
import org.apache.tinkerpop.gremlin.server.util.MetricManager;
import org.apache.tinkerpop.gremlin.server.util.ServerGremlinExecutor;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceFactory;
import org.apache.tinkerpop.gremlin.util.Tokens;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.apache.tinkerpop.gremlin.util.message.RequestMessage;
import org.apache.tinkerpop.gremlin.util.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.util.message.ResponseStatusCode;
import org.apache.tinkerpop.gremlin.util.ser.MessageTextSerializer;
import org.apache.tinkerpop.gremlin.util.ser.SerializationException;
import org.glassfish.jersey.internal.Version;
import org.slf4j.Logger;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import jakarta.ws.rs.ProcessingException;
import jakarta.ws.rs.ServiceUnavailableException;
import jakarta.ws.rs.core.MultivaluedMap;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.UriBuilder;

/**
 * In-process adapter for the TinkerPop 3.8.1 JSON HTTP contract. Materialization,
 * commit and serialization must remain inside GremlinExecutor's transform so
 * that graph transactions and the authenticated user stay on the worker.
 */
public final class EmbeddedGremlinRestExecutor implements GremlinRestExecutor {

    private static final Logger LOG = Log.logger(EmbeddedGremlinRestExecutor.class);
    private static final Logger AUDIT = Log.logger(GremlinServer.AUDIT_LOGGER_NAME);
    private static final Timer EVAL_TIMER = MetricManager.INSTANCE.getTimer(
            MetricRegistry.name(GremlinServer.class, "op", "eval"));

    private static final String USER_AGENT = userAgent();

    private final HugeConfig config;
    private final Settings settings;
    private final GraphManager graphs;
    private final GremlinExecutor executor;
    private final Semaphore admission;
    private final long waitMillis;
    private final URI target;
    private volatile MessageTextSerializer<?> serializer;
    private volatile ForwardGremlinRestExecutor forwarding;
    private volatile boolean stopping;

    public EmbeddedGremlinRestExecutor(HugeConfig config, ServerGremlinExecutor server) {
        this.config = config;
        this.settings = server.getSettings();
        this.graphs = server.getGraphManager();
        this.executor = server.getGremlinExecutor();
        this.admission = new Semaphore(config.get(ServerOptions.GREMLIN_SERVER_MAX_ROUTE), true);
        this.waitMillis = TimeUnit.SECONDS.toMillis(config.get(ServerOptions.GREMLIN_SERVER_TIMEOUT));
        this.target = URI.create(config.get(ServerOptions.GREMLIN_SERVER_URL));
    }

    /** Select the execution path once, after the listener has initialized its serializers. */
    public synchronized void initialize(MessageTextSerializer<?> serializer) {
        E.checkState(this.serializer == null && this.forwarding == null && !this.stopping,
                     "Gremlin REST executor is already initialized or stopping");
        if (serializer == null) {
            // Keep the adapter already injected into REST, and preserve the listener's HTTP behavior.
            this.forwarding = new ForwardGremlinRestExecutor(this.config);
        } else {
            this.serializer = serializer;
        }
    }

    @Override
    public Response get(String authorization, MultivaluedMap<String, String> parameters,
                        String remoteAddress) {
        ForwardGremlinRestExecutor forward = this.forwarding;
        if (forward != null) {
            return forward.get(authorization, parameters, remoteAddress);
        }
        UriBuilder builder = UriBuilder.fromUri(this.target);
        for (Map.Entry<String, List<String>> entry : parameters.entrySet()) {
            E.checkArgument(entry.getValue().size() == 1,
                            "Invalid query param '%s', can only accept one value, but got %s",
                            entry.getKey(), entry.getValue());
            builder.queryParam(entry.getKey(), entry.getValue().get(0));
        }
        final URI canonical;
        try {
            canonical = builder.build();
        } catch (IllegalArgumentException e) {
            // JerseyWebTarget.getUri() reports unresolved templates this way.
            throw new IllegalStateException(e.getMessage(), e);
        }
        String uri = canonical.getRawPath();
        if (uri == null || uri.isEmpty()) {
            uri = "/";
        }
        if (canonical.getRawQuery() != null) {
            uri += "?" + canonical.getRawQuery();
        }
        return this.execute(HttpMethod.GET, uri, new byte[0], authorization, remoteAddress);
    }

    private String path() {
        String path = this.target.getRawPath();
        return path == null || path.isEmpty() ? "/" : path;
    }

    @Override
    public Response post(String authorization, String body, String remoteAddress) {
        ForwardGremlinRestExecutor forward = this.forwarding;
        if (forward != null) {
            return forward.post(authorization, body, remoteAddress);
        }
        String uri = this.path();
        if (this.target.getRawQuery() != null) {
            uri += "?" + this.target.getRawQuery();
        }
        return this.execute(HttpMethod.POST, uri, body.getBytes(StandardCharsets.UTF_8),
                            authorization, remoteAddress);
    }

    private void checkAvailable() {
        if (this.stopping || this.serializer == null) {
            throw new ServiceUnavailableException("Gremlin REST is not accepting requests");
        }
    }

    private Response execute(HttpMethod method, String uri, byte[] body,
                             String authorization, String remoteAddress) {
        this.checkAvailable();
        boolean acquired = false;
        try {
            // Like the existing connection pool, wait for a lease without a new
            // fail-fast policy. The read timeout starts after admission.
            this.admission.acquire();
            acquired = true;
            this.checkAvailable();
            long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(this.waitMillis);
            if ((method + " " + uri + " HTTP/1.1").length() >
                this.settings.maxInitialLineLength) {
                return this.protocolError();
            }
            if (this.headerSize(method, authorization) > this.settings.maxHeaderSize) {
                return this.protocolError();
            }
            if (body.length > this.settings.maxContentLength) {
                return Response.status(413).build();
            }
            if ("/favicon.ico".equals(uri)) {
                return error(404, null, "Gremlin Server doesn't have a favicon.ico", null);
            }
            final RequestMessage request;
            FullHttpRequest http = new DefaultFullHttpRequest(
                    HttpVersion.HTTP_1_1, method, uri, Unpooled.wrappedBuffer(body));
            try {
                request = HttpHandlerUtil.getRequestMessageFromHttpRequest(http);
            } catch (IllegalArgumentException e) {
                return error(400, null, e.getMessage(), null);
            } catch (RuntimeException e) {
                return error(500, null, e.getMessage(), null);
            } finally {
                http.release();
            }
            if (this.settings.enableAuditLog) {
                HugeGraphAuthProxy.Context context = HugeGraphAuthProxy.getContext();
                boolean anonymous = AllowAllAuthenticator.class.getName().equals(
                        this.settings.authentication.authenticator);
                String user = anonymous || context == null ? "anonymous" : context.user().username();
                AUDIT.info("User {} with address {} requested: {}", user, remoteAddress,
                           request.getArgOrDefault(Tokens.ARGS_GREMLIN, ""));
            }
            Timer.Context timer = EVAL_TIMER.time();
            final Bindings bindings;
            try {
                bindings = this.bindings(request);
            } catch (IllegalStateException e) {
                return error(400, request.getRequestId(), e.getMessage(), null);
            }
            CompletableFuture<Object> evaluation;
            try {
                evaluation = this.executor.eval(
                        request.getArg(Tokens.ARGS_GREMLIN),
                        request.getArg(Tokens.ARGS_LANGUAGE), bindings,
                        request.getArgOrDefault(Tokens.ARGS_EVAL_TIMEOUT, null), value -> {
                            timer.stop();
                            return this.serialize(request, value);
                        });
            } catch (Exception e) {
                Throwable root = ExceptionUtils.getRootCause(e);
                return error(500, request.getRequestId(),
                             root == null ? e.getMessage() : root.getMessage(), null);
            }
            CompletableFuture<Response> response = evaluation.handle((value, failure) -> {
                if (failure != null) {
                    String message = failure.getMessage() != null ? failure.getMessage() :
                                     "Error encountered evaluating script: " +
                                     request.getArg(Tokens.ARGS_GREMLIN);
                    return error(500, request.getRequestId(), message, failure);
                }
                byte[] bytes = (byte[]) value;
                return Response.ok(bytes, "application/json")
                               .header("Content-Length", bytes.length).build();
            });
            // Never cancel this CompletableFuture on a REST read timeout:
            // TinkerPop owns the execution task and its timeout/rollback hooks.
            // The completion handler also preserves error metrics after a client timeout.
            try {
                long remaining = Math.max(0L, deadline - System.nanoTime());
                return response.get(remaining, TimeUnit.NANOSECONDS);
            } catch (ExecutionException e) {
                throw new ProcessingException(e.getCause());
            } catch (TimeoutException e) {
                throw new ProcessingException(new SocketTimeoutException("Read timed out"));
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ProcessingException(e);
        } finally {
            if (acquired) {
                this.admission.release();
            }
        }
    }

    private Response protocolError() {
        // The decoder replaces an invalid request with a headerless GET. The
        // standard auth handler rejects that request before the JSON handler.
        if (!AllowAllAuthenticator.class.getName().equals(this.settings.authentication.authenticator)) {
            return Response.status(401).build();
        }
        return error(400, null, "no gremlin script supplied", null);
    }

    private static String userAgent() {
        VersionInfo version = VersionInfo.loadVersionInfo(
                "org.apache.http.client", HttpClientBuilder.class
                                                                  .getClassLoader());
        String release = version == null ? "UNAVAILABLE" : version.getRelease();
        return "Jersey/" + Version.getVersion() + " (Apache HttpClient " + release + ")";
    }

    private int headerSize(HttpMethod method, String authorization) {
        // HttpObjectDecoder counts header lines without CRLF. Match the headers
        // produced by the pinned Jersey/Apache connector, not the outer REST headers.
        String host = this.target.getHost();
        int defaultPort = "https".equalsIgnoreCase(this.target.getScheme()) ? 443 : 80;
        if (this.target.getPort() != -1 && this.target.getPort() != defaultPort) {
            host += ":" + this.target.getPort();
        }
        int length = ("Accept: application/json" +
                      "User-Agent: " + USER_AGENT +
                      "Accept-Encoding: gzip" + "Host: " + host +
                      "Connection: Keep-Alive").length();
        if (authorization != null) {
            length += ("Authorization: " + authorization).getBytes(StandardCharsets.ISO_8859_1).length;
        }
        if (HttpMethod.POST.equals(method)) {
            length += "Content-Type: application/jsonTransfer-Encoding: chunked".length();
        }
        return length;
    }

    private Bindings bindings(RequestMessage request) {
        Bindings bindings = new SimpleBindings();
        Map<String, String> aliases = request.getArgOrDefault(Tokens.ARGS_ALIASES,
                                                            Collections.emptyMap());
        for (Map.Entry<String, String> alias : aliases.entrySet()) {
            Graph graph = this.graphs.getGraph(alias.getValue());
            if (graph != null) {
                bindings.put(alias.getKey(), graph);
            } else {
                TraversalSource source = this.graphs.getTraversalSource(alias.getValue());
                if (source == null) {
                    throw new IllegalStateException(String.format(
                            "Could not rebind [%s] to [%s] as [%s] not in the Graph or " +
                            "TraversalSource global bindings", alias.getKey(), alias.getValue(),
                            alias.getValue()));
                }
                bindings.put(alias.getKey(), source);
            }
        }
        bindings.putAll(request.getArgOrDefault(Tokens.ARGS_BINDINGS, Collections.emptyMap()));
        return bindings;
    }

    private byte[] serialize(RequestMessage request, Object value) {
        List<Object> results = IteratorUtils.asList(value);
        Optional<String> materialize = GremlinScriptChecker.parse(
                (String) request.getArg(Tokens.ARGS_GREMLIN)).getMaterializeProperties();
        if (materialize.isPresent() &&
            Tokens.MATERIALIZE_PROPERTIES_TOKENS.equals(materialize.get())) {
            // Keep the pinned HTTP handler's behavior, including empty-result errors.
            Object first = results.get(0);
            if (first instanceof Element) {
                for (int i = 0; i < results.size(); i++) {
                    results.set(i, ReferenceFactory.detach((Element) results.get(i)));
                }
            } else if (first instanceof AbstractTraverser) {
                for (Object item : results) {
                    ((AbstractTraverser<?>) item).detach();
                }
            }
        }
        ResponseMessage response = ResponseMessage.build(request.getRequestId())
                                                  .code(ResponseStatusCode.SUCCESS)
                                                  .result(results).create();
        if (this.settings.strictTransactionManagement) {
            Map<String, String> aliases = request.getArg(Tokens.ARGS_ALIASES);
            this.graphs.commit(new HashSet<>(aliases.values()));
        } else {
            this.graphs.commitAll();
        }
        ByteBuf buffer = null;
        try {
            buffer = this.serializer.serializeResponseAsBinary(response,
                                                               UnpooledByteBufAllocator.DEFAULT);
            byte[] bytes = new byte[buffer.readableBytes()];
            buffer.readBytes(bytes);
            return bytes;
        } catch (SerializationException e) {
            // Preserve TP's cause-free serialization failure after commit.
            throw new CompletionException(new SerializationException(
                    "Could not serialize the result with application/json - " + e.getMessage()));
        } finally {
            if (buffer != null) {
                buffer.release();
            }
        }
    }

    private static Response error(int status, UUID requestId, String message, Throwable cause) {
        LOG.warn("Invalid request - responding with {} and {}", status, message, cause);
        MetricManager.INSTANCE.getMeter(MetricRegistry.name(GremlinServer.class, "errors")).mark();
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("message", message);
        if (cause != null) {
            body.put("Exception-Class", cause.getClass().getName());
            body.put("exceptions", ExceptionUtils.getThrowableList(cause).stream()
                                            .map(t -> t.getClass().getName()).toList());
            body.put("stackTrace", ExceptionUtils.getStackTrace(cause));
        }
        if (requestId != null) {
            body.put("requestId", requestId.toString());
        }
        byte[] bytes = JsonUtil.toJson(body).getBytes(StandardCharsets.UTF_8);
        return Response.status(status).type("application/json").entity(bytes)
                       .header("Content-Length", bytes.length).build();
    }

    @Override
    public synchronized void beginShutdown() {
        this.stopping = true;
        if (this.forwarding != null) {
            this.forwarding.beginShutdown();
        }
    }

    @Override
    public synchronized void close() {
        // Shared workers, graphs and serializers are owned by GremlinServer.
        this.beginShutdown();
        if (this.forwarding != null) {
            this.forwarding.close();
        }
    }
}
