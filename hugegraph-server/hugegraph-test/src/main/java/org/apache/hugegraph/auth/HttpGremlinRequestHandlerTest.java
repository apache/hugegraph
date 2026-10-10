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

package org.apache.hugegraph.auth;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hugegraph.server.HttpGremlinRequestHandler;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.util.JsonUtil;
import org.apache.tinkerpop.gremlin.groovy.engine.GremlinExecutor;
import org.apache.tinkerpop.gremlin.server.GraphManager;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.util.ser.GraphSONUntypedMessageSerializerV1;
import org.junit.Test;
import org.mockito.Mockito;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;

public class HttpGremlinRequestHandlerTest {

    @Test
    public void testInvalidAliasRespondsOnceThenValidRequestWorks()
            throws Exception {
        try (GremlinExecutor executor = GremlinExecutor.build().create()) {
            EmbeddedChannel channel = channel(executor);
            try {
                FullHttpRequest invalid = request("missing", "101");
                channel.writeInbound(invalid);
                FullHttpResponse rejected = response(channel);
                Assert.assertEquals(HttpResponseStatus.BAD_REQUEST,
                                    rejected.status());
                rejected.release();
                Assert.assertEquals(0, invalid.refCnt());
                Assert.assertNull(channel.readOutbound());
                Assert.assertTrue(channel.isActive());

                FullHttpRequest valid = request("known", "202");
                channel.writeInbound(valid);
                FullHttpResponse accepted = response(channel);
                try {
                    Assert.assertEquals(HttpResponseStatus.OK,
                                        accepted.status());
                    Map<?, ?> body = JsonUtil.fromJson(
                            accepted.content().toString(StandardCharsets.UTF_8),
                            Map.class);
                    Map<?, ?> result = (Map<?, ?>) body.get("result");
                    Assert.assertEquals(Collections.singletonList(202),
                                        result.get("data"));
                } finally {
                    accepted.release();
                }
                Assert.assertEquals(0, valid.refCnt());
                Assert.assertNull(channel.readOutbound());
            } finally {
                channel.finishAndReleaseAll();
            }
        }
    }

    @Test
    public void testEarlyErrorsReleaseRequestExactlyOnce() throws Exception {
        try (GremlinExecutor executor = GremlinExecutor.build().create()) {
            EmbeddedChannel channel = channel(executor);
            try {
                assertRejected(channel, requestBody("{"),
                               HttpResponseStatus.BAD_REQUEST);
                assertRejected(channel, requestBody("{}"),
                               HttpResponseStatus.BAD_REQUEST);
                FullHttpRequest method = request("known", "1");
                method.setMethod(HttpMethod.PUT);
                assertRejected(channel, method,
                               HttpResponseStatus.METHOD_NOT_ALLOWED);
                FullHttpRequest accept = request("known", "1");
                accept.headers().set(HttpHeaderNames.ACCEPT,
                                     "application/unsupported");
                assertRejected(channel, accept,
                               HttpResponseStatus.BAD_REQUEST);
                FullHttpRequest favicon = request("known", "1");
                favicon.setUri("/favicon.ico");
                assertRejected(channel, favicon, HttpResponseStatus.NOT_FOUND);
            } finally {
                channel.finishAndReleaseAll();
            }
        }
    }

    @Test
    public void testConnectionCloseReleasesRequestOnSuccessAndFailure()
            throws Exception {
        try (GremlinExecutor executor = GremlinExecutor.build().create()) {
            for (String alias : new String[]{"missing", "known"}) {
                EmbeddedChannel channel = channel(executor);
                FullHttpRequest request = request(alias, "202");
                request.headers().set(HttpHeaderNames.CONNECTION,
                                      HttpHeaderValues.CLOSE);
                try {
                    channel.writeInbound(request);
                    FullHttpResponse response = response(channel);
                    try {
                        Assert.assertEquals("known".equals(alias) ?
                                            HttpResponseStatus.OK :
                                            HttpResponseStatus.BAD_REQUEST,
                                            response.status());
                    } finally {
                        response.release();
                    }
                    awaitClosed(channel);
                    Assert.assertFalse(channel.isActive());
                    Assert.assertEquals(0, request.refCnt());
                    Assert.assertNull(channel.readOutbound());
                } finally {
                    channel.finishAndReleaseAll();
                }
            }
        }
    }

    @Test
    public void testSharedBufferRetainsCallersReference() throws Exception {
        try (GremlinExecutor executor = GremlinExecutor.build().create()) {
            EmbeddedChannel channel = channel(executor);
            FullHttpRequest request = request("missing", "1");
            request.retain();
            try {
                channel.writeInbound(request);
                FullHttpResponse rejected = response(channel);
                Assert.assertEquals(HttpResponseStatus.BAD_REQUEST,
                                    rejected.status());
                rejected.release();
                Assert.assertEquals(1, request.refCnt());
                Assert.assertNull(channel.readOutbound());
            } finally {
                request.release();
                channel.finishAndReleaseAll();
            }
        }
    }

    @Test
    public void testAsyncWriteFailureDoesNotRetainRequest() throws Exception {
        CountDownLatch evaluating = new CountDownLatch(1);
        CountDownLatch proceed = new CountDownLatch(1);
        AtomicReference<FullHttpResponse> attempted = new AtomicReference<>();
        try (GremlinExecutor executor = GremlinExecutor.build()
                .beforeEval(bindings -> {
                    evaluating.countDown();
                    try {
                        Assert.assertTrue(proceed.await(10, TimeUnit.SECONDS));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException(e);
                    }
                }).create()) {
            EmbeddedChannel channel = channel(executor);
            channel.pipeline().addFirst("failed-write",
                    new ChannelOutboundHandlerAdapter() {
                        @Override
                        public void write(ChannelHandlerContext context,
                                          Object message,
                                          ChannelPromise promise) {
                            FullHttpResponse response =
                                    (FullHttpResponse) message;
                            attempted.set(response);
                            response.release();
                            promise.setFailure(new IOException("test write failure"));
                        }
                    });
            FullHttpRequest request = request("known", "202");
            try {
                channel.writeInbound(request);
                Assert.assertEquals(0, request.refCnt());
                Assert.assertTrue(evaluating.await(10, TimeUnit.SECONDS));
                proceed.countDown();
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                while (attempted.get() == null && System.nanoTime() < deadline) {
                    channel.runPendingTasks();
                    Thread.sleep(5L);
                }
                Assert.assertNotNull(attempted.get());
                Assert.assertEquals(0, attempted.get().refCnt());
                Assert.assertEquals(0, request.refCnt());
                Assert.assertNull(channel.readOutbound());
            } finally {
                proceed.countDown();
                channel.finishAndReleaseAll();
            }
        }
    }

    private static void assertRejected(EmbeddedChannel channel,
                                       FullHttpRequest request,
                                       HttpResponseStatus expected)
            throws InterruptedException {
        channel.writeInbound(request);
        FullHttpResponse response = response(channel);
        try {
            Assert.assertEquals(expected, response.status());
        } finally {
            response.release();
        }
        Assert.assertEquals(0, request.refCnt());
        Assert.assertNull(channel.readOutbound());
        Assert.assertTrue(channel.isActive());
    }

    private static EmbeddedChannel channel(GremlinExecutor executor) {
        GraphManager manager = Mockito.mock(GraphManager.class);
        Mockito.when(manager.getGraph("known"))
               .thenReturn(Mockito.mock(Graph.class));
        return new EmbeddedChannel(new HttpGremlinRequestHandler(
                Collections.singletonMap("application/json",
                        new GraphSONUntypedMessageSerializerV1()),
                executor, manager, new Settings()));
    }

    private static FullHttpRequest request(String alias, String script) {
        String body = "{\"gremlin\":\"" + script + "\",\"language\":" +
                      "\"gremlin-groovy\",\"aliases\":{\"graph\":\"" + alias + "\"}}";
        return requestBody(body);
    }

    private static FullHttpRequest requestBody(String body) {
        FullHttpRequest request = new DefaultFullHttpRequest(
                HttpVersion.HTTP_1_1, HttpMethod.POST, "/gremlin",
                Unpooled.copiedBuffer(body, StandardCharsets.UTF_8));
        request.headers().set(HttpHeaderNames.CONTENT_TYPE,
                              HttpHeaderValues.APPLICATION_JSON);
        request.headers().set(HttpHeaderNames.ACCEPT,
                              HttpHeaderValues.APPLICATION_JSON);
        request.headers().set(HttpHeaderNames.CONNECTION,
                              HttpHeaderValues.KEEP_ALIVE);
        HttpUtil.setContentLength(request, request.content().readableBytes());
        return request;
    }

    private static void awaitClosed(EmbeddedChannel channel)
            throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (channel.isActive() && System.nanoTime() < deadline) {
            channel.runPendingTasks();
            channel.runScheduledPendingTasks();
            if (channel.isActive()) {
                Thread.sleep(5L);
            }
        }
    }

    private static FullHttpResponse response(EmbeddedChannel channel)
            throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (System.nanoTime() < deadline) {
            channel.runPendingTasks();
            FullHttpResponse response = channel.readOutbound();
            if (response != null) {
                return response;
            }
            Thread.sleep(5L);
        }
        throw new AssertionError("Gremlin HTTP response did not arrive");
    }
}
