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

import static io.netty.handler.codec.http.HttpHeaderNames.CONNECTION;
import static io.netty.handler.codec.http.HttpHeaderNames.UPGRADE;
import static io.netty.handler.codec.http.HttpResponseStatus.UNAUTHORIZED;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import static org.apache.tinkerpop.gremlin.groovy.jsr223.dsl.credential.CredentialGraphTokens.PROPERTY_PASSWORD;
import static org.apache.tinkerpop.gremlin.groovy.jsr223.dsl.credential.CredentialGraphTokens.PROPERTY_USERNAME;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import org.apache.tinkerpop.gremlin.server.Settings;
import org.apache.tinkerpop.gremlin.server.auth.AuthenticationException;
import org.apache.tinkerpop.gremlin.server.auth.AuthenticatedUser;
import org.apache.tinkerpop.gremlin.server.auth.Authenticator;
import org.apache.tinkerpop.gremlin.server.handler.AbstractAuthenticationHandler;
import org.apache.tinkerpop.gremlin.server.handler.SaslAuthenticationHandler;
import org.apache.tinkerpop.gremlin.server.handler.StateKey;

import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpMessage;
import io.netty.handler.codec.http.HttpMessage;
import io.netty.util.ReferenceCountUtil;

/**
 * An Authentication Handler for doing WebSocket and Http Basic auth
 * TODO: remove this class after fixed TINKERPOP-2374
 */
@ChannelHandler.Sharable
public class WsAndHttpBasicAuthHandler extends SaslAuthenticationHandler {

    private static final String AUTHENTICATOR = "authenticator";
    private static final String HTTP_AUTH = "http-authentication";
    public WsAndHttpBasicAuthHandler(Authenticator authenticator,
                                     Settings settings) {
        super(authenticator, settings);
    }

    @Override
    public void channelRead(final ChannelHandlerContext ctx, final Object obj)
            throws Exception {
        if (obj instanceof HttpMessage && !isWebSocket((HttpMessage) obj)) {
            ChannelPipeline pipeline = ctx.pipeline();
            ChannelHandler authHandler = pipeline.get(HTTP_AUTH);
            if (authHandler != null) {
                authHandler = pipeline.remove(HTTP_AUTH);
            } else {
                authHandler = new HttpBasicAuthHandler(this.authenticator);
            }
            pipeline.addAfter(AUTHENTICATOR, HTTP_AUTH, authHandler);
            ctx.fireChannelRead(obj);
        } else {
            super.channelRead(ctx, obj);
        }
    }

    public static boolean isWebSocket(final HttpMessage msg) {
        final String connectionHeader = msg.headers().get(CONNECTION);
        final String upgradeHeader = msg.headers().get(UPGRADE);
        return "Upgrade".equalsIgnoreCase(connectionHeader) ||
               "WebSocket".equalsIgnoreCase(upgradeHeader);
    }

    @ChannelHandler.Sharable
    private static class HttpBasicAuthHandler
            extends AbstractAuthenticationHandler {

        private final Base64.Decoder decoder = Base64.getDecoder();

        public HttpBasicAuthHandler(Authenticator authenticator) {
            super(authenticator);
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            if (msg instanceof FullHttpMessage) {
                final FullHttpMessage request = (FullHttpMessage) msg;
                if (!request.headers().contains("Authorization")) {
                    sendError(ctx, msg);
                    return;
                }

                final String header = request.headers().get("Authorization");
                final Map<String, String> credentials = new HashMap<>();
                int separator = header.indexOf(' ');
                if (separator <= 0 || separator == header.length() - 1) {
                    sendError(ctx, msg);
                    return;
                }
                String scheme = header.substring(0, separator);
                String payload = header.substring(separator + 1);
                if ("Basic".equalsIgnoreCase(scheme)) {
                    if (!parseBasicCredentials(payload, credentials)) {
                        sendError(ctx, msg);
                        return;
                    }
                } else {
                    sendError(ctx, msg);
                    return;
                }

                String address = ctx.channel().remoteAddress().toString();
                if (address.startsWith("/") && address.length() > 1) {
                    address = address.substring(1);
                }

                credentials.put(HugeAuthenticator.KEY_ADDRESS, address);

                try {
                    AuthenticatedUser user =
                            this.authenticator.authenticate(credentials);
                    ctx.channel().attr(StateKey.AUTHENTICATED_USER).set(user);
                    ctx.fireChannelRead(request);
                } catch (AuthenticationException ae) {
                    sendError(ctx, msg);
                }
            }
        }

        private boolean parseBasicCredentials(String encoded,
                                              Map<String, String> credentials) {
            byte[] userPass;
            try {
                userPass = this.decoder.decode(encoded);
            } catch (IllegalArgumentException e) {
                return false;
            }
            String authorization = new String(userPass, StandardCharsets.UTF_8);
            String[] split = authorization.split(":", 2);
            if (split.length != 2) {
                return false;
            }
            credentials.put(PROPERTY_USERNAME, split[0]);
            credentials.put(PROPERTY_PASSWORD, split[1]);
            return true;
        }

        private void sendError(ChannelHandlerContext context, Object msg) {
            // Close the connection as soon as the error message is sent.
            context.writeAndFlush(new DefaultFullHttpResponse(HTTP_1_1, UNAUTHORIZED))
                   .addListener(ChannelFutureListener.CLOSE);
            ReferenceCountUtil.release(msg);
        }
    }
}
