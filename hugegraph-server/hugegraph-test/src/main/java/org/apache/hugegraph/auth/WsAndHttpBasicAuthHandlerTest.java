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

import static io.netty.handler.codec.http.HttpHeaderNames.AUTHORIZATION;
import static io.netty.handler.codec.http.HttpMethod.POST;
import static io.netty.handler.codec.http.HttpResponseStatus.UNAUTHORIZED;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;

import org.apache.tinkerpop.gremlin.server.Settings;
import org.apache.tinkerpop.gremlin.server.auth.AuthenticatedUser;
import org.apache.tinkerpop.gremlin.server.auth.Authenticator;
import org.apache.tinkerpop.gremlin.server.handler.StateKey;
import org.apache.hugegraph.testutil.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;

public class WsAndHttpBasicAuthHandlerTest {

    @SuppressWarnings("unchecked")
    @Test
    public void testBasicCredentialsStillAuthenticateHttpGremlinRequest()
            throws Exception {
        Authenticator authenticator = Mockito.mock(Authenticator.class);
        AuthenticatedUser user = new AuthenticatedUser("admin");
        Mockito.when(authenticator.authenticate(Mockito.anyMap()))
               .thenReturn(user);
        EmbeddedChannel channel = channel(authenticator);
        String encoded = Base64.getEncoder().encodeToString(
                "admin:password".getBytes(StandardCharsets.UTF_8));

        channel.writeInbound(request("Basic " + encoded));

        ArgumentCaptor<Map<String, String>> credentials =
                ArgumentCaptor.forClass(Map.class);
        Mockito.verify(authenticator).authenticate(credentials.capture());
        Assert.assertEquals("admin", credentials.getValue().get("username"));
        Assert.assertEquals("password", credentials.getValue().get("password"));
        Assert.assertFalse(credentials.getValue().containsKey(
                HugeAuthenticator.KEY_TOKEN));
        Assert.assertSame(user,
                          channel.attr(StateKey.AUTHENTICATED_USER).get());
        channel.finishAndReleaseAll();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testAuthorizationSchemeIsCaseInsensitive() throws Exception {
        Authenticator authenticator = Mockito.mock(Authenticator.class);
        Mockito.when(authenticator.authenticate(Mockito.anyMap()))
               .thenReturn(new AuthenticatedUser("admin"));
        EmbeddedChannel channel = channel(authenticator);

        String encoded = Base64.getEncoder().encodeToString(
                "admin:password".getBytes(StandardCharsets.UTF_8));
        channel.writeInbound(request("basic " + encoded));

        ArgumentCaptor<Map<String, String>> credentials =
                ArgumentCaptor.forClass(Map.class);
        Mockito.verify(authenticator).authenticate(credentials.capture());
        Assert.assertEquals("admin", credentials.getValue().get("username"));
        Assert.assertEquals("password", credentials.getValue().get("password"));
        channel.finishAndReleaseAll();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testBasicPasswordContainingColonAuthenticatesHttpRequest()
            throws Exception {
        Authenticator authenticator = Mockito.mock(Authenticator.class);
        Mockito.when(authenticator.authenticate(Mockito.anyMap()))
               .thenReturn(new AuthenticatedUser("admin"));
        EmbeddedChannel channel = channel(authenticator);
        String encoded = Base64.getEncoder().encodeToString(
                "admin:p:assword".getBytes(StandardCharsets.UTF_8));

        channel.writeInbound(request("Basic " + encoded));

        ArgumentCaptor<Map<String, String>> credentials =
                ArgumentCaptor.forClass(Map.class);
        Mockito.verify(authenticator).authenticate(credentials.capture());
        Assert.assertEquals("admin", credentials.getValue().get("username"));
        Assert.assertEquals("p:assword",
                            credentials.getValue().get("password"));
        channel.finishAndReleaseAll();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testBasicCredentialsUseStandardBase64Decoder()
            throws Exception {
        Authenticator authenticator = Mockito.mock(Authenticator.class);
        Mockito.when(authenticator.authenticate(Mockito.anyMap()))
               .thenReturn(new AuthenticatedUser("admin"));
        EmbeddedChannel channel = channel(authenticator);
        String password = "\uFF7F";
        String encoded = Base64.getEncoder().encodeToString(
                ("admin:" + password).getBytes(StandardCharsets.UTF_8));
        Assert.assertTrue(encoded.contains("/"));

        channel.writeInbound(request("Basic " + encoded));

        ArgumentCaptor<Map<String, String>> credentials =
                ArgumentCaptor.forClass(Map.class);
        Mockito.verify(authenticator).authenticate(credentials.capture());
        Assert.assertEquals(password,
                            credentials.getValue().get("password"));
        channel.finishAndReleaseAll();
    }

    @Test
    public void testBearerTokenIsRejected() throws Exception {
        assertUnauthorized("Bearer server-token");
    }

    @Test
    public void testUnsupportedAuthorizationSchemeIsRejected()
            throws Exception {
        assertUnauthorized("Digest server-token");
    }

    private static EmbeddedChannel channel(Authenticator authenticator) {
        WsAndHttpBasicAuthHandler handler =
                new WsAndHttpBasicAuthHandler(authenticator, new Settings());
        EmbeddedChannel channel = new EmbeddedChannel();
        channel.pipeline().addLast("authenticator", handler);
        return channel;
    }

    private static DefaultFullHttpRequest request(String authorization) {
        DefaultFullHttpRequest request =
                new DefaultFullHttpRequest(HTTP_1_1, POST, "/gremlin");
        request.headers().set(AUTHORIZATION, authorization);
        return request;
    }

    private static void assertUnauthorized(String authorization)
            throws Exception {
        Authenticator authenticator = Mockito.mock(Authenticator.class);
        EmbeddedChannel channel = channel(authenticator);

        channel.writeInbound(request(authorization));

        FullHttpResponse response = channel.readOutbound();
        Assert.assertEquals(UNAUTHORIZED, response.status());
        Mockito.verifyNoInteractions(authenticator);
        response.release();
        channel.finishAndReleaseAll();
    }
}
