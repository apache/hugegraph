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

package org.apache.hugegraph.pd.service;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hugegraph.pd.common.Consts;
import org.apache.hugegraph.pd.config.PDConfig;
import org.apache.hugegraph.pd.grpc.PDGrpc;
import org.apache.hugegraph.pd.grpc.Pdpb;
import org.apache.hugegraph.pd.service.interceptor.StoreShutdownAuthentication;
import org.apache.hugegraph.testutil.Whitebox;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;

public class StoreShutdownAuthenticationTest {

    private PDConfig config;
    private Server server;
    private ManagedChannel channel;
    private final AtomicInteger shutdownCalls = new AtomicInteger();

    @Before
    public void setUp() throws Exception {
        config = Mockito.mock(PDConfig.class);
        Mockito.when(config.getSecretKey()).thenReturn("test-only-secret");
        StoreShutdownAuthentication authentication = new StoreShutdownAuthentication();
        Whitebox.setInternalState(authentication, "pdConfig", config);
        server = ServerBuilder.forPort(0).intercept(authentication).addService(new PDGrpc.PDImplBase() {
            @Override
            public void reportStoreShutdown(Pdpb.SetStoreRequest request,
                                            StreamObserver<Pdpb.SetStoreResponse> observer) {
                shutdownCalls.incrementAndGet();
                observer.onNext(Pdpb.SetStoreResponse.getDefaultInstance());
                observer.onCompleted();
            }

            @Override
            public void registerStore(Pdpb.RegisterStoreRequest request,
                                      StreamObserver<Pdpb.RegisterStoreResponse> observer) {
                observer.onNext(Pdpb.RegisterStoreResponse.getDefaultInstance());
                observer.onCompleted();
            }
        }).build().start();
        channel = ManagedChannelBuilder.forAddress("127.0.0.1", server.getPort()).usePlaintext().build();
    }

    @After
    public void tearDown() throws Exception {
        if (channel != null) {
            channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }
        if (server != null) {
            server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testMissingCredentialIsRefused() {
        assertRefused(null);
    }

    @Test
    public void testWrongCredentialIsRefused() {
        assertRefused(credential("store:wrong"));
        assertRefused("not-base64!");
        assertRefused(credential("external:test-only-secret"));
    }

    @Test
    public void testMissingConfiguredSecretFailsClosed() {
        Mockito.when(config.getSecretKey()).thenReturn("");
        assertRefused(credential("store:test-only-secret"));
    }

    @Test
    public void testPrivilegedInternalCredentialPermitsReport() {
        stub(credential("store:test-only-secret")).reportStoreShutdown(Pdpb.SetStoreRequest.getDefaultInstance());
        Assert.assertEquals(1, shutdownCalls.get());
    }

    @Test
    public void testExistingRegistrationRemainsCompatible() {
        stub(null).registerStore(Pdpb.RegisterStoreRequest.getDefaultInstance());
        Assert.assertEquals(0, shutdownCalls.get());
    }

    private void assertRefused(String credential) {
        StatusRuntimeException failure = Assert.assertThrows(StatusRuntimeException.class,
                () -> stub(credential).reportStoreShutdown(Pdpb.SetStoreRequest.getDefaultInstance()));
        Assert.assertEquals(Status.Code.UNAUTHENTICATED, failure.getStatus().getCode());
        Assert.assertEquals(0, shutdownCalls.get());
    }

    private PDGrpc.PDBlockingStub stub(String credential) {
        Metadata headers = new Metadata();
        if (credential != null) {
            headers.put(Consts.CREDENTIAL_KEY, credential);
        }
        return PDGrpc.newBlockingStub(channel).withDeadlineAfter(5, TimeUnit.SECONDS)
                     .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers));
    }

    private static String credential(String value) {
        return Base64.getEncoder().encodeToString(value.getBytes(StandardCharsets.UTF_8));
    }
}
