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

package org.apache.hugegraph.pd.client;

import java.lang.reflect.Field;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.pd.grpc.PDGrpc;
import org.apache.hugegraph.pd.grpc.Pdpb;
import org.apache.hugegraph.pd.grpc.kv.KResponse;
import org.apache.hugegraph.pd.grpc.kv.KvServiceGrpc;
import org.apache.hugegraph.pd.grpc.kv.WatchResponse;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.AbstractBlockingStub;

@RunWith(Parameterized.class)
public class BlockingUnaryCallTest {

    private final boolean pdClient;
    private final int expectedAttempts;
    private final ScriptedChannel channel = new ScriptedChannel();
    private PDClient pd;
    private KvClient<WatchResponse> kv;

    public BlockingUnaryCallTest(String name, boolean pdClient, int expectedAttempts) {
        this.pdClient = pdClient;
        this.expectedAttempts = expectedAttempts;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Object[][] clients() {
        // Preserve the existing retry budgets of the two client implementations.
        return new Object[][]{{"PDClient", true, 2}, {"KvClient", false, 3}};
    }

    @Before
    public void setUp() throws Exception {
        PDConfig config = PDConfig.of("first:8686,second:8686").setAuthority("store", "secret");
        if (this.pdClient) {
            this.pd = PDClient.create(config);
            StubProxy proxy = Mockito.mock(StubProxy.class);
            Mockito.when(proxy.get()).thenReturn(PDGrpc.newBlockingStub(this.channel));
            Mockito.when(proxy.getHostCount()).thenReturn(2);
            Field field = PDClient.class.getDeclaredField("proxy");
            field.setAccessible(true);
            field.set(this.pd, proxy);
        } else {
            this.kv = new KvClient<>(config, Mockito.mock(ScheduledExecutorService.class)) {
                @Override
                protected synchronized AbstractBlockingStub getBlockingStub() {
                    return KvServiceGrpc.newBlockingStub(BlockingUnaryCallTest.this.channel);
                }
            };
        }
    }

    @After
    public void tearDown() {
        if (this.kv != null) {
            this.kv.close();
        }
    }

    @Test
    public void testSuccessfulCallReturnsResponse() throws PDException {
        this.channel.outcomes = List.of(response(Pdpb.ResponseHeader.getDefaultInstance()));

        assertSuccess();

        Assert.assertEquals(1, this.channel.calls);
    }

    @Test
    public void testStatusFailureRetriesAndCanRecover() throws PDException {
        this.channel.outcomes = List.of(Status.UNAVAILABLE.asRuntimeException(),
                                       response(Pdpb.ResponseHeader.getDefaultInstance()));

        assertSuccess();

        Assert.assertEquals(2, this.channel.calls);
    }

    @Test
    public void testExhaustedRetriesPreserveTransportFailure() {
        StatusRuntimeException transportFailure =
                Status.UNAVAILABLE.withDescription("all attempts failed").asRuntimeException();
        this.channel.outcomes = List.of(transportFailure);

        PDException failure = Assert.assertThrows(PDException.class, this::request);

        assertTransportFailure(failure, transportFailure);
        Assert.assertEquals(this.expectedAttempts, this.channel.calls);
    }

    @Test
    public void testNonStatusFailureIsNotRetried() {
        RuntimeException transportFailure = new IllegalStateException("call failed locally");
        this.channel.outcomes = List.of(transportFailure);

        PDException failure = Assert.assertThrows(PDException.class, this::request);

        assertTransportFailure(failure, transportFailure);
        Assert.assertEquals(1, this.channel.calls);
    }

    @Test
    public void testRemoteErrorKeepsItsErrorCode() {
        Pdpb.ResponseHeader header = Pdpb.ResponseHeader.newBuilder()
                                                       .setError(Pdpb.Error.newBuilder()
                                                                          .setType(Pdpb.ErrorType.NOT_FOUND)
                                                                          .setMessage("missing key"))
                                                       .build();
        this.channel.outcomes = List.of(response(header));

        PDException failure = Assert.assertThrows(PDException.class, this::request);

        Assert.assertEquals(Pdpb.ErrorType.NOT_FOUND_VALUE, failure.getErrorCode());
        Assert.assertTrue(failure.getMessage().contains("missing key"));
        Assert.assertNull(failure.getCause());
        Assert.assertEquals(1, this.channel.calls);
    }

    private Object response(Pdpb.ResponseHeader header) {
        if (this.pdClient) {
            return Pdpb.RegisterStoreResponse.newBuilder().setHeader(header).setStoreId(17L).build();
        }
        return KResponse.newBuilder().setHeader(header).setValue("value").build();
    }

    private Object request() throws PDException {
        if (this.pdClient) {
            return this.pd.registerStore(Metapb.Store.getDefaultInstance());
        }
        return this.kv.get("key");
    }

    private void assertSuccess() throws PDException {
        Object expected = this.pdClient ? 17L : response(Pdpb.ResponseHeader.getDefaultInstance());
        Assert.assertEquals(expected, request());
    }

    private void assertTransportFailure(PDException failure, Throwable cause) {
        String method = this.pdClient ? PDGrpc.getRegisterStoreMethod().getFullMethodName() :
                        KvServiceGrpc.getGetMethod().getFullMethodName();
        Assert.assertEquals(Pdpb.ErrorType.PD_UNREACHABLE_VALUE, failure.getErrorCode());
        Assert.assertSame(cause, failure.getCause());
        Assert.assertTrue(failure.getMessage().contains(method));
    }

    private static class ScriptedChannel extends Channel {

        private List<Object> outcomes;
        private int calls;

        @Override
        public String authority() {
            return "test";
        }

        @Override
        public <ReqT, RespT> ClientCall<ReqT, RespT> newCall(
                MethodDescriptor<ReqT, RespT> method, CallOptions callOptions) {
            Object outcome = this.outcomes.get(Math.min(this.calls++, this.outcomes.size() - 1));
            return new ClientCall<>() {

                private Listener<RespT> listener;

                @Override
                public void start(Listener<RespT> listener, Metadata headers) {
                    if (outcome instanceof RuntimeException failure) {
                        throw failure;
                    }
                    this.listener = listener;
                }

                @Override
                public void request(int numMessages) {
                }

                @Override
                public void cancel(String message, Throwable cause) {
                }

                @SuppressWarnings("unchecked")
                @Override
                public void halfClose() {
                    this.listener.onMessage((RespT) outcome);
                    this.listener.onClose(Status.OK, new Metadata());
                }

                @Override
                public void sendMessage(ReqT message) {
                }
            };
        }
    }
}
