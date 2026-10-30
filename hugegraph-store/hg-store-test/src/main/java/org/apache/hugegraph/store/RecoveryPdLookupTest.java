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

package org.apache.hugegraph.store;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import org.apache.hugegraph.pd.client.ClientCache;
import org.apache.hugegraph.pd.client.PDClient;
import org.apache.hugegraph.pd.client.PDConfig;
import org.apache.hugegraph.pd.client.StubProxy;
import org.apache.hugegraph.pd.client.interceptor.Authentication;
import org.apache.hugegraph.pd.common.KVPair;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.pd.grpc.PDGrpc;
import org.apache.hugegraph.pd.grpc.Pdpb;
import org.junit.Test;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;

public class RecoveryPdLookupTest {

    @Test
    public void testDirectMissingPartitionDoesNotUseCachedPartition() throws Exception {
        Lookup lookup = new Lookup();
        when(lookup.cache.getPartitionById("graph", 0)).thenReturn(new KVPair<>(
                Metapb.Partition.newBuilder().setGraphName("graph").build(),
                Metapb.Shard.getDefaultInstance()));
        lookup.response = Pdpb.GetPartitionResponse.newBuilder().setHeader(
                Pdpb.ResponseHeader.newBuilder().setError(
                        Pdpb.Error.newBuilder().setType(Pdpb.ErrorType.NOT_FOUND))).build();
        PDException missing = assertThrows(PDException.class,
                                          () -> lookup.client.getPartitionByIdDirect("graph", 0));
        assertEquals(Pdpb.ErrorType.NOT_FOUND_VALUE, missing.getErrorCode());
        assertEquals(1, lookup.requests.size());
        assertEquals("graph", lookup.requests.get(0).getGraphName());
        assertEquals(0, lookup.requests.get(0).getPartitionId());
        verifyNoInteractions(lookup.cache);
    }

    @Test
    public void testDirectPartitionDoesNotQueryOrPopulateShardGroupCache() throws Exception {
        Lookup lookup = new Lookup();
        KVPair<Metapb.Partition, Metapb.Shard> pair = lookup.client.getPartitionByIdDirect("graph", 0);
        assertEquals(lookup.response.getPartition(), pair.getKey());
        assertEquals(1, lookup.requests.size());
        verifyNoInteractions(lookup.cache);
    }

    @Test
    public void testFailedRpcRemainsFailureAndCanBeRetried() throws Exception {
        Lookup lookup = new Lookup();
        lookup.status = Status.UNAVAILABLE.withDescription("PD connection failed");
        PDException failure = assertThrows(PDException.class,
                                          () -> lookup.client.getPartitionByIdDirect("graph", 0));
        assertEquals(Pdpb.ErrorType.PD_UNREACHABLE_VALUE, failure.getErrorCode());
        lookup.status = Status.OK;
        assertEquals("graph", lookup.client.getPartitionByIdDirect("graph", 0).getKey().getGraphName());
        assertEquals(2, lookup.requests.size());
        verifyNoInteractions(lookup.cache);
    }

    private static final class Lookup extends Channel {

        private final PDClient client = mock(PDClient.class, CALLS_REAL_METHODS);
        private final ClientCache cache = mock(ClientCache.class);
        private final List<Pdpb.GetPartitionByIDRequest> requests = new ArrayList<>();
        private Pdpb.GetPartitionResponse response = Pdpb.GetPartitionResponse.newBuilder()
                .setPartition(Metapb.Partition.newBuilder().setGraphName("graph")).build();
        private Status status = Status.OK;

        private Lookup() throws Exception {
            StubProxy proxy = new StubProxy(new String[]{"unused"});
            proxy.set(PDGrpc.newBlockingStub(this));
            setField("proxy", proxy);
            setField("config", PDConfig.of("unused").setEnableCache(true));
            setField("header", Pdpb.RequestHeader.getDefaultInstance());
            setField("cache", cache);
            setField("auth", new Authentication("store", "test"));
        }

        private void setField(String name, Object value) throws Exception {
            Field field = PDClient.class.getDeclaredField(name);
            field.setAccessible(true);
            field.set(client, value);
        }

        @Override
        public String authority() {
            return "recovery-test";
        }

        @Override
        public <ReqT, RespT> ClientCall<ReqT, RespT> newCall(MethodDescriptor<ReqT, RespT> method,
                                                          CallOptions options) {
            assertEquals(PDGrpc.getGetPartitionByIDMethod(), method);
            return new ClientCall<>() {
                private Listener<RespT> listener;

                @Override
                public void start(Listener<RespT> listener, Metadata headers) {
                    this.listener = listener;
                }

                @Override
                public void request(int numMessages) {
                }

                @Override
                public void cancel(String message, Throwable cause) {
                }

                @Override
                @SuppressWarnings("unchecked")
                public void halfClose() {
                    if (status.isOk()) {
                        listener.onMessage((RespT) response);
                    }
                    listener.onClose(status, new Metadata());
                }

                @Override
                public void sendMessage(ReqT request) {
                    requests.add((Pdpb.GetPartitionByIDRequest) request);
                }
            };
        }
    }
}
