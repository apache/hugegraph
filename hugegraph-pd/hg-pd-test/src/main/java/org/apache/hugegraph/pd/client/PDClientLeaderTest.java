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
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.pd.grpc.PDGrpc;
import org.apache.hugegraph.pd.grpc.Pdpb;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import io.grpc.CallOptions;
import io.grpc.ClientCall;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;

public class PDClientLeaderTest {

    private final String prefix = "pd-leader-test-" + UUID.randomUUID();
    private final List<TestChannel> peers = new ArrayList<>();
    private ConcurrentMap<String, ManagedChannel> channels;
    private ManagedChannel previousChannel;

    @SuppressWarnings("unchecked")
    @Before
    public void setUp() throws Exception {
        this.channels = (ConcurrentMap<String, ManagedChannel>) field("channels").get(null);
        this.previousChannel = (ManagedChannel) field("channel").get(null);
    }

    @After
    public void tearDown() throws Exception {
        for (TestChannel peer : this.peers) {
            this.channels.remove(peer.address, peer);
            peer.shutdownNow();
        }
        field("channel").set(null, this.previousChannel);
    }

    @Test
    public void testRegistrationUsesDiscoveredLeader() throws Exception {
        TestChannel leader = addPeer("leader");
        TestChannel follower = addPeer("follower");
        follower.leaderAddress = leader.address;
        PDClient client = newClient(follower.address, leader.address);

        Assert.assertEquals(17L, client.registerStore(Metapb.Store.getDefaultInstance()));

        Assert.assertEquals(1, follower.discoveryCalls);
        Assert.assertEquals(0, follower.registrationCalls);
        Assert.assertEquals(1, leader.registrationCalls);
    }

    @Test
    public void testRegistrationReusesPeerWhenAlreadyLeader() throws Exception {
        TestChannel leader = addPeer("leader");
        PDClient client = newClient(leader.address, leader.address);

        Assert.assertEquals(17L, client.registerStore(Metapb.Store.getDefaultInstance()));

        Assert.assertEquals(1, leader.discoveryCalls);
        Assert.assertEquals(1, leader.registrationCalls);
    }

    @Test
    public void testDiscoveryFailureTriesNextPeer() throws Exception {
        TestChannel unavailable = addPeer("unavailable");
        unavailable.failDiscovery = true;
        TestChannel leader = addPeer("leader");
        PDClient client = newClient(unavailable.address + "," + leader.address, leader.address);

        Assert.assertEquals(17L, client.registerStore(Metapb.Store.getDefaultInstance()));

        Assert.assertEquals(1, unavailable.discoveryCalls);
        Assert.assertEquals(0, unavailable.registrationCalls);
        Assert.assertEquals(1, leader.discoveryCalls);
        Assert.assertEquals(1, leader.registrationCalls);
    }

    private TestChannel addPeer(String name) {
        TestChannel channel = new TestChannel(this.prefix + "-" + name + ":8686");
        Assert.assertNull(this.channels.putIfAbsent(channel.address, channel));
        this.peers.add(channel);
        return channel;
    }

    private PDClient newClient(String hosts, String leader) throws Exception {
        PDClient client = PDClient.create(PDConfig.of(hosts).setAuthority("store", "secret"));
        // Isolate unrelated watch streams; discovery and registration still use the real client path.
        PDWatch watch = Mockito.mock(PDWatch.class);
        Mockito.when(watch.getCurrentHost()).thenReturn(leader);
        Mockito.when(watch.checkChannel()).thenReturn(true);
        field("pdWatch").set(client, watch);
        return client;
    }

    private static Field field(String name) throws NoSuchFieldException {
        Field field = PDClient.class.getDeclaredField(name);
        field.setAccessible(true);
        return field;
    }

    private static class TestChannel extends ManagedChannel {

        private final String address;
        private String leaderAddress;
        private boolean failDiscovery;
        private boolean shutdown;
        private int discoveryCalls;
        private int registrationCalls;

        TestChannel(String address) {
            this.address = address;
            this.leaderAddress = address;
        }

        @Override
        public ManagedChannel shutdown() {
            this.shutdown = true;
            return this;
        }

        @Override
        public ManagedChannel shutdownNow() {
            return shutdown();
        }

        @Override
        public boolean isShutdown() {
            return this.shutdown;
        }

        @Override
        public boolean isTerminated() {
            return this.shutdown;
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) {
            return this.shutdown;
        }

        @Override
        public String authority() {
            return this.address;
        }

        @Override
        public <ReqT, RespT> ClientCall<ReqT, RespT> newCall(
                MethodDescriptor<ReqT, RespT> method, CallOptions callOptions) {
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

                @SuppressWarnings("unchecked")
                @Override
                public void halfClose() {
                    Object response;
                    if (method.getFullMethodName().equals(PDGrpc.getGetMembersMethod().getFullMethodName())) {
                        discoveryCalls++;
                        if (failDiscovery) {
                            this.listener.onClose(Status.UNAVAILABLE, new Metadata());
                            return;
                        }
                        response = Pdpb.GetMembersResponse.newBuilder()
                                                         .setLeader(Metapb.Member.newBuilder()
                                                                                .setGrpcUrl(leaderAddress))
                                                         .build();
                    } else {
                        Assert.assertEquals(PDGrpc.getRegisterStoreMethod().getFullMethodName(),
                                            method.getFullMethodName());
                        registrationCalls++;
                        response = Pdpb.RegisterStoreResponse.newBuilder().setStoreId(17L).build();
                    }
                    this.listener.onMessage((RespT) response);
                    this.listener.onClose(Status.OK, new Metadata());
                }

                @Override
                public void sendMessage(ReqT message) {
                }
            };
        }
    }
}
