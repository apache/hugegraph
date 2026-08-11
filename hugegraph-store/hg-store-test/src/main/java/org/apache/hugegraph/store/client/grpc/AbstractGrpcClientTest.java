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

package org.apache.hugegraph.store.client.grpc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ManagedChannel;
import io.grpc.MethodDescriptor;
import io.grpc.stub.AbstractAsyncStub;
import io.grpc.stub.AbstractBlockingStub;

/**
 * Verifies that the stub pools of {@link AbstractGrpcClient} spread their entries over every
 * channel created for a target, instead of binding all of them to a single channel.
 */
public class AbstractGrpcClientTest {

    private static final AtomicInteger TARGET_SEQ = new AtomicInteger();

    private static String uniqueTarget(String prefix) {
        return prefix + "-" + TARGET_SEQ.incrementAndGet() + ":8500";
    }

    private static Set<ManagedChannel> identitySet(Collection<ManagedChannel> channels) {
        Set<ManagedChannel> set = Collections.newSetFromMap(new IdentityHashMap<>());
        set.addAll(channels);
        return set;
    }

    @Test
    public void testBlockingStubPoolCoversEveryChannel() {
        String target = uniqueTarget("blocking");
        RecordingGrpcClient client = new RecordingGrpcClient();
        ManagedChannel[] channels = client.getChannels(target);
        assertTrue("pool must hold more than one channel", channels.length > 1);

        // Pool initialisation: one stub per channel, each bound to a different channel.
        assertNotNull(client.getBlockingStub(target));
        assertEquals("one stub per channel", channels.length, client.blockingStubChannels.size());
        Set<ManagedChannel> bound = identitySet(client.blockingStubChannels);
        assertEquals("stubs must not share a channel", channels.length, bound.size());
        assertTrue("stubs must cover the channels of the target",
                   bound.containsAll(Arrays.asList(channels)));
    }

    @Test
    public void testAsyncStubPoolCoversEveryChannel() {
        String target = uniqueTarget("async");
        RecordingGrpcClient client = new RecordingGrpcClient();
        ManagedChannel[] channels = client.getChannels(target);
        assertTrue("pool must hold more than one channel", channels.length > 1);

        assertNotNull(client.getAsyncStub(target));
        assertEquals("one stub per channel", channels.length, client.asyncStubChannels.size());
        Set<ManagedChannel> bound = identitySet(client.asyncStubChannels);
        assertEquals("stubs must not share a channel", channels.length, bound.size());
        assertTrue("stubs must cover the channels of the target",
                   bound.containsAll(Arrays.asList(channels)));
    }

    /**
     * A client whose channels and stubs are local fakes, so the test needs no PD or store node.
     */
    private static class RecordingGrpcClient extends AbstractGrpcClient {

        private final AtomicInteger channelSeq = new AtomicInteger();
        private final List<ManagedChannel> blockingStubChannels =
                Collections.synchronizedList(new ArrayList<>());
        private final List<ManagedChannel> asyncStubChannels =
                Collections.synchronizedList(new ArrayList<>());

        @Override
        protected ManagedChannel createChannel(String target) {
            return new FakeManagedChannel(target + "#" + channelSeq.getAndIncrement());
        }

        @Override
        public AbstractBlockingStub getBlockingStub(ManagedChannel channel) {
            this.blockingStubChannels.add(channel);
            return new FakeBlockingStub(channel, CallOptions.DEFAULT);
        }

        @Override
        public AbstractAsyncStub getAsyncStub(ManagedChannel channel) {
            this.asyncStubChannels.add(channel);
            return new FakeAsyncStub(channel, CallOptions.DEFAULT);
        }
    }

    private static class FakeBlockingStub extends AbstractBlockingStub<FakeBlockingStub> {

        FakeBlockingStub(Channel channel, CallOptions callOptions) {
            super(channel, callOptions);
        }

        @Override
        protected FakeBlockingStub build(Channel channel, CallOptions callOptions) {
            return new FakeBlockingStub(channel, callOptions);
        }
    }

    private static class FakeAsyncStub extends AbstractAsyncStub<FakeAsyncStub> {

        FakeAsyncStub(Channel channel, CallOptions callOptions) {
            super(channel, callOptions);
        }

        @Override
        protected FakeAsyncStub build(Channel channel, CallOptions callOptions) {
            return new FakeAsyncStub(channel, callOptions);
        }
    }

    private static class FakeManagedChannel extends ManagedChannel {

        private final String authority;
        private volatile boolean shutdown;

        FakeManagedChannel(String authority) {
            this.authority = authority;
        }

        @Override
        public String authority() {
            return this.authority;
        }

        @Override
        public <Q, S> ClientCall<Q, S> newCall(MethodDescriptor<Q, S> method,
                                               CallOptions callOptions) {
            throw new UnsupportedOperationException("no call is issued by this test");
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
    }
}
