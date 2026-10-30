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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ManagedChannel;
import io.grpc.MethodDescriptor;
import io.grpc.stub.AbstractBlockingStub;

public class AbstractGrpcClientErrorTest {

    @Test
    public void testErrorClosesPartialPoolAndCanRetry() {
        String target = "construction-error-" + System.nanoTime() + ":8500";
        AtomicInteger count = new AtomicInteger();
        List<ManagedChannel> created = new ArrayList<>();
        AssertionError failure = new AssertionError("channel linkage failed");
        AbstractGrpcClient client = new AbstractGrpcClient() {
            @Override
            protected ManagedChannel createChannel(String address) {
                if (count.getAndIncrement() == 2) {
                    throw failure;
                }
                ManagedChannel channel = new FakeManagedChannel(address);
                created.add(channel);
                return channel;
            }

            @Override
            public AbstractBlockingStub getBlockingStub(ManagedChannel channel) {
                return new FakeBlockingStub(channel, CallOptions.DEFAULT);
            }
        };
        try {
            try {
                client.getChannels(target);
                fail("Channel construction Error must be reported");
            } catch (AssertionError error) {
                assertSame(failure, error);
            }
            assertFalse(AbstractGrpcClient.channels.containsKey(target));
            assertEquals(2, created.size());
            assertTrue(created.stream().allMatch(ManagedChannel::isShutdown));

            ManagedChannel[] retry = client.getChannels(target);
            assertSame(retry, AbstractGrpcClient.channels.get(target));
            assertTrue(Arrays.stream(retry)
                             .allMatch(channel -> channel != null && !channel.isShutdown()));
        } finally {
            AbstractGrpcClient.closeChannel(target);
        }
    }

    private static final class FakeBlockingStub
            extends AbstractBlockingStub<FakeBlockingStub> {

        private FakeBlockingStub(Channel channel, CallOptions callOptions) {
            super(channel, callOptions);
        }

        @Override
        protected FakeBlockingStub build(Channel channel, CallOptions callOptions) {
            return new FakeBlockingStub(channel, callOptions);
        }
    }

    private static final class FakeManagedChannel extends ManagedChannel {

        private final String authority;
        private volatile boolean shutdown;

        private FakeManagedChannel(String authority) {
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
            return this.shutdown();
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
