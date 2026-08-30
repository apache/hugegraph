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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hugegraph.store.HgStoreSession;
import org.apache.hugegraph.store.client.HgStoreNode;
import org.apache.hugegraph.store.client.HgStoreNodeManager;
import org.apache.hugegraph.store.client.HgStoreNodeSession;
import org.apache.hugegraph.store.client.HgStoreNotice;
import org.apache.hugegraph.store.client.type.HgNodeStatus;
import org.apache.hugegraph.store.client.type.HgStoreClientException;
import org.junit.Test;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ManagedChannel;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import io.grpc.stub.AbstractAsyncStub;
import io.grpc.stub.AbstractBlockingStub;

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

        assertNotNull(client.getBlockingStub(target));
        assertEquals("one stub per channel", channels.length,
                     client.blockingStubChannels.size());
        Set<ManagedChannel> bound = identitySet(client.blockingStubChannels);
        assertEquals("stubs must not share a channel", channels.length, bound.size());
        assertTrue("stubs must cover the channels of the target",
                   bound.containsAll(Arrays.asList(channels)));
        AbstractGrpcClient.closeChannel(target);
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
        AbstractGrpcClient.closeChannel(target);
    }

    @Test
    public void testClosedTargetRebindsStubPools() {
        String target = uniqueTarget("rebind");
        RecordingGrpcClient client = new RecordingGrpcClient();
        ManagedChannel[] oldChannels = client.getChannels(target);
        client.getBlockingStub(target);
        client.getAsyncStub(target);
        int oldBlockingCount = client.blockingStubChannels.size();
        int oldAsyncCount = client.asyncStubChannels.size();

        AbstractGrpcClient.closeChannel(target);
        assertTrue(Arrays.stream(oldChannels).allMatch(ManagedChannel::isShutdown));

        ManagedChannel[] currentChannels = client.getChannels(target);
        assertNotSame(oldChannels, currentChannels);
        client.getBlockingStub(target);
        client.getAsyncStub(target);

        List<ManagedChannel> currentBlocking =
                client.blockingStubChannels.subList(oldBlockingCount,
                                                    client.blockingStubChannels.size());
        List<ManagedChannel> currentAsync =
                client.asyncStubChannels.subList(oldAsyncCount,
                                                 client.asyncStubChannels.size());
        assertEquals(identitySet(Arrays.asList(currentChannels)),
                     identitySet(currentBlocking));
        assertEquals(identitySet(Arrays.asList(currentChannels)), identitySet(currentAsync));
        Set<ManagedChannel> retired = identitySet(Arrays.asList(oldChannels));
        assertFalse(currentBlocking.stream().anyMatch(retired::contains));
        assertFalse(currentAsync.stream().anyMatch(retired::contains));
        AbstractGrpcClient.closeChannel(target);
    }

    @Test
    public void testCloseDuringStubBuildDoesNotPublishRetiredChannel() throws Exception {
        String target = uniqueTarget("concurrent-rebind");
        PausingGrpcClient client = new PausingGrpcClient();
        client.getBlockingStub(target);
        AbstractGrpcClient.closeChannel(target);

        ExecutorService executor = Executors.newSingleThreadExecutor();
        client.pauseStubCreation = true;
        Future<AbstractBlockingStub> future = executor.submit(
                () -> client.getBlockingStub(target));
        try {
            assertTrue(client.stubCreationStarted.await(5, TimeUnit.SECONDS));
            ManagedChannel[] retired = AbstractGrpcClient.channels.get(target);
            assertNotNull(retired);

            AbstractGrpcClient.closeChannel(target);
            ManagedChannel[] current = client.getChannels(target);
            client.releaseStubCreation.countDown();

            AbstractBlockingStub stub = future.get(5, TimeUnit.SECONDS);
            assertTrue(identitySet(Arrays.asList(current)).contains(stub.getChannel()));
            assertFalse(((ManagedChannel) stub.getChannel()).isShutdown());
        } finally {
            client.releaseStubCreation.countDown();
            executor.shutdownNow();
            AbstractGrpcClient.closeChannel(target);
        }
    }

    @Test
    public void testUnavailableRpcEvictsExpectedNodeAndTarget() {
        String graph = uniqueTarget("terminal-graph");
        String target = uniqueTarget("terminal-node");
        long nodeId = TARGET_SEQ.incrementAndGet();
        RecordingGrpcClient client = new RecordingGrpcClient();
        HgStoreNodeManager manager = HgStoreNodeManager.getInstance();
        HgStoreNode node = testNode(nodeId, target);
        HgStoreNodeSession session = mock(HgStoreNodeSession.class);
        when(session.getStoreNode()).thenReturn(node);

        manager.addNode(graph, node);
        ManagedChannel[] published = client.getChannels(target);
        NotifyingExecutor notifier = new NotifyingExecutor(graph, manager, session);
        try {
            notifier.invoke(() -> {
                throw Status.UNAVAILABLE.asRuntimeException();
            }, response -> true);
            fail("the unavailable RPC must still be reported to the caller");
        } catch (HgStoreClientException ignored) {
            // Expected.
        }

        assertNull(manager.getStoreNode(nodeId));
        assertFalse(AbstractGrpcClient.channels.containsKey(target));
        assertTrue(Arrays.stream(published).allMatch(ManagedChannel::isShutdown));
    }

    @Test
    public void testStaleNoticePreservesSameAddressReplacement() {
        String graph = uniqueTarget("replacement-graph");
        String target = uniqueTarget("replacement-node");
        long nodeId = TARGET_SEQ.incrementAndGet();
        RecordingGrpcClient client = new RecordingGrpcClient();
        HgStoreNodeManager manager = HgStoreNodeManager.getInstance();
        HgStoreNode staleNode = testNode(nodeId, target);
        HgStoreNode replacementNode = testNode(nodeId, target);

        manager.addNode(graph, staleNode);
        manager.addNode(graph, replacementNode);
        ManagedChannel[] published = client.getChannels(target);
        manager.notifying(graph, HgStoreNotice.of(nodeId, HgNodeStatus.NOT_WORK), staleNode);

        assertSame(replacementNode, manager.getStoreNode(nodeId));
        assertSame(published, AbstractGrpcClient.channels.get(target));
        assertTrue(Arrays.stream(published).noneMatch(ManagedChannel::isShutdown));

        manager.notifying(graph, HgStoreNotice.of(nodeId, HgNodeStatus.NOT_WORK),
                          replacementNode);
    }

    @Test
    public void testCancelledRpcDoesNotEvictCurrentNode() {
        String graph = uniqueTarget("cancelled-graph");
        String target = uniqueTarget("cancelled-node");
        long nodeId = TARGET_SEQ.incrementAndGet();
        RecordingGrpcClient client = new RecordingGrpcClient();
        HgStoreNodeManager manager = HgStoreNodeManager.getInstance();
        HgStoreNode node = testNode(nodeId, target);
        HgStoreNodeSession session = mock(HgStoreNodeSession.class);
        when(session.getStoreNode()).thenReturn(node);

        manager.addNode(graph, node);
        ManagedChannel[] published = client.getChannels(target);
        NotifyingExecutor notifier = new NotifyingExecutor(graph, manager, session);
        try {
            notifier.invoke(() -> {
                throw Status.CANCELLED.asRuntimeException();
            }, response -> true);
            fail("the cancelled RPC must still be reported to the caller");
        } catch (HgStoreClientException ignored) {
            // Expected.
        }

        assertSame(node, manager.getStoreNode(nodeId));
        assertSame(published, AbstractGrpcClient.channels.get(target));
        assertTrue(Arrays.stream(published).noneMatch(ManagedChannel::isShutdown));

        manager.notifying(graph, HgStoreNotice.of(nodeId, HgNodeStatus.NOT_WORK), node);
    }

    private static HgStoreNode testNode(long nodeId, String address) {
        return new HgStoreNode() {
            @Override
            public Long getNodeId() {
                return nodeId;
            }

            @Override
            public String getAddress() {
                return address;
            }

            @Override
            public HgStoreSession openSession(String graphName) {
                return null;
            }
        };
    }

    private static class RecordingGrpcClient extends AbstractGrpcClient {

        private final AtomicInteger channelSeq = new AtomicInteger();
        private final List<ManagedChannel> blockingStubChannels =
                Collections.synchronizedList(new ArrayList<>());
        private final List<ManagedChannel> asyncStubChannels =
                Collections.synchronizedList(new ArrayList<>());

        @Override
        public ManagedChannel[] getChannels(String target) {
            synchronized (channels) {
                ManagedChannel[] current = channels.get(target);
                if (current == null) {
                    current = new ManagedChannel[concurrency];
                    for (int i = 0; i < current.length; i++) {
                        current[i] = this.createChannel(target);
                    }
                    channels.put(target, current);
                }
                return current;
            }
        }

        @Override
        protected ManagedChannel createChannel(String target) {
            return new FakeManagedChannel(target + "#" + this.channelSeq.getAndIncrement());
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

    private static class PausingGrpcClient extends RecordingGrpcClient {

        private final AtomicBoolean paused = new AtomicBoolean();
        private final CountDownLatch stubCreationStarted = new CountDownLatch(1);
        private final CountDownLatch releaseStubCreation = new CountDownLatch(1);
        private volatile boolean pauseStubCreation;

        @Override
        public AbstractBlockingStub getBlockingStub(ManagedChannel channel) {
            if (this.pauseStubCreation && this.paused.compareAndSet(false, true)) {
                this.stubCreationStarted.countDown();
                try {
                    this.releaseStubCreation.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            }
            return super.getBlockingStub(channel);
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
