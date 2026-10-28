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
import java.util.Arrays;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import org.apache.hugegraph.pd.client.listener.PDEventListener;
import org.apache.hugegraph.pd.common.KVPair;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.pd.grpc.watch.HgPdWatchGrpc;
import org.apache.hugegraph.pd.grpc.watch.NodeEventType;
import org.apache.hugegraph.pd.grpc.watch.WatchChangeType;
import org.apache.hugegraph.pd.grpc.watch.WatchGraphResponse;
import org.apache.hugegraph.pd.grpc.watch.WatchNodeResponse;
import org.apache.hugegraph.pd.grpc.watch.WatchPartitionResponse;
import org.apache.hugegraph.pd.grpc.watch.WatchRequest;
import org.apache.hugegraph.pd.grpc.watch.WatchResponse;
import org.apache.hugegraph.pd.grpc.watch.WatchShardGroupResponse;
import org.apache.hugegraph.pd.grpc.watch.WatchType;
import org.apache.hugegraph.pd.watch.NodeEvent;
import org.apache.hugegraph.pd.watch.PartitionEvent;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

import io.grpc.CallOptions;
import io.grpc.ClientCall;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;

@RunWith(Parameterized.class)
public class PDClientWatchDispatchTest {

    private static final String GRAPH = "test-graph";
    private static final int PARTITION_ID = 3;
    private static final long STORE_ID = 7L;
    private static final Metapb.ShardGroup SHARD_GROUP =
            Metapb.ShardGroup.newBuilder().setId(PARTITION_ID).build();

    private final WatchType watchType;
    private final String address = "pd-watch-test-" + UUID.randomUUID() + ":8686";
    private final List<String> events = new ArrayList<>();
    private final WatchChannel channel = new WatchChannel();
    private ConcurrentMap<String, ManagedChannel> channels;
    private ClientCache cache;
    private PDClient client;
    private Thread dispatchThread;

    public PDClientWatchDispatchTest(WatchType watchType) {
        this.watchType = watchType;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Object[][] watchTypes() {
        return new Object[][]{
                {WatchType.WATCH_TYPE_PARTITION_CHANGE},
                {WatchType.WATCH_TYPE_STORE_NODE_CHANGE},
                {WatchType.WATCH_TYPE_GRAPH_CHANGE},
                {WatchType.WATCH_TYPE_SHARD_GROUP_CHANGE}
        };
    }

    @SuppressWarnings("unchecked")
    @Before
    public void setUp() throws Exception {
        Field channelsField = Channels.class.getDeclaredField("chs");
        channelsField.setAccessible(true);
        this.channels = (ConcurrentMap<String, ManagedChannel>) channelsField.get(null);
        Assert.assertNull(this.channels.putIfAbsent(this.address, this.channel));
        this.client = PDClient.create(PDConfig.of(this.address).setEnableCache(true));
        this.cache = Mockito.mock(ClientCache.class);
        Field cacheField = PDClient.class.getDeclaredField("cache");
        cacheField.setAccessible(true);
        cacheField.set(this.client, this.cache);
        prepareCache();
        this.dispatchThread = Thread.currentThread();

        // Exercise the public setup and the real PDWatchImpl response adapters.
        this.client.startWatch(this.address);
        Assert.assertEquals(4, this.channel.listeners.size());
    }

    @After
    public void tearDown() {
        this.channels.remove(this.address, this.channel);
        this.channel.shutdownNow();
    }

    @Test
    public void testRuntimeFailureDoesNotStopOtherListenersOrLaterFrames() {
        RuntimeException failure = new IllegalStateException("listener failed");
        this.client.addEventListener(listener("first", () -> { throw failure; }));
        this.client.addEventListener(listener("second", () -> { }));

        emit();
        emit();

        List<String> expected = expectedEvents("first", "second");
        expected.addAll(expectedEvents("first", "second"));
        Assert.assertEquals(expected, this.events);
        Assert.assertEquals(4, this.channel.listeners.size());
    }

    @Test
    public void testErrorPropagatesWithoutCallingLaterListeners() {
        AssertionError failure = new AssertionError("fatal listener failure");
        this.client.addEventListener(listener("first", () -> { throw failure; }));
        this.client.addEventListener(listener("second", () -> { }));

        Assert.assertSame(failure, Assert.assertThrows(AssertionError.class, this::emit));

        Assert.assertEquals(expectedEvents("first"), this.events);
    }

    @Test
    public void testInternalCacheBoundaryIsPreserved() {
        RuntimeException failure = new IllegalStateException("cache update failed");
        this.client.addEventListener(listener("listener", () -> { }));
        switch (this.watchType) {
            case WATCH_TYPE_PARTITION_CHANGE ->
                    Mockito.doThrow(failure).when(this.cache).getPartitionById(GRAPH, PARTITION_ID);
            case WATCH_TYPE_STORE_NODE_CHANGE ->
                    Mockito.doThrow(failure).when(this.cache).removeStore(STORE_ID);
            case WATCH_TYPE_SHARD_GROUP_CHANGE ->
                    Mockito.doThrow(failure).when(this.cache).updateShardGroup(SHARD_GROUP);
            case WATCH_TYPE_GRAPH_CHANGE -> {
                emit();
                Assert.assertEquals(List.of("listener"), this.events);
                Mockito.verifyNoInteractions(this.cache);
                return;
            }
            default -> throw new AssertionError(this.watchType);
        }
        Assert.assertSame(failure, Assert.assertThrows(IllegalStateException.class, this::emit));

        Assert.assertTrue(this.events.isEmpty());
    }

    private void prepareCache() {
        Mockito.when(this.cache.getPartitionById(GRAPH, PARTITION_ID)).thenAnswer(invocation -> {
            this.events.add("lookup-partition");
            return new KVPair<>(Metapb.Partition.newBuilder().setId(PARTITION_ID).build(),
                                Metapb.Shard.getDefaultInstance());
        });
        Mockito.doAnswer(invocation -> {
            this.events.add("remove-partition");
            return null;
        }).when(this.cache).removePartition(GRAPH, PARTITION_ID);
        Mockito.doAnswer(invocation -> {
            this.events.add("remove-graph");
            return null;
        }).when(this.cache).removeAll(GRAPH);
        Mockito.doAnswer(invocation -> {
            this.events.add("remove-store");
            return null;
        }).when(this.cache).removeStore(STORE_ID);
        Mockito.when(this.cache.updateShardGroup(SHARD_GROUP)).thenAnswer(invocation -> {
            this.events.add("update-shard-group");
            return true;
        });
    }

    private PDEventListener listener(String name, Runnable callback) {
        return new PDEventListener() {
            private void accept(WatchType type) {
                Assert.assertSame(watchType, type);
                Assert.assertSame(dispatchThread, Thread.currentThread());
                events.add(name);
                callback.run();
            }

            @Override
            public void onStoreChanged(NodeEvent event) {
                accept(WatchType.WATCH_TYPE_STORE_NODE_CHANGE);
            }

            @Override
            public void onPartitionChanged(PartitionEvent event) {
                accept(WatchType.WATCH_TYPE_PARTITION_CHANGE);
            }

            @Override
            public void onGraphChanged(WatchResponse event) {
                accept(WatchType.WATCH_TYPE_GRAPH_CHANGE);
            }

            @Override
            public void onShardGroupChanged(WatchResponse event) {
                accept(WatchType.WATCH_TYPE_SHARD_GROUP_CHANGE);
            }
        };
    }

    private List<String> expectedEvents(String... listeners) {
        List<String> cacheEvents = switch (this.watchType) {
            case WATCH_TYPE_PARTITION_CHANGE ->
                    List.of("lookup-partition", "remove-partition", "remove-graph");
            case WATCH_TYPE_STORE_NODE_CHANGE -> List.of("remove-store");
            case WATCH_TYPE_SHARD_GROUP_CHANGE -> List.of("update-shard-group");
            case WATCH_TYPE_GRAPH_CHANGE -> List.of();
            default -> throw new AssertionError(this.watchType);
        };
        List<String> expected = new ArrayList<>(cacheEvents);
        expected.addAll(Arrays.asList(listeners));
        return expected;
    }

    private void emit() {
        WatchResponse.Builder response = WatchResponse.newBuilder().setWatchType(this.watchType);
        switch (this.watchType) {
            case WATCH_TYPE_PARTITION_CHANGE -> response.setPartitionResponse(
                    WatchPartitionResponse.newBuilder().setGraph(GRAPH).setPartitionId(PARTITION_ID)
                                          .setChangeType(WatchChangeType.WATCH_CHANGE_TYPE_DEL));
            case WATCH_TYPE_STORE_NODE_CHANGE -> response.setNodeResponse(
                    WatchNodeResponse.newBuilder().setGraph(GRAPH).setNodeId(STORE_ID)
                                     .setNodeEventType(NodeEventType.NODE_EVENT_TYPE_NODE_OFFLINE));
            case WATCH_TYPE_GRAPH_CHANGE -> response.setGraphResponse(
                    WatchGraphResponse.newBuilder().setGraph(Metapb.Graph.newBuilder().setGraphName(GRAPH)));
            case WATCH_TYPE_SHARD_GROUP_CHANGE -> response.setShardGroupResponse(
                    WatchShardGroupResponse.newBuilder().setShardGroupId(PARTITION_ID)
                                           .setShardGroup(SHARD_GROUP)
                                           .setType(WatchChangeType.WATCH_CHANGE_TYPE_ALTER));
            default -> throw new AssertionError(this.watchType);
        }
        this.channel.listeners.get(this.watchType).onMessage(response.build());
    }

    private static class WatchChannel extends ManagedChannel {

        private final Map<WatchType, ClientCall.Listener<WatchResponse>> listeners =
                new EnumMap<>(WatchType.class);
        private boolean shutdown;

        @Override
        public ManagedChannel shutdown() {
            this.shutdown = true;
            this.listeners.clear();
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
            return "test";
        }

        @Override
        public <ReqT, RespT> ClientCall<ReqT, RespT> newCall(
                MethodDescriptor<ReqT, RespT> method, CallOptions callOptions) {
            Assert.assertEquals(HgPdWatchGrpc.getWatchMethod().getFullMethodName(), method.getFullMethodName());
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
                public void halfClose() {
                }

                @SuppressWarnings("unchecked")
                @Override
                public void sendMessage(ReqT message) {
                    WatchRequest request = (WatchRequest) message;
                    listeners.put(request.getCreateRequest().getWatchType(),
                                  (Listener<WatchResponse>) this.listener);
                }
            };
        }
    }
}
