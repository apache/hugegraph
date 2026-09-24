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

import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import org.apache.hugegraph.pd.grpc.watch.HgPdWatchGrpc;
import org.apache.hugegraph.pd.grpc.watch.WatchCreateRequest;
import org.apache.hugegraph.pd.grpc.watch.WatchNodeResponse;
import org.apache.hugegraph.pd.grpc.watch.WatchPartitionResponse;
import org.apache.hugegraph.pd.grpc.watch.WatchRequest;
import org.apache.hugegraph.pd.grpc.watch.WatchResponse;
import org.apache.hugegraph.pd.grpc.watch.WatchType;
import org.apache.hugegraph.pd.watch.NodeEvent;
import org.apache.hugegraph.pd.watch.PartitionEvent;

import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;

final class PDWatchImpl implements PDWatch {

    private HgPdWatchGrpc.HgPdWatchStub stub;

    private String pdServerAddress;
    private static ConcurrentHashMap<String, ManagedChannel> chs = new ConcurrentHashMap<>();

    // TODO: support several servers.
    PDWatchImpl(String pdServerAddress, PDConfig config) {
        this.pdServerAddress = pdServerAddress;
        this.stub = AbstractClient.setAsyncParams(HgPdWatchGrpc.newStub(Channels.getChannel(pdServerAddress)), config);
    }

    @Override
    public String getCurrentHost() {
        return this.pdServerAddress;
    }

    @Override
    public boolean checkChannel() {
        return stub != null && ! Channels.getChannel(this.pdServerAddress).isShutdown();
    }

    /**
     * Get Partition change watcher.
     *
     * @param listener
     * @return
     */
    @Override
    public Watcher watchPartition(Listener<PartitionEvent> listener) {
        return new PartitionWatcher(listener);
    }

    /**
     * Get Store-Node change watcher.
     *
     * @param listener
     * @return
     */
    @Override
    public Watcher watchNode(Listener<NodeEvent> listener) {
        return new NodeWatcher(listener);
    }

    @Override
    public Watcher watchGraph(Listener<WatchResponse> listener) {
        return new GraphWatcher(listener);
    }

    @Override
    public Watcher watchShardGroup(Listener<WatchResponse> listener) {
        return new ShardGroupWatcher(listener);
    }

    private class GraphWatcher extends AbstractWatcher<WatchResponse> {

        private GraphWatcher(Listener listener) {
            super(listener,
                  () -> WatchCreateRequest
                          .newBuilder()
                          .setWatchType(WatchType.WATCH_TYPE_GRAPH_CHANGE)
                          .build()
            );
        }

        @Override
        public void onNext(WatchResponse watchResponse) {
            this.listener.onNext(watchResponse);
        }
    }

    private class ShardGroupWatcher extends AbstractWatcher<WatchResponse> {

        private ShardGroupWatcher(Listener listener) {
            super(listener,
                  () -> WatchCreateRequest
                          .newBuilder()
                          .setWatchType(WatchType.WATCH_TYPE_SHARD_GROUP_CHANGE)
                          .build()
            );
        }

        @Override
        public void onNext(WatchResponse watchResponse) {
            this.listener.onNext(watchResponse);
        }
    }

    private class PartitionWatcher extends AbstractWatcher<PartitionEvent> {

        private PartitionWatcher(Listener listener) {
            super(listener,
                  () -> WatchCreateRequest
                          .newBuilder()
                          .setWatchType(WatchType.WATCH_TYPE_PARTITION_CHANGE)
                          .build()
            );
        }

        @Override
        public void onNext(WatchResponse watchResponse) {
            WatchPartitionResponse res = watchResponse.getPartitionResponse();
            PartitionEvent event = new PartitionEvent(res.getGraph(), res.getPartitionId(),
                                                      PartitionEvent.ChangeType.grpcTypeOf(
                                                              res.getChangeType()));
            this.listener.onNext(event);
        }
    }

    private class NodeWatcher extends AbstractWatcher<NodeEvent> {

        private NodeWatcher(Listener listener) {
            super(listener,
                  () -> WatchCreateRequest
                          .newBuilder()
                          .setWatchType(WatchType.WATCH_TYPE_STORE_NODE_CHANGE)
                          .build()
            );
        }

        @Override
        public void onNext(WatchResponse watchResponse) {
            WatchNodeResponse res = watchResponse.getNodeResponse();
            NodeEvent event = new NodeEvent(res.getGraph(), res.getNodeId(),
                                            NodeEvent.EventType.grpcTypeOf(res.getNodeEventType()));
            this.listener.onNext(event);
        }
    }

    private abstract class AbstractWatcher<T> implements Watcher, StreamObserver<WatchResponse> {

        Listener<T> listener;
        StreamObserver<WatchRequest> reqStream;
        Supplier<WatchCreateRequest> requestSupplier;

        private AbstractWatcher(Listener<T> listener,
                                Supplier<WatchCreateRequest> requestSupplier) {
            this.listener = listener;
            this.requestSupplier = requestSupplier;
            this.init();
        }

        void init() {
            this.reqStream = PDWatchImpl.this.stub.watch(this);
            this.reqStream.onNext(WatchRequest.newBuilder().setCreateRequest(
                    this.requestSupplier.get()
            ).build());
        }

        @Override
        public void close() {
            this.reqStream.onCompleted();
        }

        @Override
        public abstract void onNext(WatchResponse watchResponse);

        @Override
        public void onError(Throwable throwable) {

            this.listener.onError(throwable);
        }

        @Override
        public void onCompleted() {
            this.listener.onCompleted();
        }
    }

}
