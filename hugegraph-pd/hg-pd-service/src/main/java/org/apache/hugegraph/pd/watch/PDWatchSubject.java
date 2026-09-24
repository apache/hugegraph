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

package org.apache.hugegraph.pd.watch;

import static org.apache.hugegraph.pd.common.HgAssert.isArgumentNotNull;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.pd.grpc.watch.NodeEventType;
import org.apache.hugegraph.pd.grpc.watch.WatchChangeType;
import org.apache.hugegraph.pd.grpc.watch.WatchCreateRequest;
import org.apache.hugegraph.pd.grpc.watch.WatchRequest;
import org.apache.hugegraph.pd.grpc.watch.WatchResponse;
import org.apache.hugegraph.pd.grpc.watch.WatchType;

import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@ThreadSafe
public class PDWatchSubject implements StreamObserver<WatchRequest> {

    public final static Map<String, AbstractWatchSubject> subjectHolder = new ConcurrentHashMap<>();
    private final static byte[] lock = new byte[0];

    static {
        subjectHolder.put(WatchType.WATCH_TYPE_PARTITION_CHANGE.name(),
                          new PartitionChangeSubject());
        subjectHolder.put(WatchType.WATCH_TYPE_STORE_NODE_CHANGE.name(), new NodeChangeSubject());
        subjectHolder.put(WatchType.WATCH_TYPE_GRAPH_CHANGE.name(), new NodeChangeSubject());
        subjectHolder.put(WatchType.WATCH_TYPE_SHARD_GROUP_CHANGE.name(),
                          new ShardGroupChangeSubject());
    }

    private final StreamObserver<WatchResponse> responseObserver;
    private AbstractWatchSubject subject;
    private Long watcherId;

    private PDWatchSubject(StreamObserver<WatchResponse> responseObserver) {
        this.responseObserver = responseObserver;
    }

    public static StreamObserver<WatchRequest> addObserver(
            StreamObserver<WatchResponse> responseObserver) {
        isArgumentNotNull(responseObserver, "responseObserver");
        return new PDWatchSubject(responseObserver);
    }

    /**
     * Notify partition change
     *
     * @param changeType  change type
     * @param graph       name of graph
     * @param partitionId id of partition
     */
    public static void notifyPartitionChange(ChangeType changeType, String graph, int partitionId) {
        ((PartitionChangeSubject) subjectHolder.get(WatchType.WATCH_TYPE_PARTITION_CHANGE.name()))
                .notifyWatcher(changeType.getGrpcType(), graph, partitionId);

    }

    public static void notifyShardGroupChange(ChangeType changeType, int groupId,
                                              Metapb.ShardGroup group) {
        ((ShardGroupChangeSubject) subjectHolder.get(
                WatchType.WATCH_TYPE_SHARD_GROUP_CHANGE.name()))
                .notifyWatcher(changeType.getGrpcType(), groupId, group);
    }

    /**
     * Notify store-node change
     *
     * @param changeType change type
     * @param graph      name of graph
     * @param nodeId     id of partition
     */
    public static void notifyNodeChange(NodeEventType changeType, String graph, long nodeId) {
        ((NodeChangeSubject) subjectHolder.get(WatchType.WATCH_TYPE_STORE_NODE_CHANGE.name()))
                .notifyWatcher(changeType, graph, nodeId);
    }

    public static void notifyChange(WatchType type,
                                    WatchResponse.Builder builder) {
        subjectHolder.get(type.name()).notifyWatcher(builder);
    }

    public static void notifyError(int code, String message) {
        subjectHolder.forEach((k, v) -> {
            v.notifyError(code, message);
        });
    }

    private static Long createWatcherId() {
        synchronized (lock) {
            Thread.yield();
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                log.error("Failed to sleep", e);
            }

            return System.currentTimeMillis();
        }

    }

    private void cancelWatcher() {

        if (this.subject == null) {
            this.responseObserver.onError(
                    new Exception("Invoke cancel-watch before create-watch."));
            return;
        }

        this.subject.removeObserver(this.watcherId, this.responseObserver);
    }

    private WatchType getWatchType(WatchCreateRequest request) {
        WatchType watchType = request.getWatchType();

        if (watchType.equals(WatchType.WATCH_TYPE_UNKNOWN)) {
            this.responseObserver.onError(new Exception("unknown watch type."));
            return null;
        }

        return watchType;
    }

    private AbstractWatchSubject getSubject(WatchType watchType) {
        AbstractWatchSubject subject = subjectHolder.get(watchType.name());

        if (subject == null) {
            responseObserver.onError(new Exception("Unsupported watch-type: " + watchType.name()));
            return null;
        }

        return subject;
    }

    private void addWatcher(WatchCreateRequest request) {
        if (this.subject != null) {
            return;
        }
        WatchType watchType = getWatchType(request);
        if (watchType == null) {
            return;
        }

        this.subject = getSubject(watchType);
        this.watcherId = createWatcherId();

        this.subject.addObserver(this.watcherId, this.responseObserver);
    }

    @Override
    public void onNext(WatchRequest watchRequest) {

        if (watchRequest.hasCreateRequest()) {
            this.addWatcher(watchRequest.getCreateRequest());
            return;
        }

        if (watchRequest.hasCancelRequest()) {
            this.cancelWatcher();
        }

    }

    @Override
    public void onError(Throwable throwable) {
        this.cancelWatcher();
    }

    @Override
    public void onCompleted() {
        this.cancelWatcher();
    }

}
