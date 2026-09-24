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

package org.apache.hugegraph.store.cli.cmd;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hugegraph.store.grpc.GraphStoreGrpc;
import org.apache.hugegraph.store.grpc.GraphStoreGrpc.GraphStoreStub;
import org.apache.hugegraph.store.grpc.Graphpb.ScanPartitionRequest;
import org.apache.hugegraph.store.grpc.Graphpb.ScanPartitionRequest.Reply;
import org.apache.hugegraph.store.grpc.Graphpb.ScanPartitionRequest.ScanType;
import org.apache.hugegraph.store.grpc.Graphpb.ScanResponse;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

/**
 * @date 2023/10/20
 **/

public interface Scan {

    AtomicInteger sum = new AtomicInteger();
    ConcurrentHashMap<Long, StreamObserver<ScanPartitionRequest>> observers =
            new ConcurrentHashMap<>();

    default void getData(int pId, CountDownLatch latch, String address) {
        try {
            ScanPartitionRequest.Builder builder = ScanPartitionRequest.newBuilder();
            ScanPartitionRequest.Request.Builder srb = ScanPartitionRequest.Request.newBuilder();
            ScanPartitionRequest.Request request =
                    srb.setGraphName("DEFAULT/hugegraph2/g").setScanType(
                               ScanType.SCAN_EDGE)
                       .setTable("g+oe").setBoundary(0x10)
                       .setPartitionId(pId).build();
            ManagedChannel c = ManagedChannelBuilder.forTarget(address)
                                                    .usePlaintext().build();
            int maxSize = 1024 * 1024 * 1024;
            GraphStoreStub stub;
            stub = GraphStoreGrpc.newStub(c).withMaxInboundMessageSize(maxSize)
                                 .withMaxOutboundMessageSize(maxSize);
            AtomicInteger count = new AtomicInteger();
            long start = System.currentTimeMillis();
            long id = Thread.currentThread().getId();
            StreamObserver<ScanResponse> ro =
                    new StreamObserver<ScanResponse>() {
                        @Override
                        public void onNext(ScanResponse value) {
                            try {
                                int edgeSize = value.getEdgeCount();
                                int vertexSize = value.getVertexCount();
                                if (request.getScanType().equals(
                                        ScanType.SCAN_VERTEX)) {
                                    count.getAndAdd(vertexSize);
                                } else {
                                    count.getAndAdd(edgeSize);
                                }
                                // String print = JsonFormat.printer().print
                                // (value);
                                // System.out.println(print);
                                ScanPartitionRequest.Builder builder =
                                        ScanPartitionRequest.newBuilder();
                                builder.setScanRequest(request);
                                Reply.Builder reply = Reply.newBuilder();
                                reply.setSeqNo(1);
                                builder.setReplyRequest(reply);
                                observers.get(id).onNext(builder.build());

                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }

                        @Override
                        public void onError(Throwable t) {
                            observers.remove(id);
                            c.shutdown();
                            latch.countDown();
                        }

                        @Override
                        public void onCompleted() {
                            long time = System.currentTimeMillis() - start;
                            observers.remove(id);
                            c.shutdown();
                            sum.addAndGet(count.get());
                            latch.countDown();
                        }
                    };
            StreamObserver<ScanPartitionRequest> observer = stub.scanPartition(ro);
            observers.put(id, observer);
            builder.setScanRequest(request);
            observer.onNext(builder.build());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
