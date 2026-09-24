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

package org.apache.hugegraph.pd.service;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.grpc.Pdpb;
import org.apache.hugegraph.pd.raft.RaftEngine;
import org.apache.hugegraph.pd.raft.RaftStateListener;

import io.grpc.CallOptions;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.MethodDescriptor;

public interface ServiceGrpc extends RaftStateListener {

    ConcurrentHashMap<String, ManagedChannel> channels = new ConcurrentHashMap<>();
    ManagedChannel channel = null;
    Logger log = LoggerFactory.getLogger(ServiceGrpc.class);
    int deadline = 60;

    default Pdpb.ResponseHeader getResponseHeader(PDException e) {
        Pdpb.Error error =
                Pdpb.Error.newBuilder().setTypeValue(e.getErrorCode()).setMessage(e.getMessage())
                          .build();
        Pdpb.ResponseHeader header = Pdpb.ResponseHeader.newBuilder().setError(error).build();
        return header;
    }

    default Pdpb.ResponseHeader getResponseHeader() {
        Pdpb.Error error = Pdpb.Error.newBuilder().setType(Pdpb.ErrorType.OK).build();
        Pdpb.ResponseHeader header = Pdpb.ResponseHeader.newBuilder().setError(error).build();
        return header;
    }

    default boolean isLeader() {
        return RaftEngine.getInstance().isLeader();
    }

    default <ReqT, RespT> void redirectToLeader(ManagedChannel channel,
                                                MethodDescriptor<ReqT, RespT> method,
                                                ReqT req,
                                                io.grpc.stub.StreamObserver<RespT> observer) {
        try {
            String address = RaftEngine.getInstance().getLeaderGrpcAddress();
            channel = channels.compute(address, (addr, existingChannel) -> {

                if (existingChannel != null && !existingChannel.isTerminated() && !existingChannel.isShutdown()) {
                    return existingChannel;
                }

                try {
                    while(existingChannel != null && existingChannel.isShutdown() && !existingChannel.isTerminated()){
                        existingChannel.awaitTermination(50, TimeUnit.MILLISECONDS);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    log.warn("Interrupted while waiting for channel termination", e);
                }

                return ManagedChannelBuilder.forTarget(addr)
                                            .maxInboundMessageSize(Integer.MAX_VALUE)
                                            .usePlaintext()
                                            .build();
            });
            CallOptions callOptions =
                    CallOptions.DEFAULT.withDeadlineAfter(deadline, TimeUnit.SECONDS);
            io.grpc.stub.ClientCalls.asyncUnaryCall(channel.newCall(method, callOptions), req,
                                                    observer);
        } catch (Exception e) {
            log.warn("redirect to leader with error:", e);
        }
    }

    default <ReqT, RespT> void redirectToLeader(MethodDescriptor<ReqT, RespT> method,
                                                ReqT req,
                                                io.grpc.stub.StreamObserver<RespT> observer) {
        redirectToLeader(channel, method, req, observer);
    }

    @Override
    default void onRaftLeaderChanged() {
    }
}
