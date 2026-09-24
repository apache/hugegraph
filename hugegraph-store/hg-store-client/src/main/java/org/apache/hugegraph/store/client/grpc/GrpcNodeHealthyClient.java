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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.hugegraph.store.grpc.HealthyGrpc;
import org.apache.hugegraph.store.grpc.HealthyOuterClass;

import com.google.protobuf.Empty;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

@ThreadSafe
public final class GrpcNodeHealthyClient {

    private final static Map<String, ManagedChannel> CHANNEL_MAP = new ConcurrentHashMap<>();
    private final static Map<String, HealthyGrpc.HealthyBlockingStub> STUB_MAP =
            new ConcurrentHashMap<>();

    // TODO: Forbid constructing out of the package.
    public GrpcNodeHealthyClient() {

    }

    private ManagedChannel getChannel(String target) {
        ManagedChannel channel = CHANNEL_MAP.get(target);
        if (channel == null) {
            channel = ManagedChannelBuilder.forTarget(target).usePlaintext().build();
            CHANNEL_MAP.put(target, channel);
        }
        return channel;
    }

    private HealthyGrpc.HealthyBlockingStub getStub(String target) {
        HealthyGrpc.HealthyBlockingStub stub = STUB_MAP.get(target);
        if (stub == null) {
            stub = HealthyGrpc.newBlockingStub(getChannel(target));
            STUB_MAP.put(target, stub);
        }
        return stub;
    }

    boolean isHealthy(GrpcStoreNodeImpl node) {
        String target = node.getAddress();

        HealthyOuterClass.StringReply response = getStub(target).isOk(Empty.newBuilder().build());
        String res = response.getMessage();

        if ("ok".equals(res)) {
            return true;
        } else {
            System.out.printf("gRPC-res-msg: %s%n", res);
            return false;
        }
    }

    public boolean isHealthy() {
        String target = "localhost:9080";
        ManagedChannel channel = ManagedChannelBuilder.forTarget(target).usePlaintext().build();
        HealthyGrpc.HealthyBlockingStub stub = HealthyGrpc.newBlockingStub(channel);
        HealthyOuterClass.StringReply response = stub.isOk(Empty.newBuilder().build());

        String res = response.getMessage();
        System.out.printf("gRPC response message:%s%n", res);

        return "ok".equals(res);
    }
}
