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

package org.apache.hugegraph.store.client.query;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hugegraph.store.client.grpc.AbstractGrpcClient;
import org.apache.hugegraph.store.grpc.query.QueryServiceGrpc;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.AbstractAsyncStub;
import io.grpc.stub.AbstractBlockingStub;

public class QueryV2Client extends AbstractGrpcClient {

    private volatile static ManagedChannel channel = null;

    private final AtomicInteger seq = new AtomicInteger(0);

    @Override
    public AbstractBlockingStub<?> getBlockingStub(ManagedChannel channel) {
        return QueryServiceGrpc.newBlockingStub(channel);
    }

    @Override
    public AbstractAsyncStub<?> getAsyncStub(ManagedChannel channel) {
        return QueryServiceGrpc.newStub(channel);
    }

    public QueryServiceGrpc.QueryServiceBlockingStub getQueryServiceBlockingStub(String target) {
        return (QueryServiceGrpc.QueryServiceBlockingStub) getBlockingStub(target);
    }

    public QueryServiceGrpc.QueryServiceStub getQueryServiceStub(String target) {
        return (QueryServiceGrpc.QueryServiceStub) setStubOption(
                QueryServiceGrpc.newStub(getManagedChannel(target)));
        // return (QueryServiceGrpc.QueryServiceStub) getAsyncStub(target);
    }

    private ManagedChannel getManagedChannel(String target) {
        return getChannels(target)[Math.abs(seq.getAndIncrement() % concurrency)];
    }

    public static void setTestChannel(ManagedChannel directChannel) {
        channels.clear();
        channel = directChannel;
    }

    @Override
    protected ManagedChannel createChannel(String target) {
        return channel == null ? ManagedChannelBuilder.forTarget(target).usePlaintext().build() :
               channel;
    }
}
