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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

import org.apache.hugegraph.store.client.util.ExecutorPool;
import org.apache.hugegraph.store.client.util.HgStoreClientConfig;
import org.apache.hugegraph.store.term.HgPair;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.AbstractAsyncStub;
import io.grpc.stub.AbstractBlockingStub;
import io.grpc.stub.AbstractStub;

public abstract class AbstractGrpcClient {

    protected static Map<String, ManagedChannel[]> channels = new ConcurrentHashMap<>();
    private static final int n = 5;
    protected static int concurrency = 1 << n;
    private static final AtomicLong counter = new AtomicLong(0);
    private static final long limit = Long.MAX_VALUE >> 1;
    protected static final HgStoreClientConfig config = HgStoreClientConfig.of();
    private final Map<String, HgPair<ManagedChannel, AbstractBlockingStub>[]> blockingStubs =
            new ConcurrentHashMap<>();
    private final Map<String, HgPair<ManagedChannel, AbstractAsyncStub>[]> asyncStubs =
            new ConcurrentHashMap<>();
    private final ThreadPoolExecutor executor;

    {
        executor = ExecutorPool.createExecutor("common", 60, concurrency, concurrency);
    }

    public AbstractGrpcClient() {

    }

    public ManagedChannel[] getChannels(String target) {
        ManagedChannel[] tc;
        if ((tc = channels.get(target)) == null) {
            synchronized (channels) {
                if ((tc = channels.get(target)) == null) {
                    try {
                        ManagedChannel[] value = new ManagedChannel[concurrency];
                        CountDownLatch latch = new CountDownLatch(concurrency);
                        for (int i = 0; i < concurrency; i++) {
                            int fi = i;
                            executor.execute(() -> {
                                try {
                                    value[fi] = createChannel(target);
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                } finally {
                                    latch.countDown();
                                }
                            });
                        }
                        latch.await();
                        channels.put(target, tc = value);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }
        return tc;
    }

    public abstract AbstractBlockingStub getBlockingStub(ManagedChannel channel);

    public AbstractBlockingStub getBlockingStub(String target) {
        ManagedChannel[] channels = getChannels(target);
        HgPair<ManagedChannel, AbstractBlockingStub>[] pairs = blockingStubs.get(target);
        long l = counter.getAndIncrement();
        if (l >= limit) {
            counter.set(0);
        }
        int index = (int) (l & (concurrency - 1));
        if (pairs == null) {
            synchronized (blockingStubs) {
                pairs = blockingStubs.get(target);
                if (pairs == null) {
                    HgPair<ManagedChannel, AbstractBlockingStub>[] value = new HgPair[concurrency];
                    IntStream.range(0, concurrency).forEach(i -> {
                        ManagedChannel channel = channels[i];
                        AbstractBlockingStub stub = getBlockingStub(channel);
                        value[i] = new HgPair<>(channel, stub);
                        // log.info("create channel for {}",target);
                    });
                    blockingStubs.put(target, value);
                    AbstractBlockingStub stub = value[index].getValue();
                    return (AbstractBlockingStub) setBlockingStubOption(stub);
                }
            }
        }
        return (AbstractBlockingStub) setBlockingStubOption(pairs[index].getValue());
    }

    private AbstractStub setBlockingStubOption(AbstractBlockingStub stub) {
        return stub.withDeadlineAfter(config.getGrpcTimeoutSeconds(), TimeUnit.SECONDS)
                   .withMaxInboundMessageSize(
                           config.getGrpcMaxInboundMessageSize())
                   .withMaxOutboundMessageSize(
                           config.getGrpcMaxOutboundMessageSize());
    }

    public AbstractAsyncStub getAsyncStub(ManagedChannel channel) {
        return null;
    }

    public AbstractAsyncStub getAsyncStub(String target) {
        ManagedChannel[] channels = getChannels(target);
        HgPair<ManagedChannel, AbstractAsyncStub>[] pairs = asyncStubs.get(target);
        long l = counter.getAndIncrement();
        if (l >= limit) {
            counter.set(0);
        }
        int index = (int) (l & (concurrency - 1));
        if (pairs == null) {
            synchronized (asyncStubs) {
                pairs = asyncStubs.get(target);
                if (pairs == null) {
                    HgPair<ManagedChannel, AbstractAsyncStub>[] value = new HgPair[concurrency];
                    IntStream.range(0, concurrency).parallel().forEach(i -> {
                        ManagedChannel channel = channels[i];
                        AbstractAsyncStub stub = getAsyncStub(channel);
                        // stub.withMaxInboundMessageSize(config.getGrpcMaxInboundMessageSize())
                        //    .withMaxOutboundMessageSize(config.getGrpcMaxOutboundMessageSize());
                        value[i] = new HgPair<>(channel, stub);
                        // log.info("create channel for {}",target);
                    });
                    asyncStubs.put(target, value);
                    AbstractAsyncStub stub =
                            (AbstractAsyncStub) setStubOption(value[index].getValue());
                    return stub;
                }
            }
        }
        return (AbstractAsyncStub) setStubOption(pairs[index].getValue());

    }

    protected AbstractStub setStubOption(AbstractStub value) {
        return value.withMaxInboundMessageSize(
                            config.getGrpcMaxInboundMessageSize())
                    .withMaxOutboundMessageSize(
                            config.getGrpcMaxOutboundMessageSize());
    }

    protected ManagedChannel createChannel(String target) {
        return ManagedChannelBuilder.forTarget(target).usePlaintext().build();
    }

}
