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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

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
    public AbstractGrpcClient() {

    }

    public ManagedChannel[] getChannels(String target) {
        ManagedChannel[] tc;
        if ((tc = channels.get(target)) == null) {
            synchronized (channels) {
                if ((tc = channels.get(target)) == null) {
                    ManagedChannel[] value = new ManagedChannel[concurrency];
                    try {
                        // Keep lazy initialization on the calling thread. Offloading a
                        // customizable channel builder loses the caller's security context.
                        for (int i = 0; i < concurrency; i++) {
                            value[i] = createChannel(target);
                        }
                        channels.put(target, tc = value);
                    } catch (RuntimeException e) {
                        closeChannels(value, e);
                        throw new RuntimeException(e);
                    } catch (Error e) {
                        closeChannels(value, e);
                        throw e;
                    }
                }
            }
        }
        return tc;
    }

    private static void closeChannels(ManagedChannel[] value, Throwable failure) {
        for (ManagedChannel channel : value) {
            if (channel != null) {
                try {
                    channel.shutdownNow();
                } catch (RuntimeException | Error closeFailure) {
                    failure.addSuppressed(closeFailure);
                }
            }
        }
    }

    public abstract AbstractBlockingStub getBlockingStub(ManagedChannel channel);

    public AbstractBlockingStub getBlockingStub(String target) {
        while (true) {
            ManagedChannel[] channels = getChannels(target);
            HgPair<ManagedChannel, AbstractBlockingStub>[] pairs =
                    blockingStubs.get(target);
            int index = nextStubIndex();
            if (usesChannels(pairs, channels)) {
                return (AbstractBlockingStub) setBlockingStubOption(
                        pairs[index].getValue());
            }
            synchronized (blockingStubs) {
                channels = getChannels(target);
                pairs = blockingStubs.get(target);
                if (usesChannels(pairs, channels)) {
                    return (AbstractBlockingStub) setBlockingStubOption(
                            pairs[index].getValue());
                }
                ManagedChannel[] currentChannels = channels;
                HgPair<ManagedChannel, AbstractBlockingStub>[] value =
                        new HgPair[concurrency];
                IntStream.range(0, concurrency).forEach(i -> {
                    ManagedChannel channel = currentChannels[i];
                    AbstractBlockingStub stub = getBlockingStub(channel);
                    value[i] = new HgPair<>(channel, stub);
                });
                if (!publishStubs(target, currentChannels, blockingStubs, value)) {
                    continue;
                }
                return (AbstractBlockingStub) setBlockingStubOption(
                        value[index].getValue());
            }
        }
    }

    private AbstractStub setBlockingStubOption(AbstractBlockingStub stub) {
        return stub.withDeadlineAfter(config.getGrpcTimeoutSeconds(), TimeUnit.SECONDS)
                   .withMaxInboundMessageSize(config.getGrpcMaxInboundMessageSize())
                   .withMaxOutboundMessageSize(config.getGrpcMaxOutboundMessageSize());
    }

    public AbstractAsyncStub getAsyncStub(ManagedChannel channel) {
        return null;
    }

    public AbstractAsyncStub getAsyncStub(String target) {
        while (true) {
            ManagedChannel[] channels = getChannels(target);
            HgPair<ManagedChannel, AbstractAsyncStub>[] pairs = asyncStubs.get(target);
            int index = nextStubIndex();
            if (usesChannels(pairs, channels)) {
                return (AbstractAsyncStub) setStubOption(pairs[index].getValue());
            }
            synchronized (asyncStubs) {
                channels = getChannels(target);
                pairs = asyncStubs.get(target);
                if (usesChannels(pairs, channels)) {
                    return (AbstractAsyncStub) setStubOption(
                            pairs[index].getValue());
                }
                ManagedChannel[] currentChannels = channels;
                HgPair<ManagedChannel, AbstractAsyncStub>[] value =
                        new HgPair[concurrency];
                IntStream.range(0, concurrency).forEach(i -> {
                    ManagedChannel channel = currentChannels[i];
                    AbstractAsyncStub stub = getAsyncStub(channel);
                    value[i] = new HgPair<>(channel, stub);
                });
                if (!publishStubs(target, currentChannels, asyncStubs, value)) {
                    continue;
                }
                return (AbstractAsyncStub) setStubOption(value[index].getValue());
            }
        }
    }

    private static boolean usesChannels(HgPair<ManagedChannel, ?>[] pairs,
                                        ManagedChannel[] channels) {
        if (pairs == null || pairs.length != channels.length) {
            return false;
        }
        return IntStream.range(0, channels.length)
                        .allMatch(i -> pairs[i].getKey() == channels[i]);
    }

    private static int nextStubIndex() {
        long value = counter.getAndIncrement();
        if (value >= limit) {
            counter.set(0);
        }
        return (int) (value & (concurrency - 1));
    }

    private static <S> boolean publishStubs(
            String target, ManagedChannel[] currentChannels,
            Map<String, HgPair<ManagedChannel, S>[]> stubs,
            HgPair<ManagedChannel, S>[] value) {
        synchronized (channels) {
            if (channels.get(target) != currentChannels) {
                return false;
            }
            stubs.put(target, value);
            return true;
        }
    }

    protected AbstractStub setStubOption(AbstractStub value) {
        return value.withMaxInboundMessageSize(config.getGrpcMaxInboundMessageSize())
                    .withMaxOutboundMessageSize(config.getGrpcMaxOutboundMessageSize());
    }

    public static void closeChannel(String target) {
        ManagedChannel[] targetChannels;
        synchronized (channels) {
            targetChannels = channels.remove(target);
        }
        if (targetChannels != null) {
            IntStream.range(0, targetChannels.length)
                     .mapToObj(i -> targetChannels[i])
                     .filter(channel -> channel != null)
                     .forEach(ManagedChannel::shutdown);
        }
    }

    protected ManagedChannel createChannel(String target) {
        return ManagedChannelBuilder.forTarget(target).usePlaintext().build();
    }
}
