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

package org.apache.hugegraph.store.service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hugegraph.store.node.AppConfig;
import org.apache.hugegraph.store.node.grpc.GRpcServerConfig;
import org.apache.hugegraph.store.node.grpc.GrpcShutdownBarrier;
import org.apache.hugegraph.store.node.util.HgExecutorUtil;
import org.junit.Test;

import io.grpc.CallOptions;
import io.grpc.ClientCall;
import io.grpc.Metadata;
import io.grpc.ManagedChannel;
import io.grpc.MethodDescriptor;
import io.grpc.Server;
import io.grpc.ServerCall;
import io.grpc.ServerServiceDefinition;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.ServerCalls;
import io.grpc.stub.StreamObserver;

public class GrpcShutdownBarrierTest {

    @Test(timeout = 10000)
    public void testNettyCancellationSurvivesSaturatedSingleDispatchThread() throws Exception {
        CountDownLatch callsStarted = new CountDownLatch(2);
        CountDownLatch callbackStarted = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch cancelled = new CountDownLatch(2);
        MethodDescriptor.Marshaller<byte[]> marshaller = new MethodDescriptor.Marshaller<byte[]>() {
            @Override
            public InputStream stream(byte[] value) {
                return new ByteArrayInputStream(value);
            }

            @Override
            public byte[] parse(InputStream stream) {
                return new byte[0];
            }
        };
        MethodDescriptor<byte[], byte[]> method = MethodDescriptor.<byte[], byte[]>newBuilder()
                .setType(MethodDescriptor.MethodType.BIDI_STREAMING).setFullMethodName("saturation/hold")
                .setRequestMarshaller(marshaller).setResponseMarshaller(marshaller).build();
        GrpcShutdownBarrier barrier = new GrpcShutdownBarrier();
        NettyServerBuilder builder = NettyServerBuilder.forPort(0).intercept(barrier)
                .addService(ServerServiceDefinition.builder("saturation").addMethod(method, (call, headers) -> {
                    callsStarted.countDown();
                    call.request(1);
                    return new ServerCall.Listener<byte[]>() {
                        @Override
                        public void onMessage(byte[] message) {
                            callbackStarted.countDown();
                            try {
                                release.await();
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                        }

                        @Override
                        public void onCancel() {
                            cancelled.countDown();
                        }
                    };
                }).build());
        AppConfig config = mock(AppConfig.class);
        AppConfig.ThreadPoolGrpc grpc = mock(AppConfig.ThreadPoolGrpc.class);
        when(config.getThreadPoolGrpc()).thenReturn(grpc);
        when(grpc.getCore()).thenReturn(1);
        when(grpc.getMax()).thenReturn(1);
        when(grpc.getQueue()).thenReturn(0);
        GRpcServerConfig serverConfig = new GRpcServerConfig();
        Field appConfig = GRpcServerConfig.class.getDeclaredField("appConfig");
        appConfig.setAccessible(true);
        appConfig.set(serverConfig, config);
        // Isolate the production named-executor cache for this fixture only.
        Field poolsField = HgExecutorUtil.class.getDeclaredField("threadPoolMap");
        poolsField.setAccessible(true);
        @SuppressWarnings("unchecked")
        Map<String, ThreadPoolExecutor> pools = (Map<String, ThreadPoolExecutor>) poolsField.get(null);
        ThreadPoolExecutor previous = pools.remove(GRpcServerConfig.EXECUTOR_NAME);
        ThreadPoolExecutor executor = null;
        Server server = null;
        ManagedChannel channel = null;
        try {
            serverConfig.configure(builder);
            executor = HgExecutorUtil.getThreadPoolExecutor(GRpcServerConfig.EXECUTOR_NAME);
            assertEquals(1, executor.getCorePoolSize());
            assertEquals(1, executor.getMaximumPoolSize());
            server = builder.build().start();
            channel = NettyChannelBuilder.forAddress("localhost", server.getPort()).usePlaintext().build();
            ClientCall<byte[], byte[]> first = channel.newCall(method, CallOptions.DEFAULT);
            ClientCall<byte[], byte[]> second = channel.newCall(method, CallOptions.DEFAULT);
            first.start(new ClientCall.Listener<byte[]>() { }, new Metadata());
            second.start(new ClientCall.Listener<byte[]>() { }, new Metadata());
            first.request(1);
            second.request(1);
            assertTrue("both calls must register before saturation", callsStarted.await(2, TimeUnit.SECONDS));
            first.sendMessage(new byte[0]);
            assertTrue(callbackStarted.await(2, TimeUnit.SECONDS));
            second.cancel("cancel idle call while dispatch is saturated", null);
            first.cancel("cancel busy call", null);
            long queuedDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
            while (executor.getQueue().isEmpty() && System.nanoTime() < queuedDeadline) {
                Thread.yield();
            }
            assertFalse("idle call's real cancellation callback must queue behind the busy handler",
                        executor.getQueue().isEmpty());
            CountDownLatch waiting = new CountDownLatch(1);
            FutureTask<Void> drained = new FutureTask<>(() -> {
                waiting.countDown();
                barrier.awaitCallbacks();
                return null;
            });
            Thread waiter = new Thread(drained, "test-netty-saturation-drain");
            waiter.setDaemon(true);
            waiter.start();
            assertTrue(waiting.await(1, TimeUnit.SECONDS));
            try {
                drained.get(100, TimeUnit.MILLISECONDS);
                fail("active callback must still block database destruction");
            } catch (TimeoutException expected) {
                // Cancelling the transport cannot discard the application's terminal callback.
            }
            release.countDown();
            assertTrue("both cancellation callbacks must be delivered", cancelled.await(2, TimeUnit.SECONDS));
            drained.get(2, TimeUnit.SECONDS);
            executor.submit(() -> { }).get(1, TimeUnit.SECONDS);
            barrier.stopAcceptingCalls();
            server.shutdownNow();
            assertTrue(server.awaitTermination(2, TimeUnit.SECONDS));
            barrier.awaitCallbacks();
        } finally {
            release.countDown();
            if (channel != null) {
                channel.shutdownNow();
                channel.awaitTermination(2, TimeUnit.SECONDS);
            }
            if (server != null) {
                server.shutdownNow();
                server.awaitTermination(2, TimeUnit.SECONDS);
            }
            if (executor != null) {
                executor.shutdownNow();
                assertTrue(executor.awaitTermination(2, TimeUnit.SECONDS));
                pools.remove(GRpcServerConfig.EXECUTOR_NAME, executor);
            }
            if (previous != null) {
                pools.put(GRpcServerConfig.EXECUTOR_NAME, previous);
            }
        }
    }

    @Test(timeout = 7000)
    public void testTransportTerminationDoesNotReleaseActiveCallback() throws Exception {
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicBoolean finished = new AtomicBoolean();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        MethodDescriptor.Marshaller<byte[]> marshaller = new MethodDescriptor.Marshaller<byte[]>() {
            @Override
            public InputStream stream(byte[] value) {
                return new ByteArrayInputStream(value);
            }
            @Override
            public byte[] parse(InputStream stream) {
                return new byte[0];
            }
        };
        MethodDescriptor<byte[], byte[]> method = MethodDescriptor.<byte[], byte[]>newBuilder()
            .setType(MethodDescriptor.MethodType.UNARY).setFullMethodName("probe/hold")
            .setRequestMarshaller(marshaller).setResponseMarshaller(marshaller).build();
        String name = InProcessServerBuilder.generateName();
        GrpcShutdownBarrier barrier = new GrpcShutdownBarrier();
        Server server = InProcessServerBuilder.forName(name).executor(executor).intercept(barrier)
            .addService(ServerServiceDefinition.builder("probe").addMethod(method,
                ServerCalls.asyncUnaryCall((request, response) -> {
                    started.countDown();
                    try {
                        release.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        finished.set(true);
                    }
                    response.onNext(new byte[0]);
                    response.onCompleted();
                })).build()).build().start();
        ManagedChannel channel = InProcessChannelBuilder.forName(name).directExecutor().build();
        try {
            ClientCalls.asyncUnaryCall(channel.newCall(method, CallOptions.DEFAULT), new byte[0],
                new StreamObserver<byte[]>() {
                    @Override
                    public void onNext(byte[] value) {
                    }
                    @Override
                    public void onError(Throwable error) {
                    }
                    @Override
                    public void onCompleted() {
                    }
                });
            assertTrue("handler must start", started.await(2, TimeUnit.SECONDS));
            barrier.stopAcceptingCalls();
            server.shutdownNow();
            assertTrue(server.awaitTermination(1, TimeUnit.SECONDS));
            assertFalse("transport termination must not be mistaken for callback exit", finished.get());
            CountDownLatch waiting = new CountDownLatch(1);
            FutureTask<Void> drained = new FutureTask<>(() -> {
                waiting.countDown();
                barrier.awaitCallbacks();
                return null;
            });
            Thread waiter = new Thread(drained, "test-rpc-drain");
            waiter.setDaemon(true);
            waiter.start();
            assertTrue(waiting.await(1, TimeUnit.SECONDS));
            try {
                drained.get(100, TimeUnit.MILLISECONDS);
                fail("barrier passed while a handler still owns resources");
            } catch (TimeoutException expected) {
                // The active handler must prevent database destruction.
            }
            release.countDown();
            drained.get(2, TimeUnit.SECONDS);
            assertTrue(finished.get());
        } finally {
            release.countDown();
            channel.shutdownNow();
            server.shutdownNow();
            executor.shutdown();
            assertTrue("cleanup must finish", executor.awaitTermination(2, TimeUnit.SECONDS));
        }
    }
}
