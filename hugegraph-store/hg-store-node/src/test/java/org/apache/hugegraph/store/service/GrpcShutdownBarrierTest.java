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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hugegraph.store.node.grpc.GrpcShutdownBarrier;
import org.junit.Test;

import io.grpc.CallOptions;
import io.grpc.ManagedChannel;
import io.grpc.MethodDescriptor;
import io.grpc.Server;
import io.grpc.ServerServiceDefinition;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.ServerCalls;
import io.grpc.stub.StreamObserver;

public class GrpcShutdownBarrierTest {

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
