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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.lang.reflect.Field;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.apache.commons.lang3.StringUtils;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.pd.grpc.PDGrpc;
import org.apache.hugegraph.pd.grpc.Pdpb;
import org.apache.hugegraph.pd.grpc.kv.KResponse;
import org.apache.hugegraph.pd.grpc.kv.KvServiceGrpc;
import org.apache.hugegraph.pd.grpc.kv.LockRequest;
import org.apache.hugegraph.pd.grpc.kv.LockResponse;
import org.apache.hugegraph.pd.grpc.kv.ScanPrefixResponse;
import org.apache.hugegraph.pd.grpc.kv.WatchEvent;
import org.apache.hugegraph.pd.grpc.kv.WatchKv;
import org.apache.hugegraph.pd.grpc.kv.WatchRequest;
import org.apache.hugegraph.pd.grpc.kv.WatchResponse;
import org.apache.hugegraph.pd.grpc.kv.WatchState;
import org.apache.hugegraph.pd.grpc.kv.WatchType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.AbstractBlockingStub;
import io.grpc.stub.AbstractStub;
import io.grpc.stub.StreamObserver;

public class KvClientTest extends BaseClientTest {

    private KvClient<WatchResponse> client;

    @Before
    public void setUp() {
        client = new KvClient<>(getPdConfig());
    }

    @After
    public void tearDown() {
        client.close();
    }

    @Test
    public void testCreateStub() {
        // Setup
        // Run the test
        try {
            final AbstractStub result = client.createStub();
        } catch (Exception e) {
        } finally {
        }
    }

    @Test
    public void testCreateBlockingStub() {
        // Setup
        // Run the test
        try {
            final AbstractBlockingStub result = client.createBlockingStub();
        } catch (Exception e) {
        } finally {
        }
    }

    @Test
    public void testTransportInitializationDoesNotCloseClient() throws Exception {
        AtomicReference<String> grpcAddress = new AtomicReference<>();
        Server server = ServerBuilder.forPort(0)
                                     .addService(new PDGrpc.PDImplBase() {
                                         @Override
                                         public void getMembers(
                                                 Pdpb.GetMembersRequest request,
                                                 StreamObserver<Pdpb.GetMembersResponse> observer) {
                                             Metapb.Member leader =
                                                     Metapb.Member.newBuilder()
                                                                   .setGrpcUrl(grpcAddress.get())
                                                                   .build();
                                             observer.onNext(
                                                     Pdpb.GetMembersResponse.newBuilder()
                                                                            .setLeader(leader)
                                                                            .build());
                                             observer.onCompleted();
                                         }
                                     })
                                     .build()
                                     .start();
        grpcAddress.set("127.0.0.1:" + server.getPort());
        InitializableKvClient testClient =
                new InitializableKvClient(PDConfig.of(grpcAddress.get())
                                                  .setAuthority(user, pwd));

        try {
            testClient.initializeTransport();

            assertThat(isClosed(testClient)).isFalse();
            testClient.close();
            assertThat(isClosed(testClient)).isTrue();
        } finally {
            testClient.close();
            server.shutdownNow().awaitTermination(5L, TimeUnit.SECONDS);
        }
    }

    @Test(timeout = 2000L)
    public void testStreamingFailurePropagatesToListenCaller() {
        ScheduledExecutorService reconnectExecutor = mock(ScheduledExecutorService.class);
        StatusRuntimeException failure =
                Status.UNAVAILABLE.withDescription("stream failed").asRuntimeException();
        PDConfig config = PDConfig.of("first:8686,second:8686").setAuthority(user, pwd);
        try (SequentialStreamingKvClient testClient =
                     new SequentialStreamingKvClient(config, reconnectExecutor,
                                                     new FailingChannel(failure),
                                                     new FailingChannel(failure),
                                                     new FailingChannel(failure),
                                                     new FailingChannel(failure))) {
            assertThatThrownBy(() -> testClient.listen("key", response -> { }))
                    .isInstanceOf(PDException.class)
                    .hasCause(failure);
            assertThatThrownBy(() -> testClient.listen("key", response -> { }))
                    .isInstanceOf(PDException.class)
                    .hasCause(failure);
        }
    }

    @Test(timeout = 2000L)
    public void testStreamingFailureRetriesNextPeer() throws Exception {
        ScheduledExecutorService reconnectExecutor = mock(ScheduledExecutorService.class);
        StatusRuntimeException failure =
                Status.UNAVAILABLE.withDescription("first peer failed").asRuntimeException();
        AtomicBoolean secondPeerCalled = new AtomicBoolean(false);
        PDConfig config = PDConfig.of("first:8686,second:8686").setAuthority(user, pwd);
        try (SequentialStreamingKvClient testClient =
                     new SequentialStreamingKvClient(config, reconnectExecutor,
                                                     new FailingChannel(failure),
                                                     new SuccessfulChannel(secondPeerCalled))) {
            testClient.listen("key", response -> { });

            assertThat(secondPeerCalled).isTrue();
        }
    }

    @Test
    public void testReconnectRetriesAfterFirstFailure() throws Exception {
        try (WatchTestContext context = newWatchTestContext()) {
            Consumer<WatchResponse> consumer = mock(Consumer.class);
            context.client.listen("key", consumer);
            context.client.failNextCalls(1);

            context.client.call(0).observer.onError(new RuntimeException("disconnected"));
            context.runNextReconnect();
            assertThat(context.client.calls).hasSize(2);
            assertThat(context.reconnectTasks).hasSize(1);

            context.runNextReconnect();

            assertThat(context.client.calls).hasSize(3);
            assertThat(context.reconnectTasks).isEmpty();
            assertThat(context.reconnectDelaysMs).containsExactly(1000L, 1000L);
            assertThat(context.client.call(2).methodName)
                    .isEqualTo(KvServiceGrpc.getWatchMethod().getFullMethodName());
            assertThat(context.client.call(2).request.getKey()).isEqualTo("key");

            WatchResponse started = WatchResponse.newBuilder()
                                                   .setState(WatchState.Starting)
                                                   .setClientId(1L)
                                                   .build();
            WatchEvent event = WatchEvent.newBuilder().setType(WatchType.Put).build();
            WatchResponse futureEvent = WatchResponse.newBuilder()
                                                     .setState(WatchState.Started)
                                                     .setClientId(1L)
                                                     .addEvents(event)
                                                     .build();
            context.client.call(2).observer.onNext(started);
            context.client.call(2).observer.onNext(futureEvent);
            verify(consumer).accept(futureEvent);
        }
    }

    @Test
    public void testReconnectRetriesAfterConsecutiveFailures() throws Exception {
        try (WatchTestContext context = newWatchTestContext()) {
            context.client.listen("key", response -> { });
            context.client.failNextCalls(2);

            context.client.call(0).observer.onError(new RuntimeException("disconnected"));
            context.runNextReconnect();
            context.runNextReconnect();
            context.runNextReconnect();

            assertThat(context.client.calls).hasSize(4);
            assertThat(context.reconnectTasks).isEmpty();
            assertThat(context.reconnectDelaysMs).containsExactly(1000L, 1000L, 1000L);
        }
    }

    @Test
    public void testWatchReconnectPreservesLockClientId() throws Exception {
        try (WatchTestContext context = newWatchTestContext()) {
            context.client.respondWithLockClientId(42L);
            context.client.lock("lock", 1000L);
            context.client.listen("key", response -> { });

            assertThat(context.client.call(0).request.getClientId()).isZero();
            StreamObserver<WatchResponse> observer = context.client.call(0).observer;
            observer.onNext(WatchResponse.newBuilder()
                                                 .setState(WatchState.Starting)
                                                 .setClientId(7L)
                                                 .build());
            observer.onError(Status.UNAVAILABLE.asRuntimeException());
            context.runNextReconnect();

            context.client.keepAlive("lock");
            assertThat(context.client.lockCall(0).methodName)
                    .isEqualTo(KvServiceGrpc.getLockMethod().getFullMethodName());
            assertThat(context.client.lockCall(0).request.getClientId()).isZero();
            assertThat(context.client.lockCall(1).methodName)
                    .isEqualTo(KvServiceGrpc.getKeepAliveMethod().getFullMethodName());
            assertThat(context.client.lockCall(1).request.getClientId()).isEqualTo(42L);
        }
    }

    @Test
    public void testPermanentErrorStopsReconnect() throws Exception {
        for (Status status : List.of(Status.CANCELLED,
                                     Status.INVALID_ARGUMENT,
                                     Status.NOT_FOUND,
                                     Status.ALREADY_EXISTS,
                                     Status.PERMISSION_DENIED,
                                     Status.FAILED_PRECONDITION,
                                     Status.OUT_OF_RANGE,
                                     Status.UNIMPLEMENTED,
                                     Status.DATA_LOSS,
                                     Status.UNAUTHENTICATED)) {
            try (WatchTestContext context = newWatchTestContext()) {
                context.client.listen("key", response -> { });
                StreamObserver<WatchResponse> observer = context.client.call(0).observer;

                observer.onError(status.withDescription("permanent watch error")
                                       .asRuntimeException());
                observer.onCompleted();

                assertThat(context.reconnectTasks).isEmpty();
                assertThat(context.client.calls).hasSize(1);
            }
        }
    }

    @Test
    public void testRetryableErrorSchedulesReconnect() throws Exception {
        for (Status status : List.of(Status.UNAVAILABLE, Status.UNKNOWN)) {
            try (WatchTestContext context = newWatchTestContext()) {
                context.client.listen("key", response -> { });

                context.client.call(0).observer.onError(status.asRuntimeException());

                assertThat(context.reconnectTasks).hasSize(1);
                assertThat(context.reconnectDelaysMs).containsExactly(1000L);
            }
        }
    }

    @Test
    public void testLeaderChangedSchedulesReconnect() throws Exception {
        try (WatchTestContext context = newWatchTestContext()) {
            context.client.listen("key", response -> { });
            StreamObserver<WatchResponse> observer = context.client.call(0).observer;
            observer.onNext(WatchResponse.newBuilder()
                                             .setState(WatchState.Starting)
                                             .setClientId(7L)
                                             .build());

            observer.onNext(
                    WatchResponse.newBuilder().setState(WatchState.Leader_Changed).build());

            assertThat(context.reconnectTasks).hasSize(1);
            assertThat(context.reconnectDelaysMs).containsExactly(1000L);
            context.runNextReconnect();
            assertThat(context.client.calls).hasSize(2);
            assertThat(context.client.call(1).request.getClientId()).isZero();
        }
    }

    @Test
    public void testCompletedSchedulesReconnect() throws Exception {
        try (WatchTestContext context = newWatchTestContext()) {
            context.client.listen("key", response -> { });

            context.client.call(0).observer.onCompleted();

            assertThat(context.reconnectTasks).hasSize(1);
            assertThat(context.reconnectDelaysMs).containsExactly(1000L);
            context.runNextReconnect();
            assertThat(context.client.calls).hasSize(2);
        }
    }

    @Test
    public void testObserverSchedulesOnlyOneReconnect() throws Exception {
        try (WatchTestContext context = newWatchTestContext()) {
            context.client.listen("key", response -> { });
            StreamObserver<WatchResponse> observer = context.client.call(0).observer;

            observer.onError(new RuntimeException("disconnected"));
            observer.onCompleted();

            assertThat(context.reconnectTasks).hasSize(1);
            assertThat(context.reconnectDelaysMs).containsExactly(1000L);
        }
    }

    @Test
    public void testSubscriptionsReconnectIndependently() throws Exception {
        try (WatchTestContext context = newWatchTestContext()) {
            Consumer<WatchResponse> firstConsumer = mock(Consumer.class);
            Consumer<WatchResponse> secondConsumer = mock(Consumer.class);
            context.client.listen("first", firstConsumer);
            context.client.call(0).observer.onNext(startingResponse(11L));
            context.client.listenPrefix("second", secondConsumer);

            context.client.call(0).observer.onError(Status.UNAVAILABLE.asRuntimeException());
            context.client.call(1).observer.onError(Status.UNAVAILABLE.asRuntimeException());

            assertThat(context.reconnectTasks).hasSize(2);
            assertThat(context.reconnectDelaysMs).containsExactly(1000L, 1000L);

            context.runNextReconnect();
            assertThat(context.client.call(2).methodName)
                    .isEqualTo(KvServiceGrpc.getWatchMethod().getFullMethodName());
            assertThat(context.client.call(2).request.getKey()).isEqualTo("first");
            context.client.call(2).observer.onNext(startingResponse(12L));
            WatchResponse firstEvent = eventResponse(12L);
            context.client.call(2).observer.onNext(firstEvent);

            context.runNextReconnect();
            assertThat(context.client.call(3).methodName)
                    .isEqualTo(KvServiceGrpc.getWatchPrefixMethod().getFullMethodName());
            assertThat(context.client.call(3).request.getKey()).isEqualTo("second");
            context.client.call(3).observer.onNext(startingResponse(12L));
            WatchResponse secondEvent = eventResponse(13L);
            context.client.call(3).observer.onNext(secondEvent);

            verify(firstConsumer).accept(firstEvent);
            verify(secondConsumer).accept(secondEvent);
            verify(firstConsumer, never()).accept(secondEvent);
            verify(secondConsumer, never()).accept(firstEvent);
        }
    }

    @Test(timeout = 5000L)
    public void testBlockedReconnectReschedulesOtherSubscription() throws Exception {
        try (WatchTestContext context = newWatchTestContext()) {
            context.client.listen("first", response -> { });
            context.client.call(0).observer.onNext(startingResponse(11L));
            context.client.listenPrefix("second", response -> { });

            context.client.call(0).observer.onError(Status.UNAVAILABLE.asRuntimeException());
            context.client.call(1).observer.onError(Status.UNAVAILABLE.asRuntimeException());
            context.runNextReconnect();

            assertThat(context.client.calls).hasSize(3);
            ExecutorService reconnectRunner = Executors.newSingleThreadExecutor();
            try {
                Future<?> secondReconnect = reconnectRunner.submit(context::runNextReconnect);
                secondReconnect.get(1L, TimeUnit.SECONDS);
            } finally {
                reconnectRunner.shutdownNow();
                assertThat(reconnectRunner.awaitTermination(1L, TimeUnit.SECONDS)).isTrue();
            }

            assertThat(context.client.calls).hasSize(3);
            assertThat(context.reconnectTasks).hasSize(1);
            assertThat(context.reconnectDelaysMs).containsExactly(1000L, 1000L, 1000L);

            context.client.call(2).observer.onNext(startingResponse(12L));
            context.runNextReconnect();
            assertThat(context.client.calls).hasSize(4);
            assertThat(context.client.call(3).methodName)
                    .isEqualTo(KvServiceGrpc.getWatchPrefixMethod().getFullMethodName());
            assertThat(context.client.call(3).request.getKey()).isEqualTo("second");
        }
    }

    @Test
    public void testStaleObserverDoesNotScheduleReconnect() throws Exception {
        try (WatchTestContext context = newWatchTestContext()) {
            Consumer<WatchResponse> consumer = mock(Consumer.class);
            context.client.listen("key", consumer);
            StreamObserver<WatchResponse> staleObserver = context.client.call(0).observer;
            staleObserver.onError(new RuntimeException("disconnected"));
            context.runNextReconnect();

            staleObserver.onCompleted();
            WatchEvent event = WatchEvent.newBuilder().setType(WatchType.Put).build();
            WatchResponse response = WatchResponse.newBuilder()
                                                  .setState(WatchState.Started)
                                                  .setClientId(8L)
                                                  .addEvents(event)
                                                  .build();
            staleObserver.onNext(response);

            assertThat(context.client.calls).hasSize(2);
            assertThat(context.reconnectTasks).isEmpty();
            verify(consumer, never()).accept(any());

            StreamObserver<WatchResponse> currentObserver = context.client.call(1).observer;
            currentObserver.onNext(WatchResponse.newBuilder()
                                                .setState(WatchState.Starting)
                                                .setClientId(8L)
                                                .build());
            currentObserver.onNext(response);
            verify(consumer).accept(response);
        }
    }

    @Test
    public void testPrefixReconnectPreservesPrefixMethod() throws Exception {
        try (WatchTestContext context = newWatchTestContext()) {
            context.client.listenPrefix("prefix", response -> { });
            context.client.call(0).observer.onCompleted();
            context.runNextReconnect();

            assertThat(context.client.calls).hasSize(2);
            assertThat(context.client.call(1).methodName)
                    .isEqualTo(KvServiceGrpc.getWatchPrefixMethod().getFullMethodName());
            assertThat(context.client.call(1).request.getKey()).isEqualTo("prefix");
        }
    }

    @Test
    public void testCloseStopsScheduledReconnect() throws Exception {
        try (WatchTestContext context = newWatchTestContext()) {
            context.client.listen("key", response -> { });
            context.client.call(0).observer.onError(new RuntimeException("disconnected"));
            Runnable reconnect = context.takeNextReconnect();

            context.client.close();
            reconnect.run();

            assertThat(context.client.calls).hasSize(1);
            assertThat(context.reconnectTasks).isEmpty();
            verify(context.reconnectExecutor).shutdownNow();
        }
    }

    String key = "key";
    String value = "value";

    @Test
    public void testPutAndGet() throws Exception {
        // Run the test
        try {
            client.put(key, value);
            // Run the test
            KResponse result = client.get(key);

            // Verify the results
            assertThat(result.getValue()).isEqualTo(value);
            client.delete(key);
            result = client.get(key);
            assertThat(StringUtils.isEmpty(result.getValue()));
            client.deletePrefix(key);
            client.put(key + "1", value);
            client.put(key + "2", value);
            ScanPrefixResponse response = client.scanPrefix(key);
            assertThat(response.getKvsMap().size() == 2);
            client.putTTL(key + "3", value, 1000);
            client.keepTTLAlive(key + "3");
            final Consumer<WatchResponse> mockConsumer = mock(Consumer.class);

            // Run the test
            client.listen(key + "3", mockConsumer);
            client.listenPrefix(key + "4", mockConsumer);
            WatchResponse r = WatchResponse.newBuilder().addEvents(
                                                   WatchEvent.newBuilder().setCurrent(
                                                           WatchKv.newBuilder().setKey(key).setValue("value")
                                                                  .build()).setType(WatchType.Put).build())
                                           .setClientId(0L)
                                           .setState(WatchState.Starting)
                                           .build();
            client.getWatchList(r);
            client.getWatchMap(r);
            client.lock(key, 3000L);
            client.isLocked(key);
            client.unlock(key);
            client.lock(key, 3000L);
            client.keepAlive(key);
            client.close();
        } catch (Exception e) {

        }
    }

    private static boolean isClosed(KvClient<?> client) throws Exception {
        Field field = KvClient.class.getDeclaredField("closed");
        field.setAccessible(true);
        return ((AtomicBoolean) field.get(client)).get();
    }

    private static WatchResponse startingResponse(long clientId) {
        return WatchResponse.newBuilder()
                            .setState(WatchState.Starting)
                            .setClientId(clientId)
                            .build();
    }

    private static WatchResponse eventResponse(long clientId) {
        return WatchResponse.newBuilder()
                            .setState(WatchState.Started)
                            .setClientId(clientId)
                            .addEvents(WatchEvent.newBuilder().setType(WatchType.Put).build())
                            .build();
    }

    private WatchTestContext newWatchTestContext() {
        ScheduledExecutorService reconnectExecutor = mock(ScheduledExecutorService.class);
        Deque<Runnable> reconnectTasks = new ArrayDeque<>();
        List<Long> reconnectDelaysMs = new ArrayList<>();
        doAnswer(invocation -> {
            reconnectTasks.addLast(invocation.getArgument(0));
            long delay = invocation.getArgument(1);
            TimeUnit unit = invocation.getArgument(2);
            reconnectDelaysMs.add(unit.toMillis(delay));
            return mock(ScheduledFuture.class);
        }).when(reconnectExecutor).schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));
        TestKvClient testClient = new TestKvClient(getPdConfig(), reconnectExecutor);
        return new WatchTestContext(testClient, reconnectExecutor,
                                    reconnectTasks, reconnectDelaysMs);
    }

    private static class InitializableKvClient extends KvClient<WatchResponse> {

        InitializableKvClient(PDConfig pdConfig) {
            super(pdConfig);
        }

        void initializeTransport() throws PDException {
            getStub();
        }
    }

    private static class SequentialStreamingKvClient extends KvClient<WatchResponse> {

        private final Deque<Channel> channels = new ArrayDeque<>();

        SequentialStreamingKvClient(PDConfig pdConfig,
                                    ScheduledExecutorService reconnectExecutor,
                                    Channel... channels) {
            super(pdConfig, reconnectExecutor);
            for (Channel channel : channels) {
                this.channels.addLast(channel);
            }
        }

        @Override
        protected AbstractStub getStub() {
            return KvServiceGrpc.newStub(this.channels.removeFirst());
        }
    }

    private static class FailingChannel extends Channel {

        private final StatusRuntimeException failure;

        FailingChannel(StatusRuntimeException failure) {
            this.failure = failure;
        }

        @Override
        public String authority() {
            return "test";
        }

        @Override
        public <ReqT, RespT> ClientCall<ReqT, RespT> newCall(
                MethodDescriptor<ReqT, RespT> method, CallOptions callOptions) {
            return new FailingClientCall<>(this.failure);
        }
    }

    private static class FailingClientCall<ReqT, RespT> extends ClientCall<ReqT, RespT> {

        private final StatusRuntimeException failure;

        FailingClientCall(StatusRuntimeException failure) {
            this.failure = failure;
        }

        @Override
        public void start(Listener<RespT> responseListener, Metadata headers) {
            throw this.failure;
        }

        @Override
        public void request(int numMessages) {
        }

        @Override
        public void cancel(String message, Throwable cause) {
        }

        @Override
        public void halfClose() {
        }

        @Override
        public void sendMessage(ReqT message) {
        }
    }

    private static class SuccessfulChannel extends Channel {

        private final AtomicBoolean called;

        SuccessfulChannel(AtomicBoolean called) {
            this.called = called;
        }

        @Override
        public String authority() {
            return "test";
        }

        @Override
        public <ReqT, RespT> ClientCall<ReqT, RespT> newCall(
                MethodDescriptor<ReqT, RespT> method, CallOptions callOptions) {
            return new SuccessfulClientCall<>(this.called);
        }
    }

    private static class SuccessfulClientCall<ReqT, RespT> extends ClientCall<ReqT, RespT> {

        private final AtomicBoolean called;

        SuccessfulClientCall(AtomicBoolean called) {
            this.called = called;
        }

        @Override
        public void start(Listener<RespT> responseListener, Metadata headers) {
            this.called.set(true);
        }

        @Override
        public void request(int numMessages) {
        }

        @Override
        public void cancel(String message, Throwable cause) {
        }

        @Override
        public void halfClose() {
        }

        @Override
        public void sendMessage(ReqT message) {
        }
    }

    private static class WatchTestContext implements AutoCloseable {

        private final TestKvClient client;
        private final ScheduledExecutorService reconnectExecutor;
        private final Deque<Runnable> reconnectTasks;
        private final List<Long> reconnectDelaysMs;

        WatchTestContext(TestKvClient client,
                         ScheduledExecutorService reconnectExecutor,
                         Deque<Runnable> reconnectTasks,
                         List<Long> reconnectDelaysMs) {
            this.client = client;
            this.reconnectExecutor = reconnectExecutor;
            this.reconnectTasks = reconnectTasks;
            this.reconnectDelaysMs = reconnectDelaysMs;
        }

        Runnable takeNextReconnect() {
            assertThat(reconnectTasks).isNotEmpty();
            return reconnectTasks.removeFirst();
        }

        void runNextReconnect() {
            takeNextReconnect().run();
        }

        @Override
        public void close() {
            client.close();
        }
    }

    private static class TestKvClient extends KvClient<WatchResponse> {

        private final List<WatchCall> calls = new ArrayList<>();
        private final List<LockCall> lockCalls = new ArrayList<>();
        private int remainingFailures;
        private long lockClientId;

        TestKvClient(PDConfig pdConfig, ScheduledExecutorService reconnectExecutor) {
            super(pdConfig, reconnectExecutor);
        }

        void failNextCalls(int failures) {
            this.remainingFailures = failures;
        }

        WatchCall call(int index) {
            return this.calls.get(index);
        }

        void respondWithLockClientId(long clientId) {
            this.lockClientId = clientId;
        }

        LockCall lockCall(int index) {
            return this.lockCalls.get(index);
        }

        @Override
        protected <ReqT, RespT> RespT blockingUnaryCall(
                MethodDescriptor<ReqT, RespT> method, ReqT request) {
            LockRequest lockRequest = (LockRequest) request;
            this.lockCalls.add(new LockCall(method.getFullMethodName(), lockRequest));
            return (RespT) LockResponse.newBuilder()
                                       .setHeader(AbstractClient.okHeader)
                                       .setClientId(this.lockClientId)
                                       .setSucceed(true)
                                       .build();
        }

        @Override
        protected <ReqT, RespT> void streamingCall(MethodDescriptor<ReqT, RespT> method,
                                                   ReqT request,
                                                   StreamObserver<RespT> responseObserver,
                                                   int retry) throws PDException {
            this.calls.add(new WatchCall(method.getFullMethodName(),
                                         (WatchRequest) request,
                                         (StreamObserver<WatchResponse>) responseObserver));
            if (this.remainingFailures > 0) {
                this.remainingFailures--;
                throw new PDException(Pdpb.ErrorType.PD_UNREACHABLE_VALUE,
                                      "PD is still unreachable");
            }
        }
    }

    private static class WatchCall {

        private final String methodName;
        private final WatchRequest request;
        private final StreamObserver<WatchResponse> observer;

        WatchCall(String methodName, WatchRequest request,
                  StreamObserver<WatchResponse> observer) {
            this.methodName = methodName;
            this.request = request;
            this.observer = observer;
        }
    }

    private static class LockCall {

        private final String methodName;
        private final LockRequest request;

        LockCall(String methodName, LockRequest request) {
            this.methodName = methodName;
            this.request = request;
        }
    }
}
