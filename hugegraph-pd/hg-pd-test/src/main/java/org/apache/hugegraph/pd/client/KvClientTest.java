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
import static org.mockito.Mockito.when;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
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
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.config.Property;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ManagedChannel;
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

    @Test
    public void testStubCreationFailureIsReportedAsPdException() throws Exception {
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
        try (FailingStubCreationKvClient testClient =
                     new FailingStubCreationKvClient(
                             PDConfig.of(grpcAddress.get()).setAuthority(user, pwd))) {
            assertThatThrownBy(testClient::initializeTransport)
                    .isInstanceOf(PDException.class)
                    .hasMessageContaining("PD unreachable");
        } finally {
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
        AtomicReference<String> firstAddress = new AtomicReference<>();
        AtomicReference<String> secondAddress = new AtomicReference<>();
        MutableLeaderService firstPdService = new MutableLeaderService(firstAddress);
        MutableLeaderService secondPdService = new MutableLeaderService(secondAddress);
        RecordingWatchService secondWatchService = new RecordingWatchService(12L);
        Server firstServer = ServerBuilder.forPort(0)
                                          .addService(firstPdService)
                                          .build()
                                          .start();
        Server secondServer = ServerBuilder.forPort(0)
                                           .addService(secondPdService)
                                           .addService(secondWatchService)
                                           .build()
                                           .start();
        firstAddress.set("127.0.0.1:" + firstServer.getPort());
        secondAddress.set("127.0.0.1:" + secondServer.getPort());
        ScheduledExecutorService reconnectExecutor = mock(ScheduledExecutorService.class);
        when(reconnectExecutor.schedule(any(Runnable.class), anyLong(), any(TimeUnit.class)))
                .thenReturn(mock(ScheduledFuture.class));
        StatusRuntimeException failure =
                Status.UNAVAILABLE.withDescription("first peer failed").asRuntimeException();
        PDConfig config = PDConfig.of(firstAddress.get() + "," + secondAddress.get())
                                  .setAuthority(user, pwd);
        try (FailFirstStreamingKvClient testClient =
                     new FailFirstStreamingKvClient(config, reconnectExecutor, failure)) {
            testClient.listen("key", response -> { });

            Assert.assertTrue(secondWatchService.started.await(2L, TimeUnit.SECONDS));
            assertThat(firstPdService.calls).hasValue(1);
            assertThat(secondPdService.calls).hasValue(1);
            assertThat(secondWatchService.calls).hasValue(1);
            assertThat(testClient.stubCreations).hasValue(2);
        } finally {
            firstServer.shutdownNow().awaitTermination(5L, TimeUnit.SECONDS);
            secondServer.shutdownNow().awaitTermination(5L, TimeUnit.SECONDS);
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
        for (Status status : List.of(Status.INVALID_ARGUMENT,
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
    public void testChannelResetCancellationSchedulesReconnect() throws Exception {
        try (WatchTestContext context = newWatchTestContext()) {
            context.client.listen("key", response -> { });

            context.client.call(0).observer.onError(Status.CANCELLED.asRuntimeException());

            assertThat(context.reconnectTasks).hasSize(1);
            assertThat(context.client.stubInvalidations).isZero();
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
                assertThat(context.client.stubInvalidations).isEqualTo(1);
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
            assertThat(context.client.stubInvalidations).isEqualTo(1);
            context.runNextReconnect();
            assertThat(context.client.calls).hasSize(2);
            assertThat(context.client.call(1).request.getClientId()).isZero();
        }
    }

    @Test
    public void testLeaderChangedReconnectsToNewPeer() throws Exception {
        AtomicReference<String> leaderAddress = new AtomicReference<>();
        RecordingWatchService firstWatchService = new RecordingWatchService(11L);
        RecordingWatchService secondWatchService = new RecordingWatchService(12L);
        Server firstServer = ServerBuilder.forPort(0)
                                          .addService(new MutableLeaderService(leaderAddress))
                                          .addService(firstWatchService)
                                          .build()
                                          .start();
        Server secondServer = ServerBuilder.forPort(0)
                                           .addService(new MutableLeaderService(leaderAddress))
                                           .addService(secondWatchService)
                                           .build()
                                           .start();
        String firstAddress = "127.0.0.1:" + firstServer.getPort();
        String secondAddress = "127.0.0.1:" + secondServer.getPort();
        leaderAddress.set(firstAddress);
        ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
        Deque<Runnable> reconnectTasks = new ArrayDeque<>();
        CountDownLatch reconnectScheduled = new CountDownLatch(1);
        doAnswer(invocation -> {
            long delay = invocation.getArgument(1);
            if (invocation.<TimeUnit>getArgument(2).toMillis(delay) == 1000L) {
                reconnectTasks.addLast(invocation.getArgument(0));
                reconnectScheduled.countDown();
            }
            return mock(ScheduledFuture.class);
        }).when(executor).schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));
        PDConfig config = PDConfig.of(firstAddress + "," + secondAddress)
                                  .setAuthority(user, pwd);

        try (KvClient<WatchResponse> testClient = new KvClient<>(config, executor)) {
            testClient.listen("key", response -> { });
            Assert.assertTrue(firstWatchService.started.await(2L, TimeUnit.SECONDS));

            leaderAddress.set(secondAddress);
            firstWatchService.observer.get().onNext(
                    WatchResponse.newBuilder().setState(WatchState.Leader_Changed).build());
            Assert.assertTrue(reconnectScheduled.await(2L, TimeUnit.SECONDS));
            assertThat(reconnectTasks).hasSize(1);
            reconnectTasks.removeFirst().run();

            Assert.assertTrue(secondWatchService.started.await(2L, TimeUnit.SECONDS));
            assertThat(firstWatchService.calls).hasValue(1);
            assertThat(secondWatchService.calls).hasValue(1);
        } finally {
            firstServer.shutdownNow().awaitTermination(5L, TimeUnit.SECONDS);
            secondServer.shutdownNow().awaitTermination(5L, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testFollowerErrorReconnectsToNewPeer() throws Exception {
        AtomicReference<String> firstAddress = new AtomicReference<>();
        AtomicReference<String> secondAddress = new AtomicReference<>();
        RejectingWatchService firstWatchService = new RejectingWatchService();
        RecordingWatchService secondWatchService = new RecordingWatchService(12L);
        Server firstServer = ServerBuilder.forPort(0)
                                          .addService(new MutableLeaderService(firstAddress))
                                          .addService(firstWatchService)
                                          .build()
                                          .start();
        Server secondServer = ServerBuilder.forPort(0)
                                           .addService(new MutableLeaderService(secondAddress))
                                           .addService(secondWatchService)
                                           .build()
                                           .start();
        firstAddress.set("127.0.0.1:" + firstServer.getPort());
        secondAddress.set("127.0.0.1:" + secondServer.getPort());
        ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
        Deque<Runnable> reconnectTasks = new ArrayDeque<>();
        CountDownLatch reconnectScheduled = new CountDownLatch(1);
        doAnswer(invocation -> {
            long delay = invocation.getArgument(1);
            if (invocation.<TimeUnit>getArgument(2).toMillis(delay) == 1000L) {
                reconnectTasks.addLast(invocation.getArgument(0));
                reconnectScheduled.countDown();
            }
            return mock(ScheduledFuture.class);
        }).when(executor).schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));
        PDConfig config = PDConfig.of(firstAddress.get() + "," + secondAddress.get())
                                  .setAuthority(user, pwd);

        try (KvClient<WatchResponse> testClient = new KvClient<>(config, executor)) {
            testClient.listen("key", response -> { });
            Assert.assertTrue(firstWatchService.failed.await(2L, TimeUnit.SECONDS));
            Assert.assertTrue(reconnectScheduled.await(2L, TimeUnit.SECONDS));
            assertThat(reconnectTasks).hasSize(1);

            reconnectTasks.removeFirst().run();

            Assert.assertTrue(secondWatchService.started.await(2L, TimeUnit.SECONDS));
            assertThat(firstWatchService.calls).hasValue(1);
            assertThat(secondWatchService.calls).hasValue(1);
        } finally {
            firstServer.shutdownNow().awaitTermination(5L, TimeUnit.SECONDS);
            secondServer.shutdownNow().awaitTermination(5L, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testMissingFirstFrameSchedulesReconnect() throws Exception {
        try (WatchTestContext context = newWatchTestContext()) {
            context.client.listen("key", response -> { });

            assertThat(context.watchTimeoutTasks).hasSize(1);
            assertThat(context.watchTimeoutDelaysMs).containsExactly(5000L);
            context.runNextWatchTimeout();

            assertThat(context.reconnectTasks).hasSize(1);
            assertThat(context.client.stubInvalidations).isEqualTo(1);
            context.runNextReconnect();
            assertThat(context.client.calls).hasSize(2);
            assertThat(context.client.call(1).request.getClientId()).isZero();
        }
    }

    @Test
    public void testFirstFramePreventsWatchTimeoutReconnect() throws Exception {
        try (WatchTestContext context = newWatchTestContext()) {
            context.client.listen("key", response -> { });
            context.client.call(0).observer.onNext(startingResponse(7L));

            context.runNextWatchTimeout();

            assertThat(context.reconnectTasks).isEmpty();
            assertThat(context.client.stubInvalidations).isZero();
        }
    }

    @Test
    public void testFirstFrameDuringTimeoutTransitionPreventsReconnect() throws Exception {
        String loggerName = KvClient.class.getName();
        LoggerContext loggerContext = (LoggerContext) LogManager.getContext(false);
        org.apache.logging.log4j.core.config.Configuration loggerConfiguration =
                loggerContext.getConfiguration();
        LoggerConfig existingConfig = loggerConfiguration.getLoggerConfig(loggerName);
        LoggerConfig originalConfig =
                loggerName.equals(existingConfig.getName()) ? existingConfig : null;
        AtomicReference<Runnable> timeoutAction = new AtomicReference<>();
        TimeoutRaceAppender appender = new TimeoutRaceAppender(timeoutAction);
        appender.start();
        if (originalConfig != null) {
            loggerConfiguration.removeLogger(loggerName);
        }
        LoggerConfig testConfig = new LoggerConfig(loggerName, Level.WARN, false);
        testConfig.addAppender(appender, Level.WARN, null);
        loggerConfiguration.addLogger(loggerName, testConfig);
        loggerContext.updateLoggers();

        try (WatchTestContext context = newWatchTestContext()) {
            Consumer<WatchResponse> consumer = mock(Consumer.class);
            context.client.listen("key", consumer);
            StreamObserver<WatchResponse> observer = context.client.call(0).observer;
            timeoutAction.set(() -> observer.onNext(startingResponse(9L)));

            context.runNextWatchTimeout();

            assertThat(appender.triggered).isTrue();
            assertThat(context.reconnectTasks).isEmpty();
            assertThat(context.client.stubInvalidations).isZero();
            WatchResponse event = eventResponse(9L);
            observer.onNext(event);
            verify(consumer).accept(event);
        } finally {
            loggerConfiguration.removeLogger(loggerName);
            if (originalConfig != null) {
                loggerConfiguration.addLogger(loggerName, originalConfig);
            }
            loggerContext.updateLoggers();
            appender.stop();
        }
    }

    @Test
    public void testStaleWatchTimeoutDoesNotReplaceCurrentObserver() throws Exception {
        try (WatchTestContext context = newWatchTestContext()) {
            context.client.listen("key", response -> { });
            context.client.call(0).observer.onError(Status.UNAVAILABLE.asRuntimeException());
            context.runNextReconnect();

            context.runNextWatchTimeout();

            assertThat(context.client.calls).hasSize(2);
            assertThat(context.reconnectTasks).isEmpty();
            context.client.call(1).observer.onNext(startingResponse(9L));
            assertThat(context.client.call(1).request.getClientId()).isZero();
        }
    }

    @Test
    public void testPermanentReconnectFailureStopsSubscription() throws Exception {
        try (WatchTestContext context = newWatchTestContext()) {
            AtomicReference<Throwable> terminalError = new AtomicReference<>();
            context.client.listen("key", response -> { }, terminalError::set);
            StatusRuntimeException failure = Status.UNAUTHENTICATED.asRuntimeException();
            context.client.failNextCallWith(
                    new PDException(Pdpb.ErrorType.PD_UNREACHABLE_VALUE,
                                    "permanent reconnect failure", failure));

            context.client.call(0).observer.onError(Status.UNAVAILABLE.asRuntimeException());
            context.runNextReconnect();

            assertThat(context.reconnectTasks).isEmpty();
            assertThat(context.client.subscriptionCount()).isZero();
            assertThat(terminalError.get()).isSameAs(failure);
        }
    }

    @Test
    public void testPermanentAsyncErrorIsReportedOnce() throws Exception {
        try (WatchTestContext context = newWatchTestContext()) {
            List<Throwable> terminalErrors = new ArrayList<>();
            context.client.listenPrefix("prefix", response -> { }, terminalErrors::add);
            StatusRuntimeException failure = Status.PERMISSION_DENIED.asRuntimeException();
            StreamObserver<WatchResponse> observer = context.client.call(0).observer;

            observer.onError(failure);
            observer.onCompleted();

            assertThat(terminalErrors).containsExactly(failure);
            assertThat(context.client.subscriptionCount()).isZero();
        }
    }

    @Test
    public void testInvalidWatchDoesNotLeakSubscription() throws Exception {
        try (WatchTestContext context = newWatchTestContext()) {
            assertThatThrownBy(() -> context.client.listen(null, response -> { }))
                    .isInstanceOf(NullPointerException.class);

            assertThat(context.client.subscriptionCount()).isZero();
            context.client.listen("valid", response -> { });
            assertThat(context.client.calls).hasSize(1);
        }
    }

    @Test
    public void testStreamingStartFailureDoesNotLeakSubscription() throws Exception {
        try (WatchTestContext context = newWatchTestContext()) {
            context.client.failNextCalls(1);

            assertThatThrownBy(() -> context.client.listen("failed", response -> { }))
                    .isInstanceOf(PDException.class);
            assertThat(context.client.subscriptionCount()).isZero();

            context.client.listen("valid", response -> { });
            assertThat(context.client.subscriptionCount()).isEqualTo(1);
            assertThat(context.client.calls).hasSize(2);
            assertThat(context.client.call(1).request.getKey()).isEqualTo("valid");
        }
    }

    @Test
    public void testUncheckedReconnectFailureIsRetried() throws Exception {
        try (WatchTestContext context = newWatchTestContext()) {
            Consumer<WatchResponse> activeConsumer = mock(Consumer.class);
            context.client.listen("key", response -> { });
            context.client.listen("active", activeConsumer);
            context.client.failNextCallWith(new IllegalStateException("reconnect setup failed"));

            context.client.call(0).observer.onError(Status.UNKNOWN.asRuntimeException());
            context.runNextReconnect();

            assertThat(context.reconnectTasks).hasSize(1);
            assertThat(context.client.stubInvalidations).isEqualTo(1);
            WatchResponse activeEvent = eventResponse(21L);
            context.client.call(1).observer.onNext(activeEvent);
            verify(activeConsumer).accept(activeEvent);
            context.runNextReconnect();
            assertThat(context.client.calls).hasSize(4);
            assertThat(context.reconnectTasks).isEmpty();
        }
    }

    @Test
    public void testCompletedSchedulesReconnect() throws Exception {
        try (WatchTestContext context = newWatchTestContext()) {
            context.client.listen("key", response -> { });

            context.client.call(0).observer.onCompleted();

            assertThat(context.reconnectTasks).hasSize(1);
            assertThat(context.reconnectDelaysMs).containsExactly(1000L);
            assertThat(context.client.stubInvalidations).isZero();
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
            assertThat(context.client.call(1).request.getClientId()).isZero();

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
            assertThat(context.client.call(3).request.getClientId()).isZero();
            context.client.call(3).observer.onNext(startingResponse(12L));
            WatchResponse secondEvent = eventResponse(13L);
            context.client.call(3).observer.onNext(secondEvent);

            verify(firstConsumer).accept(firstEvent);
            verify(secondConsumer).accept(secondEvent);
            verify(firstConsumer, never()).accept(secondEvent);
            verify(secondConsumer, never()).accept(firstEvent);
        }
    }

    @Test
    public void testReconnectsDoNotSharePermit() throws Exception {
        try (WatchTestContext context = newWatchTestContext()) {
            context.client.listen("first", response -> { });
            context.client.call(0).observer.onNext(startingResponse(11L));
            context.client.listenPrefix("second", response -> { });

            context.client.call(0).observer.onError(Status.UNAVAILABLE.asRuntimeException());
            context.client.call(1).observer.onError(Status.UNAVAILABLE.asRuntimeException());
            context.runNextReconnect();
            context.runNextReconnect();

            assertThat(context.client.calls).hasSize(4);
            assertThat(context.client.call(2).request.getClientId()).isZero();
            assertThat(context.client.call(3).request.getClientId()).isZero();
            assertThat(context.reconnectTasks).isEmpty();
            assertThat(context.reconnectDelaysMs).containsExactly(1000L, 1000L);
        }
    }

    @Test
    public void testActiveSubscriptionDoesNotRestoreFailedSubscriptionClientId()
            throws Exception {
        try (WatchTestContext context = newWatchTestContext()) {
            context.client.listen("first", response -> { });
            context.client.call(0).observer.onNext(startingResponse(11L));
            context.client.listen("second", response -> { });

            context.client.call(0).observer.onError(Status.UNAVAILABLE.asRuntimeException());
            context.client.call(1).observer.onNext(eventResponse(11L));
            context.runNextReconnect();

            assertThat(context.client.calls).hasSize(3);
            assertThat(context.client.call(2).request.getClientId()).isZero();
        }
    }

    @Test
    public void testStaleChannelErrorDoesNotInvalidateFreshStub() throws Exception {
        try (WatchTestContext context = newWatchTestContext()) {
            context.client.listen("first", response -> { });
            context.client.listen("second", response -> { });
            StreamObserver<WatchResponse> oldSecondObserver = context.client.call(1).observer;

            context.client.call(0).observer.onError(Status.UNAVAILABLE.asRuntimeException());
            context.runNextReconnect();
            oldSecondObserver.onError(Status.CANCELLED.asRuntimeException());

            assertThat(context.client.stubInvalidations).isEqualTo(1);
            context.runNextReconnect();
            assertThat(context.client.calls).hasSize(4);
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

    @Test(timeout = 3000L)
    public void testCloseWaitsForRunningReconnectAndClosesItsChannel() throws Exception {
        ScheduledExecutorService reconnectExecutor = mock(ScheduledExecutorService.class);
        Deque<Runnable> reconnectTasks = new ArrayDeque<>();
        doAnswer(invocation -> {
            long delay = invocation.getArgument(1);
            TimeUnit unit = invocation.getArgument(2);
            if (unit.toMillis(delay) == 1000L) {
                reconnectTasks.addLast(invocation.getArgument(0));
            }
            return mock(ScheduledFuture.class);
        }).when(reconnectExecutor).schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));
        ManagedChannel reconnectChannel = mock(ManagedChannel.class);
        when(reconnectChannel.shutdownNow()).thenReturn(reconnectChannel);
        when(reconnectChannel.awaitTermination(anyLong(), any(TimeUnit.class))).thenReturn(true);
        CloseRaceKvClient testClient =
                new CloseRaceKvClient(getPdConfig(), reconnectExecutor, reconnectChannel);
        Thread reconnectThread = null;
        Thread closeThread = null;
        try {
            testClient.listen("key", response -> { });
            testClient.observer.get().onError(Status.UNKNOWN.asRuntimeException());
            Runnable reconnect = reconnectTasks.removeFirst();
            reconnectThread = new Thread(reconnect, "test-watch-reconnect");
            reconnectThread.start();
            Assert.assertTrue(testClient.reconnectStarted.await(1L, TimeUnit.SECONDS));

            closeThread = new Thread(testClient::close, "test-kv-client-close");
            closeThread.start();
            Assert.assertTrue(testClient.closeEntered.await(1L, TimeUnit.SECONDS));
            assertThat(testClient.closeReturned.await(200L, TimeUnit.MILLISECONDS)).isFalse();
        } finally {
            testClient.allowReconnectStart.countDown();
            if (reconnectThread != null) {
                reconnectThread.join(1000L);
            }
            if (closeThread != null) {
                closeThread.join(1000L);
            }
        }

        assertThat(reconnectThread).isNotNull();
        assertThat(closeThread).isNotNull();
        assertThat(reconnectThread.isAlive()).isFalse();
        assertThat(closeThread.isAlive()).isFalse();
        verify(reconnectChannel).shutdownNow();
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
        Deque<Runnable> watchTimeoutTasks = new ArrayDeque<>();
        List<Long> reconnectDelaysMs = new ArrayList<>();
        List<Long> watchTimeoutDelaysMs = new ArrayList<>();
        doAnswer(invocation -> {
            long delay = invocation.getArgument(1);
            TimeUnit unit = invocation.getArgument(2);
            long delayMs = unit.toMillis(delay);
            if (delayMs == 1000L) {
                reconnectTasks.addLast(invocation.getArgument(0));
                reconnectDelaysMs.add(delayMs);
            } else {
                watchTimeoutTasks.addLast(invocation.getArgument(0));
                watchTimeoutDelaysMs.add(delayMs);
            }
            return mock(ScheduledFuture.class);
        }).when(reconnectExecutor).schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));
        TestKvClient testClient = new TestKvClient(getPdConfig(), reconnectExecutor);
        return new WatchTestContext(testClient, reconnectExecutor,
                                    reconnectTasks, watchTimeoutTasks,
                                    reconnectDelaysMs, watchTimeoutDelaysMs);
    }

    private static class InitializableKvClient extends KvClient<WatchResponse> {

        InitializableKvClient(PDConfig pdConfig) {
            super(pdConfig);
        }

        void initializeTransport() throws PDException {
            getStub();
        }
    }

    private static class FailingStubCreationKvClient extends KvClient<WatchResponse> {

        FailingStubCreationKvClient(PDConfig pdConfig) {
            super(pdConfig);
        }

        @Override
        protected AbstractStub createStub() {
            throw new IllegalStateException("stub creation failed");
        }

        void initializeTransport() throws PDException {
            getStub();
        }
    }

    private static class MutableLeaderService extends PDGrpc.PDImplBase {

        private final AtomicReference<String> leaderAddress;
        private final AtomicInteger calls = new AtomicInteger();

        MutableLeaderService(AtomicReference<String> leaderAddress) {
            this.leaderAddress = leaderAddress;
        }

        @Override
        public void getMembers(Pdpb.GetMembersRequest request,
                               StreamObserver<Pdpb.GetMembersResponse> observer) {
            this.calls.incrementAndGet();
            Metapb.Member leader = Metapb.Member.newBuilder()
                                                 .setGrpcUrl(this.leaderAddress.get())
                                                 .build();
            observer.onNext(Pdpb.GetMembersResponse.newBuilder().setLeader(leader).build());
            observer.onCompleted();
        }
    }

    private static class RecordingWatchService extends KvServiceGrpc.KvServiceImplBase {

        private final long clientId;
        private final AtomicInteger calls = new AtomicInteger();
        private final AtomicReference<StreamObserver<WatchResponse>> observer =
                new AtomicReference<>();
        private final CountDownLatch started = new CountDownLatch(1);

        RecordingWatchService(long clientId) {
            this.clientId = clientId;
        }

        @Override
        public void watch(WatchRequest request, StreamObserver<WatchResponse> observer) {
            this.calls.incrementAndGet();
            this.observer.set(observer);
            observer.onNext(startingResponse(this.clientId));
            this.started.countDown();
        }
    }

    private static class RejectingWatchService extends KvServiceGrpc.KvServiceImplBase {

        private static final String NOT_LEADER =
                "node is not leader,it is necessary to  redirect to the leader on the client";

        private final AtomicInteger calls = new AtomicInteger();
        private final CountDownLatch failed = new CountDownLatch(1);

        @Override
        public void watch(WatchRequest request, StreamObserver<WatchResponse> observer) {
            this.calls.incrementAndGet();
            observer.onError(new PDException(-1, NOT_LEADER));
            this.failed.countDown();
        }
    }

    private static class TimeoutRaceAppender extends AbstractAppender {

        private final AtomicReference<Runnable> action;
        private final AtomicBoolean triggered = new AtomicBoolean(false);

        TimeoutRaceAppender(AtomicReference<Runnable> action) {
            super("KvClientTimeoutRaceAppender", (Filter) null,
                  (Layout<? extends Serializable>) null, false, Property.EMPTY_ARRAY);
            this.action = action;
        }

        @Override
        public void append(LogEvent event) {
            Runnable callback = this.action.get();
            if (callback != null && this.triggered.compareAndSet(false, true)) {
                callback.run();
            }
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

    private static class FailFirstStreamingKvClient extends KvClient<WatchResponse> {

        private final StatusRuntimeException firstFailure;
        private final AtomicInteger stubCreations = new AtomicInteger();

        FailFirstStreamingKvClient(PDConfig pdConfig,
                                   ScheduledExecutorService reconnectExecutor,
                                   StatusRuntimeException firstFailure) {
            super(pdConfig, reconnectExecutor);
            this.firstFailure = firstFailure;
        }

        @Override
        protected AbstractStub createStub() {
            if (this.stubCreations.getAndIncrement() == 0) {
                return KvServiceGrpc.newStub(new FailingChannel(this.firstFailure));
            }
            return super.createStub();
        }
    }

    private static class CloseRaceKvClient extends KvClient<WatchResponse> {

        private final ManagedChannel reconnectChannel;
        private final AtomicInteger calls = new AtomicInteger();
        private final AtomicReference<StreamObserver<WatchResponse>> observer =
                new AtomicReference<>();
        private final CountDownLatch reconnectStarted = new CountDownLatch(1);
        private final CountDownLatch allowReconnectStart = new CountDownLatch(1);
        private final CountDownLatch closeEntered = new CountDownLatch(1);
        private final CountDownLatch closeReturned = new CountDownLatch(1);

        CloseRaceKvClient(PDConfig pdConfig,
                          ScheduledExecutorService reconnectExecutor,
                          ManagedChannel reconnectChannel) {
            super(pdConfig, reconnectExecutor);
            this.reconnectChannel = reconnectChannel;
        }

        @Override
        protected <ReqT, RespT> void streamingCall(MethodDescriptor<ReqT, RespT> method,
                                                   ReqT request,
                                                   StreamObserver<RespT> responseObserver,
                                                   int retry) {
            this.observer.set((StreamObserver<WatchResponse>) responseObserver);
            if (this.calls.incrementAndGet() == 1) {
                return;
            }
            this.reconnectStarted.countDown();
            try {
                if (!this.allowReconnectStart.await(1L, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("Timed out waiting to start reconnect");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Reconnect was interrupted", e);
            }
            this.channel = this.reconnectChannel;
        }

        @Override
        public void close() {
            this.closeEntered.countDown();
            super.close();
            this.closeReturned.countDown();
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

    private static class WatchTestContext implements AutoCloseable {

        private final TestKvClient client;
        private final ScheduledExecutorService reconnectExecutor;
        private final Deque<Runnable> reconnectTasks;
        private final Deque<Runnable> watchTimeoutTasks;
        private final List<Long> reconnectDelaysMs;
        private final List<Long> watchTimeoutDelaysMs;

        WatchTestContext(TestKvClient client,
                         ScheduledExecutorService reconnectExecutor,
                         Deque<Runnable> reconnectTasks,
                         Deque<Runnable> watchTimeoutTasks,
                         List<Long> reconnectDelaysMs,
                         List<Long> watchTimeoutDelaysMs) {
            this.client = client;
            this.reconnectExecutor = reconnectExecutor;
            this.reconnectTasks = reconnectTasks;
            this.watchTimeoutTasks = watchTimeoutTasks;
            this.reconnectDelaysMs = reconnectDelaysMs;
            this.watchTimeoutDelaysMs = watchTimeoutDelaysMs;
        }

        Runnable takeNextReconnect() {
            assertThat(reconnectTasks).isNotEmpty();
            return reconnectTasks.removeFirst();
        }

        void runNextReconnect() {
            takeNextReconnect().run();
        }

        void runNextWatchTimeout() {
            assertThat(watchTimeoutTasks).isNotEmpty();
            watchTimeoutTasks.removeFirst().run();
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
        private PDException nextFailure;
        private RuntimeException nextRuntimeFailure;
        private int stubInvalidations;
        private long lockClientId;

        TestKvClient(PDConfig pdConfig, ScheduledExecutorService reconnectExecutor) {
            super(pdConfig, reconnectExecutor);
        }

        void failNextCalls(int failures) {
            this.remainingFailures = failures;
        }

        void failNextCallWith(PDException failure) {
            this.nextFailure = failure;
        }

        void failNextCallWith(RuntimeException failure) {
            this.nextRuntimeFailure = failure;
        }

        int subscriptionCount() throws Exception {
            Field field = KvClient.class.getDeclaredField("subscriptions");
            field.setAccessible(true);
            return ((java.util.Set<?>) field.get(this)).size();
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
            if (this.nextRuntimeFailure != null) {
                RuntimeException failure = this.nextRuntimeFailure;
                this.nextRuntimeFailure = null;
                throw failure;
            }
            if (this.nextFailure != null) {
                PDException failure = this.nextFailure;
                this.nextFailure = null;
                throw failure;
            }
            if (this.remainingFailures > 0) {
                this.remainingFailures--;
                throw new PDException(Pdpb.ErrorType.PD_UNREACHABLE_VALUE,
                                      "PD is still unreachable");
            }
        }

        @Override
        protected void invalidateAsyncStub() {
            this.stubInvalidations++;
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
