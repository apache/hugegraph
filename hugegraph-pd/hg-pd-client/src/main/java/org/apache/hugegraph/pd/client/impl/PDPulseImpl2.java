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

package org.apache.hugegraph.pd.client.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import org.apache.hugegraph.pd.client.PDConnectionManager;
import org.apache.hugegraph.pd.client.PDPulse;
import org.apache.hugegraph.pd.common.HgAssert;
import org.apache.hugegraph.pd.grpc.pulse.HgPdPulseGrpc;
import org.apache.hugegraph.pd.grpc.pulse.PartitionHeartbeatRequest;
import org.apache.hugegraph.pd.grpc.pulse.PulseAckRequest;
import org.apache.hugegraph.pd.grpc.pulse.PulseCreateRequest;
import org.apache.hugegraph.pd.grpc.pulse.PulseNoticeRequest;
import org.apache.hugegraph.pd.grpc.pulse.PulseRequest;
import org.apache.hugegraph.pd.grpc.pulse.PulseResponse;
import org.apache.hugegraph.pd.grpc.pulse.PulseType;
import org.apache.hugegraph.pd.pulse.PartitionNotice;
import org.apache.hugegraph.pd.pulse.PulseServerNotice;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.GeneratedMessageV3;

import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PDPulseImpl2 implements PDPulse {

    private static final long RECONNECT_WAITING_SEC = 3L;

    private static final ExecutorService reconnectPool = Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder().setDaemon(true).setNameFormat("reconnecting-server-pool-%d")
                                      .build()
    );

    private final PDConnectionManager connectionManager;
    private final Map<PulseType, Listener<PulseResponse>> listenerMap = new ConcurrentHashMap<>();
    private final Map<PulseType, Sender<?>> senderMap = new ConcurrentHashMap<>();
    private final Map<PulseType, Receiver> receiverMap = new ConcurrentHashMap<>();
    private final Map<PulseType, Function<PulseResponse, PulseServerNotice<PulseResponse>>>
            noticeParserMap = new HashMap<>();

    private final ExecutorService threadPool;

    private final byte[] lock = new byte[0];
    private final AtomicBoolean isReconnecting = new AtomicBoolean();

    public PDPulseImpl2(PDConnectionManager connectionManager) {
        HgAssert.isArgumentNotNull(connectionManager, "PDConnectionManager");

        this.connectionManager = connectionManager;
        threadPool = Executors.newSingleThreadExecutor(
                new ThreadFactoryBuilder().setNameFormat("ack-notice-pool-%d").build());
        init();
    }

    private void init() {
        this.noticeParserMap.put(PulseType.PULSE_TYPE_PARTITION_HEARTBEAT, this::toPartitionNotice);
        this.connectionManager.addReconnectionTask(this::reconnectServer);
    }

    @Override
    public Notifier<PartitionHeartbeatRequest.Builder> connectPartition(
            Listener<PulseResponse> listener) {
        HgAssert.isArgumentNotNull(listener, "listener");
        this.listenerMap.put(PulseType.PULSE_TYPE_PARTITION_HEARTBEAT, listener);
        return connectServer(PulseType.PULSE_TYPE_PARTITION_HEARTBEAT,
                             PartitionHeartbeatRequest.Builder.class);
    }

    @Override
    public boolean resetStub(String host, Notifier notifier) {
        return true;
    }

    private <T extends GeneratedMessageV3.Builder
            <PartitionHeartbeatRequest.Builder>> Sender<T> connectServer(PulseType pulseType,
                                                                         Class<T> t) {

        Sender<?> sender = this.senderMap.get(pulseType);
        if (sender == null) {
            synchronized (lock) {
                sender = this.senderMap.computeIfAbsent(pulseType,
                                                        k -> new Sender<T>(pulseType,
                                                                           newServerObserver(
                                                                                   pulseType),
                                                                           this::toNotifyServerReq)
                );
            }
        }

        return (Sender<T>) sender;
    }

    public void reconnectServer() {
        if (this.isReconnecting.get()) {
            log.info("[PULSE] Already in reconnecting state, skip reconnectServer");
            return;
        }
        reconnectPool.execute(this::reconnecting);
    }

    private void reconnecting() {
        if (!this.isReconnecting.compareAndSet(false, true)) {
            log.info("[PULSE] Already in reconnecting state, skip reconnecting");
            return;
        }

        log.info("[PULSE] Try to reconnect server...");
        AtomicBoolean isConnected = new AtomicBoolean(false);
        int count = 0;
        while (!isConnected.get()) {
            count++;
            log.info("[PULSE] The [ {} ]th attempt to connect...", count);
            boolean allDone = this.senderMap.entrySet().stream().allMatch(this::doEntryReconnect);
            if (allDone) {
                isConnected.set(true);
                break;
            } else {
                log.error(
                        "[PULSE] Failed to reconnect to the server; waiting [ {} ] seconds for " +
                        "the next attempt."
                        , RECONNECT_WAITING_SEC);
                isConnected.set(false);
            }

            try {
                Thread.sleep(RECONNECT_WAITING_SEC * 1000);
            } catch (InterruptedException e) {
                log.error("[PULSE] Failed to sleep thread and cancel the reconnecting process.", e);
                break;
            }
        }

        this.isReconnecting.set(false);
        if (isConnected.get()) {
            log.info("[PULSE] Reconnect server successfully!");
        } else {
            log.error("[PULSE] Reconnect server failed!");
        }
    }

    private boolean doEntryReconnect(Map.Entry<PulseType, Sender<?>> entry) {
        PulseType pulseType = entry.getKey();
        Sender<?> sender = entry.getValue();
        try {
            sender.close();
            sender.setReqStream(newServerObserver(pulseType));
            return true;
        } catch (Exception e) {
            log.error("[PULSE] Failed to reconnect server with pulse [ {} ], caused by: ",
                      pulseType, e);
        }
        return false;
    }

    private StreamObserver<PulseRequest> newServerObserver(PulseType pulseType) {
        HgPdPulseGrpc.HgPdPulseStub stub = this.connectionManager.newStub(HgPdPulseGrpc::newStub);
        Receiver receiver = this.receiverMap.compute(pulseType, (k, v) -> new Receiver(k));
        return stub.pulse(receiver);
    }

    private <T> PulseRequest toNotifyServerReq(T requestBuilder) {
        PulseNoticeRequest.Builder builder = PulseNoticeRequest.newBuilder();

        if (PartitionHeartbeatRequest.Builder.class.isInstance(requestBuilder)) {
            builder.setPartitionHeartbeatRequest(
                    (PartitionHeartbeatRequest.Builder) requestBuilder);
        } else {
            throw new IllegalStateException(
                    "Unregistered request type: " + requestBuilder.getClass());
        }

        return PulseRequest.newBuilder().setNoticeRequest(builder).build();
    }

    private Listener<PulseResponse> getListener(PulseType pulseType) {
        return this.listenerMap.get(pulseType);
    }

    private PulseServerNotice<PulseResponse> toPartitionNotice(PulseResponse pulseResponse) {
        return new PartitionNotice(pulseResponse.getNoticeId()
                , e -> this.ackNotice(PulseType.PULSE_TYPE_PARTITION_HEARTBEAT
                , pulseResponse.getNoticeId()
                , pulseResponse.getObserverId())
                , pulseResponse);
    }

    // TODO: to support other types of notice
    private void handleOnNext(PulseType pulseType, PulseResponse response) {
        Function<PulseResponse, PulseServerNotice<PulseResponse>> parser =
                this.noticeParserMap.get(pulseType);

        if (parser == null) {
            log.error("[PULSE] Notice parser is null, pulse type: {}", pulseType);
            throw new IllegalStateException("Notice parser is null, pulse type: " + pulseType);
        }

        PulseServerNotice<PulseResponse> notice = parser.apply(response);
        Listener<PulseResponse> listener = this.getListener(pulseType);
        if (listener != null) {
            try {
                listener.onNext(response);
                listener.onNotice(notice);
            } catch (Throwable e) {
                log.error("[PULSE] Listener failed to handle notice: \n{}, caused by: ", response,
                          e);
            }
        }
    }

    private void handleOnComplete(PulseType pulseType) {
        //  this.reconnectServer();
    }

    private void handleOnError(PulseType pulseType, Throwable t) {
        this.reconnectServer();
    }

    private void ackNotice(PulseType pulseType, long noticeId, long observerId) {
        Sender<?> sender = this.senderMap.get(pulseType);
        if (sender == null) {
            log.error("[PULSE] Sender is null, pulse type: {}", pulseType);
            throw new IllegalStateException("Sender is null, pulse type: " + pulseType);
        }

        this.sendingAck(sender, noticeId, observerId);
    }

    private void sendingAck(Sender<?> sender, long noticeId, long observerId) {
        threadPool.execute(() -> {
            log.info("[PULSE] Sending ack, notice id: {}, observer id: {}, ts: {}"
                    , noticeId, observerId, System.currentTimeMillis());
            sender.ack(noticeId, observerId);
        });
    }

    // -------------------------------- inner class -----------------------------------

    private class Receiver implements StreamObserver<PulseResponse> {

        private final PulseType pulseType;

        Receiver(PulseType pulseType) {
            this.pulseType = pulseType;
        }

        @Override
        public void onNext(PulseResponse pulseResponse) {
            log.info("[PULSE] Receiving a notice [ {} ], notice_id: {}, observer_id: {}"
                    , pulseResponse.getPulseType()
                    , pulseResponse.getNoticeId()
                    , pulseResponse.getObserverId());

            PDPulseImpl2.this.handleOnNext(pulseType, pulseResponse);
        }

        @Override
        public void onError(Throwable t) {
            log.error("[PULSE] Receiving an [ onError ], pulse type: {}, error:", pulseType, t);
            PDPulseImpl2.this.handleOnError(pulseType, t);
        }

        @Override
        public void onCompleted() {
            log.info("[PULSE] Receiving an [ onCompleted ], pulse type: {}", pulseType);
            PDPulseImpl2.this.handleOnComplete(pulseType);
        }
    }

    // TODO: add lock 2023/11/20
    private class Sender<T> implements Notifier<T> {

        private final PulseType pulseType;
        private final Function<T, PulseRequest> notifyServerProvider;
        private final AtomicBoolean isClosed = new AtomicBoolean(false);
        private AtomicReference<StreamObserver<PulseRequest>> reqStream = new AtomicReference<>();

        public Sender(PulseType pulseType, StreamObserver<PulseRequest> reqStream
                , Function<T, PulseRequest> notifyServerProvider) {
            this.pulseType = pulseType;
            this.notifyServerProvider = notifyServerProvider;
            this.setReqStream(reqStream);
        }

        public void setReqStream(StreamObserver<PulseRequest> reqStream) {
            this.reqStream.set(reqStream);
            this.start();
            isClosed.set(false);
        }

        void start() {
            send(PulseRequest.newBuilder()
                             .setCreateRequest(
                                     PulseCreateRequest.newBuilder().setPulseType(this.pulseType))
            );
        }

        void ack(long noticeId, long observerId) {
            send(PulseRequest.newBuilder()
                             .setAckRequest(
                                     PulseAckRequest.newBuilder().setNoticeId(noticeId)
                                                    .setObserverId(observerId)
                             )
            );
        }

        private void send(PulseRequest.Builder builder) {
            this.reqStream.get().onNext(builder.build());
        }

        @Override
        public void close() {
            if (isClosed.get()) {
                return;
            }
            isClosed.set(true);
            try {
                this.reqStream.get().onCompleted();
            } catch (Throwable e) {
                log.error("[PULSE] Sender failed to invoke [onCompleted], caused by: ", e);
            }
        }

        @Override
        public void notifyServer(T request) {
            HgAssert.isArgumentNotNull(request, "request");

            try {
                this.reqStream.get().onNext(notifyServerProvider.apply(request));
            } catch (Throwable e) {
                log.error("[PULSE] Sender failed to invoke [notifyServer], caused by: ", e);
                throw new RuntimeException(e);
            }
        }

        @Override
        public void crash(String error) {
            isClosed.set(true);
            this.reqStream.get().onError(new Throwable(error));
        }

    }

}
