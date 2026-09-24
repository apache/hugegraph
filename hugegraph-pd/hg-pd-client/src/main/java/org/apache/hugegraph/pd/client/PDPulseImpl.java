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

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hugegraph.pd.grpc.pulse.HgPdPulseGrpc;
import org.apache.hugegraph.pd.grpc.pulse.PartitionHeartbeatRequest;
import org.apache.hugegraph.pd.grpc.pulse.PulseAckRequest;
import org.apache.hugegraph.pd.grpc.pulse.PulseCreateRequest;
import org.apache.hugegraph.pd.grpc.pulse.PulseNoticeRequest;
import org.apache.hugegraph.pd.grpc.pulse.PulseRequest;
import org.apache.hugegraph.pd.grpc.pulse.PulseResponse;
import org.apache.hugegraph.pd.grpc.pulse.PulseType;
import org.apache.hugegraph.pd.pulse.PartitionNotice;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class PDPulseImpl implements PDPulse {

    private static ConcurrentHashMap<String, ManagedChannel> chs = new ConcurrentHashMap<>();
    private ExecutorService threadPool;
    private HgPdPulseGrpc.HgPdPulseStub stub;
    private String pdServerAddress;

    // TODO: support several servers.
    public PDPulseImpl(String pdServerAddress, PDConfig config) {
        this.pdServerAddress = pdServerAddress;
        this.stub = AbstractClient.setAsyncParams(
                HgPdPulseGrpc.newStub(Channels.getChannel(pdServerAddress)), config);
        var namedThreadFactory =
                new ThreadFactoryBuilder().setNameFormat("ack-notice-pool-%d").build();
        threadPool = Executors.newSingleThreadExecutor(namedThreadFactory);
    }

    private String getCurrentHost() {
        return this.pdServerAddress;
    }

    private boolean checkChannel() {
        return stub != null && !((ManagedChannel) stub.getChannel()).isShutdown();
    }

    /* TODO: handle this override problem */
    @Override
    public Notifier<PartitionHeartbeatRequest.Builder> connectPartition(Listener<PulseResponse>
                                                                                listener) {
        return new PartitionHeartbeat(listener);
    }

    @Override
    public boolean resetStub(String host, Notifier notifier) {
        log.info("reset stub: current, {}, new: {}, channel state:{}", getCurrentHost(), host,
                 checkChannel());
        if (Objects.equals(host, getCurrentHost()) && checkChannel()) {
            return false;
        }

        if (notifier != null) {
            notifier.close();
        }

        this.stub = HgPdPulseGrpc.newStub(Channels.getChannel(host));
        log.info("pd pulse connect to {}", host);
        this.pdServerAddress = host;
        return true;
    }

    /*** PartitionHeartbeat's implement  ***/
    private class PartitionHeartbeat extends
                                     AbstractConnector<PartitionHeartbeatRequest.Builder,
                                             PulseResponse> {

        private long observerId = -1;

        PartitionHeartbeat(Listener<PulseResponse> listener) {
            super(listener, PulseType.PULSE_TYPE_PARTITION_HEARTBEAT);
        }

        private void setObserverId(long observerId) {
            if (this.observerId == -1) {
                this.observerId = observerId;
            }
        }

        @Override
        public void notifyServer(PartitionHeartbeatRequest.Builder requestBuilder) {
            this.reqStream.onNext(PulseRequest.newBuilder()
                                              .setNoticeRequest(
                                                      PulseNoticeRequest.newBuilder()
                                                                        .setPartitionHeartbeatRequest(
                                                                                requestBuilder.build()
                                                                        ).build()
                                              ).build()
            );
        }

        @Override
        public void onNext(PulseResponse pulseResponse) {
            this.setObserverId(pulseResponse.getObserverId());
            long noticeId = pulseResponse.getNoticeId();
            this.listener.onNext(pulseResponse);
            this.listener.onNotice(new PartitionNotice(noticeId,
                                                       e -> super.ackNotice(e, observerId),
                                                       pulseResponse));
        }

    }

    private abstract class AbstractConnector<N, L> implements Notifier<N>,
                                                              StreamObserver<PulseResponse> {

        Listener<L> listener;
        StreamObserver<PulseRequest> reqStream;
        PulseType pulseType;
        PulseRequest.Builder reqBuilder = PulseRequest.newBuilder();
        PulseAckRequest.Builder ackBuilder = PulseAckRequest.newBuilder();

        private AbstractConnector(Listener<L> listener, PulseType pulseType) {
            this.listener = listener;
            this.pulseType = pulseType;
            this.init();
        }

        void init() {
            PulseCreateRequest.Builder builder = PulseCreateRequest.newBuilder()
                                                                   .setPulseType(this.pulseType);

            this.reqStream = PDPulseImpl.this.stub.pulse(this);
            this.reqStream.onNext(reqBuilder.clear().setCreateRequest(builder).build());
        }

        /*** notifier ***/
        @Override
        public void close() {
            this.reqStream.onCompleted();
        }

        @Override
        public abstract void notifyServer(N t);

        @Override
        public void crash(String error) {
            this.reqStream.onError(new Throwable(error));
        }

        /*** listener  ***/
        @Override
        public abstract void onNext(PulseResponse pulseResponse);

        @Override
        public void onError(Throwable throwable) {
            this.listener.onError(throwable);
        }

        @Override
        public void onCompleted() {
            this.listener.onCompleted();
        }

        protected void ackNotice(long noticeId, long observerId) {
            threadPool.execute(() -> {
                // log.info("send ack: {}, ts: {}", noticeId, System.currentTimeMillis());
                this.reqStream.onNext(reqBuilder.clear()
                                                .setAckRequest(
                                                        this.ackBuilder.clear()
                                                                       .setNoticeId(noticeId)
                                                                       .setObserverId(observerId)
                                                                       .build()
                                                ).build()
                );
            });
        }
    }
}
