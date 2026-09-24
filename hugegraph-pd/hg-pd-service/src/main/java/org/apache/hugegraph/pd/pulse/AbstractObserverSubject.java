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

package org.apache.hugegraph.pd.pulse;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Function;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.hugegraph.pd.grpc.pulse.PulseNoticeRequest;
import org.apache.hugegraph.pd.grpc.pulse.PulseResponse;
import org.apache.hugegraph.pd.grpc.pulse.PulseType;
import org.apache.hugegraph.pd.util.IdUtil;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

@ThreadSafe
@Slf4j
abstract class AbstractObserverSubject {

    /* send notice to client */
    private final Map<Long, StreamObserver<PulseResponse>> observerHolder =
            new ConcurrentHashMap<>(1024);
    /* notice from client */
    private final Map<Long, PulseListener> listenerHolder = new ConcurrentHashMap<>(1024);

    private final byte[] lock = new byte[0];
    private final PulseResponse.Builder builder = PulseResponse.newBuilder();
    private final PulseType pulseType;

    protected AbstractObserverSubject(PulseType pulseType) {
        this.pulseType = pulseType;
    }

    /**
     * Add an observer from remote client
     *
     * @param observerId
     * @param responseObserver
     */
    void addObserver(Long observerId, StreamObserver<PulseResponse> responseObserver) {
        synchronized (this.observerHolder) {

            if (this.observerHolder.containsKey(observerId)) {
                responseObserver.onError(
                        new Exception(
                                "The observer-id[" + observerId + "] of " + this.pulseType.name()
                                + " subject has been existing."));
                return;
            }

            log.info("Adding a " + this.pulseType + "'s observer, observer-id is [" + observerId +
                     "].");
            this.observerHolder.put(observerId, responseObserver);
        }

    }

    /**
     * Remove an observer by id
     *
     * @param observerId
     * @param responseObserver
     */
    void removeObserver(Long observerId, StreamObserver<PulseResponse> responseObserver) {
        synchronized (this.observerHolder) {
            log.info("Removing a " + this.pulseType + "'s observer, observer-id is [" + observerId +
                     "].");
            this.observerHolder.remove(observerId);
        }

        responseObserver.onCompleted();
    }

    abstract String toNoticeString(PulseResponse res);

    /**
     * @param c
     * @return notice ID
     */
    protected long notifyClient(Consumer<PulseResponse.Builder> c) {
        synchronized (lock) {

            if (c == null) {
                log.error(this.pulseType.name() +
                          "'s notice was abandoned, caused by: notifyObserver(null)");
                return -1;
            }

            try {
                c.accept(this.builder.clear());
            } catch (Throwable t) {
                log.error(this.pulseType.name() + "'s notice was abandoned, caused by:", t);
                return -1;
            }

            long noticeId = IdUtil.createMillisId();

            Iterator<Map.Entry<Long, StreamObserver<PulseResponse>>> iter =
                    observerHolder.entrySet().iterator();

            // long start = System.currentTimeMillis();
            while (iter.hasNext()) {
                Map.Entry<Long, StreamObserver<PulseResponse>> entry = iter.next();
                Long observerId = entry.getKey();
                PulseResponse res =
                        this.builder.setObserverId(observerId).setNoticeId(noticeId).build();

                try {
                    entry.getValue().onNext(res);
                } catch (Throwable e) {
                    log.error("Failed to send " + this.pulseType.name() + "'s notice[" +
                              toNoticeString(res)
                              + "] to observer[" + observerId + "].", e);

                    // TODO: ? try multi-times?
                    // iter.remove();
                    log.error("Removed a " + this.pulseType.name() + "'s observer[" + entry.getKey()
                              + "], because of once failure of sending.", e);
                }

            }

            // log.info("notice client: notice id: {}, ts :{}, cost: {}", noticeId, System
            // .currentTimeMillis(),
            //        (System.currentTimeMillis() - start )/1000);
            return noticeId;
        }

    }

    abstract long notifyClient(com.google.protobuf.GeneratedMessageV3 response);

    protected void notifyError(int code, String message) {
        synchronized (lock) {
            Iterator<Map.Entry<Long, StreamObserver<PulseResponse>>> iter =
                    observerHolder.entrySet().iterator();
            while (iter.hasNext()) {
                Map.Entry<Long, StreamObserver<PulseResponse>> entry = iter.next();
                Long observerId = entry.getKey();
                PulseResponse res = this.builder.setObserverId(observerId).build();
                try {
                    entry.getValue().onError(Status.fromCodeValue(code).withDescription(message)
                                                   .asRuntimeException());
                } catch (Throwable e) {
                    log.warn("Failed to send {} 's notice[{}] to observer[{}], error:{}",
                             this.pulseType.name(), toNoticeString(res), observerId,
                             e.getMessage());
                }
            }
        }
    }

    /**
     * Add a listener from local server
     *
     * @param listenerId
     * @param listener
     */
    void addListener(Long listenerId, PulseListener<?> listener) {
        synchronized (this.listenerHolder) {

            if (this.listenerHolder.containsKey(listenerId)) {
                listener.onError(
                        new Exception(
                                "The listener-id[" + listenerId + "] of " + this.pulseType.name()
                                + " subject has been existing."));
                return;
            }

            log.info("Adding a " + this.pulseType + "'s listener, listener-id is [" + listenerId +
                     "].");
            this.listenerHolder.put(listenerId, listener);

        }

    }

    /**
     * Remove a listener by id
     *
     * @param listenerId
     * @param listener
     */
    void removeListener(Long listenerId, PulseListener<?> listener) {
        synchronized (this.listenerHolder) {
            log.info("Removing a " + this.pulseType + "'s listener, listener-id is [" + listenerId +
                     "].");
            this.observerHolder.remove(listenerId);
        }

        listener.onCompleted();
    }

    abstract <T> Function<PulseNoticeRequest, T> getNoticeHandler();

    void handleClientNotice(PulseNoticeRequest noticeRequest) throws Exception {

        Iterator<Map.Entry<Long, PulseListener>> iter = listenerHolder.entrySet().iterator();

        while (iter.hasNext()) {
            Map.Entry<Long, PulseListener> entry = iter.next();
            Long listenerId = entry.getKey();
            entry.getValue().onNext(getNoticeHandler().apply(noticeRequest));
        }
    }
}
