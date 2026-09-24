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

package org.apache.hugegraph.pd.watch;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.hugegraph.pd.grpc.watch.WatchResponse;
import org.apache.hugegraph.pd.grpc.watch.WatchType;

import com.google.protobuf.util.JsonFormat;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

@ThreadSafe
@Slf4j
abstract class AbstractWatchSubject {

    private final Map<Long, StreamObserver<WatchResponse>> watcherHolder =
            new ConcurrentHashMap<>(1024);
    private final byte[] lock = new byte[0];
    private final WatchResponse.Builder builder = WatchResponse.newBuilder();
    private final WatchType watchType;

    protected AbstractWatchSubject(WatchType watchType) {
        this.watchType = watchType;
    }

    void addObserver(Long watcherId, StreamObserver<WatchResponse> responseObserver) {
        synchronized (this.watcherHolder) {

            if (this.watcherHolder.containsKey(watcherId)) {
                responseObserver.onError(
                        new Exception(
                                "The watcher-id[" + watcherId + "] of " + this.watchType.name()
                                + " subject has been existing, please unwatch it first"));
                return;
            }

            log.info("Adding a " + this.watchType + "'s watcher, watcher-id is [" + watcherId +
                     "].");
            this.watcherHolder.put(watcherId, responseObserver);
        }

    }

    void removeObserver(Long watcherId, StreamObserver<WatchResponse> responseObserver) {
        synchronized (this.watcherHolder) {
            log.info("Removing a " + this.watchType + "'s watcher, watcher-id is [" + watcherId +
                     "].");
            this.watcherHolder.remove(watcherId);
        }
        responseObserver.onCompleted();
    }

    abstract String toNoticeString(WatchResponse res);

    public void notifyError(int code, String message) {
        synchronized (lock) {
            Iterator<Map.Entry<Long, StreamObserver<WatchResponse>>> iter =
                    watcherHolder.entrySet().iterator();
            while (iter.hasNext()) {
                Map.Entry<Long, StreamObserver<WatchResponse>> entry = iter.next();
                Long watcherId = entry.getKey();
                WatchResponse res = this.builder.setWatcherId(watcherId).build();
                try {
                    entry.getValue().onError(Status.fromCodeValue(code).withDescription(message)
                                                   .asRuntimeException());
                } catch (Throwable e) {
                    // log.error("Failed to send " + this.watchType.name() + "'s error message ["
                    // + toNoticeString(res)
                    //        + "] to watcher[" + watcherId + "].", e);
                }
            }
        }
    }

    protected void notifyWatcher(WatchResponse.Builder response) {

        Iterator<Map.Entry<Long, StreamObserver<WatchResponse>>> iter = watcherHolder
                .entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<Long, StreamObserver<WatchResponse>> entry = iter.next();
            Long watcherId = entry.getKey();
            WatchResponse res = response.setWatcherId(watcherId).build();
            try {
                synchronized (lock) {
                    entry.getValue().onNext(res);
                }
            } catch (Throwable e) {
                try {
                    String msg = JsonFormat.printer().print(res);
                    log.error(
                            "failed to send to watcher [{}] with notice {} for ",
                            msg, toNoticeString(res), watcherId, e);
                } catch (Exception ex) {

                }
            }
        }
    }

    protected void notifyWatcher(Consumer<WatchResponse.Builder> c) {
        synchronized (lock) {

            if (c == null) {
                log.error(this.watchType.name() +
                          "'s notice was abandoned, caused by: notifyWatcher(null)");
                return;
            }

            try {
                c.accept(this.builder.clear());
            } catch (Throwable t) {
                log.error(this.watchType.name() + "'s notice was abandoned, caused by:", t);
                return;
            }

            Iterator<Map.Entry<Long, StreamObserver<WatchResponse>>> iter =
                    watcherHolder.entrySet().iterator();

            while (iter.hasNext()) {
                Map.Entry<Long, StreamObserver<WatchResponse>> entry = iter.next();
                Long watcherId = entry.getKey();
                WatchResponse res = this.builder.setWatcherId(watcherId).build();

                try {
                    entry.getValue().onNext(res);
                } catch (Throwable e) {
                    log.error("Failed to send " + this.watchType.name() + "'s notice[" +
                              toNoticeString(res)
                              + "] to watcher[" + watcherId + "].", e);

                    // TODO: ? try multi-times?
                    iter.remove();

                    log.error("Removed a " + this.watchType.name() + "'s watcher[" + entry.getKey()
                              + "], because of once failure of sending.", e);
                }

            }

        }

    }

}
