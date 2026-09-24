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

import static org.apache.hugegraph.pd.common.HgAssert.isArgumentNotNull;
import static org.apache.hugegraph.pd.grpc.Pdpb.ErrorType.NOT_LEADER;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.hugegraph.pd.common.HgAssert;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.pd.grpc.pulse.PartitionHeartbeatRequest;
import org.apache.hugegraph.pd.grpc.pulse.PartitionHeartbeatResponse;
import org.apache.hugegraph.pd.grpc.pulse.PdInstructionResponse;
import org.apache.hugegraph.pd.grpc.pulse.PdInstructionType;
import org.apache.hugegraph.pd.grpc.pulse.PulseCreateRequest;
import org.apache.hugegraph.pd.grpc.pulse.PulseNoticeRequest;
import org.apache.hugegraph.pd.grpc.pulse.PulseRequest;
import org.apache.hugegraph.pd.grpc.pulse.PulseResponse;
import org.apache.hugegraph.pd.grpc.pulse.PulseType;
import org.apache.hugegraph.pd.notice.NoticeBroadcaster;
import org.apache.hugegraph.pd.raft.RaftEngine;
import org.apache.hugegraph.pd.util.IdUtil;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Parser;

import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@ThreadSafe
public class PDPulseSubject {

    private final static long NOTICE_EXPIRATION_TIME = 30 * 60 * 1000;
    private final static int RETRYING_PERIOD_SECONDS = 60;
    private final static Map<String, AbstractObserverSubject> subjectHolder =
            new ConcurrentHashMap<>();
    private final static ConcurrentLinkedQueue<NoticeBroadcaster> broadcasterQueue =
            new ConcurrentLinkedQueue<>();
    private final static ScheduledExecutorService scheduledExecutor =
            Executors.newScheduledThreadPool(1);

    private static Supplier<List<Metapb.QueueItem>> queueRetrieveFunction =
            () -> Collections.emptyList();
    private static Function<Metapb.QueueItem, Boolean> queueDurableFunction = (e) -> true;
    private static Function<String, Boolean> queueRemoveFunction = (e) -> true;

    static {
        subjectHolder.put(PulseType.PULSE_TYPE_PARTITION_HEARTBEAT.name(),
                          new PartitionHeartbeatSubject());
        subjectHolder.put(PulseType.PULSE_TYPE_PD_INSTRUCTION.name(), new PdInstructionSubject());
        // add some other type here...
        // ...
    }

    //Schedule tasks
    static {
        scheduledExecutor.scheduleAtFixedRate(() -> doSchedule(), 0, RETRYING_PERIOD_SECONDS,
                                              TimeUnit.SECONDS);
    }

    private static void doSchedule() {
        appendQueue();
        expireQueue();
        //retry
        broadcasterQueue.forEach(e -> {
            e.notifying();
        });
    }

    private static void appendQueue() {
        broadcasterQueue.addAll(
                getQueueItems()
                        .parallelStream()
                        .filter(e -> !broadcasterQueue
                                .stream()
                                .anyMatch(b -> e.getItemId().equals(b.getDurableId()))
                        ).map(e -> createBroadcaster(e))
                        .peek(e -> log.info("Appending notice: {}", e))
                        .filter(e -> e != null)
                        .collect(Collectors.toList())
        );
    }

    private static void expireQueue() {
        broadcasterQueue.removeIf(e -> {
            if (System.currentTimeMillis() - e.getTimestamp() >= NOTICE_EXPIRATION_TIME) {
                log.info("Notice was expired, trying to remove, notice: {}", e);
                return e.doRemoveDurable();
            } else {
                return false;
            }
        });
    }

    private static List<Metapb.QueueItem> getQueueItems() {
        try {
            return queueRetrieveFunction.get();
        } catch (Throwable t) {
            log.error("Failed to retrieve queue from queueRetrieveFunction, cause by:", t);
        }

        return Collections.emptyList();
    }

    public static void setQueueRetrieveFunction(
            Supplier<List<Metapb.QueueItem>> queueRetrieveFunction) {
        HgAssert.isArgumentNotNull(queueRetrieveFunction, "queueRetrieveFunction");
        PDPulseSubject.queueRetrieveFunction = queueRetrieveFunction;
    }

    public static void setQueueDurableFunction(
            Function<Metapb.QueueItem, Boolean> queueDurableFunction) {
        HgAssert.isArgumentNotNull(queueDurableFunction, "queueDurableFunction");
        PDPulseSubject.queueDurableFunction = queueDurableFunction;
    }

    public static void setQueueRemoveFunction(Function<String, Boolean> queueRemoveFunction) {
        HgAssert.isArgumentNotNull(queueRemoveFunction, "queueRemoveFunction");
        PDPulseSubject.queueRemoveFunction = queueRemoveFunction;
    }

    /**
     * Add a responseObserver of client
     *
     * @param responseObserver
     * @return
     */
    public static StreamObserver<PulseRequest> addObserver(
            StreamObserver<PulseResponse> responseObserver) {
        isArgumentNotNull(responseObserver, "responseObserver");
        return new PDPulseStreamObserver(responseObserver);
    }

    /**
     * Send Notice to pd-client
     *
     * @param responseBuilder
     */
    public static void notifyClient(PartitionHeartbeatResponse.Builder responseBuilder) {
        HgAssert.isArgumentNotNull(responseBuilder, "responseBuilder");
        notifyClient(responseBuilder.build());
    }

    private static void notifyClient(PartitionHeartbeatResponse response) {
        doBroadcast(createBroadcaster(response));
    }

    public static void notifyClient(PdInstructionResponse response) {
        doBroadcast(createBroadcaster(response));
    }

    private static void doBroadcast(NoticeBroadcaster broadcaster) {
        broadcasterQueue.add(broadcaster.notifying());
    }

    private static AbstractObserverSubject getSubject(PulseType pulseType) {
        return subjectHolder.get(pulseType.name());
    }

    private static NoticeBroadcaster createBroadcaster(Metapb.QueueItem item) {
        PartitionHeartbeatResponse notice = toNotice(item);
        if (notice == null) {
            return null;
        }
        NoticeBroadcaster res = createBroadcaster(notice);
        res.setDurableId(item.getItemId());
        res.setTimestamp(item.getTimestamp());
        return res;
    }

    private static NoticeBroadcaster createBroadcaster(PartitionHeartbeatResponse notice) {
        return NoticeBroadcaster.of(getNoticeSupplier(notice))
                                .setDurableSupplier(getDurableSupplier(notice))
                                .setRemoveFunction(getRemoveFunction());
    }

    private static NoticeBroadcaster createBroadcaster(PdInstructionResponse notice) {
        return NoticeBroadcaster.of(getNoticeSupplier(notice))
                                .setDurableSupplier(getDurableSupplier(notice))
                                .setRemoveFunction(getRemoveFunction());
    }

    // public static Supplier<Long> getNoticeSupplier(PartitionHeartbeatResponse notice) {
    // TODO: PartitionHeartbeatSubject.class -> T
    //    return () -> getSubject(PulseType.PULSE_TYPE_PARTITION_HEARTBEAT,
    //    PartitionHeartbeatSubject.class)
    //            .notifyClient(notice);
    // }

    public static <T extends com.google.protobuf.GeneratedMessageV3> Supplier<Long> getNoticeSupplier(
            T notice) {
        PulseType type;
        if (notice instanceof PdInstructionResponse) {
            type = PulseType.PULSE_TYPE_PD_INSTRUCTION;
        } else if (notice instanceof PartitionHeartbeatResponse) {
            type = PulseType.PULSE_TYPE_PARTITION_HEARTBEAT;
        } else {
            throw new IllegalArgumentException("Unknown pulse type " + notice.getClass().getName());
        }
        return () -> getSubject(type).notifyClient(notice);
    }

    private static Supplier<String> getDurableSupplier(
            com.google.protobuf.GeneratedMessageV3 notice) {
        return () -> {
            Metapb.QueueItem queueItem = toQueueItem(notice);
            String res = null;

            try {
                if (queueDurableFunction.apply(queueItem)) {
                    res = queueItem.getItemId();
                } else {
                    log.error(
                            "Failed to persist queue-item that contained " +
                            "PartitionHeartbeatResponse: {}"
                            , notice);
                }
            } catch (Throwable t) {
                log.error("Failed to invoke queueDurableFunction, cause by:", t);
            }

            return res;
        };
    }

    private static Function<String, Boolean> getRemoveFunction() {
        return s -> {
            boolean flag = false;

            try {
                flag = queueRemoveFunction.apply(s);
            } catch (Throwable t) {
                log.error("Failed to invoke queueRemoveFunction, cause by:", t);
            }

            return flag;
        };
    }

    private static Metapb.QueueItem toQueueItem(com.google.protobuf.GeneratedMessageV3 notice) {
        return Metapb.QueueItem.newBuilder()
                               .setItemId(IdUtil.createMillisStr())
                               .setItemClass(notice.getClass().getTypeName())
                               .setItemContent(notice.toByteString())
                               .setTimestamp(System.currentTimeMillis())
                               .build();
    }

    private static PartitionHeartbeatResponse toNotice(Metapb.QueueItem item) {
        Parser<PartitionHeartbeatResponse> parser = PartitionHeartbeatResponse.parser();
        PartitionHeartbeatResponse buf = null;
        try {
            buf = parser.parseFrom(item.getItemContent());
        } catch (InvalidProtocolBufferException t) {
            log.error("Failed to parse queue-item to PartitionHeartbeatResponse, cause by:", t);
        }
        return buf;
    }

    public static void notifyError(int code, String message) {
        subjectHolder.forEach((k, v) -> {
            v.notifyError(code, message);
        });
    }

    /**
     * Adding notice listener, the notice is come from pd-client.
     *
     * @param listener
     */
    public static void listenPartitionHeartbeat(PulseListener<PartitionHeartbeatRequest> listener) {
        subjectHolder.get(PulseType.PULSE_TYPE_PARTITION_HEARTBEAT.name())
                     .addListener(createListenerId(), listener);
    }

    private static Long createListenerId() {
        // TODO: Maybe some other way...
        return createObserverId();
    }

    private static Long createObserverId() {
        return IdUtil.createMillisId();
    }

    /* inner classes below */

    private static class PDPulseStreamObserver implements StreamObserver<PulseRequest> {

        private final StreamObserver<PulseResponse> responseObserver;
        private AbstractObserverSubject subject;
        private Long observerId;

        PDPulseStreamObserver(StreamObserver<PulseResponse> responseObserver) {
            this.responseObserver = responseObserver;
        }

        private void cancelObserver() {

            if (this.subject == null) {
                this.responseObserver.onError(
                        new Exception("Invoke cancel-observer before create-observer."));
                return;
            }

            this.subject.removeObserver(this.observerId, this.responseObserver);
        }

        private void addObserver(PulseCreateRequest request) {
            if (this.subject != null) {
                return;
            }

            PulseType pulseType = getPulseType(request);
            if (pulseType == null) {
                return;
            }

            this.subject = getSubject(pulseType);
            this.observerId = createObserverId();

            this.subject.addObserver(this.observerId, this.responseObserver);
        }

        private void ackNotice(long noticeId, long observerId) {
            // log.info("ack noticeId, noticeId: {}, observerId: {}, ts:{}",
            // noticeId,observerId, System.currentTimeMillis());
            broadcasterQueue.removeIf(e -> e.checkAck(noticeId));
        }

        private PulseType getPulseType(PulseCreateRequest request) {
            PulseType pulseType = request.getPulseType();

            if (pulseType.equals(PulseType.PULSE_TYPE_UNKNOWN)) {
                this.responseObserver.onError(new Exception("unknown pulse type."));
                return null;
            }

            return pulseType;
        }

        private AbstractObserverSubject getSubject(PulseType pulseType) {
            AbstractObserverSubject subject = subjectHolder.get(pulseType.name());

            if (subject == null) {
                responseObserver.onError(
                        new Exception("Unsupported pulse-type: " + pulseType.name()));
                return null;
            }

            return subject;
        }

        private void handleNotice(PulseNoticeRequest noticeRequest) {
            try {
                subject.handleClientNotice(noticeRequest);
            } catch (Exception e) {
                if (e instanceof PDException) {
                    var pde = (PDException) e;
                    if (pde.getErrorCode() == NOT_LEADER.getNumber()) {
                        try {
                            log.info("send change leader command to watch, due to ERROR-100", pde);
                            notifyClient(PdInstructionResponse.newBuilder()
                                                              .setInstructionType(
                                                                      PdInstructionType.CHANGE_TO_FOLLOWER)
                                                              .setLeaderIp(RaftEngine.getInstance()
                                                                                     .getLeaderGrpcAddress())
                                                              .build());
                        } catch (ExecutionException | InterruptedException ex) {
                            log.error("send notice to observer failed, ", ex);
                        }
                    }
                } else {
                    log.error("handleNotice error", e);
                }
            }
        }

        @Override
        public void onNext(PulseRequest pulseRequest) {

            if (pulseRequest.hasCreateRequest()) {
                this.addObserver(pulseRequest.getCreateRequest());
                return;
            }

            if (pulseRequest.hasCancelRequest()) {
                this.cancelObserver();
                return;
            }

            if (pulseRequest.hasNoticeRequest()) {
                this.handleNotice(pulseRequest.getNoticeRequest());
            }

            if (pulseRequest.hasAckRequest()) {
                this.ackNotice(pulseRequest.getAckRequest().getNoticeId()
                        , pulseRequest.getAckRequest().getObserverId());
            }
        }

        @Override
        public void onError(Throwable throwable) {
            log.error("cancelObserver : ", throwable);
            this.cancelObserver();
        }

        @Override
        public void onCompleted() {
            this.cancelObserver();
        }

    }

}
