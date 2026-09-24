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

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.hugegraph.store.client.HgStoreNodeManager;
import org.apache.hugegraph.store.client.HgStoreNodeSession;
import org.apache.hugegraph.store.client.HgStoreNotice;
import org.apache.hugegraph.store.client.type.HgNodeStatus;
import org.apache.hugegraph.store.client.type.HgStoreClientException;
import org.apache.hugegraph.store.grpc.common.ResStatus;
import org.apache.hugegraph.store.grpc.session.FeedbackRes;
import org.apache.hugegraph.store.grpc.session.PartitionFaultResponse;
import org.apache.hugegraph.store.grpc.session.PartitionFaultType;
import org.apache.hugegraph.store.grpc.session.PartitionLeader;

import com.google.protobuf.util.JsonFormat;

import io.grpc.Status;
import lombok.extern.slf4j.Slf4j;

/**
 * 2021/11/18
 *
 * @version 0.3.0 on 2022/01/27
 */
@Slf4j
final class NotifyingExecutor {

    private final String graphName;
    private final HgStoreNodeManager nodeManager;
    private final HgStoreNodeSession nodeSession;

    private Map<PartitionFaultType, Consumer<PartitionFaultResponse>> partitionFaultHandlers;

    NotifyingExecutor(String graphName, HgStoreNodeManager nodeManager,
                      HgStoreNodeSession nodeSession) {
        this.graphName = graphName;
        this.nodeManager = nodeManager;
        this.nodeSession = nodeSession;
    }

    private void initHandler() {
        this.partitionFaultHandlers = new HashMap<>();

        this.partitionFaultHandlers.put(
                PartitionFaultType.PARTITION_FAULT_TYPE_NOT_LEADER, notifyPartitionLeaderConsumer()
        );

    }

    <T> Optional<T> invoke(Supplier<FeedbackRes> supplier, Function<FeedbackRes, T> okFunction) {
        FeedbackRes res = null;

        try {
            res = supplier.get();
        } catch (Throwable t) {
            log.error("Failed to invoke: " + supplier.toString() + ", caused " +
                      "by:", t);
            handleErr(t);
            throw err(t);
        }

        if (log.isDebugEnabled()) {
            log.debug("gRPC [{}] status: {}"
                    , this.nodeSession.getStoreNode().getAddress(), res.getStatus().getCode());
        }

        Optional<T> option = null;

        switch (res.getStatus().getCode()) {
            case RES_CODE_OK:
                option = Optional.of(okFunction.apply(res));
                break;
            case RES_CODE_FAIL:
                handleFail(res);
                break;
            case RES_CODE_NOT_EXIST:
                break;
            case RES_CODE_EXCESS:
                normalFail(res);
                break;
            default:
                log.error("gRPC [{}] status-msg: {}"
                        , nodeSession.getStoreNode().getAddress(), res.getStatus().getMsg());
        }

        if (option == null) {
            option = Optional.empty();
        }

        return option;
    }

    private void handleErr(Throwable t) {
        try {
            notifyErrConsumer(HgNodeStatus.NOT_WORK).accept(t);
        } catch (Throwable tt) {
            log.error("Failed to notify error to HgStoreNodeNotifier, cause:", tt);
        }
    }

    private void handleFail(FeedbackRes feedbackRes) {
        Supplier<HgStoreClientException> exSup;

        if (
                (exSup = handlePartitionFault(feedbackRes)) != null
                // add more fault-handler here.
                || (exSup = defaultExceptionSupplier(feedbackRes)) != null
        ) {
            throw exSup.get();
        }

    }

    private void normalFail(FeedbackRes res) {
        ResStatus status = res.getStatus();
        HgStoreClientException ex;
        try {
            String msg = JsonFormat.printer().omittingInsignificantWhitespace()
                                   .print(res);
            ex = err(msg);
        } catch (Exception e) {
            ex = err(status.getCode() + ", " + status.getMsg());
        }
        throw ex;
    }

    private Supplier<HgStoreClientException> defaultExceptionSupplier(FeedbackRes feedbackRes) {
        return () -> HgStoreClientException.of(err(feedbackRes.getStatus().getMsg()));
    }

    private Supplier<HgStoreClientException> handlePartitionFault(
            FeedbackRes feedbackRes) {
        PartitionFaultResponse res = feedbackRes.getPartitionFaultResponse();
        if (res == null) {
            return null;
        }
        if (this.partitionFaultHandlers == null) {
            initHandler();
        }
        Consumer<PartitionFaultResponse> consumer =
                this.partitionFaultHandlers.get(res.getFaultType());
        if (consumer == null) {
            consumer = notifyPartitionConsumer();
        }
        String msg = res.toString();
        if (msg == null || msg.length() == 0) {
            msg = feedbackRes.getStatus().getMsg();
        }
        consumer.accept(res);
        String finalMsg = msg;
        return () -> HgStoreClientException.of(
                err(res.getFaultType() + ", " +
                    finalMsg));
    }

    private HgStoreClientException err(String msg) {
        return err(msg, null);
    }

    private HgStoreClientException err(Throwable t) {
        return err(t.getMessage(), t);
    }

    private HgStoreClientException err(String reason, Throwable t) {
        StringBuilder builder = new StringBuilder().append(
                "{sessionInfo: {" + this.nodeSession.toString() +
                "}, reason: ");
        if (reason.startsWith("{")) {
            builder.append(reason);
        } else {
            builder.append("\"").append(reason).append("\"");
        }
        String msg = builder.append("}").toString();
        if (t != null) {
            return HgStoreClientException.of(msg, t);
        }
        return HgStoreClientException.of(msg);
    }

    private Consumer<PartitionFaultResponse> notifyPartitionLeaderConsumer() {
        return res -> {
            log.info("partitions' leader have changed: [partitionId - leaderId] ");
            nodeManager.notifying(
                    this.graphName,
                    HgStoreNotice.of(this.nodeSession.getStoreNode().getNodeId(),
                                     HgNodeStatus.NOT_PARTITION_LEADER)
                                 .setPartitionLeaders(
                                         res.getPartitionLeadersList()
                                            .stream()
                                            .peek((e) -> {
                                                      log.info("[{} - {}]", e.getPartitionId(),
                                                               e.getLeaderId());
                                                  }
                                            )
                                            .collect(
                                                    Collectors.toMap(
                                                            PartitionLeader::getPartitionId,
                                                            PartitionLeader::getLeaderId
                                                    )
                                            )
                                 )
            );
        };
    }

    private Consumer<PartitionFaultResponse> notifyPartitionConsumer() {
        return notifyPartitionConsumer(HgNodeStatus.PARTITION_COMMON_FAULT);
    }

    private Consumer<PartitionFaultResponse> notifyPartitionConsumer(HgNodeStatus status) {
        return res -> {
            nodeManager.notifying(
                    this.graphName,
                    HgStoreNotice.of(this.nodeSession.getStoreNode().getNodeId(), status)
                                 .setPartitionIds(res.getPartitionIdsList())
            );
        };
    }

    private Consumer<Throwable> notifyErrConsumer(HgNodeStatus status) {
        return t -> {
            HgStoreNotice notice =
                    HgStoreNotice.of(this.nodeSession.getStoreNode().getNodeId(), status,
                                     t.getMessage());
            if (Status.fromThrowable(t).getCode() == Status.Code.UNAVAILABLE) {
                nodeManager.notifying(this.graphName, notice,
                                      this.nodeSession.getStoreNode());
            } else {
                nodeManager.notifying(this.graphName, notice);
            }
        };
    }

}
