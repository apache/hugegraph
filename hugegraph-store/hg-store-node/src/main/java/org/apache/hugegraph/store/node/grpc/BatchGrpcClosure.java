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

package org.apache.hugegraph.store.node.grpc;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hugegraph.store.grpc.common.ResCode;
import org.apache.hugegraph.store.grpc.common.ResStatus;
import org.apache.hugegraph.store.grpc.session.FeedbackRes;
import org.apache.hugegraph.store.grpc.session.PartitionFaultResponse;
import org.apache.hugegraph.store.grpc.session.PartitionFaultType;
import org.apache.hugegraph.store.grpc.session.PartitionLeader;
import org.apache.hugegraph.store.raft.RaftClosure;
import org.apache.hugegraph.store.util.HgRaftError;

import com.alipay.sofa.jraft.Status;

import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

/**
 * Batch processing grpc callback wrapper class
 *
 * @param <V>
 */
@Slf4j
class BatchGrpcClosure<V> {

    private final CountDownLatch countDownLatch;
    private final List<Status> errorStatus;
    private final List<V> results;
    private final Map<Integer, Long> leaderMap;

    public BatchGrpcClosure(int count) {
        countDownLatch = new CountDownLatch(count);
        errorStatus = Collections.synchronizedList(new ArrayList<>());
        results = Collections.synchronizedList(new ArrayList<>());
        leaderMap = new ConcurrentHashMap<>();
    }

    public RaftClosure newRaftClosure() {
        return new GrpcClosure<V>() {
            @Override
            public void run(Status status) {
                countDownLatch.countDown();
                if (status.isOk()) {
                    results.add(this.getResult());
                } else {
                    leaderMap.putAll(this.getLeaderMap());
                    errorStatus.add(status);
                }
            }
        };
    }

    public RaftClosure newRaftClosure(Consumer<Status> ok) {
        return new GrpcClosure<V>() {
            @Override
            public void run(Status status) {
                countDownLatch.countDown();
                if (status.isOk()) {
                    results.add(this.getResult());
                } else {
                    leaderMap.putAll(this.getLeaderMap());
                    errorStatus.add(status);
                }
                ok.accept(status);
            }
        };
    }

    /**
     * Not using counter latch
     *
     * @return
     */
    public RaftClosure newClosureNoLatch() {
        return new GrpcClosure<V>() {
            @Override
            public void run(Status status) {
                if (status.isOk()) {
                    results.add(this.getResult());
                } else {
                    leaderMap.putAll(this.getLeaderMap());
                    errorStatus.add(status);
                }
            }
        };
    }

    public PartitionFaultResponse getErrorResponse() {
        PartitionFaultResponse errorResponse;

        if (leaderMap.size() > 0) {
            PartitionFaultResponse.Builder partitionFault =
                    PartitionFaultResponse.newBuilder().setFaultType(
                            PartitionFaultType.PARTITION_FAULT_TYPE_NOT_LEADER);
            leaderMap.forEach((k, v) -> {
                partitionFault.addPartitionLeaders(PartitionLeader.newBuilder()
                                                                  .setPartitionId(k)
                                                                  .setLeaderId(v).build());
            });
            errorResponse = partitionFault.build();
        } else {
            PartitionFaultType faultType = PartitionFaultType.PARTITION_FAULT_TYPE_UNKNOWN;
            switch (HgRaftError.forNumber(errorStatus.get(0).getCode())) {
                case NOT_LEADER:
                    faultType = PartitionFaultType.PARTITION_FAULT_TYPE_NOT_LEADER;
                    break;
                case WAIT_LEADER_TIMEOUT:
                    faultType = PartitionFaultType.PARTITION_FAULT_TYPE_WAIT_LEADER_TIMEOUT;
                    break;
                case NOT_LOCAL:
                    faultType = PartitionFaultType.PARTITION_FAULT_TYPE_NOT_LOCAL;
                    break;
                default:
                    log.error("Unmatchable errorStatus: " + errorStatus);
            }
            errorResponse = PartitionFaultResponse.newBuilder().setFaultType(faultType).build();
        }
        return errorResponse;
    }

    public String getErrorMsg() {
        StringBuilder builder = new StringBuilder();
        errorStatus.forEach(status -> {
            if (!status.isOk()) {
                builder.append(status.getErrorMsg());
                builder.append("\n");
            }
        });
        return builder.toString();
    }

    /**
     * Wait for the raft execution to complete, return the result to grpc
     */
    public void waitFinish(StreamObserver<V> observer, Function<List<V>, V> ok, long timeout) {
        try {
            countDownLatch.await(timeout, TimeUnit.MILLISECONDS);

            if (errorStatus.isEmpty()) {  // No error, merge results
                observer.onNext(ok.apply(results));
            } else {
                observer.onNext((V) FeedbackRes.newBuilder()
                                               .setStatus(ResStatus.newBuilder()
                                                                   .setCode(ResCode.RES_CODE_FAIL)
                                                                   .setMsg(getErrorMsg()))
                                               .setPartitionFaultResponse(this.getErrorResponse())
                                               .build());
            }
        } catch (InterruptedException e) {
            log.error("waitFinish exception: ", e);
            observer.onNext((V) FeedbackRes.newBuilder()
                                           .setStatus(ResStatus.newBuilder()
                                                               .setCode(ResCode.RES_CODE_FAIL)
                                                               .setMsg(e.getLocalizedMessage())
                                                               .build()).build());
        }
        observer.onCompleted();
    }

    /**
     * Select one incorrect result from multiple results, if there are no errors, return the
     * first one.
     */
    public FeedbackRes selectError(List<FeedbackRes> results) {
        if (!CollectionUtils.isEmpty(results)) {
            AtomicReference<FeedbackRes> res = new AtomicReference<>(results.get(0));
            results.forEach(e -> {
                try {
                    if (e.getStatus().getCode() != ResCode.RES_CODE_OK) {
                        res.set(e);
                    }
                } catch (Exception ex) {
                    log.error("{}", ex);
                }
            });
            return res.get();
        } else {
            return FeedbackRes.newBuilder()
                              .setStatus(ResStatus.newBuilder()
                                                  .setCode(ResCode.RES_CODE_OK).build())
                              .build();
        }

    }
}
