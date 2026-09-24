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

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.pd.grpc.Metapb.GraphMode;
import org.apache.hugegraph.store.grpc.common.Key;
import org.apache.hugegraph.store.grpc.common.Kv;
import org.apache.hugegraph.store.grpc.common.ResCode;
import org.apache.hugegraph.store.grpc.common.ResStatus;
import org.apache.hugegraph.store.grpc.session.BatchEntry;
import org.apache.hugegraph.store.grpc.session.BatchGetReq;
import org.apache.hugegraph.store.grpc.session.BatchReq;
import org.apache.hugegraph.store.grpc.session.BatchWriteReq;
import org.apache.hugegraph.store.grpc.session.CleanReq;
import org.apache.hugegraph.store.grpc.session.FeedbackRes;
import org.apache.hugegraph.store.grpc.session.GetReq;
import org.apache.hugegraph.store.grpc.session.GraphReq;
import org.apache.hugegraph.store.grpc.session.HgStoreSessionGrpc;
import org.apache.hugegraph.store.grpc.session.KeyValueResponse;
import org.apache.hugegraph.store.grpc.session.TableReq;
import org.apache.hugegraph.store.grpc.session.ValueResponse;
import org.apache.hugegraph.store.meta.Graph;
import org.apache.hugegraph.store.meta.GraphManager;
import org.apache.hugegraph.store.node.AppConfig;
import org.apache.hugegraph.store.node.util.HgGrpc;
import org.apache.hugegraph.store.node.util.HgStoreNodeUtil;
import org.apache.hugegraph.store.pd.PdProvider;
import org.apache.hugegraph.store.raft.RaftClosure;
import org.apache.hugegraph.store.util.HgStoreConst;
import org.lognet.springboot.grpc.GRpcService;
import org.springframework.beans.factory.annotation.Autowired;

import com.google.protobuf.ByteString;

import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@GRpcService
public class HgStoreSessionImpl extends HgStoreSessionGrpc.HgStoreSessionImplBase {

    @Autowired
    private AppConfig appConfig;
    @Autowired
    private HgStoreNodeService storeService;
    private HgStoreWrapperEx wrapper;
    private PdProvider pdProvider;

    private HgStoreWrapperEx getWrapper() {
        if (this.wrapper == null) {
            synchronized (this) {
                if (this.wrapper == null) {
                    this.wrapper = new HgStoreWrapperEx(
                            storeService.getStoreEngine().getBusinessHandler());
                }
            }
        }
        return this.wrapper;
    }

    private PdProvider getPD() {
        if (pdProvider == null) {
            synchronized (this) {
                if (pdProvider == null) {
                    pdProvider = storeService.getStoreEngine().getPdProvider();
                }
            }
        }
        return pdProvider;
    }

    @Override
    public void get2(GetReq request, StreamObserver<FeedbackRes> responseObserver) {
        String graph = request.getHeader().getGraph();
        String table = request.getTk().getTable();
        byte[] key = request.getTk().getKey().toByteArray();
        int code = request.getTk().getCode();
        byte[] value = getWrapper().doGet(graph, code, table, key);

        FeedbackRes.Builder builder = FeedbackRes.newBuilder();

        FeedbackRes res = null;
        if (value != null) {
            res = builder.setStatus(HgGrpc.success())
                         .setValueResponse(
                                 ValueResponse.newBuilder()
                                              .setValue(ByteString.copyFrom(value))
                         ).build();

        } else {
            res = builder.setStatus(HgGrpc.success())
                         .setStatus(HgGrpc.not())
                         .build();
        }

        responseObserver.onNext(res);
        responseObserver.onCompleted();
    }

    @Override
    public void clean(CleanReq request,
                      StreamObserver<FeedbackRes> responseObserver) {

        String graph = request.getHeader().getGraph();
        int partition = request.getPartition();
        // Send to different raft to execute
        BatchGrpcClosure<FeedbackRes> closure = new BatchGrpcClosure<>(1);
        storeService.addRaftTask(HgStoreNodeService.CLEAN_OP, graph, partition,
                                 request,
                                 closure.newRaftClosure());
        // Waiting for the return result
        closure.waitFinish(responseObserver, r -> closure.selectError(r),
                           appConfig.getRaft().getRpcTimeOut());
    }

    public void doClean(int partId, CleanReq request, RaftClosure response) {
        String graph = request.getHeader().getGraph();
        FeedbackRes.Builder builder = FeedbackRes.newBuilder();
        try {
            if (getWrapper().doClean(graph, partId)) {
                builder.setStatus(HgGrpc.success());
            } else {
                builder.setStatus(HgGrpc.not());
            }
        } catch (Throwable t) {
            String msg = "Failed to doClean, graph: " + graph + "; partitionId = " + partId;
            log.error(msg, t);
            builder.setStatus(HgGrpc.fail(msg));
        }
        GrpcClosure.setResult(response, builder.build());
    }

    @Override
    public void batchGet2(BatchGetReq request, StreamObserver<FeedbackRes> responseObserver) {
        String graph = request.getHeader().getGraph();
        String table = request.getTable();
        FeedbackRes.Builder builder = FeedbackRes.newBuilder();

        List<Key> keyList = request.getKeyList();
        if (keyList == null || keyList.isEmpty()) {
            builder.setStatus(HgGrpc.fail("keys is empty"));
            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();
            return;
        }

        KeyValueResponse.Builder keyValueBuilder = KeyValueResponse.newBuilder();

        int max = keyList.size() - 1;
        AtomicInteger count = new AtomicInteger(-1);
        Kv.Builder kvBuilder = Kv.newBuilder();
        getWrapper().batchGet(graph, table,
                              () -> {
                                  if (count.getAndAdd(1) == max) {
                                      return null;
                                  }

                                  Key key = keyList.get(count.get());
                                  if (log.isDebugEnabled()) {
                                      log.debug("batch-getMetric: " +
                                                HgStoreNodeUtil.toStr(
                                                        key.getKey()
                                                           .toByteArray()));
                                  }
                                  return HgGrpc.toHgPair(key);
                              },
                              (
                                      pair -> {
                                          if (pair.getValue() == null || pair.getKey() == null) {
                                              return;
                                          }
                                          keyValueBuilder.addKv(HgGrpc.toKv(pair, kvBuilder));
                                      }
                              )

        );

        builder.setKeyValueResponse(keyValueBuilder.build());
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();

    }

    @Override
    public void batch(BatchReq request, StreamObserver<FeedbackRes> observer) {
        String graph = request.getHeader().getGraph();
        List<BatchEntry> list = request.getWriteReq().getEntryList();
        PdProvider pd = getPD();
        try {
            GraphManager graphManager = pd.getGraphManager();
            Graph managerGraph = graphManager.getGraph(graph);
            if (managerGraph != null && graph.endsWith("/g")) {
                Metapb.Graph g = managerGraph.getProtoObj();
                if (g == null || g.getGraphState() == null) {
                    g = pd.getPDClient().getGraphWithOutException(graph);
                    managerGraph.setGraph(g);
                }
                if (g != null) {
                    Metapb.GraphState graphState = g.getGraphState();
                    if (graphState != null) {
                        GraphMode graphMode = graphState.getMode();
                        if (graphMode != null &&
                            graphMode.getNumber() == GraphMode.ReadOnly_VALUE) {
                            // When in read-only state, getMetric the latest graph state from pd,
                            // the graph's read-only state will be updated in pd's notification.
                            Metapb.Graph pdGraph =
                                    pd.getPDClient().getGraph(graph);
                            Metapb.GraphState pdGraphState =
                                    pdGraph.getGraphState();
                            if (pdGraphState != null &&
                                pdGraphState.getMode() != null &&
                                pdGraphState.getMode().getNumber() ==
                                GraphMode.ReadOnly_VALUE) {
                                // Confirm that the current state stored in pd is also read-only,
                                // then inserting data is not allowed.
                                throw new PDException(-1,
                                                      "the graph space size " +
                                                      "has " +
                                                      "reached the threshold");
                            }
                            // pd status is inconsistent with local cache, update local cache to
                            // the status in pd
                            managerGraph.setProtoObj(pdGraph);
                        }
                    }
                }
            }
        } catch (PDException e) {
            ResStatus status = ResStatus.newBuilder()
                                        .setCode(ResCode.RES_CODE_EXCESS)
                                        .setMsg(e.getMessage())
                                        .build();
            FeedbackRes feedbackRes = FeedbackRes.newBuilder()
                                                 .setStatus(status)
                                                 .build();
            observer.onNext(feedbackRes);
            observer.onCompleted();
            return;
        }

        // Split data by partition
        Map<Integer, List<BatchEntry>> groups = new HashMap<>();
        list.forEach((entry) -> {
            Key startKey = entry.getStartKey();
            if (startKey.getCode() == HgStoreConst.SCAN_ALL_PARTITIONS_ID) {
                // All Leader partitions
                List<Integer> ids =
                        storeService.getGraphLeaderPartitionIds(graph);
                ids.forEach(id -> {
                    if (!groups.containsKey(id)) {
                        groups.put(id, new LinkedList<>());
                    }
                    groups.get(id).add(entry);
                });
            } else {
                // According to keyCode to query the belonging partition ID, group by partition ID
                Integer partitionId =
                        pd.getPartitionByCode(graph, startKey.getCode())
                          .getId();
                if (!groups.containsKey(partitionId)) {
                    groups.put(partitionId, new LinkedList<>());
                }
                groups.get(partitionId).add(entry);
            }
        });

        // Send to different raft to execute
        BatchGrpcClosure<FeedbackRes> closure =
                new BatchGrpcClosure<>(groups.size());
        groups.forEach((partition, entries) -> {
            storeService.addRaftTask(HgStoreNodeService.BATCH_OP, graph,
                                     partition,
                                     BatchReq.newBuilder()
                                             .setHeader(request.getHeader())
                                             .setWriteReq(
                                                     BatchWriteReq.newBuilder()
                                                                  .addAllEntry(
                                                                          entries))
                                             .build(),
                                     closure.newRaftClosure());
        });

        if (!graph.isEmpty()) {
            log.debug(" batch: waiting raft...");
            // Wait for the return result
            closure.waitFinish(observer, r -> closure.selectError(r),
                               appConfig.getRaft().getRpcTimeOut());
            log.debug(" batch: ended waiting");
        } else {
            log.info(" batch: there is none of raft leader, graph = {}.",
                     request.getHeader().getGraph());
            observer.onNext(
                    FeedbackRes.newBuilder().setStatus(HgGrpc.success())
                               .build());
            observer.onCompleted();
        }
    }

    public void doBatch(int partId, BatchReq request, RaftClosure response) {
        String graph = request.getHeader().getGraph();
        String batchId = request.getBatchId();
        FeedbackRes.Builder builder = FeedbackRes.newBuilder();
        List<BatchEntry> entries = request.getWriteReq().getEntryList();
        try {
            getWrapper().doBatch(graph, partId, entries);
            builder.setStatus(HgGrpc.success());
        } catch (Throwable t) {
            String msg = "Failed to doBatch, graph: " + graph + "; batchId= " + batchId;
            log.error(msg, t);
            builder.setStatus(HgGrpc.fail(msg));
        }
        GrpcClosure.setResult(response, builder.build());
    }

    // private static HgBusinessHandler.Batch toBatch(BatchEntry entry) {
    //    return new HgBusinessHandler.Batch() {
    //        @Override
    //        public BatchOpType getOp() {
    //            return BatchOpType.of(entry.getOpType().getNumber());
    //        }
    //
    //        @Override
    //        public int getKeyCode() {
    //            return entry.getStartKey().getCode();
    //        }
    //
    //        @Override
    //        public String getTable() {
    //            return entry.getTable();
    //        }
    //
    //        @Override
    //        public byte[] getStartKey() {
    //            return entry.getStartKey().getKey().toByteArray();
    //        }
    //
    //        @Override
    //        public byte[] getEndKey() {
    //            return entry.getEndKey().getKey().toByteArray();
    //        }
    //
    //        @Override
    //        public byte[] getValue() {
    //            return entry.getValue().toByteArray();
    //        }
    //    };
    //
    //}

    @Override
    public void table(TableReq request, StreamObserver<FeedbackRes> observer) {
        if (log.isDebugEnabled()) {
            log.debug("table: method = {}, graph = {}, table = {}"
                    , request.getMethod().name()
                    , request.getHeader().getGraph()
                    , request.getTableName()
            );
        }

        String graph = request.getHeader().getGraph();
        // All Leader partitions
        List<Integer> ids = storeService.getGraphLeaderPartitionIds(graph);
        // Split data by partition
        Map<Integer, TableReq> groups = new HashMap<>();
        // Split data by partition
        ids.forEach(id -> {
            groups.put(id, request);
        });

        // Send to different raft for execution
        BatchGrpcClosure<FeedbackRes> closure = new BatchGrpcClosure<>(groups.size());
        groups.forEach((partition, entries) -> {
            storeService.addRaftTask(HgStoreNodeService.TABLE_OP, graph, partition,
                                     TableReq.newBuilder(request).build(),
                                     closure.newRaftClosure());
        });

        if (!groups.isEmpty()) {
            //   log.info(" table waiting raft...");
            // Wait for the return result
            closure.waitFinish(observer, r -> closure.selectError(r),
                               appConfig.getRaft().getRpcTimeOut());
            //  log.info(" table ended waiting raft");
        } else {
            //   log.info(" table none leader logic");
            ResStatus status = null;

            switch (request.getMethod()) {
                case TABLE_METHOD_EXISTS:
                    status = HgGrpc.not();
                    break;
                default:
                    status = HgGrpc.success();
            }

            //     log.info(" table none leader status: {}", status.getCode());
            observer.onNext(FeedbackRes.newBuilder().setStatus(status).build());
            observer.onCompleted();
        }

    }

    public void doTable(int partId, TableReq request, RaftClosure response) {
        if (log.isDebugEnabled()) {
            log.debug(" - doTable[{}]: graph = {}, table = {}"
                    , request.getMethod().name()
                    , request.getHeader().getGraph()
                    , request.getTableName()
            );
        }

        FeedbackRes.Builder builder = FeedbackRes.newBuilder();

        try {
            log.debug(" - starting wrapper:doTable ");
            if (getWrapper().doTable(partId,
                                     request.getMethod(),
                                     request.getHeader().getGraph(),
                                     request.getTableName())) {
                builder.setStatus(HgGrpc.success());
            } else {
                builder.setStatus(HgGrpc.not());
            }
            log.debug(" - ended wrapper:doTable ");
        } catch (Throwable t) {
            String msg = "Failed to invoke doTable[ "
                         + request.getMethod().name() + " ], graph="
                         + request.getHeader().getGraph() + " , table="
                         + request.getTableName();
            log.error(msg, t);
            builder.setStatus(HgGrpc.fail(msg));
        }
        log.debug(" - starting GrpcClosure:setResult ");
        GrpcClosure.setResult(response, builder.build());
        log.debug(" - ended GrpcClosure:setResult ");
    }

    @Override
    public void graph(GraphReq request, StreamObserver<FeedbackRes> observer) {
        if (log.isDebugEnabled()) {
            log.debug("graph: method = {}, graph = {}, table = {}"
                    , request.getMethod().name()
                    , request.getHeader().getGraph()
                    , request.getGraphName()
            );
        }

        String graph = request.getHeader().getGraph();
        // All Leader partitions
        List<Integer> ids = storeService.getGraphLeaderPartitionIds(graph);
        // Split data by partition
        Map<Integer, GraphReq> groups = new HashMap<>();
        // Split data by partitioning
        ids.forEach(id -> {
            groups.put(id, request);
        });

        // Send to different raft for execution
        BatchGrpcClosure<FeedbackRes> closure = new BatchGrpcClosure<>(groups.size());
        groups.forEach((partition, entries) -> {
            storeService.addRaftTask(HgStoreNodeService.GRAPH_OP, graph, partition,
                                     GraphReq.newBuilder(request).build(),
                                     closure.newRaftClosure());
        });

        if (!groups.isEmpty()) {
            // Waiting for the return result
            closure.waitFinish(observer, r -> closure.selectError(r),
                               appConfig.getRaft().getRpcTimeOut());

        } else {
            observer.onNext(FeedbackRes.newBuilder().setStatus(HgGrpc.success()).build());
            observer.onCompleted();
        }

    }

    public void doGraph(int partId, GraphReq request, RaftClosure response) {
        if (log.isDebugEnabled()) {
            log.debug(" - doGraph[{}]: graph = {}, table = {}"
                    , request.getMethod().name()
                    , request.getHeader().getGraph()
                    , request.getGraphName()
            );
        }

        FeedbackRes.Builder builder = FeedbackRes.newBuilder();

        try {
            if (getWrapper().doGraph(partId,
                                     request.getMethod(),
                                     request.getHeader().getGraph())) {
                builder.setStatus(HgGrpc.success());
            } else {
                builder.setStatus(HgGrpc.not());
            }
        } catch (Throwable t) {
            String msg = "Failed to invoke doGraph[ "
                         + request.getMethod().name() + " ], graph="
                         + request.getHeader().getGraph();
            log.error(msg, t);
            builder.setStatus(HgGrpc.fail(msg));
        }
        GrpcClosure.setResult(response, builder.build());
    }
}
