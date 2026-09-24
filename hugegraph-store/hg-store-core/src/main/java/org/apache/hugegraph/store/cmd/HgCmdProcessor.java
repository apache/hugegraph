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

package org.apache.hugegraph.store.cmd;

import java.io.Serializable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.hugegraph.store.HgStoreEngine;
import org.apache.hugegraph.store.PartitionEngine;
import org.apache.hugegraph.store.cmd.request.BatchPutRequest;
import org.apache.hugegraph.store.cmd.request.BlankTaskRequest;
import org.apache.hugegraph.store.cmd.request.CleanDataRequest;
import org.apache.hugegraph.store.cmd.request.CreateRaftRequest;
import org.apache.hugegraph.store.cmd.request.DestroyRaftRequest;
import org.apache.hugegraph.store.cmd.request.GetStoreInfoRequest;
import org.apache.hugegraph.store.cmd.request.RedirectRaftTaskRequest;
import org.apache.hugegraph.store.cmd.request.UpdatePartitionRequest;
import org.apache.hugegraph.store.cmd.response.BatchPutResponse;
import org.apache.hugegraph.store.cmd.response.CleanDataResponse;
import org.apache.hugegraph.store.cmd.response.CreateRaftResponse;
import org.apache.hugegraph.store.cmd.response.DefaultResponse;
import org.apache.hugegraph.store.cmd.response.DestroyRaftResponse;
import org.apache.hugegraph.store.cmd.response.GetStoreInfoResponse;
import org.apache.hugegraph.store.cmd.response.RedirectRaftTaskResponse;
import org.apache.hugegraph.store.cmd.response.UpdatePartitionResponse;
import org.apache.hugegraph.store.meta.Partition;
import org.apache.hugegraph.store.raft.RaftClosure;
import org.apache.hugegraph.store.raft.RaftOperation;
import org.apache.hugegraph.store.util.HgRaftError;

import com.alipay.sofa.jraft.rpc.RpcContext;
import com.alipay.sofa.jraft.rpc.RpcProcessor;
import com.alipay.sofa.jraft.rpc.RpcServer;

import lombok.extern.slf4j.Slf4j;

/**
 * Snapshot synchronization rpc processor, after the leader completes batch storage, reads the
 * newly added kv based on seqnum and sends it in batches to the follower.
 *
 * @param <T>
 */
@Slf4j
public class HgCmdProcessor<T extends HgCmdBase.BaseRequest> implements RpcProcessor<T> {

    private final Class<?> requestClass;
    private final HgStoreEngine engine;

    public HgCmdProcessor(Class<?> requestClass, HgStoreEngine engine) {
        this.requestClass = requestClass;
        this.engine = engine;
    }

    public static void registerProcessor(final RpcServer rpcServer, final HgStoreEngine engine) {
        rpcServer.registerProcessor(new HgCmdProcessor<>(GetStoreInfoRequest.class, engine));
        rpcServer.registerProcessor(new HgCmdProcessor<>(BatchPutRequest.class, engine));
        rpcServer.registerProcessor(new HgCmdProcessor<>(CleanDataRequest.class, engine));
        rpcServer.registerProcessor(new HgCmdProcessor<>(UpdatePartitionRequest.class, engine));
        rpcServer.registerProcessor(new HgCmdProcessor<>(CreateRaftRequest.class, engine));
        rpcServer.registerProcessor(new HgCmdProcessor<>(DestroyRaftRequest.class, engine));
        rpcServer.registerProcessor(new HgCmdProcessor<>(BlankTaskRequest.class, engine));
        rpcServer.registerProcessor(new HgCmdProcessor<>(ProcessBuilder.Redirect.class, engine));
    }

    @Override
    public void handleRequest(RpcContext rpcCtx, T request) {
        HgCmdBase.BaseResponse response = null;
        switch (request.magic()) {
            case HgCmdBase.GET_STORE_INFO: {
                response = new GetStoreInfoResponse();
                handleGetStoreInfo((GetStoreInfoRequest) request, (GetStoreInfoResponse) response);
                break;
            }
            case HgCmdBase.BATCH_PUT: {
                response = new BatchPutResponse();
                handleBatchPut((BatchPutRequest) request, (BatchPutResponse) response);
                break;
            }
            case HgCmdBase.CLEAN_DATA: {
                response = new CleanDataResponse();
                handleCleanData((CleanDataRequest) request, (CleanDataResponse) response);
                break;
            }
            case HgCmdBase.RAFT_UPDATE_PARTITION: {
                response = new UpdatePartitionResponse();
                handleUpdatePartition((UpdatePartitionRequest) request,
                                      (UpdatePartitionResponse) response);
                break;
            }
            case HgCmdBase.CREATE_RAFT: {
                response = new CreateRaftResponse();
                handleCreateRaft((CreateRaftRequest) request, (CreateRaftResponse) response);
                break;
            }
            case HgCmdBase.DESTROY_RAFT: {
                response = new DestroyRaftResponse();
                handleDestroyRaft((DestroyRaftRequest) request, (DestroyRaftResponse) response);
                break;
            }
            case HgCmdBase.BLANK_TASK: {
                response = new DefaultResponse();
                addBlankTask((BlankTaskRequest) request, (DefaultResponse) response);
                break;
            }
            case HgCmdBase.REDIRECT_RAFT_TASK: {
                response = new RedirectRaftTaskResponse();
                handleRedirectRaftTask((RedirectRaftTaskRequest) request,
                                       (RedirectRaftTaskResponse) response);
                break;
            }
            default: {
                log.warn("HgCmdProcessor magic {} is not recognized ", request.magic());
            }
        }
        rpcCtx.sendResponse(response);
    }

    @Override
    public String interest() {
        return this.requestClass.getName();
    }

    public void handleGetStoreInfo(GetStoreInfoRequest request, GetStoreInfoResponse response) {
        response.setStore(engine.getPartitionManager().getStore());
        response.setStatus(Status.OK);
    }

    public void handleUpdatePartition(UpdatePartitionRequest request,
                                      UpdatePartitionResponse response) {
        raftSyncTask(request, response, RaftOperation.RAFT_UPDATE_PARTITION);
    }

    public void handleBatchPut(BatchPutRequest request, BatchPutResponse response) {
        raftSyncTask(request, response, RaftOperation.IN_WRITE_OP);
    }

    public void handleCleanData(CleanDataRequest request, CleanDataResponse response) {
        raftSyncTask(request, response, RaftOperation.IN_CLEAN_OP);
    }

    public void handleCreateRaft(CreateRaftRequest request, CreateRaftResponse response) {
        log.info("CreateRaftNode rpc call received, {}, {}", request.getPartitions(),
                 request.getConf());
        request.getPartitions().forEach(partition -> {
            engine.createPartitionEngine(new Partition(partition), request.getConf());
        });
        response.setStatus(Status.OK);
    }

    public void handleDestroyRaft(DestroyRaftRequest request, DestroyRaftResponse response) {
        log.info("DestroyRaftNode rpc call received, partitionId={}", request.getPartitionId());
        engine.destroyPartitionEngine(request.getPartitionId(), request.getGraphNames());
        response.setStatus(Status.OK);
    }

    public void handleRedirectRaftTask(RedirectRaftTaskRequest request,
                                       RedirectRaftTaskResponse response) {
        log.info("RedirectRaftTaskNode rpc call received, {}", request.getPartitionId());
        raftSyncTask(request.getGraphName(), request.getPartitionId(), request.getRaftOp(),
                     request.getData(), response);
        response.setStatus(Status.OK);
    }

    public void addBlankTask(BlankTaskRequest request, DefaultResponse response) {
        try {
            int partitionId = request.getPartitionId();
            PartitionEngine pe = engine.getPartitionEngine(partitionId);
            if (pe.isLeader()) {
                CountDownLatch latch = new CountDownLatch(1);
                RaftClosure closure = s -> {
                    if (s.isOk()) {
                        response.setStatus(Status.OK);
                    } else {
                        log.error("doBlankTask in cmd with error: {}", s.getErrorMsg());
                        response.setStatus(Status.EXCEPTION);
                    }
                    latch.countDown();
                };
                pe.addRaftTask(RaftOperation.create(RaftOperation.SYNC_BLANK_TASK), closure);
                latch.await();
            } else {
                response.setStatus(Status.LEADER_REDIRECT);
            }
        } catch (Exception e) {
            response.setStatus(Status.EXCEPTION);
        }
    }

    /**
     * raft notify replica synchronization execution
     *
     * @param request
     * @param response
     * @param op
     */
    private void raftSyncTask(HgCmdBase.BaseRequest request, HgCmdBase.BaseResponse response,
                              final byte op) {
        raftSyncTask(request.getGraphName(), request.getPartitionId(), op, request, response);
    }

    private void raftSyncTask(String graph, int partId, byte op, Object raftReq,
                              HgCmdBase.BaseResponse response) {
        CountDownLatch latch = new CountDownLatch(1);
        engine.addRaftTask(graph, partId,
                           RaftOperation.create(op, raftReq), new RaftClosure() {
                    @Override
                    public void run(com.alipay.sofa.jraft.Status status) {
                        Status responseStatus = Status.UNKNOWN;
                        switch (HgRaftError.forNumber(status.getCode())) {
                            case OK:
                                responseStatus = Status.OK;
                                break;
                            case NOT_LEADER:
                                responseStatus = Status.LEADER_REDIRECT;
                                break;
                            case NOT_LOCAL:
                                responseStatus = Status.NO_PARTITION;
                                break;
                            case WAIT_LEADER_TIMEOUT:
                                responseStatus = Status.WAIT_LEADER_TIMEOUT;
                                break;
                            default:
                                responseStatus.setMsg(status.getErrorMsg());
                        }
                        response.setStatus(responseStatus);
                        latch.countDown();
                    }

                    @Override
                    public void onLeaderChanged(Integer partId, Long storeId) {
                        RaftClosure.super.onLeaderChanged(partId, storeId);
                        response.addPartitionLeader(
                                new HgCmdBase.BaseResponse.PartitionLeader(partId, storeId));
                    }
                });
        try {
            latch.await(1, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            log.info("handleBatchPut InterruptedException {}", e);
        }
    }

    public enum Status implements Serializable {
        UNKNOWN(-1, "unknown"),
        OK(0, "ok"),
        COMPLETE(0, "Transmission completed"),
        INCOMPLETE(1, "Incomplete transmission"),
        NO_PARTITION(10, "Partition not found"),
        IO_ERROR(11, "io error"),
        EXCEPTION(12, "exception"),
        DOWN_SNAPSHOT_ERROR(13, "download snapshot error"),
        LEADER_REDIRECT(14, "leader redirect"),
        WAIT_LEADER_TIMEOUT(15, "Waiting for leader timeout"),
        ABORT(100, "Transmission aborted");

        private final int code;
        private String msg;

        Status(int code, String msg) {
            this.code = code;
            this.msg = msg;
        }

        public int getCode() {
            return this.code;
        }

        public String getMsg() {
            return this.msg;
        }

        public Status setMsg(String msg) {
            this.msg = msg;
            return this;
        }

        public boolean isOK() {
            return this.code == 0;
        }
    }
}
