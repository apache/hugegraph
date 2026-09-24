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

package org.apache.hugegraph.store.node.task.ttl;

import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.error.RaftError;
import com.google.protobuf.ByteString;
import lombok.extern.slf4j.Slf4j;
import org.apache.hugegraph.pd.grpc.kv.V;
import org.apache.hugegraph.store.HgStoreEngine;
import org.apache.hugegraph.store.PartitionEngine;
import org.apache.hugegraph.store.business.BusinessHandler;
import org.apache.hugegraph.store.cmd.request.DbCompactionRequest;
import org.apache.hugegraph.store.grpc.common.TTLCleanRequest;
import org.apache.hugegraph.store.node.grpc.GrpcClosure;
import org.apache.hugegraph.store.node.grpc.HgStoreNodeService;
import org.apache.hugegraph.store.raft.RaftClosure;
import org.apache.hugegraph.store.raft.RaftOperation;

import java.util.LinkedList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @date 2024/5/7
 **/
@Slf4j
public class RaftTaskSubmitter extends TaskSubmitter {

    public RaftTaskSubmitter(HgStoreNodeService service, BusinessHandler handler) {
        super(service, handler);
    }

    @Override
    public Status submitClean(Integer id, String graph, String table, LinkedList<ByteString> all,
                              AtomicBoolean state, AtomicLong tableCounter,
                              AtomicLong partitionCounter) {
        AtomicReference<Status> result = new AtomicReference<>();
        try {
            TTLCleanRequest cleanRequest =
                    TTLCleanRequest.newBuilder().addAllIds(all).setGraph(graph).setPartitionId(id)
                            .setTable(table).build();
            tableCounter.getAndAdd(all.size());
            CountDownLatch latch = new CountDownLatch(1);
            GrpcClosure c = new GrpcClosure<V>() {
                @Override
                public void run(Status status) {
                    try {
                        if (!status.isOk()) {
                            log.warn("submit task got status: {}", status);
                            state.set(false);
                        } else {
                            partitionCounter.getAndAdd(all.size());
                        }
                        result.set(status);
                    } catch (Exception e) {
                        log.warn("submit task with error:", e);
                        state.set(false);
                        result.set(new Status(RaftError.UNKNOWN, e.getMessage()));
                    } finally {
                        latch.countDown();
                    }
                }
            };
            service.addRaftTask(HgStoreNodeService.TTL_CLEAN_OP, graph, id, cleanRequest, c);
            latch.await();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return result.get();
    }

    @Override
    public Status submitCompaction(Integer id) {
        DbCompactionRequest cr = new DbCompactionRequest();
        cr.setPartitionId(id);
        cr.setTableName("");
        cr.setGraphName("");
        PartitionEngine engine = HgStoreEngine.getInstance().getPartitionEngine(id);
        RaftClosure closure = status -> log.info("ttl compaction:{}, status is {}", id, status);
        RaftOperation operation = RaftOperation.create(RaftOperation.DB_COMPACTION, cr);
        engine.addRaftTask(operation, closure);
        return Status.OK();
    }
}

