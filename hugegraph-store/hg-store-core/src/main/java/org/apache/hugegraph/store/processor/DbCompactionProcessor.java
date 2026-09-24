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

package org.apache.hugegraph.store.processor;

import java.util.function.Consumer;

import org.apache.hugegraph.pd.grpc.pulse.DbCompaction;
import org.apache.hugegraph.pd.grpc.pulse.PartitionHeartbeatResponse;
import org.apache.hugegraph.store.HgStoreEngine;
import org.apache.hugegraph.store.PartitionEngine;
import org.apache.hugegraph.store.cmd.request.DbCompactionRequest;
import org.apache.hugegraph.store.meta.Partition;
import org.apache.hugegraph.store.raft.RaftOperation;

import com.google.protobuf.GeneratedMessageV3;

import lombok.extern.slf4j.Slf4j;

/**
 * @date 2023/10/10
 **/
@Slf4j
public class DbCompactionProcessor extends CommandProcessor {

    public DbCompactionProcessor(HgStoreEngine storeEngine) {
        super(storeEngine);
    }

    @Override
    public void process(long taskId, Partition partition, GeneratedMessageV3 data,
                        Consumer<Integer> raftCompleteCallback) {
        PartitionEngine engine = storeEngine.getPartitionEngine(partition.getId());
        if (engine != null) {
            DbCompaction dbCompaction = (DbCompaction) data;
            DbCompactionRequest dbCompactionRequest = new DbCompactionRequest();
            dbCompactionRequest.setPartitionId(partition.getId());
            dbCompactionRequest.setTableName(dbCompaction.getTableName());
            dbCompactionRequest.setGraphName(partition.getGraphName());

            sendRaftTask(partition.getGraphName(), partition.getId(), RaftOperation.DB_COMPACTION,
                         dbCompactionRequest,
                         status -> {
                             log.info("onRocksdbCompaction {}-{} sync partition status is {}",
                                      partition.getGraphName(), partition.getId(), status);
                             raftCompleteCallback.accept(0);
                         }
            );
        }
    }

    @Override
    public GeneratedMessageV3 getTaskMeta(PartitionHeartbeatResponse instruct) {
        if (instruct.hasDbCompaction()) {
            return instruct.getDbCompaction();
        }
        return null;
    }
}
