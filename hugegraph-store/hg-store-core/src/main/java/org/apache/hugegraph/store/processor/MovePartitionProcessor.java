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

import org.apache.hugegraph.pd.grpc.MetaTask;
import org.apache.hugegraph.pd.grpc.pulse.MovePartition;
import org.apache.hugegraph.pd.grpc.pulse.PartitionHeartbeatResponse;
import org.apache.hugegraph.store.HgStoreEngine;
import org.apache.hugegraph.store.PartitionEngine;
import org.apache.hugegraph.store.meta.Partition;

import com.google.protobuf.GeneratedMessageV3;

import lombok.extern.slf4j.Slf4j;

/**
 * @date 2023/10/10
 **/
@Slf4j
public class MovePartitionProcessor extends CommandProcessor {

    public MovePartitionProcessor(HgStoreEngine storeEngine) {
        super(storeEngine);
    }

    @Override
    public void process(long taskId, Partition partition, GeneratedMessageV3 data,
                        Consumer<Integer> consumer) {
        if (preCheckTaskId(taskId, partition.getId())) {
            return;
        }

        PartitionEngine engine = storeEngine.getPartitionEngine(partition.getId());
        if (engine != null) {
            // Respond first to avoid timeout causing pd to resend
            MovePartition movePartition = (MovePartition) data;
            String graphName = partition.getGraphName();
            int partitionId = partition.getId();
            MetaTask.Task task = MetaTask.Task.newBuilder()
                                              .setId(taskId)
                                              .setPartition(partition.getProtoObj())
                                              .setType(MetaTask.TaskType.Move_Partition)
                                              .setState(MetaTask.TaskState.Task_Ready)
                                              .setMovePartition(movePartition)
                                              .build();
            try {
                engine.moveData(task);
            } catch (Exception e) {
                log.error("Partition {}-{} onMovePartition exception {}", graphName, partitionId,
                          e);
            }
        }
    }

    @Override
    public boolean isRaftTask() {
        return false;
    }

    @Override
    public GeneratedMessageV3 getTaskMeta(PartitionHeartbeatResponse instruct) {
        if (instruct.hasMovePartition()) {
            return instruct.getMovePartition();
        }
        return null;
    }
}
