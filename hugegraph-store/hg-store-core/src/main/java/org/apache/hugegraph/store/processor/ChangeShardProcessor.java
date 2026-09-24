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
import org.apache.hugegraph.pd.grpc.pulse.ChangeShard;
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
public class ChangeShardProcessor extends CommandProcessor {

    public ChangeShardProcessor(HgStoreEngine storeEngine) {
        super(storeEngine);
    }

    @Override
    public void process(long taskId, Partition partition, GeneratedMessageV3 data,
                        Consumer<Integer> raftCompleteCallback) {
        PartitionEngine engine = storeEngine.getPartitionEngine(partition.getId());

        if (engine != null) {
            log.info("Partition {}-{} receive change shard message, {}", partition.getGraphName(),
                     partition.getId(), data);
            String graphName = partition.getGraphName();
            int partitionId = partition.getId();
            MetaTask.Task task = MetaTask.Task.newBuilder()
                                              .setId(taskId)
                                              .setPartition(partition.getProtoObj())
                                              .setType(MetaTask.TaskType.Change_Shard)
                                              .setState(MetaTask.TaskState.Task_Ready)
                                              .setChangeShard((ChangeShard) data)
                                              .build();

            engine.doChangeShard(task, status -> {
                log.info("Partition {}-{} change shard complete, status is {}",
                         graphName, partitionId, status);
                raftCompleteCallback.accept(0);
            });
        }
    }

    @Override
    public GeneratedMessageV3 getTaskMeta(PartitionHeartbeatResponse instruct) {
        if (instruct.hasChangeShard()) {
            return instruct.getChangeShard();
        }
        return null;
    }
}
