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

import java.util.List;
import java.util.function.Consumer;

import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.pd.grpc.pulse.PartitionHeartbeatResponse;
import org.apache.hugegraph.pd.grpc.pulse.PartitionKeyRange;
import org.apache.hugegraph.store.HgStoreEngine;
import org.apache.hugegraph.store.PartitionEngine;
import org.apache.hugegraph.store.meta.Partition;
import org.apache.hugegraph.store.raft.RaftOperation;

import com.google.protobuf.GeneratedMessageV3;

import lombok.extern.slf4j.Slf4j;

/**
 * @date 2023/10/10
 **/
@Slf4j
public class PartitionRangeChangeProcessor extends CommandProcessor {

    public PartitionRangeChangeProcessor(HgStoreEngine storeEngine) {
        super(storeEngine);
    }

    @Override
    public void process(long taskId, Partition partition, GeneratedMessageV3 data,
                        Consumer<Integer> raftCompleteCallback) {
        PartitionEngine engine = storeEngine.getPartitionEngine(partition.getId());
        if (engine != null) {
            PartitionKeyRange partitionKeyRange = (PartitionKeyRange) data;
            var partitionManager = storeEngine.getPartitionManager();
            var localPartition =
                    partitionManager.getPartition(partition.getGraphName(), partition.getId());

            if (localPartition == null) {
                // If partition data is empty, it will not be stored locally
                localPartition = partitionManager.getPartitionFromPD(partition.getGraphName(),
                                                                     partition.getId());
                log.info("onPartitionKeyRangeChanged, get from pd:{}-{} -> {}",
                         partition.getGraphName(), partition.getId(), localPartition);
                if (localPartition == null) {
                    return;
                }
            }

            var newPartition = localPartition.getProtoObj().toBuilder()
                                             .setStartKey(partitionKeyRange.getKeyStart())
                                             .setEndKey(partitionKeyRange.getKeyEnd())
                                             .setState(Metapb.PartitionState.PState_Normal)
                                             .build();
            partitionManager.updatePartition(newPartition, true);

            try {
                sendRaftTask(newPartition.getGraphName(), newPartition.getId(),
                             RaftOperation.SYNC_PARTITION, newPartition,
                             status -> {
                                 log.info(
                                         "onPartitionKeyRangeChanged, {}-{},key range: {}-{} " +
                                         "status{}",
                                         newPartition.getGraphName(),
                                         newPartition.getId(),
                                         partitionKeyRange.getKeyStart(),
                                         partitionKeyRange.getKeyEnd(),
                                         status);
                                 raftCompleteCallback.accept(0);
                             });
                log.info("onPartitionKeyRangeChanged: {}, update to pd", newPartition);
                partitionManager.updatePartitionToPD(List.of(newPartition));
            } catch (PDException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public GeneratedMessageV3 getTaskMeta(PartitionHeartbeatResponse instruct) {
        if (instruct.hasKeyRange()) {
            return instruct.getKeyRange();
        }
        return null;
    }
}
