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

package org.apache.hugegraph.store;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.grpc.MetaTask;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.pd.grpc.pulse.ChangeShard;
import org.apache.hugegraph.pd.grpc.pulse.CleanPartition;
import org.apache.hugegraph.pd.grpc.pulse.DbCompaction;
import org.apache.hugegraph.pd.grpc.pulse.MovePartition;
import org.apache.hugegraph.pd.grpc.pulse.PartitionKeyRange;
import org.apache.hugegraph.pd.grpc.pulse.SplitPartition;
import org.apache.hugegraph.pd.grpc.pulse.TransferLeader;
import org.apache.hugegraph.store.cmd.request.CleanDataRequest;
import org.apache.hugegraph.store.cmd.request.DbCompactionRequest;
import org.apache.hugegraph.store.meta.MetadataKeyHelper;
import org.apache.hugegraph.store.meta.Partition;
import org.apache.hugegraph.store.pd.PartitionInstructionListener;
import org.apache.hugegraph.store.raft.RaftClosure;
import org.apache.hugegraph.store.raft.RaftOperation;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.util.Utils;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * PD sends partition instruction processor to Store
 */
@Deprecated
public class PartitionInstructionProcessor implements PartitionInstructionListener {

    private static final Logger LOG = Log.logger(PartitionInstructionProcessor.class);
    private final HgStoreEngine storeEngine;
    private final ExecutorService threadPool;

    public PartitionInstructionProcessor(HgStoreEngine storeEngine) {
        this.storeEngine = storeEngine;
        ThreadFactory namedThreadFactory =
                new ThreadFactoryBuilder().setNameFormat("instruct-process-pool-%d").build();
        threadPool = new ThreadPoolExecutor(Runtime.getRuntime().availableProcessors(),
                                            1000000,
                                            180L,
                                            TimeUnit.SECONDS,
                                            new LinkedBlockingQueue<>(1000000),
                                            namedThreadFactory,
                                            new ThreadPoolExecutor.AbortPolicy());
    }

    @Override
    public void onChangeShard(long taskId, Partition partition, ChangeShard changeShard,
                              Consumer<Integer> consumer) {
        PartitionEngine engine = storeEngine.getPartitionEngine(partition.getId());

        if (engine != null) {
            // Clean up all tasks, with failures occurring.
            engine.getTaskManager()
                  .deleteTask(partition.getId(), MetaTask.TaskType.Change_Shard.name());
        }

        if (engine != null && engine.isLeader()) {
            LOG.info("Partition {}-{} Receive change shard message, {}", partition.getGraphName(),
                     partition.getId(), changeShard);
            String graphName = partition.getGraphName();
            int partitionId = partition.getId();
            MetaTask.Task task = MetaTask.Task.newBuilder()
                                              .setId(taskId)
                                              .setPartition(partition.getProtoObj())
                                              .setType(MetaTask.TaskType.Change_Shard)
                                              .setState(MetaTask.TaskState.Task_Ready)
                                              .setChangeShard(changeShard)
                                              .build();
            try {
                storeEngine.addRaftTask(graphName, partitionId,
                                        RaftOperation.create(RaftOperation.SYNC_PARTITION_TASK,
                                                             task),
                                        new RaftClosure() {
                                            @Override
                                            public void run(Status status) {
                                                LOG.info(
                                                        "Partition {}-{} onChangeShard complete, " +
                                                        "status is {}",
                                                        graphName, partitionId, status);
                                                consumer.accept(0);
                                            }
                                        });
            } catch (Exception e) {
                LOG.error("Partition {}-{} onSplitPartition exception {}",
                          graphName, partitionId, e);
            }
        }
    }

    @Override
    public void onTransferLeader(long taskId, Partition partition, TransferLeader transferLeader,
                                 Consumer<Integer> consumer) {
        PartitionEngine engine = storeEngine.getPartitionEngine(partition.getId());
        if (engine != null && engine.isLeader()) {
            consumer.accept(0);
            Utils.runInThread(() -> {
                LOG.info("Partition {}-{} receive TransferLeader instruction, new leader is {}"
                        , partition.getGraphName(), partition.getId(), transferLeader.getShard());
                engine.transferLeader(partition.getGraphName(), transferLeader.getShard());
            });
        }
    }

    /**
     * Leader receives the partition splitting task sent by PD.
     * Added to the raft task queue, task distribution is handled by raft.
     */
    @Override
    public void onSplitPartition(long taskId, Partition partition, SplitPartition splitPartition,
                                 Consumer<Integer> consumer) {
        PartitionEngine engine = storeEngine.getPartitionEngine(partition.getId());

        if (preCheckTaskId(taskId, partition.getId())) {
            return;
        }

        if (engine != null && engine.isLeader()) {
            // Respond first to avoid pd retransmission due to timeout.
            consumer.accept(0);

            String graphName = partition.getGraphName();
            int partitionId = partition.getId();
            MetaTask.Task task = MetaTask.Task.newBuilder()
                                              .setId(taskId)
                                              .setPartition(partition.getProtoObj())
                                              .setType(MetaTask.TaskType.Split_Partition)
                                              .setState(MetaTask.TaskState.Task_Ready)
                                              .setSplitPartition(splitPartition)
                                              .build();
            try {
                threadPool.submit(() -> {
                    engine.moveData(task);
                });
            } catch (Exception e) {
                LOG.error("Partition {}-{} onSplitPartition exception {}",
                          graphName, partitionId, e);
            }
        }
    }

    /**
     * Leader receives the rocksdb compaction task sent by PD
     * Added to the raft task queue, task distribution is handled by raft.
     */
    @Override
    public void onDbCompaction(long taskId, Partition partition, DbCompaction dbCompaction,
                               Consumer<Integer> consumer) {
        PartitionEngine engine = storeEngine.getPartitionEngine(partition.getId());
        if (engine != null && engine.isLeader()) {
            try {
                DbCompactionRequest dbCompactionRequest = new DbCompactionRequest();
                dbCompactionRequest.setPartitionId(partition.getId());
                dbCompactionRequest.setTableName(dbCompaction.getTableName());
                dbCompactionRequest.setGraphName(partition.getGraphName());
                engine.addRaftTask(RaftOperation.create(RaftOperation.DB_COMPACTION,
                                                        dbCompactionRequest),
                                   new RaftClosure() {
                                       @Override
                                       public void run(Status status) {
                                           LOG.info(
                                                   "onRocksdbCompaction {}-{} sync partition " +
                                                   "status is {}",
                                                   partition.getGraphName(), partition.getId(),
                                                   status);
                                       }
                                   }
                );
            } finally {
                consumer.accept(0);
            }
        }
    }

    @Override
    public void onMovePartition(long taskId, Partition partition, MovePartition movePartition,
                                Consumer<Integer> consumer) {
        PartitionEngine engine = storeEngine.getPartitionEngine(partition.getId());

        if (preCheckTaskId(taskId, partition.getId())) {
            return;
        }

        if (engine != null && engine.isLeader()) {
            // Respond first to avoid pd retransmission due to timeout.
            consumer.accept(0);

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
                threadPool.submit(() -> {
                    engine.moveData(task);
                });
            } catch (Exception e) {
                LOG.error("Partition {}-{} onMovePartition exception {}",
                          graphName, partitionId, e);
            }
        }
    }

    @Override
    public void onCleanPartition(long taskId, Partition partition, CleanPartition cleanPartition,
                                 Consumer<Integer> consumer) {

        if (preCheckTaskId(taskId, partition.getId())) {
            return;
        }

        PartitionEngine engine = storeEngine.getPartitionEngine(partition.getId());
        if (engine != null && engine.isLeader()) {
            consumer.accept(0);

            CleanDataRequest
                    request =
                    CleanDataRequest.fromCleanPartitionTask(cleanPartition, partition, taskId);

            storeEngine.addRaftTask(partition.getGraphName(), partition.getId(),
                                    RaftOperation.create(RaftOperation.IN_CLEAN_OP, request),
                                    status -> {
                                        LOG.info(
                                                "onCleanPartition {}-{}, cleanType: {}, " +
                                                "range:{}-{}, status:{}",
                                                partition.getGraphName(),
                                                partition.getId(),
                                                cleanPartition.getCleanType(),
                                                cleanPartition.getKeyStart(),
                                                cleanPartition.getKeyEnd(),
                                                status);
                                    });
        }

    }

    @Override
    public void onPartitionKeyRangeChanged(long taskId, Partition partition,
                                           PartitionKeyRange partitionKeyRange,
                                           Consumer<Integer> consumer) {
        PartitionEngine engine = storeEngine.getPartitionEngine(partition.getId());
        if (engine != null && engine.isLeader()) {
            consumer.accept(0);
            var partitionManager = storeEngine.getPartitionManager();
            var localPartition =
                    partitionManager.getPartition(partition.getGraphName(), partition.getId());

            if (localPartition == null) {
                // If the partition data is empty, it will not be stored locally.
                localPartition = partitionManager.getPartitionFromPD(partition.getGraphName(),
                                                                     partition.getId());
                LOG.info("onPartitionKeyRangeChanged, get from pd:{}-{} -> {}",
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
                engine.addRaftTask(RaftOperation.create(RaftOperation.SYNC_PARTITION, newPartition),
                                   status -> {
                                       LOG.info(
                                               "onPartitionKeyRangeChanged, {}-{},key range: " +
                                               "{}-{} status{}",
                                               newPartition.getGraphName(),
                                               newPartition.getId(),
                                               partitionKeyRange.getKeyStart(),
                                               partitionKeyRange.getKeyEnd(),
                                               status);
                                   });
                LOG.info("onPartitionKeyRangeChanged: {}, update to pd", newPartition);
                partitionManager.updatePartitionToPD(List.of(newPartition));
            } catch (PDException e) {
                throw new RuntimeException(e);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * is the task exists
     *
     * @param taskId task id
     * @param partId partition id
     * @return true if exists, false otherwise
     */
    private boolean preCheckTaskId(long taskId, int partId) {

        if (storeEngine.getPartitionEngine(partId) == null) {
            return false;
        }

        byte[] key = MetadataKeyHelper.getInstructionIdKey(taskId);
        var wrapper = storeEngine.getPartitionManager().getWrapper();
        byte[] value = wrapper.get(partId, key);

        if (value != null) {
            return true;
        }

        wrapper.put(partId, key, new byte[0]);
        return false;
    }
}
