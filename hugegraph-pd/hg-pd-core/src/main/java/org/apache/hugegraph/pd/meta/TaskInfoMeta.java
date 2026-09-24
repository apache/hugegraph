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

package org.apache.hugegraph.pd.meta;

import java.util.List;

import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.config.PDConfig;
import org.apache.hugegraph.pd.grpc.MetaTask;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.pd.grpc.pulse.MovePartition;
import org.apache.hugegraph.pd.grpc.pulse.SplitPartition;

/**
 * Task management
 */
public class TaskInfoMeta extends MetadataRocksDBStore {

    public TaskInfoMeta(PDConfig pdConfig) {
        super(pdConfig);
    }

    /**
     * Add a partition splitting task
     */
    public void addSplitTask(int groupID, Metapb.Partition partition, SplitPartition splitPartition)
            throws PDException {
        byte[] key = MetadataKeyHelper.getSplitTaskKey(partition.getGraphName(), groupID);
        MetaTask.Task task = MetaTask.Task.newBuilder()
                                          .setType(MetaTask.TaskType.Split_Partition)
                                          .setState(MetaTask.TaskState.Task_Doing)
                                          .setStartTimestamp(System.currentTimeMillis())
                                          .setPartition(partition)
                                          .setSplitPartition(splitPartition)
                                          .build();
        put(key, task.toByteString().toByteArray());
    }

    public void updateSplitTask(MetaTask.Task task) throws PDException {
        var partition = task.getPartition();
        byte[] key = MetadataKeyHelper.getSplitTaskKey(partition.getGraphName(), partition.getId());
        put(key, task.toByteString().toByteArray());
    }

    public MetaTask.Task getSplitTask(String graphName, int groupID) throws PDException {
        byte[] key = MetadataKeyHelper.getSplitTaskKey(graphName, groupID);
        return getOne(MetaTask.Task.parser(), key);
    }

    public List<MetaTask.Task> scanSplitTask(String graphName) throws PDException {
        byte[] prefix = MetadataKeyHelper.getSplitTaskPrefix(graphName);
        return scanPrefix(MetaTask.Task.parser(), prefix);
    }

    public void removeSplitTaskPrefix(String graphName) throws PDException {
        byte[] key = MetadataKeyHelper.getSplitTaskPrefix(graphName);
        removeByPrefix(key);
    }

    public boolean hasSplitTaskDoing() throws PDException {
        byte[] key = MetadataKeyHelper.getAllSplitTaskPrefix();
        return scanPrefix(key).size() > 0;
    }

    public void addMovePartitionTask(Metapb.Partition partition, MovePartition movePartition)
            throws PDException {
        byte[] key = MetadataKeyHelper.getMoveTaskKey(partition.getGraphName(),
                                                      movePartition.getTargetPartition().getId(),
                                                      partition.getId());

        MetaTask.Task task = MetaTask.Task.newBuilder()
                                          .setType(MetaTask.TaskType.Move_Partition)
                                          .setState(MetaTask.TaskState.Task_Doing)
                                          .setStartTimestamp(System.currentTimeMillis())
                                          .setPartition(partition)
                                          .setMovePartition(movePartition)
                                          .build();
        put(key, task.toByteArray());
    }

    public void updateMovePartitionTask(MetaTask.Task task)
            throws PDException {

        byte[] key = MetadataKeyHelper.getMoveTaskKey(task.getPartition().getGraphName(),
                                                      task.getMovePartition().getTargetPartition()
                                                          .getId(),
                                                      task.getPartition().getId());
        put(key, task.toByteArray());
    }

    public MetaTask.Task getMovePartitionTask(String graphName, int targetId, int partId) throws
                                                                                          PDException {
        byte[] key = MetadataKeyHelper.getMoveTaskKey(graphName, targetId, partId);
        return getOne(MetaTask.Task.parser(), key);
    }

    public List<MetaTask.Task> scanMoveTask(String graphName) throws PDException {
        byte[] prefix = MetadataKeyHelper.getMoveTaskPrefix(graphName);
        return scanPrefix(MetaTask.Task.parser(), prefix);
    }

    public List<MetaTask.Task> scanBuildIndexTask(long taskId) throws PDException {
        byte[] prefix = MetadataKeyHelper.getBuildIndexTaskPrefix(taskId);
        return scanPrefix(MetaTask.Task.parser(), prefix);
    }

    public MetaTask.Task getBuildIndexTask(long taskId, int partitionId) throws PDException {
        byte[] key = MetadataKeyHelper.getBuildIndexTaskKey(taskId, partitionId);
        return getOne(MetaTask.Task.parser(), key);
    }

    public void updateBuildIndexTask(MetaTask.Task task) throws PDException {
        var bt = task.getBuildIndex();
        byte[] key = MetadataKeyHelper.getBuildIndexTaskKey(bt.getTaskId(), bt.getPartitionId());
        put(key, task.toByteArray());
    }

    /**
     * Delete the migration task by prefixing it and group them all at once
     *
     * @param graphName graphName
     * @throws PDException io error
     */
    public void removeMoveTaskPrefix(String graphName) throws PDException {
        byte[] key = MetadataKeyHelper.getMoveTaskPrefix(graphName);
        removeByPrefix(key);
    }

    public boolean hasMoveTaskDoing() throws PDException {
        byte[] key = MetadataKeyHelper.getAllMoveTaskPrefix();
        return scanPrefix(key).size() > 0;
    }

}
