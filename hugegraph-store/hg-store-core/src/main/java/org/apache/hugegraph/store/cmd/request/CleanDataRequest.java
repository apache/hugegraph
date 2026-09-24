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

package org.apache.hugegraph.store.cmd.request;

import org.apache.hugegraph.pd.grpc.pulse.CleanPartition;
import org.apache.hugegraph.pd.grpc.pulse.CleanType;
import org.apache.hugegraph.store.cmd.HgCmdBase;
import org.apache.hugegraph.store.meta.Partition;

import lombok.Data;

@Data
public class CleanDataRequest extends HgCmdBase.BaseRequest {

    private long keyStart;
    private long keyEnd;

    private CleanType cleanType;

    private boolean deletePartition;

    private long taskId;

    public static CleanDataRequest fromCleanPartitionTask(CleanPartition task, Partition partition,
                                                          long taskId) {
        return fromCleanPartitionTask(partition.getGraphName(), partition.getId(), taskId, task);
    }

    public static CleanDataRequest fromCleanPartitionTask(String graphName, int partitionId,
                                                          long taskId,
                                                          CleanPartition task) {
        CleanDataRequest request = new CleanDataRequest();
        request.setGraphName(graphName);
        request.setPartitionId(partitionId);
        request.setCleanType(task.getCleanType());
        request.setKeyStart(task.getKeyStart());
        request.setKeyEnd(task.getKeyEnd());
        request.setDeletePartition(task.getDeletePartition());
        request.setTaskId(taskId);
        return request;
    }

    public static CleanPartition toCleanPartitionTask(CleanDataRequest request) {
        return CleanPartition.newBuilder()
                             .setKeyStart(request.keyStart)
                             .setKeyEnd(request.keyEnd)
                             .setDeletePartition(request.deletePartition)
                             .setCleanType(request.cleanType)
                             .build();
    }

    @Override
    public byte magic() {
        return HgCmdBase.CLEAN_DATA;
    }
}
