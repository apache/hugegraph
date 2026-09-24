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

package org.apache.hugegraph.pd.service;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.apache.hugegraph.pd.PartitionService;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.grpc.MetaTask;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.pd.grpc.pulse.CleanPartition;
import org.apache.hugegraph.pd.grpc.pulse.CleanType;
import org.junit.Before;
import org.junit.Test;

public class PartitionServiceTest extends PdTestBase {

    private PartitionService service;

    @Before
    public void init() {
        service = getPartitionService();
    }

    @Test
    public void testCombinePartition() throws PDException {
        buildEnv();
        // 0, 1, 2-> 0, 3,4,5->1, 6,7,8 ->2, 9,10, 11-> 3
        service.combinePartition(4);

        var partition = service.getPartitionById("graph0", 0);
        assertEquals(0, partition.getStartKey());
        assertEquals(5462, partition.getEndKey());

        var tasks = getStoreNodeService().getTaskInfoMeta().scanMoveTask("graph0");
        assertEquals(11, tasks.size());

        for (MetaTask.Task task : tasks) {
            var newTask = task.toBuilder().setState(MetaTask.TaskState.Task_Success).build();
            getTaskService().reportTask(newTask);
        }

        tasks = getStoreNodeService().getTaskInfoMeta().scanMoveTask("graph0");
        assertEquals(0, tasks.size());
    }

    @Test
    public void testCombinePartition2() throws PDException {
        buildEnv();
        // 0, 1, 2-> 0, 3,4,5->1, 6,7,8 ->2, 9,10, 11-> 3
        service.combinePartition(4);

        var partition = service.getPartitionById("graph0", 0);
        assertEquals(0, partition.getStartKey());
        assertEquals(5462, partition.getEndKey());

        var tasks = getStoreNodeService().getTaskInfoMeta().scanMoveTask("graph0");
        assertEquals(11, tasks.size());

        for (MetaTask.Task task : tasks) {
            var newTask = task.toBuilder().setState(MetaTask.TaskState.Task_Failure).build();
            getTaskService().reportTask(newTask);
        }

        tasks = getStoreNodeService().getTaskInfoMeta().scanMoveTask("graph0");
        assertEquals(0, tasks.size());
    }

    @Test
    public void testHandleCleanTask() {
        MetaTask.Task task = MetaTask.Task.newBuilder()
                                          .setType(MetaTask.TaskType.Clean_Partition)
                                          .setPartition(
                                                  Metapb.Partition.newBuilder().setGraphName("foo")
                                                                  .setId(0).build())
                                          .setCleanPartition(CleanPartition.newBuilder()
                                                                           .setCleanType(
                                                                                   CleanType.CLEAN_TYPE_KEEP_RANGE)
                                                                           .setDeletePartition(true)
                                                                           .setKeyStart(0)
                                                                           .setKeyEnd(10)
                                                                           .build())
                                          .build();
        getTaskService().reportTask(task);
    }

    private void buildEnv() throws PDException {
        var storeInfoMeta = getStoreNodeService().getStoreInfoMeta();
        storeInfoMeta.updateStore(Metapb.Store.newBuilder()
                                              .setId(99)
                                              .setState(Metapb.StoreState.Up)
                                              .build());

        long lastId = 0;
        for (int i = 0; i < 12; i++) {
            Metapb.Shard shard = Metapb.Shard.newBuilder()
                                             .setStoreId(99)
                                             .setRole(Metapb.ShardRole.Leader)
                                             .build();

            Metapb.ShardGroup shardGroup = Metapb.ShardGroup.newBuilder()
                                                            .setId(i)
                                                            .setState(
                                                                    Metapb.PartitionState.PState_Normal)
                                                            .addAllShards(List.of(shard))
                                                            .build();
            storeInfoMeta.updateShardGroup(shardGroup);

            var partitionShard = service.getPartitionByCode("graph0", lastId);
            if (partitionShard != null) {
                lastId = partitionShard.getPartition().getEndKey();
            }
        }

    }
}
