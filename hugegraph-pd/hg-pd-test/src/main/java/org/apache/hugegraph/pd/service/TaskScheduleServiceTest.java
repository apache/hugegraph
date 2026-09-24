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

import java.util.ArrayList;
import java.util.List;

import org.apache.hugegraph.pd.TaskScheduleService;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.junit.Before;
import org.junit.Test;

public class TaskScheduleServiceTest extends PdTestBase {

    TaskScheduleService service;

    @Before
    public void init() {
        service = getTaskService();
    }

    @Test
    public void testStoreOffline() {

    }

    public void testPatrolStores() {

    }

    public void testPatrolPartitions() {

    }

    public void testBalancePartitionShard() {

    }

    @Test
    public void testBalancePartitionLeader() throws PDException {

        var list = new ArrayList<Metapb.Partition>();
        for (int i = 0; i < 6; i++) {
            getStoreNodeService().getStoreInfoMeta().updateShardGroup(genShardGroup(i));
            list.add(genPartition(i));
        }

        getPdConfig().getPartition().setShardCount(3);

        getPartitionService().updatePartition(list);
        var rst = service.balancePartitionLeader(true);
        // assertTrue(rst.size() > 0 );
        // recover
        getPdConfig().getPartition().setShardCount(1);
        getStoreNodeService().getStoreInfoMeta().removeAll();
    }

    public void testSplitPartition() {

    }

    public void testSplitPartition2() {

    }

    public void testCanAllPartitionsMovedOut() {

    }

    private Metapb.ShardGroup genShardGroup(int groupId) {
        return Metapb.ShardGroup.newBuilder()
                                .setId(groupId)
                                .addAllShards(genShards())
                                .build();
    }

    private Metapb.Partition genPartition(int groupId) {
        return Metapb.Partition.newBuilder()
                               .setId(groupId)
                               .setState(Metapb.PartitionState.PState_Normal)
                               .setGraphName("graph1")
                               .setStartKey(groupId * 10)
                               .setEndKey(groupId * 10 + 10)
                               .build();
    }

    private List<Metapb.Shard> genShards() {
        return List.of(
                Metapb.Shard.newBuilder().setStoreId(1).setRole(Metapb.ShardRole.Leader).build(),
                Metapb.Shard.newBuilder().setStoreId(2).setRole(Metapb.ShardRole.Follower).build(),
                Metapb.Shard.newBuilder().setStoreId(3).setRole(Metapb.ShardRole.Follower).build());
    }

}


