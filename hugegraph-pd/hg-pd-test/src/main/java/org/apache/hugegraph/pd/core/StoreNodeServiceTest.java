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

package org.apache.hugegraph.pd.core;

import java.util.ArrayList;
import java.util.List;

import org.apache.hugegraph.pd.PartitionService;
import org.apache.hugegraph.pd.StoreNodeService;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.core.BaseCoreTest;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.junit.Assert;
import org.junit.Test;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class StoreNodeServiceTest extends BaseCoreTest {

    @Test
    public void testStoreNodeService() throws PDException {
        Assert.assertEquals(pdConfig.getPartition().getTotalCount(),
                            pdConfig.getInitialStoreMap().size() *
                            pdConfig.getPartition().getMaxShardsPerStore()
                            / pdConfig.getPartition().getShardCount());
        StoreNodeService storeService = new StoreNodeService(pdConfig);
        storeService.init(new PartitionService(pdConfig, storeService));
        int count = 6;
        Metapb.Store[] stores = new Metapb.Store[count];
        for (int i = 0; i < count; i++) {
            Metapb.Store store = Metapb.Store.newBuilder()
                                             .setId(0)
                                             .setAddress("127.0.0.1:850" + i)
                                             .setDeployPath("/data")
                                             .addLabels(Metapb.StoreLabel.newBuilder()
                                                                         .setKey("namespace")
                                                                         .setValue("default")
                                                                         .build())
                                             .build();
            stores[i] = storeService.register(store);
            System.out.println("newly registered store, id = " + stores[i].getId());
        }
        Assert.assertEquals(count, storeService.getStores("").size());

        for (Metapb.Store store : stores) {
            Metapb.StoreStats stats = Metapb.StoreStats.newBuilder()
                                                       .setStoreId(store.getId())
                                                       .build();
            storeService.heartBeat(stats);
        }

        Assert.assertEquals(6, storeService.getActiveStores("").size());

        Metapb.Graph graph = Metapb.Graph.newBuilder()
                                         .setGraphName("defaultGH")
                                         .setPartitionCount(10)
                                         .build();
        // alloc shard
        List<Metapb.Shard> shards = storeService.allocShards(graph, 1);

        Assert.assertEquals(3, shards.size());
        // set leader
        Assert.assertEquals(pdConfig.getPartition().getTotalCount(),
                            storeService.getShardGroups().size());
        Metapb.Shard leader = Metapb.Shard.newBuilder(shards.get(0))
                                          .setRole(Metapb.ShardRole.Leader).build();
        shards = new ArrayList<>(shards);
        shards.set(0, leader);
        // increase shard
        pdConfig.getPartition().setShardCount(5);

        Metapb.ShardGroup shardGroup = Metapb.ShardGroup.newBuilder()
                                                        .setId(1)
                                                        .addAllShards(shards).build();
        shards = storeService.reallocShards(shardGroup);

        Assert.assertEquals(5, shards.size());
        // decrease shard
        pdConfig.getPartition().setShardCount(3);
        shards = storeService.reallocShards(shardGroup);
        Assert.assertEquals(3, shards.size());
        // Includes the leader; the leader cannot be deleted.
        Assert.assertTrue(shards.contains(leader));

        // decrease shard
        pdConfig.getPartition().setShardCount(1);
        graph = Metapb.Graph.newBuilder(graph).build();
        shards = storeService.reallocShards(shardGroup);
        Assert.assertEquals(1, shards.size());
        // Includes the leader; the leader cannot be deleted.
        Assert.assertTrue(shards.contains(leader));

        for (Metapb.Store store : stores) {
            storeService.removeStore(store.getId());
        }
        Assert.assertEquals(0, storeService.getStores("").size());

    }

}
