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

import java.util.List;
import java.util.Map;

import org.apache.hugegraph.pd.KvService;
import org.apache.hugegraph.pd.PartitionService;
import org.apache.hugegraph.pd.StoreNodeService;
import org.apache.hugegraph.pd.TaskScheduleService;
import org.apache.hugegraph.pd.TaskScheduleService.PartitionPatrolResult;
import org.apache.hugegraph.pd.config.PDConfig;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.pd.grpc.Pdpb;
import org.apache.hugegraph.pd.rest.TaskAPI;
import org.apache.hugegraph.pd.service.PDRestService;
import org.apache.hugegraph.pd.raft.RaftEngine;
import org.apache.hugegraph.testutil.Whitebox;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.entity.PeerId;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class PartitionPatrolResultTest {

    private Node original;
    private Node node;
    private TaskScheduleService service;
    private StoreNodeService stores;
    private PartitionService partitions;

    @Before
    public void setUp() {
        original = RaftEngine.getInstance().getRaftNode();
        node = Mockito.mock(Node.class);
        Mockito.when(node.isLeader(true)).thenReturn(true);
        Whitebox.setInternalState(RaftEngine.getInstance(), "raftNode", node);
        service = Mockito.mock(TaskScheduleService.class, Mockito.CALLS_REAL_METHODS);
        stores = Mockito.mock(StoreNodeService.class);
        partitions = Mockito.mock(PartitionService.class);
        PDConfig config = new PDConfig();
        PDConfig.Partition partition = config.new Partition();
        partition.setShardCount(3);
        config.setPartition(partition);
        Whitebox.setInternalState(service, "pdConfig", config);
        Whitebox.setInternalState(service, "storeService", stores);
        Whitebox.setInternalState(service, "partitionService", partitions);
        Whitebox.setInternalState(service, "kvService", Mockito.mock(KvService.class));
    }

    @After
    public void tearDown() {
        Whitebox.setInternalState(RaftEngine.getInstance(), "raftNode", original);
    }

    @Test
    public void testReportsChangedGroupsAndOfflineRequests() throws Exception {
        Metapb.Shard oldShard = Metapb.Shard.newBuilder().setStoreId(1).build();
        Metapb.Shard added = Metapb.Shard.newBuilder().setStoreId(2).build();
        Metapb.ShardGroup changed = Metapb.ShardGroup.newBuilder().setId(7).addShards(oldShard).build();
        Metapb.ShardGroup unchanged = changed.toBuilder().setId(8).build();
        Mockito.when(stores.getShardGroups()).thenReturn(List.of(changed, unchanged));
        Mockito.when(stores.reallocShards(changed)).thenReturn(List.of(oldShard, added));
        Mockito.when(stores.reallocShards(unchanged)).thenReturn(List.of(oldShard));
        Metapb.Store retired = Metapb.Store.newBuilder().setId(9).build();
        Metapb.Partition affected = Metapb.Partition.newBuilder().setId(10).setGraphName("graph").build();
        Mockito.when(stores.getTombStores()).thenReturn(List.of(retired));
        Mockito.when(partitions.getPartitionByStore(retired)).thenReturn(List.of(affected, affected));

        PartitionPatrolResult result = service.patrolPartitionsWithResult();

        Assert.assertEquals(List.of(7), result.getReallocatedShardGroups());
        Assert.assertEquals(Map.of(10, 9L), result.getOfflineShardRequests());
        Assert.assertEquals(List.of(affected), result.getPartitions());
        Mockito.verify(partitions, Mockito.times(1)).shardOffline(affected, 9L);
    }

    @Test
    public void testFollowerDoesNotPatrol() throws Exception {
        Mockito.when(node.isLeader(true)).thenReturn(false);
        Assert.assertNull(service.patrolPartitionsWithResult());
        Mockito.verifyNoInteractions(stores, partitions);
    }

    @Test
    public void testLeadershipChangeAfterPatrolReturnsStructuredRefusal() throws Exception {
        Mockito.when(node.isLeader(true)).thenReturn(true, false);
        Mockito.when(node.getLeaderId()).thenReturn(new PeerId("127.0.0.2", 8610));
        TaskScheduleService scheduler = Mockito.mock(TaskScheduleService.class);
        Mockito.when(scheduler.patrolPartitionsWithResult()).thenReturn(new PartitionPatrolResult());
        PDRestService rest = new PDRestService();
        Whitebox.setInternalState(rest, "monitorService", scheduler);
        TaskAPI api = new TaskAPI();
        Whitebox.setInternalState(api, "pdRestService", rest);
        JsonNode response =
                new ObjectMapper().readTree(api.patrolPartitions());
        Assert.assertEquals(Pdpb.ErrorType.NOT_LEADER_VALUE,
                            response.get("status").asInt());
        Mockito.verify(scheduler).patrolPartitionsWithResult();
    }

    @Test
    public void testCompactionRefusalSurvivesLeadershipRegain() throws Exception {
        // The outer checks both see a leader, but the scheduler observed a follower in between.
        Mockito.when(node.isLeader(true)).thenReturn(true);
        TaskScheduleService scheduler = Mockito.mock(TaskScheduleService.class);
        Mockito.when(scheduler.dbCompaction("")).thenReturn(false);
        PDRestService rest = new PDRestService();
        Whitebox.setInternalState(rest, "monitorService", scheduler);
        TaskAPI api = new TaskAPI();
        Whitebox.setInternalState(api, "pdRestService", rest);
        JsonNode response = new ObjectMapper().readTree(api.dbCompaction().getBody());
        Assert.assertEquals(Pdpb.ErrorType.NOT_LEADER_VALUE, response.get("status").asInt());
        Mockito.verify(scheduler).dbCompaction("");
        Mockito.when(scheduler.dbCompaction("")).thenReturn(true);
        Assert.assertEquals("compact ok", api.dbCompaction().getBody());
    }

    @Test
    public void testReportsTombstoneReallocationWithCorrectInitialShardCount() throws Exception {
        assertTombstoneReallocation(false);
    }

    @Test
    public void testDeduplicatesTombstoneAndInitialReallocations() throws Exception {
        assertTombstoneReallocation(true);
    }

    private void assertTombstoneReallocation(boolean initiallyUnderReplicated) throws Exception {
        stores = Mockito.mock(StoreNodeService.class, Mockito.CALLS_REAL_METHODS);
        Whitebox.setInternalState(service, "storeService", stores);
        Metapb.Shard retiredShard = Metapb.Shard.newBuilder().setStoreId(9).build();
        Metapb.Shard second = Metapb.Shard.newBuilder().setStoreId(2).build();
        Metapb.Shard third = Metapb.Shard.newBuilder().setStoreId(3).build();
        Metapb.Shard replacement = Metapb.Shard.newBuilder().setStoreId(4).build();
        Metapb.ShardGroup before = Metapb.ShardGroup.newBuilder().setId(7)
                .addAllShards(List.of(retiredShard, second, third)).build();
        Metapb.ShardGroup initial = initiallyUnderReplicated ?
                                   before.toBuilder().clearShards().addShards(retiredShard).build() : before;
        Mockito.doReturn(List.of(initial)).when(stores).getShardGroups();
        Mockito.doReturn(List.of(before)).when(stores).getShardGroupsByStore(9L);
        if (initiallyUnderReplicated) {
            Mockito.doReturn(before.getShardsList()).when(stores).reallocShards(initial);
        }
        Mockito.doReturn(List.of(second, third, replacement)).when(stores)
               .reallocShards(before.toBuilder().clearShards().addAllShards(List.of(second, third)).build());
        Metapb.Store retired = Metapb.Store.newBuilder().setId(9).build();
        Mockito.doReturn(List.of(retired)).when(stores).getTombStores();
        Metapb.Partition affected = Metapb.Partition.newBuilder().setId(7).setGraphName("graph").build();
        Mockito.when(partitions.getPartitionByStore(retired)).thenReturn(List.of(affected));

        PartitionPatrolResult result = service.patrolPartitionsWithResult();
        Assert.assertEquals(List.of(7), result.getReallocatedShardGroups());
        Assert.assertEquals(Map.of(7, 9L), result.getOfflineShardRequests());
    }
}
