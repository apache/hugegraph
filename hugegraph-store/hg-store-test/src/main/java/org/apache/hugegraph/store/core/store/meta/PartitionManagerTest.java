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

package org.apache.hugegraph.store.core.store.meta;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.store.cmd.request.UpdatePartitionRequest;
import org.apache.hugegraph.store.core.StoreEngineTestBase;
import org.apache.hugegraph.store.meta.Graph;
import org.apache.hugegraph.store.meta.GraphManager;
import org.apache.hugegraph.store.meta.PartitionManager;
import org.apache.hugegraph.store.pd.FakePdServiceProvider;
import org.junit.Before;
import org.junit.Test;

public class PartitionManagerTest extends StoreEngineTestBase {

    private PartitionManager manager;

    @Before
    public void setup() {
        manager = getStoreEngine().getPartitionManager();
    }

    @Test
    public void testGetDeletedFileManager() {
        assertNotNull(manager.getDeletedFileManager());
    }

    @Test
    public void testGetPdProvider() {
        assertEquals(manager.getPdProvider().getClass(), FakePdServiceProvider.class);
    }

    @Test
    public void testGetStoreMetadata() {
        assertNotNull(manager.getStoreMetadata());
    }

    @Test
    public void testSetStore() {
        var storeId = FakePdServiceProvider.makeStoreId("127.0.0.1:6511");
        var store = manager.getStore(storeId);
        manager.setStore(store);
        var store2 = manager.getStoreMetadata().getStore();

        assertEquals(store.getId(), store2.getId());
    }

    @Test
    public void testUpdatePartition() {
        var partition = getPartition(5);
        manager.updatePartition(partition.getProtoObj(), true);

        var partition2 = manager.findPartition("graph0", 5);
        assertEquals(partition.getGraphName(), partition2.getGraphName());

        var partition3 = manager.loadPartitionFromSnapshot(partition2);
        assertEquals(partition3.getGraphName(), partition2.getGraphName());
    }

    @Test
    public void testChangeState() {
        createPartitionEngine(4);
        var partition = getPartition(4);
        manager.changeState(partition, Metapb.PartitionState.PState_Offline);
        var partition2 = manager.findPartition("graph0", 4);
        assertEquals(partition2.getWorkState(), Metapb.PartitionState.PState_Offline);
    }

    @Test
    public void testChangeKeyRange() {
        createPartitionEngine(4);
        var partition = getPartition(4);
        manager.changeKeyRange(partition, 1000, 2000);

        var partition2 = manager.findPartition("graph0", 4);
        assertEquals(partition2.getStartKey(), 1000);
        assertEquals(partition2.getEndKey(), 2000);
    }


    @Test
    public void testUpdatePartitionRangeOrState() {
        createPartitionEngine(4);
        UpdatePartitionRequest request = new UpdatePartitionRequest();
        request.setPartitionId(4);
        request.setGraphName("graph0");
        request.setStartKey(2000);
        request.setEndKey(3000);
        request.setWorkState(Metapb.PartitionState.PState_Offline);
        manager.updatePartitionRangeOrState(request);

        var partition = manager.findPartition("graph0", 4);
        assertEquals(partition.getStartKey(), 2000);
        assertEquals(partition.getEndKey(), 3000);
        assertEquals(partition.getWorkState(), Metapb.PartitionState.PState_Offline);
    }

    @Test
    public void testGetLeaderPartitionIds() {
        createPartitionEngine(0);
        createPartitionEngine(4);
        createPartitionEngine(5);
        System.out.println(manager.getLeaderPartitionIds("graph0"));
        assertEquals(manager.getLeaderPartitionIds("graph0").size(), 3);
    }

    @Test
    public void testisLocal() {
        createPartitionEngine(0);
        assertTrue(manager.isLocalPartition(0));
        assertTrue(manager.isLocalPartition(getPartition(0)));
        assertTrue(manager.isLocalStore(manager.getStore()));
    }

    @Test
    public void testUploadToPd() throws PDException {
        createPartitionEngine(0);
        var partition = manager.findPartition("graph0", 0);
        var list = new ArrayList<Metapb.Partition>();
        list.add(partition.getProtoObj());
        // fake pd, return nothing
        assertEquals(1, manager.updatePartitionToPD(list).size());

        manager.reportTask(null);

        var partitions = manager.changePartitionToOnLine(list);
        assertSame(partitions.get(0).getState(), Metapb.PartitionState.PState_Normal);
        // fake pd
        // TODO: uncomment it until fix it
        // assertNotNull(manager.findPartition("graph0", 1000));

    }

    @Test
    public void testShards2Peers() {
        var storeId = FakePdServiceProvider.makeStoreId("127.0.0.1:6511");
        Metapb.Shard shard = Metapb.Shard.newBuilder()
                                         .setStoreId(storeId)
                                         .setRole(Metapb.ShardRole.Leader)
                                         .build();

        List<Metapb.Shard> list = new ArrayList<>();
        list.add(shard);

        var peers = manager.shards2Peers(list);
        assertEquals("127.0.0.1:6510", peers.get(0));
    }

    @Test
    public void testLoad() {
        createPartitionEngine(0);
        var graphManager = new GraphManager(manager.getOptions(), manager.getPdProvider());
        var graph = Metapb.Graph.newBuilder()
                                .setGraphName("graph0")
                                .setPartitionCount(12)
                                .build();
        graphManager.updateGraph(new Graph(graph));

        manager.load();
        assertNotNull(manager.getLeaderPartitionIds("graph0"));
    }

    @Test
    public void testSyncPartitionsFromPD() throws PDException {
        createPartitionEngine(0);
        // from fake pd
        manager.syncPartitionsFromPD(partition -> {
        });

        assertTrue(manager.getPartitions().isEmpty());
    }

}
