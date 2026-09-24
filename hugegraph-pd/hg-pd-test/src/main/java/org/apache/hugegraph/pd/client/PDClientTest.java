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

package org.apache.hugegraph.pd.client;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.apache.tinkerpop.shaded.minlog.Log;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.hugegraph.pd.client.listener.PDEventListener;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.grpc.MetaTask;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.pd.grpc.Pdpb;

// TODO: Exceptions should be thrown rather than silenced.
public class PDClientTest extends BaseClientTest {

    @Test
    public void testDbCompaction() {
        System.out.println("testDbCompaction start");

        try {
            pdClient.dbCompaction("");
            pdClient.dbCompaction();
        } catch (PDException e) {
            e.printStackTrace();
        }

        System.out.println("pdclienttest testDbCompaction end");
    }

    @Test
    public void testRegisterStore() {
        Metapb.Store store = Metapb.Store.newBuilder().build();
        try {
            pdClient.registerStore(store);
        } catch (PDException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testSetGraph() {
        Metapb.Graph graph = Metapb.Graph.newBuilder().setGraphName("test").build();
        try {
            pdClient.setGraph(graph);
        } catch (PDException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testGetGraph() {
        try {
            pdClient.getGraph("test");
        } catch (PDException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testGetStore() {
        try {
            pdClient.getStore(0L);
        } catch (PDException e) {
            assert e.getErrorCode() == 101;
        }
    }

    @Test
    public void testUpdateStore() {
        Metapb.Store store = Metapb.Store.newBuilder().build();
        try {
            pdClient.updateStore(store);
        } catch (PDException e) {
        }
    }

    @Test
    public void testGetActiveStores() {
        try {
            pdClient.getActiveStores("test");
        } catch (PDException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testGetAllStores() {
        try {
            pdClient.getAllStores("test");
        } catch (PDException e) {
            e.printStackTrace();
        }
    }

//    @Test
//    public void testStoreHeartbeat(){
//        Metapb.StoreStats stats = Metapb.StoreStats.newBuilder().build();
//        try {
//            pdClient.storeHeartbeat(stats);
//        } catch (PDException e) {
//            e.printStackTrace();
//        }
//    }

    @Test
    public void testKeyToCode() {
        pdClient.keyToCode("test", "test".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    public void testScanPartitions() {
        try {
            pdClient.scanPartitions("test", "1".getBytes(StandardCharsets.UTF_8),
                                    "9".getBytes(StandardCharsets.UTF_8));
        } catch (PDException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testGetPartitionsByStore() {
        try {
            pdClient.getPartitionsByStore(0L);
        } catch (PDException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testQueryPartitions() {
        try {
            pdClient.queryPartitions(0L, 0);
        } catch (PDException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testGetPartitions() {
        try {
            pdClient.getPartitions(0L, "test");
        } catch (PDException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testUpdatePartitionLeader() {
        System.out.println("updatePartitionLeader start");

        pdClient.updatePartitionLeader("aaa", 0, 0L);
    }

    @Test
    public void testInvalidPartitionCache() {
        pdClient.invalidPartitionCache();
    }

    @Test
    public void testInvalidStoreCache() {
        pdClient.invalidStoreCache(0L);
    }

    @Test
    public void testUpdatePartitionCache() {
        Metapb.Partition partition = Metapb.Partition.newBuilder().build();
        Metapb.Shard leader = Metapb.Shard.newBuilder().build();
        pdClient.updatePartitionCache(partition, leader);
    }

    @Test
    public void testGetIdByKey() {
        try {
            pdClient.getIdByKey("test", 1);
        } catch (PDException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testResetIdByKey() {
        try {
            pdClient.resetIdByKey("test");
        } catch (PDException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testGetGetLeader() {
        try {
            pdClient.getLeader();
        } catch (PDException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testGetMembers() {
        try {
            pdClient.getMembers();
        } catch (PDException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testGetClusterStats() {
        try {
            pdClient.getClusterStats();
        } catch (PDException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testAddEventListener() {
        PDEventListener listener = Mockito.mock(PDEventListener.class);
        pdClient.addEventListener(listener);
    }

    @Test
    public void testGetWatchClient() {
        pdClient.getWatchClient();
    }

    @Test
    public void testGetPulseClient() {
        // pdClient.getPulseClient();
    }

    @Test
    public void testGetStoreStatus() {
        try {
            pdClient.getStoreStatus(true);
        } catch (PDException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testGetPartition() {
        try {
            pdClient.getPartition("test", "test".getBytes(StandardCharsets.UTF_8));
        } catch (PDException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testSetGraphSpace() {
        try {
            pdClient.setGraphSpace("test", 1L);
        } catch (PDException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testGetGraphSpace() {
        try {
            pdClient.getGraphSpace("test");
        } catch (PDException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testSetPDConfig() {
        try {
            pdClient.setPDConfig(0, "", 0, 0L);
        } catch (PDException e) {
            assert e.getErrorCode() == 112;
        }
        Metapb.PDConfig pdConfig = Metapb.PDConfig.newBuilder().build();

        try {
            pdClient.setPDConfig(pdConfig);
        } catch (PDException e) {
            assert e.getErrorCode() == 112;
        }
    }

    @Test
    public void testGetPDConfig() {
        try {
            pdClient.getPDConfig(0L);
        } catch (PDException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testChangePeerList() {
        try {
            pdClient.changePeerList("");
        } catch (PDException e) {
            assert e.getErrorCode() == -1;
        }
    }

    @Test
    public void testSplitData() {
        try {
            Metapb.PDConfig config = pdClient.getPDConfig();
            pdClient.setPDConfig(config.toBuilder()
                                       .setMaxShardsPerStore(12)
                                       .build());
            System.out.println(pdClient.getPDConfig());
            pdClient.splitData();
        } catch (PDException e) {
            Log.error("testSplitData", e);
        }
    }

    @Test
    public void testBalancePartition() {
        try {
            pdClient.balancePartition();
        } catch (PDException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testMovePartition() {
        Pdpb.OperationMode mode = Pdpb.OperationMode.Auto;
        List<Pdpb.MovePartitionParam> params = new ArrayList<>(1);
        try {
            pdClient.movePartition(mode, params);
        } catch (PDException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testReportTask() {
        MetaTask.Task task = MetaTask.Task.newBuilder().build();
        try {
            pdClient.reportTask(task);
        } catch (PDException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testBalanceLeaders() {
        try {
            pdClient.balanceLeaders();
        } catch (PDException e) {
            assert e.getErrorCode() == 1001;
        }
    }

    @Test
    public void testDelStore() {
        try {
            pdClient.delStore(0L);
        } catch (PDException e) {
        }
    }

//    @Test
//    public void testgetQuota() {
//        try {
//            pdClient.getQuota();
//        } catch (PDException e) {
//            e.printStackTrace();
//        }
//    }

    @Test
    public void testUpdatePartition() {
        List<Metapb.Partition> partitions = new ArrayList<>(1);
        try {
            pdClient.updatePartition(partitions);
        } catch (PDException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testDelPartition() {
        try {
            pdClient.delPartition("test", 0);
        } catch (PDException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testdelGraph() {
        try {
            pdClient.delGraph("test");
        } catch (PDException e) {
            e.printStackTrace();
        }
    }
}
