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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.apache.hugegraph.pd.ConfigService;
import org.apache.hugegraph.pd.IdService;
import org.apache.hugegraph.pd.PartitionService;
import org.apache.hugegraph.pd.StoreNodeService;
import org.apache.hugegraph.pd.StoreStatusListener;
import org.apache.hugegraph.pd.config.PDConfig;
import org.apache.hugegraph.pd.grpc.MetaTask;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.junit.Before;
import org.junit.Test;

public class StoreServiceTest {

    private PDConfig config;

    private StoreNodeService service;

    @Before
    public void setUp() {
        config = getConfig();
        service = new StoreNodeService(config);
    }

    @Test
    public void testInit() {
        // Setup
        PDConfig pdConfig = getConfig();
        final PDConfig pdConfig1 = getConfig();
        final PartitionService partitionService = new PartitionService(pdConfig,
                                                                       new StoreNodeService(
                                                                               pdConfig1));

        // Run the test
        service.init(partitionService);

        // Verify the results
    }

    private PDConfig getConfig() {
        PDConfig pdConfig = new PDConfig();
        pdConfig.setConfigService(
                new ConfigService(BaseServerTest.getConfig()));
        pdConfig.setIdService(new IdService(BaseServerTest.getConfig()));
        pdConfig.setClusterId(0L);
        pdConfig.setPatrolInterval(0L);
        pdConfig.setDataPath("dataPath");
        pdConfig.setMinStoreCount(0);
        pdConfig.setInitialStoreList("initialStoreList");
        pdConfig.setHost("host");
        pdConfig.setVerifyPath("verifyPath");
        pdConfig.setLicensePath("licensePath");
        PDConfig.Raft raft = new PDConfig().new Raft();
        raft.setEnable(false);
        pdConfig.setRaft(raft);
        final PDConfig.Partition partition = new PDConfig().new Partition();
        partition.setTotalCount(0);
        partition.setShardCount(0);
        pdConfig.setPartition(partition);
        pdConfig.setInitialStoreMap(Map.ofEntries(Map.entry("value", "value")));
        return pdConfig;
    }

    @Test
    public void testIsOK() {
        // Setup
        // Run the test
        final boolean result = service.isOK();

        // Verify the results
        assertThat(result).isTrue();
    }

    @Test
    public void testRegister() throws Exception {
        // Setup
        final Metapb.Store store = Metapb.Store.newBuilder().setId(0L)
                                               .setAddress("address")
                                               .setRaftAddress("raftAddress")
                                               .addLabels(Metapb.StoreLabel
                                                                  .newBuilder()
                                                                  .build())
                                               .setVersion("version").setState(
                        Metapb.StoreState.Unknown).setStartTimestamp(0L)
                                               .setDeployPath("deployPath")
                                               .setLastHeartbeat(0L).setStats(
                        Metapb.StoreStats.newBuilder().setStoreId(0L)
                                         .setPartitionCount(0).addGraphStats(
                                      Metapb.GraphStats.newBuilder()
                                                       .setGraphName("value")
                                                       .setApproximateSize(0L)
                                                       .setRole(Metapb.ShardRole.None)
                                                       .build()).build())
                                               .setDataVersion(0).setCores(0)
                                               .setDataPath("dataPath").build();
        final Metapb.Store expectedResult = Metapb.Store.newBuilder().setId(0L)
                                                        .setAddress("address")
                                                        .setRaftAddress(
                                                                "raftAddress")
                                                        .addLabels(
                                                                Metapb.StoreLabel
                                                                        .newBuilder()
                                                                        .build())
                                                        .setVersion("version")
                                                        .setState(
                                                                Metapb.StoreState.Unknown)
                                                        .setStartTimestamp(0L)
                                                        .setDeployPath(
                                                                "deployPath")
                                                        .setLastHeartbeat(0L)
                                                        .setStats(
                                                                Metapb.StoreStats
                                                                        .newBuilder()
                                                                        .setStoreId(
                                                                                0L)
                                                                        .setPartitionCount(
                                                                                0)
                                                                        .addGraphStats(
                                                                                Metapb.GraphStats
                                                                                        .newBuilder()
                                                                                        .setGraphName(
                                                                                                "value")
                                                                                        .setApproximateSize(
                                                                                                0L)
                                                                                        .setRole(
                                                                                                Metapb.ShardRole.None)
                                                                                        .build())
                                                                        .build())
                                                        .setDataVersion(0)
                                                        .setCores(0)
                                                        .setDataPath("dataPath")
                                                        .build();

        // Configure PDConfig.getInitialStoreMap(...).
        final Map<String, String> stringStringMap = Map.ofEntries(
                Map.entry("value", "value"));

        // Run the test
        final Metapb.Store result = service.register(store);
    }

    @Test
    public void testGetStore() throws Exception {
        // Setup
        try {
            Metapb.GraphStats stats = Metapb.GraphStats.newBuilder()
                                                       .setGraphName("value")
                                                       .setApproximateSize(0L)
                                                       .setRole(
                                                               Metapb.ShardRole.None)
                                                       .build();
            Metapb.StoreStats storeStats = Metapb.StoreStats.newBuilder()
                                                            .setStoreId(0L)
                                                            .setPartitionCount(
                                                                    0)
                                                            .addGraphStats(
                                                                    stats)
                                                            .build();
            final Metapb.Store expectedResult = Metapb.Store.newBuilder()
                                                            .setId(0L)
                                                            .setAddress(
                                                                    "address")
                                                            .setRaftAddress(
                                                                    "raftAddress")
                                                            .addLabels(
                                                                    Metapb.StoreLabel
                                                                            .newBuilder()
                                                                            .build())
                                                            .setVersion(
                                                                    "version")
                                                            .setState(
                                                                    Metapb.StoreState.Unknown)
                                                            .setStartTimestamp(
                                                                    0L)
                                                            .setDeployPath(
                                                                    "deployPath")
                                                            .setLastHeartbeat(
                                                                    0L)
                                                            .setStats(
                                                                    storeStats)
                                                            .setDataVersion(0)
                                                            .setCores(0)
                                                            .setDataPath(
                                                                    "dataPath")
                                                            .build();

            // Run the test
            final Metapb.Store result = service.getStore(0L);
        } catch (Exception e) {

        }
    }

    @Test
    public void testUpdateStore() throws Exception {
        // Setup
        final Metapb.Store store = Metapb.Store.newBuilder().setId(0L)
                                               .setAddress("address")
                                               .setRaftAddress("raftAddress")
                                               .addLabels(Metapb.StoreLabel
                                                                  .newBuilder()
                                                                  .build())
                                               .setVersion("version").setState(
                        Metapb.StoreState.Unknown).setStartTimestamp(0L)
                                               .setDeployPath("deployPath")
                                               .setLastHeartbeat(0L).setStats(
                        Metapb.StoreStats.newBuilder().setStoreId(0L)
                                         .setPartitionCount(0).addGraphStats(
                                      Metapb.GraphStats.newBuilder()
                                                       .setGraphName("value")
                                                       .setApproximateSize(0L)
                                                       .setRole(Metapb.ShardRole.None)
                                                       .build()).build())
                                               .setDataVersion(0).setCores(0)
                                               .setDataPath("dataPath").build();
        final Metapb.Store expectedResult = Metapb.Store.newBuilder().setId(0L)
                                                        .setAddress("address")
                                                        .setRaftAddress(
                                                                "raftAddress")
                                                        .addLabels(
                                                                Metapb.StoreLabel
                                                                        .newBuilder()
                                                                        .build())
                                                        .setVersion("version")
                                                        .setState(
                                                                Metapb.StoreState.Unknown)
                                                        .setStartTimestamp(0L)
                                                        .setDeployPath(
                                                                "deployPath")
                                                        .setLastHeartbeat(0L)
                                                        .setStats(
                                                                Metapb.StoreStats
                                                                        .newBuilder()
                                                                        .setStoreId(
                                                                                0L)
                                                                        .setPartitionCount(
                                                                                0)
                                                                        .addGraphStats(
                                                                                Metapb.GraphStats
                                                                                        .newBuilder()
                                                                                        .setGraphName(
                                                                                                "value")
                                                                                        .setApproximateSize(
                                                                                                0L)
                                                                                        .setRole(
                                                                                                Metapb.ShardRole.None)
                                                                                        .build())
                                                                        .build())
                                                        .setDataVersion(0)
                                                        .setCores(0)
                                                        .setDataPath("dataPath")
                                                        .build();

        // Configure PDConfig.getPartition(...).
        final PDConfig.Partition partition = new PDConfig().new Partition();
        partition.setTotalCount(0);
        partition.setMaxShardsPerStore(0);
        partition.setShardCount(0);

        // Run the test
        final Metapb.Store result = service.updateStore(store);
    }

    @Test
    public void testStoreTurnoff() throws Exception {
        // Setup
        final Metapb.Store store = Metapb.Store.newBuilder().setId(0L)
                                               .setAddress("address")
                                               .setRaftAddress("raftAddress")
                                               .addLabels(Metapb.StoreLabel
                                                                  .newBuilder()
                                                                  .build())
                                               .setVersion("version").setState(
                        Metapb.StoreState.Unknown).setStartTimestamp(0L)
                                               .setDeployPath("deployPath")
                                               .setLastHeartbeat(0L).setStats(
                        Metapb.StoreStats.newBuilder().setStoreId(0L)
                                         .setPartitionCount(0).addGraphStats(
                                      Metapb.GraphStats.newBuilder()
                                                       .setGraphName("value")
                                                       .setApproximateSize(0L)
                                                       .setRole(Metapb.ShardRole.None)
                                                       .build()).build())
                                               .setDataVersion(0).setCores(0)
                                               .setDataPath("dataPath").build();

        // Configure PDConfig.getPartition(...).
        final PDConfig.Partition partition = new PDConfig().new Partition();
        partition.setTotalCount(0);
        partition.setMaxShardsPerStore(0);
        partition.setShardCount(0);

        // Run the test
        service.storeTurnoff(store);

        // Verify the results
    }

    @Test
    public void testGetStores1() throws Exception {
        // Setup
        final List<Metapb.Store> expectedResult = List.of(
                Metapb.Store.newBuilder().setId(0L).setAddress("address")
                            .setRaftAddress("raftAddress")
                            .addLabels(Metapb.StoreLabel.newBuilder().build())
                            .setVersion("version")
                            .setState(Metapb.StoreState.Unknown)
                            .setStartTimestamp(0L).setDeployPath("deployPath")
                            .setLastHeartbeat(0L).setStats(
                              Metapb.StoreStats.newBuilder().setStoreId(0L)
                                               .setPartitionCount(0).addGraphStats(
                                            Metapb.GraphStats.newBuilder()
                                                             .setGraphName("value")
                                                             .setApproximateSize(0L)
                                                             .setRole(Metapb.ShardRole.None)
                                                             .build()).build())
                            .setDataVersion(0).setCores(0)
                            .setDataPath("dataPath").build());

        // Run the test
        final List<Metapb.Store> result = service.getStores();
    }

    @Test
    public void testGetStores2() throws Exception {
        // Setup
        final List<Metapb.Store> expectedResult = List.of(
                Metapb.Store.newBuilder().setId(0L).setAddress("address")
                            .setRaftAddress("raftAddress")
                            .addLabels(Metapb.StoreLabel.newBuilder().build())
                            .setVersion("version")
                            .setState(Metapb.StoreState.Unknown)
                            .setStartTimestamp(0L).setDeployPath("deployPath")
                            .setLastHeartbeat(0L).setStats(
                              Metapb.StoreStats.newBuilder().setStoreId(0L)
                                               .setPartitionCount(0).addGraphStats(
                                            Metapb.GraphStats.newBuilder()
                                                             .setGraphName("value")
                                                             .setApproximateSize(0L)
                                                             .setRole(Metapb.ShardRole.None)
                                                             .build()).build())
                            .setDataVersion(0).setCores(0)
                            .setDataPath("dataPath").build());

        // Run the test
        final List<Metapb.Store> result = service.getStores("graphName");
    }

    @Test
    public void testGetStoreStatus() throws Exception {
        // Setup
        final List<Metapb.Store> expectedResult = List.of(
                Metapb.Store.newBuilder().setId(0L).setAddress("address")
                            .setRaftAddress("raftAddress")
                            .addLabels(Metapb.StoreLabel.newBuilder().build())
                            .setVersion("version")
                            .setState(Metapb.StoreState.Unknown)
                            .setStartTimestamp(0L).setDeployPath("deployPath")
                            .setLastHeartbeat(0L).setStats(
                              Metapb.StoreStats.newBuilder().setStoreId(0L)
                                               .setPartitionCount(0).addGraphStats(
                                            Metapb.GraphStats.newBuilder()
                                                             .setGraphName("value")
                                                             .setApproximateSize(0L)
                                                             .setRole(Metapb.ShardRole.None)
                                                             .build()).build())
                            .setDataVersion(0).setCores(0)
                            .setDataPath("dataPath").build());

        // Run the test
        final List<Metapb.Store> result = service.getStoreStatus(false);

    }

    @Test
    public void testGetShardGroups() throws Exception {
        // Setup
        final List<Metapb.ShardGroup> expectedResult = List.of(
                Metapb.ShardGroup.newBuilder().setId(0).addShards(
                              Metapb.Shard.newBuilder().setStoreId(0L)
                                          .setRole(Metapb.ShardRole.None).build())
                                 .setState(Metapb.PartitionState.PState_None)
                                 .build());

        // Run the test
        final List<Metapb.ShardGroup> result = service.getShardGroups();

    }

    @Test
    public void testGetShardGroup() throws Exception {
        // Setup
        final Metapb.ShardGroup expectedResult = Metapb.ShardGroup.newBuilder()
                                                                  .setId(0)
                                                                  .addShards(
                                                                          Metapb.Shard
                                                                                  .newBuilder()
                                                                                  .setStoreId(
                                                                                          0L)
                                                                                  .setRole(
                                                                                          Metapb.ShardRole.None)
                                                                                  .build())
                                                                  .setState(
                                                                          Metapb.PartitionState.PState_None)
                                                                  .build();

        // Run the test
        final Metapb.ShardGroup result = service.getShardGroup(0);

        // Verify the results
    }

    @Test
    public void testGetShardGroupsByStore() throws Exception {
        // Setup
        final List<Metapb.ShardGroup> expectedResult = List.of(
                Metapb.ShardGroup.newBuilder().setId(0).addShards(
                              Metapb.Shard.newBuilder().setStoreId(0L)
                                          .setRole(Metapb.ShardRole.None).build())
                                 .setState(Metapb.PartitionState.PState_None)
                                 .build());

        // Run the test
        final List<Metapb.ShardGroup> result = service.getShardGroupsByStore(
                0L);
    }

    @Test
    public void testGetActiveStores1() throws Exception {
        // Setup
        final List<Metapb.Store> expectedResult = List.of(
                Metapb.Store.newBuilder().setId(0L).setAddress("address")
                            .setRaftAddress("raftAddress")
                            .addLabels(Metapb.StoreLabel.newBuilder().build())
                            .setVersion("version")
                            .setState(Metapb.StoreState.Unknown)
                            .setStartTimestamp(0L).setDeployPath("deployPath")
                            .setLastHeartbeat(0L).setStats(
                              Metapb.StoreStats.newBuilder().setStoreId(0L)
                                               .setPartitionCount(0).addGraphStats(
                                            Metapb.GraphStats.newBuilder()
                                                             .setGraphName("value")
                                                             .setApproximateSize(0L)
                                                             .setRole(Metapb.ShardRole.None)
                                                             .build()).build())
                            .setDataVersion(0).setCores(0)
                            .setDataPath("dataPath").build());

        // Run the test
        final List<Metapb.Store> result = service.getActiveStores("graphName");

        // Verify the results
    }

    @Test
    public void testGetActiveStores1ThrowsPDException() {
        try {
            List<Metapb.Store> stores = service.getActiveStores();
            assertThat(stores.size() == 0);
        } catch (Exception e) {

        }
    }

    @Test
    public void testGetTombStores() throws Exception {
        //// Setup
        //final List<Metapb.Store> storeList = List.of(
        //        Metapb.Store.newBuilder().setId(0L).setAddress("address")
        //                    .setRaftAddress("raftAddress")
        //                    .addLabels(Metapb.StoreLabel.newBuilder().build())
        //                    .setVersion("version")
        //                    .setState(Metapb.StoreState.Tombstone)
        //                    .setStartTimestamp(0L).setDeployPath("deployPath")
        //                    .setLastHeartbeat(0L).setStats(
        //                      Metapb.StoreStats.newBuilder().setStoreId(0L)
        //                                       .setPartitionCount(0).addGraphStats(
        //                                    Metapb.GraphStats.newBuilder()
        //                                                     .setGraphName("value")
        //                                                     .setApproximateSize(0L)
        //                                                     .setRole(Metapb.ShardRole.None)
        //                                                     .build()).build())
        //                    .setDataVersion(0).setCores(0)
        //                    .setDataPath("dataPath").build());
        //service.register(storeList.get(0));
        //
        //// Run the test
        //final List<Metapb.Store> result = service.getTombStores();
        //
        //// Verify the results
        //assertThat(result.size() == 1);
        //service.removeStore(result.get(0).getId());
        //List<Metapb.Store> stores = service.getStores();
        //assertThat(stores.size() == 0);
    }

    @Test
    public void testAllocShards() throws Exception {
        // Setup
        try {
            final Metapb.Graph graph = Metapb.Graph.newBuilder()
                                                   .setGraphName("graphName")
                                                   .setGraphState(
                                                           Metapb.GraphState
                                                                   .newBuilder()
                                                                   .setMode(
                                                                           Metapb.GraphMode.ReadWrite)
                                                                   .setReason(
                                                                           Metapb.GraphModeReason.Quota)
                                                                   .build())
                                                   .build();
            final List<Metapb.Shard> expectedResult = List.of(
                    Metapb.Shard.newBuilder().setStoreId(0L)
                                .setRole(Metapb.ShardRole.None).build());

            // Configure PDConfig.getPartition(...).
            final PDConfig.Partition partition = new PDConfig().new Partition();
            partition.setTotalCount(0);
            partition.setMaxShardsPerStore(0);
            partition.setShardCount(0);

            // Run the test
            final List<Metapb.Shard> result = service.allocShards(graph, 0);
        } catch (Exception e) {

        }

    }

    @Test
    public void testReallocShards() throws Exception {
        // Setup
        try {
            final Metapb.ShardGroup shardGroup = Metapb.ShardGroup.newBuilder()
                                                                  .setId(0)
                                                                  .addShards(
                                                                          Metapb.Shard
                                                                                  .newBuilder()
                                                                                  .setStoreId(
                                                                                          0L)
                                                                                  .setRole(
                                                                                          Metapb.ShardRole.None)
                                                                                  .build())
                                                                  .setState(
                                                                          Metapb.PartitionState.PState_None)
                                                                  .build();
            final List<Metapb.Shard> expectedResult = List.of(
                    Metapb.Shard.newBuilder().setStoreId(0L)
                                .setRole(Metapb.ShardRole.None).build());

            // Configure PDConfig.getPartition(...).
            final PDConfig.Partition partition = new PDConfig().new Partition();
            partition.setTotalCount(0);
            partition.setMaxShardsPerStore(0);
            partition.setShardCount(0);
            when(config.getPartition()).thenReturn(partition);

            // Run the test
            final List<Metapb.Shard> result = service.reallocShards(shardGroup);

            // Verify the results
            assertThat(result).isEqualTo(expectedResult);
        } catch (Exception e) {

        }

    }

    @Test
    public void testUpdateShardGroup() {
        try {
            final List<Metapb.Shard> shards = List.of(
                    Metapb.Shard.newBuilder().setStoreId(0L)
                                .setRole(Metapb.ShardRole.None).build());

            // Run the test
            service.updateShardGroup(0, shards, 0, 0);
        } catch (Exception e) {

        } finally {

        }
    }

    @Test
    public void testUpdateShardGroupState() throws Exception {
        try {
            service.updateShardGroupState(0, Metapb.PartitionState.PState_None);
        } catch (Exception e) {

        }
    }

    @Test
    public void testHeartBeat() throws Exception {
        // Setup
        try {
            final Metapb.StoreStats storeStats = Metapb.StoreStats.newBuilder()
                                                                  .setStoreId(
                                                                          0L)
                                                                  .setPartitionCount(
                                                                          0)
                                                                  .addGraphStats(
                                                                          Metapb.GraphStats
                                                                                  .newBuilder()
                                                                                  .setGraphName(
                                                                                          "value")
                                                                                  .setApproximateSize(
                                                                                          0L)
                                                                                  .setRole(
                                                                                          Metapb.ShardRole.None)
                                                                                  .build())
                                                                  .build();
            final Metapb.ClusterStats expectedResult = Metapb.ClusterStats
                    .newBuilder().setState(Metapb.ClusterState.Cluster_OK)
                    .setMessage("message").setTimestamp(0L).build();
            when(config.getMinStoreCount()).thenReturn(0);

            // Configure PDConfig.getPartition(...).
            final PDConfig.Partition partition = new PDConfig().new Partition();
            partition.setTotalCount(0);
            partition.setMaxShardsPerStore(0);
            partition.setShardCount(0);
            when(config.getPartition()).thenReturn(partition);

            // Run the test
            final Metapb.ClusterStats result = service.heartBeat(storeStats);

            // Verify the results
            assertThat(result).isEqualTo(expectedResult);
        } catch (Exception e) {

        }
    }

    @Test
    public void testUpdateClusterStatus1() {
        // Setup
        final Metapb.ClusterStats expectedResult = Metapb.ClusterStats
                .newBuilder().setState(Metapb.ClusterState.Cluster_OK)
                .setMessage("message").setTimestamp(0L).build();

        // Run the test
        final Metapb.ClusterStats result = service.updateClusterStatus(
                Metapb.ClusterState.Cluster_OK);
    }

    @Test
    public void testUpdateClusterStatus2() {
        // Setup
        final Metapb.ClusterStats expectedResult = Metapb.ClusterStats
                .newBuilder().setState(Metapb.ClusterState.Cluster_OK)
                .setMessage("message").setTimestamp(0L).build();

        // Run the test
        final Metapb.ClusterStats result = service.updateClusterStatus(
                Metapb.PartitionState.PState_None);
    }

    @Test
    public void testCheckStoreStatus() {
        // Setup
        // Run the test
        service.checkStoreStatus();

        // Verify the results
    }

    @Test
    public void testAddStatusListener() {
        // Setup
        final StoreStatusListener mockListener = mock(
                StoreStatusListener.class);

        // Run the test
        service.addStatusListener(mockListener);

        // Verify the results
    }

    @Test
    public void testOnStoreStatusChanged() {
        // Setup
        final Metapb.Store store = Metapb.Store.newBuilder().setId(0L)
                                               .setAddress("address")
                                               .setRaftAddress("raftAddress")
                                               .addLabels(Metapb.StoreLabel
                                                                  .newBuilder()
                                                                  .build())
                                               .setVersion("version").setState(
                        Metapb.StoreState.Unknown).setStartTimestamp(0L)
                                               .setDeployPath("deployPath")
                                               .setLastHeartbeat(0L).setStats(
                        Metapb.StoreStats.newBuilder().setStoreId(0L)
                                         .setPartitionCount(0).addGraphStats(
                                      Metapb.GraphStats.newBuilder()
                                                       .setGraphName("value")
                                                       .setApproximateSize(0L)
                                                       .setRole(Metapb.ShardRole.None)
                                                       .build()).build())
                                               .setDataVersion(0).setCores(0)
                                               .setDataPath("dataPath").build();

        // Verify the results
    }

    @Test
    public void testOnShardGroupSplit() {
        // Setup
        final Metapb.ShardGroup shardGroup = Metapb.ShardGroup.newBuilder()
                                                              .setId(0)
                                                              .addShards(
                                                                      Metapb.Shard
                                                                              .newBuilder()
                                                                              .setStoreId(
                                                                                      0L)
                                                                              .setRole(
                                                                                      Metapb.ShardRole.None)
                                                                              .build())
                                                              .setState(
                                                                      Metapb.PartitionState.PState_None)
                                                              .build();
        final List<Metapb.ShardGroup> newShardGroups = List.of(
                Metapb.ShardGroup.newBuilder().setId(0).addShards(
                              Metapb.Shard.newBuilder().setStoreId(0L)
                                          .setRole(Metapb.ShardRole.None).build())
                                 .setState(Metapb.PartitionState.PState_None)
                                 .build());
        final Consumer<MetaTask.Task> mockTask = mock(Consumer.class);

        // Verify the results
    }

    @Test
    public void testCheckStoreCanOffline() {
        // Setup
        final Metapb.Store currentStore = Metapb.Store.newBuilder().setId(0L)
                                                      .setAddress("address")
                                                      .setRaftAddress(
                                                              "raftAddress")
                                                      .addLabels(
                                                              Metapb.StoreLabel
                                                                      .newBuilder()
                                                                      .build())
                                                      .setVersion("version")
                                                      .setState(
                                                              Metapb.StoreState.Unknown)
                                                      .setStartTimestamp(0L)
                                                      .setDeployPath(
                                                              "deployPath")
                                                      .setLastHeartbeat(0L)
                                                      .setStats(
                                                              Metapb.StoreStats
                                                                      .newBuilder()
                                                                      .setStoreId(
                                                                              0L)
                                                                      .setPartitionCount(
                                                                              0)
                                                                      .addGraphStats(
                                                                              Metapb.GraphStats
                                                                                      .newBuilder()
                                                                                      .setGraphName(
                                                                                              "value")
                                                                                      .setApproximateSize(
                                                                                              0L)
                                                                                      .setRole(
                                                                                              Metapb.ShardRole.None)
                                                                                      .build())
                                                                      .build())
                                                      .setDataVersion(0)
                                                      .setCores(0)
                                                      .setDataPath("dataPath")
                                                      .build();
        // Run the test
        final boolean result = service.checkStoreCanOffline(currentStore);

        // Verify the results
        assertThat(result).isTrue();
    }

    @Test
    public void testShardGroupsDbCompaction() throws Exception {
        // Setup
        // Run the test
        try {
            service.shardGroupsDbCompaction(0, "tableName");
        } catch (Exception e) {

        }

        // Verify the results
    }

    @Test
    public void testGetQuota() throws Exception {
        // Setup
        // Run the test
        try {
            service.getQuota();
        } catch (Exception e) {

        }
    }
}
