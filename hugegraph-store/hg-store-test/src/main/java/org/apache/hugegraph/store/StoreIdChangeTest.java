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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.nio.file.Files;
import java.util.List;
import java.util.Map;

import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.store.meta.PartitionManager;
import org.apache.hugegraph.store.meta.Shard;
import org.apache.hugegraph.store.meta.ShardGroup;
import org.apache.hugegraph.store.meta.Store;
import org.apache.hugegraph.store.options.HgStoreEngineOptions;
import org.apache.hugegraph.store.pd.PdProvider;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * A Store rebuilt with an empty disk at the same raft address registers under a new store id.
 */
public class StoreIdChangeTest {

    private static final String ADDR0 = "store-0:8510";
    private static final String ADDR1 = "store-1:8510";
    private static final String ADDR2 = "store-2:8510";
    private static final long S0 = 10L;
    private static final long S1 = 11L;
    // old and new id of the Store at ADDR2
    private static final long X = 12L;
    private static final long Y = 13L;

    private PdProvider pdProvider;
    private PartitionManager partitionManager;

    @Before
    public void setUp() throws Exception {
        pdProvider = Mockito.mock(PdProvider.class);
        mockStore(S0, ADDR0);
        mockStore(S1, ADDR1);
        mockStore(X, ADDR2);
        mockStore(Y, ADDR2);
        String path = Files.createTempDirectory("store-id-change").toString();
        HgStoreEngineOptions options = new HgStoreEngineOptions();
        options.setDataPath(path);
        options.setRaftPath(path);
        partitionManager = new PartitionManager(pdProvider, options);
    }

    private void mockStore(long id, String raftAddress) {
        Store store = new Store(Metapb.Store.newBuilder().setId(id)
                                            .setRaftAddress(raftAddress).build());
        Mockito.when(pdProvider.getStoreByID(id)).thenReturn(store);
    }

    private static ShardGroup localGroup(long... storeIds) {
        ShardGroup group = new ShardGroup();
        group.setId(1);
        for (long id : storeIds) {
            Shard shard = new Shard();
            shard.setStoreId(id);
            shard.setRole(Metapb.ShardRole.Follower);
            group.getShards().add(shard);
        }
        return group;
    }

    private static Metapb.ShardGroup pdGroup(long... storeIds) {
        Metapb.ShardGroup.Builder builder = Metapb.ShardGroup.newBuilder().setId(1);
        for (long id : storeIds) {
            builder.addShards(Metapb.Shard.newBuilder().setStoreId(id)
                                          .setRole(Metapb.ShardRole.Follower));
        }
        return builder.build();
    }

    @Test
    public void testEndpointKeepsLocalIdStillInPdGroup() {
        Store store = partitionManager.getStoreByRaftEndpoint(localGroup(S0, S1, X),
                                                              pdGroup(S0, S1, X), ADDR2);
        assertEquals(X, store.getId());
    }

    @Test
    public void testEndpointTakesPdIdWhenLocalIdRetired() {
        Store store = partitionManager.getStoreByRaftEndpoint(localGroup(S0, S1, X),
                                                              pdGroup(S0, S1, Y), ADDR2);
        assertEquals(Y, store.getId());
    }

    @Test
    public void testEndpointTakesPdIdWhenLocalIdDeleted() {
        Mockito.when(pdProvider.getStoreByID(X)).thenReturn(null);
        Store store = partitionManager.getStoreByRaftEndpoint(localGroup(S0, S1, X),
                                                              pdGroup(S0, S1, Y), ADDR2);
        assertEquals(Y, store.getId());
    }

    @Test
    public void testEndpointWithoutPdGroupUsesLocalGroup() {
        Store store = partitionManager.getStoreByRaftEndpoint(localGroup(S0, S1, X), null,
                                                              ADDR2);
        assertEquals(X, store.getId());
        // unknown endpoint: id 0, the caller asks the endpoint itself
        store = partitionManager.getStoreByRaftEndpoint(localGroup(S0, S1, X),
                                                        pdGroup(S0, S1, X), "store-3:8510");
        assertEquals(0L, store.getId());
    }

    @Test
    public void testChangedEndpointsFindsNewIdAtSameAddress() {
        Map<String, Long> pdIds = partitionManager.shardIdsByEndpoint(
                pdGroup(S0, S1, Y).getShardsList());
        Map<String, Long> localIds = partitionManager.shardIdsByEndpoint(
                localGroup(S0, S1, X).getMetaPbShard());
        List<String> changed = PartitionEngine.changedShardEndpoints(
                pdIds, localIds, List.of(ADDR0, ADDR1, ADDR2), ADDR0);
        assertEquals(List.of(ADDR2), changed);
    }

    @Test
    public void testChangedEndpointsIgnoresMembershipChange() {
        Map<String, Long> pdIds = Map.of(ADDR0, S0, ADDR1, S1, "store-3:8510", Y);
        Map<String, Long> localIds = Map.of(ADDR0, S0, ADDR1, S1, ADDR2, X);
        assertTrue(PartitionEngine.changedShardEndpoints(
                pdIds, localIds, List.of(ADDR0, ADDR1, ADDR2), ADDR0).isEmpty());
    }

    @Test
    public void testChangedEndpointsNeverIncludesSelf() {
        Map<String, Long> pdIds = Map.of(ADDR0, Y, ADDR1, S1, ADDR2, X);
        Map<String, Long> localIds = Map.of(ADDR0, S0, ADDR1, S1, ADDR2, X);
        assertTrue(PartitionEngine.changedShardEndpoints(
                pdIds, localIds, List.of(ADDR0, ADDR1, ADDR2), ADDR0).isEmpty());
    }

    @Test
    public void testChangedEndpointsEmptyWhenIdsMatch() {
        Map<String, Long> ids = Map.of(ADDR0, S0, ADDR1, S1, ADDR2, Y);
        assertTrue(PartitionEngine.changedShardEndpoints(
                ids, ids, List.of(ADDR0, ADDR1, ADDR2), ADDR0).isEmpty());
    }
}
