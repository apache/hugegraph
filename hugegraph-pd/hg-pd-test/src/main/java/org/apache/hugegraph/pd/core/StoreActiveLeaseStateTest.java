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

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hugegraph.pd.StoreNodeService;
import org.apache.hugegraph.pd.config.PDConfig;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.pd.meta.StoreInfoMeta;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class StoreActiveLeaseStateTest {

    @Test
    public void testStaleActiveLeaseCannotSatisfyQuorumOrDrain() throws Exception {
        PDConfig config = new PDConfig();
        config.setInitialStoreCount(3);
        config.setMinStoreCount(0);
        PDConfig.Partition partition = config.new Partition();
        partition.setShardCount(3);
        partition.setTotalCount(1);
        partition.setMaxShardsPerStore(3);
        config.setPartition(partition);

        Map<Long, Metapb.Store> stores = new HashMap<>();
        for (long id = 1; id <= 6; id++) {
            stores.put(id, store(id, Metapb.StoreState.Up));
        }
        Metapb.Store stalePendingLease = stores.get(2L);
        Metapb.Store staleTombstoneLease = stores.get(3L);
        stores.put(2L, store(2L, Metapb.StoreState.Pending));
        stores.put(3L, store(3L, Metapb.StoreState.Tombstone));

        List<byte[]> activeLeases = List.of(
                stores.get(1L).toByteArray(),
                stalePendingLease.toByteArray(),
                staleTombstoneLease.toByteArray(),
                stores.get(4L).toByteArray(),
                stores.get(5L).toByteArray(),
                stores.get(6L).toByteArray());
        StoreInfoMeta meta = Mockito.mock(StoreInfoMeta.class, Mockito.CALLS_REAL_METHODS);
        Mockito.doReturn(activeLeases)
               .when(meta).getListWithTTL(Mockito.any(byte[].class));
        Mockito.doAnswer(call -> stores.get(call.<Long>getArgument(0)))
               .when(meta).getStore(Mockito.anyLong());
        Mockito.doReturn(List.of(group(0, 1, 2, 3))).when(meta).getShardGroups();

        StoreNodeService service =
                Mockito.mock(StoreNodeService.class, Mockito.CALLS_REAL_METHODS);
        set(service, StoreNodeService.class, "pdConfig", config);
        set(service, StoreNodeService.class, "storeInfoMeta", meta);
        set(service, StoreNodeService.class, "clusterStats",
            Metapb.ClusterStats.newBuilder()
                                .setState(Metapb.ClusterState.Cluster_Not_Ready).build());

        Assert.assertEquals(List.of(1L, 4L, 5L, 6L),
                            service.getActiveStores().stream()
                                   .map(Metapb.Store::getId).toList());

        service.checkStoreStatus();
        Assert.assertEquals(Metapb.ClusterState.Cluster_Not_Ready,
                            service.getClusterStats().getState());
        // Unrelated Up Stores satisfy placement capacity, so this specifically proves
        // stale voter leases cannot authorize another drain.
        Assert.assertFalse(service.checkStoreCanOffline(stores.get(6L)));
    }

    private static Metapb.Store store(long id, Metapb.StoreState state) {
        return Metapb.Store.newBuilder().setId(id).setState(state).build();
    }

    private static Metapb.ShardGroup group(int id, long... storeIds) {
        Metapb.ShardGroup.Builder group = Metapb.ShardGroup.newBuilder().setId(id);
        for (int i = 0; i < storeIds.length; i++) {
            group.addShards(Metapb.Shard.newBuilder()
                                        .setStoreId(storeIds[i])
                                        .setRole(i == 0 ? Metapb.ShardRole.Leader :
                                                 Metapb.ShardRole.Follower).build());
        }
        return group.build();
    }

    private static void set(Object instance, Class<?> type, String fieldName, Object value)
            throws Exception {
        Field field = type.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(instance, value);
    }
}
