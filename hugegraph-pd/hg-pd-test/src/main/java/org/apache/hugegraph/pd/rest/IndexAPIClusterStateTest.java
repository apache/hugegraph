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

package org.apache.hugegraph.pd.rest;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.hugegraph.pd.StoreNodeService;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.pd.service.PDService;
import org.junit.Assert;
import org.junit.Test;

public class IndexAPIClusterStateTest {

    @Test
    public void testRestRecomputesBeforeReadingNotReady() {
        StoreNodeService stores = mock(StoreNodeService.class);
        when(stores.getClusterStats()).thenReturn(stats(Metapb.ClusterState.Cluster_Not_Ready));
        IndexAPI api = api(stores);

        Assert.assertEquals("Cluster_Not_Ready", api.clusterState());
        verify(stores, times(1)).checkStoreStatus();
    }

    @Test
    public void testRestRecomputesBeforeReadingOk() {
        StoreNodeService stores = mock(StoreNodeService.class);
        when(stores.getClusterStats()).thenReturn(stats(Metapb.ClusterState.Cluster_OK));
        IndexAPI api = api(stores);

        Assert.assertEquals("Cluster_OK", api.clusterState());
        verify(stores, times(1)).checkStoreStatus();
    }

    private static IndexAPI api(StoreNodeService stores) {
        PDService pdService = mock(PDService.class);
        when(pdService.getStoreNodeService()).thenReturn(stores);
        IndexAPI api = new IndexAPI();
        api.pdService = pdService;
        return api;
    }

    private static Metapb.ClusterStats stats(Metapb.ClusterState state) {
        return Metapb.ClusterStats.newBuilder().setState(state).build();
    }
}
