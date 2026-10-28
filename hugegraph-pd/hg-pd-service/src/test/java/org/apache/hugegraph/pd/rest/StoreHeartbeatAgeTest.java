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

import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.pd.service.PDRestService;
import org.junit.Assert;
import org.junit.Test;

public class StoreHeartbeatAgeTest {

    @Test
    public void testStaleHeartbeatIsVisibleEvenWhileStoreIsUp() {
        long before = System.currentTimeMillis();
        StoreAPI.StoreStatistics statistics = statistics(before - 60000L);
        long after = System.currentTimeMillis();
        Assert.assertEquals("Up", statistics.getState());
        Assert.assertEquals(before - 60000L, statistics.getLastHeartBeat());
        Assert.assertTrue(statistics.getHeartbeatAgeMillis() >= 60000L);
        Assert.assertTrue(statistics.getHeartbeatAgeMillis() <= after - before + 60000L);
    }

    @Test
    public void testMissingHeartbeatHasUnknownAge() {
        Assert.assertEquals(-1L, statistics(0L).getHeartbeatAgeMillis());
    }

    @Test
    public void testFutureHeartbeatDoesNotHaveNegativeAge() {
        Assert.assertEquals(0L, statistics(System.currentTimeMillis() + 3600000L).getHeartbeatAgeMillis());
    }

    private static StoreAPI.StoreStatistics statistics(long lastHeartbeat) {
        Metapb.Store store = Metapb.Store.newBuilder().setId(7).setState(Metapb.StoreState.Up)
                                   .setLastHeartbeat(lastHeartbeat).build();
        StoreAPI api = new StoreAPI();
        api.pdRestService = new PDRestService() {
            @Override
            public Metapb.Store getStore(long storeId) {
                return store;
            }
        };
        return api.new StoreStatistics(store);
    }
}
