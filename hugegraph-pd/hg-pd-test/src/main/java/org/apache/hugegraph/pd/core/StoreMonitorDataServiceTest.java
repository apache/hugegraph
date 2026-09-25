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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;

import org.apache.hugegraph.pd.StoreMonitorDataService;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.junit.Before;
import org.junit.Test;

public class StoreMonitorDataServiceTest extends PDCoreTestBase {

    private StoreMonitorDataService service;

    @Before
    public void init() {
        this.service = getStoreMonitorDataService();
        var store = getPdConfig().getStore();
        store.setMonitorDataEnabled(true);
        store.setMonitorDataInterval("1s");
        getPdConfig().setStore(store);
    }

    @Test
    public void test() throws InterruptedException, PDException {
        long now = System.currentTimeMillis() / 1000;
        for (int i = 0; i < 5; i++) {
            this.service.saveMonitorData(genStats());
            now = System.currentTimeMillis() / 1000;
            Thread.sleep(1100);
        }
        // The one-second lookup window can expire between two reads.
        long latest = this.service.getLatestStoreMonitorDataTimeStamp(1);
        assertTrue(latest == 0 || latest == now);

        var data = this.service.getStoreMonitorData(1);
        assertEquals(5, data.size());

        assertNotNull(this.service.debugMonitorInfo(List.of(Metapb.RecordPair.newBuilder()
                                                                             .setKey("key1")
                                                                             .setValue(1)
                                                                             .build())));

        assertNotNull(this.service.getStoreMonitorDataText(1));

        this.service.removeExpiredMonitorData(1, now + 1);
        assertEquals(0, this.service.getStoreMonitorData(1).size());
    }

    private Metapb.StoreStats genStats() {
        return Metapb.StoreStats.newBuilder()
                                .setStoreId(1)
                                .addSystemMetrics(
                                        Metapb.RecordPair.newBuilder().setKey("key1").setValue(1)
                                                         .build())
                                .build();
    }

}
