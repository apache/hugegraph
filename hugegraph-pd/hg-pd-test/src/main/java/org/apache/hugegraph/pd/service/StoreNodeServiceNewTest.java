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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.apache.hugegraph.pd.StoreNodeService;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.junit.Before;
import org.junit.Test;

public class StoreNodeServiceNewTest extends PdTestBase {

    private StoreNodeService service;

    @Before
    public void init() {
        service = getStoreNodeService();
    }

    @Test
    public void testGetTaskInfoMeta() {
        assertNotNull(service.getTaskInfoMeta());
    }

    public void testGetStoreInfoMeta() {
        assertNotNull(service.getStoreInfoMeta());
    }

    @Test
    public void testRemoveShardGroup() throws PDException {
        for (int i = 0; i < 12; i++) {
            Metapb.ShardGroup group = Metapb.ShardGroup.newBuilder()
                                                       .setId(i)
                                                       .setState(
                                                               Metapb.PartitionState.PState_Offline)
                                                       .build();
            service.getStoreInfoMeta().updateShardGroup(group);
        }

        service.deleteShardGroup(11);
        service.deleteShardGroup(10);

        assertEquals(10, getPdConfig().getConfigService().getPDConfig().getPartitionCount());
        // restore
        getPdConfig().getConfigService().setPartitionCount(12);
    }
}
