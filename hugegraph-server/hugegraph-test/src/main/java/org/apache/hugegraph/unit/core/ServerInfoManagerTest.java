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

package org.apache.hugegraph.unit.core;

import java.util.concurrent.ExecutorService;

import org.apache.hugegraph.HugeGraphParams;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.masterelection.GlobalMasterInfo;
import org.apache.hugegraph.task.ServerInfoManager;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.testutil.Whitebox;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class ServerInfoManagerTest {

    private ServerInfoManager sysGraphManager;
    private ServerInfoManager hugegraphManager;

    @Before
    public void setup() {
        HugeGraphParams sysGraphParams = Mockito.mock(HugeGraphParams.class);
        Mockito.when(sysGraphParams.spaceGraphName())
               .thenReturn("DEFAULT-~sys_graph");

        HugeGraphParams hugegraphParams = Mockito.mock(HugeGraphParams.class);
        Mockito.when(hugegraphParams.spaceGraphName())
               .thenReturn("DEFAULT-hugegraph");

        ExecutorService executor = Mockito.mock(ExecutorService.class);

        this.sysGraphManager = new ServerInfoManager(sysGraphParams, executor);
        this.hugegraphManager = new ServerInfoManager(hugegraphParams, executor);
    }

    @Test
    public void testSelfNodeIdScopedByGraphWithSameNodeId() {
        GlobalMasterInfo nodeInfo = GlobalMasterInfo.master("server-1");

        Whitebox.setInternalState(this.sysGraphManager,
                                  "globalNodeInfo", nodeInfo);
        Whitebox.setInternalState(this.hugegraphManager,
                                  "globalNodeInfo", nodeInfo);

        Id sysGraphNodeId = this.sysGraphManager.selfNodeId();
        Id hugegraphNodeId = this.hugegraphManager.selfNodeId();

        Assert.assertEquals("DEFAULT-~sys_graph/server-1",
                            sysGraphNodeId.asString());
        Assert.assertEquals("DEFAULT-hugegraph/server-1",
                            hugegraphNodeId.asString());
        Assert.assertFalse(sysGraphNodeId.equals(hugegraphNodeId));
    }

    @Test
    public void testSelfNodeIdReturnsNullWhenNotInitialized() {
        Assert.assertNull(this.sysGraphManager.selfNodeId());
    }
}
