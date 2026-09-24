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

package org.apache.hugegraph.store.core.store;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.List;

import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.store.HgStoreEngine;
import org.apache.hugegraph.store.core.StoreEngineTestBase;
import org.apache.hugegraph.store.pd.FakePdServiceProvider;
import org.apache.hugegraph.testutil.Assert;
import org.junit.Before;
import org.junit.Test;

public class HgStoreEngineTest extends StoreEngineTestBase {

    private HgStoreEngine engine;

    @Before
    public void setup() {
        engine = HgStoreEngine.getInstance();
    }

    @Test
    public void testGetInstance() {
        assertNotNull(HgStoreEngine.getInstance());
    }

    // @Test
    // TODO: npe and not leader
    public void testStateChanged() {
        createPartitionEngine(0);
        var storeId = FakePdServiceProvider.makeStoreId("127.0.0.1:6511");
        var store = engine.getPartitionManager().getStore(storeId);
        engine.stateChanged(store, Metapb.StoreState.Offline, Metapb.StoreState.Up);
        assertEquals(engine.getPartitionEngines().size(), 1);
    }

    @Test
    public void testCreatePartitionEngine() {
        var partition = getPartition(0);
        assertNotNull(engine.createPartitionEngine(partition));
    }

    @Test
    public void testCreatePartitionGroups() {
        var partition = getPartition(0);
        engine.createPartitionGroups(partition);
    }

    @Test
    public void testDestroyPartitionEngine() {
        createPartitionEngine(16);
        engine.destroyPartitionEngine(16, List.of("graph0"));
        // assertEquals(engine.getPartitionEngines().size(), 0);
    }

    @Test
    public void testDeletePartition() {
        createPartitionEngine(0);
        engine.deletePartition(0, "graph0");
        // TODO: check logic
        assertEquals(engine.getPartitionEngines().size(), 1);
    }

    @Test
    public void testGetLeaderPartition() throws InterruptedException {
        createPartitionEngine(0);
        assertEquals(engine.getLeaderPartition().size(), 1);
    }

    @Test
    public void testGetAlivePeers() throws InterruptedException {
        createPartitionEngine(0);
        assertEquals(engine.getAlivePeers(0).size(), 1);
    }

    @Test
    public void testGetLeaderTerm() {
        createPartitionEngine(0);
        // no vote
        assertEquals(engine.getLeaderTerm(0), -1);
    }

    @Test
    public void testGetCommittedIndex() throws InterruptedException {
        createPartitionEngine(0);
        // write something background
        Assert.assertTrue(engine.getCommittedIndex(0) > 0);
    }

    @Test
    public void testGetRaftRpcServer() {
        assertNotNull(engine.getRaftRpcServer());
    }

    @Test
    public void testGetPartitionManager() {
        assertNotNull(engine.getPartitionManager());
    }

    @Test
    public void testGetDataManager() {
        assertNotNull(engine.getDataManager());
    }

    @Test
    public void testGetPdProvider() {
        assertNotNull(engine.getPdProvider());
    }

    @Test
    public void testGetCmdClient() {
        assertNotNull(engine.getHgCmdClient());
    }

    @Test
    public void testGetHeartbeatService() {
        assertNotNull(engine.getHeartbeatService());
    }

    @Test
    public void testIsClusterReady() throws InterruptedException {
        // wait heart beat
        Thread.sleep(2000);
        assertNotNull(engine.isClusterReady());
    }

    @Test
    public void testGetDataLocations() {
        assertEquals(engine.getDataLocations().size(), 1);
    }

    @Test
    public void testGetPartitionEngine() {
        createPartitionEngine(0);
        assertNotNull(engine.getPartitionEngine(0));
    }

    @Test
    public void testGetPartitionEngines() {
        createPartitionEngine(0);
        assertEquals(engine.getPartitionEngines().size(), 1);
    }

    @Test
    public void testGetNodeMetrics() {
        assertNotNull(engine.getNodeMetrics());
    }

    @Test
    public void testGetRaftGroupCount() {
        createPartitionEngine(0);
        assertEquals(engine.getRaftGroupCount(), 1);
    }

}
