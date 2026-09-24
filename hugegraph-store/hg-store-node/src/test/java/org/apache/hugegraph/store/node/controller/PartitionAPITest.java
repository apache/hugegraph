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

package org.apache.hugegraph.store.node.controller;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hugegraph.store.HgStoreEngine;
import org.apache.hugegraph.store.PartitionEngine;
import org.apache.hugegraph.store.business.BusinessHandler;
import org.apache.hugegraph.store.node.grpc.HgStoreNodeService;
import org.junit.Assert;
import org.junit.Test;

import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.core.State;
import com.alipay.sofa.jraft.entity.PeerId;

public class PartitionAPITest {

    private static final String NOT_LEADER = "Not leader";

    @Test
    public void testGetPartitionsOnFollower() {
        PartitionEngine follower = mockEngine(1, false);
        PartitionEngine leader = mockEngine(2, true);
        PartitionAPI api = newApi(follower, leader);

        @SuppressWarnings("unchecked")
        List<PartitionAPI.Raft> rafts =
                (List<PartitionAPI.Raft>) api.getPartitions("").get("partitions");

        Assert.assertEquals(2, rafts.size());
        PartitionAPI.Raft followerRaft = rafts.get(0);
        Assert.assertEquals(1, followerRaft.getGroupId());
        Assert.assertEquals(State.STATE_FOLLOWER.name(), followerRaft.getRole());
        Assert.assertNull(followerRaft.getConf());
        Assert.assertNull(followerRaft.getPeers());
        Assert.assertNull(followerRaft.getLearners());

        PartitionAPI.Raft leaderRaft = rafts.get(1);
        Assert.assertEquals(2, leaderRaft.getGroupId());
        Assert.assertEquals(State.STATE_LEADER.name(), leaderRaft.getRole());
        Assert.assertEquals("127.0.0.1:8510", leaderRaft.getConf());
        Assert.assertEquals(Collections.singletonList(PeerId.parsePeer("127.0.0.1:8510")),
                            leaderRaft.getPeers());
    }

    @Test
    public void testGetPartitionsWhenLeaderStepsDown() {
        // isLeader() still returns true, but the node stepped down before listPeers()
        PartitionEngine steppedDown = mockEngine(1, true, false);
        PartitionAPI api = newApi(steppedDown);

        @SuppressWarnings("unchecked")
        List<PartitionAPI.Raft> rafts =
                (List<PartitionAPI.Raft>) api.getPartitions("").get("partitions");

        Assert.assertEquals(1, rafts.size());
        PartitionAPI.Raft raft = rafts.get(0);
        Assert.assertEquals(1, raft.getGroupId());
        Assert.assertNull(raft.getConf());
        Assert.assertNull(raft.getPeers());
        Assert.assertNull(raft.getLearners());
    }

    private static PartitionAPI newApi(PartitionEngine... engines) {
        Map<Integer, PartitionEngine> partitionEngines = new LinkedHashMap<>();
        for (PartitionEngine engine : engines) {
            partitionEngines.put(engine.getGroupId(), engine);
        }
        HgStoreEngine storeEngine = mock(HgStoreEngine.class);
        when(storeEngine.getBusinessHandler()).thenReturn(mock(BusinessHandler.class));
        when(storeEngine.getPartitionEngines()).thenReturn(partitionEngines);
        HgStoreNodeService nodeService = mock(HgStoreNodeService.class);
        when(nodeService.getStoreEngine()).thenReturn(storeEngine);

        PartitionAPI api = new PartitionAPI();
        api.nodeService = nodeService;
        return api;
    }

    private static PartitionEngine mockEngine(int groupId, boolean isLeader) {
        return mockEngine(groupId, isLeader, isLeader);
    }

    private static PartitionEngine mockEngine(int groupId, boolean isLeader, boolean nodeLeads) {
        // Same contract as jraft NodeImpl: peer and learner lists are leader-only
        Node node = mock(Node.class);
        List<PeerId> peers = Collections.singletonList(PeerId.parsePeer("127.0.0.1:8510"));
        if (nodeLeads) {
            when(node.getNodeState()).thenReturn(State.STATE_LEADER);
            when(node.listPeers()).thenReturn(peers);
            when(node.listLearners()).thenReturn(Collections.emptyList());
        } else {
            when(node.getNodeState()).thenReturn(State.STATE_FOLLOWER);
            when(node.listPeers()).thenThrow(new IllegalStateException(NOT_LEADER));
            when(node.listLearners()).thenThrow(new IllegalStateException(NOT_LEADER));
        }

        PartitionEngine engine = mock(PartitionEngine.class);
        when(engine.getGroupId()).thenReturn(groupId);
        when(engine.isLeader()).thenReturn(isLeader);
        when(engine.getRaftNode()).thenReturn(node);
        when(engine.getPartitions()).thenReturn(Collections.emptyMap());
        if (nodeLeads) {
            when(engine.getCurrentConf()).thenReturn(new Configuration(peers));
        } else {
            when(engine.getCurrentConf()).thenThrow(new IllegalStateException(NOT_LEADER));
        }
        return engine;
    }
}
