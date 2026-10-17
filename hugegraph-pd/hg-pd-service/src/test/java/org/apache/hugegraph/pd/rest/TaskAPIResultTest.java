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

import java.lang.reflect.Proxy;
import java.util.Map;

import org.apache.hugegraph.pd.TaskScheduleService.PartitionPatrolResult;
import org.apache.hugegraph.pd.common.KVPair;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.pd.grpc.Pdpb;
import org.apache.hugegraph.pd.raft.RaftEngine;
import org.apache.hugegraph.pd.service.PDRestService;
import org.apache.hugegraph.testutil.Whitebox;
import org.junit.Assert;
import org.junit.Test;

import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.entity.PeerId;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class TaskAPIResultTest {

    @Test
    public void testFollowerRefusesEveryTaskWithoutInvokingScheduler() throws Exception {
        RaftEngine engine = RaftEngine.getInstance();
        Node original = engine.getRaftNode();
        Node follower = (Node) Proxy.newProxyInstance(
                Node.class.getClassLoader(), new Class<?>[]{Node.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("isLeader")) {
                        return false;
                    }
                    if (method.getName().equals("getLeaderId")) {
                        return new PeerId("127.0.0.2", 8610);
                    }
                    throw new AssertionError("unexpected node call: " + method.getName());
                });
        Whitebox.setInternalState(engine, "raftNode", follower);
        try {
            TaskAPI api = new TaskAPI();
            // Its scheduler is deliberately unset: no follower request may reach it.
            api.pdRestService = new PDRestService();
            for (String response : new String[]{api.patrolStores(), api.patrolPartitions(),
                                               api.balancePartitions(), api.balanceLeaders(),
                                               api.splitPartitions(), api.dbCompaction()}) {
                JsonNode body = json(response);
                Assert.assertEquals(Pdpb.ErrorType.NOT_LEADER_VALUE, body.get("status").asInt());
                Assert.assertTrue(body.get("error").asText().contains("127.0.0.2:8610"));
            }
            StoreAPI alias = new StoreAPI();
            alias.pdRestService = api.pdRestService;
            Assert.assertEquals(Pdpb.ErrorType.NOT_LEADER_VALUE,
                                json(alias.balanceLeaders()).get("status").asInt());
        } finally {
            Whitebox.setInternalState(engine, "raftNode", original);
        }
    }

    @Test
    public void testPatrolReportsReallocationsAndOfflineRequests() throws Exception {
        PartitionPatrolResult result = new PartitionPatrolResult();
        result.getReallocatedShardGroups().add(7);
        result.getOfflineShardRequests().put(8, 42L);
        result.getPartitions().add(Metapb.Partition.newBuilder().setId(8)
                                                 .setGraphName("graph").build());
        TaskAPI api = patrolAPI(result);

        JsonNode body = json(api.patrolPartitions());
        Assert.assertEquals(0, body.get("status").asInt());
        Assert.assertEquals(7, body.get("reallocatedShardGroups").get(0).asInt());
        Assert.assertEquals(42L, body.get("offlineShardRequests").get("8").asLong());
        Assert.assertEquals(8, body.get("partitions").get(0).get("id").asInt());
    }

    @Test
    public void testPatrolWithNothingToDoHasEmptyResults() throws Exception {
        JsonNode body = json(patrolAPI(new PartitionPatrolResult()).patrolPartitions());
        Assert.assertEquals(0, body.get("status").asInt());
        Assert.assertEquals(0, body.get("partitions").size());
        Assert.assertEquals(0, body.get("reallocatedShardGroups").size());
        Assert.assertEquals(0, body.get("offlineShardRequests").size());
    }

    @Test
    public void testSkippedBalanceDiffersFromCompletedEmptyBalance() throws Exception {
        TaskAPI skipped = new TaskAPI();
        skipped.pdRestService = balanceService(null);
        Assert.assertEquals("skipped", json(skipped.balancePartitions()).get("outcome").asText());

        TaskAPI completed = new TaskAPI();
        completed.pdRestService = balanceService(Map.of());
        Assert.assertEquals("{}", completed.balancePartitions());
    }

    private static TaskAPI patrolAPI(PartitionPatrolResult result) {
        TaskAPI api = new TaskAPI();
        api.pdRestService = new PDRestService() {
            @Override
            public PartitionPatrolResult patrolPartitionsWithResult() {
                return result;
            }
        };
        return api;
    }

    private static PDRestService balanceService(Map<Integer, KVPair<Long, Long>> result) {
        return new PDRestService() {
            @Override
            public Map<Integer, KVPair<Long, Long>> balancePartitions() {
                return result;
            }
        };
    }

    private static JsonNode json(String response) throws Exception {
        return new ObjectMapper().readTree(response);
    }
}
