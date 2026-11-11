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

package org.apache.hugegraph.pd.raft;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.hugegraph.pd.config.PDConfig;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.testutil.Whitebox;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.util.Endpoint;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RaftEngineLeaderAddressTest {

    private static final String LEADER_IP = "10.0.0.1";
    private static final int GRPC_PORT = 8686;
    private static final String LEADER_GRPC_ADDRESS = "10.0.0.1:8686";

    private Node originalRaftNode;
    private RaftRpcClient originalRaftRpcClient;
    private PDConfig.Raft originalConfig;

    private Node mockNode;
    private RaftRpcClient mockRpcClient;
    private PDConfig.Raft mockConfig;
    private PeerId mockLeader;

    @Before
    public void setUp() {
        RaftEngine engine = RaftEngine.getInstance();

        // Save originals
        originalRaftNode = engine.getRaftNode();
        originalRaftRpcClient = Whitebox.getInternalState(engine, "raftRpcClient");
        originalConfig = Whitebox.getInternalState(engine, "config");

        // Build mock leader PeerId with real Endpoint
        mockLeader = mock(PeerId.class);
        Endpoint endpoint = new Endpoint(LEADER_IP, 8610);
        when(mockLeader.getEndpoint()).thenReturn(endpoint);

        // Build mock Node that reports itself as follower with a known leader
        mockNode = mock(Node.class);
        when(mockNode.isLeader(true)).thenReturn(false);
        when(mockNode.getLeaderId()).thenReturn(mockLeader);

        // Build mock config
        // Use a short default timeout (100ms); specific tests may override getRpcTimeout()
        mockConfig = mock(PDConfig.Raft.class);
        when(mockConfig.getGrpcAddress()).thenReturn("127.0.0.1:" + GRPC_PORT);
        when(mockConfig.getGrpcPort()).thenReturn(GRPC_PORT);
        when(mockConfig.getRpcTimeout()).thenReturn(100);

        // Build mock RpcClient
        mockRpcClient = mock(RaftRpcClient.class);

        // Inject mocks
        Whitebox.setInternalState(engine, "raftNode", mockNode);
        Whitebox.setInternalState(engine, "raftRpcClient", mockRpcClient);
        Whitebox.setInternalState(engine, "config", mockConfig);
    }

    @After
    public void tearDown() {
        RaftEngine engine = RaftEngine.getInstance();
        Whitebox.setInternalState(engine, "raftNode", originalRaftNode);
        Whitebox.setInternalState(engine, "raftRpcClient", originalRaftRpcClient);
        Whitebox.setInternalState(engine, "config", originalConfig);
    }

    @Test
    public void testSuccessReturnsGrpcAddress() throws Exception {
        // RPC succeeds and returns a valid gRPC address
        RaftRpcProcessor.GetMemberResponse response =
                mock(RaftRpcProcessor.GetMemberResponse.class);
        when(response.getGrpcAddress()).thenReturn(LEADER_GRPC_ADDRESS);

        CompletableFuture<RaftRpcProcessor.GetMemberResponse> future =
                CompletableFuture.completedFuture(response);
        when(mockRpcClient.getGrpcAddress(anyString())).thenReturn(future);

        String result = RaftEngine.getInstance().getLeaderGrpcAddress();
        Assert.assertEquals(LEADER_GRPC_ADDRESS, result);
    }

    @Test
    public void testTimeoutFallsBackToDerivedAddress() throws Exception {
        // RPC times out — should fall back to leaderIp:grpcPort
        CompletableFuture<RaftRpcProcessor.GetMemberResponse> future =
                mock(CompletableFuture.class);
        when(future.get(anyLong(), eq(TimeUnit.MILLISECONDS)))
                .thenThrow(new TimeoutException("simulated timeout"));
        when(mockRpcClient.getGrpcAddress(anyString())).thenReturn(future);

        String result = RaftEngine.getInstance().getLeaderGrpcAddress();
        Assert.assertEquals(LEADER_IP + ":" + GRPC_PORT, result);
    }

    @Test
    public void testRpcExceptionFallsBackToDerivedAddress() throws Exception {
        // RPC throws ExecutionException — should fall back to leaderIp:grpcPort
        CompletableFuture<RaftRpcProcessor.GetMemberResponse> future =
                mock(CompletableFuture.class);
        when(future.get(anyLong(), eq(TimeUnit.MILLISECONDS)))
                .thenThrow(new ExecutionException("simulated rpc failure",
                                                  new RuntimeException("bolt error")));
        when(mockRpcClient.getGrpcAddress(anyString())).thenReturn(future);

        String result = RaftEngine.getInstance().getLeaderGrpcAddress();
        Assert.assertEquals(LEADER_IP + ":" + GRPC_PORT, result);
    }

    @Test
    public void testNullResponseFallsBackToDerivedAddress() throws Exception {
        // RPC returns null response — should fall back to leaderIp:grpcPort
        CompletableFuture<RaftRpcProcessor.GetMemberResponse> future =
                CompletableFuture.completedFuture(null);
        when(mockRpcClient.getGrpcAddress(anyString())).thenReturn(future);

        String result = RaftEngine.getInstance().getLeaderGrpcAddress();
        Assert.assertEquals(LEADER_IP + ":" + GRPC_PORT, result);
    }

    @Test
    public void testNullGrpcAddressInResponseFallsBackToDerivedAddress() throws Exception {
        // RPC returns a response but grpcAddress field is null — should fall back
        RaftRpcProcessor.GetMemberResponse response =
                mock(RaftRpcProcessor.GetMemberResponse.class);
        when(response.getGrpcAddress()).thenReturn(null);

        CompletableFuture<RaftRpcProcessor.GetMemberResponse> future =
                CompletableFuture.completedFuture(response);
        when(mockRpcClient.getGrpcAddress(anyString())).thenReturn(future);

        String result = RaftEngine.getInstance().getLeaderGrpcAddress();
        Assert.assertEquals(LEADER_IP + ":" + GRPC_PORT, result);
    }

    @Test
    public void testNullLeaderAfterWaitThrowsExecutionException() throws Exception {
        // Use 0ms timeout so waitingForLeader(0) skips the wait loop and returns immediately
        when(mockConfig.getRpcTimeout()).thenReturn(0);
        // Leader is still null after waitingForLeader() — should throw ExecutionException
        when(mockNode.getLeaderId()).thenReturn(null);

        try {
            RaftEngine.getInstance().getLeaderGrpcAddress();
            Assert.fail("Expected ExecutionException");
        } catch (ExecutionException e) {
            Assert.assertTrue(e.getCause() instanceof IllegalStateException);
            Assert.assertEquals("Leader is not ready", e.getCause().getMessage());
        }
    }

    @Test(timeout = 10000)
    public void testGetMembersMarksUnresponsivePeerOffline() throws Exception {
        PeerId hanging = new PeerId("10.0.0.2", 8610);
        PeerId alive = new PeerId(LEADER_IP, 8610);
        List<PeerId> peers = new ArrayList<>();
        peers.add(hanging);
        peers.add(alive);
        when(mockNode.listPeers()).thenReturn(peers);
        when(mockNode.listLearners()).thenReturn(Collections.emptyList());
        when(mockNode.getLeaderId()).thenReturn(alive);

        RaftRpcProcessor.GetMemberResponse response = new RaftRpcProcessor.GetMemberResponse();
        response.setRaftAddress(LEADER_IP + ":8610");
        response.setGrpcAddress(LEADER_GRPC_ADDRESS);
        response.setDatePath("./pd_data");
        response.setRestAddress(LEADER_IP + ":8620");
        // A peer that never answers must not block the whole member listing
        when(mockRpcClient.getGrpcAddress(hanging.getEndpoint().toString()))
                .thenReturn(new CompletableFuture<>());
        when(mockRpcClient.getGrpcAddress(alive.getEndpoint().toString()))
                .thenReturn(CompletableFuture.completedFuture(response));

        List<Metapb.Member> members = RaftEngine.getInstance().getMembers();
        Assert.assertEquals(2, members.size());
        Assert.assertEquals(Metapb.StoreState.Offline, members.get(0).getState());
        Assert.assertEquals("10.0.0.2:8610", members.get(0).getRaftUrl());
        Assert.assertEquals(Metapb.StoreState.Up, members.get(1).getState());
        Assert.assertEquals(Metapb.ShardRole.Leader, members.get(1).getRole());
        Assert.assertEquals(LEADER_GRPC_ADDRESS, members.get(1).getGrpcUrl());
    }
}
