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

import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hugegraph.testutil.Whitebox;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.rpc.CliRequests.AddLearnersRequest;
import com.alipay.sofa.jraft.rpc.CliRequests.AddPeerRequest;
import com.alipay.sofa.jraft.rpc.CliRequests.ChangePeersRequest;
import com.alipay.sofa.jraft.rpc.CliRequests.RemoveLearnersRequest;
import com.alipay.sofa.jraft.rpc.CliRequests.RemovePeerRequest;
import com.alipay.sofa.jraft.rpc.CliRequests.ResetLearnersRequest;
import com.alipay.sofa.jraft.rpc.CliRequests.ResetPeerRequest;
import com.alipay.sofa.jraft.rpc.CliRequests.TransferLeaderRequest;
import com.alipay.sofa.jraft.rpc.RpcRequests.AppendEntriesRequest;
import com.alipay.sofa.jraft.rpc.RpcRequests.GetFileRequest;
import com.alipay.sofa.jraft.rpc.RpcRequests.InstallSnapshotRequest;
import com.alipay.sofa.jraft.rpc.RpcRequests.PingRequest;
import com.alipay.sofa.jraft.rpc.RpcRequests.ReadIndexRequest;
import com.alipay.sofa.jraft.rpc.RpcRequests.RequestVoteRequest;
import com.alipay.sofa.jraft.rpc.RpcRequests.TimeoutNowRequest;
import com.alipay.sofa.jraft.rpc.RpcServer;
import com.alipay.sofa.jraft.rpc.impl.BoltRpcServer;

public class RaftEngineCliProcessorsTest {

    private RpcServer rpcServer;

    @After
    public void tearDown() {
        if (this.rpcServer != null) {
            this.rpcServer.shutdown();
            this.rpcServer = null;
        }
    }

    // The PD raft port must only serve raft replication: jraft CLI membership
    // requests would change peers or the leader bypassing the checks of
    // RaftEngine#changePeerList and PDService#updatePdRaft
    @Test
    public void testRaftRpcServerRegistersNoCliMembershipProcessors() throws Exception {
        this.rpcServer = createRaftRpcServer();
        Set<String> interests = registeredProcessorInterests(this.rpcServer);

        Assert.assertFalse(interests.contains(AddPeerRequest.class.getName()));
        Assert.assertFalse(interests.contains(RemovePeerRequest.class.getName()));
        Assert.assertFalse(interests.contains(ResetPeerRequest.class.getName()));
        Assert.assertFalse(interests.contains(ChangePeersRequest.class.getName()));
        Assert.assertFalse(interests.contains(TransferLeaderRequest.class.getName()));
        Assert.assertFalse(interests.contains(AddLearnersRequest.class.getName()));
        Assert.assertFalse(interests.contains(RemoveLearnersRequest.class.getName()));
        Assert.assertFalse(interests.contains(ResetLearnersRequest.class.getName()));
    }

    @Test
    public void testRaftRpcServerRegistersReplicationProcessors() throws Exception {
        this.rpcServer = createRaftRpcServer();
        Set<String> interests = registeredProcessorInterests(this.rpcServer);

        Assert.assertTrue(interests.contains(AppendEntriesRequest.class.getName()));
        Assert.assertTrue(interests.contains(RequestVoteRequest.class.getName()));
        Assert.assertTrue(interests.contains(InstallSnapshotRequest.class.getName()));
        Assert.assertTrue(interests.contains(GetFileRequest.class.getName()));
        Assert.assertTrue(interests.contains(ReadIndexRequest.class.getName()));
        Assert.assertTrue(interests.contains(TimeoutNowRequest.class.getName()));
        Assert.assertTrue(interests.contains(PingRequest.class.getName()));
        Assert.assertTrue(
                interests.contains(RaftRpcProcessor.GetMemberRequest.class.getName()));
    }

    private RpcServer createRaftRpcServer() throws Exception {
        int port;
        try (ServerSocket socket = new ServerSocket(0)) {
            port = socket.getLocalPort();
        }
        return Whitebox.invoke(RaftEngine.class,
                               new Class[]{String.class, List.class, boolean.class},
                               "createRaftRpcServer", RaftEngine.getInstance(),
                               "127.0.0.1:" + port, new ArrayList<PeerId>(), Boolean.FALSE);
    }

    private static Set<String> registeredProcessorInterests(RpcServer server) {
        Assert.assertTrue(server instanceof BoltRpcServer);
        com.alipay.remoting.rpc.RpcServer boltServer = ((BoltRpcServer) server).getServer();
        Map<String, ?> processors = Whitebox.getInternalState(boltServer, "userProcessors");
        return processors.keySet();
    }
}
