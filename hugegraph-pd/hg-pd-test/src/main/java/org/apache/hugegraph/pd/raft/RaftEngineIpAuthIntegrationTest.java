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
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.hugegraph.pd.config.PDConfig;
import org.apache.hugegraph.pd.raft.auth.IpAuthHandler;
import org.apache.hugegraph.testutil.Whitebox;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.alipay.remoting.ExtendedNettyChannelHandler;
import com.alipay.remoting.config.BoltServerOption;
import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.rpc.RpcServer;
import com.alipay.sofa.jraft.rpc.impl.BoltRpcServer;

import io.netty.channel.ChannelHandler;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RaftEngineIpAuthIntegrationTest {

    private Node originalRaftNode;
    private Object originalTxnPeerGate;

    @Before
    public void setUp() {
        // Save original raftNode so we can restore it after the test
        originalRaftNode = RaftEngine.getInstance().getRaftNode();
        // changePeerList probes the new peers first; here every peer answers as upgraded
        originalTxnPeerGate = Whitebox.getInternalState(RaftEngine.getInstance(),
                                                        "txnPeerGate");
        Whitebox.setInternalState(RaftEngine.getInstance(), "txnPeerGate",
                                  gateAnswering(KVOperation.TXN));
        // Reset IpAuthHandler singleton for a clean state
        Whitebox.setInternalState(IpAuthHandler.class, "instance", null);
    }

    @After
    public void tearDown() {
        // Restore original raftNode
        Whitebox.setInternalState(RaftEngine.getInstance(), "raftNode", originalRaftNode);
        Whitebox.setInternalState(RaftEngine.getInstance(), "txnPeerGate", originalTxnPeerGate);
        // Reset IpAuthHandler singleton
        Whitebox.setInternalState(IpAuthHandler.class, "instance", null);
    }

    /**
     * A gate whose every peer answers the member probe with the given highest KV opcode
     */
    private static TxnPeerGate gateAnswering(byte maxKvOp) {
        return new TxnPeerGate(Collections::emptyList, endpoint -> {
            RaftRpcProcessor.GetMemberResponse response = new RaftRpcProcessor.GetMemberResponse();
            response.setMaxKvOp(maxKvOp);
            return CompletableFuture.completedFuture(response);
        }, 3000L, 100L, System::currentTimeMillis);
    }

    /**
     * A membership change probes only the peers it adds. A kept member that answers as an
     * older release, or does not answer, does not stop it: the check before every TXN covers
     * kept members. updatePdRaft goes through the same RaftEngine.checkTxnPeers(Configuration).
     */
    @Test
    public void testChangePeerListIgnoresKeptMembers() throws Exception {
        List<String> probed = new CopyOnWriteArrayList<>();
        Whitebox.setInternalState(RaftEngine.getInstance(), "txnPeerGate", new TxnPeerGate(
                Collections::emptyList, endpoint -> {
                    probed.add(endpoint);
                    if (endpoint.startsWith("127.0.0.3")) {
                        return CompletableFuture.failedFuture(
                                new IllegalStateException("connect refused"));
                    }
                    RaftRpcProcessor.GetMemberResponse response =
                            new RaftRpcProcessor.GetMemberResponse();
                    response.setMaxKvOp(endpoint.startsWith("127.0.0.2") ? (byte) 0 :
                                        KVOperation.TXN);
                    return CompletableFuture.completedFuture(response);
                }, 3000L, 100L, System::currentTimeMillis));
        Node mockNode = mock(Node.class);
        // Kept: an old release (127.0.0.2) and a silent one (127.0.0.3); 127.0.0.4 is a learner
        when(mockNode.listPeers()).thenReturn(new ArrayList<>(List.of(
                PeerId.parsePeer("127.0.0.2:8610"), PeerId.parsePeer("127.0.0.3:8610"))));
        when(mockNode.listLearners()).thenReturn(new ArrayList<>(List.of(
                PeerId.parsePeer("127.0.0.4:8610"))));
        doAnswer(invocation -> {
            Closure closure = invocation.getArgument(1);
            closure.run(Status.OK());
            return null;
        }).when(mockNode).changePeers(any(Configuration.class), any(Closure.class));
        Whitebox.setInternalState(RaftEngine.getInstance(), "raftNode", mockNode);
        // changePeerList bounds its wait by the raft rpc timeout; this node is 127.0.0.1
        Object originalConfig = Whitebox.getInternalState(RaftEngine.getInstance(), "config");
        PDConfig.Raft raft = new PDConfig().new Raft();
        raft.setAddress("127.0.0.1:8610");
        raft.setRpcTimeout(1000);
        Whitebox.setInternalState(RaftEngine.getInstance(), "config", raft);
        try {
            // Keeps both members and adds 127.0.0.5, which answers as upgraded
            Status status = RaftEngine.getInstance().changePeerList(
                    "127.0.0.2:8610,127.0.0.3:8610,127.0.0.5:8610");
            Assert.assertTrue(status.getErrorMsg(), status.isOk());
            verify(mockNode).changePeers(any(Configuration.class), any(Closure.class));
            Assert.assertEquals(List.of("127.0.0.5:8610"), probed);

            // The path of updatePdRaft: the learner is kept as well, so nothing is probed
            probed.clear();
            Configuration next = new Configuration();
            next.parse("127.0.0.2:8610,127.0.0.3:8610");
            next.addLearner(PeerId.parsePeer("127.0.0.4:8610"));
            RaftEngine.getInstance().checkTxnPeers(next);
            Assert.assertTrue(probed.isEmpty());
        } finally {
            Whitebox.setInternalState(RaftEngine.getInstance(), "config", originalConfig);
        }
    }

    /**
     * A peer of the new configuration that answers as a release without the TXN entry stops
     * the membership change before changePeers: it would replay the log and skip TXN entries
     */
    @Test
    public void testChangePeerListRefusesAPeerWithoutTheTxnEntry() throws Exception {
        IpAuthHandler handler = IpAuthHandler.getInstance(
                Collections.singleton("10.0.0.1"));
        Whitebox.setInternalState(RaftEngine.getInstance(), "txnPeerGate",
                                  gateAnswering((byte) 0));
        Node mockNode = mock(Node.class);
        Whitebox.setInternalState(RaftEngine.getInstance(), "raftNode", mockNode);

        Status status = RaftEngine.getInstance().changePeerList("127.0.0.1:8610");

        Assert.assertFalse(status.isOk());
        Assert.assertTrue(status.getErrorMsg(), status.getErrorMsg().contains("127.0.0.1:8610"));
        verify(mockNode, never()).changePeers(any(Configuration.class), any(Closure.class));
        Assert.assertTrue(invokeIsIpAllowed(handler, "10.0.0.1"));
        Assert.assertFalse(invokeIsIpAllowed(handler, "127.0.0.1"));
    }

    @Test
    public void testChangePeerListRefreshesIpAuthHandler() throws Exception {
        // Initialize IpAuthHandler with an old IP
        IpAuthHandler handler = IpAuthHandler.getInstance(
                Collections.singleton("10.0.0.1"));
        Assert.assertTrue(invokeIsIpAllowed(handler, "10.0.0.1"));
        Assert.assertFalse(invokeIsIpAllowed(handler, "127.0.0.1"));

        // Mock Node to fire the changePeers callback synchronously with Status.OK()
        // This simulates a successful peer change without a real Raft cluster

        // Important: fire the closure synchronously or changePeerList() will
        // block on latch.await(...) until the configured timeout elapses
        Node mockNode = mock(Node.class);
        doAnswer(invocation -> {
            Closure closure = invocation.getArgument(1);
            closure.run(Status.OK());
            return null;
        }).when(mockNode).changePeers(any(Configuration.class), any(Closure.class));

        // Inject mock node into RaftEngine
        Whitebox.setInternalState(RaftEngine.getInstance(), "raftNode", mockNode);

        // Call changePeerList with new peer — must be odd count
        RaftEngine.getInstance().changePeerList("127.0.0.1:8610");

        // Verify IpAuthHandler was refreshed with the new peer IP
        Assert.assertTrue(invokeIsIpAllowed(handler, "127.0.0.1"));
        // Old IP should no longer be allowed
        Assert.assertFalse(invokeIsIpAllowed(handler, "10.0.0.1"));
    }

    @Test
    public void testChangePeerListDoesNotRefreshOnFailure() throws Exception {
        // Initialize IpAuthHandler with original IP
        IpAuthHandler handler = IpAuthHandler.getInstance(
                Collections.singleton("10.0.0.1"));
        Assert.assertTrue(invokeIsIpAllowed(handler, "10.0.0.1"));

        // Mock Node to fire callback with a failed status
        // Simulates a failed peer change — handler should NOT be refreshed

        // Important: fire the closure synchronously or changePeerList() will
        // block on latch.await(...) until the configured timeout elapses
        Node mockNode = mock(Node.class);
        doAnswer(invocation -> {
            Closure closure = invocation.getArgument(1);
            closure.run(new Status(RaftError.EINTERNAL, "simulated failure"));
            return null;
        }).when(mockNode).changePeers(any(Configuration.class), any(Closure.class));

        Whitebox.setInternalState(RaftEngine.getInstance(), "raftNode", mockNode);

        RaftEngine.getInstance().changePeerList("127.0.0.1:8610");

        // Handler should NOT be refreshed — old IP still allowed
        Assert.assertTrue(invokeIsIpAllowed(handler, "10.0.0.1"));
        Assert.assertFalse(invokeIsIpAllowed(handler, "127.0.0.1"));
    }

    @Test
    public void testIpWhitelistConfigurationControlsPipeline() {
        assertIpWhitelistPipeline(null, true);
        assertIpWhitelistPipeline(true, true);
        assertIpWhitelistPipeline(false, false);
    }

    private void assertIpWhitelistPipeline(Boolean configured,
                                           boolean expectedInstalled) {
        PDConfig pdConfig = new PDConfig();
        PDConfig.Raft raftConfig = pdConfig.new Raft();
        if (configured != null) {
            raftConfig.setIpWhitelistEnabled(configured);
        }

        com.alipay.remoting.rpc.RpcServer server =
                new com.alipay.remoting.rpc.RpcServer();
        RpcServer rpcServer = new BoltRpcServer(server);
        Whitebox.invokeStatic(
                RaftEngine.class,
                new Class[]{List.class, RpcServer.class, boolean.class},
                "configureRaftServerIpWhitelist",
                Collections.emptyList(), rpcServer,
                raftConfig.isIpWhitelistEnabled());

        ExtendedNettyChannelHandler pipeline =
                server.option(BoltServerOption.EXTENDED_NETTY_CHANNEL_HANDLER);
        if (!expectedInstalled) {
            Assert.assertNull(pipeline);
            return;
        }
        Assert.assertNotNull(pipeline);
        List<ChannelHandler> handlers = pipeline.frontChannelHandlers();
        Assert.assertEquals(1, handlers.size());
        Assert.assertTrue(handlers.get(0) instanceof IpAuthHandler);
    }

    private boolean invokeIsIpAllowed(IpAuthHandler handler, String ip) {
        return Whitebox.invoke(IpAuthHandler.class,
                               new Class[]{String.class},
                               "isIpAllowed", handler, ip);
    }
}
