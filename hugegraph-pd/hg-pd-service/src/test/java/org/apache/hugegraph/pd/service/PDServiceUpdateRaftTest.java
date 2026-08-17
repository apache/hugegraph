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

import java.util.Collections;

import org.apache.hugegraph.pd.config.PDConfig;
import org.apache.hugegraph.pd.grpc.Pdpb;
import org.apache.hugegraph.pd.raft.RaftEngine;
import org.apache.hugegraph.pd.raft.auth.IpAuthHandler;
import org.apache.hugegraph.testutil.Whitebox;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.error.RaftError;

import io.grpc.stub.StreamObserver;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PDServiceUpdateRaftTest {

    private Node originalRaftNode;
    private Node mockNode;
    private PDService service;
    private PeerId leader;

    @Before
    public void setUp() {
        this.originalRaftNode = RaftEngine.getInstance().getRaftNode();
        IpAuthHandler.shutdownInstance();

        this.leader = new PeerId();
        Assert.assertTrue(this.leader.parse("127.0.0.1:8610"));
        this.mockNode = mock(Node.class);
        when(this.mockNode.isLeader(true)).thenReturn(true);
        when(this.mockNode.getLeaderId()).thenReturn(this.leader);
        when(this.mockNode.listPeers()).thenReturn(
                Collections.singletonList(this.leader));
        Whitebox.setInternalState(RaftEngine.getInstance(), "raftNode",
                                  this.mockNode);
        IpAuthHandler.getInstance(Collections.singleton("127.0.0.1"));

        PDConfig pdConfig = new PDConfig();
        PDConfig.Raft raft = pdConfig.new Raft();
        raft.setRpcTimeout(1);
        pdConfig.setRaft(raft);
        this.service = new PDService();
        this.service.setInitConfig(pdConfig);
    }

    @After
    public void tearDown() {
        Whitebox.setInternalState(RaftEngine.getInstance(), "raftNode",
                                  this.originalRaftNode);
        IpAuthHandler.shutdownInstance();
    }

    @Test
    public void testRejectsMalformedConfigBeforeRaft() {
        Pdpb.UpdatePdRaftResponse response = update(
                "127.0.0.1:8610/leader,bad,127.0.0.2:8610/follower");

        Assert.assertEquals(6668, response.getHeader().getError().getTypeValue());
        Assert.assertTrue(response.getHeader().getError().getMessage()
                                  .contains("Invalid Raft peer"));
        verify(this.mockNode, never()).changePeers(
                any(Configuration.class), any(Closure.class));
    }

    @Test
    public void testReturnsSuccessAfterRaftCallbackAndAllowlistRefresh()
            throws Exception {
        IpAuthHandler handler = IpAuthHandler.requireActiveInstance();
        handler.refresh(Collections.singleton("10.0.0.1"));
        doAnswer(invocation -> {
            Closure closure = invocation.getArgument(1);
            closure.run(Status.OK());
            return null;
        }).when(this.mockNode).changePeers(any(Configuration.class),
                                          any(Closure.class));

        Pdpb.UpdatePdRaftResponse response = update(
                "127.0.0.1:8610/leader");

        Assert.assertEquals(Pdpb.ErrorType.OK,
                            response.getHeader().getError().getType());
        Assert.assertTrue(isIpAllowed(handler, "127.0.0.1"));
        Assert.assertFalse(isIpAllowed(handler, "10.0.0.1"));
    }

    @Test
    public void testReturnsRaftFailureFromCallback() {
        doAnswer(invocation -> {
            Closure closure = invocation.getArgument(1);
            closure.run(new Status(RaftError.EINTERNAL, "simulated failure"));
            return null;
        }).when(this.mockNode).changePeers(any(Configuration.class),
                                          any(Closure.class));

        Pdpb.UpdatePdRaftResponse response = update(
                "127.0.0.1:8610/leader");

        Assert.assertEquals(6670, response.getHeader().getError().getTypeValue());
        Assert.assertTrue(response.getHeader().getError().getMessage()
                                  .contains("simulated failure"));
    }

    @Test
    public void testReturnsTimeoutWhenRaftDoesNotCallback() {
        Pdpb.UpdatePdRaftResponse response = update(
                "127.0.0.1:8610/leader");

        Assert.assertEquals(6669, response.getHeader().getError().getTypeValue());
        Assert.assertTrue(response.getHeader().getError().getMessage()
                                  .contains("timed out"));
    }

    @Test
    public void testRejectsMissingAllowlistBeforeRaft() {
        IpAuthHandler.shutdownInstance();

        Pdpb.UpdatePdRaftResponse response = update(
                "127.0.0.1:8610/leader");

        Assert.assertEquals(6670, response.getHeader().getError().getTypeValue());
        Assert.assertTrue(response.getHeader().getError().getMessage()
                                  .contains("not active"));
        verify(this.mockNode, never()).changePeers(
                any(Configuration.class), any(Closure.class));
    }

    @Test
    public void testMapsSynchronousRaftFailure() {
        doThrow(new IllegalStateException("node stopped"))
                .when(this.mockNode)
                .changePeers(any(Configuration.class), any(Closure.class));

        Pdpb.UpdatePdRaftResponse response = update(
                "127.0.0.1:8610/leader");

        Assert.assertEquals(6670, response.getHeader().getError().getTypeValue());
        Assert.assertTrue(response.getHeader().getError().getMessage()
                                  .contains("node stopped"));
    }

    @SuppressWarnings("unchecked")
    private Pdpb.UpdatePdRaftResponse update(String config) {
        StreamObserver<Pdpb.UpdatePdRaftResponse> observer =
                mock(StreamObserver.class);
        this.service.updatePdRaft(
                Pdpb.UpdatePdRaftRequest.newBuilder().setConfig(config).build(),
                observer);
        ArgumentCaptor<Pdpb.UpdatePdRaftResponse> response =
                ArgumentCaptor.forClass(Pdpb.UpdatePdRaftResponse.class);
        verify(observer).onNext(response.capture());
        verify(observer).onCompleted();
        return response.getValue();
    }

    private boolean isIpAllowed(IpAuthHandler handler, String ip) {
        return Whitebox.invoke(IpAuthHandler.class,
                               new Class[]{String.class},
                               "isIpAllowed", handler, ip);
    }
}
