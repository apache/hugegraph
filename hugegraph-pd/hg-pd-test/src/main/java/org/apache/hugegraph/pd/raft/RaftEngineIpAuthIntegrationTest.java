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

import java.util.Collections;

import org.apache.hugegraph.pd.raft.auth.IpAuthHandler;
import org.apache.hugegraph.testutil.Whitebox;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.error.RaftError;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

public class RaftEngineIpAuthIntegrationTest {

    private Node originalRaftNode;

    @Before
    public void setUp() {
        // Save original raftNode so we can restore it after the test
        originalRaftNode = RaftEngine.getInstance().getRaftNode();
        // Reset IpAuthHandler singleton for a clean state
        Whitebox.setInternalState(IpAuthHandler.class, "instance", null);
    }

    @After
    public void tearDown() {
        // Restore original raftNode
        Whitebox.setInternalState(RaftEngine.getInstance(), "raftNode", originalRaftNode);
        // Reset IpAuthHandler singleton
        Whitebox.setInternalState(IpAuthHandler.class, "instance", null);
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

    private boolean invokeIsIpAllowed(IpAuthHandler handler, String ip) {
        return Whitebox.invoke(IpAuthHandler.class,
                               new Class[]{String.class},
                               "isIpAllowed", handler, ip);
    }
}
