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

import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.core.NodeImpl;
import com.alipay.sofa.jraft.core.Replicator;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.storage.LogManager;
import org.apache.hugegraph.testutil.Whitebox;

import org.junit.Assert;
import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RaftReflectionUtilTest {

    @Test
    public void testGetReplicatorStateWithNullNode() {
        // Setup
        PeerId peerId = mock(PeerId.class);

        // Run the test
        Replicator.State result = RaftReflectionUtil.getReplicatorState(null, peerId);

        // Verify the results
        Assert.assertNull(result);
    }

    @Test
    public void testGetReplicatorStateWithNullPeerId() {
        // Setup
        Node node = mock(Node.class);

        // Run the test
        Replicator.State result = RaftReflectionUtil.getReplicatorState(node, null);

        // Verify the results
        Assert.assertNull(result);
    }

    @Test
    public void testGetReplicatorStateWithBothNull() {
        // Run the test
        Replicator.State result = RaftReflectionUtil.getReplicatorState(null, null);

        // Verify the results
        Assert.assertNull(result);
    }

    /**
     * The replay guard reads the raft log through the private field NodeImpl.logManager of the
     * jraft in the pom. A jraft that renames or retypes it fails here, not on a production
     * restart, where every replay would go unchecked
     */
    @Test
    public void testLogManagerIsReadFromTheJraftNode() {
        NodeImpl node = new NodeImpl("pd_raft", new PeerId());
        Assert.assertNull(RaftReflectionUtil.getLogManager(node));

        LogManager logManager = mock(LogManager.class);
        when(logManager.getLastLogIndex()).thenReturn(42L);
        when(logManager.getTerm(42L)).thenReturn(7L);
        Whitebox.setInternalState(node, "logManager", logManager);
        Assert.assertSame(logManager, RaftReflectionUtil.getLogManager(node));
        Assert.assertEquals(42L, RaftReflectionUtil.getLogManager(node).getLastLogIndex());
        Assert.assertEquals(7L, RaftReflectionUtil.getLogManager(node).getTerm(42L));

        // A node that is not a NodeImpl has no such field: unknown, not a failure
        Assert.assertNull(RaftReflectionUtil.getLogManager(mock(Node.class)));
        Assert.assertNull(RaftReflectionUtil.getLogManager(null));
    }
}
