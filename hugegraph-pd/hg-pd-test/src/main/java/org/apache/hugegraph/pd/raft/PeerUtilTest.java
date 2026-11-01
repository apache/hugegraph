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

import java.util.List;

import org.apache.hugegraph.pd.common.KVPair;
import org.junit.Assert;
import org.junit.Test;

import com.alipay.sofa.jraft.entity.PeerId;

public class PeerUtilTest {

    @Test
    public void testIsPeerEqualsComparesOnlyIpAndPort() {
        PeerId peer = new PeerId("10.0.0.1", 8610);
        // Same endpoint with a different replica index and priority still names the same peer
        PeerId sameEndpoint = new PeerId("10.0.0.1", 8610, 3, 100);

        Assert.assertTrue(PeerUtil.isPeerEquals(peer, sameEndpoint));
        Assert.assertFalse(PeerUtil.isPeerEquals(peer, new PeerId("10.0.0.1", 8611)));
        Assert.assertFalse(PeerUtil.isPeerEquals(peer, new PeerId("10.0.0.2", 8610)));
    }

    @Test
    public void testIsPeerEqualsHandlesNull() {
        PeerId peer = new PeerId("10.0.0.1", 8610);

        Assert.assertTrue(PeerUtil.isPeerEquals(null, null));
        Assert.assertFalse(PeerUtil.isPeerEquals(peer, null));
        Assert.assertFalse(PeerUtil.isPeerEquals(null, peer));
    }

    @Test
    public void testParseConfigAssignsRolesFromSuffix() {
        List<KVPair<String, PeerId>> peers = PeerUtil.parseConfig(
                "10.0.0.1:8610/leader,10.0.0.2:8610/learner,10.0.0.3:8610/follower," +
                "10.0.0.4:8610");

        Assert.assertEquals(4, peers.size());
        assertPeer(peers.get(0), "leader", "10.0.0.1", 8610);
        assertPeer(peers.get(1), "learner", "10.0.0.2", 8610);
        assertPeer(peers.get(2), "follower", "10.0.0.3", 8610);
        // A peer without a role suffix defaults to follower
        assertPeer(peers.get(3), "follower", "10.0.0.4", 8610);
    }

    @Test
    public void testParseConfigKeepsReplicaIndex() {
        List<KVPair<String, PeerId>> peers = PeerUtil.parseConfig("10.0.0.1:8610:2/learner");

        Assert.assertEquals(1, peers.size());
        Assert.assertEquals("learner", peers.get(0).getKey());
        Assert.assertEquals(2, peers.get(0).getValue().getIdx());
    }

    @Test
    public void testParseConfigOfEmptyInputIsEmpty() {
        Assert.assertTrue(PeerUtil.parseConfig(null).isEmpty());
        Assert.assertTrue(PeerUtil.parseConfig("").isEmpty());
    }

    @Test
    public void testParseConfigRejectsMalformedPeer() {
        Assert.assertThrows(IllegalArgumentException.class,
                            () -> PeerUtil.parseConfig("10.0.0.1:8610,not-a-peer"));
        Assert.assertThrows(IllegalArgumentException.class,
                            () -> PeerUtil.parseConfig("10.0.0.1:port/leader"));
        // An unknown role is not stripped, so the peer string itself becomes invalid
        Assert.assertThrows(IllegalArgumentException.class,
                            () -> PeerUtil.parseConfig("10.0.0.1:8610/witness"));
    }

    private static void assertPeer(KVPair<String, PeerId> pair, String role, String ip,
                                   int port) {
        Assert.assertEquals(role, pair.getKey());
        Assert.assertEquals(ip, pair.getValue().getIp());
        Assert.assertEquals(port, pair.getValue().getPort());
    }
}
