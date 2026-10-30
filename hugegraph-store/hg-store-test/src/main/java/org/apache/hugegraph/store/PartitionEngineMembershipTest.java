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

package org.apache.hugegraph.store;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hugegraph.store.cmd.HgCmdClient;
import org.apache.hugegraph.store.meta.PartitionManager;
import org.apache.hugegraph.store.meta.ShardGroup;
import org.apache.hugegraph.store.raft.util.RaftUtils;
import org.apache.hugegraph.store.util.HgRaftError;
import org.junit.Test;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.JRaftUtils;
import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.entity.NodeId;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.error.RaftError;

public class PartitionEngineMembershipTest {

    private static final String A = "127.0.0.1:18510";
    private static final String B = "127.0.0.1:18511";
    private static final String C = "127.0.0.1:18512";
    private static final String D = "127.0.0.1:18513";
    private static final List<String> TARGET = List.of(A, B, C);

    @Test
    public void testResumesExistingLearnersWithoutRecreatingNodes() throws Exception {
        Fixture fixture = new Fixture(List.of(A), List.of(B, C));

        assertTrue(fixture.engine.changePeers(TARGET, null).isOk());

        fixture.assertMembership(TARGET, List.of());
        assertEquals(List.of(List.of(B, C)), fixture.engine.waitedPeers);
        assertTrue(fixture.created.isEmpty());
        assertEquals(0, fixture.engine.snapshots);
    }

    @Test
    public void testPromotesExistingAndNewLearnersTogether() throws Exception {
        Fixture fixture = new Fixture(List.of(A), List.of(B));

        assertTrue(fixture.engine.changePeers(TARGET, null).isOk());

        fixture.assertMembership(TARGET, List.of());
        assertEquals(List.of(C), fixture.created);
        assertEquals(List.of(List.of(B, C)), fixture.engine.waitedPeers);
    }

    @Test
    public void testResumesAfterOneLearnerWasAlreadyPromoted() throws Exception {
        Fixture fixture = new Fixture(List.of(A, B), List.of(C));

        assertTrue(fixture.engine.changePeers(TARGET, null).isOk());

        fixture.assertMembership(TARGET, List.of());
        assertEquals(List.of(List.of(C)), fixture.engine.waitedPeers);
        assertTrue(fixture.created.isEmpty());
    }

    @Test
    public void testCatchupTimeoutResumesWithoutRecreatingLearners() throws Exception {
        Fixture fixture = new Fixture(List.of(A), List.of());
        fixture.engine.waitResults.add(false);
        fixture.engine.waitResults.add(true);

        Status first = fixture.engine.changePeers(TARGET, null);
        assertEquals(HgRaftError.TASK_CONTINUE.getNumber(), first.getCode());
        fixture.assertMembership(List.of(A), List.of(B, C));

        assertTrue(fixture.engine.changePeers(TARGET, null).isOk());
        fixture.assertMembership(TARGET, List.of());
        assertEquals(List.of(B, C), fixture.created);
        assertEquals(List.of(List.of(B, C), List.of(B, C)), fixture.engine.waitedPeers);
    }

    @Test
    public void testExistingLearnersRemainLearnersUntilCaughtUp() throws Exception {
        Fixture fixture = new Fixture(List.of(A), List.of(B, C));
        fixture.engine.waitResults.add(false);

        Status result = fixture.engine.changePeers(TARGET, null);

        assertEquals(HgRaftError.TASK_CONTINUE.getNumber(), result.getCode());
        fixture.assertMembership(List.of(A), List.of(B, C));
        verify(fixture.node, never()).removeLearners(anyList(), any(Closure.class));
        assertTrue(fixture.changed.isEmpty());
    }

    @Test
    public void testLearnerRemovalFailureStopsPromotion() throws Exception {
        Fixture fixture = new Fixture(List.of(A), List.of(B, C));
        fixture.removeResults.add(new Status(RaftError.EIO, "cannot remove learners"));

        Status result = fixture.engine.changePeers(TARGET, null);

        assertFalse(result.isOk());
        fixture.assertMembership(List.of(A), List.of(B, C));
        assertTrue(fixture.changed.isEmpty());
        assertTrue(fixture.destroyed.isEmpty());
    }

    @Test
    public void testPromotionFailureIsNotReportedAsSuccess() throws Exception {
        Fixture fixture = new Fixture(List.of(A), List.of(B, C));
        fixture.changeResults.add(new Status(RaftError.EBUSY, "configuration busy"));

        Status result = fixture.engine.changePeers(TARGET, null);

        assertFalse(result.isOk());
        fixture.assertMembership(List.of(A), List.of());
        assertTrue(fixture.destroyed.isEmpty());
    }

    @Test
    public void testRetriesAfterLearnersWereRemovedButPromotionFailed() throws Exception {
        Fixture fixture = new Fixture(List.of(A), List.of(B, C));
        fixture.changeResults.add(new Status(RaftError.EBUSY, "configuration busy"));

        Status first = fixture.engine.changePeers(TARGET, null);

        assertEquals(HgRaftError.TASK_ERROR.getNumber(), first.getCode());
        fixture.assertMembership(List.of(A), List.of());
        assertTrue(fixture.created.isEmpty());

        assertTrue(fixture.engine.changePeers(TARGET, null).isOk());
        fixture.assertMembership(TARGET, List.of());
        assertEquals(List.of(B, C), fixture.created);
        assertEquals(List.of(List.of(B, C), List.of(B, C)), fixture.engine.waitedPeers);
        assertTrue(fixture.destroyed.isEmpty());
    }

    @Test
    public void testRemovesStaleLearnerEvenWhenVotersAlreadyMatch() throws Exception {
        Fixture fixture = new Fixture(TARGET, List.of(D));

        assertTrue(fixture.engine.changePeers(TARGET, null).isOk());

        fixture.assertMembership(TARGET, List.of());
        assertEquals(List.of(D), fixture.destroyed);
        assertTrue(fixture.engine.waitedPeers.isEmpty());
    }

    @Test
    public void testKeepsStaleLearnerIfTargetPromotionFails() throws Exception {
        Fixture fixture = new Fixture(List.of(A), List.of(B, C, D));
        fixture.changeResults.add(new Status(RaftError.EBUSY, "configuration busy"));

        Status result = fixture.engine.changePeers(TARGET, null);

        assertFalse(result.isOk());
        fixture.assertMembership(List.of(A), List.of(D));
        assertTrue(fixture.destroyed.isEmpty());
    }

    @Test
    public void testHealthyTargetRemainsANoOp() throws Exception {
        Fixture fixture = new Fixture(TARGET, List.of());

        assertTrue(fixture.engine.changePeers(TARGET, null).isOk());

        assertTrue(fixture.changed.isEmpty());
        assertTrue(fixture.created.isEmpty());
        assertTrue(fixture.destroyed.isEmpty());
        assertTrue(fixture.engine.waitedPeers.isEmpty());
    }

    @Test
    public void testFailedLearnerConfigurationDoesNotStartPromotion() throws Exception {
        Fixture fixture = new Fixture(List.of(A), List.of());
        fixture.changeResults.add(new Status(RaftError.EIO, "cannot add learners"));

        Status result = fixture.engine.changePeers(TARGET, null);

        assertFalse(result.isOk());
        fixture.assertMembership(List.of(A), List.of());
        assertTrue(fixture.engine.waitedPeers.isEmpty());
        verify(fixture.node, never()).removeLearners(anyList(), any(Closure.class));
    }

    @Test
    public void testRemovingLeaderStillTransfersAndRequestsContinuation() throws Exception {
        Fixture fixture = new Fixture(TARGET, List.of(D));

        Status result = fixture.engine.changePeers(List.of(B, C, D), null);

        assertEquals(HgRaftError.TASK_CONTINUE.getNumber(), result.getCode());
        fixture.assertMembership(List.of(A, B, C, D), List.of());
        verify(fixture.node).transferLeadershipTo(PeerId.ANY_PEER);
        assertTrue(fixture.destroyed.isEmpty());
    }

    private static class Fixture {

        private final Node node = mock(Node.class);
        private final TestEngine engine;
        private final AtomicReference<Configuration> membership;
        private final Queue<Status> changeResults = new ArrayDeque<>();
        private final Queue<Status> removeResults = new ArrayDeque<>();
        private final List<Configuration> changed = new ArrayList<>();
        private final List<String> created = new ArrayList<>();
        private final List<String> destroyed = new ArrayList<>();

        Fixture(List<String> voters, List<String> learners) throws Exception {
            Configuration initial = new Configuration();
            voters.forEach(peer -> initial.addPeer(JRaftUtils.getPeerId(peer)));
            learners.forEach(peer -> initial.addLearner(JRaftUtils.getPeerId(peer)));
            this.membership = new AtomicReference<>(initial);
            HgStoreEngine store = mock(HgStoreEngine.class);
            PartitionManager manager = mock(PartitionManager.class);
            HgCmdClient client = mock(HgCmdClient.class);
            when(store.getPartitionManager()).thenReturn(manager);
            when(store.getHgCmdClient()).thenReturn(client);
            this.engine = new TestEngine(store);
            Field raftNode = PartitionEngine.class.getDeclaredField("raftNode");
            raftNode.setAccessible(true);
            raftNode.set(this.engine, this.node);
            when(this.node.listPeers()).thenAnswer(invocation ->
                    new ArrayList<>(this.membership.get().listPeers()));
            when(this.node.listLearners()).thenAnswer(invocation ->
                    new ArrayList<>(this.membership.get().listLearners()));
            when(this.node.getNodeId()).thenReturn(new NodeId("hg_17", JRaftUtils.getPeerId(A)));
            doAnswer(invocation -> {
                Configuration next = ((Configuration) invocation.getArgument(0)).copy();
                this.changed.add(next);
                Status status = this.changeResults.isEmpty() ? Status.OK() : this.changeResults.remove();
                if (status.isOk()) {
                    this.membership.set(next.copy());
                }
                ((Closure) invocation.getArgument(1)).run(status);
                return null;
            }).when(this.node).changePeers(any(Configuration.class), any(Closure.class));
            doAnswer(invocation -> {
                List<PeerId> removing = invocation.getArgument(0);
                Status status = this.removeResults.isEmpty() ? Status.OK() : this.removeResults.remove();
                if (status.isOk()) {
                    Configuration next = this.membership.get().copy();
                    removing.forEach(next::removeLearner);
                    this.membership.set(next);
                }
                ((Closure) invocation.getArgument(1)).run(status);
                return null;
            }).when(this.node).removeLearners(anyList(), any(Closure.class));
            doAnswer(invocation -> {
                this.created.add(invocation.getArgument(0));
                ((Closure) invocation.getArgument(3)).run(Status.OK());
                return CompletableFuture.completedFuture(null);
            }).when(client).createRaftNode(anyString(), anyList(), any(Configuration.class), any(Closure.class));
            doAnswer(invocation -> {
                String peer = invocation.getArgument(0);
                assertFalse("destroy only after the peer is removed from the committed configuration",
                            RaftUtils.getAllEndpoints(this.membership.get()).contains(peer));
                this.destroyed.add(peer);
                ((Closure) invocation.getArgument(2)).run(Status.OK());
                return CompletableFuture.completedFuture(null);
            }).when(client).destroyRaftNode(anyString(), anyList(), any(Closure.class));
        }

        void assertMembership(List<String> voters, List<String> learners) {
            assertEquals(new HashSet<>(voters),
                         new HashSet<>(RaftUtils.getPeerEndpoints(this.membership.get())));
            assertEquals(new HashSet<>(learners),
                         new HashSet<>(RaftUtils.getLearnerEndpoints(this.membership.get())));
        }
    }

    private static class TestEngine extends PartitionEngine {

        private final Queue<Boolean> waitResults = new ArrayDeque<>();
        private final List<List<String>> waitedPeers = new ArrayList<>();
        private int snapshots;

        TestEngine(HgStoreEngine store) {
            super(store, mock(ShardGroup.class));
        }

        @Override
        public Integer getGroupId() {
            return 17;
        }

        @Override
        boolean waitForReplicate(List<String> peers) {
            this.waitedPeers.add(new ArrayList<>(peers));
            return this.waitResults.isEmpty() || this.waitResults.remove();
        }

        @Override
        protected void doSnapshot(Closure done) {
            this.snapshots++;
            done.run(Status.OK());
        }
    }
}
