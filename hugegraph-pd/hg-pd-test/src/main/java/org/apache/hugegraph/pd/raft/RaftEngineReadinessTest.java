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

import java.util.Arrays;

import org.apache.hugegraph.testutil.Whitebox;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.core.State;
import com.alipay.sofa.jraft.entity.LeaderChangeContext;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.error.RaftException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * Covers the raft-aware readiness signal behind {@code GET /v1/ready} and the
 * {@code hg.raft.*} gauges. The signal is served from the volatile copies the state machine
 * callbacks maintain, never from the raft node, so the probe stays prompt while an election
 * holds the node lock; these tests drive the callbacks the way jraft does. The alive peer
 * count is the one value that has to come off the node, so it is refreshed explicitly here,
 * the way the refresher thread does it in a running PD.
 */
public class RaftEngineReadinessTest {

    private static final PeerId LEADER = new PeerId("10.0.0.1", 8610);

    private Node originalRaftNode;
    private RaftStateMachine originalStateMachine;

    private Node mockNode;
    private RaftStateMachine stateMachine;

    @Before
    public void setUp() {
        RaftEngine engine = RaftEngine.getInstance();
        originalRaftNode = engine.getRaftNode();
        originalStateMachine = Whitebox.getInternalState(engine, "stateMachine");

        // A fresh machine so callbacks fired here reach no listeners other tests registered
        mockNode = mock(Node.class);
        stateMachine = new RaftStateMachine();
        Whitebox.setInternalState(engine, "raftNode", mockNode);
        Whitebox.setInternalState(engine, "stateMachine", stateMachine);
        // The count is cached on the singleton, so clear what an earlier test published
        engine.refreshAlivePeerCount();
    }

    @After
    public void tearDown() {
        RaftEngine engine = RaftEngine.getInstance();
        Whitebox.setInternalState(engine, "raftNode", originalRaftNode);
        Whitebox.setInternalState(engine, "stateMachine", originalStateMachine);
    }

    private static LeaderChangeContext ctx() {
        return new LeaderChangeContext(LEADER, 5, Status.OK());
    }

    @Test
    public void testNotReadyBeforeRaftNodeStarts() {
        Whitebox.setInternalState(RaftEngine.getInstance(), "raftNode", null);
        RaftEngine engine = RaftEngine.getInstance();
        engine.refreshAlivePeerCount();

        RaftEngine.RaftStatus status = engine.getRaftStatus();
        Assert.assertFalse(status.isReady());
        Assert.assertFalse(status.isLocalLeader());
        Assert.assertEquals(State.STATE_UNINITIALIZED.name(), status.getState());
        Assert.assertFalse(engine.hasLeader());
        Assert.assertEquals(-1, engine.getAlivePeerCount());
    }

    @Test
    public void testStartedNodeWithoutAnyCallbackIsNotReady() {
        RaftEngine.RaftStatus status = RaftEngine.getInstance().getRaftStatus();
        Assert.assertFalse(status.isReady());
        Assert.assertEquals(State.STATE_UNINITIALIZED.name(), status.getState());
    }

    @Test
    public void testLeaderIsReady() {
        stateMachine.onLeaderStart(5);
        when(mockNode.listAlivePeers()).thenReturn(Arrays.asList(LEADER, new PeerId("b", 1),
                                                                 new PeerId("c", 1)));
        RaftEngine engine = RaftEngine.getInstance();
        engine.refreshAlivePeerCount();

        RaftEngine.RaftStatus status = engine.getRaftStatus();
        Assert.assertTrue(status.isReady());
        Assert.assertTrue(status.isLocalLeader());
        Assert.assertEquals(State.STATE_LEADER.name(), status.getState());
        Assert.assertTrue(engine.hasLeader());
        Assert.assertEquals(3, engine.getAlivePeerCount());
    }

    @Test
    public void testFollowerWithLeaderIsReady() {
        stateMachine.onStartFollowing(ctx());
        RaftEngine engine = RaftEngine.getInstance();
        engine.refreshAlivePeerCount();

        RaftEngine.RaftStatus status = engine.getRaftStatus();
        Assert.assertTrue(status.isReady());
        Assert.assertFalse(status.isLocalLeader());
        Assert.assertEquals(State.STATE_FOLLOWER.name(), status.getState());
        Assert.assertTrue(engine.hasLeader());
        // Only the leader tracks replication, followers cannot count alive peers
        Assert.assertEquals(-1, engine.getAlivePeerCount());
    }

    @Test
    public void testFollowerLosingItsLeaderTurnsNotReady() {
        // The quorum-loss shape from the issue: heartbeats stop, jraft announces the loss
        stateMachine.onStartFollowing(ctx());
        stateMachine.onStopFollowing(ctx());
        RaftEngine engine = RaftEngine.getInstance();

        RaftEngine.RaftStatus status = engine.getRaftStatus();
        Assert.assertFalse(status.isReady());
        Assert.assertEquals(State.STATE_FOLLOWER.name(), status.getState());
        Assert.assertFalse(engine.hasLeader());
    }

    @Test
    public void testLeaderSteppingDownTurnsNotReady() {
        stateMachine.onLeaderStart(5);
        stateMachine.onLeaderStop(Status.OK());
        RaftEngine engine = RaftEngine.getInstance();
        engine.refreshAlivePeerCount();

        RaftEngine.RaftStatus status = engine.getRaftStatus();
        Assert.assertFalse(status.isReady());
        Assert.assertFalse(status.isLocalLeader());
        Assert.assertFalse(engine.hasLeader());
        Assert.assertEquals(-1, engine.getAlivePeerCount());
    }

    @Test
    public void testErrorAndShutdownAreNotReadyEvenAfterLeadership() {
        stateMachine.onLeaderStart(5);
        stateMachine.onError(mock(RaftException.class));
        Assert.assertFalse(RaftEngine.getInstance().getRaftStatus().isReady());
        Assert.assertEquals(State.STATE_ERROR.name(),
                            RaftEngine.getInstance().getRaftStatus().getState());

        stateMachine.onShutdown();
        Assert.assertFalse(RaftEngine.getInstance().getRaftStatus().isReady());
        Assert.assertEquals(State.STATE_SHUTDOWN.name(),
                            RaftEngine.getInstance().getRaftStatus().getState());
    }

    @Test
    public void testProbeNeverTouchesTheRaftNode() {
        // The point of serving from callbacks: an election holds the node lock while jraft
        // reconnects to peers, so the probe and the gauges must answer without the node.
        // getAlivePeerCount() included: it reads what the refresher thread published
        stateMachine.onStartFollowing(ctx());
        stateMachine.onStopFollowing(ctx());
        RaftEngine engine = RaftEngine.getInstance();

        engine.getRaftStatus();
        engine.hasLeader();
        engine.getAlivePeerCount();

        verifyNoInteractions(mockNode);
    }

    @Test
    public void testAlivePeerCountSurvivesLeadershipLossRace() {
        stateMachine.onLeaderStart(5);
        when(mockNode.listAlivePeers()).thenThrow(new IllegalStateException("Not leader"));
        RaftEngine.getInstance().refreshAlivePeerCount();

        Assert.assertEquals(-1, RaftEngine.getInstance().getAlivePeerCount());
    }

    @Test
    public void testShutdownStateSurvivesTheNodeBeingDropped() {
        // shutDown() drops the raft node after the shutdown callback ran, so the probe has
        // to keep reporting what the callback announced rather than falling back to
        // uninitialized, which would read as a PD that has not started yet
        stateMachine.onLeaderStart(5);
        stateMachine.onShutdown();
        Whitebox.setInternalState(RaftEngine.getInstance(), "raftNode", null);
        RaftEngine engine = RaftEngine.getInstance();

        RaftEngine.RaftStatus status = engine.getRaftStatus();
        Assert.assertFalse(status.isReady());
        Assert.assertFalse(status.isLocalLeader());
        Assert.assertEquals(State.STATE_SHUTDOWN.name(), status.getState());
        Assert.assertFalse(engine.hasLeader());
    }

    @Test
    public void testAlivePeerCountStopsAfterALeaderErrorsOrShutsDown() {
        // The terminal callbacks clear the leader term, so the refresher stops reading the
        // count off a node that errored or shut down and the gauge falls back to NaN
        stateMachine.onLeaderStart(5);
        when(mockNode.listAlivePeers()).thenReturn(Arrays.asList(LEADER, new PeerId("b", 1)));
        RaftEngine engine = RaftEngine.getInstance();
        engine.refreshAlivePeerCount();
        Assert.assertEquals(2, engine.getAlivePeerCount());

        stateMachine.onError(mock(RaftException.class));
        engine.refreshAlivePeerCount();
        Assert.assertEquals(-1, engine.getAlivePeerCount());

        stateMachine.onLeaderStart(6);
        engine.refreshAlivePeerCount();
        Assert.assertEquals(2, engine.getAlivePeerCount());

        stateMachine.onShutdown();
        engine.refreshAlivePeerCount();
        Assert.assertEquals(-1, engine.getAlivePeerCount());
        // Twice, once per leader phase: the terminal states never reached the node
        verify(mockNode, times(2)).listAlivePeers();
    }

    @Test
    public void testAlivePeerCountOnALeaderAlsoSkipsTheNode() {
        // Even on a leader, where the count means something, the gauge reads the published
        // value: listAlivePeers takes the node read lock before it checks for leadership, so
        // a scrape that called it would wait out an election that holds the write lock
        stateMachine.onLeaderStart(5);
        RaftEngine engine = RaftEngine.getInstance();

        engine.getAlivePeerCount();

        verifyNoInteractions(mockNode);
    }
}
