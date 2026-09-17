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

import org.apache.hugegraph.pd.raft.RaftEngine;
import org.apache.hugegraph.pd.raft.RaftStateMachine;
import org.apache.hugegraph.testutil.Whitebox;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.core.State;
import com.alipay.sofa.jraft.entity.LeaderChangeContext;
import com.alipay.sofa.jraft.entity.PeerId;

/**
 * Pins the status a readiness probe actually receives from {@code GET /v1/ready}: 503 while
 * this PD is outside a raft quorum, 200 once it is inside one, with the same body either way.
 * The live REST suite can only reach the 200 path, because the PD it talks to is a single node
 * group that is always its own leader.
 */
public class StoreAPIReadyTest {

    private static final PeerId LEADER = new PeerId("10.0.0.1", 8610);

    private final StoreAPI api = new StoreAPI();

    private Node originalRaftNode;
    private RaftStateMachine originalStateMachine;
    private RaftStateMachine stateMachine;

    @Before
    public void setUp() {
        RaftEngine engine = RaftEngine.getInstance();
        this.originalRaftNode = engine.getRaftNode();
        this.originalStateMachine = Whitebox.getInternalState(engine, "stateMachine");

        // A fresh machine so callbacks fired here reach no listeners of the real one
        this.stateMachine = new RaftStateMachine();
        Whitebox.setInternalState(engine, "stateMachine", this.stateMachine);
    }

    @After
    public void tearDown() {
        RaftEngine engine = RaftEngine.getInstance();
        Whitebox.setInternalState(engine, "raftNode", this.originalRaftNode);
        Whitebox.setInternalState(engine, "stateMachine", this.originalStateMachine);
    }

    @Test
    public void testNotReadyBeforeRaftStartsAnswers503() {
        Whitebox.setInternalState(RaftEngine.getInstance(), "raftNode", null);

        ResponseEntity<Map<String, Object>> response = this.api.checkReady();

        Assert.assertEquals(HttpStatus.SERVICE_UNAVAILABLE, response.getStatusCode());
        Map<String, Object> body = response.getBody();
        Assert.assertNotNull(body);
        Assert.assertEquals(Boolean.FALSE, body.get("ready"));
        Assert.assertEquals(Boolean.FALSE, body.get("isLeader"));
        Assert.assertEquals(State.STATE_UNINITIALIZED.name(), body.get("state"));
    }

    @Test
    public void testLosingTheLeaderAnswers503() {
        Whitebox.setInternalState(RaftEngine.getInstance(), "raftNode", untouchableNode());
        // The quorum-loss shape: heartbeats arrive, then they stop
        this.stateMachine.onStartFollowing(context());
        this.stateMachine.onStopFollowing(context());

        ResponseEntity<Map<String, Object>> response = this.api.checkReady();

        Assert.assertEquals(HttpStatus.SERVICE_UNAVAILABLE, response.getStatusCode());
        Map<String, Object> body = response.getBody();
        Assert.assertNotNull(body);
        Assert.assertEquals(Boolean.FALSE, body.get("ready"));
        Assert.assertEquals(Boolean.FALSE, body.get("isLeader"));
        Assert.assertEquals(State.STATE_FOLLOWER.name(), body.get("state"));
    }

    @Test
    public void testLeaderAnswers200() {
        Whitebox.setInternalState(RaftEngine.getInstance(), "raftNode", untouchableNode());
        this.stateMachine.onLeaderStart(5);

        ResponseEntity<Map<String, Object>> response = this.api.checkReady();

        Assert.assertEquals(HttpStatus.OK, response.getStatusCode());
        Map<String, Object> body = response.getBody();
        Assert.assertNotNull(body);
        Assert.assertEquals(Boolean.TRUE, body.get("ready"));
        Assert.assertEquals(Boolean.TRUE, body.get("isLeader"));
        Assert.assertEquals(State.STATE_LEADER.name(), body.get("state"));
    }

    private static LeaderChangeContext context() {
        return new LeaderChangeContext(LEADER, 5, Status.OK());
    }

    /**
     * A started raft node that fails the test if the endpoint calls it. Readiness is served
     * from the state machine callbacks precisely so that it never waits on the node lock.
     */
    private static Node untouchableNode() {
        return (Node) Proxy.newProxyInstance(
                Node.class.getClassLoader(), new Class<?>[]{Node.class},
                (proxy, method, args) -> {
                    switch (method.getName()) {
                        case "toString":
                            return "untouchable raft node";
                        case "hashCode":
                            return System.identityHashCode(proxy);
                        case "equals":
                            return proxy == args[0];
                        default:
                            throw new AssertionError("the ready path must not touch the raft " +
                                                     "node, it called " + method.getName());
                    }
                });
    }
}
