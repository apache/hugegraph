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

package org.apache.hugegraph.backend.store.raft;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.hugegraph.exception.BackendException;
import org.apache.hugegraph.backend.store.raft.rpc.RaftRequests.AddPeerRequest;
import org.apache.hugegraph.backend.store.raft.rpc.RaftRequests.ListPeersRequest;
import org.apache.hugegraph.backend.store.raft.rpc.RaftRequests.ListPeersResponse;
import org.apache.hugegraph.backend.store.raft.rpc.RaftRequests.RemovePeerRequest;
import org.apache.hugegraph.backend.store.raft.rpc.RaftRequests.SetLeaderRequest;
import org.apache.hugegraph.backend.store.raft.rpc.RaftRequests.SetLeaderResponse;
import org.apache.hugegraph.backend.store.raft.rpc.RpcForwarder;
import org.apache.hugegraph.util.E;

import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.entity.PeerId;
import com.google.protobuf.Message;

public class RaftGroupManagerImpl implements RaftGroupManager {

    private final String group;
    private final RaftNode raftNode;
    private final RpcForwarder rpcForwarder;

    public RaftGroupManagerImpl(RaftContext context) {
        this.group = context.group();
        this.raftNode = context.node();
        this.rpcForwarder = context.rpcForwarder();
    }

    @Override
    public String group() {
        return this.group;
    }

    @Override
    public List<String> listPeers() {
        if (this.raftNode.selfIsLeader()) {
            List<PeerId> peerIds = this.raftNode.node().listPeers();
            return peerIds.stream().map(PeerId::toString)
                          .collect(Collectors.toList());
        }
        // If current node is not leader, forward request to leader
        ListPeersRequest request = ListPeersRequest.getDefaultInstance();
        try {
            RaftClosure<ListPeersResponse> future;
            future = this.forwardToLeader(request);
            ListPeersResponse response = future.waitFinished();
            return response.getEndpointsList();
        } catch (Throwable e) {
            throw new BackendException("Failed to list peers", e);
        }
    }

    @Override
    public String getLeader() {
        PeerId leaderId = this.raftNode.leaderId();
        E.checkState(leaderId != null,
                     "There is no leader for raft group '%s'", this.group);
        return leaderId.toString();
    }

    @Override
    public String transferLeaderTo(String endpoint) {
        PeerId peerId = PeerId.parsePeer(endpoint);
        Status status = this.raftNode.node().transferLeadershipTo(peerId);
        if (!status.isOk()) {
            throw new BackendException(
                    "Failed to transfer leader to '%s', raft error: %s",
                    endpoint, status.getErrorMsg());
        }
        return peerId.toString();
    }

    @Override
    public String setLeader(String endpoint) {
        PeerId newLeaderId = PeerId.parsePeer(endpoint);
        Node node = this.raftNode.node();
        // If expected endpoint has already been raft leader
        if (node.getLeaderId().equals(newLeaderId)) {
            return newLeaderId.toString();
        }
        if (this.raftNode.selfIsLeader()) {
            // If current node is the leader, transfer directly
            this.transferLeaderTo(endpoint);
        } else {
            // If current node is not leader, forward request to leader
            SetLeaderRequest request = SetLeaderRequest.newBuilder()
                                                       .setEndpoint(endpoint)
                                                       .build();
            try {
                RaftClosure<SetLeaderResponse> future;
                future = this.forwardToLeader(request);
                future.waitFinished();
            } catch (Throwable e) {
                throw new BackendException("Failed to set leader to '%s'",
                                           e, endpoint);
            }
        }
        return newLeaderId.toString();
    }

    @Override
    public String addPeer(String endpoint) {
        PeerId peerId = PeerId.parsePeer(endpoint);
        try {
            RaftClosure<?> future = new RaftClosure<>();
            if (this.raftNode.selfIsLeader()) {
                this.raftNode.node().addPeer(peerId, future);
            } else {
                AddPeerRequest request = AddPeerRequest.newBuilder()
                                                       .setEndpoint(endpoint)
                                                       .build();
                future = this.forwardToLeader(request);
            }
            future.waitFinished();
        } catch (Throwable e) {
            throw new BackendException("Failed to add peer '%s'", e, endpoint);
        }
        return peerId.toString();
    }

    @Override
    public String removePeer(String endpoint) {
        PeerId peerId = PeerId.parsePeer(endpoint);
        try {
            RaftClosure<?> future = new RaftClosure<>();
            if (this.raftNode.selfIsLeader()) {
                this.raftNode.node().removePeer(peerId, future);
            } else {
                RemovePeerRequest request = RemovePeerRequest.newBuilder()
                                                             .setEndpoint(endpoint)
                                                             .build();
                future = this.forwardToLeader(request);
            }
            future.waitFinished();
        } catch (Throwable e) {
            throw new BackendException("Failed to remove peer '%s'", e, endpoint);
        }
        return peerId.toString();
    }

    private <T extends Message> RaftClosure<T> forwardToLeader(Message request) {
        PeerId leaderId = this.raftNode.leaderId();
        return this.rpcForwarder.forwardToLeader(leaderId, request);
    }
}
