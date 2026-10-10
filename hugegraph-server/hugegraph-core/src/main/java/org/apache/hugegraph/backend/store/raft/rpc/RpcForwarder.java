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

package org.apache.hugegraph.backend.store.raft.rpc;

import java.util.concurrent.ExecutionException;

import org.apache.hugegraph.exception.BackendException;
import org.apache.hugegraph.backend.store.raft.RaftClosure;
import org.apache.hugegraph.backend.store.raft.RaftContext;
import org.apache.hugegraph.backend.store.raft.RaftStoreClosure;
import org.apache.hugegraph.backend.store.raft.StoreCommand;
import org.apache.hugegraph.backend.store.raft.rpc.RaftRequests.CommonResponse;
import org.apache.hugegraph.backend.store.raft.rpc.RaftRequests.StoreCommandRequest;
import org.apache.hugegraph.backend.store.raft.rpc.RaftRequests.StoreCommandResponse;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.core.NodeImpl;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.rpc.RaftClientService;
import com.alipay.sofa.jraft.rpc.RpcResponseClosure;
import com.alipay.sofa.jraft.util.Endpoint;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.ZeroByteStringHelper;

public class RpcForwarder {

    private static final Logger LOG = Log.logger(RpcForwarder.class);

    private final PeerId nodeId;
    private final RaftClientService rpcClient;

    public RpcForwarder(Node node) {
        this.nodeId = node.getNodeId().getPeerId();
        this.rpcClient = ((NodeImpl) node).getRpcService();
        E.checkNotNull(this.rpcClient, "rpc client");
    }

    public void forwardToLeader(PeerId leaderId, StoreCommand command,
                                RaftStoreClosure future) {
        E.checkNotNull(leaderId, "leader id");
        E.checkState(!leaderId.equals(this.nodeId),
                     "Invalid state: current node is the leader, there is " +
                     "no need to forward the request");
        LOG.debug("The node {} forward request to leader {}",
                  this.nodeId, leaderId);

        StoreCommandRequest.Builder builder = StoreCommandRequest.newBuilder();
        builder.setType(command.type());
        builder.setAction(command.action());
        builder.setData(ZeroByteStringHelper.wrap(command.data()));
        StoreCommandRequest request = builder.build();

        RpcResponseClosure<StoreCommandResponse> responseClosure;
        responseClosure = new RpcResponseClosure<StoreCommandResponse>() {
            @Override
            public void setResponse(StoreCommandResponse response) {
                if (response.getStatus()) {
                    LOG.debug("StoreCommandResponse status ok");
                    // This code forwards the request to the Raft leader and considers the
                    // operation successful
                    // if it's forwarded successfully. It returns a RaftClosure because the calling
                    // logic expects a RaftClosure result. Specifically, if the current instance
                    // is the Raft leader,
                    // it executes the corresponding logic locally and notifies the calling logic
                    // asynchronously
                    // via RaftClosure. Therefore, the result is returned as a RaftClosure here.
                    RaftClosure<Status> supplierFuture = new RaftClosure<>();
                    supplierFuture.complete(Status.OK());
                    future.complete(Status.OK(), () -> supplierFuture);
                } else {
                    LOG.debug("StoreCommandResponse status error");
                    Status status = new Status(RaftError.UNKNOWN,
                                               "fowared request failed");
                    BackendException e = new BackendException(
                            "Current node isn't leader, leader " +
                            "is [%s], failed to forward request " +
                            "to leader: %s",
                            leaderId, response.getMessage());
                    future.failure(status, e);
                }
            }

            @Override
            public void run(Status status) {
                future.run(status);
            }
        };
        this.waitRpc(leaderId.getEndpoint(), request, responseClosure);
    }

    public <T extends Message> RaftClosure<T> forwardToLeader(PeerId leaderId,
                                                              Message request) {
        E.checkNotNull(leaderId, "leader id");
        E.checkState(!leaderId.equals(this.nodeId),
                     "Invalid state: current node is the leader, there is " +
                     "no need to forward the request");
        LOG.debug("The node '{}' forward request to leader '{}'",
                  this.nodeId, leaderId);

        RaftClosure<T> future = new RaftClosure<>();
        RpcResponseClosure<T> responseDone = new RpcResponseClosure<T>() {
            @Override
            public void setResponse(T response) {
                FieldDescriptor fd = response.getDescriptorForType()
                                             .findFieldByName("common");
                Object object = response.getField(fd);
                E.checkState(object instanceof CommonResponse,
                             "The common field must be instance of " +
                             "CommonResponse, actual is '%s'",
                             object != null ? object.getClass() : null);
                CommonResponse commonResponse = (CommonResponse) object;
                if (commonResponse.getStatus()) {
                    future.complete(Status.OK(), () -> response);
                } else {
                    Status status = new Status(RaftError.UNKNOWN,
                                               "fowared request failed");
                    BackendException e = new BackendException(
                            "Current node isn't leader, leader " +
                            "is [%s], failed to forward request " +
                            "to leader: %s",
                            leaderId, commonResponse.getMessage());
                    future.failure(status, e);
                }
            }

            @Override
            public void run(Status status) {
                future.run(status);
            }
        };
        this.waitRpc(leaderId.getEndpoint(), request, responseDone);
        return future;
    }

    private <T extends Message> void waitRpc(Endpoint endpoint, Message request,
                                             RpcResponseClosure<T> done) {
        E.checkNotNull(endpoint, "leader endpoint");
        try {
            this.rpcClient.invokeWithDone(endpoint, request, done,
                                          RaftContext.WAIT_RPC_TIMEOUT)
                          .get();
        } catch (InterruptedException e) {
            throw new BackendException("Invoke rpc request was interrupted, " +
                                       "please try again later", e);
        } catch (ExecutionException e) {
            throw new BackendException("Failed to invoke rpc request", e);
        }
    }
}
