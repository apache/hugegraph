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

package org.apache.hugegraph.store.client.grpc;

import java.util.Objects;

import org.apache.hugegraph.store.HgStoreSession;
import org.apache.hugegraph.store.client.HgStoreNode;
import org.apache.hugegraph.store.client.HgStoreNodeManager;

/**
 * created on 2021/10/11
 */
class GrpcStoreNodeImpl implements HgStoreNode {

    private final GrpcStoreSessionClient sessionClient;
    private final GrpcStoreStreamClient streamClient;
    private final HgStoreNodeManager nodeManager;
    private String address;
    private Long nodeId;

    GrpcStoreNodeImpl(HgStoreNodeManager nodeManager, GrpcStoreSessionClient sessionClient,
                      GrpcStoreStreamClient streamClient) {
        this.nodeManager = nodeManager;
        this.sessionClient = sessionClient;
        this.streamClient = streamClient;
    }

    @Override
    public Long getNodeId() {
        return this.nodeId;
    }

    GrpcStoreNodeImpl setNodeId(Long nodeId) {
        this.nodeId = nodeId;
        return this;
    }

    @Override
    public String getAddress() {
        return this.address;
    }

    GrpcStoreNodeImpl setAddress(String address) {
        this.address = address;
        return this;
    }

    @Override
    public HgStoreSession openSession(String graphName) {
        return new GrpcStoreNodeSessionImpl(this, graphName, this.nodeManager, this.sessionClient,
                                            this.streamClient);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        GrpcStoreNodeImpl that = (GrpcStoreNodeImpl) o;
        return Objects.equals(address, that.address) && Objects.equals(nodeId, that.nodeId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(address, nodeId);
    }

    @Override
    public String toString() {
        return "storeNode: {" +
               "address: \"" + address + "\"" +
               ", nodeId: " + nodeId +
               "}";
    }
}
