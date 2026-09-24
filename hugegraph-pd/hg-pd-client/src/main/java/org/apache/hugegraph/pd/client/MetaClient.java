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

package org.apache.hugegraph.pd.client;

import static org.apache.hugegraph.pd.grpc.MetaServiceGrpc.getGetGraphSpacesMethod;
import static org.apache.hugegraph.pd.grpc.MetaServiceGrpc.getGetGraphsMethod;
import static org.apache.hugegraph.pd.grpc.MetaServiceGrpc.getGetPartitionsMethod;
import static org.apache.hugegraph.pd.grpc.MetaServiceGrpc.getGetShardGroupsMethod;
import static org.apache.hugegraph.pd.grpc.MetaServiceGrpc.getGetStoresMethod;

import java.io.Closeable;

import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.grpc.GraphSpaces;
import org.apache.hugegraph.pd.grpc.Graphs;
import org.apache.hugegraph.pd.grpc.MetaServiceGrpc;
import org.apache.hugegraph.pd.grpc.Metapb.Graph;
import org.apache.hugegraph.pd.grpc.Metapb.GraphSpace;
import org.apache.hugegraph.pd.grpc.Metapb.Partition;
import org.apache.hugegraph.pd.grpc.Metapb.ShardGroup;
import org.apache.hugegraph.pd.grpc.Metapb.Store;
import org.apache.hugegraph.pd.grpc.Partitions;
import org.apache.hugegraph.pd.grpc.ShardGroups;
import org.apache.hugegraph.pd.grpc.Stores;
import org.apache.hugegraph.pd.grpc.VoidResponse;
import org.apache.hugegraph.pd.grpc.common.NoArg;

import io.grpc.stub.AbstractBlockingStub;
import io.grpc.stub.AbstractStub;

public class MetaClient extends AbstractClient implements Closeable {

    public MetaClient(PDConfig config) {
        super(config);
    }

    @Override
    protected AbstractStub createStub() {
        return MetaServiceGrpc.newStub(channel);
    }

    @Override
    protected AbstractBlockingStub createBlockingStub() {
        return MetaServiceGrpc.newBlockingStub(channel);
    }

    public Stores getStores() throws PDException {
        Stores res = blockingUnaryCall(getGetStoresMethod(), NoArg.newBuilder().build());
        handleErrors(res.getHeader());
        return res;
    }

    public Partitions getPartitions() throws PDException {
        Partitions res = blockingUnaryCall(getGetPartitionsMethod(), NoArg.newBuilder().build());
        handleErrors(res.getHeader());
        return res;
    }

    public ShardGroups getShardGroups() throws PDException {
        ShardGroups res = blockingUnaryCall(getGetShardGroupsMethod(), NoArg.newBuilder().build());
        handleErrors(res.getHeader());
        return res;
    }

    public GraphSpaces getGraphSpaces() throws PDException {
        GraphSpaces res = blockingUnaryCall(getGetGraphSpacesMethod(), NoArg.newBuilder().build());
        handleErrors(res.getHeader());
        return res;
    }

    public Graphs getGraphs() throws PDException {
        Graphs res = blockingUnaryCall(getGetGraphsMethod(), NoArg.newBuilder().build());
        handleErrors(res.getHeader());
        return res;
    }

    public void updateStore(Store request) throws PDException {
        VoidResponse res = blockingUnaryCall(MetaServiceGrpc.getUpdateStoreMethod(), request);
        handleErrors(res.getHeader());
    }

    public void updatePartition(Partition request) throws PDException {
        VoidResponse res = blockingUnaryCall(MetaServiceGrpc.getUpdatePartitionMethod(), request);
        handleErrors(res.getHeader());
    }

    public void updateShardGroup(ShardGroup request) throws PDException {
        VoidResponse res = blockingUnaryCall(MetaServiceGrpc.getUpdateShardGroupMethod(), request);
        handleErrors(res.getHeader());
    }

    public void updateGraphSpace(GraphSpace request) throws PDException {
        VoidResponse res = blockingUnaryCall(MetaServiceGrpc.getUpdateGraphSpaceMethod(), request);
        handleErrors(res.getHeader());
    }

    public void updateGraph(Graph request) throws PDException {
        VoidResponse res = blockingUnaryCall(MetaServiceGrpc.getUpdateGraphMethod(), request);
        handleErrors(res.getHeader());
    }

    @Override
    public void close() {
        super.close();
    }
}
