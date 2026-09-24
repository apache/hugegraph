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

package org.apache.hugegraph.pd.service;

import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.grpc.GraphSpaces;
import org.apache.hugegraph.pd.grpc.Graphs;
import org.apache.hugegraph.pd.grpc.MetaServiceGrpc;
import org.apache.hugegraph.pd.grpc.MetaServiceGrpc.MetaServiceImplBase;
import org.apache.hugegraph.pd.grpc.Metapb.Graph;
import org.apache.hugegraph.pd.grpc.Metapb.GraphSpace;
import org.apache.hugegraph.pd.grpc.Metapb.Partition;
import org.apache.hugegraph.pd.grpc.Metapb.ShardGroup;
import org.apache.hugegraph.pd.grpc.Metapb.Store;
import org.apache.hugegraph.pd.grpc.Partitions;
import org.apache.hugegraph.pd.grpc.Pdpb;
import org.apache.hugegraph.pd.grpc.ShardGroups;
import org.apache.hugegraph.pd.grpc.Stores;
import org.apache.hugegraph.pd.grpc.VoidResponse;
import org.apache.hugegraph.pd.grpc.common.NoArg;
import org.lognet.springboot.grpc.GRpcService;
import org.springframework.beans.factory.annotation.Autowired;

import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@GRpcService
public class MetaServiceGrpcImpl extends MetaServiceImplBase implements ServiceGrpc {

    @Autowired
    private MetadataService metadataService;

    public void getStores(NoArg request, StreamObserver<Stores> observer) {
        if (!isLeader()) {
            redirectToLeader(MetaServiceGrpc.getGetStoresMethod(), request, observer);
            return;
        }
        Stores response;
        Stores.Builder builder = Stores.newBuilder();
        try {
            response = metadataService.getStores();
        } catch (PDException e) {
            if (!isLeader()) {
                redirectToLeader(MetaServiceGrpc.getGetStoresMethod(), request, observer);
                return;
            }
            response = builder.setHeader(getResponseHeader(e)).build();
        }
        observer.onNext(response);
        observer.onCompleted();
    }

    public void getPartitions(NoArg request, StreamObserver<Partitions> observer) {
        if (!isLeader()) {
            redirectToLeader(MetaServiceGrpc.getGetPartitionsMethod(), request, observer);
            return;
        }
        Partitions response;
        Partitions.Builder builder = Partitions.newBuilder();
        try {
            response = metadataService.getPartitions();
        } catch (PDException e) {
            if (!isLeader()) {
                redirectToLeader(MetaServiceGrpc.getGetPartitionsMethod(), request, observer);
                return;
            }
            response = builder.setHeader(getResponseHeader(e)).build();
        }
        observer.onNext(response);
        observer.onCompleted();
    }

    public void getShardGroups(NoArg request, StreamObserver<ShardGroups> observer) {
        if (!isLeader()) {
            redirectToLeader(MetaServiceGrpc.getGetShardGroupsMethod(), request, observer);
            return;
        }
        ShardGroups response;
        ShardGroups.Builder builder = ShardGroups.newBuilder();
        try {
            response = metadataService.getShardGroups();
        } catch (PDException e) {
            if (!isLeader()) {
                redirectToLeader(MetaServiceGrpc.getGetShardGroupsMethod(), request, observer);
                return;
            }
            response = builder.setHeader(getResponseHeader(e)).build();
        }
        observer.onNext(response);
        observer.onCompleted();
    }

    public void getGraphSpaces(NoArg request, StreamObserver<GraphSpaces> observer) {
        if (!isLeader()) {
            redirectToLeader(MetaServiceGrpc.getGetGraphSpacesMethod(), request, observer);
            return;
        }
        GraphSpaces response;
        GraphSpaces.Builder builder = GraphSpaces.newBuilder();
        try {
            response = metadataService.getGraphSpaces();
        } catch (PDException e) {
            if (!isLeader()) {
                redirectToLeader(MetaServiceGrpc.getGetGraphSpacesMethod(), request, observer);
                return;
            }
            response = builder.setHeader(getResponseHeader(e)).build();
        }
        observer.onNext(response);
        observer.onCompleted();
    }

    public void getGraphs(NoArg request, StreamObserver<Graphs> observer) {
        if (!isLeader()) {
            redirectToLeader(MetaServiceGrpc.getGetGraphsMethod(), request, observer);
            return;
        }
        Graphs response;
        Graphs.Builder builder = Graphs.newBuilder();
        try {
            response = metadataService.getGraphs();
        } catch (PDException e) {
            if (!isLeader()) {
                redirectToLeader(MetaServiceGrpc.getGetGraphsMethod(), request, observer);
                return;
            }
            response = builder.setHeader(getResponseHeader(e)).build();
        }
        observer.onNext(response);
        observer.onCompleted();
    }

    public void updateStore(Store request, StreamObserver<VoidResponse> observer) {
        if (!isLeader()) {
            redirectToLeader(MetaServiceGrpc.getUpdateStoreMethod(), request, observer);
            return;
        }
        VoidResponse response;
        VoidResponse.Builder builder = VoidResponse.newBuilder();
        try {
            metadataService.updateStore(request);
            response = builder.build();
        } catch (PDException e) {
            if (!isLeader()) {
                redirectToLeader(MetaServiceGrpc.getUpdateStoreMethod(), request, observer);
                return;
            }
            Pdpb.ResponseHeader header = getResponseHeader(e);
            response = builder.setHeader(header).build();
        }
        observer.onNext(response);
        observer.onCompleted();
    }

    public void updatePartition(Partition request, StreamObserver<VoidResponse> observer) {
        if (!isLeader()) {
            redirectToLeader(MetaServiceGrpc.getUpdatePartitionMethod(), request, observer);
            return;
        }
        VoidResponse response;
        VoidResponse.Builder builder = VoidResponse.newBuilder();
        try {
            metadataService.updatePartition(request);
            response = builder.build();
        } catch (PDException e) {
            if (!isLeader()) {
                redirectToLeader(MetaServiceGrpc.getUpdatePartitionMethod(), request, observer);
                return;
            }
            Pdpb.ResponseHeader header = getResponseHeader(e);
            response = builder.setHeader(header).build();
        }
        observer.onNext(response);
        observer.onCompleted();
    }

    public void updateShardGroup(ShardGroup request, StreamObserver<VoidResponse> observer) {
        if (!isLeader()) {
            redirectToLeader(MetaServiceGrpc.getUpdateShardGroupMethod(), request, observer);
            return;
        }
        VoidResponse response;
        VoidResponse.Builder builder = VoidResponse.newBuilder();
        try {
            metadataService.updateShardGroup(request);
            response = builder.build();
        } catch (PDException e) {
            if (!isLeader()) {
                redirectToLeader(MetaServiceGrpc.getUpdateShardGroupMethod(), request, observer);
                return;
            }
            Pdpb.ResponseHeader header = getResponseHeader(e);
            response = builder.setHeader(header).build();
        }
        observer.onNext(response);
        observer.onCompleted();
    }

    public void updateGraphSpace(GraphSpace request, StreamObserver<VoidResponse> observer) {
        if (!isLeader()) {
            redirectToLeader(MetaServiceGrpc.getUpdateGraphSpaceMethod(), request, observer);
            return;
        }
        VoidResponse response;
        VoidResponse.Builder builder = VoidResponse.newBuilder();
        try {
            metadataService.updateGraphSpace(request);
            response = builder.build();
        } catch (PDException e) {
            if (!isLeader()) {
                redirectToLeader(MetaServiceGrpc.getUpdateGraphSpaceMethod(), request, observer);
                return;
            }
            Pdpb.ResponseHeader header = getResponseHeader(e);
            response = builder.setHeader(header).build();
        }
        observer.onNext(response);
        observer.onCompleted();
    }

    public void updateGraph(Graph request, StreamObserver<VoidResponse> observer) {
        if (!isLeader()) {
            redirectToLeader(MetaServiceGrpc.getUpdateGraphMethod(), request, observer);
            return;
        }
        VoidResponse response;
        VoidResponse.Builder builder = VoidResponse.newBuilder();
        try {
            metadataService.updateGraph(request);
            response = builder.build();
        } catch (PDException e) {
            if (!isLeader()) {
                redirectToLeader(MetaServiceGrpc.getUpdateGraphMethod(), request, observer);
                return;
            }
            Pdpb.ResponseHeader header = getResponseHeader(e);
            response = builder.setHeader(header).build();
        }
        observer.onNext(response);
        observer.onCompleted();
    }
}
