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

import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.hugegraph.pd.client.PDClient;
import org.apache.hugegraph.pd.client.PDConfig;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.store.grpc.state.HgStoreStateGrpc;
import org.apache.hugegraph.store.grpc.state.HgStoreStateGrpc.HgStoreStateBlockingStub;
import org.apache.hugegraph.store.grpc.state.PartitionRequest;
import org.apache.hugegraph.store.grpc.state.PeersResponse;
import org.apache.hugegraph.store.grpc.state.ScanState;
import org.apache.hugegraph.store.grpc.state.SubStateReq;

import io.grpc.ManagedChannel;
import io.grpc.stub.AbstractBlockingStub;
import lombok.extern.slf4j.Slf4j;

/**
 *
 */
@Slf4j
@ThreadSafe
public class GrpcStoreStateClient extends AbstractGrpcClient implements Closeable {

    private static final Map<String, ManagedChannel> channels = new ConcurrentHashMap<>();
    private final PDConfig pdConfig;
    private final PDClient pdClient;

    public GrpcStoreStateClient(PDConfig pdConfig) {
        this.pdConfig = pdConfig;
        pdClient = PDClient.create(this.pdConfig);
    }

    public Set<ScanState> getScanState() throws Exception {
        try {
            List<Metapb.Store> activeStores = pdClient.getActiveStores();
            Set<ScanState> states = activeStores.parallelStream().map(node -> {
                String address = node.getAddress();
                HgStoreStateBlockingStub stub = (HgStoreStateBlockingStub) getBlockingStub(address);
                SubStateReq req = SubStateReq.newBuilder().build();
                return stub.getScanState(req);
            }).collect(Collectors.toSet());
            return states;
        } catch (Exception e) {
            throw e;
        }
    }

    public String getPeers(String address, int partitionId) {
        ManagedChannel channel = channels.get(address);
        try {
            if (channel == null) {
                synchronized (channels) {
                    if ((channel = channels.get(address)) == null) {
                        channel = createChannel(address);
                        channels.put(address, channel);
                    }
                }
            }
            HgStoreStateBlockingStub stub = (HgStoreStateBlockingStub) getBlockingStub(channel);
            PeersResponse peers =
                    stub.getPeers(PartitionRequest.newBuilder().setId(partitionId).build());
            return peers.getPeers();
        } catch (Exception e) {
            throw e;
        }
    }

    @Override
    public AbstractBlockingStub getBlockingStub(ManagedChannel channel) {
        return HgStoreStateGrpc.newBlockingStub(channel);
    }

    @Override
    public synchronized void close() {
        for (ManagedChannel c : channels.values()) {
            try {
                c.shutdown();
            } catch (Exception e) {
                log.warn("Error closing channel", e);
            }
        }
        channels.clear();
    }
}
