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

import static org.apache.hugegraph.pd.watch.NodeEvent.EventType.NODE_PD_LEADER_CHANGE;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import org.apache.hugegraph.pd.client.impl.PDPulseImpl2;
import org.apache.hugegraph.pd.client.interceptor.Authentication;
import org.apache.hugegraph.pd.client.listener.PDEventListener;
import org.apache.hugegraph.pd.common.KVPair;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.common.PartitionUtils;
import org.apache.hugegraph.pd.grpc.MetaTask;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.pd.grpc.Metapb.Partition;
import org.apache.hugegraph.pd.grpc.Metapb.Shard;
import org.apache.hugegraph.pd.grpc.Metapb.ShardGroup;
import org.apache.hugegraph.pd.grpc.PDGrpc;
import org.apache.hugegraph.pd.grpc.PDGrpc.PDBlockingStub;
import org.apache.hugegraph.pd.grpc.Pdpb;
import org.apache.hugegraph.pd.grpc.Pdpb.CachePartitionResponse;
import org.apache.hugegraph.pd.grpc.Pdpb.CacheResponse;
import org.apache.hugegraph.pd.grpc.Pdpb.ErrorType;
import org.apache.hugegraph.pd.grpc.Pdpb.GetGraphRequest;
import org.apache.hugegraph.pd.grpc.Pdpb.GetPartitionByCodeRequest;
import org.apache.hugegraph.pd.grpc.Pdpb.GetPartitionRequest;
import org.apache.hugegraph.pd.grpc.Pdpb.GetPartitionResponse;
import org.apache.hugegraph.pd.grpc.Pdpb.GraphStatsResponse;
import org.apache.hugegraph.pd.grpc.watch.WatchResponse;
import org.apache.hugegraph.pd.watch.NodeEvent;
import org.apache.hugegraph.pd.watch.PartitionEvent;
import org.apache.hugegraph.pd.watch.PartitionEvent.ChangeType;

import com.google.protobuf.ByteString;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.MethodDescriptor;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.AbstractBlockingStub;
import lombok.extern.slf4j.Slf4j;

import org.apache.hugegraph.pd.watch.NodeEvent.EventType;

/**
 * PD client implementation class
 */
@Slf4j
public class PDClient {

    private static Map<String, ManagedChannel> channels = new ConcurrentHashMap();
    private static ManagedChannel channel = null;
    private final PDConfig config;
    private final Pdpb.RequestHeader header;
    private final ClientCache cache;
    private final StubProxy proxy;
    private final List<PDEventListener> listeners;
    private final PDPulse pulse;
    private final PDConnectionManager connectionManager;
    private PDWatch.Watcher partitionWatcher;
    private PDWatch.Watcher storeWatcher;
    private PDWatch.Watcher graphWatcher;
    private PDWatch.Watcher shardGroupWatcher;
    private PDWatch pdWatch;
    private Authentication auth;

    private PDClient(PDConfig config) {
        this.config = config;
        this.header = Pdpb.RequestHeader.getDefaultInstance();
        this.proxy = new StubProxy(config.getServerHost().split(","));
        this.listeners = new CopyOnWriteArrayList<>();
        this.cache = new ClientCache(this);
        this.auth = new Authentication(config.getUserName(), config.getAuthority());
        this.connectionManager = new PDConnectionManager(config, this::getLeaderIp);
        this.pulse = new PDPulseImpl2(this.connectionManager);
    }

    /**
     * Create a PD client object and initialize the stub
     *
     * @param config
     * @return
     */
    public static PDClient create(PDConfig config) {
        PDClient client = new PDClient(config);
        return client;
    }

    public static void setChannel(ManagedChannel mc) {
        channel = mc;
    }

    /**
     * Return the PD pulse client.
     *
     * @return
     */
    public PDPulse getPulse() {
        return this.pulse;
    }

    /**
     * Force a reconnection to the PD leader, regardless of whether the current connection is
     * alive or not.
     */
    public void forceReconnect() {
        this.connectionManager.forceReconnect();
    }

    private synchronized void newBlockingStub() throws PDException {
        if (proxy.get() != null) {
            return;
        }

        String host = newLeaderStub();
        if (host.isEmpty()) {
            throw new PDException(ErrorType.PD_UNREACHABLE_VALUE,
                                  "PD unreachable, pd.peers=" + config.getServerHost());
        }
        log.info("PDClient enable cache, init PDWatch object");
        startWatch(host);
        this.connectionManager.forceReconnect();
    }

    public void startWatch(String leader) {

        if (pdWatch != null && Objects.equals(pdWatch.getCurrentHost(), leader) &&
            pdWatch.checkChannel()) {
            return;
        }

        log.info("PDWatch client connect host:{}", leader);
        pdWatch = new PDWatchImpl(leader, this.config);
        partitionWatcher = pdWatch.watchPartition(new PDWatch.Listener<>() {
            @Override
            public void onNext(PartitionEvent response) {
                // log.info("PDClient receive partition event {}-{} {}",
                //        response.getGraph(), response.getPartitionId(), response.getChangeType());
                invalidPartitionCache(response.getGraph(), response.getPartitionId());
                if (response.getChangeType() == ChangeType.DEL) {
                    cache.removeAll(response.getGraph());
                }
                listeners.forEach(listener -> listener.onPartitionChanged(response));
            }

            @Override
            public void onError(Throwable throwable) {
                log.error("watchPartition exception {}", throwable.getMessage());
                closeStub(false);
            }
        });

        storeWatcher = pdWatch.watchNode(new PDWatch.Listener<>() {
            @Override
            public void onNext(NodeEvent response) {
                log.info("PDClient receive store event {} {}",
                         response.getEventType(), Long.toHexString(response.getNodeId()));

                if (response.getEventType() == EventType.NODE_PD_LEADER_CHANGE) {
                    // pd raft change
                    var leaderIp = response.getGraph();
                    log.info("watchNode: pd leader changed to {}, current watch:{}",
                             leaderIp, pdWatch.getCurrentHost());
                    closeStub(!Objects.equals(pdWatch.getCurrentHost(), leaderIp));
                    startWatch(leaderIp);
                    PDClient.this.connectionManager.forceReconnect();
                }
                if (response.getEventType() == EventType.NODE_OFFLINE) {
                    invalidStoreCache(response.getNodeId());
                } else {
                    try {
                        getStore(response.getNodeId());
                    } catch (PDException e) {
                        log.error("getStore exception", e);
                    }
                }

                listeners.forEach(listener -> listener.onStoreChanged(response));
            }

            @Override
            public void onError(Throwable throwable) {
                log.error("watchNode exception {}", throwable.getMessage());
                closeStub(false);
            }

        });

        graphWatcher = pdWatch.watchGraph(new PDWatch.Listener<>() {
            @Override
            public void onNext(WatchResponse response) {
                listeners.forEach(listener -> listener.onGraphChanged(response));
            }

            @Override
            public void onError(Throwable throwable) {
                log.warn("graphWatcher exception {}", throwable.getMessage());
            }
        });

        shardGroupWatcher = pdWatch.watchShardGroup(new PDWatch.Listener<>() {
            @Override
            public void onNext(WatchResponse response) {
                var shardResponse = response.getShardGroupResponse();
                // log.info("PDClient receive shard group event: raft {}-{}", shardResponse
                // .getShardGroupId(),
                //        shardResponse.getType());
                if (config.isEnableCache()) {
                    switch (shardResponse.getType()) {
                        case WATCH_CHANGE_TYPE_DEL:
                            cache.deleteShardGroup(shardResponse.getShardGroupId());
                            break;
                        case WATCH_CHANGE_TYPE_ALTER:
                            // fall through to case WATCH_CHANGE_TYPE_ADD
                        case WATCH_CHANGE_TYPE_ADD:
                            cache.updateShardGroup(
                                    response.getShardGroupResponse().getShardGroup());
                            break;
                        default:
                            break;
                    }
                }
                listeners.forEach(listener -> listener.onShardGroupChanged(response));
            }

            @Override
            public void onError(Throwable throwable) {
                log.warn("shardGroupWatcher exception {}", throwable.getMessage());
            }
        });

    }

    private synchronized void closeStub(boolean closeWatcher) {
        // TODO ManagedChannel  Did not close properly
        proxy.set(null);
        cache.reset();

        if (closeWatcher) {
            if (partitionWatcher != null) {
                partitionWatcher.close();
                partitionWatcher = null;
            }
            if (storeWatcher != null) {
                storeWatcher.close();
                storeWatcher = null;
            }
            if (graphWatcher != null) {
                graphWatcher.close();
                graphWatcher = null;
            }

            if (shardGroupWatcher != null) {
                shardGroupWatcher.close();
                shardGroupWatcher = null;
            }

            pdWatch = null;
        }
    }

    private PDBlockingStub getStub() throws PDException {
        if (proxy.get() == null) {
            newBlockingStub();
        }
        return getStub(proxy.get());
    }

    private PDBlockingStub getStub(PDBlockingStub stub) {
        return stub.withDeadlineAfter(config.getGrpcTimeOut(), TimeUnit.MILLISECONDS)
                   .withInterceptors(auth)
                   .withMaxInboundMessageSize(PDConfig.getInboundMessageSize());
    }

    private PDBlockingStub newStub() throws PDException {
        if (proxy.get() == null) {
            newBlockingStub();
        }
        return getStub(PDGrpc.newBlockingStub(proxy.get().getChannel()));
    }

    private String newLeaderStub() {
        String leaderHost = "";
        for (int i = 0; i < proxy.getHostCount(); i++) {
            String host = proxy.nextHost();
            ManagedChannel channel = getChannel(host);
            PDBlockingStub stub = getStub(PDGrpc.newBlockingStub(channel));
            try {
                var leaderIp = getLeaderIp(stub);
                if (!leaderIp.equalsIgnoreCase(host)) {
                    leaderHost = leaderIp;
                    proxy.set(getStub(PDGrpc.newBlockingStub(channel)));
                } else {
                    proxy.set(stub);
                    leaderHost = host;
                }
                proxy.setLeader(leaderIp);

                log.info("PDClient connect to host = {} success", leaderHost);
                break;
            } catch (Exception e) {
                log.error("PDClient connect to {} exception {}, {}", host, e.getMessage(),
                          e.getCause() != null ? e.getCause().getMessage() : "");
            }
        }
        return leaderHost;
    }

    private ManagedChannel getChannel(String host) {
        ManagedChannel c;
        if ((c = channels.get(host)) == null || c.isTerminated()) {
            synchronized (channels) {
                if ((c = channels.get(host)) == null || c.isTerminated()) {
                    channel = ManagedChannelBuilder.forTarget(host)
                                                   .maxInboundMessageSize(
                                                           PDConfig.getInboundMessageSize())
                                                   .usePlaintext().build();
                    c = channel;
                    channels.put(host, channel);
                }
            }
        }
        channel = c;
        return channel;
    }

    public String getLeaderIp() {
        try {
            return getLeaderIp(getStub());
        } catch (PDException e) {
            throw new RuntimeException(e);
        }
    }

    private String getLeaderIp(PDBlockingStub stub) {
        if (stub == null) {
            try {
                getStub();
                return proxy.getLeader();
            } catch (PDException e) {
                throw new RuntimeException(e);
            }
        }

        Pdpb.GetMembersRequest request = Pdpb.GetMembersRequest.newBuilder()
                                                               .setHeader(header)
                                                               .build();
        Metapb.Member leader = stub.getMembers(request).getLeader();
        return leader.getGrpcUrl();
    }

    /**
     * Store registration, the store ID will be returned, and the initial registration will
     * return a new ID
     *
     * @param store
     * @return
     */
    public long registerStore(Metapb.Store store) throws PDException {
        Pdpb.RegisterStoreRequest request = Pdpb.RegisterStoreRequest.newBuilder()
                                                                     .setHeader(header)
                                                                     .setStore(store).build();

        Pdpb.RegisterStoreResponse response =
                blockingUnaryCall(PDGrpc.getRegisterStoreMethod(), request);
        handleResponseError(response.getHeader());
        return response.getStoreId();
    }

    /**
     * Returns the Store object based on the store ID
     *
     * @param storeId
     * @return
     * @throws PDException
     */
    public Metapb.Store getStore(long storeId) throws PDException {
        Metapb.Store store = cache.getStoreById(storeId);
        if (store == null) {
            Pdpb.GetStoreRequest request = Pdpb.GetStoreRequest.newBuilder()
                                                               .setHeader(header)
                                                               .setStoreId(storeId).build();
            Pdpb.GetStoreResponse response = getStub().getStore(request);
            handleResponseError(response.getHeader());
            store = response.getStore();
            if (config.isEnableCache()) {
                cache.addStore(storeId, store);
            }
        }
        return store;
    }

    /**
     * Update the store information, including online and offline
     *
     * @param store
     * @return
     */
    public Metapb.Store updateStore(Metapb.Store store) throws PDException {
        Pdpb.SetStoreRequest request = Pdpb.SetStoreRequest.newBuilder()
                                                           .setHeader(header)
                                                           .setStore(store).build();

        Pdpb.SetStoreResponse response = getStub().setStore(request);
        handleResponseError(response.getHeader());
        store = response.getStore();
        if (config.isEnableCache()) {
            cache.addStore(store.getId(), store);
        }
        return store;
    }

    /**
     * Return to the active store
     *
     * @param graphName
     * @return
     */
    public List<Metapb.Store> getActiveStores(String graphName) throws PDException {
        Set<Metapb.Store> stores = new HashSet<>();
        KVPair<Partition, Shard> ptShard = this.getPartitionByCode(graphName, 0);
        while (ptShard != null) {
            stores.add(this.getStore(ptShard.getValue().getStoreId()));
            if (ptShard.getKey().getEndKey() < PartitionUtils.MAX_VALUE) {
                ptShard = this.getPartitionByCode(graphName, ptShard.getKey().getEndKey());
            } else {
                ptShard = null;
            }
        }
        return new ArrayList<>(stores);
    }

    public List<Metapb.Store> getActiveStores() throws PDException {
        Pdpb.GetAllStoresRequest request = Pdpb.GetAllStoresRequest.newBuilder()
                                                                   .setHeader(header)
                                                                   .setGraphName("")
                                                                   .setExcludeOfflineStores(true)
                                                                   .build();
        Pdpb.GetAllStoresResponse response = getStub().getAllStores(request);
        handleResponseError(response.getHeader());
        return response.getStoresList();

    }

    /**
     * Return to the active store
     *
     * @param graphName
     * @return
     */
    public List<Metapb.Store> getAllStores(String graphName) throws PDException {
        Pdpb.GetAllStoresRequest request = Pdpb.GetAllStoresRequest.newBuilder()
                                                                   .setHeader(header)
                                                                   .setGraphName(graphName)
                                                                   .setExcludeOfflineStores(false)
                                                                   .build();
        Pdpb.GetAllStoresResponse response = getStub().getAllStores(request);
        handleResponseError(response.getHeader());
        return response.getStoresList();

    }

    /**
     * Store heartbeat, call regularly, stay online
     *
     * @param stats
     * @throws PDException
     */
    public Metapb.ClusterStats storeHeartbeat(Metapb.StoreStats stats) throws PDException {
        Pdpb.StoreHeartbeatRequest request = Pdpb.StoreHeartbeatRequest.newBuilder()
                                                                       .setHeader(header)
                                                                       .setStats(stats).build();
        Pdpb.StoreHeartbeatResponse response = getStub().storeHeartbeat(request);
        handleResponseError(response.getHeader());
        return response.getClusterStats();
    }

    private KVPair<Metapb.Partition, Metapb.Shard> getKvPair(String graphName, byte[] key,
                                                             KVPair<Metapb.Partition,
                                                                     Metapb.Shard> partShard) throws
                                                                                              PDException {
        if (partShard == null) {
            GetPartitionRequest request = GetPartitionRequest.newBuilder()
                                                             .setHeader(header)
                                                             .setGraphName(graphName)
                                                             .setKey(ByteString.copyFrom(key))
                                                             .build();
            GetPartitionResponse response =
                    blockingUnaryCall(PDGrpc.getGetPartitionMethod(), request);
            handleResponseError(response.getHeader());
            partShard = new KVPair<>(response.getPartition(), response.getLeader());
            cache.update(graphName, partShard.getKey().getId(), partShard.getKey());
        }
        return partShard;
    }

    /**
     * Query the partition to which the key belongs
     *
     * @param graphName
     * @param key
     * @return
     * @throws PDException
     */
    public KVPair<Partition, Shard> getPartition(String graphName, byte[] key) throws PDException {
        KVPair<Partition, Shard> partShard = cache.getPartitionByKey(graphName, key);
        partShard = getKvPair(graphName, key, partShard);
        return partShard;
    }

    public KVPair<Metapb.Partition, Metapb.Shard> getPartition(String graphName, byte[] key,
                                                               int code) throws
                                                                         PDException {
        KVPair<Metapb.Partition, Metapb.Shard> partShard =
                cache.getPartitionByCode(graphName, code);
        partShard = getKvPair(graphName, key, partShard);
        return partShard;
    }

    /**
     * Query the partition information based on the hashcode
     *
     * @param graphName
     * @param hashCode
     * @return
     * @throws PDException
     */
    public KVPair<Metapb.Partition, Metapb.Shard> getPartitionByCode(String graphName,
                                                                     long hashCode)
            throws PDException {
        KVPair<Metapb.Partition, Metapb.Shard> partShard =
                cache.getPartitionByCode(graphName, hashCode);
        if (partShard == null) {
            GetPartitionByCodeRequest request = GetPartitionByCodeRequest.newBuilder()
                                                                         .setHeader(header)
                                                                         .setGraphName(graphName)
                                                                         .setCode(hashCode).build();
            GetPartitionResponse response =
                    blockingUnaryCall(PDGrpc.getGetPartitionByCodeMethod(), request);
            handleResponseError(response.getHeader());
            partShard = new KVPair<>(response.getPartition(), response.getLeader());
            cache.update(graphName, partShard.getKey().getId(), partShard.getKey());
            cache.updateShardGroup(getShardGroup(partShard.getKey().getId()));
        }

        if (partShard.getValue() == null) {
            ShardGroup shardGroup = getShardGroup(partShard.getKey().getId());
            if (shardGroup != null) {
                for (var shard : shardGroup.getShardsList()) {
                    if (shard.getRole() == Metapb.ShardRole.Leader) {
                        partShard.setValue(shard);
                    }
                }
            } else {
                log.error("getPartitionByCode: get shard group failed, {}",
                          partShard.getKey().getId());
            }
        }
        return partShard;
    }

    /**
     * Obtain the hash value of the key
     */
    public int keyToCode(String graphName, byte[] key) {
        return PartitionUtils.calcHashcode(key);
    }

    /**
     * Returns partition information based on the partition ID and RPC request
     *
     * @param graphName
     * @param partId
     * @return
     * @throws PDException
     */
    public KVPair<Metapb.Partition, Metapb.Shard> getPartitionById(String graphName,
                                                                   int partId) throws PDException {
        KVPair<Metapb.Partition, Metapb.Shard> partShard =
                cache.getPartitionById(graphName, partId);
        if (partShard == null) {
            Pdpb.GetPartitionByIDRequest request = Pdpb.GetPartitionByIDRequest.newBuilder()
                                                                               .setHeader(header)
                                                                               .setGraphName(
                                                                                       graphName)
                                                                               .setPartitionId(
                                                                                       partId)
                                                                               .build();
            GetPartitionResponse response =
                    blockingUnaryCall(PDGrpc.getGetPartitionByIDMethod(), request);
            handleResponseError(response.getHeader());
            partShard = new KVPair<>(response.getPartition(), response.getLeader());
            if (config.isEnableCache()) {
                cache.update(graphName, partShard.getKey().getId(), partShard.getKey());
                cache.updateShardGroup(getShardGroup(partShard.getKey().getId()));
            }
        }
        if (partShard.getValue() == null) {
            var shardGroup = getShardGroup(partShard.getKey().getId());
            if (shardGroup != null) {
                for (var shard : shardGroup.getShardsList()) {
                    if (shard.getRole() == Metapb.ShardRole.Leader) {
                        partShard.setValue(shard);
                    }
                }
            } else {
                log.error("getPartitionById: get shard group failed, {}",
                          partShard.getKey().getId());
            }
        }
        return partShard;
    }

    public ShardGroup getShardGroup(int partId) throws PDException {
        ShardGroup group = cache.getShardGroup(partId);
        if (group == null) {
            group = getShardGroupDirect(partId);
            if (config.isEnableCache()) {
                cache.updateShardGroup(group);
            }
        }
        return group;
    }

    public ShardGroup getShardGroupDirect(int partId) throws PDException {
        Pdpb.GetShardGroupRequest request = Pdpb.GetShardGroupRequest.newBuilder()
                                                                     .setHeader(header)
                                                                     .setGroupId(partId)
                                                                     .build();
        Pdpb.GetShardGroupResponse response =
                blockingUnaryCall(PDGrpc.getGetShardGroupMethod(), request);
        handleResponseError(response.getHeader());
        return response.getShardGroup();
    }

    public void updateShardGroup(ShardGroup shardGroup) throws PDException {
        Pdpb.UpdateShardGroupRequest request = Pdpb.UpdateShardGroupRequest.newBuilder()
                                                                           .setHeader(header)
                                                                           .setShardGroup(
                                                                                   shardGroup)
                                                                           .build();
        Pdpb.UpdateShardGroupResponse response =
                blockingUnaryCall(PDGrpc.getUpdateShardGroupMethod(), request);
        handleResponseError(response.getHeader());

        if (config.isEnableCache()) {
            cache.updateShardGroup(shardGroup);
        }
    }

    /**
     * Returns information about all partitions spanned by the start and end keys
     *
     * @param graphName
     * @param startKey
     * @param endKey
     * @return
     * @throws PDException
     */
    public List<KVPair<Metapb.Partition, Metapb.Shard>> scanPartitions(String graphName,
                                                                       byte[] startKey,
                                                                       byte[] endKey) throws
                                                                                      PDException {
        List<KVPair<Metapb.Partition, Metapb.Shard>> partitions = new ArrayList<>();
        KVPair<Metapb.Partition, Metapb.Shard> startPartShard = getPartition(graphName, startKey);
        KVPair<Metapb.Partition, Metapb.Shard> endPartShard = getPartition(graphName, endKey);
        if (startPartShard == null || endPartShard == null) {
            return null;
        }

        partitions.add(startPartShard);
        while (startPartShard.getKey().getEndKey() < endPartShard.getKey().getEndKey()
               && startPartShard.getKey().getEndKey() < PartitionUtils.MAX_VALUE) {
            startPartShard = getPartitionByCode(graphName, startPartShard.getKey().getEndKey());
            partitions.add(startPartShard);
        }
        return partitions;
    }

    /**
     * Query partition information based on conditions
     *
     * @return
     * @throws PDException
     */
    public List<Metapb.Partition> getPartitionsByStore(long storeId) throws PDException {

        Metapb.PartitionQuery query = Metapb.PartitionQuery.newBuilder()
                                                           .setStoreId(storeId)
                                                           .build();
        Pdpb.QueryPartitionsRequest request = Pdpb.QueryPartitionsRequest.newBuilder()
                                                                         .setQuery(query).build();
        Pdpb.QueryPartitionsResponse response =
                blockingUnaryCall(PDGrpc.getQueryPartitionsMethod(), request);

        handleResponseError(response.getHeader());
        return response.getPartitionsList();
    }

    public List<Metapb.Partition> queryPartitions(long storeId, int partitionId) throws
                                                                                 PDException {

        Metapb.PartitionQuery query = Metapb.PartitionQuery.newBuilder()
                                                           .setStoreId(storeId)
                                                           .setPartitionId(partitionId)
                                                           .build();
        Pdpb.QueryPartitionsRequest request = Pdpb.QueryPartitionsRequest.newBuilder()
                                                                         .setQuery(query).build();
        Pdpb.QueryPartitionsResponse response =
                blockingUnaryCall(PDGrpc.getQueryPartitionsMethod(), request);

        handleResponseError(response.getHeader());
        return response.getPartitionsList();
    }

    public List<Metapb.Partition> getPartitions(long storeId, String graphName) throws PDException {

        Metapb.PartitionQuery query = Metapb.PartitionQuery.newBuilder()
                                                           .setStoreId(storeId)
                                                           .setGraphName(graphName).build();
        Pdpb.QueryPartitionsRequest request = Pdpb.QueryPartitionsRequest.newBuilder()
                                                                         .setQuery(query).build();
        Pdpb.QueryPartitionsResponse response =
                blockingUnaryCall(PDGrpc.getQueryPartitionsMethod(), request);

        handleResponseError(response.getHeader());
        return response.getPartitionsList();

    }

    public Metapb.Graph setGraph(Metapb.Graph graph) throws PDException {
        Pdpb.SetGraphRequest request = Pdpb.SetGraphRequest.newBuilder()
                                                           .setGraph(graph)
                                                           .build();
        Pdpb.SetGraphResponse response =
                blockingUnaryCall(PDGrpc.getSetGraphMethod(), request);

        handleResponseError(response.getHeader());
        return response.getGraph();
    }

    public Metapb.Graph getGraph(String graphName) throws PDException {
        GetGraphRequest request = GetGraphRequest.newBuilder()
                                                 .setGraphName(graphName)
                                                 .build();
        Pdpb.GetGraphResponse response =
                blockingUnaryCall(PDGrpc.getGetGraphMethod(), request);

        handleResponseError(response.getHeader());
        return response.getGraph();
    }

    public Metapb.Graph getGraphWithOutException(String graphName) throws
                                                                   PDException {
        GetGraphRequest request = GetGraphRequest.newBuilder()
                                                 .setGraphName(
                                                         graphName)
                                                 .build();
        Pdpb.GetGraphResponse response = blockingUnaryCall(
                PDGrpc.getGetGraphMethod(), request);
        return response.getGraph();
    }

    public Metapb.Graph delGraph(String graphName) throws PDException {
        Pdpb.DelGraphRequest request = Pdpb.DelGraphRequest.newBuilder()
                                                           .setGraphName(graphName)
                                                           .build();
        Pdpb.DelGraphResponse response =
                blockingUnaryCall(PDGrpc.getDelGraphMethod(), request);

        handleResponseError(response.getHeader());
        return response.getGraph();
    }

    public List<Metapb.Partition> updatePartition(List<Metapb.Partition> partitions) throws
                                                                                     PDException {

        Pdpb.UpdatePartitionRequest request = Pdpb.UpdatePartitionRequest.newBuilder()
                                                                         .addAllPartition(
                                                                                 partitions)
                                                                         .build();
        Pdpb.UpdatePartitionResponse response =
                blockingUnaryCall(PDGrpc.getUpdatePartitionMethod(), request);
        handleResponseError(response.getHeader());
        invalidPartitionCache();

        return response.getPartitionList();
    }

    public Metapb.Partition delPartition(String graphName, int partitionId) throws PDException {

        Pdpb.DelPartitionRequest request = Pdpb.DelPartitionRequest.newBuilder()
                                                                   .setGraphName(graphName)
                                                                   .setPartitionId(partitionId)
                                                                   .build();
        Pdpb.DelPartitionResponse response =
                blockingUnaryCall(PDGrpc.getDelPartitionMethod(), request);

        handleResponseError(response.getHeader());
        invalidPartitionCache(graphName, partitionId);
        return response.getPartition();
    }

    /**
     * Delete the partitioned cache
     */
    public void invalidPartitionCache(String graphName, int partitionId) {
        if (null != cache.getPartitionById(graphName, partitionId)) {
            cache.removePartition(graphName, partitionId);
        }
    }

    /**
     * Delete the partitioned cache
     */
    public void invalidPartitionCache() {
        cache.removePartitions();
    }

    /**
     * Delete the partitioned cache
     */
    public void invalidStoreCache(long storeId) {
        cache.removeStore(storeId);
    }

    /**
     * Update the cache
     */
    public void updatePartitionLeader(String graphName, int partId, long leaderStoreId) {
        KVPair<Metapb.Partition, Metapb.Shard> partShard = null;
        try {
            partShard = this.getPartitionById(graphName, partId);

            if (partShard != null && partShard.getValue().getStoreId() != leaderStoreId) {
                var shardGroup = this.getShardGroup(partId);
                Metapb.Shard shard = null;
                List<Metapb.Shard> shards = new ArrayList<>();

                for (Metapb.Shard s : shardGroup.getShardsList()) {
                    if (s.getStoreId() == leaderStoreId) {
                        shard = s;
                        shards.add(Metapb.Shard.newBuilder(s)
                                               .setStoreId(s.getStoreId())
                                               .setRole(Metapb.ShardRole.Leader).build());
                    } else {
                        shards.add(Metapb.Shard.newBuilder(s)
                                               .setStoreId(s.getStoreId())
                                               .setRole(Metapb.ShardRole.Follower).build());
                    }
                }

                if (config.isEnableCache()) {
                    if (shard == null) {
                        cache.removePartition(graphName, partId);
                    } else {
                        cache.updateLeader(partId, shard);
                    }
                }
            }
        } catch (PDException e) {
            log.error("getPartitionException: {}", e.getMessage());
        }
    }

    /**
     * Update the cache
     *
     * @param partition
     */
    public void updatePartitionCache(Metapb.Partition partition, Metapb.Shard leader) {
        if (config.isEnableCache()) {
            cache.update(partition.getGraphName(), partition.getId(), partition);
            cache.updateLeader(partition.getId(), leader);
        }
    }

    public Pdpb.GetIdResponse getIdByKey(String key, int delta) throws PDException {
        Pdpb.GetIdRequest request = Pdpb.GetIdRequest.newBuilder()
                                                     .setHeader(header)
                                                     .setKey(key)
                                                     .setDelta(delta)
                                                     .build();
        Pdpb.GetIdResponse response = blockingUnaryCall(PDGrpc.getGetIdMethod(), request);
        handleResponseError(response.getHeader());
        return response;
    }

    public Pdpb.ResetIdResponse resetIdByKey(String key) throws PDException {
        Pdpb.ResetIdRequest request = Pdpb.ResetIdRequest.newBuilder()
                                                         .setHeader(header)
                                                         .setKey(key)
                                                         .build();
        Pdpb.ResetIdResponse response = blockingUnaryCall(PDGrpc.getResetIdMethod(), request);
        handleResponseError(response.getHeader());
        return response;
    }

    public Metapb.Member getLeader() throws PDException {
        Pdpb.GetMembersRequest request = Pdpb.GetMembersRequest.newBuilder()
                                                               .setHeader(header)
                                                               .build();
        Pdpb.GetMembersResponse response = blockingUnaryCall(PDGrpc.getGetMembersMethod(), request);
        handleResponseError(response.getHeader());
        return response.getLeader();
    }

    public Pdpb.GetMembersResponse getMembers() throws PDException {
        Pdpb.GetMembersRequest request = Pdpb.GetMembersRequest.newBuilder()
                                                               .setHeader(header)
                                                               .build();
        Pdpb.GetMembersResponse response = blockingUnaryCall(PDGrpc.getGetMembersMethod(), request);
        handleResponseError(response.getHeader());
        return response;
    }

    public Metapb.ClusterStats getClusterStats() throws PDException {
        Pdpb.GetClusterStatsRequest request = Pdpb.GetClusterStatsRequest.newBuilder()
                                                                         .setHeader(header)
                                                                         .build();
        Pdpb.GetClusterStatsResponse response =
                blockingUnaryCall(PDGrpc.getGetClusterStatsMethod(), request);
        handleResponseError(response.getHeader());
        return response.getCluster();
    }

    private <ReqT, RespT, StubT extends AbstractBlockingStub<StubT>> RespT
    blockingUnaryCall(MethodDescriptor<ReqT, RespT> method, ReqT req) throws PDException {
        return blockingUnaryCall(method, req, 1);
    }

    private <ReqT, RespT, StubT extends AbstractBlockingStub<StubT>> RespT
    blockingUnaryCall(MethodDescriptor<ReqT, RespT> method, ReqT req, int retry) throws
                                                                                 PDException {
        io.grpc.stub.AbstractBlockingStub<StubT> stub = (AbstractBlockingStub<StubT>) getStub();
        try {
            RespT resp = io.grpc.stub.ClientCalls.blockingUnaryCall(stub.getChannel(), method,
                                                                    stub.getCallOptions(), req);
            return resp;
        } catch (Exception e) {
            log.error(method.getFullMethodName() + " exception, {}", e.getMessage());
            if (e instanceof StatusRuntimeException) {
                StatusRuntimeException se = (StatusRuntimeException) e;
                if (retry < proxy.getHostCount()) {
                    closeStub(true);
                    return blockingUnaryCall(method, req, ++retry);
                }
            }
        }
        return null;
    }

    private void handleResponseError(Pdpb.ResponseHeader header) throws
                                                                 PDException {
        var errorType = header.getError().getType();
        if (header.hasError() && errorType != Pdpb.ErrorType.OK) {

            throw new PDException(header.getError().getTypeValue(),
                                  String.format(
                                          "PD request error, error code = %d, msg = %s",
                                          header.getError().getTypeValue(),
                                          header.getError().getMessage()));
        }
    }

    public void addEventListener(PDEventListener listener) {
        listeners.add(listener);
    }

    public PDWatch getWatchClient() {
        return new PDWatchImpl(proxy.getHost(), config);
    }

    /**
     * Returns the store status information
     */
    public List<Metapb.Store> getStoreStatus(boolean offlineExcluded) throws PDException {
        Pdpb.GetAllStoresRequest request = Pdpb.GetAllStoresRequest.newBuilder()
                                                                   .setHeader(header)
                                                                   .setExcludeOfflineStores(
                                                                           offlineExcluded)
                                                                   .build();
        Pdpb.GetAllStoresResponse response = getStub().getStoreStatus(request);
        handleResponseError(response.getHeader());
        List<Metapb.Store> stores = response.getStoresList();
        return stores;
    }

    public void setGraphSpace(String graphSpaceName, long storageLimit) throws PDException {
        Metapb.GraphSpace graphSpace = Metapb.GraphSpace.newBuilder().setName(graphSpaceName)
                                                        .setStorageLimit(storageLimit)
                                                        .setTimestamp(System.currentTimeMillis())
                                                        .build();
        Pdpb.SetGraphSpaceRequest request = Pdpb.SetGraphSpaceRequest.newBuilder()
                                                                     .setHeader(header)
                                                                     .setGraphSpace(graphSpace)
                                                                     .build();
        Pdpb.SetGraphSpaceResponse response = getStub().setGraphSpace(request);
        handleResponseError(response.getHeader());
    }

    public List<Metapb.GraphSpace> getGraphSpace(String graphSpaceName) throws
                                                                        PDException {
        Pdpb.GetGraphSpaceRequest.Builder builder = Pdpb.GetGraphSpaceRequest.newBuilder();
        Pdpb.GetGraphSpaceRequest request;
        builder.setHeader(header);
        if (graphSpaceName != null && graphSpaceName.length() > 0) {
            builder.setGraphSpaceName(graphSpaceName);
        }
        request = builder.build();
        Pdpb.GetGraphSpaceResponse response = getStub().getGraphSpace(request);
        List<Metapb.GraphSpace> graphSpaceList = response.getGraphSpaceList();
        handleResponseError(response.getHeader());
        return graphSpaceList;
    }

    public void setPDConfig(int partitionCount, String peerList, int shardCount,
                            long version) throws PDException {
        Metapb.PDConfig pdConfig = Metapb.PDConfig.newBuilder().setPartitionCount(partitionCount)
                                                  .setPeersList(peerList).setShardCount(shardCount)
                                                  .setVersion(version)
                                                  .setTimestamp(System.currentTimeMillis())
                                                  .build();
        Pdpb.SetPDConfigRequest request = Pdpb.SetPDConfigRequest.newBuilder()
                                                                 .setHeader(header)
                                                                 .setPdConfig(pdConfig)
                                                                 .build();
        Pdpb.SetPDConfigResponse response = getStub().setPDConfig(request);
        handleResponseError(response.getHeader());
    }

    public Metapb.PDConfig getPDConfig() throws PDException {
        Pdpb.GetPDConfigRequest request = Pdpb.GetPDConfigRequest.newBuilder()
                                                                 .setHeader(header)
                                                                 .build();
        Pdpb.GetPDConfigResponse response = getStub().getPDConfig(request);
        handleResponseError(response.getHeader());
        return response.getPdConfig();
    }

    public void setPDConfig(Metapb.PDConfig pdConfig) throws PDException {
        Pdpb.SetPDConfigRequest request = Pdpb.SetPDConfigRequest.newBuilder()
                                                                 .setHeader(header)
                                                                 .setPdConfig(pdConfig)
                                                                 .build();
        Pdpb.SetPDConfigResponse response = getStub().setPDConfig(request);
        handleResponseError(response.getHeader());
    }

    public Metapb.PDConfig getPDConfig(long version) throws PDException {
        Pdpb.GetPDConfigRequest request = Pdpb.GetPDConfigRequest.newBuilder().setHeader(
                header).setVersion(version).build();
        Pdpb.GetPDConfigResponse response = getStub().getPDConfig(request);
        handleResponseError(response.getHeader());
        return response.getPdConfig();
    }

    public void changePeerList(String peerList) throws PDException {
        Pdpb.ChangePeerListRequest request = Pdpb.ChangePeerListRequest.newBuilder()
                                                                       .setPeerList(peerList)
                                                                       .setHeader(header).build();
        Pdpb.getChangePeerListResponse response =
                blockingUnaryCall(PDGrpc.getChangePeerListMethod(), request);
        handleResponseError(response.getHeader());
    }

    /**
     * Working mode
     * Auto: If the number of partitions on each store reaches the maximum value, you need to
     * specify the store group id. The store group id is 0, which is the default partition
     * splitData(ClusterOp.OperationMode mode, int storeGroupId, List<ClusterOp.SplitDataParam>
     * params)
     * mode = Auto storeGroupId, params
     *
     * @throws PDException
     */
    public void splitData() throws PDException {
        Pdpb.SplitDataRequest request = Pdpb.SplitDataRequest.newBuilder()
                                                             .setHeader(header)
                                                             .setMode(Pdpb.OperationMode.Auto)
                                                             .build();
        Pdpb.SplitDataResponse response = getStub().splitData(request);
        handleResponseError(response.getHeader());
    }

    /**
     * Working mode
     * Auto: If the number of partitions on each store reaches the maximum value, you need to
     * specify the store group id. The store group id is 0, which is the default partition
     * Expert: Expert Mode, Specifier is required splitParams, limit SplitDataParam in the same
     * store group
     *
     * @param mode
     * @param params
     * @throws PDException
     */
    public void splitData(Pdpb.OperationMode mode, List<Pdpb.SplitDataParam> params) throws
                                                                                     PDException {
        Pdpb.SplitDataRequest request = Pdpb.SplitDataRequest.newBuilder()
                                                             .setHeader(header)
                                                             .setMode(mode)
                                                             .addAllParam(params).build();
        Pdpb.SplitDataResponse response = getStub().splitData(request);
        handleResponseError(response.getHeader());
    }

    public void splitGraphData(String graphName, int toCount) throws PDException {
        Pdpb.SplitGraphDataRequest request = Pdpb.SplitGraphDataRequest.newBuilder()
                                                                       .setHeader(header)
                                                                       .setGraphName(graphName)
                                                                       .setToCount(toCount)
                                                                       .build();
        Pdpb.SplitDataResponse response = getStub().splitGraphData(request);
        handleResponseError(response.getHeader());
    }

    /**
     * To automatically transfer to the same number of partitions on each Store, it is
     * recommended to use balancePartition(int storeGroupId) to specify the storeGroupId
     *
     * @throws PDException
     */
    public void balancePartition() throws PDException {
        Pdpb.MovePartitionRequest request = Pdpb.MovePartitionRequest.newBuilder()
                                                                     .setHeader(header)
                                                                     .setMode(
                                                                             Pdpb.OperationMode.Auto)
                                                                     .build();
        Pdpb.MovePartitionResponse response = getStub().movePartition(request);
        handleResponseError(response.getHeader());
    }

    /**
     * Migrate partitions in manual mode
     * // Working mode
     * //  Auto: Automatic transfer to the same number of partitions per Store
     * //  Expert: Expert Mode, Specifier is required transferParams
     *
     * @param params Designation transferParams, expert mode, request source store / target store
     *               in the same store group
     * @throws PDException
     */
    public void movePartition(Pdpb.OperationMode mode, List<Pdpb.MovePartitionParam> params) throws
                                                                                             PDException {
        Pdpb.MovePartitionRequest request = Pdpb.MovePartitionRequest.newBuilder()
                                                                     .setHeader(header)
                                                                     .setMode(mode)
                                                                     .addAllParam(params).build();
        Pdpb.MovePartitionResponse response = getStub().movePartition(request);
        handleResponseError(response.getHeader());
    }

    public void reportTask(MetaTask.Task task) throws PDException {
        Pdpb.ReportTaskRequest request = Pdpb.ReportTaskRequest.newBuilder()
                                                               .setHeader(header)
                                                               .setTask(task).build();
        Pdpb.ReportTaskResponse response = blockingUnaryCall(PDGrpc.getReportTaskMethod(), request);
        handleResponseError(response.getHeader());
    }

    public Metapb.PartitionStats getPartitionsStats(String graph, int partId) throws PDException {
        Pdpb.GetPartitionStatsRequest request = Pdpb.GetPartitionStatsRequest.newBuilder()
                                                                             .setHeader(header)
                                                                             .setGraphName(graph)
                                                                             .setPartitionId(partId)
                                                                             .build();
        Pdpb.GetPartitionStatsResponse response = getStub().getPartitionStats(request);
        handleResponseError(response.getHeader());
        return response.getPartitionStats();
    }

    /**
     * Balance the number of leaders in different stores
     */
    public void balanceLeaders() throws PDException {
        Pdpb.BalanceLeadersRequest request = Pdpb.BalanceLeadersRequest.newBuilder()
                                                                       .setHeader(header)
                                                                       .build();
        Pdpb.BalanceLeadersResponse response = getStub().balanceLeaders(request);
        handleResponseError(response.getHeader());
    }

    /**
     * Remove the store from the PD
     */
    public Metapb.Store delStore(long storeId) throws PDException {
        Pdpb.DetStoreRequest request = Pdpb.DetStoreRequest.newBuilder()
                                                           .setHeader(header)
                                                           .setStoreId(storeId)
                                                           .build();
        Pdpb.DetStoreResponse response = getStub().delStore(request);
        handleResponseError(response.getHeader());
        return response.getStore();
    }

    /**
     * Compaction on rocksdb as a whole
     *
     * @throws PDException
     */
    public void dbCompaction() throws PDException {
        Pdpb.DbCompactionRequest request = Pdpb.DbCompactionRequest
                .newBuilder()
                .setHeader(header)
                .build();
        Pdpb.DbCompactionResponse response = getStub().dbCompaction(request);
        handleResponseError(response.getHeader());
    }

    /**
     * Compaction on rocksdb specified tables
     *
     * @param tableName
     * @throws PDException
     */
    public void dbCompaction(String tableName) throws PDException {
        Pdpb.DbCompactionRequest request = Pdpb.DbCompactionRequest
                .newBuilder()
                .setHeader(header)
                .setTableName(tableName)
                .build();
        Pdpb.DbCompactionResponse response = getStub().dbCompaction(request);
        handleResponseError(response.getHeader());
    }

    /**
     * Merge partitions to reduce the current partition to toCount
     *
     * @param toCount The number of partitions that can be scaled down
     * @throws PDException
     */
    public void combineCluster(int toCount) throws PDException {
        Pdpb.CombineClusterRequest request = Pdpb.CombineClusterRequest
                .newBuilder()
                .setHeader(header)
                .setToCount(toCount)
                .build();
        Pdpb.CombineClusterResponse response = getStub().combineCluster(request);
        handleResponseError(response.getHeader());
    }

    /**
     * Scaling a single image to toCount is similar to splitting to ensure that the number of
     * partitions in the same store group is the same.
     * If you have special requirements, you can consider migrating to other groups
     *
     * @param graphName graph name
     * @param toCount   target count
     * @throws PDException
     */
    public void combineGraph(String graphName, int toCount) throws PDException {
        Pdpb.CombineGraphRequest request = Pdpb.CombineGraphRequest
                .newBuilder()
                .setHeader(header)
                .setGraphName(graphName)
                .setToCount(toCount)
                .build();
        Pdpb.CombineGraphResponse response = getStub().combineGraph(request);
        handleResponseError(response.getHeader());
    }

    public void deleteShardGroup(int groupId) throws PDException {
        Pdpb.DeleteShardGroupRequest request = Pdpb.DeleteShardGroupRequest
                .newBuilder()
                .setHeader(header)
                .setGroupId(groupId)
                .build();
        Pdpb.DeleteShardGroupResponse response =
                blockingUnaryCall(PDGrpc.getDeleteShardGroupMethod(), request);

        handleResponseError(response.getHeader());
    }

    /**
     * Used for the store's shard list rebuild
     *
     * @param groupId shard group id
     * @param shards  shard list, delete when shards size is 0
     */
    public void updateShardGroupOp(int groupId, List<Metapb.Shard> shards) throws PDException {
        Pdpb.ChangeShardRequest request = Pdpb.ChangeShardRequest.newBuilder()
                                                                 .setHeader(header)
                                                                 .setGroupId(groupId)
                                                                 .addAllShards(shards)
                                                                 .build();
        Pdpb.ChangeShardResponse response = getStub().updateShardGroupOp(request);
        handleResponseError(response.getHeader());
    }

    /**
     * invoke fireChangeShard command
     *
     * @param groupId shard group id
     * @param shards  shard list
     */
    public void changeShard(int groupId, List<Metapb.Shard> shards) throws PDException {
        Pdpb.ChangeShardRequest request = Pdpb.ChangeShardRequest.newBuilder()
                                                                 .setHeader(header)
                                                                 .setGroupId(groupId)
                                                                 .addAllShards(shards)
                                                                 .build();
        Pdpb.ChangeShardResponse response = getStub().changeShard(request);
        handleResponseError(response.getHeader());
    }

    public ClientCache getCache() {
        return cache;
    }

    public CacheResponse getClientCache() throws PDException {
        GetGraphRequest request = GetGraphRequest.newBuilder().setHeader(header).build();
        CacheResponse cache = getStub().getCache(request);
        handleResponseError(cache.getHeader());
        return cache;
    }

    public CachePartitionResponse getPartitionCache(String graph) throws PDException {
        GetGraphRequest request =
                GetGraphRequest.newBuilder().setHeader(header).setGraphName(graph).build();
        CachePartitionResponse ps = getStub().getPartitions(request);
        handleResponseError(ps.getHeader());
        return ps;
    }

    public void updatePdRaft(String raftConfig) throws PDException {
        Pdpb.UpdatePdRaftRequest request = Pdpb.UpdatePdRaftRequest.newBuilder()
                                                                   .setHeader(header)
                                                                   .setConfig(raftConfig)
                                                                   .build();
        Pdpb.UpdatePdRaftResponse response = getStub().updatePdRaft(request);
        handleResponseError(response.getHeader());
    }

    public GraphStatsResponse getGraphStats(String graphName) throws PDException {
        GetGraphRequest request =
                GetGraphRequest.newBuilder().setHeader(header).setGraphName(graphName).build();
        GraphStatsResponse graphStats = getStub().getGraphStats(request);
        handleResponseError(graphStats.getHeader());
        return graphStats;
    }

    public long submitBuildIndexTask(Metapb.BuildIndexParam param) throws PDException {
        Pdpb.IndexTaskCreateRequest request = Pdpb.IndexTaskCreateRequest.newBuilder()
                                                                         .setHeader(header)
                                                                         .setParam(param)
                                                                         .build();
        var response = getStub().submitTask(request);
        handleResponseError(response.getHeader());
        return response.getTaskId();
    }

    public Pdpb.IndexTaskQueryResponse queryBuildIndexTaskStatus(long taskId) throws PDException {
        Pdpb.IndexTaskQueryRequest request = Pdpb.IndexTaskQueryRequest.newBuilder()
                                                                       .setHeader(header)
                                                                       .setTaskId(taskId)
                                                                       .build();
        var response = getStub().queryTaskState(request);
        handleResponseError(response.getHeader());
        return response;
    }

    public Pdpb.IndexTaskQueryResponse retryBuildIndexTask(long taskId) throws PDException {
        Pdpb.IndexTaskQueryRequest request = Pdpb.IndexTaskQueryRequest.newBuilder()
                                                                       .setHeader(header)
                                                                       .setTaskId(taskId)
                                                                       .build();
        var response = getStub().retryIndexTask(request);
        handleResponseError(response.getHeader());
        return response;
    }
}
