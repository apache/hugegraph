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

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;

import org.apache.hugegraph.pd.common.GraphCache;
import org.apache.hugegraph.pd.common.KVPair;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.common.PartitionUtils;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.pd.grpc.Metapb.Graph;
import org.apache.hugegraph.pd.grpc.Metapb.Graph.Builder;
import org.apache.hugegraph.pd.grpc.Metapb.Partition;
import org.apache.hugegraph.pd.grpc.Metapb.Shard;
import org.apache.hugegraph.pd.grpc.Metapb.ShardGroup;
import org.apache.hugegraph.pd.grpc.Pdpb.CachePartitionResponse;
import org.apache.hugegraph.pd.grpc.Pdpb.CacheResponse;

import com.google.common.collect.RangeMap;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ClientCache {

    private AtomicBoolean initialized = new AtomicBoolean(false);
    private PDClient client;
    private volatile Map<Integer, KVPair<ShardGroup, Shard>> groups;
    private volatile Map<Long, Metapb.Store> stores;
    private volatile Map<String, GraphCache> caches = new ConcurrentHashMap<>();

    public ClientCache(org.apache.hugegraph.pd.client.PDClient pdClient) {
        groups = new ConcurrentHashMap<>();
        stores = new ConcurrentHashMap<>();
        client = pdClient;
    }

    private GraphCache getGraphCache(String graphName) { 
        GraphCache graph;
        if ((graph = caches.get(graphName)) == null) {
            synchronized (caches) {
                if ((graph = caches.get(graphName)) == null) {
                    Builder builder = Graph.newBuilder().setGraphName(graphName);
                    Graph g = builder.build();
                    graph = new GraphCache(g);
                    caches.put(graphName, graph);
                }
            }
        }
        return graph;
    }

    public KVPair<Partition, Shard> getPartitionById(String graphName, int partId) {
        try {
            GraphCache graph = initGraph(graphName);
            Partition partition = graph.getPartition(partId);
            if (partition == null) {
                return null;
            }
            KVPair<ShardGroup, Shard> group = groups.get(partId);
            if (group == null) {
                return null;
            }
            Shard shard = group.getValue();
            if (shard == null) {
                return null;
            }
            return new KVPair<>(partition, shard);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private KVPair<Partition, Shard> getPair(int partId, GraphCache graph) {
        Partition p = graph.getPartition(partId);
        KVPair<ShardGroup, Shard> pair = groups.get(partId);
        if (p != null && pair != null) {
            Shard s = pair.getValue();
            if (s == null) {
                pair.setValue(getLeader(partId));
                return new KVPair<>(p, pair.getValue());
            } else {
                return new KVPair<>(p, s);
            }
        }
        return null;
    }

    public KVPair<Partition, Shard> getPartitionByCode(String graphName, long code) {
        try {
            GraphCache graph = initGraph(graphName);
            RangeMap<Long, Integer> range = graph.getRange();
            Integer pId = range.get(code);
            if (pId != null) {
                return getPair(pId, graph);
            } else {
                ReadLock readLock = graph.getLock().readLock();
                try {
                    readLock.lock();
                    pId = range.get(code);
                } catch (Exception e) {
                    log.info("get range with error:", e);
                } finally {
                    readLock.unlock();
                }
                if (pId == null) {
                    WriteLock writeLock = graph.getLock().writeLock();
                    try {
                        writeLock.lock();
                        if ((pId = range.get(code)) == null) {
                            graph.reset();
                            initGraph(graph);
                            pId = range.get(code);
                        }
                    } catch (Exception e) {
                        log.info("reset with error:", e);
                    } finally {
                        writeLock.unlock();
                    }

                }
                if (pId != null) {
                    return getPair(pId, graph);
                }
            }
            return null;
        } catch (PDException e) {
            throw new RuntimeException(e);
        }
    }

    private GraphCache initGraph(String graphName) throws PDException {
        initCache();
        GraphCache graph = getGraphCache(graphName);
        if (!graph.getInitialized().get()) {
            synchronized (graph) {
                if (!graph.getInitialized().get()) {
                    initGraph(graph);
                    graph.getInitialized().set(true);
                }
            }
        }
        return graph;
    }

    private void initGraph(GraphCache graph) throws PDException {
        CachePartitionResponse pc = client.getPartitionCache(graph.getGraph().getGraphName());
        List<Partition> ps = pc.getPartitionsList();
        if (!CollectionUtils.isEmpty(ps)) {
            graph.init(ps);
        }
    }

    private void initCache() throws PDException {
        if (!initialized.get()) {
            synchronized (this) {
                if (!initialized.get()) {
                    CacheResponse cache = client.getClientCache();
                    List<ShardGroup> shardGroups = cache.getShardsList();
                    for (ShardGroup s : shardGroups) {
                        this.groups.put(s.getId(), new KVPair<>(s, getLeader(s)));
                    }
                    List<Metapb.Store> stores = cache.getStoresList();
                    for (Metapb.Store store : stores) {
                        this.stores.put(store.getId(), store);
                    }
                    List<Metapb.Graph> graphs = cache.getGraphsList();
                    for (Metapb.Graph g : graphs) {
                        GraphCache c = new GraphCache(g);
                        caches.put(g.getGraphName(), c);
                    }
                    initialized.set(true);
                }
            }
        }
    }

    public KVPair<Partition, Shard> getPartitionByKey(String graphName, byte[] key) {
        int code = PartitionUtils.calcHashcode(key);
        return getPartitionByCode(graphName, code);
    }

    public boolean update(String graphName, int partId, Partition partition) {
        GraphCache graph = getGraphCache(graphName);
        return graph.updatePartition(partition);
    }

    public void removePartition(String graphName, int partId) {
        GraphCache graph = getGraphCache(graphName);
        graph.removePartition(partId);
    }

    /**
     * remove all partitions
     */
    public void removePartitions() {
        try {
            groups.clear();
            stores.clear();
            caches.clear();
            initialized.set(false);
            initCache();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void removePartitions(GraphCache graph) {
        try {
            graph.removePartitions();
            initGraph(graph.getGraph().getGraphName());
        } catch (Exception e) {
            log.warn("remove partitions with error:", e);
        } finally {
        }
    }

    /**
     * remove partition cache of graphName
     *
     * @param graphName
     */
    public void removeAll(String graphName) {
        GraphCache graph = caches.get(graphName);
        if (graph != null) {
            removePartitions(graph);
        }
    }

    private StringBuffer getStack(StackTraceElement[] stackTrace) {
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < stackTrace.length; i++) {
            StackTraceElement element = stackTrace[i];
            sb.append(element.toString() + "\n");
        }
        return sb;
    }

    public boolean updateShardGroup(ShardGroup shardGroup) {
        KVPair<ShardGroup, Shard> old = groups.get(shardGroup.getId());
        Shard leader = getLeader(shardGroup);
        if (old != null) {
            old.setKey(shardGroup);
            old.setValue(leader);
            return false;
        }
        groups.put(shardGroup.getId(), new KVPair<>(shardGroup, leader));
        return true;
    }

    public void deleteShardGroup(int shardGroupId) {
        groups.remove(shardGroupId);
    }

    public ShardGroup getShardGroup(int groupId) {
        KVPair<ShardGroup, Shard> pair = groups.get(groupId);
        if (pair != null) {
            return pair.getKey();
        }
        return null;
    }

    public boolean addStore(Long storeId, Metapb.Store store) {
        Metapb.Store oldStore = stores.get(storeId);
        if (oldStore != null && oldStore.equals(store)) {
            return false;
        }
        stores.put(storeId, store);
        return true;
    }

    public Metapb.Store getStoreById(Long storeId) {
        return stores.get(storeId);
    }

    public void removeStore(Long storeId) {
        stores.remove(storeId);
    }

    public void reset() {
        try {
            groups = new ConcurrentHashMap<>();
            stores = new ConcurrentHashMap<>();
            caches = new ConcurrentHashMap<>();
            initialized.set(false);
        } finally {
        }
    }

    public Shard getLeader(int partitionId) {
        KVPair<ShardGroup, Shard> pair = groups.get(partitionId);
        if (pair != null) {
            if (pair.getValue() != null) {
                return pair.getValue();
            }
            for (Shard shard : pair.getKey().getShardsList()) {
                if (shard.getRole() == Metapb.ShardRole.Leader) {
                    pair.setValue(shard);
                    return shard;
                }
            }
        }

        return null;
    }

    public Shard getLeader(ShardGroup shardGroup) {
        if (shardGroup != null) {
            for (Shard shard : shardGroup.getShardsList()) {
                if (shard.getRole() == Metapb.ShardRole.Leader) {
                    return shard;
                }
            }
        }

        return null;
    }

    public void updateLeader(int partitionId, Shard leader) {
        KVPair<ShardGroup, Shard> pair = groups.get(partitionId);
        if (pair != null && leader != null) {
            Shard l = getLeader(partitionId);
            if (l == null || leader.getStoreId() != l.getStoreId()) {
                ShardGroup shardGroup = pair.getKey();
                ShardGroup.Builder builder = ShardGroup.newBuilder(shardGroup).clearShards();
                for (var shard : shardGroup.getShardsList()) {
                    builder.addShards(
                            Shard.newBuilder()
                                 .setStoreId(shard.getStoreId())
                                 .setRole(shard.getStoreId() == leader.getStoreId() ?
                                          Metapb.ShardRole.Leader : Metapb.ShardRole.Follower)
                                 .build()
                    );
                }
                pair.setKey(builder.build());
                pair.setValue(leader);
            }
        }
    }

    public List<String> getLeaderStoreAddresses() throws PDException {
        initCache();
        var storeIds = this.groups.values().stream()
                                  .map(KVPair::getValue)
                                  .filter(java.util.Objects::nonNull)
                                  .map(Shard::getStoreId)
                                  .collect(Collectors.toSet());
        return this.stores.values().stream()
                          .filter(store -> storeIds.contains(store.getId()))
                          .map(Metapb.Store::getAddress)
                          .collect(Collectors.toList());
    }

    public Map<Integer, String> getLeaderPartitionStoreAddress(String graphName) throws
                                                                                 PDException {
        initCache();
        return this.groups.values()
                          .stream()
                          .collect(Collectors.toMap(
                                  pair -> pair.getKey().getId(),
                                  pair -> this.stores.get(pair.getValue().getStoreId()).getAddress()
                          ));
    }
}
