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

package org.apache.hugegraph.pd.common;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.hugegraph.pd.grpc.Metapb;

import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;

/**
 * abandon copy on write way
 * 1. When the number of graph * partitions is extremely large, the efficiency is severely
 * reduced and cannot be used
 */
public class PartitionCache {

    private ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private volatile Map<String, AtomicBoolean> locks = new ConcurrentHashMap<>();
    Lock writeLock = readWriteLock.writeLock();
    // One cache per graph
    private volatile Map<String, RangeMap<Long, Integer>> keyToPartIdCache;
    // graphName + PartitionID
    private volatile Map<String, Map<Integer, Metapb.Partition>> partitionCache;
    private volatile Map<Integer, Metapb.ShardGroup> shardGroupCache;
    private volatile Map<Long, Metapb.Store> storeCache;
    private volatile Map<String, Metapb.Graph> graphCache;

    public PartitionCache() {
        keyToPartIdCache = new ConcurrentHashMap<>();
        partitionCache = new ConcurrentHashMap<>();
        shardGroupCache = new ConcurrentHashMap<>();
        storeCache = new ConcurrentHashMap<>();
        graphCache = new ConcurrentHashMap<>();
    }

    private AtomicBoolean getOrCreateGraphLock(String graphName) {
        var lock = this.locks.get(graphName);
        if (lock == null) {
            try {
                writeLock.lock();
                if ((lock = this.locks.get(graphName)) == null) {
                    lock = new AtomicBoolean();
                    locks.put(graphName, lock);
                }
            } finally {
                writeLock.unlock();
            }
        }
        return lock;
    }

    public void waitGraphLock(String graphName) {
        var lock = getOrCreateGraphLock(graphName);
        while (lock.get()) {
            Thread.onSpinWait();
        }
    }

    public void lockGraph(String graphName) {
        var lock = getOrCreateGraphLock(graphName);
        while (!lock.compareAndSet(false, true)) {
            Thread.onSpinWait();
        }
    }

    public void unlockGraph(String graphName) {
        var lock = getOrCreateGraphLock(graphName);
        lock.set(false);
    }

    /**
     * Returns partition information based on partitionId
     *
     * @param graphName
     * @param partId
     * @return
     */
    public KVPair<Metapb.Partition, Metapb.Shard> getPartitionById(String graphName, int partId) {
        waitGraphLock(graphName);
        var graphs = partitionCache.get(graphName);
        if (graphs != null) {
            var partition = graphs.get(partId);
            if (partition != null) {
                return new KVPair<>(partition, getLeaderShard(partId));
            }
        }

        return null;
    }

    /**
     * Returns the partition information where the key is located
     *
     * @param key
     * @return
     */
    public KVPair<Metapb.Partition, Metapb.Shard> getPartitionByKey(String graphName, byte[] key) {
        int code = PartitionUtils.calcHashcode(key);
        return getPartitionByCode(graphName, code);
    }

    /**
     * Returns partition information based on the hashcode of the key
     *
     * @param graphName
     * @param code
     * @return
     */
    public KVPair<Metapb.Partition, Metapb.Shard> getPartitionByCode(String graphName, long code) {
        waitGraphLock(graphName);
        RangeMap<Long, Integer> rangeMap = keyToPartIdCache.get(graphName);
        if (rangeMap != null) {
            Integer partId = rangeMap.get(code);
            if (partId != null) {
                return getPartitionById(graphName, partId);
            }
        }
        return null;
    }

    public List<Metapb.Partition> getPartitions(String graphName) {
        waitGraphLock(graphName);

        List<Metapb.Partition> partitions = new ArrayList<>();
        if (!partitionCache.containsKey(graphName)) {
            return partitions;
        }
        partitionCache.get(graphName).forEach((k, v) -> {
            partitions.add(v);
        });

        return partitions;
    }

    public boolean addPartition(String graphName, int partId, Metapb.Partition partition) {
        waitGraphLock(graphName);
        Metapb.Partition old = null;

        if (partitionCache.containsKey(graphName)) {
            old = partitionCache.get(graphName).get(partId);
        }

        if (old != null && old.equals(partition)) {
            return false;
        }
        try {

            lockGraph(graphName);

            partitionCache.computeIfAbsent(graphName, k -> new HashMap<>()).put(partId, partition);

            if (old != null) {
                // old [1-3] is covered by [2-3]. When [1-3) becomes [1-2], the original [1-3]
                // should not be deleted
                // When you confirm that the old start and end are your own, you can delete the
                // old ones. (i.e. not covered yet)
                var graphRange = keyToPartIdCache.get(graphName);
                if (Objects.equals(partition.getId(), graphRange.get(partition.getStartKey())) &&
                    Objects.equals(partition.getId(), graphRange.get(partition.getEndKey() - 1))) {
                    graphRange.remove(graphRange.getEntry(partition.getStartKey()).getKey());
                }
            }

            keyToPartIdCache.computeIfAbsent(graphName, k -> TreeRangeMap.create())
                            .put(Range.closedOpen(partition.getStartKey(),
                                                  partition.getEndKey()), partId);
        } finally {
            unlockGraph(graphName);
        }
        return true;
    }

    public void updatePartition(String graphName, int partId, Metapb.Partition partition) {
        try {
            lockGraph(graphName);
            Metapb.Partition old = null;
            var graphs = partitionCache.get(graphName);
            if (graphs != null) {
                old = graphs.get(partId);
            }

            if (old != null) {
                var graphRange = keyToPartIdCache.get(graphName);
                if (Objects.equals(partition.getId(), graphRange.get(partition.getStartKey())) &&
                    Objects.equals(partition.getId(), graphRange.get(partition.getEndKey() - 1))) {
                    graphRange.remove(graphRange.getEntry(partition.getStartKey()).getKey());
                }
            }

            partitionCache.computeIfAbsent(graphName, k -> new ConcurrentHashMap<>())
                          .put(partId, partition);
            keyToPartIdCache.computeIfAbsent(graphName, k -> TreeRangeMap.create())
                            .put(Range.closedOpen(partition.getStartKey(), partition.getEndKey()),
                                 partId);
        } finally {
            unlockGraph(graphName);
        }
    }

    public boolean updatePartition(Metapb.Partition partition) {

        var graphName = partition.getGraphName();
        var partitionId = partition.getId();

        var old = getPartitionById(graphName, partitionId);
        if (old != null && Objects.equals(partition, old.getKey())) {
            return false;
        }

        updatePartition(graphName, partitionId, partition);
        return true;
    }

    public void removePartition(String graphName, int partId) {
        try {
            lockGraph(graphName);
            var partition = partitionCache.get(graphName).remove(partId);
            if (partition != null) {
                var graphRange = keyToPartIdCache.get(graphName);

                if (Objects.equals(partition.getId(), graphRange.get(partition.getStartKey())) &&
                    Objects.equals(partition.getId(), graphRange.get(partition.getEndKey() - 1))) {
                    graphRange.remove(graphRange.getEntry(partition.getStartKey()).getKey());
                }
            }
        } finally {
            unlockGraph(graphName);
        }
    }

    /**
     * remove partition id of graph name
     *
     * @param graphName
     * @param id
     */
    public void remove(String graphName, int id) {
        removePartition(graphName, id);
    }

    /**
     * remove all partitions
     */
    public void removePartitions() {
        writeLock.lock();
        try {
            partitionCache = new ConcurrentHashMap<>();
            keyToPartIdCache = new ConcurrentHashMap<>();
            locks.clear();
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * remove partition cache of graphName
     *
     * @param graphName
     */
    public void removeAll(String graphName) {
        try {
            lockGraph(graphName);
            partitionCache.remove(graphName);
            keyToPartIdCache.remove(graphName);
            locks.remove(graphName);
        } finally {
            unlockGraph(graphName);
        }
    }

    private String makePartitionKey(String graphName, int partId) {
        return graphName + "/" + partId;
    }

    public boolean updateShardGroup(Metapb.ShardGroup shardGroup) {
        Metapb.ShardGroup oldShardGroup = shardGroupCache.get(shardGroup.getId());
        if (oldShardGroup != null && oldShardGroup.equals(shardGroup)) {
            return false;
        }
        shardGroupCache.put(shardGroup.getId(), shardGroup);
        return true;
    }

    public void deleteShardGroup(int shardGroupId) {
        shardGroupCache.remove(shardGroupId);
    }

    public Metapb.ShardGroup getShardGroup(int groupId) {
        return shardGroupCache.get(groupId);
    }

    public Map<Integer, Metapb.ShardGroup> getShardGroups() {
        return this.shardGroupCache;
    }

    public boolean addStore(Long storeId, Metapb.Store store) {
        Metapb.Store oldStore = storeCache.get(storeId);
        if (oldStore != null && oldStore.equals(store)) {
            return false;
        }
        storeCache.put(storeId, store);
        return true;
    }

    public Metapb.Store getStoreById(Long storeId) {
        return storeCache.get(storeId);
    }

    public void removeStore(Long storeId) {
        storeCache.remove(storeId);
    }

    public boolean hasGraph(String graphName) {
        return getPartitions(graphName).size() > 0;
    }

    public void updateGraph(Metapb.Graph graph) {
        if (Objects.equals(graph, getGraph(graph.getGraphName()))) {
            return;
        }
        graphCache.put(graph.getGraphName(), graph);
    }

    public Metapb.Graph getGraph(String graphName) {
        return graphCache.get(graphName);
    }

    public List<Metapb.Graph> getGraphs() {
        List<Metapb.Graph> graphs = new ArrayList<>();
        graphCache.forEach((k, v) -> {
            graphs.add(v);
        });
        return graphs;
    }

    public void reset() {
        writeLock.lock();
        try {
            partitionCache = new ConcurrentHashMap<>();
            keyToPartIdCache = new ConcurrentHashMap<>();
            shardGroupCache = new ConcurrentHashMap<>();
            storeCache = new ConcurrentHashMap<>();
            graphCache = new ConcurrentHashMap<>();
            locks.clear();
        } finally {
            writeLock.unlock();
        }
    }

    public void clear() {
        reset();
    }

    public String debugCacheByGraphName(String graphName) {
        StringBuilder builder = new StringBuilder();
        builder.append("Graph:").append(graphName).append(", cache info: range info: {");
        var rangeMap = keyToPartIdCache.get(graphName);
        builder.append(rangeMap == null ? "" : rangeMap).append("}");

        if (rangeMap != null) {
            builder.append(", partition info : {");
            rangeMap.asMapOfRanges().forEach((k, v) -> {
                var partition = partitionCache.get(graphName).get(v);
                builder.append("[part_id:").append(v);
                if (partition != null) {
                    builder.append(", start_key:").append(partition.getStartKey())
                           .append(", end_key:").append(partition.getEndKey())
                           .append(", state:").append(partition.getState().name());
                }
                builder.append("], ");
            });
            builder.append("}");
        }

        builder.append(", graph info:{");
        var graph = graphCache.get(graphName);
        if (graph != null) {
            builder.append("partition_count:").append(graph.getPartitionCount())
                   .append(", state:").append(graph.getState().name());
        }
        builder.append("}]");
        return builder.toString();
    }

    public Metapb.Shard getLeaderShard(int partitionId) {
        var shardGroup = shardGroupCache.get(partitionId);
        if (shardGroup != null) {
            for (Metapb.Shard shard : shardGroup.getShardsList()) {
                if (shard.getRole() == Metapb.ShardRole.Leader) {
                    return shard;
                }
            }
        }

        return null;
    }

    public void updateShardGroupLeader(int partitionId, Metapb.Shard leader) {
        if (shardGroupCache.containsKey(partitionId) && leader != null) {
            if (!Objects.equals(getLeaderShard(partitionId), leader)) {
                var shardGroup = shardGroupCache.get(partitionId);
                var builder = Metapb.ShardGroup.newBuilder(shardGroup).clearShards();
                for (var shard : shardGroup.getShardsList()) {
                    builder.addShards(
                            Metapb.Shard.newBuilder()
                                        .setStoreId(shard.getStoreId())
                                        .setRole(shard.getStoreId() == leader.getStoreId() ?
                                                 Metapb.ShardRole.Leader :
                                                 Metapb.ShardRole.Follower)
                                        .build()
                    );
                }
                shardGroupCache.put(partitionId, builder.build());
            }
        }
    }

    public String debugShardGroup() {
        StringBuilder builder = new StringBuilder();
        builder.append("shard group cache:{");
        shardGroupCache.forEach((partitionId, shardGroup) -> {
            builder.append(partitionId).append("::{")
                   .append("version:").append(shardGroup.getVersion())
                   .append(", conf_version:").append(shardGroup.getConfVer())
                   .append(", state:").append(shardGroup.getState().name())
                   .append(", shards:[");

            for (var shard : shardGroup.getShardsList()) {
                builder.append("{store_id:").append(shard.getStoreId())
                       .append(", role:").append(shard.getRole().name())
                       .append("},");
            }
            builder.append("], ");
        });
        builder.append("}");
        return builder.toString();
    }
}
