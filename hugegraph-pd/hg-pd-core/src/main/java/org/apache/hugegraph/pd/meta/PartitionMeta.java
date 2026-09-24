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

package org.apache.hugegraph.pd.meta;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.common.PDRuntimeException;
import org.apache.hugegraph.pd.common.PartitionCache;
import org.apache.hugegraph.pd.config.PDConfig;
import org.apache.hugegraph.pd.grpc.Metapb;

import lombok.extern.slf4j.Slf4j;

/**
 * Partition information management
 */
@Slf4j
public class PartitionMeta extends MetadataRocksDBStore {

    public static final String CID_GRAPH_ID_KEY = "GraphID";
    public static final int CID_GRAPH_ID_MAX = 0xFFFE;
    private PDConfig pdConfig;
    private PartitionCache cache;

    public PartitionMeta(PDConfig pdConfig) {
        super(pdConfig);
        this.pdConfig = pdConfig;
        //this.timeout = pdConfig.getEtcd().getTimeout();
        this.cache = new PartitionCache();
    }

    /**
     * Initialize, load all partitions
     */
    public void init() throws PDException {
        loadShardGroups();
        loadGraphs();
    }

    public void reload() throws PDException {
        cache.clear();
        loadShardGroups();
        loadGraphs();
    }

    private void loadGraphs() throws PDException {
        byte[] key = MetadataKeyHelper.getGraphPrefix();
        List<Metapb.Graph> graphs = scanPrefix(Metapb.Graph.parser(), key);
        for (Metapb.Graph graph : graphs) {
            cache.updateGraph(graph);
            loadPartitions(graph);
        }
    }

    /**
     * The partition and shard group are stored separately, and when they are init, they need to
     * be loaded
     *
     * @throws PDException
     */
    private void loadShardGroups() throws PDException {
        byte[] shardGroupPrefix = MetadataKeyHelper.getShardGroupPrefix();
        for (var shardGroup : scanPrefix(Metapb.ShardGroup.parser(), shardGroupPrefix)) {
            cache.updateShardGroup(shardGroup);
        }
    }

    private void loadPartitions(Metapb.Graph graph) throws PDException {
        byte[] prefix = MetadataKeyHelper.getPartitionPrefix(graph.getGraphName());
        List<Metapb.Partition> partitions = scanPrefix(Metapb.Partition.parser(), prefix);
        partitions.forEach(p -> {
            cache.updatePartition(p);
        });
    }

    /**
     * Find partitions by ID (first from the cache, then from the database)
     *
     * @param graphName
     * @param partId
     * @return
     * @throws PDException
     */
    public Metapb.Partition getPartitionById(String graphName, int partId) throws PDException {
        var pair = cache.getPartitionById(graphName, partId);
        Metapb.Partition partition;
        if (pair == null) {
            byte[] key = MetadataKeyHelper.getPartitionKey(graphName, partId);
            partition = getOne(Metapb.Partition.parser(), key);
            if (partition != null) {
                cache.updatePartition(partition);
            }
        } else {
            partition = pair.getKey();
        }
        return partition;
    }

    public List<Metapb.Partition> getPartitionById(int partId) throws PDException {
        List<Metapb.Partition> partitions = new ArrayList<>();
        cache.getGraphs().forEach(graph -> {
            cache.getPartitions(graph.getGraphName()).forEach(partition -> {
                if (partition.getId() == partId) {
                    partitions.add(partition);
                }
            });
        });
        return partitions;
    }

    /**
     * Find partitions based on code
     */
    public Metapb.Partition getPartitionByCode(String graphName, long code) throws PDException {
        var pair = cache.getPartitionByCode(graphName, code);
        if (pair != null) {
            return pair.getKey();
        }
        return null;
    }

    public Metapb.Graph getAndCreateGraph(String graphName) throws PDException {
        return getAndCreateGraph(graphName, pdConfig.getPartition().getTotalCount());
    }

    public Metapb.Graph getAndCreateGraph(String graphName, int partitionCount) throws PDException {

        if (partitionCount > pdConfig.getPartition().getTotalCount()) {
            partitionCount = pdConfig.getPartition().getTotalCount();
        }

        if (graphName.endsWith("/s") || graphName.endsWith("/m")) {
            partitionCount = 1;
        }

        Metapb.Graph graph = cache.getGraph(graphName);
        if (graph == null) {
            graph = Metapb.Graph.newBuilder()
                                .setGraphName(graphName)
                                .setPartitionCount(partitionCount)
                                .setState(Metapb.PartitionState.PState_Normal)
                                .build();
            updateGraph(graph);
        }
        return graph;
    }

    /**
     * Save the partition information
     *
     * @param partition
     * @return
     * @throws PDException
     */
    public Metapb.Partition updatePartition(Metapb.Partition partition) throws PDException {
        if (!cache.hasGraph(partition.getGraphName())) {
            getAndCreateGraph(partition.getGraphName());
        }
        byte[] key = MetadataKeyHelper.getPartitionKey(partition.getGraphName(), partition.getId());
        put(key, partition.toByteString().toByteArray());
        cache.updatePartition(partition);
        return partition;
    }

    /**
     * Check the database to see if the corresponding graph exists. If it does not exist, create it.
     * Update partition version, conf version, and shard list
     *
     * @param partition
     * @return
     * @throws PDException
     */
    public Metapb.Partition updateShardList(Metapb.Partition partition) throws PDException {
        if (!cache.hasGraph(partition.getGraphName())) {
            getAndCreateGraph(partition.getGraphName());
        }

        Metapb.Partition pt = getPartitionById(partition.getGraphName(), partition.getId());
        //  pt = pt.toBuilder().setVersion(partition.getVersion())
        //        .setConfVer(partition.getConfVer())
        //        .clearShards()
        //        .addAllShards(partition.getShardsList()).build();

        byte[] key = MetadataKeyHelper.getPartitionKey(pt.getGraphName(), pt.getId());
        put(key, pt.toByteString().toByteArray());
        cache.updatePartition(pt);
        return partition;
    }

    /**
     * Delete all partitions
     */
    public long removeAllPartitions(String graphName) throws PDException {
        cache.removeAll(graphName);
        byte[] prefix = MetadataKeyHelper.getPartitionPrefix(graphName);
        return removeByPrefix(prefix);
    }

    public long removePartition(String graphName, int id) throws PDException {
        cache.remove(graphName, id);
        byte[] key = MetadataKeyHelper.getPartitionKey(graphName, id);
        return remove(key);
    }

    public void updatePartitionStats(Metapb.PartitionStats stats) throws PDException {
        // for (String graphName : stats.getGraphNameList()) {
        byte[] prefix = MetadataKeyHelper.getPartitionStatusKey("", stats.getId());
        put(prefix, stats.toByteArray());
        // }
    }

    /**
     * Get the partition status
     */
    public Metapb.PartitionStats getPartitionStats(String graphName, int id) throws PDException {
        byte[] prefix = MetadataKeyHelper.getPartitionStatusKey(graphName, id);
        return getOne(Metapb.PartitionStats.parser(), prefix);
    }

    /**
     * Get the partition status
     */
    public List<Metapb.PartitionStats> getPartitionStats(String graphName) throws PDException {
        byte[] prefix = MetadataKeyHelper.getPartitionStatusPrefixKey(graphName);
        return scanPrefix(Metapb.PartitionStats.parser(), prefix);
    }

    /**
     * Update the diagram information
     *
     * @param graph
     * @return
     */
    public Metapb.Graph updateGraph(Metapb.Graph graph) throws PDException {
        log.info("updateGraph {}", graph);
        byte[] key = MetadataKeyHelper.getGraphKey(graph.getGraphName());
        // save graph information
        put(key, graph.toByteString().toByteArray());
        cache.updateGraph(graph);
        return graph;
    }

    public List<Metapb.Partition> getPartitions() {
        List<Metapb.Partition> partitions = new ArrayList<>();
        try {
            List<Metapb.Graph> graphs = cache.getGraphs();
            if (CollectionUtils.isEmpty(graphs)) {
                loadGraphs();
                graphs = cache.getGraphs();
            }
            graphs.forEach(e -> partitions.addAll(cache.getPartitions(e.getGraphName())));
        } catch (PDException e) {
            throw new PDRuntimeException(e.getErrorCode(), e);
        }
        return partitions;
    }

    public List<Metapb.Partition> getPartitions(String graphName) {
        return cache.getPartitions(graphName);
    }

    public List<Metapb.Graph> getGraphs() throws PDException {
        byte[] key = MetadataKeyHelper.getGraphPrefix();
        return scanPrefix(Metapb.Graph.parser(), key);
    }

    public Metapb.Graph getGraph(String graphName) throws PDException {
        byte[] key = MetadataKeyHelper.getGraphKey(graphName);
        return getOne(Metapb.Graph.parser(), key);
    }

    /**
     * Delete the diagram and delete the diagram ID
     */
    public long removeGraph(String graphName) throws PDException {
        byte[] key = MetadataKeyHelper.getGraphKey(graphName);
        long l = remove(key);
        return l;
    }

    public long removePartitionStats(String graphName) throws PDException {
        byte[] prefix = MetadataKeyHelper.getPartitionStatusPrefixKey(graphName);
        return removeByPrefix(prefix);
    }

    public PartitionCache getPartitionCache() {
        return cache;
    }
}
