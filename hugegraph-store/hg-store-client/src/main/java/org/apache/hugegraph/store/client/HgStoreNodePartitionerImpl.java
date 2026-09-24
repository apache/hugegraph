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

package org.apache.hugegraph.store.client;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.hugegraph.pd.client.PDClient;
import org.apache.hugegraph.pd.common.KVPair;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.common.PartitionUtils;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.store.client.type.HgNodeStatus;
import org.apache.hugegraph.store.client.util.HgStoreClientConst;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class HgStoreNodePartitionerImpl implements HgStoreNodePartitioner,
                                                   HgStoreNodeProvider,
                                                   HgStoreNodeNotifier {

    private PDClient pdClient;
    private HgStoreNodeManager nodeManager;

    protected HgStoreNodePartitionerImpl() {
    }

    public HgStoreNodePartitionerImpl(PDClient pdClient, HgStoreNodeManager nodeManager) {
        this.pdClient = pdClient;
        this.nodeManager = nodeManager;
    }

    /**
     * Query partition information, the result is returned through HgNodePartitionerBuilder.
     */
    @Override
    public int partition(HgNodePartitionerBuilder builder, String graphName,
                         byte[] startKey, byte[] endKey) {
        try {
            HashSet<HgNodePartition> partitions = null;
            if (HgStoreClientConst.ALL_PARTITION_OWNER == startKey) {
                List<Metapb.Store> stores = pdClient.getActiveStores(graphName);
                partitions = new HashSet<>(stores.size());
                for (Metapb.Store store : stores) {
                    partitions.add(HgNodePartition.of(store.getId(), -1));
                }

            } else if (endKey == HgStoreClientConst.EMPTY_BYTES
                       || startKey == endKey || Arrays.equals(startKey, endKey)) {
                KVPair<Metapb.Partition, Metapb.Shard> partShard =
                        pdClient.getPartition(graphName, startKey);
                Metapb.Shard leader = partShard.getValue();
                partitions = new HashSet<>();
                partitions.add(HgNodePartition.of(leader.getStoreId(),
                                                  pdClient.keyToCode(graphName, startKey)));
            } else {
                log.warn(
                        "StartOwnerkey is not equal to endOwnerkey, which is meaningless!!, It is" +
                        " a error!!");
                List<Metapb.Store> stores = pdClient.getActiveStores(graphName);
                partitions = new HashSet<>(stores.size());
                for (Metapb.Store store : stores) {
                    partitions.add(HgNodePartition.of(store.getId(), -1));
                }
            }
            builder.setPartitions(partitions);
        } catch (PDException e) {
            log.error("An error occurred while getting partition information :{}", e.getMessage());
            throw new RuntimeException(e.getMessage(), e);
        }
        return 0;
    }

    @Override
    public int partition(HgNodePartitionerBuilder builder, String graphName,
                         int startKey, int endKey) {
        try {
            HashSet<HgNodePartition> partitions = new HashSet<>();
            Metapb.Partition partition = null;
            while ((partition == null || partition.getEndKey() < endKey)
                   && startKey < PartitionUtils.MAX_VALUE) {
                KVPair<Metapb.Partition, Metapb.Shard> partShard =
                        pdClient.getPartitionByCode(graphName, startKey);
                if (partShard != null) {
                    partition = partShard.getKey();
                    Metapb.Shard leader = partShard.getValue();
                    partitions.add(HgNodePartition.of(leader.getStoreId(), startKey,
                                                      (int) partition.getStartKey(),
                                                      (int) partition.getEndKey()));
                    startKey = (int) partition.getEndKey();
                } else {
                    break;
                }
            }
            builder.setPartitions(partitions);
        } catch (PDException e) {
            log.error("An error occurred while getting partition information :{}", e.getMessage());
            throw new RuntimeException(e.getMessage(), e);
        }
        return 0;
    }

    @Override
    public int partition(HgNodePartitionerBuilder builder, String graphName,
                         int partitionId) {
        try {
            HashSet<HgNodePartition> partitions = new HashSet<>();
            Metapb.Partition partition = null;

            KVPair<Metapb.Partition, Metapb.Shard> partShard =
                    pdClient.getPartitionById(graphName, partitionId);
            if (partShard != null) {
                partition = partShard.getKey();
                Metapb.Shard leader = partShard.getValue();
                partitions.add(
                        HgNodePartition.of(leader.getStoreId(), (int) partition.getStartKey()));
            }
            builder.setPartitions(partitions);
        } catch (PDException e) {
            log.error("An error occurred while getting partition information :{}", e.getMessage());
            throw new RuntimeException(e.getMessage(), e);
        }
        return 0;
    }

    @Override
    public String partition(String graphName, byte[] startKey) throws PDException {
        var shard = pdClient.getPartition(graphName, startKey).getValue();
        return pdClient.getStore(shard.getStoreId()).getAddress();
    }

    @Override
    public String partition(String graphName, int code) throws PDException {
        var shard = pdClient.getPartitionByCode(graphName, code).getValue();
        return pdClient.getStore(shard.getStoreId()).getAddress();
    }

    /**
     * Query hgstore information
     *
     * @return hgstore
     */
    @Override
    public HgStoreNode apply(String graphName, Long nodeId) {
        try {
            Metapb.Store store = pdClient.getStore(nodeId);
            return nodeManager.getNodeBuilder().setNodeId(store.getId())
                              .setAddress(store.getAddress()).build();
        } catch (PDException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    /**
     * Notice to update cache
     */
    @Override
    public int notice(String graphName, HgStoreNotice storeNotice) {
        log.warn(storeNotice.toString());
        if (storeNotice.getPartitionLeaders() != null) {
            storeNotice.getPartitionLeaders().forEach((partId, leader) -> {
                pdClient.updatePartitionLeader(graphName, partId, leader);
                log.warn("updatePartitionLeader:{}-{}-{}",
                         graphName, partId, leader);
            });
        }
        if (storeNotice.getPartitionIds() != null) {
            storeNotice.getPartitionIds().forEach(partId -> {
                pdClient.invalidPartitionCache(graphName, partId);
            });
        }
        if (!storeNotice.getNodeStatus().equals(
                HgNodeStatus.PARTITION_COMMON_FAULT)
            && !storeNotice.getNodeStatus().equals(
                HgNodeStatus.NOT_PARTITION_LEADER)) {
            pdClient.invalidPartitionCache();
            log.warn("invalidPartitionCache:{} ", storeNotice.getNodeStatus());
        }
        return 0;
    }

    public Metapb.Graph delGraph(String graphName) {
        try {
            return pdClient.delGraph(graphName);
        } catch (PDException e) {
            log.error("delGraph {} exception, {}", graphName, e.getMessage());
        }
        return null;
    }

    public void setNodeManager(HgStoreNodeManager nodeManager) {
        this.nodeManager = nodeManager;
    }

    @Override
    public List<String> getStores(String graphName) throws PDException {
        var list = pdClient.getCache().getLeaderStoreAddresses();
        if (list.isEmpty()) {
            // Cache is being cleared
            return pdClient.getActiveStores(graphName).stream()
                           .map(Metapb.Store::getAddress)
                           .collect(Collectors.toList());
        }
        return list;
    }
}

