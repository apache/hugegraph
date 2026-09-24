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

package org.apache.hugegraph.pd.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import lombok.extern.slf4j.Slf4j;

import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.pd.service.PDRestService;
import org.apache.hugegraph.pd.service.PDService;

import lombok.AccessLevel;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;

@Data
@Slf4j
public class GraphStatistics {

    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    private transient PDRestService pdRestService;
    // graph statistics
    String graphName;
    long partitionCount;
    String state;
    List<Partition> partitions;
    long dataSize;
    int nodeCount;
    int edgeCount;
    long keyCount;

    public GraphStatistics(Metapb.Graph graph, PDRestService restService,
                           PDService pdService) throws PDException {
        this.pdRestService = restService;
        if (graph == null) {
            return;
        }
        Map<Integer, Long> partition2DataSize = new HashMap<>();
        graphName = graph.getGraphName();
        partitionCount = graph.getPartitionCount();
        state = String.valueOf(graph.getState());
        // data volume and number of keys
        List<Metapb.Store> stores = pdRestService.getStores(graphName);
        for (Metapb.Store store : stores) {
            List<Metapb.GraphStats> graphStatsList = store.getStats().getGraphStatsList();
            for (Metapb.GraphStats graphStats : graphStatsList) {
                if ((graphName.equals(graphStats.getGraphName()))
                    && (Metapb.ShardRole.Leader.equals(graphStats.getRole()))) {
                    keyCount += graphStats.getApproximateKeys();
                    dataSize += graphStats.getApproximateSize();
                    partition2DataSize.put(graphStats.getPartitionId(),
                                           graphStats.getApproximateSize());
                }
            }
        }
        List<Partition> resultPartitionList = new ArrayList<>();
        List<Metapb.Partition> tmpPartitions = pdRestService.getPartitions(graphName);
        if ((tmpPartitions != null) && (!tmpPartitions.isEmpty())) {
            // partition information to be returned
            for (Metapb.Partition partition : tmpPartitions) {
                Metapb.PartitionStats partitionStats =
                        pdRestService.getPartitionStats(graphName, partition.getId());
                Partition pt = new Partition(partition, partitionStats, pdService);
                pt.dataSize = partition2DataSize.getOrDefault(partition.getId(), 0L);
                resultPartitionList.add(pt);
            }
        }
        partitions = resultPartitionList;
        // remove the /g /m /s behind the graph name
        if (graphName != null && graphName.length() >= 2) {
            String suf = graphName.substring(graphName.length() - 2);
            if ("/g".equals(suf) || "/m".equals(suf) || "/s".equals(suf)) {
                graphName = graphName.substring(0, graphName.length() - 2);
            } else {
                log.error("invalid graph name in GraphStatistics: {}", graphName);
            }
        } else {
            log.error("invalid graph name in GraphStatistics: {}", graphName);
        }
    }
}
