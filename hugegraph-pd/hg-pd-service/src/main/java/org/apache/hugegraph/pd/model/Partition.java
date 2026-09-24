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
import java.util.List;

import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.pd.service.PDService;

import lombok.AccessLevel;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Data
class Partition {

    int partitionId;
    String graphName;
    String workState;
    long startKey;
    long endKey;
    List<Shard> shards;
    long dataSize;
    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    private transient PDService pdService;

    public Partition(Metapb.Partition pt, Metapb.PartitionStats stats, PDService service) {
        this.pdService = service;
        if (pt != null) {
            partitionId = pt.getId();
            startKey = pt.getStartKey();
            endKey = pt.getEndKey();
            workState = String.valueOf(pt.getState());
            graphName = pt.getGraphName();
            final int postfixLength = 2;
            if (graphName != null && graphName.length() > postfixLength) {
                graphName = graphName.substring(0, graphName.length() - postfixLength);
            } else {
                log.error("Partition graphName '{}' too short to trim postfixLength={}", graphName,
                         postfixLength);
            }
            graphName = graphName.substring(0, graphName.length() - postfixLength);
            if (stats != null) {
                List<Metapb.ShardStats> shardStatsList = stats.getShardStatsList();
                List<Shard> shardsList = new ArrayList<>();
                for (Metapb.ShardStats shardStats : shardStatsList) {
                    Shard shard = new Shard(shardStats, partitionId);
                    shardsList.add(shard);
                }
                this.shards = shardsList;
            } else {
                List<Shard> shardsList = new ArrayList<>();
                try {

                    var shardGroup = pdService.getStoreNodeService().getShardGroup(pt.getId());
                    if (shardGroup != null) {
                        for (Metapb.Shard shard1 : shardGroup.getShardsList()) {
                            shardsList.add(new Shard(shard1, partitionId));
                        }
                    } else {
                        log.error("GraphAPI.Partition(), get shard group: {} returns null",
                                  pt.getId());
                    }
                } catch (PDException e) {
                    log.error("Partition init failed, error: {}", e.getMessage());
                }
                this.shards = shardsList;
            }

        }
    }
}
