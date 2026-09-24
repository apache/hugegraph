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

package org.apache.hugegraph.store.meta;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

import org.apache.hugegraph.pd.grpc.Metapb;

import lombok.Data;

/**
 * Fragment replica group
 */
@Data
public class ShardGroup {

    private List<Shard> shards = new CopyOnWriteArrayList<>();
    private int id;
    /**
     * Leader term, leader switch increment = raftNode.leader_term
     * No practical use
     */
    private long version;
    /**
     * shards version number, incremented after each change
     */
    private long confVersion;

    public static ShardGroup from(Metapb.ShardGroup meta) {
        if (meta == null) {
            return null;
        }
        ShardGroup shardGroup = new ShardGroup();
        shardGroup.setId(meta.getId());
        shardGroup.setVersion(meta.getVersion());
        shardGroup.setConfVersion(meta.getConfVer());
        shardGroup.setShards(new CopyOnWriteArrayList<>(
                meta.getShardsList().stream().map(Shard::fromMetaPbShard)
                    .collect(Collectors.toList())));
        return shardGroup;
    }

    public ShardGroup addShard(Shard shard) {
        this.shards.add(shard);
        return this;
    }

    public synchronized ShardGroup changeLeader(long storeId) {
        shards.forEach(shard -> {
            shard.setRole(shard.getStoreId() == storeId ? Metapb.ShardRole.Leader :
                          Metapb.ShardRole.Follower);
        });
        return this;
    }

    public synchronized ShardGroup changeShardList(List<Long> peerIds, List<Long> learners,
                                                   long leaderId) {
        if (!peerIds.isEmpty()) {
            shards.clear();
            peerIds.forEach(id -> {
                shards.add(new Shard() {{
                    setStoreId(id);
                    setRole(id == leaderId ? Metapb.ShardRole.Leader : Metapb.ShardRole.Follower);
                }});
            });

            learners.forEach(id -> {
                shards.add(new Shard() {{
                    setStoreId(id);
                    setRole(Metapb.ShardRole.Learner);
                }});
            });
            confVersion = confVersion + 1;
        }
        return this;
    }

    public synchronized List<Metapb.Shard> getMetaPbShard() {
        List<Metapb.Shard> shardList = new ArrayList<>();
        shards.forEach(shard -> {
            shardList.add(shard.toMetaPbShard());
        });
        return shardList;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        shards.forEach(e -> {
            builder.append(String.format("{ id:%s,role:%s },", e.getStoreId(), e.getRole()));
        });
        return builder.length() > 0 ? builder.substring(0, builder.length() - 1) : "";
    }

    public Metapb.ShardGroup getProtoObj() {
        return Metapb.ShardGroup.newBuilder()
                                .setId(this.id)
                                .setVersion(this.version)
                                .setConfVer(this.confVersion)
                                .addAllShards(getMetaPbShard())
                                .build();
    }
}
