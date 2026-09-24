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

import lombok.Data;

import org.apache.hugegraph.pd.grpc.Metapb;

@Data
class Shard {

    long partitionId;
    long storeId;
    String state;
    String role;
    int progress;

    public Shard(Metapb.ShardStats shardStats, long partitionId) {
        this.role = String.valueOf(shardStats.getRole());
        this.storeId = shardStats.getStoreId();
        this.state = String.valueOf(shardStats.getState());
        this.partitionId = partitionId;
        this.progress = shardStats.getProgress();
    }

    public Shard(Metapb.Shard shard, long partitionId) {
        this.role = String.valueOf(shard.getRole());
        this.storeId = shard.getStoreId();
        this.state = Metapb.ShardState.SState_Normal.name();
        this.progress = 0;
        this.partitionId = partitionId;
    }
}
