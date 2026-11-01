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

import org.apache.hugegraph.pd.grpc.Metapb;

public enum PartitionRole {
    UNKNOWN(0, "unknown"),
    LEADER(1, "leader"),
    FOLLOWER(2, "follower"),
    LEARNER(3, "learner"),
    CANDIDATE(4, "candidate");
    private final int role;
    private final String name;

    PartitionRole(int role, String name) {
        this.role = role;
        this.name = name;
    }

    public static PartitionRole fromShardRole(Metapb.ShardRole shard) {
        return switch (shard) {
            case Leader -> PartitionRole.LEADER;
            case Follower -> PartitionRole.FOLLOWER;
            case Learner -> PartitionRole.LEARNER;
            default -> PartitionRole.FOLLOWER;
        };

    }

    @Override
    public String toString() {
        return this.ordinal() + "_" + this.name;
    }

    public String getName() {
        return this.name;
    }

    public Metapb.ShardRole toShardRole() {
        return switch (this) {
            case LEADER -> Metapb.ShardRole.Leader;
            case FOLLOWER -> Metapb.ShardRole.Follower;
            case LEARNER -> Metapb.ShardRole.Learner;
            default -> Metapb.ShardRole.None;
        };
    }
}
