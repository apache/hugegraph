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

package org.apache.hugegraph.store.core.store.meta;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;

import java.util.List;

import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.store.meta.Partition;
import org.apache.hugegraph.store.meta.Shard;
import org.apache.hugegraph.store.meta.ShardGroup;
import org.junit.Test;

/**
 * Partition and shard-group metadata is persisted and exchanged with PD as protobuf, so the
 * conversion in both directions must be lossless for the fields the store keeps.
 */
public class PartitionMetaCodecTest {

    private static Metapb.Partition proto(Metapb.PartitionState state) {
        return Metapb.Partition.newBuilder()
                               .setId(7)
                               .setGraphName("hugegraph/g")
                               .setStartKey(0x1000)
                               .setEndKey(0x2000)
                               .setVersion(42)
                               .setState(state)
                               .build();
    }

    private static Metapb.Shard shard(long storeId, Metapb.ShardRole role) {
        return Metapb.Shard.newBuilder().setStoreId(storeId).setRole(role).build();
    }

    @Test
    public void testPartitionProtoRoundTrip() {
        for (Metapb.PartitionState state : new Metapb.PartitionState[]{
                Metapb.PartitionState.PState_Normal, Metapb.PartitionState.PState_Warn,
                Metapb.PartitionState.PState_Offline}) {
            Metapb.Partition original = proto(state);
            Partition partition = new Partition(original);

            assertEquals(7, partition.getId());
            assertEquals("hugegraph/g", partition.getGraphName());
            assertEquals(0x1000, partition.getStartKey());
            assertEquals(0x2000, partition.getEndKey());
            assertEquals(42, partition.getVersion());
            assertEquals(state, partition.getWorkState());
            assertEquals(original, partition.getProtoObj());
        }
    }

    @Test
    public void testMissingOrUnknownPartitionStateFallsBackToNormal() {
        Partition none = new Partition(proto(Metapb.PartitionState.PState_None));
        assertEquals(Metapb.PartitionState.PState_Normal, none.getWorkState());

        Metapb.Partition future = proto(Metapb.PartitionState.PState_Normal).toBuilder()
                                                                            .setStateValue(99)
                                                                            .build();
        assertEquals(Metapb.PartitionState.UNRECOGNIZED, future.getState());
        assertEquals(Metapb.PartitionState.PState_Normal, new Partition(future).getWorkState());

        Partition fromEmpty = new Partition(Metapb.Partition.getDefaultInstance());
        assertEquals(Metapb.PartitionState.PState_Normal, fromEmpty.getWorkState());
        assertEquals("", fromEmpty.getGraphName());
        assertEquals(Metapb.PartitionState.PState_Normal, new Partition().getWorkState());
    }

    @Test
    public void testPartitionCloneIsIndependent() {
        Partition partition = new Partition(proto(Metapb.PartitionState.PState_Normal));
        Partition copy = partition.clone();

        assertNotSame(partition, copy);
        assertEquals(partition, copy);
        copy.setEndKey(0x3000);
        copy.setWorkState(Metapb.PartitionState.PState_Offline);
        assertEquals(0x2000, partition.getEndKey());
        assertEquals(Metapb.PartitionState.PState_Normal, partition.getWorkState());
    }

    @Test
    public void testShardGroupProtoRoundTrip() {
        Metapb.ShardGroup original = Metapb.ShardGroup.newBuilder()
                                                      .setId(3)
                                                      .setVersion(5)
                                                      .setConfVer(9)
                                                      .addShards(shard(11, Metapb.ShardRole.Leader))
                                                      .addShards(shard(12, Metapb.ShardRole.Follower))
                                                      .addShards(shard(13, Metapb.ShardRole.Learner))
                                                      .build();
        ShardGroup group = ShardGroup.from(original);

        assertEquals(3, group.getId());
        assertEquals(5, group.getVersion());
        assertEquals(9, group.getConfVersion());
        assertEquals(3, group.getShards().size());
        assertEquals(original, group.getProtoObj());
        assertEquals("{ id:11,role:Leader },{ id:12,role:Follower },{ id:13,role:Learner }",
                     group.toString());

        assertNull(ShardGroup.from(null));
        assertEquals("", new ShardGroup().toString());
    }

    @Test
    public void testChangeShardListAssignsRolesAndBumpsConfVersion() {
        ShardGroup group = ShardGroup.from(Metapb.ShardGroup.newBuilder()
                                                            .setId(1)
                                                            .setConfVer(4)
                                                            .addShards(shard(1, Metapb.ShardRole.Leader))
                                                            .build());

        group.changeShardList(List.of(21L, 22L), List.of(23L), 22L);

        assertEquals(5, group.getConfVersion());
        assertEquals(List.of(shard(21, Metapb.ShardRole.Follower),
                             shard(22, Metapb.ShardRole.Leader),
                             shard(23, Metapb.ShardRole.Learner)),
                     group.getMetaPbShard());
    }

    @Test
    public void testEmptyPeerListLeavesTheGroupUnchanged() {
        ShardGroup group = new ShardGroup();
        group.setConfVersion(4);
        Shard leader = new Shard();
        leader.setStoreId(1);
        leader.setRole(Metapb.ShardRole.Leader);
        group.addShard(leader);

        // Learners alone must not wipe the voters
        group.changeShardList(List.of(), List.of(9L), 9L);

        assertEquals(4, group.getConfVersion());
        assertEquals(List.of(shard(1, Metapb.ShardRole.Leader)), group.getMetaPbShard());
    }

    @Test
    public void testChangeLeaderDemotesEveryOtherShard() {
        ShardGroup group = ShardGroup.from(Metapb.ShardGroup.newBuilder()
                                                            .addShards(shard(1, Metapb.ShardRole.Leader))
                                                            .addShards(shard(2, Metapb.ShardRole.Follower))
                                                            .addShards(shard(3, Metapb.ShardRole.Follower))
                                                            .build());
        group.changeLeader(3);
        assertEquals(List.of(shard(1, Metapb.ShardRole.Follower),
                             shard(2, Metapb.ShardRole.Follower),
                             shard(3, Metapb.ShardRole.Leader)),
                     group.getMetaPbShard());
    }
}
