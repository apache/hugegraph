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

package org.apache.hugegraph.store.pd;

import java.util.function.Consumer;

import org.apache.hugegraph.pd.grpc.pulse.ChangeShard;
import org.apache.hugegraph.pd.grpc.pulse.CleanPartition;
import org.apache.hugegraph.pd.grpc.pulse.DbCompaction;
import org.apache.hugegraph.pd.grpc.pulse.MovePartition;
import org.apache.hugegraph.pd.grpc.pulse.PartitionKeyRange;
import org.apache.hugegraph.pd.grpc.pulse.SplitPartition;
import org.apache.hugegraph.pd.grpc.pulse.TransferLeader;
import org.apache.hugegraph.store.meta.Partition;

@Deprecated
public interface PartitionInstructionListener {

    void onChangeShard(long taskId, Partition partition, ChangeShard changeShard,
                       Consumer<Integer> consumer);

    void onTransferLeader(long taskId, Partition partition, TransferLeader transferLeader,
                          Consumer<Integer> consumer);

    void onSplitPartition(long taskId, Partition partition, SplitPartition splitPartition,
                          Consumer<Integer> consumer);

    void onDbCompaction(long taskId, Partition partition, DbCompaction rocksdbCompaction,
                        Consumer<Integer> consumer);

    void onMovePartition(long taskId, Partition partition, MovePartition movePartition,
                         Consumer<Integer> consumer);

    void onCleanPartition(long taskId, Partition partition, CleanPartition cleanPartition,
                          Consumer<Integer> consumer);

    void onPartitionKeyRangeChanged(long taskId, Partition partition,
                                    PartitionKeyRange partitionKeyRange,
                                    Consumer<Integer> consumer);
}
