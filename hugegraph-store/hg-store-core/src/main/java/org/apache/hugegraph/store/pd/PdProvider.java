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

import java.util.List;
import java.util.function.Consumer;

import org.apache.hugegraph.pd.client.PDClient;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.grpc.MetaTask;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.pd.grpc.Metapb.PartitionStats;
import org.apache.hugegraph.store.meta.GraphManager;
import org.apache.hugegraph.store.meta.Partition;
import org.apache.hugegraph.store.meta.Store;
import org.apache.hugegraph.store.processor.Processors;
import org.apache.hugegraph.store.util.HgStoreException;

public interface PdProvider {

    long registerStore(Store store) throws PDException;

    Store getStoreByID(Long storeId);

    Metapb.ClusterStats getClusterStats();

    Metapb.ClusterStats storeHeartbeat(Store node) throws HgStoreException, PDException;

    Partition getPartitionByID(String graph, int partId);

    Metapb.Shard getPartitionLeader(String graph, int partId);

    Metapb.Partition getPartitionByCode(String graph, int code);

    Partition delPartition(String graph, int partId);

    List<Metapb.Partition> updatePartition(List<Metapb.Partition> partitions) throws PDException;

    List<Partition> getPartitionsByStore(long storeId) throws PDException;

    void updatePartitionCache(Partition partition, Boolean changeLeader);

    void invalidPartitionCache(String graph, int partId);

    boolean startHeartbeatStream(Consumer<Throwable> onError);

    boolean setCommandProcessors(Processors processors);

    boolean partitionHeartbeat(List<PartitionStats> statsList);

    boolean partitionHeartbeat(PartitionStats stats);

    boolean isLocalPartition(long storeId, int partitionId);

    Metapb.Graph getGraph(String graphName) throws PDException;

    void reportTask(MetaTask.Task task) throws PDException;

    PDClient getPDClient();

    boolean updatePartitionLeader(String graphName, int partId, long leaderStoreId);

    GraphManager getGraphManager();

    void setGraphManager(GraphManager graphManager);

    /**
     * Delete partition shard group
     *
     * @param groupId
     */
    void deleteShardGroup(int groupId) throws PDException;

    default Metapb.ShardGroup getShardGroup(int partitionId) {
        return null;
    }

    default Metapb.ShardGroup getShardGroupDirect(int partitionId) {
        return null;
    }

    default void updateShardGroup(Metapb.ShardGroup shardGroup) throws PDException {
    }

    String getPdServerAddress();

    default void resetPulseClient() {
    }
}
