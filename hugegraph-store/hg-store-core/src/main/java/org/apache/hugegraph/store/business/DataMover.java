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

package org.apache.hugegraph.store.business;

import java.util.List;

import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.store.cmd.HgCmdClient;
import org.apache.hugegraph.store.cmd.request.BatchPutRequest;
import org.apache.hugegraph.store.cmd.request.CleanDataRequest;
import org.apache.hugegraph.store.cmd.response.UpdatePartitionResponse;

import com.alipay.sofa.jraft.Status;

/**
 * Data transfer interface, implementing partition splitting and merging, supporting
 * cross-machine data transfer.
 */
@Deprecated
public interface DataMover {

    void setBusinessHandler(BusinessHandler handler);

    void setCmdClient(HgCmdClient client);

    /**
     * Copy the data within the partition source to other partitions targets
     * One partition, migrating to multiple partitions
     *
     * @param source  source partition
     * @param targets target partitions
     * @return execution status
     * @throws Exception execution exception
     */
    Status moveData(Metapb.Partition source, List<Metapb.Partition> targets) throws Exception;

    /**
     * Copy all data from source to target.
     * Migrate from one partition to another partition
     *
     * @param source source partition
     * @param target target partition
     * @return execution result
     * @throws Exception execution exception
     */
    Status moveData(Metapb.Partition source, Metapb.Partition target) throws Exception;

    // Synchronize the partition state between replicas
    UpdatePartitionResponse updatePartitionState(Metapb.Partition partition,
                                                 Metapb.PartitionState state);

    // Synchronization of the range of partitions between replicas
    UpdatePartitionResponse updatePartitionRange(Metapb.Partition partition, int startKey,
                                                 int endKey);

    // Clean up invalid data within the partitionpartition
    void cleanData(Metapb.Partition partition);

    // Write data
    void doWriteData(BatchPutRequest request);

    void doCleanData(CleanDataRequest request);
}
