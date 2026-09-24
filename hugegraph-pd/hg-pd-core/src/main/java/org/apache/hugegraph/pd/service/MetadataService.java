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

package org.apache.hugegraph.pd.service;

import static org.apache.hugegraph.pd.grpc.Metapb.Graph;
import static org.apache.hugegraph.pd.grpc.Metapb.GraphSpace;
import static org.apache.hugegraph.pd.grpc.Metapb.Partition;
import static org.apache.hugegraph.pd.grpc.Metapb.ShardGroup;
import static org.apache.hugegraph.pd.grpc.Metapb.Store;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.config.PDConfig;
import org.apache.hugegraph.pd.consts.PoolNames;
import org.apache.hugegraph.pd.grpc.GraphSpaces;
import org.apache.hugegraph.pd.grpc.Graphs;
import org.apache.hugegraph.pd.grpc.Partitions;
import org.apache.hugegraph.pd.grpc.ShardGroups;
import org.apache.hugegraph.pd.grpc.Stores;
import org.apache.hugegraph.pd.meta.MetadataFactory;
import org.apache.hugegraph.pd.meta.MetadataKeyHelper;
import org.apache.hugegraph.pd.meta.MetadataRocksDBStore;
import org.apache.hugegraph.pd.meta.PartitionMeta;
import org.apache.hugegraph.pd.meta.StoreInfoMeta;
import org.apache.hugegraph.pd.util.ExecutorUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class MetadataService extends MetadataRocksDBStore {

    private static ThreadPoolExecutor uninterruptibleJobs;
    private static int cpus = Runtime.getRuntime().availableProcessors();
    private StoreInfoMeta store;
    private PartitionMeta partition;

    public MetadataService(@Autowired PDConfig config) {
        super(config);
        store = MetadataFactory.newStoreInfoMeta(config);
        partition = MetadataFactory.newPartitionMeta(config);
        try {
            if (uninterruptibleJobs == null) {
                PDConfig.JobConfig jobConfig = config.getJobConfig();
                int uninterruptibleCore = jobConfig.getUninterruptibleCore();
                if (uninterruptibleCore <= 0) {
                    uninterruptibleCore = cpus / 2;
                }
                uninterruptibleJobs = ExecutorUtil.createExecutor(PoolNames.U_JOB,
                                                                  uninterruptibleCore,
                                                                  jobConfig.getUninterruptibleMax(),
                                                                  jobConfig.getUninterruptibleQueueSize(),
                                                                  false);
            }
        } catch (Exception e) {
            log.error("an error occurred while creating the background job thread pool", e);
        }
    }

    public Stores getStores() throws PDException {
        Stores.Builder builder = Stores.newBuilder();
        try {
            List<Store> data = store.getStores("");
            builder.addAllData(data);
        } catch (Exception e) {
            log.error("failed to retrieve stores from metadata storage", e);
            throw e;
        }
        return builder.build();
    }

    public Partitions getPartitions() throws PDException {
        Partitions.Builder builder = Partitions.newBuilder();
        try {
            List<Partition> data = partition.getPartitions();
            builder.addAllData(data);
        } catch (Exception e) {
            log.error("failed to retrieve partitions from metadata storage", e);
            throw e;
        }
        return builder.build();
    }

    public ShardGroups getShardGroups() throws PDException {
        ShardGroups.Builder builder = ShardGroups.newBuilder();
        try {
            List<ShardGroup> data = store.getShardGroups();
            builder.addAllData(data);
        } catch (Exception e) {
            log.error("failed to retrieve shard groups from metadata storage", e);
            throw e;
        }
        return builder.build();
    }

    public GraphSpaces getGraphSpaces() throws PDException {
        GraphSpaces.Builder builder = GraphSpaces.newBuilder();
        try {
            byte[] prefix = MetadataKeyHelper.getGraphSpaceKey("");
            List<GraphSpace> data = scanPrefix(GraphSpace.parser(), prefix);
            builder.addAllData(data);
        } catch (Exception e) {
            log.error("failed to scan graph spaces", e);
            throw e;
        }
        return builder.build();
    }

    public Graphs getGraphs() throws PDException {
        Graphs.Builder builder = Graphs.newBuilder();
        try {
            List<Graph> data = partition.getGraphs();
            builder.addAllData(data);
        } catch (Exception e) {
            log.error("failed to retrieve graphs from metadata storage", e);
            throw e;
        }
        return builder.build();
    }

    public boolean updateStore(Store request) throws PDException {
        try {
            store.updateStore(request);
            return true;
        } catch (PDException e) {
            String name = request != null ? request.getId() + "@" + request.getAddress() : "null";
            log.error("failed to update store: {}", name, e);
            throw e;
        }
    }

    public boolean updatePartition(Partition request) throws PDException {
        try {
            partition.updatePartition(request);
            return true;
        } catch (Exception e) {
            String name = request != null ? request.getId() + "@" + request.getGraphName() : "null";
            log.error("failed to update partition: {}", name, e);
            throw e;
        }
    }

    public boolean updateShardGroup(ShardGroup request) throws PDException {
        try {
            store.updateShardGroup(request);
            return true;
        } catch (Exception e) {
            String name = request != null ? request.getId() + "@" + request.getState() : "null";
            log.error("failed to update shard group: {}", name, e);
            throw e;
        }
    }

    public boolean updateGraphSpace(GraphSpace request) throws PDException {
        try {
            byte[] key = MetadataKeyHelper.getGraphSpaceKey(request.getName());
            put(key, request.toByteArray());
            return true;
        } catch (Exception e) {
            String name = request != null ? request.getName() : "null";
            log.error("failed to update graph space: {}", name, e);
            throw e;
        }
    }

    public boolean updateGraph(Graph request) throws PDException {
        try {
            byte[] key = MetadataKeyHelper.getGraphKey(request.getGraphName());
            put(key, request.toByteArray());
            return true;
        } catch (Exception e) {
            String name = request != null ? request.getGraphName() : "null";
            log.error("failed to update graph: {}", name, e);
            throw e;
        }
    }

    public static ThreadPoolExecutor getUninterruptibleJobs() {
        return uninterruptibleJobs;
    }

}
