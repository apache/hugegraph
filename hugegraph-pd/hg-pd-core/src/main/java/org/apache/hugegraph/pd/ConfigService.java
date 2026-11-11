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

package org.apache.hugegraph.pd;

import java.util.List;
import java.util.stream.Stream;

import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.config.PDConfig;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.pd.grpc.Pdpb;
import org.apache.hugegraph.pd.meta.ConfigMetaStore;
import org.apache.hugegraph.pd.meta.MetadataFactory;
import org.apache.hugegraph.pd.raft.RaftStateListener;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ConfigService implements RaftStateListener {

    // PD service facades share one metadata store, but have separate ConfigService instances.
    private static final Object GRAPH_SPACE_LOCK = new Object();
    private static final int GRAPH_SPACE_DIAGNOSTIC_NAMES = 10;
    private static final int GRAPH_SPACE_DIAGNOSTIC_NAME_LENGTH = 128;

    private ConfigMetaStore meta;
    private PDConfig pdConfig;

    public ConfigService(PDConfig config) {
        this.pdConfig = config;
        config.setConfigService(this);
        meta = MetadataFactory.newConfigMeta(config);
    }

    public Metapb.PDConfig getPDConfig(long version) throws PDException {
        return this.meta.getPdConfig(version);
    }

    public Metapb.PDConfig getPDConfig() throws PDException {
        return this.meta.getPdConfig(0);
    }

    public Metapb.PDConfig setPDConfig(Metapb.PDConfig mConfig) throws PDException {
        Metapb.PDConfig oldCfg = getPDConfig();
        Metapb.PDConfig.Builder builder = oldCfg.toBuilder().mergeFrom(mConfig)
                                                .setVersion(oldCfg.getVersion() + 1)
                                                .setTimestamp(System.currentTimeMillis());
        mConfig = this.meta.setPdConfig(builder.build());
        log.info("PDConfig has been modified, new PDConfig is {}", mConfig);
        updatePDConfig(mConfig);
        return mConfig;
    }

    public List<Metapb.GraphSpace> getGraphSpace(String graphSpaceName) throws PDException {
        return this.meta.getGraphSpace(graphSpaceName);
    }

    public Metapb.GraphSpace setGraphSpace(Metapb.GraphSpace graphSpace) throws PDException {
        synchronized (GRAPH_SPACE_LOCK) {
            return this.meta.setGraphSpace(graphSpace.toBuilder()
                                                     .setTimestamp(System.currentTimeMillis())
                                                     .build());
        }
    }

    /**
     * Update a GB limit while preserving the latest usage, including updates from other facades.
     */
    public Metapb.GraphSpace setGraphSpaceLimit(Metapb.GraphSpace graphSpace) throws PDException {
        synchronized (GRAPH_SPACE_LOCK) {
            long usedSize = this.graphSpaceBuilder(graphSpace.getName()).getUsedSize();
            return this.setGraphSpace(graphSpace.toBuilder().setUsedSize(usedSize).build());
        }
    }

    /**
     * Refresh usage in KB without overwriting a concurrent limit update.
     */
    public Metapb.GraphSpace setGraphSpaceUsage(String name, long usedSize) throws PDException {
        synchronized (GRAPH_SPACE_LOCK) {
            return this.setGraphSpace(this.graphSpaceBuilder(name).setUsedSize(usedSize).build());
        }
    }

    /**
     * Recheck current PD graphs before recreating missing usage. A quota collection may
     * have started before the Server removed the graphs and their space's statistics.
     */
    public Metapb.GraphSpace refreshGraphSpaceUsage(String name, long usedSize,
                                                   PartitionService partitions) throws PDException {
        synchronized (GRAPH_SPACE_LOCK) {
            if (this.meta.getGraphSpaceExact(name) == null && !hasGraphs(name, partitions)) {
                return null;
            }
            return this.setGraphSpaceUsage(name, usedSize);
        }
    }

    public void deleteGraphSpace(String name, PartitionService partitions) throws PDException {
        if (name == null || name.isEmpty() || name.indexOf('/') >= 0) {
            throw new PDException(Pdpb.ErrorType.UNKNOWN_VALUE, "Invalid graph space name");
        }
        synchronized (GRAPH_SPACE_LOCK) {
            List<String> names = graphNames(name, partitions).toList();
            if (!names.isEmpty()) {
                List<String> sample = names.stream().limit(GRAPH_SPACE_DIAGNOSTIC_NAMES)
                                           .map(ConfigService::diagnosticGraphName).toList();
                throw new PDException(Pdpb.ErrorType.UNKNOWN_VALUE,
                                      String.format("PD still has %s graph record(s) in space '%s': %s " +
                                                    "(showing up to %s names, %s characters each). " +
                                                    "Inspect GET /v1/partitionsAndStats and compare " +
                                                    "Server graph configuration before retrying " +
                                                    "space deletion.",
                                                    names.size(), name, sample, GRAPH_SPACE_DIAGNOSTIC_NAMES,
                                                    GRAPH_SPACE_DIAGNOSTIC_NAME_LENGTH));
            }
            this.meta.deleteGraphSpace(name);
        }
    }

    private static boolean hasGraphs(String name, PartitionService partitions) throws PDException {
        return graphNames(name, partitions).findAny().isPresent();
    }

    private static Stream<String> graphNames(String name, PartitionService partitions) throws PDException {
        String prefix = name + "/";
        return partitions.getGraphs().stream().map(Metapb.Graph::getGraphName)
                         .filter(graphName -> graphName.startsWith(prefix));
    }

    private static String diagnosticGraphName(String name) {
        int end = Math.min(name.length(), GRAPH_SPACE_DIAGNOSTIC_NAME_LENGTH);
        if (end < name.length() && Character.isHighSurrogate(name.charAt(end - 1))) {
            end--;
        }
        return name.substring(0, end).replaceAll("\\p{Cntrl}", "?") + (end < name.length() ? "..." : "");
    }

    private Metapb.GraphSpace.Builder graphSpaceBuilder(String name) throws PDException {
        Metapb.GraphSpace current = this.meta.getGraphSpaceExact(name);
        return current == null ? Metapb.GraphSpace.newBuilder().setName(name) : current.toBuilder();
    }

    /**
     * Read the configuration item from the storage and overwrite the global PD Config object
     *
     * @return
     */
    public PDConfig loadConfig() {
        try {
            Metapb.PDConfig mConfig = this.meta.getPdConfig(0);
            if (mConfig == null) {
                mConfig = Metapb.PDConfig.newBuilder()
                                         .setPartitionCount(pdConfig.getInitialPartitionCount())
                                         .setShardCount(pdConfig.getPartition().getShardCount())
                                         .setMinStoreCount(Math.max(0, pdConfig.getMinStoreCount()))
                                         .setVersion(1)
                                         .setTimestamp(System.currentTimeMillis())
                                         .setMaxShardsPerStore(
                                                 pdConfig.getPartition().getMaxShardsPerStore())
                                         .build();
                this.meta.setPdConfig(mConfig);
            }
            pdConfig = updatePDConfig(mConfig);
        } catch (Exception e) {
            log.error("ConfigService loadConfig exception {}", e);
        }
        return pdConfig;
    }

    public synchronized PDConfig updatePDConfig(Metapb.PDConfig mConfig) {
        log.info("update pd config: mConfig:{}", mConfig);
        pdConfig.getPartition().setShardCount(mConfig.getShardCount());
        pdConfig.getPartition().setTotalCount(mConfig.getPartitionCount());
        pdConfig.getPartition().setMaxShardsPerStore(mConfig.getMaxShardsPerStore());
        pdConfig.setMinStoreCount(Math.max(0, mConfig.getMinStoreCount()));
        return pdConfig;
    }

    public synchronized PDConfig setPartitionCount(int count) {
        Metapb.PDConfig mConfig = null;
        try {
            mConfig = getPDConfig();
            mConfig = mConfig.toBuilder().setPartitionCount(count).build();
            setPDConfig(mConfig);
        } catch (PDException e) {
            log.error("ConfigService exception {}", e);
            e.printStackTrace();
        }
        return pdConfig;
    }

    /**
     * Meta store
     * The original partition count is not recommended due to the fact that it may be affected by
     * partition splitting/merging
     *
     * @return partition count of cluster
     * @throws PDException when io error
     */
    public int getPartitionCount() throws PDException {
        return getPDConfig().getPartitionCount();
    }

    @Override
    public void onRaftLeaderChanged() {
        loadConfig();
    }
}
