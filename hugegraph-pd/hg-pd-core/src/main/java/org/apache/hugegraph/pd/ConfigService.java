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

import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.config.PDConfig;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.pd.meta.ConfigMetaStore;
import org.apache.hugegraph.pd.meta.MetadataFactory;
import org.apache.hugegraph.pd.raft.RaftStateListener;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ConfigService implements RaftStateListener {

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
        return this.meta.setGraphSpace(graphSpace.toBuilder()
                                                 .setTimestamp(System.currentTimeMillis())
                                                 .build());
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
