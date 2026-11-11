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

package org.apache.hugegraph.pd.meta;

import java.util.List;
import java.util.Optional;

import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.config.PDConfig;
import org.apache.hugegraph.pd.grpc.Metapb;

public class ConfigMetaStore extends MetadataRocksDBStore {

    private final long clusterId;

    public ConfigMetaStore(PDConfig pdConfig) {
        super(pdConfig);
        this.clusterId = pdConfig.getClusterId();
    }

    /**
     * Update the storage status of the graph space
     *
     * @param
     */
    public Metapb.GraphSpace setGraphSpace(Metapb.GraphSpace graphSpace) throws PDException {
        byte[] graphSpaceKey = MetadataKeyHelper.getGraphSpaceKey(graphSpace.getName());
        graphSpace = graphSpace.toBuilder().setTimestamp(System.currentTimeMillis()).build();
        put(graphSpaceKey, graphSpace.toByteArray());
        return graphSpace;
    }

    public List<Metapb.GraphSpace> getGraphSpace(String graphSpace) throws PDException {
        byte[] graphSpaceKey = MetadataKeyHelper.getGraphSpaceKey(graphSpace);
        return scanPrefix(Metapb.GraphSpace.parser(), graphSpaceKey);
    }

    /**
     * Read one space without prefix-matching similarly named spaces.
     */
    public Metapb.GraphSpace getGraphSpaceExact(String name) throws PDException {
        return getOne(Metapb.GraphSpace.parser(), MetadataKeyHelper.getGraphSpaceKey(name));
    }

    public void deleteGraphSpace(String name) throws PDException {
        remove(MetadataKeyHelper.getGraphSpaceKey(name));
    }

    public Metapb.PDConfig setPdConfig(Metapb.PDConfig pdConfig) throws PDException {
        byte[] graphSpaceKey =
                MetadataKeyHelper.getPdConfigKey(String.valueOf(pdConfig.getVersion()));
        Metapb.PDConfig config = Metapb.PDConfig.newBuilder(
                pdConfig).setTimestamp(System.currentTimeMillis()).build();
        put(graphSpaceKey, config.toByteArray());
        return config;
    }

    public Metapb.PDConfig getPdConfig(long version) throws PDException {
        byte[] graphSpaceKey = MetadataKeyHelper.getPdConfigKey(version <= 0 ? null :
                                                                String.valueOf(version));
        Optional<Metapb.PDConfig> max = scanPrefix(
                Metapb.PDConfig.parser(), graphSpaceKey).stream().max(
                (o1, o2) -> (o1.getVersion() > o2.getVersion()) ? 1 : -1);
        return max.isPresent() ? max.get() : null;
    }

}
