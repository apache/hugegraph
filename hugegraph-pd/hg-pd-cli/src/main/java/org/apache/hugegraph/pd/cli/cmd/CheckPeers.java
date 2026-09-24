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

package org.apache.hugegraph.pd.cli.cmd;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.StringUtils;

import com.alipay.sofa.jraft.entity.PeerId;

import org.apache.hugegraph.pd.client.MetaClient;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.pd.grpc.Metapb.Store;
import org.apache.hugegraph.pd.grpc.ShardGroups;
import org.apache.hugegraph.store.client.grpc.GrpcStoreStateClient;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CheckPeers extends Command {

    private MetaClient metaClient;

    public CheckPeers(String pd) {
        super(pd);
        metaClient = new MetaClient(config);
    }

    @Override
    public void action(String[] params) throws PDException {
        GrpcStoreStateClient stateClient = new GrpcStoreStateClient(config);
        try {
            ConcurrentHashMap<String, Map<String, String[]>> result = new ConcurrentHashMap<>();
            List<Store> stores = pdClient.getActiveStores();
            ShardGroups shardGroups = metaClient.getShardGroups();
            stores.parallelStream().forEach(s -> {
                for (Metapb.ShardGroup sg : shardGroups.getDataList()) {
                    String groupId = "hg_" + sg.getId();
                    PeerId leader = new PeerId();
                    result.computeIfAbsent(groupId, (key) -> new ConcurrentHashMap<>());
                    try {
                        String peers = stateClient.getPeers(s.getAddress(), sg.getId());
                        if (StringUtils.isEmpty(peers)) {
                            continue;
                        }
                        Map<String, String[]> nodePeers = result.get(groupId);
                        nodePeers.put(s.getRaftAddress(), peers.split(","));
                    } catch (Exception e) {
                        if (e.getMessage() != null &&
                            (e.getMessage().contains("Fail to get leader of group") ||
                             e.getMessage().contains("Fail to find node"))) {
                            continue;
                        }
                        log.error(String.format("got %s: %s with error:", groupId, leader), e);
                    }
                }
            });
            result.entrySet().parallelStream().forEach(entry -> {
                String[] record = null;
                String groupId = entry.getKey();
                Map<String, String[]> nodePeers = entry.getValue();
                for (Map.Entry<String, String[]> e : nodePeers.entrySet()) {
                    if (record == null) {
                        record = e.getValue();
                        continue;
                    }
                    if (!Arrays.equals(record, e.getValue())) {
                        log.error("group: {} ,got error peers: {}", groupId, nodePeers);
                        break;
                    }

                }
            });
            log.info("got all node info:{}", result);
        } catch (Exception e) {
            log.error("check peers with error:", e);
            throw e;
        } finally {
            stateClient.close();
        }
    }
}
