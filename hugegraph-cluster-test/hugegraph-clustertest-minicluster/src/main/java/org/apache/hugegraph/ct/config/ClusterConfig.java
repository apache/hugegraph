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

package org.apache.hugegraph.ct.config;

import java.util.ArrayList;
import java.util.List;

import org.apache.hugegraph.ct.base.HGTestLogger;
import org.slf4j.Logger;

public class ClusterConfig {

    protected static final Logger LOG = HGTestLogger.CONFIG_LOG;
    protected List<PDConfig> pdConfigs;
    protected List<StoreConfig> storeConfigs;
    protected List<ServerConfig> serverConfigs;
    protected List<GraphConfig> graphConfigs;

    protected List<String> pdGrpcList, pdRaftList, storeGrpcList;

    public ClusterConfig(int pdCnt, int storeCnt, int serverCnt) {
        pdConfigs = new ArrayList<>();
        storeConfigs = new ArrayList<>();
        serverConfigs = new ArrayList<>();
        graphConfigs = new ArrayList<>();
        pdGrpcList = new ArrayList<>();
        pdRaftList = new ArrayList<>();
        storeGrpcList = new ArrayList<>();

        for (int i = 0; i < pdCnt; i++) {
            PDConfig pdConfig = new PDConfig();
            pdConfig.setStoreCount(storeCnt);
            pdConfigs.add(pdConfig);
            pdGrpcList.add(pdConfig.getGrpcAddress());
            pdRaftList.add(pdConfig.getRaftAddress());
        }

        for (int i = 0; i < storeCnt; i++) {
            StoreConfig storeConfig = new StoreConfig();
            storeConfig.setPDServerList(pdGrpcList);
            storeConfigs.add(storeConfig);
            storeGrpcList.add(storeConfig.getGrpcAddress());
        }

        for (int i = 0; i < serverCnt; i++) {
            ServerConfig serverConfig = new ServerConfig();
            serverConfig.setPDPeersList(pdGrpcList);
            serverConfigs.add(serverConfig);
            GraphConfig graphConfig = new GraphConfig();
            graphConfig.setPDPeersList(pdGrpcList);
            graphConfigs.add(graphConfig);
        }

        for (int i = 0; i < pdCnt; i++) {
            PDConfig pdConfig = pdConfigs.get(i);
            pdConfig.setRaftPeerList(pdRaftList);
            pdConfig.setStoreGrpcList(storeGrpcList);
        }
    }

    public PDConfig getPDConfig(int i) {
        return pdConfigs.get(i);
    }

    public StoreConfig getStoreConfig(int i) {
        return storeConfigs.get(i);
    }

    public ServerConfig getServerConfig(int i) {
        return serverConfigs.get(i);
    }

    public GraphConfig getGraphConfig(int i) {
        return graphConfigs.get(i);
    }

    public List<String> getPDRestAddrs() {
        List<String> addrs = new ArrayList<>();
        for (PDConfig pdConfig : pdConfigs) {
            addrs.add(pdConfig.getRaftAddress());
        }
        return addrs;
    }

    public List<String> getPDGrpcAddrs() {
        List<String> addrs = new ArrayList<>();
        for (PDConfig pdConfig : pdConfigs) {
            addrs.add(pdConfig.getGrpcAddress());
        }
        return addrs;
    }

    public List<String> getStoreRestAddrs() {
        List<String> addrs = new ArrayList<>();
        for (StoreConfig storeConfig : storeConfigs) {
            addrs.add("127.0.0.1" + ":" + storeConfig.getRestPort());
        }
        return addrs;
    }

    public List<String> getStoreGrpcAddrs() {
        List<String> addrs = new ArrayList<>();
        for (StoreConfig storeConfig : storeConfigs) {
            addrs.add("127.0.0.1" + ":" + storeConfig.getGrpcPort());
        }
        return addrs;
    }

    public List<String> getServerRestAddrs() {
        List<String> addrs = new ArrayList<>();
        for (ServerConfig serverConfig : serverConfigs) {
            addrs.add("127.0.0.1" + ":" + serverConfig.getRestPort());
        }
        return addrs;
    }
}
