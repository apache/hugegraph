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

package org.apache.hugegraph.ct.env;

import static org.apache.hugegraph.ct.base.ClusterConstant.CONF_DIR;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.hugegraph.ct.base.HGTestLogger;
import org.apache.hugegraph.ct.config.ClusterConfig;
import org.apache.hugegraph.ct.config.GraphConfig;
import org.apache.hugegraph.ct.config.PDConfig;
import org.apache.hugegraph.ct.config.ServerConfig;
import org.apache.hugegraph.ct.config.StoreConfig;
import org.apache.hugegraph.ct.node.AbstractNodeWrapper;
import org.apache.hugegraph.ct.node.PDNodeWrapper;
import org.apache.hugegraph.ct.node.ServerNodeWrapper;
import org.apache.hugegraph.ct.node.StoreNodeWrapper;
import org.slf4j.Logger;

import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractEnv implements BaseEnv {

    private static final Logger LOG = HGTestLogger.ENV_LOG;
    private static final int NODE_START_TIMEOUT_SECONDS = 120;
    private static final int NODE_START_POLL_MILLIS = 1000;

    protected ClusterConfig clusterConfig;
    protected List<PDNodeWrapper> pdNodeWrappers;
    protected List<ServerNodeWrapper> serverNodeWrappers;
    protected List<StoreNodeWrapper> storeNodeWrappers;
    @Setter
    protected int cluster_id = 0;

    protected AbstractEnv() {
        this.pdNodeWrappers = new ArrayList<>();
        this.serverNodeWrappers = new ArrayList<>();
        this.storeNodeWrappers = new ArrayList<>();
    }

    protected void init(int pdCnt, int storeCnt, int serverCnt) {
        this.clusterConfig = new ClusterConfig(pdCnt, storeCnt, serverCnt);
        for (int i = 0; i < pdCnt; i++) {
            PDNodeWrapper pdNodeWrapper = new PDNodeWrapper(cluster_id, i);
            PDConfig pdConfig = clusterConfig.getPDConfig(i);
            pdNodeWrappers.add(pdNodeWrapper);
            pdConfig.writeConfig(pdNodeWrapper.getNodePath() + CONF_DIR);
        }

        for (int i = 0; i < storeCnt; i++) {
            StoreNodeWrapper storeNodeWrapper = new StoreNodeWrapper(cluster_id, i);
            StoreConfig storeConfig = clusterConfig.getStoreConfig(i);
            storeNodeWrappers.add(storeNodeWrapper);
            storeConfig.writeConfig(storeNodeWrapper.getNodePath() + CONF_DIR);
        }

        for (int i = 0; i < serverCnt; i++) {
            ServerConfig serverConfig = clusterConfig.getServerConfig(i);
            ServerNodeWrapper serverNodeWrapper =
                    new ServerNodeWrapper(cluster_id, i,
                                          serverConfig.getRestPort(),
                                          serverConfig.getGremlinPort());
            serverNodeWrappers.add(serverNodeWrapper);
            serverConfig.setServerID(serverNodeWrapper.getID());
            GraphConfig graphConfig = clusterConfig.getGraphConfig(i);
            if (i == 0) {
                serverConfig.setRole("master");
            } else {
                serverConfig.setRole("worker");
            }
            serverConfig.writeConfig(serverNodeWrapper.getNodePath() + CONF_DIR);
            graphConfig.writeConfig(serverNodeWrapper.getNodePath() + CONF_DIR);
        }
    }

    public void startCluster() {
        for (PDNodeWrapper pdNodeWrapper : pdNodeWrappers) {
            startNode(pdNodeWrapper);
        }
        for (StoreNodeWrapper storeNodeWrapper : storeNodeWrappers) {
            startNode(storeNodeWrapper);
        }
        for (ServerNodeWrapper serverNodeWrapper : serverNodeWrappers) {
            startNode(serverNodeWrapper);
        }
    }

    private void startNode(AbstractNodeWrapper nodeWrapper) {
        System.out.printf("[cluster-test] starting %s in %s%n",
                          nodeWrapper.getID(), nodeWrapper.getNodePath());
        nodeWrapper.start();
        waitUntilStarted(nodeWrapper);
    }

    private static void waitUntilStarted(AbstractNodeWrapper nodeWrapper) {
        long deadline = System.nanoTime() +
                        TimeUnit.SECONDS.toNanos(NODE_START_TIMEOUT_SECONDS);
        while (System.nanoTime() < deadline) {
            if (!nodeWrapper.isAlive()) {
                nodeWrapper.dumpLog();
                throw new AssertionError(String.format(
                        "%s failed to start, process status: %s",
                        nodeWrapper.getID(), nodeWrapper.processStatus()));
            }
            if (nodeWrapper.isStarted()) {
                System.out.printf("[cluster-test] %s started%n",
                                  nodeWrapper.getID());
                return;
            }
            sleepBeforeRetry();
        }

        nodeWrapper.dumpLog();
        throw new AssertionError(String.format(
                "%s did not start within %s seconds, process status: %s",
                nodeWrapper.getID(), NODE_START_TIMEOUT_SECONDS,
                nodeWrapper.processStatus()));
    }

    private static void sleepBeforeRetry() {
        try {
            TimeUnit.MILLISECONDS.sleep(NODE_START_POLL_MILLIS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("Interrupted while waiting node start", e);
        }
    }

    public void stopCluster() {
        for (ServerNodeWrapper serverNodeWrapper : serverNodeWrappers) {
            serverNodeWrapper.stop();
        }
        for (StoreNodeWrapper storeNodeWrapper : storeNodeWrappers) {
            storeNodeWrapper.stop();
        }
        for (PDNodeWrapper pdNodeWrapper : pdNodeWrappers) {
            pdNodeWrapper.stop();
        }
    }

    public void dumpClusterStatus() {
        System.out.println("===== cluster node diagnostics =====");
        dumpNodeStatus(pdNodeWrappers);
        dumpNodeStatus(storeNodeWrappers);
        dumpNodeStatus(serverNodeWrappers);
    }

    private static void dumpNodeStatus(
            List<? extends AbstractNodeWrapper> nodeWrappers) {
        for (AbstractNodeWrapper nodeWrapper : nodeWrappers) {
            System.out.printf("[cluster-test] %s process status: %s%n",
                              nodeWrapper.getID(), nodeWrapper.processStatus());
            nodeWrapper.dumpLog();
        }
    }

    public ClusterConfig getConf() {
        return this.clusterConfig;
    }

    public List<String> getPDRestAddrs() {
        return clusterConfig.getPDRestAddrs();
    }

    public List<String> getPDGrpcAddrs() {
        return clusterConfig.getPDGrpcAddrs();
    }

    public List<String> getStoreRestAddrs() {
        return clusterConfig.getStoreRestAddrs();
    }

    public List<String> getStoreGrpcAddrs() {
        return clusterConfig.getStoreGrpcAddrs();
    }

    public List<String> getServerRestAddrs() {
        return clusterConfig.getServerRestAddrs();
    }

    public List<String> getPDNodeDir() {
        List<String> nodeDirs = new ArrayList<>();
        for (PDNodeWrapper pdNodeWrapper : pdNodeWrappers) {
            nodeDirs.add(pdNodeWrapper.getNodePath());
        }
        return nodeDirs;
    }

    public List<String> getStoreNodeDir() {
        List<String> nodeDirs = new ArrayList<>();
        for (StoreNodeWrapper storeNodeWrapper : storeNodeWrappers) {
            nodeDirs.add(storeNodeWrapper.getNodePath());
        }
        return nodeDirs;
    }

    public List<String> getServerNodeDir() {
        List<String> nodeDirs = new ArrayList<>();
        for (ServerNodeWrapper serverNodeWrapper : serverNodeWrappers) {
            nodeDirs.add(serverNodeWrapper.getNodePath());
        }
        return nodeDirs;
    }

}
