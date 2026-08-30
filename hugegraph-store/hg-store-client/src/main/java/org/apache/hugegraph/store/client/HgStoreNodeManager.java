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

package org.apache.hugegraph.store.client;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.hugegraph.store.client.grpc.AbstractGrpcClient;
import org.apache.hugegraph.store.client.grpc.GrpcStoreNodeBuilder;
import org.apache.hugegraph.store.client.type.HgNodeStatus;
import org.apache.hugegraph.store.client.type.HgStoreClientException;
import org.apache.hugegraph.store.client.util.HgAssert;
import org.apache.hugegraph.store.client.util.HgStoreClientConst;

import lombok.extern.slf4j.Slf4j;

/**
 * // TODO: Mapping to Store-Node-Cluster, one to one.
 * created on 2021/10/11
 *
 * @version 0.2.0
 */
@ThreadSafe
@Slf4j
public final class HgStoreNodeManager {

    private final static Set<String> CLUSTER_ID_SET = new HashSet<>();
    private final static HgStoreNodeManager instance = new HgStoreNodeManager();

    private final String clusterId;
    private final Map<String, HgStoreNode> addressMap = new ConcurrentHashMap<>();
    private final Map<Long, HgStoreNode> nodeIdMap = new ConcurrentHashMap<>();
    private final Map<String, List<HgStoreNode>> graphNodesMap = new ConcurrentHashMap<>();

    private HgStoreNodeProvider nodeProvider;
    private HgStoreNodePartitioner nodePartitioner;
    private HgStoreNodeNotifier nodeNotifier;

    private HgStoreNodeManager() {
        this.clusterId = HgStoreClientConst.DEFAULT_NODE_CLUSTER_ID;
    }

    private HgStoreNodeManager(String clusterId) {
        synchronized (CLUSTER_ID_SET) {
            if (CLUSTER_ID_SET.contains(clusterId)) {
                throw new RuntimeException("The cluster [" + clusterId + "] has been existing.");
            }
            CLUSTER_ID_SET.add(clusterId);
            this.clusterId = clusterId;
        }
    }

    public static HgStoreNodeManager getInstance() {
        return instance;
    }

    /**
     * Return the HgStoreNodeBuilder
     *
     * @return
     */
    public HgStoreNodeBuilder getNodeBuilder() {
        // TODO: Constructed by a provider that retrieved by SPI
        return new GrpcStoreNodeBuilder(this, HgPrivate.getInstance());
    }

    /**
     * Return an instance of  HgStoreNode whose ID is matched to the argument.
     *
     * @param nodeId
     * @return null when none of instance is matched to the argument,or argument is invalid.
     */
    public HgStoreNode getStoreNode(Long nodeId) {
        if (nodeId == null) {
            return null;
        }
        return this.nodeIdMap.get(nodeId);
    }

    /**
     * Apply a HgStoreNode instance with graph-name and node-id.
     * <b>CAUTION:</b>
     * <b>It won't work when user haven't set a HgStoreNodeProvider via setNodeProvider method.</b>
     *
     * @param graphName
     * @param nodeId
     * @return
     */
    HgStoreNode applyNode(String graphName, Long nodeId) {
        HgStoreNode node = this.nodeIdMap.get(nodeId);

        if (node != null) {
            return node;
        }

        if (this.nodeProvider == null) {
            return null;
        }

        node = this.nodeProvider.apply(graphName, nodeId);

        if (node == null) {

            log.warn("Failed to apply a HgStoreNode instance form the nodeProvider [ "
                     + this.nodeProvider.getClass().getName() + " ].");
            notifying(graphName, nodeId, HgNodeStatus.NOT_EXIST);
            return null;
        }

        this.addNode(graphName, node);

        return node;
    }

    private void notifying(String graphName, Long nodeId, HgNodeStatus status) {
        if (this.nodeNotifier != null) {
            try {
                this.nodeNotifier.notice(graphName, HgStoreNotice.of(nodeId, status));
            } catch (Throwable t) {
                log.error("Failed to invoke " + this.nodeNotifier.getClass().getSimpleName() +
                          ":notice(" + nodeId + "," + status + ")", t);
            }
        }
    }

    /**
     * @param graphName
     * @param notice
     * @return null: when there is no HgStoreNodeNotifier in the nodeManager;
     * @throws HgStoreClientException
     */
    public Integer notifying(String graphName, HgStoreNotice notice) {
        if (this.nodeNotifier != null) {

            synchronized (Thread.currentThread()) {
                try {
                    return this.nodeNotifier.notice(graphName, notice);
                } catch (Throwable t) {
                    String msg =
                            "Failed to invoke " + this.nodeNotifier.getClass().getSimpleName() +
                            ", notice: [ " + notice + " ]";
                    log.error(msg, t);
                    throw new HgStoreClientException(msg);
                }
            }

        }

        return null;
    }

    public Integer notifying(String graphName, HgStoreNotice notice,
                             HgStoreNode expectedNode) {
        if (notice.getNodeStatus() == HgNodeStatus.NOT_ONLINE ||
            notice.getNodeStatus() == HgNodeStatus.NOT_WORK) {
            this.evictNode(expectedNode);
        }
        return this.notifying(graphName, notice);
    }

    private void evictNode(HgStoreNode expectedNode) {
        synchronized (this.nodeIdMap) {
            HgStoreNode currentNode = this.nodeIdMap.get(expectedNode.getNodeId());
            if (currentNode == expectedNode) {
                this.nodeIdMap.remove(expectedNode.getNodeId(), expectedNode);
                this.addressMap.remove(expectedNode.getAddress(), expectedNode);
                AbstractGrpcClient.closeChannel(expectedNode.getAddress());
            } else if (currentNode == null ||
                       !currentNode.getAddress().equals(expectedNode.getAddress())) {
                AbstractGrpcClient.closeChannel(expectedNode.getAddress());
            }
        }
        synchronized (this.graphNodesMap) {
            for (List<HgStoreNode> nodes : this.graphNodesMap.values()) {
                nodes.removeIf(node -> node == expectedNode);
            }
        }
    }

    /**
     * Return a collection of HgStoreNode who is in charge of the graph passed in the argument.
     *
     * @param graphName
     * @return null when none matched to argument or any argument is invalid.
     */
    public List<HgStoreNode> getStoreNodes(String graphName) {
        if (HgAssert.isInvalid(graphName)) {
            return null;
        }

        return this.graphNodesMap.get(graphName);
    }

    /**
     * Adding a new Store-Node, return the argument's value if the host+port was not existing,
     * otherwise return the HgStoreNode-instance added early.
     *
     * @param storeNode
     * @return
     * @throws IllegalArgumentException when any argument is invalid.
     */
    public HgStoreNode addNode(HgStoreNode storeNode) {
        HgAssert.isFalse(storeNode == null, "the argument: storeNode is null.");

        Long nodeId = storeNode.getNodeId();

        HgStoreNode node = null;

        synchronized (this.nodeIdMap) {
            node = this.addressMap.get(nodeId);
            if (node == null) {
                node = storeNode;
                this.nodeIdMap.put(nodeId, node);
                this.addressMap.put(storeNode.getAddress(), node);
            }
        }

        return node;
    }

    /**
     * @param graphName
     * @param storeNode
     * @return
     * @throws IllegalArgumentException when any argument is invalid.
     */
    public HgStoreNode addNode(String graphName, HgStoreNode storeNode) {
        HgAssert.isFalse(HgAssert.isInvalid(graphName), "the argument is invalid: graphName");
        HgStoreNode node = this.addNode(storeNode);

        List<HgStoreNode> nodes = null;

        synchronized (this.graphNodesMap) {
            nodes = this.graphNodesMap.get(graphName);
            if (nodes == null) {
                nodes = new ArrayList<>();
                this.graphNodesMap.put(graphName, nodes);
            }
            nodes.add(node);
        }

        return node;
    }

    public HgStoreNodePartitioner getNodePartitioner() {
        return nodePartitioner;
    }

    public HgStoreNodeManager setNodePartitioner(HgStoreNodePartitioner nodePartitioner) {
        HgAssert.isFalse(nodePartitioner == null, "the argument is invalid: nodePartitioner");
        this.nodePartitioner = nodePartitioner;
        return this;
    }

    public HgStoreNodeNotifier getNodeNotifier() {
        return nodeNotifier;
    }

    public HgStoreNodeManager setNodeNotifier(HgStoreNodeNotifier nodeNotifier) {
        HgAssert.isFalse(nodeNotifier == null, "the argument is invalid: nodeNotifier");
        this.nodeNotifier = nodeNotifier;
        return this;
    }

    public HgStoreNodeManager setNodeProvider(HgStoreNodeProvider nodeProvider) {
        this.nodeProvider = nodeProvider;
        return this;
    }

}
