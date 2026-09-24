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

package org.apache.hugegraph.backend.store.raft;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.commons.io.FileUtils;
import org.apache.hugegraph.HugeException;
import org.apache.hugegraph.HugeGraphParams;
import org.apache.hugegraph.backend.cache.Cache;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.query.Query;
import org.apache.hugegraph.backend.store.BackendAction;
import org.apache.hugegraph.backend.store.BackendMutation;
import org.apache.hugegraph.backend.store.BackendStore;
import org.apache.hugegraph.backend.store.BackendStoreProvider;
import org.apache.hugegraph.backend.store.raft.compress.CompressStrategyManager;
import org.apache.hugegraph.backend.store.raft.rpc.AddPeerProcessor;
import org.apache.hugegraph.backend.store.raft.rpc.ListPeersProcessor;
import org.apache.hugegraph.backend.store.raft.rpc.RaftRequests.StoreType;
import org.apache.hugegraph.backend.store.raft.rpc.RemovePeerProcessor;
import org.apache.hugegraph.backend.store.raft.rpc.RpcForwarder;
import org.apache.hugegraph.backend.store.raft.rpc.SetLeaderProcessor;
import org.apache.hugegraph.backend.store.raft.rpc.StoreCommandProcessor;
import org.apache.hugegraph.config.CoreOptions;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.event.EventHub;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.type.define.GraphMode;
import org.apache.hugegraph.util.Bytes;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Events;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

import com.alipay.sofa.jraft.NodeManager;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.option.NodeOptions;
import com.alipay.sofa.jraft.option.RaftOptions;
import com.alipay.sofa.jraft.option.ReadOnlyOption;
import com.alipay.sofa.jraft.rpc.RaftRpcServerFactory;
import com.alipay.sofa.jraft.rpc.RpcServer;
import com.alipay.sofa.jraft.rpc.impl.BoltRpcServer;
import com.alipay.sofa.jraft.util.NamedThreadFactory;
import com.alipay.sofa.jraft.util.ThreadPoolUtil;

public final class RaftContext {

    private static final Logger LOG = Log.logger(RaftContext.class);

    // unit is ms
    public static final int NO_TIMEOUT = -1;
    public static final int POLL_INTERVAL = 5000;
    public static final int WAIT_RAFTLOG_TIMEOUT = 30 * 60 * 1000;
    public static final int WAIT_LEADER_TIMEOUT = 10 * 60 * 1000;
    public static final int BUSY_MIN_SLEEP_FACTOR = 3 * 1000;
    public static final int BUSY_MAX_SLEEP_FACTOR = 5 * 1000;
    public static final int WAIT_RPC_TIMEOUT = 30 * 60 * 1000;
    public static final int LOG_WARN_INTERVAL = 60 * 1000;

    // compress block size
    public static final int BLOCK_SIZE = (int) (Bytes.KB * 8);

    // work queue size
    public static final int QUEUE_SIZE = CoreOptions.CPUS;
    public static final long KEEP_ALIVE_SECOND = 300L;

    private final HugeGraphParams params;

    private final Configuration groupPeers;

    private final RaftBackendStore[] stores;

    private final ExecutorService readIndexExecutor;
    private final ExecutorService snapshotExecutor;
    private final ExecutorService backendExecutor;

    private RpcServer raftRpcServer;
    private PeerId endpoint;

    private RaftNode raftNode;
    private RaftGroupManager raftGroupManager;
    private RpcForwarder rpcForwarder;

    public RaftContext(HugeGraphParams params) {
        this.params = params;

        HugeConfig config = params.configuration();

        /*
         * NOTE: `raft.group_peers` option is transferred from ServerConfig
         * to CoreConfig, since it's shared by all graphs.
         */
        String groupPeersString = this.config().getString("raft.group_peers");
        E.checkArgument(groupPeersString != null,
                        "Please ensure config `raft.group_peers` in raft mode");
        this.groupPeers = new Configuration();
        if (!this.groupPeers.parse(groupPeersString)) {
            throw new HugeException("Failed to parse raft.group_peers: '%s'",
                                    groupPeersString);
        }

        this.stores = new RaftBackendStore[StoreType.ALL.getNumber()];

        if (config.get(CoreOptions.RAFT_SAFE_READ)) {
            int threads = config.get(CoreOptions.RAFT_READ_INDEX_THREADS);
            this.readIndexExecutor = this.createReadIndexExecutor(threads);
        } else {
            this.readIndexExecutor = null;
        }

        int threads = config.get(CoreOptions.RAFT_SNAPSHOT_THREADS);
        this.snapshotExecutor = this.createSnapshotExecutor(threads);

        threads = config.get(CoreOptions.RAFT_BACKEND_THREADS);
        this.backendExecutor = this.createBackendExecutor(threads);

        CompressStrategyManager.init(config);

        this.raftRpcServer = null;
        this.endpoint = null;

        this.raftNode = null;
        this.raftGroupManager = null;
        this.rpcForwarder = null;
    }

    public void initRaftNode(com.alipay.remoting.rpc.RpcServer rpcServer) {
        this.raftRpcServer = this.wrapRpcServer(rpcServer);
        this.endpoint = new PeerId(rpcServer.ip(), rpcServer.port());

        this.registerRpcRequestProcessors();
        LOG.info("Start raft server successfully: {}", this.endpoint());

        this.raftNode = new RaftNode(this);
        this.rpcForwarder = new RpcForwarder(this.raftNode.node());
        this.raftGroupManager = new RaftGroupManagerImpl(this);
    }

    public void waitRaftNodeStarted() {
        RaftNode node = this.node();
        node.waitLeaderElected(RaftContext.WAIT_LEADER_TIMEOUT);
        node.waitRaftLogSynced(RaftContext.NO_TIMEOUT);
    }

    public void close() {
        LOG.info("Stop raft server: {}", this.endpoint());

        RaftNode node = this.node();
        if (node != null) {
            node.shutdown();
        }

        this.shutdownRpcServer();
    }

    public RaftNode node() {
        return this.raftNode;
    }

    RpcServer rpcServer() {
        return this.raftRpcServer;
    }

    RpcForwarder rpcForwarder() {
        return this.rpcForwarder;
    }

    public RaftGroupManager raftNodeManager() {
        return this.raftGroupManager;
    }

    public String group() {
        // Use graph name as group name
        return this.params.spaceGraphName();
    }

    public void addStore(StoreType type, RaftBackendStore store) {
        this.stores[type.getNumber()] = store;
    }

    public StoreType storeType(String store) {
        if (BackendStoreProvider.SCHEMA_STORE.equals(store)) {
            return StoreType.SCHEMA;
        } else if (BackendStoreProvider.GRAPH_STORE.equals(store)) {
            return StoreType.GRAPH;
        } else {
            assert BackendStoreProvider.SYSTEM_STORE.equals(store);
            return StoreType.SYSTEM;
        }
    }

    RaftBackendStore[] stores() {
        return this.stores;
    }

    public BackendStore originStore(StoreType storeType) {
        RaftBackendStore raftStore = this.stores[storeType.getNumber()];
        E.checkState(raftStore != null,
                     "The raft store of type %s shouldn't be null", storeType);
        return raftStore.originStore();
    }

    public NodeOptions nodeOptions() throws IOException {
        HugeConfig config = this.config();

        NodeOptions nodeOptions = new NodeOptions();
        nodeOptions.setEnableMetrics(false);
        nodeOptions.setRpcProcessorThreadPoolSize(
                config.get(CoreOptions.RAFT_RPC_THREADS));
        nodeOptions.setRpcConnectTimeoutMs(
                config.get(CoreOptions.RAFT_RPC_CONNECT_TIMEOUT));
        nodeOptions.setRpcDefaultTimeout(
                1000 * config.get(CoreOptions.RAFT_RPC_TIMEOUT));
        nodeOptions.setRpcInstallSnapshotTimeout(
                1000 * config.get(CoreOptions.RAFT_INSTALL_SNAPSHOT_TIMEOUT));

        int electionTimeout = config.get(CoreOptions.RAFT_ELECTION_TIMEOUT);
        nodeOptions.setElectionTimeoutMs(electionTimeout);
        nodeOptions.setDisableCli(false);

        int snapshotInterval = config.get(CoreOptions.RAFT_SNAPSHOT_INTERVAL);
        nodeOptions.setSnapshotIntervalSecs(snapshotInterval);
        nodeOptions.setInitialConf(this.groupPeers);

        String raftPath = config.get(CoreOptions.RAFT_PATH);
        String logUri = Paths.get(raftPath, "log").toString();
        FileUtils.forceMkdir(new File(logUri));
        nodeOptions.setLogUri(logUri);

        String metaUri = Paths.get(raftPath, "meta").toString();
        FileUtils.forceMkdir(new File(metaUri));
        nodeOptions.setRaftMetaUri(metaUri);

        String snapshotUri = Paths.get(raftPath, "snapshot").toString();
        FileUtils.forceMkdir(new File(snapshotUri));
        nodeOptions.setSnapshotUri(snapshotUri);

        RaftOptions raftOptions = nodeOptions.getRaftOptions();
        /*
         * NOTE: if buffer size is too small(<=1024), will throw exception
         * "LogManager is busy, disk queue overload"
         */
        raftOptions.setApplyBatch(config.get(CoreOptions.RAFT_APPLY_BATCH));
        raftOptions.setDisruptorBufferSize(
                config.get(CoreOptions.RAFT_QUEUE_SIZE));
        raftOptions.setDisruptorPublishEventWaitTimeoutSecs(
                config.get(CoreOptions.RAFT_QUEUE_PUBLISH_TIMEOUT));
        raftOptions.setReplicatorPipeline(
                config.get(CoreOptions.RAFT_REPLICATOR_PIPELINE));
        raftOptions.setOpenStatistics(false);
        raftOptions.setReadOnlyOptions(
                ReadOnlyOption.valueOf(
                        config.get(CoreOptions.RAFT_READ_STRATEGY)));

        return nodeOptions;
    }

    void clearCache() {
        // Just choose two representatives used to represent schema and graph
        this.notifyCache(Cache.ACTION_CLEAR, HugeType.VERTEX_LABEL, null);
        this.notifyCache(Cache.ACTION_CLEAR, HugeType.VERTEX, null);
    }

    void updateCacheIfNeeded(BackendMutation mutation,
                             boolean forwarded) {
        // Update cache only when graph run in general mode
        if (this.graphMode() != GraphMode.NONE) {
            return;
        }
        /*
         * 1. If Follower, need to update cache from store to tx
         * 3. If Leader, request is forwarded by follower, need to update cache
         * 2. If Leader, request comes from leader, don't need to update cache,
         *    because the cache will be updated by upper layer
         */
        if (!forwarded && this.node().selfIsLeader()) {
            return;
        }
        for (HugeType type : mutation.types()) {
            List<Id> ids = new ArrayList<>((int) Query.COMMIT_BATCH);
            if (type.isSchema() || type.isGraph()) {
                java.util.Iterator<BackendAction> it = mutation.mutation(type);
                while (it.hasNext()) {
                    ids.add(it.next().entry().originId());
                }
                this.notifyCache(Cache.ACTION_INVALID, type, ids);
            } else {
                // Ignore other types due to not cached them
            }
        }
    }

    private void notifyCache(String action, HugeType type, List<Id> ids) {
        EventHub eventHub;
        if (type.isGraph()) {
            eventHub = this.params.graphEventHub();
        } else if (type.isSchema()) {
            eventHub = this.params.schemaEventHub();
        } else {
            return;
        }
        try {
            // How to avoid update cache from server info
            if (ids == null) {
                eventHub.call(Events.CACHE, action, type);
            } else {
                if (ids.size() == 1) {
                    eventHub.call(Events.CACHE, action, type, ids.get(0));
                } else {
                    eventHub.call(Events.CACHE, action, type, ids.toArray());
                }
            }
        } catch (RejectedExecutionException e) {
            LOG.warn("Can't update cache due to EventHub is too busy");
        }
    }

    public PeerId endpoint() {
        return this.endpoint;
    }

    public boolean safeRead() {
        return this.config().get(CoreOptions.RAFT_SAFE_READ);
    }

    public ExecutorService snapshotExecutor() {
        return this.snapshotExecutor;
    }

    public ExecutorService backendExecutor() {
        return this.backendExecutor;
    }

    public ExecutorService readIndexExecutor() {
        return this.readIndexExecutor;
    }

    public GraphMode graphMode() {
        return this.params.mode();
    }

    private HugeConfig config() {
        return this.params.configuration();
    }

    @SuppressWarnings("unused")
    private RpcServer initAndStartRpcServer() {
        Integer lowWaterMark = this.config().get(
                CoreOptions.RAFT_RPC_BUF_LOW_WATER_MARK);
        System.setProperty("bolt.channel_write_buf_low_water_mark",
                           String.valueOf(lowWaterMark));
        Integer highWaterMark = this.config().get(
                CoreOptions.RAFT_RPC_BUF_HIGH_WATER_MARK);
        System.setProperty("bolt.channel_write_buf_high_water_mark",
                           String.valueOf(highWaterMark));

        PeerId endpoint = this.endpoint();
        NodeManager.getInstance().addAddress(endpoint.getEndpoint());
        RpcServer rpcServer = RaftRpcServerFactory.createAndStartRaftRpcServer(
                endpoint.getEndpoint());
        LOG.info("Raft-RPC server is started successfully");
        return rpcServer;
    }

    private RpcServer wrapRpcServer(com.alipay.remoting.rpc.RpcServer rpcServer) {
        // TODO: pass ServerOptions instead of CoreOptions, to share by graphs
        Integer lowWaterMark = this.config().get(
                CoreOptions.RAFT_RPC_BUF_LOW_WATER_MARK);
        System.setProperty("bolt.channel_write_buf_low_water_mark",
                           String.valueOf(lowWaterMark));
        Integer highWaterMark = this.config().get(
                CoreOptions.RAFT_RPC_BUF_HIGH_WATER_MARK);
        System.setProperty("bolt.channel_write_buf_high_water_mark",
                           String.valueOf(highWaterMark));

        // Reference from RaftRpcServerFactory.createAndStartRaftRpcServer
        RpcServer raftRpcServer = new BoltRpcServer(rpcServer);
        RaftRpcServerFactory.addRaftRequestProcessors(raftRpcServer);

        return raftRpcServer;
    }

    private void shutdownRpcServer() {
        this.raftRpcServer.shutdown();
        PeerId endpoint = this.endpoint();
        NodeManager.getInstance().removeAddress(endpoint.getEndpoint());
    }

    private void registerRpcRequestProcessors() {
        this.raftRpcServer.registerProcessor(new AddPeerProcessor(this));
        this.raftRpcServer.registerProcessor(new RemovePeerProcessor(this));
        this.raftRpcServer.registerProcessor(new StoreCommandProcessor(this));
        this.raftRpcServer.registerProcessor(new SetLeaderProcessor(this));
        this.raftRpcServer.registerProcessor(new ListPeersProcessor(this));
    }

    private ExecutorService createReadIndexExecutor(int coreThreads) {
        int maxThreads = coreThreads << 2;
        String name = "store-read-index-callback";
        RejectedExecutionHandler handler = new ThreadPoolExecutor.AbortPolicy();
        return newPool(coreThreads, maxThreads, name, handler);
    }

    private ExecutorService createSnapshotExecutor(int coreThreads) {
        int maxThreads = coreThreads << 2;
        String name = "store-snapshot-executor";
        RejectedExecutionHandler handler;
        handler = new ThreadPoolExecutor.CallerRunsPolicy();
        return newPool(coreThreads, maxThreads, name, handler);
    }

    private ExecutorService createBackendExecutor(int threads) {
        String name = "store-backend-executor";
        RejectedExecutionHandler handler =
                new ThreadPoolExecutor.CallerRunsPolicy();
        return newPool(threads, threads, name, handler);
    }

    private static ExecutorService newPool(int coreThreads, int maxThreads,
                                           String name,
                                           RejectedExecutionHandler handler) {
        BlockingQueue<Runnable> queue = new ArrayBlockingQueue<>(QUEUE_SIZE);
        return ThreadPoolUtil.newBuilder()
                             .poolName(name)
                             .enableMetric(false)
                             .coreThreads(coreThreads)
                             .maximumThreads(maxThreads)
                             .keepAliveSeconds(KEEP_ALIVE_SECOND)
                             .workQueue(queue)
                             .threadFactory(new NamedThreadFactory(name, true))
                             .rejectedHandler(handler)
                             .build();
    }
}
