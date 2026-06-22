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

package org.apache.hugegraph;

import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.StringUtils;
import org.apache.hugegraph.analyzer.Analyzer;
import org.apache.hugegraph.analyzer.AnalyzerFactory;
import org.apache.hugegraph.auth.AuthManager;
import org.apache.hugegraph.auth.StandardAuthManager;
import org.apache.hugegraph.auth.StandardAuthManagerV2;
import org.apache.hugegraph.backend.BackendException;
import org.apache.hugegraph.backend.LocalCounter;
import org.apache.hugegraph.backend.cache.Cache;
import org.apache.hugegraph.backend.cache.CacheNotifier;
import org.apache.hugegraph.backend.cache.CacheNotifier.GraphCacheNotifier;
import org.apache.hugegraph.backend.cache.CacheNotifier.SchemaCacheNotifier;
import org.apache.hugegraph.backend.cache.CachedGraphTransaction;
import org.apache.hugegraph.backend.cache.CachedSchemaTransaction;
import org.apache.hugegraph.backend.cache.CachedSchemaTransactionV2;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.id.IdGenerator;
import org.apache.hugegraph.backend.id.SnowflakeIdGenerator;
import org.apache.hugegraph.backend.query.Query;
import org.apache.hugegraph.backend.serializer.AbstractSerializer;
import org.apache.hugegraph.backend.serializer.SerializerFactory;
import org.apache.hugegraph.backend.store.BackendFeatures;
import org.apache.hugegraph.backend.store.BackendProviderFactory;
import org.apache.hugegraph.backend.store.BackendStore;
import org.apache.hugegraph.backend.store.BackendStoreInfo;
import org.apache.hugegraph.backend.store.BackendStoreProvider;
import org.apache.hugegraph.backend.store.raft.RaftBackendStoreProvider;
import org.apache.hugegraph.backend.store.raft.RaftGroupManager;
import org.apache.hugegraph.backend.store.ram.RamTable;
import org.apache.hugegraph.backend.tx.GraphTransaction;
import org.apache.hugegraph.backend.tx.ISchemaTransaction;
import org.apache.hugegraph.config.CoreOptions;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.config.TypedOption;
import org.apache.hugegraph.event.EventHub;
import org.apache.hugegraph.event.EventListener;
import org.apache.hugegraph.exception.NotAllowException;
import org.apache.hugegraph.io.HugeGraphIoRegistry;
import org.apache.hugegraph.job.EphemeralJob;
import org.apache.hugegraph.kvstore.KvStore;
import org.apache.hugegraph.masterelection.ClusterRoleStore;
import org.apache.hugegraph.masterelection.Config;
import org.apache.hugegraph.masterelection.GlobalMasterInfo;
import org.apache.hugegraph.masterelection.RoleElectionConfig;
import org.apache.hugegraph.masterelection.RoleElectionOptions;
import org.apache.hugegraph.masterelection.RoleElectionStateMachine;
import org.apache.hugegraph.masterelection.StandardClusterRoleStore;
import org.apache.hugegraph.masterelection.StandardRoleElectionStateMachine;
import org.apache.hugegraph.memory.MemoryManager;
import org.apache.hugegraph.memory.util.RoundUtil;
import org.apache.hugegraph.meta.MetaManager;
import org.apache.hugegraph.perf.PerfUtil.Watched;
import org.apache.hugegraph.rpc.RpcServiceConfig4Client;
import org.apache.hugegraph.rpc.RpcServiceConfig4Server;
import org.apache.hugegraph.schema.EdgeLabel;
import org.apache.hugegraph.schema.IndexLabel;
import org.apache.hugegraph.schema.PropertyKey;
import org.apache.hugegraph.schema.SchemaElement;
import org.apache.hugegraph.schema.SchemaLabel;
import org.apache.hugegraph.schema.SchemaManager;
import org.apache.hugegraph.schema.VertexLabel;
import org.apache.hugegraph.structure.HugeEdge;
import org.apache.hugegraph.structure.HugeEdgeProperty;
import org.apache.hugegraph.structure.HugeFeatures;
import org.apache.hugegraph.structure.HugeVertex;
import org.apache.hugegraph.structure.HugeVertexProperty;
import org.apache.hugegraph.task.EphemeralJobQueue;
import org.apache.hugegraph.task.ServerInfoManager;
import org.apache.hugegraph.task.TaskManager;
import org.apache.hugegraph.task.TaskScheduler;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.type.define.GraphMode;
import org.apache.hugegraph.type.define.GraphReadMode;
import org.apache.hugegraph.util.ConfigUtil;
import org.apache.hugegraph.util.DateUtil;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Events;
import org.apache.hugegraph.util.LockUtil;
import org.apache.hugegraph.util.Log;
import org.apache.hugegraph.variables.HugeVariables;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.io.Io;
import org.apache.tinkerpop.gremlin.structure.util.AbstractThreadLocalTransaction;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.slf4j.Logger;

import com.alipay.remoting.rpc.RpcServer;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.RateLimiter;

/**
 * StandardHugeGraph is the entrance of the graph system, you can modify or
 * query the schema/vertex/edge data through this class.
 */
public class StandardHugeGraph implements HugeGraph {

    public static final Class<?>[] PROTECT_CLASSES = {
            StandardHugeGraph.class,
            StandardHugeGraph.StandardHugeGraphParams.class,
            TinkerPopTransaction.class,
            StandardHugeGraph.Txs.class,
            StandardHugeGraph.SysTransaction.class
    };

    public static final Set<TypedOption<?, ?>> ALLOWED_CONFIGS = ImmutableSet.of(
            CoreOptions.TASK_WAIT_TIMEOUT,
            CoreOptions.TASK_SYNC_DELETION,
            CoreOptions.TASK_TTL_DELETE_BATCH,
            CoreOptions.TASK_INPUT_SIZE_LIMIT,
            CoreOptions.TASK_RESULT_SIZE_LIMIT,
            CoreOptions.OLTP_CONCURRENT_THREADS,
            CoreOptions.OLTP_CONCURRENT_DEPTH,
            CoreOptions.OLTP_COLLECTION_TYPE,
            CoreOptions.VERTEX_DEFAULT_LABEL,
            CoreOptions.VERTEX_ENCODE_PK_NUMBER,
            CoreOptions.STORE_GRAPH,
            CoreOptions.STORE,
            CoreOptions.TASK_RETRY,
            CoreOptions.OLTP_QUERY_BATCH_SIZE,
            CoreOptions.OLTP_QUERY_BATCH_AVG_DEGREE_RATIO,
            CoreOptions.OLTP_QUERY_BATCH_EXPECT_DEGREE,
            CoreOptions.SCHEMA_INDEX_REBUILD_USING_PUSHDOWN,
            CoreOptions.QUERY_TRUST_INDEX,
            CoreOptions.QUERY_MAX_INDEXES_AVAILABLE,
            CoreOptions.QUERY_DEDUP_OPTION
    );

    private static final Logger LOG = Log.logger(StandardHugeGraph.class);
    private final String name;
    private final StandardHugeGraphParams params;
    private final HugeConfig configuration;
    private final EventHub schemaEventHub;
    private final EventHub graphEventHub;
    private final EventHub indexEventHub;
    private final LocalCounter localCounter;
    private final RateLimiter writeRateLimiter;
    private final RateLimiter readRateLimiter;
    private final TaskManager taskManager;
    private final HugeFeatures features;
    private final BackendStoreProvider storeProvider;
    private final TinkerPopTransaction tx;
    private final RamTable ramtable;
    private final String schedulerType;
    private volatile boolean started;
    private volatile boolean closed;
    private volatile GraphMode mode;
    private volatile GraphReadMode readMode;
    private volatile HugeVariables variables;
    private String graphSpace;
    private AuthManager authManager;
    private RoleElectionStateMachine roleElectionStateMachine;
    private String nickname;
    private String creator;
    private Date createTime;
    private Date updateTime;
    private KvStore kvStore;

    public StandardHugeGraph(HugeConfig config) {
        this.params = new StandardHugeGraphParams();
        this.configuration = config;
        this.graphSpace = config.get(CoreOptions.GRAPH_SPACE);

        this.schemaEventHub = new EventHub("schema");
        this.graphEventHub = new EventHub("graph");
        this.indexEventHub = new EventHub("index");

        this.localCounter = new LocalCounter();

        final int writeLimit = config.get(CoreOptions.RATE_LIMIT_WRITE);
        this.writeRateLimiter = writeLimit > 0 ?
                                RateLimiter.create(writeLimit) : null;
        final int readLimit = config.get(CoreOptions.RATE_LIMIT_READ);
        this.readRateLimiter = readLimit > 0 ?
                               RateLimiter.create(readLimit) : null;

        String graphSpace = config.getString("graphSpace");
        if (!StringUtils.isEmpty(graphSpace) && StringUtils.isEmpty(this.graphSpace())) {
            this.graphSpace(graphSpace);
        }

        boolean ramtableEnable = config.get(CoreOptions.QUERY_RAMTABLE_ENABLE);
        if (ramtableEnable) {
            long vc = config.get(CoreOptions.QUERY_RAMTABLE_VERTICES_CAPACITY);
            int ec = config.get(CoreOptions.QUERY_RAMTABLE_EDGES_CAPACITY);
            this.ramtable = new RamTable(this, vc, ec);
        } else {
            this.ramtable = null;
        }

        this.taskManager = TaskManager.instance();
        this.name = config.get(CoreOptions.STORE);
        this.started = false;
        this.closed = false;
        this.mode = GraphMode.NONE;
        this.readMode = GraphReadMode.OLTP_ONLY;
        this.schedulerType = config.get(CoreOptions.SCHEDULER_TYPE);

        LockUtil.init(this.spaceGraphName());

        MemoryManager.setMemoryMode(
                MemoryManager.MemoryMode.fromValue(config.get(CoreOptions.MEMORY_MODE)));
        MemoryManager.setMaxMemoryCapacityInBytes(config.get(CoreOptions.MAX_MEMORY_CAPACITY));
        MemoryManager.setMaxMemoryCapacityForOneQuery(
                config.get(CoreOptions.ONE_QUERY_MAX_MEMORY_CAPACITY));
        RoundUtil.setAlignment(config.get(CoreOptions.MEMORY_ALIGNMENT));

        try {
            this.storeProvider = this.loadStoreProvider();
        } catch (Exception e) {
            LockUtil.destroy(this.spaceGraphName());
            String message = "Failed to load backend store provider";
            LOG.error("{}: {}", message, e.getMessage());
            throw new HugeException(message);
        }

        if (isHstore()) {
            // TODO: parameterize the remaining configurations
            MetaManager.instance().connect("hg", MetaManager.MetaDriverType.PD,
                                           "ca", "ca", "ca",
                                           config.get(CoreOptions.PD_PEERS));
        }

        try {
            this.tx = new TinkerPopTransaction(this);
            boolean supportsPersistence = this.backendStoreFeatures().supportsPersistence();
            this.features = new HugeFeatures(this, supportsPersistence);

            SnowflakeIdGenerator.init(this.params);

            this.taskManager.addScheduler(this.params);
            if (isHstore()) {
                this.authManager = new StandardAuthManagerV2((this.params));
            } else {
                this.authManager = new StandardAuthManager(this.params);
            }
            this.variables = null;
        } catch (Exception e) {
            this.storeProvider.close();
            LockUtil.destroy(this.spaceGraphName());
            throw e;
        }
    }

    @Override
    public BackendStoreProvider storeProvider() {
        return this.storeProvider;
    }

    @Override
    public String graphSpace() {
        return this.graphSpace;
    }

    @Override
    public void graphSpace(String graphSpace) {
        this.graphSpace = graphSpace;
    }

    @Override
    public String name() {
        return this.name;
    }

    @Override
    public String spaceGraphName() {
        if (this.graphSpace == null) {
            return this.name;
        }
        return this.graphSpace + "-" + this.name;
    }

    @Override
    public HugeGraph hugegraph() {
        return this;
    }

    @Override
    public String backend() {
        return this.storeProvider.type();
    }

    public BackendStoreInfo backendStoreInfo() {
        // Just for trigger Tx.getOrNewTransaction, then load 3 stores
        // TODO: pass storeProvider.metaStore()
        this.systemTransaction();
        return new BackendStoreInfo(this.configuration, this.storeProvider);
    }

    @Override
    public BackendFeatures backendStoreFeatures() {
        return this.graphTransaction().storeFeatures();
    }

    @Override
    public void serverStarted(GlobalMasterInfo nodeInfo) {
        LOG.info("Init system info for graph '{}'", this.spaceGraphName());
        this.initSystemInfo();

        LOG.info("Init server info [{}-{}] for graph '{}'...",
                 nodeInfo.nodeId(), nodeInfo.nodeRole(), this.spaceGraphName());
        this.serverInfoManager().initServerInfo(nodeInfo);

        this.initRoleStateMachine(nodeInfo.nodeId());

        // TODO: check necessary?
        LOG.info("Check olap property-key tables for graph '{}'", this.spaceGraphName());
        for (PropertyKey pk : this.schemaTransaction().getPropertyKeys()) {
            if (pk.olap()) {
                this.graphTransaction().initAndRegisterOlapTable(pk.id());
            }
        }

        LOG.info("Restoring incomplete tasks for graph '{}'...", this.spaceGraphName());
        this.taskScheduler().restoreTasks();

        this.started = true;
    }

    private void initRoleStateMachine(Id serverId) {
        HugeConfig conf = this.configuration;
        Config roleConfig = new RoleElectionConfig(serverId.toString(),
                                                   conf.get(RoleElectionOptions.NODE_EXTERNAL_URL),
                                                   conf.get(RoleElectionOptions.EXCEEDS_FAIL_COUNT),
                                                   conf.get(
                                                           RoleElectionOptions.RANDOM_TIMEOUT_MILLISECOND),
                                                   conf.get(
                                                           RoleElectionOptions.HEARTBEAT_INTERVAL_SECOND),
                                                   conf.get(RoleElectionOptions.MASTER_DEAD_TIMES),
                                                   conf.get(
                                                           RoleElectionOptions.BASE_TIMEOUT_MILLISECOND));
        ClusterRoleStore roleStore = new StandardClusterRoleStore(this.params);
        this.roleElectionStateMachine = new StandardRoleElectionStateMachine(roleConfig, roleStore);
    }

    @Override
    public boolean started() {
        return this.started;
    }

    @Override
    public synchronized void started(boolean started) {
        this.started = started;
    }

    @Override
    public boolean closed() {
        if (this.closed && !this.tx.closed()) {
            LOG.warn("The tx is not closed while graph '{}' is closed", this);
        }
        return this.closed;
    }

    private void closeTx() {
        try {
            if (this.tx.isOpen()) {
                this.tx.close();
            }
        } finally {
            this.tx.destroyTransaction();
        }
    }

    @Override
    public GraphMode mode() {
        return this.mode;
    }

    @Override
    public void mode(GraphMode mode) {
        LOG.info("Graph {} will work in {} mode", this, mode);
        this.mode = mode;
    }

    @Override
    public GraphReadMode readMode() {
        return this.readMode;
    }

    @Override
    public void readMode(GraphReadMode readMode) {
        this.clearVertexCache();
        this.readMode = readMode;
    }

    @Override
    public void waitReady(RpcServer rpcServer) {
        // Just for trigger Tx.getOrNewTransaction, then load 3 stores
        this.schemaTransaction();
        this.storeProvider.waitReady(rpcServer);
    }

    @Override
    public String nickname() {
        return this.nickname;
    }

    @Override
    public void nickname(String nickname) {
        this.nickname = nickname;
    }

    @Override
    public String creator() {
        return this.creator;
    }

    @Override
    public void creator(String creator) {
        this.creator = creator;
    }

    @Override
    public Date createTime() {
        return this.createTime;
    }

    @Override
    public void createTime(Date createTime) {
        this.createTime = createTime;
    }

    @Override
    public Date updateTime() {
        return this.updateTime;
    }

    @Override
    public void updateTime(Date updateTime) {
        this.updateTime = updateTime;
    }

    public void waitStarted() {
        // Just for trigger Tx.getOrNewTransaction, then load 3 stores
        this.schemaTransaction();
        //this.storeProvider.waitStoreStarted();
    }

    @Override
    public void initBackend() {
        this.loadSchemaStore().open(this.configuration);
        this.loadSystemStore().open(this.configuration);
        this.loadGraphStore().open(this.configuration);

        LockUtil.lock(this.spaceGraphName(), LockUtil.GRAPH_LOCK);
        try {
            this.storeProvider.init();
            /*
             * NOTE: The main goal is to write the serverInfo to the central
             * node, such as etcd, and also create the system schema in memory,
             * which has no side effects
             */
            this.initSystemInfo();
        } finally {
            LockUtil.unlock(this.spaceGraphName(), LockUtil.GRAPH_LOCK);
            this.loadGraphStore().close();
            this.loadSystemStore().close();
            this.loadSchemaStore().close();
        }

        LOG.info("Graph '{}' has been initialized", this.spaceGraphName());
    }

    @Override
    public void clearBackend() {
        this.waitUntilAllTasksCompleted();

        this.loadSchemaStore().open(this.configuration);
        this.loadSystemStore().open(this.configuration);
        this.loadGraphStore().open(this.configuration);

        LockUtil.lock(this.spaceGraphName(), LockUtil.GRAPH_LOCK);
        try {
            this.storeProvider.clear();
        } finally {
            LockUtil.unlock(this.spaceGraphName(), LockUtil.GRAPH_LOCK);
            this.loadGraphStore().close();
            this.loadSystemStore().close();
            this.loadSchemaStore().close();
        }

        LOG.info("Graph '{}' has been cleared", this.spaceGraphName());
    }

    @Override
    public void truncateBackend() {
        this.waitUntilAllTasksCompleted();

        LockUtil.lock(this.spaceGraphName(), LockUtil.GRAPH_LOCK);
        try {
            this.storeProvider.truncate();
            // TODO: remove this after serverinfo saved in etcd
            this.serverStarted(this.serverInfoManager().globalNodeRoleInfo());
        } finally {
            LockUtil.unlock(this.spaceGraphName(), LockUtil.GRAPH_LOCK);
        }

        LOG.info("Graph '{}' has been truncated", this.spaceGraphName());
    }

    @Override
    public void kvStore(KvStore kvStore) {
        this.kvStore = kvStore;
    }

    @Override
    public KvStore kvStore() {
        return this.kvStore;
    }

    @Override
    public void initSystemInfo() {
        try {
            this.taskScheduler().init();
            this.serverInfoManager().init();
            this.authManager().init();
        } finally {
            this.closeTx();
        }
        LOG.debug("Graph '{}' system info has been initialized", this);
    }

    @Override
    public void createSnapshot() {
        LockUtil.lock(this.spaceGraphName(), LockUtil.GRAPH_LOCK);
        try {
            this.storeProvider.createSnapshot();
        } finally {
            LockUtil.unlock(this.spaceGraphName(), LockUtil.GRAPH_LOCK);
        }
        LOG.info("Graph '{}' has created snapshot", this.spaceGraphName());
    }

    @Override
    public void resumeSnapshot() {
        LockUtil.lock(this.spaceGraphName(), LockUtil.GRAPH_LOCK);
        try {
            this.storeProvider.resumeSnapshot();
        } finally {
            LockUtil.unlock(this.spaceGraphName(), LockUtil.GRAPH_LOCK);
        }
        LOG.info("Graph '{}' has resumed from snapshot", this.spaceGraphName());
    }

    private void clearVertexCache() {
        Future<?> future = this.graphEventHub.notify(Events.CACHE, "clear",
                                                     HugeType.VERTEX);
        try {
            future.get();
        } catch (Throwable e) {
            LOG.warn("Error when waiting for event execution: vertex cache " +
                     "clear", e);
        }
    }

    private boolean isHstore() {
        return this.storeProvider.isHstore();
    }

    private ISchemaTransaction openSchemaTransaction() throws HugeException {
        this.checkGraphNotClosed();
        try {
            if (isHstore()) {
                return new CachedSchemaTransactionV2(
                        MetaManager.instance().metaDriver(),
                        MetaManager.instance().cluster(), this.params);
            }
            return new CachedSchemaTransaction(this.params, loadSchemaStore());
        } catch (BackendException e) {
            String message = "Failed to open schema transaction";
            LOG.error("{}", message, e);
            throw new HugeException(message, e);
        }
    }

    private SysTransaction openSystemTransaction() throws HugeException {
        this.checkGraphNotClosed();
        try {
            return new SysTransaction(this.params, loadSystemStore());
        } catch (BackendException e) {
            String message = "Failed to open system transaction";
            LOG.error("{}", message, e);
            throw new HugeException(message);
        }
    }

    private GraphTransaction openGraphTransaction() throws HugeException {
        // Open a new one
        this.checkGraphNotClosed();
        try {
            return new CachedGraphTransaction(this.params, loadGraphStore());
        } catch (BackendException e) {
            String message = "Failed to open graph transaction";
            LOG.error("{}", message, e);
            throw new HugeException(message);
        }
    }

    private void checkGraphNotClosed() {
        E.checkState(!this.closed, "Graph '%s' has been closed", this);
    }

    private BackendStore loadSchemaStore() {
        return this.storeProvider.loadSchemaStore(this.configuration);
    }

    private BackendStore loadGraphStore() {
        return this.storeProvider.loadGraphStore(this.configuration);
    }

    private BackendStore loadSystemStore() {
        if (isHstore()) {
            return this.storeProvider.loadGraphStore(this.configuration);
        }
        return this.storeProvider.loadSystemStore(this.configuration);
    }

    private SysTransaction systemTransaction() {
        this.checkGraphNotClosed();
        /*
         * NOTE: system operations must be committed manually,
         * Maybe users need to auto open tinkerpop tx by readWrite().
         */
        this.tx.readWrite();
        return this.tx.systemTransaction();
    }

    @Watched
    private GraphTransaction graphTransaction() {
        this.checkGraphNotClosed();
        /*
         * NOTE: graph operations must be committed manually,
         * Maybe users need to auto open tinkerpop tx by readWrite().
         */
        this.tx.readWrite();
        return this.tx.graphTransaction();
    }

    private BackendStoreProvider loadStoreProvider() {
        return BackendProviderFactory.open(this.params);
    }

    private AbstractSerializer serializer() {
        String name = this.configuration.get(CoreOptions.SERIALIZER);
        LOG.debug("Loading serializer '{}' for graph '{}'", name, this.spaceGraphName());
        return SerializerFactory.serializer(this.configuration, name);
    }

    private Analyzer analyzer() {
        String name = this.configuration.get(CoreOptions.TEXT_ANALYZER);
        String mode = this.configuration.get(CoreOptions.TEXT_ANALYZER_MODE);
        LOG.debug("Loading text analyzer '{}' with mode '{}' for graph '{}'",
                  name, mode, this.spaceGraphName());
        return AnalyzerFactory.analyzer(name, mode);
    }

    protected void reloadRamtable() {
        this.reloadRamtable(false);
    }

    protected void reloadRamtable(boolean loadFromFile) {
        // Expect triggered manually, like a gremlin job
        if (this.ramtable != null) {
            this.ramtable.reload(loadFromFile, this.spaceGraphName());
        } else {
            LOG.warn("The ramtable feature is not enabled for graph {}", this);
        }
    }

    @Override
    public <C extends GraphComputer> C compute(Class<C> clazz)
            throws IllegalArgumentException {
        throw Graph.Exceptions.graphComputerNotSupported();
    }

    @Override
    public GraphComputer compute() throws IllegalArgumentException {
        throw Graph.Exceptions.graphComputerNotSupported();
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public <I extends Io> I io(final Io.Builder<I> builder) {
        return (I) builder.graph(this).onMapper(mapper ->
                                                        mapper.addRegistry(
                                                                HugeGraphIoRegistry.instance())
        ).create();
    }

    @Override
    public Vertex addVertex(Object... keyValues) {
        return this.graphTransaction().addVertex(keyValues);
    }

    @Override
    public void removeVertex(Vertex vertex) {
        this.graphTransaction().removeVertex((HugeVertex) vertex);
    }

    @Override
    public void removeVertex(String label, Object id) {
        if (label != null) {
            VertexLabel vl = this.vertexLabel(label);
            // It's OK even if exist adjacent edges `vl.existsLinkLabel()`
            if (!vl.existsIndexLabel()) {
                // Improve perf by removeVertex(id)
                Id idValue = HugeVertex.getIdValue(id);
                HugeVertex vertex = new HugeVertex(this, idValue, vl);
                this.removeVertex(vertex);
                return;
            }
        }

        this.vertex(id).remove();
    }

    @Override
    public <V> void addVertexProperty(VertexProperty<V> p) {
        this.graphTransaction().addVertexProperty((HugeVertexProperty<V>) p);
    }

    @Override
    public <V> void removeVertexProperty(VertexProperty<V> p) {
        this.graphTransaction().removeVertexProperty((HugeVertexProperty<V>) p);
    }

    @Override
    public Edge addEdge(Edge edge) {
        return this.graphTransaction().addEdge((HugeEdge) edge);
    }

    @Override
    public void canAddEdge(Edge edge) {
        // pass
    }

    @Override
    public void removeEdge(Edge edge) {
        this.graphTransaction().removeEdge((HugeEdge) edge);
    }

    @Override
    public void removeEdge(String label, Object id) {
        if (label != null) {
            EdgeLabel el = this.edgeLabel(label);
            if (!el.existsIndexLabel()) {
                // Improve perf by removeEdge(id)
                Id idValue = HugeEdge.getIdValue(id, false);
                HugeEdge edge = new HugeEdge(this, idValue, el);
                this.removeEdge(edge);
                return;
            }
        }

        this.edge(id).remove();
    }

    @Override
    public <V> void addEdgeProperty(Property<V> p) {
        this.graphTransaction().addEdgeProperty((HugeEdgeProperty<V>) p);
    }

    @Override
    public <V> void removeEdgeProperty(Property<V> p) {
        this.graphTransaction().removeEdgeProperty((HugeEdgeProperty<V>) p);
    }

    @Override
    public Vertex vertex(Object object) {
        return this.graphTransaction().queryVertex(object);
    }

    @Override
    public Iterator<Vertex> vertices(Object... objects) {
        if (objects.length == 0) {
            return this.graphTransaction().queryVertices();
        }
        return this.graphTransaction().queryVertices(objects);
    }

    @Override
    public Iterator<Vertex> vertices(Query query) {
        return this.graphTransaction().queryVertices(query);
    }

    @Override
    public Iterator<Vertex> adjacentVertex(Object id) {
        return this.graphTransaction().queryAdjacentVertices(id);
    }

    @Override
    public boolean checkAdjacentVertexExist() {
        return this.graphTransaction().checkAdjacentVertexExist();
    }

    @Override
    public Edge edge(Object object) {
        return this.graphTransaction().queryEdge(object);
    }

    @Override
    public Iterator<Edge> edges(Object... objects) {
        if (objects.length == 0) {
            return this.graphTransaction().queryEdges();
        }
        return this.graphTransaction().queryEdges(objects);
    }

    @Override
    @Watched
    public Iterator<Edge> edges(Query query) {
        return this.graphTransaction().queryEdges(query);
    }

    @Override
    public Iterator<Vertex> adjacentVertices(Iterator<Edge> edges) {
        return this.graphTransaction().queryAdjacentVertices(edges);
    }

    @Override
    public Iterator<Edge> adjacentEdges(Id vertexId) {
        return this.graphTransaction().queryEdgesByVertex(vertexId);
    }

    @Override
    public Number queryNumber(Query query) {
        return this.graphTransaction().queryNumber(query);
    }

    @Override
    public Id addPropertyKey(PropertyKey pkey) {
        assert this.spaceGraphName().equals(pkey.graph().spaceGraphName());
        if (pkey.olap()) {
            this.clearVertexCache();
        }
        return this.schemaTransaction().addPropertyKey(pkey);
    }

    @Override
    public void updatePropertyKey(PropertyKey pkey) {
        assert this.spaceGraphName().equals(pkey.graph().spaceGraphName());
        this.schemaTransaction().updatePropertyKey(pkey);
    }

    @Override
    public Id removePropertyKey(Id pkey) {
        if (this.propertyKey(pkey).olap()) {
            this.clearVertexCache();
        }
        return this.schemaTransaction().removePropertyKey(pkey);
    }

    @Override
    public Collection<PropertyKey> propertyKeys() {
        return this.schemaTransaction().getPropertyKeys();
    }

    @Override
    public PropertyKey propertyKey(Id id) {
        PropertyKey pk = this.schemaTransaction().getPropertyKey(id);
        E.checkArgument(pk != null, "Undefined property key with id: '%s'", id);
        return pk;
    }

    @Override
    public PropertyKey propertyKey(String name) {
        PropertyKey pk = this.schemaTransaction().getPropertyKey(name);
        E.checkArgument(pk != null, "Undefined property key: '%s'", name);
        return pk;
    }

    @Override
    public Id clearPropertyKey(PropertyKey propertyKey) {
        if (propertyKey.oltp()) {
            return IdGenerator.ZERO;
        }
        this.clearVertexCache();
        return this.schemaTransaction().clearOlapPk(propertyKey);
    }

    @Override
    public boolean existsPropertyKey(String name) {
        return this.schemaTransaction().getPropertyKey(name) != null;
    }

    @Override
    public void addVertexLabel(VertexLabel label) {
        assert this.spaceGraphName().equals(label.graph().spaceGraphName());
        this.schemaTransaction().addVertexLabel(label);
    }

    @Override
    public void updateVertexLabel(VertexLabel label) {
        assert this.spaceGraphName().equals(label.graph().spaceGraphName());
        this.schemaTransaction().updateVertexLabel(label);
    }

    @Override
    public Id removeVertexLabel(Id label) {
        return this.schemaTransaction().removeVertexLabel(label);
    }

    @Override
    public Collection<VertexLabel> vertexLabels() {
        return this.schemaTransaction().getVertexLabels();
    }

    @Override
    @Watched
    public VertexLabel vertexLabelOrNone(Id id) {
        VertexLabel vl = this.schemaTransaction().getVertexLabel(id);
        if (vl == null) {
            vl = VertexLabel.undefined(this, id);
        }
        return vl;
    }

    @Override
    public VertexLabel vertexLabel(Id id) {
        VertexLabel vl = this.schemaTransaction().getVertexLabel(id);
        E.checkArgument(vl != null, "Undefined vertex label with id: '%s'", id);
        return vl;
    }

    @Override
    public VertexLabel vertexLabel(String name) {
        VertexLabel vl = this.schemaTransaction().getVertexLabel(name);
        E.checkArgument(vl != null, "Undefined vertex label: '%s'", name);
        return vl;
    }

    @Override
    public boolean existsVertexLabel(String name) {
        return this.schemaTransaction().getVertexLabel(name) != null;
    }

    @Override
    public boolean existsLinkLabel(Id vertexLabel) {
        List<EdgeLabel> edgeLabels = this.schemaTransaction().getEdgeLabels();
        for (EdgeLabel edgeLabel : edgeLabels) {
            if (edgeLabel.linkWithLabel(vertexLabel)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void addEdgeLabel(EdgeLabel label) {
        assert this.spaceGraphName().equals(label.graph().spaceGraphName());
        this.schemaTransaction().addEdgeLabel(label);
    }

    @Override
    public void updateEdgeLabel(EdgeLabel label) {
        assert this.spaceGraphName().equals(label.graph().spaceGraphName());
        this.schemaTransaction().updateEdgeLabel(label);
    }

    @Override
    public Id removeEdgeLabel(Id id) {
        return this.schemaTransaction().removeEdgeLabel(id);
    }

    @Override
    public Collection<EdgeLabel> edgeLabels() {
        return this.schemaTransaction().getEdgeLabels();
    }

    @Override
    public EdgeLabel edgeLabelOrNone(Id id) {
        EdgeLabel el = this.schemaTransaction().getEdgeLabel(id);
        if (el == null) {
            el = EdgeLabel.undefined(this, id);
        }
        return el;
    }

    @Override
    public EdgeLabel edgeLabel(Id id) {
        EdgeLabel el = this.schemaTransaction().getEdgeLabel(id);
        E.checkArgument(el != null, "Undefined edge label with id: '%s'", id);
        return el;
    }

    @Override
    public EdgeLabel edgeLabel(String name) {
        EdgeLabel el = this.schemaTransaction().getEdgeLabel(name);
        E.checkArgument(el != null, "Undefined edge label: '%s'", name);
        return el;
    }

    @Override
    public boolean existsEdgeLabel(String name) {
        return this.schemaTransaction().getEdgeLabel(name) != null;
    }

    @Override
    public void addIndexLabel(SchemaLabel schemaLabel, IndexLabel indexLabel) {
        assert VertexLabel.OLAP_VL.equals(schemaLabel) ||
               this.spaceGraphName().equals(schemaLabel.graph().spaceGraphName());
        assert this.spaceGraphName().equals(indexLabel.graph().spaceGraphName());
        this.schemaTransaction().addIndexLabel(schemaLabel, indexLabel);
    }

    @Override
    public void updateIndexLabel(IndexLabel label) {
        assert this.spaceGraphName().equals(label.graph().spaceGraphName());
        this.schemaTransaction().updateIndexLabel(label);
    }

    @Override
    public Id removeIndexLabel(Id id) {
        return this.schemaTransaction().removeIndexLabel(id);
    }

    @Override
    public Id rebuildIndex(SchemaElement schema) {
        return this.schemaTransaction().rebuildIndex(schema);
    }

    @Override
    public Collection<IndexLabel> indexLabels() {
        return this.schemaTransaction().getIndexLabels();
    }

    @Override
    public IndexLabel indexLabel(Id id) {
        IndexLabel il = this.schemaTransaction().getIndexLabel(id);
        E.checkArgument(il != null, "Undefined index label with id: '%s'", id);
        return il;
    }

    @Override
    public IndexLabel indexLabel(String name) {
        IndexLabel il = this.schemaTransaction().getIndexLabel(name);
        E.checkArgument(il != null, "Undefined index label: '%s'", name);
        return il;
    }

    @Override
    public boolean existsIndexLabel(String name) {
        return this.schemaTransaction().getIndexLabel(name) != null;
    }

    @Override
    public Transaction tx() {
        return this.tx;
    }

    @Override
    public synchronized void close() throws Exception {
        if (this.closed()) {
            return;
        }

        LOG.info("Close graph {}", this);
        if (StandardAuthManager.isLocal(this.authManager)) {
            this.authManager.close();
        }
        this.taskManager.closeScheduler(this.params);
        try {
            this.closeTx();
        } finally {
            this.closed = true;
            this.storeProvider.close();
            LockUtil.destroy(this.spaceGraphName());
        }

        // Make sure that all transactions are closed in all threads
        if (!this.tx.closed()) {
            for (String key : this.tx.openedThreads) {
                LOG.warn("thread [{}] did not close transaction", key);
            }
        }
        E.checkState(this.tx.closed(),
                     "Ensure tx closed in all threads when closing graph '%s'",
                     this.spaceGraphName());

    }

    @Override
    public void create(String configPath, GlobalMasterInfo nodeInfo) {
        this.initBackend();
        this.serverStarted(nodeInfo);

        // Write config to the disk file
        String confPath = ConfigUtil.writeToFile(configPath, this.name,
                                                 this.configuration());
        this.configuration.file(confPath);
    }

    @Override
    public void drop() {
        this.clearBackend();

        HugeConfig config = this.configuration();
        this.storeProvider.onDeleteConfig(config);
        ConfigUtil.deleteFile(config.file());

        try {
            /*
             * It's hard to ensure all threads close the tx.
             * TODO:
             *  - schedule a tx-close to each thread,
             *   or
             *  - add forceClose() method to backend store.
             */
            this.close();
        } catch (Throwable e) {
            LOG.warn("Failed to close graph {} {}", this, e);
        }
    }

    @Override
    public HugeConfig cloneConfig(String newGraph) {
        HugeConfig config = (HugeConfig) this.configuration().clone();
        this.storeProvider.onCloneConfig(config, newGraph);
        return config;
    }

    public void clearSchedulerAndLock() {
        this.taskManager.forceRemoveScheduler(this.params);
        try {
            LockUtil.destroy(this.spaceGraphName());
        } catch (Exception e) {
            // Ignore
        }
    }

    @Override
    public HugeFeatures features() {
        return this.features;
    }

    @Override
    public synchronized Variables variables() {
        if (this.variables == null) {
            this.variables = new HugeVariables(this.params);
        }
        // Ensure variables() work after variables schema was cleared
        this.variables.initSchemaIfNeeded();
        return this.variables;
    }

    @Override
    public SchemaManager schema() {
        return new SchemaManager(this.schemaTransaction(), this);
    }

    public ISchemaTransaction schemaTransaction() {
        this.checkGraphNotClosed();
        /*
         * NOTE: each schema operation will be auto committed,
         * Don't need to open tinkerpop tx by readWrite() and commit manually.
         */
        return this.tx.schemaTransaction();
    }

    @Override
    public Id getNextId(HugeType type) {
        return this.schemaTransaction().getNextId(type);
    }

    @Override
    public <T> T metadata(HugeType type, String meta, Object... args) {
        return this.graphTransaction().metadata(type, meta, args);
    }

    @Override
    public TaskScheduler taskScheduler() {
        TaskScheduler scheduler = this.taskManager.getScheduler(this.params);
        E.checkState(scheduler != null,
                     "Can't find task scheduler for graph '%s'", this);
        return scheduler;
    }

    private ServerInfoManager serverInfoManager() {
        ServerInfoManager manager = this.taskManager.getServerInfoManager(this.params);
        E.checkState(manager != null,
                     "Can't find server info manager for graph '%s'", this);
        return manager;
    }

    @Override
    public AuthManager authManager() {
        // this.authManager.initSchemaIfNeeded();
        return this.authManager;
    }

    @Override
    public RoleElectionStateMachine roleElectionStateMachine() {
        return this.roleElectionStateMachine;
    }

    @Override
    public void switchAuthManager(AuthManager authManager) {
        this.authManager = authManager;
    }

    @Override
    public RaftGroupManager raftGroupManager() {
        if (!(this.storeProvider instanceof RaftBackendStoreProvider)) {
            return null;
        }
        RaftBackendStoreProvider provider =
                ((RaftBackendStoreProvider) this.storeProvider);
        return provider.raftNodeManager();
    }

    @Override
    public HugeConfig configuration() {
        return this.configuration;
    }

    @Override
    public String toString() {
        return StringFactory.graphString(this, this.spaceGraphName());
    }

    @Override
    public final void proxy(HugeGraph graph) {
        this.params.graph(graph);
    }

    @Override
    public boolean sameAs(HugeGraph graph) {
        return this == graph;
    }

    @Override
    public long now() {
        return ((TinkerPopTransaction) this.tx()).openedTime();
    }

    @Override
    public <K, V> V option(TypedOption<K, V> option) {
        HugeConfig config = this.configuration();
        if (!ALLOWED_CONFIGS.contains(option)) {
            throw new NotAllowException("Not allowed to access config: %s",
                                        option.name());
        }
        return config.get(option);
    }

    @Override
    public void registerRpcServices(RpcServiceConfig4Server serverConfig,
                                    RpcServiceConfig4Client clientConfig) {
        /*
         * Skip register cache-rpc service if it's non-shared storage,
         * because we assume cache of non-shared storage is updated by raft.
         */
        if (!this.backendStoreFeatures().supportsSharedStorage()) {
            return;
        }

        Class<GraphCacheNotifier> clazz1 = GraphCacheNotifier.class;
        // The proxy is sometimes unavailable (issue #664)
        CacheNotifier proxy = clientConfig.serviceProxy(this.spaceGraphName(), clazz1);
        serverConfig.addService(this.spaceGraphName(), clazz1, new HugeGraphCacheNotifier(
                this.graphEventHub, proxy));

        Class<SchemaCacheNotifier> clazz2 = SchemaCacheNotifier.class;
        proxy = clientConfig.serviceProxy(this.spaceGraphName(), clazz2);
        serverConfig.addService(this.spaceGraphName(), clazz2, new HugeSchemaCacheNotifier(
                this.schemaEventHub, proxy));
    }

    private void waitUntilAllTasksCompleted() {
        long timeout = this.configuration.get(CoreOptions.TASK_WAIT_TIMEOUT);
        try {
            this.taskScheduler().waitUntilAllTasksCompleted(timeout);
        } catch (TimeoutException e) {
            throw new HugeException("Failed to wait all tasks to complete", e);
        }
    }

    private static final class Txs {

        private final ISchemaTransaction schemaTx;
        private final SysTransaction systemTx;
        private final GraphTransaction graphTx;
        private long openedTime;

        public Txs(ISchemaTransaction schemaTx, SysTransaction systemTx,
                   GraphTransaction graphTx) {
            assert schemaTx != null && systemTx != null && graphTx != null;
            this.schemaTx = schemaTx;
            this.systemTx = systemTx;
            this.graphTx = graphTx;
            this.openedTime = DateUtil.now().getTime();
        }

        public void commit() {
            this.graphTx.commit();
        }

        public void rollback() {
            this.graphTx.rollback();
        }

        public void close() {
            try {
                this.graphTx.close();
            } catch (Exception e) {
                LOG.error("Failed to close GraphTransaction", e);
            }

            try {
                this.systemTx.close();
            } catch (Exception e) {
                LOG.error("Failed to close SystemTransaction", e);
            }

            try {
                this.schemaTx.close();
            } catch (Exception e) {
                LOG.error("Failed to close SchemaTransaction", e);
            }
        }

        public void openedTime(long time) {
            this.openedTime = time;
        }

        public long openedTime() {
            return this.openedTime;
        }

        @Override
        public String toString() {
            return String.format("{schemaTx=%s,systemTx=%s,graphTx=%s}",
                                 this.schemaTx, this.systemTx, this.graphTx);
        }
    }

    private static class SysTransaction extends GraphTransaction {

        public SysTransaction(HugeGraphParams graph, BackendStore store) {
            super(graph, store);
            this.autoCommit(true);
        }
    }

    private static class AbstractCacheNotifier implements CacheNotifier {

        public static final Logger LOG = Log.logger(AbstractCacheNotifier.class);

        private final EventHub hub;
        private final EventListener cacheEventListener;

        public AbstractCacheNotifier(EventHub hub, CacheNotifier proxy) {
            this.hub = hub;
            this.cacheEventListener = event -> {
                try {
                    LOG.info("Received event: {}", event);
                    Object[] args = event.args();
                    E.checkArgument(args.length > 0 && args[0] instanceof String,
                                    "Expect event action argument");
                    String action = (String) args[0];
                    LOG.debug("Event action: {}", action);
                    if (Cache.ACTION_INVALIDED.equals(action)) {
                        event.checkArgs(String.class, HugeType.class, Object.class);
                        HugeType type = (HugeType) args[1];
                        Object ids = args[2];
                        if (ids instanceof Id[]) {
                            LOG.debug("Calling proxy.invalid2 with type: {}, IDs: {}", type,
                                      Arrays.toString((Id[]) ids));
                            proxy.invalid2(type, (Id[]) ids);
                        } else if (ids instanceof Id) {
                            LOG.debug("Calling proxy.invalid with type: {}, ID: {}", type, ids);
                            proxy.invalid(type, (Id) ids);
                        } else {
                            LOG.error("Unexpected argument: {}", ids);
                            E.checkArgument(false, "Unexpected argument: %s", ids);
                        }
                        return true;
                    } else if (Cache.ACTION_CLEARED.equals(action)) {
                        event.checkArgs(String.class, HugeType.class);
                        HugeType type = (HugeType) args[1];
                        LOG.debug("Calling proxy.clear with type: {}", type);
                        proxy.clear(type);
                        return true;
                    }
                } catch (Exception e) {
                    LOG.error("Error processing cache event: {}", e.getMessage(), e);
                }
                LOG.warn("Event {} not handled", event);
                return false;
            };
            this.hub.listen(Events.CACHE, this.cacheEventListener);
            LOG.info("Cache event listener registered successfully. cacheEventListener {}",
                     this.cacheEventListener);
        }

        @Override
        public void close() {
            this.hub.unlisten(Events.CACHE, this.cacheEventListener);
        }

        @Override
        public void invalid(HugeType type, Id id) {
            this.hub.notify(Events.CACHE, Cache.ACTION_INVALID, type, id);
        }

        @Override
        public void invalid2(HugeType type, Object[] ids) {
            this.hub.notify(Events.CACHE, Cache.ACTION_INVALID, type, ids);
        }

        @Override
        public void clear(HugeType type) {
            this.hub.notify(Events.CACHE, Cache.ACTION_CLEAR, type);
        }

        @Override
        public void reload() {
            // pass
        }
    }

    private static class HugeSchemaCacheNotifier
            extends AbstractCacheNotifier
            implements SchemaCacheNotifier {

        public HugeSchemaCacheNotifier(EventHub hub, CacheNotifier proxy) {
            super(hub, proxy);
        }
    }

    private static class HugeGraphCacheNotifier
            extends AbstractCacheNotifier
            implements GraphCacheNotifier {

        public HugeGraphCacheNotifier(EventHub hub, CacheNotifier proxy) {
            super(hub, proxy);
        }
    }

    private class StandardHugeGraphParams implements HugeGraphParams {

        private final EphemeralJobQueue ephemeralJobQueue = new EphemeralJobQueue(this);
        private HugeGraph graph = StandardHugeGraph.this;

        private void graph(HugeGraph graph) {
            this.graph = graph;
        }

        @Override
        public HugeGraph graph() {
            return this.graph;
        }

        @Override
        public String name() {
            return StandardHugeGraph.this.name();
        }

        @Override
        public String spaceGraphName() {
            return StandardHugeGraph.this.spaceGraphName();
        }

        @Override
        public GraphMode mode() {
            return StandardHugeGraph.this.mode();
        }

        @Override
        public GraphReadMode readMode() {
            return StandardHugeGraph.this.readMode();
        }

        @Override
        public ISchemaTransaction schemaTransaction() {
            return StandardHugeGraph.this.schemaTransaction();
        }

        @Override
        public GraphTransaction systemTransaction() {
            return StandardHugeGraph.this.systemTransaction();
        }

        @Override
        public GraphTransaction graphTransaction() {
            return StandardHugeGraph.this.graphTransaction();
        }

        @Override
        public GraphTransaction openTransaction() {
            // Open a new one
            return StandardHugeGraph.this.openGraphTransaction();
        }

        @Override
        public void closeTx() {
            StandardHugeGraph.this.closeTx();
        }

        @Override
        public boolean started() {
            return StandardHugeGraph.this.started();
        }

        @Override
        public boolean closed() {
            return StandardHugeGraph.this.closed();
        }

        @Override
        public boolean initialized() {
            return StandardHugeGraph.this.graphTransaction().storeInitialized();
        }

        @Override
        public BackendFeatures backendStoreFeatures() {
            return StandardHugeGraph.this.backendStoreFeatures();
        }

        @Override
        public BackendStore loadSchemaStore() {
            return StandardHugeGraph.this.loadSchemaStore();
        }

        @Override
        public BackendStore loadGraphStore() {
            return StandardHugeGraph.this.loadGraphStore();
        }

        @Override
        public BackendStore loadSystemStore() {
            return StandardHugeGraph.this.loadSystemStore();
        }

        @Override
        public EventHub schemaEventHub() {
            return StandardHugeGraph.this.schemaEventHub;
        }

        @Override
        public EventHub graphEventHub() {
            return StandardHugeGraph.this.graphEventHub;
        }

        @Override
        public EventHub indexEventHub() {
            return StandardHugeGraph.this.indexEventHub;
        }

        @Override
        public HugeConfig configuration() {
            return StandardHugeGraph.this.configuration();
        }

        @Override
        public ServerInfoManager serverManager() {
            return StandardHugeGraph.this.serverInfoManager();
        }

        @Override
        public LocalCounter counter() {
            return StandardHugeGraph.this.localCounter;
        }

        @Override
        public AbstractSerializer serializer() {
            return StandardHugeGraph.this.serializer();
        }

        @Override
        public Analyzer analyzer() {
            return StandardHugeGraph.this.analyzer();
        }

        @Override
        public RateLimiter writeRateLimiter() {
            return StandardHugeGraph.this.writeRateLimiter;
        }

        @Override
        public RateLimiter readRateLimiter() {
            return StandardHugeGraph.this.readRateLimiter;
        }

        @Override
        public RamTable ramtable() {
            return StandardHugeGraph.this.ramtable;
        }

        @Override
        public <T> void submitEphemeralJob(EphemeralJob<T> job) {
            this.ephemeralJobQueue.add(job);
        }

        @Override
        public String schedulerType() {
            return StandardHugeGraph.this.schedulerType;
        }
    }

    private class TinkerPopTransaction extends AbstractThreadLocalTransaction {

        // Times opened from the upper layer
        private final AtomicInteger refs;
        private final ConcurrentHashMap.KeySetView<String, Boolean> openedThreads =
                ConcurrentHashMap.newKeySet();
        // Flag opened of each thread
        private final ThreadLocal<Boolean> opened;
        // Backend transactions
        private final ThreadLocal<Txs> transactions;

        public TinkerPopTransaction(Graph graph) {
            super(graph);

            this.refs = new AtomicInteger();
            this.opened = ThreadLocal.withInitial(() -> false);
            this.transactions = ThreadLocal.withInitial(() -> null);
        }

        public boolean closed() {
            int refs = this.refs.get();
            assert refs >= 0 : refs;
            return refs == 0;
        }

        /**
         * Commit tx if batch size reaches the specified value,
         * it may be used by Gremlin
         */
        @SuppressWarnings("unused")
        public void commitIfGtSize(int size) {
            // Only commit graph transaction data (schema auto committed)
            this.graphTransaction().commitIfGtSize(size);
        }

        @Override
        public void commit() {
            try {
                super.commit();
            } finally {
                this.setClosed();
            }
        }

        @Override
        public void rollback() {
            try {
                super.rollback();
            } finally {
                this.setClosed();
            }
        }

        @Override
        public <G extends Graph> G createThreadedTx() {
            throw Transaction.Exceptions.threadedTransactionsNotSupported();
        }

        @Override
        public boolean isOpen() {
            return this.opened.get();
        }

        @Override
        protected void doOpen() {
            this.getOrNewTransaction();
            this.setOpened();
        }

        @Override
        protected void doCommit() {
            this.verifyOpened();
            this.getOrNewTransaction().commit();
        }

        @Override
        protected void doRollback() {
            this.verifyOpened();
            this.getOrNewTransaction().rollback();
        }

        @Override
        protected void doClose() {
            this.verifyOpened();

            try {
                // Calling super.doClose() will clear listeners
                super.doClose();
            } finally {
                this.resetState();
            }
        }

        @Override
        public String toString() {
            return String.format("TinkerPopTransaction{opened=%s, txs=%s}",
                                 this.opened.get(), this.transactions.get());
        }

        public long openedTime() {
            return this.transactions.get().openedTime();
        }

        private void verifyOpened() {
            if (!this.isOpen()) {
                throw new HugeException("Transaction has not been opened");
            }
        }

        private void resetState() {
            this.setClosed();
            this.readWriteConsumerInternal.set(READ_WRITE_BEHAVIOR.AUTO);
            this.closeConsumerInternal.set(CLOSE_BEHAVIOR.ROLLBACK);
        }

        private void setOpened() {
            // The backend tx may be reused, here just set a flag
            assert !this.opened.get();
            this.opened.set(true);
            this.transactions.get().openedTime(DateUtil.now().getTime());
            this.openedThreads.add(Thread.currentThread().getName());
            this.refs.incrementAndGet();
        }

        private void setClosed() {
            // Just set flag opened=false to reuse the backend tx
            if (this.opened.get()) {
                this.opened.set(false);
                this.openedThreads.remove(Thread.currentThread().getName());
                this.refs.decrementAndGet();
            }
        }

        private ISchemaTransaction schemaTransaction() {
            return this.getOrNewTransaction().schemaTx;
        }

        private SysTransaction systemTransaction() {
            return this.getOrNewTransaction().systemTx;
        }

        private GraphTransaction graphTransaction() {
            return this.getOrNewTransaction().graphTx;
        }

        private Txs getOrNewTransaction() {
            /*
             * NOTE: this method may be called even tx is not opened,
             * the reason is for reusing backend tx.
             * so we don't call this.verifyOpened() here.
             */

            Txs txs = this.transactions.get();
            if (txs == null) {
                ISchemaTransaction schemaTransaction = null;
                SysTransaction sysTransaction = null;
                GraphTransaction graphTransaction = null;
                try {
                    schemaTransaction = openSchemaTransaction();
                    sysTransaction = openSystemTransaction();
                    graphTransaction = openGraphTransaction();
                    txs = new Txs(schemaTransaction, sysTransaction,
                                  graphTransaction);
                } catch (Throwable e) {
                    if (schemaTransaction != null) {
                        schemaTransaction.close();
                    }
                    if (sysTransaction != null) {
                        sysTransaction.close();
                    }
                    if (graphTransaction != null) {
                        graphTransaction.close();
                    }
                    throw e;
                }
                this.transactions.set(txs);
            }
            return txs;
        }

        private void destroyTransaction() {
            if (this.isOpen()) {
                throw new HugeException(
                        "Transaction should be closed before destroying");
            }

            // Do close if needed, then remove the reference
            Txs txs = this.transactions.get();
            if (txs != null) {
                txs.close();
            }
            this.transactions.remove();
        }
    }
}
