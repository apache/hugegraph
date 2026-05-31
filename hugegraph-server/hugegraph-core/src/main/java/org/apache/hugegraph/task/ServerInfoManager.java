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

package org.apache.hugegraph.task;

import static org.apache.hugegraph.backend.query.Query.NO_LIMIT;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

import org.apache.hugegraph.HugeException;
import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.HugeGraphParams;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.id.IdGenerator;
import org.apache.hugegraph.backend.page.PageInfo;
import org.apache.hugegraph.backend.query.Condition;
import org.apache.hugegraph.backend.query.ConditionQuery;
import org.apache.hugegraph.backend.query.QueryResults;
import org.apache.hugegraph.backend.tx.GraphTransaction;
import org.apache.hugegraph.exception.ConnectionException;
import org.apache.hugegraph.iterator.MapperIterator;
import org.apache.hugegraph.masterelection.GlobalMasterInfo;
import org.apache.hugegraph.schema.PropertyKey;
import org.apache.hugegraph.schema.VertexLabel;
import org.apache.hugegraph.structure.HugeVertex;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.type.define.HugeKeys;
import org.apache.hugegraph.type.define.NodeRole;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Log;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;

import com.google.common.collect.ImmutableMap;

public class ServerInfoManager {

    private static final Logger LOG = Log.logger(ServerInfoManager.class);

    public static final long MAX_SERVERS = 100000L;
    public static final long PAGE_SIZE = 10L;

    private final HugeGraphParams graph;
    private final ExecutorService dbExecutor;

    private volatile GlobalMasterInfo globalNodeInfo;

    private volatile boolean closed;

    public ServerInfoManager(HugeGraphParams graph, ExecutorService dbExecutor) {
        E.checkNotNull(graph, "graph");
        E.checkNotNull(dbExecutor, "db executor");

        this.graph = graph;
        this.dbExecutor = dbExecutor;

        this.globalNodeInfo = null;

        this.closed = false;
    }

    public void init() {
        HugeServerInfo.schema(this.graph).initSchemaIfNeeded();
    }

    public synchronized boolean close() {
        this.closed = true;
        if (!this.dbExecutor.isShutdown()) {
            this.call(() -> {
                try {
                    this.tx().close();
                } catch (ConnectionException ignored) {
                    // ConnectionException means no connection established
                }
                this.graph.closeTx();
                return null;
            });
        }
        return true;
    }

    public synchronized void initServerInfo(GlobalMasterInfo nodeInfo) {
        E.checkArgument(nodeInfo != null, "The global node info can't be null");

        this.globalNodeInfo = nodeInfo;
    }

    public synchronized void changeServerRole(NodeRole nodeRole) {
        if (this.closed || this.globalNodeInfo == null) {
            return;
        }

        this.globalNodeInfo.changeNodeRole(nodeRole);
    }

    public GlobalMasterInfo globalNodeRoleInfo() {
        return this.globalNodeInfo;
    }

    public Id selfNodeId() {
        if (this.globalNodeInfo == null) {
            return null;
        }
        Id nodeId = this.globalNodeInfo.nodeId();
        if (nodeId == null) {
            return null;
        }
        // Scope server id to graph to avoid cross-graph collision
        return IdGenerator.of(this.graph.spaceGraphName() + "/" +
                             nodeId.asString());
    }

    public NodeRole selfNodeRole() {
        if (this.globalNodeInfo == null) {
            return null;
        }
        return this.globalNodeInfo.nodeRole();
    }

    public boolean selfIsMaster() {
        return this.selfNodeRole() != null && this.selfNodeRole().master();
    }

    public synchronized void heartbeat() {
        // ServerInfo heartbeat is deprecated for local scheduling.
    }

    public int calcMaxLoad() {
        // TODO: calc max load based on CPU and Memory resources
        return 10000;
    }

    protected boolean graphIsReady() {
        return !this.closed && this.graph.started() && this.graph.initialized();
    }

    private GraphTransaction tx() {
        assert Thread.currentThread().getName().contains("server-info-db-worker");
        return this.graph.systemTransaction();
    }

    private HugeServerInfo saveServerInfo(Id serverId, NodeRole serverRole) {
        HugeServerInfo serverInfo = new HugeServerInfo(serverId, serverRole);
        serverInfo.maxLoad(this.calcMaxLoad());
        this.save(serverInfo);

        LOG.info("Init server info: {}", serverInfo);
        return serverInfo;
    }

    private Id save(HugeServerInfo serverInfo) {
        return this.call(() -> {
            // Construct vertex from server info
            HugeServerInfo.Schema schema = HugeServerInfo.schema(this.graph);
            if (!schema.existVertexLabel(HugeServerInfo.P.SERVER)) {
                throw new HugeException("Schema is missing for %s '%s'",
                                        HugeServerInfo.P.SERVER, serverInfo);
            }
            HugeVertex vertex = this.tx().constructVertex(false, serverInfo.asArray());
            // Add or update server info in backend store
            vertex = this.tx().addVertex(vertex);
            return vertex.id();
        });
    }

    private <V> V call(Callable<V> callable) {
        assert !Thread.currentThread().getName().startsWith(
                "server-info-db-worker") : "can't call by itself";
        try {
            // Pass context for db thread
            callable = new TaskManager.ContextCallable<>(callable);
            // Ensure all db operations are executed in dbExecutor thread(s)
            return this.dbExecutor.submit(callable).get();
        } catch (Throwable e) {
            throw new HugeException("Failed to update/query server info: %s",
                                    e, e.toString());
        }
    }

    private HugeServerInfo selfServerInfo() {
        HugeServerInfo selfServerInfo = this.serverInfo(this.selfNodeId());
        if (selfServerInfo == null && this.selfNodeId() != null) {
            LOG.warn("ServerInfo is missing: {}", this.selfNodeId());
        }
        return selfServerInfo;
    }

    private HugeServerInfo serverInfo(Id serverId) {
        return this.call(() -> {
            Iterator<Vertex> vertices = this.tx().queryServerInfos(serverId);
            Vertex vertex = QueryResults.one(vertices);
            if (vertex == null) {
                return null;
            }
            return HugeServerInfo.fromVertex(vertex);
        });
    }

    private HugeServerInfo removeSelfServerInfo() {
        /*
         * Check this.selfServerId != null to avoid graph.initialized() call.
         * NOTE: graph.initialized() may throw exception if we can't connect to
         * backend store, initServerInfo() is not called in this case, so
         * this.selfServerId is null at this time.
         */
        if (this.selfNodeId() != null && this.graph.initialized()) {
            return this.removeServerInfo(this.selfNodeId());
        }
        return null;
    }

    private HugeServerInfo removeServerInfo(Id serverId) {
        if (serverId == null) {
            return null;
        }
        LOG.info("Remove server info: {}", serverId);
        return this.call(() -> {
            Iterator<Vertex> vertices = this.tx().queryServerInfos(serverId);
            Vertex vertex = QueryResults.one(vertices);
            if (vertex == null) {
                return null;
            }
            this.tx().removeVertex((HugeVertex) vertex);
            return HugeServerInfo.fromVertex(vertex);
        });
    }

    protected Iterator<HugeServerInfo> serverInfos(long limit, String page) {
        return this.serverInfos(ImmutableMap.of(), limit, page);
    }

    private Iterator<HugeServerInfo> serverInfos(Map<String, Object> conditions,
                                                 long limit, String page) {
        return this.call(() -> {
            ConditionQuery query;
            if (this.graph.backendStoreFeatures().supportsTaskAndServerVertex()) {
                query = new ConditionQuery(HugeType.SERVER);
            } else {
                query = new ConditionQuery(HugeType.VERTEX);
            }
            if (page != null) {
                query.page(page);
            }

            HugeGraph graph = this.graph.graph();
            VertexLabel vl = graph.vertexLabel(HugeServerInfo.P.SERVER);
            query.eq(HugeKeys.LABEL, vl.id());
            for (Map.Entry<String, Object> entry : conditions.entrySet()) {
                PropertyKey pk = graph.propertyKey(entry.getKey());
                query.query(Condition.eq(pk.id(), entry.getValue()));
            }
            query.showHidden(true);
            if (limit != NO_LIMIT) {
                query.limit(limit);
            }
            Iterator<Vertex> vertices = this.tx().queryServerInfos(query);
            Iterator<HugeServerInfo> servers =
                    new MapperIterator<>(vertices, HugeServerInfo::fromVertex);
            // Convert iterator to list to avoid across thread tx accessed
            return QueryResults.toList(servers);
        });
    }

    private boolean supportsPaging() {
        return this.graph.graph().backendStoreFeatures().supportsQueryByPage();
    }
}
