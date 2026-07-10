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

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

import org.apache.hugegraph.HugeException;
import org.apache.hugegraph.HugeGraphParams;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.id.IdGenerator;
import org.apache.hugegraph.backend.tx.GraphTransaction;
import org.apache.hugegraph.exception.ConnectionException;
import org.apache.hugegraph.masterelection.GlobalMasterInfo;
import org.apache.hugegraph.type.define.NodeRole;
import org.apache.hugegraph.util.E;

public class ServerInfoManager {

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
        // ServerInfo is soft-disabled; keep this method for compatibility.
    }

    public synchronized boolean close() {
        // ServerInfo persistence is soft-deprecated; init() and heartbeat()
        // are no-ops, so there's nothing to clean up in close().
        this.closed = true;
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

    private GraphTransaction tx() {
        assert Thread.currentThread().getName().contains("server-info-db-worker");
        return this.graph.systemTransaction();
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
}
