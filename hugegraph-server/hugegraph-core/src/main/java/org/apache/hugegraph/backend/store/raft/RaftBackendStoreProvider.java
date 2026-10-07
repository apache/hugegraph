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

import java.util.Set;
import java.util.concurrent.Future;

import org.apache.hugegraph.HugeGraphParams;
import org.apache.hugegraph.exception.BackendException;
import org.apache.hugegraph.backend.store.BackendStore;
import org.apache.hugegraph.backend.store.BackendStoreProvider;
import org.apache.hugegraph.backend.store.raft.rpc.RaftRequests.StoreAction;
import org.apache.hugegraph.backend.store.raft.rpc.RaftRequests.StoreType;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.event.EventHub;
import org.apache.hugegraph.event.EventListener;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Events;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

import com.alipay.remoting.rpc.RpcServer;
import com.google.common.collect.ImmutableSet;

public class RaftBackendStoreProvider implements BackendStoreProvider {

    private static final Logger LOG = Log.logger(RaftBackendStoreProvider.class);

    private final BackendStoreProvider provider;
    private final RaftContext context;

    private RaftBackendStore schemaStore;
    private RaftBackendStore graphStore;
    private RaftBackendStore systemStore;

    public RaftBackendStoreProvider(HugeGraphParams params,
                                    BackendStoreProvider provider) {
        this.provider = provider;
        this.schemaStore = null;
        this.graphStore = null;
        this.systemStore = null;
        this.context = new RaftContext(params);
    }

    public RaftGroupManager raftNodeManager() {
        return this.context().raftNodeManager();
    }

    private RaftContext context() {
        if (this.context == null) {
            E.checkState(false, "Please ensure init raft context");
        }
        return this.context;
    }

    private Set<RaftBackendStore> stores() {
        return ImmutableSet.of(this.schemaStore, this.graphStore,
                               this.systemStore);
    }

    private void checkOpened() {
        E.checkState(this.graph() != null &&
                     this.schemaStore != null &&
                     this.graphStore != null &&
                     this.systemStore != null,
                     "The RaftBackendStoreProvider has not been opened");
    }

    private void checkNonSharedStore(BackendStore store) {
        E.checkArgument(!store.features().supportsSharedStorage(),
                        "Can't enable raft mode with %s backend",
                        this.type());
    }

    @Override
    public String type() {
        return this.provider.type();
    }

    @Override
    public String driverVersion() {
        return this.provider.driverVersion();
    }

    @Override
    public String storedVersion() {
        return this.provider.storedVersion();
    }

    @Override
    public String graph() {
        return this.provider.graph();
    }

    @Override
    public synchronized BackendStore loadSchemaStore(HugeConfig config) {
        if (this.schemaStore == null) {
            LOG.info("Init raft backend schema store");
            BackendStore store = this.provider.loadSchemaStore(config);
            this.checkNonSharedStore(store);
            this.schemaStore = new RaftBackendStore(store, this.context());
            this.context().addStore(StoreType.SCHEMA, this.schemaStore);
        }
        return this.schemaStore;
    }

    @Override
    public synchronized BackendStore loadGraphStore(HugeConfig config) {
        if (this.graphStore == null) {
            LOG.info("Init raft backend graph store");
            BackendStore store = this.provider.loadGraphStore(config);
            this.checkNonSharedStore(store);
            this.graphStore = new RaftBackendStore(store, this.context());
            this.context().addStore(StoreType.GRAPH, this.graphStore);
        }
        return this.graphStore;
    }

    @Override
    public synchronized BackendStore loadSystemStore(HugeConfig config) {
        if (this.systemStore == null) {
            LOG.info("Init raft backend system store");
            BackendStore store = this.provider.loadSystemStore(config);
            this.checkNonSharedStore(store);
            this.systemStore = new RaftBackendStore(store, this.context());
            this.context().addStore(StoreType.SYSTEM, this.systemStore);
        }
        return this.systemStore;
    }

    @Override
    public void open(String name) {
        this.provider.open(name);
    }

    @Override
    public void waitReady(RpcServer rpcServer) {
        this.context().initRaftNode(rpcServer);
        LOG.info("The raft node is initialized");

        this.context().waitRaftNodeStarted();
        LOG.info("The raft store is started");
    }

    @Override
    public void close() {
        this.provider.close();
        if (this.context != null) {
            this.context.close();
        }
    }

    @Override
    public void init() {
        this.checkOpened();
        for (RaftBackendStore store : this.stores()) {
            store.init();
        }
        this.notifyAndWaitEvent(Events.STORE_INIT);

        LOG.debug("Graph '{}' store has been initialized", this.graph());
    }

    @Override
    public void clear() {
        this.checkOpened();
        for (RaftBackendStore store : this.stores()) {
            // Just clear tables of store, not clear space
            store.clear(false);
        }
        for (RaftBackendStore store : this.stores()) {
            // Only clear space of store
            store.clear(true);
        }
        this.notifyAndWaitEvent(Events.STORE_CLEAR);

        LOG.debug("Graph '{}' store has been cleared", this.graph());
    }

    @Override
    public void truncate() {
        this.checkOpened();
        for (RaftBackendStore store : this.stores()) {
            store.truncate();
        }
        this.notifyAndWaitEvent(Events.STORE_TRUNCATE);
        LOG.debug("Graph '{}' store has been truncated", this.graph());
        /*
         * Take the initiative to generate a snapshot, it can avoid this
         * situation: when the server restart need to read the database
         * (such as checkBackendVersionInfo), it happens that raft replays
         * the truncate log, at the same time, the store has been cleared
         * (truncate) but init-store has not been completed, which will
         * cause reading errors.
         * When restarting, load the snapshot first and then read backend,
         * will not encounter such an intermediate state.
         */
        this.createSnapshot();
    }

    @Override
    public boolean initialized() {
        return this.provider.initialized() && this.context != null;
    }

    @Override
    public void createSnapshot() {
        // TODO: snapshot for StoreType.ALL instead of StoreType.GRAPH
        StoreCommand command = new StoreCommand(StoreType.GRAPH,
                                                StoreAction.SNAPSHOT, null);
        RaftStoreClosure closure = new RaftStoreClosure(command);
        RaftClosure<?> future = this.context().node().submitAndWait(command,
                                                                    closure);
        E.checkState(future != null, "The snapshot future can't be null");
        try {
            future.waitFinished();
            LOG.debug("Graph '{}' has writed snapshot", this.graph());
        } catch (Throwable e) {
            throw new BackendException("Failed to create snapshot", e);
        }
    }

    @Override
    public void onCloneConfig(HugeConfig config, String newGraph) {
        this.provider.onCloneConfig(config, newGraph);
    }

    @Override
    public void onDeleteConfig(HugeConfig config) {
        this.provider.onDeleteConfig(config);
    }

    @Override
    public void resumeSnapshot() {
        // Jraft doesn't expose API to load snapshot
        throw new UnsupportedOperationException("resumeSnapshot");
    }

    @Override
    public void listen(EventListener listener) {
        this.provider.listen(listener);
    }

    @Override
    public void unlisten(EventListener listener) {
        this.provider.unlisten(listener);
    }

    @Override
    public EventHub storeEventHub() {
        return this.provider.storeEventHub();
    }

    protected final void notifyAndWaitEvent(String event) {
        Future<?> future = this.storeEventHub().notify(event, this);
        try {
            future.get();
        } catch (Throwable e) {
            LOG.warn("Error when waiting for event execution: {}", event, e);
        }
    }
}
