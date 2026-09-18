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

package org.apache.hugegraph.backend.store.hbase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import org.apache.hadoop.hbase.NamespaceExistException;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hugegraph.backend.BackendException;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.query.Query;
import org.apache.hugegraph.backend.store.AbstractBackendStore;
import org.apache.hugegraph.backend.store.BackendAction;
import org.apache.hugegraph.backend.store.BackendEntry;
import org.apache.hugegraph.backend.store.BackendFeatures;
import org.apache.hugegraph.backend.store.BackendMutation;
import org.apache.hugegraph.backend.store.BackendStoreProvider;
import org.apache.hugegraph.backend.store.BackendTable;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.exception.ConnectionException;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

public abstract class HbaseStore extends AbstractBackendStore<HbaseSessions.Session> {

    private static final Logger LOG = Log.logger(HbaseStore.class);

    private final BackendFeatures features;

    private final String store;
    private final String namespace;

    private final BackendStoreProvider provider;
    private final Map<HugeType, HbaseTable> tables;
    private short vertexLogicPartitions;
    private short edgeLogicPartitions;

    private HbaseSessions sessions;

    public HbaseStore(BackendStoreProvider provider,
                      String namespace, String store, boolean enablePartition) {
        this.tables = new HashMap<>();

        this.provider = provider;
        this.namespace = namespace;
        this.store = store;
        this.sessions = null;
        this.features = new HbaseFeatures(enablePartition);

        this.registerMetaHandlers();
        LOG.debug("Store loaded: {}", store);
    }

    private void registerMetaHandlers() {
        this.registerMetaHandler("metrics", (session, meta, args) -> {
            HbaseMetrics metrics = new HbaseMetrics(this.sessions);
            return metrics.metrics();
        });

        this.registerMetaHandler("compact", (session, meta, args) -> {
            HbaseMetrics metrics = new HbaseMetrics(this.sessions);
            return metrics.compact(this.tableNames());
        });
    }

    protected void registerTableManager(HugeType type, HbaseTable table) {
        this.tables.put(type, table);
    }

    @Override
    protected final HbaseTable table(HugeType type) {
        assert type != null;
        HbaseTable table = this.tables.get(convertTaskOrServerToVertex(type));
        if (table == null) {
            throw new BackendException("Unsupported table type: %s", type);
        }
        return table;
    }

    @Override
    protected HbaseSessions.Session session(HugeType type) {
        this.checkOpened();
        return this.sessions.session();
    }

    protected List<String> tableNames() {
        return this.tables.values().stream().map(BackendTable::table)
                          .collect(Collectors.toList());
    }

    /**
     * The tables to truncate when the graph is cleared, all the tables of
     * the store by default. A store that keeps backend metadata which must
     * survive a clear excludes it here, see {@link HbaseSystemStore}.
     */
    protected List<String> tableNamesToTruncate() {
        return this.tableNames();
    }

    public String namespace() {
        return this.namespace;
    }

    @Override
    public String store() {
        return this.store;
    }

    @Override
    public String database() {
        return this.namespace;
    }

    @Override
    public BackendStoreProvider provider() {
        return this.provider;
    }

    @Override
    public BackendFeatures features() {
        return features;
    }

    @Override
    public synchronized void open(HugeConfig config) {
        E.checkNotNull(config, "config");
        this.vertexLogicPartitions = config.get(HbaseOptions.HBASE_VERTEX_PARTITION).shortValue();
        this.edgeLogicPartitions = config.get(HbaseOptions.HBASE_EDGE_PARTITION).shortValue();

        if (this.sessions == null) {
            this.sessions = new HbaseSessions(config, this.namespace, this.store);
        }

        assert this.sessions != null;
        if (!this.sessions.closed()) {
            LOG.debug("Store {} has been opened before", this.store);
            this.sessions.useSession();
            return;
        }

        try {
            // NOTE: won't throw error even if connection refused
            this.sessions.open();
        } catch (Throwable e) {
            LOG.error("Failed to open HBase '{}'", this.store, e);
            throw new ConnectionException("Failed to connect to HBase", e);
        }

        this.sessions.session().open();
        LOG.debug("Store opened: {}", this.store);
    }

    @Override
    public void close() {
        this.checkOpened();
        this.sessions.close();

        LOG.debug("Store closed: {}", this.store);
    }

    @Override
    public boolean opened() {
        this.checkConnectionOpened();
        return this.sessions.session().opened();
    }

    @Override
    public void mutate(BackendMutation mutation) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Store {} mutation: {}", this.store, mutation);
        }

        this.checkOpened();
        HbaseSessions.Session session = this.sessions.session();

        for (Iterator<BackendAction> it = mutation.mutation(); it.hasNext(); ) {
            this.mutate(session, it.next());
        }
    }

    private void mutate(HbaseSessions.Session session, BackendAction item) {
        BackendEntry entry = item.entry();
        HbaseTable table = this.table(entry.type());

        switch (item.action()) {
            case INSERT:
                table.insert(session, entry);
                break;
            case DELETE:
                table.delete(session, entry);
                break;
            case APPEND:
                table.append(session, entry);
                break;
            case ELIMINATE:
                table.eliminate(session, entry);
                break;
            case UPDATE_IF_PRESENT:
                table.updateIfPresent(session, entry);
                break;
            case UPDATE_IF_ABSENT:
                table.updateIfAbsent(session, entry);
                break;
            default:
                throw new AssertionError(String.format(
                        "Unsupported mutate action: %s", item.action()));
        }
    }

    @Override
    public Iterator<BackendEntry> query(Query query) {
        this.checkOpened();

        HbaseSessions.Session session = this.sessions.session();
        HbaseTable table = this.table(HbaseTable.tableType(query));
        return table.query(session, query);
    }

    @Override
    public Number queryNumber(Query query) {
        this.checkOpened();

        HbaseSessions.Session session = this.sessions.session();
        HbaseTable table = this.table(HbaseTable.tableType(query));
        return table.queryNumber(session, query);
    }

    @Override
    public void init() {
        this.checkConnectionOpened();

        // Create namespace
        try {
            this.sessions.createNamespace();
        } catch (NamespaceExistException ignored) {
            // Ignore due to both schema & graph store would create namespace
        } catch (IOException e) {
            throw new BackendException(
                    "Failed to create namespace '%s' for '%s' store",
                    e, this.namespace, this.store);
        }

        // Create tables
        for (String table : this.tableNames()) {
            try {
                if (table.equals("g_oe") || table.equals("g_ie")) {
                    this.sessions.createPreSplitTable(table, HbaseTable.cfs(),
                                                      this.edgeLogicPartitions);
                } else if (table.equals("g_v")) {
                    this.sessions.createPreSplitTable(table, HbaseTable.cfs(),
                                                      this.vertexLogicPartitions);
                } else {
                    this.sessions.createTable(table, HbaseTable.cfs());
                }

            } catch (TableExistsException ignored) {
                continue;
            } catch (IOException e) {
                throw new BackendException(
                        "Failed to create table '%s' for '%s' store",
                        e, table, this.store);
            }
        }

        LOG.debug("Store initialized: {}", this.store);
    }

    @Override
    public void clear(boolean clearSpace) {
        this.checkConnectionOpened();

        // Return if not exists namespace
        try {
            if (!this.sessions.existsNamespace()) {
                return;
            }
        } catch (IOException e) {
            throw new BackendException(
                    "Exception when checking for the existence of '%s'",
                    e, this.namespace);
        }

        if (!clearSpace) {
            // Drop tables
            for (String table : this.tableNames()) {
                try {
                    this.sessions.dropTable(table);
                } catch (TableNotFoundException e) {
                    LOG.warn("The table '{}' of '{}' store does not exist " +
                             "when trying to drop", table, this.store);
                } catch (IOException e) {
                    throw new BackendException(
                            "Failed to drop table '%s' of '%s' store",
                            e, table, this.store);
                }
            }
        } else {
            // Drop namespace
            try {
                this.sessions.dropNamespace();
            } catch (IOException e) {
                String notEmpty = "Only empty namespaces can be removed";
                if (e.getCause().getMessage().contains(notEmpty)) {
                    LOG.debug("Can't drop namespace '{}': {}",
                              this.namespace, e);
                } else {
                    throw new BackendException(
                            "Failed to drop namespace '%s' of '%s' store",
                            e, this.namespace, this.store);
                }
            }
        }

        LOG.debug("Store cleared: {}", this.store);
    }

    @Override
    public boolean initialized() {
        this.checkConnectionOpened();

        try {
            if (!this.sessions.existsNamespace()) {
                return false;
            }
            for (String table : this.tableNames()) {
                if (!this.sessions.existsTable(table)) {
                    return false;
                }
            }
        } catch (IOException e) {
            throw new BackendException("Failed to obtain table info", e);
        }
        return true;
    }

    @Override
    public void truncate() {
        this.checkOpened();

        // Total time may cost 3 * TRUNCATE_TIMEOUT, due to there are 3 stores
        long timeout = this.sessions.config().get(HbaseOptions.TRUNCATE_TIMEOUT);
        long start = System.currentTimeMillis();

        BiFunction<String, Future<Void>, Void> wait = (table, future) -> {
            long elapsed = System.currentTimeMillis() - start;
            long remainingTime = timeout - elapsed / 1000L;
            try {
                return future.get(remainingTime, TimeUnit.SECONDS);
            } catch (Exception e) {
                throw new BackendException(
                        "Error when truncating table '%s' of '%s' store: %s",
                        table, this.store, e.toString());
            }
        };

        // Truncate tables
        List<String> tables = this.tableNamesToTruncate();
        Map<String, Future<Void>> futures = new HashMap<>(tables.size());

        try {
            // Disable tables async
            for (String table : tables) {
                futures.put(table, this.sessions.disableTableAsync(table));
            }
            for (Map.Entry<String, Future<Void>> entry : futures.entrySet()) {
                wait.apply(entry.getKey(), entry.getValue());
            }
        } catch (Exception e) {
            this.enableTables(tables);
            throw new BackendException(
                    "Failed to disable table for '%s' store", e, this.store);
        }

        try {
            // Truncate tables async
            for (String table : tables) {
                futures.put(table, this.sessions.truncateTableAsync(table));
            }
            for (Map.Entry<String, Future<Void>> entry : futures.entrySet()) {
                wait.apply(entry.getKey(), entry.getValue());
            }
        } catch (Exception e) {
            this.enableTables(tables);
            throw new BackendException(
                    "Failed to truncate table for '%s' store", e, this.store);
        }

        LOG.debug("Store truncated: {}", this.store);
    }

    private void enableTables(List<String> tables) {
        for (String table : tables) {
            try {
                this.sessions.enableTable(table);
            } catch (Exception e) {
                LOG.warn("Failed to enable table '{}' of '{}' store",
                         table, this.store, e);
            }
        }
    }

    @Override
    public void beginTx() {
        // pass
    }

    @Override
    public void commitTx() {
        this.checkOpened();
        HbaseSessions.Session session = this.sessions.session();

        session.commit();
    }

    @Override
    public void rollbackTx() {
        this.checkOpened();
        HbaseSessions.Session session = this.sessions.session();

        session.rollback();
    }

    private void checkConnectionOpened() {
        E.checkState(this.sessions != null && this.sessions.opened(),
                     "HBase store has not been initialized");
    }

    /***************************** Store defines *****************************/

    public static class HbaseSchemaStore extends HbaseStore {

        private final HbaseTables.Counters counters;

        public HbaseSchemaStore(HugeConfig config, BackendStoreProvider provider,
                                String namespace, String store) {
            super(provider, namespace, store,
                  config.get(HbaseOptions.HBASE_ENABLE_PARTITION));

            this.counters = new HbaseTables.Counters();

            registerTableManager(HugeType.VERTEX_LABEL,
                                 new HbaseTables.VertexLabel());
            registerTableManager(HugeType.EDGE_LABEL,
                                 new HbaseTables.EdgeLabel());
            registerTableManager(HugeType.PROPERTY_KEY,
                                 new HbaseTables.PropertyKey());
            registerTableManager(HugeType.INDEX_LABEL,
                                 new HbaseTables.IndexLabel());

            registerTableManager(HugeType.SECONDARY_INDEX,
                                 new HbaseTables.SecondaryIndex(store));
        }

        @Override
        protected List<String> tableNames() {
            List<String> tableNames = super.tableNames();
            tableNames.add(this.counters.table());
            return tableNames;
        }

        @Override
        public void increaseCounter(HugeType type, long increment) {
            super.checkOpened();
            this.counters.increaseCounter(super.sessions.session(),
                                          type, increment);
        }

        @Override
        public long getCounter(HugeType type) {
            super.checkOpened();
            return this.counters.getCounter(super.sessions.session(), type);
        }

        @Override
        public boolean isSchemaStore() {
            return true;
        }
    }

    public static class HbaseGraphStore extends HbaseStore {

        private boolean enablePartition;

        public HbaseGraphStore(HugeConfig config, BackendStoreProvider provider,
                               String namespace, String store) {
            super(provider, namespace, store,
                  config.get(HbaseOptions.HBASE_ENABLE_PARTITION));
            this.enablePartition = config.get(HbaseOptions.HBASE_ENABLE_PARTITION);
            registerTableManager(HugeType.VERTEX,
                                 new HbaseTables.Vertex(store, enablePartition));

            registerTableManager(HugeType.EDGE_OUT,
                                 HbaseTables.Edge.out(store, enablePartition));
            registerTableManager(HugeType.EDGE_IN,
                                 HbaseTables.Edge.in(store, enablePartition));

            registerTableManager(HugeType.SECONDARY_INDEX,
                                 new HbaseTables.SecondaryIndex(store));
            registerTableManager(HugeType.VERTEX_LABEL_INDEX,
                                 new HbaseTables.VertexLabelIndex(store));
            registerTableManager(HugeType.EDGE_LABEL_INDEX,
                                 new HbaseTables.EdgeLabelIndex(store));
            registerTableManager(HugeType.RANGE_INT_INDEX,
                                 HbaseTables.RangeIndex.rangeInt(store));
            registerTableManager(HugeType.RANGE_FLOAT_INDEX,
                                 HbaseTables.RangeIndex.rangeFloat(store));
            registerTableManager(HugeType.RANGE_LONG_INDEX,
                                 HbaseTables.RangeIndex.rangeLong(store));
            registerTableManager(HugeType.RANGE_DOUBLE_INDEX,
                                 HbaseTables.RangeIndex.rangeDouble(store));
            registerTableManager(HugeType.SEARCH_INDEX,
                                 new HbaseTables.SearchIndex(store));
            registerTableManager(HugeType.SHARD_INDEX,
                                 new HbaseTables.ShardIndex(store));
            registerTableManager(HugeType.UNIQUE_INDEX,
                                 new HbaseTables.UniqueIndex(store));
        }

        @Override
        public boolean isSchemaStore() {
            return false;
        }

        @Override
        public Id nextId(HugeType type) {
            throw new UnsupportedOperationException(
                    "HbaseGraphStore.nextId()");
        }

        @Override
        public void increaseCounter(HugeType type, long num) {
            throw new UnsupportedOperationException(
                    "HbaseGraphStore.increaseCounter()");
        }

        @Override
        public long getCounter(HugeType type) {
            throw new UnsupportedOperationException(
                    "HbaseGraphStore.getCounter()");
        }
    }

    public static class HbaseSystemStore extends HbaseGraphStore {

        private final HbaseTables.Meta meta;

        public HbaseSystemStore(HugeConfig config, BackendStoreProvider provider,
                                String namespace, String store) {
            super(config, provider, namespace, store);

            this.meta = new HbaseTables.Meta();
        }

        @Override
        protected List<String> tableNames() {
            List<String> tableNames = super.tableNames();
            tableNames.add(this.meta.table());
            return tableNames;
        }

        @Override
        protected List<String> tableNamesToTruncate() {
            /*
             * Keep the meta table: it holds the backend version written by
             * init(), truncating it makes the version check fail at the next
             * startup (see #2209)
             */
            List<String> tableNames = this.tableNames();
            tableNames.remove(this.meta.table());
            return tableNames;
        }

        @Override
        public void init() {
            super.init();
            HbaseSessions.Session session = super.session(null);
            String driverVersion = this.provider().driverVersion();
            this.meta.writeVersion(session, driverVersion);
            LOG.info("Write down the backend version: {}", driverVersion);
        }

        @Override
        public String storedVersion() {
            HbaseSessions.Session session = super.session(null);
            return this.meta.readVersion(session);
        }
    }
}
