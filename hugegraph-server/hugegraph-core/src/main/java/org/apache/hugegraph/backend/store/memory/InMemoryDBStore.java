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

package org.apache.hugegraph.backend.store.memory;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hugegraph.exception.BackendException;
import org.apache.hugegraph.backend.LocalCounter;
import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.query.Query;
import org.apache.hugegraph.backend.serializer.TextBackendEntry;
import org.apache.hugegraph.backend.store.AbstractBackendStore;
import org.apache.hugegraph.backend.store.BackendAction;
import org.apache.hugegraph.backend.store.BackendEntry;
import org.apache.hugegraph.backend.store.BackendFeatures;
import org.apache.hugegraph.backend.store.BackendMutation;
import org.apache.hugegraph.backend.store.BackendSession;
import org.apache.hugegraph.backend.store.BackendStoreProvider;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

/**
 * NOTE:
 * InMemoryDBStore support:
 * 1.query by id (include query edges by id)
 * 2.query by condition (include query edges by condition)
 * 3.remove by id
 * 4.range query
 * 5.append/subtract index data(element-id) and vertex-property
 * 6.query edge by edge-label
 * InMemoryDBStore not support currently:
 * 1.remove by id + condition
 * 2.append/subtract edge-property
 */
public abstract class InMemoryDBStore
        extends AbstractBackendStore<BackendSession> {

    private static final Logger LOG = Log.logger(InMemoryDBStore.class);

    private final BackendStoreProvider provider;

    private final String store;
    private final String database;

    private final Map<HugeType, InMemoryDBTable> tables;

    public InMemoryDBStore(final BackendStoreProvider provider,
                           final String database, final String store) {
        this.provider = provider;
        this.database = database;
        this.store = store;
        this.tables = new HashMap<>();

        this.registerMetaHandlers();
        LOG.debug("Store loaded: {}", store);
    }

    private void registerMetaHandlers() {
        this.registerMetaHandler("metrics", (session, meta, args) -> {
            InMemoryMetrics metrics = new InMemoryMetrics();
            return metrics.metrics();
        });
    }

    protected void registerTableManager(HugeType type, InMemoryDBTable table) {
        this.tables.put(type, table);
    }

    protected Collection<InMemoryDBTable> tables() {
        return this.tables.values();
    }

    @Override
    protected final InMemoryDBTable table(HugeType type) {
        assert type != null;
        InMemoryDBTable table = this.tables.get(convertTaskOrServerToVertex(type));
        if (table == null) {
            throw new BackendException("Unsupported table type: %s", type);
        }
        return table;
    }

    @Override
    protected BackendSession session(HugeType type) {
        return null;
    }

    @Override
    public Iterator<BackendEntry> query(Query query) {
        InMemoryDBTable table = this.table(InMemoryDBTable.tableType(query));
        Iterator<BackendEntry> rs = table.query(null, query);
        LOG.debug("[store {}] has result({}) for query: {}",
                  this.store, rs.hasNext(), query);
        return rs;
    }

    @Override
    public Number queryNumber(Query query) {
        InMemoryDBTable table = this.table(InMemoryDBTable.tableType(query));
        Number result = table.queryNumber(null, query);
        LOG.debug("[store {}] get result({}) for number query: {}",
                  this.store, result, query);
        return result;
    }

    @Override
    public void mutate(BackendMutation mutation) {
        for (Iterator<BackendAction> it = mutation.mutation(); it.hasNext(); ) {
            this.mutate(it.next());
        }
    }

    protected void mutate(BackendAction item) {
        BackendEntry e = item.entry();
        assert e instanceof TextBackendEntry;
        TextBackendEntry entry = (TextBackendEntry) e;
        InMemoryDBTable table = this.table(entry.type());
        switch (item.action()) {
            case INSERT:
                LOG.debug("[store {}] add entry: {}", this.store, entry);
                table.insert(null, entry);
                break;
            case DELETE:
                LOG.debug("[store {}] remove id: {}", this.store, entry.id());
                table.delete(null, entry);
                break;
            case APPEND:
                LOG.debug("[store {}] append entry: {}", this.store, entry);
                table.append(null, entry);
                break;
            case ELIMINATE:
                LOG.debug("[store {}] eliminate entry: {}", this.store, entry);
                table.eliminate(null, entry);
                break;
            case UPDATE_IF_PRESENT:
                table.updateIfPresent(null, entry);
                break;
            case UPDATE_IF_ABSENT:
                table.updateIfAbsent(null, entry);
                break;
            default:
                throw new BackendException("Unsupported mutate type: %s",
                                           item.action());
        }
    }

    @Override
    public String store() {
        return this.store;
    }

    @Override
    public String database() {
        return this.database;
    }

    @Override
    public BackendStoreProvider provider() {
        return this.provider;
    }

    @Override
    public void open(HugeConfig config) {
        LOG.debug("Store opened: {}", this.store);
    }

    @Override
    public void close() throws BackendException {
        LOG.debug("Store closed: {}", this.store);
    }

    @Override
    public void init() {
        for (InMemoryDBTable table : this.tables()) {
            table.init(null);
        }

        LOG.debug("Store initialized: {}", this.store);
    }

    @Override
    public void clear(boolean clearSpace) {
        for (InMemoryDBTable table : this.tables()) {
            table.clear(null);
        }

        LOG.debug("Store cleared: {}", this.store);
    }

    @Override
    public void truncate() {
        for (InMemoryDBTable table : this.tables()) {
            table.clear(null);
        }

        LOG.debug("Store truncated: {}", this.store);
    }

    @Override
    public void beginTx() {
        // pass
    }

    @Override
    public void commitTx() {
        // pass
    }

    @Override
    public void rollbackTx() {
        throw new UnsupportedOperationException(
                "Unsupported rollback operation by InMemoryDBStore");
    }

    @Override
    public BackendFeatures features() {
        return FEATURES;
    }

    @Override
    public boolean opened() {
        return true;
    }

    @Override
    public boolean initialized() {
        return true;
    }

    /***************************** Store defines *****************************/

    public static class InMemorySchemaStore extends InMemoryDBStore {

        private final LocalCounter counter = new LocalCounter();

        public InMemorySchemaStore(BackendStoreProvider provider,
                                   String database, String store) {
            super(provider, database, store);

            registerTableManager(HugeType.VERTEX_LABEL,
                                 new InMemoryDBTable(HugeType.VERTEX_LABEL));
            registerTableManager(HugeType.EDGE_LABEL,
                                 new InMemoryDBTable(HugeType.EDGE_LABEL));
            registerTableManager(HugeType.PROPERTY_KEY,
                                 new InMemoryDBTable(HugeType.PROPERTY_KEY));
            registerTableManager(HugeType.INDEX_LABEL,
                                 new InMemoryDBTable(HugeType.INDEX_LABEL));
            registerTableManager(HugeType.SECONDARY_INDEX,
                                 new InMemoryDBTables.SecondaryIndex());
        }

        @Override
        public Id nextId(HugeType type) {
            return this.counter.nextId(type);
        }

        @Override
        public void increaseCounter(HugeType type, long increment) {
            this.counter.increaseCounter(type, increment);
        }

        @Override
        public long getCounter(HugeType type) {
            return this.counter.getCounter(type);
        }

        @Override
        public void clear(boolean clearSpace) {
            this.counter.reset();
            super.clear(clearSpace);
        }

        @Override
        public void truncate() {
            this.counter.reset();
            super.truncate();
        }

        @Override
        public boolean isSchemaStore() {
            return true;
        }
    }

    public static class InMemoryGraphStore extends InMemoryDBStore {

        public InMemoryGraphStore(BackendStoreProvider provider,
                                  String database, String store) {
            super(provider, database, store);

            registerTableManager(HugeType.VERTEX,
                                 new InMemoryDBTables.Vertex());
            registerTableManager(HugeType.EDGE_OUT,
                                 new InMemoryDBTables.Edge(HugeType.EDGE_OUT));
            registerTableManager(HugeType.EDGE_IN,
                                 new InMemoryDBTables.Edge(HugeType.EDGE_IN));
            registerTableManager(HugeType.SECONDARY_INDEX,
                                 new InMemoryDBTables.SecondaryIndex());
            registerTableManager(HugeType.RANGE_INT_INDEX,
                                 InMemoryDBTables.RangeIndex.rangeInt());
            registerTableManager(HugeType.RANGE_FLOAT_INDEX,
                                 InMemoryDBTables.RangeIndex.rangeFloat());
            registerTableManager(HugeType.RANGE_LONG_INDEX,
                                 InMemoryDBTables.RangeIndex.rangeLong());
            registerTableManager(HugeType.RANGE_DOUBLE_INDEX,
                                 InMemoryDBTables.RangeIndex.rangeDouble());
            registerTableManager(HugeType.SEARCH_INDEX,
                                 new InMemoryDBTables.SearchIndex());
            registerTableManager(HugeType.SHARD_INDEX,
                                 new InMemoryDBTables.ShardIndex());
            registerTableManager(HugeType.UNIQUE_INDEX,
                                 new InMemoryDBTables.UniqueIndex());
        }

        @Override
        public boolean isSchemaStore() {
            return false;
        }

        @Override
        public Id nextId(HugeType type) {
            throw new UnsupportedOperationException(
                    "InMemoryGraphStore.nextId()");
        }

        @Override
        public void increaseCounter(HugeType type, long num) {
            throw new UnsupportedOperationException(
                    "InMemoryGraphStore.increaseCounter()");
        }

        @Override
        public long getCounter(HugeType type) {
            throw new UnsupportedOperationException(
                    "InMemoryGraphStore.getCounter()");
        }
    }

    public static class InMemorySystemStore extends InMemoryGraphStore {

        public InMemorySystemStore(BackendStoreProvider provider,
                                   String database, String store) {
            super(provider, database, store);
        }

        @Override
        public String storedVersion() {
            return this.provider().driverVersion();
        }
    }

    /**
     * InMemoryDBStore features
     */
    private static final BackendFeatures FEATURES = new BackendFeatures() {

        @Override
        public boolean supportsPersistence() {
            return false;
        }

        @Override
        public boolean supportsSharedStorage() {
            return false;
        }

        @Override
        public boolean supportsScanToken() {
            return false;
        }

        @Override
        public boolean supportsScanKeyPrefix() {
            return false;
        }

        @Override
        public boolean supportsScanKeyRange() {
            return false;
        }

        @Override
        public boolean supportsQuerySchemaByName() {
            // Traversal all data in memory
            return true;
        }

        @Override
        public boolean supportsQueryByLabel() {
            // Traversal all data in memory
            return true;
        }

        @Override
        public boolean supportsQueryWithInCondition() {
            return false;
        }

        @Override
        public boolean supportsQueryWithRangeCondition() {
            return true;
        }

        @Override
        public boolean supportsQueryWithOrderBy() {
            return false;
        }

        @Override
        public boolean supportsQueryWithContains() {
            // NOTE: hasValue tests will skip
            return false;
        }

        @Override
        public boolean supportsQueryWithContainsKey() {
            // NOTE: hasKey tests will skip
            return false;
        }

        @Override
        public boolean supportsQueryByPage() {
            return false;
        }

        @Override
        public boolean supportsQuerySortByInputIds() {
            return true;
        }

        @Override
        public boolean supportsDeleteEdgeByLabel() {
            return false;
        }

        @Override
        public boolean supportsUpdateVertexProperty() {
            return false;
        }

        @Override
        public boolean supportsMergeVertexProperty() {
            return false;
        }

        @Override
        public boolean supportsUpdateEdgeProperty() {
            return false;
        }

        @Override
        public boolean supportsTransaction() {
            return false;
        }

        @Override
        public boolean supportsNumberType() {
            return false;
        }

        @Override
        public boolean supportsAggregateProperty() {
            return false;
        }

        @Override
        public boolean supportsTtl() {
            return false;
        }

        @Override
        public boolean supportsOlapProperties() {
            return false;
        }
    };
}
