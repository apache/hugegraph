/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hugegraph.backend.store.hstore;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.id.IdGenerator;
import org.apache.hugegraph.backend.query.ConditionQuery;
import org.apache.hugegraph.backend.query.ConditionQueryFlatten;
import org.apache.hugegraph.backend.query.IdPrefixQuery;
import org.apache.hugegraph.backend.query.IdQuery;
import org.apache.hugegraph.backend.query.Query;
import org.apache.hugegraph.backend.serializer.BinaryBackendEntry;
import org.apache.hugegraph.backend.serializer.BytesBuffer;
import org.apache.hugegraph.backend.serializer.MergeIterator;
import org.apache.hugegraph.backend.store.AbstractBackendStore;
import org.apache.hugegraph.backend.store.BackendAction;
import org.apache.hugegraph.backend.store.BackendEntry;
import org.apache.hugegraph.backend.store.BackendFeatures;
import org.apache.hugegraph.backend.store.BackendMutation;
import org.apache.hugegraph.backend.store.BackendStoreProvider;
import org.apache.hugegraph.backend.store.BackendTable;
import org.apache.hugegraph.backend.store.hstore.HstoreSessions.Session;
import org.apache.hugegraph.config.CoreOptions;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.iterator.CIter;
import org.apache.hugegraph.schema.EdgeLabel;
import org.apache.hugegraph.type.HugeTableType;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.type.define.Action;
import org.apache.hugegraph.type.define.GraphMode;
import org.apache.hugegraph.type.define.HugeKeys;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

import com.google.common.collect.ImmutableSet;

public abstract class HstoreStore extends AbstractBackendStore<Session> {

    private static final Logger LOG = Log.logger(HstoreStore.class);

    private static final Set<HugeType> INDEX_TYPES = ImmutableSet.of(
            HugeType.SECONDARY_INDEX, HugeType.VERTEX_LABEL_INDEX,
            HugeType.EDGE_LABEL_INDEX, HugeType.RANGE_INT_INDEX,
            HugeType.RANGE_FLOAT_INDEX, HugeType.RANGE_LONG_INDEX,
            HugeType.RANGE_DOUBLE_INDEX, HugeType.SEARCH_INDEX,
            HugeType.SHARD_INDEX, HugeType.UNIQUE_INDEX
    );

    private static final BackendFeatures FEATURES = new HstoreFeatures();
    private final String store, namespace;

    private final BackendStoreProvider provider;
    private final Map<Integer, HstoreTable> tables;
    private final ReadWriteLock storeLock;
    private boolean isGraphStore;
    private HstoreSessions sessions;

    public HstoreStore(final BackendStoreProvider provider,
                       String namespace, String store) {
        this.tables = new HashMap<>();
        this.provider = provider;
        this.namespace = namespace;
        this.store = store;
        this.sessions = null;
        this.storeLock = new ReentrantReadWriteLock();
        this.registerMetaHandlers();
        LOG.debug("Store loaded: {}", store);
    }

    private void registerMetaHandlers() {
        Supplier<List<HstoreSessions>> dbsGet = () -> {
            List<HstoreSessions> dbs = new ArrayList<>();
            dbs.add(this.sessions);
            return dbs;
        };
        this.registerMetaHandler("metrics", (session, meta, args) -> {
            HstoreMetrics metrics = new HstoreMetrics(dbsGet.get(), session);
            return metrics.metrics();
        });
        this.registerMetaHandler("mode", (session, meta, args) -> {
            E.checkArgument(args.length == 1,
                            "The args count of %s must be 1", meta);
            session.setMode((GraphMode) args[0]);
            return null;
        });
    }

    protected void registerTableManager(HugeTableType type, HstoreTable table) {
        this.tables.put((int) type.code(), table);
    }

    @Override
    protected final HstoreTable table(HugeType type) {
        assert type != null;
        HugeTableType table;
        switch (type) {
            case VERTEX:
                table = HugeTableType.VERTEX;
                break;
            case EDGE_OUT:
                table = HugeTableType.OUT_EDGE;
                break;
            case EDGE_IN:
                table = HugeTableType.IN_EDGE;
                break;
            case OLAP:
                table = HugeTableType.OLAP_TABLE;
                break;
            case TASK:
                table = HugeTableType.TASK_INFO_TABLE;
                break;
            case SERVER:
                table = HugeTableType.SERVER_INFO_TABLE;
                break;
            case SEARCH_INDEX:
            case SHARD_INDEX:
            case SECONDARY_INDEX:
            case RANGE_INT_INDEX:
            case RANGE_LONG_INDEX:
            case RANGE_FLOAT_INDEX:
            case RANGE_DOUBLE_INDEX:
            case EDGE_LABEL_INDEX:
            case VERTEX_LABEL_INDEX:
            case UNIQUE_INDEX:
                table = HugeTableType.ALL_INDEX_TABLE;
                break;
            default:
                throw new AssertionError(String.format(
                        "Invalid type: %s", type));
        }
        return this.tables.get((int) table.code());
    }

    protected List<String> tableNames() {
        return this.tables.values().stream()
                          .map(BackendTable::table)
                          .collect(Collectors.toList());
    }

    @Override
    protected Session session(HugeType type) {
        this.checkOpened();
        return this.sessions.session();
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
        return FEATURES;
    }

    @Override
    public synchronized void open(HugeConfig config) {
        E.checkNotNull(config, "config");

        if (this.sessions == null) {
            this.sessions = new HstoreSessionsImpl(config, this.namespace,
                                                   this.store);
        }

        String graphStore = config.get(CoreOptions.STORE_GRAPH);
        this.isGraphStore = this.store.equals(graphStore);
        assert this.sessions != null;
        if (!this.sessions.closed()) {
            LOG.debug("Store {} has been opened before", this.store);
            this.sessions.useSession();
            return;
        }

        try {
            // NOTE: won't throw error even if connection refused
            this.sessions.open();
        } catch (Exception e) {
            LOG.error("Failed to open Hstore '{}':{}", this.store, e);
        }
        this.sessions.session();
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
        Session session = this.sessions.session();
        assert session.opened();
        Map<HugeType, Map<Id, List<BackendAction>>> mutations = mutation.mutations();
        Set<Map.Entry<HugeType, Map<Id, List<BackendAction>>>> entries = mutations.entrySet();
        for (Map.Entry<HugeType, Map<Id, List<BackendAction>>> entry : entries) {
            HugeType key = entry.getKey();
            // in order to obtain the owner efficiently, special for edge
            boolean isEdge = key.isEdge();
            HstoreTable hTable = this.table(key);
            Map<Id, List<BackendAction>> table = entry.getValue();
            Collection<List<BackendAction>> values = table.values();
            for (List<BackendAction> items : values) {
                for (int i = 0; i < items.size(); i++) {
                    BackendAction item = items.get(i);
                    // set to ArrayList, use index to get item
                    this.mutate(session, item, hTable, isEdge);
                }
            }
        }
    }

    private void mutate(Session session, BackendAction item,
                        HstoreTable hTable, boolean isEdge) {
        BackendEntry entry = item.entry();
        HstoreTable table;
        if (!entry.olap()) {
            // Oltp table
            table = hTable;
        } else {
            if (entry.type().isIndex()) {
                // Olap index
                table = this.table(entry.type());
            } else {
                // Olap vertex
                table = this.table(HugeType.OLAP);
            }
            session = this.session(HugeType.OLAP);
        }

        if (item.action().code() == Action.INSERT.code()) {
            table.insert(session, entry, isEdge);
        } else {
            if (item.action().code() == Action.APPEND.code()) {
                table.append(session, entry);
            } else {
                switch (item.action()) {
                    case DELETE:
                        table.delete(session, entry);
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
                                "Unsupported mutate action: %s",
                                item.action()));
                }
            }
        }
    }

    private HstoreTable getTableByQuery(Query query) {
        HugeType tableType = HstoreTable.tableType(query);
        HstoreTable table;
        if (query.olap()) {
            if (query.resultType().isIndex()) {
                // Any index type is ok here
                table = this.table(HugeType.SECONDARY_INDEX);
            } else {
                table = this.table(HugeType.OLAP);
            }
        } else {
            table = this.table(tableType);
        }
        return table;
    }

    @Override
    public Iterator<BackendEntry> query(Query query) {
        Lock readLock = this.storeLock.readLock();
        readLock.lock();
        try {
            this.checkOpened();
            Session session = this.sessions.session();
            HstoreTable table = getTableByQuery(query);
            Iterator<BackendEntry> entries = table.query(session, query);
            // Merge olap results as needed
            entries = getBackendEntryIterator(entries, query);
            return entries;
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public Iterator<Iterator<BackendEntry>> query(Iterator<Query> queries,
                                                  Function<Query, Query> queryWriter,
                                                  HugeGraph hugeGraph) {
        if (queries == null || !queries.hasNext()) {
            return Collections.emptyIterator();
        }

        class QueryWrapper implements Iterator<IdPrefixQuery> {
            Query first;
            final Iterator<Query> queries;
            Iterator<Id> subEls;
            Query preQuery;
            Iterator<IdPrefixQuery> queryListIterator;

            QueryWrapper(Iterator<Query> queries, Query first) {
                this.queries = queries;
                this.first = first;
            }

            @Override
            public boolean hasNext() {
                return first != null || (this.subEls != null && this.subEls.hasNext())
                       || (queryListIterator != null && queryListIterator.hasNext()) ||
                       queries.hasNext();
            }

            @Override
            public IdPrefixQuery next() {
                if (queryListIterator != null && queryListIterator.hasNext()) {
                    return queryListIterator.next();
                }

                Query q;
                if (first != null) {
                    q = first;
                    preQuery = q.copy();
                    first = null;
                } else {
                    if (this.subEls == null || !this.subEls.hasNext()) {
                        q = queries.next();
                        preQuery = q.copy();
                    } else {
                        q = preQuery.copy();
                    }
                }

                assert q instanceof ConditionQuery;
                ConditionQuery cq = (ConditionQuery) q;
                ConditionQuery originQuery = (ConditionQuery) q.copy();

                List<IdPrefixQuery> queryList = Lists.newArrayList();
                if (hugeGraph != null) {
                    for (ConditionQuery conditionQuery :
                         ConditionQueryFlatten.flatten(cq)) {
                        Id label = this.uniqueLabel(conditionQuery);
                        /* Parent type + sortKeys: g.V("V.id").outE("parentLabel")
                           .has("sortKey","value") converted to all subtypes + sortKeys */
                        if ((this.subEls == null ||
                             !this.subEls.hasNext()) && label != null &&
                            hugeGraph.edgeLabel(label).isFather() &&
                            conditionQuery.condition(HugeKeys.SUB_LABEL) ==
                            null &&
                            conditionQuery.condition(HugeKeys.OWNER_VERTEX) !=
                            null &&
                            conditionQuery.condition(HugeKeys.DIRECTION) !=
                            null &&
                            matchEdgeSortKeys(conditionQuery, false,
                                              hugeGraph)) {
                            this.subEls =
                                getSubLabelsOfParentEl(
                                    hugeGraph.edgeLabels(),
                                    label);
                        }

                        if (this.subEls != null &&
                            this.subEls.hasNext()) {
                            conditionQuery.eq(HugeKeys.SUB_LABEL,
                                              subEls.next());
                        }

                        HugeType hugeType = conditionQuery.resultType();
                        if (hugeType != null && hugeType.isEdge() &&
                            !conditionQuery.conditions().isEmpty()) {
                            IdPrefixQuery idPrefixQuery =
                                (IdPrefixQuery) queryWriter.apply(
                                    conditionQuery);
                            idPrefixQuery.setOriginQuery(originQuery);
                            queryList.add(idPrefixQuery);
                        }
                    }

                    queryListIterator = queryList.iterator();
                    if (queryListIterator.hasNext()) {
                        return queryListIterator.next();
                    }
                }

                Id ownerId = cq.condition(HugeKeys.OWNER_VERTEX);
                assert ownerId != null;
                BytesBuffer buffer =
                    BytesBuffer.allocate(BytesBuffer.BUF_EDGE_ID);
                buffer.writeId(ownerId);
                return new IdPrefixQuery(cq, new BinaryBackendEntry.BinaryId(
                    buffer.bytes(), ownerId));
            }

            private Id uniqueLabel(ConditionQuery query) {
                Set<Object> labels = query.conditionValues(HugeKeys.LABEL);
                if (labels.size() != 1) {
                    return null;
                }
                return (Id) labels.iterator().next();
            }

            private boolean matchEdgeSortKeys(ConditionQuery query,
                                              boolean matchAll,
                                              HugeGraph graph) {
                assert query.resultType().isEdge();
                Id label = this.uniqueLabel(query);
                if (label == null) {
                    return false;
                }
                List<Id> sortKeys = graph.edgeLabel(label).sortKeys();
                if (sortKeys.isEmpty()) {
                    return false;
                }
                Set<Id> queryKeys = query.userpropKeys();
                for (int i = sortKeys.size(); i > 0; i--) {
                    List<Id> subFields = sortKeys.subList(0, i);
                    if (queryKeys.containsAll(subFields)) {
                        if (queryKeys.size() == subFields.size() || !matchAll) {
                            /*
                             * Return true if:
                             * matchAll=true and all queryKeys are in sortKeys
                             *  or
                             * partial queryKeys are in sortKeys
                             */
                            return true;
                        }
                    }
                }
                return false;
            }
        }
        Query first = queries.next();
        List<HugeType> typeList = getHugeTypes(first);
        QueryWrapper idPrefixQueries = new QueryWrapper(queries, first);

        return query(typeList, idPrefixQueries);
    }

    private Iterator<Id> getSubLabelsOfParentEl(Collection<EdgeLabel> allEls,
                                                Id label) {
        List<Id> list = new ArrayList<>();
        for (EdgeLabel el : allEls) {
            if (el.edgeLabelType().sub() && el.fatherId().equals(label)) {
                list.add(el.id());
            }
        }
        return list.iterator();
    }

    public List<CIter<BackendEntry>> query(List<HugeType> typeList,
                                           List<IdPrefixQuery> queries) {
        Lock readLock = this.storeLock.readLock();
        readLock.lock();
        LinkedList<CIter<BackendEntry>> results = new LinkedList<>();
        try {
            this.checkOpened();
            Session session = this.sessions.session();
            E.checkState(!CollectionUtils.isEmpty(queries) &&
                         !CollectionUtils.isEmpty(typeList),
                         "Please check query list or type list.");
            HstoreTable table = null;
            StringBuilder builder = new StringBuilder();
            for (HugeType type : typeList) {
                builder.append((table = this.table(type)).table()).append(",");
            }
            List<Iterator<BackendEntry>> iteratorList =
                    table.query(session, queries,
                                builder.substring(0, builder.length() - 1));
            for (int i = 0; i < iteratorList.size(); i++) {
                Iterator<BackendEntry> entries = iteratorList.get(i);
                // Merge olap results as needed
                Query query = queries.get(i);
                entries = getBackendEntryIterator(entries, query);
                if (entries instanceof CIter) {
                    results.add((CIter) entries);
                }
            }
            return results;
        } finally {
            readLock.unlock();
        }
    }

    public Iterator<Iterator<BackendEntry>> query(List<HugeType> typeList,
                                                  Iterator<IdPrefixQuery> queries) {
        Lock readLock = this.storeLock.readLock();
        readLock.lock();
        try {
            this.checkOpened();
            Session session = this.sessions.session();
            E.checkState(queries.hasNext() &&
                         !CollectionUtils.isEmpty(typeList),
                         "Please check query list or type list.");
            HstoreTable table = null;
            StringBuilder builder = new StringBuilder();
            for (HugeType type : typeList) {
                builder.append((table = this.table(type)).table()).append(",");
            }

            Iterator<Iterator<BackendEntry>> iterators =
                    table.query(session, queries,
                                builder.substring(0, builder.length() - 1));

            return iterators;
        } finally {
            readLock.unlock();
        }
    }

    private Iterator<BackendEntry> getBackendEntryIterator(
            Iterator<BackendEntry> entries,
            Query query) {
        HstoreTable table;
        Set<Id> olapPks = query.olapPks();
        if (this.isGraphStore && !olapPks.isEmpty()) {
            List<Iterator<BackendEntry>> iterators = new ArrayList<>();
            for (Id pk : olapPks) {
                // Construct OLAP table query query condition
                Query q = this.constructOlapQueryCondition(pk, query);
                table = this.table(HugeType.OLAP);
                iterators.add(table.queryOlap(this.session(HugeType.OLAP), q));
            }
            entries = new MergeIterator<>(entries, iterators,
                                          BackendEntry::mergeable);
        }
        return entries;
    }

    /**
     * Reconstruct the query OLAP table query
     * Due to the olap merged into one table, when writing olap data, the key has a pk added at the end.
     * So when making inquiries here, it is necessary to reconstruct the pk prefix.
     * Write reference BinarySerializer.writeOlapVertex
     *
     * @param pk
     * @param query
     * @return
     */
    private Query constructOlapQueryCondition(Id pk, Query query) {
        if (query instanceof IdQuery && !CollectionUtils.isEmpty((query).ids())) {
            IdQuery q = (IdQuery) query.copy();
            Iterator<Id> iterator = q.ids().iterator();
            LinkedHashSet<Id> linkedHashSet = new LinkedHashSet<>();
            while (iterator.hasNext()) {
                Id id = iterator.next();
                if (id instanceof BinaryBackendEntry.BinaryId) {
                    id = ((BinaryBackendEntry.BinaryId) id).origin();
                }

                // create binary id
                BytesBuffer buffer =
                        BytesBuffer.allocate(1 + pk.length() + 1 + id.length());
                buffer.writeId(pk);
                id = new BinaryBackendEntry.BinaryId(
                        buffer.writeId(id).bytes(), id);
                linkedHashSet.add(id);
            }
            q.resetIds();
            q.query(linkedHashSet);
            return q;
        } else {
            // create binary id
            BytesBuffer buffer = BytesBuffer.allocate(1 + pk.length());
            pk = new BinaryBackendEntry.BinaryId(
                    buffer.writeId(pk).bytes(), pk);

            IdPrefixQuery idPrefixQuery = new IdPrefixQuery(HugeType.OLAP, pk);
            return idPrefixQuery;
        }
    }

    @Override
    public Number queryNumber(Query query) {
        this.checkOpened();

        Session session = this.sessions.session();
        HstoreTable table = this.table(HstoreTable.tableType(query));
        return table.queryNumber(session, query);
    }

    @Override
    public synchronized void init() {
        Lock writeLock = this.storeLock.writeLock();
        writeLock.lock();
        try {
            // Create tables with main disk
            this.sessions.createTable(this.tableNames().toArray(new String[0]));
            LOG.debug("Store initialized: {}", this.store);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void clear(boolean clearSpace) {
        Lock writeLock = this.storeLock.writeLock();
        writeLock.lock();
        try {
            // Drop tables with main disk
            this.sessions.dropTable(this.tableNames().toArray(new String[0]));
            if (clearSpace) {
                this.sessions.clear();
            }
            LOG.debug("Store cleared: {}", this.store);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public boolean initialized() {
        return true;
    }

    @Override
    public void truncate() {
        try {
            this.sessions.session().truncate();
        } catch (Exception e) {
            LOG.error("Store truncated failed", e);
            return;
        }
        LOG.debug("Store truncated: {}", this.store);
    }

    @Override
    public void beginTx() {
        this.sessions.session().beginTx();
    }

    @Override
    public void commitTx() {
        this.checkOpened();
        Session session = this.sessions.session();
        session.commit();
    }

    @Override
    public void rollbackTx() {
        this.checkOpened();
        Session session = this.sessions.session();
        session.rollback();
    }

    private void checkConnectionOpened() {
    }

    @Override
    public Id nextId(HugeType type) {
        long counter = 0L;
        counter = this.getCounter(type);
        E.checkState(counter != 0L, "Please check whether '%s' is OK",
                     this.provider().type());
        return IdGenerator.of(counter);
    }

    @Override
    public void setCounterLowest(HugeType type, long lowest) {
        this.increaseCounter(type, lowest);
    }

    @Override
    public String storedVersion() {
        return "1.13";
    }

    /***************************** Store defines *****************************/

    public static class HstoreSchemaStore extends HstoreStore {

        public HstoreSchemaStore(BackendStoreProvider provider, String namespace, String store) {
            super(provider, namespace, store);
        }

        @Override
        public boolean isSchemaStore() {
            return true;
        }

        @Override
        public void increaseCounter(HugeType type, long num) {
            throw new UnsupportedOperationException(
                    "HstoreSchemaStore.increaseCounter()");
        }

        @Override
        public long getCounter(HugeType type) {
            throw new UnsupportedOperationException(
                    "HstoreSchemaStore.getCounter()");
        }
    }

    public static class HstoreGraphStore extends HstoreStore {

        public HstoreGraphStore(BackendStoreProvider provider,
                                String namespace, String store) {
            super(provider, namespace, store);

            registerTableManager(HugeTableType.VERTEX,
                                 new HstoreTables.Vertex(store));
            registerTableManager(HugeTableType.OUT_EDGE,
                                 HstoreTables.Edge.out(store));
            registerTableManager(HugeTableType.IN_EDGE,
                                 HstoreTables.Edge.in(store));
            registerTableManager(HugeTableType.ALL_INDEX_TABLE,
                                 new HstoreTables.IndexTable(store));
            registerTableManager(HugeTableType.OLAP_TABLE,
                                 new HstoreTables.OlapTable(store));
            registerTableManager(HugeTableType.TASK_INFO_TABLE,
                                 new HstoreTables.TaskInfo(store));
            registerTableManager(HugeTableType.SERVER_INFO_TABLE,
                                 new HstoreTables.ServerInfo(store));
        }

        @Override
        public boolean isSchemaStore() {
            return false;
        }

        @Override
        public Id nextId(HugeType type) {
            throw new UnsupportedOperationException(
                    "HstoreGraphStore.nextId()");
        }

        @Override
        public void increaseCounter(HugeType type, long num) {
            throw new UnsupportedOperationException(
                    "HstoreGraphStore.increaseCounter()");
        }

        @Override
        public long getCounter(HugeType type) {
            throw new UnsupportedOperationException(
                    "HstoreGraphStore.getCounter()");
        }

        @Override
        public void createOlapTable(Id pkId) {
            HstoreTable table = new HstoreTables.OlapTable(this.store());
            LOG.info("Hstore create olap table {}", table.table());
            super.sessions.createTable(table.table());
            LOG.info("Hstore finish create olap table");
            registerTableManager(HugeTableType.OLAP_TABLE, table);
            LOG.info("OLAP table {} has been created", table.table());
        }

        @Override
        public void checkAndRegisterOlapTable(Id pkId) {
            HstoreTable table = new HstoreTables.OlapTable(this.store());
            if (!super.sessions.existsTable(table.table())) {
                LOG.error("Found exception: Table '{}' doesn't exist, we'll " +
                          "recreate it now. Please carefully check the recent" +
                          "operation in server and computer, then ensure the " +
                          "integrity of store file.", table.table());
                this.createOlapTable(pkId);
            } else {
                registerTableManager(HugeTableType.OLAP_TABLE, table);
            }
        }

        @Override
        public void clearOlapTable(Id pkId) {
        }

        @Override
        public void removeOlapTable(Id pkId) {
        }

        @Override
        public boolean existOlapTable(Id pkId) {
            String tableName = this.olapTableName(pkId);
            return super.sessions.existsTable(tableName);
        }
    }
}
