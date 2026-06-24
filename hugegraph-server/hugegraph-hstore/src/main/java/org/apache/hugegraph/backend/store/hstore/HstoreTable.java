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
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hugegraph.backend.id.EdgeId;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.page.PageState;
import org.apache.hugegraph.backend.query.Aggregate;
import org.apache.hugegraph.backend.query.Aggregate.AggregateFunc;
import org.apache.hugegraph.backend.query.Condition;
import org.apache.hugegraph.backend.query.Condition.Relation;
import org.apache.hugegraph.backend.query.ConditionQuery;
import org.apache.hugegraph.backend.query.IdPrefixQuery;
import org.apache.hugegraph.backend.query.IdRangeQuery;
import org.apache.hugegraph.backend.query.Query;
import org.apache.hugegraph.backend.serializer.BinaryBackendEntry;
import org.apache.hugegraph.backend.serializer.BinaryEntryIterator;
import org.apache.hugegraph.backend.store.BackendEntry;
import org.apache.hugegraph.backend.store.BackendEntry.BackendColumn;
import org.apache.hugegraph.backend.store.BackendEntry.BackendColumnIterator;
import org.apache.hugegraph.backend.store.BackendEntryIterator;
import org.apache.hugegraph.backend.store.BackendTable;
import org.apache.hugegraph.backend.store.Shard;
import org.apache.hugegraph.backend.store.hstore.HstoreSessions.Countable;
import org.apache.hugegraph.backend.store.hstore.HstoreSessions.Session;
import org.apache.hugegraph.exception.NotSupportException;
import org.apache.hugegraph.pd.client.PDClient;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.store.HgOwnerKey;
import org.apache.hugegraph.store.client.util.HgStoreClientConst;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.type.define.HugeKeys;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Log;
import org.apache.hugegraph.util.StringEncoding;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.slf4j.Logger;

/**
 * This class provides the implementation for the HStore table in the backend store.
 * It provides methods for querying, inserting, deleting, and updating entries in the table.
 * It also provides methods for handling metadata and managing shards.
 */
public class HstoreTable extends BackendTable<Session, BackendEntry> {

    private static final Logger LOG = Log.logger(HstoreStore.class);

    private final HstoreShardSplitter shardSpliter;
    Function<BackendEntry, byte[]> ownerDelegate = this::getOwner;
    Function<Id, byte[]> ownerByIdDelegate = this::getOwnerId;
    BiFunction<HugeType, Id, byte[]> ownerByQueryDelegate = this::getOwnerId;
    Supplier<byte[]> ownerScanDelegate = () -> HgStoreClientConst.ALL_PARTITION_OWNER;

    public HstoreTable(String database, String table) {
        super(String.format("%s+%s", database, table));
        this.shardSpliter = new HstoreShardSplitter(this.table());
    }

    public static ConditionQuery removeDirectionCondition(ConditionQuery conditionQuery) {
        Collection<Condition> conditions = conditionQuery.conditions();
        List<Condition> newConditions = new ArrayList<>();
        for (Condition condition : conditions) {
            if (!direction(condition)) {
                newConditions.add(condition);
            }
        }
        if (newConditions.size() > 0) {
            conditionQuery.resetConditions(newConditions);
            return conditionQuery;
        } else {
            return null;
        }
    }

    private static boolean direction(Condition condition) {
        boolean direction = true;
        List<? extends Relation> relations = condition.relations();
        for (Relation r : relations) {
            if (!r.key().equals(HugeKeys.DIRECTION)) {
                direction = false;
                break;
            }
        }
        return direction;
    }

    protected static BackendEntryIterator newEntryIterator(
            BackendColumnIterator cols, Query query) {
        return new BinaryEntryIterator<>(cols, query, (entry, col) -> {
            if (entry == null || !entry.belongToMe(col)) {
                HugeType type = query.resultType();
                // NOTE: only support BinaryBackendEntry currently
                entry = new BinaryBackendEntry(type, col.name);
            }
            entry.columns(col);
            return entry;
        });
    }

    protected static BackendEntryIterator newEntryIteratorOlap(
            BackendColumnIterator cols, Query query, boolean isOlap) {
        return new BinaryEntryIterator<>(cols, query, (entry, col) -> {
            if (entry == null || !entry.belongToMe(col)) {
                HugeType type = query.resultType();
                // NOTE: only support BinaryBackendEntry currently
                entry = new BinaryBackendEntry(type, col.name, false, isOlap);
            }
            entry.columns(col);
            return entry;
        });
    }

    public static String bytes2String(byte[] bytes) {
        StringBuilder result = new StringBuilder();
        for (byte b : bytes) {
            String st = String.format("%02x", b);
            result.append(st);
        }
        return result.toString();
    }

    @Override
    protected void registerMetaHandlers() {
        this.registerMetaHandler("splits", (session, meta, args) -> {
            E.checkArgument(args.length == 1,
                            "The args count of %s must be 1", meta);
            long splitSize = (long) args[0];
            return this.shardSpliter.getSplits(session, splitSize);
        });
    }

    @Override
    public void init(Session session) {
        // pass
    }

    @Override
    public void clear(Session session) {
        // pass
    }

    public boolean isOlap() {
        return false;
    }

    private byte[] getOwner(BackendEntry entry) {
        if (entry == null) {
            return HgStoreClientConst.ALL_PARTITION_OWNER;
        }
        Id id = entry.type().isIndex() ? entry.id() : entry.originId();
        return getOwnerId(id);
    }

    public Supplier<byte[]> getOwnerScanDelegate() {
        return ownerScanDelegate;
    }

    public byte[] getInsertEdgeOwner(BackendEntry entry) {
        Id id = entry.originId();
        id = ((EdgeId) id).ownerVertexId();
        return id.asBytes();
    }

    public byte[] getInsertOwner(BackendEntry entry) {
        // To adapt label index hashing, do not focus on a single partition
        if (entry.type().isLabelIndex() && (entry.columns().size() == 1)) {
            Iterator<BackendColumn> iterator = entry.columns().iterator();
            while (iterator.hasNext()) {
                BackendColumn next = iterator.next();
                return next.name;
            }
        }

        Id id = entry.type().isIndex() ? entry.id() : entry.originId();
        return getOwnerId(id);
    }

    /**
     * Return the owner ID of the Id
     *
     * @param id
     * @return
     */
    protected byte[] getOwnerId(Id id) {
        if (id instanceof BinaryBackendEntry.BinaryId) {
            id = ((BinaryBackendEntry.BinaryId) id).origin();
        }
        if (id != null && id.edge()) {
            id = ((EdgeId) id).ownerVertexId();
        }
        return id != null ? id.asBytes() :
               HgStoreClientConst.ALL_PARTITION_OWNER;
    }

    /**
     * Return the owner ID of the Id
     *
     * @param id
     * @return
     */
    protected byte[] getOwnerId(HugeType type, Id id) {
        if (type.equals(HugeType.VERTEX) || type.equals(HugeType.EDGE) ||
            type.equals(HugeType.EDGE_OUT) || type.equals(HugeType.EDGE_IN) ||
            type.equals(HugeType.COUNTER)) {
            return getOwnerId(id);
        } else {
            return HgStoreClientConst.ALL_PARTITION_OWNER;
        }
    }

    @Override
    public void insert(Session session, BackendEntry entry) {
        byte[] owner = entry.type().isEdge() ? getInsertEdgeOwner(entry) : getInsertOwner(entry);
        ArrayList<BackendColumn> columns = new ArrayList<>(entry.columns());
        for (int i = 0; i < columns.size(); i++) {
            BackendColumn col = columns.get(i);
            session.put(this.table(), owner, col.name, col.value);
        }
    }

    public void insert(Session session, BackendEntry entry, boolean isEdge) {
        byte[] owner = isEdge ? getInsertEdgeOwner(entry) : getInsertOwner(entry);
        ArrayList<BackendColumn> columns = new ArrayList<>(entry.columns());
        for (int i = 0; i < columns.size(); i++) {
            BackendColumn col = columns.get(i);
            session.put(this.table(), owner, col.name, col.value);
        }
    }

    @Override
    public void delete(Session session, BackendEntry entry) {
        byte[] ownerKey = ownerDelegate.apply(entry);
        if (entry.columns().isEmpty()) {
            byte[] idBytes = entry.id().asBytes();
            // LOG.debug("Delete from {} with owner {}, id: {}",
            //          this.table(), bytes2String(ownerKey), idBytes);
            session.delete(this.table(), ownerKey, idBytes);
        } else {
            for (BackendColumn col : entry.columns()) {
                // LOG.debug("Delete from {} with owner {}, id: {}",
                //          this.table(), bytes2String(ownerKey),
                //          bytes2String(col.name));
                assert entry.belongToMe(col) : entry;
                session.delete(this.table(), ownerKey, col.name);
            }
        }
    }

    @Override
    public void append(Session session, BackendEntry entry) {
        assert entry.columns().size() == 1;
        this.insert(session, entry);
    }

    @Override
    public void eliminate(Session session, BackendEntry entry) {
        assert entry.columns().size() == 1;
        this.delete(session, entry);
    }

    @Override
    public boolean queryExist(Session session, BackendEntry entry) {
        Id id = entry.id();
        try (BackendColumnIterator iter = this.queryById(session, id)) {
            return iter.hasNext();
        }
    }

    @Override
    public Number queryNumber(Session session, Query query) {
        Aggregate aggregate = query.aggregateNotNull();
        if (aggregate.func() != AggregateFunc.COUNT) {
            throw new NotSupportException(aggregate.toString());
        }

        assert aggregate.func() == AggregateFunc.COUNT;
        assert query.noLimit();
        Iterator<BackendColumn> results = this.queryBy(session, query);
        if (results instanceof Countable) {
            return ((Countable) results).count();
        }
        return IteratorUtils.count(results);
    }

    @Override
    public Iterator<BackendEntry> query(Session session, Query query) {
        if (query.limit() == 0L && !query.noLimit()) {
            // LOG.debug("Return empty result(limit=0) for query {}", query);
            return Collections.emptyIterator();
        }
        return newEntryIterator(this.queryBy(session, query), query);
    }

    @Override
    public Iterator<BackendEntry> queryOlap(Session session, Query query) {
        if (query.limit() == 0L && !query.noLimit()) {
            // LOG.debug("Return empty result(limit=0) for query {}", query);
            return Collections.emptyIterator();
        }
        return newEntryIteratorOlap(this.queryBy(session, query), query, true);
    }

    public List<Iterator<BackendEntry>> query(Session session,
                                              List<IdPrefixQuery> queries,
                                              String tableName) {
        List<BackendColumnIterator> queryByPrefixList =
                this.queryByPrefixList(session, queries, tableName);
        LinkedList<Iterator<BackendEntry>> iterators = new LinkedList<>();
        for (int i = 0; i < queryByPrefixList.size(); i++) {
            IdPrefixQuery q = queries.get(i).copy();
            q.capacity(Query.NO_CAPACITY);
            q.limit(Query.NO_LIMIT);
            BackendEntryIterator iterator =
                    newEntryIterator(queryByPrefixList.get(i), q);
            iterators.add(iterator);
        }
        return iterators;
    }

    public BackendEntry.BackendIterator<Iterator<BackendEntry>> query(Session session,
                                                                      Iterator<IdPrefixQuery> queries,
                                                                      String tableName) {
        final IdPrefixQuery[] first = {queries.next()};
        int type = first[0].withProperties() ? 0 : Session.SCAN_KEY_ONLY;

        IdPrefixQuery queryTmpl = first[0].copy();
        queryTmpl.capacity(Query.NO_CAPACITY);
        queryTmpl.limit(Query.NO_LIMIT);

        ConditionQuery originQuery = (ConditionQuery) first[0].originQuery();
        if (originQuery != null) {
            originQuery = prepareConditionQueryList(originQuery);
        }
        byte[] queryBytes = originQuery == null ? null : originQuery.bytes();

        BackendEntry.BackendIterator<BackendColumnIterator> it
                = session.scan(tableName, new Iterator<HgOwnerKey>() {
            @Override
            public boolean hasNext() {
                if (first[0] != null) {
                    return true;
                }
                return queries.hasNext();
            }

            @Override
            public HgOwnerKey next() {
                IdPrefixQuery query = first[0] != null ? first[0] : queries.next();
                first[0] = null;
                byte[] prefix = ownerByQueryDelegate.apply(query.resultType(),
                                                           query.prefix());
                return HgOwnerKey.of(prefix, query.prefix().asBytes());
            }
        }, type, first[0], queryBytes);
        return new BackendEntry.BackendIterator<Iterator<BackendEntry>>() {
            @Override
            public boolean hasNext() {
                return it.hasNext();
            }

            @Override
            public Iterator<BackendEntry> next() {
                BackendEntryIterator iterator = newEntryIterator(it.next(), queryTmpl);
                return iterator;
            }

            @Override
            public void close() {
                it.close();
            }

            @Override
            public byte[] position() {
                return new byte[0];
            }
        };
    }

    protected BackendColumnIterator queryBy(Session session, Query query) {
        // Query all
        if (query.empty()) {
            return this.queryAll(session, query);
        }

        // Query by prefix
        if (query instanceof IdPrefixQuery) {
            IdPrefixQuery pq = (IdPrefixQuery) query;
            return this.queryByPrefix(session, pq);
        }

        // Query by range
        if (query instanceof IdRangeQuery) {
            IdRangeQuery rq = (IdRangeQuery) query;
            return this.queryByRange(session, rq);
        }

        // Query by id
        if (query.conditions().isEmpty()) {
            assert !query.ids().isEmpty();
            // Single id query uses the get interface to query
            if (query.ids().size() == 1) {
                return this.getById(session, query.ids().iterator().next());
            }
            // NOTE: this will lead to lazy create rocksdb iterator
            LinkedList<HgOwnerKey> hgOwnerKeys = new LinkedList<>();
            for (Id id : query.ids()) {
                hgOwnerKeys.add(HgOwnerKey.of(this.ownerByIdDelegate.apply(id),
                                              id.asBytes()));
            }
            BackendColumnIterator withBatch = session.getWithBatch(this.table(),
                                                                   hgOwnerKeys);
            return BackendColumnIterator.wrap(withBatch);
        }

        // Query by condition (or condition + id)
        ConditionQuery cq = (ConditionQuery) query;
        return this.queryByCond(session, cq);
    }

    protected BackendColumnIterator queryAll(Session session, Query query) {
        if (query.paging()) {
            PageState page = PageState.fromString(query.page());
            byte[] ownerKey = this.getOwnerScanDelegate().get();
            int scanType = Session.SCAN_ANY |
                           (query.withProperties() ? 0 : Session.SCAN_KEY_ONLY);
            byte[] queryBytes = query instanceof ConditionQuery ?
                                ((ConditionQuery) query).bytes() : null;
            // LOG.debug("query {} with ownerKeyFrom: {}, ownerKeyTo: {}, " +
            //          "keyFrom: null, keyTo: null, scanType: {}, " +
            //          "conditionQuery: {}, position: {}",
            //          this.table(), bytes2String(ownerKey),
            //          bytes2String(ownerKey), scanType,
            //          queryBytes, page.position());
            return session.scan(this.table(), ownerKey, ownerKey, null,
                                null, scanType, queryBytes,
                                page.position());
        }
        return session.scan(this.table(),
                            query instanceof ConditionQuery ?
                            ((ConditionQuery) query).bytes() : null);
    }

    protected BackendColumnIterator queryById(Session session, Id id) {
        // TODO: change to get() after vertex and schema don't use id prefix
        return session.scan(this.table(), this.ownerByIdDelegate.apply(id),
                            id.asBytes());
    }

    protected BackendColumnIterator getById(Session session, Id id) {
        byte[] value = session.get(this.table(),
                                   this.ownerByIdDelegate.apply(id),
                                   id.asBytes());
        if (value.length == 0) {
            return BackendColumnIterator.empty();
        }
        BackendColumn col = BackendColumn.of(id.asBytes(), value);
        return BackendColumnIterator.iterator(col);
    }

    protected BackendColumnIterator queryByPrefix(Session session,
                                                  IdPrefixQuery query) {
        int type = query.inclusiveStart() ?
                   Session.SCAN_GTE_BEGIN : Session.SCAN_GT_BEGIN;
        type |= Session.SCAN_PREFIX_END;
        byte[] position = null;
        if (query.paging()) {
            position = PageState.fromString(query.page()).position();
        }
        ConditionQuery originQuery = (ConditionQuery) query.originQuery();
        if (originQuery != null) {
            originQuery = prepareConditionQuery(originQuery);
        }
        byte[] ownerKeyFrom = this.ownerByQueryDelegate.apply(query.resultType(),
                                                              query.start());
        byte[] ownerKeyTo = this.ownerByQueryDelegate.apply(query.resultType(),
                                                            query.prefix());
        byte[] keyFrom = query.start().asBytes();
        // In the prefix pagination query, start is the initial position. Because in different partitions, the query starts from the start position.
        if (query.paging()) {
            keyFrom = query.prefix().asBytes();
        }
        byte[] keyTo = query.prefix().asBytes();
        byte[] queryBytes = originQuery == null ?
                            null :
                            originQuery.bytes();

        // LOG.debug("query {} with ownerKeyFrom: {}, ownerKeyTo: {}," +
        //          "keyFrom: {}, keyTo: {}, scanType: {}, conditionQuery: {}," +
        //          "position: {}",
        //          this.table(), bytes2String(ownerKeyFrom),
        //          bytes2String(ownerKeyTo), bytes2String(keyFrom),
        //          bytes2String(keyTo), type, originQuery, position);

        return session.scan(this.table(), ownerKeyFrom, ownerKeyTo, keyFrom,
                            keyTo, type, queryBytes, position);
    }

    protected List<BackendColumnIterator> queryByPrefixList(
            Session session,
            List<IdPrefixQuery> queries,
            String tableName) {
        E.checkArgument(queries.size() > 0,
                        "The size of queries must be greater than zero");
        IdPrefixQuery query = queries.get(0);
        int type = 0;
        LinkedList<HgOwnerKey> ownerKey = new LinkedList<>();
        queries.forEach((item) -> {
            byte[] prefix = this.ownerByQueryDelegate.apply(item.resultType(),
                                                            item.prefix());
            ownerKey.add(HgOwnerKey.of(prefix, item.prefix().asBytes()));
        });
        ConditionQuery originQuery = (ConditionQuery) query.originQuery();
        if (originQuery != null) {
            originQuery = prepareConditionQueryList(originQuery);
        }
        byte[] queryBytes = originQuery == null ? null : originQuery.bytes();

        // LOG.debug("query {} with scanType: {}, limit: {}, conditionQuery:
        // {}", this.table(), type, query.limit(), queryBytes);
        return session.scan(tableName, ownerKey, type,
                            query.limit(), queryBytes);
    }

    /***
     * Prepare ConditionQuery to do operator sinking, because some scenes do not need to be
     * preserved
     * @param conditionQuery
     * @return
     */
    private ConditionQuery prepareConditionQuery(ConditionQuery conditionQuery) {
        if (CollectionUtils.isEmpty(conditionQuery.userpropConditions())) {
            return null;
        }
        // only userpropConditions can send to store
        Collection<Condition> conditions = conditionQuery.conditions();
        List<Condition> newConditions = new ArrayList<>();
        for (Condition condition : conditions) {
            if (!onlyOwnerVertex(condition)) {
                newConditions.add(condition);
            }
        }
        if (newConditions.size() > 0) {
            conditionQuery.resetConditions(newConditions);
            return conditionQuery;
        } else {
            return null;
        }
    }

    /***
     * Prepare ConditionQuery to do operator sinking, because some scenes do not need to be
     * preserved
     * @param conditionQuery
     * @return
     */
    private ConditionQuery prepareConditionQueryList(ConditionQuery conditionQuery) {
        if (!conditionQuery.containsLabelOrUserpropRelation()) {
            return null;
        }
        // only userpropConditions can send to store
        Collection<Condition> conditions = conditionQuery.conditions();
        List<Condition> newConditions = new ArrayList<>();
        for (Condition condition : conditions) {
            if (!onlyOwnerVertex(condition)) {
                newConditions.add(condition);
            }
        }
        if (newConditions.size() > 0) {
            conditionQuery.resetConditions(newConditions);
            return conditionQuery;
        } else {
            return null;
        }
    }

    private boolean onlyOwnerVertex(Condition condition) {
        boolean onlyOwnerVertex = true;
        List<? extends Relation> relations = condition.relations();
        for (Relation r : relations) {
            if (!r.key().equals(HugeKeys.OWNER_VERTEX)) {
                onlyOwnerVertex = false;
                break;
            }
        }
        return onlyOwnerVertex;
    }

    protected BackendColumnIterator queryByRange(Session session,
                                                 IdRangeQuery query) {
        byte[] start = query.start().asBytes();
        byte[] end = query.end() == null ? null : query.end().asBytes();
        int type = query.inclusiveStart() ?
                   Session.SCAN_GTE_BEGIN : Session.SCAN_GT_BEGIN;
        if (end != null) {
            type |= query.inclusiveEnd() ?
                    Session.SCAN_LTE_END : Session.SCAN_LT_END;
        }
        ConditionQuery cq;
        Query origin = query.originQuery();
        byte[] position = null;
        if (query.paging() && !query.page().isEmpty()) {
            position = PageState.fromString(query.page()).position();
        }
        byte[] ownerStart = this.ownerByQueryDelegate.apply(query.resultType(),
                                                            query.start());
        byte[] ownerEnd = this.ownerByQueryDelegate.apply(query.resultType(),
                                                          query.end());
        if (origin instanceof ConditionQuery &&
            (query.resultType().isEdge() || query.resultType().isVertex())) {
            cq = (ConditionQuery) query.originQuery();

            // LOG.debug("query {} with ownerKeyFrom: {}, ownerKeyTo: {}, " +
            //          "keyFrom: {}, keyTo: {}, " +
            //          "scanType: {}, conditionQuery: {}",
            //          this.table(), bytes2String(ownerStart),
            //          bytes2String(ownerEnd), bytes2String(start),
            //          bytes2String(end), type, cq.bytes());
            return session.scan(this.table(), ownerStart,
                                ownerEnd, start, end, type, cq.bytes(), position);
        }
        return session.scan(this.table(), ownerStart,
                            ownerEnd, start, end, type, null, position);
    }

    protected BackendColumnIterator queryByCond(Session session,
                                                ConditionQuery query) {
        if (query.containsScanCondition()) {
            E.checkArgument(query.relations().size() == 1,
                            "Invalid scan with multi conditions: %s", query);
            Relation scan = query.relations().iterator().next();
            Shard shard = (Shard) scan.value();
            return this.queryByRange(session, shard, query);
        }
        // throw new NotSupportException("query: %s", query);
        return this.queryAll(session, query);
    }

    protected BackendColumnIterator queryByRange(Session session, Shard shard,
                                                 ConditionQuery query) {
        int type = Session.SCAN_GTE_BEGIN;
        type |= Session.SCAN_LT_END;
        type |= Session.SCAN_HASHCODE;
        type |= query.withProperties() ? 0 : Session.SCAN_KEY_ONLY;

        int start = Integer.parseInt(StringUtils.isEmpty(shard.start()) ?
                                     "0" : shard.start());
        int end = Integer.parseInt(StringUtils.isEmpty(shard.end()) ?
                                   "0" : shard.end());
        byte[] queryBytes = query.bytes();
        String page = query.page();
        if (page != null && !page.isEmpty()) {
            byte[] position = PageState.fromString(page).position();
            return session.scan(this.table(), start, end, type, queryBytes,
                                position);
        }
        return session.scan(this.table(), start, end, type, queryBytes);
    }

    private static class HstoreShardSplitter extends ShardSplitter<Session> {

        public HstoreShardSplitter(String table) {
            super(table);
        }

        @Override
        public List<Shard> getSplits(Session session, long splitSize) {
            E.checkArgument(splitSize >= MIN_SHARD_SIZE,
                            "The split-size must be >= %s bytes, but got %s",
                            MIN_SHARD_SIZE, splitSize);

            List<Shard> splits = new ArrayList<>();
            try {
                PDClient pdClient = HstoreSessionsImpl.getDefaultPdClient();
                List<Metapb.Partition> partitions = pdClient.getPartitions(0,
                                                                           session.getGraphName());
                for (Metapb.Partition partition : partitions) {
                    String start = String.valueOf(partition.getStartKey());
                    String end = String.valueOf(partition.getEndKey());
                    splits.add(new Shard(start, end, 0));
                }
            } catch (PDException e) {
                e.printStackTrace();
            }

            return splits.size() != 0 ?
                   splits : super.getSplits(session, splitSize);
        }

        @Override
        public long estimateDataSize(Session session) {
            return 1L;
        }

        @Override
        public long estimateNumKeys(Session session) {
            return 1L;
        }

        @Override
        public byte[] position(String position) {
            if (END.equals(position)) {
                return null;
            }
            return StringEncoding.decodeBase64(position);
        }
    }
}
