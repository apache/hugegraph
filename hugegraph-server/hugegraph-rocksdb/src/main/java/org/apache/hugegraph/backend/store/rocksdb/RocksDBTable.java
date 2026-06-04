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

package org.apache.hugegraph.backend.store.rocksdb;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;

import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.page.PageState;
import org.apache.hugegraph.backend.query.Aggregate;
import org.apache.hugegraph.backend.query.Aggregate.AggregateFunc;
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
import org.apache.hugegraph.backend.store.rocksdb.RocksDBSessions.Session;
import org.apache.hugegraph.exception.NotSupportException;
import org.apache.hugegraph.iterator.FlatMapperIterator;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.util.Bytes;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Log;
import org.apache.hugegraph.util.StringEncoding;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.slf4j.Logger;

public class RocksDBTable extends BackendTable<RocksDBSessions.Session, BackendEntry> {

    private static final Logger LOG = Log.logger(RocksDBTable.class);

    private final RocksDBShardSplitter shardSplitter;

    public RocksDBTable(String database, String table) {
        super(String.format("%s+%s", database, table));
        this.shardSplitter = new RocksDBShardSplitter(this.table());
    }

    @Override
    protected void registerMetaHandlers() {
        this.registerMetaHandler("splits", (session, meta, args) -> {
            E.checkArgument(args.length == 1, "The args count of %s must be 1", meta);
            long splitSize = (long) args[0];
            return this.shardSplitter.getSplits(session, splitSize);
        });
    }

    @Override
    public void init(RocksDBSessions.Session session) {
        // pass
    }

    @Override
    public void clear(RocksDBSessions.Session session) {
        // pass
    }

    @Override
    public void insert(RocksDBSessions.Session session, BackendEntry entry) {
        assert !entry.columns().isEmpty();
        for (BackendColumn col : entry.columns()) {
            assert entry.belongToMe(col) : entry;
            session.put(this.table(), col.name, col.value);
        }
    }

    @Override
    public void delete(RocksDBSessions.Session session, BackendEntry entry) {
        if (entry.columns().isEmpty()) {
            session.delete(this.table(), entry.id().asBytes());
        } else {
            for (BackendColumn col : entry.columns()) {
                assert entry.belongToMe(col) : entry;
                session.delete(this.table(), col.name);
            }
        }
    }

    @Override
    public void append(RocksDBSessions.Session session, BackendEntry entry) {
        assert entry.columns().size() == 1;
        this.insert(session, entry);
    }

    @Override
    public void eliminate(RocksDBSessions.Session session, BackendEntry entry) {
        assert entry.columns().size() == 1;
        this.delete(session, entry);
    }

    @Override
    public boolean queryExist(RocksDBSessions.Session session, BackendEntry entry) {
        Id id = entry.id();
        try (BackendColumnIterator iter = this.queryById(session, id)) {
            return iter.hasNext();
        }
    }

    @Override
    public Number queryNumber(RocksDBSessions.Session session, Query query) {
        Aggregate aggregate = query.aggregateNotNull();
        if (aggregate.func() != AggregateFunc.COUNT) {
            throw new NotSupportException(aggregate.toString());
        }

        assert aggregate.func() == AggregateFunc.COUNT;
        assert query.noLimit();
        try (BackendColumnIterator results = this.queryBy(session, query)) {
            if (results instanceof RocksDBSessions.Countable) {
                return ((RocksDBSessions.Countable) results).count();
            }
            return IteratorUtils.count(results);
        }
    }

    @Override
    public Iterator<BackendEntry> query(RocksDBSessions.Session session, Query query) {
        if (query.limit() == 0L && !query.noLimit()) {
            LOG.debug("Return empty result(limit=0) for query {}", query);
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

    protected BackendColumnIterator queryBy(RocksDBSessions.Session session, Query query) {
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
        if (query.conditionsSize() == 0) {
            assert query.idsSize() > 0;
            return this.queryByIds(session, query.ids());
        }

        // Query by condition (or condition + id)
        ConditionQuery cq = (ConditionQuery) query;
        return this.queryByCond(session, cq);
    }

    protected BackendColumnIterator queryAll(RocksDBSessions.Session session, Query query) {
        if (query.paging()) {
            PageState page = PageState.fromString(query.page());
            byte[] begin = page.position();
            return session.scan(this.table(), begin, null, RocksDBSessions.Session.SCAN_ANY);
        } else {
            return session.scan(this.table());
        }
    }

    protected BackendColumnIterator queryById(RocksDBSessions.Session session, Id id) {
        // TODO: change to get() after vertex and schema don't use id prefix
        return session.scan(this.table(), id.asBytes());
    }

    protected BackendColumnIterator queryByIds(RocksDBSessions.Session session,
                                               Collection<Id> ids) {
        if (ids.size() == 1) {
            return this.queryById(session, ids.iterator().next());
        }

        // NOTE: this will lead to lazy create rocksdb iterator
        return BackendColumnIterator.wrap(new FlatMapperIterator<>(
                ids.iterator(), id -> this.queryById(session, id)
        ));
    }

    protected BackendColumnIterator queryByIdsWithGet(RocksDBSessions.Session session,
                                                      Collection<Id> ids) {
        E.checkState(!session.hasChanges(),
                     "Can't queryByIds() when RocksDB session has pending changes");
        if (ids.isEmpty()) {
            return BackendColumnIterator.empty();
        }
        return this.getByIds(session, ids);
    }

    protected BackendColumnIterator getById(RocksDBSessions.Session session, Id id) {
        byte[] value = session.get(this.table(), id.asBytes());
        if (value == null) {
            return BackendColumnIterator.empty();
        }
        BackendColumn col = BackendColumn.of(id.asBytes(), value);
        return BackendColumnIterator.iterator(col);
    }

    protected BackendColumnIterator getByIds(RocksDBSessions.Session session,
                                             Collection<Id> ids) {
        if (ids.size() == 1) {
            return this.getById(session, ids.iterator().next());
        }

        List<byte[]> keys = new ArrayList<>(ids.size());
        for (Id id : ids) {
            keys.add(id.asBytes());
        }
        return session.get(this.table(), keys);
    }

    protected BackendColumnIterator queryByPrefix(RocksDBSessions.Session session,
                                                  IdPrefixQuery query) {
        int type = query.inclusiveStart() ?
                   RocksDBSessions.Session.SCAN_GTE_BEGIN : RocksDBSessions.Session.SCAN_GT_BEGIN;
        type |= RocksDBSessions.Session.SCAN_PREFIX_END;
        return session.scan(this.table(), query.start().asBytes(), query.prefix().asBytes(), type);
    }

    protected BackendColumnIterator queryByRange(RocksDBSessions.Session session,
                                                 IdRangeQuery query) {
        byte[] start = query.start().asBytes();
        byte[] end = query.end() == null ? null : query.end().asBytes();
        int type = query.inclusiveStart() ?
                   RocksDBSessions.Session.SCAN_GTE_BEGIN : RocksDBSessions.Session.SCAN_GT_BEGIN;
        if (end != null) {
            type |= query.inclusiveEnd() ?
                    RocksDBSessions.Session.SCAN_LTE_END : RocksDBSessions.Session.SCAN_LT_END;
        }
        return session.scan(this.table(), start, end, type);
    }

    protected BackendColumnIterator queryByCond(RocksDBSessions.Session session,
                                                ConditionQuery query) {
        if (query.containsScanRelation()) {
            E.checkArgument(query.relations().size() == 1,
                            "Invalid scan with multi conditions: %s", query);
            Relation scan = query.relations().iterator().next();
            Shard shard = (Shard) scan.value();
            return this.queryByRange(session, shard, query.page());
        }
        throw new NotSupportException("query: %s", query);
    }

    protected BackendColumnIterator queryByRange(RocksDBSessions.Session session, Shard shard,
                                                 String page) {
        byte[] start = this.shardSplitter.position(shard.start());
        byte[] end = this.shardSplitter.position(shard.end());
        if (page != null && !page.isEmpty()) {
            byte[] position = PageState.fromString(page).position();
            E.checkArgument(start == null || Bytes.compare(position, start) >= 0,
                            "Invalid page out of lower bound");
            start = position;
        }
        if (start == null) {
            start = ShardSplitter.START_BYTES;
        }
        int type = RocksDBSessions.Session.SCAN_GTE_BEGIN;
        if (end != null) {
            type |= RocksDBSessions.Session.SCAN_LT_END;
        }
        return session.scan(this.table(), start, end, type);
    }

    public boolean isOlap() {
        return false;
    }

    protected static BackendEntryIterator newEntryIterator(BackendColumnIterator cols,
                                                           Query query) {
        return new BinaryEntryIterator<>(cols, query, (entry, col) -> {
            if (entry == null || !entry.belongToMe(col)) {
                HugeType type = query.resultType();
                // NOTE: only support BinaryBackendEntry currently
                entry = new BinaryBackendEntry(type, col.name);
            } else {
                assert !Bytes.equals(entry.id().asBytes(), col.name);
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

    protected static long sizeOfBackendEntry(BackendEntry entry) {
        return BinaryEntryIterator.sizeOfEntry(entry);
    }

    private static class RocksDBShardSplitter extends ShardSplitter<RocksDBSessions.Session> {

        private static final String MEM_SIZE = "rocksdb.size-all-mem-tables";
        private static final String SST_SIZE = "rocksdb.total-sst-files-size";
        private static final String NUM_KEYS = "rocksdb.estimate-num-keys";

        public RocksDBShardSplitter(String table) {
            super(table);
        }

        @Override
        public List<Shard> getSplits(RocksDBSessions.Session session, long splitSize) {
            E.checkArgument(splitSize >= MIN_SHARD_SIZE,
                            "The split-size must be >= %s bytes, but got %s",
                            MIN_SHARD_SIZE, splitSize);

            Pair<byte[], byte[]> keyRange = session.keyRange(this.table());
            if (keyRange == null || keyRange.getRight() == null) {
                return super.getSplits(session, splitSize);
            }

            long size = this.estimateDataSize(session);
            if (size <= 0) {
                size = this.estimateNumKeys(session) * ESTIMATE_BYTES_PER_KV;
            }

            double count = Math.ceil(size / (double) splitSize);
            if (count <= 0) {
                count = 1;
            }

            Range range = new Range(keyRange.getLeft(), Range.increase(keyRange.getRight()));
            List<Shard> splits = new ArrayList<>((int) count);
            splits.addAll(range.splitEven((int) count));
            return splits;
        }

        @Override
        public long estimateDataSize(RocksDBSessions.Session session) {
            long mem = Long.parseLong(session.property(this.table(), MEM_SIZE));
            long sst = Long.parseLong(session.property(this.table(), SST_SIZE));
            return mem + sst;
        }

        @Override
        public long estimateNumKeys(RocksDBSessions.Session session) {
            return Long.parseLong(session.property(this.table(), NUM_KEYS));
        }

        @Override
        public byte[] position(String position) {
            // TODO: START & END is same & be empty now? remove one?
            if (START.equals(position) || END.equals(position)) {
                return null;
            }
            return StringEncoding.decodeBase64(position);
        }
    }
}
