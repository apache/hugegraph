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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.RegionMetrics;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.Size;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hugegraph.exception.BackendException;
import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.backend.page.PageState;
import org.apache.hugegraph.query.Aggregate;
import org.apache.hugegraph.query.Aggregate.AggregateFunc;
import org.apache.hugegraph.query.Condition.Relation;
import org.apache.hugegraph.query.ConditionQuery;
import org.apache.hugegraph.query.IdPrefixQuery;
import org.apache.hugegraph.query.IdRangeQuery;
import org.apache.hugegraph.query.Query;
import org.apache.hugegraph.backend.serializer.BinaryBackendEntry;
import org.apache.hugegraph.backend.serializer.BinaryEntryIterator;
import org.apache.hugegraph.backend.store.BackendEntry;
import org.apache.hugegraph.backend.BackendColumn;
import org.apache.hugegraph.backend.store.BackendEntryIterator;
import org.apache.hugegraph.backend.store.BackendTable;
import org.apache.hugegraph.backend.Shard;
import org.apache.hugegraph.exception.NotSupportException;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.util.Bytes;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.InsertionOrderUtil;
import org.apache.hugegraph.util.Log;
import org.apache.hugegraph.util.StringEncoding;
import org.slf4j.Logger;

import com.google.common.collect.ImmutableList;

public class HbaseTable extends BackendTable<HbaseSessions.Session, BackendEntry> {

    private static final Logger LOG = Log.logger(HbaseStore.class);

    protected static final byte[] CF = "f".getBytes();

    private final HbaseShardSplitter shardSplitter;

    private final boolean enablePartition;

    public HbaseTable(String table, boolean enablePartition) {
        super(table);
        this.enablePartition = enablePartition;
        this.shardSplitter = new HbaseShardSplitter(this.table());
    }

    public HbaseTable(String table) {
        this(table, false);
    }

    public static List<byte[]> cfs() {
        return ImmutableList.of(CF);
    }

    @Override
    protected void registerMetaHandlers() {
        this.registerMetaHandler("splits", (session, meta, args) -> {
            E.checkArgument(args.length == 1,
                            "The args count of %s must be 1", meta);
            long splitSize = (long) args[0];
            return this.shardSplitter.getSplits(session, splitSize);
        });
    }

    @Override
    public void init(HbaseSessions.Session session) {
        // pass
    }

    @Override
    public void clear(HbaseSessions.Session session) {
        // pass
    }

    @Override
    public void insert(HbaseSessions.Session session, BackendEntry entry) {
        assert !entry.columns().isEmpty();
        session.put(this.table(), CF, entry.id().asBytes(), entry.columns());
    }

    @Override
    public void delete(HbaseSessions.Session session, BackendEntry entry) {
        if (entry.columns().isEmpty()) {
            session.delete(this.table(), CF, entry.id().asBytes());
        } else {
            for (BackendColumn col : entry.columns()) {
                session.remove(table(), CF, entry.id().asBytes(), col.name);
            }
        }
    }

    @Override
    public void append(HbaseSessions.Session session, BackendEntry entry) {
        assert entry.columns().size() == 1;
        this.insert(session, entry);
    }

    @Override
    public void eliminate(HbaseSessions.Session session, BackendEntry entry) {
        assert entry.columns().size() == 1;
        this.delete(session, entry);
    }

    @Override
    public boolean queryExist(HbaseSessions.Session session, BackendEntry entry) {
        Id id = entry.id();
        try (HbaseSessions.RowIterator iter = this.queryById(session, id)) {
            return iter.hasNext();
        }
    }

    @Override
    public Number queryNumber(HbaseSessions.Session session, Query query) {
        Aggregate aggregate = query.aggregateNotNull();
        if (aggregate.func() != AggregateFunc.COUNT) {
            throw new NotSupportException(aggregate.toString());
        }

        assert aggregate.func() == AggregateFunc.COUNT;
        try (HbaseSessions.CountSession countSession = session.countSession()) {
            return this.query(countSession, query);
        } catch (IOException e) {
            throw new BackendException(e);
        }
    }

    @Override
    public Iterator<BackendEntry> query(HbaseSessions.Session session, Query query) {
        if (query.limit() == 0L && !query.noLimit()) {
            LOG.debug("Return empty result(limit=0) for query {}", query);
            return Collections.emptyIterator();
        }

        HbaseSessions.HbaseSession<HbaseSessions.RowIterator> hbaseSession = session;
        return this.newEntryIterator(query, this.query(hbaseSession, query));
    }

    protected <R> R query(HbaseSessions.HbaseSession<R> session, Query query) {
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
            if (query.idsSize() == 1) {
                Id id = query.ids().iterator().next();
                return this.queryById(session, id);
            } else {
                return this.queryByIds(session, query.ids());
            }
        }

        // Query by condition (or condition + id)
        ConditionQuery cq = (ConditionQuery) query;
        return this.queryByCond(session, cq);
    }

    protected <R> R queryAll(HbaseSessions.HbaseSession<R> session, Query query) {
        if (query.paging()) {
            PageState page = PageState.fromString(query.page());
            byte[] begin = page.position();
            return session.scan(this.table(), begin, null);
        } else {
            return session.scan(this.table(), -1);
        }
    }

    protected <R> R queryById(HbaseSessions.HbaseSession<R> session, Id id) {
        return session.get(this.table(), null, id.asBytes());
    }

    protected <R> R queryByIds(HbaseSessions.HbaseSession<R> session, Collection<Id> ids) {
        Set<byte[]> rowkeys = InsertionOrderUtil.newSet();
        for (Id id : ids) {
            rowkeys.add(id.asBytes());
        }
        return session.get(this.table(), null, rowkeys);
    }

    protected <R> R queryByPrefix(HbaseSessions.HbaseSession<R> session,
                                  IdPrefixQuery query) {
        return session.scan(this.table(), query.start().asBytes(),
                            query.inclusiveStart(), query.prefix().asBytes());
    }

    protected <R> R queryByRange(HbaseSessions.HbaseSession<R> session, IdRangeQuery query) {
        byte[] start = query.start().asBytes();
        byte[] end = query.end() == null ? null : query.end().asBytes();
        return session.scan(this.table(), start, query.inclusiveStart(),
                            end, query.inclusiveEnd());
    }

    protected <R> R queryByCond(HbaseSessions.HbaseSession<R> session, ConditionQuery query) {
        if (query.containsScanRelation()) {
            E.checkArgument(query.relations().size() == 1,
                            "Invalid scan with multi conditions: %s", query);
            Relation scan = query.relations().iterator().next();
            Shard shard = (Shard) scan.value();
            return this.queryByRange(session, shard, query.page());
        }
        throw new NotSupportException("query: %s", query);
    }

    protected <R> R queryByRange(HbaseSessions.HbaseSession<R> session,
                                 Shard shard, String page) {
        byte[] start = this.shardSplitter.position(shard.start());
        byte[] end = this.shardSplitter.position(shard.end());
        if (page != null && !page.isEmpty()) {
            byte[] position = PageState.fromString(page).position();
            E.checkArgument(start == null ||
                            Bytes.compare(position, start) >= 0,
                            "Invalid page out of lower bound");
            start = position;
        }
        return session.scan(this.table(), start, end);
    }

    protected BackendEntryIterator newEntryIterator(Query query,
                                                    HbaseSessions.RowIterator rows) {
        return new BinaryEntryIterator<>(rows, query, (entry, row) -> {
            E.checkState(!row.isEmpty(), "Can't parse empty HBase result");
            byte[] id = row.getRow();
            if (entry == null || !Bytes.prefixWith(id, entry.id().asBytes())) {
                HugeType type = query.resultType();
                // NOTE: only support BinaryBackendEntry currently
                entry = new BinaryBackendEntry(type, id, this.enablePartition);
            }
            try {
                this.parseRowColumns(row, entry, query, this.enablePartition);
            } catch (IOException e) {
                throw new BackendException("Failed to read HBase columns", e);
            }
            return entry;
        });
    }

    protected void parseRowColumns(Result row, BackendEntry entry, Query query,
                                   boolean enablePartition)
            throws IOException {
        CellScanner cellScanner = row.cellScanner();
        while (cellScanner.advance()) {
            Cell cell = cellScanner.current();
            entry.columns(BackendColumn.of(CellUtil.cloneQualifier(cell),
                                           CellUtil.cloneValue(cell)));
        }
    }

    private static class HbaseShardSplitter extends ShardSplitter<HbaseSessions.Session> {

        public HbaseShardSplitter(String table) {
            super(table);
        }

        @Override
        public List<Shard> getSplits(HbaseSessions.Session session, long splitSize) {
            E.checkArgument(splitSize >= MIN_SHARD_SIZE,
                            "The split-size must be >= %s bytes, but got %s",
                            MIN_SHARD_SIZE, splitSize);
            List<Shard> shards = new ArrayList<>();
            String namespace = session.namespace();
            String table = this.table();

            // Calc data size for each region
            Map<String, Double> regionSizes = regionSizes(session, namespace,
                                                          table);
            // Get token range of each region
            Map<String, Range> regionRanges = regionRanges(session, namespace,
                                                           table);
            // Split regions to shards
            for (Map.Entry<String, Double> rs : regionSizes.entrySet()) {
                String region = rs.getKey();
                double size = rs.getValue();
                Range range = regionRanges.get(region);
                int count = calcSplitCount(size, splitSize);
                shards.addAll(range.splitEven(count));
            }
            return shards;
        }

        private static Map<String, Double> regionSizes(HbaseSessions.Session session,
                                                       String namespace,
                                                       String table) {
            Map<String, Double> regionSizes = new HashMap<>();
            try (Admin admin = session.hbase().getAdmin()) {
                TableName tableName = TableName.valueOf(namespace, table);
                for (ServerName serverName : admin.getRegionServers()) {
                    List<RegionMetrics> metrics = admin.getRegionMetrics(
                            serverName, tableName);
                    for (RegionMetrics metric : metrics) {
                        double size = metric.getStoreFileSize()
                                            .get(Size.Unit.BYTE);
                        size += metric.getMemStoreSize().get(Size.Unit.BYTE);
                        regionSizes.put(metric.getNameAsString(), size);
                    }
                }
            } catch (Throwable e) {
                throw new BackendException(String.format(
                        "Failed to get region sizes of %s(%s)",
                        table, namespace), e);
            }
            return regionSizes;
        }

        private static Map<String, Range> regionRanges(HbaseSessions.Session session,
                                                       String namespace,
                                                       String table) {
            Map<String, Range> regionRanges = InsertionOrderUtil.newMap();
            TableName tableName = TableName.valueOf(namespace, table);
            try (Admin admin = session.hbase().getAdmin()) {
                for (RegionInfo regionInfo : admin.getRegions(tableName)) {
                    byte[] start = regionInfo.getStartKey();
                    byte[] end = regionInfo.getEndKey();
                    regionRanges.put(regionInfo.getRegionNameAsString(),
                                     new Range(start, end));
                }
            } catch (Throwable e) {
                throw new BackendException(String.format(
                        "Failed to get region ranges of %s(%s)",
                        table, namespace), e);
            }
            return regionRanges;
        }

        private static int calcSplitCount(double totalSize, long splitSize) {
            return (int) Math.ceil(totalSize / splitSize);
        }

        @Override
        public byte[] position(String position) {
            if (START.equals(position) || END.equals(position)) {
                return null;
            }
            return StringEncoding.decodeBase64(position);
        }

        @Override
        public long estimateDataSize(HbaseSessions.Session session) {
            try {
                return session.storeSize(this.table());
            } catch (IOException ignored) {
                return -1L;
            }
        }

        @Override
        public long estimateNumKeys(HbaseSessions.Session session) {
            // TODO: improve
            return 100000L;
        }
    }
}
