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
import java.util.Arrays;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hugegraph.query.Query;
import org.apache.hugegraph.backend.serializer.BinaryBackendEntry;
import org.apache.hugegraph.backend.serializer.BinaryEntryIterator;
import org.apache.hugegraph.serializer.BytesBuffer;
import org.apache.hugegraph.backend.store.BackendEntry;
import org.apache.hugegraph.backend.BackendColumn;
import org.apache.hugegraph.backend.store.BackendEntryIterator;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.type.define.HugeKeys;
import org.apache.hugegraph.util.NumericUtil;
import org.apache.hugegraph.util.StringEncoding;

public class HbaseTables {

    public static class Meta extends HbaseTable {

        private static final String TABLE = HugeType.META.string();
        private static final byte[] COL = Bytes.toBytes(TABLE);

        public Meta() {
            super(TABLE);
        }

        public void writeVersion(HbaseSessions.Session session, String version) {
            byte[] key = new byte[]{HugeKeys.VERSION.code()};
            byte[] value = StringEncoding.encode(version);
            session.put(this.table(), CF, key, COL, value);
            try {
                session.commit();
            } catch (Exception e) {
                session.rollback();
            }
        }

        public String readVersion(HbaseSessions.Session session) {
            byte[] key = new byte[]{HugeKeys.VERSION.code()};
            HbaseSessions.RowIterator results = session.get(this.table(), CF, key);
            if (!results.hasNext()) {
                return null;
            }
            Result row = results.next();
            return StringEncoding.decode(row.getValue(CF, COL));
        }
    }

    public static class Counters extends HbaseTable {

        private static final String TABLE = HugeType.COUNTER.string();
        private static final byte[] COL = Bytes.toBytes(TABLE);

        public Counters() {
            super(TABLE);
        }

        public long getCounter(HbaseSessions.Session session, HugeType type) {
            byte[] key = new byte[]{type.code()};
            HbaseSessions.RowIterator results = session.get(this.table(), CF, key);
            if (results.hasNext()) {
                Result row = results.next();
                return NumericUtil.bytesToLong(row.getValue(CF, COL));
            } else {
                return 0L;
            }
        }

        public void increaseCounter(HbaseSessions.Session session, HugeType type,
                                    long increment) {
            byte[] key = new byte[]{type.code()};
            session.increase(this.table(), CF, key, COL, increment);
        }
    }

    public static class VertexLabel extends HbaseTable {

        public static final String TABLE = HugeType.VERTEX_LABEL.string();

        public VertexLabel() {
            super(TABLE);
        }
    }

    public static class EdgeLabel extends HbaseTable {

        public static final String TABLE = HugeType.EDGE_LABEL.string();

        public EdgeLabel() {
            super(TABLE);
        }
    }

    public static class PropertyKey extends HbaseTable {

        public static final String TABLE = HugeType.PROPERTY_KEY.string();

        public PropertyKey() {
            super(TABLE);
        }
    }

    public static class IndexLabel extends HbaseTable {

        public static final String TABLE = HugeType.INDEX_LABEL.string();

        public IndexLabel() {
            super(TABLE);
        }
    }

    public static class Vertex extends HbaseTable {

        public static final String TABLE = HugeType.VERTEX.string();

        public Vertex(String store, boolean enablePartition) {
            super(joinTableName(store, TABLE), enablePartition);
        }

        @Override
        public void insert(HbaseSessions.Session session, BackendEntry entry) {
            long ttl = entry.ttl();
            if (ttl == 0L) {
                session.put(this.table(), CF, entry.id().asBytes(),
                            entry.columns());
            } else {
                session.put(this.table(), CF, entry.id().asBytes(),
                            entry.columns(), ttl);
            }
        }
    }

    public static class Edge extends HbaseTable {

        public static final String TABLE_SUFFIX = HugeType.EDGE.string();

        public Edge(String store, boolean out, boolean enablePartition) {
            super(joinTableName(store, table(out)), enablePartition);
        }

        private static String table(boolean out) {
            // Edge out/in table
            return (out ? 'o' : 'i') + TABLE_SUFFIX;
        }

        public static Edge out(String store, boolean enablePartition) {
            return new Edge(store, true, enablePartition);
        }

        public static Edge in(String store, boolean enablePartition) {
            return new Edge(store, false, enablePartition);
        }

        @Override
        public void insert(HbaseSessions.Session session, BackendEntry entry) {
            long ttl = entry.ttl();
            if (ttl == 0L) {
                session.put(this.table(), CF, entry.id().asBytes(),
                            entry.columns());
            } else {
                session.put(this.table(), CF, entry.id().asBytes(),
                            entry.columns(), ttl);
            }
        }

        @Override
        protected void parseRowColumns(Result row, BackendEntry entry,
                                       Query query, boolean enablePartition) throws IOException {
            /*
             * Collapse owner-vertex id from edge id, NOTE: unneeded to
             * collapse if BinarySerializer.keyWithIdPrefix set to true
             */
            byte[] key = row.getRow();
            if (enablePartition) {
                key = Arrays.copyOfRange(key, entry.id().length() + 2, key.length);
            } else {
                key = Arrays.copyOfRange(key, entry.id().length(), key.length);
            }

            long total = query.total();
            CellScanner cellScanner = row.cellScanner();
            while (cellScanner.advance() && total-- > 0) {
                Cell cell = cellScanner.current();
                assert CellUtil.cloneQualifier(cell).length == 0;
                entry.columns(BackendColumn.of(key, CellUtil.cloneValue(cell)));
            }
        }
    }

    public static class IndexTable extends HbaseTable {

        private static final long INDEX_DELETE_BATCH = Query.COMMIT_BATCH;
        protected final HugeType type;

        public IndexTable(String table, HugeType type) {
            super(table);
            this.type = type;
        }

        public HugeType type() {
            return this.type;
        }

        @Override
        public void insert(HbaseSessions.Session session, BackendEntry entry) {
            assert entry.columns().size() == 1;
            BackendColumn col = entry.columns().iterator().next();
            long ttl = entry.ttl();
            if (ttl == 0L) {
                session.put(this.table(), CF, col.name,
                            BytesBuffer.BYTES_EMPTY, col.value);
            } else {
                session.put(this.table(), CF, col.name,
                            BytesBuffer.BYTES_EMPTY, col.value, ttl);
            }
        }

        @Override
        public void eliminate(HbaseSessions.Session session, BackendEntry entry) {
            assert entry.columns().size() == 1;
            BackendColumn col = entry.columns().iterator().next();
            session.delete(this.table(), CF, col.name);
        }

        @Override
        public void delete(HbaseSessions.Session session, BackendEntry entry) {
            /*
             * Only delete index by label will come here
             * Regular index delete will call eliminate()
             */
            long count = 0L;
            for (BackendColumn column : entry.columns()) {
                session.commit();
                // Prefix query index label related indexes
                HbaseSessions.RowIterator iter = session.scan(this.table(), column.name);
                while (iter.hasNext()) {
                    session.delete(this.table(), CF, iter.next().getRow());
                    // Commit once reaching batch size
                    if (++count >= INDEX_DELETE_BATCH) {
                        session.commit();
                        count = 0L;
                    }
                }
            }
            if (count > 0L) {
                session.commit();
            }
        }

        @Override
        protected BackendEntryIterator newEntryIterator(Query query,
                                                        HbaseSessions.RowIterator rows) {
            return new BinaryEntryIterator<>(rows, query, (entry, row) -> {
                assert row.size() == 1;
                BackendColumn col = BackendColumn.of(row.getRow(), row.value());
                entry = new BinaryBackendEntry(query.resultType(), col.name);
                entry.columns(col);
                return entry;
            });
        }
    }

    public static class VertexLabelIndex extends IndexTable {

        public static final String TABLE = HugeType.VERTEX_LABEL_INDEX.string();

        public VertexLabelIndex(String store) {
            super(joinTableName(store, TABLE), HugeType.SECONDARY_INDEX);
        }
    }

    public static class EdgeLabelIndex extends IndexTable {

        public static final String TABLE = HugeType.EDGE_LABEL_INDEX.string();

        public EdgeLabelIndex(String store) {
            super(joinTableName(store, TABLE), HugeType.SECONDARY_INDEX);
        }
    }

    public static class SecondaryIndex extends IndexTable {

        public static final String TABLE = HugeType.SECONDARY_INDEX.string();

        public SecondaryIndex(String store) {
            super(joinTableName(store, TABLE), HugeType.SECONDARY_INDEX);
        }
    }

    public static class SearchIndex extends IndexTable {

        public static final String TABLE = HugeType.SEARCH_INDEX.string();

        public SearchIndex(String store) {
            super(joinTableName(store, TABLE), HugeType.SECONDARY_INDEX);
        }
    }

    public static class UniqueIndex extends IndexTable {

        public static final String TABLE = HugeType.UNIQUE_INDEX.string();

        public UniqueIndex(String store) {
            super(joinTableName(store, TABLE), HugeType.SECONDARY_INDEX);
        }
    }

    public static class RangeIndex extends IndexTable {

        public RangeIndex(String store, HugeType type) {
            super(joinTableName(store, type.string()), type);
        }

        public static RangeIndex rangeInt(String store) {
            return new RangeIndex(store, HugeType.RANGE_INT_INDEX);
        }

        public static RangeIndex rangeFloat(String store) {
            return new RangeIndex(store, HugeType.RANGE_FLOAT_INDEX);
        }

        public static RangeIndex rangeLong(String store) {
            return new RangeIndex(store, HugeType.RANGE_LONG_INDEX);
        }

        public static RangeIndex rangeDouble(String store) {
            return new RangeIndex(store, HugeType.RANGE_DOUBLE_INDEX);
        }
    }

    public static class ShardIndex extends IndexTable {

        public static final String TABLE = HugeType.SHARD_INDEX.string();

        public ShardIndex(String store) {
            super(joinTableName(store, TABLE), HugeType.SECONDARY_INDEX);
        }
    }
}
