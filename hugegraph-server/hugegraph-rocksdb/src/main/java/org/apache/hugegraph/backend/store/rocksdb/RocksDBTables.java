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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Collection;
import java.util.List;

import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.query.Condition;
import org.apache.hugegraph.backend.query.Condition.Relation;
import org.apache.hugegraph.backend.query.ConditionQuery;
import org.apache.hugegraph.backend.serializer.BinarySerializer;
import org.apache.hugegraph.backend.store.BackendEntry;
import org.apache.hugegraph.backend.store.BackendEntry.BackendColumnIterator;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.type.define.HugeKeys;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.StringEncoding;

public class RocksDBTables {

    public static class Meta extends RocksDBTable {

        private static final String TABLE = HugeType.META.string();

        public Meta(String database) {
            super(database, TABLE);
        }

        public void writeVersion(RocksDBSessions.Session session, String version) {
            byte[] key = new byte[]{HugeKeys.VERSION.code()};
            byte[] value = StringEncoding.encode(version);
            session.put(this.table(), key, value);
            try {
                session.commit();
            } catch (Exception e) {
                session.rollback();
                throw e;
            }
        }

        public String readVersion(RocksDBSessions.Session session) {
            byte[] key = new byte[]{HugeKeys.VERSION.code()};
            byte[] value = session.get(this.table(), key);
            if (value == null) {
                return null;
            }
            return StringEncoding.decode(value);
        }
    }

    public static class Counters extends RocksDBTable {

        private static final String TABLE = HugeType.COUNTER.string();

        public Counters(String database) {
            super(database, TABLE);
        }

        public long getCounter(RocksDBSessions.Session session, HugeType type) {
            byte[] key = new byte[]{type.code()};
            byte[] value = session.get(this.table(), key);
            if (value != null) {
                return toLong(value);
            } else {
                return 0L;
            }
        }

        public void increaseCounter(RocksDBSessions.Session session, HugeType type,
                                    long increment) {
            byte[] key = new byte[]{type.code()};
            session.increase(this.table(), key, toBytes(increment));
        }

        private static byte[] toBytes(long value) {
            return ByteBuffer.allocate(Long.BYTES)
                             .order(ByteOrder.nativeOrder())
                             .putLong(value).array();
        }

        private static long toLong(byte[] bytes) {
            assert bytes.length == Long.BYTES;
            return ByteBuffer.wrap(bytes)
                             .order(ByteOrder.nativeOrder())
                             .getLong();
        }
    }

    public static class SchemaTable extends RocksDBTable {

        public SchemaTable(String database, String table) {
            super(database, table);
        }

        @Override
        public void delete(RocksDBSessions.Session session, BackendEntry entry) {
            assert entry.columns().isEmpty();
            /*
             * Use `scanPrefix + delete` instead of `deletePrefix` due to
             * the bug that reused iterator can't see deleteRange: #9255
             * `deletePrefix`: session.deletePrefix(prefix)
             * `scanPrefix + delete`: session.delete(scanPrefix(prefix))
             */
            byte[] prefix = entry.id().asBytes();
            try (BackendColumnIterator results = session.scan(this.table(), prefix)) {
                while (results.hasNext()) {
                    byte[] column = results.next().name;
                    session.delete(this.table(), column);
                }
                session.commit();
            }
        }
    }

    public static class VertexLabel extends SchemaTable {

        public static final String TABLE = HugeType.VERTEX_LABEL.string();

        public VertexLabel(String database) {
            super(database, TABLE);
        }
    }

    public static class EdgeLabel extends SchemaTable {

        public static final String TABLE = HugeType.EDGE_LABEL.string();

        public EdgeLabel(String database) {
            super(database, TABLE);
        }
    }

    public static class PropertyKey extends SchemaTable {

        public static final String TABLE = HugeType.PROPERTY_KEY.string();

        public PropertyKey(String database) {
            super(database, TABLE);
        }
    }

    public static class IndexLabel extends SchemaTable {

        public static final String TABLE = HugeType.INDEX_LABEL.string();

        public IndexLabel(String database) {
            super(database, TABLE);
        }
    }

    public static class Vertex extends RocksDBTable {

        public static final String TABLE = HugeType.VERTEX.string();

        public Vertex(String database) {
            super(database, TABLE);
        }

        @Override
        protected BackendColumnIterator queryById(RocksDBSessions.Session session, Id id) {
            return this.getById(session, id);
        }

        @Override
        protected BackendColumnIterator queryByIds(RocksDBSessions.Session session,
                                                   Collection<Id> ids) {
            // TODO: use getByIds() after batch version multi-get is ready
            return super.queryByIds(session, ids);
        }
    }

    public static class Edge extends RocksDBTable {

        public static final String TABLE_SUFFIX = HugeType.EDGE.string();

        public Edge(boolean out, String database) {
            // Edge out/in table
            super(database, (out ? 'o' : 'i') + TABLE_SUFFIX);
        }

        public static Edge out(String database) {
            return new Edge(true, database);
        }

        public static Edge in(String database) {
            return new Edge(false, database);
        }

        @Override
        protected BackendColumnIterator queryById(RocksDBSessions.Session session, Id id) {
            return this.getById(session, id);
        }
    }

    public static class IndexTable extends RocksDBTable {

        public IndexTable(String database, String table) {
            super(database, table);
        }

        @Override
        // TODO: why this method is same as super.eliminate() in RocksDBTable, del it?
        public void eliminate(RocksDBSessions.Session session, BackendEntry entry) {
            assert entry.columns().size() == 1;
            super.delete(session, entry);
        }

        @Override
        public void delete(RocksDBSessions.Session session, BackendEntry entry) {
            /*
             * Only delete index by label will come here
             * Regular index delete will call eliminate()
             */
            for (BackendEntry.BackendColumn column : entry.columns()) {
                // Don't assert entry.belongToMe(column), length-prefix is 1*
                session.deletePrefix(this.table(), column.name);
            }
        }
    }

    public static class SecondaryIndex extends IndexTable {

        public static final String TABLE = HugeType.SECONDARY_INDEX.string();

        public SecondaryIndex(String database) {
            super(database, TABLE);
        }
    }

    public static class VertexLabelIndex extends IndexTable {

        public static final String TABLE = HugeType.VERTEX_LABEL_INDEX.string();

        public VertexLabelIndex(String database) {
            super(database, TABLE);
        }
    }

    public static class EdgeLabelIndex extends IndexTable {

        public static final String TABLE = HugeType.EDGE_LABEL_INDEX.string();

        public EdgeLabelIndex(String database) {
            super(database, TABLE);
        }
    }

    public static class SearchIndex extends IndexTable {

        public static final String TABLE = HugeType.SEARCH_INDEX.string();

        public SearchIndex(String database) {
            super(database, TABLE);
        }
    }

    public static class UniqueIndex extends IndexTable {

        public static final String TABLE = HugeType.UNIQUE_INDEX.string();

        public UniqueIndex(String database) {
            super(database, TABLE);
        }
    }

    public static class RangeIndex extends IndexTable {

        public RangeIndex(String database, String table) {
            super(database, table);
        }

        @Override
        protected BackendColumnIterator queryByCond(RocksDBSessions.Session session,
                                                    ConditionQuery query) {
            assert query.conditionsSize() > 0;
            List<Condition> conds = query.syspropConditions(HugeKeys.ID);
            E.checkArgument(!conds.isEmpty(), "Please specify the index conditions");

            Id prefix = null;
            Id min = null;
            boolean minEq = false;
            Id max = null;
            boolean maxEq = false;

            for (Condition c : conds) {
                Relation r = (Relation) c;
                switch (r.relation()) {
                    case PREFIX:
                        prefix = (Id) r.value();
                        break;
                    case GTE:
                        minEq = true;
                        min = (Id) r.value();
                        break;
                    case GT:
                        min = (Id) r.value();
                        break;
                    case LTE:
                        maxEq = true;
                        max = (Id) r.value();
                        break;
                    case LT:
                        max = (Id) r.value();
                        break;
                    default:
                        E.checkArgument(false, "Unsupported relation '%s'", r.relation());
                }
            }

            E.checkArgumentNotNull(min, "Range index begin key is missing");
            byte[] begin = min.asBytes();
            if (!minEq) {
                BinarySerializer.increaseOne(begin);
            }

            if (max == null) {
                E.checkArgumentNotNull(prefix, "Range index prefix is missing");
                return session.scan(this.table(), begin, prefix.asBytes(),
                                    RocksDBSessions.Session.SCAN_PREFIX_END);
            } else {
                byte[] end = max.asBytes();
                int type = maxEq ? RocksDBSessions.Session.SCAN_LTE_END
                                 : RocksDBSessions.Session.SCAN_LT_END;
                return session.scan(this.table(), begin, end, type);
            }
        }
    }

    public static class RangeIntIndex extends RangeIndex {

        public static final String TABLE = HugeType.RANGE_INT_INDEX.string();

        public RangeIntIndex(String store) {
            super(store, TABLE);
        }
    }

    public static class RangeFloatIndex extends RangeIndex {

        public static final String TABLE = HugeType.RANGE_FLOAT_INDEX.string();

        public RangeFloatIndex(String store) {
            super(store, TABLE);
        }
    }

    public static class RangeLongIndex extends RangeIndex {

        public static final String TABLE = HugeType.RANGE_LONG_INDEX.string();

        public RangeLongIndex(String store) {
            super(store, TABLE);
        }
    }

    public static class RangeDoubleIndex extends RangeIndex {

        public static final String TABLE = HugeType.RANGE_DOUBLE_INDEX.string();

        public RangeDoubleIndex(String store) {
            super(store, TABLE);
        }
    }

    public static class ShardIndex extends RangeIndex {

        public static final String TABLE = HugeType.SHARD_INDEX.string();

        public ShardIndex(String database) {
            super(database, TABLE);
        }
    }

    public static class OlapTable extends RocksDBTable {

        public static final String TABLE = HugeType.OLAP.string();

        public OlapTable(String database, Id id) {
            super(database, joinTableName(TABLE, id.asString()));
        }

        @Override
        protected BackendColumnIterator queryById(RocksDBSessions.Session session, Id id) {
            return this.getById(session, id);
        }

        @Override
        public boolean isOlap() {
            return true;
        }
    }

    public static class OlapSecondaryIndex extends SecondaryIndex {

        public static final String TABLE = HugeType.OLAP.string();

        public OlapSecondaryIndex(String store) {
            this(store, TABLE);
        }

        protected OlapSecondaryIndex(String store, String table) {
            super(joinTableName(store, table));
        }
    }

    public static class OlapRangeIntIndex extends RangeIntIndex {

        public static final String TABLE = HugeType.OLAP.string();

        public OlapRangeIntIndex(String store) {
            this(store, TABLE);
        }

        protected OlapRangeIntIndex(String store, String table) {
            super(joinTableName(store, table));
        }
    }

    public static class OlapRangeLongIndex extends RangeLongIndex {

        public static final String TABLE = HugeType.OLAP.string();

        public OlapRangeLongIndex(String store) {
            this(store, TABLE);
        }

        protected OlapRangeLongIndex(String store, String table) {
            super(joinTableName(store, table));
        }
    }

    public static class OlapRangeFloatIndex extends RangeFloatIndex {

        public static final String TABLE = HugeType.OLAP.string();

        public OlapRangeFloatIndex(String store) {
            this(store, TABLE);
        }

        protected OlapRangeFloatIndex(String store, String table) {
            super(joinTableName(store, table));
        }
    }

    public static class OlapRangeDoubleIndex extends RangeDoubleIndex {

        public static final String TABLE = HugeType.OLAP.string();

        public OlapRangeDoubleIndex(String store) {
            this(store, TABLE);
        }

        protected OlapRangeDoubleIndex(String store, String table) {
            super(joinTableName(store, table));
        }
    }
}
