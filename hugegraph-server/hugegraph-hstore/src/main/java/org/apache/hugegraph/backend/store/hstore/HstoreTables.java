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

import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Collections;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.nio.ByteBuffer;

import org.apache.hugegraph.backend.BackendColumn;
import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.backend.BinaryId;
import org.apache.hugegraph.serializer.BytesBuffer;
import org.apache.hugegraph.serializer.OlapKey;
import org.apache.hugegraph.iterator.FlatMapperIterator;
import org.apache.hugegraph.iterator.WrappedIterator;
import org.apache.hugegraph.store.HgOwnerKey;
import org.apache.hugegraph.query.Query;
import org.apache.hugegraph.query.Condition;
import org.apache.hugegraph.query.Condition.Relation;
import org.apache.hugegraph.query.ConditionQuery;
import org.apache.hugegraph.backend.serializer.BinarySerializer;
import org.apache.hugegraph.backend.serializer.BinaryBackendEntry;
import org.apache.hugegraph.backend.store.BackendEntry;
import org.apache.hugegraph.backend.store.BackendEntry.BackendColumnIterator;
import org.apache.hugegraph.backend.store.hstore.HstoreSessions.Session;
import org.apache.hugegraph.type.HugeTableType;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.type.define.HugeKeys;
import org.apache.hugegraph.util.E;

public class HstoreTables {

    public static class Vertex extends HstoreTable {

        public static final String TABLE = HugeTableType.VERTEX.string();

        public Vertex(String database) {
            super(database, TABLE);
        }

        @Override
        protected BackendColumnIterator queryById(Session session, Id id) {
            return this.getById(session, id);
        }
    }

    /**
     * task information storage table
     */
    public static class TaskInfo extends HstoreTable {

        public static final String TABLE = HugeTableType.TASK_INFO_TABLE.string();

        public TaskInfo(String database) {
            super(database, TABLE);
        }

        @Override
        protected BackendColumnIterator queryById(Session session, Id id) {
            return this.getById(session, id);
        }
    }

    public static class ServerInfo extends HstoreTable {

        public static final String TABLE = HugeTableType.SERVER_INFO_TABLE.string();

        public ServerInfo(String database) {
            super(database, TABLE);
        }

        @Override
        protected BackendColumnIterator queryById(Session session, Id id) {
            return this.getById(session, id);
        }
    }

    public static class Edge extends HstoreTable {

        public static final String TABLE_SUFFIX = HugeType.EDGE.string();

        public Edge(boolean out, String database) {
            // Edge out/in table
            super(database, (out ? HugeTableType.OUT_EDGE.string() :
                             HugeTableType.IN_EDGE.string()));
        }

        public static Edge out(String database) {
            return new Edge(true, database);
        }

        public static Edge in(String database) {
            return new Edge(false, database);
        }

        @Override
        protected BackendColumnIterator queryById(Session session, Id id) {
            return this.getById(session, id);
        }
    }

    public static class IndexTable extends HstoreTable {

        public static final String TABLE = HugeTableType.ALL_INDEX_TABLE.string();

        public IndexTable(String database) {
            super(database, TABLE);
        }

        @Override
        public void eliminate(Session session, BackendEntry entry) {
            assert entry.columns().size() == 1;
            super.delete(session, entry);
        }

        @Override
        public void delete(Session session, BackendEntry entry) {
            /*
             * Only delete index by label will come here
             * Regular index delete will call eliminate()
             */
            byte[] ownerKey = super.ownerDelegate.apply(entry);
            for (BackendColumn column : entry.columns()) {
                // Don't assert entry.belongToMe(column), length-prefix is 1*
                session.deletePrefix(this.table(), ownerKey, column.name);
            }
        }

        /**
         * Mainly used for range-type index processing
         *
         * @param session
         * @param query
         * @return
         */
        @Override
        protected BackendColumnIterator queryByCond(Session session,
                                                    ConditionQuery query) {
            assert !query.conditions().isEmpty();

            List<Condition> conds = query.syspropConditions(HugeKeys.ID);
            E.checkArgument(!conds.isEmpty(),
                            "Please specify the index conditions");

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
                    case GT:
                        min = (Id) r.value();
                        break;
                    case LTE:
                        maxEq = true;
                    case LT:
                        max = (Id) r.value();
                        break;
                    default:
                        E.checkArgument(false, "Unsupported relation '%s'",
                                        r.relation());
                }
            }

            E.checkArgumentNotNull(min, "Range index begin key is missing");
            byte[] begin = min.asBytes();
            if (!minEq) {
                BinarySerializer.increaseOne(begin);
            }
            byte[] ownerStart = this.ownerScanDelegate.get();
            byte[] ownerEnd = this.ownerScanDelegate.get();
            if (max == null) {
                E.checkArgumentNotNull(prefix, "Range index prefix is missing");
                return session.scan(this.table(), ownerStart, ownerEnd, begin,
                                    prefix.asBytes(), Session.SCAN_PREFIX_END);
            } else {
                byte[] end = max.asBytes();
                int type = maxEq ? Session.SCAN_LTE_END : Session.SCAN_LT_END;
                return session.scan(this.table(), ownerStart,
                                    ownerEnd, begin, end, type);
            }
        }
    }

    public static class OlapTable extends HstoreTable {

        public static final String TABLE = HugeTableType.OLAP_TABLE.string();
        private static final int MAX_REQUEST_KEYS = 1024;
        private static final int MAX_SELECTED_VERTICES = 128;

        public OlapTable(String database) {
            // Originally multiple ap_{pk_id} merged into one ap table
            super(database, TABLE);
        }

        @Override
        public void insert(Session session, BackendEntry entry) {
            E.checkArgumentNotNull(entry.subId(), "Merged OLAP write requires a property ID");
            byte[] owner = this.getInsertOwner(entry);
            for (BackendColumn column : entry.columns()) {
                E.checkArgument(OlapKey.matchesProperty(column.value, entry.subId()),
                                "OLAP value must match property ID '%s'", entry.subId());
                session.put(this.table(), owner, OlapKey.format(entry.subId(), entry.originId()), column.value);
            }
        }

        @Override
        public void insert(Session session, BackendEntry entry, boolean isEdge) {
            this.insert(session, entry);
        }

        @Override
        public void delete(Session session, BackendEntry entry) {
            E.checkArgumentNotNull(entry.subId(), "Merged OLAP delete requires a property ID");
            byte[] owner = this.getInsertOwner(entry);
            session.delete(this.table(), owner, OlapKey.format(entry.subId(), entry.originId()));
            // Delete an old vertex-only record only if it belongs to the requested property.
            byte[] legacyKey = BytesBuffer.allocate(entry.originId().length() + 9)
                                         .writeId(entry.originId()).bytes();
            if (OlapKey.matchesProperty(session.get(this.table(), owner, legacyKey), entry.subId())) {
                session.delete(this.table(), owner, legacyKey);
            }
        }

        @Override
        public boolean queryExist(Session session, BackendEntry entry) {
            E.checkArgumentNotNull(entry.subId(), "Merged OLAP existence query requires a property ID");
            Id key = new BinaryId(OlapKey.format(entry.subId(), entry.originId()), entry.originId());
            try (BackendColumnIterator result = this.getById(session, key)) {
                return result.hasNext();
            }
        }

        @Override
        protected BackendColumnIterator queryBy(Session session, Query query) {
            if (!query.ids().isEmpty() && query.conditions().isEmpty()) {
                return BackendColumnIterator.wrap(new FlatMapperIterator<>(
                        query.ids().iterator(), id -> this.getById(session, id)));
            }
            return super.queryBy(session, query);
        }

        @Override
        protected BackendColumnIterator getById(Session session, Id id) {
            // This query key is explicitly constructed by HstoreStore for the merged OLAP table.
            BytesBuffer key = BytesBuffer.wrap(id.asBytes());
            Id propertyId = key.readId();
            Id vertexId = key.readId();
            byte[] owner = this.getOwnerId(id instanceof BinaryId ? ((BinaryId) id).origin() : vertexId);
            byte[] value = session.get(this.table(), owner, id.asBytes());
            if (value == null || value.length == 0) {
                byte[] legacyKey = BytesBuffer.allocate(vertexId.length() + 9).writeId(vertexId).bytes();
                value = session.get(this.table(), owner, legacyKey);
            }
            if (!OlapKey.matchesProperty(value, propertyId)) {
                return BackendColumnIterator.empty();
            }
            // Normalize only the returned key, so the existing OLAP entry parser retains vertex origin.
            return BackendColumnIterator.iterator(BackendColumn.of(id.asBytes(), value));
        }

        @Override
        protected BackendColumnIterator queryById(Session session, Id id) {
            return this.getById(session, id);
        }

        Iterator<BackendEntry> mergeEntries(Session session, Iterator<BackendEntry> entries,
                                             Set<Id> properties, boolean paging) {
            if (properties == null || properties.isEmpty()) {
                return entries;
            }
            return new WrappedIterator<BackendEntry>() {
                private Iterator<BackendEntry> batch = Collections.emptyIterator();
                private final int batchSize = paging ? 1 : (int) Math.max(1L, Math.min(
                        MAX_SELECTED_VERTICES, MAX_REQUEST_KEYS / (properties.size() + 1L)));
                private boolean closed;

                @Override
                public void close() throws Exception {
                    if (!this.closed) {
                        this.closed = true;
                        this.current = none();
                        this.batch = Collections.emptyIterator();
                        super.close();
                    }
                }

                @Override
                protected Iterator<?> originIterator() {
                    return entries;
                }

                @Override
                protected boolean fetch() {
                    if (this.closed) {
                        return false;
                    }
                    try {
                        return this.fetchBatch();
                    } catch (RuntimeException | Error failure) {
                        try {
                            this.close();
                        } catch (Exception closeFailure) {
                            failure.addSuppressed(closeFailure);
                        }
                        throw failure;
                    }
                }

                private boolean fetchBatch() {
                    if (!this.batch.hasNext()) {
                        List<BackendEntry> selected = new ArrayList<>(this.batchSize);
                        while (selected.size() < this.batchSize && entries.hasNext()) {
                            selected.add(entries.next());
                        }
                        if (selected.isEmpty()) {
                            WrappedIterator.close(this);
                            return false;
                        }
                        this.mergeSelected(selected);
                        this.batch = selected.iterator();
                    }
                    this.current = this.batch.next();
                    return true;
                }

                private void mergeSelected(List<BackendEntry> selected) {
                    Map<Id, BackendColumn> legacy = new HashMap<>();
                    Iterator<Id> remaining = properties.iterator();
                    boolean firstChunk = true;
                    do {
                        int capacity = Math.min(MAX_REQUEST_KEYS,
                                                selected.size() * (Math.min(properties.size(), MAX_REQUEST_KEYS) + 1));
                        List<HgOwnerKey> keys = new ArrayList<>(capacity);
                        if (firstChunk) {
                            for (BackendEntry entry : selected) {
                                Id vertexId = entry.originId();
                                byte[] key = BytesBuffer.allocate(vertexId.length() + 9).writeId(vertexId).bytes();
                                keys.add(HgOwnerKey.of(OlapTable.this.getOwnerId(vertexId), key));
                            }
                        }
                        int propertyLimit = (MAX_REQUEST_KEYS - keys.size()) / selected.size();
                        List<Id> requestedProperties = new ArrayList<>(Math.min(propertyLimit, properties.size()));
                        while (requestedProperties.size() < propertyLimit && remaining.hasNext()) {
                            Id property = remaining.next();
                            requestedProperties.add(property);
                            for (BackendEntry entry : selected) {
                                Id vertexId = entry.originId();
                                keys.add(HgOwnerKey.of(OlapTable.this.getOwnerId(vertexId),
                                                      OlapKey.format(property, vertexId)));
                            }
                        }
                        Map<ByteBuffer, BackendColumn> values = new HashMap<>();
                        try (BackendColumnIterator columns = session.getWithBatchExact(OlapTable.this.table(), keys)) {
                            while (columns.hasNext()) {
                                BackendColumn column = columns.next();
                                values.put(ByteBuffer.wrap(column.name), column);
                            }
                        }
                        if (firstChunk) {
                            for (BackendEntry entry : selected) {
                                Id vertexId = entry.originId();
                                byte[] key = BytesBuffer.allocate(vertexId.length() + 9).writeId(vertexId).bytes();
                                legacy.put(vertexId, values.get(ByteBuffer.wrap(key)));
                            }
                            firstChunk = false;
                        }
                        for (BackendEntry entry : selected) {
                            Id vertexId = entry.originId();
                            for (Id property : requestedProperties) {
                                byte[] key = OlapKey.format(property, vertexId);
                                BackendColumn column = values.get(ByteBuffer.wrap(key));
                                if (column == null || column.value == null || column.value.length == 0) {
                                    column = legacy.get(vertexId);
                                }
                                if (column == null || !OlapKey.matchesProperty(column.value, property)) {
                                    continue;
                                }
                                BinaryBackendEntry olap = new BinaryBackendEntry(entry.type(), key, false, true);
                                olap.columns(BackendColumn.of(key, column.value));
                                E.checkState(entry.mergeable(olap), "OLAP row must belong to its selected vertex");
                            }
                        }
                    } while (remaining.hasNext());
                }
            };
        }

        @Override
        public boolean isOlap() {
            return true;
        }
    }
}
