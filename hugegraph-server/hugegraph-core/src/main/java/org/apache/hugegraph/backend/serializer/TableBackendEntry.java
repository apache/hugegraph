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

package org.apache.hugegraph.backend.serializer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.hugegraph.backend.BackendColumn;
import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.backend.store.BackendEntry;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.type.define.Cardinality;
import org.apache.hugegraph.type.define.HugeKeys;

public class TableBackendEntry implements BackendEntry {

    public static class Row {

        private HugeType type;
        private Id id;
        private Id subId;
        private final Map<HugeKeys, Object> columns;
        private long ttl;

        public Row(HugeType type) {
            this(type, null);
        }

        public Row(HugeType type, Id id) {
            this.type = type;
            this.id = id;
            this.subId = null;
            this.columns = new ConcurrentHashMap<>();
            this.ttl = 0L;
        }

        public HugeType type() {
            return this.type;
        }

        public Id id() {
            return this.id;
        }

        public Map<HugeKeys, Object> columns() {
            return this.columns;
        }

        @SuppressWarnings("unchecked")
        public <T> T column(HugeKeys key) {
            // The T must be primitive type, or list/set/map of primitive type
            return (T) this.columns.get(key);
        }

        public <T> void column(HugeKeys key, T value) {
            this.columns.put(key, value);
        }

        public <T> void column(HugeKeys key, T value, Cardinality c) {
            switch (c) {
                case SINGLE:
                    this.column(key, value);
                    break;
                case SET:
                    // Avoid creating new Set when the key exists
                    if (!this.columns.containsKey(key)) {
                        this.columns.putIfAbsent(key, new LinkedHashSet<>());
                    }
                    this.<Set<T>>column(key).add(value);
                    break;
                case LIST:
                    // Avoid creating new List when the key exists
                    if (!this.columns.containsKey(key)) {
                        this.columns.putIfAbsent(key, new LinkedList<>());
                    }
                    this.<List<T>>column(key).add(value);
                    break;
                default:
                    throw new AssertionError("Unsupported cardinality: " + c);
            }
        }

        public <T> void column(HugeKeys key, Object name, T value) {
            if (!this.columns.containsKey(key)) {
                this.columns.putIfAbsent(key, new ConcurrentHashMap<>());
            }
            this.<Map<Object, T>>column(key).put(name, value);
        }

        public void ttl(long ttl) {
            this.ttl = ttl;
        }

        public long ttl() {
            return this.ttl;
        }

        @Override
        public String toString() {
            return String.format("Row{type=%s, id=%s, columns=%s}",
                                 this.type, this.id, this.columns);
        }
    }

    private final Row row;
    private final List<Row> subRows;

    // NOTE: selfChanged is false when the row has not changed but subRows has.
    private boolean selfChanged;
    private boolean olap;

    public TableBackendEntry(Id id) {
        this(null, id);
    }

    public TableBackendEntry(HugeType type) {
        this(type, null);
    }

    public TableBackendEntry(HugeType type, Id id) {
        this(new Row(type, id));
    }

    public TableBackendEntry(Row row) {
        this.row = row;
        this.subRows = new ArrayList<>();
        this.selfChanged = true;
        this.olap = false;
    }

    @Override
    public HugeType type() {
        return this.row.type;
    }

    public void type(HugeType type) {
        this.row.type = type;
    }

    @Override
    public Id id() {
        return this.row.id;
    }

    @Override
    public Id originId() {
        return this.row.id;
    }

    public void id(Id id) {
        this.row.id = id;
    }

    @Override
    public Id subId() {
        return this.row.subId;
    }

    public void subId(Id subId) {
        this.row.subId = subId;
    }

    public void selfChanged(boolean changed) {
        this.selfChanged = changed;
    }

    public boolean selfChanged() {
        return this.selfChanged;
    }

    public void olap(boolean olap) {
        this.olap = olap;
    }

    @Override
    public boolean olap() {
        return this.olap;
    }

    public Row row() {
        return this.row;
    }

    public Map<HugeKeys, Object> columnsMap() {
        return this.row.columns();
    }

    public <T> void column(HugeKeys key, T value) {
        this.row.column(key, value);
    }

    public <T> void column(HugeKeys key, Object name, T value) {
        this.row.column(key, name, value);
    }

    public <T> void column(HugeKeys key, T value, Cardinality c) {
        this.row.column(key, value, c);
    }

    public <T> T column(HugeKeys key) {
        return this.row.column(key);
    }

    public void subRow(Row row) {
        this.subRows.add(row);
    }

    public List<Row> subRows() {
        return this.subRows;
    }

    public void ttl(long ttl) {
        this.row.ttl(ttl);
    }

    @Override
    public long ttl() {
        return this.row.ttl();
    }

    @Override
    public String toString() {
        return String.format("TableBackendEntry{%s, sub-rows: %s}",
                             this.row.toString(),
                             this.subRows.toString());
    }

    @Override
    public int columnsSize() {
        throw new NotImplementedException("Not supported by table backend");
    }

    @Override
    public Collection<BackendColumn> columns() {
        throw new NotImplementedException("Not supported by table backend");
    }

    @Override
    public void columns(Collection<BackendColumn> bytesColumns) {
        throw new NotImplementedException("Not supported by table backend");
    }

    @Override
    public void columns(BackendColumn bytesColumn) {
        throw new NotImplementedException("Not supported by table backend");
    }

    @Override
    public void merge(BackendEntry other) {
        throw new NotImplementedException("Not supported by table backend");
    }

    @Override
    public boolean mergeable(BackendEntry other) {
        if (!(other instanceof TableBackendEntry)) {
            return false;
        }
        TableBackendEntry tableEntry = (TableBackendEntry) other;
        Object selfId = this.column(HugeKeys.ID);
        Object otherId = tableEntry.column(HugeKeys.ID);
        if (!selfId.equals(otherId)) {
            return false;
        }
        Id key = tableEntry.subId();
        Object value = tableEntry.row().column(HugeKeys.PROPERTY_VALUE);
        this.row().column(HugeKeys.PROPERTIES, key.asLong(), value);
        return true;
    }

    @Override
    public void clear() {
        throw new NotImplementedException("Not supported by table backend");
    }
}
