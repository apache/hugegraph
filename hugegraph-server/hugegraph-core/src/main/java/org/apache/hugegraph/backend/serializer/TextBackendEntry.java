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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.hugegraph.backend.BackendColumn;
import org.apache.hugegraph.exception.BackendException;
import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.backend.store.BackendEntry;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.type.define.HugeKeys;
import org.apache.hugegraph.util.JsonUtil;
import org.apache.hugegraph.util.StringEncoding;

public class TextBackendEntry implements BackendEntry, Cloneable {

    public static final String VALUE_SPLITOR = "\u0003";

    private final HugeType type;
    private final Id id;
    private Id subId;
    private NavigableMap<String, String> columns;

    public TextBackendEntry(HugeType type, Id id) {
        this.type = type;
        this.id = id;
        this.subId = null;
        this.resetColumns();
    }

    @Override
    public HugeType type() {
        return this.type;
    }

    @Override
    public Id id() {
        return this.id;
    }

    @Override
    public Id originId() {
        return this.id;
    }

    @Override
    public Id subId() {
        return this.subId;
    }

    @Override
    public long ttl() {
        return 0L;
    }

    public void subId(Id subId) {
        this.subId = subId;
    }

    public Set<String> columnNames() {
        return this.columns.keySet();
    }

    public void column(HugeKeys column, String value) {
        this.columns.put(column.string(), value);
    }

    public void column(String column, String value) {
        this.columns.put(column, value);
    }

    public String column(HugeKeys column) {
        return this.columns.get(column.string());
    }

    public String column(String column) {
        return this.columns.get(column);
    }

    public BackendColumn columns(String column) {
        String value = this.columns.get(column);
        if (value == null) {
            return null;
        }
        return BackendColumn.of(StringEncoding.encode(column),
                                StringEncoding.encode(value));
    }

    public Collection<BackendColumn> columnsWithPrefix(String prefix) {
        return this.columnsWithPrefix(prefix, true, prefix);
    }

    public Collection<BackendColumn> columnsWithPrefix(String start,
                                                       boolean inclusiveStart,
                                                       String prefix) {
        List<BackendColumn> list = new ArrayList<>();
        Map<String, String> map = this.columns.tailMap(start, inclusiveStart);
        for (Map.Entry<String, String> e : map.entrySet()) {
            String key = e.getKey();
            String value = e.getValue();
            if (key.startsWith(prefix)) {
                list.add(BackendColumn.of(StringEncoding.encode(key),
                                          StringEncoding.encode(value)));
            }
        }
        return list;
    }

    public Collection<BackendColumn> columnsWithRange(String start,
                                                      boolean inclusiveStart,
                                                      String end,
                                                      boolean inclusiveEnd) {
        List<BackendColumn> list = new ArrayList<>();
        Map<String, String> map = this.columns.subMap(start, inclusiveStart,
                                                      end, inclusiveEnd);
        for (Map.Entry<String, String> e : map.entrySet()) {
            String key = e.getKey();
            String value = e.getValue();
            list.add(BackendColumn.of(StringEncoding.encode(key),
                                      StringEncoding.encode(value)));
        }
        return list;
    }

    public boolean contains(String column) {
        return this.columns.containsKey(column);
    }

    public boolean contains(String column, String value) {
        String col = this.columns.get(column);
        return col != null && col.equals(value);
    }

    public boolean containsPrefix(String column) {
        Map<String, String> map = this.columns.tailMap(column, true);
        for (String c : map.keySet()) {
            if (c.startsWith(column)) {
                return true;
            }
        }
        return false;
    }

    public boolean containsValue(String value) {
        return this.columns.containsValue(value);
    }

    public void append(TextBackendEntry entry) {
        for (Entry<String, String> col : entry.columns.entrySet()) {
            String newValue = col.getValue();
            String oldValue = this.column(col.getKey());

            // TODO: use more general method
            if (col.getKey().startsWith(HugeType.PROPERTY.string())) {
                this.columns.put(col.getKey(), col.getValue());
                continue;
            }

            // TODO: use more general method
            if (!col.getKey().endsWith(HugeKeys.ELEMENT_IDS.string())) {
                continue;
            }

            // TODO: ensure the old value is a list and json format (for index)
            if ("[]".equals(oldValue)) {
                this.column(col.getKey(), newValue);
                continue;
            }
            List<Object> values = new ArrayList<>();
            @SuppressWarnings("unchecked")
            List<Object> oldValues = JsonUtil.fromJson(oldValue, List.class);
            @SuppressWarnings("unchecked")
            List<Object> newValues = JsonUtil.fromJson(newValue, List.class);
            values.addAll(oldValues);
            values.addAll(newValues);
            // Update the old value
            this.column(col.getKey(), JsonUtil.toJson(values));
        }
    }

    public void eliminate(TextBackendEntry entry) {
        for (Entry<String, String> col : entry.columns.entrySet()) {
            String newValue = col.getValue();
            String oldValue = this.column(col.getKey());

            // TODO: use more general method
            if (col.getKey().startsWith(HugeType.PROPERTY.string()) ||
                col.getKey().startsWith(HugeType.EDGE_OUT.string()) ||
                col.getKey().startsWith(HugeType.EDGE_IN.string())) {
                this.columns.remove(col.getKey());
                continue;
            }

            // TODO: use more general method
            if (!col.getKey().endsWith(HugeKeys.ELEMENT_IDS.string())) {
                continue;
            }

            // TODO: ensure the old value is a list and json format (for index)
            @SuppressWarnings("unchecked")
            List<Object> oldValues = JsonUtil.fromJson(oldValue, List.class);
            @SuppressWarnings("unchecked")
            List<Object> newValues = JsonUtil.fromJson(newValue, List.class);
            List<Object> values = new ArrayList<>(oldValues);
            values.removeAll(newValues);
            // Update the old value
            this.column(col.getKey(), JsonUtil.toJson(values));
        }
    }

    @Override
    public String toString() {
        return String.format("%s: %s", this.id, this.columns.toString());
    }

    @Override
    public int columnsSize() {
        return this.columns.size();
    }

    @Override
    public Collection<BackendColumn> columns() {
        List<BackendColumn> list = new ArrayList<>(this.columns.size());
        for (Entry<String, String> column : this.columns.entrySet()) {
            BackendColumn bytesColumn = new BackendColumn();
            bytesColumn.name = StringEncoding.encode(column.getKey());
            bytesColumn.value = StringEncoding.encode(column.getValue());
            list.add(bytesColumn);
        }
        return list;
    }

    @Override
    public void columns(Collection<BackendColumn> bytesColumns) {
        for (BackendColumn column : bytesColumns) {
            this.columns.put(StringEncoding.decode(column.name),
                             StringEncoding.decode(column.value));
        }
    }

    @Override
    public void columns(BackendColumn bytesColumn) {
        this.columns.put(StringEncoding.decode(bytesColumn.name),
                         StringEncoding.decode(bytesColumn.value));
    }

    @Override
    public void merge(BackendEntry other) {
        TextBackendEntry text = (TextBackendEntry) other;
        this.columns.putAll(text.columns);
    }

    @Override
    public boolean mergeable(BackendEntry other) {
        if (!(other instanceof TextBackendEntry)) {
            return false;
        }
        if (!this.id().equals(other.id())) {
            return false;
        }
        this.columns(other.columns());
        return true;
    }

    @Override
    public void clear() {
        this.columns.clear();
    }

    private void resetColumns() {
        this.columns = new ConcurrentSkipListMap<>();
    }

    public TextBackendEntry copy() {
        try {
            TextBackendEntry clone = (TextBackendEntry) this.clone();
            clone.columns = new ConcurrentSkipListMap<>(this.columns);
            return clone;
        } catch (CloneNotSupportedException e) {
            throw new BackendException(e);
        }
    }

    public TextBackendEntry copyLast(int count) {
        TextBackendEntry clone;
        try {
            clone = (TextBackendEntry) this.clone();
        } catch (CloneNotSupportedException e) {
            throw new BackendException(e);
        }
        clone.resetColumns();

        // Copy the last count columns
        Iterator<Entry<String, String>> it = this.columns.entrySet().iterator();
        final int skip = this.columns.size() - count;
        for (int i = 0; it.hasNext(); i++) {
            Entry<String, String> entry = it.next();
            if (i < skip) {
                continue;
            }
            clone.columns.put(entry.getKey(), entry.getValue());
        }
        return clone;
    }

    public TextBackendEntry copyHead(int count) {
        TextBackendEntry clone;
        try {
            clone = (TextBackendEntry) this.clone();
        } catch (CloneNotSupportedException e) {
            throw new BackendException(e);
        }
        clone.resetColumns();

        // Copy the head count columns
        Iterator<Entry<String, String>> it = this.columns.entrySet().iterator();
        for (int i = 0; it.hasNext() && i < count; i++) {
            Entry<String, String> entry = it.next();
            clone.columns.put(entry.getKey(), entry.getValue());
        }
        return clone;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof TextBackendEntry)) {
            return false;
        }
        TextBackendEntry other = (TextBackendEntry) obj;
        if (this.id() != other.id() && !this.id().equals(other.id())) {
            return false;
        }
        if (this.columns().size() != other.columns().size()) {
            return false;
        }

        for (Map.Entry<String, String> e : this.columns.entrySet()) {
            String key = e.getKey();
            String value = e.getValue();
            String otherValue = other.columns.get(key);
            if (otherValue == null) {
                return false;
            }
            if (!value.equals(otherValue)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        return this.id().hashCode() ^ this.columns().hashCode();
    }
}
