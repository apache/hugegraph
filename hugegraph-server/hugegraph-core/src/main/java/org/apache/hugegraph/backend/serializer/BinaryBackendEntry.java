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
import java.util.Collections;
import java.util.List;

import org.apache.hugegraph.backend.BackendColumn;
import org.apache.hugegraph.backend.BinaryId;
import org.apache.hugegraph.backend.store.BackendEntry;
import org.apache.hugegraph.backend.store.BackendEntryIterator;
import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.serializer.BytesBuffer;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Bytes;

public class BinaryBackendEntry implements BackendEntry {

    private final HugeType type;
    private final BinaryId id;
    private Id subId;
    private final List<BackendColumn> columns;
    private long ttl;
    private boolean olap;

    public BinaryBackendEntry(HugeType type, byte[] bytes) {
        this(type, BytesBuffer.wrap(bytes).parseId(type, false));
    }

    public BinaryBackendEntry(HugeType type, byte[] bytes, boolean enablePartition) {
        this(type, BytesBuffer.wrap(bytes).parseId(type, enablePartition));
    }

    // FIXME: `enablePartition` is unused here
    public BinaryBackendEntry(HugeType type, byte[] bytes, boolean enablePartition,
                              boolean isOlap) {
        this(type, BytesBuffer.wrap(bytes).parseOlapId(type, isOlap));
    }

    public BinaryBackendEntry(HugeType type, BinaryId id) {
        this.type = type;
        this.id = id;
        this.subId = null;
        this.columns = new ArrayList<>();
        this.ttl = 0L;
        this.olap = false;
    }

    @Override
    public HugeType type() {
        return this.type;
    }

    @Override
    public BinaryId id() {
        return this.id;
    }

    @Override
    public Id originId() {
        return this.id.origin();
    }

    @Override
    public Id subId() {
        return this.subId;
    }

    public void subId(Id subId) {
        this.subId = subId;
    }

    public void ttl(long ttl) {
        this.ttl = ttl;
    }

    @Override
    public long ttl() {
        return this.ttl;
    }

    public void olap(boolean olap) {
        this.olap = olap;
    }

    @Override
    public boolean olap() {
        return this.olap;
    }

    @Override
    public String toString() {
        return String.format("%s: %s", this.id, this.columns.toString());
    }

    public BackendColumn column(byte[] name) {
        for (BackendColumn col : this.columns) {
            if (Bytes.equals(col.name, name)) {
                return col;
            }
        }
        return null;
    }

    public void column(BackendColumn column) {
        this.columns.add(column);
    }

    public void column(byte[] name, byte[] value) {
        E.checkNotNull(name, "name");
        value = value != null ? value : BytesBuffer.BYTES_EMPTY;
        this.columns.add(BackendColumn.of(name, value));
    }

    @Override
    public Collection<BackendColumn> columns() {
        return Collections.unmodifiableList(this.columns);
    }

    @Override
    public int columnsSize() {
        return this.columns.size();
    }

    @Override
    public void columns(Collection<BackendColumn> bytesColumns) {
        this.columns.addAll(bytesColumns);
    }

    @Override
    public void columns(BackendColumn bytesColumn) {
        this.columns.add(bytesColumn);
        long maxSize = BackendEntryIterator.INLINE_BATCH_SIZE;
        if (this.columns.size() > maxSize) {
            E.checkState(false, "Too many columns in one entry: %s", maxSize);
        }
    }

    public BackendColumn removeColumn(int index) {
        return this.columns.remove(index);
    }

    @Override
    public void merge(BackendEntry other) {
        for (BackendColumn col : other.columns()) {
            BackendColumn origin = this.column(col.name);
            if (origin != null) {
                origin.value = col.value;
            } else {
                this.column(col);
            }
        }
    }

    @Override
    public boolean mergeable(BackendEntry other) {
        if (!(other instanceof BinaryBackendEntry)) {
            return false;
        }
        if (!this.id().equals(other.id())) {
            // Compatible with data retrieved from ap, vertex id
            if (!this.id().origin().equals(other.originId())) {
                return false;
            }
        }
        this.columns(other.columns());
        return true;
    }

    @Override
    public void clear() {
        this.columns.clear();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof BinaryBackendEntry)) {
            return false;
        }
        BinaryBackendEntry other = (BinaryBackendEntry) obj;
        if (this.id() != other.id() && !this.id().equals(other.id())) {
            return false;
        }
        if (this.columns.size() != other.columns.size()) {
            return false;
        }
        return this.columns.containsAll(other.columns);
    }

    public int hashCode() {
        return this.id().hashCode() ^ this.columns.size();
    }
}
