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

package org.apache.hugegraph.store.business;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.List;
import java.util.Set;

import org.apache.hugegraph.HugeGraphSupplier;
import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.id.IdGenerator;
import org.apache.hugegraph.rocksdb.access.RocksDBSession.BackendColumn;
import org.apache.hugegraph.rocksdb.access.ScanIterator;
import org.apache.hugegraph.serializer.BytesBuffer;

public class SelectIterator implements ScanIterator {

    private final ScanIterator iter;
    private final Set<Integer> properties;
    private final HugeGraphSupplier graph;
    private final boolean isVertex;

    public SelectIterator(ScanIterator iterator, List<Integer> properties,
                          HugeGraphSupplier graph, boolean isVertex) {
        this.iter = iterator;
        this.properties = properties == null ? Collections.emptySet() : new HashSet<>(properties);
        this.graph = this.properties.isEmpty() ? graph : Objects.requireNonNull(graph, "graph");
        this.isVertex = isVertex;
    }

    public BackendColumn select(BackendColumn column) {
        if (this.properties.isEmpty() || column.value == null || column.value.length == 0) {
            return column;
        }
        BytesBuffer buffer = BytesBuffer.wrap(column.value);
        Id labelId = this.isVertex ? buffer.readId() : null;
        int count = buffer.readVInt();
        List<byte[]> selected = new ArrayList<>(Math.min(count, this.properties.size()));
        for (int i = 0; i < count; i++) {
            int start = buffer.position();
            int propertyId = buffer.readVInt();
            buffer.skipSchemaProperty(this.graph.propertyKey(IdGenerator.of(propertyId)));
            if (this.properties.contains(propertyId)) {
                // Preserve the stored bytes, including collection ordering and scalar representation.
                selected.add(Arrays.copyOfRange(column.value, start, buffer.position()));
            }
        }
        BytesBuffer output = BytesBuffer.allocate(column.value.length);
        if (this.isVertex) {
            output.writeId(labelId);
        }
        output.writeVInt(selected.size());
        for (byte[] property : selected) {
            output.write(property);
        }
        // TTL belongs to the row, independently of which properties were selected.
        output.write(buffer.remainingBytes());
        return BackendColumn.of(column.name, output.bytes());
    }

    @Override
    public boolean hasNext() {
        return this.iter.hasNext();
    }

    @Override
    public boolean isValid() {
        return this.iter.isValid();
    }

    @Override
    public BackendColumn next() {
        BackendColumn value = this.iter.next();
        return select(value);
    }

    @Override
    public long count() {
        return this.iter.count();
    }

    @Override
    public byte[] position() {
        return this.iter.position();
    }

    @Override
    public void seek(byte[] position) {
        this.iter.seek(position);
    }

    @Override
    public void close() {
        this.iter.close();
    }
}
