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

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.rocksdb.access.RocksDBSession.BackendColumn;
import org.apache.hugegraph.rocksdb.access.ScanIterator;
import org.apache.hugegraph.serializer.BytesBuffer;
import org.apache.hugegraph.type.define.DataType;
import org.apache.hugegraph.type.define.SerialEnum;

public class SelectIterator implements ScanIterator {

    ScanIterator iter;
    Set<Integer> properties;

    public SelectIterator(ScanIterator iterator, List<Integer> properties) {
        this.iter = iterator;
        this.properties = new HashSet<>(properties);
    }

    public BackendColumn select(BackendColumn column) {
        int size;
        if (properties == null || (size = properties.size()) == 0) {
            return column;
        }
        byte[] name = column.name;
        byte[] value = column.value;
        BytesBuffer buffer = BytesBuffer.wrap(value);
        Id labelId = buffer.readId(); // label
        int bpSize = buffer.readVInt(); // property
        if (size == bpSize) {
            return column;
        }
        BytesBuffer allocate = BytesBuffer.allocate(8 + 16 * size);
        allocate.writeId(labelId);
        allocate.writeVInt(size);
        for (int i = 0; i < bpSize; i++) {
            int propertyId = buffer.readVInt();
            byte cat = buffer.read(); // cardinality and type
            byte code = BytesBuffer.getType(cat);
            DataType dataType = SerialEnum.fromCode(DataType.class, code);
            Object bpValue = buffer.readProperty(dataType);
            if (properties.contains(propertyId)) {
                allocate.writeVInt(propertyId);
                allocate.write(cat);
                allocate.writeProperty(dataType, bpValue);
            }
        }
        return BackendColumn.of(name, allocate.bytes());
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
