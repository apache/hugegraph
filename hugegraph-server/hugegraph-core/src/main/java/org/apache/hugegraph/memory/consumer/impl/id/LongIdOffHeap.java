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

package org.apache.hugegraph.memory.consumer.impl.id;

import static org.apache.hugegraph.id.IdGenerator.compareType;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.id.IdGenerator;
import org.apache.hugegraph.memory.consumer.OffHeapObject;
import org.apache.hugegraph.memory.pool.MemoryPool;
import org.apache.hugegraph.util.NumericUtil;

import io.netty.buffer.ByteBuf;

public class LongIdOffHeap extends IdGenerator.LongId implements OffHeapObject {

    private ByteBuf idOffHeap;

    public LongIdOffHeap(MemoryPool memoryPool, long id) {
        super(id);
        serializeSelfToByteBuf(memoryPool);
        releaseOriginalVarsOnHeap();
        memoryPool.bindMemoryConsumer(this);
    }

    public LongIdOffHeap(MemoryPool memoryPool, byte[] bytes) {
        super(bytes);
        serializeSelfToByteBuf(memoryPool);
        releaseOriginalVarsOnHeap();
        memoryPool.bindMemoryConsumer(this);
    }

    @Override
    public Object zeroCopyReadFromByteBuf() {
        try {
            return new IdGenerator.LongId(idOffHeap.readLong());
        } finally {
            idOffHeap.resetReaderIndex();
        }
    }

    @Override
    public void serializeSelfToByteBuf(MemoryPool memoryPool) {
        this.idOffHeap = (ByteBuf) memoryPool.requireMemory(Long.BYTES, memoryPool);
        this.idOffHeap.markReaderIndex();
        this.idOffHeap.writeLong(id);
    }

    @Override
    public void releaseOriginalVarsOnHeap() {
        this.id = null;
    }

    @Override
    public List<ByteBuf> getAllMemoryBlock() {
        return Collections.singletonList(idOffHeap);
    }

    @Override
    public long asLong() {
        try {
            return idOffHeap.readLong();
        } finally {
            idOffHeap.resetReaderIndex();
        }
    }

    @Override
    public Object asObject() {
        return this.asLong();
    }

    @Override
    public String asString() {
        return Long.toString(this.asLong());
    }

    @Override
    public byte[] asBytes() {
        return NumericUtil.longToBytes(this.asLong());
    }

    @Override
    public int compareTo(Id other) {
        int cmp = compareType(this, other);
        if (cmp != 0) {
            return cmp;
        }
        return Long.compare(this.asLong(), other.asLong());
    }

    @Override
    public int hashCode() {
        return Objects.hash(idOffHeap);
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof Number)) {
            return false;
        }
        return this.asLong() == ((Number) other).longValue();
    }

    @Override
    public String toString() {
        return String.valueOf(this.asLong());
    }

    @Override
    public int intValue() {
        return (int) this.asLong();
    }

    @Override
    public long longValue() {
        return this.asLong();
    }

    @Override
    public float floatValue() {
        return this.asLong();
    }

    @Override
    public double doubleValue() {
        return this.asLong();
    }
}
