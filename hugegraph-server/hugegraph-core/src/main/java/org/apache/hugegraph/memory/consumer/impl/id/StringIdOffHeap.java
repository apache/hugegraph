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

import static org.apache.hugegraph.backend.id.IdGenerator.compareType;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;

import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.id.IdGenerator;
import org.apache.hugegraph.memory.consumer.OffHeapObject;
import org.apache.hugegraph.memory.pool.MemoryPool;
import org.jetbrains.annotations.TestOnly;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;

public class StringIdOffHeap extends IdGenerator.StringId implements OffHeapObject {

    private ByteBuf idOffHeap;

    public StringIdOffHeap(MemoryPool memoryPool, String id) {
        super(id);
        serializeSelfToByteBuf(memoryPool);
        releaseOriginalVarsOnHeap();
        memoryPool.bindMemoryConsumer(this);
    }

    public StringIdOffHeap(MemoryPool memoryPool, byte[] bytes) {
        super(bytes);
        serializeSelfToByteBuf(memoryPool);
        releaseOriginalVarsOnHeap();
        memoryPool.bindMemoryConsumer(this);
    }

    @Override
    public Object zeroCopyReadFromByteBuf() {
        try {
            return new IdGenerator.StringId(idOffHeap.toString(StandardCharsets.UTF_8));
        } finally {
            idOffHeap.resetReaderIndex();
        }
    }

    @Override
    public void serializeSelfToByteBuf(MemoryPool memoryPool) {
        byte[] stringBytes;
        if (this.bytes != null) {
            stringBytes = this.bytes;
        } else {
            stringBytes = this.id.getBytes((StandardCharsets.UTF_8));
        }
        this.idOffHeap = (ByteBuf) memoryPool.requireMemory(stringBytes.length, memoryPool);
        this.idOffHeap.markReaderIndex();
        this.idOffHeap.writeBytes(stringBytes);
    }

    @Override
    public void releaseOriginalVarsOnHeap() {
        this.id = null;
        this.bytes = null;
    }

    @Override
    public List<ByteBuf> getAllMemoryBlock() {
        return Collections.singletonList(idOffHeap);
    }

    @Override
    public Object asObject() {
        return this.asString();
    }

    @Override
    public String asString() {
        try {
            return this.idOffHeap.toString(StandardCharsets.UTF_8);
        } finally {
            this.idOffHeap.resetReaderIndex();
        }
    }

    @Override
    public long asLong() {
        return Long.parseLong(this.asString());
    }

    @Override
    public byte[] asBytes() {
        return ByteBufUtil.getBytes(this.idOffHeap);
    }

    @Override
    public int length() {
        return this.asString().length();
    }

    @Override
    public int compareTo(Id other) {
        int cmp = compareType(this, other);
        if (cmp != 0) {
            return cmp;
        }
        return this.asString().compareTo(other.asString());
    }

    @Override
    public int hashCode() {
        return this.idOffHeap.hashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof StringIdOffHeap)) {
            return false;
        }
        return this.idOffHeap.equals(((StringIdOffHeap) other).idOffHeap);
    }

    @Override
    public String toString() {
        return this.asString();
    }

    @TestOnly
    public ByteBuf getIdOffHeap() {
        return idOffHeap;
    }
}
