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

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.id.IdGenerator;
import org.apache.hugegraph.serializer.BytesBuffer;
import org.apache.hugegraph.memory.consumer.OffHeapObject;
import org.apache.hugegraph.memory.pool.MemoryPool;
import org.apache.hugegraph.memory.util.FurySerializationUtil;
import org.apache.hugegraph.memory.util.SerializationRuntimeException;
import org.apache.hugegraph.util.E;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;

public class UuidIdOffHeap extends IdGenerator.UuidId implements OffHeapObject {

    private static final Logger LOG = LoggerFactory.getLogger(UuidIdOffHeap.class);
    private ByteBuf idOffHeap;

    public UuidIdOffHeap(MemoryPool memoryPool, String string) {
        super(string);
        serializeSelfToByteBuf(memoryPool);
        releaseOriginalVarsOnHeap();
        memoryPool.bindMemoryConsumer(this);
    }

    public UuidIdOffHeap(MemoryPool memoryPool, byte[] bytes) {
        super(bytes);
        serializeSelfToByteBuf(memoryPool);
        releaseOriginalVarsOnHeap();
        memoryPool.bindMemoryConsumer(this);
    }

    public UuidIdOffHeap(MemoryPool memoryPool, UUID uuid) {
        super(uuid);
        serializeSelfToByteBuf(memoryPool);
        releaseOriginalVarsOnHeap();
        memoryPool.bindMemoryConsumer(this);
    }

    @Override
    public Object zeroCopyReadFromByteBuf() {
        return new IdGenerator.UuidId((UUID) FurySerializationUtil.FURY.deserialize(
                ByteBufUtil.getBytes(this.idOffHeap)));
    }

    @Override
    public void serializeSelfToByteBuf(MemoryPool memoryPool) {
        byte[] bytes = FurySerializationUtil.FURY.serialize(uuid);
        this.idOffHeap = (ByteBuf) memoryPool.requireMemory(bytes.length, memoryPool);
        this.idOffHeap.markReaderIndex();
        this.idOffHeap.writeBytes(bytes);
    }

    @Override
    public void releaseOriginalVarsOnHeap() {
        this.uuid = null;
    }

    @Override
    public List<ByteBuf> getAllMemoryBlock() {
        return Collections.singletonList(idOffHeap);
    }

    @Override
    public Object asObject() {
        return FurySerializationUtil.FURY.deserialize(
                ByteBufUtil.getBytes(this.idOffHeap));
    }

    @Override
    public String asString() {
        return this.asObject().toString();
    }

    @Override
    public byte[] asBytes() {
        try (BytesBuffer buffer = BytesBuffer.allocate(16)) {
            UUID tmp = (UUID) this.asObject();
            buffer.writeLong(tmp.getMostSignificantBits());
            buffer.writeLong(tmp.getLeastSignificantBits());
            return buffer.bytes();
        } catch (IOException e) {
            LOG.error("Unexpected error occurs when allocate bytesBuffer.", e);
            throw new SerializationRuntimeException(e);
        }
    }

    @Override
    public int compareTo(Id other) {
        E.checkNotNull(other, "compare id");
        int cmp = compareType(this, other);
        if (cmp != 0) {
            return cmp;
        }
        return ((UUID) this.asObject()).compareTo((UUID) other.asObject());
    }

    @Override
    public int hashCode() {
        return this.idOffHeap.hashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof UuidIdOffHeap)) {
            return false;
        }
        return this.asObject().equals(((UuidIdOffHeap) other).asObject());
    }

    @Override
    public String toString() {
        return this.asString();
    }
}
