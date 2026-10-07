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

import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.apache.hugegraph.id.IdGenerator;
import org.apache.hugegraph.memory.consumer.OffHeapObject;
import org.apache.hugegraph.memory.pool.MemoryPool;
import org.apache.hugegraph.memory.util.FurySerializationUtil;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;

public class ObjectIdOffHeap extends IdGenerator.ObjectId implements OffHeapObject {

    private ByteBuf objectOffHeap;

    public ObjectIdOffHeap(MemoryPool memoryPool, Object object) {
        super(object);
        serializeSelfToByteBuf(memoryPool);
        releaseOriginalVarsOnHeap();
        memoryPool.bindMemoryConsumer(this);
    }

    @Override
    public Object zeroCopyReadFromByteBuf() {
        return new IdGenerator.ObjectId(FurySerializationUtil.FURY.deserialize(
                ByteBufUtil.getBytes(this.objectOffHeap)));
    }

    @Override
    public void serializeSelfToByteBuf(MemoryPool memoryPool) {
        byte[] bytes = FurySerializationUtil.FURY.serialize(object);
        this.objectOffHeap = (ByteBuf) memoryPool.requireMemory(bytes.length, memoryPool);
        this.objectOffHeap.markReaderIndex();
        this.objectOffHeap.writeBytes(bytes);
    }

    @Override
    public void releaseOriginalVarsOnHeap() {
        this.object = null;
    }

    @Override
    public List<ByteBuf> getAllMemoryBlock() {
        return Collections.singletonList(objectOffHeap);
    }

    @Override
    public Object asObject() {
        return FurySerializationUtil.FURY.deserialize(ByteBufUtil.getBytes(objectOffHeap));
    }

    @Override
    public int hashCode() {
        return Objects.hash(objectOffHeap);
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof ObjectIdOffHeap)) {
            return false;
        }
        return this.objectOffHeap.equals(((ObjectIdOffHeap) other).objectOffHeap);
    }

    @Override
    public String toString() {
        return super.toString();
    }
}
