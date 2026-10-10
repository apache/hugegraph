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

package org.apache.hugegraph.memory.consumer.impl.property;

import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.hugegraph.memory.consumer.OffHeapObject;
import org.apache.hugegraph.memory.pool.MemoryPool;
import org.apache.hugegraph.memory.util.FurySerializationUtil;
import org.apache.hugegraph.struct.schema.PropertyKey;
import org.apache.hugegraph.structure.HugeEdge;
import org.apache.hugegraph.structure.HugeEdgeProperty;
import org.apache.hugegraph.structure.HugeElement;
import org.apache.hugegraph.structure.BaseProperty;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;

public class HugeEdgePropertyOffHeap<V> extends HugeEdgeProperty<V> implements OffHeapObject {

    private ByteBuf valueOffHeap;

    public HugeEdgePropertyOffHeap(MemoryPool memoryPool, HugeElement owner, PropertyKey key,
                                   V value) {
        super(owner, key, value);
        serializeSelfToByteBuf(memoryPool);
        releaseOriginalVarsOnHeap();
        memoryPool.bindMemoryConsumer(this);
    }

    @Override
    public Object zeroCopyReadFromByteBuf() {
        return new HugeEdgeProperty<>(this.owner, this.pkey,
                                      FurySerializationUtil.FURY.deserialize(
                                              ByteBufUtil.getBytes(this.valueOffHeap)));
    }

    @Override
    public void serializeSelfToByteBuf(MemoryPool memoryPool) {
        byte[] bytes = FurySerializationUtil.FURY.serialize(this.property.value());
        this.valueOffHeap = (ByteBuf) memoryPool.requireMemory(bytes.length, memoryPool);
        this.valueOffHeap.markReaderIndex();
        this.valueOffHeap.writeBytes(bytes);
    }

    @Override
    public void releaseOriginalVarsOnHeap() {
        this.property = new BaseProperty<V>(this.pkey, null) {
            @Override
            public V value() {
                return HugeEdgePropertyOffHeap.this.value();
            }
        };
    }

    @Override
    public List<ByteBuf> getAllMemoryBlock() {
        return Collections.singletonList(valueOffHeap);
    }

    @Override
    public HugeEdgeProperty<V> switchEdgeOwner() {
        assert this.owner instanceof HugeEdge;
        return new HugeEdgeProperty<>(((HugeEdge) this.owner).switchOwner(),
                                      this.pkey, this.value());
    }

    @Override
    public Object serialValue(boolean encodeNumber) {
        return this.pkey.serialValue(this.value(), encodeNumber);
    }

    @Override
    public boolean isPresent() {
        return this.value() != null;
    }

    @Override
    public V value() throws NoSuchElementException {
        return (V) FurySerializationUtil.FURY.deserialize(
                ByteBufUtil.getBytes(this.valueOffHeap));
    }
}
