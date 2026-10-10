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

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;

import org.apache.hugegraph.backend.cache.CachedBackendStore;
import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.query.Query;
import org.apache.hugegraph.memory.consumer.OffHeapObject;
import org.apache.hugegraph.memory.pool.MemoryPool;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;

public class QueryIdOffHeap extends CachedBackendStore.QueryId implements OffHeapObject {

    private ByteBuf queryOffHeap;

    public QueryIdOffHeap(MemoryPool memoryPool, Query q) {
        super(q);
        serializeSelfToByteBuf(memoryPool);
        releaseOriginalVarsOnHeap();
        memoryPool.bindMemoryConsumer(this);
    }

    @Override
    public Object zeroCopyReadFromByteBuf() {
        try {
            return new CachedBackendStore.QueryId(
                    this.queryOffHeap.toString(StandardCharsets.UTF_8),
                    this.hashCode);
        } finally {
            queryOffHeap.resetReaderIndex();
        }
    }

    @Override
    public void serializeSelfToByteBuf(MemoryPool memoryPool) {
        byte[] stringBytes = query.getBytes((StandardCharsets.UTF_8));
        this.queryOffHeap = (ByteBuf) memoryPool.requireMemory(stringBytes.length, memoryPool);
        this.queryOffHeap.markReaderIndex();
        this.queryOffHeap.writeBytes(stringBytes);
    }

    @Override
    public void releaseOriginalVarsOnHeap() {
        this.query = null;
    }

    @Override
    public List<ByteBuf> getAllMemoryBlock() {
        return Collections.singletonList(queryOffHeap);
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof QueryIdOffHeap)) {
            return false;
        }
        return this.queryOffHeap.equals(((QueryIdOffHeap) other).queryOffHeap);
    }

    @Override
    public int compareTo(Id o) {
        return this.asString().compareTo(o.asString());
    }

    @Override
    public Object asObject() {
        return this.asString();
    }

    @Override
    public String asString() {
        try {
            return this.queryOffHeap.toString(StandardCharsets.UTF_8);
        } finally {
            this.queryOffHeap.resetReaderIndex();
        }
    }

    @Override
    public byte[] asBytes() {
        return ByteBufUtil.getBytes(this.queryOffHeap);
    }

    @Override
    public String toString() {
        return this.asString();
    }

    @Override
    public int length() {
        return this.asString().length();
    }
}
