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

import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.backend.BinaryId;
import org.apache.hugegraph.memory.consumer.OffHeapObject;
import org.apache.hugegraph.memory.pool.MemoryPool;
import org.apache.hugegraph.util.Bytes;
import org.apache.hugegraph.util.E;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;

public class BinaryIdOffHeap extends BinaryId implements OffHeapObject {

    private final OffHeapObject originId;
    private ByteBuf bytesOffHeap;

    public BinaryIdOffHeap(byte[] bytes, Id id, MemoryPool memoryPool, OffHeapObject originId) {
        super(bytes, id);
        this.originId = originId;
        serializeSelfToByteBuf(memoryPool);
        releaseOriginalVarsOnHeap();
        memoryPool.bindMemoryConsumer(this);
    }

    @Override
    public void serializeSelfToByteBuf(MemoryPool memoryPool) {
        this.bytesOffHeap = (ByteBuf) memoryPool.requireMemory(bytes.length, memoryPool);
        this.bytesOffHeap.markReaderIndex();
        this.bytesOffHeap.writeBytes(bytes);
    }

    @Override
    public BinaryId zeroCopyReadFromByteBuf() {
        return new BinaryId(ByteBufUtil.getBytes(bytesOffHeap),
                                               (Id) originId.zeroCopyReadFromByteBuf());
    }

    @Override
    public List<ByteBuf> getAllMemoryBlock() {
        return Collections.singletonList(bytesOffHeap);
    }

    @Override
    public void releaseOriginalVarsOnHeap() {
        this.bytes = null;
        this.id = null;
    }

    @Override
    public Object asObject() {
        return bytesOffHeap.nioBuffer();
    }

    @Override
    public String toString() {
        return "0x" + Bytes.toHex(asBytes());
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof BinaryIdOffHeap)) {
            return false;
        }
        return bytesOffHeap.equals(((BinaryIdOffHeap) other).bytesOffHeap);
    }

    @Override
    public int hashCode() {
        return bytesOffHeap.hashCode();
    }

    @Override
    public int length() {
        return bytesOffHeap.readableBytes();
    }

    @Override
    public byte[] asBytes(int offset) {
        E.checkArgument(offset < this.bytesOffHeap.readableBytes(),
                        "Invalid offset %s, must be < length %s",
                        offset, this.bytesOffHeap.readableBytes());
        try {
            // zero-copy read
            byte[] tmpBytes = new byte[offset];
            this.bytesOffHeap.readBytes(tmpBytes);
            return tmpBytes;
        } finally {
            this.bytesOffHeap.resetReaderIndex();
        }
    }

    @Override
    public byte[] asBytes() {
        try {
            // zero-copy read
            byte[] tmpBytes = new byte[bytesOffHeap.readableBytes()];
            this.bytesOffHeap.readBytes(tmpBytes);
            return tmpBytes;
        } finally {
            this.bytesOffHeap.resetReaderIndex();
        }
    }

    @Override
    public int compareTo(Id other) {
        return bytesOffHeap.compareTo(((BinaryIdOffHeap) other).bytesOffHeap);
    }

    @Override
    public Id origin() {
        return (Id) originId.zeroCopyReadFromByteBuf();
    }
}
