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
import java.util.Objects;

import org.apache.hugegraph.id.EdgeId;
import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.memory.consumer.OffHeapObject;
import org.apache.hugegraph.memory.pool.MemoryPool;
import org.apache.hugegraph.structure.HugeVertex;
import org.apache.hugegraph.type.define.Directions;

import com.google.common.collect.Lists;

import io.netty.buffer.ByteBuf;

// TODO: rewrite static method in EdgeId
public class EdgeIdOffHeap extends EdgeId implements OffHeapObject {

    private final MemoryPool memoryPool;
    private final OffHeapObject ownerVertexIdOffHeap;
    private final OffHeapObject edgeLabelIdOffHeap;
    private final OffHeapObject subLabelIdOffHeap;
    private final OffHeapObject otherVertexIdOffHeap;
    private ByteBuf sortValuesOffHeap;
    private ByteBuf cacheOffHeap;

    public EdgeIdOffHeap(HugeVertex ownerVertex,
                         Directions direction,
                         Id edgeLabelId,
                         Id subLabelId,
                         String sortValues,
                         HugeVertex otherVertex,
                         MemoryPool memoryPool,
                         OffHeapObject ownerVertexIdOffHeap,
                         OffHeapObject edgeLabelIdOffHeap,
                         OffHeapObject subLabelIdOffHeap,
                         OffHeapObject otherVertexIdOffHeap) {
        super(ownerVertex.id(), direction, edgeLabelId, subLabelId, sortValues, otherVertex.id());
        this.memoryPool = memoryPool;
        this.ownerVertexIdOffHeap = ownerVertexIdOffHeap;
        this.edgeLabelIdOffHeap = edgeLabelIdOffHeap;
        this.subLabelIdOffHeap = subLabelIdOffHeap;
        this.otherVertexIdOffHeap = otherVertexIdOffHeap;
        serializeSelfToByteBuf(memoryPool);
        releaseOriginalVarsOnHeap();
        memoryPool.bindMemoryConsumer(this);
    }

    public EdgeIdOffHeap(Id ownerVertexId,
                         Directions direction,
                         Id edgeLabelId,
                         Id subLabelId,
                         String sortValues,
                         Id otherVertexId,
                         MemoryPool memoryPool,
                         OffHeapObject ownerVertexIdOffHeap,
                         OffHeapObject edgeLabelIdOffHeap,
                         OffHeapObject subLabelIdOffHeap,
                         OffHeapObject otherVertexIdOffHeap) {
        super(ownerVertexId, direction, edgeLabelId, subLabelId,
              sortValues, otherVertexId, false);
        this.memoryPool = memoryPool;
        this.ownerVertexIdOffHeap = ownerVertexIdOffHeap;
        this.edgeLabelIdOffHeap = edgeLabelIdOffHeap;
        this.subLabelIdOffHeap = subLabelIdOffHeap;
        this.otherVertexIdOffHeap = otherVertexIdOffHeap;
        serializeSelfToByteBuf(memoryPool);
        releaseOriginalVarsOnHeap();
        memoryPool.bindMemoryConsumer(this);
    }

    public EdgeIdOffHeap(Id ownerVertexId,
                         Directions direction,
                         Id edgeLabelId,
                         Id subLabelId,
                         String sortValues,
                         Id otherVertexId,
                         boolean directed,
                         MemoryPool memoryPool,
                         OffHeapObject ownerVertexIdOffHeap,
                         OffHeapObject edgeLabelIdOffHeap,
                         OffHeapObject subLabelIdOffHeap,
                         OffHeapObject otherVertexIdOffHeap) {
        super(ownerVertexId, direction, edgeLabelId, subLabelId, sortValues, otherVertexId,
              directed);
        this.memoryPool = memoryPool;
        this.ownerVertexIdOffHeap = ownerVertexIdOffHeap;
        this.edgeLabelIdOffHeap = edgeLabelIdOffHeap;
        this.subLabelIdOffHeap = subLabelIdOffHeap;
        this.otherVertexIdOffHeap = otherVertexIdOffHeap;
        serializeSelfToByteBuf(memoryPool);
        releaseOriginalVarsOnHeap();
        memoryPool.bindMemoryConsumer(this);
    }

    @Override
    public Object zeroCopyReadFromByteBuf() {
        try {
            return new EdgeId((Id) this.ownerVertexIdOffHeap.zeroCopyReadFromByteBuf(),
                              this.direction,
                              (Id) this.edgeLabelIdOffHeap.zeroCopyReadFromByteBuf(),
                              (Id) this.subLabelIdOffHeap.zeroCopyReadFromByteBuf(),
                              this.sortValuesOffHeap.toString(StandardCharsets.UTF_8),
                              (Id) this.otherVertexIdOffHeap.zeroCopyReadFromByteBuf());
        } finally {
            this.sortValuesOffHeap.resetReaderIndex();
        }
    }

    @Override
    public void serializeSelfToByteBuf(MemoryPool memoryPool) {
        byte[] stringBytes = sortValues.getBytes((StandardCharsets.UTF_8));
        this.sortValuesOffHeap =
                (ByteBuf) this.memoryPool.requireMemory(stringBytes.length, memoryPool);
        this.sortValuesOffHeap.markReaderIndex();
        this.sortValuesOffHeap.writeBytes(stringBytes);
    }

    @Override
    public void releaseOriginalVarsOnHeap() {
        this.sortValues = null;
    }

    @Override
    public List<ByteBuf> getAllMemoryBlock() {
        return this.cacheOffHeap == null ? Collections.singletonList(this.sortValuesOffHeap) :
               Lists.newArrayList(this.sortValuesOffHeap,
                                  this.cacheOffHeap);
    }

    @Override
    public EdgeId switchDirection() {
        Directions newDirection = this.direction.opposite();
        return new EdgeIdOffHeap(this.otherVertexId,
                                 newDirection,
                                 this.edgeLabelId,
                                 this.subLabelId,
                                 this.sortValues,
                                 this.ownerVertexId,
                                 this.memoryPool,
                                 this.ownerVertexIdOffHeap,
                                 this.edgeLabelIdOffHeap,
                                 this.subLabelIdOffHeap,
                                 this.otherVertexIdOffHeap);
    }

    @Override
    public EdgeId directed(boolean directed) {
        return new EdgeIdOffHeap(this.otherVertexId,
                                 this.direction,
                                 this.edgeLabelId,
                                 this.subLabelId,
                                 this.sortValues,
                                 this.ownerVertexId,
                                 directed,
                                 this.memoryPool,
                                 this.ownerVertexIdOffHeap,
                                 this.edgeLabelIdOffHeap,
                                 this.subLabelIdOffHeap,
                                 this.otherVertexIdOffHeap);
    }

    @Override
    public Id ownerVertexId() {
        return (Id) this.ownerVertexIdOffHeap.zeroCopyReadFromByteBuf();
    }

    @Override
    public Id edgeLabelId() {
        return (Id) this.edgeLabelIdOffHeap.zeroCopyReadFromByteBuf();
    }

    @Override
    public Id subLabelId() {
        return (Id) this.subLabelIdOffHeap.zeroCopyReadFromByteBuf();
    }

    @Override
    public String sortValues() {
        try {
            return this.sortValuesOffHeap.toString(StandardCharsets.UTF_8);
        } finally {
            this.sortValuesOffHeap.resetReaderIndex();
        }
    }

    @Override
    public Id otherVertexId() {
        return (Id) this.otherVertexIdOffHeap.zeroCopyReadFromByteBuf();
    }

    @Override
    public String asString() {
        if (this.cacheOffHeap != null) {
            try {
                return this.cacheOffHeap.toString(StandardCharsets.UTF_8);
            } finally {
                this.cacheOffHeap.resetReaderIndex();
            }
        }
        String tmpCache = this.formatString();
        byte[] tmpCacheBytes = tmpCache.getBytes(StandardCharsets.UTF_8);
        this.cacheOffHeap = (ByteBuf) memoryPool.requireMemory(tmpCacheBytes.length, memoryPool);
        this.cacheOffHeap.markReaderIndex();
        this.cacheOffHeap.writeBytes(tmpCacheBytes);
        return tmpCache;
    }

    @Override
    public int hashCode() {
        if (this.directed) {
            return Objects.hash(this.ownerVertexIdOffHeap,
                                this.direction,
                                this.edgeLabelIdOffHeap,
                                this.subLabelIdOffHeap,
                                this.sortValuesOffHeap,
                                this.otherVertexIdOffHeap);
        } else {
            return Objects.hash(this.otherVertexIdOffHeap,
                                this.edgeLabelIdOffHeap,
                                this.subLabelIdOffHeap,
                                this.sortValuesOffHeap,
                                this.ownerVertexIdOffHeap);
        }
    }

    @Override
    public boolean equals(Object object) {
        if (!(object instanceof EdgeIdOffHeap)) {
            return false;
        }
        EdgeIdOffHeap other = (EdgeIdOffHeap) object;
        if (this.directed) {
            return this.ownerVertexIdOffHeap.equals(other.ownerVertexIdOffHeap) &&
                   this.direction == other.direction &&
                   this.edgeLabelIdOffHeap.equals(other.edgeLabelIdOffHeap) &&
                   this.subLabelIdOffHeap.equals(other.subLabelIdOffHeap) &&
                   this.sortValuesOffHeap.equals(other.sortValuesOffHeap) &&
                   this.otherVertexIdOffHeap.equals(other.otherVertexIdOffHeap);
        } else {
            return this.otherVertexIdOffHeap.equals(other.otherVertexIdOffHeap) &&
                   this.edgeLabelIdOffHeap.equals(other.edgeLabelIdOffHeap) &&
                   this.subLabelIdOffHeap.equals(other.subLabelIdOffHeap) &&
                   this.sortValuesOffHeap.equals(other.sortValuesOffHeap) &&
                   this.ownerVertexIdOffHeap.equals(other.ownerVertexIdOffHeap);
        }
    }
}
