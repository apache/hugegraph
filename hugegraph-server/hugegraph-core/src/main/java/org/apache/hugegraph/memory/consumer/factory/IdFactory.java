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

package org.apache.hugegraph.memory.consumer.factory;

import org.apache.hugegraph.backend.BinaryId;

import java.util.UUID;

import org.apache.hugegraph.backend.cache.CachedBackendStore;
import org.apache.hugegraph.id.EdgeId;
import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.id.IdGenerator;
import org.apache.hugegraph.query.Query;
import org.apache.hugegraph.backend.serializer.BinaryBackendEntry;
import org.apache.hugegraph.memory.MemoryManager;
import org.apache.hugegraph.memory.consumer.OffHeapObject;
import org.apache.hugegraph.memory.consumer.impl.id.BinaryIdOffHeap;
import org.apache.hugegraph.memory.consumer.impl.id.EdgeIdOffHeap;
import org.apache.hugegraph.memory.consumer.impl.id.LongIdOffHeap;
import org.apache.hugegraph.memory.consumer.impl.id.ObjectIdOffHeap;
import org.apache.hugegraph.memory.consumer.impl.id.QueryIdOffHeap;
import org.apache.hugegraph.memory.consumer.impl.id.StringIdOffHeap;
import org.apache.hugegraph.memory.consumer.impl.id.UuidIdOffHeap;
import org.apache.hugegraph.memory.pool.impl.TaskMemoryPool;
import org.apache.hugegraph.structure.HugeVertex;
import org.apache.hugegraph.type.define.Directions;

// NOTE: current MemoryManager doesn't support on-heap management.
public class IdFactory {

    private MemoryManager.MemoryMode memoryMode;

    private IdFactory() {
        // empty constructor
    }

    /**
     * If using off-heap mode, param id must be OffHeapObject
     */
    public BinaryId newBinaryId(byte[] bytes, Id id) {
        switch (memoryMode) {
            case ENABLE_ON_HEAP_MANAGEMENT:
            case ENABLE_OFF_HEAP_MANAGEMENT:
                TaskMemoryPool taskMemoryPool = (TaskMemoryPool) MemoryManager.getInstance()
                                                                              .getCorrespondingTaskMemoryPool(
                                                                                      Thread.currentThread()
                                                                                            .getName());
                return new BinaryIdOffHeap(bytes, null,
                                           taskMemoryPool.getCurrentWorkingOperatorMemoryPool(),
                                           (OffHeapObject) id);
            case DISABLE_MEMORY_MANAGEMENT:
            default:
                return new BinaryId(bytes, id);
        }
    }

    public IdGenerator.LongId newLongId(long id) {
        switch (memoryMode) {
            case ENABLE_ON_HEAP_MANAGEMENT:
            case ENABLE_OFF_HEAP_MANAGEMENT:
                TaskMemoryPool taskMemoryPool = (TaskMemoryPool) MemoryManager.getInstance()
                                                                              .getCorrespondingTaskMemoryPool(
                                                                                      Thread.currentThread()
                                                                                            .getName());
                return new LongIdOffHeap(taskMemoryPool.getCurrentWorkingOperatorMemoryPool(),
                                         id);
            case DISABLE_MEMORY_MANAGEMENT:
            default:
                return new IdGenerator.LongId(id);
        }
    }

    public IdGenerator.LongId newLongId(byte[] bytes) {
        switch (memoryMode) {
            case ENABLE_ON_HEAP_MANAGEMENT:
            case ENABLE_OFF_HEAP_MANAGEMENT:
                TaskMemoryPool taskMemoryPool = (TaskMemoryPool) MemoryManager.getInstance()
                                                                              .getCorrespondingTaskMemoryPool(
                                                                                      Thread.currentThread()
                                                                                            .getName());
                return new LongIdOffHeap(taskMemoryPool.getCurrentWorkingOperatorMemoryPool(),
                                         bytes);
            case DISABLE_MEMORY_MANAGEMENT:
            default:
                return new IdGenerator.LongId(bytes);
        }
    }

    public IdGenerator.ObjectId newObjectId(Object object) {
        switch (memoryMode) {
            case ENABLE_ON_HEAP_MANAGEMENT:
            case ENABLE_OFF_HEAP_MANAGEMENT:
                TaskMemoryPool taskMemoryPool = (TaskMemoryPool) MemoryManager.getInstance()
                                                                              .getCorrespondingTaskMemoryPool(
                                                                                      Thread.currentThread()
                                                                                            .getName());
                return new ObjectIdOffHeap(taskMemoryPool.getCurrentWorkingOperatorMemoryPool(),
                                           object);
            case DISABLE_MEMORY_MANAGEMENT:
            default:
                return new IdGenerator.ObjectId(object);
        }
    }

    public CachedBackendStore.QueryId newQueryId(Query q) {
        switch (memoryMode) {
            case ENABLE_ON_HEAP_MANAGEMENT:
            case ENABLE_OFF_HEAP_MANAGEMENT:
                TaskMemoryPool taskMemoryPool = (TaskMemoryPool) MemoryManager.getInstance()
                                                                              .getCorrespondingTaskMemoryPool(
                                                                                      Thread.currentThread()
                                                                                            .getName());
                return new QueryIdOffHeap(taskMemoryPool.getCurrentWorkingOperatorMemoryPool(),
                                          q);
            case DISABLE_MEMORY_MANAGEMENT:
            default:
                return new CachedBackendStore.QueryId(q);
        }
    }

    public IdGenerator.StringId newStringId(String id) {
        switch (memoryMode) {
            case ENABLE_ON_HEAP_MANAGEMENT:
            case ENABLE_OFF_HEAP_MANAGEMENT:
                TaskMemoryPool taskMemoryPool = (TaskMemoryPool) MemoryManager.getInstance()
                                                                              .getCorrespondingTaskMemoryPool(
                                                                                      Thread.currentThread()
                                                                                            .getName());
                return new StringIdOffHeap(taskMemoryPool.getCurrentWorkingOperatorMemoryPool(),
                                           id);
            case DISABLE_MEMORY_MANAGEMENT:
            default:
                return new IdGenerator.StringId(id);
        }
    }

    public IdGenerator.StringId newStringId(byte[] bytes) {
        switch (memoryMode) {
            case ENABLE_ON_HEAP_MANAGEMENT:
            case ENABLE_OFF_HEAP_MANAGEMENT:
                TaskMemoryPool taskMemoryPool = (TaskMemoryPool) MemoryManager.getInstance()
                                                                              .getCorrespondingTaskMemoryPool(
                                                                                      Thread.currentThread()
                                                                                            .getName());
                return new StringIdOffHeap(taskMemoryPool.getCurrentWorkingOperatorMemoryPool(),
                                           bytes);
            case DISABLE_MEMORY_MANAGEMENT:
            default:
                return new IdGenerator.StringId(bytes);
        }
    }

    public IdGenerator.UuidId newUuidId(String id) {
        switch (memoryMode) {
            case ENABLE_ON_HEAP_MANAGEMENT:
            case ENABLE_OFF_HEAP_MANAGEMENT:
                TaskMemoryPool taskMemoryPool = (TaskMemoryPool) MemoryManager.getInstance()
                                                                              .getCorrespondingTaskMemoryPool(
                                                                                      Thread.currentThread()
                                                                                            .getName());
                return new UuidIdOffHeap(taskMemoryPool.getCurrentWorkingOperatorMemoryPool(),
                                         id);
            case DISABLE_MEMORY_MANAGEMENT:
            default:
                return new IdGenerator.UuidId(id);
        }
    }

    public IdGenerator.UuidId newUuidId(byte[] bytes) {
        switch (memoryMode) {
            case ENABLE_ON_HEAP_MANAGEMENT:
            case ENABLE_OFF_HEAP_MANAGEMENT:
                TaskMemoryPool taskMemoryPool = (TaskMemoryPool) MemoryManager.getInstance()
                                                                              .getCorrespondingTaskMemoryPool(
                                                                                      Thread.currentThread()
                                                                                            .getName());
                return new UuidIdOffHeap(taskMemoryPool.getCurrentWorkingOperatorMemoryPool(),
                                         bytes);
            case DISABLE_MEMORY_MANAGEMENT:
            default:
                return new IdGenerator.UuidId(bytes);
        }
    }

    public IdGenerator.UuidId newUuidId(UUID id) {
        switch (memoryMode) {
            case ENABLE_ON_HEAP_MANAGEMENT:
            case ENABLE_OFF_HEAP_MANAGEMENT:
                TaskMemoryPool taskMemoryPool = (TaskMemoryPool) MemoryManager.getInstance()
                                                                              .getCorrespondingTaskMemoryPool(
                                                                                      Thread.currentThread()
                                                                                            .getName());
                return new UuidIdOffHeap(taskMemoryPool.getCurrentWorkingOperatorMemoryPool(),
                                         id);
            case DISABLE_MEMORY_MANAGEMENT:
            default:
                return new IdGenerator.UuidId(id);
        }
    }

    public EdgeId newEdgeId(HugeVertex ownerVertex,
                            Directions direction,
                            Id edgeLabelId,
                            Id subLabelId,
                            String sortValues,
                            HugeVertex otherVertex) {
        switch (memoryMode) {
            case ENABLE_ON_HEAP_MANAGEMENT:
            case ENABLE_OFF_HEAP_MANAGEMENT:
                TaskMemoryPool taskMemoryPool = (TaskMemoryPool) MemoryManager.getInstance()
                                                                              .getCorrespondingTaskMemoryPool(
                                                                                      Thread.currentThread()
                                                                                            .getName());
                return new EdgeIdOffHeap(ownerVertex,
                                         direction,
                                         null,
                                         null,
                                         sortValues,
                                         otherVertex,
                                         taskMemoryPool.getCurrentWorkingOperatorMemoryPool(),
                                         (OffHeapObject) ownerVertex.id(),
                                         (OffHeapObject) edgeLabelId,
                                         (OffHeapObject) subLabelId,
                                         (OffHeapObject) otherVertex.id()
                );
            case DISABLE_MEMORY_MANAGEMENT:
            default:
                return new EdgeId(ownerVertex.id(), direction, edgeLabelId, subLabelId, sortValues,
                                  otherVertex.id());
        }
    }

    public EdgeId newEdgeId(Id ownerVertexId,
                            Directions direction,
                            Id edgeLabelId,
                            Id subLabelId,
                            String sortValues,
                            Id otherVertexId) {
        switch (memoryMode) {
            case ENABLE_ON_HEAP_MANAGEMENT:
            case ENABLE_OFF_HEAP_MANAGEMENT:
                TaskMemoryPool taskMemoryPool = (TaskMemoryPool) MemoryManager.getInstance()
                                                                              .getCorrespondingTaskMemoryPool(
                                                                                      Thread.currentThread()
                                                                                            .getName());
                return new EdgeIdOffHeap((Id) null,
                                         direction,
                                         null,
                                         null,
                                         sortValues,
                                         null,
                                         taskMemoryPool.getCurrentWorkingOperatorMemoryPool(),
                                         (OffHeapObject) ownerVertexId,
                                         (OffHeapObject) edgeLabelId,
                                         (OffHeapObject) subLabelId,
                                         (OffHeapObject) otherVertexId
                );
            case DISABLE_MEMORY_MANAGEMENT:
            default:
                return new EdgeId(ownerVertexId, direction, edgeLabelId, subLabelId, sortValues,
                                  otherVertexId);
        }
    }

    public EdgeId newEdgeId(Id ownerVertexId,
                            Directions direction,
                            Id edgeLabelId,
                            Id subLabelId,
                            String sortValues,
                            Id otherVertexId,
                            boolean directed) {
        switch (memoryMode) {
            case ENABLE_ON_HEAP_MANAGEMENT:
            case ENABLE_OFF_HEAP_MANAGEMENT:
                TaskMemoryPool taskMemoryPool = (TaskMemoryPool) MemoryManager.getInstance()
                                                                              .getCorrespondingTaskMemoryPool(
                                                                                      Thread.currentThread()
                                                                                            .getName());
                return new EdgeIdOffHeap(null,
                                         direction,
                                         null,
                                         null,
                                         sortValues,
                                         null,
                                         directed,
                                         taskMemoryPool.getCurrentWorkingOperatorMemoryPool(),
                                         (OffHeapObject) ownerVertexId,
                                         (OffHeapObject) edgeLabelId,
                                         (OffHeapObject) subLabelId,
                                         (OffHeapObject) otherVertexId
                );
            case DISABLE_MEMORY_MANAGEMENT:
            default:
                return new EdgeId(ownerVertexId, direction, edgeLabelId, subLabelId, sortValues,
                                  otherVertexId, directed);
        }
    }

    private static class IdFactoryHolder {

        private static final IdFactory INSTANCE = new IdFactory();

        private IdFactoryHolder() {
            // empty constructor
        }
    }

    public static IdFactory getInstance() {
        IdFactory instance = IdFactoryHolder.INSTANCE;
        if (instance.memoryMode == null) {
            instance.memoryMode = MemoryManager.getMemoryMode();
        }
        return IdFactoryHolder.INSTANCE;
    }
}
