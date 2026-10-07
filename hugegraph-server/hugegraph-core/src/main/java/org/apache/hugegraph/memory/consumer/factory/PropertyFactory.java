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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hugegraph.memory.MemoryManager;
import org.apache.hugegraph.memory.consumer.impl.property.HugeEdgePropertyOffHeap;
import org.apache.hugegraph.memory.consumer.impl.property.HugeVertexPropertyOffHeap;
import org.apache.hugegraph.memory.pool.impl.TaskMemoryPool;
import org.apache.hugegraph.struct.schema.PropertyKey;
import org.apache.hugegraph.structure.HugeEdgeProperty;
import org.apache.hugegraph.structure.HugeElement;
import org.apache.hugegraph.structure.HugeVertexProperty;

// NOTE: current MemoryManager doesn't support on-heap management.
public class PropertyFactory<V> {

    private MemoryManager.MemoryMode memoryMode;

    private PropertyFactory() {
        // empty constructor
    }

    public HugeEdgeProperty<V> newHugeEdgeProperty(HugeElement owner, PropertyKey key,
                                                   V value) {
        switch (memoryMode) {
            case ENABLE_ON_HEAP_MANAGEMENT:
            case ENABLE_OFF_HEAP_MANAGEMENT:
                TaskMemoryPool taskMemoryPool = (TaskMemoryPool) MemoryManager.getInstance()
                                                                              .getCorrespondingTaskMemoryPool(
                                                                                      Thread.currentThread()
                                                                                            .getName());
                return new HugeEdgePropertyOffHeap<>(
                        taskMemoryPool.getCurrentWorkingOperatorMemoryPool(), owner, key, value);
            case DISABLE_MEMORY_MANAGEMENT:
            default:
                return new HugeEdgeProperty<>(owner, key, value);
        }
    }

    public HugeVertexProperty<V> newHugeVertexProperty(HugeElement owner, PropertyKey key,
                                                       V value) {
        switch (memoryMode) {
            case ENABLE_ON_HEAP_MANAGEMENT:
            case ENABLE_OFF_HEAP_MANAGEMENT:
                TaskMemoryPool taskMemoryPool = (TaskMemoryPool) MemoryManager.getInstance()
                                                                              .getCorrespondingTaskMemoryPool(
                                                                                      Thread.currentThread()
                                                                                            .getName());
                return new HugeVertexPropertyOffHeap<>(
                        taskMemoryPool.getCurrentWorkingOperatorMemoryPool(), owner, key, value);
            case DISABLE_MEMORY_MANAGEMENT:
            default:
                return new HugeVertexProperty<>(owner, key, value);
        }
    }

    private static class PropertyFactoryHolder {

        private static final Map<Class<?>, PropertyFactory<?>> FACTORIES_MAP =
                new ConcurrentHashMap<>();

        private PropertyFactoryHolder() {
            // empty constructor
        }
    }

    @SuppressWarnings("unchecked")
    public static <T> PropertyFactory<T> getInstance(Class<T> clazz) {
        PropertyFactory<T> instance = (PropertyFactory<T>) PropertyFactoryHolder.FACTORIES_MAP
                .computeIfAbsent(clazz, k -> new PropertyFactory<>());
        if (instance.memoryMode == null) {
            instance.memoryMode = MemoryManager.getMemoryMode();
        }
        return instance;
    }
}
