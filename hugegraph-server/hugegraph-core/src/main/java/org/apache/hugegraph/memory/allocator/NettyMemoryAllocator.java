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

package org.apache.hugegraph.memory.allocator;

import org.apache.hugegraph.memory.MemoryManager;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.util.ReferenceCountUtil;

/**
 * This class makes fully use of Netty's efficient memory management strategy.
 */
public class NettyMemoryAllocator implements MemoryAllocator {

    private final PooledByteBufAllocator offHeapAllocator = PooledByteBufAllocator.DEFAULT;
    private final MemoryManager memoryManager;

    public NettyMemoryAllocator(MemoryManager memoryManager) {
        this.memoryManager = memoryManager;
    }

    @Override
    public ByteBuf forceAllocate(long size) {
        memoryManager.getCurrentOffHeapAllocatedMemoryInBytes().addAndGet(size);
        return offHeapAllocator.directBuffer((int) size);
    }

    @Override
    public ByteBuf tryToAllocate(long size) {
        if (memoryManager.getCurrentOnHeapAllocatedMemoryInBytes().get() +
            memoryManager.getCurrentOffHeapAllocatedMemoryInBytes().get() + size <
            MemoryManager.MAX_MEMORY_CAPACITY_IN_BYTES) {
            memoryManager.getCurrentOffHeapAllocatedMemoryInBytes().addAndGet(size);
            return offHeapAllocator.directBuffer((int) size);
        }
        return null;
    }

    @Override
    public void returnMemoryToManager(long size) {
        memoryManager.getCurrentOffHeapAllocatedMemoryInBytes().addAndGet(-size);
    }

    @Override
    public void releaseMemoryBlock(Object memoryBlock) {
        if (!(memoryBlock instanceof ByteBuf buf)) {
            throw new IllegalArgumentException(
                    String.format("memoryBlock must be ByteBuf, current " +
                                  "class: %s", memoryBlock.getClass()));
        }
        ReferenceCountUtil.safeRelease(buf);
    }
}
