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

package org.apache.hugegraph.backend.store.ram;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hugegraph.exception.HugeException;

public final class IntLongMap implements RamMap {

    // TODO: use com.carrotsearch.hppc.IntLongHashMap instead
    private final long[] array;
    private int size;

    public IntLongMap(int capacity) {
        this.array = new long[capacity];
        this.size = 0;
    }

    public void put(int key, long value) {
        if (key >= this.size || key < 0) {
            throw new HugeException("Invalid key %s", key);
        }
        this.array[key] = value;
    }

    public int add(long value) {
        if (this.size == Integer.MAX_VALUE) {
            throw new HugeException("Too many edges %s", this.size);
        }
        int index = this.size;
        this.array[index] = value;
        this.size++;
        return index;
    }

    public long get(int key) {
        if (key >= this.size || key < 0) {
            throw new HugeException("Invalid key %s", key);
        }
        return this.array[key];
    }

    @Override
    public void clear() {
        Arrays.fill(this.array, 0L);
        this.size = 0;
    }

    @Override
    public long size() {
        return this.size;
    }

    @Override
    public void writeTo(DataOutputStream buffer) throws IOException {
        buffer.writeInt(this.array.length);
        for (long value : this.array) {
            buffer.writeLong(value);
        }
    }

    @Override
    public void readFrom(DataInputStream buffer) throws IOException {
        int size = buffer.readInt();
        if (size > this.array.length) {
            throw new HugeException("Invalid size %s, expect < %s",
                                    size, this.array.length);
        }
        for (int i = 0; i < size; i++) {
            long value = buffer.readLong();
            this.array[i] = value;
        }
        this.size = size;
    }
}
