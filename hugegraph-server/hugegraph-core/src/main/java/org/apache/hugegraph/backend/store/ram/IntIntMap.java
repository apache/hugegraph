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

public final class IntIntMap implements RamMap {

    // TODO: use com.carrotsearch.hppc.IntIntHashMap instead
    private final int[] array;

    public IntIntMap(int capacity) {
        this.array = new int[capacity];
    }

    public void put(long key, int value) {
        assert 0 <= key && key < Integer.MAX_VALUE;
        this.array[(int) key] = value;
    }

    public int get(long key) {
        assert 0 <= key && key < Integer.MAX_VALUE;
        return this.array[(int) key];
    }

    @Override
    public void clear() {
        Arrays.fill(this.array, 0);
    }

    @Override
    public long size() {
        return this.array.length;
    }

    @Override
    public void writeTo(DataOutputStream buffer) throws IOException {
        buffer.writeInt(this.array.length);
        for (int value : this.array) {
            buffer.writeInt(value);
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
            int value = buffer.readInt();
            this.array[i] = value;
        }
    }
}
