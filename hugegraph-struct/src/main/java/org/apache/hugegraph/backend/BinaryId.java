/*
 * Copyright 2017 HugeGraph Authors
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hugegraph.backend;

import org.apache.hugegraph.id.Id;

import org.apache.hugegraph.util.Bytes;
import org.apache.hugegraph.util.E;

import java.nio.ByteBuffer;
import java.util.Arrays;

public final class BinaryId implements Id {

    private final byte[] bytes;
    private final Id id;

    public BinaryId(byte[] bytes, Id id) {
        this.bytes = bytes;
        this.id = id;
    }

    public Id origin() {
        return this.id;
    }

    @Override
    public IdType type() {
        return IdType.UNKNOWN;
    }

    @Override
    public Object asObject() {
        return ByteBuffer.wrap(this.bytes);
    }

    @Override
    public String asString() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long asLong() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int compareTo(Id other) {
        return Bytes.compare(this.bytes, other.asBytes());
    }

    @Override
    public byte[] asBytes() {
        return this.bytes;
    }

    public byte[] asBytes(int offset) {
        E.checkArgument(offset < this.bytes.length,
                        "Invalid offset %s, must be < length %s",
                        offset, this.bytes.length);
        return Arrays.copyOfRange(this.bytes, offset, this.bytes.length);
    }

    @Override
    public int length() {
        return this.bytes.length;
    }

    @Override
    public int hashCode() {
        return ByteBuffer.wrap(this.bytes).hashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof BinaryId)) {
            return false;
        }
        return Arrays.equals(this.bytes, ((BinaryId) other).bytes);
    }

    @Override
    public String toString() {
        return "0x" + Bytes.toHex(this.bytes);
    }
}
