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

package org.apache.hugegraph.util;

import java.util.Arrays;

import org.apache.hugegraph.util.Bytes;
import org.apache.hugegraph.util.E;

public class Blob implements Comparable<Blob> {

    public static final Blob EMPTY = new Blob(new byte[0]);

    private final byte[] bytes;

    private Blob(byte[] bytes) {
        E.checkNotNull(bytes, "bytes");
        this.bytes = bytes;
    }

    public byte[] bytes() {
        return this.bytes;
    }

    public static Blob wrap(byte[] bytes) {
        return new Blob(bytes);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(this.bytes);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Blob)) {
            return false;
        }
        Blob other = (Blob) obj;
        return Arrays.equals(this.bytes, other.bytes);
    }

    @Override
    public String toString() {
        String hex = Bytes.toHex(this.bytes);
        StringBuilder sb = new StringBuilder(6 + hex.length());
        sb.append("Blob{").append(hex).append("}");
        return sb.toString();
    }

    @Override
    public int compareTo(Blob other) {
        E.checkNotNull(other, "other blob");
        return Bytes.compare(this.bytes, other.bytes);
    }
}
