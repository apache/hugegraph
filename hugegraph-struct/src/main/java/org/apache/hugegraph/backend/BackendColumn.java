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

import java.util.Arrays;

import org.apache.hugegraph.util.Bytes;

import org.apache.hugegraph.util.StringEncoding;

public class BackendColumn implements Comparable<BackendColumn> {

    public byte[] name;
    public byte[] value;

    public static BackendColumn of(byte[] name, byte[] value) {
        BackendColumn col = new BackendColumn();
        col.name = name;
        col.value = value;
        return col;
    }

    @Override
    public String toString() {
        return String.format("%s=%s",
                             StringEncoding.decode(name),
                             StringEncoding.decode(value));
    }

    @Override
    public int compareTo(BackendColumn other) {
        if (other == null) {
            return 1;
        }
        return Bytes.compare(this.name, other.name);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof BackendColumn)) {
            return false;
        }
        BackendColumn other = (BackendColumn) obj;
        return Bytes.equals(this.name, other.name) &&
               Bytes.equals(this.value, other.value);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(this.name) | Arrays.hashCode(this.value);
    }
}
