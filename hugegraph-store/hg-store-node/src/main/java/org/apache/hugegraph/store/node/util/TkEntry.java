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

package org.apache.hugegraph.store.node.util;

import java.util.Arrays;
import java.util.Objects;

/**
 * Table Key pair.
 */
public class TkEntry {

    private final String table;
    private final byte[] key;

    public TkEntry(String table, byte[] key) {
        this.table = table;
        this.key = key;
    }

    public String getTable() {
        return table;
    }

    public byte[] getKey() {
        return key;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TkEntry tk)) {
            return false;
        }
        return Objects.equals(table, tk.table) && Arrays.equals(key, tk.key);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(table);
        result = 31 * result + Arrays.hashCode(key);
        return result;
    }

    @Override
    public String toString() {
        return "Tk{" +
               "table='" + table + '\'' +
               ", key=" + Arrays.toString(key) +
               '}';
    }
}
