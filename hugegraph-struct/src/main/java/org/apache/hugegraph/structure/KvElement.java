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

package org.apache.hugegraph.structure;

import java.util.List;

import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.type.define.HugeKeys;

/**
 * for aggregation calculation
 */
public class KvElement extends BaseElement implements Comparable<KvElement>{

    private List<Comparable> keys;

    private List<Object> values;

    private KvElement(List<Comparable> keys, List<Object> values) {
        this.keys = keys;
        this.values = values;
    }

    public static KvElement of (List<Comparable> keys, List<Object> values) {
        return new KvElement(keys, values);
    }

    public List<Comparable> getKeys() {
        return keys;
    }

    public List<Object> getValues() {
        return values;
    }

    @Override
    public Object sysprop(HugeKeys key) {
        return null;
    }

    @Override
    public String name() {
        return null;
    }

    @Override
    public HugeType type() {
        return HugeType.KV_TYPE;
    }

    /**
     * compare by keys
     * @param other the object to be compared.
     * @return -1 = this > other, 0 = this == other, 1 = this < other.
     */
    @Override
    public int compareTo(KvElement other) {
        if (this == other) {
            return 0;
        }

        if (other == null || other.keys == null) {
            return keys == null ? 0 : 1;
        }

        int len = Math.min(keys.size(), other.keys.size());
        for (int i = 0; i < len; i++) {
            var o1 = keys.get(i);
            var o2 = other.keys.get(i);
            if (o1 != o2) {
                if (o1 == null || o2 == null) {
                    return o1 == null ? -1 : 1;
                }

                int v = o1.compareTo(o2);
                if (v !=  0) {
                    return v;
                }
            }
        }

        return keys.size() - other.keys.size();
    }
}
