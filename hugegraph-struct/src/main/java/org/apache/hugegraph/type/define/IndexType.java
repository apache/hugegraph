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

package org.apache.hugegraph.type.define;

import org.apache.hugegraph.type.HugeType;

public enum IndexType implements SerialEnum {

    // For secondary query
    SECONDARY(1, "secondary"),

    // For range query
    RANGE(2, "range"),
    RANGE_INT(21, "range_int"),
    RANGE_FLOAT(22, "range_float"),
    RANGE_LONG(23, "range_long"),
    RANGE_DOUBLE(24, "range_double"),

    // For full-text query (not supported now)
    SEARCH(3, "search"),

    // For prefix + range query
    SHARD(4, "shard"),

    // For unique index
    UNIQUE(5, "unique");

    private byte code = 0;
    private String name = null;

    static {
        SerialEnum.register(IndexType.class);
    }

    IndexType(int code, String name) {
        assert code < 256;
        this.code = (byte) code;
        this.name = name;
    }

    @Override
    public byte code() {
        return this.code;
    }

    public String string() {
        return this.name;
    }

    public HugeType type() {
        switch (this) {
            case SECONDARY:
                return HugeType.SECONDARY_INDEX;
            case RANGE_INT:
                return HugeType.RANGE_INT_INDEX;
            case RANGE_FLOAT:
                return HugeType.RANGE_FLOAT_INDEX;
            case RANGE_LONG:
                return HugeType.RANGE_LONG_INDEX;
            case RANGE_DOUBLE:
                return HugeType.RANGE_DOUBLE_INDEX;
            case SEARCH:
                return HugeType.SEARCH_INDEX;
            case SHARD:
                return HugeType.SHARD_INDEX;
            case UNIQUE:
                return HugeType.UNIQUE_INDEX;
            default:
                throw new AssertionError(String.format(
                        "Unknown index type '%s'", this));
        }
    }

    public boolean isString() {
        return this == SECONDARY || this == SEARCH ||
               this == SHARD || this == UNIQUE;
    }

    public boolean isNumeric() {
        return this == RANGE_INT || this == RANGE_FLOAT ||
               this == RANGE_LONG || this == RANGE_DOUBLE ||
               this == SHARD;
    }

    public boolean isSecondary() {
        return this == SECONDARY;
    }

    public boolean isRange() {
        return this == RANGE_INT || this == RANGE_FLOAT ||
               this == RANGE_LONG || this == RANGE_DOUBLE;
    }

    public boolean isSearch() {
        return this == SEARCH;
    }

    public boolean isShard() {
        return this == SHARD;
    }

    public boolean isUnique() {
        return this == UNIQUE;
    }
}
