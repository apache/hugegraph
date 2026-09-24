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

package org.apache.hugegraph.type;

import java.util.HashMap;
import java.util.Map;

import org.apache.hugegraph.type.define.SerialEnum;

public enum HugeType implements SerialEnum {

    UNKNOWN(0, "UNKNOWN"),

    /* Schema types */
    VERTEX_LABEL(1, "VL"),
    EDGE_LABEL(2, "EL"),
    PROPERTY_KEY(3, "PK"),
    INDEX_LABEL(4, "IL"),

    META(40, "M"),
    COUNTER(50, "C"),

    /* Data types */
    VERTEX(101, "V"),
    // System meta
    SYS_PROPERTY(102, "S"),
    // Property
    PROPERTY(103, "U"),
    // Vertex aggregate property
    AGGR_PROPERTY_V(104, "VP"),
    // Edge aggregate property
    AGGR_PROPERTY_E(105, "EP"),
    // Olap property
    OLAP(106, "AP"),
    // Edge
    EDGE(120, "E"),
    // Edge's direction is OUT for the specified vertex
    EDGE_OUT(130, "O"),
    // Edge's direction is IN for the specified vertex
    EDGE_IN(140, "I"),

    SECONDARY_INDEX(150, "SI"),
    VERTEX_LABEL_INDEX(151, "VI"),
    EDGE_LABEL_INDEX(152, "EI"),
    RANGE_INT_INDEX(160, "II"),
    RANGE_FLOAT_INDEX(161, "FI"),
    RANGE_LONG_INDEX(162, "LI"),
    RANGE_DOUBLE_INDEX(163, "DI"),
    SEARCH_INDEX(170, "AI"),
    SHARD_INDEX(175, "HI"),
    UNIQUE_INDEX(178, "UI"),

    TASK(180, "T"),
    SERVER(181, "SERVER"),

    VARIABLE(185, "VA"),

    KV_TYPE(200, "KV"),
    KV_RAW(201, "KVR"),

    // System schema
    SYS_SCHEMA(250, "SS"),

    MAX_TYPE(255, "~");

    private byte type = 0;
    private final String name;

    private static final Map<String, HugeType> ALL_NAME = new HashMap<>();

    static {
        SerialEnum.register(HugeType.class);
        for (HugeType type : values()) {
            ALL_NAME.put(type.name, type);
        }
    }

    HugeType(int type, String name) {
        assert type < 256;
        this.type = (byte) type;
        this.name = name;
    }

    @Override
    public byte code() {
        return this.type;
    }

    public String string() {
        return this.name;
    }

    public String readableName() {
        return this.name().replace('_', ' ').toLowerCase();
    }

    public boolean isSchema() {
        return this == HugeType.VERTEX_LABEL ||
               this == HugeType.EDGE_LABEL ||
               this == HugeType.PROPERTY_KEY ||
               this == HugeType.INDEX_LABEL;
    }

    public boolean isGraph() {
        return this.isVertex() || this.isEdge();
    }

    public boolean isVertex() {
        // Consider task vertex variable as the same, all used to store HugeVertex structure
        return this == HugeType.VERTEX || this == HugeType.TASK ||
               this == HugeType.VARIABLE || this == HugeType.SERVER;
    }

    public boolean isEdge() {
        return this == EDGE || this == EDGE_OUT || this == EDGE_IN;
    }

    public boolean isEdgeLabel() {
        return this == EDGE_LABEL;
    }

    public boolean isIndex() {
        return this == VERTEX_LABEL_INDEX || this == EDGE_LABEL_INDEX ||
               this == SECONDARY_INDEX || this == SEARCH_INDEX ||
               this == RANGE_INT_INDEX || this == RANGE_FLOAT_INDEX ||
               this == RANGE_LONG_INDEX || this == RANGE_DOUBLE_INDEX ||
               this == SHARD_INDEX || this == UNIQUE_INDEX;
    }

    public boolean isLabelIndex() {
        return this == VERTEX_LABEL_INDEX || this == EDGE_LABEL_INDEX;
    }

    public boolean isStringIndex() {
        return this == VERTEX_LABEL_INDEX || this == EDGE_LABEL_INDEX ||
               this == SECONDARY_INDEX || this == SEARCH_INDEX ||
               this == SHARD_INDEX || this == UNIQUE_INDEX;
    }

    public boolean isNumericIndex() {
        return this == RANGE_INT_INDEX || this == RANGE_FLOAT_INDEX ||
               this == RANGE_LONG_INDEX || this == RANGE_DOUBLE_INDEX ||
               this == SHARD_INDEX;
    }

    public boolean isSecondaryIndex() {
        return this == VERTEX_LABEL_INDEX || this == EDGE_LABEL_INDEX ||
               this == SECONDARY_INDEX;
    }

    public boolean isSearchIndex() {
        return this == SEARCH_INDEX;
    }

    public boolean isRangeIndex() {
        return this == RANGE_INT_INDEX || this == RANGE_FLOAT_INDEX ||
               this == RANGE_LONG_INDEX || this == RANGE_DOUBLE_INDEX;
    }

    public boolean isRange4Index() {
        return this == RANGE_INT_INDEX || this == RANGE_FLOAT_INDEX;
    }

    public boolean isRange8Index() {
        return this == RANGE_LONG_INDEX || this == RANGE_DOUBLE_INDEX;
    }

    public boolean isShardIndex() {
        return this == SHARD_INDEX;
    }

    public boolean isUniqueIndex() {
        return this == UNIQUE_INDEX;
    }

    public boolean isVertexAggregateProperty() {
        return this == AGGR_PROPERTY_V;
    }

    public boolean isEdgeAggregateProperty() {
        return this == AGGR_PROPERTY_E;
    }

    public boolean isAggregateProperty() {
        return this.isVertexAggregateProperty() ||
               this.isEdgeAggregateProperty();
    }

    public static HugeType fromString(String type) {
        return ALL_NAME.get(type);
    }

    public static HugeType fromCode(byte code) {
        return SerialEnum.fromCode(HugeType.class, code);
    }
}
