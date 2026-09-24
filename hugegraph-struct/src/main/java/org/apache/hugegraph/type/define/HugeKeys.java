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

public enum HugeKeys {

    UNKNOWN(0, "undefined"),

    /* Column names of schema type (common) */
    ID(1, "id"),
    NAME(2, "name"),
    TIMESTAMP(3, "timestamp"),
    SCHEMA_TYPE(4, "schema_type"),
    VALUE(5, "value"),
    VERSION(6, "version"),

    USER_DATA(10, "user_data"),
    STATUS(11, "status"),

    /* Column names of schema type (VertexLabel) */
    ID_STRATEGY(50, "id_strategy"),
    PROPERTIES(51, "properties"),
    PRIMARY_KEYS(52, "primary_keys"),
    INDEX_LABELS(53, "index_labels"),
    NULLABLE_KEYS(54, "nullable_keys"),
    ENABLE_LABEL_INDEX(55, "enable_label_index"),

    /* Column names of schema type (EdgeLabel) */
    LINKS(80, "links"),
    FREQUENCY(81, "frequency"),
    SOURCE_LABEL(82, "source_label"),
    TARGET_LABEL(83, "target_label"),
    SORT_KEYS(84, "sort_keys"),
    TTL(85, "ttl"),
    TTL_START_TIME(86, "ttl_start_time"),
    EDGELABEL_TYPE(87, "edgelabel_type"),
    PARENT_LABEL(89, "parent_label"),


    /* Column names of schema type (PropertyKey) */
    DATA_TYPE(120, "data_type"),
    CARDINALITY(121, "cardinality"),
    AGGREGATE_TYPE(122, "aggregate_type"),
    WRITE_TYPE(123, "write_type"),

    /* Column names of schema type (IndexLabel) */
    BASE_TYPE(150, "base_type"),
    BASE_VALUE(151, "base_value"),
    INDEX_TYPE(152, "index_type"),
    FIELDS(153, "fields"),

    /* Column names of index data */
    INDEX_NAME(180, "index_name"),
    FIELD_VALUES(181, "field_values"),
    INDEX_LABEL_ID(182, "index_label_id"),
    ELEMENT_IDS(183, "element_ids"),

    /* Column names of data type (Vertex/Edge) */
    LABEL(200, "label"),
    OWNER_VERTEX(201, "owner_vertex"),
    OTHER_VERTEX(202, "other_vertex"),
    PROPERTY_KEY(203, "property_key"),
    PROPERTY_VALUE(204, "property_value"),
    DIRECTION(205, "direction"),
    SORT_VALUES(206, "sort_values"),
    PRIMARY_VALUES(207, "primary_values"),
    EXPIRED_TIME(208, "expired_time"),
    SUB_LABEL(211,"sub_label"),

    PROPERTY_TYPE(249, "property_type"),
    AGGREGATE_PROPERTIES(250, "aggregate_properties"),
    ;

    public static final long NORMAL_PROPERTY_ID = 0L;

    /* HugeKeys define */
    private byte code = 0;
    private String name = null;

    HugeKeys(int code, String name) {
        assert code < 256;
        this.code = (byte) code;
        this.name = name;
    }

    public byte code() {
        return this.code;
    }

    public String string() {
        return this.name;
    }
}
