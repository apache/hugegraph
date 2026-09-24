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

public enum AggregateType implements SerialEnum {

    NONE(0, "none"),
    MAX(1, "max"),
    MIN(2, "min"),
    SUM(3, "sum"),
    OLD(4, "old"),
    SET(5, "set"),
    LIST(6, "list");

    private final byte code;
    private final String name;

    static {
        SerialEnum.register(AggregateType.class);
    }

    AggregateType(int code, String name) {
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

    public boolean isNone() {
        return this == NONE;
    }

    public boolean isMax() {
        return this == MAX;
    }

    public boolean isMin() {
        return this == MIN;
    }

    public boolean isSum() {
        return this == SUM;
    }

    public boolean isNumber() {
        return this.isMax() || this.isMin() || this.isSum();
    }

    public boolean isOld() {
        return this == OLD;
    }

    public boolean isSet() {
        return this == SET;
    }

    public boolean isList() {
        return this == LIST;
    }

    public boolean isUnion() {
        return this == SET || this == LIST;
    }

    public boolean isIndexable() {
        return this == NONE || this == MAX || this == MIN || this == OLD;
    }
}
