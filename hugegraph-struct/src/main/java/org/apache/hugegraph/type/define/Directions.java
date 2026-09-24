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

public enum Directions implements SerialEnum {

    // TODO: add NONE enum for non-directional edges

    BOTH(0, "both"),

    OUT(1, "out"),

    IN(2, "in");

    private byte code = 0;
    private String name = null;

    static {
        SerialEnum.register(Directions.class);
    }

    Directions(int code, String name) {
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
            case OUT:
                return HugeType.EDGE_OUT;
            case IN:
                return HugeType.EDGE_IN;
            default:
                throw new IllegalArgumentException(String.format(
                          "Can't convert direction '%s' to HugeType", this));
        }
    }

    public Directions opposite() {
        if (this.equals(OUT)) {
            return IN;
        } else {
            return this.equals(IN) ? OUT : BOTH;
        }
    }



    public static Directions convert(HugeType edgeType) {
        switch (edgeType) {
            case EDGE_OUT:
                return OUT;
            case EDGE_IN:
                return IN;
            default:
                throw new IllegalArgumentException(String.format(
                          "Can't convert type '%s' to Direction", edgeType));
        }
    }
}
