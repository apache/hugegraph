/*
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

import org.apache.hugegraph.type.define.SerialEnum;

public enum HugeTableType implements SerialEnum {

    UNKNOWN(0, "UNKNOWN"),

    /* Schema types */
    VERTEX(1, "V"), // Vertex table
    OUT_EDGE(2, "OE"), // Out-edge table
    IN_EDGE(3, "IE"), // In-edge table
    ALL_INDEX_TABLE(4, "INDEX"), // Index Table
    TASK_INFO_TABLE(5, "TASK"), // Task Information Table
    OLAP_TABLE(6, "OLAP"), // OLAP table
    SERVER_INFO_TABLE(7, "SERVER"); // Server Information Table

    static {
        SerialEnum.register(HugeTableType.class);
    }

    private byte type = 0;
    private String name;

    HugeTableType(int type, String name) {
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
}
