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

public enum Action implements SerialEnum {

    INSERT(1, "insert"),

    APPEND(2, "append"),

    ELIMINATE(3, "eliminate"),

    DELETE(4, "delete"),

    UPDATE_IF_PRESENT(5, "update_if_present"),

    UPDATE_IF_ABSENT(6, "update_if_absent");

    private final byte code;
    private final String name;

    static {
        SerialEnum.register(Action.class);
    }

    Action(int code, String name) {
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

    public static Action fromCode(byte code) {
        switch (code) {
            case 1:
                return INSERT;
            case 2:
                return APPEND;
            case 3:
                return ELIMINATE;
            case 4:
                return DELETE;
            case 5:
                return UPDATE_IF_PRESENT;
            case 6:
                return UPDATE_IF_ABSENT;
            default:
                throw new AssertionError("Unsupported action code: " + code);
        }
    }
}
