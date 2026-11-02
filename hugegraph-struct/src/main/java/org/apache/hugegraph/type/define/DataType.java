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

import java.nio.ByteBuffer;
import java.time.OffsetDateTime;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import org.apache.hugegraph.util.Bytes;
import org.apache.hugegraph.util.DateUtil;

import org.apache.hugegraph.exception.HugeException;
import org.apache.hugegraph.serializer.BytesBuffer;
import org.apache.hugegraph.util.Blob;
import org.apache.hugegraph.util.StringEncoding;
import com.google.common.collect.ImmutableSet;

public enum DataType implements SerialEnum {

    UNKNOWN(0, "unknown", Object.class),
    OBJECT(1, "object", Object.class),
    BOOLEAN(2, "boolean", Boolean.class),
    BYTE(3, "byte", Byte.class),
    INT(4, "int", Integer.class),
    LONG(5, "long", Long.class),
    FLOAT(6, "float", Float.class),
    DOUBLE(7, "double", Double.class),
    TEXT(8, "text", String.class),
    BLOB(9, "blob", Blob.class),
    DATE(10, "date", Date.class),
    UUID(11, "uuid", UUID.class);

    private final byte code;
    private final String name;
    private final Class<?> clazz;

    private static final ImmutableSet<String> SPECIAL_FLOATS = ImmutableSet.of("-Infinity", "Infinity", "NaN");


    static {
        SerialEnum.register(DataType.class);
    }

    DataType(int code, String name, Class<?> clazz) {
        assert code < 256;
        this.code = (byte) code;
        this.name = name;
        this.clazz = clazz;
    }

    @Override
    public byte code() {
        return this.code;
    }

    public String string() {
        return this.name;
    }

    public Class<?> clazz() {
        return this.clazz;
    }

    public boolean isText() {
        return this == DataType.TEXT;
    }

    public boolean isNumber() {
        return this == BYTE || this == INT || this == LONG ||
               this == FLOAT || this == DOUBLE;
    }

    public boolean isNumber4() {
        // Store index value of Byte using 4 bytes
        return this == BYTE || this == INT || this == FLOAT;
    }

    public boolean isNumber8() {
        return this == LONG || this == DOUBLE;
    }

    public boolean isBlob() {
        return this == DataType.BLOB;
    }

    public boolean isDate() {
        return this == DataType.DATE;
    }

    public boolean isUUID() {
        return this == DataType.UUID;
    }

    public <V> Number valueToNumber(V value) {
        if (!(this.isNumber() && value instanceof Number) &&
            !(value instanceof String && SPECIAL_FLOATS.contains(value))) {
            return null;
        }
        if (this.clazz.isInstance(value)) {
            return (Number) value;
        }

        Number number;
        try {
            number = switch (this) {
                case BYTE -> Byte.valueOf(value.toString());
                case INT -> Integer.valueOf(value.toString());
                case LONG -> Long.valueOf(value.toString());
                case FLOAT -> Float.valueOf(value.toString());
                case DOUBLE -> Double.valueOf(value.toString());
                default -> throw new AssertionError(String.format(
                              "Number type only contains Byte, Integer, " +
                              "Long, Float, Double, but got %s", this.clazz()));
            };
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(String.format(
                      "Can't read '%s' as %s: %s",
                      value, this.name, e.getMessage()));
        }
        return number;
    }

    public <V> Date valueToDate(V value) {
        if (!this.isDate()) {
            return null;
        }
        if (value instanceof Date date) {
            return date;
        } else if (value instanceof OffsetDateTime dateTime) {
            return Date.from(dateTime.toInstant());
        } else if (value instanceof Integer number) {
            return new Date(number.intValue());
        } else if (value instanceof Long number) {
            return new Date(number.longValue());
        } else if (value instanceof String string) {
            return DateUtil.parse(string);
        }
        return null;
    }

    public <V> UUID valueToUUID(V value) {
        if (!this.isUUID()) {
            return null;
        }
        if (value instanceof UUID uuid) {
            return uuid;
        } else if (value instanceof String string) {
            return StringEncoding.uuid(string);
        }
        return null;
    }

    public <V> Blob valueToBlob(V value) {
        if (!this.isBlob()) {
            return null;
        }
        if (value instanceof Blob blob) {
            return blob;
        } else if (value instanceof byte[] bytes) {
            return Blob.wrap(bytes);
        } else if (value instanceof ByteBuffer buffer) {
            return Blob.wrap(buffer.array());
        } else if (value instanceof BytesBuffer buffer) {
            return Blob.wrap(buffer.bytes());
        } else if (value instanceof String str) {
            // Only base64 string or hex string accepted
            if (str.startsWith("0x")) {
                return Blob.wrap(Bytes.fromHex(str.substring(2)));
            }
            return Blob.wrap(StringEncoding.decodeBase64(str));
        } else if (value instanceof List<?> values) {
            byte[] bytes = new byte[values.size()];
            for (int i = 0; i < bytes.length; i++) {
                Object v = values.get(i);
                if (v instanceof Byte || v instanceof Integer) {
                    bytes[i] = ((Number) v).byteValue();
                } else {
                    throw new IllegalArgumentException(String.format(
                              "expect byte or int value, but got '%s'", v));
                }
            }
            return Blob.wrap(bytes);
        }
        return null;
    }

    public static DataType fromClass(Class<?> clazz) {
        for (DataType type : DataType.values()) {
            if (type.clazz() == clazz) {
                return type;
            }
        }
        throw new HugeException("Unknown clazz '%s' for DataType", clazz);
    }
}
