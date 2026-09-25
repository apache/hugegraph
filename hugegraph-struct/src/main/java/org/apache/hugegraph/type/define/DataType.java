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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
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
    UUID(11, "uuid", UUID.class),
    /*
     * Arbitrary-precision decimal (java.math.BigDecimal), see the server copy
     * of this enum: exact, but not a sort key / index / OLAP range type.
     */
    DECIMAL(12, "decimal", BigDecimal.class);

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

    public boolean isDecimal() {
        return this == DataType.DECIMAL;
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
            switch (this) {
                case BYTE:
                    number = Byte.valueOf(value.toString());
                    break;
                case INT:
                    number = Integer.valueOf(value.toString());
                    break;
                case LONG:
                    number = Long.valueOf(value.toString());
                    break;
                case FLOAT:
                    number = Float.valueOf(value.toString());
                    break;
                case DOUBLE:
                    number = Double.valueOf(value.toString());
                    break;
                default:
                    throw new AssertionError(String.format(
                              "Number type only contains Byte, Integer, " +
                              "Long, Float, Double, but got %s", this.clazz()));
            }
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(String.format(
                      "Can't read '%s' as %s: %s",
                      value, this.name, e.getMessage()));
        }
        return number;
    }

    /*
     * Bounds for a DECIMAL value: at most DECIMAL_MAX_PRECISION significant
     * digits and an absolute scale of at most DECIMAL_MAX_SCALE. uint256
     * with 18 fraction digits is 96 digits, so both fit with room to spare,
     * while "1E+999999999" (a few bytes on disk, a billion characters from
     * toPlainString() on every read) is rejected before it is stored.
     */
    public static final int DECIMAL_MAX_PRECISION = 128;
    public static final int DECIMAL_MAX_SCALE = 128;

    public <V> BigDecimal valueToDecimal(V value) {
        if (!this.isDecimal()) {
            return null;
        }
        BigDecimal decimal;
        if (value instanceof BigDecimal) {
            decimal = (BigDecimal) value;
        } else if (value instanceof BigInteger) {
            decimal = new BigDecimal((BigInteger) value);
        } else if (value instanceof Byte || value instanceof Short ||
                   value instanceof Integer || value instanceof Long) {
            decimal = BigDecimal.valueOf(((Number) value).longValue());
        } else if (!(value instanceof Number) && !(value instanceof String)) {
            return null;
        } else {
            String text = value.toString().trim();
            try {
                decimal = new BigDecimal(text);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(String.format(
                        "Can't read '%s' as decimal", value));
            }
        }
        return checkDecimalBounds(decimal);
    }

    public static BigDecimal checkDecimalBounds(BigDecimal decimal) {
        int scale = Math.abs(decimal.scale());
        int precision = decimal.precision();
        if (precision > DECIMAL_MAX_PRECISION || scale > DECIMAL_MAX_SCALE) {
            throw new IllegalArgumentException(String.format(
                    "Decimal value out of bounds: precision %d, scale %d " +
                    "(at most %d significant digits and a scale of at most " +
                    "%d in either direction)", precision, decimal.scale(),
                    DECIMAL_MAX_PRECISION, DECIMAL_MAX_SCALE));
        }
        return decimal;
    }

    public <V> Date valueToDate(V value) {
        if (!this.isDate()) {
            return null;
        }
        if (value instanceof Date) {
            return (Date) value;
        } else if (value instanceof Integer) {
            return new Date(((Number) value).intValue());
        } else if (value instanceof Long) {
            return new Date(((Number) value).longValue());
        } else if (value instanceof String) {
            return DateUtil.parse((String) value);
        }
        return null;
    }

    public <V> UUID valueToUUID(V value) {
        if (!this.isUUID()) {
            return null;
        }
        if (value instanceof UUID) {
            return (UUID) value;
        } else if (value instanceof String) {
            return StringEncoding.uuid((String) value);
        }
        return null;
    }

    public <V> Blob valueToBlob(V value) {
        if (!this.isBlob()) {
            return null;
        }
        if (value instanceof Blob) {
            return (Blob) value;
        } else if (value instanceof byte[]) {
            return Blob.wrap((byte[]) value);
        } else if (value instanceof ByteBuffer) {
            return Blob.wrap(((ByteBuffer) value).array());
        } else if (value instanceof BytesBuffer) {
            return Blob.wrap(((BytesBuffer) value).bytes());
        } else if (value instanceof String) {
            // Only base64 string or hex string accepted
            String str = ((String) value);
            if (str.startsWith("0x")) {
                return Blob.wrap(Bytes.fromHex(str.substring(2)));
            }
            return Blob.wrap(StringEncoding.decodeBase64(str));
        } else if (value instanceof List) {
            List<?> values = (List<?>) value;
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
