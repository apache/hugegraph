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

package org.apache.hugegraph.util;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.function.Function;

/**
 * This file is copied verbatim from Apache Lucene NumericUtils.java
 * Only the double/float to sortable long/int conversions are retained.
 */
public final class NumericUtil {

    private static final long FULL_LONG = Long.MIN_VALUE;
    private static final int FULL_INT = Integer.MIN_VALUE;
    private static final byte FULL_BYTE = Byte.MIN_VALUE;

    private NumericUtil() {
    }

    /**
     * Converts a <code>double</code> value to a sortable signed
     * <code>long</code>. The value is converted by getting their IEEE 754
     * floating-point &quot;double format&quot; bit layout and then some bits
     * are swapped, to be able to compare the result as long. By this the
     * precision is not reduced, but the value can be easily used as a long. The
     * sort order (including {@link Double#NaN}) is defined by
     * {@link Double#compareTo}; {@code NaN} is greater than positive infinity.
     * @param value input double value
     * @return output sortable long value
     * @see #sortableLongToDouble
     */
    public static long doubleToSortableLong(double value) {
        return sortableDoubleBits(Double.doubleToLongBits(value));
    }

    /**
     * Converts a sortable <code>long</code> back to a <code>double</code>.
     * @param value input double value
     * @return output sortable long value
     * @see #doubleToSortableLong
     */
    public static double sortableLongToDouble(long value) {
        return Double.longBitsToDouble(sortableDoubleBits(value));
    }

    /**
     * Converts a <code>float</code> value to a sortable signed
     * <code>int</code>. The value is converted by getting their IEEE 754
     * floating-point &quot;float format&quot; bit layout and then some bits are
     * swapped, to be able to compare the result as int. By this the precision
     * is not reduced, but the value can be easily used as an int. The sort order
     * (including {@link Float#NaN}) is defined by {@link Float#compareTo};
     * {@code NaN} is greater than positive infinity.
     * @param value input float value
     * @return output sortable int value
     * @see #sortableIntToFloat
     */
    public static int floatToSortableInt(float value) {
        return sortableFloatBits(Float.floatToIntBits(value));
    }

    /**
     * Converts a sortable <code>int</code> back to a <code>float</code>.
     * @param value input int value
     * @return output sortable float value
     * @see #floatToSortableInt
     */
    public static float sortableIntToFloat(int value) {
        return Float.intBitsToFloat(sortableFloatBits(value));
    }

    /**
     * Converts IEEE 754 representation of a double to sortable order (or back
     * to the original)
     * @param bits The long format of a double value
     * @return The sortable long value
     */
    public static long sortableDoubleBits(long bits) {
        return bits ^ ((bits >> 63) & 0x7fffffffffffffffL);
    }

    /**
     * Converts IEEE 754 representation of a float to sortable order (or back to
     * the original)
     * @param bits The int format of a float value
     * @return The sortable int value
     */
    public static int sortableFloatBits(int bits) {
        /*
         * Convert to its inverse digits if negative else keep the origin
         * NOTE: (bits >> 31) is 0x00000000 if bits >= 0
         *       (bits >> 31) is 0xFFFFFFFF if bits < 0
         */
        return bits ^ ((bits >> 31) & 0x7fffffff);
    }

    public static long numberToSortableLong(Number number) {
        if (number instanceof Double) {
            return doubleToSortableLong(number.doubleValue());
        } else if (number instanceof Float) {
            return floatToSortableInt(number.floatValue());
        } else if (number instanceof Long || number instanceof Integer ||
                   number instanceof Short || number instanceof Byte) {
            return number.longValue();
        } else if (number instanceof BigDecimal) {
            BigDecimal bd = (BigDecimal) number;
            boolean intNumber = bd.stripTrailingZeros().scale() <= 0;
            return intNumber ? bd.longValueExact() :
                   doubleToSortableLong(bd.doubleValue());
        }

        // TODO: support other number types
        throw unsupportedNumberType(number);
    }

    public static Number sortableLongToNumber(long value, Class<?> clazz) {
        assert NumericUtil.isNumber(clazz);

        if (clazz == Double.class) {
            return sortableLongToDouble(value);
        } else if (clazz == Float.class) {
            return sortableIntToFloat((int) value);
        } else if (clazz == Long.class) {
            return value;
        } else if (clazz == Integer.class) {
            return (int) value;
        } else if (clazz == Short.class) {
            return (short) value;
        } else if (clazz == Byte.class) {
            return (byte) value;
        }

        // TODO: support other number types
        throw unsupportedNumberType(clazz);
    }

    public static byte[] numberToSortableBytes(Number number) {
        if (number instanceof Long) {
            return longToSortableBytes(number.longValue());
        } else if (number instanceof Double) {
            long value = doubleToSortableLong(number.doubleValue());
            return longToSortableBytes(value);
        } else if (number instanceof Float) {
            int value = floatToSortableInt(number.floatValue());
            return intToSortableBytes(value);
        } else if (number instanceof Integer || number instanceof Short) {
            return intToSortableBytes(number.intValue());
        } else if (number instanceof Byte) {
            return byteToSortableBytes(number.byteValue());
        }

        // TODO: support other number types
        throw unsupportedNumberType(number);
    }

    public static Number sortableBytesToNumber(byte[] bytes, Class<?> clazz) {
        assert NumericUtil.isNumber(clazz);

        if (clazz == Long.class) {
            return sortableBytesToLong(bytes);
        } else if (clazz == Double.class) {
            return sortableLongToDouble(sortableBytesToLong(bytes));
        } else if (clazz == Float.class) {
            return sortableIntToFloat(sortableBytesToInt(bytes));
        } else if (clazz == Integer.class) {
            return sortableBytesToInt(bytes);
        } else if (clazz == Short.class) {
            return (short) sortableBytesToInt(bytes);
        } else if (clazz == Byte.class) {
            return sortableBytesToByte(bytes);
        }

        // TODO: support other number types
        throw unsupportedNumberType(clazz);
    }

    public static Number minValueOf(Class<?> clazz) {
        E.checkArgumentNotNull(clazz, "The clazz can't be null");

        if (Long.class.isAssignableFrom(clazz) ||
            Double.class.isAssignableFrom(clazz)) {
            return Long.MIN_VALUE;
        }
        if (Integer.class.isAssignableFrom(clazz) ||
            Short.class.isAssignableFrom(clazz) ||
            Float.class.isAssignableFrom(clazz)) {
            return Integer.MIN_VALUE;
        }
        if (Byte.class.isAssignableFrom(clazz)) {
            return Byte.MIN_VALUE;
        }

        // TODO: support other number types
        throw unsupportedNumberType(clazz);
    }

    public static Number maxValueOf(Class<?> clazz) {
        E.checkArgumentNotNull(clazz, "The clazz can't be null");

        if (Long.class.isAssignableFrom(clazz) ||
            Double.class.isAssignableFrom(clazz)) {
            return Long.MAX_VALUE;
        }
        if (Integer.class.isAssignableFrom(clazz) ||
            Float.class.isAssignableFrom(clazz) ||
            Short.class.isAssignableFrom(clazz)) {
            return Integer.MAX_VALUE;
        }
        if (Byte.class.isAssignableFrom(clazz)) {
            return Byte.MAX_VALUE;
        }

        // TODO: support other number types
        throw unsupportedNumberType(clazz);
    }

    public static byte[] longToSortableBytes(long value) {
        return longToBytes(value + FULL_LONG);
    }

    public static long sortableBytesToLong(byte[] bytes) {
        return bytesToLong(bytes) - FULL_LONG;
    }

    public static byte[] intToSortableBytes(int value) {
        return intToBytes(value + FULL_INT);
    }

    public static int sortableBytesToInt(byte[] bytes) {
        return bytesToInt(bytes) - FULL_INT;
    }

    public static byte[] byteToSortableBytes(byte value) {
        value += FULL_BYTE;
        return new byte[]{value};
    }

    public static byte sortableBytesToByte(byte[] bytes) {
        assert bytes.length == 1;
        byte value = bytes[0];
        value -= FULL_BYTE;
        return value;
    }

    public static byte[] longToBytes(long value) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(value);
        return buffer.array();
    }

    public static long bytesToLong(byte[] bytes) {
        assert bytes.length == Long.BYTES;
        return ByteBuffer.wrap(bytes).getLong();
    }

    public static byte[] intToBytes(int value) {
        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
        buffer.putInt(value);
        return buffer.array();
    }

    public static int bytesToInt(byte[] bytes) {
        assert bytes.length == Integer.BYTES;
        return ByteBuffer.wrap(bytes).getInt();
    }

    public static boolean isNumber(Object value) {
        if (value == null) {
            return false;
        }
        return isNumber(value.getClass());
    }

    public static boolean isNumber(Class<?> clazz) {
        if (clazz.isPrimitive()) {
            return clazz == int.class || clazz == long.class ||
                   clazz == float.class || clazz == double.class ||
                   clazz == short.class || clazz == byte.class;
        }
        return Number.class.isAssignableFrom(clazz);
    }

    public static Number convertToNumber(Object value) {
        if (value == null) {
            return null;
        }

        Number number;
        if (isNumber(value)) {
            number = (Number) value;
        } else {
            if (value instanceof Date) {
                number = ((Date) value).getTime();
            } else {
                // TODO: add some more types to convert
                number = new BigDecimal(value.toString());
            }
        }
        return number;
    }

    /**
     * Compare object with a number, the object should be a number,
     * or it can be converted to a BigDecimal
     * @param first   might be number or string
     * @param second  must be number
     * @return  0 if first is numerically equal to second;
     *          a negative int if first is numerically less than second;
     *          a positive int if first is numerically greater than second.
     */
    public static int compareNumber(Object first, Number second) {
        E.checkArgument(first != null, "The first parameter can't be null");
        E.checkArgument(second != null, "The second parameter can't be null");

        if (first instanceof Number && first instanceof Comparable &&
            first.getClass().equals(second.getClass())) {
            @SuppressWarnings("unchecked")
            Comparable<Number> cmpFirst = (Comparable<Number>) first;
            return cmpFirst.compareTo(second);
        }

        Function<Object, BigDecimal> toBig = (number) -> {
            try {
                return new BigDecimal(number.toString());
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(String.format(
                          "Can't compare between '%s' and '%s', " +
                          "they must be numbers", first, second));
            }
        };

        BigDecimal n1 = toBig.apply(first);
        BigDecimal n2 = toBig.apply(second);

        return n1.compareTo(n2);
    }

    private static IllegalArgumentException unsupportedNumberType(Class<?> c) {
        return new IllegalArgumentException(String.format(
                   "Unsupported number type: %s", c.getSimpleName()));
    }

    private static IllegalArgumentException unsupportedNumberType(Number num) {
        return new IllegalArgumentException(String.format(
                   "Unsupported number type: %s(%s)",
                   num.getClass().getSimpleName(), num));
    }
}
