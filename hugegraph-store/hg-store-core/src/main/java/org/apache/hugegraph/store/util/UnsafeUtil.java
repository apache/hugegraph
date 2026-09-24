/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hugegraph.store.util;

import java.lang.reflect.Field;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class UnsafeUtil {

    // Static field holding an instance of Unsafe, initialized via getUnsafe0() method
    private static final Object UNSAFE = getUnsafe0();

    // Static field for accessing Unsafe methods through the UnsafeAccessor class
    private static final UnsafeAccessor UNSAFE_ACCESSOR = getUnsafeAccessor0();

    // Offsets used for direct access to specific array and string fields
    private static final long BYTE_ARRAY_BASE_OFFSET = arrayBaseOffset(byte[].class);
    private static final long STRING_VALUE_OFFSET = objectFieldOffset(stringValueField());

    /**
     * Checks if the Unsafe class is available for use.
     *
     * @return true if Unsafe is available, false otherwise.
     */
    public static boolean hasUnsafe() {
        return UNSAFE != null;
    }

    /**
     * Attempts to obtain an instance of sun.misc.Unsafe.
     *
     * @return the Unsafe instance, or null if not available.
     */
    public static Object getUnsafe0() {
        Object unsafe;
        try {
            // Accessing the Unsafe class and its singleton instance
            final Class<?> unsafeClass = Class.forName("sun.misc.Unsafe");
            final Field unsafeField = unsafeClass.getDeclaredField("theUnsafe");
            unsafeField.setAccessible(true); // Bypass access checks
            unsafe = unsafeField.get(null); // Get the Unsafe instance
        } catch (final Throwable t) {
            // Log a warning if Unsafe is not available
            if (log.isWarnEnabled()) {
                log.warn("sun.misc.Unsafe.theUnsafe: unavailable.", t);
            }
            unsafe = null;
        }
        return unsafe; // Return the Unsafe instance or null
    }

    /**
     * Writes a byte value to a specified index in a byte array.
     *
     * @param target the target byte array.
     * @param index  the index to write to.
     * @param value  the byte value to write.
     */
    public static void putByte(final byte[] target, final long index, final byte value) {
        assert UNSAFE_ACCESSOR != null; // Ensure UnsafeAccessor is available
        UNSAFE_ACCESSOR.putByte(target, BYTE_ARRAY_BASE_OFFSET + index, value); // Write the byte
    }

    /**
     * Reads a byte value from a specified index in a byte array.
     *
     * @param target the target byte array.
     * @param index  the index to read from.
     * @return the byte value at the specified index.
     */
    public static byte getByte(final byte[] target, final long index) {
        assert UNSAFE_ACCESSOR != null; // Ensure UnsafeAccessor is available
        return UNSAFE_ACCESSOR.getByte(target, BYTE_ARRAY_BASE_OFFSET + index); // Read the byte
    }

    /**
     * Returns the base offset of the specified array class.
     *
     * @param clazz the class of the array.
     * @return the base offset of the array, or -1 if Unsafe is not available.
     */
    public static int arrayBaseOffset(final Class<?> clazz) {
        if (hasUnsafe()) {
            assert UNSAFE_ACCESSOR != null; // Ensure UnsafeAccessor is available
            return UNSAFE_ACCESSOR.arrayBaseOffset(clazz); // Get the array base offset
        } else {
            return -1; // Return -1 if Unsafe is not available
        }
    }

    /**
     * Creates a String instance from a character array without copying the characters.
     *
     * @param chars the character array.
     * @return the String created from the character array.
     */
    public static String moveToString(final char[] chars) {
        if (STRING_VALUE_OFFSET == -1) {
            // If STRING_VALUE_OFFSET is invalid, create a String by copying characters
            return new String(chars);
        }
        final String str;
        try {
            assert UNSAFE_ACCESSOR != null; // Ensure UnsafeAccessor is available
            // Allocate an instance of String without calling its constructor
            str = (String) UNSAFE_ACCESSOR.allocateInstance(String.class);
        } catch (final InstantiationException e) {
            // Fallback to creating a String by copying characters if allocation fails
            return new String(chars);
        }
        // Directly set the value field of the String to the character array
        UNSAFE_ACCESSOR.putObject(str, STRING_VALUE_OFFSET, chars);
        return str; // Return the created String
    }

    /**
     * Returns the offset of a given field in an object.
     *
     * @param field the field to get the offset for.
     * @return the offset of the field, or -1 if Unsafe is not available.
     */
    public static long objectFieldOffset(final Field field) {
        if (field == null || hasUnsafe()) {
            assert UNSAFE_ACCESSOR != null; // Ensure UnsafeAccessor is available
            return UNSAFE_ACCESSOR.objectFieldOffset(field); // Get the field offset
        } else {
            return -1; // Return -1 if Unsafe is not available
        }
    }

    /**
     * Retrieves the Field object for the "value" field of the String class.
     *
     * @return the Field representing the value field in String.
     */
    private static Field stringValueField() {
        return field(String.class, "value", char[].class); // Get the field for the character array
    }

    /**
     * Gets a declared field from a class by name and checks its type.
     *
     * @param clazz        the class to search in.
     * @param fieldName    the name of the field to retrieve.
     * @param expectedType the expected type of the field.
     * @return the Field object if found and type matches, otherwise null.
     */
    private static Field field(final Class<?> clazz, final String fieldName,
                               final Class<?> expectedType) {
        Field field;
        try {
            field = clazz.getDeclaredField(fieldName); // Get the declared field
            field.setAccessible(true); // Bypass access checks
            if (!field.getType().equals(expectedType)) {
                return null; // Return null if type does not match
            }
        } catch (final Throwable t) {
            // Failed to access the field; return null
            field = null;
        }
        return field; // Return the Field object or null
    }

    /**
     * Initializes the UnsafeAccessor if Unsafe is available.
     *
     * @return an UnsafeAccessor instance if Unsafe is available, null otherwise.
     */
    private static UnsafeAccessor getUnsafeAccessor0() {
        return hasUnsafe() ? new UnsafeAccessor(UNSAFE) : null; // Return UnsafeAccessor
    }

    @Getter
    public static class UnsafeAccessor {

        /**
         * An instance of {@link sun.misc.Unsafe}, which provides low-level, unsafe operations
         * that are generally not available in standard Java.
         */
        private final sun.misc.Unsafe unsafe;

        /**
         * Constructs an instance of {@link UnsafeAccessor} with the specified Unsafe object.
         *
         * @param unsafe the Unsafe instance to be used for accessing low-level operations.
         */
        public UnsafeAccessor(Object unsafe) {
            this.unsafe = (sun.misc.Unsafe) unsafe;
        }

        /**
         * Retrieves a byte value from the specified object at the given memory offset.
         *
         * @param target the object from which to read the byte value.
         * @param offset the memory offset from which to read the byte value.
         * @return the byte value at the specified offset in the target object.
         */
        public byte getByte(final Object target, final long offset) {
            return this.unsafe.getByte(target, offset);
        }

        /**
         * Writes a byte value to the specified object at the given memory offset.
         *
         * @param target the object to which to write the byte value.
         * @param offset the memory offset at which to write the byte value.
         * @param value  the byte value to be written to the target object.
         */
        public void putByte(final Object target, final long offset, final byte value) {
            this.unsafe.putByte(target, offset, value);
        }

        /**
         * Retrieves a short value from the specified object at the given memory offset.
         *
         * @param target the object from which to read the short value.
         * @param offset the memory offset from which to read the short value.
         * @return the short value at the specified offset in the target object.
         */
        public short getShort(final Object target, final long offset) {
            return this.unsafe.getShort(target, offset);
        }

        /**
         * Writes a short value to the specified object at the given memory offset.
         *
         * @param target the object to which to write the short value.
         * @param offset the memory offset at which to write the short value.
         * @param value  the short value to be written to the target object.
         */
        public void putShort(final Object target, final long offset, final short value) {
            this.unsafe.putShort(target, offset, value);
        }

        /**
         * Retrieves an integer value from the specified object at the given memory offset.
         *
         * @param target the object from which to read the integer value.
         * @param offset the memory offset from which to read the integer value.
         * @return the integer value at the specified offset in the target object.
         */
        public int getInt(final Object target, final long offset) {
            return this.unsafe.getInt(target, offset);
        }

        /**
         * Writes an integer value to the specified object at the given memory offset.
         *
         * @param target the object to which to write the integer value.
         * @param offset the memory offset at which to write the integer value.
         * @param value  the integer value to be written to the target object.
         */
        public void putInt(final Object target, final long offset, final int value) {
            this.unsafe.putInt(target, offset, value);
        }

        /**
         * Retrieves a long value from the specified object at the given memory offset.
         *
         * @param target the object from which to read the long value.
         * @param offset the memory offset from which to read the long value.
         * @return the long value at the specified offset in the target object.
         */
        public long getLong(final Object target, final long offset) {
            return this.unsafe.getLong(target, offset);
        }

        /**
         * Writes a long value to the specified object at the given memory offset.
         *
         * @param target the object to which to write the long value.
         * @param offset the memory offset at which to write the long value.
         * @param value  the long value to be written to the target object.
         */
        public void putLong(final Object target, final long offset, final long value) {
            this.unsafe.putLong(target, offset, value);
        }

        /**
         * Retrieves a boolean value from the specified object at the given memory offset.
         *
         * @param target the object from which to read the boolean value.
         * @param offset the memory offset from which to read the boolean value.
         * @return the boolean value at the specified offset in the target object.
         */
        public boolean getBoolean(final Object target, final long offset) {
            return this.unsafe.getBoolean(target, offset);
        }

        /**
         * Writes a boolean value to the specified object at the given memory offset.
         *
         * @param target the object to which to write the boolean value.
         * @param offset the memory offset at which to write the boolean value.
         * @param value  the boolean value to be written to the target object.
         */
        public void putBoolean(final Object target, final long offset, final boolean value) {
            this.unsafe.putBoolean(target, offset, value);
        }

        /**
         * Retrieves a float value from the specified object at the given memory offset.
         *
         * @param target the object from which to read the float value.
         * @param offset the memory offset from which to read the float value.
         * @return the float value at the specified offset in the target object.
         */
        public float getFloat(final Object target, final long offset) {
            return this.unsafe.getFloat(target, offset);
        }

        /**
         * Writes a float value to the specified object at the given memory offset.
         *
         * @param target the object to which to write the float value.
         * @param offset the memory offset at which to write the float value.
         * @param value  the float value to be written to the target object.
         */
        public void putFloat(final Object target, final long offset, final float value) {
            this.unsafe.putFloat(target, offset, value);
        }

        /**
         * Retrieves a double value from the specified object at the given memory offset.
         *
         * @param target the object from which to read the double value.
         * @param offset the memory offset from which to read the double value.
         * @return the double value at the specified offset in the target object.
         */
        public double getDouble(final Object target, final long offset) {
            return this.unsafe.getDouble(target, offset);
        }

        /**
         * Writes a double value to the specified object at the given memory offset.
         *
         * @param target the object to which to write the double value.
         * @param offset the memory offset at which to write the double value.
         * @param value  the double value to be written to the target object.
         */
        public void putDouble(final Object target, final long offset, final double value) {
            this.unsafe.putDouble(target, offset, value);
        }

        /**
         * Retrieves an object reference from the specified object at the given memory offset.
         *
         * @param target the object from which to read the object reference.
         * @param offset the memory offset from which to read the object reference.
         * @return the object reference at the specified offset in the target object.
         */
        public Object getObject(final Object target, final long offset) {
            return this.unsafe.getObject(target, offset);
        }

        /**
         * Writes an object reference to the specified object at the given memory offset.
         *
         * @param target the object to which to write the object reference.
         * @param offset the memory offset at which to write the object reference.
         * @param value  the object reference to be written to the target object.
         */
        public void putObject(final Object target, final long offset, final Object value) {
            this.unsafe.putObject(target, offset, value);
        }

        /**
         * Retrieves a byte value from a specific memory address.
         *
         * @param address the memory address from which to read the byte value.
         * @return the byte value at the specified memory address.
         */
        public byte getByte(final long address) {
            return this.unsafe.getByte(address);
        }

        /**
         * Writes a byte value to a specific memory address.
         *
         * @param address the memory address at which to write the byte value.
         * @param value   the byte value to be written to the specified memory address.
         */
        public void putByte(final long address, final byte value) {
            this.unsafe.putByte(address, value);
        }

        /**
         * Retrieves a short value from a specific memory address.
         *
         * @param address the memory address from which to read the short value.
         * @return the short value at the specified memory address.
         */
        public short getShort(final long address) {
            return this.unsafe.getShort(address);
        }

        /**
         * Writes a short value to a specific memory address.
         *
         * @param address the memory address at which to write the short value.
         * @param value   the short value to be written to the specified memory address.
         */
        public void putShort(final long address, final short value) {
            this.unsafe.putShort(address, value);
        }

        /**
         * Retrieves an integer value from a specific memory address.
         *
         * @param address the memory address from which to read the integer value.
         * @return the integer value at the specified memory address.
         */
        public int getInt(final long address) {
            return this.unsafe.getInt(address);
        }

        /**
         * Writes an integer value to a specific memory address.
         *
         * @param address the memory address at which to write the integer value.
         * @param value   the integer value to be written to the specified memory address.
         */
        public void putInt(final long address, final int value) {
            this.unsafe.putInt(address, value);
        }

        /**
         * Retrieves a long value from a specific memory address.
         *
         * @param address the memory address from which to read the long value.
         * @return the long value at the specified memory address.
         */
        public long getLong(final long address) {
            return this.unsafe.getLong(address);
        }

        /**
         * Writes a long value to a specific memory address.
         *
         * @param address the memory address at which to write the long value.
         * @param value   the long value to be written to the specified memory address.
         */
        public void putLong(final long address, final long value) {
            this.unsafe.putLong(address, value);
        }

        /**
         * Copies a block of memory from one location to another.
         *
         * @param srcBase   the source object from which to copy memory.
         * @param srcOffset the offset in the source object from which to start copying.
         * @param dstBase   the destination object to which to copy memory.
         * @param dstOffset the offset in the destination object at which to start writing.
         * @param bytes     the number of bytes to copy.
         */
        public void copyMemory(final Object srcBase, final long srcOffset, final Object dstBase,
                               final long dstOffset,
                               final long bytes) {
            this.unsafe.copyMemory(srcBase, srcOffset, dstBase, dstOffset, bytes);
        }

        /**
         * Copies a block of memory from one address to another.
         *
         * @param srcAddress the source memory address from which to copy.
         * @param dstAddress the destination memory address to which to copy.
         * @param bytes      the number of bytes to copy.
         */
        public void copyMemory(final long srcAddress, final long dstAddress, final long bytes) {
            this.unsafe.copyMemory(srcAddress, dstAddress, bytes);
        }

        /**
         * Retrieves a volatile byte value from the specified object at the given memory offset.
         *
         * @param target the object from which to read the volatile byte value.
         * @param offset the memory offset from which to read the volatile byte value.
         * @return the volatile byte value at the specified offset in the target object.
         */
        public byte getByteVolatile(final Object target, final long offset) {
            return this.unsafe.getByteVolatile(target, offset);
        }

        /**
         * Writes a volatile byte value to the specified object at the given memory offset.
         *
         * @param target the object to which to write the volatile byte value.
         * @param offset the memory offset at which to write the volatile byte value.
         * @param value  the volatile byte value to be written to the target object.
         */
        public void putByteVolatile(final Object target, final long offset, final byte value) {
            this.unsafe.putByteVolatile(target, offset, value);
        }

        /**
         * Retrieves a volatile short value from the specified object at the given memory offset.
         *
         * @param target the object from which to read the volatile short value.
         * @param offset the memory offset from which to read the volatile short value.
         * @return the volatile short value at the specified offset in the target object.
         */
        public short getShortVolatile(final Object target, final long offset) {
            return this.unsafe.getShortVolatile(target, offset);
        }

        /**
         * Writes a volatile short value to the specified object at the given memory offset.
         *
         * @param target the object to which to write the volatile short value.
         * @param offset the memory offset at which to write the volatile short value.
         * @param value  the volatile short value to be written to the target object.
         */
        public void putShortVolatile(final Object target, final long offset, final short value) {
            this.unsafe.putShortVolatile(target, offset, value);
        }

        /**
         * Retrieves a volatile integer value from the specified object at the given memory offset.
         *
         * @param target the object from which to read the volatile integer value.
         * @param offset the memory offset from which to read the volatile integer value.
         * @return the volatile integer value at the specified offset in the target object.
         */
        public int getIntVolatile(final Object target, final long offset) {
            return this.unsafe.getIntVolatile(target, offset);
        }

        /**
         * Writes a volatile integer value to the specified object at the given memory offset.
         *
         * @param target the object to which to write the volatile integer value.
         * @param offset the memory offset at which to write the volatile integer value.
         * @param value  the volatile integer value to be written to the target object.
         */
        public void putIntVolatile(final Object target, final long offset, final int value) {
            this.unsafe.putIntVolatile(target, offset, value);
        }

        /**
         * Retrieves a volatile long value from the specified object at the given memory offset.
         *
         * @param target the object from which to read the volatile long value.
         * @param offset the memory offset from which to read the volatile long value.
         * @return the volatile long value at the specified offset in the target object.
         */
        public long getLongVolatile(final Object target, final long offset) {
            return this.unsafe.getLongVolatile(target, offset);
        }

        /**
         * Writes a volatile long value to the specified object at the given memory offset.
         *
         * @param target the object to which to write the volatile long value.
         * @param offset the memory offset at which to write the volatile long value.
         * @param value  the volatile long value to be written to the target object.
         */
        public void putLongVolatile(final Object target, final long offset, final long value) {
            this.unsafe.putLongVolatile(target, offset, value);
        }

        /**
         * Retrieves a volatile boolean value from the specified object at the given memory offset.
         *
         * @param target the object from which to read the volatile boolean value.
         * @param offset the memory offset from which to read the volatile boolean value.
         * @return the volatile boolean value at the specified offset in the target object.
         */
        public boolean getBooleanVolatile(final Object target, final long offset) {
            return this.unsafe.getBooleanVolatile(target, offset);
        }

        /**
         * Writes a volatile boolean value to the specified object at the given memory offset.
         *
         * @param target the object to which to write the volatile boolean value.
         * @param offset the memory offset at which to write the volatile boolean value.
         * @param value  the volatile boolean value to be written to the target object.
         */
        public void putBooleanVolatile(final Object target, final long offset,
                                       final boolean value) {
            this.unsafe.putBooleanVolatile(target, offset, value);
        }

        /**
         * Retrieves a volatile float value from the specified object at the given memory offset.
         *
         * @param target the object from which to read the volatile float value.
         * @param offset the memory offset from which to read the volatile float value.
         * @return the volatile float value at the specified offset in the target object.
         */
        public float getFloatVolatile(final Object target, final long offset) {
            return this.unsafe.getFloatVolatile(target, offset);
        }

        /**
         * Writes a volatile float value to the specified object at the given memory offset.
         *
         * @param target the object to which to write the volatile float value.
         * @param offset the memory offset at which to write the volatile float value.
         * @param value  the volatile float value to be written to the target object.
         */
        public void putFloatVolatile(final Object target, final long offset, final float value) {
            this.unsafe.putFloatVolatile(target, offset, value);
        }

        /**
         * Retrieves a volatile double value from the specified object at the given memory offset.
         *
         * @param target the object from which to read the volatile double value.
         * @param offset the memory offset from which to read the volatile double value.
         * @return the volatile double value at the specified offset in the target object.
         */
        public double getDoubleVolatile(final Object target, final long offset) {
            return this.unsafe.getDoubleVolatile(target, offset);
        }

        /**
         * Writes a volatile double value to the specified object at the given memory offset.
         *
         * @param target the object to which to write the volatile double value.
         * @param offset the memory offset at which to write the volatile double value.
         * @param value  the volatile double value to be written to the target object.
         */
        public void putDoubleVolatile(final Object target, final long offset, final double value) {
            this.unsafe.putDoubleVolatile(target, offset, value);
        }

        /**
         * Retrieves a volatile object reference from the specified object at the given memory
         * offset.
         *
         * @param target the object from which to read the volatile object reference.
         * @param offset the memory offset from which to read the volatile object reference.
         * @return the volatile object reference at the specified offset in the target object.
         */
        public Object getObjectVolatile(final Object target, final long offset) {
            return this.unsafe.getObjectVolatile(target, offset);
        }

        /**
         * Writes a volatile object reference to the specified object at the given memory offset.
         *
         * @param target the object to which to write the volatile object reference.
         * @param offset the memory offset at which to write the volatile object reference.
         * @param value  the volatile object reference to be written to the target object.
         */
        public void putObjectVolatile(final Object target, final long offset, final Object value) {
            this.unsafe.putObjectVolatile(target, offset, value);
        }

        /**
         * Reports the offset of the first element in the storage allocation of a given array class.
         *
         * @param clazz the class of the array for which to report the base offset.
         * @return the offset of the first element in the storage allocation of the given array
         * class.
         */
        public int arrayBaseOffset(final Class<?> clazz) {
            return this.unsafe != null ? this.unsafe.arrayBaseOffset(clazz) : -1;
        }

        /**
         * Reports the scale factor for addressing elements in the storage allocation of a given
         * array class.
         *
         * @param clazz the class of the array for which to report the index scale.
         * @return the scale factor for addressing elements in the storage allocation of the
         * given array class.
         */
        public int arrayIndexScale(final Class<?> clazz) {
            return this.unsafe != null ? this.unsafe.arrayIndexScale(clazz) : -1;
        }

        /**
         * Returns the offset of the provided field, or {@code -1} if {@code sun.misc.Unsafe} is
         * not available.
         *
         * @param field the field for which to get the offset.
         * @return the offset of the provided field, or {@code -1} if {@code sun.misc.Unsafe} is
         * not available.
         */
        public long objectFieldOffset(final Field field) {
            return field == null || this.unsafe == null ? -1 : this.unsafe.objectFieldOffset(field);
        }

        /**
         * Allocates a new instance of the specified class without invoking any constructor.
         *
         * @param clazz the class for which to allocate an instance.
         * @return a new instance of the specified class.
         * @throws InstantiationException if the allocation fails.
         */
        public Object allocateInstance(final Class<?> clazz) throws InstantiationException {
            return this.unsafe.allocateInstance(clazz);
        }

        /**
         * Throws the specified throwable as an exception.
         *
         * @param t the throwable to be thrown as an exception.
         */
        public void throwException(final Throwable t) {
            this.unsafe.throwException(t);
        }
    }
}
