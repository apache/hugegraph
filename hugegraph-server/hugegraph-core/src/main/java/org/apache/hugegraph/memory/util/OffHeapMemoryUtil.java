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

package org.apache.hugegraph.memory.util;

import java.lang.reflect.InvocationTargetException;

import org.apache.hugegraph.util.UnsafeUtil;

import sun.misc.Unsafe;

// NOTE: due to different layout of klass in various versions of JDK, this class may easily crash!
public class OffHeapMemoryUtil {

    private static final Unsafe UNSAFE = UnsafeUtil.getUnsafe();

    public static long allocateObjectToOffHeap(Object obj) {
        long size = sizeOf(obj);
        System.out.println(size);
        long address = getUnsafe().allocateMemory(size);
        // test object was copied to off-heap
        getUnsafe().copyMemory(
                obj,            // source object
                0,              // source offset is zero - copy an entire object
                null,
                // destination is specified by absolute address, so destination object is null
                address,        // destination address
                size
        );
        return address;
    }

    public static <T> T retrieveObjectFromOffHeap(Class<T> clazz, long address) throws
                                                                                NoSuchFieldException,
                                                                                NoSuchMethodException,
                                                                                InstantiationException,
                                                                                IllegalAccessException,
                                                                                InvocationTargetException {
        T pointer = clazz.getDeclaredConstructor().newInstance();
        // pointer is just a handler that stores address of some object
        // NOTE: we agree that all pointer classes must have a pointer field.
        long offset = getUnsafe().objectFieldOffset(clazz.getDeclaredField("pointer"));
        // set pointer to off-heap copy of the test object
        getUnsafe().putLong(pointer, offset, address);
        return pointer;
    }

    public static void freeOffHeapMemory(long address) {
        getUnsafe().freeMemory(address);
    }

    private static Unsafe getUnsafe() {
        return UNSAFE;
    }

    private static long sizeOf(Object object) {
        Unsafe unsafe = getUnsafe();
        return unsafe.getAddress(normalize(unsafe.getInt(object, 4L)) + 8L);
    }

    private static long normalize(int value) {
        if (value >= 0) return value;
        return (~0L >>> 32) & value;
    }
}
