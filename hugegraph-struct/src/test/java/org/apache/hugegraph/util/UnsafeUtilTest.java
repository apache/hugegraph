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

package org.apache.hugegraph.util;

import org.apache.hugegraph.util.collection.IntSet;
import org.junit.Assert;
import org.junit.Test;

import sun.misc.Unsafe;

public class UnsafeUtilTest {

    @Test
    public void testCollectionsShareUnsafeInstance() {
        Assert.assertSame(UnsafeUtil.getUnsafe(), IntSet.UNSAFE);
    }

    @Test
    public void testNativeMemoryReadWrite() {
        Unsafe unsafe = UnsafeUtil.getUnsafe();
        long address = unsafe.allocateMemory(Long.BYTES);
        try {
            unsafe.putLong(address, 0x0123456789abcdefL);
            Assert.assertEquals(0x0123456789abcdefL, unsafe.getLong(address));
        } finally {
            unsafe.freeMemory(address);
        }
    }
}
