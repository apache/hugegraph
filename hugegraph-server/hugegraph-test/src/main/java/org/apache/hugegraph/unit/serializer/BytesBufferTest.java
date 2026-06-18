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

package org.apache.hugegraph.unit.serializer;

import java.awt.Point;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.TimeZone;
import java.util.UUID;

import org.apache.hugegraph.HugeFactory;
import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.id.IdGenerator;
import org.apache.hugegraph.backend.id.IdGenerator.UuidId;
import org.apache.hugegraph.backend.serializer.BytesBuffer;
import org.apache.hugegraph.config.CoreOptions;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.schema.PropertyKey;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.type.define.Cardinality;
import org.apache.hugegraph.type.define.DataType;
import org.apache.hugegraph.unit.BaseUnitTest;
import org.apache.hugegraph.unit.FakeObjects;
import org.apache.hugegraph.util.Blob;
import org.apache.hugegraph.util.LZ4Util;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

public class BytesBufferTest extends BaseUnitTest {

    @Before
    public void setUp() throws Exception {
        resetMaxBufferCapacity();
    }

    @After
    public void tearDown() throws Exception {
        resetMaxBufferCapacity();
    }

    private static void resetMaxBufferCapacity() throws Exception {
        Field field = BytesBuffer.class.getDeclaredField("maxBufferCapacity");
        field.setAccessible(true);
        field.set(null, null);
    }

    @Test
    public void testAllocate() {
        Assert.assertEquals(0, BytesBuffer.allocate(0).array().length);
        Assert.assertEquals(0, BytesBuffer.allocate(0).bytes().length);

        Assert.assertEquals(4, BytesBuffer.allocate(4).array().length);
        Assert.assertEquals(0, BytesBuffer.allocate(4).bytes().length);

        BytesBuffer buf4 = BytesBuffer.allocate(4);
        buf4.write(new byte[4]);
        Assert.assertArrayEquals(new byte[]{0, 0, 0, 0},
                                 buf4.bytes());

        BytesBuffer buf2 = BytesBuffer.allocate(2);
        buf2.write(new byte[4]);
        Assert.assertArrayEquals(new byte[]{0, 0, 0, 0},
                                 buf2.bytes());

        BytesBuffer buf0 = BytesBuffer.allocate(0);
        buf0.write(new byte[4]);
        Assert.assertArrayEquals(new byte[]{0, 0, 0, 0},
                                 buf0.bytes());
    }

    @Test
    public void testAllocateWithMaxBufferCapacity() {
        Assert.assertEquals(BytesBuffer.MAX_BUFFER_CAPACITY,
                            BytesBuffer.maxBufferCapacity());

        BytesBuffer.initMaxBufferCapacity(128);
        Assert.assertEquals(128, BytesBuffer.maxBufferCapacity());
        BytesBuffer.initMaxBufferCapacity(128);
        Assert.assertEquals(128, BytesBuffer.maxBufferCapacity());
        Assert.assertEquals(128, BytesBuffer.allocate(128).array().length);

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            BytesBuffer.allocate(129);
        }, e -> {
            Assert.assertContains("Capacity 129 exceeds max buffer " +
                                  "capacity: 128",
                                  e.getMessage());
        });
    }

    @Test
    public void testResizeWithMaxBufferCapacity() {
        BytesBuffer.initMaxBufferCapacity(128);
        BytesBuffer buffer = BytesBuffer.allocate(64);
        buffer.write(new byte[64]);
        buffer.write((byte) 1);
        Assert.assertEquals(65, buffer.bytes().length);
        Assert.assertEquals(128, buffer.array().length);

        buffer.write(new byte[63]);

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            buffer.write((byte) 1);
        }, e -> {
            Assert.assertContains("Capacity 129 exceeds max buffer " +
                                  "capacity: 128",
                                  e.getMessage());
        });
    }

    @Test
    public void testResizeCappedAtMaxBufferCapacity() {
        BytesBuffer.initMaxBufferCapacity(128);

        // Allocate small buffer and fill it completely
        BytesBuffer buffer = BytesBuffer.allocate(64);
        buffer.write(new byte[64]);

        // Writing 2 more bytes triggers resize:
        //   requiredCapacity = 64 + 2 = 66
        //   naive newCapacity = 66 + DEFAULT_CAPACITY(64) = 130
        //   Math.min(130, maxCapacity=128) = 128, capped
        buffer.write(new byte[2]);
        Assert.assertEquals(128, buffer.array().length);
        Assert.assertEquals(66, buffer.bytes().length);

        // Buffer now at capacity 128, position 66, remaining 62.
        // Fill remaining space, should succeed without resize.
        buffer.write(new byte[62]);
        Assert.assertEquals(128, buffer.bytes().length);

        // One more byte exceeds the max, should throw.
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            buffer.write((byte) 1);
        }, e -> {
            Assert.assertContains("Capacity 129 exceeds max buffer " +
                                  "capacity: 128",
                                  e.getMessage());
        });
    }

    @Test
    public void testSetMaxBufferCapacityWithInvalidValue() {
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            BytesBuffer.initMaxBufferCapacity(
                    BytesBuffer.DEFAULT_CAPACITY - 1);
        }, e -> {
            Assert.assertContains("Max buffer capacity must be in range",
                                  e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            BytesBuffer.initMaxBufferCapacity(
                    BytesBuffer.MAX_BUFFER_CAPACITY_UPPER_BOUND + 1);
        }, e -> {
            Assert.assertContains("Max buffer capacity must be in range",
                                  e.getMessage());
        });
    }

    @Test
    public void testSetMaxBufferCapacityAtUpperBound() {
        BytesBuffer.initMaxBufferCapacity(
                BytesBuffer.MAX_BUFFER_CAPACITY_UPPER_BOUND);
        Assert.assertEquals(BytesBuffer.MAX_BUFFER_CAPACITY_UPPER_BOUND,
                            BytesBuffer.maxBufferCapacity());
    }

    @Test
    public void testDefaultMaxBufferCapacityDefersExplicitInit() {
        BytesBuffer.initMaxBufferCapacity(BytesBuffer.MAX_BUFFER_CAPACITY,
                                          false);
        Assert.assertEquals(BytesBuffer.MAX_BUFFER_CAPACITY,
                            BytesBuffer.maxBufferCapacity());

        BytesBuffer.initMaxBufferCapacity(256);
        Assert.assertEquals(256,
                            BytesBuffer.maxBufferCapacity());
    }

    @Test
    public void testLZ4DecompressWithMaxBufferCapacity() {
        byte[] bytes = genBytes(256);
        BytesBuffer compressed = LZ4Util.compress(bytes, 64);

        BytesBuffer.initMaxBufferCapacity(128);

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            LZ4Util.decompress(compressed.bytes(), 64);
        }, e -> {
            Assert.assertContains("exceeds max buffer capacity: 128",
                                  e.getMessage());
        });
    }

    @Test
    public void testLZ4DecompressSucceedsWithCustomMaxBufferCapacity() {
        byte[] bytes = genBytes(64);
        BytesBuffer compressed = LZ4Util.compress(bytes, 64);

        // Set a custom max that is large enough for the decompressed data
        BytesBuffer.initMaxBufferCapacity(256);

        BytesBuffer decompressed = LZ4Util.decompress(compressed.bytes(), 64);
        // Verify the custom cap is active and decompression succeeded
        Assert.assertEquals(256, BytesBuffer.maxBufferCapacity());
        Assert.assertArrayEquals(bytes, decompressed.bytes());
    }

    @Test
    public void testMaxBufferCapacityFromGraphConfig() throws Exception {
        HugeConfig config = FakeObjects.newConfig();
        config.setProperty(CoreOptions.STORE.name(), "buffer_capacity");
        config.setProperty(CoreOptions.SERIALIZER_BUFFER_MAX_CAPACITY.name(),
                           256);

        HugeGraph graph = HugeFactory.open(config);
        try {
            Assert.assertEquals(256, BytesBuffer.maxBufferCapacity());
        } finally {
            graph.close();
        }
    }

    @Test
    public void testMaxBufferCapacityRejectsConflictingGraphConfig()
           throws Exception {
        HugeConfig config1 = FakeObjects.newConfig();
        config1.setProperty(CoreOptions.STORE.name(), "buffer_capacity_1");
        config1.setProperty(CoreOptions.SERIALIZER_BUFFER_MAX_CAPACITY.name(),
                            256);

        HugeGraph graph1 = HugeFactory.open(config1);
        try {
            Assert.assertEquals(256, BytesBuffer.maxBufferCapacity());

            HugeConfig config2 = FakeObjects.newConfig();
            config2.setProperty(CoreOptions.STORE.name(),
                                "buffer_capacity_2");
            config2.setProperty(
                    CoreOptions.SERIALIZER_BUFFER_MAX_CAPACITY.name(), 512);

            Assert.assertThrows(IllegalArgumentException.class, () -> {
                HugeFactory.open(config2);
            }, e -> {
                Assert.assertContains("process-wide serializer buffer max " +
                                      "capacity has been initialized to 256",
                                      e.getMessage());
                Assert.assertContains("conflicting value 512",
                                      e.getMessage());
            });
            Assert.assertEquals(256, BytesBuffer.maxBufferCapacity());
        } finally {
            graph1.close();
        }
    }

    @Test
    public void testMaxBufferCapacityInheritsProcessWideValue()
           throws Exception {
        HugeConfig config1 = FakeObjects.newConfig();
        config1.setProperty(CoreOptions.STORE.name(), "buffer_inherit_1");
        config1.setProperty(CoreOptions.SERIALIZER_BUFFER_MAX_CAPACITY.name(),
                            256);

        HugeGraph graph1 = HugeFactory.open(config1);
        try {
            Assert.assertEquals(256, BytesBuffer.maxBufferCapacity());

            // Second graph omits SERIALIZER_BUFFER_MAX_CAPACITY, so it
            // receives the default value and should inherit the process-wide
            // cap (256) instead of throwing a conflict error.
            HugeConfig config2 = FakeObjects.newConfig();
            config2.setProperty(CoreOptions.STORE.name(), "buffer_inherit_2");

            HugeGraph graph2 = HugeFactory.open(config2);
            try {
                Assert.assertEquals(256, BytesBuffer.maxBufferCapacity());
            } finally {
                graph2.close();
            }
        } finally {
            graph1.close();
        }
    }

    @Test
    public void testMaxBufferCapacityDefersToExplicitConfigAfterDefaultGraph()
           throws Exception {
        HugeConfig config1 = FakeObjects.newConfig();
        config1.setProperty(CoreOptions.STORE.name(), "buffer_default_first");

        HugeGraph graph1 = HugeFactory.open(config1);
        try {
            Assert.assertEquals(BytesBuffer.MAX_BUFFER_CAPACITY,
                                BytesBuffer.maxBufferCapacity());

            HugeConfig config2 = FakeObjects.newConfig();
            config2.setProperty(CoreOptions.STORE.name(),
                                "buffer_custom_second");
            config2.setProperty(
                    CoreOptions.SERIALIZER_BUFFER_MAX_CAPACITY.name(), 256);

            HugeGraph graph2 = HugeFactory.open(config2);
            try {
                Assert.assertEquals(256, BytesBuffer.maxBufferCapacity());
            } finally {
                graph2.close();
            }
        } finally {
            graph1.close();
        }
    }

    @Test
    public void testWrap() {
        BytesBuffer buf4 = BytesBuffer.wrap(new byte[]{1, 2, 3, 4});
        Assert.assertArrayEquals(new byte[]{1, 2, 3, 4}, buf4.array());
        Assert.assertArrayEquals(new byte[]{1, 2, 3, 4}, buf4.read(4));
    }

    @Test
    public void testStringId() {
        Id id = IdGenerator.of("abc");
        byte[] bytes = new byte[]{(byte) 0x82, 97, 98, 99};

        Assert.assertArrayEquals(bytes, BytesBuffer.allocate(4)
                                                   .writeId(id).bytes());
        Assert.assertEquals(id, BytesBuffer.wrap(bytes).readId());

        id = IdGenerator.of("abcd");
        bytes = new byte[]{(byte) 0x83, 97, 98, 99, 100};

        Assert.assertArrayEquals(bytes, BytesBuffer.allocate(5)
                                                   .writeId(id).bytes());
        Assert.assertEquals(id, BytesBuffer.wrap(bytes).readId());
    }

    @Test
    public void testStringIdWithBigSize() {
        Id id = IdGenerator.of(genString(127));
        byte[] bytes = genBytes(129);
        bytes[0] = (byte) 0xc0;
        bytes[1] = (byte) 0x7e;
        Assert.assertArrayEquals(bytes, BytesBuffer.allocate(0)
                                                   .writeId(id).bytes());
        Assert.assertEquals(id, BytesBuffer.wrap(bytes).readId());

        id = IdGenerator.of(genString(128));
        bytes = genBytes(130);
        bytes[0] = (byte) 0xc0;
        bytes[1] = (byte) 0x7f;
        Assert.assertArrayEquals(bytes, BytesBuffer.allocate(0)
                                                   .writeId(id).bytes());
        Assert.assertEquals(id, BytesBuffer.wrap(bytes).readId());

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            BytesBuffer.allocate(0).writeId(IdGenerator.of(genString(BytesBuffer.ID_LEN_MAX + 1)));
        }, e -> {
            Assert.assertContains(String.format("Big id max length is %s, but got %s",
                                                BytesBuffer.ID_LEN_MAX,
                                                BytesBuffer.ID_LEN_MAX + 1),
                                  e.getMessage());
        });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            BytesBuffer.allocate(0).writeId(IdGenerator.of(genString(BytesBuffer.ID_LEN_MAX + 2)));
        }, e -> {
            Assert.assertContains(String.format("Big id max length is %s, but got %s",
                                                BytesBuffer.ID_LEN_MAX,
                                                BytesBuffer.ID_LEN_MAX + 2),
                                  e.getMessage());
        });
    }

    @Test
    public void testStringBigId() {
        Id id = IdGenerator.of(genString(128));
        byte[] bytes = genBytes(130);
        bytes[0] = (byte) 0xc0;
        bytes[1] = (byte) 0x7f;
        Assert.assertArrayEquals(bytes, BytesBuffer.allocate(0)
                                                   .writeId(id).bytes());
        Assert.assertEquals(id, BytesBuffer.wrap(bytes).readId());

        id = IdGenerator.of(genString(BytesBuffer.ID_LEN_MAX - 1));
        bytes = genBytes(BytesBuffer.ID_LEN_MAX + 1);
        bytes[0] = (byte) 0xff;
        bytes[1] = (byte) 0xfe;
        Assert.assertArrayEquals(bytes, BytesBuffer.allocate(0)
                                                   .writeId(id).bytes());
        Assert.assertEquals(id, BytesBuffer.wrap(bytes).readId());

        id = IdGenerator.of(genString(BytesBuffer.ID_LEN_MAX));
        bytes = genBytes(BytesBuffer.ID_LEN_MAX + 2);
        bytes[0] = (byte) 0xff;
        bytes[1] = (byte) 0xff;
        Assert.assertArrayEquals(bytes, BytesBuffer.allocate(0)
                                                   .writeId(id).bytes());
        Assert.assertEquals(id, BytesBuffer.wrap(bytes).readId());

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            BytesBuffer.allocate(0).writeId(IdGenerator.of(genString(BytesBuffer.ID_LEN_MAX + 1)));
        }, e -> {
            Assert.assertContains(String.format("Big id max length is %s, but got %s",
                                                BytesBuffer.ID_LEN_MAX,
                                                BytesBuffer.ID_LEN_MAX + 1),
                                  e.getMessage());
        });
    }

    @Test
    public void testNumberId() {
        // 2 bytes
        Id id = IdGenerator.of(0);
        byte[] bytes = genBytes("0800");
        Assert.assertArrayEquals(bytes, BytesBuffer.allocate(2)
                                                   .writeId(id).bytes());
        Assert.assertEquals(id, BytesBuffer.wrap(bytes).readId());

        id = IdGenerator.of(1);
        bytes = genBytes("0801");
        Assert.assertArrayEquals(bytes, BytesBuffer.allocate(2)
                                                   .writeId(id).bytes());
        Assert.assertEquals(id, BytesBuffer.wrap(bytes).readId());

        id = IdGenerator.of(127);
        bytes = genBytes("087f");
        Assert.assertArrayEquals(bytes, BytesBuffer.allocate(2)
                                                   .writeId(id).bytes());
        Assert.assertEquals(id, BytesBuffer.wrap(bytes).readId());

        id = IdGenerator.of(128);
        bytes = genBytes("0880");
        Assert.assertArrayEquals(bytes, BytesBuffer.allocate(2)
                                                   .writeId(id).bytes());
        Assert.assertEquals(id, BytesBuffer.wrap(bytes).readId());

        id = IdGenerator.of(255);
        bytes = genBytes("08ff");
        Assert.assertArrayEquals(bytes, BytesBuffer.allocate(2)
                                                   .writeId(id).bytes());
        Assert.assertEquals(id, BytesBuffer.wrap(bytes).readId());

        id = IdGenerator.of(256);
        bytes = genBytes("0900");
        Assert.assertArrayEquals(bytes, BytesBuffer.allocate(2)
                                                   .writeId(id).bytes());
        Assert.assertEquals(id, BytesBuffer.wrap(bytes).readId());

        id = IdGenerator.of(1573);
        bytes = genBytes("0e25");
        Assert.assertArrayEquals(bytes, BytesBuffer.allocate(2)
                                                   .writeId(id).bytes());
        Assert.assertEquals(id, BytesBuffer.wrap(bytes).readId());

        id = IdGenerator.of(0x7ff);
        bytes = genBytes("0fff");
        Assert.assertArrayEquals(bytes, BytesBuffer.allocate(2)
                                                   .writeId(id).bytes());
        Assert.assertEquals(id, BytesBuffer.wrap(bytes).readId());

        // 3 bytes
        id = IdGenerator.of(0x7ff + 1);
        bytes = genBytes("180800");
        Assert.assertArrayEquals(bytes, BytesBuffer.allocate(2)
                                                   .writeId(id).bytes());
        Assert.assertEquals(id, BytesBuffer.wrap(bytes).readId());

        id = IdGenerator.of(0x7ffff);
        bytes = genBytes("1fffff");
        Assert.assertArrayEquals(bytes, BytesBuffer.allocate(2)
                                                   .writeId(id).bytes());
        Assert.assertEquals(id, BytesBuffer.wrap(bytes).readId());

        // 4 bytes
        id = IdGenerator.of(0x7ffff + 1);
        bytes = genBytes("28080000");
        Assert.assertArrayEquals(bytes, BytesBuffer.allocate(2)
                                                   .writeId(id).bytes());
        Assert.assertEquals(id, BytesBuffer.wrap(bytes).readId());

        id = IdGenerator.of(0x7ffffff);
        bytes = genBytes("2fffffff");
        Assert.assertArrayEquals(bytes, BytesBuffer.allocate(2)
                                                   .writeId(id).bytes());
        Assert.assertEquals(id, BytesBuffer.wrap(bytes).readId());

        // 5 bytes
        id = IdGenerator.of(0x7ffffff + 1);
        bytes = genBytes("3808000000");
        Assert.assertArrayEquals(bytes, BytesBuffer.allocate(2)
                                                   .writeId(id).bytes());
        Assert.assertEquals(id, BytesBuffer.wrap(bytes).readId());

        id = IdGenerator.of(0x7ffffffffL);
        bytes = genBytes("3fffffffff");
        Assert.assertArrayEquals(bytes, BytesBuffer.allocate(2)
                                                   .writeId(id).bytes());
        Assert.assertEquals(id, BytesBuffer.wrap(bytes).readId());

        // 6 bytes
        id = IdGenerator.of(0x7ffffffffL + 1L);
        bytes = genBytes("480800000000");
        Assert.assertArrayEquals(bytes, BytesBuffer.allocate(2)
                                                   .writeId(id).bytes());
        Assert.assertEquals(id, BytesBuffer.wrap(bytes).readId());

        id = IdGenerator.of(0x7ffffffffffL);
        bytes = genBytes("4fffffffffff");
        Assert.assertArrayEquals(bytes, BytesBuffer.allocate(2)
                                                   .writeId(id).bytes());
        Assert.assertEquals(id, BytesBuffer.wrap(bytes).readId());

        // 7 bytes
        id = IdGenerator.of(0x7ffffffffffL + 1L);
        bytes = genBytes("58080000000000");
        Assert.assertArrayEquals(bytes, BytesBuffer.allocate(2)
                                                   .writeId(id).bytes());
        Assert.assertEquals(id, BytesBuffer.wrap(bytes).readId());

        id = IdGenerator.of(0x7ffffffffffffL);
        bytes = genBytes("5fffffffffffff");
        Assert.assertArrayEquals(bytes, BytesBuffer.allocate(2)
                                                   .writeId(id).bytes());
        Assert.assertEquals(id, BytesBuffer.wrap(bytes).readId());

        // 8 bytes
        id = IdGenerator.of(0x7ffffffffffffL + 1L);
        bytes = genBytes("6808000000000000");
        Assert.assertArrayEquals(bytes, BytesBuffer.allocate(2)
                                                   .writeId(id).bytes());
        Assert.assertEquals(id, BytesBuffer.wrap(bytes).readId());

        id = IdGenerator.of(0x7ffffffffffffffL);
        bytes = genBytes("6fffffffffffffff");
        Assert.assertArrayEquals(bytes, BytesBuffer.allocate(2)
                                                   .writeId(id).bytes());
        Assert.assertEquals(id, BytesBuffer.wrap(bytes).readId());

        // 9 bytes
        id = IdGenerator.of(0x7ffffffffffffffL + 1L);
        bytes = genBytes("780800000000000000");
        Assert.assertArrayEquals(bytes, BytesBuffer.allocate(2)
                                                   .writeId(id).bytes());
        Assert.assertEquals(id, BytesBuffer.wrap(bytes).readId());

        // others
        id = IdGenerator.of(Short.MAX_VALUE);
        bytes = genBytes("187fff");
        Assert.assertArrayEquals(bytes, BytesBuffer.allocate(2)
                                                   .writeId(id).bytes());
        Assert.assertEquals(id, BytesBuffer.wrap(bytes).readId());

        id = IdGenerator.of(Short.MAX_VALUE + 1);
        bytes = genBytes("188000");
        Assert.assertArrayEquals(bytes, BytesBuffer.allocate(2)
                                                   .writeId(id).bytes());
        Assert.assertEquals(id, BytesBuffer.wrap(bytes).readId());

        id = IdGenerator.of(Integer.MAX_VALUE);
        bytes = genBytes("387fffffff");
        Assert.assertArrayEquals(bytes, BytesBuffer.allocate(2)
                                                   .writeId(id).bytes());
        Assert.assertEquals(id, BytesBuffer.wrap(bytes).readId());

        id = IdGenerator.of(Integer.MAX_VALUE + 1L);
        bytes = genBytes("3880000000");
        Assert.assertArrayEquals(bytes, BytesBuffer.allocate(2)
                                                   .writeId(id).bytes());
        Assert.assertEquals(id, BytesBuffer.wrap(bytes).readId());

        id = IdGenerator.of(Long.MAX_VALUE);
        bytes = genBytes("787fffffffffffffff");
        Assert.assertArrayEquals(bytes, BytesBuffer.allocate(2)
                                                   .writeId(id).bytes());
        Assert.assertEquals(id, BytesBuffer.wrap(bytes).readId());
    }

    @Test
    public void testNumberIdWithNegValue() {
        // 2 bytes
        Id id = IdGenerator.of(0);
        byte[] bytes = genBytes("0800");
        Assert.assertArrayEquals(bytes, BytesBuffer.allocate(2)
                                                   .writeId(id).bytes());
        Assert.assertEquals(id, BytesBuffer.wrap(bytes).readId());

        id = IdGenerator.of(-1);
        bytes = genBytes("07ff");
        Assert.assertArrayEquals(bytes, BytesBuffer.allocate(2)
                                                   .writeId(id).bytes());
        Assert.assertEquals(id, BytesBuffer.wrap(bytes).readId());

        id = IdGenerator.of(-127);
        bytes = genBytes("0781");
        Assert.assertArrayEquals(bytes, BytesBuffer.allocate(2)
                                                   .writeId(id).bytes());
        Assert.assertEquals(id, BytesBuffer.wrap(bytes).readId());

        id = IdGenerator.of(-128);
        bytes = genBytes("0780");
        Assert.assertArrayEquals(bytes, BytesBuffer.allocate(2)
                                                   .writeId(id).bytes());
        Assert.assertEquals(id, BytesBuffer.wrap(bytes).readId());

        id = IdGenerator.of(-129);
        bytes = genBytes("077f");
        Assert.assertArrayEquals(bytes, BytesBuffer.allocate(2)
                                                   .writeId(id).bytes());
        Assert.assertEquals(id, BytesBuffer.wrap(bytes).readId());

        id = IdGenerator.of(-255);
        bytes = genBytes("0701");
        Assert.assertArrayEquals(bytes, BytesBuffer.allocate(2)
                                                   .writeId(id).bytes());
        Assert.assertEquals(id, BytesBuffer.wrap(bytes).readId());

        id = IdGenerator.of(-256);
        bytes = genBytes("0700");
        Assert.assertArrayEquals(bytes, BytesBuffer.allocate(2)
                                                   .writeId(id).bytes());
        Assert.assertEquals(id, BytesBuffer.wrap(bytes).readId());

        id = IdGenerator.of(-1573);
        bytes = genBytes("01db");
        Assert.assertArrayEquals(bytes, BytesBuffer.allocate(2)
                                                   .writeId(id).bytes());
        Assert.assertEquals(id, BytesBuffer.wrap(bytes).readId());

        id = IdGenerator.of(~0x7ff);
        bytes = genBytes("0000");
        Assert.assertArrayEquals(bytes, BytesBuffer.allocate(2)
                                                   .writeId(id).bytes());
        Assert.assertEquals(id, BytesBuffer.wrap(bytes).readId());

        // 3 bytes
        id = IdGenerator.of(~0x7ff - 1);
        bytes = genBytes("17f7ff");
        Assert.assertArrayEquals(bytes, BytesBuffer.allocate(2)
                                                   .writeId(id).bytes());
        Assert.assertEquals(id, BytesBuffer.wrap(bytes).readId());

        id = IdGenerator.of(~0x7ffff);
        bytes = genBytes("100000");
        Assert.assertArrayEquals(bytes, BytesBuffer.allocate(2)
                                                   .writeId(id).bytes());
        Assert.assertEquals(id, BytesBuffer.wrap(bytes).readId());

        // 4 bytes
        id = IdGenerator.of(~0x7ffff - 1);
        bytes = genBytes("27f7ffff");
        Assert.assertArrayEquals(bytes, BytesBuffer.allocate(2)
                                                   .writeId(id).bytes());
        Assert.assertEquals(id, BytesBuffer.wrap(bytes).readId());

        id = IdGenerator.of(~0x7ffffff);
        bytes = genBytes("20000000");
        Assert.assertArrayEquals(bytes, BytesBuffer.allocate(2)
                                                   .writeId(id).bytes());
        Assert.assertEquals(id, BytesBuffer.wrap(bytes).readId());

        // 5 bytes
        id = IdGenerator.of(~0x7ffffff - 1);
        bytes = genBytes("37f7ffffff");
        Assert.assertArrayEquals(bytes, BytesBuffer.allocate(2)
                                                   .writeId(id).bytes());
        Assert.assertEquals(id, BytesBuffer.wrap(bytes).readId());

        id = IdGenerator.of(~0x7ffffffffL);
        bytes = genBytes("3000000000");
        Assert.assertArrayEquals(bytes, BytesBuffer.allocate(2)
                                                   .writeId(id).bytes());
        Assert.assertEquals(id, BytesBuffer.wrap(bytes).readId());

        // 6 bytes
        id = IdGenerator.of(~0x7ffffffffL - 1L);
        bytes = genBytes("47f7ffffffff");
        Assert.assertArrayEquals(bytes, BytesBuffer.allocate(2)
                                                   .writeId(id).bytes());
        Assert.assertEquals(id, BytesBuffer.wrap(bytes).readId());

        id = IdGenerator.of(~0x7ffffffffffL);
        bytes = genBytes("400000000000");
        Assert.assertArrayEquals(bytes, BytesBuffer.allocate(2)
                                                   .writeId(id).bytes());
        Assert.assertEquals(id, BytesBuffer.wrap(bytes).readId());

        // 7 bytes
        id = IdGenerator.of(~0x7ffffffffffL - 1L);
        bytes = genBytes("57f7ffffffffff");
        Assert.assertArrayEquals(bytes, BytesBuffer.allocate(2)
                                                   .writeId(id).bytes());
        Assert.assertEquals(id, BytesBuffer.wrap(bytes).readId());

        id = IdGenerator.of(~0x7ffffffffffffL);
        bytes = genBytes("50000000000000");
        Assert.assertArrayEquals(bytes, BytesBuffer.allocate(2)
                                                   .writeId(id).bytes());
        Assert.assertEquals(id, BytesBuffer.wrap(bytes).readId());

        // 8 bytes
        id = IdGenerator.of(~0x7ffffffffffffL - 1L);
        bytes = genBytes("67f7ffffffffffff");
        Assert.assertArrayEquals(bytes, BytesBuffer.allocate(2)
                                                   .writeId(id).bytes());
        Assert.assertEquals(id, BytesBuffer.wrap(bytes).readId());

        id = IdGenerator.of(~0x7ffffffffffffffL);
        bytes = genBytes("6000000000000000");
        Assert.assertArrayEquals(bytes, BytesBuffer.allocate(2)
                                                   .writeId(id).bytes());
        Assert.assertEquals(id, BytesBuffer.wrap(bytes).readId());

        // 9 bytes
        id = IdGenerator.of(~0x7ffffffffffffffL - 1L);
        bytes = genBytes("70f7ffffffffffffff");
        Assert.assertArrayEquals(bytes, BytesBuffer.allocate(2)
                                                   .writeId(id).bytes());
        Assert.assertEquals(id, BytesBuffer.wrap(bytes).readId());

        // others
        id = IdGenerator.of(Short.MIN_VALUE);
        bytes = genBytes("178000");
        Assert.assertArrayEquals(bytes, BytesBuffer.allocate(2)
                                                   .writeId(id).bytes());
        Assert.assertEquals(id, BytesBuffer.wrap(bytes).readId());

        id = IdGenerator.of(Short.MIN_VALUE - 1);
        bytes = genBytes("177fff");
        Assert.assertArrayEquals(bytes, BytesBuffer.allocate(2)
                                                   .writeId(id).bytes());
        Assert.assertEquals(id, BytesBuffer.wrap(bytes).readId());

        id = IdGenerator.of(Integer.MIN_VALUE);
        bytes = genBytes("3780000000");
        Assert.assertArrayEquals(bytes, BytesBuffer.allocate(2)
                                                   .writeId(id).bytes());
        Assert.assertEquals(id, BytesBuffer.wrap(bytes).readId());

        id = IdGenerator.of(Integer.MIN_VALUE - 1L);
        bytes = genBytes("377fffffff");
        Assert.assertArrayEquals(bytes, BytesBuffer.allocate(2)
                                                   .writeId(id).bytes());
        Assert.assertEquals(id, BytesBuffer.wrap(bytes).readId());

        id = IdGenerator.of(Long.MIN_VALUE);
        bytes = genBytes("708000000000000000");
        Assert.assertArrayEquals(bytes, BytesBuffer.allocate(2)
                                                   .writeId(id).bytes());
        Assert.assertEquals(id, BytesBuffer.wrap(bytes).readId());
    }

    @Test
    public void testUuidId() {
        Id id = IdGenerator.of("835e1153928149578691cf79258e90eb", true);
        byte[] bytes = genBytes("7f835e1153928149578691cf79258e90eb");

        Assert.assertArrayEquals(bytes, BytesBuffer.allocate(17)
                                                   .writeId(id).bytes());
        Assert.assertEquals(id, BytesBuffer.wrap(bytes).readId());

        id = new UuidId(UUID.randomUUID());
        bytes = BytesBuffer.allocate(0).writeId(id).bytes();
        Assert.assertEquals(id, BytesBuffer.wrap(bytes).readId());
    }

    @Test
    public void testVarInt() {
        Assert.assertArrayEquals(new byte[]{0},
                                 BytesBuffer.allocate(5).writeVInt(0).bytes());
        Assert.assertArrayEquals(new byte[]{1},
                                 BytesBuffer.allocate(5).writeVInt(1).bytes());
        Assert.assertArrayEquals(new byte[]{(byte) 0x7f},
                                 BytesBuffer.allocate(5)
                                            .writeVInt(127)
                                            .bytes());
        Assert.assertArrayEquals(new byte[]{(byte) 0x81, 0},
                                 BytesBuffer.allocate(5)
                                            .writeVInt(128)
                                            .bytes());
        Assert.assertArrayEquals(new byte[]{(byte) 0xff, (byte) 0x7f},
                                 BytesBuffer.allocate(5)
                                            .writeVInt(16383)
                                            .bytes());
        Assert.assertArrayEquals(new byte[]{(byte) 0x81, (byte) 0x80, 0},
                                 BytesBuffer.allocate(5)
                                            .writeVInt(16384)
                                            .bytes());
        Assert.assertArrayEquals(new byte[]{(byte) 0x81, (byte) 0x80, 1},
                                 BytesBuffer.allocate(5)
                                            .writeVInt(16385)
                                            .bytes());

        Assert.assertArrayEquals(new byte[]{-113, -1, -1, -1, 127},
                                 BytesBuffer.allocate(5).writeVInt(-1).bytes());
        Assert.assertArrayEquals(new byte[]{-121, -1, -1, -1, 127},
                                 BytesBuffer.allocate(5)
                                            .writeVInt(Integer.MAX_VALUE)
                                            .bytes());
        Assert.assertArrayEquals(new byte[]{-120, -128, -128, -128, 0},
                                 BytesBuffer.allocate(5)
                                            .writeVInt(Integer.MIN_VALUE)
                                            .bytes());

        for (int i = Short.MIN_VALUE; i < Short.MAX_VALUE; i++) {
            BytesBuffer buf = BytesBuffer.allocate(5).writeVInt(i);
            Assert.assertEquals(i, buf.forReadWritten().readVInt());
        }

        Random random = new Random();
        for (int i = Integer.MIN_VALUE; i < Integer.MAX_VALUE; ) {
            BytesBuffer buf = BytesBuffer.allocate(5).writeVInt(i);
            Assert.assertEquals(i, buf.forReadWritten().readVInt());

            int old = i;
            i += random.nextInt(Short.MAX_VALUE);
            if (old > 0 && i < 0) {
                // overflow
                break;
            }
        }
    }

    @Test
    public void testVarLong() {
        Assert.assertArrayEquals(new byte[]{0},
                                 BytesBuffer.allocate(5).writeVLong(0).bytes());
        Assert.assertArrayEquals(new byte[]{1},
                                 BytesBuffer.allocate(5).writeVLong(1).bytes());
        Assert.assertArrayEquals(new byte[]{(byte) 0x7f},
                                 BytesBuffer.allocate(5)
                                            .writeVLong(127)
                                            .bytes());
        Assert.assertArrayEquals(new byte[]{(byte) 0x81, 0},
                                 BytesBuffer.allocate(5)
                                            .writeVLong(128)
                                            .bytes());
        Assert.assertArrayEquals(new byte[]{(byte) 0xff, (byte) 0x7f},
                                 BytesBuffer.allocate(5)
                                            .writeVLong(16383)
                                            .bytes());
        Assert.assertArrayEquals(new byte[]{(byte) 0x81, (byte) 0x80, 0},
                                 BytesBuffer.allocate(5)
                                            .writeVLong(16384)
                                            .bytes());
        Assert.assertArrayEquals(new byte[]{(byte) 0x81, (byte) 0x80, 1},
                                 BytesBuffer.allocate(5)
                                            .writeVLong(16385)
                                            .bytes());

        Assert.assertArrayEquals(new byte[]{-127, -1, -1, -1, -1,
                                            -1, -1, -1, -1, 127},
                                 BytesBuffer.allocate(5).writeVLong(-1).bytes());
        Assert.assertArrayEquals(new byte[]{-121, -1, -1, -1, 127},
                                 BytesBuffer.allocate(5)
                                            .writeVLong(Integer.MAX_VALUE)
                                            .bytes());
        Assert.assertArrayEquals(new byte[]{-127, -1, -1, -1, -1,
                                            -8, -128, -128, -128, 0},
                                 BytesBuffer.allocate(5)
                                            .writeVLong(Integer.MIN_VALUE)
                                            .bytes());
        Assert.assertArrayEquals(new byte[]{-1, -1, -1, -1, -1,
                                            -1, -1, -1, 127},
                                 BytesBuffer.allocate(5)
                                            .writeVLong(Long.MAX_VALUE)
                                            .bytes());
        Assert.assertArrayEquals(new byte[]{-127, -128, -128, -128, -128,
                                            -128, -128, -128, -128, 0},
                                 BytesBuffer.allocate(5)
                                            .writeVLong(Long.MIN_VALUE)
                                            .bytes());

        for (long i = Short.MIN_VALUE; i < Short.MAX_VALUE; i++) {
            BytesBuffer buf = BytesBuffer.allocate(10).writeVLong(i);
            Assert.assertEquals(i, buf.forReadWritten().readVLong());
        }

        Random random = new Random();
        for (long i = Long.MIN_VALUE; i < Long.MAX_VALUE; ) {
            BytesBuffer buf = BytesBuffer.allocate(10).writeVLong(i);
            Assert.assertEquals(i, buf.forReadWritten().readVLong());

            long old = i;
            i += (random.nextLong() >>> 8);
            if (old > 0 && i < 0) {
                // overflow
                break;
            }
        }
    }

    @Test
    public void testProperty() {
        BytesBuffer buf = BytesBuffer.allocate(0);
        PropertyKey pkey = genPkey(DataType.BOOLEAN);
        Object value = true;
        byte[] bytes = genBytes("01");
        Assert.assertArrayEquals(bytes, buf.writeProperty(pkey, value).bytes());
        Assert.assertEquals(value, BytesBuffer.wrap(bytes).readProperty(pkey));

        value = false;
        bytes = genBytes("00");
        buf.forReadWritten();
        Assert.assertArrayEquals(bytes, buf.writeProperty(pkey, value).bytes());
        Assert.assertEquals(value, BytesBuffer.wrap(bytes).readProperty(pkey));

        pkey = genPkey(DataType.BYTE);
        value = (byte) 127;
        bytes = genBytes("7f");
        buf.forReadWritten();
        Assert.assertArrayEquals(bytes, buf.writeProperty(pkey, value).bytes());
        Assert.assertEquals(value, BytesBuffer.wrap(bytes).readProperty(pkey));

        pkey = genPkey(DataType.INT);
        value = 127;
        bytes = genBytes("7f");
        buf.forReadWritten();
        Assert.assertArrayEquals(bytes, buf.writeProperty(pkey, value).bytes());
        Assert.assertEquals(value, BytesBuffer.wrap(bytes).readProperty(pkey));

        pkey = genPkey(DataType.INT);
        value = 128;
        bytes = genBytes("8100");
        buf.forReadWritten();
        Assert.assertArrayEquals(bytes, buf.writeProperty(pkey, value).bytes());
        Assert.assertEquals(value, BytesBuffer.wrap(bytes).readProperty(pkey));

        pkey = genPkey(DataType.FLOAT);
        value = 1.0f;
        bytes = genBytes("3f800000");
        buf.forReadWritten();
        Assert.assertArrayEquals(bytes, buf.writeProperty(pkey, value).bytes());
        Assert.assertEquals(value, BytesBuffer.wrap(bytes).readProperty(pkey));

        pkey = genPkey(DataType.FLOAT);
        value = 3.14f;
        bytes = genBytes("4048f5c3");
        buf.forReadWritten();
        Assert.assertArrayEquals(bytes, buf.writeProperty(pkey, value).bytes());
        Assert.assertEquals(value, BytesBuffer.wrap(bytes).readProperty(pkey));

        pkey = genPkey(DataType.FLOAT);
        value = -1.0f;
        bytes = genBytes("bf800000");
        buf.forReadWritten();
        Assert.assertArrayEquals(bytes, buf.writeProperty(pkey, value).bytes());
        Assert.assertEquals(value, BytesBuffer.wrap(bytes).readProperty(pkey));

        pkey = genPkey(DataType.FLOAT);
        value = Float.MAX_VALUE;
        bytes = genBytes("7f7fffff");
        buf.forReadWritten();
        Assert.assertArrayEquals(bytes, buf.writeProperty(pkey, value).bytes());
        Assert.assertEquals(value, BytesBuffer.wrap(bytes).readProperty(pkey));

        pkey = genPkey(DataType.LONG);
        value = 127L;
        bytes = genBytes("7f");
        buf.forReadWritten();
        Assert.assertArrayEquals(bytes, buf.writeProperty(pkey, value).bytes());
        Assert.assertEquals(value, BytesBuffer.wrap(bytes).readProperty(pkey));

        pkey = genPkey(DataType.DOUBLE);
        value = 3.14d;
        bytes = genBytes("40091eb851eb851f");
        buf.forReadWritten();
        Assert.assertArrayEquals(bytes, buf.writeProperty(pkey, value).bytes());
        Assert.assertEquals(value, BytesBuffer.wrap(bytes).readProperty(pkey));

        pkey = genPkey(DataType.DATE);
        Calendar c = Calendar.getInstance(TimeZone.getTimeZone("Beijing"));
        c.setTimeInMillis(1565851529514L);
        value = c.getTime();
        bytes = genBytes("adc9a098e22a");
        buf.forReadWritten();
        Assert.assertArrayEquals(bytes, buf.writeProperty(pkey, value).bytes());
        Assert.assertEquals(value, BytesBuffer.wrap(bytes).readProperty(pkey));

        pkey = genPkey(DataType.TEXT);
        value = "abc";
        bytes = genBytes("03616263");
        buf.forReadWritten();
        Assert.assertArrayEquals(bytes, buf.writeProperty(pkey, value).bytes());
        Assert.assertEquals(value, BytesBuffer.wrap(bytes).readProperty(pkey));

        pkey = genPkey(DataType.BLOB);
        value = genBytes("001199aabbcc");
        bytes = genBytes("06001199aabbcc");
        buf.forReadWritten();
        Assert.assertArrayEquals(bytes, buf.writeProperty(pkey, value).bytes());
        Assert.assertEquals(Blob.wrap((byte[]) value),
                            BytesBuffer.wrap(bytes).readProperty(pkey));

        pkey = genPkey(DataType.UUID);
        value = UUID.fromString("3cfcafc8-7906-4ab7-a207-4ded056f58de");
        bytes = genBytes("3cfcafc879064ab7a2074ded056f58de");
        buf.forReadWritten();
        Assert.assertArrayEquals(bytes, buf.writeProperty(pkey, value).bytes());
        Assert.assertEquals(value, BytesBuffer.wrap(bytes).readProperty(pkey));

        pkey = genPkey(DataType.OBJECT);
        value = new Point(3, 8);
        bytes = genBytes("1301006a6176612e6177742e506f696ef4010610");
        buf.forReadWritten();
        Assert.assertArrayEquals(bytes, buf.writeProperty(pkey, value).bytes());
        Assert.assertEquals(value, BytesBuffer.wrap(bytes).readProperty(pkey));

        pkey = genPkey(DataType.OBJECT);
        value = UUID.fromString("3cfcafc8-7906-4ab7-a207-4ded056f58de");
        bytes = genBytes("2101006a6176612e7574696c2e555549c401" +
                         "3cfcafc879064ab7a2074ded056f58de");
        buf.forReadWritten();
        Assert.assertArrayEquals(bytes, buf.writeProperty(pkey, value).bytes());
        Assert.assertEquals(value, BytesBuffer.wrap(bytes).readProperty(pkey));

        pkey = genPkey(DataType.OBJECT);
        value = new int[]{1, 3, 8};
        bytes = genBytes("0901005bc90104020610");
        buf.forReadWritten();
        Assert.assertArrayEquals(bytes, buf.writeProperty(pkey, value).bytes());
        Assert.assertArrayEquals((int[]) value, (int[])
                BytesBuffer.wrap(bytes).readProperty(pkey));
    }

    @Test
    public void testPropertyWithList() {
        BytesBuffer buf = BytesBuffer.allocate(0);
        PropertyKey pkey = genListPkey(DataType.BOOLEAN);
        Object value = ImmutableList.of(true, false);
        byte[] bytes = genBytes("020100");
        Assert.assertArrayEquals(bytes, buf.writeProperty(pkey, value).bytes());
        Assert.assertEquals(value, BytesBuffer.wrap(bytes).readProperty(pkey));

        pkey = genListPkey(DataType.BYTE);
        value = ImmutableList.of();
        bytes = genBytes("00");
        buf.forReadWritten();
        Assert.assertArrayEquals(bytes, buf.writeProperty(pkey, value).bytes());
        Assert.assertEquals(value, BytesBuffer.wrap(bytes).readProperty(pkey));

        pkey = genListPkey(DataType.BYTE);
        value = ImmutableList.of((byte) 127, (byte) 128);
        bytes = genBytes("027f8fffffff00");
        buf.forReadWritten();
        Assert.assertArrayEquals(bytes, buf.writeProperty(pkey, value).bytes());
        Assert.assertEquals(value, BytesBuffer.wrap(bytes).readProperty(pkey));

        pkey = genListPkey(DataType.INT);
        value = ImmutableList.of(127, 128);
        bytes = genBytes("027f8100");
        buf.forReadWritten();
        Assert.assertArrayEquals(bytes, buf.writeProperty(pkey, value).bytes());
        Assert.assertEquals(value, BytesBuffer.wrap(bytes).readProperty(pkey));

        pkey = genListPkey(DataType.FLOAT);
        value = ImmutableList.of(1.0f, 3.14f);
        bytes = genBytes("023f8000004048f5c3");
        buf.forReadWritten();
        Assert.assertArrayEquals(bytes, buf.writeProperty(pkey, value).bytes());
        Assert.assertEquals(value, BytesBuffer.wrap(bytes).readProperty(pkey));

        pkey = genListPkey(DataType.LONG);
        value = ImmutableList.of(127L, 128L);
        bytes = genBytes("027f8100");
        buf.forReadWritten();
        Assert.assertArrayEquals(bytes, buf.writeProperty(pkey, value).bytes());
        Assert.assertEquals(value, BytesBuffer.wrap(bytes).readProperty(pkey));

        pkey = genListPkey(DataType.DOUBLE);
        value = ImmutableList.of(1.0d, 3.14d);
        bytes = genBytes("023ff000000000000040091eb851eb851f");
        buf.forReadWritten();
        Assert.assertArrayEquals(bytes, buf.writeProperty(pkey, value).bytes());
        Assert.assertEquals(value, BytesBuffer.wrap(bytes).readProperty(pkey));

        pkey = genListPkey(DataType.DATE);
        Calendar c = Calendar.getInstance(TimeZone.getTimeZone("Beijing"));
        c.setTimeInMillis(1565851529514L);
        value = ImmutableList.of(c.getTime(), c.getTime());
        bytes = genBytes("02adc9a098e22aadc9a098e22a");
        buf.forReadWritten();
        Assert.assertArrayEquals(bytes, buf.writeProperty(pkey, value).bytes());
        Assert.assertEquals(value, BytesBuffer.wrap(bytes).readProperty(pkey));

        pkey = genListPkey(DataType.TEXT);
        value = ImmutableList.of("abc", "123");
        bytes = genBytes("020361626303313233");
        buf.forReadWritten();
        Assert.assertArrayEquals(bytes, buf.writeProperty(pkey, value).bytes());
        Assert.assertEquals(value, BytesBuffer.wrap(bytes).readProperty(pkey));

        pkey = genListPkey(DataType.BLOB);
        value = ImmutableList.of(genBytes("001199aabbcc"), genBytes("5566"));
        bytes = genBytes("0206001199aabbcc025566");
        buf.forReadWritten();
        Assert.assertArrayEquals(bytes, buf.writeProperty(pkey, value).bytes());
        List<?> list = (List<?>) BytesBuffer.wrap(bytes).readProperty(pkey);
        Assert.assertEquals(Blob.wrap(genBytes("001199aabbcc")), list.get(0));
        Assert.assertEquals(Blob.wrap(genBytes("5566")), list.get(1));

        pkey = genListPkey(DataType.UUID);
        UUID uuid = UUID.fromString("3cfcafc8-7906-4ab7-a207-4ded056f58de");
        value = ImmutableList.of(uuid, uuid);
        bytes = genBytes("023cfcafc879064ab7a2074ded056f58de" +
                         "3cfcafc879064ab7a2074ded056f58de");
        buf.forReadWritten();
        Assert.assertArrayEquals(bytes, buf.writeProperty(pkey, value).bytes());
        Assert.assertEquals(value, BytesBuffer.wrap(bytes).readProperty(pkey));

        pkey = genListPkey(DataType.OBJECT);
        value = ImmutableList.of(new Point(3, 8), new Point(3, 9));
        bytes = genBytes("021301006a6176612e6177742e506f696ef4010610" +
                         "1301006a6176612e6177742e506f696ef4010612");
        buf.forReadWritten();
        Assert.assertArrayEquals(bytes, buf.writeProperty(pkey, value).bytes());
        Assert.assertEquals(value, BytesBuffer.wrap(bytes).readProperty(pkey));

        pkey = genListPkey(DataType.OBJECT);
        value = ImmutableList.of(new int[]{1, 3}, new int[]{2, 5});
        bytes = genBytes("020801005bc9010302060801005bc90103040a");
        buf.forReadWritten();
        Assert.assertArrayEquals(bytes, buf.writeProperty(pkey, value).bytes());
        list = (List<?>) BytesBuffer.wrap(bytes).readProperty(pkey);
        Assert.assertArrayEquals(new int[]{1, 3}, (int[]) list.get(0));
        Assert.assertArrayEquals(new int[]{2, 5}, (int[]) list.get(1));
    }

    @Test
    public void testPropertyWithSet() {
        BytesBuffer buf = BytesBuffer.allocate(0);
        PropertyKey pkey = genSetPkey(DataType.BOOLEAN);
        Object value = ImmutableSet.of(true, false);
        byte[] bytes = genBytes("020100");
        Assert.assertArrayEquals(bytes, buf.writeProperty(pkey, value).bytes());
        Assert.assertEquals(value, BytesBuffer.wrap(bytes).readProperty(pkey));

        pkey = genSetPkey(DataType.BYTE);
        value = ImmutableSet.of();
        bytes = genBytes("00");
        buf.forReadWritten();
        Assert.assertArrayEquals(bytes, buf.writeProperty(pkey, value).bytes());
        Assert.assertEquals(value, BytesBuffer.wrap(bytes).readProperty(pkey));

        pkey = genSetPkey(DataType.BYTE);
        value = ImmutableSet.of((byte) 127, (byte) 128);
        bytes = genBytes("027f8fffffff00");
        buf.forReadWritten();
        Assert.assertArrayEquals(bytes, buf.writeProperty(pkey, value).bytes());
        Assert.assertEquals(value, BytesBuffer.wrap(bytes).readProperty(pkey));

        pkey = genSetPkey(DataType.INT);
        value = ImmutableSet.of(127, 128);
        bytes = genBytes("027f8100");
        buf.forReadWritten();
        Assert.assertArrayEquals(bytes, buf.writeProperty(pkey, value).bytes());
        Assert.assertEquals(value, BytesBuffer.wrap(bytes).readProperty(pkey));

        pkey = genSetPkey(DataType.FLOAT);
        value = ImmutableSet.of(1.0f, 3.14f);
        bytes = genBytes("023f8000004048f5c3");
        buf.forReadWritten();
        Assert.assertArrayEquals(bytes, buf.writeProperty(pkey, value).bytes());
        Assert.assertEquals(value, BytesBuffer.wrap(bytes).readProperty(pkey));

        pkey = genSetPkey(DataType.LONG);
        value = ImmutableSet.of(127L, 128L);
        bytes = genBytes("027f8100");
        buf.forReadWritten();
        Assert.assertArrayEquals(bytes, buf.writeProperty(pkey, value).bytes());
        Assert.assertEquals(value, BytesBuffer.wrap(bytes).readProperty(pkey));

        pkey = genSetPkey(DataType.DOUBLE);
        value = ImmutableSet.of(1.0d, 3.14d);
        bytes = genBytes("023ff000000000000040091eb851eb851f");
        buf.forReadWritten();
        Assert.assertArrayEquals(bytes, buf.writeProperty(pkey, value).bytes());
        Assert.assertEquals(value, BytesBuffer.wrap(bytes).readProperty(pkey));

        pkey = genSetPkey(DataType.DATE);
        Calendar c = Calendar.getInstance(TimeZone.getTimeZone("Beijing"));
        c.setTimeInMillis(1565851529514L);
        value = ImmutableSet.of(c.getTime(), c.getTime());
        bytes = genBytes("01adc9a098e22a");
        buf.forReadWritten();
        Assert.assertArrayEquals(bytes, buf.writeProperty(pkey, value).bytes());
        Assert.assertEquals(value, BytesBuffer.wrap(bytes).readProperty(pkey));

        pkey = genSetPkey(DataType.TEXT);
        value = ImmutableSet.of("abc", "123");
        bytes = genBytes("020361626303313233");
        buf.forReadWritten();
        Assert.assertArrayEquals(bytes, buf.writeProperty(pkey, value).bytes());
        Assert.assertEquals(value, BytesBuffer.wrap(bytes).readProperty(pkey));

        pkey = genSetPkey(DataType.BLOB);
        value = ImmutableSet.of(genBytes("001199aabbcc"), genBytes("5566"));
        bytes = genBytes("0206001199aabbcc025566");
        buf.forReadWritten();
        Assert.assertArrayEquals(bytes, buf.writeProperty(pkey, value).bytes());
        Set<?> set = (Set<?>) BytesBuffer.wrap(bytes).readProperty(pkey);
        Iterator<?> iterator = set.iterator();
        Assert.assertEquals(Blob.wrap(genBytes("001199aabbcc")),
                            iterator.next());
        Assert.assertEquals(Blob.wrap(genBytes("5566")), iterator.next());

        pkey = genSetPkey(DataType.UUID);
        UUID uuid = UUID.fromString("3cfcafc8-7906-4ab7-a207-4ded056f58de");
        value = ImmutableSet.of(uuid, uuid);
        bytes = genBytes("013cfcafc879064ab7a2074ded056f58de");
        buf.forReadWritten();
        Assert.assertArrayEquals(bytes, buf.writeProperty(pkey, value).bytes());
        Assert.assertEquals(value, BytesBuffer.wrap(bytes).readProperty(pkey));

        pkey = genSetPkey(DataType.OBJECT);
        value = ImmutableSet.of(new Point(3, 8), new Point(3, 9));
        bytes = genBytes("021301006a6176612e6177742e506f696ef4010610" +
                         "1301006a6176612e6177742e506f696ef4010612");
        buf.forReadWritten();
        Assert.assertArrayEquals(bytes, buf.writeProperty(pkey, value).bytes());
        Assert.assertEquals(value, BytesBuffer.wrap(bytes).readProperty(pkey));

        pkey = genSetPkey(DataType.OBJECT);
        value = ImmutableSet.of(new int[]{1, 3}, new int[]{2, 5});
        bytes = genBytes("020801005bc9010302060801005bc90103040a");
        buf.forReadWritten();
        Assert.assertArrayEquals(bytes, buf.writeProperty(pkey, value).bytes());
        set = (Set<?>) BytesBuffer.wrap(bytes).readProperty(pkey);
        iterator = set.iterator();
        Assert.assertArrayEquals(new int[]{1, 3}, (int[]) iterator.next());
        Assert.assertArrayEquals(new int[]{2, 5}, (int[]) iterator.next());
    }

    @Test
    public void testString() {
        BytesBuffer buf = BytesBuffer.allocate(0);
        buf.writeStringRaw("any");
        byte[] bytes = genBytes("616e79");
        Assert.assertArrayEquals(bytes, buf.bytes());

        buf.forReadWritten();
        Assert.assertEquals("any", buf.readStringFromRemaining());

        bytes = genBytes("61626364");
        buf = BytesBuffer.allocate(0);
        Assert.assertArrayEquals(bytes,
                                 buf.writeStringToRemaining("abcd").bytes());
        Assert.assertEquals("abcd",
                            BytesBuffer.wrap(bytes).readStringFromRemaining());

        bytes = genBytes("0461626364");
        buf = BytesBuffer.allocate(0);
        Assert.assertArrayEquals(bytes, buf.writeString("abcd").bytes());
        Assert.assertEquals("abcd", BytesBuffer.wrap(bytes).readString());

        bytes = genBytes("6162636400");
        buf = BytesBuffer.allocate(0);
        Assert.assertArrayEquals(bytes,
                                 buf.writeStringWithEnding("abcd").bytes());
        Assert.assertEquals("abcd",
                            BytesBuffer.wrap(bytes).readStringWithEnding());

        bytes = genBytes("61620100");
        buf = BytesBuffer.allocate(0);
        Assert.assertArrayEquals(bytes,
                                 buf.writeStringWithEnding("ab\u0001").bytes());
        Assert.assertEquals("ab\u0001",
                            BytesBuffer.wrap(bytes).readStringWithEnding());

        bytes = genBytes("61627f00");
        buf = BytesBuffer.allocate(0);
        Assert.assertArrayEquals(bytes,
                                 buf.writeStringWithEnding("ab\u007f").bytes());
        Assert.assertEquals("ab\u007f",
                            BytesBuffer.wrap(bytes).readStringWithEnding());

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            BytesBuffer.allocate(0).writeStringWithEnding("ab\u0000");
        }, e -> {
            Assert.assertContains("Can't contains byte '0x00' in string",
                                  e.getMessage());
        });

        buf = BytesBuffer.allocate(0);
        buf.writeStringWithEnding("ab\uffff");
        buf.forReadWritten();
        Assert.assertEquals("ab\uffff", buf.readStringWithEnding());
    }

    private static String genString(int len) {
        return new String(new char[len]).replace("\0", "a");
    }

    private byte[] genBytes(int len) {
        byte[] bytes = new byte[len];
        Arrays.fill(bytes, (byte) 'a');
        return bytes;
    }

    private byte[] genBytes(String string) {
        int size = string.length() / 2;
        byte[] bytes = new byte[size];
        for (int i = 0; i < size; i++) {
            String b = string.substring(i * 2, i * 2 + 2);
            bytes[i] = Integer.valueOf(b, 16).byteValue();
        }
        return bytes;
    }

    private static PropertyKey genPkey(DataType type) {
        Id id = IdGenerator.of(0L);
        return new FakeObjects().newPropertyKey(id, "fake-name", type);
    }

    private static PropertyKey genListPkey(DataType type) {
        Id id = IdGenerator.of(0L);
        return new FakeObjects().newPropertyKey(id, "fake-name", type,
                                                Cardinality.LIST);
    }

    private static PropertyKey genSetPkey(DataType type) {
        Id id = IdGenerator.of(0L);
        return new FakeObjects().newPropertyKey(id, "fake-name", type,
                                                Cardinality.SET);
    }
}
