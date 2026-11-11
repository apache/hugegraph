/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hugegraph.serializer;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.LinkedHashSet;
import java.util.Random;
import java.util.UUID;

import org.apache.hugegraph.backend.BinaryId;
import org.apache.hugegraph.id.EdgeId;
import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.id.IdGenerator;
import org.apache.hugegraph.struct.schema.PropertyKey;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.type.define.Cardinality;
import org.apache.hugegraph.type.define.DataType;
import org.apache.hugegraph.type.define.Directions;
import org.apache.hugegraph.util.Bytes;
import org.apache.hugegraph.util.StringEncoding;
import org.junit.Assert;
import org.junit.Test;
import org.junit.function.ThrowingRunnable;

/**
 * Behaviour of the struct BytesBuffer that the legacy golden fixtures in
 * compatibility/LegacyCoreCodecTest do not reach: encoding width boundaries,
 * malformed input rejection and buffer capacity rules.
 */
public class BytesBufferTest {

    @Test
    public void testReadVarIntRejectsMalformedEncodings() {
        // Five continuation bytes never terminate within the int width
        assertIllegalArgument(() -> wrap(0x81, 0x81, 0x81, 0x81, 0x81).readVInt(),
                              "too many bytes");
        // A five byte varint can carry only four payload bits in its lead
        assertIllegalArgument(() -> wrap(0x90, 0x80, 0x80, 0x80, 0x00).readVInt(),
                              "leading byte");
        Assert.assertEquals(Integer.MAX_VALUE, wrap(0x87, 0xff, 0xff, 0xff, 0x7f).readVInt());
    }

    @Test
    public void testReadVarLongRejectsMalformedEncodings() {
        int[] neverEnding = new int[10];
        Arrays.fill(neverEnding, 0x81);
        assertIllegalArgument(() -> wrap(neverEnding).readVLong(), "too many bytes");

        byte[] minusOne = BytesBuffer.allocate(16).writeVLong(-1L).bytes();
        Assert.assertEquals(-1L, BytesBuffer.wrap(minusOne).readVLong());
        // A ten byte varlong can carry only one payload bit in its lead
        byte[] overflow = minusOne.clone();
        overflow[0] = (byte) 0x83;
        assertIllegalArgument(() -> BytesBuffer.wrap(overflow).readVLong(), "leading byte");
    }

    @Test
    public void testUnsignedIntegersRoundTripAtTheirLimits() {
        BytesBuffer buffer = BytesBuffer.allocate(16);
        buffer.writeUInt8(BytesBuffer.UINT8_MAX).writeUInt8(0);
        buffer.writeUInt16(BytesBuffer.UINT16_MAX).writeUInt16(0);
        buffer.writeUInt32(BytesBuffer.UINT32_MAX).writeUInt32(0L);
        Assert.assertEquals(14, buffer.position());
        Assert.assertEquals("ff00ffff0000ffffffff00000000", Bytes.toHex(buffer.bytes()));

        buffer.forReadWritten();
        Assert.assertEquals(255, buffer.readUInt8());
        Assert.assertEquals(0, buffer.readUInt8());
        Assert.assertEquals(65535, buffer.readUInt16());
        Assert.assertEquals(0, buffer.readUInt16());
        Assert.assertEquals(4294967295L, buffer.readUInt32());
        Assert.assertEquals(0L, buffer.readUInt32());
    }

    @Test
    public void testBytesAreLengthPrefixedAndLimited() {
        BytesBuffer buffer = BytesBuffer.allocate(4);
        buffer.writeBytes(BytesBuffer.BYTES_EMPTY);
        byte[] big = new byte[300];
        Arrays.fill(big, (byte) 0x5a);
        buffer.writeBigBytes(big);
        // Empty payload is a single zero length; 300 needs a two byte vint
        Assert.assertEquals(1 + 2 + 300, buffer.position());

        buffer.forReadWritten();
        Assert.assertArrayEquals(BytesBuffer.BYTES_EMPTY, buffer.readBytes());
        Assert.assertArrayEquals(big, buffer.readBigBytes());
        Assert.assertEquals(0, buffer.remaining());

        byte[] tooLong = new byte[(int) BytesBuffer.BYTES_LEN_MAX + 1];
        BytesBuffer writer = BytesBuffer.allocate(8);
        assertIllegalArgument(() -> writer.writeBytes(tooLong), "max length of bytes");
        Assert.assertEquals("Rejected bytes must not be partially written", 0, writer.position());
    }

    @Test
    public void testStringsWithEndingAreSequentialAndOrderPreserving() {
        BytesBuffer buffer = BytesBuffer.allocate(8);
        buffer.writeStringWithEnding("ab").writeStringWithEnding("").writeStringWithEnding("顶点");
        buffer.forReadWritten();
        Assert.assertEquals("ab", buffer.readStringWithEnding());
        Assert.assertEquals("", buffer.readStringWithEnding());
        Assert.assertEquals("顶点", buffer.readStringWithEnding());
        Assert.assertEquals(0, buffer.remaining());

        // The 0x00 ending keeps a prefix ordered before its extensions
        byte[] prefix = BytesBuffer.allocate(8).writeStringWithEnding("ab").bytes();
        byte[] longer = BytesBuffer.allocate(8).writeStringWithEnding("ab!").bytes();
        Assert.assertTrue(Bytes.compare(prefix, longer) < 0);

        assertIllegalArgument(() -> wrap('a', 'b').readStringWithEnding(), "Not found ending");
    }

    @Test
    public void testEdgeIdIsMarkedAndValidatedOnRead() {
        EdgeId edge = new EdgeId(IdGenerator.of("alice"), Directions.IN, IdGenerator.of(3L),
                                 IdGenerator.of(4L), "2026", IdGenerator.of(99L));
        byte[] bytes = BytesBuffer.allocate(8).writeId(edge).bytes();
        Assert.assertEquals((byte) 0x7e, bytes[0]);
        BytesBuffer reader = BytesBuffer.wrap(bytes);
        EdgeId actual = (EdgeId) reader.readId();
        Assert.assertEquals(edge, actual);
        Assert.assertEquals(Directions.IN, actual.direction());
        Assert.assertEquals(0, reader.remaining());

        EdgeId nulSort = new EdgeId(IdGenerator.of("alice"), Directions.OUT, IdGenerator.of(3L),
                                    IdGenerator.of(4L), "a\u0000", IdGenerator.of(99L));
        assertIllegalArgument(() -> BytesBuffer.allocate(8).writeId(nulSort), "0x00");

        // A column type byte that is neither EDGE_OUT nor EDGE_IN is corrupt
        BytesBuffer corrupt = BytesBuffer.allocate(32);
        corrupt.writeId(IdGenerator.of("alice"));
        corrupt.write(HugeType.VERTEX.code());
        corrupt.writeId(IdGenerator.of(3L)).writeId(IdGenerator.of(4L));
        corrupt.writeStringWithEnding("").writeId(IdGenerator.of(99L));
        Assert.assertThrows(IllegalStateException.class,
                            () -> corrupt.forReadWritten().readEdgeId());

        byte[] withoutOwner = Arrays.copyOfRange(bytes, 1 + 6, bytes.length);
        assertIllegalArgument(() -> BytesBuffer.wrap(withoutOwner).readEdgeId(false, null),
                              "owner");
    }

    @Test
    public void testIndexIdEndingDependsOnIndexType() {
        byte[] label = {(byte) 150, 1, 2, 3, 4, 'v'};
        byte[] ended = BytesBuffer.allocate(8)
                                  .writeIndexId(new BinaryId(label, null), HugeType.SECONDARY_INDEX)
                                  .bytes();
        Assert.assertEquals(label.length + 1, ended.length);
        Assert.assertEquals(BytesBuffer.STRING_ENDING_BYTE, ended[label.length]);
        byte[] open = BytesBuffer.allocate(8)
                                 .writeIndexId(new BinaryId(label, null),
                                               HugeType.SECONDARY_INDEX, false)
                                 .bytes();
        Assert.assertArrayEquals(label, open);

        byte[] range = {(byte) 160, 0, 0, 0, 3, (byte) 0x80, 0, 0, 7};
        byte[] written = BytesBuffer.allocate(8)
                                    .writeIndexId(new BinaryId(range, null),
                                                  HugeType.RANGE_INT_INDEX)
                                    .bytes();
        Assert.assertArrayEquals("Range index ids have a fixed width and no ending",
                                 range, written);
        // Range ids may legally contain 0x00 while string index ids may not
        byte[] nul = {(byte) 150, 0, 'v'};
        assertIllegalArgument(() -> BytesBuffer.allocate(8).writeIndexId(
                new BinaryId(nul, null), HugeType.SECONDARY_INDEX), "0x00");
        assertIllegalArgument(() -> BytesBuffer.allocate(8).writeIndexId(
                new BinaryId(new byte[0], null), HugeType.RANGE_INT_INDEX), "empty id");
    }

    @Test
    public void testParseIdSkipsPartitionAndOlapPrefixes() {
        Id vertex = IdGenerator.of("v1");
        byte[] vertexBytes = BytesBuffer.allocate(8).writeId(vertex).bytes();

        BytesBuffer partitioned = BytesBuffer.allocate(16);
        partitioned.writeShort((short) 7).writeId(vertex).forReadWritten();
        BinaryId parsed = partitioned.parseId(HugeType.VERTEX, true);
        Assert.assertArrayEquals(vertexBytes, parsed.asBytes());
        Assert.assertEquals(vertex, parsed.origin());
        Assert.assertEquals(0, partitioned.remaining());

        Id propertyKey = IdGenerator.of(5L);
        BytesBuffer olap = BytesBuffer.allocate(16);
        olap.writeId(propertyKey).writeId(vertex).forReadWritten();
        BinaryId olapId = olap.parseOlapId(HugeType.VERTEX, true);
        // The OLAP binary id keeps its property prefix but resolves to the vertex
        byte[] keyBytes = BytesBuffer.allocate(8).writeId(propertyKey).bytes();
        Assert.assertEquals(keyBytes.length + vertexBytes.length, olapId.length());
        Assert.assertEquals(vertex, olapId.origin());
    }

    @Test
    public void testTaggedCollectionsRestoreContainerType() {
        PropertyKey key = new PropertyKey(null, IdGenerator.of(1L), "k");
        UUID first = UUID.fromString("00000000-0000-0000-0000-000000000002");
        UUID second = UUID.fromString("00000000-0000-0000-0000-000000000001");
        BytesBuffer buffer = BytesBuffer.allocate(8);
        buffer.writeProperty(Cardinality.LIST, DataType.TEXT, Arrays.asList("b", "a", "b"));
        buffer.writeProperty(Cardinality.SET, DataType.UUID,
                             new LinkedHashSet<>(Arrays.asList(first, second)));
        buffer.writeProperty(Cardinality.LIST, DataType.LONG, new ArrayList<Long>());
        buffer.forReadWritten();

        Object list = buffer.readProperty(key);
        Assert.assertTrue(list instanceof ArrayList);
        Assert.assertEquals(Arrays.asList("b", "a", "b"), list);
        Assert.assertEquals(Cardinality.LIST, key.cardinality());
        Assert.assertEquals(DataType.TEXT, key.dataType());

        Object set = buffer.readProperty(key);
        Assert.assertTrue(set instanceof LinkedHashSet);
        Assert.assertEquals(Arrays.asList(first, second), new ArrayList<>((LinkedHashSet<?>) set));
        Assert.assertEquals(DataType.UUID, key.dataType());

        Assert.assertEquals(new ArrayList<>(), buffer.readProperty(key));
        Assert.assertEquals(DataType.LONG, key.dataType());
        Assert.assertEquals(0, buffer.remaining());
    }

    @Test
    public void testSkipRejectsTruncatedOrNegativeLengths() {
        PropertyKey text = new PropertyKey(null, IdGenerator.of(1L), "text");
        text.dataType(DataType.TEXT);
        text.cardinality(Cardinality.SINGLE);
        BytesBuffer truncated = BytesBuffer.allocate(8).writeVInt(10).writeStringRaw("abc");
        int start = truncated.forReadWritten().position();
        assertIllegalArgument(() -> truncated.skipSchemaProperty(text), "skipped byte length");
        Assert.assertTrue("Rejected skip must not move past the buffer",
                          truncated.position() <= start + 1);

        PropertyKey list = new PropertyKey(null, IdGenerator.of(2L), "list");
        list.dataType(DataType.INT);
        list.cardinality(Cardinality.LIST);
        BytesBuffer negative = BytesBuffer.allocate(8).writeVInt(-1).forReadWritten();
        assertIllegalArgument(() -> negative.skipSchemaProperty(list), "collection size");
    }

    @Test
    public void testAllocatedBufferGrowsButWrappedBufferDoesNot() {
        BytesBuffer buffer = BytesBuffer.allocate(2);
        buffer.writeShort((short) 0x0102).writeLong(0x0304050607080910L).writeString("xyz");
        Assert.assertEquals(2 + 8 + 1 + 3, buffer.position());
        Assert.assertEquals("010203040506070809100378797a", Bytes.toHex(buffer.bytes()));

        BytesBuffer wrapped = BytesBuffer.wrap(new byte[3]);
        wrapped.writeShort((short) 1);
        Assert.assertThrows(IllegalStateException.class, () -> wrapped.writeShort((short) 2));
        Assert.assertEquals(2, wrapped.position());
    }

    @Test
    public void testIncrementalWritesDoNotRepeatedlyCopyTheEntireBuffer() {
        int blockSize = 4096;
        byte[] block = new byte[blockSize];
        byte[] expected = new byte[1024 * 1024];
        BytesBuffer buffer = BytesBuffer.allocate(BytesBuffer.DEFAULT_CAPACITY);
        int capacity = buffer.array().length;
        int growths = 0;
        for (int offset = 0; offset < expected.length; offset += blockSize) {
            byte value = (byte) (offset / blockSize);
            Arrays.fill(block, value);
            Arrays.fill(expected, offset, offset + blockSize, value);
            buffer.write(block);
            if (buffer.array().length != capacity) {
                capacity = buffer.array().length;
                growths++;
            }
        }
        Assert.assertTrue("Incremental writes must use amortized buffer growth", growths <= 12);
        Assert.assertArrayEquals(expected, buffer.bytes());
    }

    @Test
    public void testGrowthClampsToConfiguredMaximumWithoutLosingBytes() throws Exception {
        Field field = BytesBuffer.class.getDeclaredField("maxBufferCapacity");
        field.setAccessible(true);
        Object previous = field.get(null);
        try {
            field.set(null, 257);
            BytesBuffer buffer = BytesBuffer.allocate(64);
            byte[] expected = new byte[257];
            Arrays.fill(expected, (byte) 0x5a);
            buffer.write(expected, 0, 65);
            Assert.assertTrue(buffer.array().length <= 257);
            buffer.write(expected, 65, expected.length - 65);
            Assert.assertEquals(257, buffer.array().length);
            Assert.assertArrayEquals(expected, buffer.bytes());
            assertIllegalArgument(() -> buffer.write((byte) 1), "exceeds max buffer capacity");
            Assert.assertEquals(257, buffer.position());
            Assert.assertArrayEquals(expected, buffer.bytes());
        } finally {
            field.set(null, previous);
        }
    }

    @Test
    public void testIncompressibleTextRoundTripsAcrossBufferGrowth() {
        Assert.assertArrayEquals(Bytes.fromHex("4c5a34426c6f636b12030000000300000022b24c0d" +
                                               "6162634c5a34426c6f636b12000000000000000000000000"),
                                 StringEncoding.compress("abc"));
        byte[] bytes = new byte[256 * 1024];
        new Random(7).nextBytes(bytes);
        String text = Base64.getEncoder().encodeToString(bytes);
        byte[] compressed = StringEncoding.compress(text);
        Assert.assertEquals(text, StringEncoding.decompress(compressed));
    }

    @Test
    public void testDecompressionGrowsForHighlyCompressibleText() {
        String text = "x".repeat(1024 * 1024);
        byte[] compressed = StringEncoding.compress(text);
        Assert.assertTrue(compressed.length < text.length() / 10);
        Assert.assertEquals(text, StringEncoding.decompress(compressed));
    }

    private static BytesBuffer wrap(int... values) {
        byte[] bytes = new byte[values.length];
        for (int i = 0; i < values.length; i++) {
            bytes[i] = (byte) values[i];
        }
        return BytesBuffer.wrap(bytes);
    }

    private static void assertIllegalArgument(ThrowingRunnable runnable, String message) {
        IllegalArgumentException e = Assert.assertThrows(IllegalArgumentException.class, runnable);
        Assert.assertTrue("Unexpected message: " + e.getMessage(),
                          e.getMessage().contains(message));
    }
}
