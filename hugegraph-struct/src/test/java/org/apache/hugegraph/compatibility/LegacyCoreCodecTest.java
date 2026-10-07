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

package org.apache.hugegraph.compatibility;

import java.io.IOException;
import java.io.InputStream;
import java.nio.BufferUnderflowException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedHashSet;
import java.util.Properties;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Locale;
import java.util.UUID;

import org.apache.hugegraph.backend.BinaryId;
import org.apache.hugegraph.id.EdgeId;
import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.id.IdGenerator;
import org.apache.hugegraph.query.Aggregate.AggregateFunc;
import org.apache.hugegraph.query.Condition;
import org.apache.hugegraph.query.ConditionQuery;
import org.apache.hugegraph.serializer.BytesBuffer;
import org.apache.hugegraph.serializer.BytesBuffer.IndexColumnName;
import org.apache.hugegraph.serializer.BytesBuffer.IndexExpiryLayout;
import org.apache.hugegraph.struct.schema.PropertyKey;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.type.define.Cardinality;
import org.apache.hugegraph.type.define.DataType;
import org.apache.hugegraph.type.define.Directions;
import org.apache.hugegraph.type.define.HugeKeys;
import org.apache.hugegraph.util.Blob;
import org.apache.hugegraph.util.Bytes;
import org.junit.Assert;
import org.junit.Test;

import com.google.gson.JsonParser;

/**
 * Golden fixtures were written by the pre-consolidation core, not this module.
 * See compatibility/2f827d6e8/README.md for provenance and coverage limits.
 */
public class LegacyCoreCodecTest {

    private static final String FIXTURES = "/compatibility/2f827d6e8/";

    @Test
    public void testLegacyLongIdsAndVarLongs() throws IOException {
        long[] values = {Long.MIN_VALUE, -16385L, -16384L, -129L, -128L, -1L,
                         0L, 1L, 127L, 128L, 16383L, 16384L, Long.MAX_VALUE};
        for (long value : values) {
            byte[] idBytes = fixture("id.long." + value);
            BytesBuffer reader = BytesBuffer.wrap(idBytes);
            Id actual = reader.readId();
            Assert.assertEquals(IdGenerator.of(value), actual);
            Assert.assertEquals(0, reader.remaining());
            Assert.assertArrayEquals(idBytes,
                                     BytesBuffer.allocate(64).writeId(actual).bytes());

            byte[] varLongBytes = fixture("vlong." + value);
            reader = BytesBuffer.wrap(varLongBytes);
            Assert.assertEquals(value, reader.readVLong());
            Assert.assertEquals(0, reader.remaining());
            Assert.assertArrayEquals(varLongBytes,
                                     BytesBuffer.allocate(64).writeVLong(value).bytes());
        }
    }

    @Test
    public void testLegacyVarInts() throws IOException {
        int[] values = {Integer.MIN_VALUE, -129, -1, 0, 127, 128, 16383, 16384,
                        Integer.MAX_VALUE};
        for (int value : values) {
            byte[] expected = fixture("vint." + value);
            BytesBuffer reader = BytesBuffer.wrap(expected);
            Assert.assertEquals(value, reader.readVInt());
            Assert.assertEquals(0, reader.remaining());
            Assert.assertArrayEquals(expected,
                                     BytesBuffer.allocate(64).writeVInt(value).bytes());
        }
    }

    @Test
    public void testLegacyTextAndUuidIds() throws IOException {
        assertId("id.text", IdGenerator.of("alice"));
        assertId("id.unicode", IdGenerator.of("顶点😀"));
        assertId("id.uuid", IdGenerator.of(
                 UUID.fromString("01234567-89ab-cdef-0123-456789abcdef")));
    }

    @Test
    public void testLegacyTextIdWidthBoundaries() throws IOException {
        for (int length : new int[]{1, 64, 65, 256, 257, 16384}) {
            String key = "id.text.length." + length;
            Id expected = IdGenerator.of("x".repeat(length));
            assertId(key, expected);
            // The historical struct boolean overload used the same layout.
            byte[] legacy = fixture(key);
            BytesBuffer reader = BytesBuffer.wrap(legacy);
            Assert.assertEquals(expected, reader.readId(true));
            Assert.assertEquals(0, reader.remaining());
            Assert.assertArrayEquals(legacy,
                                     BytesBuffer.allocate(64).writeId(expected, true).bytes());
        }
        Assert.assertThrows(IllegalArgumentException.class,
                            () -> BytesBuffer.allocate(64).writeId(IdGenerator.of("")));
        Assert.assertThrows(IllegalArgumentException.class,
                            () -> BytesBuffer.allocate(64).writeId(IdGenerator.of("x".repeat(16385))));
    }

    @Test
    public void testLegacyEdgeIds() throws IOException {
        assertEdge("edge.out", new EdgeId(IdGenerator.of("alice"), Directions.OUT,
                   IdGenerator.of(3L), IdGenerator.of(4L), "2026!x", IdGenerator.of(99L)));
        assertEdge("edge.in", new EdgeId(IdGenerator.of(99L), Directions.IN,
                   IdGenerator.of(3L), IdGenerator.of(4L), "2026!x", IdGenerator.of("alice")));
    }

    @Test
    public void testLegacyEdgeKeyPrefixAndSortSkipping() throws IOException {
        byte[] legacy = fixture("edge.out");
        Id owner = IdGenerator.of("alice");
        byte[] withoutOwner = Arrays.copyOfRange(legacy, fixture("id.text").length, legacy.length);
        BytesBuffer prefixless = BytesBuffer.wrap(withoutOwner);
        EdgeId edge = prefixless.readEdgeId(false, owner);
        Assert.assertEquals(owner, edge.ownerVertexId());
        Assert.assertEquals(Directions.OUT, edge.direction());
        Assert.assertEquals(IdGenerator.of(3L), edge.edgeLabelId());
        Assert.assertEquals(IdGenerator.of(4L), edge.subLabelId());
        Assert.assertEquals("2026!x", edge.sortValues());
        Assert.assertEquals(IdGenerator.of(99L), edge.otherVertexId());
        Assert.assertEquals(0, prefixless.remaining());
        BytesBuffer skipped = BytesBuffer.wrap(legacy);
        EdgeId withoutSort = (EdgeId) skipped.readEdgeIdSkipSortValues();
        Assert.assertEquals(owner, withoutSort.ownerVertexId());
        Assert.assertEquals("", withoutSort.sortValues());
        Assert.assertEquals(IdGenerator.of(99L), withoutSort.otherVertexId());
        Assert.assertEquals(0, skipped.remaining());
    }

    @Test
    public void testLegacyIndexKeyLayoutBoundaries() throws IOException {
        Properties rows = new Properties();
        try (InputStream input = this.getClass().getResourceAsStream(FIXTURES + "core-rows.properties")) {
            Assert.assertNotNull(input);
            rows.load(input);
        }
        byte[] timed = Bytes.fromHex(rows.getProperty("index.secondary.column.0.name"));
        byte[] indexPrefix = Bytes.fromHex(rows.getProperty("index.secondary.id"));
        byte[] prefixless = Arrays.copyOfRange(timed, indexPrefix.length, timed.length);
        BytesBuffer withPrefix = BytesBuffer.wrap(timed);
        IndexColumnName parsed = withPrefix.readIndexColumnName(HugeType.SECONDARY_INDEX, true,
                                                               IndexExpiryLayout.REQUIRED);
        Assert.assertArrayEquals(Arrays.copyOf(indexPrefix, indexPrefix.length - 1), parsed.indexId().asBytes());
        Assert.assertEquals(IdGenerator.of(1L), parsed.elementId());
        Assert.assertEquals(1609459260000L, parsed.expiredTime());
        Assert.assertTrue(parsed.hasExpiredTime());
        Assert.assertEquals(0, withPrefix.remaining());
        BytesBuffer noPrefix = BytesBuffer.wrap(prefixless);
        parsed = noPrefix.readIndexColumnName(HugeType.SECONDARY_INDEX, false, IndexExpiryLayout.REQUIRED);
        Assert.assertNull(parsed.indexId());
        Assert.assertEquals(IdGenerator.of(1L), parsed.elementId());
        Assert.assertEquals(1609459260000L, parsed.expiredTime());
        Assert.assertEquals(0, noPrefix.remaining());
        byte[] bare = Bytes.fromHex(rows.getProperty("index.system.column.0.name"));
        parsed = BytesBuffer.wrap(bare).readIndexColumnName(HugeType.VERTEX_LABEL_INDEX, true,
                                                           IndexExpiryLayout.OPTIONAL);
        Assert.assertEquals(0L, parsed.expiredTime());
        Assert.assertFalse(parsed.hasExpiredTime());
        byte[] withZero = Arrays.copyOf(bare, bare.length + 1);
        withZero[bare.length] = fixture("vlong.0")[0];
        parsed = BytesBuffer.wrap(withZero).readIndexColumnName(HugeType.VERTEX_LABEL_INDEX, true,
                                                               IndexExpiryLayout.OPTIONAL);
        Assert.assertEquals(0L, parsed.expiredTime());
        Assert.assertTrue(parsed.hasExpiredTime());
        Assert.assertThrows(BufferUnderflowException.class,
                            () -> BytesBuffer.wrap(bare).readIndexColumnName(
                                  HugeType.VERTEX_LABEL_INDEX, true, IndexExpiryLayout.REQUIRED));
    }

    @Test
    public void testLegacyScalarProperties() throws IOException {
        assertProperty(DataType.BOOLEAN, true);
        assertProperty(DataType.BYTE, (byte) -7);
        assertProperty(DataType.INT, -123456);
        assertProperty(DataType.LONG, Long.MIN_VALUE);
        assertProperty(DataType.FLOAT, 1.25f);
        assertProperty(DataType.DOUBLE, -123.125d);
        assertProperty(DataType.DATE, new Date(1609459200123L));
        assertProperty(DataType.TEXT, "属性😀\u0000");
        assertProperty(DataType.BLOB, Blob.wrap(new byte[]{0, -1, 1, 127, -128}));
        assertProperty(DataType.UUID, UUID.fromString("01234567-89ab-cdef-0123-456789abcdef"));
    }

    @Test
    public void testLegacyKryoProperties() throws IOException {
        assertProperty(DataType.OBJECT, new ArrayList<>(Arrays.asList("legacy", 7, true)));
        assertProperty(DataType.UNKNOWN, "legacy-object");
    }

    @Test
    public void testLegacySchemaDrivenProperties() throws IOException {
        assertSchemaProperty("single", Cardinality.SINGLE, 7);
        assertSchemaProperty("list", Cardinality.LIST, Arrays.asList(7, -1, 7));
        assertSchemaProperty("set", Cardinality.SET,
                             new LinkedHashSet<>(Arrays.asList(7, -1)));
    }

    @Test
    public void testLegacyIndexIds() throws IOException {
        assertIndex("index.string", HugeType.SECONDARY_INDEX,
                    "label!alice".getBytes(StandardCharsets.UTF_8));
        assertIndex("index.int", HugeType.RANGE_INT_INDEX,
                    new byte[]{(byte) 160, 0, 0, 0, 3, (byte) 128, 0, 0, 7});
        assertIndex("index.long", HugeType.RANGE_LONG_INDEX,
                    new byte[]{(byte) 162, 0, 0, 0, 3, (byte) 128, 0, 0, 0, 0, 0, 0, 7});
    }

    @Test
    public void testLegacyTaggedScalarProperties() throws IOException {
        assertTaggedScalar(DataType.BOOLEAN, true);
        assertTaggedScalar(DataType.BYTE, (byte) -7);
        assertTaggedScalar(DataType.INT, -123456);
        assertTaggedScalar(DataType.LONG, Long.MIN_VALUE);
        assertTaggedScalar(DataType.FLOAT, 1.25f);
        assertTaggedScalar(DataType.DOUBLE, -123.125d);
        assertTaggedScalar(DataType.DATE, new Date(1609459200123L));
        assertTaggedScalar(DataType.TEXT, "属性😀\u0000");
        assertTaggedScalar(DataType.BLOB, Blob.wrap(new byte[]{0, -1, 1, 127, -128}));
        assertTaggedScalar(DataType.UUID,
                           UUID.fromString("01234567-89ab-cdef-0123-456789abcdef"));
    }

    @Test
    public void testLegacyTaggedCollectionProperties() throws IOException {
        assertTaggedProperty("list", DataType.INT, Cardinality.LIST,
                             Arrays.asList(7, -1, 7), "schema.list");
        assertTaggedProperty("set", DataType.INT, Cardinality.SET,
                             new LinkedHashSet<>(Arrays.asList(7, -1)), "schema.set");
    }

    @Test
    public void testSkippingLegacyPropertiesPreservesTrailingTtl() throws IOException {
        for (DataType type : DataType.values()) {
            this.assertSkipProperty("property." + type.name(), type, Cardinality.SINGLE);
            byte[] legacy = type == DataType.UNKNOWN || type == DataType.OBJECT ? null :
                            fixture("struct-tagged-properties.hex", type.name());
            if (legacy != null) {
                BytesBuffer buffer = withTrailingMarker(legacy);
                buffer.skipProperty();
                assertTrailingMarker(buffer);
            }
        }
        this.assertSkipProperty("schema.list", DataType.INT, Cardinality.LIST);
        this.assertSkipProperty("schema.set", DataType.INT, Cardinality.SET);
        // An existing core Kryo limitation must not prevent skipping data:
        // this collection's baseline writer can encode it, but its reader
        // cannot instantiate Arrays$ArrayList.
        this.assertSkipProperty("property.OBJECT_UNSUPPORTED_COLLECTION",
                                DataType.OBJECT, Cardinality.SINGLE);
    }

    private void assertSkipProperty(String name, DataType type, Cardinality cardinality)
            throws IOException {
        PropertyKey key = new PropertyKey(null, IdGenerator.of(101L), "legacy");
        key.dataType(type);
        key.cardinality(cardinality);
        BytesBuffer buffer = withTrailingMarker(fixture(name));
        buffer.skipSchemaProperty(key);
        assertTrailingMarker(buffer);
    }

    private static BytesBuffer withTrailingMarker(byte[] bytes) {
        BytesBuffer buffer = BytesBuffer.allocate(bytes.length + 16);
        buffer.write(bytes);
        buffer.writeUInt8(0x55);
        buffer.writeVLong(1609459260000L);
        return buffer.forReadWritten();
    }

    private static void assertTrailingMarker(BytesBuffer buffer) {
        Assert.assertEquals(0x55, buffer.readUInt8());
        Assert.assertEquals(1609459260000L, buffer.readVLong());
        Assert.assertEquals(0, buffer.remaining());
    }

    @Test
    public void testLegacyVertexQuery() throws IOException, ParseException {
        // Legacy JSON date strings represent wall time in the process time
        // zone. Do not mutate it after Gson's date adapter initialization.
        ConditionQuery query = ConditionQuery.fromBytes(jsonFixture("core-query.json.txt"));
        Assert.assertEquals(HugeType.VERTEX, query.resultType());
        Assert.assertEquals(7L, query.offset());
        Assert.assertEquals(19L, query.limit());
        Assert.assertEquals(IdGenerator.of(3L), query.condition(HugeKeys.LABEL));
        Condition.Relation range = query.relation(IdGenerator.of(11L));
        Assert.assertEquals(Condition.RelationType.GTE, range.relation());
        Assert.assertEquals(18, range.value());
        Assert.assertNull(query.condition(IdGenerator.of(11L)));
        Assert.assertEquals(Arrays.asList(20L, 30L), query.condition(IdGenerator.of(12L)));
        Date expectedDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS", Locale.ROOT)
                        .parse("2021-01-01 00:00:00.123");
        Assert.assertEquals(expectedDate, query.condition(IdGenerator.of(13L)));
        Assert.assertEquals(5, query.conditions().size());
        Condition disjunction = query.conditions().stream()
                                     .filter(c -> c instanceof Condition.Or)
                                     .findFirst().orElseThrow(AssertionError::new);
        Assert.assertTrue(disjunction.test("alice"));
        Assert.assertTrue(disjunction.test("bob"));
        Assert.assertFalse(disjunction.test("charlie"));
    }

    @Test
    public void testLegacyEdgeQuery() throws IOException {
        ConditionQuery query = ConditionQuery.fromBytes(jsonFixture("core-edge-query.json.txt"));
        Assert.assertEquals(HugeType.EDGE, query.resultType());
        Assert.assertEquals(IdGenerator.of("alice"), query.condition(HugeKeys.OWNER_VERTEX));
        Assert.assertEquals(Directions.OUT, query.condition(HugeKeys.DIRECTION));
        Assert.assertEquals(IdGenerator.of(3L), query.condition(HugeKeys.LABEL));
        Assert.assertEquals("AQIDBA==", query.page());
        Assert.assertEquals(5L, query.limit());
        Assert.assertEquals(3, query.conditions().size());
    }

    @Test
    public void testLegacyCountAggregateQuery() throws IOException {
        byte[] payload = jsonFixture("core-count-query.json.txt");
        ConditionQuery query = ConditionQuery.fromBytes(payload);
        Assert.assertEquals(AggregateFunc.COUNT, query.aggregateNotNull().func());
        Assert.assertTrue(query.aggregateNotNull().countAll());
        Assert.assertNull(query.aggregateNotNull().column());
        Assert.assertEquals(5L, query.aggregateNotNull()
                                        .reduce(Arrays.<Number>asList(2L, 3L).iterator()).longValue());
        assertAggregateJson(payload, query.bytes());
    }

    @Test
    public void testLegacyAvgAggregateQuery() throws IOException {
        byte[] payload = jsonFixture("core-avg-query.json.txt");
        ConditionQuery query = ConditionQuery.fromBytes(payload);
        Assert.assertEquals(AggregateFunc.AVG, query.aggregateNotNull().func());
        Assert.assertFalse(query.aggregateNotNull().countAll());
        Assert.assertEquals("age", query.aggregateNotNull().column());
        Assert.assertEquals(25d, query.aggregateNotNull()
                                        .reduce(Arrays.<Number>asList(20, 30).iterator()).doubleValue(),
                            0d);
        Assert.assertEquals(IdGenerator.of(3L), query.condition(HugeKeys.LABEL));
        assertAggregateJson(payload, query.bytes());
    }

    private static void assertAggregateJson(byte[] legacy, byte[] current) {
        Assert.assertEquals(JsonParser.parseString(new String(legacy, StandardCharsets.UTF_8))
                                      .getAsJsonObject().get("aggregate"),
                            JsonParser.parseString(new String(current, StandardCharsets.UTF_8))
                                      .getAsJsonObject().get("aggregate"));
    }

    private static void assertId(String key, Id expected) throws IOException {
        byte[] legacy = fixture(key);
        BytesBuffer reader = BytesBuffer.wrap(legacy);
        Assert.assertEquals(expected, reader.readId());
        Assert.assertEquals(0, reader.remaining());
        Assert.assertArrayEquals(legacy, BytesBuffer.allocate(64).writeId(expected).bytes());
    }

    private static void assertEdge(String key, EdgeId expected) throws IOException {
        byte[] legacy = fixture(key);
        BytesBuffer reader = BytesBuffer.wrap(legacy);
        EdgeId actual = (EdgeId) reader.readEdgeId();
        Assert.assertEquals(expected.ownerVertexId(), actual.ownerVertexId());
        Assert.assertEquals(expected.direction(), actual.direction());
        Assert.assertEquals(expected.edgeLabelId(), actual.edgeLabelId());
        Assert.assertEquals(expected.subLabelId(), actual.subLabelId());
        Assert.assertEquals(expected.sortValues(), actual.sortValues());
        Assert.assertEquals(expected.otherVertexId(), actual.otherVertexId());
        Assert.assertEquals(0, reader.remaining());
        Assert.assertArrayEquals(legacy, BytesBuffer.allocate(128).writeEdgeId(expected).bytes());
    }

    private static void assertProperty(DataType type, Object expected) throws IOException {
        byte[] legacy = fixture("property." + type.name());
        BytesBuffer reader = BytesBuffer.wrap(legacy);
        Assert.assertEquals(expected, reader.readProperty(type));
        Assert.assertEquals(0, reader.remaining());
        BytesBuffer writer = BytesBuffer.allocate(128);
        writer.writeProperty(type, expected);
        Assert.assertArrayEquals(legacy, writer.bytes());
    }

    private static void assertSchemaProperty(String name, Cardinality cardinality,
                                             Object expected) throws IOException {
        PropertyKey key = new PropertyKey(null, IdGenerator.of(101L), "legacy");
        key.dataType(DataType.INT);
        key.cardinality(cardinality);
        byte[] legacy = fixture("schema." + name);
        BytesBuffer reader = BytesBuffer.wrap(legacy);
        Assert.assertEquals(expected, reader.readSchemaProperty(key));
        Assert.assertEquals(0, reader.remaining());
        Assert.assertEquals(cardinality, key.cardinality());
        Assert.assertEquals(DataType.INT, key.dataType());
        Assert.assertArrayEquals(legacy,
                                 BytesBuffer.allocate(64).writeSchemaProperty(key, expected).bytes());
    }

    private static void assertIndex(String name, HugeType type, byte[] expected)
            throws IOException {
        byte[] legacy = fixture(name);
        BytesBuffer reader = BytesBuffer.wrap(legacy);
        Assert.assertArrayEquals(expected, reader.readIndexId(type).asBytes());
        Assert.assertEquals(0, reader.remaining());
        Assert.assertArrayEquals(legacy, BytesBuffer.allocate(64)
                                                   .writeIndexId(new BinaryId(expected, null), type)
                                                   .bytes());
    }

    private static void assertTaggedScalar(DataType type, Object expected) throws IOException {
        assertTaggedProperty(type.name(), type, Cardinality.SINGLE,
                             expected, "property." + type.name());
    }

    private static void assertTaggedProperty(String name, DataType type,
                                             Cardinality cardinality, Object expected,
                                             String rawKey) throws IOException {
        byte[] legacy = fixture("struct-tagged-properties.hex", name);
        byte[] raw = fixture(rawKey);
        Assert.assertEquals(raw.length + 1, legacy.length);
        Assert.assertArrayEquals(raw, Arrays.copyOfRange(legacy, 1, legacy.length));
        PropertyKey key = new PropertyKey(null, IdGenerator.of(101L), "legacy");
        // Tagged readers must recover metadata, even when the caller starts
        // with metadata different from the encoded property.
        key.dataType(DataType.UNKNOWN);
        key.cardinality(Cardinality.SET);
        BytesBuffer reader = BytesBuffer.wrap(legacy);
        Assert.assertEquals(expected, reader.readProperty(key));
        Assert.assertEquals(0, reader.remaining());
        Assert.assertEquals(type, key.dataType());
        Assert.assertEquals(cardinality, key.cardinality());
        Assert.assertArrayEquals(legacy,
                                 BytesBuffer.allocate(128).writeProperty(key, expected).bytes());
    }

    private static byte[] fixture(String key) throws IOException {
        return fixture("core-codecs.hex", key);
    }

    private static byte[] fixture(String file, String key) throws IOException {
        Properties properties = new Properties();
        try (InputStream input = LegacyCoreCodecTest.class.getResourceAsStream(
                                 FIXTURES + file)) {
            Assert.assertNotNull("Missing codec fixture resource", input);
            properties.load(input);
        }
        String hex = properties.getProperty(key);
        Assert.assertNotNull("Missing legacy fixture " + key, hex);
        return Bytes.fromHex(hex);
    }

    private static byte[] jsonFixture(String name) throws IOException {
        try (InputStream input = LegacyCoreCodecTest.class.getResourceAsStream(FIXTURES + name)) {
            Assert.assertNotNull("Missing legacy fixture " + name, input);
            String text = new String(input.readAllBytes(), StandardCharsets.UTF_8);
            Assert.assertTrue("Missing fixture license prolog", text.startsWith("/*"));
            int payloadStart = text.indexOf("*/\n") + 3;
            Assert.assertTrue("Missing fixture payload", payloadStart > 3);
            Assert.assertTrue("Missing fixture file newline", text.endsWith("\n"));
            return text.substring(payloadStart, text.length() - 1).getBytes(StandardCharsets.UTF_8);
        }
    }
}
