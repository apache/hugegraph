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

package org.apache.hugegraph.id;

import java.util.UUID;

import org.apache.hugegraph.backend.BinaryId;
import org.apache.hugegraph.exception.NotFoundException;
import org.apache.hugegraph.serializer.BytesBuffer;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.type.define.Directions;
import org.junit.Assert;
import org.junit.Test;

/**
 * Text form and parsing rules of the struct EdgeId and SplicingIdGenerator.
 * Server unit tests cover successful parsing and equality; this class covers
 * the directed form, escaping and the rejection paths.
 */
public class EdgeIdTest {

    private static final Id OWNER = IdGenerator.of("alice");
    private static final Id OTHER = IdGenerator.of(42L);
    private static final Id LABEL = IdGenerator.of(3L);
    private static final Id SUB_LABEL = IdGenerator.of(4L);

    @Test
    public void testUndirectedTextIsWrittenFromSourceToTarget() {
        EdgeId in = new EdgeId(OWNER, Directions.IN, LABEL, SUB_LABEL, "s", OTHER);
        // An IN edge stored under alice is the OUT edge 42 -> alice
        Assert.assertEquals("L42>3>4>s>Salice", in.asString());

        EdgeId out = in.switchDirection();
        Assert.assertEquals(OTHER, out.ownerVertexId());
        Assert.assertEquals(Directions.OUT, out.direction());
        Assert.assertEquals(OWNER, out.otherVertexId());
        Assert.assertEquals(in.asString(), out.asString());
        Assert.assertEquals(in, out);
        Assert.assertEquals(in.hashCode(), out.hashCode());
        Assert.assertEquals(0, in.compareTo(out));

        EdgeId parsed = EdgeId.parse(in.asString());
        Assert.assertEquals(Directions.OUT, parsed.direction());
        Assert.assertEquals(OTHER, parsed.ownerVertexId());
        Assert.assertEquals(in, parsed);
    }

    @Test
    public void testDirectedTextKeepsOwnerAndDirection() {
        EdgeId directed = new EdgeId(OWNER, Directions.IN, LABEL, SUB_LABEL, "s", OTHER, true);
        Assert.assertEquals("Salice>I>3>4>s>L42", directed.asString());
        // Directed identity distinguishes the two sides of the same edge
        Assert.assertNotEquals(directed, directed.switchDirection());
        Assert.assertEquals(directed, directed.directed(false).directed(true));

        EdgeId parsed = EdgeId.parse(directed.asString());
        Assert.assertEquals(OWNER, parsed.ownerVertexId());
        Assert.assertEquals(Directions.IN, parsed.direction());
        Assert.assertEquals(LABEL, parsed.edgeLabelId());
        Assert.assertEquals(SUB_LABEL, parsed.subLabelId());
        Assert.assertEquals("s", parsed.sortValues());
        Assert.assertEquals(OTHER, parsed.otherVertexId());
        Assert.assertEquals(directed.directed(false), parsed);
    }

    @Test
    public void testSeparatorsInsidePartsSurviveTextRoundTrip() {
        Id owner = IdGenerator.of("a>b");
        EdgeId edge = new EdgeId(owner, Directions.OUT, LABEL, SUB_LABEL, "x>y`z", OTHER);
        Assert.assertEquals("Sa`>b>3>4>x`>y`z>L42", edge.asString());
        Assert.assertEquals(5, EdgeId.split(edge).length);

        EdgeId parsed = EdgeId.parse(edge.asString());
        Assert.assertEquals(owner, parsed.ownerVertexId());
        Assert.assertEquals("x>y`z", parsed.sortValues());
        Assert.assertEquals(edge, parsed);
    }

    @Test
    public void testParseRejectsMalformedText() {
        String[] invalid = {
                "Salice>3>4>L42",            // too few parts
                "Salice>O>3>4>s>L42>extra",  // too many parts
                "Salice>x>4>s>L42",          // non numeric label
                "Salice>3>4>s>Lnan",         // non numeric long vertex
                "Salice>Z>3>4>s>L42",        // unknown direction
                "Salice>VL>3>4>s>L42"        // a type that is not a direction
        };
        for (String text : invalid) {
            Assert.assertThrows(text, NotFoundException.class, () -> EdgeId.parse(text));
            Assert.assertNull(text, EdgeId.parse(text, true));
        }
        NotFoundException e = Assert.assertThrows(NotFoundException.class,
                                                  () -> EdgeId.parse("a>b"));
        Assert.assertTrue(e.getMessage().contains("5~6 parts, but got 2 parts"));
        e = Assert.assertThrows(NotFoundException.class, () -> EdgeId.parse("Salice>x>4>s>L42"));
        Assert.assertTrue(e.getCause() instanceof NumberFormatException);
    }

    @Test
    public void testStoredStringRequiresFiveParts() {
        EdgeId edge = new EdgeId(OWNER, Directions.IN, LABEL, SUB_LABEL, "s", OTHER);
        String stored = EdgeId.asStoredString(edge);
        Assert.assertEquals(edge, EdgeId.parseStoredString(stored));

        Assert.assertThrows(IllegalArgumentException.class,
                            () -> EdgeId.parseStoredString("Salice>D>E>s"));
        Assert.assertThrows(IllegalArgumentException.class,
                            () -> EdgeId.parseStoredString(stored + ">extra"));
    }

    @Test
    public void testDirectionCodes() {
        Assert.assertEquals(HugeType.EDGE_OUT.code(), EdgeId.directionToCode(Directions.OUT));
        Assert.assertEquals(HugeType.EDGE_IN.code(), EdgeId.directionToCode(Directions.IN));
        Assert.assertTrue(EdgeId.isOutDirectionFromCode(HugeType.EDGE_OUT.code()));
        Assert.assertFalse(EdgeId.isOutDirectionFromCode(HugeType.EDGE_IN.code()));
        Assert.assertThrows(IllegalArgumentException.class,
                            () -> EdgeId.directionFromCode(HugeType.VERTEX.code()));
        Assert.assertThrows(IllegalArgumentException.class,
                            () -> EdgeId.directionToCode(Directions.BOTH));
        Assert.assertThrows(UnsupportedOperationException.class,
                            () -> new EdgeId(OWNER, Directions.OUT, LABEL, SUB_LABEL, "",
                                             OTHER).asLong());
    }

    @Test
    public void testSplicingEscapesOnlyItsOwnSeparator() {
        Id spliced = SplicingIdGenerator.splicing("1", "a:b", "c>d");
        Assert.assertEquals("1:a`:b:c>d", spliced.asString());
        Assert.assertArrayEquals(new String[]{"1", "a:b", "c>d"},
                                 SplicingIdGenerator.parse(spliced));

        Id raw = SplicingIdGenerator.splicingWithNoEscape("1", "a:b");
        Assert.assertArrayEquals(new String[]{"1", "a", "b"}, SplicingIdGenerator.parse(raw));

        Assert.assertEquals("v`!1!2!", SplicingIdGenerator.concatValues("v!1", 2, ""));
        Assert.assertArrayEquals(new String[]{"", ""}, SplicingIdGenerator.split(">"));
    }

    @Test
    public void testGenerateBinaryIdMatchesBinaryIdLayout() {
        Id[] ids = {IdGenerator.of("v1"), IdGenerator.of(1L << 40), IdGenerator.of(-1L),
                    IdGenerator.of(UUID.fromString("00112233-4455-6677-8899-aabbccddeeff"))};
        for (Id id : ids) {
            Id binary = SplicingIdGenerator.generateBinaryId(id);
            Assert.assertTrue(binary instanceof BinaryId);
            Assert.assertArrayEquals(BytesBuffer.allocate(32).writeId(id).bytes(), binary.asBytes());
            Assert.assertEquals(id, ((BinaryId) binary).origin());
            Assert.assertEquals(id, BytesBuffer.wrap(binary.asBytes()).readId());
            // Already binary ids are returned unchanged
            Assert.assertSame(binary, SplicingIdGenerator.generateBinaryId(binary));
        }
    }
}
