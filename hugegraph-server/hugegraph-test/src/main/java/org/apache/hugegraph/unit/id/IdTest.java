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

package org.apache.hugegraph.unit.id;

import java.nio.ByteBuffer;
import java.util.UUID;

import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.id.Id.IdType;
import org.apache.hugegraph.id.IdGenerator;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.unit.BaseUnitTest;
import org.apache.hugegraph.util.NumericUtil;
import org.apache.hugegraph.util.StringEncoding;
import org.junit.Test;

import com.google.common.primitives.Bytes;

public class IdTest extends BaseUnitTest {

    @Test
    public void testStringId() {
        Id id = IdGenerator.of("test-id");

        Assert.assertEquals(IdType.STRING, id.type());
        Assert.assertTrue(id.string());
        Assert.assertFalse(id.number());
        Assert.assertFalse(id.uuid());

        Assert.assertEquals(7, id.length());

        Assert.assertEquals("test-id", id.asString());
        Assert.assertEquals("test-id", id.toString());
        Assert.assertEquals("test-id", id.asObject());
        Assert.assertArrayEquals(StringEncoding.encode("test-id"),
                                 id.asBytes());

        Assert.assertEquals("test-id".hashCode(), id.hashCode());
        Assert.assertEquals(IdGenerator.of("test-id"), id);
        Assert.assertNotEquals(IdGenerator.of("test-id2"), id);

        Assert.assertThrows(IllegalArgumentException.class, id::asLong);

        Assert.assertEquals("test-id", IdGenerator.asStoredString(id));
        Assert.assertEquals(id, IdGenerator.ofStoredString("test-id",
                                                           IdType.STRING));
    }

    @Test
    public void testLongId() {
        Id id = IdGenerator.of(123L);

        Assert.assertEquals(IdType.LONG, id.type());
        Assert.assertFalse(id.string());
        Assert.assertTrue(id.number());
        Assert.assertFalse(id.uuid());

        Assert.assertEquals(8, id.length());

        Assert.assertEquals(123L, id.asLong());
        Assert.assertEquals(123L, id.asObject());
        Assert.assertEquals("123", id.asString());
        Assert.assertEquals("123", id.toString());
        Assert.assertArrayEquals(NumericUtil.longToBytes(123L),
                                 id.asBytes());

        Assert.assertEquals(Long.hashCode(123L), id.hashCode());
        Assert.assertEquals(IdGenerator.of(123L), id);
        Assert.assertEquals(IdGenerator.of(123), id);
        Assert.assertNotEquals(IdGenerator.of(1233), id);
        Assert.assertNotEquals(IdGenerator.of("123"), id);

        Assert.assertEquals("1w", IdGenerator.asStoredString(id));
        Assert.assertEquals(id, IdGenerator.ofStoredString("1w", IdType.LONG));
    }

    @Test
    public void testUuidId() {
        Id id = IdGenerator.of("835e1153-9281-4957-8691-cf79258e90eb", true);

        Assert.assertEquals(IdType.UUID, id.type());
        Assert.assertFalse(id.string());
        Assert.assertFalse(id.number());
        Assert.assertTrue(id.uuid());

        Assert.assertEquals(16, id.length());

        Assert.assertEquals("835e1153-9281-4957-8691-cf79258e90eb",
                            id.asObject().toString());
        Assert.assertEquals("835e1153-9281-4957-8691-cf79258e90eb",
                            id.asString());
        Assert.assertEquals("835e1153-9281-4957-8691-cf79258e90eb",
                            id.toString());

        byte[] h = NumericUtil.longToBytes(
                Long.parseUnsignedLong("835e115392814957", 16));
        byte[] l = NumericUtil.longToBytes(
                Long.parseUnsignedLong("8691cf79258e90eb", 16));
        Assert.assertArrayEquals(Bytes.concat(h, l), id.asBytes());

        Id id2 = IdGenerator.of("835e1153928149578691cf79258e90eb", true);
        Id id3 = IdGenerator.of("835e1153928149578691cf79258e90eb", false);
        Id id4 = IdGenerator.of("835e1153928149578691cf79258e90ee", true);
        Assert.assertEquals(id2.hashCode(), id.hashCode());
        Assert.assertEquals(id2, id);
        Assert.assertNotEquals(id3, id);
        Assert.assertNotEquals(id4, id);

        Assert.assertThrows(UnsupportedOperationException.class, id::asLong);

        Assert.assertEquals("g14RU5KBSVeGkc95JY6Q6w==",
                            IdGenerator.asStoredString(id));
        Assert.assertEquals(id, IdGenerator.ofStoredString(
                "g14RU5KBSVeGkc95JY6Q6w==", IdType.UUID));
    }

    @Test
    public void testObjectId() {
        Object object = ByteBuffer.wrap(new byte[]{1, 2});
        Object object2 = ByteBuffer.wrap(new byte[]{2, 2});
        Id id = IdGenerator.of(object);
        Assert.assertEquals(IdType.UNKNOWN, id.type());
        Assert.assertEquals(object, id.asObject());
        Assert.assertEquals(object.hashCode(), id.hashCode());
        Assert.assertEquals(object.toString(), id.toString());
        Assert.assertEquals(id, IdGenerator.of(object));
        Assert.assertNotEquals(id, IdGenerator.of(object2));
        Assert.assertNotEquals(id, object);

        Assert.assertThrows(UnsupportedOperationException.class, id::asString);
        Assert.assertThrows(UnsupportedOperationException.class, id::asLong);
        Assert.assertThrows(UnsupportedOperationException.class, id::asBytes);
        Assert.assertThrows(UnsupportedOperationException.class, () -> {
            id.compareTo(id);
        });
        Assert.assertThrows(UnsupportedOperationException.class, id::length);
    }

    @Test
    public void testOfObjectId() {
        Object any1 = 123;
        Id id1 = IdGenerator.of(any1);
        Assert.assertEquals(IdType.LONG, id1.type());
        Assert.assertEquals(123L, id1.asObject());

        Object any2 = 123L;
        Id id2 = IdGenerator.of(any2);
        Assert.assertEquals(id1, id2);

        Object any3 = "123";
        Id id3 = IdGenerator.of(any3);
        Assert.assertEquals(IdType.STRING, id3.type());
        Assert.assertEquals("123", id3.asObject());

        Object any4 = "12" + "3";
        Id id4 = IdGenerator.of(any4);
        Assert.assertEquals(id3, id4);

        Object any5 = UUID.randomUUID();
        Id id5 = IdGenerator.of(any5);
        Assert.assertEquals(IdType.UUID, id5.type());
        Assert.assertEquals(any5, id5.asObject());

        Object any6 = UUID.fromString(any5.toString());
        Id id6 = IdGenerator.of(any6);
        Assert.assertEquals(id5, id6);

        Object any7 = ByteBuffer.wrap(new byte[]{1, 2});
        Id id7 = IdGenerator.of(any7);
        Assert.assertEquals(IdType.UNKNOWN, id7.type());
        Assert.assertEquals(ByteBuffer.wrap(new byte[]{1, 2}), id7.asObject());

        Object any8 = ByteBuffer.wrap(new byte[]{1, 2});
        Id id8 = IdGenerator.of(any8);
        Assert.assertEquals(id7, id8);

        Object any9 = id1;
        Id id9 = IdGenerator.of(any9);
        Assert.assertEquals(any9, id9);
    }

    @Test
    public void testIdType() {
        Assert.assertEquals(IdType.LONG, IdType.valueOfPrefix("L"));
        Assert.assertEquals(IdType.UUID, IdType.valueOfPrefix("U"));
        Assert.assertEquals(IdType.STRING, IdType.valueOfPrefix("S"));
        Assert.assertEquals(IdType.EDGE, IdType.valueOfPrefix("E"));
        Assert.assertEquals(IdType.UNKNOWN, IdType.valueOfPrefix("N"));

        Assert.assertEquals('L', IdType.LONG.prefix());
        Assert.assertEquals('U', IdType.UUID.prefix());
        Assert.assertEquals('S', IdType.STRING.prefix());
        Assert.assertEquals('E', IdType.EDGE.prefix());
        Assert.assertEquals('N', IdType.UNKNOWN.prefix());
    }
}
