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

package org.apache.hugegraph.unit.core;

import java.math.BigInteger;
import java.math.BigDecimal;
import java.util.Date;
import java.util.UUID;

import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.testutil.Utils;
import org.apache.hugegraph.type.define.DataType;
import org.junit.Test;

public class DataTypeTest {

    @Test
    public void testString() {
        Assert.assertEquals("object", DataType.OBJECT.string());
        Assert.assertEquals("boolean", DataType.BOOLEAN.string());
        Assert.assertEquals("byte", DataType.BYTE.string());
        Assert.assertEquals("int", DataType.INT.string());
        Assert.assertEquals("long", DataType.LONG.string());
        Assert.assertEquals("float", DataType.FLOAT.string());
        Assert.assertEquals("double", DataType.DOUBLE.string());
        Assert.assertEquals("text", DataType.TEXT.string());
        Assert.assertEquals("blob", DataType.BLOB.string());
        Assert.assertEquals("date", DataType.DATE.string());
        Assert.assertEquals("uuid", DataType.UUID.string());
        Assert.assertEquals("decimal", DataType.DECIMAL.string());
    }

    @Test
    public void testValueToNumber() {
        Assert.assertNull(DataType.BOOLEAN.valueToNumber(1));
        Assert.assertNull(DataType.INT.valueToNumber("not number"));
        // decimal is not a "number" in the fixed-width sense
        Assert.assertNull(DataType.DECIMAL.valueToNumber(1));

        Assert.assertEquals((byte) 1, DataType.BYTE.valueToNumber(1));
        Assert.assertEquals(1, DataType.INT.valueToNumber(1));
        Assert.assertEquals(1, DataType.INT.valueToNumber((byte) 1));
        Assert.assertEquals(1L, DataType.LONG.valueToNumber(1));
        Assert.assertEquals(1.0F, DataType.FLOAT.valueToNumber(1));
        Assert.assertEquals(1.0D, DataType.DOUBLE.valueToNumber(1));

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            DataType.INT.valueToNumber(1.0F);
        }, e -> {
            Assert.assertContains("Can't read '1.0' as int", e.getMessage());
        });
    }

    @Test
    public void testValueToDate() {
        Date date = Utils.date("2019-01-01 12:00:00");
        Assert.assertEquals(date, DataType.DATE.valueToDate(date));
        Assert.assertEquals(date,
                            DataType.DATE.valueToDate("2019-01-01 12:00:00"));
        Assert.assertEquals(date, DataType.DATE.valueToDate(date.getTime()));

        Assert.assertNull(DataType.TEXT.valueToDate("2019-01-01 12:00:00"));
        Assert.assertNull(DataType.DATE.valueToDate(true));
    }

    @Test
    public void testValueToUUID() {
        UUID uuid = UUID.randomUUID();
        Assert.assertEquals(uuid, DataType.UUID.valueToUUID(uuid));
        Assert.assertEquals(uuid, DataType.UUID.valueToUUID(uuid.toString()));

        Assert.assertNull(DataType.TEXT.valueToUUID("2019-01-01 12:00:00"));
        Assert.assertNull(DataType.UUID.valueToUUID(true));
    }

    @Test
    public void testDecimal() {
        Assert.assertTrue(DataType.DECIMAL.isDecimal());
        Assert.assertFalse(DataType.DECIMAL.isNumber());
        Assert.assertFalse(DataType.DECIMAL.isNumber4());
        Assert.assertFalse(DataType.DECIMAL.isNumber8());
        Assert.assertFalse(DataType.DOUBLE.isDecimal());
        Assert.assertEquals(BigDecimal.class, DataType.DECIMAL.clazz());
        Assert.assertEquals(DataType.DECIMAL,
                            DataType.fromClass(BigDecimal.class));
    }

    @Test
    public void testValueToDecimalBounds() {
        // a huge exponent is a few bytes on disk and a billion characters
        // from toPlainString() on every read: rejected before it is stored
        for (String bad : new String[]{"1E+999999999", "1E-999999999",
                                       "1E+129", "1E-129"}) {
            Assert.assertThrows(IllegalArgumentException.class, () -> {
                DataType.DECIMAL.valueToDecimal(bad);
            }, e -> {
                Assert.assertContains("out of bounds", e.getMessage());
            });
        }
        // the same check applies to a BigDecimal that arrives ready-made
        // (Gremlin literal, SUM result)
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            DataType.DECIMAL.valueToDecimal(new BigDecimal("1E+999999999"));
        });
        // 129 significant digits rejected, 128 accepted
        String digits128 = "1".repeat(128);
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            DataType.DECIMAL.valueToDecimal(digits128 + "1");
        });
        Assert.assertEquals(new BigDecimal(digits128),
                            DataType.DECIMAL.valueToDecimal(digits128));
        // uint256 max with 18 fraction digits (96 digits) is inside
        String uint256Max = "115792089237316195423570985008687907853" +
                            "269984665640564039457584007913129639935";
        BigDecimal wide = new BigDecimal(uint256Max + ".000000000000000001");
        Assert.assertEquals(wide, DataType.DECIMAL.valueToDecimal(wide));
        // scale boundary in both directions
        Assert.assertEquals(new BigDecimal("1E+128"),
                            DataType.DECIMAL.valueToDecimal("1E+128"));
        Assert.assertEquals(new BigDecimal("1E-128"),
                            DataType.DECIMAL.valueToDecimal("1E-128"));
    }

    @Test
    public void testValueToDecimal() {
        // uint256 max: 78 digits, far beyond long and double
        String uint256Max = "115792089237316195423570985008687907853" +
                            "269984665640564039457584007913129639935";
        BigDecimal expected = new BigDecimal(uint256Max);
        Assert.assertSame(expected, DataType.DECIMAL.valueToDecimal(expected));
        Assert.assertEquals(expected,
                            DataType.DECIMAL.valueToDecimal(uint256Max));
        Assert.assertEquals(expected, DataType.DECIMAL.valueToDecimal(
                            new BigInteger(uint256Max)));
        Assert.assertEquals(uint256Max, DataType.DECIMAL.valueToDecimal(
                            " " + uint256Max + " ").toPlainString());

        // scale is preserved: 1 wei on top of 1 ether, in ether
        BigDecimal wei = DataType.DECIMAL.valueToDecimal(
                         "1.000000000000000001");
        Assert.assertEquals(18, wei.scale());
        Assert.assertEquals("1.000000000000000001", wei.toPlainString());

        // integral java numbers are exact
        Assert.assertEquals(new BigDecimal("42"),
                            DataType.DECIMAL.valueToDecimal(42));
        Assert.assertEquals(new BigDecimal("42"),
                            DataType.DECIMAL.valueToDecimal(42L));
        Assert.assertEquals(new BigDecimal("-7"),
                            DataType.DECIMAL.valueToDecimal((byte) -7));
        // binary floats arrive as their shortest decimal representation
        Assert.assertEquals(new BigDecimal("0.1"),
                            DataType.DECIMAL.valueToDecimal(0.1D));
        Assert.assertEquals(new BigDecimal("1.5"),
                            DataType.DECIMAL.valueToDecimal(1.5F));
        // negative and zero
        Assert.assertEquals(new BigDecimal("-0.5"),
                            DataType.DECIMAL.valueToDecimal("-0.5"));
        Assert.assertEquals(BigDecimal.ZERO,
                            DataType.DECIMAL.valueToDecimal("0"));

        // not convertible
        Assert.assertNull(DataType.DECIMAL.valueToDecimal(true));
        Assert.assertNull(DataType.DECIMAL.valueToDecimal(new Date()));
        Assert.assertNull(DataType.TEXT.valueToDecimal("1.5"));
        Assert.assertNull(DataType.DOUBLE.valueToDecimal(1.5D));

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            DataType.DECIMAL.valueToDecimal("12abc");
        }, e -> {
            Assert.assertContains("Can't read '12abc' as decimal",
                                  e.getMessage());
        });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            DataType.DECIMAL.valueToDecimal("");
        });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            DataType.DECIMAL.valueToDecimal("0x10");
        });
    }
}
