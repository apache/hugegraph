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

package org.apache.hugegraph.struct.schema;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;
import java.util.Date;
import java.util.Set;

import org.apache.hugegraph.id.IdGenerator;
import org.apache.hugegraph.serializer.BytesBuffer;
import org.apache.hugegraph.type.define.Cardinality;
import org.apache.hugegraph.type.define.DataType;
import org.apache.hugegraph.util.DateUtil;
import org.junit.Assert;
import org.junit.Test;

public class PropertyKeyTest {

    @Test
    public void testDefaultValueNormalizedToDate() {
        // Userdata reloaded from JSON keeps ~default_value as a String;
        // defaultValue() must normalize it to the data type's runtime type
        // (#3028).
        String formatted = "2026-05-14 10:11:12.345";
        PropertyKey propertyKey = new PropertyKey(null, IdGenerator.of(1),
                                                  "joinDate");
        propertyKey.dataType(DataType.DATE);
        propertyKey.userdata(Userdata.DEFAULT_VALUE, formatted);

        Object value = propertyKey.defaultValue();
        Assert.assertTrue("DEFAULT_VALUE should be a Date, was " +
                          (value == null ? "null" : value.getClass()),
                          value instanceof Date);
        Assert.assertEquals(DateUtil.parse(formatted), value);
    }

    @Test
    public void testSetDefaultValueCollapsesDuplicatesAndReturnsSet() {
        String formatted = "2026-05-14 10:11:12.345";
        PropertyKey propertyKey = new PropertyKey(null, IdGenerator.of(1),
                                                  "joinDate");
        propertyKey.dataType(DataType.DATE);
        propertyKey.cardinality(Cardinality.SET);
        propertyKey.userdata(Userdata.DEFAULT_VALUE,
                             Arrays.asList(formatted, formatted));

        Object value = propertyKey.defaultValue();
        Assert.assertTrue("DEFAULT_VALUE should be a Set, was " +
                          (value == null ? "null" : value.getClass()),
                          value instanceof Set);

        Set<?> values = (Set<?>) value;
        Assert.assertEquals(1, values.size());
        Assert.assertTrue(values.contains(DateUtil.parse(formatted)));
    }

    @Test
    public void testDecimalPropertyRoundTripAndSchema() {
        PropertyKey propertyKey = new PropertyKey(null, IdGenerator.of(2),
                                                  "balance");
        propertyKey.dataType(DataType.DECIMAL);
        Assert.assertTrue(propertyKey.dataType().isDecimal());
        Assert.assertFalse(propertyKey.dataType().isNumber());
        Assert.assertTrue(propertyKey.convert2Groovy(false).contains(".asDecimal()"));

        // uint256 max survives the struct BytesBuffer used by the store
        BigDecimal value = new BigDecimal(
                "115792089237316195423570985008687907853" +
                "269984665640564039457584007913129639935");
        BytesBuffer buffer = BytesBuffer.allocate(64);
        buffer.writeProperty(DataType.DECIMAL, value);
        Object read = BytesBuffer.wrap(buffer.bytes())
                                 .readProperty(DataType.DECIMAL);
        Assert.assertEquals(value, read);

        BigDecimal wei = new BigDecimal("1.000000000000000001");
        buffer = BytesBuffer.allocate(64);
        buffer.writeProperty(DataType.DECIMAL, wei);
        read = BytesBuffer.wrap(buffer.bytes()).readProperty(DataType.DECIMAL);
        Assert.assertEquals(wei, read);
        Assert.assertEquals(18, ((BigDecimal) read).scale());
    }

    @Test
    public void testDecimalValueConversion() {
        // The struct copy must convert the same inputs as the server copy:
        // strings (userdata and JSON), integral numbers, BigInteger
        PropertyKey propertyKey = new PropertyKey(null, IdGenerator.of(3),
                                                  "balance");
        propertyKey.dataType(DataType.DECIMAL);

        Assert.assertEquals(new BigDecimal("1.5"),
                            propertyKey.validValueOrThrow("1.5"));
        Assert.assertEquals(new BigDecimal("42"),
                            propertyKey.validValueOrThrow(42L));
        Assert.assertEquals(new BigDecimal("7"),
                            propertyKey.validValueOrThrow(7));
        String uint256Max = "115792089237316195423570985008687907853" +
                            "269984665640564039457584007913129639935";
        Assert.assertEquals(new BigDecimal(uint256Max),
                            propertyKey.validValueOrThrow(uint256Max));
        Assert.assertEquals(new BigDecimal(uint256Max),
                            propertyKey.validValueOrThrow(
                                    new BigInteger(uint256Max)));
        Assert.assertEquals(new BigDecimal("0.000000000000000001"),
                            propertyKey.validValueOrThrow("1E-18"));
        // already the expected type: returned as is
        BigDecimal exact = new BigDecimal("-1.50");
        Assert.assertSame(exact, propertyKey.validValueOrThrow(exact));

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            propertyKey.validValueOrThrow("1,5");
        });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            propertyKey.validValueOrThrow(new Date());
        });
        // bounds: a huge exponent must not reach the store
        for (String bad : new String[]{"1E+999999999", "1E-999999999"}) {
            Assert.assertThrows(IllegalArgumentException.class, () -> {
                propertyKey.validValueOrThrow(bad);
            });
        }
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            propertyKey.validValueOrThrow(new BigDecimal("1E+999999999"));
        });
        Assert.assertEquals(new BigDecimal("1E+128"),
                            propertyKey.validValueOrThrow("1E+128"));
    }

    @Test
    public void testDefaultValueNormalizedToDecimal() {
        // Userdata reloaded from JSON keeps ~default_value as a String;
        // defaultValue() must hand back a BigDecimal, exactly
        PropertyKey propertyKey = new PropertyKey(null, IdGenerator.of(4),
                                                  "balance");
        propertyKey.dataType(DataType.DECIMAL);
        propertyKey.userdata(Userdata.DEFAULT_VALUE, "1000000000000000000001");

        Object value = propertyKey.defaultValue();
        Assert.assertTrue("DEFAULT_VALUE should be a BigDecimal, was " +
                          (value == null ? "null" : value.getClass()),
                          value instanceof BigDecimal);
        Assert.assertEquals(new BigDecimal("1000000000000000000001"), value);

        // a number literal in the JSON is exact as long as it is integral
        propertyKey.userdata(Userdata.DEFAULT_VALUE, 5L);
        Assert.assertEquals(new BigDecimal("5"), propertyKey.defaultValue());

        // list cardinality: every element converted
        PropertyKey listKey = new PropertyKey(null, IdGenerator.of(5),
                                              "limits");
        listKey.dataType(DataType.DECIMAL);
        listKey.cardinality(Cardinality.LIST);
        listKey.userdata(Userdata.DEFAULT_VALUE, Arrays.asList("1", "2.5"));
        Object list = listKey.defaultValue();
        Assert.assertTrue(list instanceof List);
        Assert.assertEquals(Arrays.asList(new BigDecimal("1"),
                                          new BigDecimal("2.5")), list);
    }
}
