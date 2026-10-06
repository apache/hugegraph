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

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
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
    }

    @Test
    public void testValueToNumber() {
        Assert.assertNull(DataType.BOOLEAN.valueToNumber(1));
        Assert.assertNull(DataType.INT.valueToNumber("not number"));

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
    public void testValueToDateFromOffsetDateTime() {
        OffsetDateTime value = OffsetDateTime.parse("2023-08-02T08:00:00.123456789+08:00");
        Date expected = Date.from(value.toInstant());
        Assert.assertEquals(expected, DataType.DATE.valueToDate(value));
        Assert.assertEquals(expected, DataType.DATE.valueToDate(
                value.withOffsetSameInstant(ZoneOffset.UTC)));
        Assert.assertEquals(value.toInstant().toEpochMilli(), expected.getTime());
        Assert.assertNull(DataType.TEXT.valueToDate(value));
    }

    @Test
    public void testValueToUUID() {
        UUID uuid = UUID.randomUUID();
        Assert.assertEquals(uuid, DataType.UUID.valueToUUID(uuid));
        Assert.assertEquals(uuid, DataType.UUID.valueToUUID(uuid.toString()));

        Assert.assertNull(DataType.TEXT.valueToUUID("2019-01-01 12:00:00"));
        Assert.assertNull(DataType.UUID.valueToUUID(true));
    }
}
