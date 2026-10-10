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

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.Set;

import org.apache.hugegraph.id.IdGenerator;
import org.apache.hugegraph.structure.BaseEdge;
import org.apache.hugegraph.structure.BaseVertex;
import org.apache.hugegraph.type.define.Cardinality;
import org.apache.hugegraph.type.define.DataType;
import org.apache.hugegraph.type.define.IdStrategy;
import org.apache.hugegraph.util.DateUtil;
import org.apache.hugegraph.util.LongEncoding;
import org.junit.Assert;
import org.junit.Test;

public class PropertyKeyTest {

    @Test
    public void testOffsetDateTimeNormalizedToDate() {
        OffsetDateTime value = OffsetDateTime.parse("2026-05-14T10:11:12.345678+08:00");
        OffsetDateTime utc = value.withOffsetSameInstant(ZoneOffset.UTC);
        Date expected = new Date(value.toInstant().toEpochMilli());
        PropertyKey propertyKey = new PropertyKey(null, IdGenerator.of(1), "joinDate");
        propertyKey.dataType(DataType.DATE);

        Object normalized = propertyKey.validValue(value);
        Assert.assertEquals(expected, normalized);
        Assert.assertEquals(expected, propertyKey.validValue(utc));
        Assert.assertEquals(345L, ((Date) normalized).getTime() % 1000L);
        Assert.assertNull(DataType.TEXT.valueToDate(value));
        Assert.assertNull(DataType.DATE.valueToDate(value.toLocalDateTime()));
    }

    @Test
    public void testOffsetDateTimeSerializedAsDateNavigationKeys() {
        OffsetDateTime value = OffsetDateTime.parse("2026-05-14T10:11:12.345678+08:00");
        Date expected = new Date(value.toInstant().toEpochMilli());
        PropertyKey propertyKey = new PropertyKey(null, IdGenerator.of(1), "joinDate");
        propertyKey.dataType(DataType.DATE);
        Object encoded = LongEncoding.encodeNumber(expected);

        Assert.assertEquals(encoded, propertyKey.serialValue(value, true));
        Assert.assertEquals(expected.toString(), propertyKey.serialValue(value, false));

        VertexLabel vertexLabel = new VertexLabel(null, IdGenerator.of(2), "person");
        vertexLabel.idStrategy(IdStrategy.PRIMARY_KEY);
        vertexLabel.primaryKey(propertyKey.id());
        BaseVertex vertex = new BaseVertex(null, vertexLabel);
        vertex.addProperty(propertyKey, value);
        Assert.assertEquals(Collections.singletonList(encoded), vertex.primaryValues());

        EdgeLabel edgeLabel = new EdgeLabel(null, IdGenerator.of(3), "joined");
        edgeLabel.sortKey(propertyKey.id());
        BaseEdge edge = new BaseEdge(null, edgeLabel);
        edge.addProperty(propertyKey, value.withOffsetSameInstant(ZoneOffset.UTC));
        Assert.assertEquals(Collections.singletonList(encoded), edge.sortValues());
    }

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
}
