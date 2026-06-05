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

import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hugegraph.backend.id.IdGenerator;
import org.apache.hugegraph.schema.PropertyKey;
import org.apache.hugegraph.schema.SchemaElement;
import org.apache.hugegraph.schema.Userdata;
import org.apache.hugegraph.schema.VertexLabel;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.type.define.Cardinality;
import org.apache.hugegraph.type.define.DataType;
import org.apache.hugegraph.unit.FakeObjects;
import org.apache.hugegraph.util.DateUtil;
import org.junit.Test;

public class SchemaElementTest {

    private static SchemaElement newSchema() {
        return new PropertyKey(null, IdGenerator.of(1L), "test");
    }

    @Test
    public void testSingleSetterNormalizesCreateTimeStringToDate() {
        SchemaElement schema = newSchema();
        String formatted = "2026-05-14 10:11:12.345";

        schema.userdata(Userdata.CREATE_TIME, formatted);

        Object value = schema.userdata().get(Userdata.CREATE_TIME);
        Assert.assertTrue("CREATE_TIME should be a Date, was " +
                          (value == null ? "null" : value.getClass()),
                          value instanceof Date);
        Assert.assertEquals(DateUtil.parse(formatted), value);
    }

    @Test
    public void testSingleSetterKeepsCreateTimeDateUnchanged() {
        SchemaElement schema = newSchema();
        Date now = DateUtil.now();

        schema.userdata(Userdata.CREATE_TIME, now);

        Assert.assertSame(now, schema.userdata().get(Userdata.CREATE_TIME));
    }

    @Test
    public void testSingleSetterRejectsInvalidCreateTimeString() {
        SchemaElement schema = newSchema();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.userdata(Userdata.CREATE_TIME, "not-a-date");
        }, e -> {
            Assert.assertContains(Userdata.CREATE_TIME, e.getMessage());
            Assert.assertContains("not-a-date", e.getMessage());
            Assert.assertNotNull(e.getCause());
        });
    }

    @Test
    public void testSingleSetterRejectsNullCreateTime() {
        SchemaElement schema = newSchema();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.userdata(Userdata.CREATE_TIME, null);
        }, e -> {
            Assert.assertContains("userdata value", e.getMessage());
        });
    }

    @Test
    public void testSingleSetterPassesThroughBlankCreateTime() {
        // "" is the key-only placeholder for the eliminate()/DELETE builder
        // flow (.userdata(CREATE_TIME, "").eliminate()); it must not be parsed.
        SchemaElement schema = newSchema();

        schema.userdata(Userdata.CREATE_TIME, "");

        Object value = schema.userdata().get(Userdata.CREATE_TIME);
        Assert.assertEquals("", value);
    }

    @Test
    public void testSingleSetterLeavesOtherStringKeysUntouched() {
        SchemaElement schema = newSchema();

        schema.userdata("note", "2026-05-14 10:11:12.345");

        Object value = schema.userdata().get("note");
        Assert.assertTrue(value instanceof String);
        Assert.assertEquals("2026-05-14 10:11:12.345", value);
    }

    @Test
    public void testUserdataConstructorNormalizesCreateTimeString() {
        String formatted = "2026-05-14 10:11:12.345";
        Map<String, Object> map = new HashMap<>();
        map.put(Userdata.CREATE_TIME, formatted);

        Userdata userdata = new Userdata(map);

        Object createTime = userdata.get(Userdata.CREATE_TIME);
        Assert.assertTrue(createTime instanceof Date);
        Assert.assertEquals(DateUtil.parse(formatted),
                            createTime);
    }

    @Test
    public void testUserdataConstructorLeavesOtherEntriesUntouched() {
        Map<String, Object> map = new HashMap<>();
        map.put("note", "2026-05-14 10:11:12.345");
        map.put("count", 42);

        Userdata userdata = new Userdata(map);

        Assert.assertEquals("2026-05-14 10:11:12.345",
                            userdata.get("note"));
        Assert.assertEquals(42, userdata.get("count"));
    }

    @Test
    public void testUserdataConstructorRejectsInvalidCreateTimeString() {
        Map<String, Object> map = new HashMap<>();
        map.put(Userdata.CREATE_TIME, "not-a-date");

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            new Userdata(map);
        }, e -> {
            Assert.assertContains(Userdata.CREATE_TIME, e.getMessage());
            Assert.assertContains("not-a-date", e.getMessage());
            Assert.assertNotNull(e.getCause());
        });
    }

    @Test
    public void testBulkSetterNormalizesCreateTimeAndKeepsOtherEntries() {
        SchemaElement schema = newSchema();
        Userdata bulk = new Userdata();
        String formatted = "2026-05-14 10:11:12.345";
        bulk.put(Userdata.CREATE_TIME, formatted);
        bulk.put("note", "hello");
        bulk.put("count", 42);

        schema.userdata(bulk);

        Object createTime = schema.userdata().get(Userdata.CREATE_TIME);
        Assert.assertTrue(createTime instanceof Date);
        Assert.assertEquals(DateUtil.parse(formatted), createTime);
        Assert.assertEquals("hello", schema.userdata().get("note"));
        Assert.assertEquals(42, schema.userdata().get("count"));
    }

    @Test
    public void testBulkSetterKeepsCreateTimeDateUnchanged() {
        SchemaElement schema = newSchema();
        Userdata bulk = new Userdata();
        Date now = DateUtil.now();
        bulk.put(Userdata.CREATE_TIME, now);

        schema.userdata(bulk);

        Assert.assertSame(now, schema.userdata().get(Userdata.CREATE_TIME));
    }

    @Test
    public void testVertexLabelFromMapNormalizesCreateTimeString() {
        String formatted = "2026-05-14 10:11:12.345";
        Map<String, Object> userdata = new HashMap<>();
        userdata.put(Userdata.CREATE_TIME, formatted);

        Map<String, Object> map = new HashMap<>();
        map.put(VertexLabel.P.ID, 1);
        map.put(VertexLabel.P.NAME, "person");
        map.put(VertexLabel.P.USERDATA, userdata);

        VertexLabel vertexLabel = VertexLabel.fromMap(map,
                                                      new FakeObjects().graph());

        Object createTime = vertexLabel.userdata().get(Userdata.CREATE_TIME);
        Assert.assertTrue(createTime instanceof Date);
        Assert.assertEquals(DateUtil.parse(formatted),
                            createTime);
    }

    @Test
    public void testPropertyKeyFromMapNormalizesDateDefaultValue() {
        String formatted = "2026-05-14 10:11:12.345";
        Map<String, Object> userdata = new HashMap<>();
        userdata.put(Userdata.DEFAULT_VALUE, formatted);

        Map<String, Object> map = new HashMap<>();
        map.put(PropertyKey.P.ID, 1);
        map.put(PropertyKey.P.NAME, "birth");
        map.put(PropertyKey.P.DATA_TYPE, DataType.DATE.string());
        map.put(PropertyKey.P.CARDINALITY, Cardinality.SINGLE.string());
        map.put(PropertyKey.P.USERDATA, userdata);

        PropertyKey propertyKey = PropertyKey.fromMap(map,
                                                      new FakeObjects().graph());

        Object value = propertyKey.defaultValue();
        Assert.assertTrue("DEFAULT_VALUE should be a Date, was " +
                          (value == null ? "null" : value.getClass()),
                          value instanceof Date);
        Assert.assertEquals(DateUtil.parse(formatted), value);
    }

    @Test
    public void testPropertyKeyFromMapNormalizesDateSetDefaultValue() {
        String first = "2026-05-14 10:11:12.345";
        String second = "2026-05-15 11:12:13.456";
        Map<String, Object> userdata = new HashMap<>();
        userdata.put(Userdata.DEFAULT_VALUE, Arrays.asList(first, second));

        Map<String, Object> map = new HashMap<>();
        map.put(PropertyKey.P.ID, 1);
        map.put(PropertyKey.P.NAME, "tags");
        map.put(PropertyKey.P.DATA_TYPE, DataType.DATE.string());
        map.put(PropertyKey.P.CARDINALITY, Cardinality.SET.string());
        map.put(PropertyKey.P.USERDATA, userdata);

        PropertyKey propertyKey = PropertyKey.fromMap(map,
                                                      new FakeObjects().graph());

        Object value = propertyKey.defaultValue();
        Assert.assertTrue("DEFAULT_VALUE should be a Collection, was " +
                          (value == null ? "null" : value.getClass()),
                          value instanceof Collection);
        Collection<?> values = (Collection<?>) value;
        Assert.assertEquals(2, values.size());
        for (Object element : values) {
            Assert.assertTrue("each element should be a Date, was " +
                              (element == null ? "null" : element.getClass()),
                              element instanceof Date);
        }
        List<Date> expected = Arrays.asList(DateUtil.parse(first),
                                            DateUtil.parse(second));
        Assert.assertTrue(values.containsAll(expected));
    }

    @Test
    public void testBulkSetterRejectsNullUserdata() {
        SchemaElement schema = newSchema();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.userdata(null);
        }, e -> {
            Assert.assertContains("userdata", e.getMessage());
        });
    }
}
