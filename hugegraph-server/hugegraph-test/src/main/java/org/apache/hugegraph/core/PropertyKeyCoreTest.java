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

package org.apache.hugegraph.core;

import java.util.Date;

import org.apache.hugegraph.HugeException;
import org.apache.hugegraph.exception.ExistedException;
import org.apache.hugegraph.exception.NotAllowException;
import org.apache.hugegraph.exception.NotFoundException;
import org.apache.hugegraph.schema.PropertyKey;
import org.apache.hugegraph.schema.SchemaManager;
import org.apache.hugegraph.schema.Userdata;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.type.define.AggregateType;
import org.apache.hugegraph.type.define.Cardinality;
import org.apache.hugegraph.type.define.DataType;
import org.apache.hugegraph.type.define.WriteType;
import org.apache.hugegraph.util.DateUtil;
import org.junit.Assume;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

public class PropertyKeyCoreTest extends SchemaCoreTest {

    @Test
    public void testAddPropertyKey() {
        SchemaManager schema = graph().schema();
        PropertyKey id = schema.propertyKey("id")
                               .asText()
                               .valueSingle()
                               .create();

        Assert.assertEquals("id", id.name());
        Assert.assertEquals(DataType.TEXT, id.dataType());
        Assert.assertEquals(Cardinality.SINGLE, id.cardinality());
    }

    @Test
    public void testAddPropertyKeyWithValidName() {
        SchemaManager schema = graph().schema();

        // One space and single char
        schema.propertyKey(" s").create();
        schema.propertyKey("s s").create();

        schema.propertyKey(" .").create();
        schema.propertyKey(". .").create();

        schema.propertyKey("@$%^&*()_+`-={}|[]\"<?;'~,./\\").create();
        schema.propertyKey("azAZ0123456789").create();

        schema.propertyKey(" ~").create();
        schema.propertyKey("x~").create();
    }

    @Test
    public void testAddPropertyKeyWithIllegalName() {
        SchemaManager schema = graph().schema();

        // Empty string
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.propertyKey("").create();
        });
        // One space
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.propertyKey(" ").create();
        });
        // Multi spaces
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.propertyKey("    ").create();
        });

        // End with spaces
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.propertyKey("s ").create();
        });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.propertyKey(" . ").create();
        });

        // Internal characters
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.propertyKey("#").create();
        });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.propertyKey(">").create();
        });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.propertyKey(":").create();
        });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.propertyKey("!").create();
        });

        // Start with '~'
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.propertyKey("~").create();
        });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.propertyKey("~ ").create();
        });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.propertyKey("~x").create();
        });
    }

    @Test
    public void testAddPropertyKeyWithoutDataType() {
        SchemaManager schema = graph().schema();
        PropertyKey id = schema.propertyKey("id").valueSingle().create();
        Assert.assertEquals(DataType.TEXT, id.dataType());
    }

    @Test
    public void testAddPropertyKeyWithoutCardinality() {
        SchemaManager schema = graph().schema();
        PropertyKey id = schema.propertyKey("id").asText().create();
        Assert.assertEquals(Cardinality.SINGLE, id.cardinality());
    }

    @Test
    public void testAddPropertyKeyWithoutDataTypeAndCardinality() {
        SchemaManager schema = graph().schema();
        PropertyKey id = schema.propertyKey("id").create();
        Assert.assertEquals(DataType.TEXT, id.dataType());
        Assert.assertEquals(Cardinality.SINGLE, id.cardinality());
    }

    @Test
    public void testAddPropertyKeyWithAggregateType() {
        SchemaManager schema = graph().schema();
        PropertyKey startTime = schema.propertyKey("startTime")
                                      .asDate().valueSingle().calcMin()
                                      .ifNotExist().create();
        Assert.assertEquals(DataType.DATE, startTime.dataType());
        Assert.assertEquals(Cardinality.SINGLE, startTime.cardinality());
        Assert.assertEquals(AggregateType.MIN, startTime.aggregateType());
        startTime = schema.getPropertyKey("startTime");
        Assert.assertEquals(DataType.DATE, startTime.dataType());
        Assert.assertEquals(Cardinality.SINGLE, startTime.cardinality());
        Assert.assertEquals(AggregateType.MIN, startTime.aggregateType());

        PropertyKey endTime = schema.propertyKey("endTime")
                                    .asDate().valueSingle().calcMax()
                                    .ifNotExist().create();
        Assert.assertEquals(DataType.DATE, endTime.dataType());
        Assert.assertEquals(Cardinality.SINGLE, endTime.cardinality());
        Assert.assertEquals(AggregateType.MAX, endTime.aggregateType());
        endTime = schema.getPropertyKey("endTime");
        Assert.assertEquals(DataType.DATE, endTime.dataType());
        Assert.assertEquals(Cardinality.SINGLE, endTime.cardinality());
        Assert.assertEquals(AggregateType.MAX, endTime.aggregateType());

        PropertyKey times = schema.propertyKey("times")
                                  .asLong().valueSingle().calcSum()
                                  .ifNotExist().create();
        Assert.assertEquals(DataType.LONG, times.dataType());
        Assert.assertEquals(Cardinality.SINGLE, times.cardinality());
        Assert.assertEquals(AggregateType.SUM, times.aggregateType());
        times = schema.getPropertyKey("times");
        Assert.assertEquals(DataType.LONG, times.dataType());
        Assert.assertEquals(Cardinality.SINGLE, times.cardinality());
        Assert.assertEquals(AggregateType.SUM, times.aggregateType());

        PropertyKey oldProp = schema.propertyKey("oldProp")
                                    .asLong().valueSingle().calcOld()
                                    .ifNotExist().create();
        Assert.assertEquals(DataType.LONG, oldProp.dataType());
        Assert.assertEquals(Cardinality.SINGLE, oldProp.cardinality());
        Assert.assertEquals(AggregateType.OLD, oldProp.aggregateType());
        oldProp = schema.getPropertyKey("oldProp");
        Assert.assertEquals(DataType.LONG, oldProp.dataType());
        Assert.assertEquals(Cardinality.SINGLE, oldProp.cardinality());
        Assert.assertEquals(AggregateType.OLD, oldProp.aggregateType());

        PropertyKey setProp = schema.propertyKey("setProp")
                                    .asLong().valueSet().calcSet()
                                    .ifNotExist().create();
        Assert.assertEquals(DataType.LONG, setProp.dataType());
        Assert.assertEquals(Cardinality.SET, setProp.cardinality());
        Assert.assertEquals(AggregateType.SET, setProp.aggregateType());
        setProp = schema.getPropertyKey("setProp");
        Assert.assertEquals(DataType.LONG, setProp.dataType());
        Assert.assertEquals(Cardinality.SET, setProp.cardinality());
        Assert.assertEquals(AggregateType.SET, setProp.aggregateType());

        PropertyKey listProp = schema.propertyKey("listProp")
                                     .asLong().valueList().calcList()
                                     .ifNotExist().create();
        Assert.assertEquals(DataType.LONG, listProp.dataType());
        Assert.assertEquals(Cardinality.LIST, listProp.cardinality());
        Assert.assertEquals(AggregateType.LIST, listProp.aggregateType());
        listProp = schema.getPropertyKey("listProp");
        Assert.assertEquals(DataType.LONG, listProp.dataType());
        Assert.assertEquals(Cardinality.LIST, listProp.cardinality());
        Assert.assertEquals(AggregateType.LIST, listProp.aggregateType());

        PropertyKey regular = schema.propertyKey("regular").create();
        Assert.assertEquals(DataType.TEXT, regular.dataType());
        Assert.assertEquals(Cardinality.SINGLE, regular.cardinality());
        Assert.assertEquals(AggregateType.NONE, regular.aggregateType());
        regular = schema.getPropertyKey("regular");
        Assert.assertEquals(DataType.TEXT, regular.dataType());
        Assert.assertEquals(Cardinality.SINGLE, regular.cardinality());
        Assert.assertEquals(AggregateType.NONE, regular.aggregateType());
    }

    @Test
    public void testAddPropertyKeyWithAggregateTypeInvalid() {
        SchemaManager schema = graph().schema();

        // Invalid cardinality
        Assert.assertThrows(NotAllowException.class, () -> {
            schema.propertyKey("aggregateProperty")
                  .asDate().valueList().calcMin().create();
        });

        Assert.assertThrows(NotAllowException.class, () -> {
            schema.propertyKey("aggregateProperty")
                  .asDate().valueSet().calcMin().create();
        });

        Assert.assertThrows(NotAllowException.class, () -> {
            schema.propertyKey("aggregateProperty")
                  .asDate().valueList().calcMax().create();
        });

        Assert.assertThrows(NotAllowException.class, () -> {
            schema.propertyKey("aggregateProperty")
                  .asDate().valueSet().calcMax().create();
        });

        Assert.assertThrows(NotAllowException.class, () -> {
            schema.propertyKey("aggregateProperty")
                  .asDate().valueList().calcSum().create();
        });

        Assert.assertThrows(NotAllowException.class, () -> {
            schema.propertyKey("aggregateProperty")
                  .asDate().valueSet().calcSum().create();
        });

        Assert.assertThrows(NotAllowException.class, () -> {
            schema.propertyKey("aggregateProperty")
                  .asDate().valueList().calcOld().create();
        });

        Assert.assertThrows(NotAllowException.class, () -> {
            schema.propertyKey("aggregateProperty")
                  .asDate().valueSet().calcOld().create();
        });

        Assert.assertThrows(NotAllowException.class, () -> {
            schema.propertyKey("aggregateProperty")
                  .asDate().valueSingle().calcSet().create();
        });

        Assert.assertThrows(NotAllowException.class, () -> {
            schema.propertyKey("aggregateProperty")
                  .asDate().valueSingle().calcList().create();
        });

        Assert.assertThrows(NotAllowException.class, () -> {
            schema.propertyKey("aggregateProperty")
                  .asDate().valueSet().calcList().create();
        });

        Assert.assertThrows(NotAllowException.class, () -> {
            schema.propertyKey("aggregateProperty")
                  .asDate().valueList().calcSet().create();
        });

        // Invalid data type
        Assert.assertThrows(NotAllowException.class, () -> {
            schema.propertyKey("aggregateProperty")
                  .asText().valueSingle().calcMin().create();
        });

        Assert.assertThrows(NotAllowException.class, () -> {
            schema.propertyKey("aggregateProperty")
                  .asBlob().valueSingle().calcMin().create();
        });

        Assert.assertThrows(NotAllowException.class, () -> {
            schema.propertyKey("aggregateProperty")
                  .asBoolean().valueSingle().calcMin().create();
        });

        Assert.assertThrows(NotAllowException.class, () -> {
            schema.propertyKey("aggregateProperty")
                  .asUUID().valueSingle().calcMin().create();
        });

        Assert.assertThrows(NotAllowException.class, () -> {
            schema.propertyKey("aggregateProperty")
                  .asText().valueSingle().calcMax().create();
        });

        Assert.assertThrows(NotAllowException.class, () -> {
            schema.propertyKey("aggregateProperty")
                  .asBlob().valueSingle().calcMax().create();
        });

        Assert.assertThrows(NotAllowException.class, () -> {
            schema.propertyKey("aggregateProperty")
                  .asBoolean().valueSingle().calcMax().create();
        });

        Assert.assertThrows(NotAllowException.class, () -> {
            schema.propertyKey("aggregateProperty")
                  .asUUID().valueSingle().calcMax().create();
        });

        Assert.assertThrows(NotAllowException.class, () -> {
            schema.propertyKey("aggregateProperty")
                  .asText().valueSingle().calcSum().create();
        });

        Assert.assertThrows(NotAllowException.class, () -> {
            schema.propertyKey("aggregateProperty")
                  .asBlob().valueSingle().calcSum().create();
        });

        Assert.assertThrows(NotAllowException.class, () -> {
            schema.propertyKey("aggregateProperty")
                  .asBoolean().valueSingle().calcSum().create();
        });

        Assert.assertThrows(NotAllowException.class, () -> {
            schema.propertyKey("aggregateProperty")
                  .asUUID().valueSingle().calcSum().create();
        });

        Assert.assertThrows(NotAllowException.class, () -> {
            schema.propertyKey("aggregateProperty")
                  .asDate().valueSingle().calcSum().create();
        });
    }

    @Test
    public void testAddOlapPropertyKey() {
        Assume.assumeTrue("Not support olap properties",
                          storeFeatures().supportsOlapProperties());

        SchemaManager schema = graph().schema();
        PropertyKey olap = schema.propertyKey("olap")
                                 .asText().valueSingle()
                                 .writeType(WriteType.OLAP_COMMON)
                                 .ifNotExist().create();

        Assert.assertEquals("olap", olap.name());
        Assert.assertEquals(DataType.TEXT, olap.dataType());
        Assert.assertEquals(Cardinality.SINGLE, olap.cardinality());
        Assert.assertEquals(WriteType.OLAP_COMMON, olap.writeType());

        PropertyKey pagerank = schema.propertyKey("pagerank")
                                     .asDouble().valueSingle()
                                     .writeType(WriteType.OLAP_RANGE)
                                     .ifNotExist().create();

        Assert.assertEquals("pagerank", pagerank.name());
        Assert.assertEquals(DataType.DOUBLE, pagerank.dataType());
        Assert.assertEquals(Cardinality.SINGLE, pagerank.cardinality());
        Assert.assertEquals(WriteType.OLAP_RANGE, pagerank.writeType());

        PropertyKey wcc = schema.propertyKey("wcc")
                                .asText().valueSingle()
                                .writeType(WriteType.OLAP_SECONDARY)
                                .ifNotExist().create();

        Assert.assertEquals("wcc", wcc.name());
        Assert.assertEquals(DataType.TEXT, wcc.dataType());
        Assert.assertEquals(Cardinality.SINGLE, wcc.cardinality());
        Assert.assertEquals(WriteType.OLAP_SECONDARY, wcc.writeType());
    }

    @Test
    public void testClearOlapPropertyKey() {
        Assume.assumeTrue("Not support olap properties",
                          storeFeatures().supportsOlapProperties());

        SchemaManager schema = graph().schema();
        PropertyKey olap = schema.propertyKey("olap")
                                 .asText().valueSingle()
                                 .writeType(WriteType.OLAP_COMMON)
                                 .ifNotExist().create();

        Assert.assertEquals("olap", olap.name());
        Assert.assertEquals(DataType.TEXT, olap.dataType());
        Assert.assertEquals(Cardinality.SINGLE, olap.cardinality());
        Assert.assertEquals(WriteType.OLAP_COMMON, olap.writeType());

        graph().clearPropertyKey(olap);

        olap = graph().propertyKey("olap");
        Assert.assertEquals("olap", olap.name());
        Assert.assertEquals(DataType.TEXT, olap.dataType());
        Assert.assertEquals(Cardinality.SINGLE, olap.cardinality());
        Assert.assertEquals(WriteType.OLAP_COMMON, olap.writeType());

        PropertyKey pagerank = schema.propertyKey("pagerank")
                                     .asDouble().valueSingle()
                                     .writeType(WriteType.OLAP_RANGE)
                                     .ifNotExist().create();

        Assert.assertEquals("pagerank", pagerank.name());
        Assert.assertEquals(DataType.DOUBLE, pagerank.dataType());
        Assert.assertEquals(Cardinality.SINGLE, pagerank.cardinality());
        Assert.assertEquals(WriteType.OLAP_RANGE, pagerank.writeType());

        graph().clearPropertyKey(pagerank);

        pagerank = graph().propertyKey("pagerank");
        Assert.assertEquals("pagerank", pagerank.name());
        Assert.assertEquals(DataType.DOUBLE, pagerank.dataType());
        Assert.assertEquals(Cardinality.SINGLE, pagerank.cardinality());
        Assert.assertEquals(WriteType.OLAP_RANGE, pagerank.writeType());

        PropertyKey wcc = schema.propertyKey("wcc")
                                .asText().valueSingle()
                                .writeType(WriteType.OLAP_SECONDARY)
                                .ifNotExist().create();

        Assert.assertEquals("wcc", wcc.name());
        Assert.assertEquals(DataType.TEXT, wcc.dataType());
        Assert.assertEquals(Cardinality.SINGLE, wcc.cardinality());
        Assert.assertEquals(WriteType.OLAP_SECONDARY, wcc.writeType());

        graph().clearPropertyKey(wcc);

        wcc = graph().propertyKey("wcc");
        Assert.assertEquals("wcc", wcc.name());
        Assert.assertEquals(DataType.TEXT, wcc.dataType());
        Assert.assertEquals(Cardinality.SINGLE, wcc.cardinality());
        Assert.assertEquals(WriteType.OLAP_SECONDARY, wcc.writeType());
    }

    @Test
    public void testRemoveOlapPropertyKey() {
        Assume.assumeTrue("Not support olap properties",
                          storeFeatures().supportsOlapProperties());

        SchemaManager schema = graph().schema();
        PropertyKey olap = schema.propertyKey("olap")
                                 .asText().valueSingle()
                                 .writeType(WriteType.OLAP_COMMON)
                                 .ifNotExist().create();

        Assert.assertEquals("olap", olap.name());
        Assert.assertEquals(DataType.TEXT, olap.dataType());
        Assert.assertEquals(Cardinality.SINGLE, olap.cardinality());
        Assert.assertEquals(WriteType.OLAP_COMMON, olap.writeType());

        schema.propertyKey("olap").remove();

        Assert.assertThrows(NotFoundException.class, () -> {
            schema.getPropertyKey("olap");
        });

        PropertyKey pagerank = schema.propertyKey("pagerank")
                                     .asDouble().valueSingle()
                                     .writeType(WriteType.OLAP_RANGE)
                                     .ifNotExist().create();

        Assert.assertEquals("pagerank", pagerank.name());
        Assert.assertEquals(DataType.DOUBLE, pagerank.dataType());
        Assert.assertEquals(Cardinality.SINGLE, pagerank.cardinality());
        Assert.assertEquals(WriteType.OLAP_RANGE, pagerank.writeType());

        schema.propertyKey("pagerank").remove();

        Assert.assertThrows(NotFoundException.class, () -> {
            schema.getPropertyKey("pagerank");
        });

        PropertyKey wcc = schema.propertyKey("wcc")
                                .asText().valueSingle()
                                .writeType(WriteType.OLAP_SECONDARY)
                                .ifNotExist().create();

        Assert.assertEquals("wcc", wcc.name());
        Assert.assertEquals(DataType.TEXT, wcc.dataType());
        Assert.assertEquals(Cardinality.SINGLE, wcc.cardinality());
        Assert.assertEquals(WriteType.OLAP_SECONDARY, wcc.writeType());

        schema.propertyKey("wcc").remove();

        Assert.assertThrows(NotFoundException.class, () -> {
            schema.getPropertyKey("wcc");
        });
    }

    @Test
    public void testRemovePropertyKey() {
        SchemaManager schema = graph().schema();
        schema.propertyKey("id").valueSingle().create();
        Assert.assertNotNull(schema.getPropertyKey("id"));

        schema.propertyKey("id").remove();
        Assert.assertThrows(NotFoundException.class, () -> {
            schema.getPropertyKey("id");
        });
    }

    @Test
    public void testRemoveNotExistPropertyKey() {
        SchemaManager schema = graph().schema();
        schema.propertyKey("not-exist-pk").remove();
    }

    @Test
    public void testRemovePropertyKeyUsedByVertexLabel() {
        super.initPropertyKeys();
        SchemaManager schema = graph().schema();

        schema.vertexLabel("person")
              .properties("name", "age", "city")
              .primaryKeys("name")
              .nullableKeys("city")
              .create();

        Assert.assertThrows(HugeException.class, () -> {
            schema.propertyKey("name").remove();
        });
    }

    @Test
    public void testRemovePropertyKeyUsedByEdgeLabel() {
        super.initPropertyKeys();
        SchemaManager schema = graph().schema();

        schema.vertexLabel("person")
              .properties("name", "age", "city")
              .primaryKeys("name")
              .nullableKeys("city")
              .create();
        schema.vertexLabel("book")
              .properties("name")
              .primaryKeys("name")
              .create();
        schema.edgeLabel("write").link("person", "book")
              .properties("time", "weight")
              .create();

        Assert.assertThrows(HugeException.class, () -> {
            schema.propertyKey("time").remove();
        });
    }

    @Test
    public void testAddPropertyKeyWithUserdata() {
        SchemaManager schema = graph().schema();

        PropertyKey age = schema.propertyKey("age")
                                .userdata("min", 0)
                                .userdata("max", 100)
                                .create();
        Assert.assertEquals(3, age.userdata().size());
        Assert.assertEquals(0, age.userdata().get("min"));
        Assert.assertEquals(100, age.userdata().get("max"));

        PropertyKey id = schema.propertyKey("id")
                               .userdata("length", 15)
                               .userdata("length", 18)
                               .create();
        // The same key user data will be overwritten
        Assert.assertEquals(2, id.userdata().size());
        Assert.assertEquals(18, id.userdata().get("length"));

        PropertyKey sex = schema.propertyKey("sex")
                                .userdata("range",
                                          ImmutableList.of("male", "female"))
                                .create();
        Assert.assertEquals(2, sex.userdata().size());
        Assert.assertEquals(ImmutableList.of("male", "female"),
                            sex.userdata().get("range"));
    }

    @Test
    public void testAppendPropertyKeyWithUserdata() {
        SchemaManager schema = graph().schema();

        PropertyKey age = schema.propertyKey("age")
                                .userdata("min", 0)
                                .create();
        Assert.assertEquals(2, age.userdata().size());
        Assert.assertEquals(0, age.userdata().get("min"));

        age = schema.propertyKey("age")
                    .userdata("min", 1)
                    .userdata("max", 100)
                    .append();
        Assert.assertEquals(3, age.userdata().size());
        Assert.assertEquals(1, age.userdata().get("min"));
        Assert.assertEquals(100, age.userdata().get("max"));
    }

    @Test
    public void testEliminatePropertyKeyWithUserdata() {
        SchemaManager schema = graph().schema();

        PropertyKey age = schema.propertyKey("age")
                                .userdata("min", 0)
                                .userdata("max", 100)
                                .create();
        Assert.assertEquals(3, age.userdata().size());
        Assert.assertEquals(0, age.userdata().get("min"));
        Assert.assertEquals(100, age.userdata().get("max"));

        age = schema.propertyKey("age")
                    .userdata("max", "")
                    .eliminate();
        Assert.assertEquals(2, age.userdata().size());
        Assert.assertEquals(0, age.userdata().get("min"));
    }

    @Test
    public void testEliminatePropertyKeyCreateTimeUserdata() {
        SchemaManager schema = graph().schema();

        PropertyKey age = schema.propertyKey("age")
                                .userdata("min", 0)
                                .create();
        Assert.assertEquals(2, age.userdata().size());
        Assert.assertTrue(age.userdata().containsKey(Userdata.CREATE_TIME));

        // "" is a key-only placeholder for eliminate; it must not be parsed
        // as a date (regression: normalization on Userdata.put()).
        age = schema.propertyKey("age")
                    .userdata(Userdata.CREATE_TIME, "")
                    .eliminate();
        Assert.assertEquals(1, age.userdata().size());
        Assert.assertFalse(age.userdata().containsKey(Userdata.CREATE_TIME));
        Assert.assertEquals(0, age.userdata().get("min"));
    }

    @Test
    public void testUpdatePropertyKeyWithoutUserdata() {
        SchemaManager schema = graph().schema();

        schema.propertyKey("age")
              .asInt()
              .valueSingle()
              .userdata("min", 0)
              .create();

        Assert.assertThrows(HugeException.class, () -> {
            schema.propertyKey("age").asLong().append();
        });

        Assert.assertThrows(HugeException.class, () -> {
            schema.propertyKey("age").valueList().append();
        });

        Assert.assertThrows(HugeException.class, () -> {
            schema.propertyKey("age").asLong().eliminate();
        });

        Assert.assertThrows(HugeException.class, () -> {
            schema.propertyKey("age").valueList().eliminate();
        });
    }

    @Test
    public void testCreateTime() {
        SchemaManager schema = graph().schema();
        PropertyKey id = schema.propertyKey("id")
                               .asText()
                               .valueSingle()
                               .create();

        Date createTime = (Date) id.userdata().get(Userdata.CREATE_TIME);
        Date now = DateUtil.now();
        Assert.assertFalse(createTime.after(now));

        id = schema.getPropertyKey("id");
        createTime = (Date) id.userdata().get(Userdata.CREATE_TIME);
        Assert.assertFalse(createTime.after(now));
    }

    @Test
    public void testDuplicatePropertyKeyWithIdentityProperties() {
        SchemaManager schema = graph().schema();
        schema.propertyKey("fakePropKey")
              .asText()
              .ifNotExist()
              .create();
        schema.propertyKey("fakePropKey")
              .asText()
              .checkExist(false)
              .create();
        schema.propertyKey("fakePropKey")
              .userdata("b", "") // won't check userdata
              .asText()
              .checkExist(false)
              .create();
    }

    @Test
    public void testDuplicatePropertyKeyWithDifferentProperties() {
        SchemaManager schema = graph().schema();
        schema.propertyKey("fakePropKey")
              .asText()
              .ifNotExist()
              .create();
        Assert.assertThrows(ExistedException.class, () -> {
            schema.propertyKey("fakePropKey")
                  .asDouble()
                  .checkExist(false)
                  .create();
        });
        Assert.assertThrows(ExistedException.class, () -> {
            schema.propertyKey("fakePropKey")
                  .asText()
                  .valueList()
                  .checkExist(false)
                  .create();
        });
        Assert.assertThrows(ExistedException.class, () -> {
            schema.propertyKey("fakePropKey")
                  .asText()
                  .aggregateType(AggregateType.MAX)
                  .checkExist(false)
                  .create();
        });
    }
}
