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

import java.util.Arrays;
import java.util.Date;
import java.util.Set;

import org.apache.hugegraph.id.IdGenerator;
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
}
