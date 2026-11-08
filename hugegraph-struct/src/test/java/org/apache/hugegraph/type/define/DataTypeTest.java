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

package org.apache.hugegraph.type.define;

import org.junit.Assert;
import org.junit.Test;

public class DataTypeTest {

    @Test
    public void testValueToNumberReturnsNullForNonNumberTypes() {
        Assert.assertNull(DataType.TEXT.valueToNumber("NaN"));
        Assert.assertNull(DataType.BOOLEAN.valueToNumber("Infinity"));
        Assert.assertNull(DataType.DATE.valueToNumber("-Infinity"));
        Assert.assertNull(DataType.BOOLEAN.valueToNumber(1));
    }

    @Test
    public void testValueToNumberKeepsNumberTypeConversions() {
        Assert.assertEquals(3, DataType.INT.valueToNumber(3));
        Assert.assertEquals(3L, DataType.LONG.valueToNumber(3));
        Assert.assertTrue(Double.isNaN(DataType.DOUBLE.valueToNumber("NaN").doubleValue()));
        Assert.assertEquals(Float.POSITIVE_INFINITY,
                            DataType.FLOAT.valueToNumber("Infinity"));
        Assert.assertNull(DataType.INT.valueToNumber("3"));
    }
}
