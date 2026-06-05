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

package org.apache.hugegraph.query;

import org.apache.hugegraph.type.define.HugeKeys;
import org.junit.Assert;
import org.junit.Test;

public class ConditionTest {

    @Test
    public void testConditionBooleanRange() {
        Condition lt = Condition.lt(HugeKeys.ID, true);
        Assert.assertTrue(lt.test(false));
        Assert.assertFalse(lt.test(true));

        Condition lte = Condition.lte(HugeKeys.ID, false);
        Assert.assertTrue(lte.test(false));
        Assert.assertFalse(lte.test(true));

        Condition gt = Condition.gt(HugeKeys.ID, false);
        Assert.assertTrue(gt.test(true));
        Assert.assertFalse(gt.test(false));

        Condition gte = Condition.gte(HugeKeys.ID, true);
        Assert.assertTrue(gte.test(true));
        Assert.assertFalse(gte.test(false));

        IllegalArgumentException exception = Assert.assertThrows(
                IllegalArgumentException.class,
                () -> Condition.lt(HugeKeys.ID, true).test(1));
        Assert.assertEquals("Can't compare between 1(Integer) and true(Boolean)",
                            exception.getMessage());

        exception = Assert.assertThrows(IllegalArgumentException.class,
                                        () -> Condition.lt(HugeKeys.ID, true)
                                                       .test((Object) null));
        Assert.assertEquals("Can't compare between null(null) and true(Boolean)",
                            exception.getMessage());
    }
}
