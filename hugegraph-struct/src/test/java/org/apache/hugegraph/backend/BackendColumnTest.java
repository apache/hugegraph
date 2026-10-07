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

package org.apache.hugegraph.backend;

import java.util.HashSet;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

public class BackendColumnTest {

    @Test
    public void testColumnsWithEqualBytesHaveEqualHashes() {
        BackendColumn first = BackendColumn.of(new byte[]{1, 2}, new byte[]{3, 4});
        BackendColumn same = BackendColumn.of(new byte[]{1, 2}, new byte[]{3, 4});
        Assert.assertEquals(first, same);
        Assert.assertEquals(first.hashCode(), same.hashCode());
        Set<BackendColumn> columns = new HashSet<>();
        columns.add(first);
        Assert.assertTrue(columns.contains(same));
        Assert.assertFalse(columns.add(same));
        Assert.assertNotEquals(first, BackendColumn.of(new byte[]{1, 2}, new byte[]{5}));
    }

    @Test
    public void testSharedColumnRetainsMutableByteArraysAndNameOrdering() {
        byte[] name = new byte[]{1};
        byte[] value = new byte[]{2};
        BackendColumn column = BackendColumn.of(name, value);
        Assert.assertSame(name, column.name);
        Assert.assertSame(value, column.value);
        Assert.assertTrue(column.compareTo(BackendColumn.of(new byte[]{2}, value)) < 0);
        Assert.assertEquals(0, column.compareTo(BackendColumn.of(new byte[]{1}, new byte[]{3})));
        Assert.assertTrue(column.compareTo(null) > 0);
        name[0] = 3;
        Assert.assertEquals(3, column.name[0]);
    }
}
