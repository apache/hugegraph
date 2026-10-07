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

package org.apache.hugegraph.unit.serializer;

import org.apache.hugegraph.backend.serializer.BinaryBackendEntry;
import org.apache.hugegraph.backend.BackendColumn;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.unit.BaseUnitTest;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

public class BinaryBackendEntryTest extends BaseUnitTest {

    @Test
    public void testColumns() {
        BinaryBackendEntry entry = new BinaryBackendEntry(HugeType.VERTEX,
                                                          new byte[]{1, 2});
        BackendColumn col = BackendColumn.of(new byte[]{1, 2},
                                             new byte[]{3, 4});

        entry.columns(ImmutableList.of(col));
        Assert.assertEquals(1, entry.columnsSize());
        Assert.assertEquals(ImmutableList.of(col), entry.columns());
        Assert.assertSame(col, entry.columns().iterator().next());

        entry.columns(ImmutableList.of(col, col));
        Assert.assertEquals(3, entry.columnsSize());
        Assert.assertEquals(ImmutableList.of(col, col, col), entry.columns());
    }

    @Test
    public void testClear() {
        BinaryBackendEntry entry = new BinaryBackendEntry(HugeType.VERTEX,
                                                          new byte[]{1, 2});
        BackendColumn col = BackendColumn.of(new byte[]{1, 2},
                                             new byte[]{3, 4});

        entry.column(col);
        Assert.assertEquals(1, entry.columnsSize());
        Assert.assertEquals(ImmutableList.of(col), entry.columns());

        entry.clear();
        Assert.assertEquals(0, entry.columnsSize());
        Assert.assertEquals(ImmutableList.of(), entry.columns());
    }

    @Test
    public void testMerge() {
        BinaryBackendEntry entry = new BinaryBackendEntry(HugeType.VERTEX,
                                                          new byte[]{1, 2});
        BinaryBackendEntry entry2 = new BinaryBackendEntry(HugeType.VERTEX,
                                                           new byte[]{2, 2});
        BackendColumn col = BackendColumn.of(new byte[]{1, 2},
                                             new byte[]{3, 4});
        BackendColumn col2 = BackendColumn.of(new byte[]{5, 6},
                                              new byte[]{7, 8});

        entry.column(col);
        entry2.column(col2);
        Assert.assertEquals(1, entry.columnsSize());
        Assert.assertEquals(ImmutableList.of(col), entry.columns());

        entry.merge(entry2);
        Assert.assertEquals(2, entry.columnsSize());
        Assert.assertEquals(ImmutableList.of(col, col2), entry.columns());
    }

    @Test
    public void testEquals() {
        BinaryBackendEntry entry = new BinaryBackendEntry(HugeType.VERTEX,
                                                          new byte[]{1, 2});
        BinaryBackendEntry entry2 = new BinaryBackendEntry(HugeType.VERTEX,
                                                           new byte[]{2, 2});
        BinaryBackendEntry entry3 = new BinaryBackendEntry(HugeType.VERTEX,
                                                           new byte[]{1, 2});
        BinaryBackendEntry entry4 = new BinaryBackendEntry(HugeType.VERTEX,
                                                           new byte[]{1, 2});
        BinaryBackendEntry entry5 = new BinaryBackendEntry(HugeType.VERTEX,
                                                           new byte[]{1, 2});
        BackendColumn col = BackendColumn.of(new byte[]{1, 2},
                                             new byte[]{3, 4});
        BackendColumn col2 = BackendColumn.of(new byte[]{5, 6},
                                              new byte[]{7, 8});

        entry.column(col);
        entry2.column(col2);
        entry3.column(col2);
        entry4.column(col);
        entry4.column(col2);
        entry5.column(col);

        Assert.assertNotEquals(entry, entry2);
        Assert.assertNotEquals(entry, entry3);
        Assert.assertNotEquals(entry, entry4);
        Assert.assertEquals(entry, entry5);
    }
}
