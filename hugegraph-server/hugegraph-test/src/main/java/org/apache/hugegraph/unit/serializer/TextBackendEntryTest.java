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

import org.apache.hugegraph.id.IdGenerator;
import org.apache.hugegraph.backend.serializer.TextBackendEntry;
import org.apache.hugegraph.backend.BackendColumn;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.type.define.HugeKeys;
import org.apache.hugegraph.unit.BaseUnitTest;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

public class TextBackendEntryTest extends BaseUnitTest {

    @Test
    public void testColumns() {
        TextBackendEntry entry = new TextBackendEntry(HugeType.VERTEX,
                                                      IdGenerator.of(1));
        entry.column(HugeKeys.ID, "1");
        entry.column(HugeKeys.NAME, "tom");

        BackendColumn col1 = BackendColumn.of(new byte[]{'i', 'd'},
                                              new byte[]{'1'});
        BackendColumn col2 = BackendColumn.of(new byte[]{'n', 'a', 'm', 'e'},
                                              new byte[]{'t', 'o', 'm'});

        Assert.assertEquals(2, entry.columnsSize());
        Assert.assertEquals(ImmutableList.of(col1, col2),
                            entry.columns());
    }

    @Test
    public void testCopy() {
        TextBackendEntry entry = new TextBackendEntry(HugeType.VERTEX,
                                                      IdGenerator.of(1));
        entry.column(HugeKeys.ID, "1");
        entry.column(HugeKeys.NAME, "tom");
        Assert.assertEquals(2, entry.columnsSize());

        TextBackendEntry entry2 = entry.copy();
        Assert.assertEquals(2, entry2.columnsSize());
        Assert.assertEquals("1", entry2.column(HugeKeys.ID));
        Assert.assertEquals("tom", entry2.column(HugeKeys.NAME));

        entry2.clear();
        Assert.assertEquals(0, entry2.columnsSize());

        Assert.assertEquals(2, entry.columnsSize());
    }

    @Test
    public void testEquals() {
        TextBackendEntry entry = new TextBackendEntry(HugeType.VERTEX,
                                                      IdGenerator.of(1));
        TextBackendEntry entry2 = new TextBackendEntry(HugeType.VERTEX,
                                                       IdGenerator.of(2));
        TextBackendEntry entry3 = new TextBackendEntry(HugeType.VERTEX,
                                                       IdGenerator.of(1));
        TextBackendEntry entry4 = new TextBackendEntry(HugeType.VERTEX,
                                                       IdGenerator.of(1));
        TextBackendEntry entry5 = new TextBackendEntry(HugeType.VERTEX,
                                                       IdGenerator.of(1));
        entry.column(HugeKeys.NAME, "tom");
        entry2.column(HugeKeys.NAME, "tom");
        entry3.column(HugeKeys.NAME, "tom2");
        entry4.column(HugeKeys.NAME, "tom");
        entry4.column(HugeKeys.LABEL, "person");
        entry5.column(HugeKeys.NAME, "tom");

        Assert.assertNotEquals(entry, entry2);
        Assert.assertNotEquals(entry, entry3);
        Assert.assertNotEquals(entry, entry4);
        Assert.assertNotEquals(entry4, entry);
        Assert.assertEquals(entry, entry5);
    }
}
