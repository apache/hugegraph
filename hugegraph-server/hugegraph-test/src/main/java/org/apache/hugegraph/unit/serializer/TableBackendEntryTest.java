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

import org.apache.commons.lang3.NotImplementedException;
import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.id.IdGenerator;
import org.apache.hugegraph.backend.serializer.TableBackendEntry;
import org.apache.hugegraph.backend.BackendColumn;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.type.define.Cardinality;
import org.apache.hugegraph.type.define.HugeKeys;
import org.apache.hugegraph.unit.BaseUnitTest;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

public class TableBackendEntryTest extends BaseUnitTest {

    @Test
    public void testType() {
        Id id = IdGenerator.of(1L);
        TableBackendEntry entry = new TableBackendEntry(id);
        Assert.assertNull(entry.type());
        Assert.assertEquals(id, entry.id());

        entry.type(HugeType.VERTEX);
        Assert.assertEquals(HugeType.VERTEX, entry.type());
    }

    @Test
    public void testId() {
        TableBackendEntry entry = new TableBackendEntry(HugeType.VERTEX);
        Assert.assertNull(entry.id());

        Id id = IdGenerator.of(1L);
        entry.id(id);
        Assert.assertEquals(HugeType.VERTEX, entry.type());
        Assert.assertEquals(id, entry.id());

        Assert.assertNull(entry.subId());
        entry.subId(id);
        Assert.assertEquals(id, entry.subId());
    }

    @Test
    public void testSelfChanged() {
        TableBackendEntry entry = new TableBackendEntry(HugeType.VERTEX);
        Assert.assertTrue(entry.selfChanged());

        entry.selfChanged(false);
        Assert.assertFalse(entry.selfChanged());
    }

    @Test
    public void testColumn() {
        TableBackendEntry entry = new TableBackendEntry(HugeType.VERTEX,
                                                        IdGenerator.of(1L));
        entry.column(HugeKeys.ID, "v1");
        Assert.assertEquals("v1", entry.column(HugeKeys.ID));
        Assert.assertEquals("TableBackendEntry{Row{type=VERTEX, id=1, " +
                            "columns={ID=v1}}, sub-rows: []}",
                            entry.toString());

        entry.column(HugeKeys.ID, "v2");
        Assert.assertEquals("v2", entry.column(HugeKeys.ID));
        Assert.assertEquals("TableBackendEntry{Row{type=VERTEX, id=1, " +
                            "columns={ID=v2}}, sub-rows: []}",
                            entry.toString());

        entry.column(HugeKeys.NAME, "tom");
        Assert.assertEquals("tom", entry.column(HugeKeys.NAME));
        Assert.assertEquals("v2", entry.column(HugeKeys.ID));
    }

    @Test
    public void testColumnOfMap() {
        TableBackendEntry entry = new TableBackendEntry(HugeType.VERTEX);
        entry.column(HugeKeys.PROPERTIES, "k1", "v1");
        Assert.assertEquals(ImmutableMap.of("k1", "v1"),
                            entry.column(HugeKeys.PROPERTIES));

        entry.column(HugeKeys.PROPERTIES, "k2", "v2");
        Assert.assertEquals(ImmutableMap.of("k1", "v1", "k2", "v2"),
                            entry.column(HugeKeys.PROPERTIES));

        entry.column(HugeKeys.PROPERTIES, "k3", "v3");
        Assert.assertEquals(ImmutableMap.of("k1", "v1", "k2", "v2", "k3", "v3"),
                            entry.column(HugeKeys.PROPERTIES));

        entry.column(HugeKeys.PROPERTIES, "k1", "v0");
        Assert.assertEquals(ImmutableMap.of("k1", "v0", "k2", "v2", "k3", "v3"),
                            entry.column(HugeKeys.PROPERTIES));
    }

    @Test
    public void testColumnWithCardinalitySingle() {
        TableBackendEntry entry = new TableBackendEntry(HugeType.VERTEX);
        entry.column(HugeKeys.ID, "v1", Cardinality.SINGLE);
        Assert.assertEquals("v1", entry.column(HugeKeys.ID));

        entry.column(HugeKeys.ID, "v2", Cardinality.SINGLE);
        Assert.assertEquals("v2", entry.column(HugeKeys.ID));

        Assert.assertThrows(ClassCastException.class, () -> {
            entry.column(HugeKeys.ID, "v3", Cardinality.SET);
        });
    }

    @Test
    public void testColumnWithCardinalityList() {
        TableBackendEntry entry = new TableBackendEntry(HugeType.VERTEX);
        entry.column(HugeKeys.PROPERTIES, "v1", Cardinality.LIST);
        Assert.assertEquals(ImmutableList.of("v1"),
                            entry.column(HugeKeys.PROPERTIES));

        entry.column(HugeKeys.PROPERTIES, "v2", Cardinality.LIST);
        Assert.assertEquals(ImmutableList.of("v1", "v2"),
                            entry.column(HugeKeys.PROPERTIES));

        entry.column(HugeKeys.PROPERTIES, "v3", Cardinality.LIST);
        Assert.assertEquals(ImmutableList.of("v1", "v2", "v3"),
                            entry.column(HugeKeys.PROPERTIES));

        entry.column(HugeKeys.PROPERTIES, "v2", Cardinality.LIST);
        Assert.assertEquals(ImmutableList.of("v1", "v2", "v3", "v2"),
                            entry.column(HugeKeys.PROPERTIES));
    }

    @Test
    public void testColumnWithCardinalitySet() {
        TableBackendEntry entry = new TableBackendEntry(HugeType.VERTEX);
        entry.column(HugeKeys.PROPERTIES, "v1", Cardinality.SET);
        Assert.assertEquals(ImmutableSet.of("v1"),
                            entry.column(HugeKeys.PROPERTIES));

        entry.column(HugeKeys.PROPERTIES, "v2", Cardinality.SET);
        Assert.assertEquals(ImmutableSet.of("v1", "v2"),
                            entry.column(HugeKeys.PROPERTIES));

        entry.column(HugeKeys.PROPERTIES, "v3", Cardinality.SET);
        Assert.assertEquals(ImmutableSet.of("v1", "v2", "v3"),
                            entry.column(HugeKeys.PROPERTIES));

        entry.column(HugeKeys.PROPERTIES, "v2", Cardinality.SET);
        Assert.assertEquals(ImmutableSet.of("v1", "v2", "v3"),
                            entry.column(HugeKeys.PROPERTIES));
    }

    @Test
    public void testNotImplemented() {
        TableBackendEntry entry = new TableBackendEntry(HugeType.VERTEX);
        BackendColumn col = BackendColumn.of(new byte[]{1}, new byte[]{12});

        Assert.assertThrows(NotImplementedException.class, entry::columnsSize);
        Assert.assertThrows(NotImplementedException.class, () -> {
            entry.columns();
        });
        Assert.assertThrows(NotImplementedException.class, () -> {
            entry.columns(ImmutableList.of(col));
        });
        Assert.assertThrows(NotImplementedException.class, () -> {
            entry.columns(col);
        });
        Assert.assertThrows(NotImplementedException.class, () -> {
            entry.merge(entry);
        });
        Assert.assertThrows(NotImplementedException.class, entry::clear);
    }
}
