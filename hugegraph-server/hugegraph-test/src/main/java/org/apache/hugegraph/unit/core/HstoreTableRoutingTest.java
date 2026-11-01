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

import org.apache.hugegraph.backend.BinaryId;
import org.apache.hugegraph.backend.serializer.BinaryBackendEntry;
import org.apache.hugegraph.backend.store.hstore.HstoreTable;
import org.apache.hugegraph.exception.NotSupportException;
import org.apache.hugegraph.id.EdgeId;
import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.id.IdGenerator;
import org.apache.hugegraph.query.Condition;
import org.apache.hugegraph.query.ConditionQuery;
import org.apache.hugegraph.store.client.util.HgStoreClientConst;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.type.define.Directions;
import org.apache.hugegraph.type.define.HugeKeys;
import org.junit.Test;

/**
 * Owner routing and argument validation of {@link HstoreTable} that can be
 * exercised without a PD/Store cluster. The store client compares the
 * all-partition owner by reference, so those cases use assertSame.
 */
public class HstoreTableRoutingTest {

    private static final Id MARKO = IdGenerator.of("1:marko");
    private static final Id JOSH = IdGenerator.of("1:josh");

    private final RoutingTable table = new RoutingTable("Hugegraph", "G+V");

    @Test
    public void testTableNameCombinesDatabaseAndTableInLowerCase() {
        Assert.assertEquals("hugegraph+g+v", this.table.table());
    }

    @Test
    public void testOwnerOfVertexAndEdgeIds() {
        Assert.assertArrayEquals(MARKO.asBytes(), this.table.ownerOf(MARKO));

        // Both edge directions are routed to the vertex that owns the edge
        Assert.assertArrayEquals(MARKO.asBytes(), this.table.ownerOf(edge(MARKO, JOSH,
                                                                          Directions.OUT)));
        Assert.assertArrayEquals(JOSH.asBytes(), this.table.ownerOf(edge(JOSH, MARKO,
                                                                         Directions.IN)));

        // Serialized ids are unwrapped to their origin before routing
        BinaryId binaryEdge = new BinaryId(new byte[]{1, 2, 3},
                                           edge(MARKO, JOSH, Directions.OUT));
        Assert.assertArrayEquals(MARKO.asBytes(), this.table.ownerOf(binaryEdge));

        Assert.assertSame(HgStoreClientConst.ALL_PARTITION_OWNER, this.table.ownerOf(null));
        Assert.assertSame(HgStoreClientConst.ALL_PARTITION_OWNER,
                          this.table.ownerOf(new BinaryId(new byte[]{1}, null)));
    }

    @Test
    public void testOwnerByTypeRoutesOnlyGraphDataToSinglePartition() {
        Id edgeId = edge(MARKO, JOSH, Directions.OUT);
        Assert.assertArrayEquals(MARKO.asBytes(), this.table.ownerOf(HugeType.VERTEX, MARKO));
        Assert.assertArrayEquals(MARKO.asBytes(), this.table.ownerOf(HugeType.EDGE, edgeId));
        Assert.assertArrayEquals(MARKO.asBytes(), this.table.ownerOf(HugeType.EDGE_OUT, edgeId));
        Assert.assertArrayEquals(MARKO.asBytes(), this.table.ownerOf(HugeType.EDGE_IN, edgeId));
        Assert.assertArrayEquals(MARKO.asBytes(), this.table.ownerOf(HugeType.COUNTER, MARKO));

        // Schema, index and system types must fan out to every partition
        HugeType[] fanOut = {HugeType.VERTEX_LABEL, HugeType.PROPERTY_KEY,
                             HugeType.SECONDARY_INDEX, HugeType.VERTEX_LABEL_INDEX,
                             HugeType.RANGE_INT_INDEX, HugeType.TASK, HugeType.OLAP};
        for (HugeType type : fanOut) {
            Assert.assertSame(type.name(), HgStoreClientConst.ALL_PARTITION_OWNER,
                              this.table.ownerOf(type, MARKO));
        }
    }

    @Test
    public void testInsertOwnerOfEntries() {
        BinaryBackendEntry vertex = new BinaryBackendEntry(HugeType.VERTEX,
                                                           new BinaryId(new byte[]{9}, MARKO));
        Assert.assertArrayEquals(MARKO.asBytes(), this.table.getInsertOwner(vertex));

        BinaryId edgeId = new BinaryId(new byte[]{8}, edge(JOSH, MARKO, Directions.IN));
        BinaryBackendEntry edge = new BinaryBackendEntry(HugeType.EDGE_IN, edgeId);
        Assert.assertArrayEquals(JOSH.asBytes(), this.table.getInsertOwner(edge));
        Assert.assertArrayEquals(JOSH.asBytes(), this.table.getInsertEdgeOwner(edge));

        // A label index entry with exactly one column is hashed by that column
        Id indexId = IdGenerator.of("label-index");
        BinaryBackendEntry labelIndex = new BinaryBackendEntry(HugeType.VERTEX_LABEL_INDEX,
                                                               new BinaryId(new byte[]{7},
                                                                            indexId));
        labelIndex.column(new byte[]{1, 1}, new byte[0]);
        Assert.assertArrayEquals(new byte[]{1, 1}, this.table.getInsertOwner(labelIndex));

        // With more columns it falls back to the index id itself
        labelIndex.column(new byte[]{2, 2}, new byte[0]);
        Assert.assertArrayEquals(indexId.asBytes(), this.table.getInsertOwner(labelIndex));
    }

    @Test
    public void testRemoveDirectionCondition() {
        ConditionQuery onlyDirection = new ConditionQuery(HugeType.EDGE);
        onlyDirection.eq(HugeKeys.DIRECTION, Directions.OUT);
        Assert.assertNull(HstoreTable.removeDirectionCondition(onlyDirection));

        ConditionQuery mixed = new ConditionQuery(HugeType.EDGE);
        mixed.eq(HugeKeys.OWNER_VERTEX, MARKO);
        mixed.eq(HugeKeys.DIRECTION, Directions.OUT);
        ConditionQuery result = HstoreTable.removeDirectionCondition(mixed);
        Assert.assertSame(mixed, result);
        Assert.assertEquals(1, result.conditionsSize());
        Assert.assertEquals(MARKO, result.condition(HugeKeys.OWNER_VERTEX));
        Assert.assertNull(result.condition(HugeKeys.DIRECTION));

        // A compound condition that also constrains another key is kept whole
        ConditionQuery compound = new ConditionQuery(HugeType.EDGE);
        compound.query(Condition.and(Condition.eq(HugeKeys.DIRECTION, Directions.IN),
                                     Condition.eq(HugeKeys.LABEL, IdGenerator.of(1))));
        Assert.assertSame(compound, HstoreTable.removeDirectionCondition(compound));
        Assert.assertEquals(1, compound.conditionsSize());
    }

    @Test
    public void testBytes2StringIsZeroPaddedLowerCaseHex() {
        Assert.assertEquals("", HstoreTable.bytes2String(new byte[0]));
        Assert.assertEquals("000f7f80ff",
                            HstoreTable.bytes2String(new byte[]{0x00, 0x0f, 0x7f,
                                                                (byte) 0x80, (byte) 0xff}));
    }

    @Test
    public void testSplitsMetadataValidatesArgumentsBeforeContactingPd() {
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            this.table.metaDispatcher().dispatchMetaHandler(null, "splits", new Object[0]);
        }, e -> {
            Assert.assertContains("args count of splits must be 1", e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            this.table.metaDispatcher().dispatchMetaHandler(null, "splits",
                                                            new Object[]{1024L});
        }, e -> {
            Assert.assertContains("split-size must be >= 1048576 bytes, but got 1024",
                                  e.getMessage());
        });

        Assert.assertThrows(NotSupportException.class, () -> {
            this.table.metaDispatcher().dispatchMetaHandler(null, "unknown-meta",
                                                            new Object[0]);
        });
    }

    private static EdgeId edge(Id owner, Id other, Directions direction) {
        return new EdgeId(owner, direction, IdGenerator.of(1), IdGenerator.of(1), "", other);
    }

    private static class RoutingTable extends HstoreTable {

        RoutingTable(String database, String table) {
            super(database, table);
        }

        byte[] ownerOf(Id id) {
            return this.getOwnerId(id);
        }

        byte[] ownerOf(HugeType type, Id id) {
            return this.getOwnerId(type, id);
        }
    }
}
