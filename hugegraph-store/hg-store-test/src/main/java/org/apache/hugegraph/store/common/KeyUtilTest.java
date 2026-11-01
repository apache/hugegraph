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

package org.apache.hugegraph.store.common;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import org.apache.hugegraph.backend.BinaryId;
import org.apache.hugegraph.id.EdgeId;
import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.id.IdGenerator;
import org.apache.hugegraph.id.IdUtil;
import org.apache.hugegraph.store.constant.HugeServerTables;
import org.apache.hugegraph.store.query.util.KeyUtil;
import org.apache.hugegraph.type.define.Directions;
import org.junit.Test;

public class KeyUtilTest {

    private static final Id OWNER = IdGenerator.of("owner-vertex");
    private static final Id OTHER = IdGenerator.of(987654321L);

    private static EdgeId edge(Directions direction) {
        return new EdgeId(OWNER, direction, IdGenerator.of(3L), IdGenerator.of(3L), "", OTHER);
    }

    @Test
    public void testEdgeKeysOfBothDirectionsRouteToTheOwnerVertex() {
        byte[] expected = KeyUtil.idToBytes(OWNER);
        for (Directions direction : new Directions[]{Directions.OUT, Directions.IN}) {
            byte[] edgeKey = IdUtil.asBytes(edge(direction));
            String table = direction == Directions.OUT ? HugeServerTables.OUT_EDGE_TABLE :
                           HugeServerTables.IN_EDGE_TABLE;

            byte[] ownerKey = KeyUtil.getOwnerKey(table, edgeKey);

            assertArrayEquals(expected, ownerKey);
            // The owner key is a serialized id that decodes back to the owner vertex
            assertEquals(OWNER, IdUtil.fromBytes(ownerKey));
        }
    }

    @Test
    public void testOwnerKeyMatchesVertexKeySoEdgesColocateWithTheirVertex() {
        byte[] vertexKey = IdUtil.asBytes(OWNER);
        byte[] edgeOwnerKey = KeyUtil.getOwnerKey(HugeServerTables.OUT_EDGE_TABLE,
                                                  IdUtil.asBytes(edge(Directions.OUT)));
        assertArrayEquals(KeyUtil.getOwnerKey(HugeServerTables.VERTEX_TABLE, vertexKey),
                          edgeOwnerKey);
    }

    @Test
    public void testNonEdgeTablesReturnTheKeyItself() {
        byte[] key = IdUtil.asBytes(edge(Directions.OUT));
        for (String table : new String[]{HugeServerTables.VERTEX_TABLE,
                                         HugeServerTables.INDEX_TABLE,
                                         HugeServerTables.UNKNOWN_TABLE, null}) {
            assertSame(key, KeyUtil.getOwnerKey(table, key));
        }
        assertFalse(HugeServerTables.isEdgeTable(null));
        assertFalse(HugeServerTables.isEdgeTable("g+oe "));
        assertTrue(HugeServerTables.isEdgeTable("g+ie"));
    }

    @Test
    public void testNullOrEmptyKeyYieldsEmptyOwnerKey() {
        assertEquals(0, KeyUtil.getOwnerKey(HugeServerTables.OUT_EDGE_TABLE, null).length);
        assertEquals(0, KeyUtil.getOwnerKey(HugeServerTables.IN_EDGE_TABLE, new byte[0]).length);
        assertEquals(0, KeyUtil.getOwnerKey(HugeServerTables.VERTEX_TABLE, null).length);
    }

    @Test
    public void testGetOwnerIdUnwrapsBinaryAndEdgeIds() {
        assertArrayEquals(OWNER.asBytes(), KeyUtil.getOwnerId(OWNER));
        assertArrayEquals(OWNER.asBytes(), KeyUtil.getOwnerId(edge(Directions.IN)));

        EdgeId edgeId = edge(Directions.OUT);
        BinaryId wrapped = new BinaryId(IdUtil.asBytes(edgeId), edgeId);
        assertArrayEquals(OWNER.asBytes(), KeyUtil.getOwnerId(wrapped));

        assertEquals(0, KeyUtil.getOwnerId(null).length);
        assertEquals(0, KeyUtil.getOwnerId(new BinaryId(new byte[]{1}, null)).length);
    }
}
