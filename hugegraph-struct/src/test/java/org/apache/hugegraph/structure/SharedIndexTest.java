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

package org.apache.hugegraph.structure;

import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.hugegraph.HugeGraphSupplier;
import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.id.IdGenerator;
import org.apache.hugegraph.struct.schema.IndexLabel;
import org.apache.hugegraph.struct.schema.PropertyKey;
import org.apache.hugegraph.struct.schema.VertexLabel;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.type.define.DataType;
import org.apache.hugegraph.type.define.IndexType;
import org.junit.Assert;
import org.junit.Test;

public class SharedIndexTest {

    @Test
    public void testLegacyIndexIdAndHashBytes() {
        // Fixed Server bytes from compatibility/2f827d6e8 fixtures, not a
        // round-trip assertion using the same writer and reader.
        Assert.assertArrayEquals(new byte[]{(byte) 160, 0, 0, 0, 3, (byte) 128, 0, 0, 7},
                                 Index.formatIndexId(HugeType.RANGE_INT_INDEX,
                                                     id(3), 7).asBytes());
        Assert.assertArrayEquals(new byte[]{(byte) 162, 0, 0, 0, 3,
                                           (byte) 128, 0, 0, 0, 0, 0, 0, 7},
                                 Index.formatIndexId(HugeType.RANGE_LONG_INDEX,
                                                     id(3), 7L).asBytes());
        // Frozen Server index-builder-contract.properties stores this hash.
        Assert.assertArrayEquals("SI:F:224054a2".getBytes(StandardCharsets.UTF_8),
                                 Index.formatIndexHashId(HugeType.SECONDARY_INDEX, id(15),
                                                         "long-field-".repeat(20)).asBytes());
        Assert.assertThrows(IllegalStateException.class,
                            () -> Index.formatIndexHashId(HugeType.RANGE_INT_INDEX, id(3), 7));
    }

    @Test
    public void testLegacyRangeIdParsesWithSchemaType() {
        IndexLabel label = label(null);
        label.indexType(IndexType.RANGE_INT);
        label.indexFields(id(2));
        HugeGraphSupplier graph = graph(label, 100);
        Index parsed = Index.parseIndexId(graph, HugeType.RANGE_INT_INDEX,
                                           new byte[]{(byte) 160, 0, 0, 0, 3,
                                                      (byte) 128, 0, 0, 7});
        Assert.assertSame(label, parsed.indexLabel());
        Assert.assertEquals(7, parsed.fieldValues());
    }

    @Test
    public void testCloneResetKeepsEveryExpirationEntry() {
        IndexLabel label = label(null);
        Index original = new Index(graph(label, 100), label);
        original.fieldValues("value");
        original.elementIds(id(1), 90);
        original.elementIds(id(2), 95);
        original.elementIds(id(3), 110);
        Set<Index.IdWithExpiredTime> expired = original.expiredElementIds();
        Assert.assertEquals(2, expired.size());

        Index deletion = original.clone();
        Assert.assertEquals(original.id(), deletion.id());
        deletion.resetElementIds();
        for (Index.IdWithExpiredTime entry : expired) {
            deletion.elementIds(entry.id(), entry.expiredTime());
        }
        Assert.assertEquals(ids(1, 2), deletion.elementIds());
        Assert.assertEquals(ids(3), original.elementIds());
        Set<Index.IdWithExpiredTime> copied = deletion.expiredElementIds();
        long[] times = copied.stream().mapToLong(Index.IdWithExpiredTime::expiredTime).toArray();
        Assert.assertArrayEquals(new long[]{90, 95}, times);
        Assert.assertEquals(ids(3), original.elementIds());
    }

    @Test
    public void testExpirationBoundaryAndTtl() {
        IndexLabel label = label(null);
        HugeGraphSupplier graph = graph(label, 100);
        Index index = new Index(graph, label(graph));
        index.elementIds(id(1), 0);
        index.elementIds(id(2), 99);
        index.elementIds(id(3), 100);
        index.elementIds(id(4), 101);
        Assert.assertEquals(1, index.expiredElementIds().size());
        Assert.assertEquals(ids(1, 3, 4), index.elementIds());
        index.resetElementIds();
        index.elementIds(id(4), 101);
        Assert.assertTrue(index.hasTtl());
        Assert.assertEquals(1, index.ttl());
        Assert.assertEquals(101, index.expiredTime());
    }

    @Test
    public void testSystemIndexDoesNotAcquirePropertyTtl() {
        HugeGraphSupplier graph = graph(label(null), 100);
        Index index = new Index(graph, IndexLabel.label(HugeType.VERTEX));
        index.elementIds(id(1), 120);
        Assert.assertFalse(index.hasTtl());
        Assert.assertEquals(120, index.expiredTime());
    }

    @Test
    public void testWriteModeResetAndExpiration() {
        IndexLabel label = label(null);
        Index index = new Index(graph(label, 100), label, true);
        index.elementIds(id(1), 99);
        Assert.assertEquals(1, index.expiredElementIds().size());
        Assert.assertThrows(IllegalStateException.class, index::elementId);
        index.elementIds(id(2), 150);
        Index clone = index.clone();
        clone.resetElementIds();
        clone.elementIds(id(3), 160);
        Assert.assertEquals(id(2), index.elementId());
        Assert.assertEquals(150, index.expiredTime());
        Assert.assertEquals(id(3), clone.elementId());
        Assert.assertEquals(160, clone.expiredTime());
        Assert.assertTrue(clone.expiredElementIds().isEmpty());
    }

    private static Id id(long value) {
        return IdGenerator.of(value);
    }

    private static Set<Id> ids(long... values) {
        Set<Id> result = new LinkedHashSet<>();
        Arrays.stream(values).mapToObj(SharedIndexTest::id).forEach(result::add);
        return result;
    }

    private static IndexLabel label(HugeGraphSupplier graph) {
        IndexLabel label = new IndexLabel(graph, id(3), "by_value");
        label.baseType(HugeType.VERTEX_LABEL);
        label.baseValue(id(1));
        label.indexType(IndexType.SECONDARY);
        return label;
    }

    private static HugeGraphSupplier graph(IndexLabel label, long now) {
        final HugeGraphSupplier[] holder = new HugeGraphSupplier[1];
        holder[0] = (HugeGraphSupplier) Proxy.newProxyInstance(
                HugeGraphSupplier.class.getClassLoader(), new Class<?>[]{HugeGraphSupplier.class},
                (proxy, method, args) -> {
                    switch (method.getName()) {
                        case "now":
                            return now;
                        case "indexLabel":
                            return label;
                        case "vertexLabel":
                            VertexLabel vertexLabel = new VertexLabel(holder[0], id(1), "person");
                            vertexLabel.ttl(1000);
                            return vertexLabel;
                        case "propertyKey":
                            PropertyKey key = new PropertyKey(holder[0], id(2), "value");
                            key.dataType(DataType.INT);
                            return key;
                        default:
                            throw new UnsupportedOperationException(method.getName());
                    }
                });
        return holder[0];
    }
}
