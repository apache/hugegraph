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
import org.apache.hugegraph.backend.serializer.BinaryScatterSerializer;
import org.apache.hugegraph.backend.store.BackendEntry;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.structure.HugeEdge;
import org.apache.hugegraph.structure.HugeVertex;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.unit.BaseUnitTest;
import org.apache.hugegraph.unit.FakeObjects;
import org.junit.Test;

public class BinaryScatterSerializerTest extends BaseUnitTest {

    @Test
    public void testVertex() {
        HugeConfig config = FakeObjects.newConfig();
        BinaryScatterSerializer ser = new BinaryScatterSerializer(config);
        HugeEdge edge = new FakeObjects().newEdge(123, 456);

        BackendEntry entry1 = ser.writeVertex(edge.sourceVertex());
        HugeVertex vertex1 = ser.readVertex(edge.graph(), entry1);
        Assert.assertEquals(edge.sourceVertex(), vertex1);
        assertCollectionEquals(edge.sourceVertex().getProperties(),
                               vertex1.getProperties());

        BackendEntry entry2 = ser.writeVertex(edge.targetVertex());
        HugeVertex vertex2 = ser.readVertex(edge.graph(), entry2);
        Assert.assertEquals(edge.targetVertex(), vertex2);
        assertCollectionEquals(edge.targetVertex().getProperties(),
                               vertex2.getProperties());

        vertex2.element().removed(true);
        Assert.assertTrue(vertex2.removed());
        BackendEntry entry3 = ser.writeVertex(vertex2);
        Assert.assertEquals(0, entry3.columnsSize());

        Assert.assertNull(ser.readVertex(edge.graph(), null));
    }

    @Test
    public void testEdge() {
        HugeConfig config = FakeObjects.newConfig();
        BinaryScatterSerializer ser = new BinaryScatterSerializer(config);

        FakeObjects objects = new FakeObjects();
        HugeEdge edge1 = objects.newEdge(123, 456);
        HugeEdge edge2 = objects.newEdge(147, 789);

        BackendEntry entry1 = ser.writeEdge(edge1);
        HugeVertex vertex1 = ser.readVertex(edge1.graph(), parse(entry1));
        Assert.assertEquals(1, vertex1.getEdges().size());
        HugeEdge edge = vertex1.getEdges().iterator().next();
        Assert.assertEquals(edge1, edge);
        assertCollectionEquals(edge1.getProperties(), edge.getProperties());

        BackendEntry entry2 = ser.writeEdge(edge2);
        HugeVertex vertex2 = ser.readVertex(edge1.graph(), parse(entry2));
        Assert.assertEquals(1, vertex2.getEdges().size());
        edge = vertex2.getEdges().iterator().next();
        Assert.assertEquals(edge2, edge);
        assertCollectionEquals(edge2.getProperties(), edge.getProperties());
    }

    private static BackendEntry parse(BackendEntry originEntry) {
        byte[] bytes = originEntry.id().asBytes();
        BackendEntry parsedEntry = new BinaryBackendEntry(originEntry.type(), bytes);
        parsedEntry.columns(originEntry.columns());
        return parsedEntry;
    }
}
