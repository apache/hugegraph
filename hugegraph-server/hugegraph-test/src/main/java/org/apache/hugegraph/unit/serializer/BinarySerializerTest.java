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

import java.util.Arrays;
import java.util.Date;
import java.util.Set;

import org.apache.hugegraph.backend.id.IdGenerator;
import org.apache.hugegraph.backend.serializer.BinarySerializer;
import org.apache.hugegraph.backend.store.BackendEntry;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.schema.PropertyKey;
import org.apache.hugegraph.schema.Userdata;
import org.apache.hugegraph.structure.HugeEdge;
import org.apache.hugegraph.structure.HugeVertex;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.testutil.Whitebox;
import org.apache.hugegraph.type.define.Cardinality;
import org.apache.hugegraph.type.define.DataType;
import org.apache.hugegraph.unit.BaseUnitTest;
import org.apache.hugegraph.unit.FakeObjects;
import org.apache.hugegraph.util.DateUtil;
import org.junit.Test;

public class BinarySerializerTest extends BaseUnitTest {

    @Test
    public void testVertex() {
        HugeConfig config = FakeObjects.newConfig();
        BinarySerializer ser = new BinarySerializer(config);
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

        Whitebox.setInternalState(vertex2, "removed", true);
        Assert.assertTrue(vertex2.removed());
        BackendEntry entry3 = ser.writeVertex(vertex2);
        Assert.assertEquals(0, entry3.columnsSize());

        Assert.assertNull(ser.readVertex(edge.graph(), null));
    }

    @Test
    public void testEdge() {
        HugeConfig config = FakeObjects.newConfig();
        BinarySerializer ser = new BinarySerializer(config);

        FakeObjects objects = new FakeObjects();
        HugeEdge edge1 = objects.newEdge(123, 456);
        HugeEdge edge2 = objects.newEdge(147, 789);

        BackendEntry entry1 = ser.writeEdge(edge1);
        HugeVertex vertex1 = ser.readVertex(edge1.graph(), entry1);
        Assert.assertEquals(1, vertex1.getEdges().size());
        HugeEdge edge = vertex1.getEdges().iterator().next();
        Assert.assertEquals(edge1, edge);
        assertCollectionEquals(edge1.getProperties(), edge.getProperties());

        BackendEntry entry2 = ser.writeEdge(edge2);
        HugeVertex vertex2 = ser.readVertex(edge1.graph(), entry2);
        Assert.assertEquals(1, vertex2.getEdges().size());
        edge = vertex2.getEdges().iterator().next();
        Assert.assertEquals(edge2, edge);
        assertCollectionEquals(edge2.getProperties(), edge.getProperties());
    }

    @Test
    public void testVertexForPartition() {
        BinarySerializer ser = new BinarySerializer(true, true, true);
        HugeEdge edge = new FakeObjects().newEdge("123", "456");

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

        Whitebox.setInternalState(vertex2, "removed", true);
        Assert.assertTrue(vertex2.removed());
        BackendEntry entry3 = ser.writeVertex(vertex2);
        Assert.assertEquals(0, entry3.columnsSize());

        Assert.assertNull(ser.readVertex(edge.graph(), null));
    }

    @Test
    public void testPropertyKeyUserdataCreateTimeRoundTripsAsDate() {
        HugeConfig config = FakeObjects.newConfig();
        BinarySerializer ser = new BinarySerializer(config);

        FakeObjects objects = new FakeObjects();
        PropertyKey original = objects.newPropertyKey(IdGenerator.of(1L),
                                                      "name");
        Date created = DateUtil.parse("2026-05-14 10:11:12.345");
        original.userdata(Userdata.CREATE_TIME, created);

        BackendEntry entry = ser.writePropertyKey(original);
        PropertyKey reloaded = ser.readPropertyKey(objects.graph(), entry);

        Object value = reloaded.userdata().get(Userdata.CREATE_TIME);
        Assert.assertTrue("CREATE_TIME should be a Date after round-trip, " +
                          "was " + (value == null ? "null" : value.getClass()),
                          value instanceof Date);
        Assert.assertEquals(created, value);
    }

    @Test
    public void testPropertyKeyDefaultValueRoundTripsAsDate() {
        HugeConfig config = FakeObjects.newConfig();
        BinarySerializer ser = new BinarySerializer(config);

        FakeObjects objects = new FakeObjects();
        PropertyKey original = objects.newPropertyKey(IdGenerator.of(1L),
                                                      "name", DataType.DATE);
        Date defaultValue = DateUtil.parse("2026-05-14 10:11:12.345");
        original.userdata(Userdata.DEFAULT_VALUE, defaultValue);

        BackendEntry entry = ser.writePropertyKey(original);
        PropertyKey reloaded = ser.readPropertyKey(objects.graph(), entry);

        Object value = reloaded.defaultValue();
        Assert.assertTrue("DEFAULT_VALUE should be a Date after round-trip, " +
                          "was " + (value == null ? "null" : value.getClass()),
                          value instanceof Date);
        Assert.assertEquals(defaultValue, value);
    }

    @Test
    public void testPropertyKeySetDefaultValueRoundTripsAsDate() {
        HugeConfig config = FakeObjects.newConfig();
        BinarySerializer ser = new BinarySerializer(config);

        FakeObjects objects = new FakeObjects();
        PropertyKey original = objects.newPropertyKey(IdGenerator.of(2L),
                                                      "tags", DataType.DATE);
        original.cardinality(Cardinality.SET);

        String dateStr = "2026-05-14 10:11:12.345";
        Date expected = DateUtil.parse(dateStr);
        // ArrayList<String> with duplicates — what JSON deserialization produces
        original.userdata(Userdata.DEFAULT_VALUE, Arrays.asList(dateStr, dateStr));

        BackendEntry entry = ser.writePropertyKey(original);
        PropertyKey reloaded = ser.readPropertyKey(objects.graph(), entry);

        Object value = reloaded.defaultValue();
        Assert.assertTrue("DEFAULT_VALUE should be a Set after round-trip, was " +
                          (value == null ? "null" : value.getClass()),
                          value instanceof Set);
        Set<?> values = (Set<?>) value;
        Assert.assertEquals("duplicates must be collapsed", 1, values.size());
        Assert.assertTrue(values.contains(expected));
    }

    @Test
    public void testEdgeForPartition() {
        BinarySerializer ser = new BinarySerializer(true, true, true);

        FakeObjects objects = new FakeObjects();
        HugeEdge edge1 = objects.newEdge("123", "456");
        HugeEdge edge2 = objects.newEdge("147", "789");

        BackendEntry entry1 = ser.writeEdge(edge1);
        HugeVertex vertex1 = ser.readVertex(edge1.graph(), ser.parse(entry1));
        Assert.assertEquals(1, vertex1.getEdges().size());
        HugeEdge edge = vertex1.getEdges().iterator().next();
        Assert.assertEquals(edge1, edge);
        assertCollectionEquals(edge1.getProperties(), edge.getProperties());

        BackendEntry entry2 = ser.writeEdge(edge2);
        HugeVertex vertex2 = ser.readVertex(edge1.graph(), ser.parse(entry2));
        Assert.assertEquals(1, vertex2.getEdges().size());
        edge = vertex2.getEdges().iterator().next();
        Assert.assertEquals(edge2, edge);
        assertCollectionEquals(edge2.getProperties(), edge.getProperties());
    }
}
