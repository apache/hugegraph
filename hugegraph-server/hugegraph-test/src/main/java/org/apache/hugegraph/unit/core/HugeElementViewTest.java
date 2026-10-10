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

import java.util.Collection;
import java.util.Iterator;

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.id.IdGenerator;
import org.apache.hugegraph.struct.schema.EdgeLabel;
import org.apache.hugegraph.struct.schema.PropertyKey;
import org.apache.hugegraph.struct.schema.VertexLabel;
import org.apache.hugegraph.structure.BaseEdge;
import org.apache.hugegraph.structure.BaseProperty;
import org.apache.hugegraph.structure.BaseVertex;
import org.apache.hugegraph.structure.HugeEdge;
import org.apache.hugegraph.structure.HugeProperty;
import org.apache.hugegraph.structure.HugeVertex;
import org.apache.hugegraph.structure.HugeVertexProperty;
import org.apache.hugegraph.type.define.Frequency;
import org.apache.hugegraph.type.define.IdStrategy;
import org.apache.hugegraph.unit.FakeObjects;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class HugeElementViewTest {

    @Test
    public void testPropertyViewWrapsOnlyConsumedEntries() {
        FakeObjects objects = new FakeObjects();
        PropertyKey name = objects.newPropertyKey(IdGenerator.of(1), "name");
        PropertyKey city = objects.newPropertyKey(IdGenerator.of(2), "city");
        VertexLabel label = objects.newVertexLabel(IdGenerator.of(1), "person",
                                                   IdStrategy.CUSTOMIZE_NUMBER,
                                                   name.id(), city.id());
        CountingVertex vertex = new CountingVertex(objects.graph(), label);
        vertex.addProperty(name, "tom");
        vertex.wrapped = 0;

        Collection<HugeProperty<?>> properties = vertex.getProperties();
        Assert.assertEquals(1, properties.size());
        Assert.assertEquals(0, vertex.wrapped);
        Iterator<HugeProperty<?>> iterator = properties.iterator();
        Assert.assertTrue(iterator.hasNext());
        Assert.assertEquals(0, vertex.wrapped);
        Assert.assertEquals("tom", iterator.next().value());
        Assert.assertEquals(1, vertex.wrapped);

        vertex.addProperty(city, "Beijing");
        vertex.wrapped = 0;
        Assert.assertEquals(2, properties.size());
        Assert.assertEquals(0, vertex.wrapped);
        properties.clear();
        Assert.assertEquals(0, vertex.sizeOfProperties());
        Assert.assertEquals(0, vertex.wrapped);
    }

    @Test
    public void testPropertyViewIteratorRemovesSharedEntry() {
        FakeObjects objects = new FakeObjects();
        PropertyKey name = objects.newPropertyKey(IdGenerator.of(1), "name");
        VertexLabel label = objects.newVertexLabel(IdGenerator.of(1), "person",
                                                   IdStrategy.CUSTOMIZE_NUMBER, name.id());
        CountingVertex vertex = new CountingVertex(objects.graph(), label);
        vertex.addProperty(name, "tom");
        Iterator<HugeProperty<?>> iterator = vertex.getProperties().iterator();
        iterator.next();
        iterator.remove();
        Assert.assertFalse(vertex.hasProperty(name.id()));
        Assert.assertFalse(vertex.element().hasProperty(name.id()));
    }

    @Test
    public void testFreshEngineStateViewsDoNotShareMutableEmptyProperties() {
        FakeObjects objects = new FakeObjects();
        PropertyKey key = objects.newPropertyKey(IdGenerator.of(1), "name");
        VertexLabel label = objects.newVertexLabel(IdGenerator.of(1), "person",
                                                   IdStrategy.CUSTOMIZE_NUMBER, key.id());
        CountingVertex first = new CountingVertex(objects.graph(), label);
        CountingVertex second = new CountingVertex(objects.graph(), label);
        Collection<HugeProperty<?>> secondView = second.getProperties();
        Assert.assertEquals(0, secondView.size());
        Assert.assertFalse(secondView.iterator().hasNext());
        Assert.assertEquals(0, second.wrapped);
        first.element().properties().put(1, new BaseProperty<>(key, "first"));
        Assert.assertEquals("first", first.getPropertyValue(key.id()));
        Assert.assertTrue(secondView.isEmpty());
        secondView.clear();
        Assert.assertEquals("first", first.getPropertyValue(key.id()));
        Assert.assertEquals(0, second.wrapped);
    }

    @Test
    public void testAdoptedAdjacencyPreservesOwnerStateAndStableWrappers() {
        FakeObjects objects = new FakeObjects();
        PropertyKey name = objects.newPropertyKey(IdGenerator.of(1), "name");
        VertexLabel label = objects.newVertexLabel(IdGenerator.of(1), "person",
                                                   IdStrategy.CUSTOMIZE_NUMBER, name.id());
        EdgeLabel link = objects.newEdgeLabel(IdGenerator.of(2), "link", Frequency.SINGLE,
                                              label.id(), label.id());
        for (boolean out : new boolean[]{true, false}) {
            BaseVertex owner = new BaseVertex(IdGenerator.of(1), label);
            owner.addProperty(name, "tom");
            BaseEdge base = BaseEdge.constructEdge(objects.graph(), owner, out, link, "", IdGenerator.of(2));
            base.expiredTime(123);
            HugeVertex vertex = new HugeVertex(objects.graph(), owner);
            Assert.assertEquals(1, vertex.getEdges().size());
            HugeEdge edge = vertex.getEdges().iterator().next();
            Assert.assertSame(vertex, edge.ownerVertex());
            Assert.assertSame(base, edge.element());
            Assert.assertSame(owner.getProperty(name.id()), vertex.getProperty(name.id()).baseProperty());
            Assert.assertSame(edge, vertex.getEdges().iterator().next());
            Assert.assertEquals(out, edge.element().isOutEdge());
            Assert.assertEquals(123, edge.expiredTime());
            Assert.assertFalse(vertex.isPropLoaded());
            vertex.removeEdge(edge);
            Assert.assertTrue(owner.edges().isEmpty());
        }
    }

    @Test
    public void testCanonicalSelfLoopAndCopyResetKeepMembershipConsistent() {
        FakeObjects objects = new FakeObjects();
        VertexLabel label = objects.newVertexLabel(IdGenerator.of(1), "person", IdStrategy.CUSTOMIZE_NUMBER);
        EdgeLabel link = objects.newEdgeLabel(IdGenerator.of(2), "link", Frequency.SINGLE,
                                              label.id(), label.id());
        BaseVertex base = new BaseVertex(IdGenerator.of(1), label);
        BaseEdge loop = new BaseEdge(link, true);
        loop.vertices(true, base, base);
        loop.name("");
        loop.assignId();
        base.addEdge(loop);
        HugeVertex vertex = new HugeVertex(objects.graph(), base);
        HugeEdge edge = vertex.getEdges().iterator().next();
        Assert.assertTrue(edge.selfLoop());
        Assert.assertSame(vertex, edge.sourceVertex());
        Assert.assertSame(vertex, edge.targetVertex());
        Assert.assertSame(vertex, edge.switchOwner().ownerVertex());
        HugeVertex copy = vertex.copy();
        copy.resetEdges();
        Assert.assertTrue(copy.getEdges().isEmpty());
        Assert.assertEquals(1, vertex.getEdges().size());
        Assert.assertEquals(1, base.edges().size());
    }

    @Test
    public void testConstructedEdgeUsesOneAdjacencyMembershipAndOriginalOwner() {
        FakeObjects objects = new FakeObjects();
        VertexLabel label = objects.newVertexLabel(IdGenerator.of(1), "person", IdStrategy.CUSTOMIZE_NUMBER);
        EdgeLabel link = objects.newEdgeLabel(IdGenerator.of(2), "link", Frequency.SINGLE,
                                              label.id(), label.id());
        HugeVertex vertex = new HugeVertex(objects.graph(), IdGenerator.of(1), label);
        HugeEdge edge = HugeEdge.constructEdge(vertex, false, link, "", IdGenerator.of(2));
        Assert.assertSame(vertex, edge.ownerVertex());
        Assert.assertSame(edge, vertex.getEdges().iterator().next());
        Assert.assertEquals(1, vertex.element().edges().size());
        Assert.assertEquals(1, edge.otherVertex().element().edges().size());
        Assert.assertSame(edge.element(), vertex.element().edges().iterator().next());
        Assert.assertSame(edge.otherVertex(), edge.otherVertex().getEdges().iterator().next().ownerVertex());
    }

    @Test
    public void testEdgeConstructionWithoutSchemaLookupRetainsEngineGraph() {
        FakeObjects objects = new FakeObjects();
        VertexLabel label = objects.newVertexLabel(IdGenerator.of(1), "person", IdStrategy.CUSTOMIZE_NUMBER);
        EdgeLabel link = objects.newEdgeLabel(IdGenerator.of(2), "link", Frequency.SINGLE,
                                              label.id(), label.id());
        HugeVertex owner = new HugeVertex(objects.graph(), IdGenerator.of(1), label);
        HugeEdge withoutLabel = HugeEdge.constructEdgeWithoutLabel(owner, true, "", IdGenerator.of(2));
        Assert.assertSame(owner, withoutLabel.ownerVertex());
        Assert.assertSame(label, owner.schemaLabel());
        Assert.assertTrue(withoutLabel.otherVertex().schemaLabel().undefined());
        Assert.assertEquals(VertexLabel.NONE.id(), withoutLabel.otherVertex().schemaLabel().id());
        Assert.assertSame(objects.graph(), withoutLabel.schemaLabel().graph());
        Assert.assertSame(objects.graph(), withoutLabel.otherVertex().schemaLabel().graph());
        Assert.assertEquals(1, owner.element().edges().size());

        owner.resetEdges();
        HugeEdge withoutLookup = HugeEdge.constructEdgeWithoutGraph(owner, true, link, "", IdGenerator.of(2));
        Assert.assertSame(objects.graph(), withoutLookup.graph());
        Assert.assertSame(owner, withoutLookup.ownerVertex());
        Assert.assertTrue(withoutLookup.otherVertex().schemaLabel().undefined());
        Assert.assertSame(objects.graph(), withoutLookup.schemaLabel().graph());
        Assert.assertSame(objects.graph(), withoutLookup.ownerVertex().schemaLabel().graph());
        Assert.assertSame(objects.graph(), withoutLookup.otherVertex().schemaLabel().graph());
        Assert.assertEquals(1, owner.element().edges().size());
        Mockito.verify(objects.graph(), Mockito.never()).vertexLabelOrNone(Mockito.any());
    }

    private static final class CountingVertex extends HugeVertex {

        private int wrapped;

        private CountingVertex(HugeGraph graph, VertexLabel label) {
            super(graph, IdGenerator.of(1), label);
        }

        @Override
        protected <V> HugeVertexProperty<V> wrapProperty(BaseProperty<V> property) {
            this.wrapped++;
            return super.wrapProperty(property);
        }
    }
}
