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

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.id.IdGenerator;
import org.apache.hugegraph.query.Condition;
import org.apache.hugegraph.struct.schema.EdgeLabel;
import org.apache.hugegraph.struct.schema.PropertyKey;
import org.apache.hugegraph.struct.schema.Userdata;
import org.apache.hugegraph.struct.schema.VertexLabel;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.type.define.Cardinality;
import org.apache.hugegraph.type.define.DataType;
import org.apache.hugegraph.type.define.IdStrategy;
import org.junit.Assert;
import org.junit.Test;

public class SharedElementTest {

    @Test
    public void testSingleUpdateCallbackSeesPreviousValue() {
        BaseVertex vertex = vertex(1);
        PropertyKey key = key(Cardinality.SINGLE);
        vertex.addProperty(key, "before");
        AtomicBoolean notified = new AtomicBoolean();
        vertex.addProperty(key, "after", BaseProperty::new, property -> {
            Assert.assertEquals("before", vertex.getPropertyValue(key.id()));
            Assert.assertEquals("after", property.value());
            notified.set(true);
        }, property -> Assert.fail("Single update notified after replacement"));
        Assert.assertTrue(notified.get());
        Assert.assertEquals("after", vertex.getPropertyValue(key.id()));
    }

    @Test
    public void testCollectionUpdateSharesPropertyAndNotifiesAfterMutation() {
        BaseVertex vertex = vertex(1);
        PropertyKey key = key(Cardinality.SET);
        BaseProperty<?> property = vertex.addProperty(key, "first");
        vertex.addProperty(key, Arrays.asList("first", "second"), BaseProperty::new,
                           value -> Assert.fail("Collection invoked single callback"), value -> {
                               Assert.assertSame(property, value);
                               Assert.assertEquals(2, ((Collection<?>) value.value()).size());
                           });
        Assert.assertSame(property, vertex.getProperty(key.id()));
    }

    @Test
    public void testCloneStateAndCopyPropertyMapAreIndependent() throws Exception {
        BaseVertex original = vertex(1);
        PropertyKey key = key(Cardinality.SINGLE);
        original.addProperty(key, "original");
        BaseVertex copy = (BaseVertex) original.clone();
        copy.copyProperties(original);
        copy.removed(true);
        copy.expiredTime(100);
        copy.addProperty(key, "replacement");
        Assert.assertFalse(original.removed());
        Assert.assertEquals(0, original.expiredTime());
        Assert.assertEquals("original", original.getPropertyValue(key.id()));
        Assert.assertEquals("replacement", copy.getPropertyValue(key.id()));
    }

    @Test
    public void testSwitchOwnerPreservesSharedPropertiesAndOriginalDirection() {
        EdgeLabel label = new EdgeLabel(null, IdGenerator.of(3), "link");
        BaseVertex source = vertex(1);
        BaseVertex target = vertex(2);
        BaseEdge original = new BaseEdge(label, true);
        original.vertices(true, source, target);
        original.name("");
        original.assignId();
        PropertyKey key = key(Cardinality.SINGLE);
        original.addProperty(key, "value");
        BaseEdge reverse = original.switchOwner();
        Assert.assertTrue(original.isOutEdge());
        Assert.assertFalse(reverse.isOutEdge());
        Assert.assertEquals(source.id(), original.ownerVertexId());
        Assert.assertEquals(target.id(), reverse.ownerVertexId());
        Assert.assertSame(original.getProperty(key.id()), reverse.getProperty(key.id()));
        reverse.resetProperties();
        Assert.assertTrue(original.hasProperty(key.id()));
        Assert.assertFalse(reverse.hasProperty(key.id()));
    }

    @Test
    public void testLazyPropertyReadsAreUsedByValueMapAndEncoding() {
        BaseVertex vertex = vertex(1);
        PropertyKey key = key(Cardinality.SINGLE);
        BaseProperty<String> lazy = new BaseProperty<String>(key, null) {
            @Override
            public String value() {
                return "loaded";
            }
        };
        vertex.addProperty(lazy);
        Assert.assertEquals("loaded", vertex.getPropertiesMap().get(key.id()));
        Assert.assertEquals(key.serialValue("loaded", true), lazy.serialValue(true));
        Assert.assertTrue(Condition.eq(key.id(), "loaded").test(vertex));
    }

    @Test
    public void testQueryDoesNotLoadPropertiesOrInsertDefaultsImplicitly() {
        BaseVertex vertex = vertex(1);
        PropertyKey key = key(Cardinality.SINGLE);
        key.userdata(Userdata.DEFAULT_VALUE, "default");
        vertex.schemaLabel().property(key.id());
        vertex.propLoaded(false);
        Assert.assertFalse(Condition.eq(key.id(), "default").test(vertex));
        Assert.assertFalse(vertex.propLoaded());
        Assert.assertFalse(vertex.defaultValueUpdated());

        // Engine accessors explicitly load backend data before updating defaults.
        vertex.propLoaded(true);
        vertex.updateDefaultValues(id -> key);
        Assert.assertTrue(Condition.eq(key.id(), "default").test(vertex));
        Assert.assertTrue(vertex.defaultValueUpdated());
        vertex.addProperty(key, "explicit");
        vertex.updateDefaultValues(id -> key);
        Assert.assertEquals("explicit", vertex.getPropertyValue(key.id()));
    }

    @Test
    public void testFreshPropertyMapsCannotAffectOtherElements() throws Exception {
        BaseVertex first = vertex(1);
        BaseVertex second = vertex(2);
        BaseVertex clonedEmpty = (BaseVertex) first.clone();
        PropertyKey key = key(Cardinality.SINGLE);
        Assert.assertNull(first.removeProperty(key.id()));
        first.properties().put(BaseElement.intFromId(key.id()), new BaseProperty<>(key, "first"));
        Assert.assertEquals("first", first.getPropertyValue(key.id()));
        Assert.assertFalse(second.hasProperties());
        Assert.assertFalse(clonedEmpty.hasProperties());
        second.properties().put(BaseElement.intFromId(key.id()), new BaseProperty<>(key, "second"));
        first.properties().clear();
        Assert.assertEquals("second", second.getPropertyValue(key.id()));
        Assert.assertFalse(clonedEmpty.hasProperties());
        BaseVertex copied = (BaseVertex) second.clone();
        copied.copyProperties(second);
        copied.properties().clear();
        Assert.assertEquals("second", second.getPropertyValue(key.id()));
    }

    @Test
    public void testSharedPrimaryNameUsesReservedValueContract() {
        PropertyKey key = key(Cardinality.SINGLE);
        VertexLabel label = new VertexLabel(null, IdGenerator.of(10), "person");
        label.idStrategy(IdStrategy.PRIMARY_KEY);
        label.primaryKeys(key.id());
        BaseVertex vertex = new BaseVertex(null, label);
        vertex.addProperty(key, "");
        Assert.assertThrows(IllegalArgumentException.class, vertex::name);
        for (char prefix = 0; prefix <= 3; prefix++) {
            vertex.addProperty(key, prefix + "value");
            Assert.assertThrows(IllegalArgumentException.class, vertex::name);
        }
        vertex.addProperty(key, "a!b");
        Assert.assertEquals("a`!b", vertex.name());
    }

    @Test
    public void testSharedEdgeNamePreservesSortValueNormalization() {
        PropertyKey key = key(Cardinality.SINGLE);
        EdgeLabel label = new EdgeLabel(null, IdGenerator.of(3), "link");
        BaseEdge edge = new BaseEdge(null, label);
        Assert.assertEquals("", edge.name());
        label.sortKeys(key.id());
        edge.name(null);
        edge.addProperty(key, "");
        Assert.assertEquals("", edge.name());
        edge.name(null);
        edge.addProperty(key, "a!b");
        Assert.assertEquals("a`!b", edge.name());
        edge.name(null);
        edge.addProperty(key, "\u0000value");
        Assert.assertThrows(IllegalArgumentException.class, edge::name);
    }

    @Test
    public void testStandaloneUnknownSchemaFactoryRequiresNoSupplier() {
        BaseVertex owner = vertex(1);
        EdgeLabel label = new EdgeLabel(null, IdGenerator.of(3), "link");
        label.sourceLabel(IdGenerator.of(10));
        label.targetLabel(IdGenerator.of(11));
        BaseEdge edge = BaseEdge.createEdge(null, owner, true, label, "sort", IdGenerator.of(2));
        Assert.assertSame(owner, edge.ownerVertex());
        Assert.assertTrue(owner.schemaLabel().undefined());
        Assert.assertTrue(edge.otherVertex().schemaLabel().undefined());
        Assert.assertEquals(IdGenerator.of(10), owner.schemaLabel().id());
        Assert.assertEquals(IdGenerator.of(11), edge.otherVertex().schemaLabel().id());
        Assert.assertEquals("sort", edge.name());
        Assert.assertEquals(IdGenerator.of(2), edge.otherVertexId());
    }

    @Test
    public void testUniqueAdjacencyUsesSharedEdgeIdEquality() {
        BaseVertex owner = vertex(1);
        BaseVertex other = vertex(2);
        EdgeLabel label = new EdgeLabel(null, IdGenerator.of(3), "link");
        owner.resetEdges(true);
        BaseEdge first = new BaseEdge(label, true);
        first.vertices(true, owner, other);
        first.name("");
        first.assignId();
        BaseEdge duplicate = first.clone();
        Assert.assertTrue(owner.addEdge(first));
        Assert.assertFalse(owner.addEdge(duplicate));
        Assert.assertEquals(1, owner.edges().size());
        Assert.assertSame(first, owner.removeEdge(duplicate));
        Assert.assertTrue(owner.edges().isEmpty());
    }

    @Test
    public void testClassificationPreservesStorageAndEngineContracts() {
        VertexLabel variables = new VertexLabel(null, IdGenerator.of(10), "~variables");
        BaseVertex stored = new BaseVertex(IdGenerator.of(1), variables);
        BaseVertex engine = new BaseVertex(IdGenerator.of(1), variables,
                                           BaseVertex.TypeContext.ENGINE);
        Assert.assertEquals(HugeType.TASK, stored.type());
        Assert.assertEquals(HugeType.VERTEX, engine.type());

        VertexLabel server = new VertexLabel(null, IdGenerator.of(11), "~server");
        stored.schemaLabel(server);
        engine.schemaLabel(server);
        Assert.assertEquals(HugeType.VERTEX, stored.type());
        Assert.assertEquals(HugeType.SERVER, engine.type());
    }

    private static BaseVertex vertex(long value) {
        Id id = IdGenerator.of(value);
        return new BaseVertex(id, new VertexLabel(null, IdGenerator.of(10), "person"));
    }

    private static PropertyKey key(Cardinality cardinality) {
        PropertyKey key = new PropertyKey(null, IdGenerator.of(1), "name");
        key.dataType(DataType.TEXT);
        key.cardinality(cardinality);
        return key;
    }
}
