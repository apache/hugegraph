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

package org.apache.hugegraph.unit.util.collection;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.testutil.Whitebox;
import org.apache.hugegraph.type.define.CollectionType;
import org.apache.hugegraph.util.collection.CollectionFactory;
import org.apache.hugegraph.util.collection.IdSet;
import org.eclipse.collections.api.map.primitive.MutableIntObjectMap;
import org.eclipse.collections.impl.list.mutable.FastList;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;
import org.eclipse.collections.impl.set.mutable.UnifiedSet;
import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;

public class CollectionFactoryTest {

    private static CollectionFactory factory;

    @Test
    public void testCollectionFactoryConstructor() {
        factory = new CollectionFactory();
        CollectionType type = Whitebox.getInternalState(factory, "type");
        Assert.assertEquals(CollectionType.EC, type);

        factory = new CollectionFactory(CollectionType.EC);
        type = Whitebox.getInternalState(factory, "type");
        Assert.assertEquals(CollectionType.EC, type);

        factory = new CollectionFactory(CollectionType.FU);
        type = Whitebox.getInternalState(factory, "type");
        Assert.assertEquals(CollectionType.FU, type);

        factory = new CollectionFactory(CollectionType.JCF);
        type = Whitebox.getInternalState(factory, "type");
        Assert.assertEquals(CollectionType.JCF, type);
    }

    @Test
    public void testNewList() {
        // With type
        factory = new CollectionFactory();
        List<?> list = factory.newList();
        Assert.assertInstanceOf(FastList.class, list);

        factory = new CollectionFactory(CollectionType.EC);
        list = factory.newList();
        Assert.assertInstanceOf(FastList.class, list);

        factory = new CollectionFactory(CollectionType.FU);
        list = factory.newList();
        Assert.assertInstanceOf(ObjectArrayList.class, list);

        factory = new CollectionFactory(CollectionType.JCF);
        list = factory.newList();
        Assert.assertInstanceOf(ArrayList.class, list);

        // With initial capacity
        int initialCapacity = 10;

        factory = new CollectionFactory();
        list = factory.newList(initialCapacity);
        Assert.assertInstanceOf(FastList.class, list);
        Object[] items = Whitebox.getInternalState(list, "items");
        Assert.assertEquals(initialCapacity, items.length);

        factory = new CollectionFactory(CollectionType.EC);
        list = factory.newList(initialCapacity);
        Assert.assertInstanceOf(FastList.class, list);
        items = Whitebox.getInternalState(list, "items");
        Assert.assertEquals(initialCapacity, items.length);

        factory = new CollectionFactory(CollectionType.FU);
        list = factory.newList(initialCapacity);
        Assert.assertInstanceOf(ObjectArrayList.class, list);
        items = Whitebox.getInternalState(list, "a");
        Assert.assertEquals(initialCapacity, items.length);

        factory = new CollectionFactory(CollectionType.JCF);
        list = factory.newList(initialCapacity);
        Assert.assertInstanceOf(ArrayList.class, list);
        items = Whitebox.getInternalState(list, "elementData");
        Assert.assertEquals(initialCapacity, items.length);

        // With collection
        Collection<Integer> integers = ImmutableSet.of(1, 2, 3, 4);

        factory = new CollectionFactory();
        list = factory.newList(integers);
        Assert.assertInstanceOf(FastList.class, list);
        Assert.assertEquals(integers.size(), list.size());
        for (int integer : integers) {
            Assert.assertTrue(list.contains(integer));
        }

        factory = new CollectionFactory(CollectionType.EC);
        list = factory.newList(integers);
        Assert.assertInstanceOf(FastList.class, list);
        Assert.assertEquals(integers.size(), list.size());
        for (int integer : integers) {
            Assert.assertTrue(list.contains(integer));
        }

        factory = new CollectionFactory(CollectionType.FU);
        list = factory.newList(integers);
        Assert.assertInstanceOf(ObjectArrayList.class, list);
        Assert.assertEquals(integers.size(), list.size());
        for (int integer : integers) {
            Assert.assertTrue(list.contains(integer));
        }

        factory = new CollectionFactory(CollectionType.JCF);
        list = factory.newList(integers);
        Assert.assertInstanceOf(ArrayList.class, list);
        Assert.assertEquals(integers.size(), list.size());
        for (int integer : integers) {
            Assert.assertTrue(list.contains(integer));
        }
    }

    @Test
    public void testNewSet() {
        // With type
        factory = new CollectionFactory();
        Set<?> set = factory.newSet();
        Assert.assertInstanceOf(UnifiedSet.class, set);

        factory = new CollectionFactory(CollectionType.EC);
        set = factory.newSet();
        Assert.assertInstanceOf(UnifiedSet.class, set);

        factory = new CollectionFactory(CollectionType.FU);
        set = factory.newSet();
        Assert.assertInstanceOf(ObjectOpenHashSet.class, set);

        factory = new CollectionFactory(CollectionType.JCF);
        set = factory.newSet();
        Assert.assertInstanceOf(HashSet.class, set);

        // With initial capacity
        int initialCapacity = 10;

        factory = new CollectionFactory();
        set = factory.newSet(initialCapacity);
        Assert.assertInstanceOf(UnifiedSet.class, set);
        Object[] items = Whitebox.getInternalState(set, "table");
        // Initial size of UnifiedSet is (initialCapacity / 0.75)
        Assert.assertEquals(16, items.length);

        factory = new CollectionFactory(CollectionType.EC);
        set = factory.newSet(initialCapacity);
        Assert.assertInstanceOf(UnifiedSet.class, set);
        items = Whitebox.getInternalState(set, "table");
        // Initial size of UnifiedSet is (initialCapacity / 0.75)
        Assert.assertEquals(16, items.length);

        factory = new CollectionFactory(CollectionType.FU);
        set = factory.newSet(initialCapacity);
        Assert.assertInstanceOf(ObjectOpenHashSet.class, set);
        items = Whitebox.getInternalState(set, "key");
        Assert.assertEquals(17, items.length);

        factory = new CollectionFactory(CollectionType.JCF);
        set = factory.newSet(initialCapacity);
        Assert.assertInstanceOf(HashSet.class, set);
        Map<?, ?> map = Whitebox.getInternalState(set, "map");
        Assert.assertInstanceOf(HashMap.class, map);
        Assert.assertEquals(0, map.size());

        // With collection
        Collection<Integer> integers = ImmutableSet.of(1, 2, 3, 4);

        factory = new CollectionFactory();
        set = factory.newSet(integers);
        Assert.assertInstanceOf(UnifiedSet.class, set);
        Assert.assertEquals(integers.size(), set.size());
        for (int integer : integers) {
            Assert.assertTrue(set.contains(integer));
        }

        factory = new CollectionFactory(CollectionType.EC);
        set = factory.newSet(integers);
        Assert.assertInstanceOf(UnifiedSet.class, set);
        Assert.assertEquals(integers.size(), set.size());
        for (int integer : integers) {
            Assert.assertTrue(set.contains(integer));
        }

        factory = new CollectionFactory(CollectionType.FU);
        set = factory.newSet(integers);
        Assert.assertInstanceOf(ObjectOpenHashSet.class, set);
        Assert.assertEquals(integers.size(), set.size());
        for (int integer : integers) {
            Assert.assertTrue(set.contains(integer));
        }

        factory = new CollectionFactory(CollectionType.JCF);
        set = factory.newSet(integers);
        Assert.assertInstanceOf(HashSet.class, set);
        Assert.assertEquals(integers.size(), set.size());
        for (int integer : integers) {
            Assert.assertTrue(set.contains(integer));
        }
    }

    @Test
    public void testNewMap() {
        // With type
        factory = new CollectionFactory();
        Map<?, ?> map = factory.newMap();
        Assert.assertInstanceOf(UnifiedMap.class, map);

        factory = new CollectionFactory(CollectionType.EC);
        map = factory.newMap();
        Assert.assertInstanceOf(UnifiedMap.class, map);

        factory = new CollectionFactory(CollectionType.FU);
        map = factory.newMap();
        Assert.assertInstanceOf(Object2ObjectOpenHashMap.class, map);

        factory = new CollectionFactory(CollectionType.JCF);
        map = factory.newMap();
        Assert.assertInstanceOf(HashMap.class, map);

        // With initial capacity
        int initialCapacity = 10;

        factory = new CollectionFactory();
        map = factory.newMap(initialCapacity);
        Assert.assertInstanceOf(UnifiedMap.class, map);
        Object[] items = Whitebox.getInternalState(map, "table");
        // Initial size of UnifiedSet is (initialCapacity / 0.75) * 2
        Assert.assertEquals(32, items.length);

        factory = new CollectionFactory(CollectionType.EC);
        map = factory.newMap(initialCapacity);
        Assert.assertInstanceOf(UnifiedMap.class, map);
        items = Whitebox.getInternalState(map, "table");
        // Initial size of UnifiedSet is (initialCapacity / 0.75) * 2
        Assert.assertEquals(32, items.length);

        factory = new CollectionFactory(CollectionType.FU);
        map = factory.newMap(initialCapacity);
        Assert.assertInstanceOf(Object2ObjectOpenHashMap.class, map);
        items = Whitebox.getInternalState(map, "key");
        Assert.assertEquals(17, items.length);
        items = Whitebox.getInternalState(map, "value");
        Assert.assertEquals(17, items.length);

        factory = new CollectionFactory(CollectionType.JCF);
        map = factory.newMap(initialCapacity);
        Assert.assertInstanceOf(HashMap.class, map);
        items = Whitebox.getInternalState(map, "table");
        Assert.assertNull(items);

        // With collection
        Map<Integer, String> keyValues = ImmutableMap.of(1, "A", 2, "B",
                                                         3, "C", 4, "D");

        factory = new CollectionFactory();
        map = factory.newMap(keyValues);
        Assert.assertInstanceOf(UnifiedMap.class, map);
        Assert.assertEquals(keyValues.size(), map.size());
        for (Map.Entry<Integer, String> entry : keyValues.entrySet()) {
            Assert.assertEquals(entry.getValue(), map.get(entry.getKey()));
        }

        factory = new CollectionFactory(CollectionType.EC);
        map = factory.newMap(keyValues);
        Assert.assertInstanceOf(UnifiedMap.class, map);
        Assert.assertEquals(keyValues.size(), map.size());
        for (Map.Entry<Integer, String> entry : keyValues.entrySet()) {
            Assert.assertEquals(entry.getValue(), map.get(entry.getKey()));
        }

        factory = new CollectionFactory(CollectionType.FU);
        map = factory.newMap(keyValues);
        Assert.assertInstanceOf(Object2ObjectOpenHashMap.class, map);
        Assert.assertEquals(keyValues.size(), map.size());
        for (Map.Entry<Integer, String> entry : keyValues.entrySet()) {
            Assert.assertEquals(entry.getValue(), map.get(entry.getKey()));
        }

        factory = new CollectionFactory(CollectionType.JCF);
        map = factory.newMap(keyValues);
        Assert.assertInstanceOf(HashMap.class, map);
        Assert.assertEquals(keyValues.size(), map.size());
        for (Map.Entry<Integer, String> entry : keyValues.entrySet()) {
            Assert.assertEquals(entry.getValue(), map.get(entry.getKey()));
        }
    }

    @Test
    public void testIntObjectMap() {
        MutableIntObjectMap<?> map = CollectionFactory.newIntObjectMap();
        Assert.assertInstanceOf(IntObjectHashMap.class, map);

        map = CollectionFactory.newIntObjectMap(10);
        Assert.assertInstanceOf(IntObjectHashMap.class, map);
        int[] keys = Whitebox.getInternalState(map, "keys");
        Assert.assertEquals(32, keys.length);
        Object[] values = Whitebox.getInternalState(map, "values");
        Assert.assertEquals(32, values.length);

        map = CollectionFactory.newIntObjectMap(1, "A", 2, "B");
        map = CollectionFactory.newIntObjectMap(map);
        Assert.assertInstanceOf(IntObjectHashMap.class, map);
        Assert.assertEquals(2, map.size());
        Assert.assertEquals("A", map.get(1));
        Assert.assertEquals("B", map.get(2));
    }

    @Test
    public void testIdSet() {
        factory = new CollectionFactory(CollectionType.EC);
        IdSet idSet = factory.newIdSet();
        Set<Id> ids = Whitebox.getInternalState(idSet, "nonNumberIds");
        Assert.assertInstanceOf(UnifiedSet.class, ids);
        Assert.assertInstanceOf(LongHashSet.class,
                                Whitebox.getInternalState(idSet, "numberIds"));

        factory = new CollectionFactory(CollectionType.FU);
        idSet = factory.newIdSet();
        ids = Whitebox.getInternalState(idSet, "nonNumberIds");
        Assert.assertInstanceOf(ObjectOpenHashSet.class, ids);
        Assert.assertInstanceOf(LongHashSet.class,
                                Whitebox.getInternalState(idSet, "numberIds"));

        factory = new CollectionFactory(CollectionType.JCF);
        idSet = factory.newIdSet();
        ids = Whitebox.getInternalState(idSet, "nonNumberIds");
        Assert.assertInstanceOf(HashSet.class, ids);
        Assert.assertInstanceOf(LongHashSet.class,
                                Whitebox.getInternalState(idSet, "numberIds"));

        idSet = CollectionFactory.newIdSet(CollectionType.EC);
        ids = Whitebox.getInternalState(idSet, "nonNumberIds");
        Assert.assertInstanceOf(UnifiedSet.class, ids);
        Assert.assertInstanceOf(LongHashSet.class,
                                Whitebox.getInternalState(idSet, "numberIds"));

        idSet = CollectionFactory.newIdSet(CollectionType.FU);
        ids = Whitebox.getInternalState(idSet, "nonNumberIds");
        Assert.assertInstanceOf(ObjectOpenHashSet.class, ids);
        Assert.assertInstanceOf(LongHashSet.class,
                                Whitebox.getInternalState(idSet, "numberIds"));

        idSet = CollectionFactory.newIdSet(CollectionType.JCF);
        ids = Whitebox.getInternalState(idSet, "nonNumberIds");
        Assert.assertInstanceOf(HashSet.class, ids);
        Assert.assertInstanceOf(LongHashSet.class,
                                Whitebox.getInternalState(idSet, "numberIds"));
    }
}
