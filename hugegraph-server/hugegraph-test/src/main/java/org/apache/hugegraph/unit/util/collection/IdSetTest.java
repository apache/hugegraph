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

import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.id.IdGenerator;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.testutil.Whitebox;
import org.apache.hugegraph.type.define.CollectionType;
import org.apache.hugegraph.util.collection.IdSet;
import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class IdSetTest {

    private static final int SIZE = 10000;

    private static IdSet idSet;

    @Before
    public void setup() {
        // pass
    }

    @After
    public void teardown() {
        // pass
    }

    @Test
    public void testIdSetWithOnlyNumberId() {
        Random random = new Random();
        Set<Long> numbers = new HashSet<>();
        for (CollectionType type : CollectionType.values()) {
            idSet = new IdSet(type);
            for (int i = 1; i < SIZE; i++) {
                long number = random.nextLong();
                numbers.add(number);
                idSet.add(IdGenerator.of(number));
            }
            Assert.assertEquals(numbers.size(), idSet.size());

            LongHashSet numberIds = Whitebox.getInternalState(idSet,
                                                              "numberIds");
            Assert.assertEquals(numbers.size(), numberIds.size());
            Set<Id> nonNumberIds = Whitebox.getInternalState(idSet,
                                                             "nonNumberIds");
            Assert.assertTrue(nonNumberIds.isEmpty());

            numbers.clear();
            idSet.clear();
        }
    }

    @Test
    public void testIdSetWithOnlyUUIDId() {
        Set<UUID> uuids = new HashSet<>();
        for (CollectionType type : CollectionType.values()) {
            idSet = new IdSet(type);
            for (int i = 0; i < SIZE; i++) {
                UUID uuid = UUID.randomUUID();
                uuids.add(uuid);
                idSet.add(IdGenerator.of(uuid));
            }
            Assert.assertEquals(uuids.size(), idSet.size());

            LongHashSet numberIds = Whitebox.getInternalState(idSet,
                                                              "numberIds");
            Assert.assertTrue(numberIds.isEmpty());
            Set<Id> nonNumberIds = Whitebox.getInternalState(idSet,
                                                             "nonNumberIds");
            Assert.assertEquals(uuids.size(), nonNumberIds.size());

            uuids.clear();
            idSet.clear();
        }
    }

    @Test
    public void testIdSetWithOnlyStringId() {
        Set<String> strings = new HashSet<>();
        for (CollectionType type : CollectionType.values()) {
            idSet = new IdSet(type);
            for (int i = 0; i < SIZE; i++) {
                String string = RandomStringUtils.randomAlphanumeric(10);
                strings.add(string);
                idSet.add(IdGenerator.of(string));
            }
            Assert.assertEquals(strings.size(), idSet.size());

            LongHashSet numberIds = Whitebox.getInternalState(idSet,
                                                              "numberIds");
            Assert.assertTrue(numberIds.isEmpty());
            Set<Id> nonNumberIds = Whitebox.getInternalState(idSet,
                                                             "nonNumberIds");
            Assert.assertEquals(strings.size(), nonNumberIds.size());

            strings.clear();
            idSet.clear();
        }
    }

    @Test
    public void testIdSetWithMixedId() {
        Random random = new Random();
        Set<Long> numbers = new HashSet<>();
        Set<UUID> uuids = new HashSet<>();
        Set<String> strings = new HashSet<>();
        for (CollectionType type : CollectionType.values()) {
            idSet = new IdSet(type);
            for (int i = 1; i < SIZE; i++) {
                long number = random.nextLong();
                numbers.add(number);
                idSet.add(IdGenerator.of(number));
            }
            for (int i = 0; i < SIZE; i++) {
                UUID uuid = UUID.randomUUID();
                uuids.add(uuid);
                idSet.add(IdGenerator.of(uuid));
            }
            for (int i = 0; i < SIZE; i++) {
                String string = RandomStringUtils.randomAlphanumeric(10);
                strings.add(string);
                idSet.add(IdGenerator.of(string));
            }
            Assert.assertEquals(numbers.size() + uuids.size() + strings.size(),
                                idSet.size());

            LongHashSet numberIds = Whitebox.getInternalState(idSet,
                                                              "numberIds");
            Assert.assertEquals(numbers.size(), numberIds.size());
            Set<Id> nonNumberIds = Whitebox.getInternalState(idSet,
                                                             "nonNumberIds");
            Assert.assertEquals(uuids.size() + strings.size(),
                                nonNumberIds.size());

            numbers.clear();
            uuids.clear();
            strings.clear();
            idSet.clear();
        }
    }

    @Test
    public void testIdSetContains() {
        Random random = new Random();
        Set<Long> numbers = new HashSet<>();
        Set<UUID> uuids = new HashSet<>();
        Set<String> strings = new HashSet<>();
        long number = 0L;
        UUID uuid = null;
        String string = null;
        for (CollectionType type : CollectionType.values()) {
            idSet = new IdSet(type);
            for (int i = 1; i < SIZE; i++) {
                number = random.nextLong();
                numbers.add(number);
                idSet.add(IdGenerator.of(number));
            }
            for (int i = 0; i < SIZE; i++) {
                uuid = UUID.randomUUID();
                uuids.add(uuid);
                idSet.add(IdGenerator.of(uuid));
            }
            for (int i = 0; i < SIZE; i++) {
                string = RandomStringUtils.randomAlphanumeric(10);
                strings.add(string);
                idSet.add(IdGenerator.of(string));
            }
            Assert.assertEquals(numbers.size() + uuids.size() + strings.size(),
                                idSet.size());

            LongHashSet numberIds = Whitebox.getInternalState(idSet,
                                                              "numberIds");
            Assert.assertEquals(numbers.size(), numberIds.size());
            Set<Id> nonNumberIds = Whitebox.getInternalState(idSet,
                                                             "nonNumberIds");
            Assert.assertEquals(uuids.size() + strings.size(),
                                nonNumberIds.size());

            Assert.assertTrue(idSet.contains(IdGenerator.of(number)));
            Assert.assertTrue(idSet.contains(IdGenerator.of(uuid)));
            Assert.assertTrue(idSet.contains(IdGenerator.of(string)));

            numbers.clear();
            uuids.clear();
            strings.clear();
            idSet.clear();
        }
    }

    @Test
    public void testIdSetIterator() {
        Random random = new Random();
        Set<Long> numbers = new HashSet<>();
        Set<UUID> uuids = new HashSet<>();
        Set<String> strings = new HashSet<>();
        for (CollectionType type : CollectionType.values()) {
            idSet = new IdSet(type);
            for (int i = 1; i < SIZE; i++) {
                long number = random.nextLong();
                numbers.add(number);
                idSet.add(IdGenerator.of(number));
            }
            for (int i = 0; i < SIZE; i++) {
                UUID uuid = UUID.randomUUID();
                uuids.add(uuid);
                idSet.add(IdGenerator.of(uuid));
            }
            for (int i = 0; i < SIZE; i++) {
                String string = RandomStringUtils.randomAlphanumeric(10);
                strings.add(string);
                idSet.add(IdGenerator.of(string));
            }
            Assert.assertEquals(numbers.size() + uuids.size() + strings.size(),
                                idSet.size());

            LongHashSet numberIds = Whitebox.getInternalState(idSet,
                                                              "numberIds");
            Assert.assertEquals(numbers.size(), numberIds.size());
            Set<Id> nonNumberIds = Whitebox.getInternalState(idSet,
                                                             "nonNumberIds");
            Assert.assertEquals(uuids.size() + strings.size(),
                                nonNumberIds.size());

            for (Id id : idSet) {
                if (id instanceof IdGenerator.LongId) {
                    Assert.assertTrue(numbers.contains(id.asLong()));
                } else if (id instanceof IdGenerator.UuidId) {
                    Assert.assertTrue(id.uuid() &&
                                      uuids.contains(id.asObject()));
                } else {
                    Assert.assertTrue(id instanceof IdGenerator.StringId);
                    Assert.assertTrue(strings.contains(id.asString()));
                }
            }

            numbers.clear();
            uuids.clear();
            strings.clear();
            idSet.clear();
        }
    }

    @Test
    public void testIdSetRemove() {
        Random random = new Random();
        Set<Long> numbers = new HashSet<>();
        Set<UUID> uuids = new HashSet<>();
        Set<String> strings = new HashSet<>();
        for (CollectionType type : CollectionType.values()) {
            idSet = new IdSet(type);
            for (int i = 1; i < SIZE; i++) {
                long number = random.nextLong();
                numbers.add(number);
                idSet.add(IdGenerator.of(number));
            }
            for (int i = 0; i < SIZE; i++) {
                UUID uuid = UUID.randomUUID();
                uuids.add(uuid);
                idSet.add(IdGenerator.of(uuid));
            }
            for (int i = 0; i < SIZE; i++) {
                String string = RandomStringUtils.randomAlphanumeric(10);
                strings.add(string);
                idSet.add(IdGenerator.of(string));
            }
            Assert.assertEquals(numbers.size() + uuids.size() + strings.size(),
                                idSet.size());

            LongHashSet numberIds = Whitebox.getInternalState(idSet,
                                                              "numberIds");
            Assert.assertEquals(numbers.size(), numberIds.size());
            Set<Id> nonNumberIds = Whitebox.getInternalState(idSet,
                                                             "nonNumberIds");
            Assert.assertEquals(uuids.size() + strings.size(),
                                nonNumberIds.size());

            for (long number : numbers) {
                idSet.remove(IdGenerator.of(number));
            }
            Assert.assertEquals(nonNumberIds.size(), idSet.size());

            for (UUID uuid : uuids) {
                idSet.remove(IdGenerator.of(uuid));
            }
            for (String string : strings) {
                idSet.remove(IdGenerator.of(string));
            }
            Assert.assertTrue(idSet.isEmpty());

            numbers.clear();
            uuids.clear();
            strings.clear();
            idSet.clear();
        }
    }
}
