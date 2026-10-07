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

import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.id.IdGenerator;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.util.collection.ObjectIntMapping;
import org.apache.hugegraph.util.collection.ObjectIntMappingFactory;
import org.junit.After;
import org.junit.Test;

public class ObjectIntMappingTest {

    private static final int OBJECT_NUMBER = 1000000;
    private static ObjectIntMapping<Id> mapping =
            ObjectIntMappingFactory.newObjectIntMapping();

    @After
    public void clear() {
        mapping.clear();
    }

    @Test
    public void testNumberIdMapping() {
        Set<Integer> codes = new LinkedHashSet<>();
        for (int i = 0; i < OBJECT_NUMBER; i++) {
            codes.add(mapping.object2Code(IdGenerator.of(i)));
        }
        Assert.assertEquals(OBJECT_NUMBER, codes.size());

        int j = 0;
        for (Integer code : codes) {
            Assert.assertEquals(IdGenerator.of(j++), mapping.code2Object(code));
        }
    }

    @Test
    public void testStringIdMapping() {
        Set<String> strings = new LinkedHashSet<>();
        Set<Integer> codes = new LinkedHashSet<>();
        for (int i = 0; i < OBJECT_NUMBER; i++) {
            String string = RandomStringUtils.randomAlphanumeric(10);
            strings.add(string);
            codes.add(mapping.object2Code(IdGenerator.of(string)));
        }
        Assert.assertEquals(strings.size(), codes.size());

        Iterator<String> strIter = strings.iterator();
        Iterator<Integer> codeIter = codes.iterator();
        while (strIter.hasNext() && codeIter.hasNext()) {
            Assert.assertEquals(IdGenerator.of(strIter.next()),
                                mapping.code2Object(codeIter.next()));
        }

        Assert.assertFalse(strIter.hasNext());
        Assert.assertFalse(codeIter.hasNext());
    }

    @Test
    public void testUUIDIdMapping() {
        Set<UUID> uuids = new LinkedHashSet<>();
        Set<Integer> codes = new LinkedHashSet<>();
        for (int i = 0; i < OBJECT_NUMBER; i++) {
            UUID uuid = UUID.randomUUID();
            uuids.add(uuid);
            codes.add(mapping.object2Code(IdGenerator.of(uuid)));
        }
        Assert.assertEquals(uuids.size(), codes.size());

        Iterator<UUID> uuidIter = uuids.iterator();
        Iterator<Integer> codeIter = codes.iterator();
        while (uuidIter.hasNext() && codeIter.hasNext()) {
            Assert.assertEquals(IdGenerator.of(uuidIter.next()),
                                mapping.code2Object(codeIter.next()));
        }

        Assert.assertFalse(uuidIter.hasNext());
        Assert.assertFalse(codeIter.hasNext());
    }

    @Test
    public void testMixedIdMapping() {
        Set<Integer> codes = new LinkedHashSet<>();
        Set<Object> objects = new LinkedHashSet<>();
        Object object;

        for (int i = 0; i < OBJECT_NUMBER; i++) {
            object = IdGenerator.of(i);
            objects.add(object);
            codes.add(mapping.object2Code(object));
        }

        for (int i = 0; i < OBJECT_NUMBER; i++) {
            String string = RandomStringUtils.randomAlphanumeric(10);
            object = IdGenerator.of(string);
            objects.add(object);
            codes.add(mapping.object2Code(object));
        }

        for (int i = 0; i < OBJECT_NUMBER; i++) {
            UUID uuid = UUID.randomUUID();
            object = IdGenerator.of(uuid);
            objects.add(object);
            codes.add(mapping.object2Code(object));
        }
        Assert.assertEquals(objects.size(), codes.size());

        Iterator<Object> objectIter = objects.iterator();
        Iterator<Integer> codeIter = codes.iterator();
        while (objectIter.hasNext() && codeIter.hasNext()) {
            Assert.assertEquals(objectIter.next(),
                                mapping.code2Object(codeIter.next()));
        }

        Assert.assertFalse(objectIter.hasNext());
        Assert.assertFalse(codeIter.hasNext());
    }
}
