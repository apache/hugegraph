/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hugegraph.backend.store.hstore;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.hugegraph.backend.BackendColumn;
import org.apache.hugegraph.backend.store.BackendEntry.BackendColumnIterator;
import org.apache.hugegraph.store.HgKvEntry;
import org.apache.hugegraph.store.HgStoreSession;
import org.apache.hugegraph.store.HgOwnerKey;
import org.apache.hugegraph.id.IdGenerator;
import org.apache.hugegraph.serializer.OlapKey;
import org.apache.hugegraph.store.HgKvIterator;
import org.junit.Assert;
import org.junit.Test;

public class HstoreSessionsImplTest {

    @Test
    public void testProductionClassDoesNotReferenceTestAssert() throws IOException {
        String className = HstoreSessionsImpl.class.getSimpleName() + ".class";
        try (InputStream stream = HstoreSessionsImpl.class.getResourceAsStream(className)) {
            Assert.assertNotNull(stream);
            String classFile = new String(stream.readAllBytes(), StandardCharsets.ISO_8859_1);
            Assert.assertFalse(classFile.contains(
                             "org/apache/hugegraph/testutil/Assert"));
        }
    }

    @Test
    public void testColumnIteratorPositionTracksNextUnreadKey()
            throws Exception {
        BackendColumnIterator iterator = newColumnIterator(
                new TestIterator(1, 2));

        Assert.assertTrue(iterator.hasNext());
        BackendColumn first = iterator.next();
        Assert.assertArrayEquals(keyBytes(1), first.name);

        Assert.assertTrue(iterator.hasNext());
        Assert.assertArrayEquals(keyBytes(2), iterator.position());
    }

    @Test
    public void testColumnIteratorClearsPositionWhenFullyExhausted()
            throws Exception {
        BackendColumnIterator iterator = newColumnIterator(
                new TestIterator(1));

        Assert.assertTrue(iterator.hasNext());
        iterator.next();
        Assert.assertFalse(iterator.hasNext());
        Assert.assertNull(iterator.position());
    }

    @Test
    public void testExactOlapBatchUsesNativePointKeysAndOwnerRouting() {
        byte[] vertexOwner = IdGenerator.of(9).asBytes();
        byte[] namespaced = OlapKey.format(IdGenerator.of(9), IdGenerator.of(9));
        List<HgOwnerKey> keys = List.of(HgOwnerKey.of(vertexOwner, namespaced));
        int[] calls = {0};
        HgStoreSession graph = (HgStoreSession) Proxy.newProxyInstance(HgStoreSession.class.getClassLoader(),
                new Class<?>[]{HgStoreSession.class}, (proxy, method, args) -> {
                    Assert.assertEquals("batchGetOwner", method.getName());
                    Assert.assertEquals("g+olap", args[0]);
                    Assert.assertSame(keys, args[1]);
                    Assert.assertArrayEquals(vertexOwner, keys.get(0).getOwner());
                    Assert.assertArrayEquals(namespaced, keys.get(0).getKey());
                    calls[0]++;
                    return List.of(new TestEntry(namespaced));
                });
        try (BackendColumnIterator result = HstoreSessionsImpl.getWithBatchExact(graph, "g+olap", keys)) {
            Assert.assertTrue(result.hasNext());
            Assert.assertArrayEquals(namespaced, result.next().name);
            Assert.assertFalse(result.hasNext());
        }
        Assert.assertEquals(1, calls[0]);
    }

    private static BackendColumnIterator newColumnIterator(
            HgKvIterator<HgKvEntry> iterator) throws Exception {
        Class<?> clazz = Class.forName(HstoreSessionsImpl.class.getName() +
                                       "$ColumnIterator");
        Constructor<?> constructor = clazz.getDeclaredConstructor(
                String.class, HgKvIterator.class, byte[].class, byte[].class,
                int.class);
        constructor.setAccessible(true);
        return (BackendColumnIterator) constructor.newInstance(
                "test", iterator, null, null, 0);
    }

    private static byte[] keyBytes(int key) {
        return new byte[]{(byte) key};
    }

    private static final class TestIterator implements HgKvIterator<HgKvEntry> {

        private final List<Integer> keys;
        private int offset;
        private HgKvEntry current;

        private TestIterator(Integer... keys) {
            this.keys = Arrays.asList(keys);
            this.offset = 0;
            this.current = null;
        }

        @Override
        public boolean hasNext() {
            return this.offset < this.keys.size();
        }

        @Override
        public HgKvEntry next() {
            if (!this.hasNext()) {
                throw new NoSuchElementException();
            }
            this.current = new TestEntry(keyBytes(this.keys.get(this.offset++)));
            return this.current;
        }

        @Override
        public byte[] key() {
            return this.current == null ? null : this.current.key();
        }

        @Override
        public byte[] value() {
            return this.current == null ? null : this.current.value();
        }

        @Override
        public byte[] position() {
            return this.key();
        }
    }

    private static final class TestEntry implements HgKvEntry {

        private final byte[] key;

        private TestEntry(byte[] key) {
            this.key = key;
        }

        @Override
        public byte[] key() {
            return this.key;
        }

        @Override
        public byte[] value() {
            return this.key;
        }
    }
}
