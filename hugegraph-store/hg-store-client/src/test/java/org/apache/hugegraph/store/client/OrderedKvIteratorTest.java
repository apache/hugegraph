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

package org.apache.hugegraph.store.client;

import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.hugegraph.store.HgKvEntry;
import org.apache.hugegraph.store.HgKvIterator;
import org.junit.Assert;
import org.junit.Test;

public class OrderedKvIteratorTest {

    @Test
    public void testMergeByKeyWithLimit() {
        OrderedKvIterator iterator = new OrderedKvIterator(Arrays.asList(
                new TestIterator(1, 4),
                new TestIterator(2, 3)
        ), 3L);

        Assert.assertTrue(iterator.hasNext());
        Assert.assertEquals(1, key(iterator.next()));
        Assert.assertArrayEquals(keyBytes(1), iterator.position());

        Assert.assertTrue(iterator.hasNext());
        Assert.assertEquals(2, key(iterator.next()));
        Assert.assertArrayEquals(keyBytes(2), iterator.position());

        Assert.assertTrue(iterator.hasNext());
        Assert.assertEquals(3, key(iterator.next()));
        Assert.assertArrayEquals(keyBytes(3), iterator.position());

        Assert.assertFalse(iterator.hasNext());
    }

    @Test
    public void testNoLimitReturnsAllKeysInOrder() {
        OrderedKvIterator iterator = new OrderedKvIterator(Arrays.asList(
                new TestIterator(1, 5),
                new TestIterator(2, 4)
        ), 0L);

        Assert.assertEquals(1, key(iterator.next()));
        Assert.assertEquals(2, key(iterator.next()));
        Assert.assertEquals(4, key(iterator.next()));
        Assert.assertEquals(5, key(iterator.next()));
        Assert.assertFalse(iterator.hasNext());
    }

    @Test
    public void testSeekBeforeReadSkipsBoundaryKey() {
        OrderedKvIterator iterator = new OrderedKvIterator(Arrays.asList(
                new TestIterator(1, 4),
                new TestIterator(2, 3)
        ), 0L);

        iterator.seek(keyBytes(3));

        Assert.assertEquals(4, key(iterator.next()));
        Assert.assertFalse(iterator.hasNext());
    }

    @Test
    public void testSeekDoesNotSkipLowerSourceWithLargerKeys() {
        OrderedKvIterator iterator = new OrderedKvIterator(Arrays.asList(
                new TestIterator(1, 100),
                new TestIterator(2, 3)
        ), 0L);

        iterator.seek(keyBytes(2));

        Assert.assertEquals(3, key(iterator.next()));
        Assert.assertEquals(100, key(iterator.next()));
        Assert.assertFalse(iterator.hasNext());
    }

    @Test
    public void testCloseUnderlyingIteratorsWhenLimitReached() {
        TestIterator first = new TestIterator(1, 4);
        TestIterator second = new TestIterator(2, 3);
        OrderedKvIterator iterator = new OrderedKvIterator(Arrays.asList(first, second), 1L);

        Assert.assertEquals(1, key(iterator.next()));
        Assert.assertFalse(iterator.hasNext());
        Assert.assertTrue(first.closed());
        Assert.assertTrue(second.closed());
    }

    @Test
    public void testCloseUnderlyingIteratorsWhenExhausted() {
        TestIterator first = new TestIterator(1);
        TestIterator second = new TestIterator();
        OrderedKvIterator iterator = new OrderedKvIterator(Arrays.asList(first, second), 0L);

        Assert.assertEquals(1, key(iterator.next()));
        Assert.assertFalse(iterator.hasNext());
        Assert.assertTrue(first.closed());
        Assert.assertTrue(second.closed());
    }

    private static int key(HgKvEntry entry) {
        return entry.key()[0] & 0xFF;
    }

    private static byte[] keyBytes(int key) {
        return new byte[]{(byte) key};
    }

    private static final class TestIterator implements HgKvIterator<HgKvEntry> {

        private final List<Integer> keys;
        private int offset;
        private HgKvEntry current;
        private boolean closed;

        private TestIterator(Integer... keys) {
            this.keys = Arrays.asList(keys);
            this.offset = 0;
            this.current = null;
            this.closed = false;
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
            int key = this.keys.get(this.offset++);
            this.current = new TestEntry(keyBytes(key));
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
            byte[] key = this.key();
            return key == null ? null : new byte[]{(byte) 0xFF, key[0]};
        }

        @Override
        public void seek(byte[] position) {
            int target = position[0] & 0xFF;
            while (this.offset < this.keys.size() &&
                   this.keys.get(this.offset) < target) {
                this.offset++;
            }
        }

        @Override
        public void close() {
            this.closed = true;
        }

        private boolean closed() {
            return this.closed;
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
