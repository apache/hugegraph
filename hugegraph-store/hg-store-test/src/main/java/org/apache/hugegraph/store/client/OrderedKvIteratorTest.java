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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.hugegraph.store.HgKvEntry;
import org.apache.hugegraph.store.HgKvIterator;
import org.junit.Assert;
import org.junit.Test;

public class OrderedKvIteratorTest {

    @Test
    public void testMergeInterleavedSourcesByUnsignedKey() {
        TestIterator first = new TestIterator(1, 4);
        TestIterator second = new TestIterator(2, 3);
        OrderedKvIterator iterator = new OrderedKvIterator(
                Arrays.asList(first, second), 0L);

        Assert.assertEquals(Arrays.asList(1, 2, 3, 4), keys(iterator));
        Assert.assertArrayEquals(keyBytes(4), iterator.position());
        Assert.assertTrue(first.closed);
        Assert.assertTrue(second.closed);
    }

    @Test
    public void testMergeIsLazyAndStopsAtRawLimit() {
        TestIterator first = new TestIterator(1, 4, 5);
        TestIterator second = new TestIterator(2, 3, 6);
        OrderedKvIterator iterator = new OrderedKvIterator(
                Arrays.asList(first, second), 3L);

        Assert.assertEquals(0, first.nextCalls);
        Assert.assertEquals(0, second.nextCalls);

        Assert.assertEquals(Arrays.asList(1, 2, 3), keys(iterator));
        Assert.assertEquals(4, first.nextCalls + second.nextCalls);
        Assert.assertTrue(first.closed);
        Assert.assertTrue(second.closed);
    }

    @Test
    public void testMergeComparesKeysAsUnsignedBytes() {
        TestIterator first = new TestIterator(0x80);
        TestIterator second = new TestIterator(0x7f);
        OrderedKvIterator iterator = new OrderedKvIterator(
                Arrays.asList(first, second), 0L);

        Assert.assertEquals(Arrays.asList(0x7f, 0x80), keys(iterator));
    }

    @Test
    public void testMergeUsesStableSourceOrderForEqualKeys() {
        TestIterator first = new TestIterator(1);
        TestIterator second = new TestIterator(1);
        OrderedKvIterator iterator = new OrderedKvIterator(
                Arrays.asList(first, second), 0L);

        Assert.assertSame(first.entries.get(0), iterator.next());
        Assert.assertSame(second.entries.get(0), iterator.next());
        Assert.assertFalse(iterator.hasNext());
    }

    @Test
    public void testMergeClosesAllSourcesWhenAdvanceFails() {
        TestIterator first = new TestIterator(1, 3);
        TestIterator second = new TestIterator(2, 4);
        first.failOnHasNextAfter(1);
        OrderedKvIterator iterator = new OrderedKvIterator(
                Arrays.asList(first, second), 0L);

        Assert.assertThrows(IllegalStateException.class, iterator::next);
        Assert.assertTrue(first.closed);
        Assert.assertTrue(second.closed);
    }

    @Test
    public void testMergePrimesSourcesConcurrently() {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch bothStarted = new CountDownLatch(2);
        TestIterator first = new TestIterator(1);
        TestIterator second = new TestIterator(2);
        first.blockFirstHasNext(bothStarted);
        second.blockFirstHasNext(bothStarted);
        OrderedKvIterator iterator = new OrderedKvIterator(
                Arrays.asList(first, second), 0L, executor);
        try {
            Assert.assertTrue(iterator.hasNext());
            Assert.assertEquals(0L, bothStarted.getCount());
        } finally {
            iterator.close();
            executor.shutdownNow();
        }
    }

    @Test
    public void testMergeClosesAllSourcesWhenConcurrentInitializeFails() {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        TestIterator first = new TestIterator(1);
        TestIterator second = new TestIterator(2);
        first.failOnHasNextAfter(0);
        OrderedKvIterator iterator = new OrderedKvIterator(
                Arrays.asList(first, second), 0L, executor);
        try {
            Assert.assertThrows(IllegalStateException.class,
                                iterator::hasNext);
            Assert.assertTrue(first.closed);
            Assert.assertTrue(second.closed);
        } finally {
            executor.shutdownNow();
        }
    }

    private static List<Integer> keys(HgKvIterator<HgKvEntry> iterator) {
        List<Integer> keys = new ArrayList<>();
        while (iterator.hasNext()) {
            keys.add(iterator.next().key()[0] & 0xff);
        }
        return keys;
    }

    private static byte[] keyBytes(int key) {
        return new byte[]{(byte) key};
    }

    private static final class TestIterator implements HgKvIterator<HgKvEntry> {

        private final List<HgKvEntry> entries;
        private int offset;
        private int nextCalls;
        private HgKvEntry current;
        private boolean closed;
        private int failOnHasNextAfter;
        private CountDownLatch firstHasNextBarrier;
        private boolean firstHasNextBlocked;

        private TestIterator(Integer... keys) {
            this.entries = new ArrayList<>(keys.length);
            for (int key : keys) {
                this.entries.add(new TestEntry(keyBytes(key)));
            }
            this.offset = 0;
            this.nextCalls = 0;
            this.current = null;
            this.closed = false;
            this.failOnHasNextAfter = -1;
            this.firstHasNextBarrier = null;
            this.firstHasNextBlocked = false;
        }

        private void failOnHasNextAfter(int nextCalls) {
            this.failOnHasNextAfter = nextCalls;
        }

        private void blockFirstHasNext(CountDownLatch barrier) {
            this.firstHasNextBarrier = barrier;
        }

        @Override
        public boolean hasNext() {
            if (this.nextCalls == this.failOnHasNextAfter) {
                throw new IllegalStateException("injected failure");
            }
            if (this.firstHasNextBarrier != null &&
                !this.firstHasNextBlocked) {
                this.firstHasNextBlocked = true;
                this.firstHasNextBarrier.countDown();
                try {
                    if (!this.firstHasNextBarrier.await(5L,
                                                        TimeUnit.SECONDS)) {
                        throw new IllegalStateException(
                                "Timed out waiting for concurrent source");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(e);
                }
            }
            return this.offset < this.entries.size();
        }

        @Override
        public HgKvEntry next() {
            if (!this.hasNext()) {
                throw new NoSuchElementException();
            }
            this.nextCalls++;
            this.current = this.entries.get(this.offset++);
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
        public void close() {
            this.closed = true;
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
