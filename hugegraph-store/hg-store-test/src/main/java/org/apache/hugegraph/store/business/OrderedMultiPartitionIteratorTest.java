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

package org.apache.hugegraph.store.business;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hugegraph.rocksdb.access.RocksDBSession.BackendColumn;
import org.apache.hugegraph.rocksdb.access.ScanIterator;
import org.junit.Assert;
import org.junit.Test;

public class OrderedMultiPartitionIteratorTest {

    @Test
    public void testOrderedScanIsBackwardCompatibleDefaultMethod()
            throws Exception {
        Assert.assertTrue(BusinessHandler.class.getMethod(
                "scanOrdered", String.class, String.class, byte[].class,
                byte[].class, int.class).isDefault());
    }

    @Test
    public void testMergeByUnsignedKeyAndTrackPartitionPosition() {
        Map<Integer, TestIterator> sources = new HashMap<>();
        sources.put(1, new TestIterator(1, 4));
        sources.put(3, new TestIterator(2, 3));
        AtomicInteger supplierCalls = new AtomicInteger();
        OrderedMultiPartitionIterator iterator =
                OrderedMultiPartitionIterator.of(Arrays.asList(3, 1), id -> {
                    supplierCalls.incrementAndGet();
                    return sources.get(id);
                });

        Assert.assertEquals(0, supplierCalls.get());
        List<Integer> keys = new ArrayList<>();
        List<Integer> positions = new ArrayList<>();
        while (iterator.hasNext()) {
            BackendColumn column = iterator.next();
            keys.add(column.name[0] & 0xff);
            positions.add(ByteBuffer.wrap(iterator.position()).getInt());
        }

        Assert.assertEquals(Arrays.asList(1, 2, 3, 4), keys);
        Assert.assertEquals(Arrays.asList(1, 3, 3, 1), positions);
        Assert.assertEquals(2, supplierCalls.get());
        Assert.assertTrue(sources.get(1).closed);
        Assert.assertTrue(sources.get(3).closed);
    }

    @Test
    public void testMergeComparesKeysAsUnsignedBytes() {
        Map<Integer, TestIterator> sources = new HashMap<>();
        sources.put(1, new TestIterator(0x80));
        sources.put(2, new TestIterator(0x7f));
        OrderedMultiPartitionIterator iterator =
                OrderedMultiPartitionIterator.of(Arrays.asList(1, 2), sources::get);

        List<Integer> keys = new ArrayList<>();
        while (iterator.hasNext()) {
            BackendColumn column = iterator.next();
            keys.add(column.name[0] & 0xff);
        }

        Assert.assertEquals(Arrays.asList(0x7f, 0x80), keys);
    }

    @Test
    public void testCountConsumesAllRemainingEntries() {
        Map<Integer, TestIterator> sources = new HashMap<>();
        sources.put(1, new TestIterator(1, 4));
        sources.put(2, new TestIterator(2, 3));
        OrderedMultiPartitionIterator iterator =
                OrderedMultiPartitionIterator.of(Arrays.asList(1, 2),
                                                 sources::get);

        Assert.assertEquals(4L, iterator.count());
        Assert.assertFalse(iterator.hasNext());
        Assert.assertTrue(sources.get(1).closed);
        Assert.assertTrue(sources.get(2).closed);
    }

    @Test
    public void testInitializationFailureClosesOpenedIterators() {
        TestIterator first = new TestIterator(1);
        OrderedMultiPartitionIterator iterator =
                OrderedMultiPartitionIterator.of(Arrays.asList(1, 2), id -> {
                    if (id == 1) {
                        return first;
                    }
                    throw new IllegalStateException("injected failure");
                });

        Assert.assertThrows(IllegalStateException.class, iterator::hasNext);
        Assert.assertTrue(first.closed);
    }

    @Test
    public void testCloseBeforeInitializationDoesNotOpenSources() {
        AtomicInteger supplierCalls = new AtomicInteger();
        OrderedMultiPartitionIterator iterator =
                OrderedMultiPartitionIterator.of(Arrays.asList(1, 2), id -> {
                    supplierCalls.incrementAndGet();
                    return new TestIterator(id);
                });

        iterator.close();

        Assert.assertEquals(0, supplierCalls.get());
        Assert.assertFalse(iterator.hasNext());
    }

    private static byte[] keyBytes(int key) {
        return new byte[]{(byte) key};
    }

    private static final class TestIterator implements ScanIterator {

        private final List<BackendColumn> columns;
        private int offset;
        private boolean closed;

        private TestIterator(Integer... keys) {
            this.columns = new ArrayList<>(keys.length);
            for (int key : keys) {
                this.columns.add(BackendColumn.of(keyBytes(key), keyBytes(key)));
            }
            this.offset = 0;
            this.closed = false;
        }

        @Override
        public boolean hasNext() {
            return !this.closed && this.offset < this.columns.size();
        }

        @Override
        public boolean isValid() {
            return this.hasNext();
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T> T next() {
            if (!this.hasNext()) {
                throw new NoSuchElementException();
            }
            return (T) this.columns.get(this.offset++);
        }

        @Override
        public void close() {
            this.closed = true;
        }
    }
}
