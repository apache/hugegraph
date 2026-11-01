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

package org.apache.hugegraph.store.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.hugegraph.rocksdb.access.RocksDBSession.BackendColumn;
import org.apache.hugegraph.rocksdb.access.ScanIterator;
import org.apache.hugegraph.store.business.itrv2.IntersectionFilterIterator;
import org.apache.hugegraph.store.business.itrv2.IntersectionWrapper;
import org.apache.hugegraph.store.business.itrv2.MapLimitIterator;
import org.apache.hugegraph.store.business.itrv2.UnionFilterIterator;
import org.apache.hugegraph.store.business.itrv2.io.SortShuffleSerializer;
import org.junit.Test;

/**
 * A RocksDB scan allocates a new BackendColumn for every row, so the hash-based union,
 * intersection and limit-dedup iterators must treat byte-equal columns from separate scans as
 * one key. Every row below is built from fresh arrays; reusing one object per key would hide a
 * missing hashCode().
 */
public class FilterIteratorColumnEqualityTest {

    private static final int KEYS = 100;
    private static final Set<Integer> ALL_KEYS =
            IntStream.range(0, KEYS).boxed().collect(Collectors.toSet());

    @Test
    public void testEqualColumnsFromSeparateArraysShareHashCode() {
        BackendColumn left = column(7);
        BackendColumn right = column(7);

        assertEquals(left, right);
        assertEquals(left.hashCode(), right.hashCode());

        Set<BackendColumn> set = new HashSet<>();
        set.add(left);
        set.add(right);
        assertEquals(1, set.size());
    }

    @Test
    public void testColumnsWithEqualNamesButDifferentValuesStayDistinct() {
        BackendColumn left = BackendColumn.of(name(7), new byte[]{1});
        BackendColumn right = BackendColumn.of(name(7), new byte[]{2});

        assertNotEquals(left, right);

        Set<BackendColumn> set = new HashSet<>();
        set.add(left);
        set.add(right);
        assertEquals(2, set.size());
    }

    @Test
    public void testIntersectionCombinesEqualRowsFromFreshObjects() {
        IntersectionWrapper<BackendColumn> wrapper = wrapper();
        ScanIterator filter = new IntersectionFilterIterator(scanTwice(), wrapper, 2);

        assertEquals(ALL_KEYS, drain(filter));
    }

    @Test
    public void testUnionBelowSortThresholdCombinesEqualRowsFromFreshObjects() {
        IntersectionWrapper<BackendColumn> wrapper = wrapper();
        ScanIterator filter = new UnionFilterIterator<>(
                scanTwice(), wrapper, (left, right) -> Arrays.compare(left.name, right.name),
                SortShuffleSerializer.ofBackendColumnSerializer());

        assertEquals(ALL_KEYS, drain(filter));
    }

    @Test
    public void testLimitDedupDropsEqualRowsFromFreshObjects() {
        ScanIterator filter = new MapLimitIterator<BackendColumn>(scanTwice());

        assertEquals(ALL_KEYS, drain(filter));
    }

    /**
     * Marks every key as a repeated candidate, the way a multi-list query does once each key
     * has been seen in more than one source.
     */
    private static IntersectionWrapper<BackendColumn> wrapper() {
        IntersectionWrapper<BackendColumn> wrapper = new IntersectionWrapper<>(
                scanTwice(), column -> ByteBuffer.wrap(column.name).getInt());
        wrapper.proc();
        return wrapper;
    }

    /**
     * Each key appears twice, each time as a newly allocated column with its own arrays.
     */
    private static ScanIterator scanTwice() {
        List<BackendColumn> rows = new ArrayList<>();
        for (int round = 0; round < 2; round++) {
            for (int key = 0; key < KEYS; key++) {
                rows.add(column(key));
            }
        }
        return new ListScanIterator(rows);
    }

    private static BackendColumn column(int key) {
        return BackendColumn.of(name(key), name(key));
    }

    private static byte[] name(int key) {
        return ByteBuffer.allocate(Integer.BYTES).putInt(key).array();
    }

    /**
     * Returns the keys the iterator emits, failing on a key emitted twice so that a wrong row
     * count can't hide behind a set.
     */
    private static Set<Integer> drain(ScanIterator iterator) {
        Set<Integer> keys = new HashSet<>();
        try {
            while (iterator.hasNext()) {
                BackendColumn column = iterator.next();
                int key = ByteBuffer.wrap(column.name).getInt();
                if (!keys.add(key)) {
                    throw new AssertionError("key " + key + " emitted twice");
                }
            }
        } finally {
            iterator.close();
        }
        return keys;
    }

    private static class ListScanIterator implements ScanIterator {

        private final Iterator<BackendColumn> rows;

        private ListScanIterator(List<BackendColumn> rows) {
            this.rows = rows.iterator();
        }

        @Override
        public boolean hasNext() {
            return this.rows.hasNext();
        }

        @Override
        public boolean isValid() {
            return this.rows.hasNext();
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T> T next() {
            return (T) this.rows.next();
        }

        @Override
        public void close() {
        }
    }
}
