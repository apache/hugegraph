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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.ToLongFunction;

import org.apache.hugegraph.rocksdb.access.RocksDBSession.BackendColumn;
import org.apache.hugegraph.rocksdb.access.ScanIterator;
import org.apache.hugegraph.store.business.itrv2.IntersectionWrapper;
import org.apache.hugegraph.store.business.itrv2.UnionFilterIterator;
import org.apache.hugegraph.store.business.itrv2.io.SortShuffleSerializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class UnionFilterIteratorTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private String previousBasePath;

    @Before
    public void setUp() {
        this.previousBasePath = SortShuffle.getBasePath();
        SortShuffle.setBasePath(this.folder.getRoot().getAbsolutePath() + File.separator);
    }

    @After
    public void tearDown() {
        SortShuffle.setBasePath(this.previousBasePath);
    }

    @Test
    public void testRepeatedKeysAcrossMapFlushesAreReturnedOnce() {
        int keys = 20000;
        List<BackendColumn> rows = new ArrayList<>(keys * 2);
        for (int pass = 0; pass < 2; pass++) {
            for (int key = 0; key < keys; key++) {
                rows.add(column(key));
            }
        }

        assertUnion(rows, column -> SortShuffleSerializer.toInt(column.name), keys);
    }

    @Test
    public void testMixedGroupsIncludeRepeatedLastKey() {
        int keys = 10001;
        List<BackendColumn> rows = new ArrayList<>();
        for (int key = keys - 1; key >= 0; key--) {
            for (int copy = 0; copy <= key % 3; copy++) {
                rows.add(column(key));
            }
        }

        // Hash collisions send singletons and repeated groups through the same reducer.
        assertUnion(rows, column -> 0, keys);
    }

    private void assertUnion(List<BackendColumn> rows, ToLongFunction<BackendColumn> hash,
                             int expectedKeys) {
        IntersectionWrapper<BackendColumn> wrapper = new IntersectionWrapper<>(
                new RowIterator(rows), hash);
        wrapper.proc();
        try (ScanIterator iterator = new UnionFilterIterator<>(
                new RowIterator(rows), wrapper, BackendColumn::compareTo,
                SortShuffleSerializer.ofBackendColumnSerializer())) {
            // Exercise direct next(), then repeated probes across group boundaries and EOF.
            BackendColumn first = iterator.next();
            assertEquals(0, SortShuffleSerializer.toInt(first.name));
            for (int key = 1; key < expectedKeys; key++) {
                assertTrue(iterator.hasNext());
                assertTrue(iterator.hasNext());
                BackendColumn column = iterator.next();
                assertEquals(key, SortShuffleSerializer.toInt(column.name));
            }
            assertFalse(iterator.hasNext());
            assertFalse(iterator.hasNext());
            assertThrows(NoSuchElementException.class, iterator::next);
        }
        assertEquals(0, this.folder.getRoot().list().length);
    }

    private static BackendColumn column(int key) {
        // Fresh objects and arrays keep this regression independent of content hashing.
        return BackendColumn.of(SortShuffleSerializer.toByte(key), new byte[]{1});
    }

    private static class RowIterator implements ScanIterator {

        private final Iterator<BackendColumn> rows;

        private RowIterator(List<BackendColumn> rows) {
            this.rows = rows.iterator();
        }

        @Override
        public boolean hasNext() {
            return this.rows.hasNext();
        }

        @Override
        public boolean isValid() {
            return hasNext();
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
