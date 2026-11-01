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
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;

import org.apache.hugegraph.rocksdb.access.RocksDBSession.BackendColumn;
import org.apache.hugegraph.store.business.itrv2.io.SortShuffleSerializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class SortShuffleTest {

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
    public void testInMemoryOrderingUsesComparatorAndRetainsDuplicates() throws Exception {
        Comparator<BackendColumn> comparator = (left, right) -> Arrays.compare(right.name, left.name);
        SortShuffle<BackendColumn> shuffle = new SortShuffle<>(
                comparator, SortShuffleSerializer.ofBackendColumnSerializer());
        try {
            append(shuffle, 2, 1, 3, 2);
            shuffle.finish();

            assertEquals(List.of(3, 2, 2, 1), keys(shuffle.getIterator()));
        } finally {
            shuffle.close();
        }
        assertEquals(0, this.folder.getRoot().list().length);
    }

    @Test
    public void testEmptyShuffleClosesWithoutLeavingDirectory() throws Exception {
        SortShuffle<BackendColumn> shuffle = newShuffle();
        try {
            shuffle.finish();
            assertFalse(shuffle.getIterator().hasNext());
        } finally {
            shuffle.close();
        }
        assertEquals(0, this.folder.getRoot().list().length);
    }

    @Test
    public void testSpillMergeOrdersRecordsAndCloseStopsEveryReader() throws Exception {
        SortShuffle<BackendColumn> shuffle = newShuffle();
        try {
            addSpill(shuffle, "first", 1, 4);
            addSpill(shuffle, "second", 2, 5);
            append(shuffle, 6, 3);
            shuffle.finish();
            assertEquals(List.of(1, 2, 3, 4, 5, 6), keys(shuffle.getIterator()));

            Iterator<BackendColumn> first = shuffle.getIterator();
            Iterator<BackendColumn> second = shuffle.getIterator();
            assertTrue(first.hasNext());
            assertTrue(second.hasNext());
            shuffle.close();

            assertFalse(first.hasNext());
            assertFalse(second.hasNext());
            assertEquals(0, this.folder.getRoot().list().length);
        } finally {
            shuffle.close();
        }
    }

    @Test
    public void testFailedMergeRemainsCleanable() throws Exception {
        SortShuffle<BackendColumn> shuffle = newShuffle();
        try {
            File corrupt = addSpill(shuffle, "corrupt", 1);
            Files.write(corrupt.toPath(), new byte[]{0, 0});
            addSpill(shuffle, "valid", 2);

            assertThrows(UncheckedIOException.class, shuffle::finish);
        } finally {
            shuffle.close();
        }
        assertEquals(0, this.folder.getRoot().list().length);
    }

    private static SortShuffle<BackendColumn> newShuffle() {
        return new SortShuffle<>((left, right) -> Arrays.compare(left.name, right.name),
                                 SortShuffleSerializer.ofBackendColumnSerializer());
    }

    private static void append(SortShuffle<BackendColumn> shuffle, int... keys) throws Exception {
        for (int key : keys) {
            shuffle.append(BackendColumn.of(new byte[]{(byte) key}, new byte[]{1}));
        }
    }

    private static List<Integer> keys(Iterator<BackendColumn> iterator) {
        List<Integer> keys = new ArrayList<>();
        iterator.forEachRemaining(column -> keys.add((int) column.name[0]));
        return keys;
    }

    @SuppressWarnings("unchecked")
    private static File addSpill(SortShuffle<BackendColumn> shuffle, String name, int... keys)
            throws Exception {
        // Seed small real spill files without changing the million-record production threshold.
        Field path = SortShuffle.class.getDeclaredField("path");
        path.setAccessible(true);
        File file = new File((String) path.get(shuffle), name);
        try (OutputStream output = Files.newOutputStream(file.toPath())) {
            for (int key : keys) {
                SortShuffleSerializer.ofBackendColumnSerializer().write(
                        output, BackendColumn.of(new byte[]{(byte) key}, new byte[]{1}));
            }
        }
        Field files = SortShuffle.class.getDeclaredField("files");
        files.setAccessible(true);
        ((Deque<String>) files.get(shuffle)).add(file.getAbsolutePath());
        return file;
    }
}
