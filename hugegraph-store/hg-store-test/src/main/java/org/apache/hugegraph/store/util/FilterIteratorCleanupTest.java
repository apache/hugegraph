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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Deque;
import java.util.Map;

import org.apache.hugegraph.rocksdb.access.RocksDBSession.BackendColumn;
import org.apache.hugegraph.rocksdb.access.ScanIterator;
import org.apache.hugegraph.store.business.itrv2.FileObjectIterator;
import org.apache.hugegraph.store.business.itrv2.IntersectionFilterIterator;
import org.apache.hugegraph.store.business.itrv2.IntersectionWrapper;
import org.apache.hugegraph.store.business.itrv2.UnionFilterIterator;
import org.apache.hugegraph.store.business.itrv2.io.SortShuffleSerializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class FilterIteratorCleanupTest {

    @Parameterized.Parameters(name = "{0}")
    public static Object[] iterators() {
        return new Object[]{"intersection", "union"};
    }

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private final String kind;
    private String previousBasePath;

    public FilterIteratorCleanupTest(String kind) {
        this.kind = kind;
    }

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
    public void testCloseReleasesShuffleFilesAndReader() throws Exception {
        verifyClose(null, null);
    }

    @Test
    public void testUpstreamCloseFailureStillReleasesShuffleFilesAndReader() throws Exception {
        verifyClose(new IllegalStateException("upstream close failed"), null);
    }

    @Test
    public void testShuffleCloseFailureIsPreserved() throws Exception {
        verifyClose(null, new IllegalStateException("shuffle close failed"));
    }

    @Test
    public void testCleanupFailureIsSuppressedByUpstreamFailure() throws Exception {
        verifyClose(new IllegalStateException("upstream close failed"),
                    new IllegalStateException("shuffle close failed"));
    }

    @SuppressWarnings("unchecked")
    private void verifyClose(RuntimeException upstreamFailure, RuntimeException shuffleFailure)
            throws Exception {
        TrackingIterator upstream = new TrackingIterator(upstreamFailure);
        IntersectionWrapper<BackendColumn> wrapper = new IntersectionWrapper<>(
                upstream, column -> column.name[0]);
        ScanIterator filter = this.kind.equals("intersection") ?
                              new IntersectionFilterIterator(upstream, wrapper, 2) :
                              new UnionFilterIterator<>(upstream, wrapper,
                                                        (left, right) -> Arrays.compare(left.name,
                                                                                       right.name),
                                                        SortShuffleSerializer.ofBackendColumnSerializer());
        SortShuffle<BackendColumn> shuffle = new SortShuffle<>(
                (left, right) -> Arrays.compare(left.name, right.name),
                SortShuffleSerializer.ofBackendColumnSerializer()) {
            @Override
            public void close() {
                super.close();
                if (shuffleFailure != null) {
                    throw shuffleFailure;
                }
            }
        };
        try {
            File spill = new File((String) field(SortShuffle.class, "path").get(shuffle), "spill");
            try (OutputStream output = Files.newOutputStream(spill.toPath())) {
                SortShuffleSerializer.ofBackendColumnSerializer().write(
                        output, BackendColumn.of(new byte[]{1}, new byte[]{2}));
            }
            ((Deque<String>) field(SortShuffle.class, "files").get(shuffle))
                    .add(spill.getAbsolutePath());
            FileObjectIterator<BackendColumn> reader =
                    (FileObjectIterator<BackendColumn>) shuffle.getIterator();
            assertTrue(reader.hasNext());
            FileInputStream stream = (FileInputStream) field(FileObjectIterator.class, "fis")
                    .get(reader);
            assertTrue(stream.getFD().valid());
            field(filter.getClass(), "sortShuffle").set(filter, shuffle);
            Map<Object, Integer> map = (Map<Object, Integer>) field(filter.getClass(), "map").get(filter);
            map.put(new Object(), 1);

            RuntimeException expected = upstreamFailure != null ? upstreamFailure : shuffleFailure;
            if (expected == null) {
                filter.close();
            } else {
                RuntimeException failure = assertThrows(RuntimeException.class, filter::close);
                assertSame(expected, failure);
                Throwable[] suppressed = upstreamFailure != null && shuffleFailure != null ?
                                         new Throwable[]{shuffleFailure} : new Throwable[0];
                assertArrayEquals(suppressed, failure.getSuppressed());
            }

            assertEquals(1, upstream.closeCalls);
            assertFalse(reader.hasNext());
            assertFalse(stream.getFD().valid());
            assertFalse(spill.exists());
            assertEquals(0, this.folder.getRoot().list().length);
            assertTrue(map.isEmpty());
            assertNull(field(filter.getClass(), "sortShuffle").get(filter));
        } finally {
            try {
                shuffle.close();
            } catch (RuntimeException cleanup) {
                if (cleanup != shuffleFailure) {
                    throw cleanup;
                }
            }
        }
    }

    private static Field field(Class<?> owner, String name) throws Exception {
        Field field = owner.getDeclaredField(name);
        field.setAccessible(true);
        return field;
    }

    private static class TrackingIterator implements ScanIterator {

        private final RuntimeException failure;
        private int closeCalls;

        private TrackingIterator(RuntimeException failure) {
            this.failure = failure;
        }

        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public boolean isValid() {
            return false;
        }

        @Override
        public <T> T next() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {
            this.closeCalls++;
            if (this.failure != null) {
                throw this.failure;
            }
        }
    }
}
