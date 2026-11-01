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
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hugegraph.rocksdb.access.RocksDBSession.BackendColumn;
import org.apache.hugegraph.rocksdb.access.ScanIterator;
import org.apache.hugegraph.store.business.itrv2.FileObjectIterator;
import org.apache.hugegraph.store.business.itrv2.TypeTransIterator;
import org.apache.hugegraph.store.business.itrv2.io.SortShuffleSerializer;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TypeTransIteratorTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void testDirectNextAndRepeatedProbePreserveFilteredResultsAndFinalValue() {
        AtomicInteger mappings = new AtomicInteger();
        AtomicInteger finalValues = new AtomicInteger();
        TypeTransIterator<Integer, Integer> converted = new TypeTransIterator<>(
                Arrays.asList(null, 1, 2, 3).iterator(), value -> {
                    mappings.incrementAndGet();
                    return value == 2 ? null : value * 10;
                }, () -> {
                    finalValues.incrementAndGet();
                    return 99;
                });
        Iterator<Integer> iterator = converted.toIterator();

        assertEquals(Integer.valueOf(10), iterator.next());
        assertTrue(iterator.hasNext());
        assertTrue(iterator.hasNext());
        assertEquals(Integer.valueOf(30), iterator.next());
        assertEquals(Integer.valueOf(99), iterator.next());
        assertFalse(iterator.hasNext());
        assertFalse(iterator.hasNext());
        assertThrows(NoSuchElementException.class, iterator::next);
        assertEquals(3, mappings.get());
        assertEquals(1, finalValues.get());
    }

    @Test
    public void testEmptySourceAndNullFinalValueAreOnlyProbedOnce() {
        TrackingIterator source = new TrackingIterator(List.of(), null);
        AtomicInteger finalValues = new AtomicInteger();
        Iterator<Integer> iterator = new TypeTransIterator<>(source, value -> value, () -> {
            finalValues.incrementAndGet();
            return null;
        }).toIterator();

        assertFalse(iterator.hasNext());
        assertFalse(iterator.hasNext());
        assertThrows(NoSuchElementException.class, iterator::next);
        assertEquals(1, source.probes);
        assertEquals(1, finalValues.get());
    }

    @Test
    public void testFailedFinalValueCanBeRetriedWithoutBecomingEof() {
        AtomicInteger attempts = new AtomicInteger();
        IllegalStateException cause = new IllegalStateException("final value failed");
        Iterator<Integer> iterator = new TypeTransIterator<>(List.<Integer>of().iterator(),
                                                             value -> value, () -> {
            if (attempts.getAndIncrement() == 0) {
                throw cause;
            }
            return 99;
        }).toIterator();

        assertSame(cause, assertThrows(IllegalStateException.class, iterator::hasNext));
        assertEquals(Integer.valueOf(99), iterator.next());
        assertFalse(iterator.hasNext());
        assertFalse(iterator.hasNext());
        assertEquals(2, attempts.get());
    }

    @Test
    public void testAdapterCloseClosesOwnedScanOnceAndDiscardsPendingValue() {
        ScanIterator source = mock(ScanIterator.class);
        when(source.hasNext()).thenReturn(true);
        when(source.next()).thenReturn(1);
        TypeTransIterator<Integer, Integer> converted = new TypeTransIterator<>(source, value -> value);
        Iterator<Integer> iterator = converted.toIterator();
        assertTrue(iterator.hasNext());

        ((ScanIterator) iterator).close();
        converted.close();
        ((ScanIterator) iterator).close();

        verify(source, times(1)).close();
        assertFalse(iterator.hasNext());
        assertFalse(((ScanIterator) iterator).isValid());
        assertThrows(NoSuchElementException.class, iterator::next);
        verify(source, times(1)).hasNext();
    }

    @Test
    public void testAdapterCloseReleasesRealFileReaderBeforeEof() throws Exception {
        File file = this.folder.newFile();
        try (OutputStream output = Files.newOutputStream(file.toPath())) {
            SortShuffleSerializer.ofBackendColumnSerializer().write(
                    output, BackendColumn.of(new byte[]{1}, new byte[]{2}));
        }
        try (FileObjectIterator<BackendColumn> source = new FileObjectIterator<>(
                file.getAbsolutePath(), SortShuffleSerializer.ofBackendColumnSerializer())) {
            Iterator<Integer> iterator = new TypeTransIterator<>(source, column -> (int) column.name[0])
                    .toIterator();
            assertTrue(iterator.hasNext());
            Field field = FileObjectIterator.class.getDeclaredField("fis");
            field.setAccessible(true);
            FileInputStream stream = (FileInputStream) field.get(source);
            assertTrue(stream.getFD().valid());

            ((ScanIterator) iterator).close();

            assertFalse(stream.getFD().valid());
            assertFalse(source.hasNext());
            assertFalse(iterator.hasNext());
        }
    }

    @Test
    public void testCheckedCloseFailureRetainsCauseAndLeavesAdapterClosed() {
        IOException cause = new IOException("close failed");
        TrackingIterator source = new TrackingIterator(List.of(1), cause);
        Iterator<Integer> iterator = new TypeTransIterator<>(source, value -> value).toIterator();
        assertTrue(iterator.hasNext());

        IllegalStateException failure = assertThrows(IllegalStateException.class,
                                                     () -> ((ScanIterator) iterator).close());

        assertSame(cause, failure.getCause());
        assertFalse(iterator.hasNext());
        assertSame(failure, assertThrows(IllegalStateException.class,
                                        () -> ((ScanIterator) iterator).close()));
        assertEquals(1, source.closes);
    }

    @Test
    public void testRuntimeCloseFailureIsNotWrapped() {
        IllegalArgumentException cause = new IllegalArgumentException("close failed");
        TrackingIterator source = new TrackingIterator(List.of(1), cause);
        TypeTransIterator<Integer, Integer> converted = new TypeTransIterator<>(source, value -> value);

        assertSame(cause, assertThrows(IllegalArgumentException.class, converted::close));
        assertSame(cause, assertThrows(IllegalArgumentException.class, converted::close));
        assertEquals(1, source.closes);
        assertFalse(converted.hasNext());
    }

    @Test
    public void testInterruptedClosePreservesInterruptAndCause() {
        InterruptedException cause = new InterruptedException("close interrupted");
        TrackingIterator source = new TrackingIterator(List.of(1), cause);
        TypeTransIterator<Integer, Integer> converted = new TypeTransIterator<>(source, value -> value);
        try {
            IllegalStateException failure = assertThrows(IllegalStateException.class, converted::close);
            assertSame(cause, failure.getCause());
            assertTrue(Thread.currentThread().isInterrupted());
            assertFalse(converted.hasNext());
        } finally {
            Thread.interrupted();
        }
    }

    private static class TrackingIterator implements Iterator<Integer>, AutoCloseable {

        private final Iterator<Integer> iterator;
        private final Exception closeFailure;
        private int probes;
        private int closes;

        private TrackingIterator(List<Integer> values, Exception closeFailure) {
            this.iterator = values.iterator();
            this.closeFailure = closeFailure;
        }

        @Override
        public boolean hasNext() {
            this.probes++;
            return this.iterator.hasNext();
        }

        @Override
        public Integer next() {
            return this.iterator.next();
        }

        @Override
        public void close() throws Exception {
            this.closes++;
            if (this.closeFailure != null) {
                throw this.closeFailure;
            }
        }
    }
}
