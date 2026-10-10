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

package org.apache.hugegraph.rocksdb.access;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;

import org.apache.hugegraph.rocksdb.access.RocksDBSession.BackendColumn;
import org.apache.hugegraph.store.business.InnerKeyFilter;
import org.apache.hugegraph.store.business.MultiPartitionIterator;
import org.apache.hugegraph.store.term.Bits;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.InOrder;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;

public class RocksDBScanIteratorCloseTest {

    @Rule
    public TemporaryFolder directory = new TemporaryFolder();

    @Test
    @SuppressWarnings("unchecked")
    public void testRealNativeEmptyIteratorRetainsOneTimeCallbackFailure() throws Exception {
        RocksDB.loadLibrary();
        try (Options options = new Options().setCreateIfMissing(true);
             RocksDB database = RocksDB.open(options, this.directory.newFolder("empty").getAbsolutePath())) {
            RocksIterator raw = database.newIterator();
            RocksDBSession.RefCounter reference = mock(RocksDBSession.RefCounter.class);
            Consumer<Boolean> closeOp = mock(Consumer.class);
            IllegalStateException failure = new IllegalStateException("close callback failed once");
            doThrow(failure).doNothing().when(closeOp).accept(true);
            RocksDBScanIterator<?> iterator = new RocksDBScanIterator<>(raw, null, null,
                    ScanIterator.Trait.SCAN_ANY, reference, closeOp);

            assertSame(failure, assertThrows(IllegalStateException.class, iterator::hasNext));
            assertFalse("JNI must release the native iterator before callback failure", raw.isOwningHandle());
            assertSame(failure, assertThrows(IllegalStateException.class, iterator::close));
            assertSame(failure, assertThrows(IllegalStateException.class, iterator::close));
            verify(closeOp).accept(true);
            verify(reference, never()).release();
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testRealNativeExhaustedIteratorRetainsOneTimeReferenceReleaseError() throws Exception {
        RocksDB.loadLibrary();
        try (Options options = new Options().setCreateIfMissing(true);
             RocksDB database = RocksDB.open(options, this.directory.newFolder("exhausted").getAbsolutePath())) {
            database.put(new byte[]{1}, new byte[]{2});
            RocksIterator raw = database.newIterator();
            RocksDBSession.RefCounter reference = mock(RocksDBSession.RefCounter.class);
            Consumer<Boolean> closeOp = mock(Consumer.class);
            AssertionError failure = new AssertionError("reference release failed once");
            doThrow(failure).doNothing().when(reference).release();
            RocksDBScanIterator<?> iterator = new RocksDBScanIterator<>(raw, null, null,
                    ScanIterator.Trait.SCAN_ANY, reference, closeOp);

            assertTrue(iterator.hasNext());
            iterator.next();
            assertSame(failure, assertThrows(AssertionError.class, iterator::hasNext));
            assertFalse("JNI must release the native iterator before reference failure", raw.isOwningHandle());
            assertSame(failure, assertThrows(AssertionError.class, iterator::close));
            assertSame(failure, assertThrows(AssertionError.class, iterator::close));
            InOrder order = inOrder(closeOp, reference);
            order.verify(closeOp).accept(true);
            order.verify(reference).release();
        }
    }

    @Test
    public void testEmptyIteratorRetainsOneTimeNativeCloseFailure() {
        Fixture fixture = new Fixture();
        IllegalStateException failure = new IllegalStateException("native close failed once");
        doThrow(failure).doNothing().when(fixture.raw).close();

        assertAutomaticFailureIsRetained(fixture, failure);
        verify(fixture.closeOp, never()).accept(true);
        verify(fixture.reference, never()).release();
    }

    @Test
    public void testExhaustedIteratorRetainsOneTimeNativeCloseError() {
        Fixture fixture = new Fixture();
        when(fixture.raw.isValid()).thenReturn(true, false);
        when(fixture.raw.key()).thenReturn(new byte[]{1});
        when(fixture.raw.value()).thenReturn(new byte[]{2});
        AssertionError failure = new AssertionError("native close failed once");
        doThrow(failure).doNothing().when(fixture.raw).close();

        assertTrue(fixture.iterator.hasNext());
        fixture.iterator.next();
        assertAutomaticFailureIsRetained(fixture, failure);
        verify(fixture.closeOp, never()).accept(true);
        verify(fixture.reference, never()).release();
    }

    @Test
    public void testAutomaticCloseRetainsOneTimeCallbackFailure() {
        Fixture fixture = new Fixture();
        IllegalStateException failure = new IllegalStateException("close callback failed once");
        doThrow(failure).doNothing().when(fixture.closeOp).accept(true);

        assertAutomaticFailureIsRetained(fixture, failure);
        InOrder order = inOrder(fixture.raw, fixture.closeOp);
        order.verify(fixture.raw).close();
        order.verify(fixture.closeOp).accept(true);
        verify(fixture.reference, never()).release();
    }

    @Test
    public void testAutomaticCloseRetainsOneTimeReferenceReleaseError() {
        Fixture fixture = new Fixture();
        AssertionError failure = new AssertionError("reference release failed once");
        doThrow(failure).doNothing().when(fixture.reference).release();

        assertAutomaticFailureIsRetained(fixture, failure);
        InOrder order = inOrder(fixture.raw, fixture.closeOp, fixture.reference);
        order.verify(fixture.raw).close();
        order.verify(fixture.closeOp).accept(true);
        order.verify(fixture.reference).release();
    }

    @Test
    public void testSuccessfulCloseIsIdempotentAndPreservesReleaseOrder() {
        Fixture fixture = new Fixture();

        fixture.iterator.close();
        fixture.iterator.close();
        assertFalse(fixture.iterator.hasNext());
        InOrder order = inOrder(fixture.raw, fixture.closeOp, fixture.reference);
        order.verify(fixture.raw).close();
        order.verify(fixture.closeOp).accept(true);
        order.verify(fixture.reference).release();
        verify(fixture.raw).close();
        verify(fixture.closeOp).accept(true);
        verify(fixture.reference).release();
    }

    @Test(timeout = 5000)
    public void testConcurrentCloseWaitsForSuccessfulRelease() throws Exception {
        assertConcurrentCloseWaits(null);
    }

    @Test(timeout = 5000)
    public void testConcurrentCloseWaitsAndReportsTheSameFailure() throws Exception {
        assertConcurrentCloseWaits(new IllegalStateException("close callback failed once"));
    }

    @Test
    public void testAllInnerKeyConstructorsDeferReadingUntilOwnershipTransfer() {
        for (int constructor = 0; constructor < 3; constructor++) {
            ScanIterator raw = mock(ScanIterator.class);
            when(raw.hasNext()).thenReturn(true);
            when(raw.next()).thenReturn(column(1, 15));
            InnerKeyFilter<BackendColumn> filter = filter(raw, constructor);

            verify(raw, never()).hasNext();
            verify(raw, never()).next();
            assertTrue(filter.hasNext());
            verify(raw).hasNext();
            verify(raw).next();
            filter.close();
            verify(raw).close();
        }
    }

    @Test
    public void testPlainInnerKeyFilterStripsGraphAndCodeAndPreservesPrefetch() {
        ScanIterator raw = mock(ScanIterator.class);
        when(raw.hasNext()).thenReturn(true, true, false);
        when(raw.next()).thenReturn(column(1, 5), column(2, 19));
        InnerKeyFilter<BackendColumn> filter = new InnerKeyFilter<>(raw);

        assertTrue(filter.hasNext());
        assertTrue(filter.hasNext());
        assertArrayEquals(new byte[]{1}, filter.next().name);
        assertTrue(filter.hasNext());
        assertArrayEquals(new byte[]{2}, filter.next().name);
        assertFalse(filter.hasNext());
        verify(raw, times(2)).next();
        verify(raw, times(3)).hasNext();
        filter.close();
    }

    @Test
    public void testCodeFilterKeepsHashSuffix() {
        ScanIterator raw = mock(ScanIterator.class);
        when(raw.hasNext()).thenReturn(true, false);
        when(raw.next()).thenReturn(column(3, 15));
        InnerKeyFilter<BackendColumn> filter = new InnerKeyFilter<>(raw, true);

        assertArrayEquals(new byte[]{3, 0, 15}, filter.next().name);
        assertFalse(filter.hasNext());
        filter.close();
    }

    @Test
    public void testCodeRangeFilterPreservesInclusiveLowerAndExclusiveUpperBounds() {
        ScanIterator raw = mock(ScanIterator.class);
        when(raw.hasNext()).thenReturn(true, true, true, true, false);
        when(raw.next()).thenReturn(column(1, 5), column(2, 10), column(3, 20), column(4, 19));
        InnerKeyFilter<BackendColumn> filter = new InnerKeyFilter<>(raw, 10, 20);

        assertTrue(filter.hasNext());
        assertArrayEquals(new byte[]{2, 0, 10}, filter.next().name);
        assertTrue(filter.hasNext());
        assertArrayEquals(new byte[]{4, 0, 19}, filter.next().name);
        assertFalse(filter.hasNext());
        verify(raw, times(4)).next();
        filter.close();
    }

    @Test
    public void testInnerKeyFilterNextBeforeHasNextPreservesTheFirstColumn() {
        ScanIterator raw = mock(ScanIterator.class);
        when(raw.hasNext()).thenReturn(true, false);
        when(raw.next()).thenReturn(column(7, 15));
        InnerKeyFilter<BackendColumn> filter = new InnerKeyFilter<>(raw);

        assertArrayEquals(new byte[]{7}, filter.next().name);
        assertFalse(filter.hasNext());
        verify(raw).next();
        filter.close();
    }

    @Test
    public void testInnerKeyFilterIsValidUsesTheExistingPrefetchedDelegatePosition() {
        ScanIterator raw = mock(ScanIterator.class);
        when(raw.hasNext()).thenReturn(true);
        when(raw.next()).thenReturn(column(1, 15));
        when(raw.isValid()).thenAnswer(invocation -> {
            verify(raw).next();
            return false;
        });
        InnerKeyFilter<BackendColumn> filter = new InnerKeyFilter<>(raw);

        assertFalse(filter.isValid());
        assertTrue(filter.hasNext());
        verify(raw).next();
        filter.close();
    }

    @Test
    public void testInnerKeyFilterCountKeepsTheExistingPrefetchedDelegatePosition() {
        ScanIterator raw = mock(ScanIterator.class);
        when(raw.hasNext()).thenReturn(true);
        when(raw.next()).thenReturn(column(1, 15));
        when(raw.count()).thenAnswer(invocation -> {
            verify(raw).next();
            return 7L;
        });
        InnerKeyFilter<BackendColumn> filter = new InnerKeyFilter<>(raw);

        assertEquals(7L, filter.count());
        verify(raw).next();
        filter.close();
    }

    @Test
    public void testInnerKeyFilterCloseBeforeUseDoesNotPrefetch() {
        for (int constructor = 0; constructor < 3; constructor++) {
            ScanIterator raw = mock(ScanIterator.class);
            InnerKeyFilter<BackendColumn> filter = filter(raw, constructor);

            filter.close();
            assertFalse(filter.hasNext());
            verify(raw, never()).hasNext();
            verify(raw, never()).next();
            verify(raw).close();
        }
    }

    @Test
    public void testInnerKeyFilterPreservesPartitionResumeKey() {
        ScanIterator raw = mock(ScanIterator.class);
        when(raw.hasNext()).thenReturn(true, false);
        when(raw.next()).thenReturn(column(9, 15));
        AtomicInteger supplied = new AtomicInteger();
        MultiPartitionIterator iterator = MultiPartitionIterator.of(Arrays.asList(1, 2), (id, position) -> {
            assertEquals(Integer.valueOf(2), id);
            assertArrayEquals(new byte[]{9}, position);
            supplied.incrementAndGet();
            return new InnerKeyFilter<>(raw);
        });
        iterator.seek(ByteBuffer.allocate(Integer.BYTES + 1).putInt(2).put((byte) 9).array());

        assertTrue(iterator.hasNext());
        BackendColumn result = iterator.next();
        assertArrayEquals(new byte[]{9}, result.name);
        assertArrayEquals(ByteBuffer.allocate(Integer.BYTES).putInt(2).array(), iterator.position());
        assertFalse(iterator.hasNext());
        assertEquals(1, supplied.get());
        verify(raw).close();
    }

    private static InnerKeyFilter<BackendColumn> filter(ScanIterator raw, int constructor) {
        if (constructor == 0) {
            return new InnerKeyFilter<>(raw);
        }
        if (constructor == 1) {
            return new InnerKeyFilter<>(raw, true);
        }
        return new InnerKeyFilter<>(raw, 10, 20);
    }

    private static BackendColumn column(int key, int code) {
        byte[] name = new byte[Short.BYTES + 1 + Short.BYTES];
        Bits.putShort(name, 0, 1);
        name[Short.BYTES] = (byte) key;
        Bits.putShort(name, name.length - Short.BYTES, code);
        return BackendColumn.of(name, new byte[]{(byte) key});
    }

    private static void assertAutomaticFailureIsRetained(Fixture fixture, Throwable failure) {
        assertSame(failure, assertThrows(Throwable.class, fixture.iterator::hasNext));
        assertSame(failure, assertThrows(Throwable.class, fixture.iterator::close));
        assertSame(failure, assertThrows(Throwable.class, fixture.iterator::close));
        verify(fixture.raw).close();
    }

    private static void assertConcurrentCloseWaits(Throwable failure) throws Exception {
        Fixture fixture = new Fixture();
        CountDownLatch nativeCloseStarted = new CountDownLatch(1);
        CountDownLatch allowNativeClose = new CountDownLatch(1);
        doAnswer(invocation -> {
            nativeCloseStarted.countDown();
            allowNativeClose.await();
            return null;
        }).when(fixture.raw).close();
        if (failure != null) {
            doThrow(failure).doNothing().when(fixture.closeOp).accept(true);
        }
        FutureTask<Void> first = new FutureTask<>(() -> {
            fixture.iterator.close();
            return null;
        });
        FutureTask<Void> second = new FutureTask<>(() -> {
            fixture.iterator.close();
            return null;
        });
        Thread firstThread = new Thread(first, "test-first-native-close");
        Thread secondThread = new Thread(second, "test-second-native-close");
        firstThread.setDaemon(true);
        secondThread.setDaemon(true);
        try {
            firstThread.start();
            assertTrue(nativeCloseStarted.await(1, TimeUnit.SECONDS));
            secondThread.start();
            awaitBlockedClose(secondThread);
            assertFalse("second close cannot claim release before native close finishes", second.isDone());
            verify(fixture.reference, never()).release();
            allowNativeClose.countDown();
            if (failure == null) {
                first.get(2, TimeUnit.SECONDS);
                second.get(2, TimeUnit.SECONDS);
                verify(fixture.reference).release();
            } else {
                assertSame(failure, assertThrows(ExecutionException.class,
                        () -> first.get(2, TimeUnit.SECONDS)).getCause());
                assertSame(failure, assertThrows(ExecutionException.class,
                        () -> second.get(2, TimeUnit.SECONDS)).getCause());
                verify(fixture.reference, never()).release();
            }
            verify(fixture.raw).close();
            verify(fixture.closeOp).accept(true);
        } finally {
            allowNativeClose.countDown();
            firstThread.join(1000);
            secondThread.join(1000);
        }
    }

    private static void awaitBlockedClose(Thread thread) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
        while (System.nanoTime() < deadline) {
            if (thread.getState() == Thread.State.BLOCKED) {
                return;
            }
            if (thread.getState() == Thread.State.TERMINATED) {
                break;
            }
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
        }
        fail("concurrent close must wait for the first resource release");
    }

    private static final class Fixture {

        private final RocksIterator raw = mock(RocksIterator.class);
        private final RocksDBSession.RefCounter reference = mock(RocksDBSession.RefCounter.class);
        @SuppressWarnings("unchecked")
        private final Consumer<Boolean> closeOp = mock(Consumer.class);
        private final RocksDBScanIterator<?> iterator;

        private Fixture() {
            when(this.raw.isOwningHandle()).thenReturn(true);
            this.iterator = new RocksDBScanIterator<>(this.raw, null, null,
                    ScanIterator.Trait.SCAN_ANY, this.reference, this.closeOp);
        }
    }
}
