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

package org.apache.hugegraph.unit.iterator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.junit.Test;

import org.apache.hugegraph.iterator.BatchMapperIterator;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.unit.BaseUnitTest;
import org.apache.hugegraph.unit.iterator.ExtendableIteratorTest.CloseableItor;
import com.google.common.collect.ImmutableList;

@SuppressWarnings("resource")
public class BatchMapperIteratorTest extends BaseUnitTest {

    private static final Iterator<Integer> EMPTY = Collections.emptyIterator();

    private static final List<Integer> DATA1 = ImmutableList.of(1);
    private static final List<Integer> DATA2 = ImmutableList.of(2, 3);
    private static final List<Integer> DATA3 = ImmutableList.of(4, 5, 6);

    private static final Function<List<Integer>, Iterator<Integer>> MAPPER =
                         batch -> batch.iterator();

    @Test
    public void testBatchMapper() {
        Iterator<Integer> results;

        results = new BatchMapperIterator<>(1, DATA1.iterator(), MAPPER);
        Assert.assertEquals(ImmutableList.of(1), ImmutableList.copyOf(results));

        results = new BatchMapperIterator<>(1, DATA2.iterator(), MAPPER);
        Assert.assertEquals(ImmutableList.of(2, 3),
                            ImmutableList.copyOf(results));

        results = new BatchMapperIterator<>(1, DATA3.iterator(), MAPPER);
        Assert.assertEquals(ImmutableList.of(4, 5, 6),
                            ImmutableList.copyOf(results));
    }

    @Test
    public void testBatch() {
        Iterator<Integer> results;

        results = new BatchMapperIterator<>(2, DATA2.iterator(), MAPPER);
        Assert.assertEquals(ImmutableList.of(2, 3),
                            ImmutableList.copyOf(results));

        results = new BatchMapperIterator<>(2, DATA3.iterator(), MAPPER);
        Assert.assertEquals(ImmutableList.of(4, 5, 6),
                            ImmutableList.copyOf(results));

        results = new BatchMapperIterator<>(3, DATA3.iterator(), MAPPER);
        Assert.assertEquals(ImmutableList.of(4, 5, 6),
                            ImmutableList.copyOf(results));

        results = new BatchMapperIterator<>(4, DATA3.iterator(), MAPPER);
        Assert.assertEquals(ImmutableList.of(4, 5, 6),
                            ImmutableList.copyOf(results));
    }

    @Test
    public void testInvalidBatch() {
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            new BatchMapperIterator<>(0, DATA1.iterator(), MAPPER);
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            new BatchMapperIterator<>(-1, DATA1.iterator(), MAPPER);
        });
    }

    @Test
    public void testHasNext() {
        Iterator<Integer> results;

        results = new BatchMapperIterator<>(1, EMPTY, MAPPER);
        Assert.assertFalse(results.hasNext());

        results = new BatchMapperIterator<>(1, DATA1.iterator(), MAPPER);
        Assert.assertTrue(results.hasNext());
    }

    @Test
    public void testHasNextAndNextWithMultiTimes() {
        Iterator<Integer> results;

        results = new BatchMapperIterator<>(1, DATA3.iterator(), MAPPER);

        for (int i = 0; i < 5; i++) {
            Assert.assertTrue(results.hasNext());
        }

        for (int i = 0; i < 3; i++) {
            results.next();
        }

        Assert.assertFalse(results.hasNext());
        Assert.assertFalse(results.hasNext());

        Assert.assertThrows(NoSuchElementException.class, results::next);
        Assert.assertThrows(NoSuchElementException.class, results::next);
    }

    @Test
    public void testNext() {
        Iterator<Integer> results;

        results = new BatchMapperIterator<>(1, DATA1.iterator(), MAPPER);
        // Call next() without hasNext()
        Assert.assertEquals(1, results.next());

        results = new BatchMapperIterator<>(1, DATA3.iterator(), MAPPER);
        Assert.assertEquals(4, results.next());
        Assert.assertEquals(5, results.next());
        Assert.assertEquals(6, results.next());

        results = new BatchMapperIterator<>(2, DATA3.iterator(), MAPPER);
        Assert.assertEquals(4, results.next());
        Assert.assertEquals(5, results.next());
        Assert.assertEquals(6, results.next());
    }

    @Test
    public void testNextWithMultiTimes() {
        Iterator<Integer> results;

        results = new BatchMapperIterator<>(1, DATA2.iterator(), MAPPER);

        for (int i = 0; i < 2; i++) {
            results.next();
        }
        Assert.assertThrows(NoSuchElementException.class, results::next);
    }

    @Test
    public void testMapperWithBatch() {
        Iterator<Integer> results;

        results = new BatchMapperIterator<>(1, DATA3.iterator(), batch -> {
            Assert.assertEquals(1, batch.size());
            return batch.iterator();
        });
        Assert.assertEquals(4, results.next());
        Assert.assertEquals(5, results.next());
        Assert.assertEquals(6, results.next());
        Assert.assertFalse(results.hasNext());

        results = new BatchMapperIterator<>(2, DATA3.iterator(), batch -> {
            if (batch.size() == 1) {
                Assert.assertEquals(6, batch.get(0));
            } else {
                Assert.assertEquals(2, batch.size());
            }
            return batch.iterator();
        });
        Assert.assertEquals(4, results.next());
        Assert.assertEquals(5, results.next());
        Assert.assertEquals(6, results.next());
        Assert.assertFalse(results.hasNext());

        results = new BatchMapperIterator<>(3, DATA3.iterator(), batch -> {
            Assert.assertEquals(3, batch.size());
            return batch.iterator();
        });
        Assert.assertEquals(4, results.next());
        Assert.assertEquals(5, results.next());
        Assert.assertEquals(6, results.next());
        Assert.assertFalse(results.hasNext());

        results = new BatchMapperIterator<>(4, DATA3.iterator(), batch -> {
            Assert.assertEquals(3, batch.size());
            return batch.iterator();
        });
        Assert.assertEquals(4, results.next());
        Assert.assertEquals(5, results.next());
        Assert.assertEquals(6, results.next());
        Assert.assertFalse(results.hasNext());
    }

    @Test
    public void testMapperThenReturn2X() {
        Iterator<Integer> results;

        results = new BatchMapperIterator<>(1, DATA3.iterator(), batch -> {
            List<Integer> list = new ArrayList<>();
            for (int i : batch) {
                list.add(2 * i);
            }
            return list.iterator();
        });

        Assert.assertEquals(8, results.next());
        Assert.assertEquals(10, results.next());
        Assert.assertEquals(12, results.next());
        Assert.assertFalse(results.hasNext());
    }

    @Test
    public void testMapperReturnNullThenHasNext() {
        Iterator<Integer> results;

        results = new BatchMapperIterator<>(1, DATA3.iterator(), batch -> {
            return null;
        });
        Assert.assertFalse(results.hasNext());
        Assert.assertFalse(results.hasNext());

        AtomicInteger count1 = new AtomicInteger(0);
        results = new BatchMapperIterator<>(1, DATA3.iterator(), batch -> {
            if (count1.incrementAndGet() == 1) {
                return null;
            }
            return batch.iterator();
        });
        Assert.assertTrue(results.hasNext());
        Assert.assertEquals(5, results.next());
        Assert.assertEquals(6, results.next());
        Assert.assertFalse(results.hasNext());

        AtomicInteger count2 = new AtomicInteger(0);
        results = new BatchMapperIterator<>(1, DATA3.iterator(), batch -> {
            if (count2.incrementAndGet() == 2) {
                return null;
            }
            return batch.iterator();
        });
        Assert.assertTrue(results.hasNext());
        Assert.assertEquals(4, results.next());
        Assert.assertEquals(6, results.next());
        Assert.assertFalse(results.hasNext());

        AtomicInteger count3 = new AtomicInteger(0);
        results = new BatchMapperIterator<>(1, DATA3.iterator(), batch -> {
            if (count3.incrementAndGet() == 3) {
                return null;
            }
            return batch.iterator();
        });
        Assert.assertTrue(results.hasNext());
        Assert.assertEquals(4, results.next());
        Assert.assertEquals(5, results.next());
        Assert.assertFalse(results.hasNext());
    }

    @Test
    public void testMapperReturnNullThenNext() {
        Iterator<Integer> results;

        results = new BatchMapperIterator<>(1, DATA3.iterator(), batch -> {
            return null;
        });
        Assert.assertThrows(NoSuchElementException.class, results::next);
        Assert.assertThrows(NoSuchElementException.class, results::next);
    }

    @Test
    public void testMapperReturnEmptyThenHasNext() {
        Iterator<Integer> results;

        results = new BatchMapperIterator<>(1, DATA3.iterator(), batch -> {
            return Collections.emptyIterator();
        });
        Assert.assertFalse(results.hasNext());
        Assert.assertFalse(results.hasNext());

        AtomicInteger count1 = new AtomicInteger(0);
        results = new BatchMapperIterator<>(1, DATA3.iterator(), batch -> {
            if (count1.incrementAndGet() == 1) {
                return Collections.emptyIterator();
            }
            return batch.iterator();
        });
        Assert.assertTrue(results.hasNext());
        Assert.assertEquals(5, results.next());
        Assert.assertEquals(6, results.next());
        Assert.assertFalse(results.hasNext());

        AtomicInteger count2 = new AtomicInteger(0);
        results = new BatchMapperIterator<>(1, DATA3.iterator(), batch -> {
            if (count2.incrementAndGet() == 2) {
                return Collections.emptyIterator();
            }
            return batch.iterator();
        });
        Assert.assertTrue(results.hasNext());
        Assert.assertEquals(4, results.next());
        Assert.assertEquals(6, results.next());
        Assert.assertFalse(results.hasNext());

        AtomicInteger count3 = new AtomicInteger(0);
        results = new BatchMapperIterator<>(1, DATA3.iterator(), batch -> {
            if (count3.incrementAndGet() == 3) {
                return Collections.emptyIterator();
            }
            return batch.iterator();
        });
        Assert.assertTrue(results.hasNext());
        Assert.assertEquals(4, results.next());
        Assert.assertEquals(5, results.next());
        Assert.assertFalse(results.hasNext());
    }

    @Test
    public void testClose() throws Exception {
        CloseableItor<Integer> vals = new CloseableItor<>(DATA1.iterator());

        Iterator<Integer> results = new BatchMapperIterator<>(1, vals, MAPPER);

        Assert.assertFalse(vals.closed());
        ((BatchMapperIterator<?, ?>) results).close();
        Assert.assertTrue(vals.closed());
    }

    @Test
    public void testCloseClosesCurrentBatchIterator() throws Exception {
        CloseableItor<Integer> vals = new CloseableItor<>(DATA3.iterator());
        List<CloseableItor<Integer>> batches = new ArrayList<>();

        BatchMapperIterator<Integer, Integer> results;
        results = new BatchMapperIterator<>(3, vals, batch -> {
            CloseableItor<Integer> itor = new CloseableItor<>(batch.iterator());
            batches.add(itor);
            return itor;
        });

        Assert.assertEquals(4, results.next());
        Assert.assertEquals(1, batches.size());
        Assert.assertFalse(batches.get(0).closed());

        results.close();
        Assert.assertTrue(batches.get(0).closed());
        Assert.assertTrue(vals.closed());
    }

    @Test
    public void testBatchCloseFailureKeepsCause() {
        Exception failure = new Exception("Controlled close failure");

        BatchMapperIterator<Integer, Integer> results;
        results = new BatchMapperIterator<>(1, DATA1.iterator(), batch -> {
            return new CloseableItor<>(batch.iterator(), failure);
        });

        Assert.assertEquals(1, results.next());
        // Exhausting the batch closes it, and its close() fails
        Throwable exception = Assert.assertThrows(IllegalStateException.class, () -> {
            results.hasNext();
        });
        Assert.assertSame(failure, exception.getCause());
    }

    @Test
    public void testCloseWithFailingBatchKeepsCause() {
        Exception failure = new Exception("Controlled close failure");
        CloseableItor<Integer> origin = new CloseableItor<>(DATA2.iterator());

        BatchMapperIterator<Integer, Integer> results;
        results = new BatchMapperIterator<>(2, origin, batch -> {
            return new CloseableItor<>(batch.iterator(), failure);
        });

        Assert.assertEquals(2, results.next());
        Throwable exception = Assert.assertThrows(IllegalStateException.class, () -> {
            results.close();
        });
        Assert.assertSame(failure, exception.getCause());
        Assert.assertTrue(origin.closed());
    }

    @Test
    public void testClosePreservesBothFailures() {
        Exception batchFailure = new Exception("Batch close failure");
        Exception originFailure = new Exception("Origin close failure");
        CloseableItor<Integer> origin = new CloseableItor<>(DATA2.iterator(), originFailure);
        BatchMapperIterator<Integer, Integer> results =
                new BatchMapperIterator<>(2, origin,
                                          batch -> new CloseableItor<>(batch.iterator(),
                                                                       batchFailure));

        Assert.assertEquals(2, results.next());
        Throwable exception = Assert.assertThrows(IllegalStateException.class, () -> {
            results.close();
        });
        Assert.assertSame(batchFailure, exception.getCause());
        Assert.assertTrue(origin.closed());
        Assert.assertEquals(1, exception.getSuppressed().length);
        Assert.assertSame(originFailure, exception.getSuppressed()[0]);
    }
}
