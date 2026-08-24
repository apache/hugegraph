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
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.junit.Test;

import org.apache.hugegraph.iterator.ExtendableIterator;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.unit.BaseUnitTest;
import com.google.common.collect.ImmutableList;

@SuppressWarnings("resource")
public class ExtendableIteratorTest extends BaseUnitTest {

    private static final List<Integer> DATA1 = ImmutableList.of(1);
    private static final List<Integer> DATA2 = ImmutableList.of(2, 3);
    private static final List<Integer> DATA3 = ImmutableList.of(4, 5, 6);

    @Test
    public void testConcatTwoIterators() {
        Iterator<Integer> results = new ExtendableIterator<>(DATA1.iterator(),
                                                             DATA2.iterator());

        List<Integer> actual = new ArrayList<>();
        while (results.hasNext()) {
            actual.add(results.next());
        }

        Assert.assertEquals(3, actual.size());
        Assert.assertEquals(ImmutableList.of(1, 2, 3), actual);
    }

    @Test
    public void testExtendIterators() {
        ExtendableIterator<Integer> results = new ExtendableIterator<>();
        results.extend(DATA1.iterator())
               .extend(DATA2.iterator())
               .extend(DATA3.iterator());

        List<Integer> actual = new ArrayList<>();
        while (results.hasNext()) {
            actual.add(results.next());
        }

        Assert.assertEquals(6, actual.size());
        Assert.assertEquals(ImmutableList.of(1, 2, 3, 4, 5, 6), actual);
    }

    @Test
    public void testHasNext() {
        Iterator<Integer> results = new ExtendableIterator<>(DATA1.iterator());
        Assert.assertTrue(results.hasNext());
        Assert.assertTrue(results.hasNext());
    }

    @Test
    public void testExtendAfterHasNext() {
        ExtendableIterator<Integer> results = new ExtendableIterator<>(
                                              DATA1.iterator());
        Assert.assertTrue(results.hasNext());
        Assert.assertThrows(IllegalStateException.class, () -> {
            results.extend(DATA2.iterator());
        });
    }

    @Test
    public void testNext() {
        Iterator<Integer> results = new ExtendableIterator<>(DATA1.iterator());
        Assert.assertEquals(1, (int) results.next());
        Assert.assertThrows(NoSuchElementException.class, results::next);
    }

    @Test
    public void testNextWithMultiTimes() {
        Iterator<Integer> results = new ExtendableIterator<>(DATA1.iterator(),
                                                             DATA2.iterator());
        Assert.assertEquals(1, (int) results.next());
        Assert.assertEquals(2, (int) results.next());
        Assert.assertEquals(3, (int) results.next());
        Assert.assertThrows(NoSuchElementException.class, results::next);
    }

    @Test
    public void testHasNextAndNext() {
        Iterator<Integer> results = new ExtendableIterator<>(DATA1.iterator());
        Assert.assertTrue(results.hasNext());
        Assert.assertTrue(results.hasNext());
        Assert.assertEquals(1, (int) results.next());
        Assert.assertFalse(results.hasNext());
        Assert.assertFalse(results.hasNext());
        Assert.assertThrows(NoSuchElementException.class, results::next);
    }

    @Test
    public void testRemove() {
        List<Integer> list1 = new ArrayList<>(DATA1);
        List<Integer> list3 = new ArrayList<>(DATA3);
        Iterator<Integer> results = new ExtendableIterator<>(
                                    list1.iterator(), list3.iterator());

        Assert.assertEquals(ImmutableList.of(1), list1);
        Assert.assertEquals(ImmutableList.of(4, 5, 6), list3);

        results.next();
        results.remove();

        results.next();
        results.next();
        results.remove();

        Assert.assertEquals(0, list1.size());
        Assert.assertEquals(ImmutableList.of(4, 6), list3);
    }

    @Test
    public void testRemoveWithoutResult() {
        Iterator<Integer> results = new ExtendableIterator<>();
        Assert.assertThrows(NoSuchElementException.class, results::remove);

        List<Integer> list = new ArrayList<>();
        Iterator<Integer> results2 = new ExtendableIterator<>(list.iterator());
        Assert.assertThrows(NoSuchElementException.class, results2::remove);
    }

    @Test
    public void testClose() throws Exception {
        CloseableItor<Integer> c1 = new CloseableItor<>(DATA1.iterator());
        CloseableItor<Integer> c2 = new CloseableItor<>(DATA2.iterator());
        CloseableItor<Integer> c3 = new CloseableItor<>(DATA3.iterator());

        ExtendableIterator<Integer> results = new ExtendableIterator<>();
        results.extend(c1).extend(c2).extend(c3);

        Assert.assertFalse(c1.closed());
        Assert.assertFalse(c2.closed());
        Assert.assertFalse(c3.closed());

        results.close();

        Assert.assertTrue(c1.closed());
        Assert.assertTrue(c2.closed());
        Assert.assertTrue(c3.closed());
    }

    @Test
    public void testCloseAllWhenCloseFails() {
        Exception firstFailure = new Exception("first");
        Exception lastFailure = new Exception("last");
        CloseableItor<Integer> c1 = new CloseableItor<>(DATA1.iterator(),
                                                       firstFailure);
        CloseableItor<Integer> c2 = new CloseableItor<>(DATA2.iterator());
        CloseableItor<Integer> c3 = new CloseableItor<>(DATA3.iterator(),
                                                       lastFailure);
        ExtendableIterator<Integer> results = new ExtendableIterator<>();
        results.extend(c1).extend(c2).extend(c3);

        Throwable actual = Assert.assertThrows(
                           Exception.class,
                           (Assert.ThrowableRunnable) results::close);

        Assert.assertSame(firstFailure, actual);
        Assert.assertTrue(c1.closed());
        Assert.assertTrue(c2.closed());
        Assert.assertTrue(c3.closed());
        Assert.assertEquals(1, actual.getSuppressed().length);
        Assert.assertSame(lastFailure, actual.getSuppressed()[0]);
    }

    @Test
    public void testCloseAfterNext1() throws Exception {
        CloseableItor<Integer> c1 = new CloseableItor<>(DATA1.iterator());
        CloseableItor<Integer> c2 = new CloseableItor<>(DATA2.iterator());
        CloseableItor<Integer> c3 = new CloseableItor<>(DATA3.iterator());

        ExtendableIterator<Integer> results = new ExtendableIterator<>();
        results.extend(c1).extend(c2).extend(c3);

        results.next();
        results.hasNext();

        Assert.assertTrue(c1.closed()); // close after iterated
        Assert.assertFalse(c2.closed());
        Assert.assertFalse(c3.closed());

        results.close();

        Assert.assertTrue(c1.closed());
        Assert.assertTrue(c2.closed());
        Assert.assertTrue(c3.closed());
    }

    @Test
    public void testCloseAfterNext3() throws Exception {
        CloseableItor<Integer> c1 = new CloseableItor<>(DATA1.iterator());
        CloseableItor<Integer> c2 = new CloseableItor<>(DATA2.iterator());
        CloseableItor<Integer> c3 = new CloseableItor<>(DATA3.iterator());

        ExtendableIterator<Integer> results = new ExtendableIterator<>();
        results.extend(c1).extend(c2).extend(c3);

        Assert.assertFalse(c1.closed());
        Assert.assertFalse(c2.closed());
        Assert.assertFalse(c3.closed());

        while (results.hasNext()) {
            results.next();
        }

        Assert.assertTrue(c1.closed());
        Assert.assertTrue(c2.closed());
        Assert.assertFalse(c3.closed());

        results.close();

        Assert.assertTrue(c1.closed());
        Assert.assertTrue(c2.closed());
        Assert.assertTrue(c3.closed());
    }

    protected static class CloseableItor<V> implements Iterator<V>,
                                                       AutoCloseable {

        private final Iterator<V> iter;
        private final Exception closeFailure;
        private boolean closed = false;

        public CloseableItor(Iterator<V> iter) {
            this(iter, null);
        }

        public CloseableItor(Iterator<V> iter, Exception closeFailure) {
            this.iter = iter;
            this.closeFailure = closeFailure;
        }

        @Override
        public boolean hasNext() {
            return this.iter.hasNext();
        }

        @Override
        public V next() {
            return this.iter.next();
        }

        @Override
        public void close() throws Exception {
            this.closed  = true;
            if (this.closeFailure != null) {
                throw this.closeFailure;
            }
        }

        public boolean closed() {
            return this.closed;
        }
    }
}
