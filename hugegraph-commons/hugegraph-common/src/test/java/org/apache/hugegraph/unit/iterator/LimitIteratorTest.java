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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.junit.Test;

import org.apache.hugegraph.iterator.LimitIterator;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.unit.BaseUnitTest;
import org.apache.hugegraph.unit.iterator.ExtendableIteratorTest.CloseableItor;
import com.google.common.collect.ImmutableList;

@SuppressWarnings("resource")
public class LimitIteratorTest extends BaseUnitTest {

    private static final List<Integer> DATA = ImmutableList.of(1, 2, 3, 4);

    @Test
    public void testLimit() {
        AtomicInteger callbackCount = new AtomicInteger(0);

        Iterator<Integer> values = DATA.iterator();

        int limit = 2;
        Function<Integer, Boolean> filter = value -> {
            return callbackCount.incrementAndGet() > limit;
        };

        Iterator<Integer> results = new LimitIterator<>(values, filter);

        List<Integer> actual = new ArrayList<>();
        while (results.hasNext()) {
            actual.add(results.next());
        }

        Assert.assertEquals(3, callbackCount.get());
        Assert.assertEquals(ImmutableList.of(1, 2), actual);
    }

    @Test
    public void testHasNext() {
        Iterator<Integer> vals = DATA.iterator();

        Iterator<Integer> results = new LimitIterator<>(vals, val -> false);
        Assert.assertTrue(results.hasNext());
    }

    @Test
    public void testHasNextWithMultiTimesWithoutAnyResult() {
        Iterator<Integer> vals = DATA.iterator();

        Iterator<Integer> results = new LimitIterator<>(vals, val -> true);
        Assert.assertFalse(results.hasNext());
        Assert.assertFalse(results.hasNext());
    }

    @Test
    public void testHasNextAndNextWithMultiTimes() {
        Iterator<Integer> vals = DATA.iterator();

        Iterator<Integer> results = new LimitIterator<>(vals, val -> false);

        for (int i = 0; i < 5; i++) {
            Assert.assertTrue(results.hasNext());
        }

        for (int i = 0; i < 4; i++) {
            results.next();
        }

        Assert.assertFalse(results.hasNext());
        Assert.assertFalse(results.hasNext());

        Assert.assertThrows(NoSuchElementException.class, results::next);
        Assert.assertThrows(NoSuchElementException.class, results::next);

        Iterator<Integer> results2 = new LimitIterator<>(vals, val -> false);
        Assert.assertFalse(results2.hasNext());
    }

    @Test
    public void testNext() {
        Iterator<Integer> vals = DATA.iterator();

        Iterator<Integer> results = new LimitIterator<>(vals, val -> false);
        // Call next() without testNext()
        results.next();
    }

    @Test
    public void testNextWithMultiTimes() {
        Iterator<Integer> vals = DATA.iterator();

        Iterator<Integer> results = new LimitIterator<>(vals, val -> false);
        for (int i = 0; i < 4; i++) {
            results.next();
        }
        Assert.assertThrows(NoSuchElementException.class, results::next);
    }

    @Test
    public void testNextWithMultiTimesWithoutAnyResult() {
        Iterator<Integer> vals = DATA.iterator();

        Iterator<Integer> results = new LimitIterator<>(vals, val -> true);
        Assert.assertThrows(NoSuchElementException.class, results::next);
        Assert.assertThrows(NoSuchElementException.class, results::next);
    }

    @Test
    public void testNextWithOriginIteratorReturnNullElem() {
        List<Integer> list = new ArrayList<>();
        list.add(1);
        list.add(null);
        list.add(3);
        Iterator<Integer> vals = list.iterator();

        AtomicInteger callbackCount = new AtomicInteger(0);

        Iterator<Integer> results = new LimitIterator<>(vals, val -> {
            callbackCount.incrementAndGet();
            return false;
        });
        Assert.assertTrue(results.hasNext());
        for (int i = 0; i < 2; i++) {
            results.next();
        }
        Assert.assertFalse(results.hasNext());
        Assert.assertEquals(2, callbackCount.get());
    }

    @Test
    public void testRemove() {
        List<Integer> list = new ArrayList<>(DATA);

        Iterator<Integer> results = new LimitIterator<>(list.iterator(),
                                                        val -> false);

        Assert.assertEquals(ImmutableList.of(1, 2, 3, 4), list);

        results.next();
        results.next();
        results.remove();

        Assert.assertEquals(ImmutableList.of(1, 3, 4), list);
    }

    @Test
    public void testClose() throws Exception {
        CloseableItor<Integer> vals = new CloseableItor<>(DATA.iterator());

        LimitIterator<Integer> results = new LimitIterator<>(vals,
                                                             val -> true);

        Assert.assertFalse(vals.closed());
        results.close();
        Assert.assertTrue(vals.closed());
    }

    @Test
    public void testCloseOnlyOnceWhenReachLimit() throws Exception {
        AtomicInteger closeCount = new AtomicInteger();
        CloseableItor<Integer> vals =
                new CloseableItor<Integer>(DATA.iterator()) {
            @Override
            public void close() throws Exception {
                closeCount.incrementAndGet();
                super.close();
            }
        };
        LimitIterator<Integer> results = new LimitIterator<>(vals,
                                                             val -> true);

        Assert.assertFalse(results.hasNext());
        results.close();

        Assert.assertEquals(1, closeCount.get());
    }
}
