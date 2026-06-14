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

package org.apache.hugegraph.unit.core;

import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.id.IdGenerator;
import org.apache.hugegraph.backend.page.IdHolder.PagingIdHolder;
import org.apache.hugegraph.backend.page.IdHolderList;
import org.apache.hugegraph.backend.page.PageIds;
import org.apache.hugegraph.backend.page.PageState;
import org.apache.hugegraph.backend.page.QueryList;
import org.apache.hugegraph.backend.query.ConditionQuery;
import org.apache.hugegraph.backend.query.Query;
import org.apache.hugegraph.backend.query.QueryResults;
import org.apache.hugegraph.iterator.CIter;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.type.HugeType;
import org.junit.Test;

public class IdHolderTest {

    @Test
    public void testPagingHolderStopsShortPageByDefault() {
        ConditionQuery query = new ConditionQuery(HugeType.SECONDARY_INDEX);
        query.page("");

        AtomicInteger calls = new AtomicInteger();
        PagingIdHolder holder = new PagingIdHolder(query, q -> {
            calls.incrementAndGet();
            return new PageIds(Collections.singleton(IdGenerator.of(1L)),
                               new PageState(new byte[]{1}, 0, 1));
        });

        PageIds first = holder.fetchNext("", 2L);
        Assert.assertEquals(1, first.ids().size());

        PageIds second = holder.fetchNext(first.page(), 2L);
        Assert.assertTrue(second.empty());
        Assert.assertEquals(1, calls.get());
    }

    @Test
    public void testOrderedPagingHolderStopsShortPageByDefault() {
        ConditionQuery query = new ConditionQuery(HugeType.SECONDARY_INDEX);
        query.page("");

        AtomicInteger calls = new AtomicInteger();
        PagingIdHolder holder = new PagingIdHolder(query, q -> {
            calls.incrementAndGet();
            return new PageIds(Collections.singleton(IdGenerator.of(1L)),
                               new PageState(new byte[]{1}, 0, 1));
        }, true);

        Assert.assertTrue(holder.keepOrder());

        PageIds first = holder.fetchNext("", 2L);
        Assert.assertEquals(1, first.ids().size());

        PageIds second = holder.fetchNext(first.page(), 2L);
        Assert.assertTrue(second.empty());
        Assert.assertEquals(1, calls.get());
    }

    @Test
    public void testPagingHolderCanExplicitlyContinueShortPage() {
        ConditionQuery query = new ConditionQuery(HugeType.SECONDARY_INDEX);
        query.page("");

        AtomicInteger calls = new AtomicInteger();
        PagingIdHolder holder = new PagingIdHolder(query, q -> {
            int call = calls.incrementAndGet();
            if (call == 1) {
                return new PageIds(Collections.singleton(IdGenerator.of(1L)),
                                   new PageState(new byte[]{1}, 0, 1));
            }
            if (call == 2) {
                return new PageIds(Collections.singleton(IdGenerator.of(2L)),
                                   PageState.EMPTY);
            }
            return PageIds.EMPTY;
        }, true, false, true);

        PageIds first = holder.fetchNext("", 2L);
        Assert.assertEquals(1, first.ids().size());

        PageIds second = holder.fetchNext(first.page(), 2L);
        Assert.assertEquals(1, second.ids().size());
        Assert.assertEquals(2, calls.get());
    }

    @Test
    public void testPagingHolderStopsEmptyPageByDefault() {
        ConditionQuery query = new ConditionQuery(HugeType.SECONDARY_INDEX);
        query.page("");

        AtomicInteger calls = new AtomicInteger();
        PagingIdHolder holder = new PagingIdHolder(query, q -> {
            calls.incrementAndGet();
            return new PageIds(Collections.emptySet(),
                               new PageState(new byte[]{1}, 0, 0));
        });

        PageIds first = holder.fetchNext("", 1L);
        Assert.assertTrue(first.empty());
        Assert.assertNull(first.page());

        PageIds second = holder.fetchNext(first.page(), 1L);
        Assert.assertTrue(second.empty());
        Assert.assertEquals(1, calls.get());
    }

    @Test
    public void testOrderedPagingIndexQueryStopsEmptyPageByDefault() {
        ConditionQuery parent = new ConditionQuery(HugeType.VERTEX);
        parent.page("");

        ConditionQuery indexQuery = new ConditionQuery(HugeType.RANGE_INT_INDEX);
        indexQuery.page("");

        AtomicInteger calls = new AtomicInteger();
        PagingIdHolder holder = new PagingIdHolder(indexQuery, q -> {
            int call = calls.incrementAndGet();
            if (call == 1) {
                return new PageIds(Collections.emptySet(),
                                   new PageState(new byte[]{1}, 0, 0));
            }
            if (call == 2) {
                return new PageIds(Collections.singleton(IdGenerator.of(2L)),
                                   new PageState(PageState.EMPTY_BYTES, 0, 1));
            }
            return PageIds.EMPTY;
        }, true);

        IdHolderList holders = new IdHolderList(true);
        holders.add(holder);

        QueryList<Id> queries = new QueryList<>(parent, q -> {
            return new QueryResults<>(q.ids().iterator(), q);
        });
        queries.add(holders, Query.QUERY_BATCH);

        QueryResults<Id> results = queries.fetch(1);
        Iterator<Id> iterator = results.iterator();
        Assert.assertFalse(iterator.hasNext());
        Assert.assertEquals(1, calls.get());
    }

    @Test
    public void testPagingIndexQueryCanExplicitlyContinueEmptyPage() {
        ConditionQuery parent = new ConditionQuery(HugeType.VERTEX);
        parent.page("");

        ConditionQuery indexQuery = new ConditionQuery(HugeType.RANGE_INT_INDEX);
        indexQuery.page("");

        AtomicInteger calls = new AtomicInteger();
        PagingIdHolder holder = new PagingIdHolder(indexQuery, q -> {
            int call = calls.incrementAndGet();
            if (call == 1) {
                return new PageIds(Collections.emptySet(),
                                   new PageState(new byte[]{1}, 0, 0));
            }
            if (call == 2) {
                return new PageIds(Collections.singleton(IdGenerator.of(2L)),
                                   new PageState(PageState.EMPTY_BYTES, 0, 1));
            }
            return PageIds.EMPTY;
        }, true, true, true);

        IdHolderList holders = new IdHolderList(true);
        holders.add(holder);

        QueryList<Id> queries = new QueryList<>(parent, q -> {
            return new QueryResults<>(q.ids().iterator(), q);
        });
        queries.add(holders, Query.QUERY_BATCH);

        QueryResults<Id> results = queries.fetch(1);
        Iterator<Id> iterator = results.iterator();
        Assert.assertTrue(iterator.hasNext());
        Assert.assertEquals(IdGenerator.of(2L), iterator.next());
        Assert.assertFalse(iterator.hasNext());
        Assert.assertEquals(2, calls.get());
    }

    @Test
    public void testOptimizedQueryStopsEmptyPageWithPageState() {
        ConditionQuery parent = new ConditionQuery(HugeType.VERTEX);
        parent.page("");

        AtomicInteger calls = new AtomicInteger();
        QueryList<Id> queries = new QueryList<>(parent, q -> {
            calls.incrementAndGet();
            return new QueryResults<>(
                    new EmptyPageIterator<>(new PageState(new byte[]{1}, 0, 0)),
                    q);
        });
        queries.add(new ConditionQuery(HugeType.VERTEX));

        QueryResults<Id> results = queries.fetch(1);
        Iterator<Id> iterator = results.iterator();
        Assert.assertFalse(iterator.hasNext());
        Assert.assertEquals(1, calls.get());
    }

    private static class EmptyPageIterator<T> implements CIter<T> {

        private final PageState pageState;

        EmptyPageIterator(PageState pageState) {
            this.pageState = pageState;
        }

        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public T next() {
            throw new NoSuchElementException();
        }

        @Override
        public Object metadata(String meta, Object... args) {
            return this.pageState;
        }

        @Override
        public void close() throws Exception {
            // pass
        }
    }
}
