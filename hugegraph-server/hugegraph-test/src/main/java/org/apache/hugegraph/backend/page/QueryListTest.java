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

package org.apache.hugegraph.backend.page;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.id.IdGenerator;
import org.apache.hugegraph.backend.page.IdHolder.BatchIdHolder;
import org.apache.hugegraph.backend.page.IdHolder.FixedIdHolder;
import org.apache.hugegraph.backend.page.IdHolder.PagingIdHolder;
import org.apache.hugegraph.backend.query.ConditionQuery.OptimizedType;
import org.apache.hugegraph.backend.query.ConditionQuery;
import org.apache.hugegraph.backend.query.QueryResults;
import org.apache.hugegraph.backend.serializer.TextBackendEntry;
import org.apache.hugegraph.backend.store.BackendEntry;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.type.Idfiable;
import org.apache.hugegraph.util.InsertionOrderUtil;
import org.junit.Test;

public class QueryListTest {

    @Test
    public void testPagingRetainsOnlyCurrentQuery() throws Exception {
        ConditionQuery query = new ConditionQuery(HugeType.VERTEX);
        query.page("");
        query.limit(10000L);
        int[] pages = {0};
        IdHolderList holders = new IdHolderList(true);
        holders.add(new PagingIdHolder(query, page -> {
            long id = ++pages[0];
            return new PageIds(ids(id), new PageState(new byte[]{1}, 0, 1));
        }));
        QueryList<Item> list = new QueryList<>(query, batch -> new QueryResults<>(
                Collections.singletonList(new Item(batch.ids().iterator().next())).iterator(),
                batch));
        list.add(holders, 1L);
        QueryResults<Item> results = list.fetch(1);
        Iterator<Item> iterator = results.iterator();
        for (long id = 1L; id <= 10000L; id++) {
            Assert.assertEquals(IdGenerator.of(id), iterator.next().id());
            Assert.assertEquals(1, results.queries().size());
            Assert.assertEquals(Collections.singletonList(IdGenerator.of(id)),
                                results.queries().get(0).ids());
        }
        Assert.assertEquals(10000, pages[0]);
        ((AutoCloseable) iterator).close();
        Assert.assertTrue(results.queries().isEmpty());
    }

    @Test
    public void testOrderingDoesNotFetchNextIndexPage() {
        ConditionQuery query = new ConditionQuery(HugeType.VERTEX);
        query.page("");
        query.limit(4);
        int[] pageFetches = {0};
        int[] backendFetches = {0};
        IdHolderList holders = new IdHolderList(true);
        holders.add(new PagingIdHolder(query, page -> {
            Assert.assertEquals(pageFetches[0] == 0 ? "" :
                                new PageState(new byte[]{1}, 0, 2).toString(), page.page());
            Assert.assertEquals(2L, page.limit());
            int offset = pageFetches[0]++ * 2;
            Set<Id> ids = InsertionOrderUtil.newSet();
            ids.add(IdGenerator.of(offset + 2L));
            ids.add(IdGenerator.of(offset + 1L));
            return new PageIds(ids, new PageState(
                    pageFetches[0] == 1 ? new byte[]{1} : new byte[0], 0, 2));
        }, true));
        QueryList<Item> queries = new QueryList<>(query, batch -> {
            backendFetches[0]++;
            List<Item> items = new ArrayList<>();
            batch.ids().stream().sorted().forEach(id -> items.add(new Item(id)));
            return new QueryResults<>(items.iterator(), batch);
        });
        queries.add(holders, 2);
        QueryResults<Item> results = queries.fetch(2);
        Iterator<Item> ordered = results.<Item>keepInputOrderIfNeeded().iterator();
        Assert.assertEquals(IdGenerator.of(2L), ordered.next().id());
        Assert.assertEquals(IdGenerator.of(1L), ordered.next().id());
        Assert.assertEquals(1, pageFetches[0]);
        Assert.assertEquals(1, backendFetches[0]);
        PageInfo nextPage = PageInfo.fromString(PageInfo.pageInfo(ordered));
        Assert.assertEquals(0, nextPage.offset());
        Assert.assertEquals(new PageState(new byte[]{1}, 0, 2).toString(), nextPage.page());
        Assert.assertEquals(1, pageFetches[0]);
        Assert.assertEquals(IdGenerator.of(4L), ordered.next().id());
        Assert.assertEquals(IdGenerator.of(3L), ordered.next().id());
        Assert.assertFalse(ordered.hasNext());
        Assert.assertEquals(2, pageFetches[0]);
        Assert.assertEquals(2, backendFetches[0]);
        Assert.assertNull(PageInfo.pageInfo(ordered));
    }

    @Test
    public void testLimitStopsAtPageBoundaryAndRetainsCursor() {
        ConditionQuery query = new ConditionQuery(HugeType.VERTEX);
        query.page("");
        query.limit(1L);
        int[] fetches = {0};
        PageState next = new PageState(new byte[]{7}, 0, 1);
        IdHolderList holders = new IdHolderList(true);
        holders.add(new PagingIdHolder(query, page -> {
            Assert.assertEquals("", page.page());
            Assert.assertEquals(1L, page.limit());
            fetches[0]++;
            return new PageIds(ids(1L), next);
        }));
        QueryList<Item> list = new QueryList<>(query, batch ->
                new QueryResults<>(Collections.singletonList(new Item(IdGenerator.of(1L)))
                                              .iterator(), batch));
        list.add(holders, 2L);
        Iterator<Item> values = list.fetch(2).iterator();
        Assert.assertEquals(IdGenerator.of(1L), values.next().id());
        Assert.assertFalse(values.hasNext());
        Assert.assertEquals(1, fetches[0]);
        Assert.assertEquals(next.toString(), PageInfo.fromString(PageInfo.pageInfo(values)).page());
    }

    @Test
    public void testEmptyHolderAdvancesToNextHolder() {
        ConditionQuery query = new ConditionQuery(HugeType.VERTEX);
        query.page("");
        query.limit(2L);
        int[] fetches = {0};
        IdHolderList holders = new IdHolderList(true);
        holders.add(new PagingIdHolder(query, page -> PageIds.EMPTY));
        holders.add(new PagingIdHolder(query, page -> {
            Assert.assertEquals("", page.page());
            fetches[0]++;
            return new PageIds(ids(1L), PageState.EMPTY);
        }));
        QueryList<Item> list = new QueryList<>(query, batch ->
                new QueryResults<>(Collections.singletonList(new Item(IdGenerator.of(1L)))
                                              .iterator(), batch));
        list.add(holders, 2L);
        Iterator<Item> values = list.fetch(2).iterator();
        Assert.assertEquals(IdGenerator.of(1L), values.next().id());
        Assert.assertFalse(values.hasNext());
        Assert.assertNull(PageInfo.pageInfo(values));
        Assert.assertEquals(1, fetches[0]);
    }

    @Test
    public void testCopiedSearchFiltersAndEmptyHoldersStayLocal() {
        ConditionQuery root = new ConditionQuery(HugeType.VERTEX);
        root.optimized(OptimizedType.INDEX_FILTER);
        ConditionQuery first = root.copy();
        ConditionQuery second = root.copy();
        ConditionQuery.ResultsFilter firstFilter = element -> true;
        ConditionQuery.ResultsFilter secondFilter = element -> false;
        first.registerResultsFilter(firstFilter);
        second.registerResultsFilter(secondFilter);
        IdHolderList holders = new IdHolderList(false);
        holders.add(new FixedIdHolder(first.copy(), ids(2L, 1L)));
        holders.add(new FixedIdHolder(first.copy(), Collections.emptySet()));
        holders.add(new FixedIdHolder(second.copy(), ids(4L, 3L)));
        int[] fetched = {0};
        QueryList<Item> list = new QueryList<>(root, query -> {
            Assert.assertEquals(HugeType.VERTEX, query.resultType());
            fetched[0]++;
            List<Item> values = new ArrayList<>();
            query.ids().forEach(id -> values.add(new Item(id)));
            return new QueryResults<>(values.iterator(), query);
        });
        list.add(holders, 2L);
        List<Id> accepted = new ArrayList<>();
        QueryResults<Item> results = list.fetch(2).filter((context, item) -> {
            boolean firstBatch = item.id().asLong() <= 2L;
            Assert.assertSame(firstBatch ? firstFilter : secondFilter, context.resultsFilter());
            Assert.assertEquals(firstBatch ? 1 : 2, fetched[0]);
            Assert.assertSame(root, context.matchQuery());
            return item.id().asLong() % 2 == 0;
        });
        results.iterator().forEachRemaining(item -> accepted.add(item.id()));
        Assert.assertEquals(Arrays.asList(IdGenerator.of(2L), IdGenerator.of(4L)), accepted);
        Assert.assertEquals(2, fetched[0]);
    }

    @Test
    public void testMultipleIndexHoldersFetchOneIdBatchAtATime() {
        ConditionQuery root = new ConditionQuery(HugeType.VERTEX);
        IdHolderList holders = new IdHolderList(false);
        int[] idFetches = {0};
        for (long start : new long[]{1L, 5L}) {
            List<BackendEntry> entries = new ArrayList<>();
            for (long id = start; id < start + 4L; id++) {
                entries.add(new TextBackendEntry(HugeType.VERTEX, IdGenerator.of(id)));
            }
            Iterator<BackendEntry> source = entries.iterator();
            ConditionQuery index = new ConditionQuery(HugeType.SECONDARY_INDEX, root);
            holders.add(new BatchIdHolder(index, source, size -> {
                idFetches[0]++;
                Set<Id> ids = InsertionOrderUtil.newSet();
                while (ids.size() < size && source.hasNext()) {
                    ids.add(source.next().id());
                }
                return ids;
            }, true));
        }
        QueryList<Item> list = new QueryList<>(root, query -> {
            Assert.assertEquals(HugeType.VERTEX, query.resultType());
            List<Item> values = new ArrayList<>();
            query.ids().forEach(id -> values.add(0, new Item(id)));
            return new QueryResults<>(values.iterator(), query);
        });
        list.add(holders, 2L);
        Iterator<Item> values = list.fetch(2).<Item>keepInputOrderIfNeeded().iterator();
        for (long id = 1L; id <= 8L; id++) {
            Assert.assertEquals(IdGenerator.of(id), values.next().id());
            Assert.assertEquals((int) ((id + 1L) / 2L), idFetches[0]);
        }
        Assert.assertFalse(values.hasNext());
    }

    private static Set<Id> ids(Long... values) {
        Set<Id> ids = InsertionOrderUtil.newSet();
        for (long value : values) {
            ids.add(IdGenerator.of(value));
        }
        return ids;
    }

    private static final class Item implements Idfiable {

        private final Id id;

        private Item(Id id) {
            this.id = id;
        }

        @Override
        public Id id() {
            return this.id;
        }
    }
}
