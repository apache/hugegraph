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

package org.apache.hugegraph.unit.core;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.Consumer;

import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.id.IdGenerator;
import org.apache.hugegraph.backend.query.IdQuery;
import org.apache.hugegraph.backend.query.Query;
import org.apache.hugegraph.backend.query.QueryResults;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.type.Idfiable;
import org.apache.hugegraph.util.InsertionOrderUtil;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

public class QueryResultsTest {

    @Test
    public void testKeepInputOrderForPagingIdQuery() {
        Id id1 = IdGenerator.of(1L);
        Id id2 = IdGenerator.of(2L);
        Query pagingQuery = new Query(HugeType.VERTEX);
        pagingQuery.page("page-1");
        pagingQuery.limit(2L);

        Set<Id> ids = InsertionOrderUtil.newSet();
        ids.add(id2);
        ids.add(id1);

        IdQuery idQuery = new IdQuery(pagingQuery, ids);
        idQuery.mustSortByInput(true);
        QueryResults<TestIdfiable> results = new QueryResults<>(
                Arrays.asList(new TestIdfiable(id1),
                              new TestIdfiable(id2)).iterator(),
                idQuery);

        List<Id> orderedIds = new ArrayList<>();
        results.keepInputOrderIfNeeded(
                Arrays.asList(new TestIdfiable(id1),
                              new TestIdfiable(id2)).iterator())
               .forEachRemaining(item -> orderedIds.add(item.id()));

        Assert.assertEquals(ImmutableList.of(id2, id1), orderedIds);
    }

    @Test
    public void testKeepInputOrderAcrossBatches() {
        List<Long> firstInput = new ArrayList<>();
        List<Long> firstOutput = new ArrayList<>();
        for (long id = 0L; id < Query.QUERY_BATCH; id++) {
            firstInput.add(Query.QUERY_BATCH - id - 1L);
            firstOutput.add(id);
        }
        List<Long> secondInput = ImmutableList.of(
                Query.QUERY_BATCH + 1L, Query.QUERY_BATCH);
        List<Long> secondOutput = ImmutableList.of(
                Query.QUERY_BATCH, Query.QUERY_BATCH + 1L);
        QueryResults<TestIdfiable> first = resultsOf(
                firstInput, firstOutput);
        QueryResults<TestIdfiable> second = resultsOf(
                secondInput, secondOutput);
        QueryResults<TestIdfiable> results = QueryResults.flatMap(
                ImmutableList.of(first, second).iterator(), result -> result);

        List<Id> orderedIds = new ArrayList<>();
        results.keepInputOrderIfNeeded(results.iterator())
               .forEachRemaining(item -> orderedIds.add(item.id()));

        List<Id> expected = new ArrayList<>();
        firstInput.forEach(id -> expected.add(IdGenerator.of(id)));
        secondInput.forEach(id -> expected.add(IdGenerator.of(id)));
        Assert.assertTrue(orderedIds.size() > Query.QUERY_BATCH);
        Assert.assertEquals(expected, orderedIds);
    }

    @Test
    public void testKeepBackendOrderWhenQueryOnlyDescribesPartOfResults() {
        QueryResults<TestIdfiable> results = resultsOf(
                ImmutableList.of(2L, 3L),
                ImmutableList.of(1L, 2L, 3L));

        List<Id> orderedIds = new ArrayList<>();
        results.keepInputOrderIfNeeded(results.iterator())
               .forEachRemaining(item -> orderedIds.add(item.id()));

        Assert.assertEquals(ImmutableList.of(IdGenerator.of(1L),
                                             IdGenerator.of(2L),
                                             IdGenerator.of(3L)),
                            orderedIds);
    }

    @Test
    public void testKeepInputOrderDoesNotDrainFollowingPages() {
        IdQuery firstQuery = queryOf(2L, 1L);
        IdQuery secondQuery = queryOf(4L, 3L);
        @SuppressWarnings("unchecked")
        QueryResults<TestIdfiable>[] holder = new QueryResults[1];
        PagingIterator origin = new PagingIterator(
                ImmutableList.of(new TestIdfiable(IdGenerator.of(1L)),
                                 new TestIdfiable(IdGenerator.of(2L)),
                                 new TestIdfiable(IdGenerator.of(3L)),
                                 new TestIdfiable(IdGenerator.of(4L))),
                2,
                query -> holder[0].setQuery(query),
                ImmutableList.of(firstQuery, secondQuery));
        holder[0] = new QueryResults<>(origin, firstQuery);

        Iterator<TestIdfiable> ordered =
                holder[0].keepInputOrderIfNeeded(holder[0].iterator());

        Assert.assertEquals(IdGenerator.of(2L), ordered.next().id());
        Assert.assertEquals(IdGenerator.of(1L), ordered.next().id());
        Assert.assertEquals(2, origin.consumed());
        Assert.assertEquals(IdGenerator.of(4L), ordered.next().id());
        Assert.assertEquals(IdGenerator.of(3L), ordered.next().id());
        Assert.assertFalse(ordered.hasNext());
    }

    private static QueryResults<TestIdfiable> resultsOf(List<Long> input,
                                                         List<Long> output) {
        List<TestIdfiable> results = new ArrayList<>(output.size());
        for (Long id : output) {
            results.add(new TestIdfiable(IdGenerator.of(id)));
        }
        return new QueryResults<>(results.iterator(), queryOf(input));
    }

    private static IdQuery queryOf(Long... ids) {
        return queryOf(Arrays.asList(ids));
    }

    private static IdQuery queryOf(List<Long> ids) {
        Set<Id> queryIds = InsertionOrderUtil.newSet();
        for (Long id : ids) {
            queryIds.add(IdGenerator.of(id));
        }
        IdQuery query = new IdQuery(new Query(HugeType.VERTEX), queryIds);
        query.mustSortByInput(true);
        return query;
    }

    private static final class TestIdfiable implements Idfiable {

        private final Id id;

        private TestIdfiable(Id id) {
            this.id = id;
        }

        @Override
        public Id id() {
            return this.id;
        }
    }

    private static final class PagingIterator
            implements Iterator<TestIdfiable> {

        private final List<TestIdfiable> results;
        private final int pageSize;
        private final Consumer<IdQuery> pageListener;
        private final List<IdQuery> queries;

        private int current;
        private int announcedPage;

        private PagingIterator(List<TestIdfiable> results, int pageSize,
                               Consumer<IdQuery> pageListener,
                               List<IdQuery> queries) {
            this.results = results;
            this.pageSize = pageSize;
            this.pageListener = pageListener;
            this.queries = queries;
            this.current = 0;
            this.announcedPage = 0;
        }

        @Override
        public boolean hasNext() {
            if (this.current >= this.results.size()) {
                return false;
            }
            int page = this.current / this.pageSize;
            if (page != this.announcedPage) {
                this.pageListener.accept(this.queries.get(page));
                this.announcedPage = page;
            }
            return true;
        }

        @Override
        public TestIdfiable next() {
            if (!this.hasNext()) {
                throw new NoSuchElementException();
            }
            return this.results.get(this.current++);
        }

        private int consumed() {
            return this.current;
        }
    }
}
