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
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.id.IdGenerator;
import org.apache.hugegraph.backend.page.PageInfo;
import org.apache.hugegraph.backend.page.PageState;
import org.apache.hugegraph.backend.query.ConditionQuery;
import org.apache.hugegraph.backend.query.IdQuery;
import org.apache.hugegraph.backend.query.Query;
import org.apache.hugegraph.backend.query.QueryResultContext;
import org.apache.hugegraph.backend.query.QueryResults;
import org.apache.hugegraph.exception.LimitExceedException;
import org.apache.hugegraph.iterator.CIter;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.type.Idfiable;
import org.apache.hugegraph.util.InsertionOrderUtil;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

public class QueryResultsTest {

    @Test
    public void testMaterializationKeepsPageMetadataAfterClosingSource() {
        PageState page = new PageState(new byte[]{1, 2}, 0, 2);
        CountingIterator source = new CountingIterator(1L, 2L);
        CIter<TestIdfiable> paged = new CIter<TestIdfiable>() {
            @Override
            public boolean hasNext() {
                return source.hasNext();
            }

            @Override
            public TestIdfiable next() {
                return source.next();
            }

            @Override
            public void close() {
                source.close();
            }

            @Override
            public Object metadata(String meta, Object... args) {
                Assert.assertEquals(PageInfo.PAGE, meta);
                return page;
            }
        };
        QueryResults<TestIdfiable> results = new QueryResults<>(paged, queryOf(1L, 2L));
        Assert.assertTrue(results.iterator().hasNext());
        QueryResults<TestIdfiable> fetched = results.toList();
        Assert.assertEquals(1, source.closed);
        Assert.assertSame(page, PageInfo.pageState(results.iterator()));
        Assert.assertSame(page, PageInfo.pageState(fetched.iterator()));
        List<Id> actual = new ArrayList<>();
        fetched.iterator().forEachRemaining(item -> actual.add(item.id()));
        Assert.assertEquals(Arrays.asList(IdGenerator.of(1L), IdGenerator.of(2L)), actual);
        Assert.assertSame(page, PageInfo.pageState(fetched.iterator()));
    }

    @Test
    public void testMaterializeAfterPeekAndPartialConsumption() {
        for (int consumed = 0; consumed <= 2; consumed++) {
            CountingIterator first = new CountingIterator(1L, 2L);
            CountingIterator second = new CountingIterator(3L, 4L);
            QueryResults<TestIdfiable> results = QueryResults.flatMap(
                    Arrays.asList(0, 1).iterator(), index -> new QueryResults<>(
                            index == 0 ? first : second,
                            index == 0 ? queryOf(1L, 2L) : queryOf(3L, 4L)));
            Iterator<TestIdfiable> iterator = results.iterator();
            for (int i = 0; i < consumed; i++) {
                Assert.assertEquals(IdGenerator.of(i + 1L), iterator.next().id());
            }
            Assert.assertTrue(iterator.hasNext());
            QueryResults<TestIdfiable> fetched = results.toList();
            List<Id> actual = new ArrayList<>();
            fetched.filter((context, item) -> {
                Assert.assertTrue(context.inputIds().contains(item.id()));
                return true;
            }).iterator().forEachRemaining(item -> actual.add(item.id()));
            List<Id> expected = new ArrayList<>();
            for (long id = consumed + 1L; id <= 4L; id++) {
                expected.add(IdGenerator.of(id));
            }
            Assert.assertEquals(expected, actual);
            Assert.assertFalse(iterator.hasNext());
            Assert.assertEquals(1, first.closed);
            Assert.assertEquals(1, second.closed);
        }
    }

    @Test
    public void testMapAfterPeekKeepsCurrentBatch() {
        CountingIterator source = new CountingIterator(1L, 2L);
        QueryResults<TestIdfiable> results = new QueryResults<>(source, queryOf(1L, 2L));
        Assert.assertTrue(results.iterator().hasNext());
        List<Id> actual = new ArrayList<>();
        results.map(TestIdfiable::id).iterator().forEachRemaining(actual::add);
        Assert.assertEquals(Arrays.asList(IdGenerator.of(1L), IdGenerator.of(2L)), actual);
        Assert.assertEquals(1, source.closed);
    }

    @Test
    public void testQueryDiagnosticsDoNotDuplicateAndClearOnClose() throws Exception {
        IdQuery query = queryOf(1L, 2L);
        QueryResults<TestIdfiable> results = new QueryResults<>(
                new CountingIterator(1L, 2L), query);
        Assert.assertEquals(Collections.singletonList(query), results.queries());
        Iterator<TestIdfiable> iterator = results.iterator();
        Assert.assertTrue(iterator.hasNext());
        Assert.assertEquals(Collections.singletonList(query), results.queries());
        ((AutoCloseable) iterator).close();
        Assert.assertTrue(results.queries().isEmpty());
    }

    @Test
    public void testMaterializationCapacityAppliesAcrossBatches() {
        Query query = new Query(HugeType.VERTEX);
        List<Integer> values = Collections.nCopies((int) Query.DEFAULT_CAPACITY / 2 + 1, 1);
        QueryResults<Integer> results = QueryResults.flatMap(Arrays.asList(1, 2).iterator(),
                ignored -> new QueryResults<>(values.iterator(), query));
        Assert.assertThrows(LimitExceedException.class, () -> {
            results.toList();
        });
    }

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
        results.<TestIdfiable>keepInputOrderIfNeeded().iterator()
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
        results.<TestIdfiable>keepInputOrderIfNeeded().iterator()
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
        results.<TestIdfiable>keepInputOrderIfNeeded().iterator()
               .forEachRemaining(item -> orderedIds.add(item.id()));

        Assert.assertEquals(ImmutableList.of(IdGenerator.of(1L),
                                             IdGenerator.of(2L),
                                             IdGenerator.of(3L)),
                            orderedIds);
    }

    @Test
    public void testKeepInputOrderDoesNotDrainFollowingPages() {
        CountingIterator first = new CountingIterator(1L, 2L);
        CountingIterator second = new CountingIterator(3L, 4L);
        QueryResults<TestIdfiable> results = QueryResults.flatMap(
                ImmutableList.of(0, 1).iterator(), index -> index == 0 ?
                new QueryResults<>(first, queryOf(2L, 1L)) :
                new QueryResults<>(second, queryOf(4L, 3L)));
        Iterator<TestIdfiable> ordered = results.<TestIdfiable>keepInputOrderIfNeeded().iterator();
        Assert.assertEquals(IdGenerator.of(2L), ordered.next().id());
        Assert.assertEquals(IdGenerator.of(1L), ordered.next().id());
        Assert.assertEquals(2, first.consumed);
        Assert.assertEquals(0, second.consumed);
        Assert.assertEquals(IdGenerator.of(4L), ordered.next().id());
        Assert.assertEquals(IdGenerator.of(3L), ordered.next().id());
        Assert.assertFalse(ordered.hasNext());
        Assert.assertEquals(1, first.closed);
        Assert.assertEquals(1, second.closed);
    }

    @Test
    public void testOrderingRequirementIsLocalToEachBatch() {
        IdQuery firstQuery = queryOf(1L);
        firstQuery.mustSortByInput(false);
        QueryResults<TestIdfiable> first = new QueryResults<>(
                new CountingIterator(1L), firstQuery);
        QueryResults<TestIdfiable> second = resultsOf(
                ImmutableList.of(3L, 2L), ImmutableList.of(2L, 3L));
        QueryResults<TestIdfiable> results = QueryResults.flatMap(
                ImmutableList.of(first, second).iterator(), result -> result);
        List<Id> ids = new ArrayList<>();
        results.<TestIdfiable>keepInputOrderIfNeeded().iterator()
               .forEachRemaining(item -> ids.add(item.id()));
        Assert.assertEquals(ImmutableList.of(IdGenerator.of(1L),
                                             IdGenerator.of(3L),
                                             IdGenerator.of(2L)), ids);
    }

    @Test
    public void testOrderingDoesNotActivateFollowingQuery() {
        int[] fetches = {0};
        QueryResults<TestIdfiable> results = QueryResults.flatMap(
                ImmutableList.of(0, 1).iterator(), index -> {
                    fetches[0]++;
                    return resultsOf(ImmutableList.of(2L, 1L),
                                     ImmutableList.of(1L, 2L));
                });
        Iterator<TestIdfiable> ordered =
                results.<TestIdfiable>keepInputOrderIfNeeded().iterator();
        Assert.assertEquals(IdGenerator.of(2L), ordered.next().id());
        Assert.assertEquals(1, fetches[0]);
        Assert.assertEquals(IdGenerator.of(1L), ordered.next().id());
        Assert.assertEquals(1, fetches[0]);
        Assert.assertTrue(ordered.hasNext());
        Assert.assertEquals(2, fetches[0]);
    }

    @Test
    public void testNullAndExpandedResultsKeepTheirBatchContext() {
        int[] activated = {0};
        QueryResults<TestIdfiable> results = QueryResults.flatMap(
                ImmutableList.of(0, 1).iterator(), index -> {
                    activated[0]++;
                    return resultsOf(ImmutableList.of((long) index + 1),
                                     ImmutableList.of((long) index + 1));
                });
        QueryResults<TestIdfiable> expanded = results.flatMap(item ->
                Arrays.asList(null, item, item).iterator());
        Iterator<TestIdfiable> values = expanded.filter((context, item) -> {
            Assert.assertTrue(context.inputIds().contains(item.id()));
            Assert.assertEquals(item.id().asLong(), (long) activated[0]);
            return true;
        }).iterator();
        Assert.assertEquals(IdGenerator.of(1L), values.next().id());
        Assert.assertEquals(IdGenerator.of(1L), values.next().id());
        Assert.assertEquals(1, activated[0]);
        Assert.assertEquals(IdGenerator.of(2L), values.next().id());
        Assert.assertEquals(IdGenerator.of(2L), values.next().id());
        Assert.assertFalse(values.hasNext());
    }

    @Test
    public void testCloseDoesNotActivateRemainingBatch() throws Exception {
        CountingIterator source = new CountingIterator(1L, 2L);
        int[] activated = {0};
        QueryResults<TestIdfiable> results = QueryResults.flatMap(
                ImmutableList.of(0, 1).iterator(), index -> {
                    activated[0]++;
                    return new QueryResults<>(source, queryOf(1L, 2L));
                });
        Iterator<TestIdfiable> values = results.map(item -> item).iterator();
        Assert.assertTrue(values.hasNext());
        ((AutoCloseable) values).close();
        ((AutoCloseable) values).close();
        Assert.assertFalse(values.hasNext());
        Assert.assertEquals(1, activated[0]);
        Assert.assertEquals(1, source.closed);
    }

    @Test
    public void testSourceFailuresCloseMappedResultsOnce() throws Exception {
        for (boolean failHasNext : new boolean[]{true, false}) {
            CountingIterator source = new CountingIterator(1L);
            RuntimeException failure = new IllegalStateException("source");
            if (failHasNext) {
                source.hasNextFailure = failure;
            } else {
                source.nextFailure = failure;
            }
            Iterator<TestIdfiable> values = new QueryResults<>(source, queryOf(1L))
                    .map(item -> item).iterator();
            try {
                values.hasNext();
                Assert.fail("Expected source failure");
            } catch (IllegalStateException actual) {
                Assert.assertSame(failure, actual);
            }
            Assert.assertEquals(1, source.closed);
            Assert.assertFalse(values.hasNext());
            ((AutoCloseable) values).close();
            Assert.assertEquals(1, source.closed);
        }
    }

    @Test
    public void testCloseBeforeMappedSourceActivation() throws Exception {
        CountingIterator source = new CountingIterator(1L);
        source.hasNextFailure = new IllegalStateException("Source must not be probed");
        Iterator<TestIdfiable> values = new QueryResults<>(source, queryOf(1L))
                .map(item -> item).iterator();
        ((AutoCloseable) values).close();
        ((AutoCloseable) values).close();
        Assert.assertEquals(0, source.consumed);
        Assert.assertEquals(1, source.closed);
        Assert.assertFalse(values.hasNext());
    }

    @Test
    public void testMapperExceptionPreservesCloseFailure() {
        CountingIterator source = new CountingIterator(1L);
        RuntimeException failure = new IllegalArgumentException("mapper");
        source.closeFailure = new IllegalStateException("close");
        QueryResults<TestIdfiable> results = new QueryResults<>(source, queryOf(1L));
        Iterator<TestIdfiable> values = results.<TestIdfiable>map(item -> {
            throw failure;
        }).iterator();
        try {
            values.hasNext();
            Assert.fail("Expected mapper failure");
        } catch (IllegalArgumentException actual) {
            Assert.assertSame(failure, actual);
            Assert.assertArrayEquals(new Throwable[]{source.closeFailure}, actual.getSuppressed());
        }
        Assert.assertEquals(1, source.closed);
    }

    @Test
    public void testEmptyBatchClosesBeforeNextSupplier() {
        CountingIterator empty = new CountingIterator();
        CountingIterator source = new CountingIterator(1L);
        QueryResults<TestIdfiable> results = QueryResults.flatMap(
                ImmutableList.of(0, 1).iterator(), index -> {
                    if (index == 1) {
                        Assert.assertEquals(1, empty.closed);
                    }
                    return new QueryResults<>(index == 0 ? empty : source, queryOf(1L));
                });
        Assert.assertEquals(IdGenerator.of(1L), results.one().id());
        Assert.assertEquals(1, empty.closed);
        Assert.assertEquals(1, source.closed);
    }

    @Test
    public void testExpandedChildClosesExactlyOnceOnEarlyExit() throws Exception {
        CountingIterator source = new CountingIterator(1L, 2L);
        CountingIterator child = new CountingIterator(3L, 4L);
        Iterator<TestIdfiable> values = new QueryResults<>(source, queryOf(1L, 2L))
                .flatMap(item -> child).iterator();
        Assert.assertEquals(IdGenerator.of(3L), values.next().id());
        ((AutoCloseable) values).close();
        ((AutoCloseable) values).close();
        Assert.assertEquals(1, source.closed);
        Assert.assertEquals(1, child.closed);
        Assert.assertEquals(1, source.consumed);
        Assert.assertEquals(1, child.consumed);
    }

    @Test
    public void testOrderedBatchThenUnorderedBatchPreservesBackendOrder() {
        QueryResults<TestIdfiable> first = resultsOf(
                ImmutableList.of(2L, 1L), ImmutableList.of(1L, 2L));
        IdQuery secondQuery = queryOf(4L, 3L);
        secondQuery.mustSortByInput(false);
        QueryResults<TestIdfiable> second = new QueryResults<>(
                new CountingIterator(3L, 4L), secondQuery);
        QueryResults<TestIdfiable> results = QueryResults.flatMap(
                ImmutableList.of(first, second).iterator(), result -> result);
        List<Id> ids = new ArrayList<>();
        results.<TestIdfiable>keepInputOrderIfNeeded().iterator()
               .forEachRemaining(item -> ids.add(item.id()));
        Assert.assertEquals(Arrays.asList(IdGenerator.of(2L), IdGenerator.of(1L),
                                          IdGenerator.of(3L), IdGenerator.of(4L)), ids);
    }

    @Test
    public void testContextFindsFilterAboveCopiedQueries() {
        ConditionQuery root = new ConditionQuery(HugeType.VERTEX);
        ConditionQuery search = root.copy();
        ConditionQuery.ResultsFilter filter = element -> true;
        search.registerResultsFilter(filter);
        ConditionQuery copied = search.copy();
        QueryResultContext context = new QueryResultContext(
                new IdQuery(copied, IdGenerator.of(1L)));
        Assert.assertSame(filter, context.resultsFilter());
        Assert.assertSame(root, context.matchQuery());
        ConditionQuery other = root.copy();
        other.registerResultsFilter(element -> false);
        Assert.assertSame(filter, context.resultsFilter());
        Assert.assertNotSame(context.resultsFilter(),
                             new QueryResultContext(other).resultsFilter());
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

    private static final class CountingIterator
            implements Iterator<TestIdfiable>, AutoCloseable {

        private final Iterator<Long> values;
        private int consumed;
        private int closed;
        private RuntimeException closeFailure;
        private RuntimeException hasNextFailure;
        private RuntimeException nextFailure;

        private CountingIterator(Long... values) {
            this.values = Arrays.asList(values).iterator();
        }

        @Override
        public boolean hasNext() {
            if (this.hasNextFailure != null) {
                throw this.hasNextFailure;
            }
            return this.values.hasNext();
        }

        @Override
        public TestIdfiable next() {
            if (this.nextFailure != null) {
                throw this.nextFailure;
            }
            this.consumed++;
            return new TestIdfiable(IdGenerator.of(this.values.next()));
        }

        @Override
        public void close() {
            this.closed++;
            if (this.closeFailure != null) {
                throw this.closeFailure;
            }
        }
    }
}
