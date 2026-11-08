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

package org.apache.hugegraph.store.node.grpc.query;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.hugegraph.rocksdb.access.RocksDBSession.BackendColumn;
import org.apache.hugegraph.rocksdb.access.ScanIterator;
import org.apache.hugegraph.store.business.itrv2.InAccurateIntersectionIterator;
import org.apache.hugegraph.store.business.itrv2.InAccurateUnionFilterIterator;
import org.apache.hugegraph.store.business.itrv2.IntersectionFilterIterator;
import org.apache.hugegraph.store.business.itrv2.IntersectionWrapper;
import org.apache.hugegraph.store.business.itrv2.MapLimitIterator;
import org.apache.hugegraph.store.business.itrv2.MapValueFilterIterator;
import org.apache.hugegraph.store.business.itrv2.MultiListIterator;
import org.apache.hugegraph.store.business.itrv2.UnionFilterIterator;
import org.apache.hugegraph.store.business.itrv2.io.SortShuffleSerializer;
import org.apache.hugegraph.store.grpc.query.QueryRequest;
import org.apache.hugegraph.store.grpc.query.QueryResponse;
import org.apache.hugegraph.store.node.grpc.query.model.QueryPlan;
import org.junit.Test;

import io.grpc.stub.StreamObserver;

public class FilterIteratorContractTest extends AggregativeQueryTestSupport {

    @Test
    public void testMapFilterSupportsDirectNextAndRepeatedProbes() {
        Map<String, Integer> counts = new LinkedHashMap<>();
        counts.put("skip-first", 1);
        counts.put("first", 2);
        counts.put("skip-middle", 3);
        counts.put("second", 2);
        counts.put("third", 2);
        counts.put("skip-last", 0);
        Iterator<String> iterator = new MapValueFilterIterator<>(counts, count -> count == 2);

        assertEquals("first", iterator.next());
        assertTrue(iterator.hasNext());
        assertTrue(iterator.hasNext());
        assertEquals("second", iterator.next());
        assertEquals("third", iterator.next());
        assertFalse(iterator.hasNext());
        assertFalse(iterator.hasNext());
        assertThrows(NoSuchElementException.class, iterator::next);
    }

    @Test
    public void testMapFilterDoesNotRepeatConsumedValue() {
        Iterator<String> iterator = new MapValueFilterIterator<>(Map.of("only", 1), count -> count > 0);

        assertTrue(iterator.hasNext());
        assertEquals("only", iterator.next());
        assertThrows(NoSuchElementException.class, iterator::next);
        assertFalse(iterator.hasNext());
    }

    @Test
    public void testEmptyAndRejectedMapsRemainExhausted() {
        for (Map<String, Integer> counts : List.of(Map.<String, Integer>of(), Map.of("rejected", 1))) {
            Iterator<String> iterator = new MapValueFilterIterator<>(counts, count -> count == 2);
            assertFalse(iterator.hasNext());
            assertFalse(iterator.hasNext());
            assertThrows(NoSuchElementException.class, iterator::next);
        }
    }

    @Test
    public void testUnionUniqueStreamPreservesPendingResults() {
        assertFilterContract(FilterKind.UNION_UNIQUE);
    }

    @Test
    public void testUnionBufferedResultsPreservePendingResults() {
        assertFilterContract(FilterKind.UNION_BUFFERED);
    }

    @Test
    public void testIntersectionInMemoryPreservesPendingResults() {
        assertFilterContract(FilterKind.INTERSECTION);
    }

    @Test
    public void testLimitDedupPreservesPendingResults() {
        assertFilterContract(FilterKind.LIMIT_DEDUP);
    }

    @Test
    public void testInAccurateUnionPreservesPendingResults() {
        assertFilterContract(FilterKind.INACCURATE_UNION);
    }

    @Test
    public void testInAccurateIntersectionPreservesPendingResults() {
        assertFilterContract(FilterKind.INACCURATE_INTERSECTION);
    }

    @Test
    public void testEmptyUnionRemainsExhausted() {
        IntersectionWrapper<BackendColumn> wrapper = new IntersectionWrapper<>(
                new RowIterator(List.of()), column -> column.name[0]);
        wrapper.proc();
        try (ScanIterator iterator = new UnionFilterIterator<>(new RowIterator(List.of()), wrapper,
                                                              BackendColumn::compareTo,
                                                              SortShuffleSerializer.ofBackendColumnSerializer())) {
            assertFalse(iterator.hasNext());
            assertFalse(iterator.hasNext());
            assertThrows(NoSuchElementException.class, iterator::next);
        }
    }

    @Test
    public void testCloseDiscardsPendingResultsWithoutProbingClosedSource() {
        for (FilterKind kind : FilterKind.values()) {
            FilterFixture fixture = filter(kind);
            assertTrue(fixture.iterator.hasNext());

            fixture.iterator.close();

            assertClosedWithoutProbingSource(fixture);
        }
    }

    @Test
    public void testCloseFailureStillDiscardsPendingResultsAndStopsProbing() {
        for (FilterKind kind : FilterKind.values()) {
            FilterFixture fixture = filter(kind);
            IllegalStateException failure = new IllegalStateException("source close failed");
            fixture.source.closeFailure = failure;
            assertTrue(fixture.iterator.hasNext());

            assertSame(failure, assertThrows(IllegalStateException.class, fixture.iterator::close));

            assertClosedWithoutProbingSource(fixture);
        }
    }

    @Test
    public void testMultiListCloseBeforeIterationClosesAllSources() {
        RowIterator first = singleRow(1);
        RowIterator second = singleRow(2);
        MultiListIterator iterator = new MultiListIterator(List.of(first, second));

        iterator.close();

        assertEquals(1, first.closes);
        assertEquals(1, second.closes);
        assertFalse(iterator.hasNext());

        iterator.close();

        assertEquals(1, first.closes);
        assertEquals(1, second.closes);
    }

    @Test
    public void testLimitDedupCloseBeforeIterationClosesMultiListSources() {
        RowIterator first = singleRow(1);
        RowIterator second = singleRow(2);
        ScanIterator iterator = new MapLimitIterator<BackendColumn>(
                new MultiListIterator(List.of(first, second)));

        iterator.close();

        assertEquals(1, first.closes);
        assertEquals(1, second.closes);
    }

    @Test
    public void testMultiListClosesEachSourceOnceAfterPartialIteration() {
        RowIterator first = singleRow(1);
        RowIterator second = singleRow(2);
        RowIterator third = singleRow(3);
        MultiListIterator iterator = new MultiListIterator(List.of(first, second, third));

        assertTrue(iterator.hasNext());
        assertEquals(1, (int) iterator.<BackendColumn>next().name[0]);
        assertTrue(iterator.hasNext());
        assertEquals(1, first.closes);

        iterator.close();

        assertEquals(1, first.closes);
        assertEquals(1, second.closes);
        assertEquals(1, third.closes);
    }

    @Test
    public void testMultiListCloseFailureStillClosesRemainingSources() {
        RowIterator first = singleRow(1);
        RowIterator second = singleRow(2);
        RowIterator third = singleRow(3);
        IllegalStateException firstFailure = new IllegalStateException("first close failed");
        IllegalStateException thirdFailure = new IllegalStateException("third close failed");
        first.closeFailure = firstFailure;
        third.closeFailure = thirdFailure;
        MultiListIterator iterator = new MultiListIterator(List.of(first, second, third));

        assertSame(firstFailure, assertThrows(IllegalStateException.class, iterator::close));

        assertEquals(1, first.closes);
        assertEquals(1, second.closes);
        assertEquals(1, third.closes);
        assertEquals(List.of(thirdFailure), Arrays.asList(firstFailure.getSuppressed()));
        assertTrue(iterator.getIterators().isEmpty());
        assertFalse(iterator.hasNext());
    }

    @Test
    public void testMultiListCloseFailureDuringIterationStillClosesRemainingSources() {
        RowIterator first = singleRow(1);
        RowIterator second = singleRow(2);
        IllegalStateException firstFailure = new IllegalStateException("first close failed");
        IllegalStateException secondFailure = new IllegalStateException("second close failed");
        first.closeFailure = firstFailure;
        second.closeFailure = secondFailure;
        MultiListIterator iterator = new MultiListIterator(List.of(first, second));
        assertTrue(iterator.hasNext());

        assertSame(firstFailure, assertThrows(IllegalStateException.class, iterator::close));

        assertEquals(1, first.closes);
        assertEquals(1, second.closes);
        assertEquals(List.of(secondFailure), Arrays.asList(firstFailure.getSuppressed()));
        assertTrue(iterator.getIterators().isEmpty());
    }

    @Test
    public void testMultiListCloseErrorStillClosesRemainingSources() {
        RowIterator first = singleRow(1);
        RowIterator second = singleRow(2);
        RowIterator third = singleRow(3);
        LinkageError firstError = new LinkageError("first close failed");
        IllegalStateException secondFailure = new IllegalStateException("second close failed");
        first.closeError = firstError;
        second.closeFailure = secondFailure;
        MultiListIterator iterator = new MultiListIterator(List.of(first, second, third));

        assertSame(firstError, assertThrows(LinkageError.class, iterator::close));

        assertEquals(1, first.closes);
        assertEquals(1, second.closes);
        assertEquals(1, third.closes);
        assertEquals(List.of(secondFailure), Arrays.asList(firstError.getSuppressed()));
        assertTrue(iterator.getIterators().isEmpty());

        iterator.close();
        assertEquals(1, first.closes);
        assertEquals(1, second.closes);
        assertEquals(1, third.closes);
    }

    @Test
    public void testMultiListNextWithoutHasNext() {
        MultiListIterator iterator = new MultiListIterator(List.of(singleRow(1), singleRow(2)));

        assertEquals(1, (int) iterator.<BackendColumn>next().name[0]);
        assertEquals(2, (int) iterator.<BackendColumn>next().name[0]);
        assertThrows(NoSuchElementException.class, iterator::next);

        assertThrows(NoSuchElementException.class, new MultiListIterator()::next);
    }

    @Test
    public void testMultiListProbesAdvancingSourceOncePerRow() {
        MultiListIterator iterator = new MultiListIterator(List.of(
                new AdvancingIterator(List.of(BackendColumn.of(new byte[]{1}, new byte[]{11}),
                                              BackendColumn.of(new byte[]{2}, new byte[]{12}))),
                new AdvancingIterator(List.of(BackendColumn.of(new byte[]{3}, new byte[]{13})))));

        assertTrue(iterator.hasNext());
        assertTrue(iterator.hasNext());
        assertTrue(iterator.isValid());
        assertEquals(1, (int) iterator.<BackendColumn>next().name[0]);
        assertEquals(2, (int) iterator.<BackendColumn>next().name[0]);
        assertTrue(iterator.hasNext());
        assertEquals(3, (int) iterator.<BackendColumn>next().name[0]);
        assertFalse(iterator.hasNext());
        assertTrue(iterator.isValid());
        assertThrows(NoSuchElementException.class, iterator::next);
    }

    @Test
    public void testMultiListCloseDuringIterationStopsProbingSources() {
        RowIterator first = singleRow(1);
        RowIterator second = singleRow(2);
        MultiListIterator iterator = new MultiListIterator(List.of(first, second));
        assertTrue(iterator.hasNext());
        int firstProbes = first.probes;
        int secondProbes = second.probes;

        iterator.close();

        assertFalse(iterator.hasNext());
        assertFalse(iterator.isValid());
        assertEquals(firstProbes, first.probes);
        assertEquals(secondProbes, second.probes);
        assertEquals(1, first.closes);
        assertEquals(1, second.closes);

        assertThrows(NoSuchElementException.class, iterator::next);
        iterator.close();
        assertEquals(firstProbes, first.probes);
        assertEquals(1, first.closes);
        assertEquals(1, second.closes);
    }

    @Test(timeout = 5000)
    public void testUnionUniqueStreamDoesNotLoseRecordsBetweenSingleRecordBatches() throws Exception {
        assertSingleRecordBatches(FilterKind.UNION_UNIQUE);
    }

    @Test(timeout = 5000)
    public void testUnionBufferedResultsDoNotLoseRecordsBetweenSingleRecordBatches() throws Exception {
        assertSingleRecordBatches(FilterKind.UNION_BUFFERED);
    }

    @Test(timeout = 5000)
    public void testIntersectionDoesNotLoseRecordsBetweenSingleRecordBatches() throws Exception {
        assertSingleRecordBatches(FilterKind.INTERSECTION);
    }

    @Test(timeout = 5000)
    public void testLimitDedupDoesNotLoseRecordsBetweenSingleRecordBatches() throws Exception {
        assertSingleRecordBatches(FilterKind.LIMIT_DEDUP);
    }

    @Test(timeout = 5000)
    public void testInAccurateUnionDoesNotLoseRecordsBetweenSingleRecordBatches() throws Exception {
        assertSingleRecordBatches(FilterKind.INACCURATE_UNION);
    }

    @Test(timeout = 5000)
    public void testInAccurateIntersectionDoesNotLoseRecordsBetweenSingleRecordBatches()
            throws Exception {
        assertSingleRecordBatches(FilterKind.INACCURATE_INTERSECTION);
    }

    private static void assertFilterContract(FilterKind kind) {
        FilterFixture fixture = filter(kind);
        List<Integer> keys = new ArrayList<>();
        try (ScanIterator iterator = fixture.iterator) {
            // Intersection initializes its reduction on the first probe; keep that API unchanged.
            if (kind == FilterKind.INTERSECTION) {
                assertTrue(iterator.hasNext());
                assertTrue(iterator.hasNext());
            }
            BackendColumn first = iterator.next();
            keys.add((int) first.name[0]);
            while (iterator.hasNext()) {
                assertTrue(iterator.hasNext());
                assertTrue(iterator.hasNext());
                BackendColumn column = iterator.next();
                keys.add((int) column.name[0]);
            }
            assertFalse(iterator.hasNext());
            assertThrows(NoSuchElementException.class, iterator::next);
        }
        keys.sort(Integer::compareTo);
        assertEquals(List.of(1, 2, 3), keys);
        assertTrue(fixture.source.closed);
    }

    private static void assertClosedWithoutProbingSource(FilterFixture fixture) {
        int probes = fixture.source.probes;
        assertFalse(fixture.iterator.hasNext());
        assertFalse(fixture.iterator.hasNext());
        assertFalse(fixture.iterator.isValid());
        assertThrows(NoSuchElementException.class, fixture.iterator::next);
        assertEquals(probes, fixture.source.probes);
        assertTrue(fixture.source.closed);
    }

    private static void assertSingleRecordBatches(FilterKind kind) throws Exception {
        FilterFixture fixture = filter(kind);
        ThreadPoolExecutor pool = pool(1);
        QueryPlan plan = new QueryPlan();
        AggregativeQueryService service = new AggregativeQueryService(pool, 500, 1) {
            @Override
            AggregativeQueryObserver newObserver(StreamObserver<QueryResponse> sender) {
                return fixture(super.newObserver(sender), fixture.iterator, plan, null);
            }
        };
        ResponseRecorder response = new ResponseRecorder();
        try {
            service.query(response).onNext(QueryRequest.newBuilder().setQueryId(kind.name()).build());
            assertTrue(response.finished.await(2, TimeUnit.SECONDS));

            List<Integer> keys = new ArrayList<>();
            assertEquals(3, response.responses.size());
            for (int i = 0; i < response.responses.size(); i++) {
                QueryResponse batch = response.responses.get(i);
                assertTrue(batch.getIsOk());
                assertEquals(kind.name(), batch.getQueryId());
                assertEquals(1, batch.getDataCount());
                assertEquals(i == response.responses.size() - 1, batch.getIsFinished());
                keys.add((int) batch.getData(0).getKey().byteAt(0));
            }
            keys.sort(Integer::compareTo);
            assertEquals(List.of(1, 2, 3), keys);
            assertEquals(1, response.completed.get());
            assertEquals(0, response.errors.get());
            assertTrue(fixture.source.closed);
            service.shutdownQueries();
        } finally {
            pool.shutdownNow();
            pool.awaitTermination(1, TimeUnit.SECONDS);
            fixture.iterator.close();
        }
    }

    private static RowIterator singleRow(int key) {
        return new RowIterator(List.of(BackendColumn.of(new byte[]{(byte) key},
                                                        new byte[]{(byte) (key + 10)})));
    }

    private static FilterFixture filter(FilterKind kind) {
        BackendColumn first = BackendColumn.of(new byte[]{1}, new byte[]{11});
        BackendColumn second = BackendColumn.of(new byte[]{2}, new byte[]{12});
        BackendColumn third = BackendColumn.of(new byte[]{3}, new byte[]{13});
        List<BackendColumn> rows = kind == FilterKind.UNION_UNIQUE ? List.of(first, second, third) :
                                   Arrays.asList(first, second, third, first, second, third);
        RowIterator source = new RowIterator(rows);
        switch (kind) {
            case LIMIT_DEDUP:
                return new FilterFixture(new MapLimitIterator<BackendColumn>(source), source);
            case INACCURATE_UNION:
                return new FilterFixture(new InAccurateUnionFilterIterator<BackendColumn>(
                        source, column -> column.name[0]), source);
            case INACCURATE_INTERSECTION:
                return new FilterFixture(new InAccurateIntersectionIterator<BackendColumn>(
                        new MultiListIterator(List.of(source)), column -> column.name[0]), source);
            default:
                break;
        }
        IntersectionWrapper<BackendColumn> wrapper = new IntersectionWrapper<>(
                new RowIterator(rows), column -> column.name[0]);
        wrapper.proc();
        ScanIterator iterator = kind == FilterKind.INTERSECTION ?
                                new IntersectionFilterIterator(source, wrapper, 2) :
                                new UnionFilterIterator<>(source, wrapper, BackendColumn::compareTo,
                                                          SortShuffleSerializer.ofBackendColumnSerializer());
        return new FilterFixture(iterator, source);
    }

    private enum FilterKind {
        UNION_UNIQUE, UNION_BUFFERED, INTERSECTION, LIMIT_DEDUP, INACCURATE_UNION,
        INACCURATE_INTERSECTION
    }

    private static class FilterFixture {

        private final ScanIterator iterator;
        private final RowIterator source;

        private FilterFixture(ScanIterator iterator, RowIterator source) {
            this.iterator = iterator;
            this.source = source;
        }
    }

    private static class RowIterator implements ScanIterator {

        private final Iterator<BackendColumn> rows;
        private boolean closed;
        private int closes;
        private int probes;
        private RuntimeException closeFailure;
        private Error closeError;

        private RowIterator(List<BackendColumn> rows) {
            this.rows = rows.iterator();
        }

        @Override
        public boolean hasNext() {
            this.probes++;
            return !this.closed && this.rows.hasNext();
        }

        @Override
        public boolean isValid() {
            return hasNext();
        }

        @SuppressWarnings("unchecked")
        @Override
        public <T> T next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return (T) this.rows.next();
        }

        @Override
        public void close() {
            this.closed = true;
            this.closes++;
            if (this.closeFailure != null) {
                throw this.closeFailure;
            }
            if (this.closeError != null) {
                throw this.closeError;
            }
        }
    }

    /**
     * models the dedup iterators before #3424: hasNext() pulls the next row and replaces
     * the pending one, so a second hasNext() before next() skips a row
     */
    private static class AdvancingIterator implements ScanIterator {

        private final Iterator<BackendColumn> rows;
        private BackendColumn current;

        private AdvancingIterator(List<BackendColumn> rows) {
            this.rows = rows.iterator();
        }

        @Override
        public boolean hasNext() {
            this.current = this.rows.hasNext() ? this.rows.next() : null;
            return this.current != null;
        }

        @Override
        public boolean isValid() {
            return this.current != null;
        }

        @SuppressWarnings("unchecked")
        @Override
        public <T> T next() {
            if (this.current == null) {
                throw new NoSuchElementException();
            }
            BackendColumn row = this.current;
            this.current = null;
            return (T) row;
        }

        @Override
        public void close() {
        }
    }
}
