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
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.hugegraph.rocksdb.access.RocksDBSession.BackendColumn;
import org.apache.hugegraph.rocksdb.access.ScanIterator;
import org.apache.hugegraph.store.business.itrv2.IntersectionFilterIterator;
import org.apache.hugegraph.store.business.itrv2.IntersectionFilterIterator.ReduceIterator;
import org.apache.hugegraph.store.business.itrv2.IntersectionWrapper;
import org.apache.hugegraph.store.business.itrv2.io.SortShuffleSerializer;
import org.apache.hugegraph.store.grpc.query.QueryRequest;
import org.apache.hugegraph.store.grpc.query.QueryResponse;
import org.apache.hugegraph.store.node.grpc.query.model.QueryPlan;
import org.apache.hugegraph.store.util.SortShuffle;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import io.grpc.stub.StreamObserver;

public class IntersectionSpillIteratorTest extends AggregativeQueryTestSupport {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private String previousBasePath;

    @Before
    public void setUp() {
        this.previousBasePath = SortShuffle.getBasePath();
        SortShuffle.setBasePath(this.folder.getRoot().getAbsolutePath() + File.separator);
    }

    @After
    public void tearDown() {
        SortShuffle.setBasePath(this.previousBasePath);
    }

    @Test
    public void testReduceIteratorSupportsDirectNextAndRepeatedProbes() {
        Iterator<Integer> iterator = new ReduceIterator<>(List.of(1, 1, 2, 2, 3, 3).iterator(),
                                                          Integer::compareTo, 2);

        assertEquals(Integer.valueOf(1), iterator.next());
        assertTrue(iterator.hasNext());
        assertTrue(iterator.hasNext());
        assertEquals(Integer.valueOf(2), iterator.next());
        assertEquals(Integer.valueOf(3), iterator.next());
        assertFalse(iterator.hasNext());
        assertFalse(iterator.hasNext());
        assertThrows(NoSuchElementException.class, iterator::next);
    }

    @Test
    public void testEmptyReduceIteratorRemainsExhausted() {
        for (int size : new int[]{-1, 1, 2, 3}) {
            Iterator<Integer> iterator = new ReduceIterator<>(List.<Integer>of().iterator(),
                                                              Integer::compareTo, size);
            assertFalse(iterator.hasNext());
            assertFalse(iterator.hasNext());
            assertThrows(NoSuchElementException.class, iterator::next);
        }
    }

    @Test
    public void testSpillKeepsLastRepresentativeAtGroupBoundaryAndEof() throws Exception {
        assertEquals(List.of(List.of(1, 12), List.of(2, 22)),
                     readSpill(2, column(1, 11), column(1, 12), column(2, 21), column(2, 22)));
    }

    @Test
    public void testExactSizeRejectsShortFinalGroup() throws Exception {
        assertEquals(List.of(List.of(1, 13)),
                     readSpill(3, column(1, 11), column(1, 12), column(1, 13),
                               column(2, 21), column(2, 22)));
    }

    @Test
    public void testExactSizeRejectsOversizedFinalGroup() throws Exception {
        assertEquals(List.of(List.of(1, 12)),
                     readSpill(2, column(1, 11), column(1, 12), column(2, 21),
                               column(2, 22), column(2, 23)));
    }

    @Test
    public void testExactSizeUsesSamePredicateForEveryGroup() throws Exception {
        assertEquals(List.of(List.of(2, 23)),
                     readSpill(3, column(1, 11), column(1, 12),
                               column(2, 21), column(2, 22), column(2, 23),
                               column(3, 31), column(3, 32), column(3, 33), column(3, 34)));
    }

    @Test
    public void testDefaultDuplicatePredicateSkipsSingletonsAtBothEnds() throws Exception {
        assertEquals(List.of(List.of(2, 22), List.of(4, 43)),
                     readSpill(-1, column(1, 11), column(2, 21), column(2, 22), column(3, 31),
                                column(4, 41), column(4, 42), column(4, 43), column(5, 51)));
        assertEquals(List.of(List.of(2, 22)),
                     readSpill(-1, column(1, 11), column(2, 21), column(2, 22)));
    }

    @Test
    public void testExactOneReturnsSingletonWithoutNullOrStaleRepresentative() throws Exception {
        assertEquals(List.of(List.of(1, 11), List.of(3, 31)),
                     readSpill(1, column(1, 11), column(2, 21), column(2, 22), column(3, 31)));
    }

    @Test(timeout = 5000)
    public void testSingleRecordRpcBatchesPreserveEverySpilledGroup() throws Exception {
        ScanIterator iterator = spill(2, column(1, 11), column(1, 12), column(2, 21), column(2, 22),
                                      column(3, 31), column(3, 32));
        ThreadPoolExecutor pool = pool(1);
        QueryPlan plan = new QueryPlan();
        AggregativeQueryService service = new AggregativeQueryService(pool, 500, 1) {
            @Override
            AggregativeQueryObserver newObserver(StreamObserver<QueryResponse> sender) {
                return fixture(super.newObserver(sender), iterator, plan, null);
            }
        };
        ResponseRecorder response = new ResponseRecorder();
        try {
            service.query(response).onNext(QueryRequest.newBuilder().setQueryId("intersection-spill").build());
            assertTrue(response.finished.await(2, TimeUnit.SECONDS));

            assertEquals(3, response.responses.size());
            for (int i = 0; i < response.responses.size(); i++) {
                QueryResponse batch = response.responses.get(i);
                assertTrue(batch.getIsOk());
                assertEquals("intersection-spill", batch.getQueryId());
                assertEquals(1, batch.getDataCount());
                assertEquals(i + 1, batch.getData(0).getKey().byteAt(0));
                assertEquals((i + 1) * 10 + 2, batch.getData(0).getValue().byteAt(0));
                assertEquals(i == 2, batch.getIsFinished());
            }
            assertEquals(1, response.completed.get());
            assertEquals(0, response.errors.get());
            assertEquals(0, this.folder.getRoot().list().length);
            service.shutdownQueries();
        } finally {
            pool.shutdownNow();
            pool.awaitTermination(1, TimeUnit.SECONDS);
            iterator.close();
        }
    }

    private List<List<Integer>> readSpill(int size, BackendColumn... columns) throws Exception {
        List<List<Integer>> result = new ArrayList<>();
        try (ScanIterator iterator = spill(size, columns)) {
            while (iterator.hasNext()) {
                assertTrue(iterator.hasNext());
                assertTrue(iterator.hasNext());
                BackendColumn column = iterator.next();
                result.add(List.of((int) column.name[0], (int) column.value[0]));
            }
            assertFalse(iterator.hasNext());
            assertFalse(iterator.hasNext());
            assertThrows(NoSuchElementException.class, iterator::next);
        }
        assertEquals(0, this.folder.getRoot().list().length);
        return result;
    }

    @SuppressWarnings("unchecked")
    private static ScanIterator spill(int size, BackendColumn... columns) throws Exception {
        SortShuffle<BackendColumn> shuffle = new SortShuffle<>(
                (left, right) -> Arrays.compare(left.name, right.name),
                SortShuffleSerializer.ofBackendColumnSerializer());
        try {
            // Seed a real sorted spill without changing the production spill thresholds.
            File spill = new File((String) field(SortShuffle.class, "path").get(shuffle), "seed");
            try (OutputStream output = Files.newOutputStream(spill.toPath())) {
                for (BackendColumn column : columns) {
                    SortShuffleSerializer.ofBackendColumnSerializer().write(output, column);
                }
            }
            ((Deque<String>) field(SortShuffle.class, "files").get(shuffle)).add(spill.getAbsolutePath());
            ScanIterator source = new ScanIterator() {
                @Override
                public boolean hasNext() {
                    return false;
                }

                @Override
                public boolean isValid() {
                    return false;
                }

                @Override
                public <T> T next() {
                    throw new NoSuchElementException();
                }

                @Override
                public void close() {
                }
            };
            IntersectionWrapper<BackendColumn> wrapper = new IntersectionWrapper<>(source, column -> column.name[0]);
            IntersectionFilterIterator iterator = new IntersectionFilterIterator(source, wrapper, size);
            field(IntersectionFilterIterator.class, "sortShuffle").set(iterator, shuffle);
            return iterator;
        } catch (Exception | Error failure) {
            shuffle.close();
            throw failure;
        }
    }

    private static BackendColumn column(int key, int value) {
        return BackendColumn.of(new byte[]{(byte) key}, new byte[]{(byte) value});
    }

    private static Field field(Class<?> owner, String name) throws Exception {
        Field field = owner.getDeclaredField(name);
        field.setAccessible(true);
        return field;
    }
}
