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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.Field;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hugegraph.id.IdGenerator;
import org.apache.hugegraph.id.IdUtil;
import org.apache.hugegraph.rocksdb.access.ScanIterator;
import org.apache.hugegraph.store.business.itrv2.io.SortShuffleSerializer;
import org.apache.hugegraph.store.grpc.query.QueryRequest;
import org.apache.hugegraph.store.node.grpc.query.model.PipelineResult;
import org.apache.hugegraph.store.node.grpc.query.model.QueryPlan;
import org.apache.hugegraph.store.node.grpc.query.stages.OrderByStage;
import org.apache.hugegraph.store.util.MultiKv;
import org.apache.hugegraph.store.util.SortShuffle;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.google.protobuf.ByteString;

public class OrderByStageTest extends AggregativeQueryTestSupport {

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
    public void testAppendFailurePreservesOriginalCause() throws Exception {
        verifyIoFailure("append");
    }

    @Test
    public void testFinishFailurePreservesOriginalCause() throws Exception {
        verifyIoFailure("finish");
    }

    @Test
    public void testOpenIteratorFailurePreservesOriginalCause() throws Exception {
        verifyIoFailure("iterator");
    }

    @Test
    public void testRuntimeFailureIsNotSwallowedOrReplaced() throws Exception {
        IllegalArgumentException cause = new IllegalArgumentException("invalid sort key");
        SortShuffle<MultiKv> shuffle = new SortShuffle<>(MultiKv::compareTo,
                                                       SortShuffleSerializer.ofMultiKvSerializer()) {
            @Override
            public void append(MultiKv value) {
                throw cause;
            }
        };
        OrderByStage stage = stage(shuffle);
        try {
            assertSame(cause, assertThrows(IllegalArgumentException.class,
                                          () -> stage.handleIterator(result(1))));
        } finally {
            stage.close();
        }
    }

    @Test(timeout = 5000)
    public void testSortFailureReachesRpcErrorAndReleasesPlanAndSource() throws Exception {
        AtomicInteger closes = new AtomicInteger();
        SortShuffle<MultiKv> shuffle = new SortShuffle<>(MultiKv::compareTo,
                                                       SortShuffleSerializer.ofMultiKvSerializer()) {
            @Override
            public void finish() throws IOException {
                throw new IOException("spill read failed");
            }

            @Override
            public void close() {
                super.close();
                closes.incrementAndGet();
            }
        };
        QueryPlan plan = new QueryPlan();
        plan.addStage(stage(shuffle));
        ScanIterator source = mock(ScanIterator.class);
        ThreadPoolExecutor pool = pool(1);
        AggregativeQueryService service = service(pool, source, plan, 500);
        ResponseRecorder response = new ResponseRecorder();
        try {
            service.query(response).onNext(QueryRequest.newBuilder().setQueryId("sort-failure").build());
            assertTrue(response.finished.await(1, TimeUnit.SECONDS));

            assertError(response, "sort-failure");
            assertTrue(plan.isEmpty());
            assertEquals(1, closes.get());
            verify(source).close();
            assertEquals(0, this.folder.getRoot().list().length);
            service.shutdownQueries();
        } finally {
            pool.shutdownNow();
            pool.awaitTermination(1, TimeUnit.SECONDS);
            shuffle.close();
        }
    }

    @Test
    public void testSortedResultsSupportDirectNextAndEmitFinalValueOnce() {
        OrderByStage stage = new OrderByStage();
        ByteString key = ByteString.copyFrom(IdUtil.asBytes(IdGenerator.of("key")));
        stage.init(List.of(key), List.of(key), true, true);
        try {
            assertNull(stage.handleIterator(result(3)));
            assertNull(stage.handleIterator(result(1)));
            assertNull(stage.handleIterator(result(2)));
            Iterator<PipelineResult> iterator = stage.handleIterator(PipelineResult.EMPTY);

            assertEquals(List.of(1), iterator.next().getKv().getKeys());
            assertTrue(iterator.hasNext());
            assertTrue(iterator.hasNext());
            assertEquals(List.of(2), iterator.next().getKv().getKeys());
            assertEquals(List.of(3), iterator.next().getKv().getKeys());
            assertSame(PipelineResult.EMPTY, iterator.next());
            assertFalse(iterator.hasNext());
            assertFalse(iterator.hasNext());
            assertThrows(NoSuchElementException.class, iterator::next);
            assertEquals(0, this.folder.getRoot().list().length);
        } finally {
            stage.close();
        }
    }

    private void verifyIoFailure(String operation) throws Exception {
        IOException cause = new IOException(operation + " failed");
        SortShuffle<MultiKv> shuffle = new SortShuffle<>(MultiKv::compareTo,
                                                       SortShuffleSerializer.ofMultiKvSerializer()) {
            @Override
            public void append(MultiKv value) throws IOException {
                if (operation.equals("append")) {
                    throw cause;
                }
                super.append(value);
            }

            @Override
            public void finish() throws IOException {
                if (operation.equals("finish")) {
                    throw cause;
                }
                super.finish();
            }

            @Override
            public Iterator<MultiKv> getIterator() throws IOException {
                if (operation.equals("iterator")) {
                    throw cause;
                }
                return super.getIterator();
            }
        };
        OrderByStage stage = stage(shuffle);
        try {
            PipelineResult input = operation.equals("append") ? result(1) : PipelineResult.EMPTY;
            UncheckedIOException failure = assertThrows(UncheckedIOException.class,
                                                        () -> stage.handleIterator(input));
            assertSame(cause, failure.getCause());
        } finally {
            stage.close();
        }
        assertEquals(0, this.folder.getRoot().list().length);
    }

    private static OrderByStage stage(SortShuffle<MultiKv> shuffle) throws Exception {
        OrderByStage stage = new OrderByStage();
        Field field = OrderByStage.class.getDeclaredField("sortShuffle");
        field.setAccessible(true);
        field.set(stage, shuffle);
        return stage;
    }

    private static PipelineResult result(int key) {
        return new PipelineResult(MultiKv.of(List.of(key), List.of(key)));
    }
}
