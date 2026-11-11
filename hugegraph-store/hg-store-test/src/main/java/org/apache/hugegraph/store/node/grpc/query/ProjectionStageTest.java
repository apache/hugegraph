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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.hugegraph.rocksdb.access.RocksDBSession.BackendColumn;
import org.apache.hugegraph.rocksdb.access.ScanIterator;
import org.apache.hugegraph.store.business.itrv2.MapLimitIterator;
import org.apache.hugegraph.store.grpc.query.QueryRequest;
import org.apache.hugegraph.store.node.grpc.query.model.PipelineResult;
import org.apache.hugegraph.store.node.grpc.query.model.QueryPlan;
import org.apache.hugegraph.store.node.grpc.query.stages.EarlyStopException;
import org.apache.hugegraph.store.node.grpc.query.stages.ProjectionStage;
import org.junit.Test;

/**
 * MapLimitIterator keeps every column it emits as a key of its dedup set, so a later stage must
 * not change that column in place. An id-only scan runs each emitted row through ProjectionStage
 * with every property removed before the next row is read.
 */
public class ProjectionStageTest {

    private static final int KEYS = 100;

    @Test
    public void testRemoveAllPropertyLeavesInputColumnUnchanged() {
        byte[] name = name(7);
        byte[] value = new byte[]{1, 2, 3};
        BackendColumn input = BackendColumn.of(name, value);

        PipelineResult result = removeAllProperty().handle(new PipelineResult(input));

        assertArrayEquals(name, result.getColumn().name);
        assertArrayEquals(new byte[0], result.getColumn().value);
        assertSame(value, input.value);
        assertArrayEquals(new byte[]{1, 2, 3}, input.value);
    }

    @Test
    public void testLimitDedupStillDropsCopiesAfterProjection() {
        ProjectionStage projection = removeAllProperty();
        ScanIterator dedup = new MapLimitIterator<BackendColumn>(scanTwice());

        Set<Integer> keys = new HashSet<>();
        try {
            while (dedup.hasNext()) {
                BackendColumn column = dedup.next();
                PipelineResult result = projection.handle(new PipelineResult(column));
                int key = ByteBuffer.wrap(result.getColumn().name).getInt();
                assertTrue("key " + key + " emitted twice", keys.add(key));
            }
        } finally {
            dedup.close();
        }

        assertEquals(IntStream.range(0, KEYS).boxed().collect(Collectors.toSet()), keys);
    }

    @Test
    public void testLimitDedupStillDropsCopiesThroughIdOnlyPlan() throws EarlyStopException {
        // sample_factor defaults to 0.0, which builds a stop-only plan
        QueryRequest request = QueryRequest.newBuilder()
                                           .setSampleFactor(1.0)
                                           .setNullProperty(true)
                                           .build();
        QueryPlan plan = QueryUtil.buildPlan(request);
        assertEquals("QueryPlan{stages=[PROJECTION_STAGE]}", plan.toString());
        ScanIterator dedup = new MapLimitIterator<BackendColumn>(scanTwice());

        Set<Integer> keys = new HashSet<>();
        try {
            while (dedup.hasNext()) {
                BackendColumn column = dedup.next();
                PipelineResult result = (PipelineResult) plan.execute(new PipelineResult(column));
                assertArrayEquals(new byte[0], result.getColumn().value);
                int key = ByteBuffer.wrap(result.getColumn().name).getInt();
                assertTrue("key " + key + " emitted twice", keys.add(key));
            }
        } finally {
            dedup.close();
        }

        assertEquals(IntStream.range(0, KEYS).boxed().collect(Collectors.toSet()), keys);
    }

    private static ProjectionStage removeAllProperty() {
        ProjectionStage stage = new ProjectionStage();
        stage.init(List.of(), true);
        return stage;
    }

    /**
     * Each key appears twice, each time as a newly allocated column with its own arrays.
     */
    private static ScanIterator scanTwice() {
        List<BackendColumn> rows = new ArrayList<>();
        for (int round = 0; round < 2; round++) {
            for (int key = 0; key < KEYS; key++) {
                rows.add(BackendColumn.of(name(key), name(key)));
            }
        }
        return new ListScanIterator(rows);
    }

    private static byte[] name(int key) {
        return ByteBuffer.allocate(Integer.BYTES).putInt(key).array();
    }

    private static class ListScanIterator implements ScanIterator {

        private final Iterator<BackendColumn> rows;

        private ListScanIterator(List<BackendColumn> rows) {
            this.rows = rows.iterator();
        }

        @Override
        public boolean hasNext() {
            return this.rows.hasNext();
        }

        @Override
        public boolean isValid() {
            return this.rows.hasNext();
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T> T next() {
            return (T) this.rows.next();
        }

        @Override
        public void close() {
        }
    }
}
