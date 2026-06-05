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

package org.apache.hugegraph.unit.vectorIndex;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.unit.BaseUnitTest;
import org.apache.hugegraph.vector.AbstractVectorRuntime;
import org.apache.hugegraph.vector.UpdatableRandomAccessVectorValues;
import org.apache.hugegraph.vector.VectorRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.github.jbellis.jvector.graph.GraphIndexBuilder;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;

/**
 * Unit tests for AbstractVectorRuntime.
 * Uses a concrete TestVectorRuntime to test the abstract class's public API.
 */
public class AbstractVectorRuntimeTest extends BaseUnitTest {

    private static final VectorTypeSupport vts =
            VectorizationProvider.getInstance().getVectorTypeSupport();
    private static final int DIMENSION = 128;

    private Path testDir;
    private TestVectorRuntime runtime;

    @Before
    public void setup() throws IOException {
        Path parent = Paths.get("");
        testDir = Files.createTempDirectory(parent, "abstract_vector_runtime_test_");
        runtime = new TestVectorRuntime(testDir.toString());
    }

    @After
    public void cleanup() throws IOException {
        if (testDir != null && Files.exists(testDir)) {
            Files.walk(testDir)
                 .sorted(Comparator.reverseOrder())
                 .forEach(p -> {
                     try { Files.delete(p); } catch (Exception e) { }
                 });
        }
    }

    // ========== Lifecycle Tests ==========

    @Test
    public void testInitAndStop() throws IOException {
        runtime.init();
        runtime.stop();
        // No exception means success
    }

    // ========== Watermark & Sequence Tests ==========

    @Test
    public void testGetCurrentWaterMarkReturnsNegativeForNonexistent() {
        long watermark = runtime.getCurrentWaterMark("nonexistent");
        Assert.assertEquals(-1L, watermark);
    }

    @Test
    public void testGetNextVectorIdReturnsNegativeForNonexistent() {
        int vectorId = runtime.getNextVectorId("nonexistent");
        Assert.assertEquals(-1, vectorId);
    }

    @Test
    public void testGetNextSequenceReturnsNegativeForNonexistent() {
        long sequence = runtime.getNextSequence("nonexistent");
        Assert.assertEquals(-1L, sequence);
    }

    // ========== Update & Search Flow Tests ==========

    @Test
    public void testUpdateWithBuildingRecords() {

        String indexLabelId = "testIndex";
        List<VectorRecord> records = Arrays.asList(
            new VectorRecord(1, generateVector(DIMENSION), false, 1L),
            new VectorRecord(2, generateVector(DIMENSION), false, 2L)
        );

        runtime.update(indexLabelId, records.iterator());

        // test update the vector
        Assert.assertTrue(runtime.checkVectorIdExists(indexLabelId, 1));
        Assert.assertTrue(runtime.checkVectorIdExists(indexLabelId, 2));
    }

    @Test
    public void testUpdateWithDeletedRecords() {
        String indexLabelId = "testIndex";

        // First add records
        List<VectorRecord> addRecords = Arrays.asList(
            new VectorRecord(0, generateVector(DIMENSION), false, 1L)
        );
        runtime.update(indexLabelId, addRecords.iterator());

        // Then delete
        List<VectorRecord> deleteRecords = Arrays.asList(
            new VectorRecord(0, generateVector(DIMENSION), true, 2L)
        );

        runtime.update(indexLabelId, deleteRecords.iterator());
        // No exception means success
    }

    @Test
    public void testSearchReturnsIterator() {
        String indexLabelId = "testIndex";
        // Initialize context by update
        runtime.update(indexLabelId, Arrays.asList(
            new VectorRecord(0, generateVector(DIMENSION), false, 1L)
        ).iterator());

        Iterator<Integer> results = runtime.search(indexLabelId, generateVector(DIMENSION), 5);
        Assert.assertNotNull(results);
    }

    // ========== MetaData Tests ==========

    @Test
    public void testIsUpdateMetaDataReturnsFalseInitially() {
        String indexLabelId = "testIndex";
        runtime.update(indexLabelId, Arrays.<VectorRecord>asList().iterator());

        Assert.assertFalse(runtime.isUpdateMetaData(indexLabelId));
    }

    @Test
    public void testUpdateMetaDataChangesState() {
        String indexLabelId = "testIndex";
        runtime.update(indexLabelId, Arrays.<VectorRecord>asList().iterator());

        runtime.updateMetaData(indexLabelId, 100, 500L);

        Assert.assertTrue(runtime.isUpdateMetaData(indexLabelId));
        // getNextVectorId increments, so 100 -> 101
        Assert.assertEquals(101, runtime.getNextVectorId(indexLabelId));
    }

    @Test
    public void testUpdateMetaDataOnNonexistentIndexDoesNothing() {
        runtime.updateMetaData("nonexistent", 100, 500L);
        // Should not throw, and nonexistent still returns -1
        Assert.assertEquals(-1, runtime.getNextVectorId("nonexistent"));
    }

    // ========== Helper Methods ==========

    private float[] generateVector(int dimension) {
        float[] vector = new float[dimension];
        for (int i = 0; i < dimension; i++) {
            vector[i] = (float) Math.random();
        }
        return vector;
    }

    // ========== Test Implementation ==========

    /**
     * Concrete implementation of AbstractVectorRuntime for testing.
     * This simulates the runtime without requiring HugeGraphParams.
     */
    static class TestVectorRuntime extends AbstractVectorRuntime<String> {

        public TestVectorRuntime(String basePath) {
            super(basePath);
            this.basePath = basePath;
        }

        @Override
        public void update(String indexLabelId, Iterator<VectorRecord> records) {
            // Ensure context exists
            AbstractVectorRuntime.IndexContext<String> context = obtainContext(indexLabelId);
            while (records.hasNext()) {
                VectorRecord record = records.next();
                if (record.isDeleted()) {
                    if (context.builder.getGraph().containsNode(record.getVectorId())) {
                        context.builder.markNodeDeleted(record.getVectorId());
                    }
                } else {
                    VectorFloat<?> vector = vts.createFloatVector(record.getVectorData());
                    context.vectors.addNode(record.getVectorId(), vector);
                    context.builder.addGraphNode(record.getVectorId(), vector);
                }
            }
        }

        @Override
        public Iterator<Integer> search(String indexLabelId, float[] queryVector, int topK) {
            // Simple implementation for testing
            return java.util.Collections.emptyIterator();
        }

        @Override
        public boolean isUpdateMetaData(String indexLabelId) {
            AbstractVectorRuntime.IndexContext<String> context = getContext(indexLabelId);
            return context != null && context.metaData().isUpdateFromLog();
        }

        @Override
        protected AbstractVectorRuntime.IndexContext<String> createNewContext(String indexLabelId) {
            Map<Integer, VectorFloat<?>> map = new HashMap<>();
            UpdatableRandomAccessVectorValues ravv = new UpdatableRandomAccessVectorValues(map, DIMENSION);
            GraphIndexBuilder builder = new GraphIndexBuilder(
                    ravv, VectorSimilarityFunction.COSINE, 16, 100, 1.2f, 1.2f);
            AbstractVectorRuntime.IndexContext<String> context =
                    new AbstractVectorRuntime.IndexContext<String>(
                            indexLabelId, ravv, builder, 0L, DIMENSION, VectorSimilarityFunction.COSINE);
            vectorMap.put(indexLabelId, context);
            return context;
        }

        @Override
        protected String idToString(String id) {
            return id;
        }

        public boolean checkVectorIdExists(String id, int vector) {


            return this.vectorMap.get(id).builder.getGraph().containsNode(vector);
        }
    }
}

