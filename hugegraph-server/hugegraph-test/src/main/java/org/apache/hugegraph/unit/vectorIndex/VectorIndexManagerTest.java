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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.unit.BaseUnitTest;
import org.apache.hugegraph.vector.VectorIndexManager;
import org.apache.hugegraph.vector.VectorIndexRuntime;
import org.apache.hugegraph.vector.VectorIndexStateStore;
import org.apache.hugegraph.vector.VectorRecord;
import org.apache.hugegraph.vector.VectorTaskScheduler;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for VectorIndexManager.
 * Tests the coordination between StateStore, Runtime, and Scheduler components.
 */
public class VectorIndexManagerTest extends BaseUnitTest {

    private MockVectorStateStore stateStore;
    private MockVectorRuntime runtime;
    private MockVectorScheduler scheduler;
    private VectorIndexManager<String> manager;

    @Before
    public void setup() {
        stateStore = new MockVectorStateStore();
        runtime = new MockVectorRuntime();
        scheduler = new MockVectorScheduler();
        manager = new VectorIndexManager<>(stateStore, runtime, scheduler);
    }

    // ========== Lifecycle Tests ==========

    @Test
    public void testInitCallsRuntimeInit() {
        manager.init();
        Assert.assertTrue(runtime.initCalled);
    }

    @Test
    public void testStopCallsBothRuntimeAndStateStore() throws IOException {
        manager.stop();
        Assert.assertTrue(runtime.stopCalled);
        Assert.assertTrue(stateStore.stopCalled);
    }

    // ========== Signal & ProcessIndex Flow Tests ==========

    @Test
    public void testSignalSchedulesTask() {
        manager.signal("indexLabel1");
        Assert.assertTrue(scheduler.taskExecuted);
        Assert.assertEquals(1, scheduler.executedTaskCount);
    }

    @Test
    public void testSignalTriggersProcessIndexFlow() {
        // Setup: stateStore has delta records, runtime has watermark
        runtime.watermark = 10L;
        stateStore.deltaRecords = Arrays.asList(
            new VectorRecord(1, new float[]{1.0f}, false, 11L),
            new VectorRecord(2, new float[]{2.0f}, false, 12L)
        );

        manager.signal("testIndex");

        // Verify: runtime.update() was called with records from stateStore
        Assert.assertTrue(runtime.updateCalled);
        Assert.assertEquals("testIndex", runtime.lastUpdatedIndexLabelId);
    }

    @Test
    public void testSignalWithNegativeWatermarkStartsFromZero() {
        runtime.watermark = -1L;  // No existing watermark
        stateStore.deltaRecords = Collections.singletonList(
            new VectorRecord(0, new float[]{1.0f}, false, 1L)
        );

        manager.signal("newIndex");

        Assert.assertTrue(runtime.updateCalled);
        // Verify scanDeltas was called with fromSeq=0
        Assert.assertEquals(0L, stateStore.lastScanFromSeq);
    }

    // ========== Search Flow Tests ==========

    @Test
    public void testSearchVectorReturnsVertexIds() {
        String indexLabelId = "testIndex";
        float[] queryVector = new float[]{1.0f, 2.0f, 3.0f};
        int topK = 5;

        // Setup: runtime returns vectorIds, stateStore maps to vertexIds
        runtime.searchResults = Arrays.asList(1, 2, 3).iterator();
        stateStore.vertexIds = new HashSet<>(Arrays.asList("v1", "v2", "v3"));

        Set<String> results = manager.searchVector(indexLabelId, queryVector, topK);

        Assert.assertNotNull(results);
        Assert.assertEquals(3, results.size());
        Assert.assertTrue(results.containsAll(Arrays.asList("v1", "v2", "v3")));
    }

    @Test
    public void testSearchVectorEmptyResults() {
        runtime.searchResults = Collections.emptyIterator();
        stateStore.vertexIds = Collections.emptySet();

        Set<String> results = manager.searchVector("testIndex", new float[]{1.0f}, 10);

        Assert.assertNotNull(results);
        Assert.assertTrue(results.isEmpty());
    }

    // ========== MetaData Sync Flow Tests ==========

    @Test
    public void testGetNextVectorIndexWhenMetaDataAlreadyUpdated() {
        runtime.isMetaDataUpdated = true;
        runtime.nextVectorId = 100;

        int nextId = manager.getNextVectorId("testIndex");

        Assert.assertEquals(100, nextId);
        Assert.assertFalse(stateStore.getCurrentMaxVectorIdCalled);
    }

    @Test
    public void testGetNextVectorIndexSyncsFromStateStore() {
        runtime.isMetaDataUpdated = false;
        runtime.nextVectorId = 50;
        stateStore.maxVectorId = 100;
        stateStore.maxSequence = 200L;

        manager.getNextVectorId("testIndex");

        // Verify: updateMetaData was called with values from stateStore
        Assert.assertTrue(runtime.updateMetaDataCalled);
    }

    @Test
    public void testGetNextSequenceWhenMetaDataAlreadyUpdated() {
        runtime.isMetaDataUpdated = true;
        runtime.nextSequence = 500L;

        long nextSeq = manager.getNextSequence("testIndex");

        Assert.assertEquals(500L, nextSeq);
        Assert.assertFalse(stateStore.getCurrentMaxSequenceCalled);
    }

    @Test
    public void testGetNextSequenceSyncsFromStateStore() {
        runtime.isMetaDataUpdated = false;
        runtime.nextSequence = 100L;
        stateStore.maxVectorId = 50;
        stateStore.maxSequence = 300L;

        long nextSequence = manager.getNextSequence("testIndex");

        Assert.assertEquals(301, nextSequence);
    }

    // ========== Mock Classes ==========

    static class MockVectorStateStore implements VectorIndexStateStore<String> {
        boolean stopCalled = false;
        Set<String> vertexIds = new HashSet<>();
        int maxVectorId = 0;
        long maxSequence = 0L;
        List<VectorRecord> deltaRecords = new ArrayList<>();
        long lastScanFromSeq = -1L;
        boolean getCurrentMaxVectorIdCalled = false;
        boolean getCurrentMaxSequenceCalled = false;

        @Override
        public void stop() { stopCalled = true; }

        @Override
        public Iterable<VectorRecord> scanDeltas(String indexLabelId, long fromSeq) {
            lastScanFromSeq = fromSeq;
            return deltaRecords;
        }

        @Override
        public Set<String> getVertex(String indexLabelId, Iterator<Integer> vectorIds) {
            return vertexIds;
        }

        @Override
        public int getCurrentMaxVectorId(String indexLabelId, int currentMaxVectorId) {
            getCurrentMaxVectorIdCalled = true;
            return Math.max(maxVectorId, currentMaxVectorId);
        }

        @Override
        public long getCurrentMaxSequence(String indexLabelId, long currentMaxSeq) {
            getCurrentMaxSequenceCalled = true;
            return Math.max(maxSequence, currentMaxSeq);
        }
    }

    static class MockVectorRuntime implements VectorIndexRuntime<String> {
        boolean initCalled = false;
        boolean stopCalled = false;
        boolean updateCalled = false;
        boolean updateMetaDataCalled = false;
        boolean isMetaDataUpdated = false;
        String lastUpdatedIndexLabelId = null;
        int nextVectorId = 0;
        long nextSequence = 0L;
        long watermark = 0L;
        Iterator<Integer> searchResults = Collections.emptyIterator();

        @Override
        public void init() { initCalled = true; }

        @Override
        public void stop() { stopCalled = true; }

        @Override
        public void update(String indexLabelId, Iterator<VectorRecord> records) {
            updateCalled = true;
            lastUpdatedIndexLabelId = indexLabelId;
            while (records.hasNext()) { records.next(); }
        }

        @Override
        public void flush(String indexLabelId) { }

        @Override
        public Iterator<Integer> search(String indexLabelId, float[] queryVector, int topK) {
            return searchResults;
        }

        @Override
        public long getCurrentWaterMark(String indexLabelId) { return watermark; }

        @Override
        public int getNextVectorId(String indexLabelId) { return nextVectorId; }

        @Override
        public long getNextSequence(String indexLabelId) { return nextSequence; }

        @Override
        public boolean isUpdateMetaData(String indexLabelId) { return isMetaDataUpdated; }

        @Override
        public void updateMetaData(String indexLabelId, int vectorId, long sequence) {
            updateMetaDataCalled = true;
            this.nextVectorId = vectorId;
            this.nextSequence = sequence;
            this.isMetaDataUpdated = true;
        }
    }

    static class MockVectorScheduler implements VectorTaskScheduler {
        boolean taskExecuted = false;
        int executedTaskCount = 0;

        @Override
        public void execute(Runnable task) {
            taskExecuted = true;
            executedTaskCount++;
            task.run();
        }
    }
}

