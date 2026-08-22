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

package org.apache.hugegraph.store.meta;

import static org.apache.hugegraph.store.constant.HugeServerTables.VERTEX_TABLE;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.hugegraph.rocksdb.access.RocksDBSession;
import org.apache.hugegraph.rocksdb.access.SessionOperator;
import org.apache.hugegraph.store.UnitTestBase;
import org.apache.hugegraph.store.business.BusinessHandlerImpl;
import org.apache.hugegraph.store.meta.base.DBSessionBuilder;
import org.apache.hugegraph.store.term.Bits;
import org.apache.hugegraph.store.util.HgStoreException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.protobuf.Int64Value;

public class GraphIDManagerTest extends UnitTestBase {

    private static final int PARTITION_ID = 0;
    private static final int GRAPH_ID_LIMIT = 8;

    private String dbName;
    private File dbPath;
    private RocksDBSession session;
    private DBSessionBuilder sessionBuilder;
    private int previousMaxGraphId;

    @Before
    public void init() {
        this.previousMaxGraphId = GraphIdManager.maxGraphID;
        GraphIdManager.maxGraphID = GRAPH_ID_LIMIT;
        this.dbName = "graph-id-manager-" + System.nanoTime();
        this.dbPath = new File("target/graph-id-manager-test", this.dbName);
        Map<String, Object> config = new HashMap<>();
        config.put("rocksdb.write_buffer_size", "1048576");
        config.put("rocksdb.bloom_filter_bits_per_key", "10");
        BusinessHandlerImpl.initRocksdb(config, null);
        this.session = factory.createGraphDB(this.dbPath.getAbsolutePath(),
                                             this.dbName);
        this.sessionBuilder = partId -> this.session.clone();
    }

    @After
    public void clear() {
        GraphIdManager.maxGraphID = this.previousMaxGraphId;
        if (this.session != null) {
            this.session.close();
        }
        if (this.dbName != null) {
            factory.releaseGraphDB(this.dbName);
        }
        if (this.dbPath != null) {
            UnitTestBase.deleteDir(this.dbPath);
        }
    }

    @Test
    public void testAllocateBoundaryIdBeforeWrap() {
        GraphIdManager manager = this.newManager();

        Assert.assertEquals(0L, manager.getCId("boundary", 4));
        Assert.assertEquals(1L, manager.getCId("boundary", 4));
        Assert.assertEquals(2L, manager.getCId("boundary", 4));
        Assert.assertEquals(3L, manager.getCId("boundary", 4));
    }

    @Test
    public void testReturnMinusOneWhenAllIdsAreUsed() {
        GraphIdManager manager = this.newManager();

        for (int i = 0; i < 4; i++) {
            Assert.assertEquals(i, manager.getCId("full", 4));
        }
        Assert.assertEquals(-1L, manager.getCId("full", 4));
    }

    @Test
    public void testReuseReleasedId() {
        GraphIdManager manager = this.newManager();

        for (int i = 0; i < 4; i++) {
            Assert.assertEquals(i, manager.getCId("release", 4));
        }
        manager.delCId("release", 1L);

        Assert.assertEquals(1L, manager.getCId("release", 4));
        Assert.assertEquals(-1L, manager.getCId("release", 4));
    }

    @Test
    public void testWrapToBeginningAfterBoundary() {
        GraphIdManager manager = this.newManager();

        Assert.assertEquals(0L, manager.getCId("wrap", 4));
        Assert.assertEquals(1L, manager.getCId("wrap", 4));
        manager.delCId("wrap", 0L);
        Assert.assertEquals(2L, manager.getCId("wrap", 4));
        Assert.assertEquals(3L, manager.getCId("wrap", 4));

        Assert.assertEquals(0L, manager.getCId("wrap", 4));
    }

    @Test
    public void testPersistGraphIdAcrossManagerRestart() {
        GraphIdManager firstManager = this.newManager();

        Assert.assertEquals(0L, firstManager.getGraphIdOrCreate("first"));

        this.reopenDatabase();
        GraphIdManager restartedManager = this.newManager();
        Assert.assertEquals(0L, restartedManager.getGraphId("first"));
        Assert.assertEquals(1L, restartedManager.getGraphIdOrCreate("second"));
    }

    @Test
    public void testSkipGraphIdMappingWithoutSlot() {
        this.persistGraphId("existing", 0L);

        Assert.assertEquals(1L,
                            this.newManager().getGraphIdOrCreate("new"));
    }

    @Test
    public void testUpdateGraphIdsRejectsPersistedCollisionBeforeWriting() {
        this.persistGraphId("existing", 2L);
        Map<String, Long> graphIds = new LinkedHashMap<>();
        graphIds.put("graph-a", 1L);
        graphIds.put("graph-b", 2L);

        HgStoreException exception = Assert.assertThrows(
                HgStoreException.class,
                () -> this.newManager().updateGraphIds(graphIds));
        Assert.assertTrue(exception.getMessage().contains("existing"));
        Assert.assertTrue(exception.getMessage().contains("graph-b"));
        Assert.assertTrue(exception.getMessage().contains("2"));
        Assert.assertEquals(GRAPH_ID_LIMIT,
                            this.newManager().getGraphId("graph-a"));
        Assert.assertEquals(GRAPH_ID_LIMIT,
                            this.newManager().getGraphId("graph-b"));
    }

    @Test
    public void testUpdateGraphIdsPersistsMappingAndSlot() {
        GraphIdManager manager = this.newManager();

        manager.updateGraphIds(Collections.singletonMap("graph-a", 3L));

        Assert.assertEquals(3L, this.newManager().getGraphId("graph-a"));
        Int64Value slot = manager.get(
                Int64Value.parser(),
                manager.genCIDSlotKey(GraphIdManager.GRAPH_ID_PREFIX, 3L));
        Assert.assertNotNull(slot);
        Assert.assertEquals(3L, slot.getValue());
    }

    @Test
    public void testUpdateGraphIdsAllowsIdempotentAssignment() {
        GraphIdManager manager = this.newManager();
        Map<String, Long> graphIds =
                Collections.singletonMap("graph-a", 3L);

        manager.updateGraphIds(graphIds);
        manager.updateGraphIds(graphIds);

        Assert.assertEquals(3L, this.newManager().getGraphId("graph-a"));
    }

    @Test
    public void testReusePreviousGraphIdAfterRemapAndRelease() {
        GraphIdManager manager = this.newManager();

        manager.updateGraphIds(Collections.singletonMap("graph-a", 3L));
        manager.updateGraphIds(Collections.singletonMap("graph-a", 4L));
        Assert.assertEquals(4L, manager.releaseGraphId("graph-a"));
        manager.put(MetadataKeyHelper.getCidKey(GraphIdManager.GRAPH_ID_PREFIX),
                    Int64Value.of(3L));

        Assert.assertEquals(3L,
                            this.newManager().getGraphIdOrCreate("graph-b"));
    }

    @Test
    public void testKeepPreviousGraphIdSlotWhenStillAssigned() {
        GraphIdManager manager = this.newManager();
        manager.updateGraphIds(Collections.singletonMap("graph-a", 3L));
        this.persistGraphId("graph-b", 3L);

        manager.updateGraphIds(Collections.singletonMap("graph-a", 4L));

        Int64Value slot = manager.get(
                Int64Value.parser(),
                manager.genCIDSlotKey(GraphIdManager.GRAPH_ID_PREFIX, 3L));
        Assert.assertNotNull(slot);
        Assert.assertEquals(3L, slot.getValue());
        Assert.assertEquals(3L, this.newManager().getGraphId("graph-b"));
    }

    @Test
    public void testRejectPersistedGraphIdsOutsideAllocatableDomain() {
        long[] invalidGraphIds = {-1L, GRAPH_ID_LIMIT, 65536L};

        for (long graphId : invalidGraphIds) {
            String readGraph = "invalid-read-" + graphId;
            this.persistGraphId(readGraph, graphId);
            this.assertInvalidPersistedGraphId(
                    readGraph, graphId,
                    () -> this.newManager().getGraphId(readGraph));

            String writeGraph = "invalid-write-" + graphId;
            this.persistGraphId(writeGraph, graphId);
            this.assertInvalidPersistedGraphId(
                    writeGraph, graphId,
                    () -> this.newManager().getGraphIdOrCreate(writeGraph));
        }
    }

    @Test
    public void testSkipVertexDataWithoutGraphIdSlot() {
        GraphIdManager manager = this.newManager();
        this.persistVertexWithGraphId(0);

        Assert.assertEquals(1L, manager.getCId("stale-forward", 4));
    }

    @Test
    public void testSkipVertexDataWithoutGraphIdSlotAfterWrap() {
        String key = "stale-wrap";
        GraphIdManager manager = this.newManager();
        this.persistVertexWithGraphId(0);
        manager.put(MetadataKeyHelper.getCidKey(key), Int64Value.of(2L));
        manager.put(manager.genCIDSlotKey(key, 2L), Int64Value.of(2L));
        manager.put(manager.genCIDSlotKey(key, 3L), Int64Value.of(3L));

        Assert.assertEquals(1L, manager.getCId(key, 4));
    }

    @Test
    public void testGraphIdDomainExcludesMissingSentinel() {
        GraphIdManager manager = this.newManager();

        for (int i = 0; i < GRAPH_ID_LIMIT; i++) {
            Assert.assertEquals(i, manager.getGraphIdOrCreate("graph-" + i));
        }
        Assert.assertEquals(GRAPH_ID_LIMIT, manager.getGraphId("missing"));
        Assert.assertThrows(HgStoreException.class,
                            () -> manager.getGraphIdOrCreate("overflow"));
    }

    @Test
    public void testConcurrentCreateSameGraphReturnsSameId() throws Exception {
        int threadCount = 8;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<Long>> futures = new ArrayList<>(threadCount);

        try {
            for (int i = 0; i < threadCount; i++) {
                futures.add(executor.submit(() -> {
                    GraphIdManager manager = this.newManager();
                    start.await();
                    return manager.getGraphIdOrCreate("same-graph");
                }));
            }
            start.countDown();

            Set<Long> graphIds = new HashSet<>();
            for (Future<Long> future : futures) {
                graphIds.add(future.get(10L, TimeUnit.SECONDS));
            }
            Assert.assertEquals(1, graphIds.size());
            Assert.assertTrue(graphIds.contains(0L));
        } finally {
            executor.shutdownNow();
        }
    }

    private GraphIdManager newManager() {
        return new GraphIdManager(this.sessionBuilder, PARTITION_ID);
    }

    private void persistGraphId(String graphName, long graphId) {
        GraphIdManager manager = this.newManager();
        manager.put(MetadataKeyHelper.getGraphIDKey(graphName),
                    Int64Value.of(graphId));
        manager.flush();
    }

    private void persistVertexWithGraphId(int graphId) {
        this.session.createTables(VERTEX_TABLE);
        byte[] key = new byte[3];
        Bits.putShort(key, 0, graphId);
        key[2] = 1;

        SessionOperator operator = this.session.sessionOp();
        try {
            operator.prepare();
            operator.put(VERTEX_TABLE, key, new byte[]{1});
            operator.commit();
        } catch (RuntimeException e) {
            operator.rollback();
            throw e;
        }
    }

    private void assertInvalidPersistedGraphId(String graphName, long graphId,
                                               Runnable action) {
        HgStoreException exception =
                Assert.assertThrows(HgStoreException.class, action::run);
        Assert.assertTrue(exception.getMessage().contains("Invalid graph ID"));
        Assert.assertTrue(exception.getMessage().contains(String.valueOf(graphId)));
        Assert.assertTrue(exception.getMessage().contains(graphName));
    }

    private void reopenDatabase() {
        this.session.close();
        this.session = null;
        factory.releaseGraphDB(this.dbName);
        this.session = factory.createGraphDB(this.dbPath.getAbsolutePath(),
                                             this.dbName);
        this.sessionBuilder = partId -> this.session.clone();
    }
}
