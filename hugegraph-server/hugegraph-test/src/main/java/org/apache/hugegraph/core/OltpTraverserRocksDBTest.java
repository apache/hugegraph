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

package org.apache.hugegraph.core;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.configuration2.MapConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.hugegraph.HugeFactory;
import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.backend.store.rocksdb.RocksDBSessions;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.testutil.Utils;
import org.apache.hugegraph.testutil.Whitebox;
import org.apache.hugegraph.traversal.algorithm.KneighborTraverser;
import org.apache.hugegraph.traversal.algorithm.OltpTraverser;
import org.apache.hugegraph.type.define.Directions;
import org.apache.hugegraph.util.Consumers;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;

public class OltpTraverserRocksDBTest {

    @Before
    public void requireRocksDB() {
        Assume.assumeTrue("Native lease regression requires the RocksDB profile",
                          "rocksdb".equals(Utils.getConf().getString("backend")));
        CoreTestSuite.initEnv();
    }

    @Test
    public void testKneighborReturnsWorkerLeasesAndAllowsImmediateReopen() throws Exception {
        this.checkNativeCleanup(false);
    }

    @Test
    public void testKneighborPreservesPendingCallerTransaction() throws Exception {
        this.checkNativeCleanup(true);
    }

    private void checkNativeCleanup(boolean pendingCaller) throws Exception {
        Path directory = Files.createTempDirectory("oltp-bfs-rocks-");
        HugeConfig config = new HugeConfig(new MapConfiguration(Collections.emptyMap()));
        config.setProperty("backend", "rocksdb");
        config.setProperty("serializer", "binary");
        config.setProperty("store", "oltp_bfs");
        config.setProperty("oltp.concurrent_threads", 1);
        config.setProperty("rocksdb.data_path", directory.toString());
        config.setProperty("rocksdb.wal_path", directory.toString());
        OltpTraverser.destroy();
        HugeGraph graph;
        try {
            graph = HugeFactory.open(config);
        } catch (RuntimeException | Error failure) {
            FileUtils.deleteDirectory(directory.toFile());
            throw failure;
        }
        ExecutorService worker = null;
        Consumers.ExecutorPool executors = null;
        RocksDBSessions sessions = null;
        try {
            graph.initBackend();
            graph.schema().propertyKey("name").asText().create();
            graph.schema().vertexLabel("person").properties("name").primaryKeys("name").create();
            graph.schema().edgeLabel("knows").sourceLabel("person").targetLabel("person").create();
            Vertex a = graph.addVertex(T.label, "person", "name", "a");
            Vertex b = graph.addVertex(T.label, "person", "name", "b");
            Vertex c = graph.addVertex(T.label, "person", "name", "c");
            a.addEdge("knows", b);
            b.addEdge("knows", c);
            graph.tx().commit();
            HugeFactory.closeCurrentThreadTransactions();
            Vertex pending = pendingCaller ? graph.addVertex(T.label, "person", "name", "pending") : null;
            try (KneighborTraverser traverser = new KneighborTraverser(graph)) {
                // Pending writes prohibit a limited backend query; use unlimited degree for that case.
                long degree = pendingCaller ? OltpTraverser.NO_LIMIT : 1L;
                Assert.assertEquals(Set.of(b.id(), c.id()),
                                    traverser.kneighbor((Id) a.id(), Directions.OUT, "knows", 2, degree, 100L));
            }
            Object store = graph.storeProvider().loadGraphStore(config);
            sessions = Whitebox.getInternalState(store, "sessions");
            Map<Long, ?> leases = Whitebox.getInternalState(sessions, "sessions");
            executors = Whitebox.getInternalState(OltpTraverser.class, "executors");
            worker = executors.getExecutor();
            worker.submit(() -> {
                Assert.assertFalse(graph.tx().isOpen());
                Assert.assertFalse("OLTP worker retained a native session despite closing its owner",
                                   leases.containsKey(Thread.currentThread().getId()));
            }).get(10L, TimeUnit.SECONDS);
            if (pendingCaller) {
                Assert.assertTrue(graph.tx().isOpen());
                this.assertVertexName(graph, pending.id(), "pending");
                graph.tx().commit();
                this.assertVertexName(graph, pending.id(), "pending");
            }
            HugeFactory.closeCurrentThreadTransactions();
            graph.close();
            // Preserve the original directory and all CFs: the first open must reuse this database.
            Path data = directory.resolve("g");
            Assert.assertTrue(Files.isRegularFile(data.resolve("CURRENT")));
            List<byte[]> families;
            try (Options options = new Options()) {
                families = RocksDB.listColumnFamilies(options, data.toString());
            }
            Assert.assertTrue("Fixture must contain the original graph CFs", families.size() > 1);
            try (DBOptions options = new DBOptions().setCreateIfMissing(false);
                 ColumnFamilyOptions familyOptions = new ColumnFamilyOptions()) {
                List<ColumnFamilyDescriptor> descriptors = new ArrayList<>();
                for (byte[] family : families) {
                    descriptors.add(new ColumnFamilyDescriptor(family, familyOptions));
                }
                List<ColumnFamilyHandle> handles = new ArrayList<>();
                RocksDB reopened = null;
                try {
                    reopened = RocksDB.open(options, data.toString(), descriptors, handles);
                    Assert.assertTrue(reopened.isOwningHandle());
                    Assert.assertEquals(families.size(), handles.size());
                } finally {
                    try {
                        for (ColumnFamilyHandle handle : handles) {
                            handle.close();
                        }
                    } finally {
                        if (reopened != null) {
                            reopened.close();
                        }
                    }
                }
            }
        } finally {
            try {
                if (worker != null) {
                    RocksDBSessions cleanupSessions = sessions;
                    try {
                        worker.submit(() -> {
                            HugeFactory.closeCurrentThreadTransactions();
                            // Repair leaked leases only in teardown so the failing baseline releases native locks.
                            ThreadLocal<?> local = Whitebox.getInternalState(cleanupSessions, "threadLocalSession");
                            for (int i = 0; local.get() != null && i < 8; i++) {
                                cleanupSessions.close();
                            }
                            Assert.assertNull(local.get());
                        }).get(10L, TimeUnit.SECONDS);
                    } finally {
                        executors.returnExecutor(worker);
                    }
                }
            } finally {
                try {
                    HugeFactory.closeCurrentThreadTransactions();
                    if (!graph.closed()) {
                        graph.drop();
                    }
                } finally {
                    HugeFactory.remove(graph);
                    OltpTraverser.destroy();
                    FileUtils.deleteDirectory(directory.toFile());
                }
            }
        }
    }

    private void assertVertexName(HugeGraph graph, Object id, String name) {
        Iterator<Vertex> vertices = graph.vertices(id);
        try {
            Assert.assertTrue(vertices.hasNext());
            Assert.assertEquals(name, vertices.next().value("name"));
            Assert.assertFalse(vertices.hasNext());
        } finally {
            CloseableIterator.closeIterator(vertices);
        }
    }
}
