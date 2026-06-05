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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.hugegraph.HugeFactory;
import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.HugeGraphParams;
import org.apache.hugegraph.backend.BackendException;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.id.IdGenerator;
import org.apache.hugegraph.dist.RegisterUtil;
import org.apache.hugegraph.masterelection.GlobalMasterInfo;
import org.apache.hugegraph.schema.IndexLabel;
import org.apache.hugegraph.testutil.Whitebox;
import org.apache.hugegraph.type.define.Cardinality;
import org.apache.hugegraph.type.define.DataType;
import org.apache.hugegraph.type.define.IndexType;
import org.apache.hugegraph.vector.ServerVectorRuntime;
import org.apache.hugegraph.vector.ServerVectorScheduler;
import org.apache.hugegraph.vector.ServerVectorStateStore;
import org.apache.hugegraph.vector.VectorIndexManager;
import org.apache.hugegraph.vector.VectorIndexRuntime;
import org.apache.hugegraph.vector.VectorIndexStateStore;
import org.apache.hugegraph.vector.VectorTaskScheduler;
import org.apache.tinkerpop.gremlin.structure.T;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * End-to-end integration test for the vector index feature.
 *
 * This test creates its OWN HugeGraph instance with RocksDB backend +
 * BinarySerializer at an ISOLATED temporary data directory.
 * It does NOT conflict with CoreTestSuite's graph.
 *
 * Run from the PROJECT ROOT directory:
 *   mvn test -pl hugegraph-server/hugegraph-test -am -P rocksdb \
 *     -Dtest=VectorIndexEndToEndIntegrationTest
 *
 * Test flow:
 *   1. Register backends, create isolated temp RocksDB dir
 *   2. Open HugeGraph (RocksDB+binary), clear+init
 *   3. Start server as master (for schema ops)
 *   4. Inject VectorIndexManager into StandardHugeGraph
 *   5. Create schema: PropertyKey(FLOAT,LIST), VertexLabel, VECTOR IndexLabel
 *   6. Add vertices → auto signal → processIndex → JVector HNSW build
 *   7. Search nearest neighbors and verify
 */
public class VectorIndexEndToEndIntegrationTest {

    private static final int DIMENSION = 128;
    private static final String INDEX_LABEL_NAME = "productByEmbedding";
    private static final String PROPERTY_KEY_NAME = "embedding";
    private static final String VERTEX_LABEL_NAME = "product";

    private static HugeGraph graph;
    private static HugeGraphParams params;
    private static VectorIndexManager<Id> vectorIndexManager;
    private static Path rocksdbDir;
    private static Path jvectorDir;
    private static ExecutorService schedulerExecutor;

    @BeforeClass
    public static void setup() throws Exception {
        // 1. Register backend providers (must happen before opening)
        try {
            RegisterUtil.registerBackends();
        } catch (BackendException e) {
            if (!e.getMessage().contains("Exists BackendStoreProvider")) {
                throw e;
            }
            // Already registered by suite, skip
        }

        // 2. Create ISOLATED temp directories (unique absolute paths)
        rocksdbDir = Files.createTempDirectory("vector_e2e_rocksdb_");
        jvectorDir = Files.createTempDirectory("vector_e2e_jvector_");

        // 3. Load filtered hugegraph.properties and override paths
        PropertiesConfiguration config = loadProperties();
        config.setProperty("store", "vector_integration_test");
        config.setProperty("rocksdb.data_path", rocksdbDir.toString());
        config.setProperty("rocksdb.wal_path", rocksdbDir.toString());

        // 4. Open HugeGraph
        graph = HugeFactory.open(config);
        graph.clearBackend();
        graph.initBackend();
        graph.serverStarted(GlobalMasterInfo.master("server-test"));

        // 5. Get params and inject VectorIndexManager
        params = Whitebox.getInternalState(graph, "params");
        initVectorIndexManager();

        // 6. Create vector schema
        createVectorSchema();
    }

    @AfterClass
    public static void cleanup() throws Exception {
        Exception ex = null;
        if (vectorIndexManager != null) {
            try { vectorIndexManager.stop(); } catch (Exception e) { ex = e; }
        }
        if (schedulerExecutor != null) schedulerExecutor.shutdownNow();
        if (graph != null) {
            try { graph.clearBackend(); } catch (Exception ignored) { }
            try { graph.close(); } catch (Exception ignored) { }
        }
        // Remove temp directories
        for (Path dir : new Path[]{rocksdbDir, jvectorDir}) {
            if (dir != null && Files.exists(dir)) {
                try { Files.walk(dir).sorted(Comparator.reverseOrder())
                           .forEach(p -> { try { Files.delete(p); } catch (Exception ignored) { } });
                } catch (Exception ignored) { }
            }
        }
        if (ex != null) throw ex;
    }

    private static PropertiesConfiguration loadProperties() throws Exception {
        String confPath = System.getProperty("config_path", "hugegraph.properties");
        java.net.URL url = VectorIndexEndToEndIntegrationTest.class
                .getClassLoader().getResource(confPath);
        assertNotNull("Config not found: " + confPath, url);
        return new Configurations().properties(new File(url.toURI()));
    }

    private static void initVectorIndexManager() {
        VectorIndexStateStore<Id> stateStore = new ServerVectorStateStore(params);
        VectorIndexRuntime<Id> runtime = new ServerVectorRuntime(
                jvectorDir.toString(), params);
        schedulerExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "vector-scheduler");
            t.setDaemon(true);
            return t;
        });
        VectorTaskScheduler scheduler = new ServerVectorScheduler(
                schedulerExecutor, params.graphEventHub());
        vectorIndexManager = new VectorIndexManager<>(stateStore, runtime, scheduler);
        vectorIndexManager.init();
        Whitebox.setInternalState(graph, "vectorIndexManager", vectorIndexManager);
    }

    private static void createVectorSchema() {
        graph.schema().propertyKey(PROPERTY_KEY_NAME)
             .dataType(DataType.FLOAT).cardinality(Cardinality.LIST).create();
        graph.schema().propertyKey("name")
             .dataType(DataType.TEXT).cardinality(Cardinality.SINGLE).create();
        graph.schema().vertexLabel(VERTEX_LABEL_NAME)
             .properties("name", PROPERTY_KEY_NAME)
             .useCustomizeNumberId().enableLabelIndex(true).create();
        // VECTOR IndexLabel: must use rebuild(false) since VECTOR indexes
        // cannot use the standard async rebuild task.
        graph.schema().indexLabel(INDEX_LABEL_NAME)
             .onV(VERTEX_LABEL_NAME).by(PROPERTY_KEY_NAME).vector()
             .userdata("dimension", DIMENSION)
             .userdata("similarityFunction", "COSINE")
             .rebuild(false).create();
    }

    // ══════════════════════ Tests ══════════════════════

    @Test
    public void testCompleteWorkflow() throws Exception {
        for (int i = 0; i < 10; i++) {
            graph.addVertex(T.label, VERTEX_LABEL_NAME, T.id, 1000L + i,
                            "name", "p-" + i, PROPERTY_KEY_NAME,
                            toFloatList(generateRandomVector(DIMENSION, i)));
        }
        graph.tx().commit();
        Thread.sleep(1_000); // Wait for async EventHub signal processing

        Id ilId = graph.schema().getIndexLabel(INDEX_LABEL_NAME).id();
        Set<Id> results = vectorIndexManager.searchVector(
                ilId, generateRandomVector(DIMENSION, 0), 5);

        assertNotNull(results);
        assertTrue("≥1 result, got " + results.size(), results.size() > 0);
        assertTrue("≤topK=5, got " + results.size(), results.size() <= 5);
    }

    @Test
    public void testSelfSearchReturnsSelf() throws Exception {
        // Add 10 vectors, then search for a specific one
        long targetVid = 2005L;
        float[] targetVec = generateRandomVector(DIMENSION, 105);

        for (int i = 0; i < 10; i++) {
            float[] vec = i == 5 ? targetVec : generateRandomVector(DIMENSION, 100 + i);
            graph.addVertex(T.label, VERTEX_LABEL_NAME, T.id, 2000L + i,
                            "name", "bulk-" + i, PROPERTY_KEY_NAME,
                            toFloatList(vec));
        }
        graph.tx().commit();
        Thread.sleep(1_000);

        Id ilId = graph.schema().getIndexLabel(INDEX_LABEL_NAME).id();
        Set<Id> results = vectorIndexManager.searchVector(ilId, targetVec, 5);
        assertNotNull(results);
        assertTrue("≥1 result, got " + results.size(), results.size() > 0);
        assertTrue("Expected target vertex " + targetVid + " in results: " + results,
                   results.contains(IdGenerator.of(targetVid)));
    }

    @Test
    public void testIncrementalAddAndSearch() throws Exception {
        long c = 8000L;
        for (int i = 0; i < 5; i++)
            graph.addVertex(T.label, VERTEX_LABEL_NAME, T.id, c++,
                            "name", "b1-" + i, PROPERTY_KEY_NAME,
                            toFloatList(generateRandomVector(DIMENSION, 100 + i)));
        graph.tx().commit();
        Thread.sleep(500);

        for (int i = 0; i < 5; i++)
            graph.addVertex(T.label, VERTEX_LABEL_NAME, T.id, c++,
                            "name", "b2-" + i, PROPERTY_KEY_NAME,
                            toFloatList(generateRandomVector(DIMENSION, 200 + i)));
        graph.tx().commit();
        Thread.sleep(500);

        Id ilId = graph.schema().getIndexLabel(INDEX_LABEL_NAME).id();
        Set<Id> results = vectorIndexManager.searchVector(
                ilId, generateRandomVector(DIMENSION, 100), 10);
        assertNotNull(results);
        assertTrue("≥1 result", results.size() >= 1);
    }

    @Test
    public void testVectorIndexLabelCreated() {
        IndexLabel il = graph.schema().getIndexLabel(INDEX_LABEL_NAME);
        assertNotNull(il);
        assertEquals(IndexType.VECTOR, il.indexType());
        assertEquals(DIMENSION, il.userdata().get("dimension"));
        assertEquals("COSINE", il.userdata().get("similarityFunction"));
    }

    // ══════════════════════ Helpers ══════════════════════

    private static float[] generateRandomVector(int dim, long seed) {
        Random rng = new Random(seed);
        float[] v = new float[dim];
        float sum = 0f;
        for (int i = 0; i < dim; i++) { v[i] = rng.nextFloat(); sum += v[i] * v[i]; }
        float norm = (float) Math.sqrt(sum);
        for (int i = 0; i < dim; i++) v[i] /= norm;
        return v;
    }

    private static List<Float> toFloatList(float[] arr) {
        List<Float> list = new ArrayList<>(arr.length);
        for (float v : arr) list.add(v);
        return list;
    }
}
