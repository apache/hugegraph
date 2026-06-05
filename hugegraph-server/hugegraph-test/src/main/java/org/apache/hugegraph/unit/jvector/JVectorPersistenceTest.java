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

package org.apache.hugegraph.unit.jvector;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.github.jbellis.jvector.disk.ReaderSupplier;
import io.github.jbellis.jvector.disk.SimpleMappedReader;
import io.github.jbellis.jvector.graph.GraphIndexBuilder;
import io.github.jbellis.jvector.graph.GraphSearcher;
import io.github.jbellis.jvector.graph.MapRandomAccessVectorValues;
import io.github.jbellis.jvector.graph.OnHeapGraphIndex;
import io.github.jbellis.jvector.graph.RandomAccessVectorValues;
import io.github.jbellis.jvector.graph.SearchResult;
import io.github.jbellis.jvector.graph.disk.OnDiskGraphIndex;
import io.github.jbellis.jvector.util.Bits;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;

/**
 * Test JVector OnDiskGraphIndex persistence
 */
public class JVectorPersistenceTest {

    private static final VectorTypeSupport vts =
            VectorizationProvider.getInstance().getVectorTypeSupport();
    private Path testDir;
    private final int dimension = 128;
    private final int vectorCount = 500;

    @Before
    public void setup() throws IOException {
        testDir = Files.createTempDirectory("jvector_test_");
        System.out.println("Test directory: " + testDir);
    }

    @After
    public void cleanup() throws IOException {
        // Keep files for inspection, comment out to auto-delete
        // Files.walk(testDir).sorted(Comparator.reverseOrder())
        //      .forEach(p -> { try { Files.delete(p); } catch (Exception e) {} });System.out.println("\n[Files kept in: " + testDir + "]");
    }

    @Test
    public void testPersistAndLoad() throws Exception {
        //1. Build vectors in memory
        System.out.println("\n=== Step 1: Build vectors ===");
        Map<Integer, VectorFloat<?>> vectorMap = new HashMap<>();
        Random random = new Random(42);

        for (int i = 0; i < vectorCount; i++) {
            float[] data = new float[dimension];
            for (int j = 0; j < dimension; j++) {
                data[j] = random.nextFloat();
            }
            vectorMap.put(i, vts.createFloatVector(data));
        }

        RandomAccessVectorValues ravv = new MapRandomAccessVectorValues(vectorMap, dimension);
        System.out.printf("Created %d vectors, dimension=%d%n", vectorCount, dimension);

        // 2. Build HNSW index (using existing API style from ServerVectorRuntime)
        System.out.println("\n=== Step 2: Build HNSW index ===");
        GraphIndexBuilder builder = new GraphIndexBuilder(
                ravv,
                VectorSimilarityFunction.COSINE,
                16,     // M
                100,    // beamWidth
                1.2f,   // neighborOverflow
                1.2f    // alpha
        );

        OnHeapGraphIndex graph = builder.getGraph();  // 从 GraphIndexBuilder 获取
        int upperBound = graph.getIdUpperBound();

        for (int i = 0; i < vectorCount; i++) {
            builder.addGraphNode(i, ravv);
        }
        System.out.println("Graph size: " + builder.getGraph().size());

        // 3. Persist to disk
        System.out.println("\n=== Step 3: Persist to disk ===");
        Path indexPath =  Files.createTempFile(testDir, "sift", ".inline");
        // write the index to disk with default options
        builder.cleanup();
        OnDiskGraphIndex.write(builder.getGraph(), ravv, indexPath);

        System.out.println("Persisted to: " + indexPath);

        // 4. List generated files
        System.out.println("\n=== Step 4: Generated files ===");
        Files.walk(testDir).filter(Files::isRegularFile)
             .forEach(p -> {
                 try {
                     long size = Files.size(p);
                     System.out.printf("%s: %,d bytes (%.2f KB)%n",
                                       p.getFileName(), size, size / 1024.0);
                 } catch (IOException e) {
                     e.printStackTrace();
                 }
             });

        // 5. Load from disk
        System.out.println("\n=== Step 5: Load from disk ===");
        ReaderSupplier readerSupplier = new SimpleMappedReader.Supplier(indexPath);
        OnDiskGraphIndex diskIndex = OnDiskGraphIndex.load(readerSupplier);

        System.out.println("Loaded index:");
        System.out.println("  - size: " + diskIndex.size());
        System.out.println("  - dimension: " + diskIndex.getDimension());

        // 6. Search test
        System.out.println("\n=== Step 6: Search test ===");
        float[] queryData = new float[dimension];
        for (int i = 0; i < dimension; i++) {
            queryData[i] = random.nextFloat();
        }
        VectorFloat<?> queryVector = vts.createFloatVector(queryData);

        // Get view for search
        RandomAccessVectorValues diskView = diskIndex.getView();

        SearchResult results = GraphSearcher.search(
                 queryVector,
                10,  // topK
                diskView,
                VectorSimilarityFunction.COSINE,
                diskIndex,
                Bits.ALL
        );

        System.out.println("Top 10 results:");
        for (SearchResult.NodeScore ns : results.getNodes()) {
            System.out.printf("  node=%d, score=%.4f%n", ns.node, ns.score);
        }

        // Cleanup
        readerSupplier.close();
        System.out.println("\n=== Test Complete ===");
    }
}
