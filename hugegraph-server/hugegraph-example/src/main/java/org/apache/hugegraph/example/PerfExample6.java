/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hugegraph.example;

import org.apache.hugegraph.HugeFactory;
import org.apache.hugegraph.util.Log;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;

/**
 * Perf test for: query vertices/adj-edges by ids with edges insert
 */
public class PerfExample6 extends PerfExample5 {

    private static final Logger LOG = Log.logger(PerfExample6.class);

    /**
     * Main method
     * @param args 3 arguments, 1st should be 1, meaning single thread,
     *             product of 2nd and 3rd is total number of "person" vertices
     * @throws InterruptedException
     */
    public static void main(String[] args) throws Exception {
        PerfExample6 tester = new PerfExample6();
        tester.test(args);

        // Stop daemon thread
        HugeFactory.shutdown(30L);
    }

    @Override
    protected void testInsert(GraphManager graph, int times, int multiple) {
        final int TIMES = times * multiple;
        final int BATCH = 100;
        long total = 0;
        Vertex lastV = null;
        // Insert in order
        for (int i = 0; i < TIMES; i++) {
            for (int j = 0; j < BATCH; j++) {
                String name = String.format("p-%08d", total++);
                Vertex v = graph.addVertex(T.label, "person", "name", name,
                                           "city", "Hongkong", "age", 3);
                this.vertices.add(v.id());

                if (lastV != null) {
                    v.addEdge("knows", lastV);
                }
                lastV = v;
            }
            graph.tx().commit();
        }
        LOG.info("Insert {} vertices and {} edges", total, total - 1L);
    }
}
