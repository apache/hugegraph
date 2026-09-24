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

import java.util.Iterator;

import org.apache.hugegraph.HugeFactory;
import org.apache.hugegraph.type.define.Directions;
import org.apache.hugegraph.util.Log;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.slf4j.Logger;

/**
 * Perf test for: query vertices/adj-edges by ids
 */
public class PerfExample5 extends PerfExample3 {

    private static final Logger LOG = Log.logger(PerfExample5.class);

    /**
     * Main method
     * @param args 3 arguments, 1st should be 1, meaning single thread,
     *             product of 2nd and 3rd is total number of "person" vertices
     * @throws InterruptedException
     */
    public static void main(String[] args) throws Exception {
        PerfExample5 tester = new PerfExample5();
        tester.test(args);

        // Stop daemon thread
        HugeFactory.shutdown(30L);
    }

    @Override
    protected void testQueryVertex(GraphManager graph,
                                   int threads,
                                   int thread,
                                   int multiple) {
        int totalV = 0;
        long start = System.currentTimeMillis();
        for (int i = 0; i < multiple; i++) {
            int j = 0;
            for (Object id : this.vertices) {
                if (j++ % threads != thread) {
                    continue;
                }
                graph.queryVertex(id);
                totalV++;
            }
        }
        long cost = elapsed(start);
        LOG.info("Query {} vertices with thread({}): {} vertices/s",
                 totalV, thread, totalV * 1000 / cost);
    }

    @Override
    protected void testQueryEdge(GraphManager graph,
                                 int threads,
                                 int thread,
                                 int multiple) {
        int totalV = 0;
        int totalE = 0;
        long start = System.currentTimeMillis();

        for (int i = 0; i < multiple; i++) {
            int j = 0;
            for (Object id : this.vertices) {
                if (j++ % threads != thread) {
                    continue;
                }

                Iterator<Edge> edges = graph.queryVertexEdge(id, Directions.OUT);
                while (edges.hasNext()) {
                    edges.next();
                    totalE++;
                }
                totalV++;
            }
        }
        long cost = elapsed(start);
        LOG.info("Query {} edges of vertices({}) with thread({}): " +
                 "{} vertices/s, {} edges/s",
                 totalE, totalV, thread,
                 totalV * 1000 / cost, totalE * 1000 / cost);
    }
}
