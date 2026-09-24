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

package org.apache.hugegraph.example;

import java.util.List;

import org.apache.hugegraph.HugeFactory;
import org.apache.hugegraph.util.Log;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;

/**
 * Perf test for: query vertices with indexes and limit
 */
public class PerfExample4 extends PerfExample3 {

    private static final Logger LOG = Log.logger(PerfExample4.class);

    /**
     * Main method
     *
     * @param args 3 arguments, 1st should be 1, meaning single thread,
     *             product of 2nd and 3rd is total number of "person" vertices
     * @throws InterruptedException
     */
    public static void main(String[] args) throws Exception {
        PerfExample4 tester = new PerfExample4();
        tester.test(args);

        // Stop daemon thread
        HugeFactory.shutdown(30L);
    }

    @Override
    protected void testQueryVertex(GraphManager graph,
                                   int threads,
                                   int thread,
                                   int multiple) {
        int total = threads * multiple * 100;
        for (int i = 1; i <= total; i *= 10) {
            LOG.info(">>>> limit {} <<<<", i);
            long current = System.currentTimeMillis();
            List<Vertex> persons = graph.traversal().V()
                                        .hasLabel("person")
                                        .limit(i).toList();
            assert persons.size() == i;
            LOG.info(">>>> query by label index, cost: {}ms", elapsed(current));

            current = System.currentTimeMillis();
            persons = graph.traversal().V()
                           .has("city", "Hongkong")
                           .limit(i)
                           .toList();
            assert persons.size() == i;
            LOG.info(">>>> query by secondary index, cost: {}ms",
                     elapsed(current));

            current = System.currentTimeMillis();
            persons = graph.traversal().V()
                           .has("age", 3)
                           .limit(i)
                           .toList();
            assert persons.size() == i;
            LOG.info(">>>> query by range index, cost: {}ms", elapsed(current));
        }
    }
}
