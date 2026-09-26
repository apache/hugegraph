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

package org.apache.hugegraph.unit.traversal;

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.config.CoreOptions;
import org.apache.hugegraph.traversal.algorithm.OltpTraverser;
import org.apache.hugegraph.type.define.CollectionType;
import org.apache.hugegraph.unit.BaseUnitTest;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.junit.After;
import org.junit.Test;
import org.mockito.Mockito;

public class OltpTraverserTest extends BaseUnitTest {

    @After
    public void teardown() {
        OltpTraverser.destroy();
    }

    @Test
    public void testCloseOpenTransaction() {
        HugeGraph graph = graph();
        Transaction tx = graph.tx();
        Mockito.when(tx.isOpen()).thenReturn(true);

        new TestTraverser(graph).close();

        Mockito.verify(tx).close();
    }

    @Test
    public void testCloseClosedTransaction() {
        HugeGraph graph = graph();
        Transaction tx = graph.tx();
        Mockito.when(tx.isOpen()).thenReturn(false);

        new TestTraverser(graph).close();

        Mockito.verify(tx, Mockito.never()).close();
    }

    private static HugeGraph graph() {
        HugeGraph graph = Mockito.mock(HugeGraph.class);
        Transaction tx = Mockito.mock(Transaction.class);
        Mockito.when(graph.tx()).thenReturn(tx);
        Mockito.when(graph.option(CoreOptions.OLTP_COLLECTION_TYPE))
               .thenReturn(CollectionType.JCF);
        Mockito.when(graph.option(CoreOptions.OLTP_CONCURRENT_THREADS))
               .thenReturn(1);
        return graph;
    }

    private static class TestTraverser extends OltpTraverser {

        TestTraverser(HugeGraph graph) {
            super(graph);
        }
    }
}
