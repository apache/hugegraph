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

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.Map;

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.backend.id.EdgeId;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.id.IdGenerator;
import org.apache.hugegraph.config.CoreOptions;
import org.apache.hugegraph.structure.HugeEdge;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.traversal.algorithm.HugeTraverser.Path;
import org.apache.hugegraph.traversal.algorithm.ShortestPathTraverser;
import org.apache.hugegraph.type.define.CollectionType;
import org.apache.hugegraph.type.define.Directions;
import org.apache.hugegraph.unit.BaseUnitTest;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.junit.Test;
import org.mockito.Mockito;

public class ShortestPathTraverserTest extends BaseUnitTest {

    @Test
    public void testCloseEdgesWhenPathFoundForward() {
        Id source = IdGenerator.of(1L);
        Id target = IdGenerator.of(2L);
        TrackingIterator edges = edges(edgeTo(target));
        TestTraverser traverser = new TestTraverser(edges);

        Path path = shortestPath(traverser, source, target, 1, 0L);

        Assert.assertEquals(Arrays.asList(source, target), path.vertices());
        Assert.assertTrue(edges.closed());
    }

    @Test
    public void testCloseEdgesWhenPathFoundBackward() {
        Id source = IdGenerator.of(1L);
        Id middle = IdGenerator.of(2L);
        Id target = IdGenerator.of(3L);
        TrackingIterator forwardEdges = edges(edgeTo(middle));
        TrackingIterator backwardEdges = edges(edgeTo(middle));
        TestTraverser traverser = new TestTraverser(forwardEdges,
                                                    backwardEdges);

        Path path = shortestPath(traverser, source, target, 2, 0L);

        Assert.assertEquals(Arrays.asList(source, middle, target),
                            path.vertices());
        Assert.assertTrue(forwardEdges.closed());
        Assert.assertTrue(backwardEdges.closed());
    }

    @Test
    public void testCloseEdgesWhenCheckingSuperNode() {
        Id source = IdGenerator.of(1L);
        Id target = IdGenerator.of(2L);
        TrackingIterator sourceEdges = edges(edgeTo(target));
        TrackingIterator targetEdges = edges();
        TestTraverser traverser = new TestTraverser(sourceEdges, targetEdges);

        Path path = shortestPath(traverser, source, target, 1, 2L);

        Assert.assertEquals(Arrays.asList(source, target), path.vertices());
        Assert.assertTrue(sourceEdges.closed());
        Assert.assertTrue(targetEdges.closed());
    }

    private static Path shortestPath(TestTraverser traverser, Id source,
                                     Id target, int depth, long skipDegree) {
        return traverser.shortestPath(source, target, Directions.OUT,
                                      Collections.emptyList(), depth, 1L,
                                      skipDegree, 100L);
    }

    private static HugeEdge edgeTo(Id target) {
        HugeEdge edge = Mockito.mock(HugeEdge.class);
        EdgeId edgeId = Mockito.mock(EdgeId.class);
        Mockito.when(edge.id()).thenReturn(edgeId);
        Mockito.when(edgeId.otherVertexId()).thenReturn(target);
        return edge;
    }

    private static TrackingIterator edges(Edge... edges) {
        return new TrackingIterator(Arrays.asList(edges).iterator());
    }

    private static HugeGraph mockGraph() {
        HugeGraph graph = Mockito.mock(HugeGraph.class);
        Mockito.when(graph.option(CoreOptions.OLTP_COLLECTION_TYPE))
               .thenReturn(CollectionType.JCF);
        return graph;
    }

    private static class TestTraverser extends ShortestPathTraverser {

        private final Deque<Iterator<Edge>> edges;

        @SafeVarargs
        public TestTraverser(Iterator<Edge>... edges) {
            super(mockGraph());
            this.edges = new ArrayDeque<>(Arrays.asList(edges));
        }

        @Override
        protected void checkVertexExist(Id vertexId, String name) {
            // Pass: iterator lifecycle is isolated from graph lookup.
        }

        @Override
        protected Iterator<Edge> edgesOfVertex(Id source, Directions dir,
                                               Map<Id, String> labels,
                                               long limit) {
            return this.edges.removeFirst();
        }
    }

    private static class TrackingIterator implements Iterator<Edge>,
                                                     AutoCloseable {

        private final Iterator<Edge> edges;
        private boolean closed;

        public TrackingIterator(Iterator<Edge> edges) {
            this.edges = edges;
            this.closed = false;
        }

        @Override
        public boolean hasNext() {
            return this.edges.hasNext();
        }

        @Override
        public Edge next() {
            return this.edges.next();
        }

        @Override
        public void close() {
            this.closed = true;
        }

        public boolean closed() {
            return this.closed;
        }
    }
}
