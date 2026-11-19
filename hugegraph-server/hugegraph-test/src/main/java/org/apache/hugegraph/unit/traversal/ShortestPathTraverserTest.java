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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.id.EdgeId;
import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.id.IdGenerator;
import org.apache.hugegraph.config.CoreOptions;
import org.apache.hugegraph.structure.HugeEdge;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.traversal.algorithm.HugeTraverser;
import org.apache.hugegraph.traversal.algorithm.HugeTraverser.Path;
import org.apache.hugegraph.traversal.algorithm.HugeTraverser.PathSet;
import org.apache.hugegraph.traversal.algorithm.ShortestPathTraverser;
import org.apache.hugegraph.traversal.algorithm.records.ShortestPathRecords;
import org.apache.hugegraph.type.define.CollectionType;
import org.apache.hugegraph.type.define.Directions;
import org.apache.hugegraph.unit.BaseUnitTest;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.junit.Test;
import org.mockito.Mockito;

public class ShortestPathTraverserTest extends BaseUnitTest {

    @Test
    public void testManySparseIdsInOneLayer() {
        // Initialize the traverser's collection factory for PathSet.
        new TestTraverser();
        Id source = IdGenerator.of("person:7763");
        Id target = IdGenerator.of("person:17592186046524");
        ShortestPathRecords records = new ShortestPathRecords(source, target);
        records.startOneLayer(true);
        Assert.assertEquals(source, records.nextKey());
        Id middle = null;
        for (int i = 0; i < 1000; i++) {
            Id vertex = IdGenerator.of("person:" + (i * 1000000007L));
            if (i == 500) {
                middle = vertex;
            }
            Assert.assertTrue(records.findPath(vertex, id -> true, false,
                                               false).isEmpty());
        }
        records.finishOneLayer();
        records.startOneLayer(false);
        Assert.assertEquals(target, records.nextKey());
        PathSet paths = records.findPath(middle, id -> true, false, false);

        Assert.assertEquals(1, paths.size());
        Assert.assertEquals(Arrays.asList(source, middle, target),
                            paths.iterator().next().vertices());
    }

    @Test
    public void testShortestPathWithSparseStringIds() {
        Id source = IdGenerator.of("person:7763");
        Id first = IdGenerator.of("person:15393162799074");
        Id second = IdGenerator.of("person:17592186046524");
        Id target = IdGenerator.of("person:24189255811086");
        TrackingIterator forward = edges(edgeTo(first));
        TrackingIterator backward = edges(edgeTo(second));
        TrackingIterator next = edges(edgeTo(second));
        TestTraverser traverser = new TestTraverser(forward, backward, next);

        Path path = shortestPath(traverser, source, target, 3, 0L);

        Assert.assertEquals(Arrays.asList(source, first, second, target),
                            path.vertices());
        Assert.assertTrue(forward.closed());
        Assert.assertTrue(backward.closed());
        Assert.assertTrue(next.closed());
    }

    @Test
    public void testUnreachableSparseStringIds() {
        Id source = IdGenerator.of("person:30786325581562");
        Id target = IdGenerator.of("person:24189255811086");
        TrackingIterator forward = edges(edgeTo(IdGenerator.of("person:7763")));
        TrackingIterator backward = edges(edgeTo(IdGenerator.of("person:15393162799074")));
        TrackingIterator next = edges();
        TestTraverser traverser = new TestTraverser(forward, backward, next);

        Path path = shortestPath(traverser, source, target, 3, 0L);

        Assert.assertTrue(path.vertices().isEmpty());
        Assert.assertTrue(forward.closed());
        Assert.assertTrue(backward.closed());
        Assert.assertTrue(next.closed());
    }

    @Test
    public void testShortestPathWithMixedSparseIds() {
        Id source = IdGenerator.of(Integer.MAX_VALUE - 1L);
        Id middle = IdGenerator.of("person:7763");
        Id target = IdGenerator.of(-9876543210L);
        TestTraverser traverser = new TestTraverser(edges(edgeTo(middle)),
                                                    edges(edgeTo(middle)));

        Path path = shortestPath(traverser, source, target, 2, 0L);

        Assert.assertEquals(Arrays.asList(source, middle, target),
                            path.vertices());
    }

    @Test
    public void testShortestPathWithHashCollisionAndZeroId() {
        Id source = IdGenerator.of("Aa");
        Id middle = IdGenerator.of("BB");
        Id target = IdGenerator.of(0L);
        Assert.assertEquals(source.hashCode(), middle.hashCode());
        TestTraverser traverser = new TestTraverser(edges(edgeTo(middle)),
                                                    edges(edgeTo(middle)));

        Path path = shortestPath(traverser, source, target, 2, 0L);

        Assert.assertEquals(Arrays.asList(source, middle, target),
                            path.vertices());
    }

    @Test
    public void testShortestPathWithUnlimitedDegreeAndCapacity() {
        Id source = IdGenerator.of("person:21990232557366");
        Id middle = IdGenerator.of("person:21990232565255");
        Id target = IdGenerator.of("person:30786325581208");
        TestTraverser traverser = new TestTraverser(edges(edgeTo(middle)),
                                                    edges(edgeTo(middle)));

        Path path = traverser.shortestPath(source, target, Directions.OUT,
                                           Collections.emptyList(), 3, -1L,
                                           0L, -1L);

        Assert.assertEquals(Arrays.asList(source, middle, target),
                            path.vertices());
    }

    @Test
    public void testAllShortestPathsWithSparseStringIds() {
        Id source = IdGenerator.of("person:7763");
        Id first = IdGenerator.of("person:15393162799074");
        Id second = IdGenerator.of("person:17592186046524");
        Id target = IdGenerator.of("person:24189255811086");
        TestTraverser traverser = new TestTraverser(edges(edgeTo(first), edgeTo(second)),
                                                    edges(edgeTo(first), edgeTo(second)));

        PathSet paths = traverser.allShortestPaths(source, target, Directions.OUT,
                                                  Collections.emptyList(), 2, 2L, 0L, 100L);

        Assert.assertEquals(2, paths.size());
        for (Path path : paths) {
            Assert.assertTrue(path.vertices().equals(Arrays.asList(source, first, target)) ||
                              path.vertices().equals(Arrays.asList(source, second, target)));
        }
    }

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

    @Test
    public void testCloseEdgesWhenSkipDegreeReached() {
        Id source = IdGenerator.of(1L);
        Id target = IdGenerator.of(2L);
        Id other = IdGenerator.of(3L);
        TrackingIterator sourceEdges = edges(edgeTo(target), edgeTo(other));
        TestTraverser traverser = new TestTraverser(sourceEdges);

        Path path = shortestPath(traverser, source, target, 1, 2L);

        Assert.assertTrue(path.vertices().isEmpty());
        Assert.assertTrue(sourceEdges.closed());
    }

    @Test
    public void testCloseEdgesWhenTargetIsSuperNode() {
        Id source = IdGenerator.of(1L);
        Id target = IdGenerator.of(2L);
        Id other = IdGenerator.of(3L);
        TrackingIterator sourceEdges = edges(edgeTo(target));
        TrackingIterator targetEdges = edges(edgeTo(source), edgeTo(other));
        TestTraverser traverser = new TestTraverser(sourceEdges, targetEdges);

        Path path = shortestPath(traverser, source, target, 1, 2L);

        Assert.assertTrue(path.vertices().isEmpty());
        Assert.assertTrue(sourceEdges.closed());
        Assert.assertTrue(targetEdges.closed());
    }

    @Test
    public void testCloseEdgesThroughLabelAndLimitWrappers() {
        Id source = IdGenerator.of(1L);
        Id target = IdGenerator.of(2L);
        TrackingIterator backendEdges = edges(edgeTo(target));
        TestTraverser traverser = new TestTraverser(backendEdges);

        Path path = traverser.shortestPath(
                    source, target, Directions.OUT,
                    Collections.singletonList("link"), 1, 1L, 0L, 100L);

        Assert.assertEquals(Arrays.asList(source, target), path.vertices());
        Assert.assertTrue(backendEdges.closed());
    }

    @Test
    public void testCloseAllEdgesThroughWrappersWhenCloseFails() {
        Id source = IdGenerator.of(1L);
        Id target = IdGenerator.of(2L);
        RuntimeException closeFailure = new IllegalStateException("close");
        TrackingIterator first = new TrackingIterator(
                                 Arrays.<Edge>asList(edgeTo(target)).iterator(),
                                 null,
                                 closeFailure);
        TrackingIterator second = edges();
        TestTraverser traverser = new TestTraverser(first, second);

        Throwable actual = Assert.assertThrows(
                           RuntimeException.class,
                           () -> traverser.shortestPath(
                                 source, target, Directions.OUT,
                                 Arrays.asList("first", "second"),
                                 1, 1L, 0L, 100L));

        Assert.assertSame(closeFailure, actual.getCause());
        Assert.assertTrue(first.closed());
        Assert.assertTrue(second.closed());
    }

    @Test
    public void testCloseEdgesForAllShortestPaths() {
        Id source = IdGenerator.of(1L);
        Id target = IdGenerator.of(2L);
        TrackingIterator sourceEdges = edges(edgeTo(target));
        TestTraverser traverser = new TestTraverser(sourceEdges);

        PathSet paths = traverser.allShortestPaths(
                        source, target, Directions.OUT,
                        Collections.emptyList(), 1, 1L, 0L, 100L);

        Assert.assertEquals(1, paths.size());
        Assert.assertEquals(Arrays.asList(source, target),
                            paths.iterator().next().vertices());
        Assert.assertTrue(sourceEdges.closed());
    }

    @Test
    public void testCloseAllOpenedEdgesWhenMapLabelQueryFails() {
        Id source = IdGenerator.of(1L);
        RuntimeException closeFailure = new IllegalStateException("close");
        RuntimeException queryFailure = new IllegalArgumentException("query");
        TrackingIterator first = throwingCloseEdges(closeFailure);
        TrackingIterator second = edges();
        TestTraverser traverser = new TestTraverser(first, second,
                                                    queryFailure);
        Map<Id, String> labels = new LinkedHashMap<>();
        labels.put(IdGenerator.of(11L), "first");
        labels.put(IdGenerator.of(12L), "second");
        labels.put(IdGenerator.of(13L), "third");

        Throwable actual = Assert.assertThrows(
                           IllegalArgumentException.class,
                           () -> traverser.queryEdges(
                                 source, labels, HugeTraverser.NO_LIMIT));

        Assert.assertSame(queryFailure, actual);
        Assert.assertTrue(first.closed());
        Assert.assertTrue(second.closed());
        assertSuppressedCloseFailure(actual, closeFailure);
    }

    @Test
    public void testCloseOpenedEdgesWhenListLabelQueryFails() {
        Id source = IdGenerator.of(1L);
        RuntimeException queryFailure = new IllegalArgumentException("query");
        TrackingIterator first = edges();
        TestTraverser traverser = new TestTraverser(first, queryFailure);
        List<Id> labels = Arrays.asList(IdGenerator.of(11L),
                                       IdGenerator.of(12L));

        Throwable actual = Assert.assertThrows(
                           IllegalArgumentException.class,
                           () -> traverser.queryEdges(
                                 source, labels, HugeTraverser.NO_LIMIT));

        Assert.assertSame(queryFailure, actual);
        Assert.assertTrue(first.closed());
    }

    @Test
    public void testPreserveTraversalFailureWhenClosingEdgesFails() {
        Id source = IdGenerator.of(1L);
        Id target = IdGenerator.of(2L);
        RuntimeException traversalFailure =
                new IllegalArgumentException("hasNext");
        RuntimeException closeFailure = new IllegalStateException("close");
        TrackingIterator sourceEdges = throwingEdges(traversalFailure,
                                                     closeFailure);
        TestTraverser traverser = new TestTraverser(sourceEdges);

        Throwable actual = Assert.assertThrows(
                           IllegalArgumentException.class,
                           () -> shortestPath(traverser, source, target,
                                              1, 0L));

        Assert.assertSame(traversalFailure, actual);
        Assert.assertTrue(sourceEdges.closed());
        assertSuppressedCloseFailure(actual, closeFailure);
    }

    @Test
    public void testPreserveBackwardFailureWhenClosingEdgesFails() {
        Id source = IdGenerator.of(1L);
        Id middle = IdGenerator.of(2L);
        Id target = IdGenerator.of(3L);
        RuntimeException traversalFailure =
                new IllegalArgumentException("hasNext");
        RuntimeException closeFailure = new IllegalStateException("close");
        TrackingIterator forwardEdges = edges(edgeTo(middle));
        TrackingIterator backwardEdges = throwingEdges(traversalFailure,
                                                        closeFailure);
        TestTraverser traverser = new TestTraverser(forwardEdges,
                                                    backwardEdges);

        Throwable actual = Assert.assertThrows(
                           IllegalArgumentException.class,
                           () -> shortestPath(traverser, source, target,
                                              2, 0L));

        Assert.assertSame(traversalFailure, actual);
        Assert.assertTrue(forwardEdges.closed());
        Assert.assertTrue(backwardEdges.closed());
        assertSuppressedCloseFailure(actual, closeFailure);
    }

    @Test
    public void testPreserveSuperNodeFailureWhenClosingEdgesFails() {
        Id source = IdGenerator.of(1L);
        Id target = IdGenerator.of(2L);
        RuntimeException traversalFailure =
                new IllegalArgumentException("hasNext");
        RuntimeException closeFailure = new IllegalStateException("close");
        TrackingIterator sourceEdges = edges(edgeTo(target));
        TrackingIterator targetEdges = throwingEdges(traversalFailure,
                                                      closeFailure);
        TestTraverser traverser = new TestTraverser(sourceEdges, targetEdges);

        Throwable actual = Assert.assertThrows(
                           IllegalArgumentException.class,
                           () -> shortestPath(traverser, source, target,
                                              1, 2L));

        Assert.assertSame(traversalFailure, actual);
        Assert.assertTrue(sourceEdges.closed());
        Assert.assertTrue(targetEdges.closed());
        assertSuppressedCloseFailure(actual, closeFailure);
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

    private static TrackingIterator throwingCloseEdges(
                                    RuntimeException closeFailure) {
        return new TrackingIterator(Collections.emptyIterator(), null,
                                    closeFailure);
    }

    private static TrackingIterator throwingEdges(
                                    RuntimeException traversalFailure,
                                    RuntimeException closeFailure) {
        return new TrackingIterator(Collections.emptyIterator(),
                                    traversalFailure, closeFailure);
    }

    private static void assertSuppressedCloseFailure(
                        Throwable failure, RuntimeException closeFailure) {
        Assert.assertEquals(1, failure.getSuppressed().length);
        Assert.assertSame(closeFailure, failure.getSuppressed()[0].getCause());
    }

    private static HugeGraph mockGraph() {
        HugeGraph graph = Mockito.mock(HugeGraph.class);
        Mockito.when(graph.option(CoreOptions.OLTP_COLLECTION_TYPE))
               .thenReturn(CollectionType.JCF);
        return graph;
    }

    private static class TestTraverser extends ShortestPathTraverser {

        private final Deque<Object> edges;

        public TestTraverser(Object... edges) {
            super(mockGraph());
            this.edges = new ArrayDeque<>(Arrays.asList(edges));
        }

        @Override
        protected void checkVertexExist(Id vertexId, String name) {
            // Pass: iterator lifecycle is isolated from graph lookup.
        }

        @Override
        protected Id getEdgeLabelIdOrNull(Object label) {
            return label == null ? null : IdGenerator.of(label.toString());
        }

        @Override
        @SuppressWarnings("unchecked")
        protected Iterator<Edge> edgesOfVertex(Id source, Directions dir,
                                               Id label,
                                               long limit) {
            Object result = this.edges.removeFirst();
            if (result instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            if (result instanceof Error error) {
                throw error;
            }
            return (Iterator<Edge>) result;
        }

        public Iterator<Edge> queryEdges(Id source, Map<Id, String> labels,
                                         long limit) {
            return super.edgesOfVertex(source, Directions.OUT, labels, limit);
        }

        public Iterator<Edge> queryEdges(Id source, List<Id> labels,
                                         long limit) {
            return super.edgesOfVertex(source, Directions.OUT, labels, limit);
        }
    }

    private static class TrackingIterator implements Iterator<Edge>,
                                                     AutoCloseable {

        private final Iterator<Edge> edges;
        private final RuntimeException traversalFailure;
        private final RuntimeException closeFailure;
        private boolean closed;

        public TrackingIterator(Iterator<Edge> edges) {
            this(edges, null, null);
        }

        public TrackingIterator(Iterator<Edge> edges,
                                RuntimeException traversalFailure,
                                RuntimeException closeFailure) {
            this.edges = edges;
            this.traversalFailure = traversalFailure;
            this.closeFailure = closeFailure;
            this.closed = false;
        }

        @Override
        public boolean hasNext() {
            if (this.traversalFailure != null) {
                throw this.traversalFailure;
            }
            return this.edges.hasNext();
        }

        @Override
        public Edge next() {
            return this.edges.next();
        }

        @Override
        public void close() {
            this.closed = true;
            if (this.closeFailure != null) {
                throw this.closeFailure;
            }
        }

        public boolean closed() {
            return this.closed;
        }
    }
}
