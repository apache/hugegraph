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

package org.apache.hugegraph.traversal.algorithm;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.perf.PerfUtil.Watched;
import org.apache.hugegraph.structure.HugeEdge;
import org.apache.hugegraph.traversal.algorithm.records.ShortestPathRecords;
import org.apache.hugegraph.traversal.algorithm.steps.EdgeStep;
import org.apache.hugegraph.type.define.Directions;
import org.apache.hugegraph.util.E;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import com.google.common.collect.ImmutableList;

public class ShortestPathTraverser extends HugeTraverser {

    public ShortestPathTraverser(HugeGraph graph) {
        super(graph);
    }

    @Watched
    public Path shortestPath(Id sourceV, Id targetV, Directions dir,
                             List<String> labels, int depth, long degree,
                             long skipDegree, long capacity) {
        E.checkNotNull(sourceV, "source vertex id");
        E.checkNotNull(targetV, "target vertex id");
        this.checkVertexExist(sourceV, "source vertex");
        this.checkVertexExist(targetV, "target vertex");
        E.checkNotNull(dir, "direction");
        checkPositive(depth, "max depth");
        checkDegree(degree);
        checkCapacity(capacity);
        checkSkipDegree(skipDegree, degree, capacity);

        if (sourceV.equals(targetV)) {
            return new Path(ImmutableList.of(sourceV));
        }

        Map<Id, String> labelMap = newMap(labels.size());
        for (String label : labels) {
            labelMap.put(this.getEdgeLabelIdOrNull(label), label);
        }
        Traverser traverser = new Traverser(sourceV, targetV, dir, labelMap,
                                            degree, skipDegree, capacity);
        PathSet paths;
        while (true) {
            // Found, reach max depth or reach capacity, stop searching
            if (!(paths = traverser.forward(false)).isEmpty() ||
                --depth <= 0) {
                break;
            }
            checkCapacity(traverser.capacity, traverser.accessed(),
                          "shortest path");

            if (!(paths = traverser.backward(false)).isEmpty() ||
                --depth <= 0) {
                break;
            }
            checkCapacity(traverser.capacity, traverser.accessed(),
                          "shortest path");
        }

        this.vertexIterCounter.addAndGet(traverser.vertexCount);
        this.edgeIterCounter.addAndGet(traverser.pathResults.accessed());

        Path path = paths.isEmpty() ? Path.EMPTY : paths.iterator().next();

        Set<Edge> edges = traverser.edgeResults.getEdges(path);
        path.setEdges(edges);
        return path;
    }

    public Path shortestPath(Id sourceV, Id targetV, EdgeStep step,
                             int depth, long capacity) {
        return this.shortestPath(sourceV, targetV, step.direction(),
                                 newList(step.labels().values()),
                                 depth, step.degree(), step.skipDegree(),
                                 capacity);
    }

    public PathSet allShortestPaths(Id sourceV, Id targetV, Directions dir,
                                    List<String> labels, int depth, long degree,
                                    long skipDegree, long capacity) {
        E.checkNotNull(sourceV, "source vertex id");
        E.checkNotNull(targetV, "target vertex id");
        this.checkVertexExist(sourceV, "source vertex");
        this.checkVertexExist(targetV, "target vertex");
        E.checkNotNull(dir, "direction");
        checkPositive(depth, "max depth");
        checkDegree(degree);
        checkCapacity(capacity);
        checkSkipDegree(skipDegree, degree, capacity);

        PathSet paths = new PathSet();
        if (sourceV.equals(targetV)) {
            paths.add(new Path(ImmutableList.of(sourceV)));
            return paths;
        }

        Map<Id, String> labelMap = newMap(labels.size());
        for (String label : labels) {
            labelMap.put(this.getEdgeLabelIdOrNull(label), label);
        }
        Traverser traverser = new Traverser(sourceV, targetV, dir, labelMap,
                                            degree, skipDegree, capacity);
        while (true) {
            paths = traverser.traverse(true);
            // Found, reach max depth or reach capacity, stop searching
            if (!paths.isEmpty() || --depth <= 0) {
                break;
            }
            checkCapacity(traverser.capacity, traverser.accessed(),
                          "shortest path");
        }

        this.vertexIterCounter.addAndGet(traverser.vertexCount);
        this.edgeIterCounter.addAndGet(traverser.pathResults.accessed());

        paths.setEdges(traverser.edgeResults.getEdges(paths));
        return paths;
    }

    private class Traverser {

        private final ShortestPathRecords pathResults;
        private final EdgeRecord edgeResults;
        private final Directions direction;
        private final Map<Id, String> labels;
        private final long degree;
        private final long skipDegree;
        private final long capacity;
        private long vertexCount;

        public Traverser(Id sourceV, Id targetV, Directions dir,
                         Map<Id, String> labels, long degree,
                         long skipDegree, long capacity) {
            this.pathResults = new ShortestPathRecords(sourceV, targetV);
            this.edgeResults = new EdgeRecord(false);
            this.direction = dir;
            this.labels = labels;
            this.degree = degree;
            this.skipDegree = skipDegree;
            this.capacity = capacity;
            this.vertexCount = 0L;
        }

        public PathSet traverse(boolean all) {
            return this.pathResults.sourcesLessThanTargets() ?
                   this.forward(all) : this.backward(all);
        }

        /**
         * Search forward from source
         */
        @Watched
        public PathSet forward(boolean all) {
            PathSet results = new PathSet();
            long degree = this.skipDegree > 0L ? this.skipDegree : this.degree;

            this.pathResults.startOneLayer(true);
            while (this.pathResults.hasNextKey()) {
                Id source = this.pathResults.nextKey();

                Iterator<Edge> sourceEdges = edgesOfVertex(
                        source, this.direction, this.labels, degree);
                Throwable failure = null;
                try {
                    Iterator<Edge> edges = skipSuperNodeIfNeeded(
                            sourceEdges, this.degree, this.skipDegree);

                    this.vertexCount += 1L;

                    while (edges.hasNext()) {
                        HugeEdge edge = (HugeEdge) edges.next();
                        Id target = edge.id().otherVertexId();

                        this.edgeResults.addEdge(source, target, edge);

                        PathSet paths = this.pathResults.findPath(
                                target,
                                t -> !this.superNode(t, this.direction),
                                all, false);

                        if (paths.isEmpty()) {
                            continue;
                        }
                        results.addAll(paths);
                        if (!all) {
                            return paths;
                        }
                    }
                } catch (RuntimeException | Error e) {
                    failure = e;
                    throw e;
                } finally {
                    closeIterator(sourceEdges, failure);
                }
            }

            this.pathResults.finishOneLayer();

            return results;
        }

        /**
         * Search backward from target
         */
        @Watched
        public PathSet backward(boolean all) {
            PathSet results = new PathSet();
            long degree = this.skipDegree > 0L ? this.skipDegree : this.degree;
            Directions opposite = this.direction.opposite();

            this.pathResults.startOneLayer(false);
            while (this.pathResults.hasNextKey()) {
                Id source = this.pathResults.nextKey();

                Iterator<Edge> sourceEdges = edgesOfVertex(
                        source, opposite, this.labels, degree);
                Throwable failure = null;
                try {
                    Iterator<Edge> edges = skipSuperNodeIfNeeded(
                            sourceEdges, this.degree, this.skipDegree);

                    this.vertexCount += 1L;

                    while (edges.hasNext()) {
                        HugeEdge edge = (HugeEdge) edges.next();
                        Id target = edge.id().otherVertexId();

                        this.edgeResults.addEdge(source, target, edge);

                        PathSet paths = this.pathResults.findPath(
                                target, t -> !this.superNode(t, opposite),
                                all, false);

                        if (paths.isEmpty()) {
                            continue;
                        }
                        results.addAll(paths);
                        if (!all) {
                            return results;
                        }
                    }
                } catch (RuntimeException | Error e) {
                    failure = e;
                    throw e;
                } finally {
                    closeIterator(sourceEdges, failure);
                }
            }

            // Re-init targets
            this.pathResults.finishOneLayer();

            return results;
        }

        private boolean superNode(Id vertex, Directions direction) {
            if (this.skipDegree <= 0L) {
                return false;
            }
            Iterator<Edge> edges = edgesOfVertex(vertex, direction,
                                                 this.labels, this.skipDegree);
            Throwable failure = null;
            try {
                return IteratorUtils.count(edges) >= this.skipDegree;
            } catch (RuntimeException | Error e) {
                failure = e;
                throw e;
            } finally {
                closeIterator(edges, failure);
            }
        }

        private long accessed() {
            return this.pathResults.accessed();
        }
    }
}
