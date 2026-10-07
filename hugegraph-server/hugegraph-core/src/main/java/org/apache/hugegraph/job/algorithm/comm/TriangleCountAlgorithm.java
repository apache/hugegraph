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

package org.apache.hugegraph.job.algorithm.comm;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang.mutable.MutableLong;
import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.id.IdGenerator;
import org.apache.hugegraph.job.UserJob;
import org.apache.hugegraph.structure.HugeEdge;
import org.apache.hugegraph.type.define.Directions;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.InsertionOrderUtil;
import org.apache.tinkerpop.gremlin.structure.Edge;

import com.google.common.collect.ImmutableMap;

public class TriangleCountAlgorithm extends AbstractCommAlgorithm {

    public static final String ALGO_NAME = "triangle_count";

    protected static int workersWhenBoth(Map<String, Object> parameters) {
        Directions direction = direction4Out(parameters);
        int workers = workers(parameters);
        E.checkArgument(direction == Directions.BOTH || workers <= 0,
                        "The workers must be not set when direction!=BOTH, " +
                        "but got workers=%s and direction=%s",
                        workers, direction);
        return workers;
    }

    @Override
    public String name() {
        return ALGO_NAME;
    }

    @Override
    public void checkParameters(Map<String, Object> parameters) {
        direction4Out(parameters);
        degree(parameters);
        workersWhenBoth(parameters);
    }

    @Override
    public Object call(UserJob<Object> job, Map<String, Object> parameters) {
        int workers = workersWhenBoth(parameters);
        try (Traverser traverser = new Traverser(job, workers)) {
            return traverser.triangleCount(direction4Out(parameters),
                                           degree(parameters));
        }
    }

    protected static class Traverser extends AlgoTraverser {

        protected static final String KEY_TRIANGLES = "triangles";
        protected static final String KEY_TRIADS = "triads";

        public Traverser(UserJob<Object> job, int workers) {
            super(job, ALGO_NAME, workers);
        }

        protected Traverser(UserJob<Object> job, String name, int workers) {
            super(job, name, workers);
        }

        protected static <V> Set<V> newOrderedSet() {
            return new TreeSet<>();
        }

        public Object triangleCount(Directions direction, long degree) {
            Map<String, Long> results = triangles(direction, degree);
            results = InsertionOrderUtil.newMap(results);
            results.remove(KEY_TRIADS);
            return results;
        }

        protected Map<String, Long> triangles(Directions direction,
                                              long degree) {
            if (direction == null || direction == Directions.BOTH) {
                return this.trianglesForBothDir(degree);
            }

            assert direction == Directions.OUT || direction == Directions.IN;

            Iterator<Edge> edges = this.edges(direction);

            long triangles = 0L;
            long triads = 0L;
            long totalEdges = 0L;
            long totalVertices = 0L;
            Id vertex = null;

            Set<Id> adjVertices = newOrderedSet();
            while (edges.hasNext()) {
                HugeEdge edge = (HugeEdge) edges.next();
                this.updateProgress(++totalEdges);

                Id source = edge.ownerVertex().id();
                Id target = edge.otherVertex().id();
                if (vertex == source) {
                    // Ignore and skip the target vertex if exceed degree
                    if (adjVertices.size() < degree || degree == NO_LIMIT) {
                        adjVertices.add(target);
                    }
                    continue;
                }

                if (vertex != null) {
                    assert vertex != source;
                    /*
                     * Find graph mode like this:
                     * A -> [B,C,D,E,F]
                     *      B -> [D,F]
                     *      E -> [B,C,F]
                     */
                    triangles += this.intersect(degree, adjVertices);
                    triads += this.localTriads(adjVertices.size());
                    totalVertices++;
                    // Reset for the next source
                    adjVertices = newOrderedSet();
                }
                vertex = source;
                adjVertices.add(target);
            }

            if (vertex != null) {
                triangles += this.intersect(degree, adjVertices);
                triads += this.localTriads(adjVertices.size());
                totalVertices++;
            }

            String suffix = "_" + direction.string();
            return ImmutableMap.of("edges" + suffix, totalEdges,
                                   "vertices" + suffix, totalVertices,
                                   KEY_TRIANGLES, triangles,
                                   KEY_TRIADS, triads);
        }

        protected Map<String, Long> trianglesForBothDir(long degree) {
            AtomicLong triangles = new AtomicLong(0L);
            AtomicLong triads = new AtomicLong(0L);
            AtomicLong totalEdges = new AtomicLong(0L);
            AtomicLong totalVertices = new AtomicLong(0L);

            this.traverse(null, null, v -> {
                Id source = (Id) v.id();

                MutableLong edgesCount = new MutableLong(0L);
                Set<Id> adjVertices = this.adjacentVertices(source, degree,
                                                            edgesCount);

                triangles.addAndGet(this.intersect(degree, adjVertices));
                triads.addAndGet(this.localTriads(adjVertices.size()));

                totalVertices.incrementAndGet();
                totalEdges.addAndGet(edgesCount.longValue());
            });

            assert totalEdges.get() % 2L == 0L;
            assert triangles.get() % 3L == 0L;
            // totalEdges /= 2L
            totalEdges.getAndAccumulate(2L, (l, w) -> l / w);
            // triangles /= 3L
            triangles.getAndAccumulate(3L, (l, w) -> l / w);
            // triads -= triangles * 2L
            triads.addAndGet(triangles.get() * -2L);

            return ImmutableMap.of("edges", totalEdges.get(),
                                   "vertices", totalVertices.get(),
                                   KEY_TRIANGLES, triangles.get(),
                                   KEY_TRIADS, triads.get());
        }

        private Set<Id> adjacentVertices(Id source, long degree,
                                         MutableLong edgesCount) {
            Iterator<Id> adjVertices = this.adjacentVertices(source,
                                                             Directions.BOTH,
                                                             (Id) null, degree);
            Set<Id> set = newOrderedSet();
            while (adjVertices.hasNext()) {
                edgesCount.increment();
                set.add(adjVertices.next());
            }
            return set;
        }

        protected long intersect(long degree, Set<Id> adjVertices) {
            long count = 0L;
            Directions dir = Directions.OUT;
            Id empty = IdGenerator.ZERO;
            Iterator<Id> vertices;
            for (Id v : adjVertices) {
                vertices = this.adjacentVertices(v, dir, (Id) null, degree);
                Id lastVertex = empty;
                while (vertices.hasNext()) {
                    Id vertex = vertices.next();
                    if (lastVertex.equals(vertex)) {
                        // Skip duplicated target vertex (through sortkeys)
                        continue;
                    }
                    lastVertex = vertex;

                    /*
                     * FIXME: deduplicate two edges with opposite directions
                     * between two specified adjacent vertices
                     */
                    if (adjVertices.contains(vertex)) {
                        count++;
                    }
                }
            }
            return count;
        }

        protected long localTriads(int size) {
            return size * (size - 1L) / 2L;
        }
    }
}
