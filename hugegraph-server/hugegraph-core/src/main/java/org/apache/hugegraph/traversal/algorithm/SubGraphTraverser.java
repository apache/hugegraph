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
import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.structure.HugeEdge;
import org.apache.hugegraph.type.define.Directions;
import org.apache.hugegraph.util.E;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import jakarta.ws.rs.core.MultivaluedMap;

public class SubGraphTraverser extends HugeTraverser {

    public SubGraphTraverser(HugeGraph graph) {
        super(graph);
    }

    private static boolean hasMultiEdges(List<Edge> edges, Id target) {
        boolean hasOutEdge = false;
        boolean hasInEdge = false;
        for (Edge edge : edges) {
            if (((HugeEdge) edge).id().otherVertexId().equals(target)) {
                if (((HugeEdge) edge).direction() == Directions.OUT) {
                    hasOutEdge = true;
                } else {
                    hasInEdge = true;
                }
                if (hasOutEdge && hasInEdge) {
                    return true;
                }
            }
        }
        return false;
    }

    public PathSet rays(Id sourceV, Directions dir, String label, int depth,
                        long degree, long capacity, long limit) {
        return this.subGraphPaths(sourceV, dir, label, depth, degree, capacity,
                                  limit, false, false);
    }

    public PathSet rings(Id sourceV, Directions dir, String label, int depth,
                         boolean sourceInRing, long degree, long capacity,
                         long limit) {
        return this.subGraphPaths(sourceV, dir, label, depth, degree, capacity,
                                  limit, true, sourceInRing);
    }

    private PathSet subGraphPaths(Id sourceV, Directions dir, String label,
                                  int depth, long degree, long capacity,
                                  long limit, boolean rings,
                                  boolean sourceInRing) {
        E.checkNotNull(sourceV, "source vertex id");
        this.checkVertexExist(sourceV, "source vertex");
        E.checkNotNull(dir, "direction");
        checkPositive(depth, "max depth");
        checkDegree(degree);
        checkCapacity(capacity);
        checkLimit(limit);

        Id labelId = this.getEdgeLabelIdOrNull(label);
        Traverser traverser = new Traverser(sourceV, labelId, depth, degree,
                                            capacity, limit, rings,
                                            sourceInRing);
        PathSet paths = new PathSet();
        do {
            paths.addAll(traverser.forward(dir));
        } while (--depth > 0 && !traverser.reachLimit() &&
                 !traverser.finished());
        this.vertexIterCounter.addAndGet(traverser.accessedVertices.size());
        this.edgeIterCounter.addAndGet(traverser.edgeCount);
        paths.setEdges(traverser.edgeRecord.getEdges(paths));
        return paths;
    }

    private static class RingPath extends Path {

        public RingPath(Id crosspoint, List<Id> vertices) {
            super(crosspoint, vertices);
        }

        @Override
        public int hashCode() {
            int hashCode = 0;
            for (Id id : this.vertices()) {
                hashCode ^= id.hashCode();
            }
            return hashCode;
        }

        /**
         * Compares the specified object with this path for equality.
         * Returns <tt>true</tt> if other path is equal to or
         * reversed of this path.
         *
         * @param other the object to be compared
         * @return <tt>true</tt> if the specified object is equal to or
         * reversed of this path
         */
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof RingPath)) {
                return false;
            }
            List<Id> vertices = this.vertices();
            List<Id> otherVertices = ((Path) other).vertices();

            if (vertices.equals(otherVertices)) {
                return true;
            }
            if (vertices.size() != otherVertices.size()) {
                return false;
            }
            for (int i = 0, size = vertices.size(); i < size; i++) {
                int j = size - i - 1;
                if (!vertices.get(i).equals(otherVertices.get(j))) {
                    return false;
                }
            }
            return true;
        }
    }

    private class Traverser {

        private final Id source;
        private final Id label;
        private final long degree;
        private final long capacity;
        private final long limit;
        private final boolean rings;
        private final boolean sourceInRing;
        private final Set<Id> accessedVertices = newIdSet();
        private final EdgeRecord edgeRecord;
        private MultivaluedMap<Id, Node> sources = newMultivalueMap();
        private int depth;
        private long pathCount;
        private long edgeCount;

        public Traverser(Id sourceV, Id label, int depth, long degree,
                         long capacity, long limit, boolean rings,
                         boolean sourceInRing) {
            this.source = sourceV;
            this.sources.add(sourceV, new Node(sourceV));
            this.accessedVertices.add(sourceV);
            this.label = label;
            this.depth = depth;
            this.degree = degree;
            this.capacity = capacity;
            this.limit = limit;
            this.rings = rings;
            this.sourceInRing = sourceInRing;
            this.pathCount = 0L;
            this.edgeCount = 0L;
            this.edgeRecord = new EdgeRecord(false);
        }

        /**
         * Search forward from source
         */
        public PathSet forward(Directions direction) {
            PathSet paths = new PathSet();
            MultivaluedMap<Id, Node> newVertices = newMultivalueMap();
            Iterator<Edge> edges;
            // Traversal vertices of previous level
            for (Map.Entry<Id, List<Node>> entry : this.sources.entrySet()) {
                Id vid = entry.getKey();
                // Record edgeList to determine if multiple edges exist
                List<Edge> edgeList = IteratorUtils.list(edgesOfVertex(
                        vid, direction, this.label, this.degree));
                edges = edgeList.iterator();

                if (!edges.hasNext()) {
                    // Reach the end, rays found
                    if (this.rings) {
                        continue;
                    }
                    for (Node n : entry.getValue()) {
                        // Store rays
                        paths.add(new Path(n.path()));
                        this.pathCount++;
                        if (this.reachLimit()) {
                            return paths;
                        }
                    }
                }

                int neighborCount = 0;
                Set<Id> currentNeighbors = newIdSet();
                while (edges.hasNext()) {
                    neighborCount++;
                    HugeEdge edge = (HugeEdge) edges.next();
                    this.edgeCount += 1L;
                    Id target = edge.id().otherVertexId();

                    this.edgeRecord.addEdge(vid, target, edge);

                    // Avoid deduplicate path
                    if (currentNeighbors.contains(target)) {
                        continue;
                    }
                    currentNeighbors.add(target);
                    this.accessedVertices.add(target);
                    for (Node node : entry.getValue()) {
                        // No ring, continue
                        if (!node.contains(target)) {
                            // Add node to next start-nodes
                            newVertices.add(target, new Node(target, node));
                            continue;
                        }

                        // Rays found if it's fake ring like:
                        // path is pattern: A->B<-A && A is only neighbor of B
                        boolean uniqueEdge = neighborCount == 1 &&
                                             !edges.hasNext();
                        boolean bothBack = target.equals(node.parent().id()) &&
                                           direction == Directions.BOTH;
                        if (!this.rings && bothBack && uniqueEdge) {
                            paths.add(new Path(node.path()));
                            this.pathCount++;
                            if (this.reachLimit()) {
                                return paths;
                            }
                        }

                        // Actual rings found
                        if (this.rings) {
                            boolean ringsFound = false;
                            // 1. sourceInRing is false, or
                            // 2. sourceInRing is true and target == source
                            if (!sourceInRing || target.equals(this.source)) {
                                if (!target.equals(node.parent().id())) {
                                    ringsFound = true;
                                } else if (direction != Directions.BOTH) {
                                    ringsFound = true;
                                } else if (hasMultiEdges(edgeList, target)) {
                                    ringsFound = true;
                                }
                            }

                            if (ringsFound) {
                                List<Id> path = node.path();
                                path.add(target);
                                paths.add(new RingPath(null, path));
                                this.pathCount++;
                                if (this.reachLimit()) {
                                    return paths;
                                }
                            }
                        }
                    }
                }
            }
            // Re-init sources
            this.sources = newVertices;

            if (!this.rings && --this.depth <= 0) {
                for (List<Node> list : newVertices.values()) {
                    for (Node n : list) {
                        paths.add(new Path(n.path()));
                        this.pathCount++;
                        if (this.reachLimit()) {
                            return paths;
                        }
                    }
                }
            }

            return paths;
        }

        private boolean reachLimit() {
            checkCapacity(this.capacity, this.accessedVertices.size(),
                          this.rings ? "rings" : "rays");
            return this.limit != NO_LIMIT && this.pathCount >= this.limit;
        }

        private boolean finished() {
            return this.sources.isEmpty();
        }
    }
}
