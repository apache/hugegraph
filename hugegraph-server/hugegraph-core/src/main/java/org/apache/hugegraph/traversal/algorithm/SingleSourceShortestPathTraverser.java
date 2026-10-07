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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.backend.query.QueryResults;
import org.apache.hugegraph.structure.HugeEdge;
import org.apache.hugegraph.type.define.Directions;
import org.apache.hugegraph.util.CollectionUtil;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.InsertionOrderUtil;
import org.apache.hugegraph.util.NumericUtil;
import org.apache.tinkerpop.gremlin.structure.Edge;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

public class SingleSourceShortestPathTraverser extends HugeTraverser {

    public SingleSourceShortestPathTraverser(HugeGraph graph) {
        super(graph);
    }

    public WeightedPaths singleSourceShortestPaths(Id sourceV, Directions dir,
                                                   String label, String weight,
                                                   long degree, long skipDegree,
                                                   long capacity, long limit) {
        E.checkNotNull(sourceV, "source vertex id");
        this.checkVertexExist(sourceV, "source vertex");
        E.checkNotNull(dir, "direction");
        checkDegree(degree);
        checkCapacity(capacity);
        checkSkipDegree(skipDegree, degree, capacity);
        checkLimit(limit);

        Id labelId = this.getEdgeLabelIdOrNull(label);
        Traverser traverser = new Traverser(sourceV, dir, labelId, weight,
                                            degree, skipDegree, capacity, limit);
        while (true) {
            // Found, reach max depth or reach capacity, stop searching
            traverser.forward();
            if (traverser.done()) {
                this.vertexIterCounter.addAndGet(traverser.vertexCount);
                this.edgeIterCounter.addAndGet(traverser.edgeCount);
                WeightedPaths paths = traverser.shortestPaths();
                List<List<Id>> pathList = paths.pathList();
                Set<Edge> edges = new HashSet<>();
                for (List<Id> path : pathList) {
                    edges.addAll(traverser.edgeRecord.getEdges(path.iterator()));
                }
                paths.setEdges(edges);
                return paths;
            }
            checkCapacity(traverser.capacity, traverser.size, "shortest path");
        }
    }

    public NodeWithWeight weightedShortestPath(Id sourceV, Id targetV,
                                               Directions dir, String label,
                                               String weight, long degree,
                                               long skipDegree, long capacity) {
        E.checkNotNull(sourceV, "source vertex id");
        E.checkNotNull(targetV, "target vertex id");
        this.checkVertexExist(sourceV, "source vertex");
        this.checkVertexExist(targetV, "target vertex");
        E.checkNotNull(dir, "direction");
        E.checkNotNull(weight, "weight property");
        checkDegree(degree);
        checkCapacity(capacity);
        checkSkipDegree(skipDegree, degree, capacity);

        Id labelId = this.getEdgeLabelIdOrNull(label);
        Traverser traverser = new Traverser(sourceV, dir, labelId, weight,
                                            degree, skipDegree, capacity,
                                            NO_LIMIT);
        while (true) {
            traverser.forward();
            Map<Id, NodeWithWeight> results = traverser.shortestPaths();
            if (results.containsKey(targetV) || traverser.done()) {
                this.vertexIterCounter.addAndGet(traverser.vertexCount);
                this.edgeIterCounter.addAndGet(traverser.edgeCount);
                NodeWithWeight nodeWithWeight = results.get(targetV);
                if (nodeWithWeight != null) {
                    Iterator<Id> vertexIter = nodeWithWeight.node.path().iterator();
                    Set<Edge> edges = traverser.edgeRecord.getEdges(vertexIter);
                    nodeWithWeight.setEdges(edges);
                }
                return nodeWithWeight;
            }
            checkCapacity(traverser.capacity, traverser.size, "shortest path");
        }
    }

    public static class NodeWithWeight implements Comparable<NodeWithWeight> {

        private final double weight;
        private final Node node;

        private Set<Edge> edges = Collections.emptySet();

        public NodeWithWeight(double weight, Node node) {
            this.weight = weight;
            this.node = node;
        }

        public NodeWithWeight(double weight, Id id, NodeWithWeight prio) {
            this(weight, new Node(id, prio.node()));
        }

        public Set<Edge> getEdges() {
            return edges;
        }

        public void setEdges(Set<Edge> edges) {
            this.edges = edges;
        }

        public double weight() {
            return weight;
        }

        public Node node() {
            return this.node;
        }

        public Map<String, Object> toMap() {
            return ImmutableMap.of("weight", this.weight,
                                   "vertices", this.node().path());
        }

        @Override
        public int compareTo(NodeWithWeight other) {
            return Double.compare(this.weight, other.weight);
        }
    }

    public static class WeightedPaths extends LinkedHashMap<Id, NodeWithWeight> {

        private static final long serialVersionUID = -313873642177730993L;
        private Set<Edge> edges = Collections.emptySet();

        public Set<Edge> getEdges() {
            return edges;
        }

        public void setEdges(Set<Edge> edges) {
            this.edges = edges;
        }

        public Set<Id> vertices() {
            Set<Id> vertices = newIdSet();
            vertices.addAll(this.keySet());
            for (NodeWithWeight nw : this.values()) {
                vertices.addAll(nw.node().path());
            }
            return vertices;
        }

        public List<List<Id>> pathList() {
            List<List<Id>> pathList = new ArrayList<>();
            for (NodeWithWeight nw : this.values()) {
                pathList.add(nw.node.path());
            }
            return pathList;
        }

        public Map<Id, Map<String, Object>> toMap() {
            Map<Id, Map<String, Object>> results = newMap();
            for (Map.Entry<Id, NodeWithWeight> entry : this.entrySet()) {
                Id source = entry.getKey();
                NodeWithWeight nw = entry.getValue();
                Map<String, Object> result = nw.toMap();
                results.put(source, result);
            }
            return results;
        }
    }

    private class Traverser {

        private final Directions direction;
        private final Id label;
        private final String weight;
        private final long degree;
        private final long skipDegree;
        private final long capacity;
        private final long limit;
        private final WeightedPaths findingNodes = new WeightedPaths();
        private final WeightedPaths foundNodes = new WeightedPaths();
        private final EdgeRecord edgeRecord;
        private final Id source;
        private final long size;
        private Set<NodeWithWeight> sources;
        private long vertexCount;
        private long edgeCount;
        private boolean done = false;

        public Traverser(Id sourceV, Directions dir, Id label, String weight,
                         long degree, long skipDegree, long capacity, long limit) {
            this.source = sourceV;
            this.sources = ImmutableSet.of(new NodeWithWeight(
                    0D, new Node(sourceV, null)));
            this.direction = dir;
            this.label = label;
            this.weight = weight;
            this.degree = degree;
            this.skipDegree = skipDegree;
            this.capacity = capacity;
            this.limit = limit;
            this.size = 0L;
            this.vertexCount = 0L;
            this.edgeCount = 0L;
            this.edgeRecord = new EdgeRecord(false);
        }

        /**
         * Search forward from source
         */
        public void forward() {
            long degree = this.skipDegree > 0L ? this.skipDegree : this.degree;
            for (NodeWithWeight node : this.sources) {
                Iterator<Edge> edges = edgesOfVertex(node.node().id(),
                                                     this.direction,
                                                     this.label, degree);
                edges = this.skipSuperNodeIfNeeded(edges);
                while (edges.hasNext()) {
                    HugeEdge edge = (HugeEdge) edges.next();
                    Id target = edge.id().otherVertexId();

                    this.edgeCount += 1L;

                    if (this.foundNodes.containsKey(target) ||
                        this.source.equals(target)) {
                        // Already find shortest path for target, skip
                        continue;
                    }

                    this.edgeRecord.addEdge(node.node().id(), target, edge);

                    double currentWeight = this.edgeWeight(edge);
                    double weight = currentWeight + node.weight();
                    NodeWithWeight nw = new NodeWithWeight(weight, target, node);
                    NodeWithWeight exist = this.findingNodes.get(target);
                    if (exist == null || weight < exist.weight()) {
                        /*
                         * There are 2 scenarios to update finding nodes:
                         * 1. The 'target' found first time, add current path
                         * 2. Already exist path for 'target' and current
                         *    path is shorter, update path for 'target'
                         */
                        this.findingNodes.put(target, nw);
                    }
                }
            }
            this.vertexCount += sources.size();

            Map<Id, NodeWithWeight> sorted = CollectionUtil.sortByValue(
                    this.findingNodes, true);
            double minWeight = 0;
            Set<NodeWithWeight> newSources = InsertionOrderUtil.newSet();
            for (Map.Entry<Id, NodeWithWeight> entry : sorted.entrySet()) {
                Id id = entry.getKey();
                NodeWithWeight wn = entry.getValue();
                if (minWeight == 0) {
                    minWeight = wn.weight();
                } else if (wn.weight() > minWeight) {
                    break;
                }
                // Move shortest paths from 'findingNodes' to 'foundNodes'
                this.foundNodes.put(id, wn);
                if (this.limit != NO_LIMIT &&
                    this.foundNodes.size() >= this.limit) {
                    this.done = true;
                    return;
                }
                this.findingNodes.remove(id);
                // Re-init 'sources'
                newSources.add(wn);
            }
            this.sources = newSources;
            if (this.sources.isEmpty()) {
                this.done = true;
            }
        }

        public boolean done() {
            return this.done;
        }

        public WeightedPaths shortestPaths() {
            return this.foundNodes;
        }

        private double edgeWeight(HugeEdge edge) {
            double edgeWeight;
            if (this.weight == null ||
                !edge.property(this.weight).isPresent()) {
                edgeWeight = 1.0;
            } else {
                edgeWeight = NumericUtil.convertToNumber(
                        edge.value(this.weight)).doubleValue();
            }
            return edgeWeight;
        }

        private Iterator<Edge> skipSuperNodeIfNeeded(Iterator<Edge> edges) {
            if (this.skipDegree <= 0L) {
                return edges;
            }
            List<Edge> edgeList = newList();
            int count = 0;
            while (edges.hasNext()) {
                if (count < this.degree) {
                    edgeList.add(edges.next());
                }
                if (count >= this.skipDegree) {
                    return QueryResults.emptyIterator();
                }
                count++;
            }
            return edgeList.iterator();
        }
    }
}
