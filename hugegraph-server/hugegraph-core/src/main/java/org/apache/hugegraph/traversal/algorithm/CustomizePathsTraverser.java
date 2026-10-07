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

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.structure.HugeEdge;
import org.apache.hugegraph.structure.HugeVertex;
import org.apache.hugegraph.traversal.algorithm.steps.WeightedEdgeStep;
import org.apache.hugegraph.util.CollectionUtil;
import org.apache.hugegraph.util.E;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import jakarta.ws.rs.core.MultivaluedMap;

public class CustomizePathsTraverser extends HugeTraverser {

    private final EdgeRecord edgeResults;

    public CustomizePathsTraverser(HugeGraph graph) {
        super(graph);
        this.edgeResults = new EdgeRecord(false);
    }

    public static List<Path> topNPath(List<Path> paths,
                                      boolean incr, long limit) {
        paths.sort((p1, p2) -> {
            WeightPath wp1 = (WeightPath) p1;
            WeightPath wp2 = (WeightPath) p2;
            int result = Double.compare(wp1.totalWeight(), wp2.totalWeight());
            return incr ? result : -result;
        });

        if (limit == NO_LIMIT || paths.size() <= limit) {
            return paths;
        }
        return paths.subList(0, (int) limit);
    }

    private static List<Node> sample(List<Node> nodes, long sample) {
        if (nodes.size() <= sample) {
            return nodes;
        }
        List<Node> result = newList((int) sample);
        int size = nodes.size();
        for (int random : CollectionUtil.randomSet(0, size, (int) sample)) {
            result.add(nodes.get(random));
        }
        return result;
    }

    public List<Path> customizedPaths(Iterator<Vertex> vertices,
                                      List<WeightedEdgeStep> steps, boolean sorted,
                                      long capacity, long limit) {
        E.checkArgument(vertices.hasNext(),
                        "The source vertices can't be empty");
        E.checkArgument(!steps.isEmpty(), "The steps can't be empty");
        checkCapacity(capacity);
        checkLimit(limit);

        MultivaluedMap<Id, Node> sources = newMultivalueMap();
        while (vertices.hasNext()) {
            HugeVertex vertex = (HugeVertex) vertices.next();
            Node node = sorted ?
                        new WeightNode(vertex.id(), null, 0) :
                        new Node(vertex.id(), null);
            sources.add(vertex.id(), node);
        }
        int stepNum = steps.size();
        int pathCount = 0;
        long access = 0;
        MultivaluedMap<Id, Node> newVertices = null;
        root:
        for (WeightedEdgeStep step : steps) {
            stepNum--;
            newVertices = newMultivalueMap();
            Iterator<Edge> edges;

            // Traversal vertices of previous level
            for (Map.Entry<Id, List<Node>> entry : sources.entrySet()) {
                List<Node> adjacency = newList();
                edges = this.edgesOfVertex(entry.getKey(), step.step());
                while (edges.hasNext()) {
                    HugeEdge edge = (HugeEdge) edges.next();
                    this.edgeIterCounter.addAndGet(1L);
                    Id target = edge.id().otherVertexId();

                    this.edgeResults.addEdge(entry.getKey(), target, edge);

                    for (Node n : entry.getValue()) {
                        // If have loop, skip target
                        if (n.contains(target)) {
                            continue;
                        }
                        Node newNode;
                        if (sorted) {
                            double w = step.weightBy() != null ?
                                       edge.value(step.weightBy().name()) :
                                       step.defaultWeight();
                            newNode = new WeightNode(target, n, w);
                        } else {
                            newNode = new Node(target, n);
                        }
                        adjacency.add(newNode);

                        checkCapacity(capacity, ++access, "customized paths");
                    }
                }

                if (step.sample() > 0) {
                    // Sample current node's adjacent nodes
                    adjacency = sample(adjacency, step.sample());
                }

                // Add current node's adjacent nodes
                for (Node node : adjacency) {
                    newVertices.add(node.id(), node);
                    // Avoid exceeding limit
                    if (stepNum == 0) {
                        if (limit != NO_LIMIT && !sorted &&
                            ++pathCount >= limit) {
                            break root;
                        }
                    }
                }
            }
            this.vertexIterCounter.addAndGet(sources.size());
            // Re-init sources
            sources = newVertices;
        }
        if (stepNum != 0) {
            return ImmutableList.of();
        }
        List<Path> paths = newList();
        if (newVertices == null) {
            return ImmutableList.of();
        }
        for (List<Node> nodes : newVertices.values()) {
            for (Node n : nodes) {
                if (sorted) {
                    WeightNode wn = (WeightNode) n;
                    paths.add(new WeightPath(wn.path(), wn.weights()));
                } else {
                    paths.add(new Path(n.path()));
                }
            }
        }
        return paths;
    }

    public EdgeRecord edgeResults() {
        return edgeResults;
    }

    public static class WeightNode extends Node {

        private final double weight;

        public WeightNode(Id id, Node parent, double weight) {
            super(id, parent);
            this.weight = weight;
        }

        public List<Double> weights() {
            List<Double> weights = newList();
            WeightNode current = this;
            while (current.parent() != null) {
                weights.add(current.weight);
                current = (WeightNode) current.parent();
            }
            Collections.reverse(weights);
            return weights;
        }
    }

    public static class WeightPath extends Path {

        private final List<Double> weights;
        private double totalWeight;

        public WeightPath(List<Id> vertices,
                          List<Double> weights) {
            super(vertices);
            this.weights = weights;
            this.calcTotalWeight();
        }

        public WeightPath(Id crosspoint, List<Id> vertices,
                          List<Double> weights) {
            super(crosspoint, vertices);
            this.weights = weights;
            this.calcTotalWeight();
        }

        public List<Double> weights() {
            return this.weights;
        }

        public double totalWeight() {
            return this.totalWeight;
        }

        @Override
        public void reverse() {
            super.reverse();
            Collections.reverse(this.weights);
        }

        @Override
        public Map<String, Object> toMap(boolean withCrossPoint) {
            if (withCrossPoint) {
                return ImmutableMap.of("crosspoint", this.crosspoint(),
                                       "objects", this.vertices(),
                                       "weights", this.weights());
            } else {
                return ImmutableMap.of("objects", this.vertices(),
                                       "weights", this.weights());
            }
        }

        private void calcTotalWeight() {
            double sum = 0;
            for (double w : this.weights()) {
                sum += w;
            }
            this.totalWeight = sum;
        }
    }
}
