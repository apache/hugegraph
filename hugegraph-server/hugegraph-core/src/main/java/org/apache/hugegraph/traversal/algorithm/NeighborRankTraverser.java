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
import org.apache.hugegraph.traversal.algorithm.steps.EdgeStep;
import org.apache.hugegraph.type.define.Directions;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.OrderLimitMap;
import org.apache.tinkerpop.gremlin.structure.Edge;

import jakarta.ws.rs.core.MultivaluedMap;

public class NeighborRankTraverser extends HugeTraverser {

    public static final int MAX_TOP = 1000;

    private final double alpha;
    private final long capacity;

    public NeighborRankTraverser(HugeGraph graph, double alpha, long capacity) {
        super(graph);
        checkCapacity(capacity);
        this.alpha = alpha;
        this.capacity = capacity;
    }

    public List<Map<Id, Double>> neighborRank(Id source, List<Step> steps) {
        E.checkNotNull(source, "source vertex id");
        this.checkVertexExist(source, "source vertex");
        E.checkArgument(!steps.isEmpty(), "The steps can't be empty");

        MultivaluedMap<Id, Node> sources = newMultivalueMap();
        sources.add(source, new Node(source, null));

        boolean sameLayerTransfer = true;
        long access = 0;
        // Results: ranks of each layer
        List<Ranks> ranks = newList();
        ranks.add(Ranks.of(source, 1.0));

        for (Step step : steps) {
            Ranks lastLayerRanks = ranks.get(ranks.size() - 1);
            Map<Id, Double> sameLayerIncrRanks = newMap();
            List<Adjacencies> adjacencies = newList();
            MultivaluedMap<Id, Node> newVertices = newMultivalueMap();
            // Traversal vertices of previous level
            for (Map.Entry<Id, List<Node>> entry : sources.entrySet()) {
                Id vertex = entry.getKey();
                Iterator<Edge> edges = this.edgesOfVertex(vertex,
                                                          step.edgeStep);

                Adjacencies adjacenciesV = new Adjacencies(vertex);
                Set<Id> sameLayerNodesV = newIdSet();
                Map<Integer, Set<Id>> prevLayerNodesV = newMap();
                while (edges.hasNext()) {
                    HugeEdge edge = (HugeEdge) edges.next();
                    Id target = edge.id().otherVertexId();
                    // Determine whether it belongs to the same layer
                    if (this.belongToSameLayer(sources.keySet(), target,
                                               sameLayerNodesV)) {
                        continue;
                    }
                    /*
                     * Determine whether it belongs to the previous layers,
                     * if it belongs to, update the weight, but don't pass
                     * any more
                     */
                    if (this.belongToPrevLayers(ranks, target,
                                                prevLayerNodesV)) {
                        continue;
                    }

                    for (Node n : entry.getValue()) {
                        // If have loop, skip target
                        if (n.contains(target)) {
                            continue;
                        }
                        Node newNode = new Node(target, n);
                        adjacenciesV.add(newNode);
                        // Add adjacent nodes to sources of next step
                        newVertices.add(target, newNode);

                        checkCapacity(this.capacity, ++access, "neighbor rank");
                    }
                }
                long degree = sameLayerNodesV.size() + prevLayerNodesV.size() +
                              adjacenciesV.nodes().size();
                if (degree == 0L) {
                    continue;
                }
                adjacenciesV.degree(degree);
                adjacencies.add(adjacenciesV);

                double incr = lastLayerRanks.getOrDefault(vertex, 0.0) *
                              this.alpha / degree;
                // Merge the increment of the same layer node
                this.mergeSameLayerIncrRanks(sameLayerNodesV, incr,
                                             sameLayerIncrRanks);
                // Adding contributions to the previous layers
                this.contributePrevLayers(ranks, incr, prevLayerNodesV);
            }

            Ranks newLayerRanks;
            if (sameLayerTransfer) {
                // First contribute to last layer, then pass to the new layer
                this.contributeLastLayer(sameLayerIncrRanks, lastLayerRanks);
                newLayerRanks = this.contributeNewLayer(adjacencies,
                                                        lastLayerRanks,
                                                        step.capacity);
            } else {
                // First pass to the new layer, then contribute to last layer
                newLayerRanks = this.contributeNewLayer(adjacencies,
                                                        lastLayerRanks,
                                                        step.capacity);
                this.contributeLastLayer(sameLayerIncrRanks, lastLayerRanks);
            }
            ranks.add(newLayerRanks);

            // Re-init sources
            sources = newVertices;
        }
        return this.topRanks(ranks, steps);
    }

    private boolean belongToSameLayer(Set<Id> sources, Id target,
                                      Set<Id> sameLayerNodes) {
        if (sources.contains(target)) {
            sameLayerNodes.add(target);
            return true;
        } else {
            return false;
        }
    }

    private boolean belongToPrevLayers(List<Ranks> ranks, Id target,
                                       Map<Integer, Set<Id>> prevLayerNodes) {
        for (int i = ranks.size() - 2; i > 0; i--) {
            Ranks prevLayerRanks = ranks.get(i);
            if (prevLayerRanks.containsKey(target)) {
                Set<Id> nodes = prevLayerNodes.computeIfAbsent(
                        i, HugeTraverser::newSet);
                nodes.add(target);
                return true;
            }
        }
        return false;
    }

    private void mergeSameLayerIncrRanks(Set<Id> sameLayerNodesV, double incr,
                                         Map<Id, Double> sameLayerIncrRanks) {
        for (Id node : sameLayerNodesV) {
            double oldRank = sameLayerIncrRanks.getOrDefault(node, 0.0);
            sameLayerIncrRanks.put(node, oldRank + incr);
        }
    }

    private void contributePrevLayers(List<Ranks> ranks, double incr,
                                      Map<Integer, Set<Id>> prevLayerNodesV) {
        for (Map.Entry<Integer, Set<Id>> e : prevLayerNodesV.entrySet()) {
            Ranks prevLayerRanks = ranks.get(e.getKey());
            for (Id node : e.getValue()) {
                double oldRank = prevLayerRanks.get(node);
                prevLayerRanks.put(node, oldRank + incr);
            }
        }
    }

    private void contributeLastLayer(Map<Id, Double> rankIncrs,
                                     Ranks lastLayerRanks) {
        for (Map.Entry<Id, Double> entry : rankIncrs.entrySet()) {
            double originRank = lastLayerRanks.get(entry.getKey());
            double incrRank = entry.getValue();
            lastLayerRanks.put(entry.getKey(), originRank + incrRank);
        }
    }

    private Ranks contributeNewLayer(List<Adjacencies> adjacencies,
                                     Ranks lastLayerRanks, int capacity) {
        Ranks newLayerRanks = new Ranks(capacity);
        for (Adjacencies adjacenciesV : adjacencies) {
            Id source = adjacenciesV.source();
            long degree = adjacenciesV.degree();
            for (Node node : adjacenciesV.nodes()) {
                double rank = newLayerRanks.getOrDefault(node.id(), 0.0);
                rank += (lastLayerRanks.get(source) * this.alpha / degree);
                newLayerRanks.put(node.id(), rank);
            }
        }
        return newLayerRanks;
    }

    private List<Map<Id, Double>> topRanks(List<Ranks> ranks,
                                           List<Step> steps) {
        assert !ranks.isEmpty();
        List<Map<Id, Double>> results = newList(ranks.size());
        // The first layer is root node so skip i=0
        results.add(ranks.get(0));
        for (int i = 1; i < ranks.size(); i++) {
            Step step = steps.get(i - 1);
            Ranks origin = ranks.get(i);
            if (origin.size() > step.top) {
                results.add(origin.topN(step.top));
            } else {
                results.add(origin);
            }
        }
        return results;
    }

    public static class Step {

        private final EdgeStep edgeStep;
        private final int top;
        private final int capacity;

        public Step(HugeGraph g, Directions direction, List<String> labels,
                    long degree, long skipDegree, int top, int capacity) {
            E.checkArgument(top > 0 && top <= MAX_TOP,
                            "The top of each layer must be in (0, %s], but " +
                            "got %s", MAX_TOP, top);
            E.checkArgument(capacity > 0,
                            "The capacity of each layer must be > 0, " +
                            "but got %s", capacity);
            this.edgeStep = new EdgeStep(g, direction, labels, null,
                                         degree, skipDegree);
            this.top = top;
            this.capacity = capacity;
        }
    }

    private static class Adjacencies {

        private final Id source;
        private final List<Node> nodes;
        private long degree;

        public Adjacencies(Id source) {
            this.source = source;
            this.nodes = newList();
            this.degree = -1L;
        }

        public Id source() {
            return this.source;
        }

        public List<Node> nodes() {
            return this.nodes;
        }

        public void add(Node node) {
            this.nodes.add(node);
        }

        public long degree() {
            E.checkArgument(this.degree > 0,
                            "The max degree must be > 0, but got %s",
                            this.degree);
            return this.degree;
        }

        public void degree(long degree) {
            this.degree = degree;
        }
    }

    private static class Ranks extends OrderLimitMap<Id, Double> {

        private static final long serialVersionUID = 2041529946017356029L;

        public Ranks(int capacity) {
            super(capacity);
        }

        public static Ranks of(Id key, Double value) {
            Ranks ranks = new Ranks(1);
            ranks.put(key, value);
            return ranks;
        }
    }
}
