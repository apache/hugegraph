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

package org.apache.hugegraph.job.algorithm.cent;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.lang3.mutable.MutableFloat;
import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.query.Query;
import org.apache.hugegraph.job.UserJob;
import org.apache.hugegraph.job.algorithm.BfsTraverser;
import org.apache.hugegraph.structure.HugeVertex;
import org.apache.hugegraph.traversal.algorithm.HugeTraverser;
import org.apache.hugegraph.type.define.Directions;
import org.apache.tinkerpop.gremlin.structure.Vertex;

public class BetweennessCentralityAlgorithmV2 extends AbstractCentAlgorithm {

    @Override
    public String name() {
        return "betweenness_centrality";
    }

    @Override
    public void checkParameters(Map<String, Object> parameters) {
        super.checkParameters(parameters);
    }

    @Override
    public Object call(UserJob<Object> job, Map<String, Object> parameters) {
        try (Traverser traverser = new Traverser(job)) {
            return traverser.betweenessCentrality(direction(parameters),
                                                  edgeLabel(parameters),
                                                  depth(parameters),
                                                  degree(parameters),
                                                  sample(parameters),
                                                  sourceLabel(parameters),
                                                  sourceSample(parameters),
                                                  sourceCLabel(parameters),
                                                  top(parameters));
        }
    }

    private static class Traverser extends BfsTraverser<BetweennessNode> {

        private Map<Id, MutableFloat> globalBetweennesses;

        private Traverser(UserJob<Object> job) {
            super(job);
        }

        private Object betweenessCentrality(Directions direction,
                                            String label,
                                            int depth,
                                            long degree,
                                            long sample,
                                            String sourceLabel,
                                            long sourceSample,
                                            String sourceCLabel,
                                            long topN) {
            assert depth > 0;
            assert degree > 0L || degree == NO_LIMIT;
            assert topN >= 0L || topN == NO_LIMIT;

            this.globalBetweennesses = new HashMap<>();
            Id edgeLabelId = this.getEdgeLabelIdOrNull(label);

            // TODO: sample the startVertices
            Iterator<Vertex> startVertices = this.vertices(sourceLabel,
                                                           sourceCLabel,
                                                           Query.NO_LIMIT);
            while (startVertices.hasNext()) {
                Id startVertex = ((HugeVertex) startVertices.next()).id();
                this.globalBetweennesses.putIfAbsent(startVertex,
                                                     new MutableFloat());
                this.compute(startVertex, direction, edgeLabelId,
                             degree, depth);
            }
            if (topN > 0L || topN == NO_LIMIT) {
                return HugeTraverser.topN(this.globalBetweennesses,
                                          true, topN);
            } else {
                return this.globalBetweennesses;
            }
        }

        @Override
        protected BetweennessNode createNode(BetweennessNode parentNode) {
            return new BetweennessNode(parentNode);
        }

        @Override
        protected void meetNode(Id currentVertex, BetweennessNode currentNode,
                                Id parentVertex, BetweennessNode parentNode,
                                boolean firstTime) {
            currentNode.addParentNode(parentNode, parentVertex);
        }

        @Override
        protected BetweennessNode createStartNode() {
            return new BetweennessNode(1, 0);
        }

        @Override
        protected void backtrack(Id startVertex, Id currentVertex,
                                 Map<Id, BetweennessNode> localNodes) {
            if (startVertex.equals(currentVertex)) {
                return;
            }
            MutableFloat betweenness = this.globalBetweennesses.get(
                    currentVertex);
            if (betweenness == null) {
                betweenness = new MutableFloat(0.0F);
                this.globalBetweennesses.put(currentVertex, betweenness);
            }
            BetweennessNode node = localNodes.get(currentVertex);
            betweenness.add(node.betweenness());

            // Contribute to parents
            for (Id v : node.parents()) {
                BetweennessNode parentNode = localNodes.get(v);
                parentNode.increaseBetweenness(node);
            }
        }
    }

    /**
     * Temp data structure for a vertex used in computing process.
     */
    private static class BetweennessNode extends BfsTraverser.Node {

        private float betweenness;

        public BetweennessNode(BetweennessNode parentNode) {
            this(0, parentNode.distance() + 1);
        }

        public BetweennessNode(int pathCount, int distance) {
            super(pathCount, distance);
            this.betweenness = 0.0F;
        }

        public void increaseBetweenness(BetweennessNode childNode) {
            float increase = (float) this.pathCount() / childNode.pathCount() *
                             (1.0F + childNode.betweenness);
            this.betweenness += increase;
        }

        public float betweenness() {
            return this.betweenness;
        }
    }
}
