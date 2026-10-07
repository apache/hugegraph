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

import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.query.Query;
import org.apache.hugegraph.job.UserJob;
import org.apache.hugegraph.job.algorithm.BfsTraverser;
import org.apache.hugegraph.structure.HugeVertex;
import org.apache.hugegraph.traversal.algorithm.HugeTraverser;
import org.apache.hugegraph.type.define.Directions;
import org.apache.tinkerpop.gremlin.structure.Vertex;

public class StressCentralityAlgorithmV2 extends AbstractCentAlgorithm {

    @Override
    public String name() {
        return "stress_centrality";
    }

    @Override
    public void checkParameters(Map<String, Object> parameters) {
        super.checkParameters(parameters);
    }

    @Override
    public Object call(UserJob<Object> job, Map<String, Object> parameters) {
        try (Traverser traverser = new Traverser(job)) {
            return traverser.stressCentrality(direction(parameters),
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

    private static class Traverser extends BfsTraverser<StressNode> {

        private final Map<Id, MutableLong> globalStresses;

        private Traverser(UserJob<Object> job) {
            super(job);
            this.globalStresses = new HashMap<>();
        }

        private Object stressCentrality(Directions direction,
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

            Id edgeLabelId = this.getEdgeLabelIdOrNull(label);

            // TODO: sample the startVertices
            Iterator<Vertex> startVertices = this.vertices(sourceLabel,
                                                           sourceCLabel,
                                                           Query.NO_LIMIT);
            while (startVertices.hasNext()) {
                Id startVertex = ((HugeVertex) startVertices.next()).id();
                this.globalStresses.putIfAbsent(startVertex, new MutableLong(0L));
                this.compute(startVertex, direction, edgeLabelId,
                             degree, depth);
            }
            if (topN > 0L || topN == NO_LIMIT) {
                return HugeTraverser.topN(this.globalStresses, true, topN);
            } else {
                return this.globalStresses;
            }
        }

        @Override
        protected StressNode createStartNode() {
            return new StressNode(1, 0);
        }

        @Override
        protected StressNode createNode(StressNode parentNode) {
            return new StressNode(parentNode);
        }

        @Override
        protected void meetNode(Id currentVertex, StressNode currentNode,
                                Id parentVertex, StressNode parentNode,
                                boolean firstTime) {
            currentNode.addParentNode(parentNode, parentVertex);
        }

        @Override
        protected void backtrack(Id startVertex, Id currentVertex,
                                 Map<Id, StressNode> localNodes) {
            if (startVertex.equals(currentVertex)) {
                return;
            }
            StressNode currentNode = localNodes.get(currentVertex);

            // Add local stresses to global stresses
            MutableLong stress = this.globalStresses.get(currentVertex);
            if (stress == null) {
                stress = new MutableLong(0L);
                this.globalStresses.put(currentVertex, stress);
            }
            stress.add(currentNode.stress());

            // Contribute to parents
            for (Id v : currentNode.parents()) {
                StressNode parentNode = localNodes.get(v);
                parentNode.increaseStress(currentNode);
            }
        }
    }

    /**
     * Temp data structure for a vertex used in computing process.
     */
    private static class StressNode extends BfsTraverser.Node {

        private long stress;

        public StressNode(StressNode parentNode) {
            this(0, parentNode.distance() + 1);
        }

        public StressNode(int pathCount, int distance) {
            super(pathCount, distance);
            this.stress = 0L;
        }

        public void increaseStress(StressNode childNode) {
            /*
             * `childNode.stress` is the contribution after child node.
             * `childNode.pathCount` is the contribution of the child node.
             * The sum of them is contribution to current node, there may be
             * multi parents node of the child node, so contribute to current
             * node proportionally.
             */
            long total = childNode.stress + childNode.pathCount();
            long received = total * this.pathCount() / childNode.pathCount();
            this.stress += received;
        }

        public long stress() {
            return this.stress;
        }
    }
}
