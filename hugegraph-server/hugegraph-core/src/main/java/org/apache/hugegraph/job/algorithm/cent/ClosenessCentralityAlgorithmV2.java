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

import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.query.Query;
import org.apache.hugegraph.exception.NotSupportException;
import org.apache.hugegraph.job.UserJob;
import org.apache.hugegraph.job.algorithm.BfsTraverser;
import org.apache.hugegraph.structure.HugeVertex;
import org.apache.hugegraph.traversal.algorithm.HugeTraverser;
import org.apache.hugegraph.type.define.Directions;
import org.apache.tinkerpop.gremlin.structure.Vertex;

public class ClosenessCentralityAlgorithmV2 extends AbstractCentAlgorithm {

    @Override
    public String name() {
        return "closeness_centrality";
    }

    @Override
    public void checkParameters(Map<String, Object> parameters) {
        super.checkParameters(parameters);
    }

    @Override
    public Object call(UserJob<Object> job, Map<String, Object> parameters) {
        try (Traverser traverser = new Traverser(job)) {
            return traverser.closenessCentrality(direction(parameters),
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

    private static class Traverser extends BfsTraverser<BfsTraverser.Node> {

        private final Map<Id, Float> globalCloseness;

        private float startVertexCloseness;

        private Traverser(UserJob<Object> job) {
            super(job);
            this.globalCloseness = new HashMap<>();
        }

        private Object closenessCentrality(Directions direction,
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
                this.startVertexCloseness = 0.0F;
                Id startVertex = ((HugeVertex) startVertices.next()).id();
                this.traverse(startVertex, direction, edgeLabelId,
                              degree, depth);
                this.globalCloseness.put(startVertex,
                                         this.startVertexCloseness);
            }
            if (topN > 0L || topN == NO_LIMIT) {
                return HugeTraverser.topN(this.globalCloseness, true, topN);
            } else {
                return this.globalCloseness;
            }
        }

        @Override
        protected Node createStartNode() {
            return new Node(1, 0);
        }

        @Override
        protected Node createNode(Node parentNode) {
            return new Node(parentNode);
        }

        @Override
        protected void meetNode(Id currentVertex, Node currentNode,
                                Id parentVertex, Node parentNode,
                                boolean firstTime) {
            if (firstTime) {
                this.startVertexCloseness += 1.0F / currentNode.distance();
            }
        }

        @Override
        protected void backtrack(Id startVertex, Id currentVertex,
                                 Map<Id, Node> localNodes) {
            throw new NotSupportException("backtrack()");
        }
    }
}
