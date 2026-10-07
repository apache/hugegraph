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

package org.apache.hugegraph.job.algorithm.rank;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.job.UserJob;
import org.apache.hugegraph.job.algorithm.comm.AbstractCommAlgorithm;
import org.apache.hugegraph.schema.SchemaManager;
import org.apache.hugegraph.struct.schema.VertexLabel;
import org.apache.hugegraph.structure.HugeEdge;
import org.apache.hugegraph.structure.HugeVertex;
import org.apache.hugegraph.traversal.algorithm.HugeTraverser;
import org.apache.hugegraph.type.define.Directions;
import org.apache.hugegraph.util.Log;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;

import com.google.common.collect.ImmutableMap;

public class PageRankAlgorithm extends AbstractCommAlgorithm {

    protected static final Logger LOG = Log.logger(PageRankAlgorithm.class);

    @Override
    public String name() {
        return "page_rank";
    }

    @Override
    public String category() {
        return CATEGORY_RANK;
    }

    @Override
    public void checkParameters(Map<String, Object> parameters) {
        alpha(parameters);
        times(parameters);
        precision(parameters);
        degree(parameters);
        directionOutIn(parameters);
        top(parameters);
    }

    @Override
    public Object call(UserJob<Object> job, Map<String, Object> parameters) {
        try (Traverser traverser = new Traverser(job)) {
            return traverser.pageRank(alpha(parameters),
                                      times(parameters),
                                      precision(parameters),
                                      degree(parameters),
                                      directionOutIn(parameters),
                                      top(parameters));
        } catch (Throwable e) {
            job.graph().tx().rollback();
            throw e;
        }
    }

    private static class Traverser extends AlgoTraverser {

        /*
         * DoublePair.left is rank computed by previous step,
         * DoublePair.right is rank computed by current step.
         */
        private final Map<Id, DoublePair> vertexRankMap;

        public Traverser(UserJob<Object> job) {
            super(job);
            this.vertexRankMap = new HashMap<>();
        }

        /**
         * If topN > 0, then return topN elements with rank value in json.
         */
        private Object pageRank(double alpha,
                                int maxTimes,
                                double precision,
                                long degree,
                                Directions direction,
                                long topN) {
            this.initSchema();

            int times;
            double changedRank = 0.0;
            long numOfVertices = this.initRankMap();

            for (times = 0; times < maxTimes; times++) {
                Id currentSourceVertexId = null;
                // the edges are ordered by ownerVertex
                Iterator<Edge> edges = this.edges(direction);
                List<Id> adjacentVertices = new ArrayList<>();

                while (edges.hasNext()) {
                    HugeEdge edge = (HugeEdge) edges.next();
                    Id sourceVertexId = edge.ownerVertex().id();
                    Id targetVertexId = edge.otherVertex().id();

                    if (currentSourceVertexId == null) {
                        currentSourceVertexId = sourceVertexId;
                        adjacentVertices.add(targetVertexId);
                    } else if (currentSourceVertexId.equals(sourceVertexId)) {
                        if (adjacentVertices.size() < degree) {
                            adjacentVertices.add(targetVertexId);
                        }
                    } else {
                        this.contributeToAdjacentVertices(currentSourceVertexId,
                                                          adjacentVertices);
                        adjacentVertices = new ArrayList<>();
                        currentSourceVertexId = sourceVertexId;
                        adjacentVertices.add(targetVertexId);
                    }
                }

                // deal with the last vertex
                this.contributeToAdjacentVertices(currentSourceVertexId,
                                                  adjacentVertices);

                double sumRank = this.computeRank(alpha, numOfVertices);

                double compensatedRank = 1.0 - sumRank;
                changedRank = this.compensateRank(compensatedRank /
                                                  numOfVertices);
                LOG.debug("PageRank execution times:{}, changedRank:{} ",
                          times, changedRank);
                if (changedRank < precision) {
                    break;
                }
            }

            this.writeBackRankValues();

            if (topN > 0) {
                Object topNJson = this.getTopRank(topN);
                return ImmutableMap.of("alpha", alpha,
                                       "iteration_times", times,
                                       "last_changed_rank", changedRank,
                                       "times", maxTimes,
                                       "top", topNJson);
            }
            return ImmutableMap.of("alpha", alpha,
                                   "iteration_times", times,
                                   "last_changed_rank", changedRank,
                                   "times", maxTimes);
        }

        private Object getTopRank(long topN) {
            JsonMap jsonMap = new JsonMap();
            jsonMap.startObject();
            Map<Id, DoublePair> topNMap =
                    HugeTraverser.topN(this.vertexRankMap, true, topN);
            for (Map.Entry<Id, DoublePair> e : topNMap.entrySet()) {
                jsonMap.append(e.getKey().toString(), e.getValue().left);
            }
            jsonMap.endObject();
            return jsonMap.asJson();
        }

        private long initRankMap() {
            long vertexCount = 0;
            Iterator<Vertex> vertices = this.vertices();
            while (vertices.hasNext()) {
                Id vertex = ((HugeVertex) vertices.next()).id();
                DoublePair pair = new DoublePair(0.0, 0.0);
                this.vertexRankMap.put(vertex, pair);
                vertexCount++;
            }

            double initValue = 1.0 / vertexCount;
            for (DoublePair pair : this.vertexRankMap.values()) {
                pair.left(initValue);
            }
            return vertexCount;
        }

        private void contributeToAdjacentVertices(Id sourceVertexId,
                                                  List<Id> adjacentVertices) {
            if (adjacentVertices.isEmpty()) {
                return;
            }
            DoublePair sourcePair = this.vertexRankMap.get(sourceVertexId);
            // sourceVertexId not in vertices.
            if (sourcePair == null) {
                LOG.info("source vertex {} not exists.", sourceVertexId);
                return;
            }
            double distributedValue = sourcePair.left() /
                                      adjacentVertices.size();
            for (Id targetId : adjacentVertices) {
                DoublePair targetPair = this.vertexRankMap.get(targetId);
                if (targetPair == null) {
                    // targetId not in vertices
                    LOG.warn("target vertex {} not exists.", targetId);
                    continue;
                }
                targetPair.addRight(distributedValue);
            }
        }

        private double compensateRank(double compensatedRank) {
            double changedRank = 0.0;
            for (DoublePair pair : this.vertexRankMap.values()) {
                double previousRank = pair.left();
                double currentRank = pair.right() + compensatedRank;
                changedRank += Math.abs(previousRank - currentRank);
                pair.left(currentRank);
                pair.right(0.0);
            }
            return changedRank;
        }

        private void initSchema() {
            SchemaManager schema = this.graph().schema();
            schema.propertyKey(R_RANK).asDouble().ifNotExist().create();
            for (VertexLabel vl : schema.getVertexLabels()) {
                schema.vertexLabel(vl.name()).properties(R_RANK)
                      .nullableKeys(R_RANK).append();
            }
        }

        private void writeBackRankValues() {
            for (Map.Entry<Id, DoublePair> e : this.vertexRankMap.entrySet()) {
                Id vertexId = e.getKey();
                Vertex vertex = this.vertex(vertexId);
                if (vertex != null) {
                    vertex.property(R_RANK, e.getValue().left());
                    this.commitIfNeeded();
                }
            }
            this.graph().tx().commit();
        }

        private double computeRank(double alpha, long numOfVertices) {
            double oneMinusAlpha = 1.0 - alpha;
            double sum = 0.0;
            double baseRank = alpha / numOfVertices;
            for (DoublePair pair : this.vertexRankMap.values()) {
                double rankValue = baseRank + pair.right() * oneMinusAlpha;
                pair.right(rankValue);
                sum += rankValue;
            }
            return sum;
        }
    }

    public static class DoublePair implements Comparable<DoublePair> {

        private double left;
        private double right;

        private DoublePair(double left, double right) {
            this.left = left;
            this.right = right;
        }

        public void addLeft(double value) {
            this.left += value;
        }

        public void addRight(double value) {
            this.right += value;
        }

        public double left() {
            return this.left;
        }

        public void left(double value) {
            this.left = value;
        }

        public double right() {
            return this.right;
        }

        public void right(double value) {
            this.right = value;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("left:").append(this.left)
              .append(", right: ").append(this.right);
            return sb.toString();
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof DoublePair)) {
                return false;
            }
            DoublePair other = (DoublePair) obj;
            return this.left == other.left && this.right == other.right;
        }

        @Override
        public int hashCode() {
            return Double.hashCode(this.left) ^ Double.hashCode(this.right);
        }

        // only left saves the rank value.
        @Override
        public int compareTo(DoublePair o) {
            double result = this.left - o.left;
            if (result > 0.0) {
                return 1;
            } else if (result < 0.0) {
                return -1;
            }
            return 0;
        }
    }
}
