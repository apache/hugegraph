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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.job.UserJob;
import org.apache.hugegraph.schema.SchemaManager;
import org.apache.hugegraph.struct.schema.VertexLabel;
import org.apache.hugegraph.structure.HugeEdge;
import org.apache.hugegraph.structure.HugeVertex;
import org.apache.hugegraph.type.define.Directions;
import org.apache.hugegraph.util.Log;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;

import com.google.common.collect.ImmutableMap;

public class WeakConnectedComponent extends AbstractCommAlgorithm {

    protected static final Logger LOG = Log.logger(WeakConnectedComponent.class);

    @Override
    public String name() {
        return "weak_connected_component";
    }

    @Override
    public void checkParameters(Map<String, Object> parameters) {
        times(parameters);
        directionOutIn(parameters);
        degree(parameters);
    }

    @Override
    public Object call(UserJob<Object> job, Map<String, Object> parameters) {
        try (Traverser traverser = new Traverser(job)) {
            return traverser.connectedComponent(times(parameters),
                                                directionOutIn(parameters),
                                                degree(parameters));
        } catch (Throwable e) {
            job.graph().tx().rollback();
            throw e;
        }
    }

    protected static class Traverser extends AlgoTraverser {

        private final Map<Id, Id> vertexComponentMap = new HashMap<>();

        public Traverser(UserJob<Object> job) {
            super(job);
        }

        public Object connectedComponent(int maxTimes,
                                         Directions direction,
                                         long degree) {
            this.initSchema();
            this.initVertexComponentMap();
            int times;

            for (times = 0; times < maxTimes; times++) {
                long changeCount = 0;
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
                        changeCount += this.findAndSetMinComponent(
                                currentSourceVertexId,
                                adjacentVertices);
                        adjacentVertices = new ArrayList<>();
                        currentSourceVertexId = sourceVertexId;
                        adjacentVertices.add(targetVertexId);
                    }
                }
                changeCount += this.findAndSetMinComponent(
                        currentSourceVertexId,
                        adjacentVertices);
                LOG.debug("iterationTimes:{}, changeCount:{}",
                          times, changeCount);

                if (changeCount == 0L) {
                    break;
                }
            }

            int compCount = writeBackValue();
            return ImmutableMap.of("components", compCount,
                                   "iteration_times", times,
                                   "times", maxTimes);
        }

        private void initSchema() {
            String cl = C_LABEL;
            SchemaManager schema = this.graph().schema();
            schema.propertyKey(cl).asText().ifNotExist().create();
            for (VertexLabel vl : schema.getVertexLabels()) {
                schema.vertexLabel(vl.name()).properties(cl)
                      .nullableKeys(cl).append();
            }
        }

        private void initVertexComponentMap() {
            Iterator<Vertex> vertices = this.vertices();
            while (vertices.hasNext()) {
                Id id = ((HugeVertex) vertices.next()).id();
                this.vertexComponentMap.put(id, id);
            }
        }

        /**
         * process for a vertex and its adjacentVertices
         *
         * @param sourceVertexId   the source vertex
         * @param adjacentVertices the adjacent vertices attached to source
         *                         vertex
         * @return the count of vertex that changed Component
         */
        private long findAndSetMinComponent(Id sourceVertexId,
                                            List<Id> adjacentVertices) {
            if (!this.vertexComponentMap.containsKey(sourceVertexId)) {
                return 0L;
            }
            Id min = this.findMinComponent(sourceVertexId, adjacentVertices);
            return this.updateComponentIfNeeded(min,
                                                sourceVertexId,
                                                adjacentVertices);
        }

        private Id findMinComponent(Id sourceVertexId,
                                    List<Id> adjacentVertices) {
            Id min = this.vertexComponentMap.get(sourceVertexId);
            for (Id vertex : adjacentVertices) {
                Id comp = this.vertexComponentMap.get(vertex);
                if (comp != null && comp.compareTo(min) < 0) {
                    min = comp;
                }
            }
            return min;
        }

        private long updateComponentIfNeeded(Id min,
                                             Id sourceVertexId,
                                             List<Id> adjacentVertices) {
            long changedCount = 0;
            Id comp = this.vertexComponentMap.get(sourceVertexId);
            if (comp.compareTo(min) > 0) {
                this.vertexComponentMap.put(sourceVertexId, min);
                changedCount++;
            }
            for (Id vertex : adjacentVertices) {
                comp = this.vertexComponentMap.get(vertex);
                if (comp != null && comp.compareTo(min) > 0) {
                    this.vertexComponentMap.put(vertex, min);
                    changedCount++;
                }
            }
            return changedCount;
        }

        /**
         * @return the count of components
         */
        private int writeBackValue() {
            Map<Id, Integer> componentIndexMap = new HashMap<>();
            int index = 0;
            for (Map.Entry<Id, Id> entry : this.vertexComponentMap.entrySet()) {
                Id comp = entry.getValue();
                Integer componentIndex = componentIndexMap.get(comp);
                if (componentIndex == null) {
                    componentIndex = index;
                    componentIndexMap.put(comp, componentIndex);
                    index++;
                }
                Vertex vertex = this.vertex(entry.getKey());
                if (vertex != null) {
                    vertex.property(C_LABEL, String.valueOf(componentIndex));
                    this.commitIfNeeded();
                }
            }
            this.graph().tx().commit();
            return index;
        }
    }
}
