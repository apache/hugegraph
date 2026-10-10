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
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.job.UserJob;
import org.apache.hugegraph.schema.SchemaManager;
import org.apache.hugegraph.struct.schema.VertexLabel;
import org.apache.hugegraph.type.define.Directions;
import org.apache.hugegraph.util.E;
import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import com.google.common.collect.ImmutableMap;

public class LpaAlgorithm extends AbstractCommAlgorithm {

    public static final String ALGO_NAME = "lpa";

    @Override
    public String name() {
        return ALGO_NAME;
    }

    @Override
    public void checkParameters(Map<String, Object> parameters) {
        times(parameters);
        precision(parameters);
        sourceLabel(parameters);
        edgeLabel(parameters);
        direction(parameters);
        degree(parameters);
        showCommunity(parameters);
        workers(parameters);
    }

    @Override
    public Object call(UserJob<Object> job, Map<String, Object> parameters) {
        int workers = workers(parameters);
        String showComm = showCommunity(parameters);

        try (Traverser traverser = new Traverser(job, workers)) {
            if (showComm != null) {
                return traverser.showCommunity(showComm);
            } else {
                return traverser.lpa(sourceLabel(parameters),
                                     edgeLabel(parameters),
                                     direction(parameters),
                                     degree(parameters),
                                     times(parameters),
                                     precision(parameters));
            }
        } catch (Throwable e) {
            job.graph().tx().rollback();
            throw e;
        }
    }

    private static class Traverser extends AlgoTraverser {

        private static final long LIMIT = MAX_QUERY_LIMIT;

        private final Random R = new Random();

        public Traverser(UserJob<Object> job, int workers) {
            super(job, ALGO_NAME, workers);
        }

        public Object lpa(String sourceLabel, String edgeLabel,
                          Directions dir, long degree,
                          int maxTimes, double precision) {
            assert maxTimes > 0;
            assert precision > 0d;

            this.initSchema();

            int times = maxTimes;
            double changedPercent = 0d;

            /*
             * Iterate until:
             *  1.it has stabilized
             *  2.or the maximum number of times is reached
             */
            for (int i = 0; i < maxTimes; i++) {
                changedPercent = this.detectCommunities(sourceLabel, edgeLabel,
                                                        dir, degree);
                if (changedPercent <= precision) {
                    times = i + 1;
                    break;
                }
            }

            Number communities = tryNext(this.graph().traversal().V()
                                             .filter(__.properties(C_LABEL))
                                             .groupCount().by(C_LABEL)
                                             .count(Scope.local));
            return ImmutableMap.of("iteration_times", times,
                                   "last_precision", changedPercent,
                                   "times", maxTimes,
                                   "communities", communities);
        }

        public Object showCommunity(String clabel) {
            E.checkNotNull(clabel, "clabel");
            // all vertices with specified c-label
            Iterator<Vertex> vertices = this.vertices(null, clabel, LIMIT);

            JsonMap json = new JsonMap();
            json.startList();
            while (vertices.hasNext()) {
                this.updateProgress(++this.progress);
                json.append(vertices.next().id().toString());
            }
            json.endList();

            return json.asJson();
        }

        private double detectCommunities(String sourceLabel, String edgeLabel,
                                         Directions dir, long degree) {
            // shuffle: r.order().by(shuffle)
            // r = this.graph().traversal().V().sample((int) LIMIT);

            // detect all vertices
            AtomicLong changed = new AtomicLong(0L);
            long total = this.traverse(sourceLabel, null, v -> {
                // called by multi-threads
                if (this.voteCommunityAndUpdate(v, edgeLabel, dir, degree)) {
                    changed.incrementAndGet();
                }
            }, () -> {
                // commit when finished
                this.graph().tx().commit();
            });

            return total == 0L ? 0d : changed.doubleValue() / total;
        }

        private boolean voteCommunityAndUpdate(Vertex vertex, String edgeLabel,
                                               Directions dir, long degree) {
            String label = this.voteCommunityOfVertex(vertex, edgeLabel,
                                                      dir, degree);
            // update label if it's absent or changed
            if (!labelPresent(vertex) || !label.equals(labelOfVertex(vertex))) {
                this.updateLabelOfVertex(vertex, label);
                return true;
            }
            return false;
        }

        private String voteCommunityOfVertex(Vertex vertex, String edgeLabel,
                                             Directions dir, long degree) {
            // neighbors of source vertex v
            Id source = (Id) vertex.id();
            Id labelId = this.getEdgeLabelIdOrNull(edgeLabel);
            Iterator<Id> neighbors = this.adjacentVertices(source, dir,
                                                           labelId, degree);

            // whether include vertex itself, greatly affects the result.
            // get a larger number of small communities if include itself
            //neighbors.inject(v);

            // calculate label frequency
            Map<String, MutableInt> labels = new HashMap<>();
            while (neighbors.hasNext()) {
                String label = this.labelOfVertex(neighbors.next());
                if (label == null) {
                    // ignore invalid or not-exist vertex
                    continue;
                }
                MutableInt labelCount = labels.get(label);
                if (labelCount != null) {
                    labelCount.increment();
                } else {
                    labels.put(label, new MutableInt(1));
                }
            }

            // isolated vertex
            if (labels.isEmpty()) {
                return this.labelOfVertex(vertex);
            }

            // get the labels with maximum frequency
            List<String> maxLabels = new ArrayList<>();
            int maxFreq = 1;
            for (Map.Entry<String, MutableInt> e : labels.entrySet()) {
                int value = e.getValue().intValue();
                if (value > maxFreq) {
                    maxFreq = value;
                    maxLabels.clear();
                }
                if (value == maxFreq) {
                    maxLabels.add(e.getKey());
                }
            }

            /*
             * TODO:
             * keep origin label with probability to prevent monster communities
             */

            // random choice
            int selected = this.R.nextInt(maxLabels.size());
            return maxLabels.get(selected);
        }

        private boolean labelPresent(Vertex vertex) {
            return vertex.property(C_LABEL).isPresent();
        }

        private String labelOfVertex(Vertex vertex) {
            if (!labelPresent(vertex)) {
                return vertex.id().toString();
            }
            return vertex.value(C_LABEL);
        }

        private String labelOfVertex(Id vid) {
            // TODO: cache with Map<Id, String>
            Vertex vertex = this.vertex(vid);
            if (vertex == null) {
                return null;
            }
            return this.labelOfVertex(vertex);
        }

        private void updateLabelOfVertex(Vertex v, String label) {
            // TODO: cache with Map<Id, String>
            v.property(C_LABEL, label);
            this.commitIfNeeded();
        }

        private void initSchema() {
            String cl = C_LABEL;
            SchemaManager schema = this.graph().schema();
            schema.propertyKey(cl).asText().ifNotExist().create();
            for (VertexLabel vl : schema.getVertexLabels()) {
                schema.vertexLabel(vl.name())
                      .properties(cl).nullableKeys(cl)
                      .append();
            }
        }
    }
}
