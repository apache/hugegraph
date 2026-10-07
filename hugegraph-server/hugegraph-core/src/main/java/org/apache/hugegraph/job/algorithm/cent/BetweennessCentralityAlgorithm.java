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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.mutable.MutableFloat;
import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.id.SplicingIdGenerator;
import org.apache.hugegraph.job.UserJob;
import org.apache.hugegraph.structure.HugeElement;
import org.apache.hugegraph.type.define.Directions;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Pop;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Vertex;

public class BetweennessCentralityAlgorithm extends AbstractCentAlgorithm {

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
            return traverser.betweennessCentrality(direction(parameters),
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

    private static class Traverser extends AbstractCentAlgorithm.Traverser {

        public Traverser(UserJob<Object> job) {
            super(job);
        }

        public Object betweennessCentrality(Directions direction,
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

            GraphTraversal<Vertex, Vertex> t = constructSource(sourceLabel,
                                                               sourceSample,
                                                               sourceCLabel);
            t = constructPath(t, direction, label, degree, sample,
                              sourceLabel, sourceCLabel);
            t = t.emit().until(__.loops().is(P.gte(depth)));
            t = filterNonShortestPath(t, false);

            GraphTraversal<Vertex, ?> tg = this.groupPathByEndpoints(t);
            tg = this.computeBetweenness(tg);
            GraphTraversal<Vertex, ?> tLimit = topN(tg, topN);

            return this.execute(tLimit, tLimit::next);
        }

        protected GraphTraversal<Vertex, ?> groupPathByEndpoints(
                GraphTraversal<Vertex, Vertex> t) {
            return t.map(it -> {
                // t.select(Pop.all, "v").unfold().id()
                List<HugeElement> path = it.path(Pop.all, "v");
                List<Id> pathById = new ArrayList<>(path.size());
                for (HugeElement v : path) {
                    pathById.add(v.id());
                }
                return pathById;
            }).group().by(it -> {
                // group by the first and last vertex
                @SuppressWarnings("unchecked")
                List<Id> path = (List<Id>) it;
                assert path.size() >= 2;
                String first = path.get(0).toString();
                String last = path.get(path.size() - 1).toString();
                return SplicingIdGenerator.concat(first, last);
            }).unfold();
        }

        protected GraphTraversal<Vertex, ?> computeBetweenness(
                GraphTraversal<Vertex, ?> t) {
            return t.fold(new HashMap<Id, MutableFloat>(), (results, it) -> {
                @SuppressWarnings("unchecked")
                Map.Entry<Id, List<?>> entry = (Map.Entry<Id, List<?>>) it;
                @SuppressWarnings("unchecked")
                List<List<Id>> paths = (List<List<Id>>) entry.getValue();
                for (List<Id> path : paths) {
                    int len = path.size();
                    if (len <= 2) {
                        // only two vertex, no betweenness vertex
                        continue;
                    }
                    // skip the first and last vertex
                    for (int i = 1; i < len - 1; i++) {
                        Id vertex = path.get(i);
                        MutableFloat value = results.get(vertex);
                        if (value == null) {
                            value = new MutableFloat();
                            results.put(vertex, value);
                        }
                        value.add(1.0f / paths.size());
                    }
                }
                return results;
            });
        }
    }
}
