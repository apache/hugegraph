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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.job.UserJob;
import org.apache.hugegraph.traversal.algorithm.FusiformSimilarityTraverser;
import org.apache.hugegraph.type.define.Directions;
import org.apache.hugegraph.util.CollectionUtil;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.JsonUtil;
import org.apache.hugegraph.util.ParameterUtil;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import com.google.common.collect.ImmutableSet;

public class KCoreAlgorithm extends AbstractCommAlgorithm {

    public static final String ALGO_NAME = "k_core";

    public static final String KEY_K = "k";
    public static final String KEY_MERGED = "merged";

    public static final int DEFAULT_K = 3;

    @Override
    public String name() {
        return ALGO_NAME;
    }

    @Override
    public void checkParameters(Map<String, Object> parameters) {
        k(parameters);
        alpha(parameters);
        merged(parameters);
        degree(parameters);
        sourceLabel(parameters);
        sourceCLabel(parameters);
        direction(parameters);
        edgeLabel(parameters);
        workers(parameters);
    }

    @Override
    public Object call(UserJob<Object> job, Map<String, Object> parameters) {
        int workers = workers(parameters);
        try (Traverser traverser = new Traverser(job, workers)) {
            return traverser.kcore(sourceLabel(parameters),
                                   sourceCLabel(parameters),
                                   direction(parameters),
                                   edgeLabel(parameters),
                                   k(parameters),
                                   alpha(parameters),
                                   degree(parameters),
                                   merged(parameters));
        }
    }

    protected static int k(Map<String, Object> parameters) {
        if (!parameters.containsKey(KEY_K)) {
            return DEFAULT_K;
        }
        int k = ParameterUtil.parameterInt(parameters, KEY_K);
        E.checkArgument(k > 1, "The k of kcore must be > 1, but got %s", k);
        return k;
    }

    protected static boolean merged(Map<String, Object> parameters) {
        if (!parameters.containsKey(KEY_MERGED)) {
            return false;
        }
        return ParameterUtil.parameterBoolean(parameters, KEY_MERGED);
    }

    private static class Traverser extends AlgoTraverser {

        public Traverser(UserJob<Object> job, int workers) {
            super(job, ALGO_NAME, workers);
        }

        public Object kcore(String sourceLabel, String sourceCLabel,
                            Directions dir, String label, int k, double alpha,
                            long degree, boolean merged) {
            HugeGraph graph = this.graph();

            KcoreTraverser traverser = new KcoreTraverser(graph);
            JsonMap kcoresJson = new JsonMap();
            kcoresJson.startObject();
            kcoresJson.appendKey("kcores");
            kcoresJson.startList();

            Set<Set<Id>> kcores = new HashSet<>();

            this.traverse(sourceLabel, sourceCLabel, v -> {
                Set<Id> kcore = traverser.kcore(IteratorUtils.of(v),
                                                dir, label, k, alpha, degree);
                if (kcore.isEmpty()) {
                    return;
                }
                if (merged) {
                    synchronized (kcores) {
                        mergeKcores(kcores, kcore);
                    }
                } else {
                    String json = JsonUtil.toJson(kcore);
                    synchronized (kcoresJson) {
                        kcoresJson.appendRaw(json);
                    }
                }
            });

            if (merged) {
                for (Set<Id> kcore : kcores) {
                    kcoresJson.appendRaw(JsonUtil.toJson(kcore));
                }
            }

            kcoresJson.endList();
            kcoresJson.endObject();

            return kcoresJson.asJson();
        }

        private static void mergeKcores(Set<Set<Id>> kcores, Set<Id> kcore) {
            boolean merged = false;
            /*
             * Iterate to collect merging kcores firstly, because merging
             * kcores will be removed from all kcores.
             * Besides one new kcore may connect to multiple existing kcores.
             */
            Set<Set<Id>> mergingKcores = new HashSet<>();
            for (Set<Id> existedKcore : kcores) {
                if (CollectionUtil.hasIntersection(existedKcore, kcore)) {
                    mergingKcores.add(existedKcore);
                    merged = true;
                }
            }
            if (merged) {
                for (Set<Id> mergingKcore : mergingKcores) {
                    kcores.remove(mergingKcore);
                    kcore.addAll(mergingKcore);
                }
            }
            kcores.add(kcore);
        }
    }

    public static class KcoreTraverser extends FusiformSimilarityTraverser {

        public KcoreTraverser(HugeGraph graph) {
            super(graph);
        }

        public Set<Id> kcore(Iterator<Vertex> vertices, Directions direction,
                             String label, int k, double alpha, long degree) {
            int minNeighbors = (int) Math.floor(1.0 / alpha * k);
            SimilarsMap map = fusiformSimilarity(vertices, direction, label,
                                                 minNeighbors, alpha, k - 1,
                                                 0, null, 0, degree,
                                                 NO_LIMIT, NO_LIMIT, true);
            if (map.isEmpty()) {
                return ImmutableSet.of();
            }
            return extractKcore(map, k);
        }

        @SuppressWarnings("unchecked")
        private static Set<Id> extractKcore(SimilarsMap similarsMap, int k) {
            assert similarsMap.size() == 1;
            Map.Entry<Id, Set<Similar>> entry = similarsMap.entrySet()
                                                           .iterator().next();
            Id source = entry.getKey();
            Set<KcoreSimilar> similars = new HashSet<>();
            for (Similar similar : entry.getValue()) {
                similars.add(new KcoreSimilar(similar));
            }

            boolean stop;
            do {
                stop = true;
                // Do statistics
                Map<Id, MutableInt> counts = new HashMap<>();
                for (KcoreSimilar similar : similars) {
                    for (Id id : similar.ids()) {
                        MutableInt count = counts.get(id);
                        if (count == null) {
                            count = new MutableInt(0);
                            counts.put(id, count);
                        }
                        count.increment();
                    }
                }
                /*
                 * Iterate similars to:
                 * 1. delete failed similar
                 * 2. delete failed intermediaries in survive similar
                 * 3. update statistics
                 */
                Set<KcoreSimilar> failedSimilars = new HashSet<>();
                for (KcoreSimilar similar : similars) {
                    Set<Id> failedIds = new HashSet<>();
                    for (Id id : similar.ids()) {
                        MutableInt count = counts.get(id);
                        if (count.getValue() < k - 1) {
                            count.decrement();
                            failedIds.add(id);
                            stop = false;
                        }
                    }

                    Set<Id> survivedIds = new HashSet<>(CollectionUtils
                                                                .subtract(similar.ids(),
                                                                          failedIds));
                    if (survivedIds.size() < k) {
                        for (Id id : survivedIds) {
                            counts.get(id).decrement();
                        }
                        failedSimilars.add(similar);
                    } else {
                        similar.ids(survivedIds);
                    }
                }
                similars = new HashSet<>(CollectionUtils.subtract(
                        similars, failedSimilars));
            } while (!stop);

            if (similars.isEmpty()) {
                return ImmutableSet.of();
            }
            Set<Id> kcores = new HashSet<>();
            kcores.add(source);
            for (KcoreSimilar similar : similars) {
                kcores.add(similar.id());
                kcores.addAll(similar.ids());
            }
            return kcores;
        }
    }

    private static class KcoreSimilar extends
                                      FusiformSimilarityTraverser.Similar {

        private Set<Id> ids;

        public KcoreSimilar(Id id, double score, List<Id> intermediaries) {
            super(id, score, intermediaries);
            this.ids = null;
        }

        public KcoreSimilar(FusiformSimilarityTraverser.Similar similar) {
            super(similar.id(), similar.score(), similar.intermediaries());
            this.ids = new HashSet<>(this.intermediaries());
        }

        public Set<Id> ids() {
            if (this.ids == null) {
                this.ids = new HashSet<>(this.intermediaries());
            }
            return this.ids;
        }

        public void ids(Set<Id> ids) {
            this.ids = ids;
        }
    }
}
