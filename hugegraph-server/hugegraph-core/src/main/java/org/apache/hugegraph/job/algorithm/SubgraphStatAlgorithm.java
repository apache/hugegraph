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

package org.apache.hugegraph.job.algorithm;

import java.util.Iterator;
import java.util.Map;

import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.StandardHugeGraph;
import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.config.CoreOptions;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.job.UserJob;
import org.apache.hugegraph.task.HugeTask;
import org.apache.hugegraph.traversal.algorithm.HugeTraverser;
import org.apache.hugegraph.traversal.optimize.HugeScriptTraversal;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.InsertionOrderUtil;
import org.apache.hugegraph.util.Log;
import org.apache.hugegraph.util.ParameterUtil;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;

import com.google.common.collect.ImmutableMap;

public class SubgraphStatAlgorithm extends AbstractAlgorithm {

    public static final String KEY_SUBGRAPH = "subgraph";
    public static final String KEY_COPY_SCHEMA = "copy_schema";

    private static final Logger LOG = Log.logger(SubgraphStatAlgorithm.class);

    @Override
    public String name() {
        return "subgraph_stat";
    }

    @Override
    public String category() {
        return CATEGORY_AGGR;
    }

    @Override
    public void checkParameters(Map<String, Object> parameters) {
        subgraph(parameters);
    }

    @Override
    public Object call(UserJob<Object> job, Map<String, Object> parameters) {
        HugeGraph graph = this.createTempGraph(job);
        try (Traverser traverser = new Traverser(job)) {
            this.initGraph(job.graph(), graph,
                           subgraph(parameters), copySchema(parameters));
            UserJob<Object> tmpJob = new TempJob<>(graph, job, job.task());
            return traverser.subgraphStat(tmpJob);
        } finally {
            // Use clearBackend instead of truncateBackend due to no server-id
            graph.clearBackend();
            try {
                graph.close();
            } catch (Throwable e) {
                LOG.warn("Can't close subgraph_stat temp graph {}: {}",
                         graph, e.getMessage(), e);
            }
        }
    }

    private HugeGraph createTempGraph(UserJob<Object> job) {
        Id id = job.task().id();
        String name = "tmp_" + id;
        PropertiesConfiguration config = new PropertiesConfiguration();
        config.setProperty(CoreOptions.BACKEND.name(), "memory");
        config.setProperty(CoreOptions.STORE.name(), name);
        /*
         * NOTE: this temp graph don't need to init backend because no task info
         * required, also not set started because no task to be scheduled.
         */
        return new StandardHugeGraph(new HugeConfig(config));
    }

    @SuppressWarnings("resource")
    private void initGraph(HugeGraph parent, HugeGraph graph,
                           String script, boolean copySchema) {
        if (copySchema) {
            graph.schema().copyFrom(parent.schema());
        }
        new HugeScriptTraversal<>(graph.traversal(), "gremlin-groovy",
                                  script, ImmutableMap.of(),
                                  ImmutableMap.of()).iterate();
        graph.tx().commit();
    }

    protected static String subgraph(Map<String, Object> parameters) {
        Object subgraph = parameters.get(KEY_SUBGRAPH);
        E.checkArgument(subgraph != null,
                        "Must pass parameter '%s'", KEY_SUBGRAPH);
        E.checkArgument(subgraph instanceof String,
                        "Invalid parameter '%s', expect a String, but got %s",
                        KEY_SUBGRAPH, subgraph.getClass().getSimpleName());
        return (String) subgraph;
    }

    protected static boolean copySchema(Map<String, Object> parameters) {
        if (!parameters.containsKey(KEY_COPY_SCHEMA)) {
            return false;
        }
        return ParameterUtil.parameterBoolean(parameters, KEY_COPY_SCHEMA);
    }

    private static class Traverser extends AlgoTraverser {

        private static final Map<String, Object> PARAMS = ImmutableMap.of("depth", 10L,
                                                                          "degree", -1L,
                                                                          "sample", -1L,
                                                                          "top", -1L /* sorted */,
                                                                          "workers", 0);

        public Traverser(UserJob<Object> job) {
            super(job);
        }

        public Object subgraphStat(UserJob<Object> job) {
            AlgorithmPool pool = AlgorithmPool.instance();
            Map<String, Object> results = InsertionOrderUtil.newMap();

            GraphTraversalSource g = job.graph().traversal();
            results.put("vertices_count", g.V().count().next());
            results.put("edges_count", g.E().count().next());

            Algorithm algo = pool.get("degree_centrality");
            Map<String, Object> parameters = ImmutableMap.copyOf(PARAMS);
            results.put("degrees", algo.call(job, parameters));

            algo = pool.get("stress_centrality");
            results.put("stress", algo.call(job, parameters));

            algo = pool.get("betweenness_centrality");
            results.put("betweenness", algo.call(job, parameters));

            algo = pool.get("eigenvector_centrality");
            results.put("eigenvectors", algo.call(job, parameters));

            algo = pool.get("closeness_centrality");
            results.put("closeness", algo.call(job, parameters));

            results.put("page_ranks", pageRanks(job));

            algo = pool.get("cluster_coefficient");
            results.put("cluster_coefficient", algo.call(job, parameters));

            algo = pool.get("rings");
            parameters = ImmutableMap.<String, Object>builder()
                                     .putAll(PARAMS)
                                     .put("count_only", true)
                                     .put("each_limit", NO_LIMIT)
                                     .put("limit", NO_LIMIT)
                                     .build();
            results.put("rings", algo.call(job, parameters));

            return results;
        }

        private Map<Object, Double> pageRanks(UserJob<Object> job) {
            Algorithm algo = AlgorithmPool.instance().get("page_rank");
            algo.call(job, ImmutableMap.of("alpha", 0.15));

            // Collect page ranks
            Map<Object, Double> ranks = InsertionOrderUtil.newMap();
            Iterator<Vertex> vertices = job.graph().vertices();
            while (vertices.hasNext()) {
                Vertex vertex = vertices.next();
                ranks.put(vertex.id(), vertex.value(R_RANK));
            }
            ranks = HugeTraverser.topN(ranks, true, NO_LIMIT);
            return ranks;
        }
    }

    private static class TempJob<V> extends UserJob<V> {

        private final UserJob<V> parent;

        public TempJob(HugeGraph graph, UserJob<V> job, HugeTask<V> task) {
            this.graph(graph);
            this.task(task);
            this.parent = job;
        }

        @Override
        public String type() {
            return "temp";
        }

        @Override
        public V execute() throws Exception {
            return null;
        }

        @Override
        public void updateProgress(int progress) {
            this.parent.updateProgress(progress);
        }
    }
}
