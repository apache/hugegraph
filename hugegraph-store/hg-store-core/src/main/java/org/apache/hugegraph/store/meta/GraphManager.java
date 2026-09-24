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

package org.apache.hugegraph.store.meta;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.store.meta.base.GlobalMetaStore;
import org.apache.hugegraph.store.options.MetadataOptions;
import org.apache.hugegraph.store.pd.PdProvider;

public class GraphManager extends GlobalMetaStore {

    private final PdProvider pdProvider;

    private final Map<String, Graph> graphs;

    public GraphManager(MetadataOptions options, PdProvider pdProvider) {
        super(options);
        this.graphs = new ConcurrentHashMap<>();
        this.pdProvider = pdProvider;
        this.pdProvider.setGraphManager(this);
    }

    /**
     * Modify image
     * This place does not add a lock, requiring the graph to be cloned, forbidden to modify the
     * original object.
     *
     * @param graph
     * @return
     */
    public Graph updateGraph(Graph graph) {
        this.graphs.put(graph.getGraphName(), graph);
        byte[] key = MetadataKeyHelper.getGraphKey(graph.getGraphName());
        if (graph.getProtoObj() != null) {
            put(key, graph.getProtoObj().toByteArray());
        }
        return graph;
    }

    public void load() {
        byte[] key = MetadataKeyHelper.getGraphKeyPrefix();
        List<Metapb.Graph> values = scan(Metapb.Graph.parser(), key);
        values.forEach(graph -> {
            graphs.put(graph.getGraphName(), new Graph(graph));
        });
    }

    public Map<String, Graph> getGraphs() {
        return graphs;
    }

    public Graph getGraph(String graphName) {
        return graphs.get(graphName);
    }

    public Graph getCloneGraph(String graphName) {
        if (graphs.containsKey(graphName)) {
            return graphs.get(graphName).clone();
        }
        return new Graph();
    }

    public Graph removeGraph(String graphName) {
        byte[] key = MetadataKeyHelper.getGraphKey(graphName);
        delete(key);
        return graphs.remove(graphName);
    }
}
