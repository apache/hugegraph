/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hugegraph.meta.managers;

import static org.apache.hugegraph.meta.MetaManager.META_PATH_ADD;
import static org.apache.hugegraph.meta.MetaManager.META_PATH_CLEAR;
import static org.apache.hugegraph.meta.MetaManager.META_PATH_DEFAULT_GS;
import static org.apache.hugegraph.meta.MetaManager.META_PATH_DELIMITER;
import static org.apache.hugegraph.meta.MetaManager.META_PATH_EDGE_LABEL;
import static org.apache.hugegraph.meta.MetaManager.META_PATH_EVENT;
import static org.apache.hugegraph.meta.MetaManager.META_PATH_GRAPH;
import static org.apache.hugegraph.meta.MetaManager.META_PATH_GRAPHSPACE;
import static org.apache.hugegraph.meta.MetaManager.META_PATH_GRAPH_CONF;
import static org.apache.hugegraph.meta.MetaManager.META_PATH_HUGEGRAPH;
import static org.apache.hugegraph.meta.MetaManager.META_PATH_JOIN;
import static org.apache.hugegraph.meta.MetaManager.META_PATH_REMOVE;
import static org.apache.hugegraph.meta.MetaManager.META_PATH_SCHEMA;
import static org.apache.hugegraph.meta.MetaManager.META_PATH_SYS_GRAPH_CONF;
import static org.apache.hugegraph.meta.MetaManager.META_PATH_UPDATE;
import static org.apache.hugegraph.meta.MetaManager.META_PATH_VERTEX_LABEL;
import static org.apache.hugegraph.meta.MetaManager.schemaCacheClearEventValue;

import java.util.Map;
import java.util.function.Consumer;

import org.apache.commons.lang3.StringUtils;
import org.apache.hugegraph.meta.MetaDriver;
import org.apache.hugegraph.type.define.CollectionType;
import org.apache.hugegraph.util.JsonUtil;
import org.apache.hugegraph.util.collection.CollectionFactory;
import org.apache.logging.log4j.util.Strings;

public class GraphMetaManager extends AbstractMetaManager {

    public GraphMetaManager(MetaDriver metaDriver, String cluster) {
        super(metaDriver, cluster);
    }

    private static String graphName(String graphSpace, String name) {
        return String.join(META_PATH_JOIN, graphSpace, name);
    }

    public Map<String, Map<String, Object>> graphConfigs(String graphSpace) {
        Map<String, Map<String, Object>> configs =
                CollectionFactory.newMap(CollectionType.EC);
        Map<String, String> keyValues = this.metaDriver.scanWithPrefix(
                this.graphConfPrefix(graphSpace));
        for (Map.Entry<String, String> entry : keyValues.entrySet()) {
            String key = entry.getKey();
            String[] parts = key.split(META_PATH_DELIMITER);
            String name = parts[parts.length - 1];
            String graphName = String.join("-", graphSpace, name);
            configs.put(graphName, configMap(entry.getValue()));
        }
        return configs;
    }

    public void removeGraphConfig(String graphSpace, String graph) {
        this.metaDriver.delete(this.graphConfKey(graphSpace, graph));
    }

    public void notifyGraphAdd(String graphSpace, String graph) {
        this.metaDriver.put(this.graphAddKey(),
                            graphName(graphSpace, graph));
    }

    public void notifyGraphRemove(String graphSpace, String graph) {
        this.metaDriver.put(this.graphRemoveKey(),
                            graphName(graphSpace, graph));
    }

    public void notifyGraphUpdate(String graphSpace, String graph) {
        this.metaDriver.put(this.graphUpdateKey(),
                            graphName(graphSpace, graph));
    }

    public void notifyGraphClear(String graphSpace, String graph) {
        this.metaDriver.put(this.graphClearKey(),
                            graphName(graphSpace, graph));
    }

    public void notifySchemaCacheClear(String graphSpace, String graph) {
        this.notifySchemaCacheClear(graphSpace, graph, null);
    }

    public void notifySchemaCacheClear(String graphSpace, String graph,
                                       String source) {
        this.metaDriver.put(this.schemaCacheClearKey(),
                            schemaCacheClearEventValue(
                                    graphName(graphSpace, graph), source));
    }

    public void notifyGraphCacheClear(String graphSpace, String graph) {
        this.metaDriver.put(this.graphCacheClearKey(),
                            graphName(graphSpace, graph));
    }

    /**
     * Notice point information cache clear
     *
     * @param graphSpace
     * @param graph
     */
    public void notifyGraphVertexCacheClear(String graphSpace, String graph) {
        this.metaDriver.put(this.graphVertexCacheClearKey(),
                            graphName(graphSpace, graph));
    }

    /**
     * Notice Edge Information cache clear
     *
     * @param graphSpace
     * @param graph
     */
    public void notifyGraphEdgeCacheClear(String graphSpace, String graph) {
        this.metaDriver.put(this.graphEdgeCacheClearKey(),
                            graphName(graphSpace, graph));
    }

    public Map<String, Object> getGraphConfig(String graphSpace, String graph) {
        return configMap(this.metaDriver.get(this.graphConfKey(graphSpace,
                                                               graph)));
    }

    public void addGraphConfig(String graphSpace, String graph,
                               Map<String, Object> configs) {
        this.metaDriver.put(this.graphConfKey(graphSpace, graph),
                            JsonUtil.toJson(configs));
    }

    public void updateGraphConfig(String graphSpace, String graph,
                                  Map<String, Object> configs) {
        this.metaDriver.put(this.graphConfKey(graphSpace, graph),
                            JsonUtil.toJson(configs));
    }

    public void addSysGraphConfig(Map<String, Object> configs) {
        this.metaDriver.put(this.sysGraphConfKey(), JsonUtil.toJson(configs));
    }

    public Map<String, Object> getSysGraphConfig() {
        String content = this.metaDriver.get(this.sysGraphConfKey());
        if (StringUtils.isEmpty(content)) {
            return null;
        }
        return configMap(content);
    }

    public void removeSysGraphConfig() {
        this.metaDriver.delete(this.sysGraphConfKey());
    }

    public <T> void listenGraphAdd(Consumer<T> consumer) {
        this.listen(this.graphAddKey(), consumer);
    }

    public <T> void listenGraphUpdate(Consumer<T> consumer) {
        this.listen(this.graphUpdateKey(), consumer);
    }

    public <T> void listenGraphRemove(Consumer<T> consumer) {
        this.listen(this.graphRemoveKey(), consumer);
    }

    public <T> void listenGraphClear(Consumer<T> consumer) {
        this.listen(this.graphClearKey(), consumer);
    }

    public <T> void listenSchemaCacheClear(Consumer<T> consumer) {
        this.listen(this.schemaCacheClearKey(), consumer);
    }

    public <T> void listenGraphCacheClear(Consumer<T> consumer) {
        this.listen(this.graphCacheClearKey(), consumer);
    }

    public <T> void listenGraphVertexCacheClear(Consumer<T> consumer) {
        this.listen(this.graphVertexCacheClearKey(), consumer);
    }

    public <T> void listenGraphEdgeCacheClear(Consumer<T> consumer) {
        this.listen(this.graphEdgeCacheClearKey(), consumer);
    }

    private String graphConfPrefix(String graphSpace) {
        return this.graphConfKey(graphSpace, Strings.EMPTY);
    }

    private String graphConfKey(String graphSpace, String graph) {
        // HUGEGRAPH/{cluster}/GRAPHSPACE/{graphspace}/GRAPH_CONF/{graph}
        return String.join(META_PATH_DELIMITER,
                           META_PATH_HUGEGRAPH,
                           this.cluster,
                           META_PATH_GRAPHSPACE,
                           graphSpace,
                           META_PATH_GRAPH_CONF,
                           graph);
    }

    private String sysGraphConfKey() {
        // HUGEGRAPH/{cluster}/GRAPHSPACE/DEFAULT/SYS_GRAPH_CONF
        return String.join(META_PATH_DELIMITER,
                           META_PATH_HUGEGRAPH,
                           this.cluster,
                           META_PATH_GRAPHSPACE,
                           META_PATH_DEFAULT_GS,
                           META_PATH_SYS_GRAPH_CONF);
    }

    private String graphAddKey() {
        // HUGEGRAPH/{cluster}/EVENT/GRAPH/ADD
        return String.join(META_PATH_DELIMITER,
                           META_PATH_HUGEGRAPH,
                           this.cluster,
                           META_PATH_EVENT,
                           META_PATH_GRAPH,
                           META_PATH_ADD);
    }

    private String graphRemoveKey() {
        // HUGEGRAPH/{cluster}/EVENT/GRAPH/REMOVE
        return String.join(META_PATH_DELIMITER,
                           META_PATH_HUGEGRAPH,
                           this.cluster,
                           META_PATH_EVENT,
                           META_PATH_GRAPH,
                           META_PATH_REMOVE);
    }

    private String graphUpdateKey() {
        // HUGEGRAPH/{cluster}/EVENT/GRAPH/UPDATE
        return String.join(META_PATH_DELIMITER,
                           META_PATH_HUGEGRAPH,
                           this.cluster,
                           META_PATH_EVENT,
                           META_PATH_GRAPH,
                           META_PATH_UPDATE);
    }

    private String graphClearKey() {
        // HUGEGRAPH/{cluster}/EVENT/GRAPH/CLEAR
        return String.join(META_PATH_DELIMITER,
                           META_PATH_HUGEGRAPH,
                           this.cluster,
                           META_PATH_EVENT,
                           META_PATH_GRAPH,
                           META_PATH_CLEAR);
    }

    private String schemaCacheClearKey() {
        // HUGEGRAPH/{cluster}/EVENT/GRAPH/SCHEMA/CLEAR
        return String.join(META_PATH_DELIMITER,
                           META_PATH_HUGEGRAPH,
                           this.cluster,
                           META_PATH_EVENT,
                           META_PATH_GRAPH,
                           META_PATH_SCHEMA,
                           META_PATH_CLEAR);
    }

    private String graphCacheClearKey() {
        // HUGEGRAPH/{cluster}/EVENT/GRAPH/GRAPH/CLEAR
        return String.join(META_PATH_DELIMITER,
                           META_PATH_HUGEGRAPH,
                           this.cluster,
                           META_PATH_EVENT,
                           META_PATH_GRAPH,
                           META_PATH_GRAPH,
                           META_PATH_CLEAR);
    }

    /**
     * pd listen to vertex label updated key
     *
     * @return
     */
    private String graphVertexCacheClearKey() {
        // HUGEGRAPH/{cluster}/EVENT/GRAPH/GRAPH/META_PATH_VERTEX_LABEL/CLEAR
        return String.join(META_PATH_DELIMITER,
                           META_PATH_HUGEGRAPH,
                           this.cluster,
                           META_PATH_EVENT,
                           META_PATH_GRAPH,
                           META_PATH_GRAPH,
                           META_PATH_VERTEX_LABEL,
                           META_PATH_CLEAR);
    }

    /**
     * pd listen edge label updated key
     *
     * @return
     */
    private String graphEdgeCacheClearKey() {
        // HUGEGRAPH/{cluster}/EVENT/GRAPH/GRAPH/META_PATH_EDGE_LABEL/CLEAR
        return String.join(META_PATH_DELIMITER,
                           META_PATH_HUGEGRAPH,
                           this.cluster,
                           META_PATH_EVENT,
                           META_PATH_GRAPH,
                           META_PATH_GRAPH,
                           META_PATH_EDGE_LABEL,
                           META_PATH_CLEAR);
    }
}
