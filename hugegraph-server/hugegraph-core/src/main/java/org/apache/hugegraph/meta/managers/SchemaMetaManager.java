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

import static org.apache.hugegraph.meta.MetaManager.META_PATH_DELIMITER;
import static org.apache.hugegraph.meta.MetaManager.META_PATH_EDGE_LABEL;
import static org.apache.hugegraph.meta.MetaManager.META_PATH_GRAPHSPACE;
import static org.apache.hugegraph.meta.MetaManager.META_PATH_HUGEGRAPH;
import static org.apache.hugegraph.meta.MetaManager.META_PATH_ID;
import static org.apache.hugegraph.meta.MetaManager.META_PATH_INDEX_LABEL;
import static org.apache.hugegraph.meta.MetaManager.META_PATH_NAME;
import static org.apache.hugegraph.meta.MetaManager.META_PATH_PROPERTY_KEY;
import static org.apache.hugegraph.meta.MetaManager.META_PATH_SCHEMA;
import static org.apache.hugegraph.meta.MetaManager.META_PATH_VERTEX_LABEL;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.id.IdGenerator;
import org.apache.hugegraph.meta.MetaDriver;
import org.apache.hugegraph.meta.PdMetaDriver;
import org.apache.hugegraph.struct.schema.EdgeLabel;
import org.apache.hugegraph.struct.schema.IndexLabel;
import org.apache.hugegraph.struct.schema.PropertyKey;
import org.apache.hugegraph.struct.schema.VertexLabel;
import org.apache.hugegraph.util.JsonUtil;

public class SchemaMetaManager extends AbstractMetaManager {
    private final HugeGraph graph;

    public SchemaMetaManager(MetaDriver metaDriver, String cluster, HugeGraph graph) {
        super(metaDriver, cluster);
        this.graph = graph;
    }

    public static void main(String[] args) {
        MetaDriver metaDriver = new PdMetaDriver("127.0.0.1:8686");
        SchemaMetaManager schemaMetaManager = new SchemaMetaManager(metaDriver, "hg", null);
        PropertyKey propertyKey = new PropertyKey(null, IdGenerator.of(5), "test");
        propertyKey.userdata("key1", "value1");
        propertyKey.userdata("key2", 23);
        schemaMetaManager.addPropertyKey("DEFAULT1", "hugegraph", propertyKey);

//        PropertyKey propertyKey1 = schemaMetaManager.getPropertyKey("DEFAULT1", "hugegraph",
//        IdGenerator.of(1));
        schemaMetaManager.removePropertyKey("DEFAULT", "hugegraph", IdGenerator.of(1));

//        propertyKey1 = schemaMetaManager.getPropertyKey("DEFAULT1", "hugegraph", "test");
//        System.out.println(propertyKey1 );
//
//        propertyKey1 = schemaMetaManager.getPropertyKey("DEFAULT1", "hugegraph", "5");
//        System.out.println(propertyKey1 );
    }

    public void addPropertyKey(String graphSpace, String graph,
                               PropertyKey propertyKey) {
        String content = serialize(propertyKey);
        this.metaDriver.put(propertyKeyIdKey(graphSpace, graph,
                                             propertyKey.id()), content);
        this.metaDriver.put(propertyKeyNameKey(graphSpace, graph,
                                               propertyKey.name()), content);
    }

    public void updatePropertyKey(String graphSpace, String graph,
                                  PropertyKey pkey) {
        this.addPropertyKey(graphSpace, graph, pkey);
    }

    @SuppressWarnings("unchecked")
    public PropertyKey getPropertyKey(String graphSpace, String graph,
                                      Id propertyKey) {
        String content = this.metaDriver.get(propertyKeyIdKey(graphSpace, graph,
                                                              propertyKey));
        if (content == null || content.length() == 0) {
            return null;
        } else {
            return PropertyKey.fromMap(JsonUtil.fromJson(content, Map.class), this.graph);
        }
    }

    @SuppressWarnings("unchecked")
    public PropertyKey getPropertyKey(String graphSpace, String graph,
                                      String propertyKey) {
        String content = this.metaDriver.get(propertyKeyNameKey(graphSpace,
                                                                graph,
                                                                propertyKey));
        if (content == null || content.length() == 0) {
            return null;
        } else {
            return PropertyKey.fromMap(JsonUtil.fromJson(content, Map.class), this.graph);
        }
    }

    @SuppressWarnings("unchecked")
    public List<PropertyKey> getPropertyKeys(String graphSpace, String graph) {
        Map<String, String> propertyKeysKvs = this.metaDriver.scanWithPrefix(
                propertyKeyPrefix(graphSpace, graph));
        List<PropertyKey> propertyKeys =
                new ArrayList<>(propertyKeysKvs.size());
        for (String value : propertyKeysKvs.values()) {
            propertyKeys.add(PropertyKey.fromMap(JsonUtil.fromJson(value, Map.class), this.graph));
        }
        return propertyKeys;
    }

    public Id removePropertyKey(String graphSpace, String graph,
                                Id propertyKey) {
        PropertyKey p = this.getPropertyKey(graphSpace, graph, propertyKey);
        this.metaDriver.delete(propertyKeyNameKey(graphSpace, graph,
                                                  p.name()));
        this.metaDriver.delete(propertyKeyIdKey(graphSpace, graph,
                                                propertyKey));
        return IdGenerator.ZERO;
    }

    public void addVertexLabel(String graphSpace, String graph,
                               VertexLabel vertexLabel) {
        String content = serialize(vertexLabel);
        this.metaDriver.put(vertexLabelIdKey(graphSpace, graph,
                                             vertexLabel.id()), content);
        this.metaDriver.put(vertexLabelNameKey(graphSpace, graph,
                                               vertexLabel.name()), content);
    }

    public void updateVertexLabel(String graphSpace, String graph,
                                  VertexLabel vertexLabel) {
        this.addVertexLabel(graphSpace, graph, vertexLabel);
    }

    @SuppressWarnings("unchecked")
    public VertexLabel getVertexLabel(String graphSpace, String graph,
                                      Id vertexLabel) {
        String content = this.metaDriver.get(vertexLabelIdKey(graphSpace, graph,
                                                              vertexLabel));
        if (content == null || content.length() == 0) {
            return null;
        } else {
            return VertexLabel.fromMap(JsonUtil.fromJson(content, Map.class), this.graph);
        }
    }

    @SuppressWarnings("unchecked")
    public VertexLabel getVertexLabel(String graphSpace, String graph,
                                      String vertexLabel) {
        String content = this.metaDriver.get(vertexLabelNameKey(graphSpace,
                                                                graph,
                                                                vertexLabel));
        if (content == null || content.length() == 0) {
            return null;
        } else {
            return VertexLabel.fromMap(JsonUtil.fromJson(content, Map.class), this.graph);
        }
    }

    @SuppressWarnings("unchecked")
    public List<VertexLabel> getVertexLabels(String graphSpace, String graph) {
        Map<String, String> vertexLabelKvs = this.metaDriver.scanWithPrefix(
                vertexLabelPrefix(graphSpace, graph));
        List<VertexLabel> vertexLabels =
                new ArrayList<>(vertexLabelKvs.size());
        for (String value : vertexLabelKvs.values()) {
            vertexLabels.add(VertexLabel.fromMap(
                    JsonUtil.fromJson(value, Map.class), this.graph));
        }
        return vertexLabels;
    }

    public Id removeVertexLabel(String graphSpace, String graph,
                                Id vertexLabel) {
        VertexLabel v = this.getVertexLabel(graphSpace, graph,
                                            vertexLabel);
        this.metaDriver.delete(vertexLabelNameKey(graphSpace, graph,
                                                  v.name()));
        this.metaDriver.delete(vertexLabelIdKey(graphSpace, graph,
                                                vertexLabel));
        return IdGenerator.ZERO;
    }

    public void addEdgeLabel(String graphSpace, String graph,
                             EdgeLabel edgeLabel) {
        String content = serialize(edgeLabel);
        this.metaDriver.put(edgeLabelIdKey(graphSpace, graph,
                                           edgeLabel.id()), content);
        this.metaDriver.put(edgeLabelNameKey(graphSpace, graph,
                                             edgeLabel.name()), content);
    }

    public void updateEdgeLabel(String graphSpace, String graph,
                                EdgeLabel edgeLabel) {
        this.addEdgeLabel(graphSpace, graph, edgeLabel);
    }

    @SuppressWarnings("unchecked")
    public EdgeLabel getEdgeLabel(String graphSpace, String graph,
                                  Id edgeLabel) {
        String content = this.metaDriver.get(edgeLabelIdKey(graphSpace, graph,
                                                            edgeLabel));
        if (content == null || content.length() == 0) {
            return null;
        } else {
            return EdgeLabel.fromMap(JsonUtil.fromJson(content, Map.class), this.graph);
        }
    }

    @SuppressWarnings("unchecked")
    public EdgeLabel getEdgeLabel(String graphSpace, String graph,
                                  String edgeLabel) {
        String content = this.metaDriver.get(edgeLabelNameKey(graphSpace,
                                                              graph,
                                                              edgeLabel));
        if (content == null || content.length() == 0) {
            return null;
        } else {
            return EdgeLabel.fromMap(JsonUtil.fromJson(content, Map.class), this.graph);
        }
    }

    @SuppressWarnings("unchecked")
    public List<EdgeLabel> getEdgeLabels(String graphSpace, String graph) {
        Map<String, String> edgeLabelKvs = this.metaDriver.scanWithPrefix(
                edgeLabelPrefix(graphSpace, graph));
        List<EdgeLabel> edgeLabels =
                new ArrayList<>(edgeLabelKvs.size());
        for (String value : edgeLabelKvs.values()) {
            edgeLabels.add(EdgeLabel.fromMap(
                    JsonUtil.fromJson(value, Map.class), this.graph));
        }
        return edgeLabels;
    }

    public Id removeEdgeLabel(String graphSpace, String graph,
                              Id edgeLabel) {
        EdgeLabel e = this.getEdgeLabel(graphSpace, graph,
                                        edgeLabel);
        this.metaDriver.delete(edgeLabelNameKey(graphSpace, graph,
                                                e.name()));
        this.metaDriver.delete(edgeLabelIdKey(graphSpace, graph,
                                              edgeLabel));
        return IdGenerator.ZERO;
    }

    public void addIndexLabel(String graphSpace, String graph,
                              IndexLabel indexLabel) {
        String content = serialize(indexLabel);
        this.metaDriver.put(indexLabelIdKey(graphSpace, graph,
                                            indexLabel.id()), content);
        this.metaDriver.put(indexLabelNameKey(graphSpace, graph,
                                              indexLabel.name()), content);
    }

    public void updateIndexLabel(String graphSpace, String graph,
                                 IndexLabel indexLabel) {
        this.addIndexLabel(graphSpace, graph, indexLabel);
    }

    @SuppressWarnings("unchecked")
    public IndexLabel getIndexLabel(String graphSpace, String graph,
                                    Id indexLabel) {
        String content = this.metaDriver.get(indexLabelIdKey(graphSpace, graph,
                                                             indexLabel));
        if (content == null || content.length() == 0) {
            return null;
        } else {
            return IndexLabel.fromMap(JsonUtil.fromJson(content, Map.class), this.graph);
        }
    }

    @SuppressWarnings("unchecked")
    public IndexLabel getIndexLabel(String graphSpace, String graph,
                                    String edgeLabel) {
        String content = this.metaDriver.get(indexLabelNameKey(graphSpace,
                                                               graph,
                                                               edgeLabel));
        if (content == null || content.length() == 0) {
            return null;
        } else {
            return IndexLabel.fromMap(JsonUtil.fromJson(content, Map.class), this.graph);
        }
    }

    @SuppressWarnings("unchecked")
    public List<IndexLabel> getIndexLabels(String graphSpace, String graph) {
        Map<String, String> indexLabelKvs = this.metaDriver.scanWithPrefix(
                indexLabelPrefix(graphSpace, graph));
        List<IndexLabel> indexLabels =
                new ArrayList<>(indexLabelKvs.size());
        for (String value : indexLabelKvs.values()) {
            indexLabels.add(IndexLabel.fromMap(
                    JsonUtil.fromJson(value, Map.class), this.graph));
        }
        return indexLabels;
    }

    public Id removeIndexLabel(String graphSpace, String graph, Id indexLabel) {
        IndexLabel i = this.getIndexLabel(graphSpace, graph,
                                          indexLabel);
        this.metaDriver.delete(indexLabelNameKey(graphSpace, graph,
                                                 i.name()));
        this.metaDriver.delete(indexLabelIdKey(graphSpace, graph,
                                               indexLabel));
        return IdGenerator.ZERO;
    }

    private String propertyKeyPrefix(String graphSpace, String graph) {
        // HUGEGRAPH/{cluster}/GRAPHSPACE/{graphspace}/GRAPH/{graph
        // }/SCHEMA/PROPERTY_KEY/NAME
        return String.join(META_PATH_DELIMITER,
                           META_PATH_HUGEGRAPH,
                           this.cluster,
                           META_PATH_GRAPHSPACE,
                           graphSpace,
                           graph,
                           META_PATH_SCHEMA,
                           META_PATH_PROPERTY_KEY,
                           META_PATH_NAME);
    }

    private String propertyKeyIdKey(String graphSpace, String graph, Id id) {
        // HUGEGRAPH/{cluster}/GRAPHSPACE/{graphspace}/GRAPH/{graph
        // }/SCHEMA/PROPERTY_KEY/ID/{id}
        return String.join(META_PATH_DELIMITER,
                           META_PATH_HUGEGRAPH,
                           this.cluster,
                           META_PATH_GRAPHSPACE,
                           graphSpace,
                           graph,
                           META_PATH_SCHEMA,
                           META_PATH_PROPERTY_KEY,
                           META_PATH_ID,
                           id.asString());
    }

    private String propertyKeyNameKey(String graphSpace, String graph,
                                      String name) {
        // HUGEGRAPH/{cluster}/GRAPHSPACE/{graphspace}/GRAPH/{graph
        // }/SCHEMA/PROPERTY_KEY/NAME/{name}
        return String.join(META_PATH_DELIMITER,
                           META_PATH_HUGEGRAPH,
                           this.cluster,
                           META_PATH_GRAPHSPACE,
                           graphSpace,
                           graph,
                           META_PATH_SCHEMA,
                           META_PATH_PROPERTY_KEY,
                           META_PATH_NAME,
                           name);
    }

    private String vertexLabelPrefix(String graphSpace, String graph) {
        // HUGEGRAPH/{cluster}/GRAPHSPACE/{graphspace}/GRAPH/{graph
        // }/SCHEMA/VERTEX_LABEL/NAME
        return String.join(META_PATH_DELIMITER,
                           META_PATH_HUGEGRAPH,
                           this.cluster,
                           META_PATH_GRAPHSPACE,
                           graphSpace,
                           graph,
                           META_PATH_SCHEMA,
                           META_PATH_VERTEX_LABEL,
                           META_PATH_NAME);
    }

    private String vertexLabelIdKey(String graphSpace, String graph, Id id) {
        // HUGEGRAPH/{cluster}/GRAPHSPACE/{graphspace}/GRAPH/{graph
        // }/SCHEMA/VERTEX_LABEL/ID/{id}
        return String.join(META_PATH_DELIMITER,
                           META_PATH_HUGEGRAPH,
                           this.cluster,
                           META_PATH_GRAPHSPACE,
                           graphSpace,
                           graph,
                           META_PATH_SCHEMA,
                           META_PATH_VERTEX_LABEL,
                           META_PATH_ID,
                           id.asString());
    }

    private String vertexLabelNameKey(String graphSpace, String graph,
                                      String name) {
        // HUGEGRAPH/{cluster}/GRAPHSPACE/{graphspace}/GRAPH/{graph
        // }/SCHEMA/VERTEX_LABEL/NAME/{name}
        return String.join(META_PATH_DELIMITER,
                           META_PATH_HUGEGRAPH,
                           this.cluster,
                           META_PATH_GRAPHSPACE,
                           graphSpace,
                           graph,
                           META_PATH_SCHEMA,
                           META_PATH_VERTEX_LABEL,
                           META_PATH_NAME,
                           name);
    }

    private String edgeLabelPrefix(String graphSpace, String graph) {
        // HUGEGRAPH/{cluster}/GRAPHSPACE/{graphspace}/GRAPH/{graph
        // }/SCHEMA/PROPERTYKEY/NAME
        return String.join(META_PATH_DELIMITER,
                           META_PATH_HUGEGRAPH,
                           this.cluster,
                           META_PATH_GRAPHSPACE,
                           graphSpace,
                           graph,
                           META_PATH_SCHEMA,
                           META_PATH_EDGE_LABEL,
                           META_PATH_NAME);
    }

    private String edgeLabelIdKey(String graphSpace, String graph, Id id) {
        // HUGEGRAPH/{cluster}/GRAPHSPACE/{graphspace}/GRAPH/{graph
        // }/SCHEMA/PROPERTYKEY/ID/{id}
        return String.join(META_PATH_DELIMITER,
                           META_PATH_HUGEGRAPH,
                           this.cluster,
                           META_PATH_GRAPHSPACE,
                           graphSpace,
                           graph,
                           META_PATH_SCHEMA,
                           META_PATH_EDGE_LABEL,
                           META_PATH_ID,
                           id.asString());
    }

    private String edgeLabelNameKey(String graphSpace, String graph,
                                    String name) {
        // HUGEGRAPH/{cluster}/GRAPHSPACE/{graphspace}/GRAPH/{graph
        // }/SCHEMA/EDGE_LABEL/NAME/{name}
        return String.join(META_PATH_DELIMITER,
                           META_PATH_HUGEGRAPH,
                           this.cluster,
                           META_PATH_GRAPHSPACE,
                           graphSpace,
                           graph,
                           META_PATH_SCHEMA,
                           META_PATH_EDGE_LABEL,
                           META_PATH_NAME,
                           name);
    }

    private String indexLabelPrefix(String graphSpace, String graph) {
        // HUGEGRAPH/{cluster}/GRAPHSPACE/{graphspace}/GRAPH/{graph
        // }/SCHEMA/INDEX_LABEL/NAME
        return String.join(META_PATH_DELIMITER,
                           META_PATH_HUGEGRAPH,
                           this.cluster,
                           META_PATH_GRAPHSPACE,
                           graphSpace,
                           graph,
                           META_PATH_SCHEMA,
                           META_PATH_INDEX_LABEL,
                           META_PATH_NAME);
    }

    private String indexLabelIdKey(String graphSpace, String graph, Id id) {
        // HUGEGRAPH/{cluster}/GRAPHSPACE/{graphspace}/GRAPH/{graph
        // }/SCHEMA/INDEX_LABEL/ID/{id}
        return String.join(META_PATH_DELIMITER,
                           META_PATH_HUGEGRAPH,
                           this.cluster,
                           META_PATH_GRAPHSPACE,
                           graphSpace,
                           graph,
                           META_PATH_SCHEMA,
                           META_PATH_INDEX_LABEL,
                           META_PATH_ID,
                           id.asString());
    }

    private String indexLabelNameKey(String graphSpace, String graph,
                                     String name) {
        // HUGEGRAPH/{cluster}/GRAPHSPACE/{graphspace}/GRAPH/{graph
        // }/SCHEMA/INDEX_LABEL/NAME/{name}
        return String.join(META_PATH_DELIMITER,
                           META_PATH_HUGEGRAPH,
                           this.cluster,
                           META_PATH_GRAPHSPACE,
                           graphSpace,
                           graph,
                           META_PATH_SCHEMA,
                           META_PATH_INDEX_LABEL,
                           META_PATH_NAME,
                           name);
    }

    private String graphNameKey(String graphSpace, String graph) {
        // HUGEGRAPH/{cluster}/GRAPHSPACE/{graphspace}/GRAPH/{graph}/SCHEMA
        return String.join(META_PATH_DELIMITER,
                           META_PATH_HUGEGRAPH,
                           this.cluster,
                           META_PATH_GRAPHSPACE,
                           graphSpace,
                           graph,
                           META_PATH_SCHEMA);
    }

    public void clearAllSchema(String graphSpace, String graph) {
        this.metaDriver.deleteWithPrefix(graphNameKey(graphSpace, graph));
    }

}
