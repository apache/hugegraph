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

package org.apache.hugegraph.auth;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.hugegraph.HugeException;
import org.apache.hugegraph.HugeGraphParams;
import org.apache.hugegraph.auth.SchemaDefine.Entity;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.id.IdGenerator;
import org.apache.hugegraph.schema.VertexLabel;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.JsonUtil;
import org.apache.tinkerpop.gremlin.structure.Graph.Hidden;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.shaded.jackson.core.type.TypeReference;

public class HugeTarget extends Entity {

    private static final long serialVersionUID = -3361487778656878418L;

    private String name;
    public static final Map<String, List<HugeResource>> EMPTY = new HashMap<>();
    private String graph;
    private String description;
    private String url;
    private String graphSpace = "DEFAULT";
    private Map<String, List<HugeResource>> resources;

    public HugeTarget(Id id) {
        this(id, null, null, null, EMPTY);
    }

    public HugeTarget(String name, String url) {
        this(StringUtils.isNotEmpty(name) ? IdGenerator.of(name) : null, name, name, url, EMPTY);
    }

    public HugeTarget(String name, String graph, String url) {
        this(StringUtils.isNotEmpty(name) ? IdGenerator.of(name) : null, name, graph, url, EMPTY);
    }

    public HugeTarget(String name, String graph, String url,
                      Map<String, List<HugeResource>> resources) {
        this(StringUtils.isNotEmpty(name) ? IdGenerator.of(name) : null, name, graph, url,
             resources);
    }

    public HugeTarget(Map<String, List<HugeResource>> resources, String name, String graph,
                      String graphSpace
    ) {
        this.resources = resources;
        this.name = name;
        this.graph = graph;
        this.graphSpace = graphSpace;
        this.id = IdGenerator.of(name);
    }

    private HugeTarget(Id id, String name, String graph, String url,
                       Map<String, List<HugeResource>> resources) {
        this.id = id;
        this.name = name;
        this.graph = graph;
        this.url = url;
        this.resources = resources;
    }

    @Override
    public ResourceType type() {
        return ResourceType.TARGET;
    }

    @Override
    public String label() {
        return P.TARGET;
    }

    @Override
    public String name() {
        return this.name;
    }

    public String graphSpace() {
        return this.graphSpace;
    }

    public void graphSpace(String graphSpace) {
        this.graphSpace = graphSpace;
    }

    public String graph() {
        return this.graph;
    }

    public void graph(String graph) {
        this.graph = graph;
    }

    public String description() {
        return this.description;
    }

    public void description(String description) {
        this.description = description;
    }

    public String url() {
        return this.url;
    }

    public void url(String url) {
        this.url = url;
    }

    public Map<String, List<HugeResource>> resources() {
        return this.resources;
    }

    public void resources(String resources) {
        try {
            this.resources = HugeResource.parseResources(resources);
        } catch (Exception e) {
            throw new HugeException("Invalid format of resources: %s",
                                    e, resources);
        }
    }

    public void resources(Map<String, List<HugeResource>> resources) {
        E.checkNotNull(resources, "resources");
        this.resources = resources;
    }

    @Override
    public String toString() {
        return String.format("HugeTarget(%s)%s", this.id, this.asMap());
    }

    @Override
    protected boolean property(String key, Object value) {
        if (super.property(key, value)) {
            return true;
        }
        switch (key) {
            case P.GRAPHSPACE:
                this.graphSpace = (String) value;
                break;
            case P.NAME:
                this.name = (String) value;
                break;
            case P.GRAPH:
                this.graph = (String) value;
                break;
            case P.URL:
                this.url = (String) value;
                break;
            case P.DESCRIPTION:
                this.description = (String) value;
                break;
            case P.RESS:
                if (value instanceof String) {
                    this.resources = JsonUtil.fromJson(
                            (String) value,
                            new TypeReference<Map<String,
                                    List<HugeResource>>>() {
                            });
                } else {
                    // Handle case where value is already a Map or other object
                    this.resources =
                            JsonUtil.fromJson(
                                    JsonUtil.toJson(value),
                                    new TypeReference<Map<String,
                                            List<HugeResource>>>() {
                                    });
                }
                break;
            default:
                throw new AssertionError("Unsupported key: " + key);
        }
        return true;
    }

    @Override
    protected Object[] asArray() {
        E.checkState(this.name != null, "Target name can't be null");
        E.checkState(this.url != null, "Target url can't be null");

        List<Object> list = new ArrayList<>(16);

        list.add(T.label);
        list.add(P.TARGET);

        list.add(P.NAME);
        list.add(this.name);

        list.add(P.GRAPH);
        list.add(this.graph);

        list.add(P.URL);
        list.add(this.url);

        if (this.graphSpace != null) {
            list.add(P.GRAPHSPACE);
            list.add(this.graphSpace);
        }

        if (this.description != null) {
            list.add(P.DESCRIPTION);
            list.add(this.description);
        }

        if (!this.isResourceEmpty()) {
            list.add(P.RESS);
            list.add(JsonUtil.toJson(this.resources));
        }

        return super.asArray(list);
    }

    public boolean isResourceEmpty() {
        return this.resources == null || this.resources == EMPTY;
    }

    @Override
    public Map<String, Object> asMap() {
        E.checkState(this.name != null, "Target name can't be null");

        Map<String, Object> map = new HashMap<>();

        map.put(Hidden.unHide(P.GRAPHSPACE), this.graphSpace);
        map.put(Hidden.unHide(P.NAME), this.name);
        map.put(Hidden.unHide(P.GRAPH), this.graph);
        map.put(Hidden.unHide(P.URL), this.url);

        if (this.description != null) {
            map.put(Hidden.unHide(P.DESCRIPTION), this.description);
        }

        if (this.resources != null && this.resources != EMPTY) {
            map.put(Hidden.unHide(P.RESS), this.resources);
        }

        return super.asMap(map);
    }

    public static HugeTarget fromVertex(Vertex vertex) {
        HugeTarget target = new HugeTarget((Id) vertex.id());
        return fromVertex(vertex, target);
    }

    public static Schema schema(HugeGraphParams graph) {
        return new Schema(graph);
    }

    public static final class P {

        public static final String TARGET = Hidden.hide("target");

        public static final String ID = T.id.getAccessor();
        public static final String LABEL = T.label.getAccessor();

        public static final String GRAPHSPACE = "~graphspace";
        public static final String NAME = "~target_name";
        public static final String GRAPH = "~target_graph";
        public static final String URL = "~target_url";
        public static final String DESCRIPTION = "~target_description";
        public static final String RESS = "~target_resources";

        public static String unhide(String key) {
            final String prefix = Hidden.hide("target_");
            if (key.startsWith(prefix)) {
                return key.substring(prefix.length());
            }
            return key;
        }
    }

    public static class Schema extends SchemaDefine {

        public Schema(HugeGraphParams graph) {
            this(graph, P.TARGET);
        }

        protected Schema(HugeGraphParams graph, String label) {
            super(graph, label);
        }

        @Override
        public void initSchemaIfNeeded() {
            if (this.existVertexLabel(this.label)) {
                this.upgradeSchemaIfNeeded();
                return;
            }

            String[] properties = this.initProperties();

            // Create vertex label
            VertexLabel label = this.schema().vertexLabel(this.label)
                                    .properties(properties)
                                    .usePrimaryKeyId()
                                    .primaryKeys(P.NAME)
                                    .nullableKeys(P.GRAPHSPACE, P.DESCRIPTION,
                                                  P.RESS)
                                    .enableLabelIndex(true)
                                    .build();
            this.graph.schemaTransaction().addVertexLabel(label);
        }

        private void upgradeSchemaIfNeeded() {
            VertexLabel label = this.graph.graph().vertexLabel(this.label);
            boolean changed = this.ensureNullableProperty(label,
                                                          P.GRAPHSPACE);
            changed |= this.ensureNullableProperty(label, P.DESCRIPTION);
            if (changed) {
                this.graph.schemaTransaction().updateVertexLabel(label);
            }
        }

        private boolean ensureNullableProperty(VertexLabel label,
                                                String property) {
            if (!this.graph.graph().existsPropertyKey(property)) {
                createPropertyKey(property);
            }
            Id propertyId = this.graph.graph().propertyKey(property).id();
            boolean changed = false;
            if (!label.properties().contains(propertyId)) {
                label.property(propertyId);
                changed = true;
            }
            if (!label.nullableKeys().contains(propertyId)) {
                label.nullableKey(propertyId);
                changed = true;
            }
            return changed;
        }

        private String[] initProperties() {
            List<String> props = new ArrayList<>();

            props.add(createPropertyKey(P.NAME));
            props.add(createPropertyKey(P.GRAPH));
            props.add(createPropertyKey(P.URL));
            props.add(createPropertyKey(P.GRAPHSPACE));
            props.add(createPropertyKey(P.DESCRIPTION));
            props.add(createPropertyKey(P.RESS));

            return super.initProperties(props);
        }
    }

    public static HugeTarget fromMap(Map<String, Object> map) {
        HugeTarget target = new HugeTarget(null);
        return fromMap(map, target);
    }
}
