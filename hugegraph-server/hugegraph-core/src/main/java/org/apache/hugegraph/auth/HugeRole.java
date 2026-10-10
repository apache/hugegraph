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

package org.apache.hugegraph.auth;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.hugegraph.HugeGraphParams;
import org.apache.hugegraph.auth.SchemaDefine.Entity;
import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.id.IdGenerator;
import org.apache.hugegraph.struct.schema.VertexLabel;
import org.apache.hugegraph.util.E;
import org.apache.tinkerpop.gremlin.structure.Graph.Hidden;
import org.apache.tinkerpop.gremlin.structure.T;

public class HugeRole extends Entity {

    private static final long serialVersionUID = 2330399818352242686L;

    private String name;
    private String nickname;
    private String graphSpace;
    private String description;

    public HugeRole(Id id, String name, String graphSpace) {
        this.id = id;
        this.name = name;
        this.graphSpace = graphSpace;
        this.description = null;
    }

    public HugeRole(String name, String graphSpace) {
        this(StringUtils.isNotEmpty(name) ? IdGenerator.of(name) : null,
             name, graphSpace);
    }

    public HugeRole(Id id, String graphSpace) {
        this(id, id.asString(), graphSpace);
    }

    public static HugeRole fromMap(Map<String, Object> map) {
        HugeRole role = new HugeRole("", "");
        return fromMap(map, role);
    }

    public static Schema schema(HugeGraphParams graph) {
        return new Schema(graph);
    }

    @Override
    public ResourceType type() {
        return ResourceType.GRANT;
    }

    @Override
    public String label() {
        return P.ROLE;
    }

    @Override
    public String name() {
        return this.name;
    }

    public void name(String name) {
        this.name = name;
    }

    public String nickname() {
        return this.nickname;
    }

    public void nickname(String nickname) {
        this.nickname = nickname;
    }

    public String graphSpace() {
        return this.graphSpace;
    }

    public String description() {
        return this.description;
    }

    public void description(String description) {
        this.description = description;
    }

    @Override
    public String toString() {
        return String.format("HugeGroup(%s)", this.id);
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
            case P.NICKNAME:
                this.nickname = (String) value;
                break;
            case P.DESCRIPTION:
                this.description = (String) value;
                break;
            default:
                throw new AssertionError("Unsupported key: " + key);
        }
        return true;
    }

    @Override
    protected Object[] asArray() {
        E.checkState(this.name != null, "Group name can't be null");

        List<Object> list = new ArrayList<>(12);

        list.add(T.label);
        list.add(P.ROLE);

        list.add(P.GRAPHSPACE);
        list.add(this.graphSpace);

        list.add(P.NAME);
        list.add(this.name);

        if (this.nickname != null) {
            list.add(P.NICKNAME);
            list.add(this.nickname);
        }

        if (this.description != null) {
            list.add(P.DESCRIPTION);
            list.add(this.description);
        }

        return super.asArray(list);
    }

    @Override
    public Map<String, Object> asMap() {
        E.checkState(this.name != null, "Group name can't be null");

        Map<String, Object> map = new HashMap<>();

        map.put(Hidden.unHide(P.NAME), this.name);
        map.put(Hidden.unHide(P.GRAPHSPACE), this.graphSpace);
        if (this.description != null) {
            map.put(Hidden.unHide(P.DESCRIPTION), this.description);
        }

        if (this.nickname != null) {
            map.put(Hidden.unHide(P.NICKNAME), this.nickname);
        }

        return super.asMap(map);
    }

    public static final class P {

        public static final String ROLE = Hidden.hide("role");

        public static final String ID = T.id.getAccessor();
        public static final String LABEL = T.label.getAccessor();

        public static final String NAME = "~role_name";
        public static final String NICKNAME = "~role_nickname";
        public static final String GRAPHSPACE = "~graphspace";
        public static final String DESCRIPTION = "~role_description";

        public static String unhide(String key) {
            final String prefix = Hidden.hide("role_");
            if (key.startsWith(prefix)) {
                return key.substring(prefix.length());
            }
            return key;
        }
    }

    public static final class Schema extends SchemaDefine {

        public Schema(HugeGraphParams graph) {
            super(graph, P.ROLE);
        }

        @Override
        public void initSchemaIfNeeded() {
            if (this.existVertexLabel(this.label)) {
                return;
            }

            String[] properties = this.initProperties();

            // Create vertex label
            VertexLabel label = this.schema().vertexLabel(this.label)
                                    .properties(properties)
                                    .usePrimaryKeyId()
                                    .primaryKeys(P.NAME)
                                    .nullableKeys(P.DESCRIPTION, P.NICKNAME)
                                    .enableLabelIndex(true)
                                    .build();
            this.graph.schemaTransaction().addVertexLabel(label);
        }

        protected String[] initProperties() {
            List<String> props = new ArrayList<>();

            props.add(createPropertyKey(P.NAME));
            props.add(createPropertyKey(P.DESCRIPTION));
            props.add(createPropertyKey(P.NICKNAME));

            return super.initProperties(props);
        }
    }
}
