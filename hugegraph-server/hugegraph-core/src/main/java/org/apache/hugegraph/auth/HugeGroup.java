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
import org.apache.hugegraph.HugeGraphParams;
import org.apache.hugegraph.auth.SchemaDefine.Entity;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.id.IdGenerator;
import org.apache.hugegraph.schema.VertexLabel;
import org.apache.hugegraph.util.E;
import org.apache.tinkerpop.gremlin.structure.Graph.Hidden;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;

public class HugeGroup extends Entity {

    private static final long serialVersionUID = 2330399818352242686L;

    public static final String ID_PREFIX = "group-";

    private String name;
    private String nickname;
    private String description;

    public HugeGroup(String name) {
        this(StringUtils.isNotEmpty(name) ? IdGenerator.of(name) : null,
             name);
    }

    public HugeGroup(Id id) {
        this(id, null);
    }

    public HugeGroup(Id id, String name) {
        this.id = id;
        this.name = name;
        this.description = null;
    }

    public static boolean isGroup(String id) {
        return StringUtils.isNotEmpty(id) && id.startsWith(ID_PREFIX);
    }

    @Override
    public ResourceType type() {
        return ResourceType.USER_GROUP;
    }

    @Override
    public String label() {
        return P.GROUP;
    }

    @Override
    public String name() {
        return this.name;
    }

    public String nickname() {
        return this.nickname;
    }

    public void nickname(String nickname) {
        this.nickname = nickname;
    }

    public String description() {
        return this.description;
    }

    public void description(String description) {
        this.description = description;
    }

    @Override
    public String toString() {
        return String.format("HugeGroup(%s)%s", this.id, this.asMap());
    }

    @Override
    protected boolean property(String key, Object value) {
        if (super.property(key, value)) {
            return true;
        }
        switch (key) {
            case P.NAME:
                this.name = (String) value;
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
        list.add(P.GROUP);

        list.add(P.NAME);
        list.add(this.name);

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
        if (this.description != null) {
            map.put(Hidden.unHide(P.DESCRIPTION), this.description);
        }

        return super.asMap(map);
    }

    public static HugeGroup fromVertex(Vertex vertex) {
        HugeGroup group = new HugeGroup((Id) vertex.id());
        return fromVertex(vertex, group);
    }

    public static Schema schema(HugeGraphParams graph) {
        return new Schema(graph);
    }

    public static final class P {

        public static final String GROUP = Hidden.hide("group");

        public static final String ID = T.id.getAccessor();
        public static final String LABEL = T.label.getAccessor();

        public static final String NAME = "~group_name";
        public static final String DESCRIPTION = "~group_description";

        public static String unhide(String key) {
            final String prefix = Hidden.hide("group_");
            if (key.startsWith(prefix)) {
                return key.substring(prefix.length());
            }
            return key;
        }
    }

    public static final class Schema extends SchemaDefine {

        public Schema(HugeGraphParams graph) {
            super(graph, P.GROUP);
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
                                    .nullableKeys(P.DESCRIPTION)
                                    .enableLabelIndex(true)
                                    .build();
            this.graph.schemaTransaction().addVertexLabel(label);
        }

        private String[] initProperties() {
            List<String> props = new ArrayList<>();

            props.add(createPropertyKey(P.NAME));
            props.add(createPropertyKey(P.DESCRIPTION));

            return super.initProperties(props);
        }
    }

    public static HugeGroup fromMap(Map<String, Object> map) {
        HugeGroup group = new HugeGroup("");
        return fromMap(map, group);
    }
}
