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

public class HugeUser extends Entity {

    private static final long serialVersionUID = -8951193710873772717L;

    private String name;
    private String nickname;
    private String password;
    private String phone;
    private String email;
    private String avatar;
    private String description;
    // This field is just for cache
    private RolePermission role;

    public HugeUser(String name) {
        this(StringUtils.isNotEmpty(name) ? IdGenerator.of(name) : null, name);
    }

    public HugeUser(Id id) {
        this(id, null);
    }

    public HugeUser(Id id, String name) {
        this.id = id;
        this.name = name;
        this.role = null;
    }

    @Override
    public ResourceType type() {
        return ResourceType.USER_GROUP;
    }

    @Override
    public String label() {
        return P.USER;
    }

    @Override
    public String name() {
        return this.name;
    }

    public String nickname() {
        return nickname;
    }

    public void nickname(String nickname) {
        this.nickname = nickname;
    }

    public String password() {
        return this.password;
    }

    public void password(String password) {
        this.password = password;
    }

    public String phone() {
        return this.phone;
    }

    public void phone(String phone) {
        this.phone = phone;
    }

    public String email() {
        return this.email;
    }

    public void email(String email) {
        this.email = email;
    }

    public String avatar() {
        return this.avatar;
    }

    public void avatar(String avatar) {
        this.avatar = avatar;
    }

    public String description() {
        return this.description;
    }

    public void description(String description) {
        this.description = description;
    }

    public RolePermission role() {
        return this.role;
    }

    public void role(RolePermission role) {
        this.role = role;
    }

    @Override
    public String toString() {
        return String.format("HugeUser(%s)%s", this.id, this.asMap());
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
            case P.NICKNAME:
                this.nickname = (String) value;
                break;
            case P.PASSWORD:
                this.password = (String) value;
                break;
            case P.PHONE:
                this.phone = (String) value;
                break;
            case P.EMAIL:
                this.email = (String) value;
                break;
            case P.AVATAR:
                this.avatar = (String) value;
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
        E.checkState(this.name != null, "User name can't be null");
        E.checkState(this.password != null, "User password can't be null");

        List<Object> list = new ArrayList<>(20);

        list.add(T.label);
        list.add(P.USER);

        list.add(P.NAME);
        list.add(this.name);

        list.add(P.PASSWORD);
        list.add(this.password);

        if (this.nickname != null) {
            list.add(P.NICKNAME);
            list.add(this.nickname);
        }

        if (this.phone != null) {
            list.add(P.PHONE);
            list.add(this.phone);
        }

        if (this.email != null) {
            list.add(P.EMAIL);
            list.add(this.email);
        }

        if (this.avatar != null) {
            list.add(P.AVATAR);
            list.add(this.avatar);
        }

        if (this.description != null) {
            list.add(P.DESCRIPTION);
            list.add(this.description);
        }

        return super.asArray(list);
    }

    @Override
    public Map<String, Object> asMap() {
        E.checkState(this.name != null, "User name can't be null");
        E.checkState(this.password != null, "User password can't be null");

        Map<String, Object> map = new HashMap<>();

        map.put(Hidden.unHide(P.NAME), this.name);
        map.put(Hidden.unHide(P.PASSWORD), this.password);

        if (this.nickname != null) {
            map.put(Hidden.unHide(P.NICKNAME), this.nickname);
        }

        if (this.phone != null) {
            map.put(Hidden.unHide(P.PHONE), this.phone);
        }

        if (this.email != null) {
            map.put(Hidden.unHide(P.EMAIL), this.email);
        }

        if (this.avatar != null) {
            map.put(Hidden.unHide(P.AVATAR), this.avatar);
        }

        if (this.description != null) {
            map.put(Hidden.unHide(P.DESCRIPTION), this.description);
        }

        return super.asMap(map);
    }

    public static HugeUser fromVertex(Vertex vertex) {
        HugeUser user = new HugeUser((Id) vertex.id());
        return fromVertex(vertex, user);
    }

    public static Schema schema(HugeGraphParams graph) {
        return new Schema(graph);
    }

    public static final class P {

        public static final String USER = Hidden.hide("user");
        public static final String USER_HIDDEN = USER;

        public static final String ID = T.id.getAccessor();
        public static final String LABEL = T.label.getAccessor();

        public static final String NAME = "~user_name";
        public static final String NICKNAME = "~user_nickname";
        public static final String PASSWORD = "~user_password";
        public static final String PHONE = "~user_phone";
        public static final String EMAIL = "~user_email";
        public static final String AVATAR = "~user_avatar";
        public static final String DESCRIPTION = "~user_description";

        public static String unhide(String key) {
            final String prefix = Hidden.hide("user_");
            if (key.startsWith(prefix)) {
                return key.substring(prefix.length());
            }
            return key;
        }
    }

    public static final class Schema extends SchemaDefine {

        public Schema(HugeGraphParams graph) {
            super(graph, P.USER);
        }

        @Override
        public void initSchemaIfNeeded() {
            if (this.existVertexLabel(this.label)) {
                // Schema already exists: do incremental upgrade for new properties
                upgradeSchemaIfNeeded();
                return;
            }

            String[] properties = this.initProperties();

            // Create vertex label
            VertexLabel label = this.schema().vertexLabel(this.label)
                                    .properties(properties)
                                    .usePrimaryKeyId()
                                    .primaryKeys(P.NAME)
                                    .nullableKeys(P.NICKNAME, P.PHONE, P.EMAIL, P.AVATAR, P.DESCRIPTION)
                                    .enableLabelIndex(true)
                                    .build();
            this.graph.schemaTransaction().addVertexLabel(label);
        }

        private void upgradeSchemaIfNeeded() {
            // Add user_nickname property key if missing (new in this version)
            if (!this.graph.graph().existsPropertyKey(P.NICKNAME)) {
                createPropertyKey(P.NICKNAME);
                VertexLabel vl = this.graph.graph().vertexLabel(this.label);
                Id nickname = this.graph.graph().propertyKey(P.NICKNAME).id();
                vl.property(nickname);
                vl.nullableKey(nickname);
                this.graph.schemaTransaction().updateVertexLabel(vl);
            }
        }

        private String[] initProperties() {
            List<String> props = new ArrayList<>();

            props.add(createPropertyKey(P.NAME));
            props.add(createPropertyKey(P.NICKNAME));
            props.add(createPropertyKey(P.PASSWORD));
            props.add(createPropertyKey(P.PHONE));
            props.add(createPropertyKey(P.EMAIL));
            props.add(createPropertyKey(P.AVATAR));
            props.add(createPropertyKey(P.DESCRIPTION));

            return super.initProperties(props);
        }
    }

    public static HugeUser fromMap(Map<String, Object> map) {
        HugeUser user = new HugeUser("");
        return fromMap(map, user);
    }
}
