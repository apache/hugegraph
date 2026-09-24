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

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.JsonUtil;
import org.apache.tinkerpop.shaded.jackson.annotation.JsonProperty;
import org.apache.tinkerpop.shaded.jackson.core.JsonGenerator;
import org.apache.tinkerpop.shaded.jackson.core.JsonParser;
import org.apache.tinkerpop.shaded.jackson.core.type.TypeReference;
import org.apache.tinkerpop.shaded.jackson.databind.DeserializationContext;
import org.apache.tinkerpop.shaded.jackson.databind.JsonMappingException;
import org.apache.tinkerpop.shaded.jackson.databind.SerializerProvider;
import org.apache.tinkerpop.shaded.jackson.databind.deser.std.StdDeserializer;
import org.apache.tinkerpop.shaded.jackson.databind.module.SimpleModule;
import org.apache.tinkerpop.shaded.jackson.databind.ser.std.StdSerializer;

public class RolePermission {

    public static final String ALL = "*";
    public static final RolePermission NONE =
            RolePermission.role(ALL, ALL, HugePermission.NONE);
    public static final RolePermission ADMIN =
            RolePermission.role(ALL, ALL, HugePermission.ADMIN);
    public static final String ANY_LABEL = "*";
    public static final String POUND_SEPARATOR = "#";
    private final String defaultGraphSpace = "DEFAULT";

    static {
        SimpleModule module = new SimpleModule();

        module.addSerializer(RolePermission.class, new RolePermissionSer());
        module.addDeserializer(RolePermission.class, new RolePermissionDeser());

        JsonUtil.registerModule(module);
    }

    // Mapping of: graphSpace -> graph -> action -> resource
    @JsonProperty("roles")
    private final Map<String, Map<String, Map<HugePermission,
            Map<String, List<HugeResource>>>>> roles;

    public RolePermission() {
        this(new TreeMap<>());
    }

    RolePermission(Map<String, Map<String, Map<HugePermission,
            Map<String, List<HugeResource>>>>> roles) {
        this.roles = roles;
    }

    public static RolePermission all(String graph) {
        return role("*", "*", HugePermission.ADMIN);
    }

    public static RolePermission role(String graphSpace, String graph,
                                      HugePermission perm) {
        RolePermission role = new RolePermission();
        if (perm.ordinal() <= HugePermission.EXECUTE.ordinal() &&
            perm.ordinal() >= HugePermission.READ.ordinal()) {
            role.add(graphSpace, graph, perm, HugeResource.ALL_RES);
        } else {
            // if perm is not read, write, delete or execute, set resources null
            role.add(graphSpace, graph, perm, null);
        }
        return role;
    }

    public static RolePermission role(String graph,
                                      HugePermission perm) {
        return role(admin().defaultGraphSpace, graph, perm);
    }

    public static RolePermission none() {
        return role(ALL, ALL, HugePermission.NONE);
    }

    public static RolePermission admin() {
        return role(ALL, ALL, HugePermission.ADMIN);
    }

    public static boolean isAdmin(RolePermission role) {
        return role.isAdmin();
    }

    @Override
    public boolean equals(Object object) {
        if (!(object instanceof RolePermission)) {
            return false;
        }
        RolePermission other = (RolePermission) object;
        return Objects.equals(this.roles, other.roles);
    }

    public int hashCode() {
        return Objects.hash(this.roles);
    }

    @Override
    public String toString() {
        return this.roles.toString();
    }

    public String toJson() {
        return JsonUtil.toJson(this);
    }

    public static RolePermission fromJson(Object json) {
        RolePermission role;
        if (json instanceof String) {
            role = JsonUtil.fromJson((String) json, RolePermission.class);
        } else {
            // Optimized json with RolePermission object
            E.checkArgument(json instanceof RolePermission,
                            "Invalid role value: %s", json);
            role = (RolePermission) json;
        }
        return role;
    }

    public Map<String, Map<String, Map<HugePermission,
            Map<String, List<HugeResource>>>>> roles() {
        return this.roles;
    }

    protected Map<String, Map<String, Map<HugePermission,
            Map<String, List<HugeResource>>>>> map() {
        return Collections.unmodifiableMap(this.roles);
    }

    protected void add(String graphSpace, String graph, String action,
                       Map<String, List<HugeResource>> resources) {
        this.add(graphSpace, graph, HugePermission.valueOf(action), resources);
    }

    protected void add(String graph, HugePermission action,
                       Map<String, List<HugeResource>> resources) {
        this.add(defaultGraphSpace, graph, action, resources);
    }

    protected void add(String graphSpace, String graph, HugePermission action,
                       Map<String, List<HugeResource>> resources) {
        if (!(action == HugePermission.ADMIN ||
              action == HugePermission.SPACE) &&
            (resources == null || resources == HugeTarget.EMPTY)) {
            return;
        }

        Map<String, Map<HugePermission, Map<String, List<HugeResource>>>> graphPermissions =
                this.roles.get(graphSpace);
        if (graphPermissions == null) {
            graphPermissions = new TreeMap<>();
        }

        Map<HugePermission, Map<String, List<HugeResource>>> permissions =
                graphPermissions.get(graph);
        if (permissions == null) {
            permissions = new TreeMap<>();
            // Ensure resources maintain order even on first add
            Map<String, List<HugeResource>> orderedResources = new java.util.LinkedHashMap<>();
            if (resources != null) {
                orderedResources.putAll(resources);
            }
            permissions.put(action, orderedResources);
            graphPermissions.put(graph, permissions);
        } else {
            Map<String, List<HugeResource>> mergedResources = permissions.get(action);
            if (mergedResources == null) {
                mergedResources = new java.util.LinkedHashMap<>();
                permissions.put(action, mergedResources);
            }

            for (Map.Entry<String, List<HugeResource>> entry : resources.entrySet()) {
                String typeLabel = entry.getKey();
                List<HugeResource> resourcesList =
                        mergedResources.get(typeLabel);
                if (resourcesList != null) {
                    resourcesList.addAll(entry.getValue());
                } else {
                    mergedResources.put(typeLabel, entry.getValue());
                }
            }

            if (mergedResources.isEmpty()) {
                permissions.put(action, null);
            }
        }

        this.roles.put(graphSpace, graphPermissions);
    }

    protected boolean contains(RolePermission other) {
        if (this.isAdmin()) {
            return true;
        }

        for (Map.Entry<String, Map<String, Map<HugePermission, Map<String,
                List<HugeResource>>>>> e1 : other.roles.entrySet()) {
            String graphSpace = e1.getKey();
            Map<String, Map<HugePermission, Map<String, List<HugeResource>>>>
                    resGraph = this.roles.get(graphSpace);
            if (resGraph == null) {
                return false;
            }
            for (Map.Entry<String, Map<HugePermission,
                    Map<String, List<HugeResource>>>> e2 :
                    e1.getValue().entrySet()) {
                Map<HugePermission, Map<String, List<HugeResource>>>
                        resPerm = resGraph.get(e2.getKey());
                if (resPerm == null) {
                    return false;
                }

                for (Map.Entry<HugePermission, Map<String, List<HugeResource>>>
                        e3 : e2.getValue().entrySet()) {
                    Map<String, List<HugeResource>> resType =
                            resPerm.get(e3.getKey());
                    if (resType == null) {
                        return false;
                    }

                    for (Map.Entry<String, List<HugeResource>> e4 :
                            e3.getValue().entrySet()) {
                        // Just check whether resType contains e4
                        String[] typeAndLabel =
                                e4.getKey().split(POUND_SEPARATOR);
                        ResourceType requiredType =
                                ResourceType.valueOf(typeAndLabel[0]);
                        boolean checkLabel = requiredType.isGraphOrSchema();

                        for (HugeResource r : e4.getValue()) {
                            // for every r, resType must contain r
                            boolean contains = false;

                            for (Map.Entry<String, List<HugeResource>> ressMap :
                                    resType.entrySet()) {
                                String[] key = ressMap.getKey().
                                                      split(POUND_SEPARATOR);
                                ResourceType ressType =
                                        ResourceType.valueOf(key[0]);
                                if (!ressType.match(requiredType)) {
                                    continue;
                                }

                                List<HugeResource> ress = ressMap.getValue();
                                if (ress == null) {
                                    continue;
                                } else if (!checkLabel) {
                                    contains = true;
                                    break;
                                }

                                // check label
                                if (!(key[1].equals(ANY_LABEL) ||
                                      typeAndLabel[1].matches(key[1]))) {
                                    continue;
                                }

                                if (!requiredType.isGraph()) {
                                    contains = true;
                                    break;
                                }
                                // check properties
                                for (HugeResource res : ress) {
                                    if (res.matchProperties(r)) {
                                        contains = true;
                                        break;
                                    }
                                }
                            }

                            if (!contains) {
                                return false;
                            }
                        }
                    }
                }
            }
        }
        return true;
    }

    public boolean isAdmin() {
        return this.roles.containsKey(ALL) &&
               this.roles.get(ALL).containsKey(ALL) &&
               this.roles.get(ALL).get(ALL).containsKey(HugePermission.ADMIN);
    }

    public static RolePermission builtin(RolePermission role) {
        E.checkNotNull(role, "role");
        if (role == ADMIN || role.equals(ADMIN)) {
            return ADMIN;
        }
        if (role == NONE || role.equals(NONE)) {
            return NONE;
        }
        return role;
    }

    private static class RolePermissionSer
            extends StdSerializer<RolePermission> {

        private static final long serialVersionUID = -2533310506459479383L;

        public RolePermissionSer() {
            super(RolePermission.class);
        }

        @Override
        public void serialize(RolePermission role, JsonGenerator generator,
                              SerializerProvider provider)
                throws IOException {
            generator.writeStartObject();
            generator.writeObjectField("roles", role.roles);
            generator.writeEndObject();
        }
    }

    private static class RolePermissionDeser
            extends StdDeserializer<RolePermission> {

        private static final long serialVersionUID = -2038234657843260957L;

        public RolePermissionDeser() {
            super(RolePermission.class);
        }

        @Override
        public RolePermission deserialize(JsonParser parser,
                                          DeserializationContext ctxt)
                throws IOException {
            TypeReference<?> type = new TypeReference<TreeMap<String, TreeMap<String,
                    TreeMap<HugePermission, TreeMap<String,
                            List<HugeResource>>>>>>() {
            };
            if ("roles".equals(parser.nextFieldName())) {
                parser.nextValue();
                return new RolePermission(parser.readValueAs(type));
            }
            throw JsonMappingException.from(parser, "Expect field roles");
        }
    }
}
