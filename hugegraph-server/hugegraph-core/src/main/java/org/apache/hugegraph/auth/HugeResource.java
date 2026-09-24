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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.hugegraph.HugeException;
import org.apache.hugegraph.auth.SchemaDefine.AuthElement;
import org.apache.hugegraph.structure.HugeElement;
import org.apache.hugegraph.traversal.optimize.TraversalUtil;
import org.apache.hugegraph.type.Nameable;
import org.apache.hugegraph.type.Typeable;
import org.apache.hugegraph.util.JsonUtil;
import org.apache.tinkerpop.gremlin.structure.Graph.Hidden;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.shaded.jackson.annotation.JsonProperty;
import org.apache.tinkerpop.shaded.jackson.core.JsonGenerator;
import org.apache.tinkerpop.shaded.jackson.core.JsonParser;
import org.apache.tinkerpop.shaded.jackson.core.JsonToken;
import org.apache.tinkerpop.shaded.jackson.core.type.TypeReference;
import org.apache.tinkerpop.shaded.jackson.databind.DeserializationContext;
import org.apache.tinkerpop.shaded.jackson.databind.SerializerProvider;
import org.apache.tinkerpop.shaded.jackson.databind.deser.std.StdDeserializer;
import org.apache.tinkerpop.shaded.jackson.databind.module.SimpleModule;
import org.apache.tinkerpop.shaded.jackson.databind.ser.std.StdSerializer;

import com.google.common.collect.ImmutableSet;

public class HugeResource {

    public static final String ANY = "*";
    public static final String POUND_SEPARATOR = "#";

    public static final HugeResource ALL = new HugeResource(ResourceType.ALL,
                                                            ANY, null);
    public static final Map<String, List<HugeResource>> ALL_RES =
            new HashMap<>() {
                {
                    put("ALL", List.of(ALL));
                }
            };

    private static final Set<ResourceType> CHECK_NAME_RESS = ImmutableSet.of(
            ResourceType.META);

    static {
        SimpleModule module = new SimpleModule();

        module.addSerializer(HugeResource.class, new HugeResourceSer());
        module.addDeserializer(HugeResource.class, new HugeResourceDeser());

        JsonUtil.registerModule(module);
    }

    @JsonProperty("type")
    private ResourceType type = ResourceType.NONE;

    @JsonProperty("label")
    private String label = ANY;

    // value can be predicate
    @JsonProperty("properties")
    private Map<String, Object> properties;

    public HugeResource() {
        // pass
    }

    public HugeResource(ResourceType type, String label,
                        Map<String, Object> properties) {
        this.type = type;
        this.label = label;
        this.properties = properties;
        this.checkFormat();
    }

    public static Map<String, List<HugeResource>> parseResources(String resources) {
        TypeReference<?> type = new TypeReference<List<HugeResource>>() {
        };
        List<HugeResource> hugeResources = JsonUtil.fromJson(resources, type);
        Map<String, List<HugeResource>> ress = new LinkedHashMap<>();
        for (HugeResource hr : hugeResources) {
            hr.checkFormat();
            String typeLabel;
            if (hr.type.isGraphOrSchema()) {
                typeLabel = hr.type.toString() + POUND_SEPARATOR + hr.label;
            } else {
                typeLabel = hr.type.toString();
            }

            List<HugeResource> ressType = ress.get(typeLabel);
            if (ressType == null) {
                ressType = new ArrayList<>();
                ress.put(typeLabel, ressType);
            }
            ressType.add(hr);
        }
        return ress;
    }

    public String label() {
        return this.label;
    }

    public void checkFormat() {
        if (this.properties == null) {
            return;
        }
        for (Map.Entry<String, Object> entry : this.properties.entrySet()) {
            String propName = entry.getKey();
            Object propValue = entry.getValue();
            if (propName.equals(ANY) && propValue.equals(ANY)) {
                continue;
            }
            if (propValue instanceof String &&
                ((String) propValue).startsWith(TraversalUtil.P_CALL)) {
                TraversalUtil.parsePredicate((String) propValue);
            }
        }
    }

    public boolean filter(ResourceObject<?> resourceObject) {
        if (this.type == null) {
            return false;
        }

        if (!this.type.match(resourceObject.type())) {
            return false;
        }

        if (resourceObject.operated() != NameObject.ANY) {
            ResourceType resType = resourceObject.type();
            if (resType.isGraph()) {
                return this.filter((HugeElement) resourceObject.operated());
            }
            if (resType.isAuth()) {
                return this.filter((AuthElement) resourceObject.operated());
            }
            if (resType.isSchema() || CHECK_NAME_RESS.contains(resType)) {
                return this.filter((Nameable) resourceObject.operated());
            }
        }

        /*
         * Allow any others resource if the type is matched:
         * VAR, GREMLIN, GREMLIN_JOB, TASK
         */
        return true;
    }

    private boolean filter(AuthElement element) {
        assert this.type.match(element.type());
        if (element instanceof Nameable) {
            return this.filter((Nameable) element);
        }
        return true;
    }

    private boolean filter(Nameable element) {
        assert !(element instanceof Typeable) || this.type.match(
                ResourceType.from(((Typeable) element).type()));

        return this.matchLabel(element.name());
    }

    private boolean filter(HugeElement element) {
        assert this.type.match(ResourceType.from(element.type()));

        if (!this.matchLabel(element.label())) {
            return false;
        }

        if (this.properties == null) {
            return true;
        }
        for (Map.Entry<String, Object> entry : this.properties.entrySet()) {
            String propName = entry.getKey();
            Object expected = entry.getValue();
            if (propName.equals(ANY) && expected.equals(ANY)) {
                return true;
            }
            Property<Object> prop = element.property(propName);
            if (!prop.isPresent()) {
                return false;
            }
            try {
                if (!TraversalUtil.testProperty(prop, expected)) {
                    return false;
                }
            } catch (IllegalArgumentException e) {
                throw new HugeException("Invalid resource '%s' for '%s': %s",
                                        expected, propName, e.getMessage());
            }
        }
        return true;
    }

    private boolean matchLabel(String other) {
        // Label value may be vertex/edge label or schema name
        if (this.label == null || other == null) {
            return false;
        }
        // It's ok if wildcard match or regular match
        return this.label.equals(ANY) || other.matches(this.label);
    }

    public Map<String, Object> getProperties() {
        return properties;
    }

    private boolean matchProperties(Map<String, Object> other) {
        if (this.properties == null) {
            // Any property is OK
            return true;
        }
        if (other == null) {
            return false;
        }
        for (Map.Entry<String, Object> p : other.entrySet()) {
            Object value = this.properties.get(p.getKey());
            if (!Objects.equals(value, p.getValue())) {
                return false;
            }
        }
        return true;
    }

    protected boolean contains(HugeResource other) {
        if (this.equals(other)) {
            return true;
        }
        if (this.type == null) {
            return false;
        }
        if (!this.type.match(other.type)) {
            return false;
        }
        if (!this.matchLabel(other.label)) {
            return false;
        }
        return this.matchProperties(other.properties);
    }

    @Override
    public boolean equals(Object object) {
        if (!(object instanceof HugeResource)) {
            return false;
        }
        HugeResource other = (HugeResource) object;
        return this.type == other.type &&
               Objects.equals(this.label, other.label) &&
               Objects.equals(this.properties, other.properties);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.type, this.label, this.properties);
    }

    @Override
    public String toString() {
        return JsonUtil.toJson(this);
    }

    public static boolean allowed(ResourceObject<?> resourceObject) {
        // Allowed to access system(hidden) schema by anyone
        if (resourceObject.type().isSchema()) {
            Nameable schema = (Nameable) resourceObject.operated();
            return Hidden.isHidden(schema.name());
        }

        return false;
    }

    public static HugeResource parseResource(String resource) {
        return JsonUtil.fromJson(resource, HugeResource.class);
    }

    public boolean matchProperties(HugeResource other) {
        return matchProperties(other.properties);
    }
    public static class NameObject implements Nameable {

        public static final NameObject ANY = new NameObject("*");

        private final String name;

        public static NameObject of(String name) {
            return new NameObject(name);
        }

        private NameObject(String name) {
            this.name = name;
        }

        @Override
        public String name() {
            return this.name;
        }

        @Override
        public String toString() {
            return this.name;
        }
    }

    private static class HugeResourceSer extends StdSerializer<HugeResource> {

        private static final long serialVersionUID = -138482122210181714L;

        public HugeResourceSer() {
            super(HugeResource.class);
        }

        @Override
        public void serialize(HugeResource res, JsonGenerator generator,
                              SerializerProvider provider)
                throws IOException {
            generator.writeStartObject();

            generator.writeObjectField("type", res.type);
            generator.writeObjectField("label", res.label);
            generator.writeObjectField("properties", res.properties);

            generator.writeEndObject();
        }
    }

    private static class HugeResourceDeser extends StdDeserializer<HugeResource> {

        private static final long serialVersionUID = -2499038590503066483L;

        public HugeResourceDeser() {
            super(HugeResource.class);
        }

        @Override
        public HugeResource deserialize(JsonParser parser,
                                        DeserializationContext ctxt)
                throws IOException {
            HugeResource res = new HugeResource();
            while (parser.nextToken() != JsonToken.END_OBJECT) {
                String key = parser.getCurrentName();
                if ("type".equals(key)) {
                    if (parser.nextToken() != JsonToken.VALUE_NULL) {
                        res.type = ctxt.readValue(parser, ResourceType.class);
                    } else {
                        res.type = null;
                    }
                } else if ("label".equals(key)) {
                    if (parser.nextToken() != JsonToken.VALUE_NULL) {
                        res.label = parser.getValueAsString();
                    } else {
                        res.label = null;
                    }
                } else if ("properties".equals(key)) {
                    if (parser.nextToken() != JsonToken.VALUE_NULL) {
                        @SuppressWarnings("unchecked")
                        Map<String, Object> prop = ctxt.readValue(parser,
                                                                  Map.class);
                        res.properties = prop;
                    } else {
                        res.properties = null;
                    }
                }
            }
            res.checkFormat();
            return res;
        }
    }
}
