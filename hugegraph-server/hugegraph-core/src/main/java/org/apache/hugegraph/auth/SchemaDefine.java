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

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.hugegraph.HugeException;
import org.apache.hugegraph.HugeGraphParams;
import org.apache.hugegraph.auth.HugeTarget.P;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.id.IdGenerator;
import org.apache.hugegraph.schema.IndexLabel;
import org.apache.hugegraph.schema.PropertyKey;
import org.apache.hugegraph.schema.SchemaManager;
import org.apache.hugegraph.schema.VertexLabel;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.type.define.Cardinality;
import org.apache.hugegraph.type.define.DataType;
import org.apache.hugegraph.util.DateUtil;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.SafeDateUtil;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph.Hidden;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

public abstract class SchemaDefine {

    public static String FORMATTER = "yyyy-MM-dd HH:mm:ss.SSS";

    protected final HugeGraphParams graph;
    protected final String label;

    public SchemaDefine(HugeGraphParams graph, String label) {
        this.graph = graph;
        this.label = label;
    }

    public abstract void initSchemaIfNeeded();

    protected SchemaManager schema() {
        return this.graph.graph().schema();
    }

    protected boolean existVertexLabel(String label) {
        return this.graph.graph().existsVertexLabel(label);
    }

    protected boolean existEdgeLabel(String label) {
        return this.graph.graph().existsEdgeLabel(label);
    }

    protected String createPropertyKey(String name) {
        return this.createPropertyKey(name, DataType.TEXT);
    }

    protected String createPropertyKey(String name, DataType dataType) {
        return this.createPropertyKey(name, dataType, Cardinality.SINGLE);
    }

    protected String createPropertyKey(String name, DataType dataType,
                                       Cardinality cardinality) {
        SchemaManager schema = this.schema();
        PropertyKey propertyKey = schema.propertyKey(name)
                                        .dataType(dataType)
                                        .cardinality(cardinality)
                                        .build();
        this.graph.schemaTransaction().addPropertyKey(propertyKey);
        return name;
    }

    protected String[] initProperties(List<String> props) {
        String label = this.label;
        props.add(createPropertyKey(hideField(label, AuthElement.CREATE),
                                    DataType.DATE));
        props.add(createPropertyKey(hideField(label, AuthElement.UPDATE),
                                    DataType.DATE));
        props.add(createPropertyKey(hideField(label, AuthElement.CREATOR)));

        return props.toArray(new String[0]);
    }

    protected IndexLabel createRangeIndex(VertexLabel label, String field) {
        SchemaManager schema = this.schema();
        String name = Hidden.hide(label + "-index-by-" + field);
        IndexLabel indexLabel = schema.indexLabel(name).range()
                                      .on(HugeType.VERTEX_LABEL, this.label)
                                      .by(field)
                                      .build();
        this.graph.schemaTransaction().addIndexLabel(label, indexLabel);
        return indexLabel;
    }

    protected static String hideField(String label, String key) {
        return label + "_" + key;
    }

    protected static String unhideField(String label, String key) {
        return Hidden.unHide(label) + "_" + key;
    }

    public abstract static class AuthElement implements Serializable {

        private static final long serialVersionUID = 8746691160192814973L;

        protected static final String HIDE_ID = "~id";
        protected static final String CREATE = "create";
        protected static final String UPDATE = "update";
        protected static final String CREATOR = "creator";

        protected Id id;
        protected Date create;
        protected Date update;
        protected String creator;

        public AuthElement() {
            this.create = new Date();
            this.update = this.create;
        }

        public Id id() {
            return this.id;
        }

        public void id(Id id) {
            this.id = id;
        }

        public String idString() {
            return Hidden.unHide(this.label()) + "(" + this.id + ")";
        }

        public Date create() {
            return this.create;
        }

        public void create(Date create) {
            this.create = create;
        }

        public Date update() {
            return this.update;
        }

        public void update(Date update) {
            this.update = update;
        }

        public void onUpdate() {
            this.update = new Date();
        }

        public String creator() {
            return this.creator;
        }

        public void creator(String creator) {
            this.creator = creator;
        }

        protected Map<String, Object> asMap(Map<String, Object> map) {
            E.checkState(this.create != null,
                         "Property %s time can't be null", CREATE);
            E.checkState(this.update != null,
                         "Property %s time can't be null", UPDATE);
            E.checkState(this.creator != null,
                         "Property %s can't be null", CREATOR);

            if (this.id != null) {
                // The id is null when creating
                map.put(Hidden.unHide(P.ID), this.id);
            }

            map.put(unhideField(this.label(), CREATE), this.create);
            map.put(unhideField(this.label(), UPDATE), this.update);
            map.put(unhideField(this.label(), CREATOR), this.creator);

            return map;
        }

        protected boolean property(String key, Object value) {
            E.checkNotNull(key, "property key");
            try {
                if (key.equals(hideField(this.label(), CREATE))) {
                    this.create = parseFlexibleDate(value);
                    return true;
                }
                if (key.equals(hideField(this.label(), UPDATE))) {
                    this.update = parseFlexibleDate(value);
                    return true;
                }
                if (key.equals(hideField(this.label(), CREATOR))) {
                    this.creator = (String) value;
                    return true;
                }
                if (key.equals(HIDE_ID)) {
                    this.id = IdGenerator.of(value.toString());
                    return true;
                }
            } catch (ParseException e) {
                throw new HugeException("Failed to parse date property '%s' with value '%s': %s",
                                        key, value, e.getMessage());
            }
            return false;
        }

        //FIXME: Unify the date format instead of using this method
        private Date parseFlexibleDate(Object value) throws ParseException {
            if (value instanceof Date) {
                // If it's already a Date object, return it directly
                return (Date) value;
            }

            String dateStr = value.toString();

            // Try multiple date formats - millisecond precision format first
            String[] dateFormats = {
                    FORMATTER,
                    // "yyyy-MM-dd HH:mm:ss.SSS" (primary format with milliseconds)
                    "yyyy-MM-dd HH:mm:ss",
                    // "yyyy-MM-dd HH:mm:ss" (compatible with legacy format)
                    "EEE MMM dd HH:mm:ss zzz yyyy",    // "Fri Sep 26 11:04:47 CST 2025"
                    "yyyy-MM-dd'T'HH:mm:ss.SSSZ",     // ISO format with timezone
                    "yyyy-MM-dd'T'HH:mm:ss'Z'",       // ISO format UTC
                    "yyyy-MM-dd"                       // Date only
            };

            for (String format : dateFormats) {
                try {
                    if (format.equals("EEE MMM dd HH:mm:ss zzz yyyy")) {
                        SimpleDateFormat sdf = new SimpleDateFormat(format, Locale.ENGLISH);
                        return sdf.parse(dateStr);
                    } else {
                        return SafeDateUtil.parse(dateStr, format);
                    }
                } catch (ParseException e) {
                }
            }

            try {
                return DateUtil.parse(dateStr);
            } catch (Exception e) {
                throw new ParseException("Unable to parse date: " + dateStr +
                                         ", tried formats: " +
                                         java.util.Arrays.toString(dateFormats), 0);
            }
        }

        protected Object[] asArray(List<Object> list) {
            E.checkState(this.create != null,
                         "Property %s time can't be null", CREATE);
            E.checkState(this.update != null,
                         "Property %s time can't be null", UPDATE);
            E.checkState(this.creator != null,
                         "Property %s can't be null", CREATOR);

            list.add(hideField(this.label(), CREATE));
            list.add(this.create);

            list.add(hideField(this.label(), UPDATE));
            list.add(this.update);

            list.add(hideField(this.label(), CREATOR));
            list.add(this.creator);

            return list.toArray();
        }

        public abstract ResourceType type();

        public abstract String label();

        public abstract Map<String, Object> asMap();

        protected abstract Object[] asArray();
    }

    // NOTE: travis-ci fails if class Entity implements Nameable
    public abstract static class Entity extends AuthElement
            implements org.apache.hugegraph.type.Nameable {

        private static final long serialVersionUID = 4113319546914811762L;

        public static <T extends Entity> T fromMap(Map<String, Object> map, T entity) {
            for (Map.Entry<String, Object> item : map.entrySet()) {
                entity.property(Hidden.hide(item.getKey()), item.getValue());
            }
            entity.id(IdGenerator.of(entity.name()));
            return entity;
        }

        protected static String hideField(String label, String key) {
            return label + "_" + key;
        }

        public static <T extends Entity> T fromVertex(Vertex vertex, T entity) {
            E.checkArgument(vertex.label().equals(entity.label()),
                            "Illegal vertex label '%s' for entity '%s'",
                            vertex.label(), entity.label());
            entity.id((Id) vertex.id());
            for (Iterator<VertexProperty<Object>> iter = vertex.properties();
                 iter.hasNext(); ) {
                VertexProperty<Object> prop = iter.next();
                entity.property(prop.key(), prop.value());
            }
            return entity;
        }

        @Override
        public String idString() {
            String label = Hidden.unHide(this.label());
            String name = this.name();
            return label + "(" + name + ")";
        }
    }

    public abstract static class Relationship extends AuthElement {

        private static final long serialVersionUID = -1406157381685832493L;

        public abstract String graphSpace();

        public abstract String sourceLabel();

        public abstract String targetLabel();

        public abstract Id source();

        public abstract Id target();

        public void setId() {
            this.id(IdGenerator.of(this.source().asString() + "->" +
                                   this.target().asString()));
        }

        public static <T extends Relationship> T fromMap(Map<String, Object> map, T entity) {
            for (Map.Entry<String, Object> item : map.entrySet()) {
                entity.property(Hidden.hide(item.getKey()), item.getValue());
            }
            entity.setId();
            return entity;
        }

        public static <T extends Relationship> T fromEdge(Edge edge,
                                                          T relationship) {
            E.checkArgument(edge.label().equals(relationship.label()),
                            "Illegal edge label '%s' for relationship '%s'",
                            edge.label(), relationship.label());
            relationship.id((Id) edge.id());
            for (Iterator<Property<Object>> iter = edge.properties();
                 iter.hasNext(); ) {
                Property<Object> prop = iter.next();
                relationship.property(prop.key(), prop.value());
            }
            return relationship;
        }

        @Override
        public String idString() {
            String label = Hidden.unHide(this.label());
            String sb = label + "(" + this.source() +
                        "->" + this.target() + ")";
            return sb;
        }
    }
}
