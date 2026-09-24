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

package org.apache.hugegraph.struct.schema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.hugegraph.HugeGraphSupplier;
import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.id.IdGenerator;
import org.apache.hugegraph.struct.schema.builder.SchemaBuilder;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.type.define.IdStrategy;
import org.apache.hugegraph.type.define.SchemaStatus;
import org.apache.hugegraph.util.GraphUtils;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableSet;

public class VertexLabel extends SchemaLabel {

    public static final VertexLabel NONE = new VertexLabel(null, NONE_ID, UNDEF);
    public static final VertexLabel GENERAL =
            new VertexLabel(null, NONE_ID, VertexLabel.GENERAL_VL);


    // OLAP_VL_ID means all of vertex label ids
    private static final Id OLAP_VL_ID = IdGenerator.of(SchemaLabel.OLAP_VL_ID);
    // OLAP_VL_NAME means all of vertex label names
    private static final String OLAP_VL_NAME = "*olap";
    // OLAP_VL means all of vertex labels
    public static final VertexLabel OLAP_VL = new VertexLabel(null, OLAP_VL_ID,
                                                              OLAP_VL_NAME);

    public static final String GENERAL_VL = "~general_vl";

    private IdStrategy idStrategy;
    private final List<Id> primaryKeys;

    public VertexLabel(final HugeGraphSupplier graph, Id id, String name) {
        super(graph, id, name);
        this.idStrategy = IdStrategy.DEFAULT;
        this.primaryKeys = new ArrayList<>();
    }

    @Override
    public HugeType type() {
        return HugeType.VERTEX_LABEL;
    }

    public boolean olap() {
        return VertexLabel.OLAP_VL.id().equals(this.id());
    }

    public IdStrategy idStrategy() {
        return this.idStrategy;
    }

    public void idStrategy(IdStrategy idStrategy) {
        this.idStrategy = idStrategy;
    }

    public List<Id> primaryKeys() {
        return Collections.unmodifiableList(this.primaryKeys);
    }

    public void primaryKey(Id id) {
        this.primaryKeys.add(id);
    }

    public void primaryKeys(Id... ids) {
        this.primaryKeys.addAll(Arrays.asList(ids));
    }


    @Override
    public Set<Id> extendProperties() {
        Set<Id> properties = new HashSet<>();
        properties.addAll(this.properties());
        properties.addAll(this.primaryKeys);

        this.graph().propertyKeys().stream().forEach(pk -> {
            if (pk.olap()) {
                properties.add(pk.id());
            }
        });

        return Collections.unmodifiableSet(properties);
    }

    @Override
    public Set<Id> extendIndexLabels() {
        Set<Id> indexes = new HashSet<>();

        indexes.addAll(this.indexLabels());

        for (IndexLabel il : this.graph.indexLabels()) {
            if (il.olap()) {
                indexes.add(il.id());
            }
        }

        return ImmutableSet.copyOf(indexes);
    }

    public boolean existsLinkLabel() {
        return this.graph().existsLinkLabel(this.id());
    }

    public boolean hasSameContent(VertexLabel other) {
        return super.hasSameContent(other) &&
               this.idStrategy == other.idStrategy &&
               Objects.equal(this.graph.mapPkId2Name(this.primaryKeys),
                             other.graph.mapPkId2Name(other.primaryKeys));
    }

    public static VertexLabel undefined(HugeGraphSupplier graph) {
        return new VertexLabel(graph, NONE_ID, UNDEF);
    }

    public static VertexLabel undefined(HugeGraphSupplier graph, Id id) {
        return new VertexLabel(graph, id, UNDEF);
    }

    public String convert2Groovy(boolean attachIdFlag) {
        StringBuilder builder = new StringBuilder(SCHEMA_PREFIX);
        // Name
        if (!attachIdFlag) {
            builder.append("vertexLabel").append("('")
                   .append(this.name())
                   .append("')");
        } else {
            builder.append("vertexLabel").append("(")
                   .append(longId()).append(", '")
                   .append(this.name())
                   .append("')");
        }

        // Properties
        Set<Id> properties = this.properties();
        if (!properties.isEmpty()) {
            builder.append(".").append("properties(");

            int size = properties.size();
            for (Id id : this.properties()) {
                PropertyKey pk = this.graph.propertyKey(id);
                builder.append("'")
                       .append(pk.name())
                       .append("'");
                if (--size > 0) {
                    builder.append(",");
                }
            }
            builder.append(")");
        }

        // Id strategy
        switch (this.idStrategy()) {
            case PRIMARY_KEY:
                builder.append(".primaryKeys(");
                List<Id> pks = this.primaryKeys();
                int size = pks.size();
                for (Id id : pks) {
                    PropertyKey pk = this.graph.propertyKey(id);
                    builder.append("'")
                           .append(pk.name())
                           .append("'");
                    if (--size > 0) {
                        builder.append(",");
                    }
                }
                builder.append(")");
                break;
            case CUSTOMIZE_STRING:
                builder.append(".useCustomizeStringId()");
                break;
            case CUSTOMIZE_NUMBER:
                builder.append(".useCustomizeNumberId()");
                break;
            case CUSTOMIZE_UUID:
                builder.append(".useCustomizeUuidId()");
                break;
            case AUTOMATIC:
                builder.append(".useAutomaticId()");
                break;
            default:
                throw new AssertionError(String.format(
                        "Invalid id strategy '%s'", this.idStrategy()));
        }

        // Nullable keys
        properties = this.nullableKeys();
        if (!properties.isEmpty()) {
            builder.append(".").append("nullableKeys(");
            int size = properties.size();
            for (Id id : properties) {
                PropertyKey pk = this.graph.propertyKey(id);
                builder.append("'")
                       .append(pk.name())
                       .append("'");
                if (--size > 0) {
                    builder.append(",");
                }
            }
            builder.append(")");
        }

        // TTL
        if (this.ttl() != 0) {
            builder.append(".ttl(")
                   .append(this.ttl())
                   .append(")");
            if (this.ttlStartTime() != null &&
                !this.ttlStartTime().equals(SchemaLabel.NONE_ID)) {
                PropertyKey pk = this.graph.propertyKey(this.ttlStartTime());
                builder.append(".ttlStartTime('")
                       .append(pk.name())
                       .append("')");
            }
        }

        // Enable label index
        if (this.enableLabelIndex()) {
            builder.append(".enableLabelIndex(true)");
        } else {
            builder.append(".enableLabelIndex(false)");
        }

        // User data
        Map<String, Object> userdata = this.userdata();
        if (userdata.isEmpty()) {
            return builder.toString();
        }
        for (Map.Entry<String, Object> entry : userdata.entrySet()) {
            if (GraphUtils.isHidden(entry.getKey())) {
                continue;
            }
            builder.append(".userdata('")
                   .append(entry.getKey())
                   .append("',")
                   .append(entry.getValue())
                   .append(")");
        }

        builder.append(".ifNotExist().create();");
        return builder.toString();
    }

    public interface Builder extends SchemaBuilder<VertexLabel> {

        Id rebuildIndex();

        Builder idStrategy(IdStrategy idStrategy);

        Builder useAutomaticId();

        Builder usePrimaryKeyId();

        Builder useCustomizeStringId();

        Builder useCustomizeNumberId();

        Builder useCustomizeUuidId();

        Builder properties(String... properties);

        Builder primaryKeys(String... keys);

        Builder nullableKeys(String... keys);

        Builder ttl(long ttl);

        Builder ttlStartTime(String ttlStartTime);

        Builder enableLabelIndex(boolean enable);

        Builder userdata(String key, Object value);

        Builder userdata(Map<String, Object> userdata);
    }

    @Override
    public Map<String, Object> asMap() {
        HashMap<String, Object> map = new HashMap();

        map.put(P.PROPERTIES, this.properties());

        map.put(P.NULLABLE_KEYS, this.nullableKeys());

        map.put(P.INDEX_LABELS, this.indexLabels());

        map.put(P.ENABLE_LABEL_INDEX, this.enableLabelIndex());

        map.put(P.TTL, String.valueOf(this.ttl()));

        map.put(P.TT_START_TIME, this.ttlStartTime().asString());

        map.put(P.ID_STRATEGY, this.idStrategy().string());

        map.put(P.PRIMARY_KEYS, this.primaryKeys());

        return super.asMap(map);
    }

    public boolean generalVl(){
        return this.name() == GENERAL_VL;
    }

    @SuppressWarnings("unchecked")
    public static VertexLabel fromMap(Map<String, Object> map, HugeGraphSupplier graph) {
        Id id = IdGenerator.of((int) map.get(VertexLabel.P.ID));
        String name = (String) map.get(VertexLabel.P.NAME);

        VertexLabel vertexLabel = new VertexLabel(graph, id, name);
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            switch (entry.getKey()) {
                case P.ID:
                case P.NAME:
                    break;
                case P.STATUS:
                    vertexLabel.status(
                        SchemaStatus.valueOf(((String) entry.getValue()).toUpperCase()));
                    break;
                case P.USERDATA:
                    vertexLabel.userdata(new Userdata((Map<String, Object>) entry.getValue()));
                    break;
                case P.PROPERTIES:
                    Set<Id> ids = ((List<Integer>) entry.getValue()).stream().map(
                            IdGenerator::of).collect(Collectors.toSet());
                    vertexLabel.properties(ids);
                    break;
                case P.NULLABLE_KEYS:
                    ids = ((List<Integer>) entry.getValue()).stream().map(
                            IdGenerator::of).collect(Collectors.toSet());
                    vertexLabel.nullableKeys(ids);
                    break;
                case P.INDEX_LABELS:
                    ids = ((List<Integer>) entry.getValue()).stream().map(
                            IdGenerator::of).collect(Collectors.toSet());
                    vertexLabel.addIndexLabels(ids.toArray(new Id[0]));
                    break;
                case P.ENABLE_LABEL_INDEX:
                    boolean enableLabelIndex = (Boolean) entry.getValue();
                    vertexLabel.enableLabelIndex(enableLabelIndex);
                    break;
                case P.TTL:
                    long ttl = Long.parseLong((String) entry.getValue());
                    vertexLabel.ttl(ttl);
                    break;
                case P.TT_START_TIME:
                    long ttlStartTime =
                            Long.parseLong((String) entry.getValue());
                    vertexLabel.ttlStartTime(IdGenerator.of(ttlStartTime));
                    break;
                case P.ID_STRATEGY:
                    IdStrategy idStrategy =
                            IdStrategy.valueOf(((String) entry.getValue()).toUpperCase());
                    vertexLabel.idStrategy(idStrategy);
                    break;
                case P.PRIMARY_KEYS:
                    ids = ((List<Integer>) entry.getValue()).stream().map(
                            IdGenerator::of).collect(Collectors.toSet());
                    vertexLabel.primaryKeys(ids.toArray(new Id[0]));
                    break;
                default:
                    throw new AssertionError(String.format(
                            "Invalid key '%s' for vertex label",
                            entry.getKey()));
            }
        }
        return vertexLabel;
    }

    public static final class P {

        public static final String ID = "id";
        public static final String NAME = "name";

        public static final String STATUS = "status";
        public static final String USERDATA = "userdata";

        public static final String PROPERTIES = "properties";
        public static final String NULLABLE_KEYS = "nullableKeys";
        public static final String INDEX_LABELS = "indexLabels";

        public static final String ENABLE_LABEL_INDEX = "enableLabelIndex";
        public static final String TTL = "ttl";
        public static final String TT_START_TIME = "ttlStartTime";
        public static final String ID_STRATEGY = "idStrategy";
        public static final String PRIMARY_KEYS = "primaryKeys";
    }
}
