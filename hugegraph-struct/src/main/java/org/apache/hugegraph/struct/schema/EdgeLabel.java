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

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hugegraph.HugeGraphSupplier;
import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.id.IdGenerator;
import org.apache.hugegraph.struct.schema.builder.SchemaBuilder;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.type.define.Directions;
import org.apache.hugegraph.type.define.EdgeLabelType;
import org.apache.hugegraph.type.define.Frequency;
import org.apache.hugegraph.type.define.SchemaStatus;
import org.apache.hugegraph.util.E;

import com.google.common.base.Objects;

public class EdgeLabel extends SchemaLabel {

    public static final EdgeLabel NONE = new EdgeLabel(null, NONE_ID, UNDEF);

    private Set<Pair<Id, Id>> links = new HashSet<>();
    private Id sourceLabel = NONE_ID;
    private Id targetLabel = NONE_ID;
    private Frequency frequency;
    private final List<Id> sortKeys;

    private EdgeLabelType edgeLabelType = EdgeLabelType.NORMAL;
    private Id fatherId;

    public EdgeLabel(final HugeGraphSupplier graph, Id id, String name) {
        super(graph, id, name);
        this.frequency = Frequency.DEFAULT;
        this.sortKeys = new ArrayList<>();
    }

    @Override
    public HugeType type() {
        return HugeType.EDGE_LABEL;
    }

    public boolean isFather() {
        return this.edgeLabelType.parent();
    }

    public void edgeLabelType(EdgeLabelType type) {
        this.edgeLabelType = type;
    }

    public EdgeLabelType edgeLabelType() {
        return this.edgeLabelType;
    }

    public boolean hasFather() {
        return this.edgeLabelType.sub();
    }

    public boolean general() {
        return this.edgeLabelType.general();
    }

    public Id fatherId() {
        return this.fatherId;
    }

    public void fatherId(Id fatherId) {
        this.fatherId = fatherId;
    }

    public Frequency frequency() {
        return this.frequency;
    }

    public void frequency(Frequency frequency) {
        this.frequency = frequency;
    }

    public boolean directed() {
        // TODO: implement (do we need this method?)
        return true;
    }

    public String sourceLabelName() {
        E.checkState(this.links.size() == 1,
                     "Only edge label has single vertex label pair can call " +
                     "sourceLabelName(), but current edge label got %s",
                     this.links.size());
        return this.graph.vertexLabelOrNone(this.links.iterator().next().getLeft()).name();
    }

    public List<Id> linksIds() {
        List<Id> ids = new ArrayList<>(this.links.size() * 2);
        for (Pair<Id, Id> link : this.links) {
            ids.add(link.getLeft());
            ids.add(link.getRight());
        }
        return ids;
    }

    public void linksIds(Id[] ids) {
        this.links = new HashSet<>(ids.length / 2);
        for (int i = 0; i < ids.length; i += 2) {
            this.links.add(Pair.of(ids[i], ids[i + 1]));
        }
    }

    public Id sourceLabel() {
        if (links.size() == 1) {
            return links.iterator().next().getLeft();
        }
        return NONE_ID;
    }

    public void sourceLabel(Id id) {
        E.checkArgument(this.links.isEmpty(),
                        "Not allowed add source label to an edge label which " +
                        "already has links");
        if (this.targetLabel != NONE_ID) {
            this.links.add(Pair.of(id, this.targetLabel));
            this.targetLabel = NONE_ID;
        } else {
            this.sourceLabel = id;
        }
    }

    public String targetLabelName() {
        E.checkState(this.links.size() == 1,
                     "Only edge label has single vertex label pair can call " +
                     "sourceLabelName(), but current edge label got %s",
                     this.links.size());
        return this.graph.vertexLabelOrNone(this.links.iterator().next().getRight()).name();
    }

    public Id targetLabel() {
        if (links.size() == 1) {
            return links.iterator().next().getRight();
        }
        return NONE_ID;
    }

    public void targetLabel(Id id) {
        E.checkArgument(this.links.isEmpty(),
                        "Not allowed add source label to an edge label which " +
                        "already has links");
        if (this.sourceLabel != NONE_ID) {
            this.links.add(Pair.of(this.sourceLabel, id));
            this.sourceLabel = NONE_ID;
        } else {
            this.targetLabel = id;
        }
    }

    public boolean linkWithLabel(Id id) {
        for (Pair<Id, Id> link : this.links) {
            if (link.getLeft().equals(id) || link.getRight().equals(id)) {
                return true;
            }
        }
        return false;
    }

    public boolean linkWithVertexLabel(Id label, Directions dir) {
        return this.links.stream().anyMatch(pair -> {
            Id sourceLabel = pair.getLeft();
            Id targetLabel = pair.getRight();
            if (dir.equals(Directions.IN)) {
                return targetLabel.equals(label);
            } else if (dir.equals(Directions.OUT)) {
                return sourceLabel.equals(label);
            } else if (dir.equals(Directions.BOTH)) {
                return targetLabel.equals(label) || sourceLabel.equals(label);
            }
            return false;
        });
    }

    public boolean checkLinkEqual(Id sourceLabel, Id targetLabel) {
        return this.links.contains(Pair.of(sourceLabel, targetLabel));
    }

    public Set<Pair<Id, Id>> links() {
        return this.links;
    }

    public void links(Pair<Id, Id> link) {
        if (this.links == null) {
            this.links = new HashSet<>();
        }
        this.links.add(link);
    }

    public boolean existSortKeys() {
        return !this.sortKeys.isEmpty();
    }

    public List<Id> sortKeys() {
        return Collections.unmodifiableList(this.sortKeys);
    }

    public void sortKey(Id id) {
        this.sortKeys.add(id);
    }

    public void sortKeys(Id... ids) {
        this.sortKeys.addAll(Arrays.asList(ids));
    }

    public boolean hasSameContent(EdgeLabel other) {
        return super.hasSameContent(other) &&
               this.frequency == other.frequency &&
               Objects.equal(this.sourceLabelName(), other.sourceLabelName()) &&
               Objects.equal(this.targetLabelName(), other.targetLabelName()) &&
               Objects.equal(this.graph.mapPkId2Name(this.sortKeys),
                             other.graph.mapPkId2Name(other.sortKeys));
    }

    public static EdgeLabel undefined(HugeGraphSupplier graph, Id id) {
        return new EdgeLabel(graph, id, UNDEF);
    }

    public interface Builder extends SchemaBuilder<EdgeLabel> {

        Id rebuildIndex();

        Builder asBase();

        Builder withBase(String fatherLabel);

        Builder link(String sourceLabel, String targetLabel);

        @Deprecated
        Builder sourceLabel(String label);

        @Deprecated
        Builder targetLabel(String label);

        Builder singleTime();

        Builder multiTimes();

        Builder sortKeys(String... keys);

        Builder properties(String... properties);

        Builder nullableKeys(String... keys);

        Builder frequency(Frequency frequency);

        Builder ttl(long ttl);

        Builder ttlStartTime(String ttlStartTime);

        Builder enableLabelIndex(boolean enable);

        Builder userdata(String key, Object value);

        Builder userdata(Map<String, Object> userdata);
    }

    @Override
    public Map<String, Object> asMap() {
        Map<String, Object> map = new HashMap<>();

        if (this.sourceLabel() != null && this.sourceLabel() != NONE_ID) {
            map.put(P.SOURCE_LABEL, this.sourceLabel().asString());
        }

        if (this.targetLabel() != null && this.targetLabel() != NONE_ID) {
            map.put(P.TARGET_LABEL, this.targetLabel().asString());
        }

        if (this.properties() != null) {
            map.put(P.PROPERTIES, this.properties());
        }

        if (this.nullableKeys() != null) {
            map.put(P.NULLABLE_KEYS, this.nullableKeys());
        }

        if (this.indexLabels() != null) {
            map.put(P.INDEX_LABELS, this.indexLabels());
        }

        if (this.ttlStartTime() != null) {
            map.put(P.TT_START_TIME, this.ttlStartTime().asString());
        }

        if (this.sortKeys() != null) {
            map.put(P.SORT_KEYS, this.sortKeys);
        }

        map.put(P.EDGELABEL_TYPE, this.edgeLabelType);
        if (this.fatherId() != null) {
            map.put(P.FATHER_ID, this.fatherId().asString());
        }
        map.put(P.ENABLE_LABEL_INDEX, this.enableLabelIndex());
        map.put(P.TTL, String.valueOf(this.ttl()));
        map.put(P.LINKS, this.links());
        map.put(P.FREQUENCY, this.frequency().toString());

        return super.asMap(map);
    }

    @SuppressWarnings("unchecked")
    public static EdgeLabel fromMap(Map<String, Object> map, HugeGraphSupplier graph) {
        Id id = IdGenerator.of((int) map.get(EdgeLabel.P.ID));
        String name = (String) map.get(EdgeLabel.P.NAME);
        EdgeLabel edgeLabel = new EdgeLabel(graph, id, name);
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            switch (entry.getKey()) {
                case P.ID:
                case P.NAME:
                    break;
                case P.STATUS:
                    edgeLabel.status(
                        SchemaStatus.valueOf(((String) entry.getValue()).toUpperCase()));
                    break;
                case P.USERDATA:
                    edgeLabel.userdata(new Userdata((Map<String, Object>) entry.getValue()));
                    break;
                case P.PROPERTIES:
                    Set<Id> ids = ((List<Integer>) entry.getValue()).stream().map(
                            IdGenerator::of).collect(Collectors.toSet());
                    edgeLabel.properties(ids);
                    break;
                case P.NULLABLE_KEYS:
                    ids = ((List<Integer>) entry.getValue()).stream().map(
                            IdGenerator::of).collect(Collectors.toSet());
                    edgeLabel.nullableKeys(ids);
                    break;
                case P.INDEX_LABELS:
                    ids = ((List<Integer>) entry.getValue()).stream().map(
                            IdGenerator::of).collect(Collectors.toSet());
                    edgeLabel.addIndexLabels(ids.toArray(new Id[0]));
                    break;
                case P.ENABLE_LABEL_INDEX:
                    boolean enableLabelIndex = (Boolean) entry.getValue();
                    edgeLabel.enableLabelIndex(enableLabelIndex);
                    break;
                case P.TTL:
                    long ttl = Long.parseLong((String) entry.getValue());
                    edgeLabel.ttl(ttl);
                    break;
                case P.TT_START_TIME:
                    long ttlStartTime =
                            Long.parseLong((String) entry.getValue());
                    edgeLabel.ttlStartTime(IdGenerator.of(ttlStartTime));
                    break;
                case P.LINKS:
                    // TODO: serialize and deserialize
                    List<Map> list = (List<Map>) entry.getValue();
                    for (Map m : list) {
                        for (Object key : m.keySet()) {
                            Id sid = IdGenerator.of(Long.parseLong((String) key));
                            Id tid = IdGenerator.of(Long.parseLong(String.valueOf(m.get(key))));
                            edgeLabel.links(Pair.of(sid, tid));
                        }
                    }
                    break;
                case P.SOURCE_LABEL:
                    long sourceLabel =
                            Long.parseLong((String) entry.getValue());
                    edgeLabel.sourceLabel(IdGenerator.of(sourceLabel));
                    break;
                case P.TARGET_LABEL:
                    long targetLabel =
                            Long.parseLong((String) entry.getValue());
                    edgeLabel.targetLabel(IdGenerator.of(targetLabel));
                    break;
                case P.FATHER_ID:
                    long fatherId =
                            Long.parseLong((String) entry.getValue());
                    edgeLabel.fatherId(IdGenerator.of(fatherId));
                    break;
                case P.EDGELABEL_TYPE:
                    EdgeLabelType edgeLabelType =
                            EdgeLabelType.valueOf(
                                    ((String) entry.getValue()).toUpperCase());
                    edgeLabel.edgeLabelType(edgeLabelType);
                    break;
                case P.FREQUENCY:
                    Frequency frequency =
                            Frequency.valueOf(((String) entry.getValue()).toUpperCase());
                    edgeLabel.frequency(frequency);
                    break;
                case P.SORT_KEYS:
                    ids = ((List<Integer>) entry.getValue()).stream().map(
                            IdGenerator::of).collect(Collectors.toSet());
                    edgeLabel.sortKeys(ids.toArray(new Id[0]));
                    break;
                default:
                    throw new AssertionError(String.format(
                            "Invalid key '%s' for edge label",
                            entry.getKey()));
            }
        }
        return edgeLabel;
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
        public static final String LINKS = "links";
        public static final String SOURCE_LABEL = "sourceLabel";
        public static final String TARGET_LABEL = "targetLabel";
        public static final String EDGELABEL_TYPE = "edgeLabelType";
        public static final String FATHER_ID = "fatherId";
        public static final String FREQUENCY = "frequency";
        public static final String SORT_KEYS = "sortKeys";
    }
}
