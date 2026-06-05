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

package org.apache.hugegraph.schema.builder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.id.IdGenerator;
import org.apache.hugegraph.backend.tx.ISchemaTransaction;
import org.apache.hugegraph.exception.ExistedException;
import org.apache.hugegraph.exception.NotAllowException;
import org.apache.hugegraph.exception.NotFoundException;
import org.apache.hugegraph.schema.EdgeLabel;
import org.apache.hugegraph.schema.PropertyKey;
import org.apache.hugegraph.schema.Userdata;
import org.apache.hugegraph.schema.VertexLabel;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.type.define.Action;
import org.apache.hugegraph.type.define.EdgeLabelType;
import org.apache.hugegraph.type.define.Frequency;
import org.apache.hugegraph.util.CollectionUtil;
import org.apache.hugegraph.util.E;

import com.google.common.collect.ImmutableList;

public class EdgeLabelBuilder extends AbstractBuilder
        implements EdgeLabel.Builder {

    private Id id;
    private String name;
    private Set<Pair<String, String>> links;
    private EdgeLabelType edgeLabelType;
    private String fatherLabel;
    private String sourceLabel;
    private String targetLabel;
    private Frequency frequency;
    private Set<String> properties;
    private List<String> sortKeys;
    private Set<String> nullableKeys;
    private Long ttl;
    private String ttlStartTime;
    private Boolean enableLabelIndex;
    private Userdata userdata;
    private boolean checkExist;

    public EdgeLabelBuilder(ISchemaTransaction transaction,
                            HugeGraph graph, String name) {
        super(transaction, graph);
        E.checkNotNull(name, "name");
        this.id = null;
        this.name = name;
        this.links = new HashSet<>();
        this.edgeLabelType = EdgeLabelType.NORMAL;
        this.fatherLabel = null;
        this.frequency = Frequency.DEFAULT;
        this.properties = new HashSet<>();
        this.sortKeys = new ArrayList<>();
        this.nullableKeys = new HashSet<>();
        this.ttl = null;
        this.ttlStartTime = null;
        this.enableLabelIndex = null;
        this.userdata = new Userdata();
        this.checkExist = true;
    }

    public EdgeLabelBuilder(ISchemaTransaction transaction,
                            HugeGraph graph, EdgeLabel copy) {
        super(transaction, graph);
        E.checkNotNull(copy, "copy");
        HugeGraph origin = copy.graph();
        this.id = null;
        this.name = copy.name();
        this.links = mapPairId2Name(origin, copy.links());
        this.edgeLabelType = copy.edgeLabelType();
        if (copy.edgeLabelType().sub()) {
            this.fatherLabel = mapElId2Name(origin, copy.fatherId());
        }
        this.frequency = copy.frequency();
        this.properties = mapPkId2Name(origin, copy.properties());
        this.sortKeys = mapPkId2Name(origin, copy.sortKeys());
        this.nullableKeys = mapPkId2Name(origin, copy.nullableKeys());
        this.ttl = copy.ttl();
        this.ttlStartTime = copy.ttlStartTimeName();
        this.enableLabelIndex = copy.enableLabelIndex();
        this.userdata = new Userdata(copy.userdata());
        this.checkExist = false;
    }

    @Override
    public EdgeLabel build() {
        Id id = this.validOrGenerateId(HugeType.EDGE_LABEL,
                                       this.id, this.name);
        HugeGraph graph = this.graph();
        EdgeLabel edgeLabel = new EdgeLabel(graph, id, this.name);
        for (Pair<String, String> link : this.links) {
            edgeLabel.links(Pair.of(graph.vertexLabel(link.getLeft()).id(),
                                    graph.vertexLabel(link.getRight()).id()));
        }
        edgeLabel.frequency(this.frequency == Frequency.DEFAULT ?
                            Frequency.SINGLE : this.frequency);
        this.updateTTL(edgeLabel);
        edgeLabel.enableLabelIndex(this.enableLabelIndex == null ||
                                   this.enableLabelIndex);
        for (String key : this.properties) {
            PropertyKey propertyKey = graph.propertyKey(key);
            edgeLabel.property(propertyKey.id());
        }
        for (String key : this.sortKeys) {
            PropertyKey propertyKey = graph.propertyKey(key);
            edgeLabel.sortKey(propertyKey.id());
        }
        for (String key : this.nullableKeys) {
            PropertyKey propertyKey = graph.propertyKey(key);
            edgeLabel.nullableKey(propertyKey.id());
        }
        edgeLabel.userdata(this.userdata);
        if (this.edgeLabelType.sub()) {
            edgeLabel.edgeLabelType(EdgeLabelType.SUB);
            EdgeLabel fatherEl = graph.edgeLabel(this.fatherLabel);
            edgeLabel.fatherId(fatherEl.id());
            registerInfoToFatherEl(fatherEl, edgeLabel);
        } else {
            edgeLabel.edgeLabelType(this.edgeLabelType);
        }
        return edgeLabel;
    }

    private void registerInfoToFatherEl(EdgeLabel fatherEl, EdgeLabel subEl) {
        HugeGraph graph = this.graph();
        // When the new edge label is a subtype, register the links information
        // of the sub edgelabel to the parent edge Label
        for (Pair<String, String> link : this.links) {
            fatherEl.links(Pair.of(this.graph().vertexLabel(link.getLeft()).id(),
                                   graph.vertexLabel(link.getRight()).id()));
        }

        List<Id> fatherSortKeys = fatherEl.sortKeys();
        List<Id> subSortKeys = subEl.sortKeys();
        if (fatherSortKeys == null || fatherSortKeys.size() == 0) {
            for (Id sortKey : subSortKeys) {
                fatherEl.sortKeys(sortKey);
            }
        } else {
            E.checkArgument(fatherSortKeys.size() == subSortKeys.size(),
                            "The sortKeys of each sub edgelabel need to be " +
                            "consistent. " + "Currently, the sortKeys of already exist edgelabel " +
                            "are " + "'%s', " + "and the sortKeys of newly added " +
                            "sub edgelabel are '%s'",
                            fatherSortKeys, subSortKeys);
            for (int i = 0; i < fatherSortKeys.size(); i++) {
                E.checkArgument(fatherSortKeys.get(i).equals(subSortKeys.get(i)),
                                "The sortKeys of each sub edgelabel need to be " +
                                "consistent. " +
                                "Currently, the sortKeys of already exist edgelabel " +
                                "are " + "'%s', " + "and the sortKeys of newly added " +
                                "sub edgelabel are '%s'",
                                fatherSortKeys, subSortKeys);
            }
        }
        this.graph().updateEdgeLabel(fatherEl);
    }

    @Override
    public EdgeLabel create() {
        HugeType type = HugeType.EDGE_LABEL;
        this.checkSchemaName(this.name);

        return this.lockCheckAndCreateSchema(type, this.name, name -> {
            EdgeLabel edgeLabel = this.edgeLabelOrNull(this.name);
            if (edgeLabel != null) {
                if (this.checkExist || !hasSameProperties(edgeLabel)) {
                    throw new ExistedException(type, this.name);
                }
                return edgeLabel;
            }
            this.checkSchemaIdIfRestoringMode(type, this.id);

            // These methods will check params and fill to member variables
            this.checkRelation();
            this.checkProperties(Action.INSERT);
            this.checkSortKeys();
            this.checkNullableKeys(Action.INSERT);
            Userdata.check(this.userdata, Action.INSERT);
            this.checkTTL();
            this.checkUserdata(Action.INSERT);

            edgeLabel = this.build();
            assert edgeLabel.name().equals(name);
            this.graph().addEdgeLabel(edgeLabel);
            return edgeLabel;
        });
    }

    /**
     * Check whether this has same properties with existedEdgeLabel.
     * Only sourceId, targetId, frequency, enableLabelIndex, properties, sortKeys,
     * nullableKeys are checked.
     * The id, ttl, ttlStartTime, userdata are not checked.
     *
     * @param existedEdgeLabel to be compared with
     * @return true if this has same properties with existedVertexLabel
     */
    private boolean hasSameProperties(EdgeLabel existedEdgeLabel) {
        HugeGraph graph = this.graph();
        for (Pair<Id, Id> link : existedEdgeLabel.links()) {
            String sourceName = graph.vertexLabel(link.getLeft()).name();
            String targetName = graph.vertexLabel(link.getRight()).name();
            if (!this.links.contains(Pair.of(sourceName, targetName))) {
                return false;
            }
        }

        if ((this.frequency == Frequency.DEFAULT &&
             existedEdgeLabel.frequency() != Frequency.SINGLE) ||
            (this.frequency != Frequency.DEFAULT &&
             this.frequency != existedEdgeLabel.frequency())) {
            return false;
        }

        // this.enableLabelIndex == null, it means true.
        if (this.enableLabelIndex == null || this.enableLabelIndex) {
            if (!existedEdgeLabel.enableLabelIndex()) {
                return false;
            }
        } else { // this false
            if (existedEdgeLabel.enableLabelIndex()) {
                return false;
            }
        }

        Set<Id> existedProperties = existedEdgeLabel.properties();
        if (this.properties.size() != existedProperties.size()) {
            return false;
        }
        for (String key : this.properties) {
            PropertyKey propertyKey = graph.propertyKey(key);
            if (!existedProperties.contains(propertyKey.id())) {
                return false;
            }
        }

        List<Id> existedSortKeys = existedEdgeLabel.sortKeys();
        if (this.sortKeys.size() != existedSortKeys.size()) {
            return false;
        }
        for (String key : this.sortKeys) {
            PropertyKey propertyKey = graph.propertyKey(key);
            if (!existedSortKeys.contains(propertyKey.id())) {
                return false;
            }
        }

        Set<Id> existedNullableKeys = existedEdgeLabel.nullableKeys();
        if (this.nullableKeys.size() != existedNullableKeys.size()) {
            return false;
        }
        for (String nullableKeyName : this.nullableKeys) {
            PropertyKey nullableKey = graph.propertyKey(nullableKeyName);
            if (!existedNullableKeys.contains(nullableKey.id())) {
                return false;
            }
        }
        return true;
    }

    @Override
    public EdgeLabel append() {
        EdgeLabel edgeLabel = this.edgeLabelOrNull(this.name);
        if (edgeLabel == null) {
            throw new NotFoundException("Can't update edge label '%s' " +
                                        "since it doesn't exist", this.name);
        }
        // These methods will check params and fill to member variables
        this.checkStableVars();
        this.checkProperties(Action.APPEND);
        this.checkNullableKeys(Action.APPEND);
        Userdata.check(this.userdata, Action.APPEND);

        for (String key : this.properties) {
            PropertyKey propertyKey = this.graph().propertyKey(key);
            edgeLabel.property(propertyKey.id());
        }
        for (String key : this.nullableKeys) {
            PropertyKey propertyKey = this.graph().propertyKey(key);
            edgeLabel.nullableKey(propertyKey.id());
        }
        this.updateTTL(edgeLabel);
        edgeLabel.userdata(this.userdata);
        this.graph().updateEdgeLabel(edgeLabel);
        return edgeLabel;
    }

    @Override
    public EdgeLabel eliminate() {
        EdgeLabel edgeLabel = this.edgeLabelOrNull(this.name);
        if (edgeLabel == null) {
            throw new NotFoundException("Can't update edge label '%s' " +
                                        "since it doesn't exist", this.name);
        }
        // Only allowed to eliminate user data
        this.checkStableVars();
        this.checkProperties(Action.ELIMINATE);
        this.checkNullableKeys(Action.ELIMINATE);
        Userdata.check(this.userdata, Action.ELIMINATE);

        edgeLabel.removeUserdata(this.userdata);
        this.graph().updateEdgeLabel(edgeLabel);
        return edgeLabel;
    }

    @Override
    public Id remove() {
        EdgeLabel edgeLabel = this.edgeLabelOrNull(this.name);
        if (edgeLabel == null) {
            return null;
        }
        return this.graph().removeEdgeLabel(edgeLabel.id());
    }

    @Override
    public Id rebuildIndex() {
        EdgeLabel edgeLabel = this.edgeLabelOrNull(this.name);
        if (edgeLabel == null) {
            return null;
        }
        return this.graph().rebuildIndex(edgeLabel);
    }

    @Override
    public EdgeLabel.Builder asBase() {
        this.edgeLabelType = EdgeLabelType.PARENT;
        return this;
    }

    @Override
    public EdgeLabel.Builder withBase(String fatherLabel) {
        // Check if fatherLabel is reasonable (if it exists or not)
        E.checkArgumentNotNull(fatherLabel, "When creating a subtype edgeLabel, " +
                                            "the edgeLabel name of the parent type edgeLabel must" +
                                            " be entered");
        EdgeLabel edgeLabel = this.edgeLabelOrNull(fatherLabel);
        if (edgeLabel == null) {
            throw new NotFoundException("Can't create subtype edge label '%s' " +
                                        "since it's parent edge label doesn't exist", this.name);
        }
        this.edgeLabelType = EdgeLabelType.SUB;
        this.fatherLabel = fatherLabel;
        return this;
    }

    @Override
    public EdgeLabelBuilder id(long id) {
        E.checkArgument(id != 0L, "Not allowed to assign 0 as edge label id");
        this.id = IdGenerator.of(id);
        return this;
    }

    @Override
    public EdgeLabelBuilder properties(String... properties) {
        this.properties.addAll(Arrays.asList(properties));
        return this;
    }

    @Override
    public EdgeLabelBuilder nullableKeys(String... keys) {
        this.nullableKeys.addAll(Arrays.asList(keys));
        return this;
    }

    @Override
    public EdgeLabelBuilder sortKeys(String... keys) {
        if (keys.length == 0) {
            return this;
        }

        E.checkArgument(this.sortKeys.isEmpty(),
                        "Not allowed to assign sort keys multitimes");

        List<String> sortKeys = Arrays.asList(keys);
        E.checkArgument(CollectionUtil.allUnique(sortKeys),
                        "Invalid sort keys %s, which contains some " +
                        "duplicate properties", sortKeys);
        this.sortKeys.addAll(sortKeys);
        return this;
    }

    @Override
    public EdgeLabelBuilder link(String sourceLabel, String targetLabel) {
        if (this.links == null) {
            this.links = new HashSet<>();
        }
        this.links.add(Pair.of(sourceLabel, targetLabel));
        return this;
    }

    @Override
    public EdgeLabelBuilder sourceLabel(String label) {
        E.checkArgument(this.links.isEmpty(),
                        "Not allowed add source label to an edge label which " +
                        "already has links");
        if (this.targetLabel != null) {
            this.links.add(Pair.of(label, this.targetLabel));
            this.targetLabel = null;
        } else {
            this.sourceLabel = label;
        }
        return this;
    }

    @Override
    public EdgeLabelBuilder targetLabel(String label) {
        E.checkArgument(this.links.isEmpty(),
                        "Not allowed add source label to an edge label which " +
                        "already has links");
        if (this.sourceLabel != null) {
            this.links.add(Pair.of(this.sourceLabel, label));
            this.sourceLabel = null;
        } else {
            this.targetLabel = label;
        }
        return this;
    }

    @Override
    public EdgeLabelBuilder singleTime() {
        this.frequency = Frequency.SINGLE;
        return this;
    }

    @Override
    public EdgeLabelBuilder multiTimes() {
        this.frequency = Frequency.MULTIPLE;
        return this;
    }

    @Override
    public EdgeLabelBuilder ifNotExist() {
        this.checkExist = false;
        return this;
    }

    @Override
    public EdgeLabelBuilder frequency(Frequency frequency) {
        this.frequency = frequency;
        return this;
    }

    @Override
    public EdgeLabel.Builder ttl(long ttl) {
        this.ttl = ttl;
        return this;
    }

    @Override
    public EdgeLabel.Builder ttlStartTime(String ttlStartTime) {
        this.ttlStartTime = ttlStartTime;
        return this;
    }

    @Override
    public EdgeLabelBuilder enableLabelIndex(boolean enable) {
        this.enableLabelIndex = enable;
        return this;
    }

    @Override
    public EdgeLabelBuilder userdata(String key, Object value) {
        this.userdata.put(key, value);
        return this;
    }

    @Override
    public EdgeLabelBuilder userdata(Map<String, Object> userdata) {
        this.userdata.putAll(userdata);
        return this;
    }

    @Override
    public EdgeLabelBuilder checkExist(boolean checkExist) {
        this.checkExist = checkExist;
        return this;
    }

    private void checkProperties(Action action) {
        switch (action) {
            case INSERT:
            case APPEND:
                for (String key : this.properties) {
                    this.graph().propertyKey(key);
                }
                break;
            case ELIMINATE:
                if (!this.properties.isEmpty()) {
                    throw new NotAllowException(
                            "Not support to eliminate properties " +
                            "for edge label currently");
                }
                break;
            case DELETE:
                break;
            default:
                throw new AssertionError(String.format(
                        "Unknown schema action '%s'", action));
        }
    }

    @SuppressWarnings("unchecked")
    private void checkNullableKeys(Action action) {
        // Not using switch-case to avoid indent too much
        if (action == Action.ELIMINATE) {
            if (!this.nullableKeys.isEmpty()) {
                throw new NotAllowException(
                        "Not support to eliminate nullableKeys " +
                        "for edge label currently");
            }
            return;
        }

        EdgeLabel edgeLabel = this.edgeLabelOrNull(this.name);
        // The originProps is empty when firstly create edge label
        List<String> originProps = edgeLabel == null ?
                                   ImmutableList.of() :
                                   this.graph()
                                       .mapPkId2Name(edgeLabel.properties());
        Set<String> appendProps = this.properties;

        E.checkArgument(CollectionUtil.union(originProps, appendProps)
                                      .containsAll(this.nullableKeys),
                        "The nullableKeys: %s to be created or appended " +
                        "must belong to the origin/new properties: %s/%s ",
                        this.nullableKeys, originProps, appendProps);

        List<String> sortKeys = edgeLabel == null ?
                                this.sortKeys :
                                this.graph()
                                    .mapPkId2Name(edgeLabel.sortKeys());
        E.checkArgument(!CollectionUtil.hasIntersection(sortKeys,
                                                        this.nullableKeys),
                        "The nullableKeys: %s are not allowed to " +
                        "belong to sortKeys: %s of edge label '%s'",
                        this.nullableKeys, sortKeys, this.name);

        if (action == Action.APPEND) {
            Collection<String> newAddedProps = CollectionUtils.subtract(
                    appendProps, originProps);
            E.checkArgument(this.nullableKeys.containsAll(newAddedProps),
                            "The new added properties: %s must be nullable",
                            newAddedProps);
        }
    }

    private void checkSortKeys() {
        if (this.frequency == Frequency.SINGLE ||
            this.frequency == Frequency.DEFAULT) {
            E.checkArgument(this.sortKeys.isEmpty(),
                            "EdgeLabel can't contain sortKeys " +
                            "when the cardinality property is single");
        } else {
            E.checkState(this.sortKeys != null,
                         "The sortKeys can't be null when the " +
                         "cardinality property is multiple");
            E.checkArgument(!this.sortKeys.isEmpty(),
                            "EdgeLabel must contain sortKeys " +
                            "when the cardinality property is multiple");
        }

        if (this.sortKeys.isEmpty()) {
            return;
        }

        // Check whether the properties contains the specified keys
        E.checkArgument(!this.properties.isEmpty(),
                        "The properties can't be empty when exist " +
                        "sort keys for edge label '%s'", this.name);

        for (String key : this.sortKeys) {
            E.checkArgument(this.properties.contains(key),
                            "The sort key '%s' must be contained in " +
                            "properties '%s' for edge label '%s'",
                            key, this.name, this.properties);
        }
    }

    private void checkRelation() {
        if (this.edgeLabelType.parent()) {
            E.checkArgument(this.links.isEmpty(),
                            "The links of the parent edge label must be empty");
        } else {
            E.checkArgument(!this.links.isEmpty(),
                            "The links of standard and subtype edge label " +
                            "can't be empty");
            E.checkArgument(this.links.size() == 1,
                            "The links size of standard and subtype edge " +
                            "label must be 1");
            for (Pair<String, String> link : this.links) {
                String srcLabel = link.getLeft();
                String tgtLabel = link.getRight();
                E.checkArgument(srcLabel != null && tgtLabel != null,
                                "Must set source and target label " +
                                "for edge label '%s'", this.name);
                E.checkArgumentNotNull(this.vertexLabelOrNull(srcLabel),
                                       "Undefined source vertex label '%s' " +
                                       "in edge label '%s'", srcLabel,
                                       this.name);
                E.checkArgumentNotNull(this.vertexLabelOrNull(tgtLabel),
                                       "Undefined target vertex label '%s' " +
                                       "in edge label '%s'", tgtLabel,
                                       this.name);
            }
        }
    }

    private void checkStableVars() {
        if (this.sourceLabel != null) {
            throw new NotAllowException(
                    "Not allowed to update source label " +
                    "for edge label '%s', it must be null", this.name);
        }
        if (this.targetLabel != null) {
            throw new NotAllowException(
                    "Not allowed to update target label " +
                    "for edge label '%s', it must be null", this.name);
        }
        if (this.links != null && !this.links.isEmpty()) {
            throw new NotAllowException(
                    "Not allowed to update source/target label " +
                    "for edge label '%s', it must be null", this.name);
        }
        if (this.frequency != Frequency.DEFAULT) {
            throw new NotAllowException(
                    "Not allowed to update frequency " +
                    "for edge label '%s'", this.name);
        }
        if (!this.sortKeys.isEmpty()) {
            throw new NotAllowException(
                    "Not allowed to update sort keys " +
                    "for edge label '%s'", this.name);
        }
        if (this.enableLabelIndex != null) {
            throw new NotAllowException(
                    "Not allowed to update enable_label_index " +
                    "for edge label '%s'", this.name);
        }
    }

    /**
     * Update TTL in two cases:
     * 1) ttl > 0L: set or change a positive TTL
     * 2) ttl == 0L and existing ttl > 0L: explicitly clear an existing TTL
     * This allows removing TTL from a label that previously had TTL configured.
     * Note: ttl == null means not set, so we skip the update.
     */
    private void updateTTL(EdgeLabel edgeLabel) {
        if (this.ttl == null) {
            return;
        }
        if (this.ttl > 0L) {
            edgeLabel.ttl(this.ttl);
            if (this.ttlStartTime != null) {
                edgeLabel.ttlStartTime(this.graph().propertyKey(this.ttlStartTime).id());
            }
        } else if (this.ttl == 0L && edgeLabel.ttl() > 0L) {
            // Clear TTL and ttlStartTime
            edgeLabel.ttl(0L);
            edgeLabel.ttlStartTime(IdGenerator.ZERO);
        }
    }

    private void checkTTL() {
        if (this.ttl == null) {
            E.checkArgument(this.ttlStartTime == null,
                            "Can't set ttl start time if ttl is not set");
            return;
        }
        E.checkArgument(this.ttl >= 0,
                        "The ttl must be >= 0, but got: %s", this.ttl);
        if (this.ttl == 0L) {
            E.checkArgument(this.ttlStartTime == null,
                            "Can't set ttl start time if ttl is not set");
            return;
        }
        for (Pair<String, String> link : this.links) {
            VertexLabel source = this.graph().vertexLabel(link.getLeft());
            VertexLabel target = this.graph().vertexLabel(link.getRight());
            E.checkArgument((source.ttl() == 0L || this.ttl <= source.ttl()) &&
                            (target.ttl() == 0L || this.ttl <= target.ttl()),
                            "The ttl(%s) of edge label '%s' should less than " +
                            "ttl(%s) of source label '%s' and ttl(%s) of target " +
                            "label '%s'", this.ttl, this.name,
                            source.ttl(), link.getLeft(),
                            target.ttl(), link.getRight());
        }
        if (this.ttlStartTime == null) {
            return;
        }
        // Check whether the properties contains the specified keys
        E.checkArgument(!this.properties.isEmpty(),
                        "The properties can't be empty when exist " +
                        "ttl start time for edge label '%s'", this.name);
        E.checkArgument(this.properties.contains(this.ttlStartTime),
                        "The ttl start time '%s' must be contained in " +
                        "properties '%s' for edge label '%s'",
                        this.ttlStartTime, this.name, this.properties);
        PropertyKey pkey = this.graph().propertyKey(this.ttlStartTime);
        E.checkArgument(pkey.dataType().isDate(),
                        "The ttl start time property must be date type," +
                        "but got '%s(%s)'", this.ttlStartTime, pkey.dataType());
    }

    private void checkUserdata(Action action) {
        switch (action) {
            case INSERT:
            case APPEND:
                for (Map.Entry<String, Object> e : this.userdata.entrySet()) {
                    if (e.getValue() == null) {
                        throw new NotAllowException(
                                "Not allowed pass null userdata value when " +
                                "create or append edge label");
                    }
                }
                break;
            case ELIMINATE:
            case DELETE:
                // pass
                break;
            default:
                throw new AssertionError(String.format(
                        "Unknown schema action '%s'", action));
        }
    }

    private static Set<String> mapPkId2Name(HugeGraph graph, Set<Id> ids) {
        return new HashSet<>(graph.mapPkId2Name(ids));
    }

    private static List<String> mapPkId2Name(HugeGraph graph, List<Id> ids) {
        return graph.mapPkId2Name(ids);
    }

    private static String mapElId2Name(HugeGraph graph, Id fatherId) {
        return graph.mapElId2Name(ImmutableList.of(fatherId)).get(0);
    }

    private static Set<Pair<String, String>> mapPairId2Name(HugeGraph graph,
                                                            Set<Pair<Id, Id>> pairs) {
        return graph.mapPairId2Name(pairs);
    }
}
