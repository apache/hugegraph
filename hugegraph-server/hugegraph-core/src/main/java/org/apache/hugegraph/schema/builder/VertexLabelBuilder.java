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
import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.id.IdGenerator;
import org.apache.hugegraph.backend.tx.ISchemaTransaction;
import org.apache.hugegraph.exception.ExistedException;
import org.apache.hugegraph.exception.NotAllowException;
import org.apache.hugegraph.exception.NotFoundException;
import org.apache.hugegraph.schema.PropertyKey;
import org.apache.hugegraph.schema.Userdata;
import org.apache.hugegraph.schema.VertexLabel;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.type.define.Action;
import org.apache.hugegraph.type.define.IdStrategy;
import org.apache.hugegraph.util.CollectionUtil;
import org.apache.hugegraph.util.E;

import com.google.common.collect.ImmutableList;

public class VertexLabelBuilder extends AbstractBuilder implements VertexLabel.Builder {

    private Id id;
    private final String name;
    private IdStrategy idStrategy;
    private final Set<String> properties;
    private final List<String> primaryKeys;
    private final Set<String> nullableKeys;
    private Long ttl;
    private String ttlStartTime;
    private Boolean enableLabelIndex;
    private final Userdata userdata;
    private boolean checkExist;

    public VertexLabelBuilder(ISchemaTransaction transaction,
                              HugeGraph graph, String name) {
        super(transaction, graph);
        E.checkNotNull(name, "name");
        this.id = null;
        this.name = name;
        this.idStrategy = IdStrategy.DEFAULT;
        this.properties = new HashSet<>();
        this.primaryKeys = new ArrayList<>();
        this.nullableKeys = new HashSet<>();
        this.ttl = null;
        this.ttlStartTime = null;
        this.enableLabelIndex = null;
        this.userdata = new Userdata();
        this.checkExist = true;
    }

    public VertexLabelBuilder(ISchemaTransaction transaction,
                              HugeGraph graph, VertexLabel copy) {
        super(transaction, graph);
        E.checkNotNull(copy, "copy");
        HugeGraph origin = copy.graph();
        this.id = null;
        this.name = copy.name();
        this.idStrategy = copy.idStrategy();
        this.properties = mapPkId2Name(origin, copy.properties());
        this.primaryKeys = mapPkId2Name(origin, copy.primaryKeys());
        this.nullableKeys = mapPkId2Name(origin, copy.nullableKeys());
        this.ttl = copy.ttl();
        this.ttlStartTime = copy.ttlStartTimeName();
        this.enableLabelIndex = copy.enableLabelIndex();
        this.userdata = new Userdata(copy.userdata());
        this.checkExist = false;
    }

    @Override
    public VertexLabel build() {
        Id id = this.validOrGenerateId(HugeType.VERTEX_LABEL,
                                       this.id, this.name);
        VertexLabel vertexLabel = new VertexLabel(this.graph(), id, this.name);
        vertexLabel.idStrategy(this.idStrategy);
        vertexLabel.enableLabelIndex(this.enableLabelIndex == null || this.enableLabelIndex);
        this.updateTTL(vertexLabel);
        // Assign properties
        for (String key : this.properties) {
            PropertyKey propertyKey = this.graph().propertyKey(key);
            vertexLabel.property(propertyKey.id());
        }
        for (String key : this.primaryKeys) {
            PropertyKey propertyKey = this.graph().propertyKey(key);
            vertexLabel.primaryKey(propertyKey.id());
        }
        for (String key : this.nullableKeys) {
            PropertyKey propertyKey = this.graph().propertyKey(key);
            vertexLabel.nullableKey(propertyKey.id());
        }
        vertexLabel.userdata(this.userdata);
        return vertexLabel;
    }

    @Override
    public VertexLabel create() {
        HugeType type = HugeType.VERTEX_LABEL;
        this.checkSchemaName(this.name);

        return this.lockCheckAndCreateSchema(type, this.name, name -> {
            VertexLabel vertexLabel = this.vertexLabelOrNull(name);
            if (vertexLabel != null) {
                if (this.checkExist || !hasSameProperties(vertexLabel)) {
                    throw new ExistedException(type, name);
                }
                return vertexLabel;
            }
            this.checkSchemaIdIfRestoringMode(type, this.id);

            this.checkProperties(Action.INSERT);
            this.checkIdStrategy();
            this.checkNullableKeys(Action.INSERT);
            Userdata.check(this.userdata, Action.INSERT);
            this.checkTTL();
            this.checkUserdata(Action.INSERT);

            vertexLabel = this.build();
            assert vertexLabel.name().equals(name);
            this.graph().addVertexLabel(vertexLabel);
            return vertexLabel;
        });
    }

    /**
     * Check whether this has same properties with existedVertexLabel.
     * Only properties, primaryKeys, nullableKeys, enableLabelIndex are checked.
     * The id, idStrategy, checkExist, userdata are not checked.
     *
     * @param existedVertexLabel to be compared with
     * @return true if this has same properties with existedVertexLabel
     */
    private boolean hasSameProperties(VertexLabel existedVertexLabel) {
        HugeGraph graph = this.graph();

        Set<Id> existedProperties = existedVertexLabel.properties();
        if (this.properties.size() != existedProperties.size()) {
            return false;
        }
        for (String propertyName : this.properties) {
            PropertyKey propertyKey = graph.propertyKey(propertyName);
            if (!existedProperties.contains(propertyKey.id())) {
                return false;
            }
        }

        List<Id> existedPrimaryKeys = existedVertexLabel.primaryKeys();
        if (this.primaryKeys.size() != existedPrimaryKeys.size()) {
            return false;
        }
        for (String primaryKeyName : this.primaryKeys) {
            PropertyKey primaryKey = graph.propertyKey(primaryKeyName);
            if (!existedPrimaryKeys.contains(primaryKey.id())) {
                return false;
            }
        }

        Set<Id> existedNullableKeys = existedVertexLabel.nullableKeys();
        if (this.nullableKeys.size() != existedNullableKeys.size()) {
            return false;
        }
        for (String nullableKeyName : this.nullableKeys) {
            PropertyKey nullableKey = graph.propertyKey(nullableKeyName);
            if (!existedNullableKeys.contains(nullableKey.id())) {
                return false;
            }
        }

        // this.enableLabelIndex == null, it means true.
        if (this.enableLabelIndex == null || this.enableLabelIndex) {
            return existedVertexLabel.enableLabelIndex();
        } else { // this.enableLabelIndex is false
            return !existedVertexLabel.enableLabelIndex();
        }
    }

    @Override
    public VertexLabel append() {
        VertexLabel vertexLabel = this.vertexLabelOrNull(this.name);
        if (vertexLabel == null) {
            throw new NotFoundException("Can't update vertex label '%s' " +
                                        "since it doesn't exist", this.name);
        }

        this.checkStableVars();
        this.checkProperties(Action.APPEND);
        this.checkNullableKeys(Action.APPEND);
        Userdata.check(this.userdata, Action.APPEND);

        for (String key : this.properties) {
            PropertyKey propertyKey = this.graph().propertyKey(key);
            vertexLabel.property(propertyKey.id());
        }
        for (String key : this.nullableKeys) {
            PropertyKey propertyKey = this.graph().propertyKey(key);
            vertexLabel.nullableKey(propertyKey.id());
        }
        this.updateTTL(vertexLabel);
        vertexLabel.userdata(this.userdata);
        this.graph().updateVertexLabel(vertexLabel);
        return vertexLabel;
    }

    @Override
    public VertexLabel eliminate() {
        VertexLabel vertexLabel = this.vertexLabelOrNull(this.name);
        if (vertexLabel == null) {
            throw new NotFoundException("Can't update vertex label '%s' " +
                                        "since it doesn't exist", this.name);
        }
        // Only allowed to eliminate user data
        this.checkStableVars();
        this.checkProperties(Action.ELIMINATE);
        this.checkNullableKeys(Action.ELIMINATE);
        Userdata.check(this.userdata, Action.ELIMINATE);

        vertexLabel.removeUserdata(this.userdata);
        this.graph().updateVertexLabel(vertexLabel);
        return vertexLabel;
    }

    @Override
    public Id remove() {
        VertexLabel vertexLabel = this.vertexLabelOrNull(this.name);
        if (vertexLabel == null) {
            return null;
        }
        return this.graph().removeVertexLabel(vertexLabel.id());
    }

    @Override
    public Id rebuildIndex() {
        VertexLabel vertexLabel = this.vertexLabelOrNull(this.name);
        if (vertexLabel == null) {
            return null;
        }
        return this.graph().rebuildIndex(vertexLabel);
    }

    @Override
    public VertexLabelBuilder id(long id) {
        E.checkArgument(id != 0L,
                        "Not allowed to assign 0 as vertex label id");
        this.id = IdGenerator.of(id);
        return this;
    }

    @Override
    public VertexLabelBuilder idStrategy(IdStrategy idStrategy) {
        E.checkArgument(this.idStrategy == IdStrategy.DEFAULT || this.idStrategy == idStrategy,
                        "Not allowed to change id strategy for " +
                        "vertex label '%s'", this.name);
        this.idStrategy = idStrategy;
        return this;
    }

    @Override
    public VertexLabelBuilder useAutomaticId() {
        E.checkArgument(this.idStrategy == IdStrategy.DEFAULT ||
                        this.idStrategy == IdStrategy.AUTOMATIC,
                        "Not allowed to change id strategy for " +
                        "vertex label '%s'", this.name);
        this.idStrategy = IdStrategy.AUTOMATIC;
        return this;
    }

    @Override
    public VertexLabelBuilder usePrimaryKeyId() {
        E.checkArgument(this.idStrategy == IdStrategy.DEFAULT ||
                        this.idStrategy == IdStrategy.PRIMARY_KEY,
                        "Not allowed to change id strategy for " +
                        "vertex label '%s'", this.name);
        this.idStrategy = IdStrategy.PRIMARY_KEY;
        return this;
    }

    @Override
    public VertexLabelBuilder useCustomizeStringId() {
        E.checkArgument(this.idStrategy == IdStrategy.DEFAULT ||
                        this.idStrategy == IdStrategy.CUSTOMIZE_STRING,
                        "Not allowed to change id strategy for " +
                        "vertex label '%s'", this.name);
        this.idStrategy = IdStrategy.CUSTOMIZE_STRING;
        return this;
    }

    @Override
    public VertexLabelBuilder useCustomizeNumberId() {
        E.checkArgument(this.idStrategy == IdStrategy.DEFAULT ||
                        this.idStrategy == IdStrategy.CUSTOMIZE_NUMBER,
                        "Not allowed to change id strategy for " +
                        "vertex label '%s'", this.name);
        this.idStrategy = IdStrategy.CUSTOMIZE_NUMBER;
        return this;
    }

    @Override
    public VertexLabelBuilder useCustomizeUuidId() {
        E.checkArgument(this.idStrategy == IdStrategy.DEFAULT ||
                        this.idStrategy == IdStrategy.CUSTOMIZE_UUID,
                        "Not allowed to change id strategy for " +
                        "vertex label '%s'", this.name);
        this.idStrategy = IdStrategy.CUSTOMIZE_UUID;
        return this;
    }

    @Override
    public VertexLabelBuilder properties(String... properties) {
        this.properties.addAll(Arrays.asList(properties));
        return this;
    }

    @Override
    public VertexLabelBuilder primaryKeys(String... keys) {
        if (keys.length == 0) {
            return this;
        }

        E.checkArgument(this.primaryKeys.isEmpty(),
                        "Not allowed to assign primary keys multitimes");

        List<String> primaryKeys = Arrays.asList(keys);
        E.checkArgument(CollectionUtil.allUnique(primaryKeys),
                        "Invalid primary keys %s, which contains some " +
                        "duplicate properties", primaryKeys);
        this.primaryKeys.addAll(primaryKeys);
        return this;
    }

    @Override
    public VertexLabelBuilder nullableKeys(String... keys) {
        this.nullableKeys.addAll(Arrays.asList(keys));
        return this;
    }

    @Override
    public VertexLabel.Builder ttl(long ttl) {
        this.ttl = ttl;
        return this;
    }

    @Override
    public VertexLabel.Builder ttlStartTime(String ttlStartTime) {
        this.ttlStartTime = ttlStartTime;
        return this;
    }

    @Override
    public VertexLabelBuilder enableLabelIndex(boolean enable) {
        this.enableLabelIndex = enable;
        return this;
    }

    @Override
    public VertexLabelBuilder userdata(String key, Object value) {
        this.userdata.put(key, value);
        return this;
    }

    @Override
    public VertexLabelBuilder userdata(Map<String, Object> userdata) {
        this.userdata.putAll(userdata);
        return this;
    }

    @Override
    public VertexLabelBuilder ifNotExist() {
        this.checkExist = false;
        return this;
    }

    @Override
    public VertexLabelBuilder checkExist(boolean checkExist) {
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
                            "for vertex label currently");
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
                        "Not support to eliminate nullableKeys for vertex label currently");
            }
            return;
        }

        VertexLabel vertexLabel = this.vertexLabelOrNull(this.name);
        // The originProps is empty when firstly create vertex label
        List<String> originProps = vertexLabel == null ? ImmutableList.of() :
                                   this.graph().mapPkId2Name(vertexLabel.properties());
        Set<String> appendProps = this.properties;

        E.checkArgument(CollectionUtil.union(originProps, appendProps)
                                      .containsAll(this.nullableKeys),
                        "The nullableKeys: %s to be created or appended " +
                        "must belong to the origin/new properties: %s/%s",
                        this.nullableKeys, originProps, appendProps);

        List<String> primaryKeys = vertexLabel == null ? this.primaryKeys :
                                   this.graph().mapPkId2Name(vertexLabel.primaryKeys());
        E.checkArgument(!CollectionUtil.hasIntersection(primaryKeys,
                                                        this.nullableKeys),
                        "The nullableKeys: %s are not allowed to " +
                        "belong to primaryKeys: %s of vertex label '%s'",
                        this.nullableKeys, primaryKeys, this.name);

        if (action == Action.APPEND) {
            Collection<String> newAddedProps = CollectionUtils.subtract(appendProps, originProps);
            E.checkArgument(this.nullableKeys.containsAll(newAddedProps),
                            "The new added properties: %s must be nullable", newAddedProps);
        }
    }

    private void checkIdStrategy() {
        IdStrategy strategy = this.idStrategy;
        boolean hasPrimaryKey = !this.primaryKeys.isEmpty();
        switch (strategy) {
            case DEFAULT:
                if (hasPrimaryKey) {
                    this.idStrategy = IdStrategy.PRIMARY_KEY;
                } else {
                    this.idStrategy = IdStrategy.AUTOMATIC;
                }
                break;
            case AUTOMATIC:
            case CUSTOMIZE_STRING:
            case CUSTOMIZE_NUMBER:
            case CUSTOMIZE_UUID:
                E.checkArgument(!hasPrimaryKey,
                                "Not allowed to assign primary keys " +
                                "when using '%s' id strategy", strategy);
                break;
            case PRIMARY_KEY:
                E.checkArgument(hasPrimaryKey,
                                "Must assign some primary keys " +
                                "when using '%s' id strategy", strategy);
                break;
            default:
                throw new AssertionError(String.format("Unknown id strategy '%s'", strategy));
        }
        if (this.idStrategy == IdStrategy.PRIMARY_KEY) {
            this.checkPrimaryKeys();
        }
    }

    private void checkPrimaryKeys() {
        E.checkArgument(this.idStrategy == IdStrategy.DEFAULT ||
                        this.idStrategy == IdStrategy.PRIMARY_KEY,
                        "Not allowed to use id strategy '%s' and assign " +
                        "primary keys at the same time for vertex label '%s'",
                        this.idStrategy, this.name);

        E.checkArgument(!this.properties.isEmpty(),
                        "The properties of vertex label '%s' " +
                        "can't be empty when id strategy is '%s'",
                        this.name, IdStrategy.PRIMARY_KEY);

        E.checkNotEmpty(this.primaryKeys, "primary keys", this.name);
        // Use loop instead containAll for more detailed exception info.
        for (String key : this.primaryKeys) {
            E.checkArgument(this.properties.contains(key),
                            "The primary key '%s' of vertex label '%s' " +
                            "must be contained in properties: %s",
                            key, this.name, this.properties);
        }
    }

    private void checkStableVars() {
        if (!this.primaryKeys.isEmpty()) {
            throw new NotAllowException(
                    "Not allowed to update primary keys " +
                    "for vertex label '%s'", this.name);
        }
        if (this.idStrategy != IdStrategy.DEFAULT) {
            throw new NotAllowException(
                    "Not allowed to update id strategy " +
                    "for vertex label '%s'", this.name);
        }
        if (this.enableLabelIndex != null) {
            throw new NotAllowException(
                    "Not allowed to update enable_label_index " +
                    "for vertex label '%s'", this.name);
        }
    }

    /**
     * Update TTL in two cases:
     * 1) ttl > 0L: set or change a positive TTL
     * 2) ttl == 0L and existing ttl > 0L: explicitly clear an existing TTL
     * This allows removing TTL from a label that previously had TTL configured.
     * Note: ttl == null means not set, so we skip the update.
     */
    private void updateTTL(VertexLabel vertexLabel) {
        if (this.ttl == null) {
            return;
        }
        if (this.ttl > 0L) {
            vertexLabel.ttl(this.ttl);
            if (this.ttlStartTime != null) {
                vertexLabel.ttlStartTime(this.graph().propertyKey(this.ttlStartTime).id());
            }
        } else if (this.ttl == 0L && vertexLabel.ttl() > 0L) {
            // Clear TTL and ttlStartTime
            vertexLabel.ttl(0L);
            vertexLabel.ttlStartTime(IdGenerator.ZERO);
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
        if (this.ttlStartTime == null) {
            return;
        }
        // Check whether the properties contain the specified keys
        E.checkArgument(!this.properties.isEmpty(),
                        "The properties can't be empty when exist " +
                        "ttl start time for edge label '%s'", this.name);
        E.checkArgument(this.properties.contains(this.ttlStartTime),
                        "The ttl start time '%s' must be contained in " +
                        "properties '%s' for vertex label '%s'",
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
                throw new AssertionError(String.format("Unknown schema action '%s'", action));
        }
    }

    private static Set<String> mapPkId2Name(HugeGraph graph, Set<Id> ids) {
        return new HashSet<>(graph.mapPkId2Name(ids));
    }

    private static List<String> mapPkId2Name(HugeGraph graph, List<Id> ids) {
        return graph.mapPkId2Name(ids);
    }
}
