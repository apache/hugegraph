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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.hugegraph.HugeGraphSupplier;
import org.apache.hugegraph.exception.HugeException;
import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.id.IdGenerator;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.type.Indexfiable;
import org.apache.hugegraph.type.Propfiable;
import org.apache.hugegraph.util.E;

import com.google.common.base.Objects;

public abstract class SchemaLabel extends SchemaElement
        implements Indexfiable, Propfiable {

    private final Set<Id> properties;
    private final Set<Id> nullableKeys;
    private final Set<Id> indexLabels;
    private boolean enableLabelIndex;
    private long ttl;
    private Id ttlStartTime;

    public SchemaLabel(final HugeGraphSupplier graph, Id id, String name) {
        super(graph, id, name);
        this.properties = new HashSet<>();
        this.nullableKeys = new HashSet<>();
        this.indexLabels = new HashSet<>();
        this.enableLabelIndex = true;
        this.ttl = 0L;
        this.ttlStartTime = SchemaElement.NONE_ID;
    }

    @Override
    public Set<Id> properties() {
        return Collections.unmodifiableSet(this.properties);
    }

    public Set<Id> extendProperties() {
        return this.properties();
    }

    public void properties(Set<Id> properties) {
        this.properties.addAll(properties);
    }

    public SchemaLabel properties(Id... ids) {
        this.properties.addAll(Arrays.asList(ids));
        return this;
    }

    public void property(Id id) {
        this.properties.add(id);
    }

    public Set<Id> nullableKeys() {
        return Collections.unmodifiableSet(this.nullableKeys);
    }

    public void nullableKey(Id id) {
        this.nullableKeys.add(id);
    }

    public void nullableKeys(Id... ids) {
        this.nullableKeys.addAll(Arrays.asList(ids));
    }

    public void nullableKeys(Set<Id> nullableKeys) {
        this.nullableKeys.addAll(nullableKeys);
    }

    @Override
    public Set<Id> indexLabels() {
        return Collections.unmodifiableSet(this.indexLabels);
    }

    public Set<Id> extendIndexLabels() {
        return this.indexLabels();
    }

    public void indexLabel(Id id) {
        this.indexLabels.add(id);
    }

    public void indexLabels(Id... ids) {
        this.indexLabels.addAll(Arrays.asList(ids));
    }

    public void addIndexLabel(Id id) {
        this.indexLabels.add(id);
    }

    public void addIndexLabels(Id... ids) {
        this.indexLabels.addAll(Arrays.asList(ids));
    }

    public boolean existsIndexLabel() {
        return !this.indexLabels().isEmpty();
    }

    public void removeIndexLabel(Id id) {
        this.indexLabels.remove(id);
    }

    public boolean enableLabelIndex() {
        return this.enableLabelIndex;
    }

    public void enableLabelIndex(boolean enable) {
        this.enableLabelIndex = enable;
    }

    public boolean undefined() {
        return this.name() == UNDEF;
    }

    public void ttl(long ttl) {
        assert ttl >= 0L;
        this.ttl = ttl;
    }

    public long ttl() {
        assert this.ttl >= 0L;
        return this.ttl;
    }

    public void ttlStartTime(Id id) {
        this.ttlStartTime = id;
    }

    public Id ttlStartTime() {
        return this.ttlStartTime;
    }

    public String ttlStartTimeName() {
        return NONE_ID.equals(this.ttlStartTime) ? null :
               this.graph.propertyKey(this.ttlStartTime).name();
    }

    public boolean hasSameContent(SchemaLabel other) {
        return super.hasSameContent(other) && this.ttl == other.ttl &&
               this.enableLabelIndex == other.enableLabelIndex &&
               Objects.equal(this.graph.mapPkId2Name(this.properties),
                             other.graph.mapPkId2Name(other.properties)) &&
               Objects.equal(this.graph.mapPkId2Name(this.nullableKeys),
                             other.graph.mapPkId2Name(other.nullableKeys)) &&
               Objects.equal(this.graph.mapIlId2Name(this.indexLabels),
                             other.graph.mapIlId2Name(other.indexLabels)) &&
               Objects.equal(this.ttlStartTimeName(), other.ttlStartTimeName());
    }

    public static Id getLabelId(HugeGraphSupplier graph, HugeType type, Object label) {
        E.checkNotNull(graph, "graph");
        E.checkNotNull(type, "type");
        E.checkNotNull(label, "label");
        if (label instanceof Number) {
            return IdGenerator.of(((Number) label).longValue());
        } else if (label instanceof String) {
            if (type.isVertex()) {
                return graph.vertexLabel((String) label).id();
            } else if (type.isEdge()) {
                return graph.edgeLabel((String) label).id();
            } else {
                throw new HugeException(
                        "Not support query from '%s' with label '%s'",
                        type, label);
            }
        } else {
            throw new HugeException(
                    "The label type must be number or string, but got '%s'",
                    label.getClass());
        }
    }

    public static Id getVertexLabelId(HugeGraphSupplier graph, Object label) {
        return SchemaLabel.getLabelId(graph, HugeType.VERTEX, label);
    }

    public static Id getEdgeLabelId(HugeGraphSupplier graph, Object label) {
        return SchemaLabel.getLabelId(graph, HugeType.EDGE, label);
    }
}
