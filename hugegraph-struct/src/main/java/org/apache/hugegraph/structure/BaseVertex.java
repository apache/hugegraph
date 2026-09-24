/*
 * Copyright 2017 HugeGraph Authors
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hugegraph.structure;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.id.SplicingIdGenerator;
import org.apache.hugegraph.perf.PerfUtil;
import org.apache.hugegraph.struct.schema.SchemaLabel;
import org.apache.hugegraph.struct.schema.VertexLabel;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.type.define.CollectionType;
import org.apache.hugegraph.type.define.HugeKeys;
import org.apache.hugegraph.type.define.IdStrategy;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.collection.CollectionFactory;

import com.google.common.collect.ImmutableList;

public class BaseVertex extends BaseElement implements Cloneable {
    private static final List<BaseEdge> EMPTY_LIST = ImmutableList.of();


    protected Collection<BaseEdge> edges;

    public BaseVertex(Id id) {
        this.edges = EMPTY_LIST;
        id(id);
    }

    public BaseVertex(Id id, SchemaLabel label) {
        // Note:
        // If vertex is OLAP Vertex, id is the id of the vertex that the olap property belongs to, not including the olap property id.
        this(id);
        this.schemaLabel(label);
    }

    @Override
    public String name() {
        E.checkState(this.schemaLabel().idStrategy() == IdStrategy.PRIMARY_KEY,
                "Only primary key vertex has name, " +
                        "but got '%s' with id strategy '%s'",
                this, this.schemaLabel().idStrategy());
        String name;
        if (this.id() != null) {
            String[] parts = SplicingIdGenerator.parse(this.id());
            E.checkState(parts.length == 2,
                    "Invalid primary key vertex id '%s'", this.id());
            name = parts[1];
        } else {
            assert this.id() == null;
            List<Object> propValues = this.primaryValues();
            E.checkState(!propValues.isEmpty(),
                    "Primary values must not be empty " +
                            "(has properties %s)", hasProperties());
            name = SplicingIdGenerator.concatValues(propValues);
            E.checkArgument(!name.isEmpty(),
                    "The value of primary key can't be empty");
        }
        return name;
    }

    @PerfUtil.Watched(prefix = "vertex")
    public List<Object> primaryValues() {
        E.checkArgument(this.schemaLabel().idStrategy() == IdStrategy.PRIMARY_KEY,
                "The id strategy '%s' don't have primary keys",
                this.schemaLabel().idStrategy());
        List<Id> primaryKeys = this.schemaLabel().primaryKeys();
        E.checkArgument(!primaryKeys.isEmpty(),
                "Primary key can't be empty for id strategy '%s'",
                IdStrategy.PRIMARY_KEY);

        List<Object> propValues = new ArrayList<>(primaryKeys.size());
        for (Id pk : primaryKeys) {
            BaseProperty<?> property = this.getProperty(pk);
            E.checkState(property != null,
                    "The value of primary key '%s' can't be null"
                    /*this.graph().propertyKey(pk).name() complete log*/);
            propValues.add(property.serialValue(true));
        }
        return propValues;
    }

    public void addEdge(BaseEdge edge) {
        if (this.edges == EMPTY_LIST) {
            this.edges = CollectionFactory.newList(CollectionType.EC);
        }
        this.edges.add(edge);
    }

    public void correctVertexLabel(VertexLabel correctLabel) {
        E.checkArgumentNotNull(correctLabel, "Vertex label can't be null");
        if (this.schemaLabel() != null && !this.schemaLabel().undefined() &&
            !correctLabel.undefined() && !this.schemaLabel().generalVl() && !correctLabel.generalVl()) {
            E.checkArgument(this.schemaLabel().equals(correctLabel),
                            "[%s]'s Vertex label can't be changed from '%s' " +
                            "to '%s'", this.id(), this.schemaLabel(),
                                                 correctLabel);
        }
        this.schemaLabel(correctLabel);
    }
    public Collection<BaseEdge> edges() {
        return this.edges;
    }

    public void edges(Collection<BaseEdge> edges) {
        this.edges = edges;
    }

    @Override
    public Object sysprop(HugeKeys key) {
        switch (key) {
            case ID:
                return this.id();
            case LABEL:
                return this.schemaLabel().id();
            case PRIMARY_VALUES:
                return this.name();
            case PROPERTIES:
                return this.getPropertiesMap();
            default:
                E.checkArgument(false,
                        "Invalid system property '%s' of Vertex", key);
                return null;
        }
    }

    public VertexLabel schemaLabel() {
        return (VertexLabel)super.schemaLabel();
    }

    public boolean olap() {
        return VertexLabel.OLAP_VL.equals(this.schemaLabel());
    }

    public HugeType type() {
        // For Vertex type, when label is task, return TASK type, convenient for getting storage table information based on type
        /* Magic: ~task ~taskresult ~variables*/
        if (schemaLabel() != null &&
            (schemaLabel().name().equals("~task") ||
             schemaLabel().name().equals("~taskresult") ||
             schemaLabel().name().equals("~variables"))) {
            return HugeType.TASK;
        }
        return HugeType.VERTEX;
    }
}
