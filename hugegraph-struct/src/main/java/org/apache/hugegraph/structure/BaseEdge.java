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
import java.util.List;

import org.apache.hugegraph.HugeGraphSupplier;
import org.apache.hugegraph.exception.HugeException;
import org.apache.hugegraph.id.EdgeId;
import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.id.SplicingIdGenerator;
import org.apache.hugegraph.serializer.BytesBuffer;
import org.apache.hugegraph.struct.schema.EdgeLabel;
import org.apache.hugegraph.struct.schema.SchemaLabel;
import org.apache.hugegraph.struct.schema.VertexLabel;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.type.define.Directions;
import org.apache.hugegraph.type.define.HugeKeys;
import org.apache.hugegraph.util.E;

import com.google.common.collect.ImmutableList;

/* Only as basic data container, id generation logic relies on upper layer encapsulation*/
public class BaseEdge extends BaseElement implements Cloneable {

    private BaseVertex sourceVertex;
    private BaseVertex targetVertex;
    boolean isOutEdge;

    private String name;

    public BaseEdge(Id id, EdgeLabel label) {
        this.id(id);
        this.schemaLabel(label);
    }

    public BaseEdge(SchemaLabel label, boolean isOutEdge) {
        this.schemaLabel(label);
        this.isOutEdge = isOutEdge;
    }


    public boolean isOutEdge() {
        return isOutEdge;
    }

    public void isOutEdge(boolean isOutEdge) {
        this.isOutEdge = isOutEdge;
    }

    public EdgeId idWithDirection() {
        return ((EdgeId) this.id()).directed(true);
    }

    @Override
    public String name() {
        if (this.name == null) {
            this.name = SplicingIdGenerator.concatValues(sortValues());
        }
        return this.name;
    }

    public void name(String name) {
        this.name = name;
    }

    @Override
    public HugeType type() {
        // NOTE: we optimize the edge type that let it include direction
        return this.isOutEdge() ? HugeType.EDGE_OUT : HugeType.EDGE_IN;
    }

    public List<Object> sortValues() {
        List<Id> sortKeys = this.schemaLabel().sortKeys();
        if (sortKeys.isEmpty()) {
            return ImmutableList.of();
        }
        List<Object> propValues = new ArrayList<>(sortKeys.size());
        for (Id sk : sortKeys) {
            BaseProperty<?> property = this.getProperty(sk);
            E.checkState(property != null,
                    "The value of sort key '%s' can't be null", sk);
            propValues.add(property.propertyKey().serialValue(property.value(), true));
        }
        return propValues;
    }

    public Directions direction() {
        return this.isOutEdge ? Directions.OUT : Directions.IN;
    }

    public Id sourceVertexId() {
        return this.sourceVertex.id();
    }

    public Id targetVertexId() {
        return this.targetVertex.id();
    }

    public void sourceVertex(BaseVertex sourceVertex) {
        this.sourceVertex = sourceVertex;
    }

    public BaseVertex sourceVertex() {
        return this.sourceVertex;
    }

    public void targetVertex(BaseVertex targetVertex) {
        this.targetVertex = targetVertex;
    }

    public BaseVertex targetVertex() {
        return this.targetVertex;
    }

    public Id ownerVertexId() {
        return this.isOutEdge() ? this.sourceVertexId() : this.targetVertexId();
    }

    public Id otherVertexId() {
        return this.isOutEdge() ? this.targetVertexId() : this.sourceVertexId() ;
    }

    public void vertices(boolean outEdge, BaseVertex owner, BaseVertex other) {
        this.isOutEdge = outEdge ;
        if (outEdge) {
            this.sourceVertex(owner);
            this.targetVertex(other);
        } else {
            this.sourceVertex(other);
            this.targetVertex(owner);
        }
    }



    public EdgeLabel schemaLabel() {
        return (EdgeLabel) super.schemaLabel();
    }

    public BaseVertex ownerVertex() {
        return this.isOutEdge() ? this.sourceVertex() : this.targetVertex();
    }

    public BaseVertex otherVertex() {
        return this.isOutEdge() ? this.targetVertex() : this.sourceVertex();
    }

    public void assignId() {
        // Generate an id and assign
        if (this.schemaLabel().hasFather()) {
            this.id(new EdgeId(this.ownerVertex().id(), this.direction(),
                    this.schemaLabel().fatherId(),
                    this.schemaLabel().id(),
                    this.name(),
                    this.otherVertex().id()));
        } else {
            this.id(new EdgeId(this.ownerVertex().id(), this.direction(),
                    this.schemaLabel().id(),
                    this.schemaLabel().id(),
                    this.name(), this.otherVertex().id()));
        }


        if (this.fresh()) {
            int len = this.id().length();
            E.checkArgument(len <= BytesBuffer.BIG_ID_LEN_MAX,
                    "The max length of edge id is %s, but got %s {%s}",
                    BytesBuffer.BIG_ID_LEN_MAX, len, this.id());
        }
    }
    @Override
    public Object sysprop(HugeKeys key) {
        switch (key) {
            case ID:
                return this.id();
            case OWNER_VERTEX:
                return this.ownerVertexId();
            case LABEL:
                if (this.schemaLabel().fatherId() != null) {
                    return this.schemaLabel().fatherId();
                } else {
                    return this.schemaLabel().id();
                }
            case DIRECTION:
                return this.direction();

            case SUB_LABEL:
                return this.schemaLabel().id();

            case OTHER_VERTEX:
                return this.otherVertexId();
            case SORT_VALUES:
                return this.name();
            case PROPERTIES:
                return this.getPropertiesMap();
            default:
                E.checkArgument(false,
                        "Invalid system property '%s' of Edge", key);
                return null;
        }

    }

    @Override
    public BaseEdge clone() {
        try {
            return (BaseEdge) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new HugeException("Failed to clone HugeEdge", e);
        }
    }

    public BaseEdge switchOwner() {
        BaseEdge edge = this.clone();
        edge.isOutEdge(!edge.isOutEdge());
        if (edge.id() != null) {
            edge.id(((EdgeId) edge.id()).switchDirection());
        }
        return edge;
    }

    public static BaseEdge constructEdge(HugeGraphSupplier graph,
                                         BaseVertex ownerVertex,
                                         boolean isOutEdge,
                                         EdgeLabel edgeLabel,
                                         String sortValues,
                                         Id otherVertexId) {
        Id ownerLabelId = edgeLabel.sourceLabel();
        Id otherLabelId = edgeLabel.targetLabel();
        VertexLabel srcLabel;
        VertexLabel tgtLabel;
        if (graph == null) {
            srcLabel = new VertexLabel(null, ownerLabelId, "UNDEF");
            tgtLabel = new VertexLabel(null, otherLabelId, "UNDEF");
        } else {
            if (edgeLabel.general()) {
                srcLabel = VertexLabel.GENERAL;
                tgtLabel = VertexLabel.GENERAL;
            } else {
                srcLabel = graph.vertexLabelOrNone(ownerLabelId);
                tgtLabel = graph.vertexLabelOrNone(otherLabelId);
            }
        }

        VertexLabel otherVertexLabel;
        if (isOutEdge) {
            ownerVertex.correctVertexLabel(srcLabel);
            otherVertexLabel = tgtLabel;
        } else {
            ownerVertex.correctVertexLabel(tgtLabel);
            otherVertexLabel = srcLabel;
        }
        BaseVertex otherVertex = new BaseVertex(otherVertexId, otherVertexLabel);

        ownerVertex.propLoaded(false);
        otherVertex.propLoaded(false);

        BaseEdge edge = new BaseEdge(edgeLabel, isOutEdge);
        edge.name(sortValues);
        edge.vertices(isOutEdge, ownerVertex, otherVertex);
        edge.assignId();

        ownerVertex.addEdge(edge);
        otherVertex.addEdge(edge.switchOwner());

        return edge;
    }

}
