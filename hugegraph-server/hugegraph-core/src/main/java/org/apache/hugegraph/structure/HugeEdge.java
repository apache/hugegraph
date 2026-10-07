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

package org.apache.hugegraph.structure;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hugegraph.exception.HugeException;
import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.id.EdgeId;
import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.backend.query.QueryResults;
import org.apache.hugegraph.backend.tx.GraphTransaction;
import org.apache.hugegraph.perf.PerfUtil.Watched;
import org.apache.hugegraph.struct.schema.EdgeLabel;
import org.apache.hugegraph.struct.schema.PropertyKey;
import org.apache.hugegraph.struct.schema.VertexLabel;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.type.define.Cardinality;
import org.apache.hugegraph.type.define.Directions;
import org.apache.hugegraph.type.define.HugeKeys;
import org.apache.hugegraph.util.E;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyProperty;


public class HugeEdge extends HugeElement implements Edge, Cloneable {

    private BaseEdge element;

    private HugeVertex sourceVertex;
    private HugeVertex targetVertex;


    public HugeEdge(HugeVertex owner, Id id, EdgeLabel label,
                    HugeVertex other) {
        this(owner.graph(), id, label);
        this.fresh(true);
        this.vertices(owner, other);
    }

    public HugeEdge(final HugeGraph graph, Id id, EdgeLabel label) {
        super(graph);

        E.checkArgumentNotNull(label, "Edge label can't be null");
        this.element = new BaseEdge(id, label);
        this.element.isOutEdge(true);
        this.sourceVertex = null;
        this.targetVertex = null;
    }

    /** Adopt decoded shared state and attach engine endpoint wrappers. */
    public HugeEdge(final HugeGraph graph, BaseEdge element) {
        this(graph, element, null);
    }

    public HugeEdge(final HugeGraph graph, BaseEdge element, HugeVertex owner) {
        super(graph);
        E.checkArgumentNotNull(element, "Edge element can't be null");
        E.checkArgumentNotNull(element.schemaLabel(), "Edge label can't be null");
        this.element = element;
        BaseVertex source = element.sourceVertex();
        BaseVertex target = element.targetVertex();
        this.sourceVertex = this.wrapVertex(graph, source, owner);
        this.targetVertex = target == source ? this.sourceVertex : this.wrapVertex(graph, target, owner);
    }

    private HugeVertex wrapVertex(HugeGraph graph, BaseVertex vertex, HugeVertex owner) {
        if (vertex == null) {
            return null;
        }
        return owner != null && owner.element() == vertex ? owner : new HugeVertex(graph, vertex);
    }

    @Override
    public BaseEdge element() {
        return this.element;
    }

    @Override
    public HugeType type() {
        // NOTE: we optimize the edge type that let it include direction
        return this.element.isOutEdge() ? HugeType.EDGE_OUT : HugeType.EDGE_IN;
    }

    @Override
    public EdgeId id() {
        return (EdgeId) this.element.id();
    }

    @Override
    public EdgeLabel schemaLabel() {
        assert this.graph().sameAs(this.element.schemaLabel().graph());
        return this.element.schemaLabel();
    }

    @Override
    public String name() {
        return this.element.name();
    }

    public void name(String name) {
        this.element.name(name);
    }

    @Override
    public String label() {
        return this.element.schemaLabel().name();
    }

    public boolean selfLoop() {
        return this.sourceVertex != null &&
               this.sourceVertex == this.targetVertex;
    }

    public Directions direction() {
        return this.element.isOutEdge() ? Directions.OUT : Directions.IN;
    }

    public boolean matchDirection(Directions direction) {
        if (direction == Directions.BOTH || this.selfLoop()) {
            return true;
        }
        return this.isDirection(direction);
    }

    public boolean isDirection(Directions direction) {
        return this.element.isOutEdge() && direction == Directions.OUT ||
               !this.element.isOutEdge() && direction == Directions.IN;
    }

    @Watched(prefix = "edge")
    public void assignId() {
        this.element.assignId();
    }

    @Watched(prefix = "edge")
    public EdgeId idWithDirection() {
        return this.element.idWithDirection();
    }

    @Watched(prefix = "edge")
    protected List<Object> sortValues() {
        return this.element.sortValues();
    }

    @Override
    public void remove() {
        this.removed(true);
        this.sourceVertex.removeEdge(this);
        this.targetVertex.removeEdge(this);

        GraphTransaction tx = this.tx();
        if (tx != null) {
            assert this.fresh();
            tx.removeEdge(this);
        } else {
            this.graph().removeEdge(this);
        }
    }

    @Override
    public <V> Property<V> property(String key, V value) {
        PropertyKey propertyKey = this.graph().propertyKey(key);
        // Check key in edge label
        E.checkArgument(this.element.schemaLabel().properties().contains(propertyKey.id()),
                        "Invalid property '%s' for edge label '%s'",
                        key, this.label());
        if (value == null) {
            this.removeProperty(propertyKey.id());
            return EmptyProperty.instance();
        }

        // Sort-Keys can only be set once
        if (this.schemaLabel().sortKeys().contains(propertyKey.id())) {
            E.checkArgument(!this.hasProperty(propertyKey.id()),
                            "Can't update sort key: '%s'", key);
        }
        return this.addProperty(propertyKey, value, !this.fresh());
    }

    @Override
    protected GraphTransaction tx() {
        if (this.ownerVertex() == null || !this.fresh()) {
            return null;
        }
        return this.ownerVertex().tx();
    }

    @Watched(prefix = "edge")
    @Override
    protected <V> HugeEdgeProperty<V> newProperty(PropertyKey pkey, V val) {
        return new HugeEdgeProperty<>(this, pkey, val);
    }

    @Override
    protected <V> HugeEdgeProperty<V> wrapProperty(BaseProperty<V> property) {
        return new HugeEdgeProperty<>(this, property);
    }

    @Watched(prefix = "edge")
    @Override
    protected <V> void onUpdateProperty(Cardinality cardinality,
                                        HugeProperty<V> prop) {
        if (prop != null) {
            assert prop instanceof HugeEdgeProperty;
            HugeEdgeProperty<V> edgeProp = (HugeEdgeProperty<V>) prop;
            GraphTransaction tx = this.tx();
            if (tx != null) {
                assert this.fresh();
                tx.addEdgeProperty(edgeProp);
            } else {
                this.graph().addEdgeProperty(edgeProp);
            }
        }
    }

    @Watched(prefix = "edge")
    @Override
    protected boolean ensureFilledProperties(boolean throwIfNotExist) {
        if (this.isPropLoaded()) {
            this.updateToDefaultValueIfNone();
            return true;
        }

        // Skip query if there is no any property key in schema
        if (this.schemaLabel().properties().isEmpty()) {
            this.propLoaded();
            return true;
        }

        // Seems there is no scene to be here
        Iterator<Edge> edges = this.graph().edges(this.id());
        Edge edge = QueryResults.one(edges);
        if (edge == null && !throwIfNotExist) {
            return false;
        }
        E.checkState(edge != null, "Edge '%s' does not exist", this.element.id());
        this.copyProperties((HugeEdge) edge);
        this.updateToDefaultValueIfNone();
        return true;
    }

    @Watched(prefix = "edge")
    @SuppressWarnings("unchecked") // (Property<V>) prop
    @Override
    public <V> Iterator<Property<V>> properties(String... keys) {
        this.ensureFilledProperties(true);

        // Capacity should be about the following size
        int propsCapacity = keys.length == 0 ?
                            this.sizeOfProperties() :
                            keys.length;
        List<Property<V>> props = new ArrayList<>(propsCapacity);

        if (keys.length == 0) {
            for (HugeProperty<?> prop : this.getProperties()) {
                assert prop != null;
                props.add((Property<V>) prop);
            }
        } else {
            for (String key : keys) {
                Id pkeyId;
                try {
                    pkeyId = this.graph().propertyKey(key).id();
                } catch (IllegalArgumentException ignored) {
                    continue;
                }
                HugeProperty<?> prop = this.getProperty(pkeyId);
                if (prop == null) {
                    // Not found
                    continue;
                }
                props.add((Property<V>) prop);
            }
        }
        return props.iterator();
    }

    @Override
    public Object sysprop(HugeKeys key) {
        return this.element.sysprop(key);
    }

    @Override
    public Iterator<Vertex> vertices(Direction direction) {
        List<Vertex> vertices = new ArrayList<>(2);
        switch (direction) {
            case OUT:
                vertices.add(this.sourceVertex());
                break;
            case IN:
                vertices.add(this.targetVertex());
                break;
            case BOTH:
                vertices.add(this.sourceVertex());
                vertices.add(this.targetVertex());
                break;
            default:
                throw new AssertionError("Unsupported direction: " + direction);
        }

        return vertices.iterator();
    }

    @Override
    public Vertex outVertex() {
        return this.sourceVertex();
    }

    @Override
    public Vertex inVertex() {
        return this.targetVertex();
    }

    public void vertices(HugeVertex owner, HugeVertex other) {
        Id ownerLabel = owner.schemaLabel().id();
        Id otherLabel = other.schemaLabel().id();
        for (Pair<Id, Id> link : this.element.schemaLabel().links()) {
            if (ownerLabel.equals(link.getLeft()) &&
                otherLabel.equals(link.getRight())) {
                this.vertices(true, owner, other);
            } else if (ownerLabel.equals(link.getRight()) &&
                       otherLabel.equals(link.getLeft())) {
                this.vertices(false, owner, other);
            }
        }
    }

    public void vertices(boolean outEdge, HugeVertex owner, HugeVertex other) {
        this.element.vertices(outEdge, owner.element(), other.element());
        if (this.element.isOutEdge()) {
            this.sourceVertex = owner;
            this.targetVertex = other;
        } else {
            this.sourceVertex = other;
            this.targetVertex = owner;
        }
    }

    @Watched
    public HugeEdge switchOwner() {
        return this.cloneWithElement(this.element.switchOwner());
    }

    public HugeEdge switchToOutDirection() {
        if (this.direction() == Directions.IN) {
            return this.switchOwner();
        }
        return this;
    }

    public HugeVertex ownerVertex() {
        return this.element.isOutEdge() ? this.sourceVertex() : this.targetVertex();
    }

    public HugeVertex sourceVertex() {
        this.checkAdjacentVertexExist(this.sourceVertex);
        return this.sourceVertex;
    }

    public void sourceVertex(HugeVertex sourceVertex) {
        this.sourceVertex = sourceVertex;
        this.element.sourceVertex(sourceVertex.element());
    }

    public HugeVertex targetVertex() {
        this.checkAdjacentVertexExist(this.targetVertex);
        return this.targetVertex;
    }

    public void targetVertex(HugeVertex targetVertex) {
        this.targetVertex = targetVertex;
        this.element.targetVertex(targetVertex.element());
    }

    private void checkAdjacentVertexExist(HugeVertex vertex) {
        if (vertex.schemaLabel().undefined() &&
            this.graph().checkAdjacentVertexExist()) {
            throw new HugeException("Vertex '%s' does not exist", vertex.id());
        }
    }

    public boolean belongToLabels(String... edgeLabels) {
        if (edgeLabels.length == 0) {
            return true;
        }

        // Does edgeLabels contain me
        for (String label : edgeLabels) {
            if (label.equals(this.label())) {
                return true;
            }
        }
        return false;
    }

    public boolean belongToVertex(HugeVertex vertex) {
        return vertex != null && (vertex.equals(this.sourceVertex) ||
                                  vertex.equals(this.targetVertex));
    }

    public HugeVertex otherVertex(HugeVertex vertex) {
        if (vertex == this.sourceVertex()) {
            return this.targetVertex();
        } else {
            E.checkArgument(vertex == this.targetVertex(),
                            "Invalid argument vertex '%s', must be in [%s, %s]",
                            vertex, this.sourceVertex(), this.targetVertex());
            return this.sourceVertex();
        }
    }

    public HugeVertex otherVertex() {
        return this.element.isOutEdge() ? this.targetVertex() : this.sourceVertex();
    }

    /**
     * Clear properties of the edge, and set `removed` true
     *
     * @return a new edge
     */
    public HugeEdge prepareRemoved() {
        HugeEdge edge = this.clone();
        edge.removed(true);
        edge.resetProperties();
        return edge;
    }

    @Override
    public HugeEdge copy() {
        HugeEdge edge = this.clone();
        edge.copyProperties(this);
        return edge;
    }

    @Override
    protected HugeEdge clone() {
        return this.cloneWithElement(this.element.clone());
    }

    private HugeEdge cloneWithElement(BaseEdge element) {
        try {
            HugeEdge edge = (HugeEdge) super.clone();
            edge.element = element;
            return edge;
        } catch (CloneNotSupportedException e) {
            throw new HugeException("Failed to clone HugeEdge", e);
        }
    }

    @Override
    public String toString() {
        return StringFactory.edgeString(this);
    }

    public static final EdgeId getIdValue(Object idValue,
                                          boolean returnNullIfError) {
        Id id = getIdValue(idValue);
        if (id == null || id instanceof EdgeId) {
            return (EdgeId) id;
        }
        return EdgeId.parse(id.asString(), returnNullIfError);
    }

    @Watched
    public static HugeEdge constructEdge(HugeVertex ownerVertex,
                                         boolean isOutEdge,
                                         EdgeLabel edgeLabel,
                                         String sortValues,
                                         Id otherVertexId) {
        BaseEdge element = BaseEdge.createEdge(ownerVertex.graph(), ownerVertex.element(), isOutEdge,
                                               edgeLabel, sortValues, otherVertexId);
        return attachEdge(ownerVertex, element);
    }

    private static HugeEdge attachEdge(HugeVertex ownerVertex, BaseEdge element) {
        HugeEdge edge = new HugeEdge(ownerVertex.graph(), element, ownerVertex);
        HugeVertex otherVertex = edge.otherVertex();
        if (element.isOutEdge()) {
            ownerVertex.addOutEdge(edge);
            otherVertex.addInEdge(edge.switchOwner());
        } else {
            ownerVertex.addInEdge(edge);
            otherVertex.addOutEdge(edge.switchOwner());
        }
        return edge;
    }

    public static HugeEdge constructEdgeWithoutLabel(HugeVertex ownerVertex,
                                                     boolean isOutEdge,
                                                     String sortValues,
                                                     Id otherVertexId) {
        HugeGraph graph = ownerVertex.graph();
        EdgeLabel label = EdgeLabel.undefined(graph, EdgeLabel.NONE.id());
        VertexLabel otherLabel = VertexLabel.undefined(graph);
        BaseEdge element = BaseEdge.createEdge(ownerVertex.element(), isOutEdge, label,
                                               sortValues, otherVertexId, otherLabel);
        return attachEdge(ownerVertex, element);
    }

    public static HugeEdge constructEdgeWithoutGraph(HugeVertex ownerVertex,
                                                     boolean isOutEdge,
                                                     EdgeLabel edgeLabel,
                                                     String sortValues,
                                                     Id otherVertexId) {
        BaseEdge element = BaseEdge.createEdgeWithoutSchema(ownerVertex.graph(), ownerVertex.element(),
                                                            isOutEdge, edgeLabel, sortValues, otherVertexId);
        return attachEdge(ownerVertex, element);
    }
}
