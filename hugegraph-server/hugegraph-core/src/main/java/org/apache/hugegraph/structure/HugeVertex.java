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

import java.util.AbstractCollection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hugegraph.exception.HugeException;
import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.id.IdGenerator;
import org.apache.hugegraph.backend.id.SnowflakeIdGenerator;
import org.apache.hugegraph.id.SplicingIdGenerator;
import org.apache.hugegraph.query.Query;
import org.apache.hugegraph.backend.query.QueryResults;
import org.apache.hugegraph.serializer.BytesBuffer;
import org.apache.hugegraph.backend.tx.GraphTransaction;
import org.apache.hugegraph.config.CoreOptions;
import org.apache.hugegraph.perf.PerfUtil.Watched;
import org.apache.hugegraph.struct.schema.EdgeLabel;
import org.apache.hugegraph.struct.schema.PropertyKey;
import org.apache.hugegraph.struct.schema.VertexLabel;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.type.define.Cardinality;
import org.apache.hugegraph.type.define.Directions;
import org.apache.hugegraph.type.define.HugeKeys;
import org.apache.hugegraph.type.define.IdStrategy;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.TinkerPopUtil;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyVertexProperty;


public class HugeVertex extends HugeElement implements Vertex, Cloneable {

    private BaseVertex element;
    private Map<BaseEdge, HugeEdge> edgeWrappers;

    public HugeVertex(final HugeGraph graph, Id id, VertexLabel label) {
        super(graph);

        E.checkArgumentNotNull(label, "Vertex label can't be null");
        this.element = new BaseVertex(id, label, BaseVertex.TypeContext.ENGINE);
        this.element.encodeNumber(this.graph().option(CoreOptions.VERTEX_ENCODE_PK_NUMBER));
        this.edgeWrappers = new IdentityHashMap<>();
        if (this.element.id() != null) {
            if (label.idStrategy() == IdStrategy.CUSTOMIZE_UUID) {
                this.assignId(id);
            } else {
                this.checkIdLength();
            }
        }
    }

    /** Adopt decoded shared state without copying its property values. */
    public HugeVertex(final HugeGraph graph, BaseVertex element) {
        super(graph);
        E.checkArgumentNotNull(element, "Vertex element can't be null");
        E.checkArgumentNotNull(element.schemaLabel(), "Vertex label can't be null");
        this.element = element;
        this.element.typeContext(BaseVertex.TypeContext.ENGINE);
        this.element.encodeNumber(this.graph().option(CoreOptions.VERTEX_ENCODE_PK_NUMBER));
        this.edgeWrappers = new IdentityHashMap<>();
        if (element.id() != null) {
            this.checkIdLength();
        }
    }

    @Override
    public HugeType type() {
        return this.element.type();
    }

    @Override
    public Id id() {
        return this.element.id();
    }

    @Override
    public VertexLabel schemaLabel() {
        assert VertexLabel.OLAP_VL.equals(this.element.schemaLabel()) ||
               this.graph().sameAs(this.element.schemaLabel().graph());
        return this.element.schemaLabel();
    }

    @Override
    public String name() {
        return this.element.name();
    }

    @Override
    public BaseVertex element() {
        return this.element;
    }

    public void assignId(Id id) {
        this.assignId(id, false);
    }

    @Watched(prefix = "vertex")
    public void assignId(Id id, boolean force) {
        IdStrategy strategy = this.element.schemaLabel().idStrategy();
        // Generate an id and assign
        switch (strategy) {
            case CUSTOMIZE_STRING:
                assert !id.number();
                this.element.id(id);
                break;
            case CUSTOMIZE_NUMBER:
                assert id.number();
                this.element.id(id);
                break;
            case CUSTOMIZE_UUID:
                this.element.id(id.uuid() ? id : IdGenerator.of(id.asString(), true));
                break;
            case PRIMARY_KEY:
                this.element.id(SplicingIdGenerator.instance().generate(this.element()));
                break;
            case AUTOMATIC:
                if (force) {
                    // Resume id for AUTOMATIC id strategy in restoring mode
                    assert id.number();
                    this.element.id(id);
                } else {
                    this.element.id(SnowflakeIdGenerator.instance(this.graph())
                                                  .generate(this.element()));
                }
                break;
            default:
                throw new AssertionError(String.format("Unknown id strategy '%s'", strategy));
        }
        this.checkIdLength();
    }

    protected void checkIdLength() {
        assert this.element.id() != null;
        int len = this.element.id().asBytes().length;
        if (len <= BytesBuffer.ID_LEN_MAX) {
            return;
        }
        E.checkArgument(false,
                        "The max length of vertex id is %s, but got %s {%s}",
                        BytesBuffer.ID_LEN_MAX, len, this.element.id());
    }

    @Override
    public String label() {
        return this.schemaLabel().name();
    }

    public void correctVertexLabel(VertexLabel correctLabel) {
        this.element.correctVertexLabel(correctLabel);
    }

    @Watched(prefix = "vertex")
    protected List<Object> primaryValues() {
        return this.element.primaryValues();
    }

    public boolean existsEdges() {
        return !this.element.edges().isEmpty();
    }

    public Collection<HugeEdge> getEdges() {
        Collection<BaseEdge> edges = this.element.edges();
        return Collections.unmodifiableCollection(new AbstractCollection<HugeEdge>() {
            @Override
            public Iterator<HugeEdge> iterator() {
                Iterator<BaseEdge> iterator = edges.iterator();
                return new Iterator<HugeEdge>() {
                    @Override
                    public boolean hasNext() {
                        return iterator.hasNext();
                    }

                    @Override
                    public HugeEdge next() {
                        BaseEdge edge = iterator.next();
                        return HugeVertex.this.edgeWrappers.computeIfAbsent(
                                edge, value -> new HugeEdge(HugeVertex.this.graph(), value,
                                                           HugeVertex.this));
                    }
                };
            }

            @Override
            public int size() {
                return edges.size();
            }
        });
    }

    public void resetEdges() {
        this.resetEdges(false);
    }

    protected void resetEdges(boolean unique) {
        this.element.resetEdges(unique);
        this.edgeWrappers.clear();
    }

    public void removeEdge(HugeEdge edge) {
        BaseEdge removed = this.element.removeEdge(edge.element());
        if (removed != null) {
            this.edgeWrappers.remove(removed);
        }
    }

    public void addEdge(HugeEdge edge) {
        if (this.element.addEdge(edge.element())) {
            this.edgeWrappers.put(edge.element(), edge);
        }
    }

    /**
     * Add one edge between this vertex and other vertex
     * <p>
     * *** this method is not thread safe, must clone this vertex first before
     * multi thread access e.g. `vertex.copy().resetTx();` ***
     */
    @Watched(prefix = "vertex")
    @Override
    public HugeEdge addEdge(String label, Vertex vertex, Object... keyValues) {
        HugeVertex targetVertex = (HugeVertex) vertex;

        HugeEdge edge = this.constructEdge(label, targetVertex, keyValues);

        // Attach edge to vertex
        this.addOutEdge(edge);
        targetVertex.addInEdge(edge.switchOwner());

        if (this.fresh()) {
            this.graph().canAddEdge(edge);
            return this.tx().addEdge(edge);
        } else {
            return (HugeEdge) this.graph().addEdge(edge);
        }
    }

    public HugeEdge constructEdge(String label, HugeVertex vertex,
                                  Object... keyValues) {
        ElementKeys elemKeys = classifyKeys(keyValues);
        // Check id (must be null)
        if (elemKeys.id() != null) {
            throw Edge.Exceptions.userSuppliedIdsNotSupported();
        }
        Id id = null;

        // Check target vertex
        E.checkArgumentNotNull(vertex, "Target vertex can't be null");
        // Check label
        E.checkArgument(label != null && !label.isEmpty(),
                        "Edge label can't be null or empty");
        EdgeLabel edgeLabel = this.graph().edgeLabel(label);
        E.checkArgument(!edgeLabel.isFather(), "Adding an edge of parent type " +
                                               "is not allowed");
        // Check link
        E.checkArgument(edgeLabel.checkLinkEqual(this.schemaLabel().id(),
                                                 vertex.schemaLabel().id()),
                        "Undefined link of edge label '%s': '%s' -> '%s'",
                        label, this.label(), vertex.label());
        // Check sortKeys
        List<Id> keys = this.graph().mapPkName2Id(elemKeys.keys());
        E.checkArgument(new HashSet<>(keys).containsAll(edgeLabel.sortKeys()),
                        "The sort key(s) must be set for the edge " +
                        "with label: '%s'", edgeLabel.name());

        // Check whether passed all non-null props
        @SuppressWarnings("unchecked")
        Collection<Id> nonNullKeys = CollectionUtils.subtract(
                edgeLabel.properties(),
                edgeLabel.nullableKeys());
        if (!new HashSet<>(keys).containsAll(nonNullKeys)) {
            @SuppressWarnings("unchecked")
            Collection<Id> missed = CollectionUtils.subtract(nonNullKeys, keys);
            E.checkArgument(false, "All non-null property keys: %s " +
                                   "of edge label '%s' must be set, " +
                                   "but missed keys: %s",
                            this.graph().mapPkId2Name(nonNullKeys),
                            edgeLabel.name(),
                            this.graph().mapPkId2Name(missed));
        }

        HugeEdge edge = new HugeEdge(this, id, edgeLabel, vertex);

        // Set properties
        ElementHelper.attachProperties(edge, keyValues);

        edge.assignId();
        return edge;
    }

    /**
     * Add edge with direction OUT
     *
     * @param edge the out edge
     */
    @Watched
    public void addOutEdge(HugeEdge edge) {
        if (edge.ownerVertex() == null) {
            edge.sourceVertex(this);
        }
        E.checkState(edge.isDirection(Directions.OUT),
                     "The owner vertex('%s') of OUT edge '%s' should be '%s'",
                     edge.ownerVertex().id(), edge, this.id());
        this.addEdge(edge);
    }

    /**
     * Add edge with direction IN
     *
     * @param edge the in edge
     */
    @Watched
    public void addInEdge(HugeEdge edge) {
        if (edge.ownerVertex() == null) {
            edge.targetVertex(this);
        }
        E.checkState(edge.isDirection(Directions.IN),
                     "The owner vertex('%s') of IN edge '%s' should be '%s'",
                     edge.ownerVertex().id(), edge, this.id());
        this.addEdge(edge);
    }

    public Iterator<Edge> getEdges(Directions direction, String... edgeLabels) {
        List<Edge> list = new LinkedList<>();
        for (HugeEdge edge : this.getEdges()) {
            if (edge.matchDirection(direction) &&
                edge.belongToLabels(edgeLabels)) {
                list.add(edge);
            }
        }
        return list.iterator();
    }

    public Iterator<Vertex> getVertices(Directions direction,
                                        String... edgeLabels) {
        List<Vertex> list = new LinkedList<>();
        Iterator<Edge> edges = this.getEdges(direction, edgeLabels);
        while (edges.hasNext()) {
            HugeEdge edge = (HugeEdge) edges.next();
            list.add(edge.otherVertex(this));
        }
        return list.iterator();
    }

    @Watched(prefix = "vertex")
    @Override
    public Iterator<Edge> edges(Direction tinkerpopDir, String... edgeLabels) {
        Directions direction = TinkerPopUtil.direction(tinkerpopDir);
        Id[] edgeLabelIds = this.graph().mapElName2Id(edgeLabels);
        Query query = GraphTransaction.constructEdgesQuery(this.id(), direction,
                                                           edgeLabelIds);
        return this.graph().edges(query);
    }

    @Watched(prefix = "vertex")
    @Override
    public Iterator<Vertex> vertices(Direction direction,
                                     String... edgeLabels) {
        Iterator<Edge> edges = this.edges(direction, edgeLabels);
        return this.graph().adjacentVertices(edges);
    }

    @Watched(prefix = "vertex")
    @Override
    public void remove() {
        this.removed(true);
        /*
         * Call by tx or by graph to remove vertex,
         * call by tx if the vertex is new because the context is dependent
         */
        GraphTransaction tx = this.tx();
        if (tx != null) {
            assert this.fresh();
            tx.removeVertex(this);
        } else {
            assert !this.fresh();
            this.graph().removeVertex(this);
        }
    }

    @Watched(prefix = "vertex")
    @Override
    public <V> VertexProperty<V> property(VertexProperty.Cardinality cardinality,
                                          String key, V value, Object... objects) {
        if (objects.length != 0 && objects[0].equals(T.id)) {
            throw VertexProperty.Exceptions.userSuppliedIdsNotSupported();
        }
        // TODO: extra props: objects
        if (objects.length != 0) {
            throw VertexProperty.Exceptions.metaPropertiesNotSupported();
        }

        PropertyKey propertyKey = this.graph().propertyKey(key);
        /*
         * g.AddV("xxx").property("key1", val1).property("key2", val2)
         * g.AddV("xxx").property(single, "key1", val1)
         *              .property(list, "key2", val2)
         *
         * The cardinality single may be user supplied single, it may also be
         * that user doesn't supply cardinality, when it is latter situation,
         * we shouldn't check it. Because of this reason, we are forced to
         * give up the check of user supplied cardinality single.
         * The cardinality not single must be user supplied, so should check it
         */
        if (cardinality != VertexProperty.Cardinality.single) {
            E.checkArgument(propertyKey.cardinality() ==
                            TinkerPopUtil.cardinality(cardinality),
                            "Invalid cardinality '%s' for property key '%s', " +
                            "expect '%s'", cardinality, key,
                            propertyKey.cardinality().string());
        }

        // Check key in vertex label
        E.checkArgument(VertexLabel.OLAP_VL.equals(this.element.schemaLabel()) ||
                        this.element.schemaLabel().properties().contains(propertyKey.id()),
                        "Invalid property '%s' for vertex label '%s'",
                        key, this.element.schemaLabel());
        // Primary-Keys can only be set once
        if (this.schemaLabel().primaryKeys().contains(propertyKey.id())) {
            E.checkArgument(!this.hasProperty(propertyKey.id()),
                            "Can't update primary key: '%s'", key);
        }
        if (value == null) {
            this.removeProperty(propertyKey.id());
            return EmptyVertexProperty.instance();
        }

        @SuppressWarnings("unchecked")
        VertexProperty<V> prop = (VertexProperty<V>) this.addProperty(propertyKey,
                                                                      value, !this.fresh());
        return prop;
    }

    @Override
    protected GraphTransaction tx() {
        return null;
    }

    @Watched(prefix = "vertex")
    @Override
    protected <V> HugeVertexProperty<V> newProperty(PropertyKey pkey, V val) {
        return new HugeVertexProperty<>(this, pkey, val);
    }

    @Override
    protected <V> HugeVertexProperty<V> wrapProperty(BaseProperty<V> property) {
        return new HugeVertexProperty<>(this, property);
    }

    @Watched(prefix = "vertex")
    @Override
    protected <V> void onUpdateProperty(Cardinality cardinality,
                                        HugeProperty<V> prop) {
        if (prop != null) {
            assert prop instanceof HugeVertexProperty;
            /*
             * Call tx or graph to update property,
             * call by tx if the vertex is new because the context is dependent
             * (should update cache even if it's new)
             */
            GraphTransaction tx = this.tx();
            if (tx != null) {
                assert this.fresh();
                tx.addVertexProperty((HugeVertexProperty<V>) prop);
            } else {
                assert !this.fresh();
                this.graph().addVertexProperty((HugeVertexProperty<V>) prop);
            }
        }
    }

    @Watched(prefix = "vertex")
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

        // NOTE: only adjacent vertex will reach here
        Iterator<Vertex> vertices = this.graph().adjacentVertex(this.id());
        HugeVertex vertex = (HugeVertex) QueryResults.one(vertices);
        if (vertex == null && !throwIfNotExist) {
            return false;
        }
        E.checkState(vertex != null, "Vertex '%s' does not exist", this.element.id());
        if (vertex.schemaLabel().undefined() ||
            !vertex.schemaLabel().equals(this.schemaLabel())) {
            // Update vertex label of dangling edge to undefined
            this.correctVertexLabel(VertexLabel.undefined(this.graph()));
            vertex.resetProperties();
        }
        this.copyProperties(vertex);
        this.updateToDefaultValueIfNone();
        return true;
    }

    @Watched(prefix = "vertex")
    @SuppressWarnings("unchecked") // (VertexProperty<V>) prop
    @Override
    public <V> Iterator<VertexProperty<V>> properties(String... keys) {
        // TODO: Compatible with TinkerPop properties() (HugeGraph-742)
        this.ensureFilledProperties(true);

        // Capacity should be about the following size
        int propsCapacity = keys.length == 0 ?
                            this.sizeOfProperties() :
                            keys.length;
        List<VertexProperty<V>> props = new ArrayList<>(propsCapacity);

        if (keys.length == 0) {
            for (HugeProperty<?> prop : this.getProperties()) {
                assert prop instanceof VertexProperty;
                props.add((VertexProperty<V>) prop);
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
                assert prop instanceof VertexProperty;
                props.add((VertexProperty<V>) prop);
            }
        }

        return props.iterator();
    }

    @Override
    public Object sysprop(HugeKeys key) {
        return this.element.sysprop(key);
    }

    public boolean valid() {
        try {
            return this.ensureFilledProperties(false);
        } catch (Throwable e) {
            // Generally the program can't get here
            return false;
        }
    }

    /**
     * Clear edges/properties of the cloned vertex, and set `removed` true.
     *
     * @return a new vertex
     */
    public HugeVertex prepareRemoved() {
        HugeVertex vertex = this.clone();
        vertex.removed(true);
        vertex.resetEdges();
        vertex.resetProperties();
        return vertex;
    }

    public boolean olap() {
        return this.element.schemaLabel().olap();
    }

    @Override
    public HugeVertex copy() {
        HugeVertex vertex = this.clone();
        vertex.copyProperties(this);
        return vertex;
    }

    @Override
    protected HugeVertex clone() {
        try {
            HugeVertex vertex = (HugeVertex) super.clone();
            vertex.element = (BaseVertex) this.element.clone();
            vertex.edgeWrappers = new IdentityHashMap<>(this.edgeWrappers);
            return vertex;
        } catch (CloneNotSupportedException e) {
            throw new HugeException("Failed to clone HugeVertex", e);
        }
    }

    @Override
    public String toString() {
        return StringFactory.vertexString(this);
    }

    public static final Id getIdValue(Object idValue) {
        return HugeElement.getIdValue(idValue);
    }

    public static HugeVertex undefined(HugeGraph graph, Id id) {
        VertexLabel label = VertexLabel.undefined(graph);
        return new HugeVertex(graph, id, label);
    }

    public static HugeVertex create(final GraphTransaction tx,
                                    Id id, VertexLabel label) {
        return new HugeVertex4Insert(tx, id, label);
    }



    private static final class HugeVertex4Insert extends HugeVertex {

        private GraphTransaction tx;

        public HugeVertex4Insert(final GraphTransaction tx,
                                 Id id, VertexLabel label) {
            super(tx.graph(), id, label);
            /*
             * Use Set to hold edges inserted into vertex
             * to avoid duplicated edges
             */
            this.resetEdges(true);
            this.tx = tx;
            this.fresh(true);
        }

        @Override
        public void resetEdges() {
            this.resetEdges(true);
        }

        @Override
        public void committed() {
            super.committed();
            this.tx = null;
        }

        @Override
        protected GraphTransaction tx() {
            if (this.fresh()) {
                E.checkNotNull(this.tx, "tx");
                return this.tx;
            }
            return null;
        }
    }
}
