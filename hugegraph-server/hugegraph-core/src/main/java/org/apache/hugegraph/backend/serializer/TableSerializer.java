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

package org.apache.hugegraph.backend.serializer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.exception.BackendException;
import org.apache.hugegraph.id.EdgeId;
import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.id.IdGenerator;
import org.apache.hugegraph.id.IdUtil;
import org.apache.hugegraph.query.Condition;
import org.apache.hugegraph.query.ConditionQuery;
import org.apache.hugegraph.query.Query;
import org.apache.hugegraph.backend.store.BackendEntry;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.struct.schema.EdgeLabel;
import org.apache.hugegraph.struct.schema.IndexLabel;
import org.apache.hugegraph.struct.schema.PropertyKey;
import org.apache.hugegraph.struct.schema.SchemaElement;
import org.apache.hugegraph.struct.schema.SchemaLabel;
import org.apache.hugegraph.struct.schema.VertexLabel;
import org.apache.hugegraph.structure.HugeEdge;
import org.apache.hugegraph.structure.HugeEdgeProperty;
import org.apache.hugegraph.structure.HugeElement;
import org.apache.hugegraph.structure.Index;
import org.apache.hugegraph.structure.HugeProperty;
import org.apache.hugegraph.structure.HugeVertex;
import org.apache.hugegraph.structure.HugeVertexProperty;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.type.define.AggregateType;
import org.apache.hugegraph.type.define.Cardinality;
import org.apache.hugegraph.type.define.DataType;
import org.apache.hugegraph.type.define.Directions;
import org.apache.hugegraph.type.define.Frequency;
import org.apache.hugegraph.type.define.HugeKeys;
import org.apache.hugegraph.type.define.IdStrategy;
import org.apache.hugegraph.type.define.IndexType;
import org.apache.hugegraph.type.define.SchemaStatus;
import org.apache.hugegraph.type.define.SerialEnum;
import org.apache.hugegraph.type.define.WriteType;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.JsonUtil;

/**
 * without father and sub edge label
 */
public abstract class TableSerializer extends AbstractSerializer {

    public TableSerializer(HugeConfig config) {
        super(config);
    }

    @Override
    public TableBackendEntry newBackendEntry(HugeType type, Id id) {
        return new TableBackendEntry(type, id);
    }

    protected TableBackendEntry newBackendEntry(HugeElement e) {
        return newBackendEntry(e.type(), e.id());
    }

    protected TableBackendEntry newBackendEntry(SchemaElement e) {
        return newBackendEntry(e.type(), e.id());
    }

    protected TableBackendEntry newBackendEntry(Index index) {
        return newBackendEntry(index.type(), index.id());
    }

    protected abstract TableBackendEntry newBackendEntry(TableBackendEntry.Row row);

    @Override
    protected abstract TableBackendEntry convertEntry(BackendEntry backendEntry);

    protected void formatProperty(HugeProperty<?> property,
                                  TableBackendEntry.Row row) {
        long pkid = property.propertyKey().id().asLong();
        row.column(HugeKeys.PROPERTIES, pkid, this.writeProperty(property));
    }

    protected void parseProperty(Id key, Object colValue, HugeElement owner) {
        // Get PropertyKey by PropertyKey id
        PropertyKey pkey = owner.graph().propertyKey(key);

        // Parse value
        Object value = this.readProperty(pkey, colValue);

        // Set properties of vertex/edge
        if (pkey.cardinality() == Cardinality.SINGLE) {
            owner.addProperty(pkey, value);
        } else {
            if (!(value instanceof Collection)) {
                throw new BackendException(
                        "Invalid value of non-single property: %s", value);
            }
            owner.addProperty(pkey, value);
        }
    }

    protected Object writeProperty(HugeProperty<?> property) {
        return this.writeProperty(property.propertyKey(), property.value());
    }

    protected Object writeProperty(PropertyKey propertyKey, Object value) {
        return JsonUtil.toJson(value);
    }

    @SuppressWarnings("unchecked")
    protected <T> T readProperty(PropertyKey pkey, Object value) {
        Class<T> clazz = (Class<T>) pkey.implementClazz();
        T result = JsonUtil.fromJson(value.toString(), clazz);
        if (pkey.cardinality() != Cardinality.SINGLE) {
            Collection<?> values = (Collection<?>) result;
            List<Object> newValues = new ArrayList<>(values.size());
            for (Object v : values) {
                newValues.add(JsonUtil.castNumber(v, pkey.dataType().clazz()));
            }
            result = (T) newValues;
        }
        return result;
    }

    protected TableBackendEntry.Row formatEdge(HugeEdge edge) {
        EdgeId id = edge.idWithDirection();
        TableBackendEntry.Row row = new TableBackendEntry.Row(edge.type(), id);
        if (edge.hasTtl()) {
            row.ttl(edge.ttl());
            row.column(HugeKeys.EXPIRED_TIME, edge.expiredTime());
        }
        // Id: ownerVertex + direction + edge-label + sortValues + otherVertex
        row.column(HugeKeys.OWNER_VERTEX, this.writeId(id.ownerVertexId()));
        row.column(HugeKeys.DIRECTION, id.directionCode());
        row.column(HugeKeys.LABEL, id.edgeLabelId().asLong());
        row.column(HugeKeys.SORT_VALUES, id.sortValues());
        row.column(HugeKeys.OTHER_VERTEX, this.writeId(id.otherVertexId()));

        this.formatProperties(edge, row);
        return row;
    }

    /**
     * Parse an edge from a entry row
     *
     * @param row    edge entry
     * @param vertex null or the source vertex
     * @param graph  the HugeGraph context object
     * @return the source vertex
     */
    protected HugeEdge parseEdge(TableBackendEntry.Row row,
                                 HugeVertex vertex, HugeGraph graph) {
        Object ownerVertexId = row.column(HugeKeys.OWNER_VERTEX);
        Number dir = row.column(HugeKeys.DIRECTION);
        boolean direction = EdgeId.isOutDirectionFromCode(dir.byteValue());
        Number label = row.column(HugeKeys.LABEL);
        String sortValues = row.column(HugeKeys.SORT_VALUES);
        Object otherVertexId = row.column(HugeKeys.OTHER_VERTEX);
        Number expiredTime = row.column(HugeKeys.EXPIRED_TIME);

        if (vertex == null) {
            Id ownerId = this.readId(ownerVertexId);
            vertex = new HugeVertex(graph, ownerId, VertexLabel.NONE);
        }

        EdgeLabel edgeLabel = graph.edgeLabelOrNone(this.toId(label));
        Id otherId = this.readId(otherVertexId);

        // Construct edge
        HugeEdge edge = HugeEdge.constructEdge(vertex, direction, edgeLabel,
                                               sortValues, otherId);

        // Parse edge properties
        this.parseProperties(edge, row);

        // The expired time is null when the edge is non-ttl
        long expired = edge.hasTtl() ? expiredTime.longValue() : 0L;
        edge.expiredTime(expired);

        return edge;
    }

    @Override
    public BackendEntry writeVertex(HugeVertex vertex) {
        if (vertex.olap()) {
            return this.writeOlapVertex(vertex);
        }
        TableBackendEntry entry = newBackendEntry(vertex);
        if (vertex.hasTtl()) {
            entry.ttl(vertex.ttl());
            entry.column(HugeKeys.EXPIRED_TIME, vertex.expiredTime());
        }
        entry.column(HugeKeys.ID, this.writeId(vertex.id()));
        entry.column(HugeKeys.LABEL, vertex.schemaLabel().id().asLong());
        // Add all properties of a Vertex
        this.formatProperties(vertex, entry.row());
        return entry;
    }

    @Override
    public BackendEntry writeVertexProperty(HugeVertexProperty<?> prop) {
        HugeVertex vertex = prop.element();
        TableBackendEntry entry = newBackendEntry(vertex);
        if (vertex.hasTtl()) {
            entry.ttl(vertex.ttl());
            entry.column(HugeKeys.EXPIRED_TIME, vertex.expiredTime());
        }
        entry.subId(IdGenerator.of(prop.key()));
        entry.column(HugeKeys.ID, this.writeId(vertex.id()));
        entry.column(HugeKeys.LABEL, vertex.schemaLabel().id().asLong());

        this.formatProperty(prop, entry.row());
        return entry;
    }

    @Override
    public HugeVertex readVertex(HugeGraph graph, BackendEntry backendEntry) {
        E.checkNotNull(graph, "serializer graph");
        if (backendEntry == null) {
            return null;
        }

        TableBackendEntry entry = this.convertEntry(backendEntry);
        assert entry.type().isVertex();

        Id id = this.readId(entry.column(HugeKeys.ID));
        Number label = entry.column(HugeKeys.LABEL);
        Number expiredTime = entry.column(HugeKeys.EXPIRED_TIME);

        VertexLabel vertexLabel = VertexLabel.NONE;
        if (label != null) {
            vertexLabel = graph.vertexLabelOrNone(this.toId(label));
        }
        HugeVertex vertex = new HugeVertex(graph, id, vertexLabel);

        // Parse all properties of a Vertex
        this.parseProperties(vertex, entry.row());
        // Parse all edges of a Vertex
        for (TableBackendEntry.Row edge : entry.subRows()) {
            this.parseEdge(edge, vertex, graph);
        }
        // The expired time is null when this is fake vertex of edge or non-ttl
        if (expiredTime != null) {
            vertex.expiredTime(expiredTime.longValue());
        }
        return vertex;
    }

    @Override
    public BackendEntry writeEdge(HugeEdge edge) {
        return newBackendEntry(this.formatEdge(edge));
    }

    @Override
    public BackendEntry writeEdgeProperty(HugeEdgeProperty<?> prop) {
        HugeEdge edge = prop.element();
        EdgeId id = edge.idWithDirection();
        TableBackendEntry.Row row = new TableBackendEntry.Row(edge.type(), id);
        if (edge.hasTtl()) {
            row.ttl(edge.ttl());
            row.column(HugeKeys.EXPIRED_TIME, edge.expiredTime());
        }
        // Id: ownerVertex + direction + edge-label + sortValues + otherVertex
        row.column(HugeKeys.OWNER_VERTEX, this.writeId(id.ownerVertexId()));
        row.column(HugeKeys.DIRECTION, id.directionCode());
        row.column(HugeKeys.LABEL, id.edgeLabelId().asLong());
        row.column(HugeKeys.SORT_VALUES, id.sortValues());
        row.column(HugeKeys.OTHER_VERTEX, this.writeId(id.otherVertexId()));

        // Format edge property
        this.formatProperty(prop, row);

        TableBackendEntry entry = newBackendEntry(row);
        entry.subId(IdGenerator.of(prop.key()));
        return entry;
    }

    @Override
    public HugeEdge readEdge(HugeGraph graph, BackendEntry backendEntry) {
        E.checkNotNull(graph, "serializer graph");
        if (backendEntry == null) {
            return null;
        }

        TableBackendEntry entry = this.convertEntry(backendEntry);
        return this.parseEdge(entry.row(), null, graph);
    }

    @Override
    public BackendEntry writeIndex(Index index) {
        TableBackendEntry entry = newBackendEntry(index);
        /*
         * When field-values is null and elementIds size is 0, it is
         * meaningful for deletion of index data in secondary/range index.
         */
        if (index.fieldValues() == null && index.elementIds().isEmpty()) {
            entry.column(HugeKeys.INDEX_LABEL_ID, index.indexLabel().longId());
        } else {
            entry.column(HugeKeys.FIELD_VALUES, index.fieldValues());
            entry.column(HugeKeys.INDEX_LABEL_ID, index.indexLabel().longId());
            entry.column(HugeKeys.ELEMENT_IDS, this.writeId(index.elementId()));
            entry.subId(index.elementId());
            if (index.hasTtl()) {
                entry.ttl(index.ttl());
                entry.column(HugeKeys.EXPIRED_TIME, index.expiredTime());
            }
        }
        return entry;
    }

    @Override
    public Index readIndex(HugeGraph graph, ConditionQuery query,
                               BackendEntry backendEntry) {
        E.checkNotNull(graph, "serializer graph");
        if (backendEntry == null) {
            return null;
        }

        TableBackendEntry entry = this.convertEntry(backendEntry);

        Object indexValues = entry.column(HugeKeys.FIELD_VALUES);
        Number indexLabelId = entry.column(HugeKeys.INDEX_LABEL_ID);
        Set<Object> elemIds = this.parseIndexElemIds(entry);
        Number expiredTime = entry.column(HugeKeys.EXPIRED_TIME);

        IndexLabel indexLabel = graph.indexLabel(this.toId(indexLabelId));
        Index index = new Index(graph, indexLabel);
        index.fieldValues(indexValues);
        long expired = index.hasTtl() ? expiredTime.longValue() : 0L;
        for (Object elemId : elemIds) {
            index.elementIds(this.readId(elemId), expired);
        }
        return index;
    }

    @Override
    public BackendEntry writeId(HugeType type, Id id) {
        return newBackendEntry(type, id);
    }

    @Override
    protected Id writeQueryId(HugeType type, Id id) {
        if (type.isEdge()) {
            if (!(id instanceof EdgeId)) {
                id = EdgeId.parse(id.asString());
            }
        } else if (type.isGraph()) {
            id = IdGenerator.of(this.writeId(id));
        }
        return id;
    }

    @Override
    protected Query writeQueryEdgeCondition(Query query) {
        query = this.writeQueryCondition(query);
        return query.idsSize() == 0 ? query : null;
    }

    @Override
    protected Query writeQueryCondition(Query query) {
        ConditionQuery result = (ConditionQuery) query;
        // No user-prop when serialize
        assert result.allSysprop();
        for (Condition.Relation r : result.relations()) {
            if (!r.value().equals(r.serialValue())) {
                // Has been serialized before (maybe share a query multi times)
                continue;
            }
            HugeKeys key = (HugeKeys) r.key();
            if (r.relation() == Condition.RelationType.IN) {
                E.checkArgument(r.value() instanceof List,
                                "Expect list value for IN condition: %s", r);
                List<?> values = (List<?>) r.value();
                List<Object> serializedValues = new ArrayList<>(values.size());
                for (Object v : values) {
                    serializedValues.add(this.serializeValue(key, v));
                }
                r.serialValue(serializedValues);
            } else if (r.relation() == Condition.RelationType.CONTAINS_VALUE &&
                       query.resultType().isGraph()) {
                r.serialValue(this.writeProperty(null, r.value()));
            } else {
                r.serialValue(this.serializeValue(key, r.value()));
            }
        }

        return result;
    }

    @Override
    public BackendEntry writeVertexLabel(VertexLabel vertexLabel) {
        TableBackendEntry entry = newBackendEntry(vertexLabel);
        entry.column(HugeKeys.ID, vertexLabel.id().asLong());
        entry.column(HugeKeys.NAME, vertexLabel.name());
        entry.column(HugeKeys.ID_STRATEGY, vertexLabel.idStrategy().code());
        entry.column(HugeKeys.PROPERTIES,
                     this.toLongSet(vertexLabel.properties()));
        entry.column(HugeKeys.PRIMARY_KEYS,
                     this.toLongList(vertexLabel.primaryKeys()));
        entry.column(HugeKeys.NULLABLE_KEYS,
                     this.toLongSet(vertexLabel.nullableKeys()));
        entry.column(HugeKeys.INDEX_LABELS,
                     this.toLongSet(vertexLabel.indexLabels()));
        this.writeEnableLabelIndex(vertexLabel, entry);
        this.writeUserdata(vertexLabel, entry);
        entry.column(HugeKeys.STATUS, vertexLabel.status().code());
        entry.column(HugeKeys.TTL, vertexLabel.ttl());
        entry.column(HugeKeys.TTL_START_TIME,
                     vertexLabel.ttlStartTime().asLong());
        return entry;
    }

    @Override
    public BackendEntry writeEdgeLabel(EdgeLabel edgeLabel) {
        TableBackendEntry entry = newBackendEntry(edgeLabel);
        entry.column(HugeKeys.ID, edgeLabel.id().asLong());
        entry.column(HugeKeys.NAME, edgeLabel.name());
        entry.column(HugeKeys.FREQUENCY, edgeLabel.frequency().code());
        entry.column(HugeKeys.SOURCE_LABEL, edgeLabel.sourceLabel().asLong());
        entry.column(HugeKeys.TARGET_LABEL, edgeLabel.targetLabel().asLong());
        entry.column(HugeKeys.PROPERTIES,
                     this.toLongSet(edgeLabel.properties()));
        entry.column(HugeKeys.SORT_KEYS,
                     this.toLongList(edgeLabel.sortKeys()));
        entry.column(HugeKeys.NULLABLE_KEYS,
                     this.toLongSet(edgeLabel.nullableKeys()));
        entry.column(HugeKeys.INDEX_LABELS,
                     this.toLongSet(edgeLabel.indexLabels()));
        this.writeEnableLabelIndex(edgeLabel, entry);
        this.writeUserdata(edgeLabel, entry);
        entry.column(HugeKeys.STATUS, edgeLabel.status().code());
        entry.column(HugeKeys.TTL, edgeLabel.ttl());
        entry.column(HugeKeys.TTL_START_TIME,
                     edgeLabel.ttlStartTime().asLong());
        return entry;
    }

    @Override
    public BackendEntry writePropertyKey(PropertyKey propertyKey) {
        TableBackendEntry entry = newBackendEntry(propertyKey);
        entry.column(HugeKeys.ID, propertyKey.id().asLong());
        entry.column(HugeKeys.NAME, propertyKey.name());
        entry.column(HugeKeys.DATA_TYPE, propertyKey.dataType().code());
        entry.column(HugeKeys.CARDINALITY, propertyKey.cardinality().code());
        entry.column(HugeKeys.AGGREGATE_TYPE,
                     propertyKey.aggregateType().code());
        entry.column(HugeKeys.WRITE_TYPE,
                     propertyKey.writeType().code());
        entry.column(HugeKeys.PROPERTIES,
                     this.toLongSet(propertyKey.properties()));
        this.writeUserdata(propertyKey, entry);
        entry.column(HugeKeys.STATUS, propertyKey.status().code());
        return entry;
    }

    @Override
    public VertexLabel readVertexLabel(HugeGraph graph,
                                       BackendEntry backendEntry) {
        if (backendEntry == null) {
            return null;
        }

        TableBackendEntry entry = this.convertEntry(backendEntry);

        Number id = schemaColumn(entry, HugeKeys.ID);
        String name = schemaColumn(entry, HugeKeys.NAME);
        IdStrategy idStrategy = schemaEnum(entry, HugeKeys.ID_STRATEGY,
                                           IdStrategy.class);
        Object properties = schemaColumn(entry, HugeKeys.PROPERTIES);
        Object primaryKeys = schemaColumn(entry, HugeKeys.PRIMARY_KEYS);
        Object nullableKeys = schemaColumn(entry, HugeKeys.NULLABLE_KEYS);
        Object indexLabels = schemaColumn(entry, HugeKeys.INDEX_LABELS);
        SchemaStatus status = schemaEnum(entry, HugeKeys.STATUS,
                                         SchemaStatus.class);
        Number ttl = schemaColumn(entry, HugeKeys.TTL);
        Number ttlStartTime = schemaColumn(entry, HugeKeys.TTL_START_TIME);

        VertexLabel vertexLabel = new VertexLabel(graph, this.toId(id), name);
        vertexLabel.idStrategy(idStrategy);
        vertexLabel.properties(this.toIdArray(properties));
        vertexLabel.primaryKeys(this.toIdArray(primaryKeys));
        vertexLabel.nullableKeys(this.toIdArray(nullableKeys));
        vertexLabel.addIndexLabels(this.toIdArray(indexLabels));
        vertexLabel.status(status);
        vertexLabel.ttl(ttl.longValue());
        vertexLabel.ttlStartTime(this.toId(ttlStartTime));
        this.readEnableLabelIndex(vertexLabel, entry);
        this.readUserdata(vertexLabel, entry);
        return vertexLabel;
    }

    @Override
    public EdgeLabel readEdgeLabel(HugeGraph graph, BackendEntry backendEntry) {
        if (backendEntry == null) {
            return null;
        }

        TableBackendEntry entry = this.convertEntry(backendEntry);

        Number id = schemaColumn(entry, HugeKeys.ID);
        String name = schemaColumn(entry, HugeKeys.NAME);
        Frequency frequency = schemaEnum(entry, HugeKeys.FREQUENCY,
                                         Frequency.class);
        Number sourceLabel = schemaColumn(entry, HugeKeys.SOURCE_LABEL);
        Number targetLabel = schemaColumn(entry, HugeKeys.TARGET_LABEL);
        Object sortKeys = schemaColumn(entry, HugeKeys.SORT_KEYS);
        Object nullableKeys = schemaColumn(entry, HugeKeys.NULLABLE_KEYS);
        Object properties = schemaColumn(entry, HugeKeys.PROPERTIES);
        Object indexLabels = schemaColumn(entry, HugeKeys.INDEX_LABELS);
        SchemaStatus status = schemaEnum(entry, HugeKeys.STATUS,
                                         SchemaStatus.class);
        Number ttl = schemaColumn(entry, HugeKeys.TTL);
        Number ttlStartTime = schemaColumn(entry, HugeKeys.TTL_START_TIME);

        EdgeLabel edgeLabel = new EdgeLabel(graph, this.toId(id), name);
        edgeLabel.frequency(frequency);
        edgeLabel.sourceLabel(this.toId(sourceLabel));
        edgeLabel.targetLabel(this.toId(targetLabel));
        edgeLabel.properties(this.toIdArray(properties));
        edgeLabel.sortKeys(this.toIdArray(sortKeys));
        edgeLabel.nullableKeys(this.toIdArray(nullableKeys));
        edgeLabel.addIndexLabels(this.toIdArray(indexLabels));
        edgeLabel.status(status);
        edgeLabel.ttl(ttl.longValue());
        edgeLabel.ttlStartTime(this.toId(ttlStartTime));
        this.readEnableLabelIndex(edgeLabel, entry);
        this.readUserdata(edgeLabel, entry);
        return edgeLabel;
    }

    @Override
    public PropertyKey readPropertyKey(HugeGraph graph,
                                       BackendEntry backendEntry) {
        if (backendEntry == null) {
            return null;
        }

        TableBackendEntry entry = this.convertEntry(backendEntry);

        Number id = schemaColumn(entry, HugeKeys.ID);
        String name = schemaColumn(entry, HugeKeys.NAME);
        DataType dataType = schemaEnum(entry, HugeKeys.DATA_TYPE,
                                       DataType.class);
        Cardinality cardinality = schemaEnum(entry, HugeKeys.CARDINALITY,
                                             Cardinality.class);
        AggregateType aggregateType = schemaEnum(entry, HugeKeys.AGGREGATE_TYPE,
                                                 AggregateType.class);
        WriteType writeType = schemaEnumOrDefault(
                entry, HugeKeys.WRITE_TYPE,
                WriteType.class, WriteType.OLTP);
        Object properties = schemaColumn(entry, HugeKeys.PROPERTIES);
        SchemaStatus status = schemaEnum(entry, HugeKeys.STATUS,
                                         SchemaStatus.class);

        PropertyKey propertyKey = new PropertyKey(graph, this.toId(id), name);
        propertyKey.dataType(dataType);
        propertyKey.cardinality(cardinality);
        propertyKey.aggregateType(aggregateType);
        propertyKey.writeType(writeType);
        propertyKey.properties(this.toIdArray(properties));
        propertyKey.status(status);
        this.readUserdata(propertyKey, entry);
        return propertyKey;
    }

    @Override
    public BackendEntry writeIndexLabel(IndexLabel indexLabel) {
        TableBackendEntry entry = newBackendEntry(indexLabel);
        entry.column(HugeKeys.ID, indexLabel.id().asLong());
        entry.column(HugeKeys.NAME, indexLabel.name());
        entry.column(HugeKeys.BASE_TYPE, indexLabel.baseType().code());
        entry.column(HugeKeys.BASE_VALUE, indexLabel.baseValue().asLong());
        entry.column(HugeKeys.INDEX_TYPE, indexLabel.indexType().code());
        entry.column(HugeKeys.FIELDS,
                     this.toLongList(indexLabel.indexFields()));
        this.writeUserdata(indexLabel, entry);
        entry.column(HugeKeys.STATUS, indexLabel.status().code());
        return entry;
    }

    @Override
    public IndexLabel readIndexLabel(HugeGraph graph,
                                     BackendEntry backendEntry) {
        if (backendEntry == null) {
            return null;
        }

        TableBackendEntry entry = this.convertEntry(backendEntry);

        Number id = schemaColumn(entry, HugeKeys.ID);
        String name = schemaColumn(entry, HugeKeys.NAME);
        HugeType baseType = schemaEnum(entry, HugeKeys.BASE_TYPE,
                                       HugeType.class);
        Number baseValueId = schemaColumn(entry, HugeKeys.BASE_VALUE);
        IndexType indexType = schemaEnum(entry, HugeKeys.INDEX_TYPE,
                                         IndexType.class);
        Object indexFields = schemaColumn(entry, HugeKeys.FIELDS);
        SchemaStatus status = schemaEnum(entry, HugeKeys.STATUS,
                                         SchemaStatus.class);

        IndexLabel indexLabel = new IndexLabel(graph, this.toId(id), name);
        indexLabel.baseType(baseType);
        indexLabel.baseValue(this.toId(baseValueId));
        indexLabel.indexType(indexType);
        indexLabel.indexFields(this.toIdArray(indexFields));
        indexLabel.status(status);
        this.readUserdata(indexLabel, entry);
        return indexLabel;
    }

    protected abstract Id toId(Number number);

    protected abstract Id[] toIdArray(Object object);

    protected abstract Object toLongSet(Collection<Id> ids);

    protected abstract Object toLongList(Collection<Id> ids);

    protected abstract Set<Object> parseIndexElemIds(TableBackendEntry entry);

    protected abstract void formatProperties(HugeElement element,
                                             TableBackendEntry.Row row);

    protected abstract void parseProperties(HugeElement element,
                                            TableBackendEntry.Row row);

    protected Object writeId(Id id) {
        return IdUtil.writeStoredString(id);
    }

    protected Id readId(Object id) {
        return IdUtil.readStoredString(id.toString());
    }

    protected Object serializeValue(HugeKeys key, Object value) {
        if (value instanceof Directions) {
            value = ((Directions) value).type().code();
        } else if (value instanceof Id) {
            if (key == HugeKeys.OWNER_VERTEX || key == HugeKeys.OTHER_VERTEX) {
                // Serialize vertex id
                value = this.writeId((Id) value);
            } else {
                // Serialize other id value, like label id
                value = ((Id) value).asObject();
            }
        }

        return value;
    }

    protected void writeEnableLabelIndex(SchemaLabel schema,
                                         TableBackendEntry entry) {
        entry.column(HugeKeys.ENABLE_LABEL_INDEX, schema.enableLabelIndex());
    }

    protected void readEnableLabelIndex(SchemaLabel schema,
                                        TableBackendEntry entry) {
        Boolean enableLabelIndex = schemaColumn(entry,
                                                HugeKeys.ENABLE_LABEL_INDEX);
        schema.enableLabelIndex(enableLabelIndex);
    }

    protected abstract void writeUserdata(SchemaElement schema,
                                          TableBackendEntry entry);

    protected abstract void readUserdata(SchemaElement schema,
                                         TableBackendEntry entry);

    private static <T> T schemaColumn(TableBackendEntry entry, HugeKeys key) {
        assert entry.type().isSchema();

        T value = entry.column(key);
        E.checkState(value != null,
                     "Not found key '%s' from entry %s", key, entry);
        return value;
    }

    private static <T extends SerialEnum> T schemaEnum(TableBackendEntry entry,
                                                       HugeKeys key,
                                                       Class<T> clazz) {
        Number value = schemaColumn(entry, key);
        return SerialEnum.fromCode(clazz, value.byteValue());
    }

    private static <T extends SerialEnum> T schemaEnumOrDefault(
            TableBackendEntry entry,
            HugeKeys key, Class<T> clazz,
            T defaultValue) {
        assert entry.type().isSchema();

        Number value = entry.column(key);
        if (value == null) {
            return defaultValue;
        }
        return SerialEnum.fromCode(clazz, value.byteValue());
    }
}
