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

import static org.apache.hugegraph.schema.SchemaElement.UNDEF;
import static org.apache.hugegraph.structure.HugeIndex.number2bytes;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.NotImplementedException;
import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.backend.BackendException;
import org.apache.hugegraph.backend.id.EdgeId;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.id.IdGenerator;
import org.apache.hugegraph.backend.page.PageState;
import org.apache.hugegraph.backend.query.Condition;
import org.apache.hugegraph.backend.query.Condition.RangeConditions;
import org.apache.hugegraph.backend.query.ConditionQuery;
import org.apache.hugegraph.backend.query.IdPrefixQuery;
import org.apache.hugegraph.backend.query.IdRangeQuery;
import org.apache.hugegraph.backend.query.Query;
import org.apache.hugegraph.backend.serializer.BinaryBackendEntry.BinaryId;
import org.apache.hugegraph.backend.store.BackendEntry;
import org.apache.hugegraph.backend.store.BackendEntry.BackendColumn;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.iterator.CIter;
import org.apache.hugegraph.iterator.MapperIterator;
import org.apache.hugegraph.schema.EdgeLabel;
import org.apache.hugegraph.schema.IndexLabel;
import org.apache.hugegraph.schema.PropertyKey;
import org.apache.hugegraph.schema.SchemaElement;
import org.apache.hugegraph.schema.VertexLabel;
import org.apache.hugegraph.structure.HugeEdge;
import org.apache.hugegraph.structure.HugeEdgeProperty;
import org.apache.hugegraph.structure.HugeElement;
import org.apache.hugegraph.structure.HugeIndex;
import org.apache.hugegraph.structure.HugeProperty;
import org.apache.hugegraph.structure.HugeVectorIndexMap;
import org.apache.hugegraph.structure.HugeVertex;
import org.apache.hugegraph.structure.HugeVertexProperty;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.type.define.AggregateType;
import org.apache.hugegraph.type.define.Cardinality;
import org.apache.hugegraph.type.define.DataType;
import org.apache.hugegraph.type.define.Directions;
import org.apache.hugegraph.type.define.EdgeLabelType;
import org.apache.hugegraph.type.define.Frequency;
import org.apache.hugegraph.type.define.HugeKeys;
import org.apache.hugegraph.type.define.IdStrategy;
import org.apache.hugegraph.type.define.IndexType;
import org.apache.hugegraph.type.define.IndexVectorState;
import org.apache.hugegraph.type.define.SchemaStatus;
import org.apache.hugegraph.type.define.SerialEnum;
import org.apache.hugegraph.type.define.WriteType;
import org.apache.hugegraph.util.Bytes;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.JsonUtil;
import org.apache.hugegraph.util.NumericUtil;
import org.apache.hugegraph.util.StringEncoding;
import org.apache.tinkerpop.gremlin.structure.Edge;

public class BinarySerializer extends AbstractSerializer {

    /*
     * Id is stored in column name if keyWithIdPrefix=true like RocksDB,
     * else stored in rowkey like HBase.
     */
    private final boolean keyWithIdPrefix;
    private final boolean indexWithIdPrefix;
    private final boolean enablePartition;

    public BinarySerializer() {
        this(true, true, false);
    }

    public BinarySerializer(HugeConfig config) {
        this(true, true, false);
    }

    public BinarySerializer(boolean keyWithIdPrefix,
                            boolean indexWithIdPrefix,
                            boolean enablePartition) {
        this.keyWithIdPrefix = keyWithIdPrefix;
        this.indexWithIdPrefix = indexWithIdPrefix;
        this.enablePartition = enablePartition;
    }

    @Override
    protected BinaryBackendEntry newBackendEntry(HugeType type, Id id) {
        if (type.isVertex()) {
            BytesBuffer buffer = BytesBuffer.allocate(2 + 1 + id.length());
            writePartitionedId(HugeType.VERTEX, id, buffer);
            return new BinaryBackendEntry(type, new BinaryId(buffer.bytes(), id));
        }

        if (type.isEdge()) {
            E.checkState(id instanceof BinaryId,
                         "Expect a BinaryId for BackendEntry with edge id");
            return new BinaryBackendEntry(type, (BinaryId) id);
        }

        if (type.isIndex()) {
            if (this.enablePartition) {
                if (type.isStringIndex()) {
                    // TODO: add string index partition
                }
                if (type.isNumericIndex()) {
                    // TODO: add numeric index partition
                }
            }
            BytesBuffer buffer = BytesBuffer.allocate(1 + id.length());
            byte[] idBytes = buffer.writeIndexId(id, type).bytes();
            return new BinaryBackendEntry(type, new BinaryId(idBytes, id));
        }

        BytesBuffer buffer = BytesBuffer.allocate(1 + id.length());
        byte[] idBytes = buffer.writeId(id).bytes();
        return new BinaryBackendEntry(type, new BinaryId(idBytes, id));
    }

    protected final BinaryBackendEntry newBackendEntry(HugeVertex vertex) {
        return newBackendEntry(vertex.type(), vertex.id());
    }

    protected final BinaryBackendEntry newBackendEntry(HugeEdge edge) {
        BinaryId id = writeEdgeId(edge.idWithDirection());
        return newBackendEntry(edge.type(), id);
    }

    protected final BinaryBackendEntry newBackendEntry(SchemaElement elem) {
        return newBackendEntry(elem.type(), elem.id());
    }

    @Override
    protected BinaryBackendEntry convertEntry(BackendEntry entry) {
        assert entry instanceof BinaryBackendEntry;
        return (BinaryBackendEntry) entry;
    }

    protected byte[] formatSyspropName(Id id, HugeKeys col) {
        int idLen = this.keyWithIdPrefix ? 1 + id.length() : 0;
        BytesBuffer buffer = BytesBuffer.allocate(idLen + 1 + 1);
        byte sysprop = HugeType.SYS_PROPERTY.code();
        if (this.keyWithIdPrefix) {
            buffer.writeId(id);
        }
        return buffer.write(sysprop).write(col.code()).bytes();
    }

    protected byte[] formatSyspropName(BinaryId id, HugeKeys col) {
        int idLen = this.keyWithIdPrefix ? id.length() : 0;
        BytesBuffer buffer = BytesBuffer.allocate(idLen + 1 + 1);
        byte sysprop = HugeType.SYS_PROPERTY.code();
        if (this.keyWithIdPrefix) {
            buffer.write(id.asBytes());
        }
        return buffer.write(sysprop).write(col.code()).bytes();
    }

    protected BackendColumn formatLabel(HugeElement elem) {
        BackendColumn col = new BackendColumn();
        col.name = this.formatSyspropName(elem.id(), HugeKeys.LABEL);
        Id label = elem.schemaLabel().id();
        BytesBuffer buffer = BytesBuffer.allocate(label.length() + 1);
        col.value = buffer.writeId(label).bytes();
        return col;
    }

    protected byte[] formatPropertyName(HugeProperty<?> prop) {
        Id id = prop.element().id();
        int idLen = this.keyWithIdPrefix ? 1 + id.length() : 0;
        Id pkeyId = prop.propertyKey().id();
        BytesBuffer buffer = BytesBuffer.allocate(idLen + 2 + pkeyId.length());
        if (this.keyWithIdPrefix) {
            buffer.writeId(id);
        }
        buffer.write(prop.type().code());
        buffer.writeId(pkeyId);
        return buffer.bytes();
    }

    protected BackendColumn formatProperty(HugeProperty<?> prop) {
        BytesBuffer buffer = BytesBuffer.allocate(BytesBuffer.BUF_PROPERTY);
        buffer.writeProperty(prop.propertyKey(), prop.value());
        return BackendColumn.of(this.formatPropertyName(prop), buffer.bytes());
    }

    protected void parseProperty(Id pkeyId, BytesBuffer buffer,
                                 HugeElement owner) {
        PropertyKey pkey = owner.graph().propertyKey(pkeyId);

        // Parse value
        Object value = buffer.readProperty(pkey);

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

    protected void formatProperties(Collection<HugeProperty<?>> props,
                                    BytesBuffer buffer) {
        // Write properties size
        buffer.writeVInt(props.size());

        // Write properties data
        for (HugeProperty<?> property : props) {
            PropertyKey pkey = property.propertyKey();
            buffer.writeVInt(SchemaElement.schemaId(pkey.id()));
            buffer.writeProperty(pkey, property.value());
        }
    }

    protected void parseProperties(BytesBuffer buffer, HugeElement owner) {
        int size = buffer.readVInt();
        assert size >= 0;
        for (int i = 0; i < size; i++) {
            Id pkeyId = IdGenerator.of(buffer.readVInt());
            this.parseProperty(pkeyId, buffer, owner);
        }
    }

    protected void formatExpiredTime(long expiredTime, BytesBuffer buffer) {
        buffer.writeVLong(expiredTime);
    }

    protected void parseExpiredTime(BytesBuffer buffer, HugeElement element) {
        element.expiredTime(buffer.readVLong());
    }

    protected byte[] formatEdgeValue(HugeEdge edge) {
        int propsCount = edge.sizeOfProperties();
        BytesBuffer buffer = BytesBuffer.allocate(4 + 16 * propsCount);

        // Write edge id
        //buffer.writeId(edge.id());

        // Write edge properties
        this.formatProperties(edge.getProperties(), buffer);

        // Write edge expired time if needed
        if (edge.hasTtl()) {
            this.formatExpiredTime(edge.expiredTime(), buffer);
        }

        return buffer.bytes();
    }

    protected void parseEdge(BackendColumn col, HugeVertex vertex,
                             HugeGraph graph) {
        // owner-vertex + dir + edge-label + sort-values + other-vertex

        BytesBuffer buffer = BytesBuffer.wrap(col.name);
        if (this.keyWithIdPrefix) {
            // Consume owner-vertex id
            buffer.readId();
        }
        byte type = buffer.read();
        Id labelId = buffer.readId();
        Id subLabelId = buffer.readId();
        String sortValues = buffer.readStringWithEnding();
        Id otherVertexId = buffer.readId();

        boolean direction = EdgeId.isOutDirectionFromCode(type);

        HugeEdge edge;
        if (graph == null) { /* when calculation sinking */
            EdgeLabel edgeLabel = new EdgeLabel(null, subLabelId, UNDEF);
            if (subLabelId != labelId) {
                edgeLabel.edgeLabelType(EdgeLabelType.SUB);
                edgeLabel.fatherId(labelId);
            }
            edge = HugeEdge.constructEdgeWithoutGraph(vertex, direction, edgeLabel,
                                                      sortValues, otherVertexId);
        } else {
            EdgeLabel edgeLabel = graph.edgeLabelOrNone(subLabelId);
            edge = HugeEdge.constructEdge(vertex, direction, edgeLabel,
                                          sortValues, otherVertexId);
        }

        // Parse edge-id + edge-properties
        buffer = BytesBuffer.wrap(col.value);

        //Id id = buffer.readId();

        // Parse edge properties
        this.parseProperties(buffer, edge);

        // Parse edge expired time if needed
        if (edge.hasTtl()) {
            this.parseExpiredTime(buffer, edge);
        }
    }

    protected void parseVertex(byte[] value, HugeVertex vertex) {
        BytesBuffer buffer = BytesBuffer.wrap(value);

        // Parse vertex label
        VertexLabel label = vertex.graph().vertexLabelOrNone(buffer.readId());
        vertex.correctVertexLabel(label);

        // Parse properties
        this.parseProperties(buffer, vertex);

        // Parse vertex expired time if needed
        if (vertex.hasTtl()) {
            this.parseExpiredTime(buffer, vertex);
        }
    }

    protected void parseColumn(BackendColumn col, HugeVertex vertex) {
        BytesBuffer buffer = BytesBuffer.wrap(col.name);
        Id id = this.keyWithIdPrefix ? buffer.readId() : vertex.id();
        E.checkState(buffer.remaining() > 0, "Missing column type");
        byte type = buffer.read();
        // Parse property
        if (type == HugeType.PROPERTY.code()) {
            Id pkeyId = buffer.readId();
            this.parseProperty(pkeyId, BytesBuffer.wrap(col.value), vertex);
        }
        // Parse edge
        else if (type == HugeType.EDGE_IN.code() ||
                 type == HugeType.EDGE_OUT.code()) {
            this.parseEdge(col, vertex, vertex.graph());
        }
        // Parse system property
        else if (type == HugeType.SYS_PROPERTY.code()) {
            // pass
        }
        // Invalid entry
        else {
            E.checkState(false, "Invalid entry(%s) with unknown type(%s): 0x%s",
                         id, type & 0xff, Bytes.toHex(col.name));
        }
    }

    protected byte[] formatIndexName(HugeIndex index) {
        BytesBuffer buffer;
        Id elemId = index.elementId();
        if (!this.indexWithIdPrefix) {
            int idLen = 1 + elemId.length();
            buffer = BytesBuffer.allocate(idLen);
        } else {
            Id indexId = index.id();
            HugeType type = index.type();
            if (!type.isNumericIndex() && indexIdLengthExceedLimit(indexId)) {
                indexId = index.hashId();
            }
            int idLen = 1 + elemId.length() + 1 + indexId.length();
            buffer = BytesBuffer.allocate(idLen);
            // Write index-id
            buffer.writeIndexId(indexId, type);
        }
        // Write element-id
        buffer.writeId(elemId);
        // Write expired time if needed
        if (index.hasTtl()) {
            buffer.writeVLong(index.expiredTime());
        }

        return buffer.bytes();
    }

    protected void parseIndexName(HugeGraph graph, ConditionQuery query,
                                  BinaryBackendEntry entry,
                                  HugeIndex index, Object fieldValues) {
        boolean isVectorIndex = index.type() != HugeType.VECTOR_INDEX_MAP;
        for (BackendColumn col : entry.columns()) {
            if(isVectorIndex && isVectorDleted(col.value) ||
                indexFieldValuesUnmatched(col.value, fieldValues)){
                // Skip if field-values is not matched (just the same hash)
                continue;
            }
            BytesBuffer buffer = BytesBuffer.wrap(col.name);
            if (this.indexWithIdPrefix) {
                buffer.readIndexId(index.type());
            }
            Id elemId = buffer.readId();
            long expiredTime = index.hasTtl() ? buffer.readVLong() : 0L;
            index.elementIds(elemId, expiredTime);
        }
    }

    protected byte[] formatVectorSequenceName(HugeVectorIndexMap vectorIndexMap) {
        BytesBuffer buffer;
        Id sequenceId = vectorIndexMap.sequenceId();
        Object vectorId = vectorIndexMap.fieldValues();

        // len_prefix(1byte) + sequenceId_length + vectorId(int)
        int idLen = 1 + sequenceId.length() + 4;
        buffer = BytesBuffer.allocate(idLen);
        // Write index-id
        buffer.writeId(sequenceId);

        byte[] bytes = number2bytes((Number) vectorId);
        buffer.write(bytes);
        return buffer.bytes();
    }

    protected void parseVectorSequenceName(BinaryBackendEntry entry,
                                           HugeVectorIndexMap vectorIndexMap) {
        for (BackendColumn col : entry.columns()) {
            BytesBuffer buffer = BytesBuffer.wrap(col.name);

            // dirty_prefix(1byte) + index id(4bytes) + sequence(8byte)
            buffer.read(vectorIndexMap.sequenceId().length()+1);
            Object fieldValue = buffer.readInt();
            vectorIndexMap.fieldValues(fieldValue);
        }
    }

    @Override
    public BackendEntry writeVertex(HugeVertex vertex) {
        if (vertex.olap()) {
            return this.writeOlapVertex(vertex);
        }

        BinaryBackendEntry entry = newBackendEntry(vertex);

        if (vertex.removed()) {
            return entry;
        }

        int propsCount = vertex.sizeOfProperties();
        BytesBuffer buffer = BytesBuffer.allocate(8 + 16 * propsCount);

        // Write vertex label
        buffer.writeId(vertex.schemaLabel().id());

        // Write all properties of the vertex
        this.formatProperties(vertex.getProperties(), buffer);

        // Write vertex expired time if needed
        if (vertex.hasTtl()) {
            entry.ttl(vertex.ttl());
            this.formatExpiredTime(vertex.expiredTime(), buffer);
        }

        // Fill column
        byte[] name = this.keyWithIdPrefix ?
                      entry.id().asBytes() : BytesBuffer.BYTES_EMPTY;
        entry.column(name, buffer.bytes());

        return entry;
    }

    @Override
    public BackendEntry writeOlapVertex(HugeVertex vertex) {
        // TODO: adapt to hstore (merge olap table)
        BinaryBackendEntry entry = newBackendEntry(HugeType.OLAP, vertex.id());
        BytesBuffer buffer = BytesBuffer.allocate(8 + 16);

        Collection<HugeProperty<?>> properties = vertex.getProperties();
        if (properties.size() != 1) {
            E.checkArgument(false,
                            "Expect 1 property for olap vertex, but got %s",
                            properties.size());
        }
        HugeProperty<?> property = properties.iterator().next();
        PropertyKey propertyKey = property.propertyKey();
        buffer.writeVInt(SchemaElement.schemaId(propertyKey.id()));
        buffer.writeProperty(propertyKey, property.value());

        // Fill column
        byte[] name = this.keyWithIdPrefix ?
                      entry.id().asBytes() : BytesBuffer.BYTES_EMPTY;
        entry.column(name, buffer.bytes());
        entry.subId(propertyKey.id());
        entry.olap(true);
        return entry;
    }

    @Override
    public BackendEntry writeVertexProperty(HugeVertexProperty<?> prop) {
        throw new NotImplementedException("Unsupported writeVertexProperty()");
    }

    @Override
    public HugeVertex readVertex(HugeGraph graph, BackendEntry bytesEntry) {
        if (bytesEntry == null) {
            return null;
        }
        BinaryBackendEntry entry = this.convertEntry(bytesEntry);

        // Parse id
        Id id = entry.id().origin();
        Id vid = id.edge() ? ((EdgeId) id).ownerVertexId() : id;
        HugeVertex vertex = new HugeVertex(graph, vid, VertexLabel.NONE);

        // Parse all properties and edges of a Vertex
        Iterator<BackendColumn> iterator = entry.columns().iterator();
        for (int index = 0; iterator.hasNext(); index++) {
            BackendColumn col = iterator.next();
            if (entry.type().isEdge()) {
                // NOTE: the entry id type is vertex even if entry type is edge
                // Parse vertex edges
                this.parseColumn(col, vertex);
            } else {
                assert entry.type().isVertex();
                // Parse vertex properties
                assert entry.columnsSize() >= 1 : entry.columnsSize();
                if (index == 0) {
                    this.parseVertex(col.value, vertex);
                } else {
                    this.parseVertexOlap(col.value, vertex);
                }
            }
        }

        return vertex;
    }

    protected void parseVertexOlap(byte[] value, HugeVertex vertex) {
        BytesBuffer buffer = BytesBuffer.wrap(value);
        Id pkeyId = IdGenerator.of(buffer.readVInt());
        this.parseProperty(pkeyId, buffer, vertex);
    }

    @Override
    public BackendEntry writeEdge(HugeEdge edge) {
        BinaryBackendEntry entry = newBackendEntry(edge);
        byte[] name = this.keyWithIdPrefix ?
                      entry.id().asBytes() : BytesBuffer.BYTES_EMPTY;
        byte[] value = this.formatEdgeValue(edge);
        entry.column(name, value);

        if (edge.hasTtl()) {
            entry.ttl(edge.ttl());
        }

        return entry;
    }

    @Override
    public BackendEntry writeEdgeProperty(HugeEdgeProperty<?> prop) {
        // TODO: entry.column(this.formatProperty(prop));
        throw new NotImplementedException("Unsupported writeEdgeProperty()");
    }

    @Override
    public HugeEdge readEdge(HugeGraph graph, BackendEntry bytesEntry) {
        HugeVertex vertex = this.readVertex(graph, bytesEntry);
        Collection<HugeEdge> edges = vertex.getEdges();
        if (edges.size() != 1) {
            E.checkState(false,
                         "Expect 1 edge in vertex, but got %s",
                         edges.size());
        }
        return edges.iterator().next();
    }

    @Override
    public CIter<Edge> readEdges(HugeGraph graph, BackendEntry bytesEntry) {
        BinaryBackendEntry entry = this.convertEntry(bytesEntry);

        // Parse id
        Id id = entry.id().origin();
        Id vid = id.edge() ? ((EdgeId) id).ownerVertexId() : id;
        HugeVertex vertex = new HugeVertex(graph, vid, VertexLabel.NONE);

        // Parse all properties and edges of a Vertex
        Iterator<BackendColumn> iterator = entry.columns().iterator();
        for (int index = 0; iterator.hasNext(); index++) {
            BackendColumn col = iterator.next();
            if (entry.type().isEdge()) {
                // NOTE: the entry id type is vertex even if entry type is edge
                // Parse vertex edges
                this.parseColumn(col, vertex);
            } else {
                assert entry.type().isVertex();
                // Parse vertex properties
                assert entry.columnsSize() >= 1 : entry.columnsSize();
                if (index == 0) {
                    this.parseVertex(col.value, vertex);
                } else {
                    this.parseVertexOlap(col.value, vertex);
                }
            }
        }
        // convert to CIter
        return new MapperIterator<>(vertex.getEdges().iterator(),
                                    (edge) -> edge);
    }

    @Override
    public BackendEntry writeIndex(HugeIndex index) {
        BinaryBackendEntry entry;
        if (index.fieldValues() == null && index.elementIds().isEmpty()) {
            /*
             * When field-values is null and elementIds size is 0, it is
             * meaningful for deletion of index data by index label.
             * TODO: improve
             */
            entry = this.formatILDeletion(index);
        } else {
            Id id = index.id();
            HugeType type = index.type();
            byte[] value = null;
            if (!type.isNumericIndex() && indexIdLengthExceedLimit(id)) {
                id = index.hashId();
                // Save field-values as column value if the key is a hash string
                value = StringEncoding.encode(index.fieldValues().toString());
            } else if (index instanceof HugeVectorIndexMap) {
                HugeVectorIndexMap indexMap = (HugeVectorIndexMap) index;
                BytesBuffer buffer = BytesBuffer.allocate(9);
                buffer.writeLong(indexMap.sequence());
                buffer.write((byte) indexMap.vectorState().code());
                value = buffer.bytes();
            }
            // 添加type等于vector 的writeindex
            entry = newBackendEntry(type, id);
            if (index.indexLabel().olap()) {
                entry.olap(true);
            }
            entry.column(this.formatIndexName(index), value);
            entry.subId(index.elementId());

            if (index.hasTtl()) {
                entry.ttl(index.ttl());
            }
        }
        return entry;
    }

    @Override
    public HugeIndex readIndex(HugeGraph graph, ConditionQuery query,
                               BackendEntry bytesEntry) {
        if (bytesEntry == null) {
            return null;
        }

        BinaryBackendEntry entry = this.convertEntry(bytesEntry);
        // NOTE: index id without length prefix
        byte[] bytes = entry.id().asBytes();
        HugeIndex index = HugeIndex.parseIndexId(graph, entry.type(), bytes);

        Object fieldValues = null;
        if (!index.type().isRangeIndex()) {
            fieldValues = query.condition(HugeKeys.FIELD_VALUES);
            if (!index.fieldValues().equals(fieldValues)) {
                // Update field-values for hashed or encoded index-id
                index.fieldValues(fieldValues);
            }
        }

        this.parseIndexName(graph, query, entry, index, fieldValues);
        return index;
    }

    @Override
    public BackendEntry writeVectorSequence(HugeVectorIndexMap indexMap) {
        BinaryBackendEntry entry;
        Id id = indexMap.sequenceId();
        entry = newBackendEntry(HugeType.VECTOR_SEQUENCE, id);
        byte[] value = null;
        entry.column(formatVectorSequenceName(indexMap), value);
        return entry;
    }

    @Override
    public HugeVectorIndexMap readVectorSequence(HugeGraph graph, ConditionQuery query,
                                  BackendEntry bytesEntry) {
        if (bytesEntry == null) {
            return null;
        }
        BinaryBackendEntry entry = this.convertEntry(bytesEntry);
        byte[] bytes = entry.originId().asBytes();
        HugeVectorIndexMap indexMap = HugeVectorIndexMap.parseSequenceId(graph, bytes);

        this.parseVectorSequenceName(entry, indexMap);
        return indexMap;
    }

    @Override
    public BackendEntry writeId(HugeType type, Id id) {
        return newBackendEntry(type, id);
    }

    @Override
    protected Id writeQueryId(HugeType type, Id id) {
        if (type.isEdge()) {
            id = writeEdgeId(id);
        } else if (type.isVertex()) {
            BytesBuffer buffer = BytesBuffer.allocate(2 + 1 + id.length());
            writePartitionedId(HugeType.VERTEX, id, buffer);
            id = new BinaryId(buffer.bytes(), id);
        } else {
            BytesBuffer buffer = BytesBuffer.allocate(1 + id.length());
            id = new BinaryId(buffer.writeId(id).bytes(), id);
        }
        return id;
    }

    @Override
    protected Query writeQueryEdgeCondition(Query query) {
        ConditionQuery cq = (ConditionQuery) query;
        if (cq.hasRangeCondition()) {
            return this.writeQueryEdgeRangeCondition(cq);
        } else {
            return this.writeQueryEdgePrefixCondition(cq);
        }
    }

    private Query writeQueryEdgeRangeCondition(ConditionQuery cq) {
        List<Condition> sortValues = cq.syspropConditions(HugeKeys.SORT_VALUES);
        E.checkArgument(sortValues.size() >= 1 && sortValues.size() <= 2,
                        "Edge range query must be with sort-values range");
        // Would ignore target vertex
        Id vertex = cq.condition(HugeKeys.OWNER_VERTEX);
        Directions direction = cq.condition(HugeKeys.DIRECTION);
        if (direction == null) {
            direction = Directions.OUT;
        }
        Id label = cq.condition(HugeKeys.LABEL);

        BytesBuffer start = BytesBuffer.allocate(BytesBuffer.BUF_EDGE_ID);
        writePartitionedId(HugeType.EDGE, vertex, start);
        start.write(direction.type().code());
        start.writeId(label);

        Id subLabel = cq.condition(HugeKeys.SUB_LABEL);
        if (subLabel != null) {
            start.writeId(subLabel);
        }

        BytesBuffer end = BytesBuffer.allocate(BytesBuffer.BUF_EDGE_ID);
        end.copyFrom(start);

        RangeConditions range = new RangeConditions(sortValues);
        if (range.keyMin() != null) {
            start.writeStringRaw((String) range.keyMin());
        }
        if (range.keyMax() != null) {
            end.writeStringRaw((String) range.keyMax());
        }
        // Sort-value will be empty if there is no start sort-value
        Id startId = new BinaryId(start.bytes(), null);
        // Set endId as prefix if there is no end sort-value
        Id endId = new BinaryId(end.bytes(), null);

        boolean includeStart = range.keyMinEq();
        if (cq.paging() && !cq.page().isEmpty()) {
            includeStart = true;
            byte[] position = PageState.fromString(cq.page()).position();
            // FIXME: Due to the inconsistency in the definition of `position` of RocksDB
            //  scan iterator and Hstore, temporarily remove the following check.
            // E.checkArgument(Bytes.compare(position, startId.asBytes()) >= 0,
            //                 "Invalid page out of lower bound");
            startId = new BinaryId(position, null);
        }
        if (range.keyMax() == null) {
            return new IdPrefixQuery(cq, startId, includeStart, endId);
        }
        return new IdRangeQuery(cq, startId, includeStart, endId,
                                range.keyMaxEq());
    }

    private Query writeQueryEdgePrefixCondition(ConditionQuery cq) {
        int count = 0;
        BytesBuffer buffer = BytesBuffer.allocate(BytesBuffer.BUF_EDGE_ID);
        for (HugeKeys key : EdgeId.KEYS) {
            Object value = cq.condition(key);

            if (value != null) {
                count++;
            } else {
                if (key == HugeKeys.DIRECTION) {
                    // Direction is null, set to OUT
                    value = Directions.OUT;
                } else {
                    break;
                }
            }

            if (key == HugeKeys.OWNER_VERTEX ||
                key == HugeKeys.OTHER_VERTEX) {
                writePartitionedId(HugeType.EDGE, (Id) value, buffer);
            } else if (key == HugeKeys.DIRECTION) {
                byte t = ((Directions) value).type().code();
                buffer.write(t);
            } else if (key == HugeKeys.LABEL) {
                assert value instanceof Id;
                buffer.writeId((Id) value);
            } else if (key == HugeKeys.SUB_LABEL) {
                assert value instanceof Id;
                buffer.writeId((Id) value);
            } else if (key == HugeKeys.SORT_VALUES) {
                assert value instanceof String;
                buffer.writeStringWithEnding((String) value);
            } else {
                assert false : key;
            }
        }

        if (count > 0) {
            assert count == cq.conditionsSize();
            return prefixQuery(cq, new BinaryId(buffer.bytes(), null));
        }

        return null;
    }

    @Override
    protected Query writeQueryCondition(Query query) {
        HugeType type = query.resultType();
        if (!type.isIndex()) {
            return query;
        }

        ConditionQuery cq = (ConditionQuery) query;

        if (type.isNumericIndex()) {
            // Convert range-index/shard-index query to id range query
            return this.writeRangeIndexQuery(cq);
        } else {
            assert type.isSearchIndex() || type.isSecondaryIndex() ||
                   type.isUniqueIndex();
            // Convert secondary-index or search-index query to id query
            return this.writeStringIndexQuery(cq);
        }
    }

    private Query writeStringIndexQuery(ConditionQuery query) {
        E.checkArgument(query.allSysprop() &&
                        query.conditionsSize() == 2,
                        "There should be two conditions: " +
                        "INDEX_LABEL_ID and FIELD_VALUES" +
                        "in secondary index query");

        Id index = query.condition(HugeKeys.INDEX_LABEL_ID);
        Object key = query.condition(HugeKeys.FIELD_VALUES);

        E.checkArgument(index != null, "Please specify the index label");
        E.checkArgument(key != null, "Please specify the index key");

        Id prefix = formatIndexId(query.resultType(), index, key, true);
        return prefixQuery(query, prefix);
    }

    private Query writeRangeIndexQuery(ConditionQuery query) {
        Id index = query.condition(HugeKeys.INDEX_LABEL_ID);
        E.checkArgument(index != null, "Please specify the index label");

        List<Condition> fields = query.syspropConditions(HugeKeys.FIELD_VALUES);
        E.checkArgument(!fields.isEmpty(),
                        "Please specify the index field values");

        HugeType type = query.resultType();
        Id start = null;
        if (query.paging() && !query.page().isEmpty()) {
            byte[] position = PageState.fromString(query.page()).position();
            start = new BinaryId(position, null);
        }

        RangeConditions range = new RangeConditions(fields);
        if (range.keyEq() != null) {
            Id id = formatIndexId(type, index, range.keyEq(), true);
            if (start == null) {
                return new IdPrefixQuery(query, id);
            }
            // FIXME: Due to the inconsistency in the definition of `position` of RocksDB
            //  scan iterator and Hstore, temporarily remove the following check.
            // E.checkArgument(Bytes.compare(start.asBytes(), id.asBytes()) >= 0,
            //                 "Invalid page out of lower bound");
            return new IdPrefixQuery(query, start, id);
        }

        Object keyMin = range.keyMin();
        Object keyMax = range.keyMax();
        boolean keyMinEq = range.keyMinEq();
        boolean keyMaxEq = range.keyMaxEq();
        if (keyMin == null) {
            E.checkArgument(keyMax != null,
                            "Please specify at least one condition");
            // Set keyMin to min value
            keyMin = NumericUtil.minValueOf(keyMax.getClass());
            keyMinEq = true;
        }

        Id min = formatIndexId(type, index, keyMin, false);
        if (!keyMinEq) {
            /*
             * Increase 1 to keyMin, index GT query is a scan with GT prefix,
             * inclusiveStart=false will also match index started with keyMin
             */
            increaseOne(min.asBytes());
            keyMinEq = true;
        }

        if (start == null) {
            start = min;
        } else {
            // FIXME: Due to the inconsistency in the definition of `position` of RocksDB
            //  scan iterator and Hstore, temporarily remove the following check.
            // E.checkArgument(Bytes.compare(start.asBytes(), min.asBytes()) >= 0,
            //                 "Invalid page out of lower bound");
        }

        if (keyMax == null) {
            keyMax = NumericUtil.maxValueOf(keyMin.getClass());
            keyMaxEq = true;
        }
        Id max = formatIndexId(type, index, keyMax, false);
        if (keyMaxEq) {
            keyMaxEq = false;
            increaseOne(max.asBytes());
        }
        return new IdRangeQuery(query, start, keyMinEq, max, keyMaxEq);
    }

    private BinaryBackendEntry formatILDeletion(HugeIndex index) {
        Id id = index.indexLabelId();
        BinaryId bid = new BinaryId(id.asBytes(), id);
        BinaryBackendEntry entry = new BinaryBackendEntry(index.type(), bid);
        if (index.type().isStringIndex()) {
            byte[] idBytes = IdGenerator.of(id.asString()).asBytes();
            BytesBuffer buffer = BytesBuffer.allocate(idBytes.length);
            buffer.write(idBytes);
            entry.column(buffer.bytes(), null);
        } else {
            assert index.type().isRangeIndex();
            BytesBuffer buffer = BytesBuffer.allocate(4);
            buffer.writeInt((int) id.asLong());
            entry.column(buffer.bytes(), null);
        }
        return entry;
    }

    private BinaryId writeEdgeId(Id id) {
        EdgeId edgeId;
        if (id instanceof EdgeId) {
            edgeId = (EdgeId) id;
        } else {
            edgeId = EdgeId.parse(id.asString());
        }
        BytesBuffer buffer = BytesBuffer.allocate(BytesBuffer.BUF_EDGE_ID);
        if (this.enablePartition) {
            buffer.writeShort(getPartition(HugeType.EDGE, edgeId.ownerVertexId()));
            buffer.writeEdgeId(edgeId);
        } else {
            buffer.writeEdgeId(edgeId);
        }
        return new BinaryId(buffer.bytes(), id);
    }

    private void writePartitionedId(HugeType type, Id id, BytesBuffer buffer) {
        if (this.enablePartition) {
            buffer.writeShort(getPartition(type, id));
            buffer.writeId(id);
        } else {
            buffer.writeId(id);
        }
    }

    protected short getPartition(HugeType type, Id id) {
        return 0;
    }

    public BackendEntry parse(BackendEntry originEntry) {
        byte[] bytes = originEntry.id().asBytes();
        BinaryBackendEntry parsedEntry = new BinaryBackendEntry(originEntry.type(),
                                                                bytes,
                                                                this.enablePartition);
        if (this.enablePartition) {
            bytes = Arrays.copyOfRange(bytes, parsedEntry.id().length() + 2, bytes.length);
        } else {
            bytes = Arrays.copyOfRange(bytes, parsedEntry.id().length(), bytes.length);
        }
        BytesBuffer buffer = BytesBuffer.allocate(BytesBuffer.BUF_EDGE_ID);
        buffer.write(parsedEntry.id().asBytes());
        buffer.write(bytes);
        parsedEntry = new BinaryBackendEntry(originEntry.type(),
                                             new BinaryId(buffer.bytes(),
                                                          BytesBuffer.wrap(buffer.bytes())
                                                                     .readEdgeId()));

        for (BackendColumn col : originEntry.columns()) {
            parsedEntry.column(buffer.bytes(), col.value);
        }
        return parsedEntry;
    }

    private static Query prefixQuery(ConditionQuery query, Id prefix) {
        Query newQuery;
        if (query.paging() && !query.page().isEmpty()) {
            /*
             * If used paging and the page number is not empty, deserialize
             * the page to id and use it as the starting row for this query
             */
            byte[] position = PageState.fromString(query.page()).position();
            // FIXME: Due to the inconsistency in the definition of `position` of RocksDB
            //  scan iterator and Hstore, temporarily remove the following check.
            // E.checkArgument(Bytes.compare(position, prefix.asBytes()) >= 0,
            //                "Invalid page out of lower bound");
            BinaryId start = new BinaryId(position, null);
            newQuery = new IdPrefixQuery(query, start, prefix);
        } else {
            newQuery = new IdPrefixQuery(query, prefix);
        }
        return newQuery;
    }

    protected static BinaryId formatIndexId(HugeType type, Id indexLabel,
                                            Object fieldValues,
                                            boolean equal) {
        boolean withEnding = type.isRangeIndex() || equal;
        Id id = HugeIndex.formatIndexId(type, indexLabel, fieldValues);
        if (!type.isNumericIndex() && indexIdLengthExceedLimit(id)) {
            id = HugeIndex.formatIndexHashId(type, indexLabel, fieldValues);
        }
        BytesBuffer buffer = BytesBuffer.allocate(1 + id.length());
        byte[] idBytes = buffer.writeIndexId(id, type, withEnding).bytes();
        return new BinaryId(idBytes, id);
    }

    protected static boolean indexIdLengthExceedLimit(Id id) {
        return id.asBytes().length > BytesBuffer.INDEX_HASH_ID_THRESHOLD;
    }

    protected static boolean indexFieldValuesUnmatched(byte[] value,
                                                       Object fieldValues) {
        if (value != null && value.length > 0 && fieldValues != null) {
            return !StringEncoding.decode(value).equals(fieldValues);
        }
        return false;
    }

    protected static boolean isVectorDleted(byte[] value) {
        BytesBuffer buffer = BytesBuffer.wrap(value);
        buffer.readLong();
        return IndexVectorState.DELETING.code() == buffer.read();
    }

    public static void increaseOne(byte[] bytes) {
        final byte BYTE_MAX_VALUE = (byte) 0xff;
        final byte INCREASE_STEP = 0x01;
        assert bytes.length > 0;
        byte last = bytes[bytes.length - 1];
        if (last != BYTE_MAX_VALUE) {
            bytes[bytes.length - 1] += INCREASE_STEP;
        } else {
            // Process overflow (like [1, 255] => [2, 0])
            int i = bytes.length - 1;
            for (; i > 0 && bytes[i] == BYTE_MAX_VALUE; --i) {
                bytes[i] += INCREASE_STEP;
            }
            if (bytes[i] == BYTE_MAX_VALUE) {
                assert i == 0;
                throw new BackendException("Unable to increase bytes: %s",
                                           Bytes.toHex(bytes));
            }
            bytes[i] += INCREASE_STEP;
        }
    }

    @Override
    public BackendEntry writeVertexLabel(VertexLabel vertexLabel) {
        SchemaSerializer serializer = new SchemaSerializer();
        return serializer.writeVertexLabel(vertexLabel);
    }

    @Override
    public VertexLabel readVertexLabel(HugeGraph graph,
                                       BackendEntry backendEntry) {
        if (backendEntry == null) {
            return null;
        }
        BinaryBackendEntry entry = this.convertEntry(backendEntry);

        SchemaSerializer serializer = new SchemaSerializer();
        return serializer.readVertexLabel(graph, entry);
    }

    @Override
    public BackendEntry writeEdgeLabel(EdgeLabel edgeLabel) {
        SchemaSerializer serializer = new SchemaSerializer();
        return serializer.writeEdgeLabel(edgeLabel);
    }

    @Override
    public EdgeLabel readEdgeLabel(HugeGraph graph, BackendEntry backendEntry) {
        if (backendEntry == null) {
            return null;
        }
        BinaryBackendEntry entry = this.convertEntry(backendEntry);

        SchemaSerializer serializer = new SchemaSerializer();
        return serializer.readEdgeLabel(graph, entry);
    }

    @Override
    public BackendEntry writePropertyKey(PropertyKey propertyKey) {
        SchemaSerializer serializer = new SchemaSerializer();
        return serializer.writePropertyKey(propertyKey);
    }

    @Override
    public PropertyKey readPropertyKey(HugeGraph graph,
                                       BackendEntry backendEntry) {
        if (backendEntry == null) {
            return null;
        }
        BinaryBackendEntry entry = this.convertEntry(backendEntry);

        SchemaSerializer serializer = new SchemaSerializer();
        return serializer.readPropertyKey(graph, entry);
    }

    @Override
    public BackendEntry writeIndexLabel(IndexLabel indexLabel) {
        SchemaSerializer serializer = new SchemaSerializer();
        return serializer.writeIndexLabel(indexLabel);
    }

    @Override
    public IndexLabel readIndexLabel(HugeGraph graph,
                                     BackendEntry backendEntry) {
        if (backendEntry == null) {
            return null;
        }
        BinaryBackendEntry entry = this.convertEntry(backendEntry);

        SchemaSerializer serializer = new SchemaSerializer();
        return serializer.readIndexLabel(graph, entry);
    }

    private final class SchemaSerializer {

        private BinaryBackendEntry entry;

        public BinaryBackendEntry writeVertexLabel(VertexLabel schema) {
            this.entry = newBackendEntry(schema);
            writeString(HugeKeys.NAME, schema.name());
            writeEnum(HugeKeys.ID_STRATEGY, schema.idStrategy());
            writeIds(HugeKeys.PROPERTIES, schema.properties());
            writeIds(HugeKeys.PRIMARY_KEYS, schema.primaryKeys());
            writeIds(HugeKeys.NULLABLE_KEYS, schema.nullableKeys());
            writeIds(HugeKeys.INDEX_LABELS, schema.indexLabels());
            writeBool(HugeKeys.ENABLE_LABEL_INDEX, schema.enableLabelIndex());
            writeEnum(HugeKeys.STATUS, schema.status());
            writeLong(HugeKeys.TTL, schema.ttl());
            writeId(HugeKeys.TTL_START_TIME, schema.ttlStartTime());
            writeUserdata(schema);
            return this.entry;
        }

        public VertexLabel readVertexLabel(HugeGraph graph,
                                           BinaryBackendEntry entry) {
            E.checkNotNull(entry, "entry");
            this.entry = entry;
            Id id = entry.id().origin();
            String name = readString(HugeKeys.NAME);

            VertexLabel vertexLabel = new VertexLabel(graph, id, name);
            vertexLabel.idStrategy(readEnum(HugeKeys.ID_STRATEGY,
                                            IdStrategy.class));
            vertexLabel.properties(readIds(HugeKeys.PROPERTIES));
            vertexLabel.primaryKeys(readIds(HugeKeys.PRIMARY_KEYS));
            vertexLabel.nullableKeys(readIds(HugeKeys.NULLABLE_KEYS));
            vertexLabel.addIndexLabels(readIds(HugeKeys.INDEX_LABELS));
            vertexLabel.enableLabelIndex(readBool(HugeKeys.ENABLE_LABEL_INDEX));
            vertexLabel.status(readEnum(HugeKeys.STATUS, SchemaStatus.class));
            vertexLabel.ttl(readLong(HugeKeys.TTL));
            vertexLabel.ttlStartTime(readId(HugeKeys.TTL_START_TIME));
            readUserdata(vertexLabel);
            return vertexLabel;
        }

        public BinaryBackendEntry writeEdgeLabel(EdgeLabel schema) {
            this.entry = newBackendEntry(schema);
            writeString(HugeKeys.NAME, schema.name());
            writeIds(HugeKeys.LINKS, schema.linksIds());
            writeEnum(HugeKeys.FREQUENCY, schema.frequency());
            writeIds(HugeKeys.PROPERTIES, schema.properties());
            writeIds(HugeKeys.SORT_KEYS, schema.sortKeys());
            writeIds(HugeKeys.NULLABLE_KEYS, schema.nullableKeys());
            writeIds(HugeKeys.INDEX_LABELS, schema.indexLabels());
            writeBool(HugeKeys.ENABLE_LABEL_INDEX, schema.enableLabelIndex());
            writeEnum(HugeKeys.STATUS, schema.status());
            writeLong(HugeKeys.TTL, schema.ttl());
            writeId(HugeKeys.TTL_START_TIME, schema.ttlStartTime());
            writeUserdata(schema);
            return this.entry;
        }

        public EdgeLabel readEdgeLabel(HugeGraph graph,
                                       BinaryBackendEntry entry) {
            E.checkNotNull(entry, "entry");
            this.entry = entry;
            Id id = entry.id().origin();
            String name = readString(HugeKeys.NAME);

            EdgeLabel edgeLabel = new EdgeLabel(graph, id, name);
            edgeLabel.linksIds(readIds(HugeKeys.LINKS));
            edgeLabel.frequency(readEnum(HugeKeys.FREQUENCY, Frequency.class));
            edgeLabel.properties(readIds(HugeKeys.PROPERTIES));
            edgeLabel.sortKeys(readIds(HugeKeys.SORT_KEYS));
            edgeLabel.nullableKeys(readIds(HugeKeys.NULLABLE_KEYS));
            edgeLabel.addIndexLabels(readIds(HugeKeys.INDEX_LABELS));
            edgeLabel.enableLabelIndex(readBool(HugeKeys.ENABLE_LABEL_INDEX));
            edgeLabel.status(readEnum(HugeKeys.STATUS, SchemaStatus.class));
            edgeLabel.ttl(readLong(HugeKeys.TTL));
            edgeLabel.ttlStartTime(readId(HugeKeys.TTL_START_TIME));
            readUserdata(edgeLabel);
            return edgeLabel;
        }

        public BinaryBackendEntry writePropertyKey(PropertyKey schema) {
            this.entry = newBackendEntry(schema);
            writeString(HugeKeys.NAME, schema.name());
            writeEnum(HugeKeys.DATA_TYPE, schema.dataType());
            writeEnum(HugeKeys.CARDINALITY, schema.cardinality());
            writeEnum(HugeKeys.AGGREGATE_TYPE, schema.aggregateType());
            writeEnum(HugeKeys.WRITE_TYPE, schema.writeType());
            writeIds(HugeKeys.PROPERTIES, schema.properties());
            writeEnum(HugeKeys.STATUS, schema.status());
            writeUserdata(schema);
            return this.entry;
        }

        public PropertyKey readPropertyKey(HugeGraph graph,
                                           BinaryBackendEntry entry) {
            E.checkNotNull(entry, "entry");
            this.entry = entry;
            Id id = entry.id().origin();
            String name = readString(HugeKeys.NAME);

            PropertyKey propertyKey = new PropertyKey(graph, id, name);
            propertyKey.dataType(readEnum(HugeKeys.DATA_TYPE, DataType.class));
            propertyKey.cardinality(readEnum(HugeKeys.CARDINALITY,
                                             Cardinality.class));
            propertyKey.aggregateType(readEnum(HugeKeys.AGGREGATE_TYPE,
                                               AggregateType.class));
            propertyKey.writeType(readEnumOrDefault(HugeKeys.WRITE_TYPE,
                                                    WriteType.class,
                                                    WriteType.OLTP));
            propertyKey.properties(readIds(HugeKeys.PROPERTIES));
            propertyKey.status(readEnum(HugeKeys.STATUS, SchemaStatus.class));
            readUserdata(propertyKey);
            return propertyKey;
        }

        public BinaryBackendEntry writeIndexLabel(IndexLabel schema) {
            this.entry = newBackendEntry(schema);
            writeString(HugeKeys.NAME, schema.name());
            writeEnum(HugeKeys.BASE_TYPE, schema.baseType());
            writeId(HugeKeys.BASE_VALUE, schema.baseValue());
            writeEnum(HugeKeys.INDEX_TYPE, schema.indexType());
            writeIds(HugeKeys.FIELDS, schema.indexFields());
            writeEnum(HugeKeys.STATUS, schema.status());
            writeUserdata(schema);
            return this.entry;
        }

        public IndexLabel readIndexLabel(HugeGraph graph,
                                         BinaryBackendEntry entry) {
            E.checkNotNull(entry, "entry");
            this.entry = entry;
            Id id = entry.id().origin();
            String name = readString(HugeKeys.NAME);

            IndexLabel indexLabel = new IndexLabel(graph, id, name);
            indexLabel.baseType(readEnum(HugeKeys.BASE_TYPE, HugeType.class));
            indexLabel.baseValue(readId(HugeKeys.BASE_VALUE));
            indexLabel.indexType(readEnum(HugeKeys.INDEX_TYPE,
                                          IndexType.class));
            indexLabel.indexFields(readIds(HugeKeys.FIELDS));
            indexLabel.status(readEnum(HugeKeys.STATUS, SchemaStatus.class));
            readUserdata(indexLabel);
            return indexLabel;
        }

        private void writeUserdata(SchemaElement schema) {
            String userdataStr = JsonUtil.toJson(schema.userdata());
            writeString(HugeKeys.USER_DATA, userdataStr);
        }

        private void readUserdata(SchemaElement schema) {
            // Parse all user data of a schema element
            byte[] userdataBytes = column(HugeKeys.USER_DATA);
            String userdataStr = StringEncoding.decode(userdataBytes);
            @SuppressWarnings("unchecked")
            Map<String, Object> userdata = JsonUtil.fromJson(userdataStr,
                                                             Map.class);
            for (Map.Entry<String, Object> e : userdata.entrySet()) {
                schema.userdata(e.getKey(), e.getValue());
            }
        }

        private void writeString(HugeKeys key, String value) {
            this.entry.column(formatColumnName(key),
                              StringEncoding.encode(value));
        }

        private String readString(HugeKeys key) {
            return StringEncoding.decode(column(key));
        }

        private void writeEnum(HugeKeys key, SerialEnum value) {
            this.entry.column(formatColumnName(key), new byte[]{value.code()});
        }

        private <T extends SerialEnum> T readEnum(HugeKeys key,
                                                  Class<T> clazz) {
            byte[] value = column(key);
            E.checkState(value.length == 1,
                         "The length of column '%s' must be 1, but is '%s'",
                         key, value.length);
            return SerialEnum.fromCode(clazz, value[0]);
        }

        private <T extends SerialEnum> T readEnumOrDefault(HugeKeys key,
                                                           Class<T> clazz,
                                                           T defaultValue) {
            BackendColumn column = this.entry.column(formatColumnName(key));
            if (column == null) {
                return defaultValue;
            }
            E.checkNotNull(column.value, "column.value");
            return SerialEnum.fromCode(clazz, column.value[0]);
        }

        private void writeLong(HugeKeys key, long value) {
            @SuppressWarnings("resource")
            BytesBuffer buffer = new BytesBuffer(8);
            buffer.writeVLong(value);
            this.entry.column(formatColumnName(key), buffer.bytes());
        }

        private long readLong(HugeKeys key) {
            byte[] value = column(key);
            BytesBuffer buffer = BytesBuffer.wrap(value);
            return buffer.readVLong();
        }

        private void writeId(HugeKeys key, Id value) {
            this.entry.column(formatColumnName(key), writeId(value));
        }

        private Id readId(HugeKeys key) {
            return readId(column(key));
        }

        private void writeIds(HugeKeys key, Collection<Id> value) {
            this.entry.column(formatColumnName(key), writeIds(value));
        }

        private Id[] readIds(HugeKeys key) {
            return readIds(column(key));
        }

        private void writeBool(HugeKeys key, boolean value) {
            this.entry.column(formatColumnName(key),
                              new byte[]{(byte) (value ? 1 : 0)});
        }

        private boolean readBool(HugeKeys key) {
            byte[] value = column(key);
            E.checkState(value.length == 1,
                         "The length of column '%s' must be 1, but is '%s'",
                         key, value.length);
            return value[0] != (byte) 0;
        }

        private byte[] writeId(Id id) {
            int size = 1 + id.length();
            BytesBuffer buffer = BytesBuffer.allocate(size);
            buffer.writeId(id);
            return buffer.bytes();
        }

        private Id readId(byte[] value) {
            BytesBuffer buffer = BytesBuffer.wrap(value);
            return buffer.readId();
        }

        private byte[] writeIds(Collection<Id> ids) {
            E.checkState(ids.size() <= BytesBuffer.MAX_PROPERTIES,
                         "The number of properties of vertex/edge label " +
                         "can't exceed '%s'", BytesBuffer.MAX_PROPERTIES);
            int size = 2;
            for (Id id : ids) {
                size += (1 + id.length());
            }
            BytesBuffer buffer = BytesBuffer.allocate(size);
            buffer.writeUInt16(ids.size());
            for (Id id : ids) {
                buffer.writeId(id);
            }
            return buffer.bytes();
        }

        private Id[] readIds(byte[] value) {
            BytesBuffer buffer = BytesBuffer.wrap(value);
            int size = buffer.readUInt16();
            Id[] ids = new Id[size];
            for (int i = 0; i < size; i++) {
                Id id = buffer.readId();
                ids[i] = id;
            }
            return ids;
        }

        private byte[] column(HugeKeys key) {
            BackendColumn column = this.entry.column(formatColumnName(key));
            E.checkState(column != null, "Not found key '%s' from entry %s",
                         key, this.entry);
            E.checkNotNull(column.value, "column.value");
            return column.value;
        }

        private byte[] formatColumnName(HugeKeys key) {
            Id id = this.entry.id().origin();
            int size = 1 + id.length() + 1;
            BytesBuffer buffer = BytesBuffer.allocate(size);
            buffer.writeId(id);
            buffer.write(key.code());
            return buffer.bytes();
        }
    }
}
