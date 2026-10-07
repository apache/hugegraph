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

package org.apache.hugegraph.serializer;

import static org.apache.hugegraph.struct.schema.SchemaElement.UNDEF;

import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.function.BiConsumer;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.NotImplementedException;
import org.apache.hugegraph.HugeGraphSupplier;
import org.apache.hugegraph.backend.BackendColumn;
import org.apache.hugegraph.backend.BinaryId;
import org.apache.hugegraph.exception.BackendException;
import org.apache.hugegraph.id.EdgeId;
import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.id.IdGenerator;
import org.apache.hugegraph.struct.schema.EdgeLabel;
import org.apache.hugegraph.struct.schema.IndexLabel;
import org.apache.hugegraph.struct.schema.PropertyKey;
import org.apache.hugegraph.struct.schema.SchemaElement;
import org.apache.hugegraph.struct.schema.VertexLabel;
import org.apache.hugegraph.structure.BaseEdge;
import org.apache.hugegraph.structure.BaseElement;
import org.apache.hugegraph.structure.BaseProperty;
import org.apache.hugegraph.structure.BaseVertex;
import org.apache.hugegraph.structure.BaseVertex.TypeContext;
import org.apache.hugegraph.structure.Index;
import org.apache.hugegraph.serializer.BytesBuffer.IndexColumnName;
import org.apache.hugegraph.serializer.BytesBuffer.IndexExpiryLayout;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.type.define.Cardinality;
import org.apache.hugegraph.type.define.EdgeLabelType;
import org.apache.hugegraph.type.define.Directions;
import org.apache.hugegraph.util.Bytes;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Log;
import org.apache.hugegraph.util.StringEncoding;
import org.slf4j.Logger;

import com.google.common.primitives.Longs;

public class BinaryElementSerializer {
    static final BinaryElementSerializer INSTANCE =
            new BinaryElementSerializer();
    static Logger log = Log.logger(BinaryElementSerializer.class);

    public static BinaryElementSerializer getInstance() {
        return INSTANCE;
    }

    /**
     * Calculate owner ID of vertex/edge
     *
     * @param element
     * @return
     */
    public static Id ownerId(BaseElement element) {
        if (element instanceof BaseVertex) {
            return element.id();
        } else if (element instanceof BaseEdge) {
            return ((EdgeId) element.id()).ownerVertexId();
        } else {
            throw new IllegalArgumentException("Only support get ownerid" +
                    " of BaseVertex or BaseEdge");
        }
    }

    /**
     * Calculate owner ID of index
     *
     * @param index
     * @return
     */
    public static Id ownerId(Index index) {
        Id elementId = index.elementId();

        Id ownerId = null;
        if (elementId instanceof EdgeId) {
            // Edge ID
            ownerId = ((EdgeId) elementId).ownerVertexId();
        } else {
            // OLAP index
            // Normal vertex index
            // Normal secondary index
            // Vertex/Edge LabelIndex
            ownerId = elementId;
        }

        return ownerId;
    }


    protected void parseProperty(HugeGraphSupplier graph, Id pkeyId,
                                 BytesBuffer buffer, BaseElement owner) {
        this.parseProperty(graph, pkeyId, buffer, true, owner::addProperty);
    }

    public void parseSchemaProperty(HugeGraphSupplier graph, Id pkeyId,
                                    BytesBuffer buffer,
                                    BiConsumer<PropertyKey, Object> propertySink) {
        E.checkArgumentNotNull(graph, "Schema property decoding requires a schema supplier");
        this.parseProperty(graph, pkeyId, buffer, false, propertySink);
    }

    private void parseProperty(HugeGraphSupplier graph, Id pkeyId,
                               BytesBuffer buffer, boolean tagged,
                               BiConsumer<PropertyKey, Object> propertySink) {
        PropertyKey pkey = graph != null ? graph.propertyKey(pkeyId) :
                           new PropertyKey(null, pkeyId, "");
        Object value = tagged ? buffer.readProperty(pkey) : buffer.readSchemaProperty(pkey);
        if (pkey.cardinality() != Cardinality.SINGLE && !(value instanceof Collection)) {
            throw new BackendException("Invalid value of non-single property: %s", value);
        }
        propertySink.accept(pkey, value);
    }

    public void parseProperties(HugeGraphSupplier graph, BytesBuffer buffer,
                                BaseElement owner) {
        this.parseProperties(graph, buffer, true, owner::addProperty);
    }

    public void parseSchemaProperties(HugeGraphSupplier graph, BytesBuffer buffer,
                                      BiConsumer<PropertyKey, Object> propertySink) {
        E.checkArgumentNotNull(graph, "Schema property decoding requires a schema supplier");
        this.parseProperties(graph, buffer, false, propertySink);
    }

    private void parseProperties(HugeGraphSupplier graph, BytesBuffer buffer,
                                 boolean tagged, BiConsumer<PropertyKey, Object> propertySink) {
        int size = buffer.readVInt();
        assert size >= 0;
        for (int i = 0; i < size; i++) {
            this.parsePropertyRecord(graph, buffer, tagged, propertySink);
        }
    }

    /**
     * Deserialize vertex KV data into BaseVertex type vertex
     *
     * @param vertexCol Must be vertex data column
     * @param vertex    When vertex==null, used for operator sinking, deserialize col data into BaseVertex;
     *                  When vertex!=null, add col information to vertex
     */
    public BaseVertex parseVertex(HugeGraphSupplier graph, BackendColumn vertexCol,
                                  BaseVertex vertex) {
        return this.parseVertex(graph, vertexCol, vertex, true, TypeContext.STORAGE);
    }

    /** Decode a persisted Server row using schema-supplied property metadata. */
    public BaseVertex parseSchemaVertex(HugeGraphSupplier graph, BackendColumn vertexCol,
                                        BaseVertex vertex) {
        return this.parseSchemaVertex(graph, vertexCol, vertex, TypeContext.STORAGE);
    }

    public BaseVertex parseSchemaVertex(HugeGraphSupplier graph, BackendColumn vertexCol,
                                        BaseVertex vertex, TypeContext typeContext) {
        E.checkArgumentNotNull(graph, "Schema vertex decoding requires a schema supplier");
        return this.parseVertex(graph, vertexCol, vertex, false, typeContext);
    }

    private BaseVertex parseVertex(HugeGraphSupplier graph, BackendColumn vertexCol,
                                   BaseVertex vertex, boolean tagged, TypeContext typeContext) {
        if (vertex == null) {
            BinaryId binaryId =
                    BytesBuffer.wrap(vertexCol.name).parseId(HugeType.VERTEX);
            vertex = new BaseVertex(binaryId.origin(), VertexLabel.NONE, typeContext);
        }

        if (ArrayUtils.isEmpty(vertexCol.value)) {
            // No need to parse vertex properties
            return vertex;
        }
        this.parseVertexValue(graph, vertexCol.value, vertex, tagged, vertex::addProperty);
        return vertex;
    }

    public void parseSchemaVertexValue(HugeGraphSupplier graph, byte[] value,
                                       BaseVertex vertex,
                                       BiConsumer<PropertyKey, Object> propertySink) {
        E.checkArgumentNotNull(graph, "Schema vertex decoding requires a schema supplier");
        this.parseVertexValue(graph, value, vertex, false, propertySink);
    }

    private void parseVertexValue(HugeGraphSupplier graph, byte[] value, BaseVertex vertex,
                                   boolean tagged, BiConsumer<PropertyKey, Object> propertySink) {
        BytesBuffer buffer = BytesBuffer.wrap(value);
        Id labelId = buffer.readId();
        VertexLabel label = graph != null ? graph.vertexLabelOrNone(labelId) :
                            new VertexLabel(null, labelId, UNDEF);
        vertex.correctVertexLabel(label);
        this.parseElementValue(graph, buffer, vertex, tagged, propertySink);
    }

    /**
     * Reverse sequence the vertex kv data into vertices of type BaseVertex
     *
     * @param olapVertexCol It must be a column of vertex data
     * @param vertex The destination vertex; null creates one from the OLAP key.
     */
    public BaseVertex parseVertexOlap(HugeGraphSupplier graph,
                                      BackendColumn olapVertexCol, BaseVertex vertex) {
        return this.parseVertexOlap(graph, olapVertexCol, vertex, true, TypeContext.STORAGE);
    }

    public BaseVertex parseSchemaVertexOlap(HugeGraphSupplier graph,
                                          BackendColumn olapVertexCol, BaseVertex vertex) {
        return this.parseSchemaVertexOlap(graph, olapVertexCol, vertex, TypeContext.STORAGE);
    }

    public BaseVertex parseSchemaVertexOlap(HugeGraphSupplier graph,
                                          BackendColumn olapVertexCol, BaseVertex vertex,
                                          TypeContext typeContext) {
        E.checkArgumentNotNull(graph, "Schema OLAP decoding requires a schema supplier");
        return this.parseVertexOlap(graph, olapVertexCol, vertex, false, typeContext);
    }

    private BaseVertex parseVertexOlap(HugeGraphSupplier graph, BackendColumn olapVertexCol,
                                       BaseVertex vertex, boolean tagged, TypeContext typeContext) {
        if (vertex == null) {
            BytesBuffer idBuffer = BytesBuffer.wrap(olapVertexCol.name);
            // Transient Store OLAP keys include the property ID; Server's
            // persisted OLAP column name contains only the vertex ID.
            if (tagged) {
                idBuffer.readId();
            }
            Id vertexId = idBuffer.readId();
            vertex = new BaseVertex(vertexId, VertexLabel.NONE, typeContext);
        }

        this.parsePropertyRecord(graph, BytesBuffer.wrap(olapVertexCol.value),
                                 tagged, vertex::addProperty);
        return vertex;
    }

    public void parseSchemaPropertyRecord(HugeGraphSupplier graph, BytesBuffer buffer,
                                          BiConsumer<PropertyKey, Object> propertySink) {
        E.checkArgumentNotNull(graph, "Schema property decoding requires a schema supplier");
        this.parsePropertyRecord(graph, buffer, false, propertySink);
    }

    private void parsePropertyRecord(HugeGraphSupplier graph, BytesBuffer buffer,
                                     boolean tagged, BiConsumer<PropertyKey, Object> propertySink) {
        Id pkeyId = IdGenerator.of(buffer.readVInt());
        this.parseProperty(graph, pkeyId, buffer, tagged, propertySink);
    }

    /**
     * @param cols The first column contains vertex data; subsequent columns contain OLAP properties.
     */
    public BaseVertex parseVertexFromCols(HugeGraphSupplier graph,
                                          BackendColumn... cols) {
        return this.parseVertexFromCols(graph, true, cols);
    }

    public BaseVertex parseSchemaVertexFromCols(HugeGraphSupplier graph,
                                               BackendColumn... cols) {
        E.checkArgumentNotNull(graph, "Schema vertex decoding requires a schema supplier");
        return this.parseVertexFromCols(graph, false, cols);
    }

    private BaseVertex parseVertexFromCols(HugeGraphSupplier graph, boolean tagged,
                                           BackendColumn... cols) {
        assert cols.length > 0;
        BaseVertex vertex = null;
        for (int index = 0; index < cols.length; index++) {
            BackendColumn col = cols[index];
            if (index == 0) {
                vertex = this.parseVertex(graph, col, vertex, tagged, TypeContext.STORAGE);
            } else {
                this.parseVertexOlap(graph, col, vertex, tagged, TypeContext.STORAGE);
            }
        }
        return vertex;
    }

    public Id parseLabelFromCol(BackendColumn col, boolean isVertex) {
        BytesBuffer buffer;
        if (isVertex) {
            buffer = BytesBuffer.wrap(col.value);
            // next buffer.readId() is the label id of vertex
        } else {
            buffer = BytesBuffer.wrap(col.name);
            Id ownerVertexId = buffer.readId();
            E.checkState(buffer.remaining() > 0, "Missing column type");
            byte type = buffer.read();
            Id labelId = buffer.readId();
            // next buffer.readId() is the sub-label id of edge
        }
        return buffer.readId();
    }

    public BaseEdge parseEdge(HugeGraphSupplier graph, BackendColumn edgeCol,
                              BaseVertex ownerVertex, boolean withEdgeProperties) {
        return this.parseEdge(graph, edgeCol, ownerVertex, withEdgeProperties,
                              true, TypeContext.STORAGE);
    }

    public BaseEdge parseSchemaEdge(HugeGraphSupplier graph, BackendColumn edgeCol,
                                    BaseVertex ownerVertex, boolean withEdgeProperties) {
        return this.parseSchemaEdge(graph, edgeCol, ownerVertex, withEdgeProperties,
                                    TypeContext.STORAGE);
    }

    public BaseEdge parseSchemaEdge(HugeGraphSupplier graph, BackendColumn edgeCol,
                                    BaseVertex ownerVertex, boolean withEdgeProperties,
                                    TypeContext typeContext) {
        E.checkArgumentNotNull(graph, "Schema edge decoding requires a schema supplier");
        return this.parseEdge(graph, edgeCol, ownerVertex, withEdgeProperties,
                              false, typeContext);
    }

    private BaseEdge parseEdge(HugeGraphSupplier graph, BackendColumn edgeCol,
                               BaseVertex ownerVertex, boolean withEdgeProperties,
                               boolean tagged, TypeContext typeContext) {
        EdgeId id = BytesBuffer.wrap(edgeCol.name).readEdgeId(true, null);
        if (ownerVertex == null) {
            ownerVertex = new BaseVertex(id.ownerVertexId(), VertexLabel.NONE, typeContext);
        }
        boolean direction = id.direction() == Directions.OUT;
        EdgeLabel edgeLabel = this.edgeLabel(graph, id);
        BaseEdge edge;
        edge = BaseEdge.constructEdge(graph, ownerVertex, direction,
                                      edgeLabel, id.sortValues(), id.otherVertexId());
        edge.otherVertex().typeContext(typeContext);

        if (!withEdgeProperties /*&& !edge.hasTtl()*/) {
            // only skip properties for edge without ttl
            // todo: save expiredTime before properties
            return edge;
        }

        if (ArrayUtils.isEmpty(edgeCol.value)) {
            // There is no edge-properties here.
            return edge;
        }

        this.parseElementValue(graph, BytesBuffer.wrap(edgeCol.value), edge,
                               tagged, edge::addProperty);
        return edge;
    }

    public EdgeLabel edgeLabel(HugeGraphSupplier graph, EdgeId id) {
        return this.edgeLabel(graph, id, graph);
    }

    public EdgeLabel edgeLabel(HugeGraphSupplier lookupGraph, EdgeId id,
                               HugeGraphSupplier schemaGraph) {
        if (lookupGraph != null) {
            return lookupGraph.edgeLabelOrNone(id.subLabelId());
        }
        EdgeLabel label = new EdgeLabel(schemaGraph, id.subLabelId(), UNDEF);
        if (id.subLabelId() != id.edgeLabelId()) {
            label.edgeLabelType(EdgeLabelType.SUB);
            label.fatherId(id.edgeLabelId());
        }
        return label;
    }

    public void parseSchemaEdgeValue(HugeGraphSupplier graph, byte[] value,
                                     BaseEdge edge,
                                     BiConsumer<PropertyKey, Object> propertySink) {
        E.checkArgumentNotNull(graph, "Schema edge decoding requires a schema supplier");
        this.parseElementValue(graph, BytesBuffer.wrap(value), edge, false, propertySink);
    }

    private void parseElementValue(HugeGraphSupplier graph, BytesBuffer buffer,
                                    BaseElement element, boolean tagged,
                                    BiConsumer<PropertyKey, Object> propertySink) {
        this.parseProperties(graph, buffer, tagged, propertySink);
        if (tagged ? buffer.remaining() > 0 : element.hasTtl()) {
            this.parseExpiredTime(buffer, element);
        }
    }

    /**
     * @param graph When parsing index, graph cannot be null
     * @param index When null, used for operator sinking, store can restore index based on one col data
     */
    public Index parseIndex(HugeGraphSupplier graph, BackendColumn indexCol,
                            Index index) {
        return this.parseIndex(graph, indexCol, index, false);
    }

    /** Decode Server index rows, whose optional expiry is in the column name. */
    public Index parseSchemaIndex(HugeGraphSupplier graph, BackendColumn indexCol) {
        E.checkArgumentNotNull(graph, "Schema index decoding requires a schema supplier");
        return this.parseIndex(graph, indexCol, null, true);
    }

    private Index parseIndex(HugeGraphSupplier graph, BackendColumn indexCol,
                              Index index, boolean schema) {
        HugeType indexType = parseIndexType(indexCol);

        IndexColumnName name = BytesBuffer.wrap(indexCol.name).readIndexColumnName(
                indexType, true, schema ? IndexExpiryLayout.OPTIONAL : IndexExpiryLayout.NONE);
        if (index == null) {
            index = Index.parseIndexId(graph, indexType, name.indexId().asBytes());
        }

        long expiredTime = name.expiredTime();

        if (schema) {
            boolean hasTtl = !index.indexLabel().system() &&
                             index.indexLabel().baseLabel().ttl() > 0;
            Long legacyExpiredTime = null;
            if (!name.hasExpiredTime()) {
                // Legacy Store writers kept TTL in an exact value envelope.
                // Only use it after the index/element name fields end, never
                // search arbitrary field-value bytes for a delimiter.
                legacyExpiredTime = this.legacyStoreIndexExpiredTime(indexCol.value);
                E.checkState(!hasTtl || legacyExpiredTime != null,
                             "Missing TTL in schema index column");
                expiredTime = legacyExpiredTime == null ? 0L : legacyExpiredTime;
            }
            if (legacyExpiredTime == null && indexType.isStringIndex() &&
                !ArrayUtils.isEmpty(indexCol.value)) {
                index.fieldValues(StringEncoding.decode(indexCol.value));
            }
        } else if (indexCol.value.length > 0) {

            // Get delimiter address
            int delimiterIndex =
                    Bytes.indexOf(indexCol.value, BytesBuffer.STRING_ENDING_BYTE);

            if (delimiterIndex >= 0) {
                // Delimiter is in the data, need to parse from data
                // 1. field value real content
                byte[] fieldValueBytes =
                        Arrays.copyOfRange(indexCol.value, 0, delimiterIndex);
                if (fieldValueBytes.length > 0) {
                    index.fieldValues(StringEncoding.decode(fieldValueBytes));
                }

                // 2. Expiration time
                byte[] expiredTimeBytes =
                        Arrays.copyOfRange(indexCol.value, delimiterIndex + 1,
                                indexCol.value.length);

                if (expiredTimeBytes.length > 0) {
                    byte[] rawBytes =
                            Base64.getDecoder().decode(expiredTimeBytes);
                    if (rawBytes.length >= Longs.BYTES) {
                        expiredTime = Longs.fromByteArray(rawBytes);
                    }
                }
            } else {
                // Only field value data
                index.fieldValues(StringEncoding.decode(indexCol.value));
            }
        }

        index.elementIds(name.elementId(), expiredTime);
        return index;
    }

    /** Decode the existing Store label envelope only at a complete label-index key boundary. */
    public Long storedLabelIndexExpiredTime(HugeType type, boolean atNameEnd, byte[] value) {
        return type.isLabelIndex() && atNameEnd ? this.legacyStoreIndexExpiredTime(value) : null;
    }

    private Long legacyStoreIndexExpiredTime(byte[] value) {
        // A Store TTL writer emits exactly 0x00 + Base64(long), with no fields.
        if (value == null || value.length != 13 || value[0] != BytesBuffer.STRING_ENDING_BYTE) {
            return null;
        }
        try {
            byte[] expiry = Base64.getDecoder().decode(Arrays.copyOfRange(value, 1, value.length));
            if (expiry.length != Longs.BYTES ||
                !Arrays.equals(Base64.getEncoder().encode(expiry),
                               Arrays.copyOfRange(value, 1, value.length))) {
                return null;
            }
            return Longs.fromByteArray(expiry);
        } catch (IllegalArgumentException ignored) {
            return null;
        }
    }

    public BackendColumn parseIndex(BackendColumn indexCol) {
        // Self-parsing index
        throw new NotImplementedException(
                "BinaryElementSerializer.parseIndex");
    }

    public BackendColumn writeVertex(BaseVertex vertex) {
        if (vertex.olap()) {
            return this.writeOlapVertex(vertex);
        }

        BytesBuffer bufferName = BytesBuffer.allocate(vertex.id().length());
        bufferName.writeId(vertex.id());

        return BackendColumn.of(bufferName.bytes(), this.formatVertexValue(vertex, true));
    }

    public byte[] formatSchemaVertexValue(BaseVertex vertex) {
        return this.formatVertexValue(vertex, false);
    }

    private byte[] formatVertexValue(BaseVertex vertex, boolean tagged) {
        BytesBuffer buffer = BytesBuffer.allocate(8 + 16 * vertex.sizeOfProperties());
        buffer.writeId(vertex.schemaLabel().id());
        this.formatElementProperties(vertex, buffer, tagged);
        this.formatElementExpiredTime(vertex, buffer);
        return buffer.bytes();
    }

    public BackendColumn writeOlapVertex(BaseVertex vertex) {
        BytesBuffer buffer = BytesBuffer.allocate(8 + 16);

        BaseProperty<?> baseProperty = vertex.getProperties().values()
                .iterator().next();
        PropertyKey propertyKey = baseProperty.propertyKey();
        this.formatProperty(baseProperty, buffer, true);

        return BackendColumn.of(OlapKey.format(propertyKey.id(), vertex.id()), buffer.bytes());
    }

    public BackendColumn writeEdge(BaseEdge edge) {
        byte[] name = this.formatEdgeName(edge);
        byte[] value = this.formatEdgeValue(edge);
        return BackendColumn.of(name, value);
    }

    /**
     * Convert an index data to a BackendColumn
     */
    public BackendColumn writeIndex(Index index) {
        return BackendColumn.of(formatIndexName(index),
                formatIndexValue(index));
    }

    /** Write Store-owned rows with stable keys and the existing storage label-expiry envelope. */
    public BackendColumn writeSchemaIndex(Index index) {
        BackendColumn column = this.formatSchemaIndex(
                index.type(), index.indexLabelId(), index.fieldValues(), index.elementId(),
                true, index.hasTtl(), index.expiredTime());
        if (this.storageLabelHasTtl(index)) {
            // Preserve label-index keys so Core append/remove addresses the same row.
            column.value = this.formatIndexValue(index);
        }
        return column;
    }

    public BackendColumn formatSchemaIndex(HugeType type, Id indexLabelId, Object fieldValues,
                                           Id elementId, boolean withIdPrefix,
                                           boolean hasTtl, long expiredTime) {
        Id rawId = Index.formatIndexId(type, indexLabelId, fieldValues);
        boolean hashed = !type.isNumericIndex() && indexIdLengthExceedLimit(rawId);
        Id indexId = schemaIndexId(type, indexLabelId, fieldValues);
        int capacity = 1 + elementId.length() + (withIdPrefix ? 1 + indexId.length() : 0);
        BytesBuffer name = BytesBuffer.allocate(capacity);
        if (withIdPrefix) {
            name.writeIndexId(indexId, type);
        }
        name.writeId(elementId);
        if (hasTtl) {
            name.writeVLong(expiredTime);
        }
        byte[] value = hashed ? StringEncoding.encode(fieldValues.toString()) : BytesBuffer.BYTES_EMPTY;
        return BackendColumn.of(name.bytes(), value);
    }

    public static Id schemaIndexId(HugeType type, Id indexLabelId, Object fieldValues) {
        Id id = Index.formatIndexId(type, indexLabelId, fieldValues);
        if (!type.isNumericIndex() && indexIdLengthExceedLimit(id)) {
            id = Index.formatIndexHashId(type, indexLabelId, fieldValues);
        }
        return id;
    }

    public static boolean indexIdLengthExceedLimit(Id id) {
        return id.asBytes().length > BytesBuffer.INDEX_HASH_ID_THRESHOLD;
    }

    private byte[] formatIndexName(Index index) {
        BytesBuffer buffer;
        Id elemId = index.elementId();
        Id indexId = index.id();
        HugeType type = index.type();
        int idLen = 1 + elemId.length() + 1 + indexId.length();
        buffer = BytesBuffer.allocate(idLen);
        // Write index-id
        buffer.writeIndexId(indexId, type);
        // Write element-id
        buffer.writeId(elemId);

        return buffer.bytes();
    }

    /**
     * @param index value
     * @return format
     * | empty(field-value) | 0x00  |  base64(expiredtime) |
     */
    private boolean storageLabelHasTtl(Index index) {
        return (index.indexLabel() == IndexLabel.label(HugeType.VERTEX) ||
                index.indexLabel() == IndexLabel.label(HugeType.EDGE)) && index.expiredTime() > 0;
    }

    private byte[] formatIndexValue(Index index) {
        if (this.storageLabelHasTtl(index) || index.hasTtl()) {
            BytesBuffer valueBuffer = BytesBuffer.allocate(14);

            valueBuffer.write(BytesBuffer.STRING_ENDING_BYTE);
            byte[] ttlBytes =
                    Base64.getEncoder().encode(Longs.toByteArray(index.expiredTime()));
            valueBuffer.write(ttlBytes);

            return valueBuffer.bytes();
        }

        return null;
    }

    public BackendColumn mergeCols(BackendColumn vertexCol, BackendColumn... olapVertexCols) {
        if (olapVertexCols.length == 0) {
            return vertexCol;
        }
        BytesBuffer mergedBuffer = BytesBuffer.allocate(
                vertexCol.value.length + olapVertexCols.length * 16);

        BytesBuffer buffer = BytesBuffer.wrap(vertexCol.value);
        Id vl = buffer.readId();
        int size = buffer.readVInt();

        mergedBuffer.writeId(vl);
        mergedBuffer.writeVInt(size + olapVertexCols.length);
        // Prioritize writing vertexCol properties, because vertexCol may contain TTL
        for (BackendColumn olapVertexCol : olapVertexCols) {
            mergedBuffer.write(olapVertexCol.value);
        }
        mergedBuffer.write(buffer.remainingBytes());

        return BackendColumn.of(vertexCol.name, mergedBuffer.bytes());
    }

    public BaseElement index2Element(HugeGraphSupplier graph,
                                     BackendColumn indexCol) {
        throw new NotImplementedException(
                "BinaryElementSerializer.index2Element");
    }

    public byte[] formatEdgeName(BaseEdge edge) {
        // owner-vertex + dir + edge-label + sort-values + other-vertex
        return BytesBuffer.allocate(BytesBuffer.BUF_EDGE_ID)
                .writeEdgeId(edge.id()).bytes();
    }

    protected byte[] formatEdgeValue(BaseEdge edge) {
        return this.formatEdgeValue(edge, true);
    }

    public byte[] formatSchemaEdgeValue(BaseEdge edge) {
        return this.formatEdgeValue(edge, false);
    }

    private byte[] formatEdgeValue(BaseEdge edge, boolean tagged) {
        BytesBuffer buffer = BytesBuffer.allocate(4 + 16 * edge.sizeOfProperties());
        this.formatElementProperties(edge, buffer, tagged);
        this.formatElementExpiredTime(edge, buffer);
        return buffer.bytes();
    }

    private void formatElementProperties(BaseElement element, BytesBuffer buffer,
                                         boolean tagged) {
        if (tagged) {
            this.formatProperties(element.getProperties().values(), buffer);
        } else {
            this.formatSchemaProperties(element, buffer);
        }
    }

    private void formatElementExpiredTime(BaseElement element, BytesBuffer buffer) {
        if (element.hasTtl()) {
            this.formatExpiredTime(element.expiredTime(), buffer);
        }
    }

    public void formatProperties(Collection<BaseProperty<?>> props,
                                 BytesBuffer buffer) {
        this.formatProperties(props, props.size(), buffer, true);
    }

    public void formatSchemaProperties(BaseElement owner, BytesBuffer buffer) {
        this.formatProperties(owner.properties().values(), owner.sizeOfProperties(), buffer, false);
    }

    private void formatProperties(Iterable<? extends BaseProperty<?>> props, int size,
                                   BytesBuffer buffer, boolean tagged) {
        buffer.writeVInt(size);
        for (BaseProperty<?> property : props) {
            this.formatProperty(property, buffer, tagged);
        }
    }

    public void formatSchemaProperty(BaseProperty<?> property, BytesBuffer buffer) {
        this.formatProperty(property, buffer, false);
    }

    private void formatProperty(BaseProperty<?> property, BytesBuffer buffer,
                                boolean tagged) {
        PropertyKey pkey = property.propertyKey();
        buffer.writeVInt(SchemaElement.schemaId(pkey.id()));
        if (tagged) {
            buffer.writeProperty(pkey, property.value());
        } else {
            buffer.writeSchemaProperty(pkey, property.value());
        }
    }

    public void formatExpiredTime(long expiredTime, BytesBuffer buffer) {
        buffer.writeVLong(expiredTime);
    }

    public void parseExpiredTime(BytesBuffer buffer, BaseElement element) {
        element.expiredTime(buffer.readVLong());
    }

    private HugeType parseIndexType(BackendColumn col) {
        /**
         *   Reference formatIndexName method
         *   For range type index, col.name first byte writes type.code (1 byte)
         *   Other type indexes will write type.name in first two bytes (2 byte)
         */
        byte first = col.name[0];
        byte second = col.name[1];
        if (first < 0) {
            return HugeType.fromCode(first);
        }
        assert second >= 0;
        String type = new String(new byte[]{first, second});
        return HugeType.fromString(type);
    }
}
