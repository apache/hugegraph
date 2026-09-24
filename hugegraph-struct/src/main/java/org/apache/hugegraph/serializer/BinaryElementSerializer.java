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
import java.util.Map;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.NotImplementedException;
import org.apache.hugegraph.HugeGraphSupplier;
import org.apache.hugegraph.backend.BackendColumn;
import org.apache.hugegraph.backend.BinaryId;
import org.apache.hugegraph.exception.HugeException;
import org.apache.hugegraph.id.EdgeId;
import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.id.IdGenerator;
import org.apache.hugegraph.struct.schema.EdgeLabel;
import org.apache.hugegraph.struct.schema.PropertyKey;
import org.apache.hugegraph.struct.schema.SchemaElement;
import org.apache.hugegraph.struct.schema.VertexLabel;
import org.apache.hugegraph.structure.BaseEdge;
import org.apache.hugegraph.structure.BaseElement;
import org.apache.hugegraph.structure.BaseProperty;
import org.apache.hugegraph.structure.BaseVertex;
import org.apache.hugegraph.structure.Index;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.type.define.Cardinality;
import org.apache.hugegraph.type.define.EdgeLabelType;
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
                                 BytesBuffer buffer,
                                 BaseElement owner) {
        PropertyKey pkey = graph != null ?
                graph.propertyKey(pkeyId) :
                new PropertyKey(graph, pkeyId, "");
        // Parse value
        Object value = buffer.readProperty(pkey);
        // Set properties of vertex/edge
        if (pkey.cardinality() == Cardinality.SINGLE) {
            owner.addProperty(pkey, value);
        } else {
            if (!(value instanceof Collection)) {
                throw new HugeException(
                        "Invalid value of non-single property: %s", value);
            }
            owner.addProperty(pkey, value);
        }
    }

    public void parseProperties(HugeGraphSupplier graph, BytesBuffer buffer,
                                BaseElement owner) {
        int size = buffer.readVInt();
        assert size >= 0;
        for (int i = 0; i < size; i++) {
            Id pkeyId = IdGenerator.of(buffer.readVInt());
            this.parseProperty(graph, pkeyId, buffer, owner);
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
        if (vertex == null) {
            BinaryId binaryId =
                    BytesBuffer.wrap(vertexCol.name).parseId(HugeType.VERTEX);
            vertex = new BaseVertex(binaryId.origin(), VertexLabel.NONE);
        }

        if (ArrayUtils.isEmpty(vertexCol.value)) {
            // No need to parse vertex properties
            return vertex;
        }
        BytesBuffer buffer = BytesBuffer.wrap(vertexCol.value);
        Id labelId = buffer.readId();
        // Parse vertex label
        if (graph != null) {
            VertexLabel label = graph.vertexLabelOrNone(labelId);
            vertex.correctVertexLabel(label);
        } else {
            VertexLabel label = new VertexLabel(null, labelId, UNDEF);
            vertex.correctVertexLabel(label);
        }
        // Parse properties
        this.parseProperties(graph, buffer, vertex);

        // Parse vertex expired time if needed
        if (buffer.remaining() > 0 /*edge.hasTtl()*/) {
            this.parseExpiredTime(buffer, vertex);
        }
        return vertex;
    }

    /**
     * Reverse sequence the vertex kv data into vertices of type BaseVertex
     *
     * @param olapVertexCol It must be a column of vertex data
     * @param vertex        When vertex==null, it is used for operator sinking to reverse sequence the col data into olapBaseVertex.
     *                      vertex! When =null, add the col information to olapBaseVertex
     */
    public BaseVertex parseVertexOlap(HugeGraphSupplier graph,
                                      BackendColumn olapVertexCol, BaseVertex vertex) {
        if (vertex == null) {
            BytesBuffer idBuffer = BytesBuffer.wrap(olapVertexCol.name);
            // read olap property id
            idBuffer.readId();
            // read vertex id which olap property belongs to
            Id vertexId = idBuffer.readId();
            vertex = new BaseVertex(vertexId, VertexLabel.NONE);
        }

        BytesBuffer buffer = BytesBuffer.wrap(olapVertexCol.value);
        Id pkeyId = IdGenerator.of(buffer.readVInt());
        this.parseProperty(graph, pkeyId, buffer, vertex);
        return vertex;
    }

    /**
     * @param cols Deserializing a complete vertex may require multiple cols
     *             The first col represents the common vertex information in the g+v table, and each subsequent col represents the olap vertices stored in the olap table
     */
    public BaseVertex parseVertexFromCols(HugeGraphSupplier graph,
                                          BackendColumn... cols) {
        assert cols.length > 0;
        BaseVertex vertex = null;
        for (int index = 0; index < cols.length; index++) {
            BackendColumn col = cols[index];
            if (index == 0) {
                vertex = this.parseVertex(graph, col, vertex);
            } else {
                this.parseVertexOlap(graph, col, vertex);
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
                              BaseVertex ownerVertex,
                              boolean withEdgeProperties) {
        // owner-vertex + dir + edge-label.id() + subLabel.id() +
        // + sort-values + other-vertex

        BytesBuffer buffer = BytesBuffer.wrap(edgeCol.name);
        // Consume owner-vertex id
        Id id = buffer.readId();
        if (ownerVertex == null) {
            ownerVertex = new BaseVertex(id, VertexLabel.NONE);
        }

        E.checkState(buffer.remaining() > 0, "Missing column type");

        byte type = buffer.read();
        if (type == HugeType.EDGE_IN.code() ||
                type == HugeType.EDGE_OUT.code()) {
            E.checkState(true,
                    "Invalid column(%s) with unknown type(%s): 0x%s",
                    id, type & 0xff, Bytes.toHex(edgeCol.name));
        }

        Id labelId = buffer.readId();
        Id subLabelId = buffer.readId();
        String sortValues = buffer.readStringWithEnding();
        Id otherVertexId = buffer.readId();
        boolean direction = EdgeId.isOutDirectionFromCode(type);
        BaseEdge edge;
        EdgeLabel edgeLabel;
        if (graph == null) { /* when calculation sinking */
            edgeLabel = new EdgeLabel(null, subLabelId, UNDEF);
            // If not equal here, need to add fatherId for correct operator sinking
            if (subLabelId != labelId) {
                edgeLabel.edgeLabelType(EdgeLabelType.SUB);
                edgeLabel.fatherId(labelId);
            }

        } else {
            edgeLabel = graph.edgeLabelOrNone(subLabelId);
        }
        edge = BaseEdge.constructEdge(graph, ownerVertex, direction,
                edgeLabel, sortValues, otherVertexId);

        if (!withEdgeProperties /*&& !edge.hasTtl()*/) {
            // only skip properties for edge without ttl
            // todo: save expiredTime before properties
            return edge;
        }

        if (ArrayUtils.isEmpty(edgeCol.value)) {
            // There is no edge-properties here.
            return edge;
        }

        // Parse edge-id + edge-properties
        buffer = BytesBuffer.wrap(edgeCol.value);

        // Parse edge properties
        this.parseProperties(graph, buffer, edge);

        /* Skip TTL parsing process first
         * Can't determine if edge has TTL through edge, need to judge by bytebuffer length */
//        // Parse edge expired time if needed
        if (buffer.remaining() > 0 /*edge.hasTtl()*/) {
            this.parseExpiredTime(buffer, edge);
        }
        return edge;
    }

    /**
     * @param graph When parsing index, graph cannot be null
     * @param index When null, used for operator sinking, store can restore index based on one col data
     */
    public Index parseIndex(HugeGraphSupplier graph, BackendColumn indexCol,
                            Index index) {
        HugeType indexType = parseIndexType(indexCol);

        BytesBuffer buffer = BytesBuffer.wrap(indexCol.name);
        BinaryId indexId = buffer.readIndexId(indexType);
        Id elemId = buffer.readId();

        if (index == null) {
            index = Index.parseIndexId(graph, indexType, indexId.asBytes());
        }

        long expiredTime = 0L;

        if (indexCol.value.length > 0) {

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

        index.elementIds(elemId, expiredTime);
        return index;
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

        int propsCount = vertex.getProperties().size();
        BytesBuffer buffer = BytesBuffer.allocate(8 + 16 * propsCount);

        // Write vertex label
        buffer.writeId(vertex.schemaLabel().id());

        // Write all properties of the vertex
        this.formatProperties(vertex.getProperties().values(), buffer);

        // Write vertex expired time if needed
        if (vertex.hasTtl()) {
            this.formatExpiredTime(vertex.expiredTime(), buffer);
        }

        return BackendColumn.of(bufferName.bytes(), buffer.bytes());
    }

    public BackendColumn writeOlapVertex(BaseVertex vertex) {
        BytesBuffer buffer = BytesBuffer.allocate(8 + 16);

        BaseProperty<?> baseProperty = vertex.getProperties().values()
                .iterator().next();
        PropertyKey propertyKey = baseProperty.propertyKey();
        buffer.writeVInt(SchemaElement.schemaId(propertyKey.id()));
        buffer.writeProperty(propertyKey.cardinality(), propertyKey.dataType(),
                baseProperty.value());

        // OLAP table merge, key is {property_key_id}{vertex_id}
        BytesBuffer bufferName =
                BytesBuffer.allocate(1 + propertyKey.id().length() + 1 +
                        vertex.id().length());
        bufferName.writeId(propertyKey.id());
        bufferName.writeId(vertex.id()).bytes();

        return BackendColumn.of(bufferName.bytes(), buffer.bytes());
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
    private byte[] formatIndexValue(Index index) {
        if (index.hasTtl()) {
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
        Map<Id, BaseProperty<?>> properties = edge.getProperties();
        int propsCount = properties.size();
        BytesBuffer buffer = BytesBuffer.allocate(4 + 16 * propsCount);

        // Write edge properties
        this.formatProperties(properties.values(), buffer);

        // Write edge expired time if needed
        if (edge.hasTtl()) {
            this.formatExpiredTime(edge.expiredTime(), buffer);
        }

        return buffer.bytes();
    }

    public void formatProperties(Collection<BaseProperty<?>> props,
                                 BytesBuffer buffer) {
        // Write properties size
        buffer.writeVInt(props.size());

        // Write properties data
        for (BaseProperty<?> property : props) {
            PropertyKey pkey = property.propertyKey();
            buffer.writeVInt(SchemaElement.schemaId(pkey.id()));
            buffer.writeProperty(pkey.cardinality(), pkey.dataType(),
                    property.value());
        }
    }

    public void formatExpiredTime(long expiredTime, BytesBuffer buffer) {
        buffer.writeVLong(expiredTime);
    }

    protected void parseExpiredTime(BytesBuffer buffer, BaseElement element) {
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
