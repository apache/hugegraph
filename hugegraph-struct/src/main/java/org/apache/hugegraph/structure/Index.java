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

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.hugegraph.HugeGraphSupplier;
import org.apache.hugegraph.exception.HugeException;
import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.id.Id.IdType;
import org.apache.hugegraph.id.IdGenerator;
import org.apache.hugegraph.id.SplicingIdGenerator;
import org.apache.hugegraph.serializer.BytesBuffer;
import org.apache.hugegraph.struct.schema.IndexLabel;
import org.apache.hugegraph.struct.schema.SchemaElement;
import org.apache.hugegraph.type.GraphType;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.type.define.DataType;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.InsertionOrderUtil;
import org.apache.hugegraph.util.NumericUtil;

import com.google.common.collect.ImmutableSet;

public class Index implements GraphType, Cloneable {

    private final HugeGraphSupplier graph;
    private Object fieldValues;
    private final IndexLabel indexLabel;
    /*
     * Index read use elementIds, Index write always one element, use
     * elementId
     */
    private Set<IdWithExpiredTime> elementIds;
    private IdWithExpiredTime elementId;

    public Index(HugeGraphSupplier graph, IndexLabel indexLabel) {
        E.checkNotNull(graph, "graph");
        E.checkNotNull(indexLabel, "label");
        E.checkNotNull(indexLabel.id(), "label id");
        this.graph = graph;
        this.indexLabel = indexLabel;
        this.elementIds = new LinkedHashSet<>();
        this.fieldValues = null;
    }

    public Index(HugeGraphSupplier graph, IndexLabel indexLabel, boolean write) {
        E.checkNotNull(graph, "graph");
        E.checkNotNull(indexLabel, "label");
        E.checkNotNull(indexLabel.id(), "label id");
        this.graph = graph;
        this.indexLabel = indexLabel;
        if (!write) {
            this.elementIds = new LinkedHashSet<>();
        }
        this.elementId = null;
        this.fieldValues = null;
    }

    @Override
    public String name() {
        return this.indexLabel.name();
    }

    @Override
    public HugeType type() {
        if (this.indexLabel == IndexLabel.label(HugeType.VERTEX)) {
            return HugeType.VERTEX_LABEL_INDEX;
        } else if (this.indexLabel == IndexLabel.label(HugeType.EDGE)) {
            return HugeType.EDGE_LABEL_INDEX;
        }
        return this.indexLabel.indexType().type();
    }

    public HugeGraphSupplier graph() {
        return this.graph;
    }

    public Id id() {
        return formatIndexId(type(), this.indexLabelId(), this.fieldValues());
    }

    public Object fieldValues() {
        return this.fieldValues;
    }

    public void fieldValues(Object fieldValues) {
        this.fieldValues = fieldValues;
    }

    public Id indexLabelId() {
        return this.indexLabel.id();
    }

    public IndexLabel indexLabel() {
        return this.indexLabel;
    }

    public IdWithExpiredTime elementIdWithExpiredTime() {
        if (this.elementIds == null) {
            return this.elementId;
        }
        E.checkState(this.elementIds.size() == 1,
                     "Expect one element id, actual %s",
                     this.elementIds.size());
        return this.elementIds.iterator().next();
    }

    public Id elementId() {
        return this.elementIdWithExpiredTime().id();
    }

    public Set<Id> elementIds() {
        if (this.elementIds == null) {
            return ImmutableSet.of();
        }
        Set<Id> ids = InsertionOrderUtil.newSet(this.elementIds.size());
        for (IdWithExpiredTime idWithExpiredTime : this.elementIds) {
            ids.add(idWithExpiredTime.id());
        }
        return Collections.unmodifiableSet(ids);
    }

    public Set<IdWithExpiredTime> expiredElementIds() {
        long now = this.graph.now();
        Set<IdWithExpiredTime> expired = InsertionOrderUtil.newSet();
        for (IdWithExpiredTime id : this.elementIds) {
            if (0L < id.expiredTime && id.expiredTime < now) {
                expired.add(id);
            }
        }
        this.elementIds.removeAll(expired);
        return expired;
    }

    public void elementIds(Id elementId) {
        this.elementIds(elementId, 0L);
    }

    public void elementIds(Id elementId, long expiredTime) {
        if (this.elementIds == null) {
            this.elementId = new IdWithExpiredTime(elementId, expiredTime);
        } else {
            this.elementIds.add(new IdWithExpiredTime(elementId, expiredTime));
        }
    }

    public void resetElementIds() {
        this.elementIds = null;
    }

    public long expiredTime() {
        return this.elementIdWithExpiredTime().expiredTime();
    }

    public boolean hasTtl() {
        if ((this.indexLabel() == IndexLabel.label(HugeType.VERTEX) ||
            this.indexLabel() == IndexLabel.label(HugeType.EDGE)) &&
            this.expiredTime() > 0) {
            // LabelIndex index, if element has expiration time, then index also has TTL
            return true;
        }

        if (this.indexLabel.system()) {
            return false;
        }
        return this.indexLabel.baseElement().ttl() > 0L;
    }

    public long ttl() {
        return this.expiredTime() - this.graph.now();
    }

    @Override
    public Index clone() {
        try {
            return (Index) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new HugeException("Failed to clone Index", e);
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Index)) {
            return false;
        }

        Index other = (Index) obj;
        return this.id().equals(other.id());
    }

    @Override
    public int hashCode() {
        return this.id().hashCode();
    }

    @Override
    public String toString() {
        return String.format("{label=%s<%s>, fieldValues=%s, elementIds=%s}",
                             this.indexLabel.name(),
                             this.indexLabel.indexType().string(),
                             this.fieldValues, this.elementIds);
    }


    public static Id formatIndexId(HugeType type, Id indexLabelId,
                                   Object fieldValues) {
        if (type.isStringIndex()) {
            String value = "";
            if (fieldValues instanceof Id) {
                value = IdGenerator.asStoredString((Id) fieldValues);
            } else if (fieldValues != null) {
                value = fieldValues.toString();
            }
            /*
             * Modify order between index label and field-values to put the
             * index label in front(hugegraph-1317)
             */
            String strIndexLabelId = IdGenerator.asStoredString(indexLabelId);
            // Add id prefix according to type
            return SplicingIdGenerator.splicing(type.string(),  strIndexLabelId, value);
        } else {
            assert type.isRangeIndex();
            int length = type.isRange4Index() ? 4 : 8;
            // 1 is table type, 4 is labelId, length is value
            BytesBuffer buffer = BytesBuffer.allocate(1 + 4 + length);
            // Add table type id
            buffer.write(type.code());

            buffer.writeInt(SchemaElement.schemaId(indexLabelId));
            if (fieldValues != null) {
                E.checkState(fieldValues instanceof Number,
                             "Field value of range index must be number:" +
                             " %s", fieldValues.getClass().getSimpleName());
                byte[] bytes = number2bytes((Number) fieldValues);
                buffer.write(bytes);
            }
            return buffer.asId();
        }
    }

    public static Index parseIndexId(HugeGraphSupplier graph, HugeType type,
                                     byte[] id) {
        Object values;
        IndexLabel indexLabel;
        if (type.isStringIndex()) {
            Id idObject = IdGenerator.of(id, IdType.STRING);
            String[] parts = SplicingIdGenerator.parse(idObject);
            E.checkState(parts.length == 3, "Invalid secondary index id");
            Id label = IdGenerator.ofStoredString(parts[1], IdType.LONG);
            indexLabel = IndexLabel.label(graph, label);
            values = parts[2];
        } else {
            assert type.isRange4Index() || type.isRange8Index();
            final int labelLength = 4;
            E.checkState(id.length > labelLength, "Invalid range index id");
            BytesBuffer buffer = BytesBuffer.wrap(id);
            // Read the first byte representing the table type
            final int hugeTypeCodeLength = 1;
            byte[] read = buffer.read(hugeTypeCodeLength);

            Id label = IdGenerator.of(buffer.readInt());
            indexLabel = IndexLabel.label(graph, label);
            List<Id> fields = indexLabel.indexFields();
            E.checkState(fields.size() == 1, "Invalid range index fields");
            DataType dataType = graph.propertyKey(fields.get(0)).dataType();
            E.checkState(dataType.isNumber() || dataType.isDate(),
                         "Invalid range index field type");
            Class<?> clazz = dataType.isNumber() ?
                             dataType.clazz() : DataType.LONG.clazz();
            values = bytes2number(buffer.read(id.length - labelLength - hugeTypeCodeLength), clazz);
        }
        Index index = new Index(graph, indexLabel);
        index.fieldValues(values);
        return index;
    }

    public static byte[] number2bytes(Number number) {
        if (number instanceof Byte) {
            // Handle byte as integer to store as 4 bytes in RANGE4_INDEX
            number = number.intValue();
        }
        return NumericUtil.numberToSortableBytes(number);
    }

    public static Number bytes2number(byte[] bytes, Class<?> clazz) {
        return NumericUtil.sortableBytesToNumber(bytes, clazz);
    }

    public static class IdWithExpiredTime {

        private final Id id;
        private final long expiredTime;

        public IdWithExpiredTime(Id id, long expiredTime) {
            this.id = id;
            this.expiredTime = expiredTime;
        }

        public Id id() {
            return this.id;
        }

        public long expiredTime() {
            return this.expiredTime;
        }

        @Override
        public String toString() {
            return String.format("%s(%s)", this.id, this.expiredTime);
        }
    }
}
