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

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.hugegraph.HugeException;
import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.id.Id.IdType;
import org.apache.hugegraph.backend.id.IdGenerator;
import org.apache.hugegraph.backend.id.SplicingIdGenerator;
import org.apache.hugegraph.backend.serializer.BytesBuffer;
import org.apache.hugegraph.schema.IndexLabel;
import org.apache.hugegraph.schema.SchemaElement;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.type.define.DataType;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.HashUtil;
import org.apache.hugegraph.util.InsertionOrderUtil;
import org.apache.hugegraph.util.NumericUtil;

public class HugeIndex implements GraphType, Cloneable {

    private final HugeGraph graph;
    private Object fieldValues;
    private IndexLabel indexLabel;
    private Set<IdWithExpiredTime> elementIds;
    private static final int HUGE_TYPE_CODE_LENGTH = 1;

    public HugeIndex(HugeGraph graph, IndexLabel indexLabel) {
        E.checkNotNull(graph, "graph");
        E.checkNotNull(indexLabel, "label");
        E.checkNotNull(indexLabel.id(), "label id");
        this.graph = graph;
        this.indexLabel = indexLabel;
        this.elementIds = new LinkedHashSet<>();
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

    public HugeGraph graph() {
        return this.graph;
    }

    public Id id() {
        return formatIndexId(type(), this.indexLabelId(), this.fieldValues());
    }

    public Id hashId() {
        return formatIndexHashId(type(), this.indexLabelId(), this.fieldValues());
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
        E.checkState(this.elementIds.size() == 1,
                     "Expect one element id, actual %s",
                     this.elementIds.size());
        return this.elementIds.iterator().next();
    }

    public Id elementId() {
        return this.elementIdWithExpiredTime().id();
    }

    public Set<Id> elementIds() {
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
        this.elementIds.add(new IdWithExpiredTime(elementId, expiredTime));
    }

    public void resetElementIds() {
        this.elementIds = new LinkedHashSet<>();
    }

    public long expiredTime() {
        return this.elementIdWithExpiredTime().expiredTime();
    }

    public boolean hasTtl() {
        if (this.indexLabel.system()) {
            return false;
        }
        return this.indexLabel.baseLabel().ttl() > 0L;
    }

    public long ttl() {
        return this.expiredTime() - this.graph.now();
    }

    @Override
    public HugeIndex clone() {
        try {
            return (HugeIndex) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new HugeException("Failed to clone HugeIndex", e);
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof HugeIndex)) {
            return false;
        }

        HugeIndex other = (HugeIndex) obj;
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

    public static Id formatIndexHashId(HugeType type, Id indexLabel,
                                       Object fieldValues) {
        E.checkState(!type.isRangeIndex(),
                     "RangeIndex can't return a hash id");
        String value = fieldValues == null ? "" : fieldValues.toString();
        return formatIndexId(type, indexLabel, HashUtil.hash(value));
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
            return SplicingIdGenerator.splicing(type.string(), strIndexLabelId, value);
        } else {
            assert (type.isRangeIndex() || type.isVectorIndex());
            int length = type.isRange4Index() || type.isVectorIndex() ? 4 : 8;
            BytesBuffer buffer = BytesBuffer.allocate(HUGE_TYPE_CODE_LENGTH + 4 + length);
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

    public static HugeIndex parseIndexId(HugeGraph graph, HugeType type,
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
            assert type.isRange4Index() || type.isRange8Index() || type.isVectorIndex();
            final int labelLength = 4;
            E.checkState(id.length > labelLength, "Invalid range index id");
            BytesBuffer buffer = BytesBuffer.wrap(id);
            buffer.read(HUGE_TYPE_CODE_LENGTH);
            Id label = IdGenerator.of(buffer.readInt());
            indexLabel = IndexLabel.label(graph, label);
            List<Id> fields = indexLabel.indexFields();
            E.checkState(fields.size() == 1, "Invalid range index fields");
            DataType dataType = graph.propertyKey(fields.get(0)).dataType();
            E.checkState(dataType.isNumber() || dataType.isDate(),
                         "Invalid range index field type");
            Class<?> clazz = dataType.isNumber() ?
                             dataType.clazz() : DataType.LONG.clazz();
            values = bytes2number(buffer.read(id.length - labelLength - HUGE_TYPE_CODE_LENGTH),
                                  clazz);
        }
        HugeIndex index = new HugeIndex(graph, indexLabel);
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

        private Id id;
        private long expiredTime;

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
