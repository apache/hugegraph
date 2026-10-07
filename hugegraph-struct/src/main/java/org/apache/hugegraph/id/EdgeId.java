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

package org.apache.hugegraph.id;

import org.apache.hugegraph.exception.NotFoundException;
import org.apache.hugegraph.perf.PerfUtil.Watched;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.type.define.Directions;
import org.apache.hugegraph.type.define.HugeKeys;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.StringEncoding;

/**
 * Class used to format and parse id of edge, the edge id consists of:
 * EdgeId = { source-vertex-id > direction > parentEdgeLabelId > subEdgeLabelId
 * > sortKeys > target-vertex-id }
 * NOTE:
 * <p>1. for edges with edgeLabelType = NORMAL: edgeLabelId = parentEdgeLabelId = subEdgeLabelId;
 * for edges with edgeLabelType = PARENT: edgeLabelId = subEdgeLabelId, parentEdgeLabelId =
 * edgeLabelId.fatherId
 * <p>2.if we use `entry.type()` which is IN or OUT as a part of id,
 * an edge's id will be different due to different directions (belongs
 * to 2 owner vertex)
 */
public class EdgeId implements Id {

    public static final HugeKeys[] KEYS = new HugeKeys[]{
            HugeKeys.OWNER_VERTEX,
            HugeKeys.DIRECTION,
            HugeKeys.LABEL,
            HugeKeys.SUB_LABEL,
            HugeKeys.SORT_VALUES,
            HugeKeys.OTHER_VERTEX
    };

    protected final Id ownerVertexId;
    protected final Id edgeLabelId;
    protected final Id subLabelId;
    protected final Id otherVertexId;
    protected final Directions direction;
    protected final boolean directed;
    protected String sortValues;
    protected String cache;

    public EdgeId(Id ownerVertexId, Directions direction, Id edgeLabelId, Id subLabelId,
                  String sortValues, Id otherVertexId) {
        this(ownerVertexId, direction, edgeLabelId, subLabelId,
             sortValues, otherVertexId, false);
    }

    public EdgeId(Id ownerVertexId, Directions direction, Id edgeLabelId, Id subLabelId,
                  String sortValues, Id otherVertexId, boolean directed) {
        this.ownerVertexId = ownerVertexId;
        this.direction = direction;
        this.edgeLabelId = edgeLabelId;
        this.subLabelId = subLabelId;
        this.sortValues = sortValues;
        this.otherVertexId = otherVertexId;
        this.directed = directed;
        this.cache = null;
    }

    @Watched
    public EdgeId switchDirection() {
        Directions direction = this.direction.opposite();
        return new EdgeId(this.otherVertexId, direction, this.edgeLabelId, this.subLabelId,
                          this.sortValues, this.ownerVertexId, this.directed);
    }

    public EdgeId directed(boolean directed) {
        return new EdgeId(this.ownerVertexId, this.direction, this.edgeLabelId, this.subLabelId,
                          this.sortValues, this.otherVertexId, directed);
    }

    private Id sourceVertexId() {
        return this.direction == Directions.OUT ?
               this.ownerVertexId :
               this.otherVertexId;
    }

    private Id targetVertexId() {
        return this.direction == Directions.OUT ?
               this.otherVertexId :
               this.ownerVertexId;
    }

    public Id ownerVertexId() {
        return this.ownerVertexId;
    }

    public Id edgeLabelId() {
        return this.edgeLabelId;
    }

    public Id subLabelId() {
        return this.subLabelId;
    }

    public Directions direction() {
        return this.direction;
    }

    public byte directionCode() {
        return directionToCode(this.direction);
    }

    public String sortValues() {
        return this.sortValues;
    }

    public Id otherVertexId() {
        return this.otherVertexId;
    }

    @Override
    public Object asObject() {
        return this.asString();
    }

    @Override
    public String asString() {
        if (this.cache != null) {
            return this.cache;
        }
        this.cache = this.formatString();
        return this.cache;
    }

    /**
     * Format the shared edge identity without retaining a heap cache. Read
     * components through accessors so storage adapters can keep them off heap.
     */
    protected final String formatString() {
        Id owner = this.ownerVertexId();
        Id other = this.otherVertexId();
        if (this.directed) {
            return SplicingIdGenerator.concat(
                    IdUtil.writeString(owner),
                    this.direction().type().string(),
                    IdUtil.writeLong(this.edgeLabelId()),
                    IdUtil.writeLong(this.subLabelId()),
                    this.sortValues(),
                    IdUtil.writeString(other));
        }
        Id source = this.direction() == Directions.OUT ? owner : other;
        Id target = this.direction() == Directions.OUT ? other : owner;
        return SplicingIdGenerator.concat(
                IdUtil.writeString(source),
                IdUtil.writeLong(this.edgeLabelId()),
                IdUtil.writeLong(this.subLabelId()),
                this.sortValues(),
                IdUtil.writeString(target));
    }

    @Override
    public long asLong() {
        throw new UnsupportedOperationException();
    }

    @Override
    public byte[] asBytes() {
        return StringEncoding.encode(this.asString());
    }

    @Override
    public int length() {
        return this.asString().length();
    }

    @Override
    public IdType type() {
        return IdType.EDGE;
    }

    @Override
    public int compareTo(Id other) {
        return this.asString().compareTo(other.asString());
    }

    @Override
    public int hashCode() {
        if (this.directed) {
            return this.ownerVertexId.hashCode() ^
                   this.direction.hashCode() ^
                   this.edgeLabelId.hashCode() ^
                   this.subLabelId.hashCode() ^
                   this.sortValues.hashCode() ^
                   this.otherVertexId.hashCode();
        } else {
            return this.sourceVertexId().hashCode() ^
                   this.edgeLabelId.hashCode() ^
                   this.subLabelId.hashCode() ^
                   this.sortValues.hashCode() ^
                   this.targetVertexId().hashCode();
        }
    }

    @Override
    public boolean equals(Object object) {
        if (!(object instanceof EdgeId)) {
            return false;
        }
        EdgeId other = (EdgeId) object;
        if (this.directed) {
            return this.ownerVertexId.equals(other.ownerVertexId) &&
                   this.direction == other.direction &&
                   this.edgeLabelId.equals(other.edgeLabelId) &&
                   this.subLabelId.equals(other.subLabelId) &&
                   this.sortValues.equals(other.sortValues) &&
                   this.otherVertexId.equals(other.otherVertexId);
        } else {
            return this.sourceVertexId().equals(other.sourceVertexId()) &&
                   this.edgeLabelId.equals(other.edgeLabelId) &&
                   this.subLabelId.equals(other.subLabelId) &&
                   this.sortValues.equals(other.sortValues) &&
                   this.targetVertexId().equals(other.targetVertexId());
        }
    }

    @Override
    public String toString() {
        return this.asString();
    }

    public static byte directionToCode(Directions direction) {
        return direction.type().code();
    }

    public static Directions directionFromCode(byte code) {
        return Directions.convert(HugeType.fromCode(code));
    }

    public static boolean isOutDirectionFromCode(byte code) {
        return code == HugeType.EDGE_OUT.code();
    }

    public static EdgeId parse(String id) throws NotFoundException {
        return parse(id, false);
    }

    public static EdgeId parse(String id, boolean returnNullIfError)
            throws NotFoundException {
        String[] idParts = SplicingIdGenerator.split(id);
        if (!(idParts.length == 5 || idParts.length == 6)) {
            if (returnNullIfError) {
                return null;
            }
            throw new NotFoundException("Edge id must be formatted as 5~6 " +
                                        "parts, but got %s parts: '%s'",
                                        idParts.length, id);
        }
        try {
            if (idParts.length == 5) {
                Id ownerVertexId = IdUtil.readString(idParts[0]);
                Id edgeLabelId = IdUtil.readLong(idParts[1]);
                Id subLabelId = IdUtil.readLong(idParts[2]);
                String sortValues = idParts[3];
                Id otherVertexId = IdUtil.readString(idParts[4]);
                return new EdgeId(ownerVertexId, Directions.OUT, edgeLabelId, subLabelId,
                                  sortValues, otherVertexId);
            } else {
                assert idParts.length == 6;
                Id ownerVertexId = IdUtil.readString(idParts[0]);
                HugeType direction = HugeType.fromString(idParts[1]);
                Id edgeLabelId = IdUtil.readLong(idParts[2]);
                Id subLabelId = IdUtil.readLong(idParts[3]);
                String sortValues = idParts[4];
                Id otherVertexId = IdUtil.readString(idParts[5]);
                return new EdgeId(ownerVertexId, Directions.convert(direction),
                                  edgeLabelId, subLabelId, sortValues, otherVertexId);
            }
        } catch (Throwable e) {
            if (returnNullIfError) {
                return null;
            }
            throw new NotFoundException("Invalid format of edge id '%s'",
                                        e, id);
        }
    }

    public static Id parseStoredString(String id) {
        String[] idParts = split(id);
        E.checkArgument(idParts.length == 5, "Invalid id format: %s", id);
        Id ownerVertexId = IdUtil.readStoredString(idParts[0]);
        Id edgeLabelId = IdGenerator.ofStoredString(idParts[1], IdType.LONG);
        Id subLabelId = IdGenerator.ofStoredString(idParts[2], IdType.LONG);
        String sortValues = idParts[3];
        Id otherVertexId = IdUtil.readStoredString(idParts[4]);
        return new EdgeId(ownerVertexId, Directions.OUT, edgeLabelId, subLabelId,
                          sortValues, otherVertexId);
    }

    public static String asStoredString(Id id) {
        EdgeId eid = (EdgeId) id;
        return SplicingIdGenerator.concat(
                IdUtil.writeStoredString(eid.sourceVertexId()),
                IdGenerator.asStoredString(eid.edgeLabelId()),
                IdGenerator.asStoredString(eid.subLabelId()),
                eid.sortValues(),
                IdUtil.writeStoredString(eid.targetVertexId()));
    }

    public static String concat(String... ids) {
        return SplicingIdGenerator.concat(ids);
    }

    public static String[] split(Id id) {
        return EdgeId.split(id.asString());
    }

    public static String[] split(String id) {
        return SplicingIdGenerator.split(id);
    }
}
