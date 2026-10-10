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

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.id.EdgeId;
import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.id.IdGenerator;
import org.apache.hugegraph.backend.store.BackendEntry;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.struct.schema.EdgeLabel;
import org.apache.hugegraph.struct.schema.VertexLabel;
import org.apache.hugegraph.structure.HugeEdge;
import org.apache.hugegraph.structure.HugeEdgeProperty;
import org.apache.hugegraph.structure.HugeVertex;
import org.apache.hugegraph.type.define.HugeKeys;

/**
 * with father and sub edge label
 */
public abstract class TableSerializerV2 extends TableSerializer {

    public TableSerializerV2(HugeConfig config) {
        super(config);
    }

    @Override
    protected TableBackendEntry.Row formatEdge(HugeEdge edge) {
        EdgeId id = edge.idWithDirection();
        TableBackendEntry.Row row = new TableBackendEntry.Row(edge.type(), id);
        if (edge.hasTtl()) {
            row.ttl(edge.ttl());
            row.column(HugeKeys.EXPIRED_TIME, edge.expiredTime());
        }
        // Id: ownerVertex + direction + edge-label + sub-edge-label + sortValues + otherVertex
        row.column(HugeKeys.OWNER_VERTEX, this.writeId(id.ownerVertexId()));
        row.column(HugeKeys.DIRECTION, id.directionCode());
        row.column(HugeKeys.LABEL, id.edgeLabelId().asLong());
        row.column(HugeKeys.SUB_LABEL, id.subLabelId().asLong());
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
    @Override
    protected HugeEdge parseEdge(TableBackendEntry.Row row,
                                 HugeVertex vertex, HugeGraph graph) {
        Object ownerVertexId = row.column(HugeKeys.OWNER_VERTEX);
        Number dir = row.column(HugeKeys.DIRECTION);
        boolean direction = EdgeId.isOutDirectionFromCode(dir.byteValue());
        Number label = row.column(HugeKeys.LABEL);
        Number subLabel = row.column(HugeKeys.SUB_LABEL);
        String sortValues = row.column(HugeKeys.SORT_VALUES);
        Object otherVertexId = row.column(HugeKeys.OTHER_VERTEX);
        Number expiredTime = row.column(HugeKeys.EXPIRED_TIME);

        if (vertex == null) {
            Id ownerId = this.readId(ownerVertexId);
            vertex = new HugeVertex(graph, ownerId, VertexLabel.NONE);
        }

        EdgeLabel edgeLabel = graph.edgeLabelOrNone(this.toId(label));
        EdgeLabel subEdgeLabel = graph.edgeLabelOrNone(this.toId(subLabel));
        Id otherId = this.readId(otherVertexId);

        // Construct edge
        HugeEdge edge = HugeEdge.constructEdge(vertex, direction, subEdgeLabel,
                                               sortValues, otherId);

        // Parse edge properties
        this.parseProperties(edge, row);

        // The expired time is null when the edge is non-ttl
        long expired = edge.hasTtl() ? expiredTime.longValue() : 0L;
        edge.expiredTime(expired);

        return edge;
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
        // Id: ownerVertex + direction + edge-label + sub-edge-label + sortValues + otherVertex
        row.column(HugeKeys.OWNER_VERTEX, this.writeId(id.ownerVertexId()));
        row.column(HugeKeys.DIRECTION, id.directionCode());
        row.column(HugeKeys.LABEL, id.edgeLabelId().asLong());
        row.column(HugeKeys.SUB_LABEL, id.subLabelId().asLong());
        row.column(HugeKeys.SORT_VALUES, id.sortValues());
        row.column(HugeKeys.OTHER_VERTEX, this.writeId(id.otherVertexId()));

        // Format edge property
        this.formatProperty(prop, row);

        TableBackendEntry entry = newBackendEntry(row);
        entry.subId(IdGenerator.of(prop.key()));
        return entry;
    }
}
