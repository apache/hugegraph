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

package org.apache.hugegraph.io;

import java.util.LinkedHashMap;
import java.util.Map;

import com.google.common.collect.ImmutableList;

import org.apache.hugegraph.HugeGraphSupplier;
import org.apache.hugegraph.struct.schema.EdgeLabel;
import org.apache.hugegraph.struct.schema.IndexLabel;
import org.apache.hugegraph.struct.schema.PropertyKey;
import org.apache.hugegraph.struct.schema.VertexLabel;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.type.define.EdgeLabelType;
import org.apache.hugegraph.type.define.HugeKeys;

public class GraphSONSchemaSerializer {

    public Map<HugeKeys, Object> writeVertexLabel(VertexLabel vertexLabel) {
        HugeGraphSupplier graph = vertexLabel.graph();
        assert graph != null;

        Map<HugeKeys, Object> map = new LinkedHashMap<>();
        map.put(HugeKeys.ID, vertexLabel.id().asLong());
        map.put(HugeKeys.NAME, vertexLabel.name());
        map.put(HugeKeys.ID_STRATEGY, vertexLabel.idStrategy());
        map.put(HugeKeys.PRIMARY_KEYS,
                graph.mapPkId2Name(vertexLabel.primaryKeys()));
        map.put(HugeKeys.NULLABLE_KEYS,
                graph.mapPkId2Name(vertexLabel.nullableKeys()));
        map.put(HugeKeys.INDEX_LABELS,
                graph.mapIlId2Name(vertexLabel.indexLabels()));
        map.put(HugeKeys.PROPERTIES,
                graph.mapPkId2Name(vertexLabel.properties()));
        map.put(HugeKeys.STATUS, vertexLabel.status());
        map.put(HugeKeys.TTL, vertexLabel.ttl());
        String ttlStartTimeName = vertexLabel.ttlStartTimeName();
        if (ttlStartTimeName != null) {
            map.put(HugeKeys.TTL_START_TIME, ttlStartTimeName);
        }
        map.put(HugeKeys.ENABLE_LABEL_INDEX, vertexLabel.enableLabelIndex());
        map.put(HugeKeys.USER_DATA, vertexLabel.userdata());
        return map;
    }

    public Map<HugeKeys, Object> writeEdgeLabel(EdgeLabel edgeLabel) {
        HugeGraphSupplier graph = edgeLabel.graph();
        assert graph != null;

        Map<HugeKeys, Object> map = new LinkedHashMap<>();
        map.put(HugeKeys.ID, edgeLabel.id().asLong());
        map.put(HugeKeys.NAME, edgeLabel.name());
        if (edgeLabel.isFather()) {
            map.put(HugeKeys.EDGELABEL_TYPE, EdgeLabelType.PARENT);
            if (edgeLabel.links().size() > 0) {
                map.put(HugeKeys.LINKS,
                        graph.mapPairId2Name(edgeLabel.links()));
            }
        } else if (edgeLabel.hasFather()) {
            map.put(HugeKeys.EDGELABEL_TYPE, EdgeLabelType.SUB);
            map.put(HugeKeys.PARENT_LABEL,
                    graph.mapElId2Name(ImmutableList.of(edgeLabel.fatherId()))
                         .get(0));
        } else {
            map.put(HugeKeys.EDGELABEL_TYPE, edgeLabel.edgeLabelType());
        }

        if (!edgeLabel.isFather()) {
            map.put(HugeKeys.SOURCE_LABEL, edgeLabel.sourceLabelName());
            map.put(HugeKeys.TARGET_LABEL, edgeLabel.targetLabelName());
            map.put(HugeKeys.LINKS,
                    graph.mapPairId2Name(edgeLabel.links()));
            map.put(HugeKeys.FREQUENCY, edgeLabel.frequency());
            map.put(HugeKeys.SORT_KEYS,
                    graph.mapPkId2Name(edgeLabel.sortKeys()));
            map.put(HugeKeys.NULLABLE_KEYS,
                    graph.mapPkId2Name(edgeLabel.nullableKeys()));
            map.put(HugeKeys.INDEX_LABELS,
                    graph.mapIlId2Name(edgeLabel.indexLabels()));
            map.put(HugeKeys.PROPERTIES,
                    graph.mapPkId2Name(edgeLabel.properties()));
            map.put(HugeKeys.STATUS, edgeLabel.status());
            map.put(HugeKeys.TTL, edgeLabel.ttl());
            String ttlStartTimeName = edgeLabel.ttlStartTimeName();
            if (ttlStartTimeName != null) {
                map.put(HugeKeys.TTL_START_TIME, ttlStartTimeName);
            }
            map.put(HugeKeys.ENABLE_LABEL_INDEX, edgeLabel.enableLabelIndex());
            map.put(HugeKeys.USER_DATA, edgeLabel.userdata());
        }

        return map;
    }

    public Map<HugeKeys, Object> writePropertyKey(PropertyKey propertyKey) {
        HugeGraphSupplier graph = propertyKey.graph();
        assert graph != null;

        Map<HugeKeys, Object> map = new LinkedHashMap<>();
        map.put(HugeKeys.ID, propertyKey.id().asLong());
        map.put(HugeKeys.NAME, propertyKey.name());
        map.put(HugeKeys.DATA_TYPE, propertyKey.dataType());
        map.put(HugeKeys.CARDINALITY, propertyKey.cardinality());
        map.put(HugeKeys.AGGREGATE_TYPE, propertyKey.aggregateType());
        map.put(HugeKeys.WRITE_TYPE, propertyKey.writeType());
        map.put(HugeKeys.PROPERTIES,
                graph.mapPkId2Name(propertyKey.properties()));
        map.put(HugeKeys.STATUS, propertyKey.status());
        map.put(HugeKeys.USER_DATA, propertyKey.userdata());
        return map;
    }

    public Map<HugeKeys, Object> writeIndexLabel(IndexLabel indexLabel) {
        HugeGraphSupplier graph = indexLabel.graph();
        assert graph != null;

        Map<HugeKeys, Object> map = new LinkedHashMap<>();
        map.put(HugeKeys.ID, indexLabel.id().asLong());
        map.put(HugeKeys.NAME, indexLabel.name());
        map.put(HugeKeys.BASE_TYPE, indexLabel.baseType());
        if (indexLabel.baseType() == HugeType.VERTEX_LABEL) {
            map.put(HugeKeys.BASE_VALUE,
                    graph.vertexLabel(indexLabel.baseValue()).name());
        } else {
            assert indexLabel.baseType() == HugeType.EDGE_LABEL;
            map.put(HugeKeys.BASE_VALUE,
                    graph.edgeLabel(indexLabel.baseValue()).name());
        }
        map.put(HugeKeys.INDEX_TYPE, indexLabel.indexType());
        map.put(HugeKeys.FIELDS, graph.mapPkId2Name(indexLabel.indexFields()));
        map.put(HugeKeys.STATUS, indexLabel.status());
        map.put(HugeKeys.USER_DATA, indexLabel.userdata());
        return map;
    }
}
