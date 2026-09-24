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

package org.apache.hugegraph.store.node.grpc.query.stages;

import static org.apache.hugegraph.store.constant.HugeServerTables.OLAP_TABLE;

import java.util.ArrayList;
import java.util.List;

import org.apache.hugegraph.backend.BackendColumn;
import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.pd.common.PartitionUtils;
import org.apache.hugegraph.rocksdb.access.RocksDBSession;
import org.apache.hugegraph.serializer.BinaryElementSerializer;
import org.apache.hugegraph.serializer.BytesBuffer;
import org.apache.hugegraph.store.business.BusinessHandler;
import org.apache.hugegraph.store.node.grpc.query.QueryStage;
import org.apache.hugegraph.store.node.grpc.query.QueryUtil;
import org.apache.hugegraph.store.node.grpc.query.model.PipelineResult;
import org.apache.hugegraph.store.node.grpc.query.model.PipelineResultType;
import org.apache.hugegraph.structure.BaseVertex;

import com.google.protobuf.ByteString;

import lombok.extern.slf4j.Slf4j;

/**
 * OLAP query
 */
@Slf4j
public class OlapStage implements QueryStage {

    private final BusinessHandler handler = new QueryUtil().getHandler();
    private final BinaryElementSerializer serializer = new BinaryElementSerializer();
    private String graph;
    private String table;
    private List<Id> properties;

    @Override
    public void init(Object... objects) {
        this.graph = (String) objects[0];
        this.table = (String) objects[1];
        this.properties = QueryUtil.fromStringBytes((List<ByteString>) objects[2]);
    }

    @Override
    public PipelineResult handle(PipelineResult result) {
        if (result == null) {
            return null;
        }

        if (result.getResultType() == PipelineResultType.HG_ELEMENT) {
            var element = result.getElement();
            var code =
                    PartitionUtils.calcHashcode(BinaryElementSerializer.ownerId(element).asBytes());

            for (Id property : properties) {
                // Build key
                var key = getOlapKey(property, element.id());
                var values = handler.doGet(this.graph, code, OLAP_TABLE, key);
                if (values != null) {
                    var column = BackendColumn.of(key, values);
                    QueryUtil.parseOlap(column, (BaseVertex) element);
                }
            }
        } else if (result.getResultType() == PipelineResultType.BACKEND_COLUMN) {
            var column = result.getColumn();
            try {
                var vertexOnlyId =
                        serializer.parseVertex(null, BackendColumn.of(column.name, null), null);
                var code = PartitionUtils.calcHashcode(
                        BinaryElementSerializer.ownerId(vertexOnlyId).asBytes());
                // todo: Wait for structure to change to byte[] operations
                var list = new ArrayList<BackendColumn>();
                for (Id property : properties) {
                    var key = getOlapKey(property, vertexOnlyId.id());
                    var values = handler.doGet(this.graph, code, OLAP_TABLE, key);
                    if (values != null) {
                        list.add(BackendColumn.of(key, values));
                    }
                }
                var vertex =
                        QueryUtil.combineColumn(BackendColumn.of(column.name, column.value), list);
                result.setColumn(RocksDBSession.BackendColumn.of(vertex.name, vertex.value));
            } catch (Exception e) {
                log.error("parse olap error, graph: {}, table : {}", graph, table, e);
                return null;
            }
        }
        return result;
    }

    private byte[] getOlapKey(Id propertyId, Id vertexId) {
        BytesBuffer bufferName =
                BytesBuffer.allocate(1 + propertyId.length() + 1 + vertexId.length());
        bufferName.writeId(propertyId);
        return bufferName.writeId(vertexId).bytes();
    }

    @Override
    public String getName() {
        return "OLAP_STAGE";
    }

    @Override
    public void close() {
        this.properties.clear();
    }
}
