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

import java.util.List;
import java.util.stream.Collectors;

import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.store.node.grpc.query.QueryStage;
import org.apache.hugegraph.store.node.grpc.query.QueryUtil;
import org.apache.hugegraph.store.node.grpc.query.model.PipelineResult;
import org.apache.hugegraph.store.node.grpc.query.model.PipelineResultType;
import org.apache.hugegraph.store.util.MultiKv;
import org.apache.hugegraph.structure.BaseElement;

import com.google.protobuf.ByteString;

/**
 * Extract fields required by aggregation functions
 */
public class ExtractAggFieldStage implements QueryStage {

    private List<Id> groupBys;

    private List<Id> fields;

    private boolean groupByElementSchemaId;
    private boolean isVertex;

    /**
     * Initialization function for initializing objects
     *
     * @param objects object array
     */
    @Override
    public void init(Object... objects) {
        // Group by follows the order of properties, facilitating subsequent pruning
        this.groupBys = QueryUtil.fromStringBytes((List<ByteString>) objects[0]);
        this.fields = QueryUtil.fromStringBytes((List<ByteString>) objects[1]);
        this.groupByElementSchemaId = (boolean) objects[2];
        this.isVertex = (boolean) objects[3];
    }

    /**
     * Override parent class method handle for processing PipelineResult results
     *
     * @param result PipelineResult result object
     * @return return the processed PipelineResult result object
     */
    @Override
    public PipelineResult handle(PipelineResult result) {
        if (result == null) {
            return null;
        }

        if (this.groupByElementSchemaId && !result.isEmpty()) {
            return new PipelineResult(MultiKv.of(List.of(QueryUtil.getLabelId(result.getColumn(),
                                                                              this.isVertex)),
                                                 List.of(1L)));
        } else if (result.getResultType() == PipelineResultType.HG_ELEMENT) {
            var element = result.getElement();
            return new PipelineResult(MultiKv.of(getFields(this.groupBys, element),
                                                 getFields(this.fields, element)));
        }
        return result;
    }

    private List<Object> getFields(List<Id> ids, BaseElement element) {
        return ids.stream()
                  .map(id -> id == null ? null : element.getPropertyValue(id))
                  .collect(Collectors.toList());
    }

    private List<Object> getSchemaId(BaseElement element) {
        return List.of(element.schemaLabel().id().asLong());
    }

    @Override
    public String getName() {
        return "EXTRACT_AGG_FIELD_STAGE";
    }

    @Override
    public void close() {
        this.fields.clear();
        this.groupBys.clear();
    }
}
