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

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.store.node.grpc.query.QueryStage;
import org.apache.hugegraph.store.node.grpc.query.QueryUtil;
import org.apache.hugegraph.store.node.grpc.query.model.PipelineResult;
import org.apache.hugegraph.store.node.grpc.query.model.PipelineResultType;

import com.google.protobuf.ByteString;

/**
 * Pruning
 */
public class ProjectionStage implements QueryStage {

    private Set<Id> propertySet;

    private boolean removeAllProperty;

    @Override
    public void init(Object... objects) {
        this.propertySet = new HashSet<>(QueryUtil.fromStringBytes((List<ByteString>) objects[0]));
        this.removeAllProperty = (Boolean) objects[1];
    }

    @Override
    public PipelineResult handle(PipelineResult result) {
        if (result == null) {
            return null;
        }

        if (result.getResultType() == PipelineResultType.HG_ELEMENT) {
            var element = result.getElement();
            for (var id : element.getProperties().entrySet()) {
                if (!this.propertySet.contains(id.getKey()) || this.removeAllProperty) {
                    element.removeProperty(id.getKey());
                }
            }
            return result;
        } else if (result.getResultType() == PipelineResultType.BACKEND_COLUMN &&
                   this.removeAllProperty) {
            var column = result.getColumn();
            column.value = new byte[0];
        }
        return result;
    }

    @Override
    public String getName() {
        return "PROJECTION_STAGE";
    }

    @Override
    public void close() {
        this.propertySet.clear();
    }
}
