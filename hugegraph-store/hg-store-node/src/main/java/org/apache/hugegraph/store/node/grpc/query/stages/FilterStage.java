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

import org.apache.hugegraph.query.ConditionQuery;
import org.apache.hugegraph.store.node.grpc.query.QueryStage;
import org.apache.hugegraph.store.node.grpc.query.model.PipelineResult;

/**
 * Filter
 */
public class FilterStage implements QueryStage {

    private ConditionQuery conditionQUery;

    @Override
    public void init(Object... objects) {
        this.conditionQUery = ConditionQuery.fromBytes((byte[]) objects[0]);
    }

    @Override
    public PipelineResult handle(PipelineResult result) {
        if (result == null || result.isEmpty()) {
            return result;
        }

        if (result.getElement() == null) {
            return null;
        }

        if (conditionQUery.resultType().isVertex() || conditionQUery.resultType().isEdge()) {
            if (!conditionQUery.test(result.getElement())) {
                return null;
            }
        }
        return result;
    }

    @Override
    public String getName() {
        return "FILTER_STAGE";
    }
}
