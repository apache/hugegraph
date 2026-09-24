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

import org.apache.hugegraph.serializer.DirectBinarySerializer;
import org.apache.hugegraph.store.node.grpc.query.QueryStage;
import org.apache.hugegraph.store.node.grpc.query.model.PipelineResult;
import org.apache.hugegraph.store.node.grpc.query.model.PipelineResultType;

import lombok.extern.slf4j.Slf4j;

/**
 * check element ttl
 */
@Slf4j
public class TtlCheckStage implements QueryStage {

    private boolean isVertex;

    private final DirectBinarySerializer serializer = new DirectBinarySerializer();
    private long now;

    @Override
    public void init(Object... objects) {
        this.isVertex = (boolean) objects[0];
        now = System.currentTimeMillis();
    }

    @Override
    public PipelineResult handle(PipelineResult result) {
        if (result.getResultType() == PipelineResultType.BACKEND_COLUMN) {
            var col = result.getColumn();
            try {
                var element = isVertex ? serializer.parseVertex(col.name, col.value) :
                              serializer.parseEdge(col.name, col.value);
                if (element.expiredTime() > 0 && element.expiredTime() < now) {
                    return null;
                }
            } catch (Exception e) {
                log.error("parse element error", e);
                return null;
            }
        }
        return result;
    }

    @Override
    public String getName() {
        return "TTL_CHECK_STAGE";
    }
}
