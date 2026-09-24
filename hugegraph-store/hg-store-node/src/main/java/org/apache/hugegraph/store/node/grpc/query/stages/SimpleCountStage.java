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

import static org.apache.hugegraph.store.node.grpc.query.QueryUtil.EMPTY_AGG_KEY;

import java.util.ArrayList;
import java.util.List;

import org.apache.hugegraph.store.node.grpc.query.QueryStage;
import org.apache.hugegraph.store.node.grpc.query.model.PipelineResult;
import org.apache.hugegraph.store.util.MultiKv;

/**
 * Simple count counting
 */
public class SimpleCountStage implements QueryStage {

    private int aggCount = 0;

    @Override
    public void init(Object... objects) {
        this.aggCount = (int) objects[0];
    }

    @Override
    public PipelineResult handle(PipelineResult result) {
        if (result.isEmpty()) {
            return result;
        }

        MultiKv multiKv = new MultiKv(EMPTY_AGG_KEY, createArray(aggCount));
        return new PipelineResult(multiKv);
    }

    @Override
    public String getName() {
        return "SIMPLE_COUNT_STAGE";
    }

    public List<Object> createArray(int count) {
        List<Object> list = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            list.add(0L);
        }
        return list;
    }
}
