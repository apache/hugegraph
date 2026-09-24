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

import java.util.Objects;
import java.util.Random;

import org.apache.hugegraph.store.node.grpc.query.QueryStage;
import org.apache.hugegraph.store.node.grpc.query.model.PipelineResult;

/**
 * Sampling
 */
public class SampleStage implements QueryStage {

    private double factor;

    private Random rand;

    @Override
    public void init(Object... objects) {
        factor = (double) objects[0];
        rand = new Random(System.currentTimeMillis());
    }

    @Override
    public PipelineResult handle(PipelineResult result) {
        if (Objects.equals(result, PipelineResult.EMPTY) || rand.nextDouble() <= this.factor) {
            return result;
        }

        return null;
    }

    @Override
    public String getName() {
        return "SAMPLE_STAGE";
    }
}
