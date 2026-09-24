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

package org.apache.hugegraph.store.node.grpc.query;

import org.apache.hugegraph.store.node.grpc.query.stages.AggStage;
import org.apache.hugegraph.store.node.grpc.query.stages.DeserializationStage;
import org.apache.hugegraph.store.node.grpc.query.stages.ExtractAggFieldStage;
import org.apache.hugegraph.store.node.grpc.query.stages.FilterStage;
import org.apache.hugegraph.store.node.grpc.query.stages.LimitStage;
import org.apache.hugegraph.store.node.grpc.query.stages.OlapStage;
import org.apache.hugegraph.store.node.grpc.query.stages.OrderByStage;
import org.apache.hugegraph.store.node.grpc.query.stages.ProjectionStage;
import org.apache.hugegraph.store.node.grpc.query.stages.SampleStage;
import org.apache.hugegraph.store.node.grpc.query.stages.SimpleCountStage;
import org.apache.hugegraph.store.node.grpc.query.stages.StopStage;
import org.apache.hugegraph.store.node.grpc.query.stages.TopStage;
import org.apache.hugegraph.store.node.grpc.query.stages.TtlCheckStage;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class QueryStages {

    public static QueryStage ofFilterStage() {
        return new FilterStage();
    }

    public static QueryStage ofProjectionStage() {
        return new ProjectionStage();
    }

    public static QueryStage ofDeserializationStage() {
        return new DeserializationStage();
    }

    public static QueryStage ofOlapStage() {
        return new OlapStage();
    }

    public static QueryStage ofExtractAggFieldStage() {
        return new ExtractAggFieldStage();
    }

    public static QueryStage ofAggStage() {
        return new AggStage();
    }

    public static QueryStage ofOrderByStage() {
        return new OrderByStage();
    }

    public static QueryStage ofLimitStage() {
        return new LimitStage();
    }

    public static QueryStage ofSampleStage() {
        return new SampleStage();
    }

    public static QueryStage ofSimpleCountStage() {
        return new SimpleCountStage();
    }

    public static QueryStage ofStopStage() {
        return new StopStage();
    }

    public static QueryStage ofTopStage() {
        return new TopStage();
    }

    public static QueryStage ofTtlCheckStage() {
        return new TtlCheckStage();
    }
}
