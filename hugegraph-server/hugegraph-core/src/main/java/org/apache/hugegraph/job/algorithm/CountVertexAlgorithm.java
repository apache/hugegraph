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

package org.apache.hugegraph.job.algorithm;

import java.util.Map;

import org.apache.hugegraph.job.UserJob;

public class CountVertexAlgorithm extends AbstractAlgorithm {

    @Override
    public String name() {
        return "count_vertex";
    }

    @Override
    public String category() {
        return CATEGORY_AGGR;
    }

    @Override
    public Object call(UserJob<Object> job, Map<String, Object> parameters) {
        try (Traverser traverser = new Traverser(job)) {
            return traverser.count();
        }
    }

    private static class Traverser extends AlgoTraverser {

        public Traverser(UserJob<Object> job) {
            super(job);
        }

        public Object count() {
            return this.countElements(this.vertices());
        }
    }
}
