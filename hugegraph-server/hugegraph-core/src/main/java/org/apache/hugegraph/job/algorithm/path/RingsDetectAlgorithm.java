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

package org.apache.hugegraph.job.algorithm.path;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.job.UserJob;
import org.apache.hugegraph.job.algorithm.AbstractAlgorithm;
import org.apache.hugegraph.job.algorithm.Consumers.StopExecution;
import org.apache.hugegraph.traversal.algorithm.SubGraphTraverser;
import org.apache.hugegraph.type.define.Directions;
import org.apache.hugegraph.util.JsonUtil;
import org.apache.hugegraph.util.ParameterUtil;

public class RingsDetectAlgorithm extends AbstractAlgorithm {

    public static final String ALGO_NAME = "rings";

    public static final String KEY_COUNT_ONLY = "count_only";

    @Override
    public String category() {
        return CATEGORY_PATH;
    }

    @Override
    public String name() {
        return ALGO_NAME;
    }

    @Override
    public void checkParameters(Map<String, Object> parameters) {
        depth(parameters);
        degree(parameters);
        eachLimit(parameters);
        limit(parameters);
        sourceLabel(parameters);
        sourceCLabel(parameters);
        direction(parameters);
        edgeLabel(parameters);
        countOnly(parameters);
        workers(parameters);
    }

    @Override
    public Object call(UserJob<Object> job, Map<String, Object> parameters) {
        int workers = workers(parameters);
        try (Traverser traverser = new Traverser(job, workers)) {
            return traverser.rings(sourceLabel(parameters),
                                   sourceCLabel(parameters),
                                   direction(parameters),
                                   edgeLabel(parameters),
                                   depth(parameters),
                                   degree(parameters),
                                   eachLimit(parameters),
                                   limit(parameters),
                                   countOnly(parameters));
        }
    }

    protected boolean countOnly(Map<String, Object> parameters) {
        if (!parameters.containsKey(KEY_COUNT_ONLY)) {
            return false;
        }
        return ParameterUtil.parameterBoolean(parameters, KEY_COUNT_ONLY);
    }

    private static class Traverser extends AlgoTraverser {

        public Traverser(UserJob<Object> job, int workers) {
            super(job, ALGO_NAME, workers);
        }

        public Object rings(String sourceLabel, String sourceCLabel,
                            Directions dir, String label, int depth,
                            long degree, long eachLimit, long limit,
                            boolean countOnly) {
            JsonMap ringsJson = new JsonMap();
            ringsJson.startObject();
            if (countOnly) {
                ringsJson.appendKey("rings_count");
            } else {
                ringsJson.appendKey("rings");
                ringsJson.startList();
            }

            SubGraphTraverser traverser = new SubGraphTraverser(this.graph());
            AtomicLong count = new AtomicLong(0L);

            this.traverse(sourceLabel, sourceCLabel, v -> {
                Id source = (Id) v.id();
                PathSet rings = traverser.rings(source, dir, label, depth,
                                                true, degree, MAX_CAPACITY,
                                                eachLimit);
                assert eachLimit == NO_LIMIT || rings.size() <= eachLimit;
                for (Path ring : rings) {
                    if (eachLimit == NO_LIMIT && !ring.ownedBy(source)) {
                        // Only dedup rings when each_limit!=NO_LIMIT
                        continue;
                    }

                    if (count.incrementAndGet() > limit && limit != NO_LIMIT) {
                        throw new StopExecution("exceed limit %s", limit);
                    }
                    if (!countOnly) {
                        String ringJson = JsonUtil.toJson(ring.vertices());
                        synchronized (ringsJson) {
                            ringsJson.appendRaw(ringJson);
                        }
                    }
                }
            });

            if (countOnly) {
                long counted = count.get();
                if (limit != NO_LIMIT && counted > limit) {
                    // The count increased by multi threads and exceed limit
                    counted = limit;
                }
                ringsJson.append(counted);
            } else {
                ringsJson.endList();
            }
            ringsJson.endObject();

            return ringsJson.asJson();
        }
    }
}
