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

package org.apache.hugegraph.traversal.algorithm.steps;

import static org.apache.hugegraph.traversal.algorithm.HugeTraverser.DEFAULT_MAX_DEGREE;
import static org.apache.hugegraph.traversal.algorithm.HugeTraverser.DEFAULT_SAMPLE;
import static org.apache.hugegraph.traversal.algorithm.HugeTraverser.NO_LIMIT;

import java.util.List;
import java.util.Map;

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.struct.schema.PropertyKey;
import org.apache.hugegraph.type.define.Directions;
import org.apache.hugegraph.util.E;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class WeightedEdgeStep {

    private final EdgeStep edgeStep;
    private final PropertyKey weightBy;
    private final double defaultWeight;
    private final long sample;

    public WeightedEdgeStep(HugeGraph g, Directions direction) {
        this(g, direction, ImmutableList.of());
    }

    public WeightedEdgeStep(HugeGraph g, List<String> labels) {
        this(g, Directions.BOTH, labels);
    }

    public WeightedEdgeStep(HugeGraph g, Map<String, Object> properties) {
        this(g, Directions.BOTH, ImmutableList.of(), properties);
    }

    public WeightedEdgeStep(HugeGraph g, Directions direction, List<String> labels) {
        this(g, direction, labels, ImmutableMap.of());
    }

    public WeightedEdgeStep(HugeGraph g, Directions direction, List<String> labels,
                            Map<String, Object> properties) {
        this(g, direction, labels, properties,
             Long.parseLong(DEFAULT_MAX_DEGREE), 0L, null, 0.0D,
             Long.parseLong(DEFAULT_SAMPLE));
    }

    public WeightedEdgeStep(HugeGraph g, Directions direction, List<String> labels,
                            Map<String, Object> properties,
                            long maxDegree, long skipDegree,
                            String weightBy, double defaultWeight, long sample) {
        E.checkArgument(sample > 0L || sample == NO_LIMIT,
                        "The sample must be > 0 or == -1, but got: %s",
                        sample);
        E.checkArgument(maxDegree == NO_LIMIT || maxDegree >= sample,
                        "The max degree must be greater than or equal to " +
                        "sample, but got max degree %s and sample %s",
                        maxDegree, sample);

        this.edgeStep = new EdgeStep(g, direction, labels, properties,
                                     maxDegree, skipDegree);
        if (weightBy != null) {
            this.weightBy = g.propertyKey(weightBy);
        } else {
            this.weightBy = null;
        }
        this.defaultWeight = defaultWeight;
        this.sample = sample;
    }

    public EdgeStep step() {
        return this.edgeStep;
    }

    public PropertyKey weightBy() {
        return this.weightBy;
    }

    public double defaultWeight() {
        return this.defaultWeight;
    }

    public long sample() {
        return this.sample;
    }
}
