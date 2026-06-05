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

package org.apache.hugegraph.api.traversers;

import static org.apache.hugegraph.traversal.algorithm.HugeTraverser.DEFAULT_MAX_DEGREE;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.api.API;
import org.apache.hugegraph.traversal.algorithm.steps.EdgeStep;
import org.apache.hugegraph.traversal.algorithm.steps.Steps;
import org.apache.hugegraph.type.define.Directions;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.swagger.v3.oas.annotations.media.Schema;

public class TraverserAPI extends API {

    protected static EdgeStep step(HugeGraph graph, Step step) {
        return new EdgeStep(graph, step.direction, step.labels, step.properties,
                            step.maxDegree, step.skipDegree);
    }

    protected static Steps steps(HugeGraph graph, VESteps steps) {
        Map<String, Map<String, Object>> vSteps = new HashMap<>();
        if (steps.vSteps != null) {
            for (VEStepEntity vStep : steps.vSteps) {
                vSteps.put(vStep.label, vStep.properties);
            }
        }

        Map<String, Map<String, Object>> eSteps = new HashMap<>();
        if (steps.eSteps != null) {
            for (VEStepEntity eStep : steps.eSteps) {
                eSteps.put(eStep.label, eStep.properties);
            }
        }

        return new Steps(graph, steps.direction, vSteps, eSteps,
                         steps.maxDegree, steps.skipDegree);
    }

    protected static class Step {

        @JsonProperty("direction")
        @Schema(description = "The direction of traversal", example = "BOTH")
        public Directions direction;
        @JsonProperty("labels")
        @Schema(description = "The edge labels to traverse")
        public List<String> labels;
        @JsonProperty("properties")
        @Schema(description = "The properties to filter edges")
        public Map<String, Object> properties;
        @JsonAlias("degree")
        @JsonProperty("max_degree")
        @Schema(description = "The maximum degree of vertices to traverse")
        public long maxDegree = Long.parseLong(DEFAULT_MAX_DEGREE);
        @JsonProperty("skip_degree")
        @Schema(description = "The degree to skip when traversing")
        public long skipDegree = 0L;

        @Override
        public String toString() {
            return String.format("Step{direction=%s,labels=%s,properties=%s," +
                                 "maxDegree=%s,skipDegree=%s}",
                                 this.direction, this.labels, this.properties,
                                 this.maxDegree, this.skipDegree);
        }
    }

    protected static class VEStepEntity {

        @JsonProperty("label")
        @Schema(description = "The label of the step")
        public String label;

        @JsonProperty("properties")
        @Schema(description = "The properties for the step")
        public Map<String, Object> properties;

        @Override
        public String toString() {
            return String.format("VEStepEntity{label=%s,properties=%s}",
                                 this.label, this.properties);
        }
    }

    protected static class VESteps {

        @JsonProperty("direction")
        @Schema(description = "The direction of traversal", example = "BOTH")
        public Directions direction;
        @JsonAlias("degree")
        @JsonProperty("max_degree")
        @Schema(description = "The maximum degree of vertices to traverse")
        public long maxDegree = Long.parseLong(DEFAULT_MAX_DEGREE);
        @JsonProperty("skip_degree")
        @Schema(description = "The degree to skip when traversing")
        public long skipDegree = 0L;
        @JsonProperty("vertex_steps")
        @Schema(description = "The vertex steps in the traversal")
        public List<VEStepEntity> vSteps;
        @JsonProperty("edge_steps")
        @Schema(description = "The edge steps in the traversal")
        public List<VEStepEntity> eSteps;

        @Override
        public String toString() {
            return String.format("Steps{direction=%s,maxDegree=%s," +
                                 "skipDegree=%s,vSteps=%s,eSteps=%s}",
                                 this.direction, this.maxDegree,
                                 this.skipDegree, this.vSteps, this.eSteps);
        }
    }
}
