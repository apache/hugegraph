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

package org.apache.hugegraph.traversal.optimize;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy.ProviderOptimizationStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.Mutating;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.AddVertexStartStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.AddVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.AddPropertyStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality;

public class HugePrimaryKeyStrategy
        extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy>
        implements ProviderOptimizationStrategy {

    private static final long serialVersionUID = 6307847098226016416L;
    private static final HugePrimaryKeyStrategy INSTANCE = new HugePrimaryKeyStrategy();

    public static HugePrimaryKeyStrategy instance() {
        return INSTANCE;
    }

    @Override
    public void apply(Traversal.Admin<?, ?> traversal) {

        List<Step> removeSteps = new LinkedList<>();
        Mutating curAddStep = null;
        List<Step> stepList = traversal.getSteps();

        for (int i = 0, s = stepList.size(); i < s; i++) {
            Step step = stepList.get(i);

            if (i == 0 && step instanceof AddVertexStartStep) {
                curAddStep = (Mutating) step;
                continue;
            } else if (curAddStep == null && (step) instanceof AddVertexStep) {
                curAddStep = (Mutating) step;
                continue;
            }

            if (curAddStep == null) {
                continue;
            }

            if (!(step instanceof AddPropertyStep)) {
                curAddStep = null;
                continue;
            }

            AddPropertyStep propertyStep = (AddPropertyStep) step;

            if (propertyStep.getCardinality() == Cardinality.single
                || propertyStep.getCardinality() == null) {

                Object[] kvs = new Object[2];
                boolean extraParams = false;

                for (Map.Entry<Object, List<Object>> param :
                     propertyStep.getParameters().getRaw().entrySet()) {
                    Object paramKey = param.getKey();
                    if (T.key.equals(paramKey)) {
                        kvs[0] = param.getValue().get(0);
                    } else if (T.value.equals(paramKey)) {
                        kvs[1] = param.getValue().get(0);
                    } else {
                        extraParams = true;
                    }
                }

                curAddStep.configure(kvs);

                if (extraParams) {
                    /*
                     * Rejecting here would run for every child traversal, so an
                     * unreachable coalesce()/choose() branch would fail too. Leave
                     * the step for HugeVertex.property() to reject when it really
                     * runs, and stop folding across it.
                     */
                    curAddStep = null;
                } else {
                    removeSteps.add(step);
                }
            } else {
                curAddStep = null;
            }

        }

        for (Step index : removeSteps) {
            traversal.removeStep(index);
        }
    }
}
