/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hugegraph.traversal.optimize;

import java.util.LinkedList;
import java.util.List;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy.ProviderOptimizationStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.Mutating;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.AddEdgeStep;
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
            }

            if (curAddStep == null && step instanceof AddVertexStep) {
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

                List<Object> kvList = new LinkedList<>();

                propertyStep.getParameters().getRaw().forEach((k, v) -> {
                    if (!T.key.equals(k) && !T.value.equals(k)) {
                        kvList.add(k.toString());
                        kvList.add(v.get(0));
                    }
                });

                if (!kvList.isEmpty()) {
                    applyParameters(curAddStep, kvList);
                }

                removeSteps.add(step);
            } else {
                curAddStep = null;
            }
        }

        for (Step step : removeSteps) {
            traversal.removeStep(step);
        }
    }

    private void applyParameters(Mutating step, List<Object> kvList) {

        Object[] kvs = kvList.toArray(new Object[0]);

        if (step instanceof AddVertexStep) {
            AddVertexStep<?> vertexStep = (AddVertexStep<?>) step;
            vertexStep.getParameters().set(null, kvs);

        } else if (step instanceof AddEdgeStep) {
            AddEdgeStep<?> edgeStep = (AddEdgeStep<?>) step;
            edgeStep.getParameters().set(null, kvs);
        }
    }
}
