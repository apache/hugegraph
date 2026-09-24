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

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.AndStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.OrStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.InlineFilterStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

public final class HugeConnectiveLabelStepStrategy
        extends AbstractTraversalStrategy<TraversalStrategy.OptimizationStrategy>
        implements TraversalStrategy.OptimizationStrategy {

    private static final long serialVersionUID = 2532355470697047377L;

    private static final HugeConnectiveLabelStepStrategy INSTANCE =
            new HugeConnectiveLabelStepStrategy();

    private HugeConnectiveLabelStepStrategy() {
        // pass
    }

    @Override
    public void apply(Traversal.Admin<?, ?> traversal) {
        Set<Traversal.Admin<?, ?>> visited =
                Collections.newSetFromMap(new IdentityHashMap<>());
        markConnectiveLabelSteps(traversal, visited);
    }

    @Override
    public Set<Class<? extends TraversalStrategy.OptimizationStrategy>> applyPost() {
        return Collections.singleton(InlineFilterStrategy.class);
    }

    public static HugeConnectiveLabelStepStrategy instance() {
        return INSTANCE;
    }

    private static void markConnectiveLabelSteps(Traversal.Admin<?, ?> traversal,
                                                Set<Traversal.Admin<?, ?>> visited) {
        if (!visited.add(traversal)) {
            return;
        }

        for (AndStep<?> step : TraversalHelper.getStepsOfClass(AndStep.class,
                                                               traversal)) {
            markConnectiveLabelChildren(step, step);
        }
        for (OrStep<?> step : TraversalHelper.getStepsOfClass(OrStep.class,
                                                              traversal)) {
            markConnectiveLabelChildren(step, step);
        }

        for (Step<?, ?> step : traversal.getSteps()) {
            if (!(step instanceof TraversalParent)) {
                continue;
            }
            TraversalParent parent = (TraversalParent) step;
            for (Traversal.Admin<?, ?> child : parent.getLocalChildren()) {
                markConnectiveLabelSteps(child, visited);
            }
            for (Traversal.Admin<?, ?> child : parent.getGlobalChildren()) {
                markConnectiveLabelSteps(child, visited);
            }
        }
    }

    private static void markConnectiveLabelChildren(Step<?, ?> step,
                                                    TraversalParent parent) {
        if (!(step.getPreviousStep() instanceof HasStep)) {
            return;
        }
        for (Traversal.Admin<?, ?> child : parent.getLocalChildren()) {
            markPositiveLabelOnlyTraversal(child);
        }
    }

    private static void markPositiveLabelOnlyTraversal(
            Traversal.Admin<?, ?> traversal) {
        for (Step<?, ?> step : traversal.getSteps()) {
            if (!(step instanceof HasStep)) {
                return;
            }
            HasStep<?> hasStep = (HasStep<?>) step;
            if (!hasOnlyPositiveLabelContainers(hasStep)) {
                return;
            }
        }

        for (Step<?, ?> step : traversal.getSteps()) {
            TraversalUtil.markConnectiveLabelStep(step);
        }
    }

    private static boolean hasOnlyPositiveLabelContainers(HasStep<?> step) {
        if (step.getHasContainers().isEmpty()) {
            return false;
        }
        for (HasContainer has : step.getHasContainers()) {
            if (!TraversalUtil.isPositiveLabelContainer(has)) {
                return false;
            }
        }
        return true;
    }
}
