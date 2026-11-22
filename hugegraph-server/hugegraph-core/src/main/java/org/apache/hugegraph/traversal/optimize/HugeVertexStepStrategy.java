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

import java.util.List;

import org.apache.hugegraph.HugeGraph;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy.ProviderOptimizationStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.ConnectiveStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.NotStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.TraversalFilterStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PathStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.TreeStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.EmptyTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

public final class HugeVertexStepStrategy
        extends AbstractTraversalStrategy<ProviderOptimizationStrategy>
        implements ProviderOptimizationStrategy {

    private static final long serialVersionUID = 491355700217483162L;

    private static final HugeVertexStepStrategy INSTANCE;

    static {
        INSTANCE = new HugeVertexStepStrategy();
    }

    private HugeVertexStepStrategy() {
        // pass
    }

    @Override
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void apply(final Traversal.Admin<?, ?> traversal) {
        TraversalUtil.convAllHasSteps(traversal);

        List<VertexStep> steps = TraversalHelper.getStepsOfClass(
                VertexStep.class, traversal);

        boolean batchOptimize = false;
        if (!steps.isEmpty()) {
            boolean withPath = HugeVertexStepStrategy.containsPath(traversal);
            boolean withTree = HugeVertexStepStrategy.containsTree(traversal);
            /*
             * The graph of traversal may be null when `__` step is followed
             * by `count().is(0)` step, like the following gremlin:
             * `g.V(id).repeat(in()).until(or(inE().count().is(0), loops().is(2)))`
             * TODO: remove this `graph!=null` check after fixed the bug #1699
             */
            boolean supportIn = false;
            HugeGraph graph = TraversalUtil.tryGetGraph(steps.get(0));
            if (graph != null) {
                supportIn = graph.backendStoreFeatures()
                                 .supportsQueryWithInCondition();
            }
            batchOptimize = !withTree && !withPath && supportIn;
        }

        for (VertexStep originStep : steps) {
            HugeVertexStep<?> newStep = batchOptimize ?
                                        new HugeVertexStepByBatch<>(originStep) :
                                        new HugeVertexStep<>(originStep);
            TraversalHelper.replaceStep(originStep, newStep, traversal);

            TraversalUtil.extractHasContainer(newStep, traversal);

            // TODO: support order-by optimize
            // TraversalUtil.extractOrder(newStep, traversal);

            TraversalUtil.extractRange(newStep, traversal, true);

            TraversalUtil.extractCount(newStep, traversal);

            if (onlyTestsExistence(newStep, traversal)) {
                // The first result is enough, e.g. not(out().has(k, v))
                newStep.queryInfo().batchSizeHint(1L);
            }
        }
    }

    /**
     * Whether the results of a step are only used to test if any exists:
     * the step ends a child traversal of not(), filter(), where(traversal),
     * and() or or(), which only call hasNext() on the child traversal
     *
     * @param step      the step to check
     * @param traversal the traversal which the step belongs to
     * @return true if one result is enough for the step
     */
    protected static boolean onlyTestsExistence(Step<?, ?> step,
                                                Traversal.Admin<?, ?> traversal) {
        if (step != traversal.getEndStep()) {
            // Like not(out().count().is(0)), all results of the step matter
            return false;
        }
        TraversalParent parent = traversal.getParent();
        if (!(parent instanceof NotStep || parent instanceof TraversalFilterStep ||
              parent instanceof ConnectiveStep)) {
            return false;
        }
        return parent.getLocalChildren().stream()
                     .anyMatch(child -> child == traversal);
    }

    /**
     * Does a Traversal contain any Path step
     *
     * @param traversal
     * @return the traversal or its parents contain at least one Path step
     */
    protected static boolean containsPath(Traversal.Admin<?, ?> traversal) {
        boolean hasPath = !TraversalHelper.getStepsOfClass(
                PathStep.class, traversal).isEmpty();
        if (hasPath) {
            return true;
        } else if (traversal instanceof EmptyTraversal) {
            return false;
        }

        TraversalParent parent = traversal.getParent();
        return containsPath(parent.asStep().getTraversal());
    }

    /**
     * Does a Traversal contain any Tree step
     *
     * @param traversal
     * @return the traversal or its parents contain at least one Tree step
     */
    protected static boolean containsTree(Traversal.Admin<?, ?> traversal) {
        boolean hasTree = !TraversalHelper.getStepsOfClass(
                TreeStep.class, traversal).isEmpty();
        if (hasTree) {
            return true;
        } else if (traversal instanceof EmptyTraversal) {
            return false;
        }

        TraversalParent parent = traversal.getParent();
        return containsTree(parent.asStep().getTraversal());
    }

    public static HugeVertexStepStrategy instance() {
        return INSTANCE;
    }
}
