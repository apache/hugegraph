/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hugegraph.traversal.optimize;

import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiPredicate;

import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.Contains;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.ConnectiveStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.FilterStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.IsStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.NotStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.CountGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MatchStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.IdentityStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SideEffectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.ConnectiveP;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

/**
 * HugeGraph keeps a local copy of TinkerPop's CountStrategy so we can
 * safely patch negative-threshold handling without waiting for a full
 * dependency upgrade. Count() never yields a negative result, so any
 * optimization that turns a negative bound into not() or a negative range
 * changes the traversal semantics.
 */
public final class HugeCountStrategy
        extends AbstractTraversalStrategy<TraversalStrategy.OptimizationStrategy>
        implements TraversalStrategy.OptimizationStrategy {

    private static final Map<BiPredicate, Long> RANGE_PREDICATES =
            new HashMap<BiPredicate, Long>() {{
                put(Contains.within, 1L);
                put(Contains.without, 0L);
            }};
    private static final Set<Compare> INCREASED_OFFSET_SCALAR_PREDICATES =
            EnumSet.of(Compare.eq, Compare.neq, Compare.lte, Compare.gt);

    private static final HugeCountStrategy INSTANCE = new HugeCountStrategy();

    private HugeCountStrategy() {
    }

    public static HugeCountStrategy instance() {
        return INSTANCE;
    }

    @Override
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void apply(final Traversal.Admin<?, ?> traversal) {
        final TraversalParent parent = traversal.getParent();
        int size = traversal.getSteps().size();
        Step prev = null;
        for (int i = 0; i < size; i++) {
            final Step curr = traversal.getSteps().get(i);
            if (i < size - 1 && doStrategy(curr)) {
                final IsStep isStep = (IsStep) traversal.getSteps().get(i + 1);
                final P isStepPredicate = isStep.getPredicate();
                Long highRange = null;
                boolean useNotStep = false;
                boolean dismissCountIs = false;
                boolean hasNegativeCompareBound = false;

                for (P p : isStepPredicate instanceof ConnectiveP ?
                           ((ConnectiveP<?>) isStepPredicate).getPredicates() :
                           Collections.singletonList(isStepPredicate)) {
                    final Object value = p.getValue();
                    final BiPredicate predicate = p.getBiPredicate();
                    if (value instanceof Number) {
                        final long highRangeOffset =
                                INCREASED_OFFSET_SCALAR_PREDICATES.contains(predicate) ?
                                1L : 0L;
                        final Long highRangeCandidate =
                                (long) Math.ceil(((Number) value).doubleValue()) +
                                highRangeOffset;

                        if ((predicate.equals(Compare.gt) ||
                             predicate.equals(Compare.gte) ||
                             predicate.equals(Compare.lt) ||
                             predicate.equals(Compare.lte)) &&
                            highRangeCandidate < 1L) {
                            hasNegativeCompareBound = true;
                        }

                        final boolean update = highRange == null ||
                                               highRangeCandidate > highRange;
                        if (update) {
                            if (parent instanceof EmptyStep) {
                                useNotStep = false;
                            } else if (parent instanceof RepeatStep) {
                                final RepeatStep repeatStep = (RepeatStep) parent;
                                dismissCountIs = useNotStep =
                                        Objects.equals(traversal,
                                                       repeatStep.getUntilTraversal()) ||
                                        Objects.equals(traversal,
                                                       repeatStep.getEmitTraversal());
                            } else {
                                dismissCountIs = useNotStep =
                                        parent instanceof FilterStep ||
                                        parent instanceof SideEffectStep;
                            }
                            highRange = highRangeCandidate;
                            useNotStep &= curr.getLabels().isEmpty() &&
                                          isStep.getLabels().isEmpty() &&
                                          isStep.getNextStep() instanceof EmptyStep &&
                                          ((highRange <= 1L &&
                                            predicate.equals(Compare.lt)) ||
                                           (highRange == 1L &&
                                            (predicate.equals(Compare.eq) ||
                                             predicate.equals(Compare.lte))));
                            dismissCountIs &= curr.getLabels().isEmpty() &&
                                              isStep.getLabels().isEmpty() &&
                                              isStep.getNextStep() instanceof EmptyStep &&
                                              (highRange == 1L &&
                                               (predicate.equals(Compare.gt) ||
                                                predicate.equals(Compare.gte)));
                        }
                        if (hasNegativeCompareBound) {
                            /*
                             * TINKERPOP-2893:
                             * count() is always >= 0, so optimizations like
                             * is(lt(-3)) -> not(...) are not semantics-safe.
                             */
                            useNotStep = false;
                            dismissCountIs = false;
                        }
                    } else {
                        final Long highRangeOffset = RANGE_PREDICATES.get(predicate);
                        if (value instanceof Collection && highRangeOffset != null) {
                            final Object high = Collections.max((Collection) value);
                            if (high instanceof Number) {
                                final Long highRangeCandidate =
                                        ((Number) high).longValue() + highRangeOffset;
                                final boolean update = highRange == null ||
                                                       highRangeCandidate > highRange;
                                if (update) {
                                    highRange = highRangeCandidate;
                                }
                            }
                        }
                    }
                }

                /*
                 * HugeGraph extracts RangeGlobalStep into backend queries. A
                 * negative upper bound is never useful for count(), and would
                 * become an invalid backend range like [0, -3).
                 */
                if (highRange != null && highRange < 0L) {
                    highRange = null;
                }

                if (highRange != null) {
                    if (useNotStep || dismissCountIs) {
                        traversal.asAdmin().removeStep(isStep);
                        traversal.asAdmin().removeStep(curr);
                        size -= 2;
                        if (!dismissCountIs) {
                            final TraversalParent p;
                            if ((p = traversal.getParent()) instanceof FilterStep &&
                                !(p instanceof ConnectiveStep)) {
                                final Step<?, ?> filterStep = parent.asStep();
                                final Traversal.Admin parentTraversal =
                                        filterStep.getTraversal();
                                final Step notStep = new NotStep<>(
                                        parentTraversal,
                                        traversal.getSteps().isEmpty() ?
                                        __.identity() : traversal);
                                filterStep.getLabels().forEach(notStep::addLabel);
                                TraversalHelper.replaceStep(filterStep, notStep,
                                                            parentTraversal);
                            } else {
                                final Traversal.Admin inner;
                                if (prev != null) {
                                    inner = __.start().asAdmin();
                                    for (;;) {
                                        final Step pp = prev.getPreviousStep();
                                        inner.addStep(0, prev);
                                        if (pp instanceof EmptyStep ||
                                            pp instanceof GraphStep ||
                                            !(prev instanceof FilterStep ||
                                              prev instanceof SideEffectStep)) {
                                            break;
                                        }
                                        traversal.removeStep(prev);
                                        prev = pp;
                                        size--;
                                    }
                                } else {
                                    inner = __.identity().asAdmin();
                                }
                                if (prev != null) {
                                    TraversalHelper.replaceStep(
                                            prev,
                                            new NotStep<>(traversal, inner),
                                            traversal);
                                } else {
                                    traversal.asAdmin().addStep(
                                            new NotStep<>(traversal, inner));
                                }
                            }
                        } else if (size == 0) {
                            final Step parentStep = traversal.getParent().asStep();
                            if (!(parentStep instanceof EmptyStep)) {
                                final Traversal.Admin parentTraversal =
                                        parentStep.getTraversal();
                                TraversalHelper.replaceStep(
                                        parentStep,
                                        new IdentityStep<>(parentTraversal),
                                        parentTraversal);
                            }
                        }
                    } else {
                        TraversalHelper.insertBeforeStep(
                                new RangeGlobalStep<>(traversal, 0L, highRange),
                                curr, traversal);
                    }
                    i++;
                }
            }
            prev = curr;
        }
    }

    private boolean doStrategy(final Step step) {
        if (!(step instanceof CountGlobalStep) ||
            !(step.getNextStep() instanceof IsStep) ||
            step.getPreviousStep() instanceof RangeGlobalStep) {
            return false;
        }

        final Step parent = step.getTraversal().getParent().asStep();
        return (parent instanceof FilterStep || parent.getLabels().isEmpty()) &&
               !(parent.getNextStep() instanceof MatchStep.MatchEndStep &&
                 ((MatchStep.MatchEndStep) parent.getNextStep())
                         .getMatchKey().isPresent());
    }
}
