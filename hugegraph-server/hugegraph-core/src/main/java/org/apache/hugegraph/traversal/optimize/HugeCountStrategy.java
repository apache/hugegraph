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
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.Contains;
import org.apache.tinkerpop.gremlin.process.traversal.NotP;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.PBiPredicate;
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
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeGlobalStepContract;
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

    private static final Map<PBiPredicate, Long> RANGE_PREDICATES =
            new HashMap<PBiPredicate, Long>() {{
                put(Contains.within, 1L);
                put(Contains.without, 1L);
            }};
    private static final Set<PBiPredicate<?, ?>>
            INCREASED_OFFSET_SCALAR_PREDICATES =
            Set.of(Compare.eq, Compare.neq, Compare.lte, Compare.gt,
                   new NotP.NotPBiPredicate<>(Compare.eq),
                   new NotP.NotPBiPredicate<>(Compare.neq),
                   new NotP.NotPBiPredicate<>(Compare.lte),
                   new NotP.NotPBiPredicate<>(Compare.gt));

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
                    final PBiPredicate predicate = p.getBiPredicate();
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

                if (isStepPredicate instanceof ConnectiveP) {
                    useNotStep = false;
                    dismissCountIs = false;
                }

                /*
                 * HugeGraph extracts RangeGlobalStep into backend queries.
                 * Non-positive upper bounds cannot preserve count(). A zero
                 * bound from without(-1) would replace a nonempty count with 0.
                 */
                if (highRange != null && highRange <= 0L) {
                    highRange = null;
                }

                if (highRange != null) {
                    if (useNotStep || dismissCountIs) {
                        traversal.asAdmin().removeStep(isStep);
                        traversal.asAdmin().removeStep(curr);
                        size -= 2;
                        if (!dismissCountIs) {
                            if (parent instanceof ConnectiveStep) {
                                final Step<?, ?> notStep = this.transformToNotStep(
                                        traversal, parent);
                                TraversalHelper.removeAllSteps(traversal);
                                traversal.addStep(notStep);
                            } else if (parent instanceof FilterStep) {
                                final Step filterStep = parent.asStep();
                                final Step<?, ?> notStep = this.transformToNotStep(
                                        traversal, parent);
                                TraversalHelper.replaceStep(filterStep, notStep,
                                                            filterStep.getTraversal());
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

    private Step<?, ?> transformToNotStep(final Traversal.Admin<?, ?> traversal,
                                          final TraversalParent parent) {
        final Step<?, ?> filterStep = parent.asStep();
        final Traversal.Admin<?, ?> parentTraversal = filterStep.getTraversal();
        final Step<?, ?> notStep = new NotStep<>(
                parentTraversal,
                traversal.getSteps().isEmpty() ? __.identity() : traversal.clone());
        filterStep.getLabels().forEach(notStep::addLabel);
        return notStep;
    }

    private boolean doStrategy(final Step step) {
        if (!(step instanceof CountGlobalStep) ||
            !(step.getNextStep() instanceof IsStep) ||
            step.getPreviousStep() instanceof RangeGlobalStepContract) {
            return false;
        }

        final P<?> predicate = ((IsStep<?>) step.getNextStep()).getPredicate();
        if (!this.supportsCountPredicate(predicate)) {
            return false;
        }

        final Step parent = step.getTraversal().getParent().asStep();
        return (parent instanceof FilterStep || parent.getLabels().isEmpty()) &&
               !(parent.getNextStep() instanceof MatchStep.MatchEndStep &&
                 ((MatchStep.MatchEndStep) parent.getNextStep())
                         .getMatchKey().isPresent());
    }

    private boolean supportsCountPredicate(P<?> predicate) {
        if (predicate instanceof ConnectiveP) {
            for (P<?> child : ((ConnectiveP<?>) predicate).getPredicates()) {
                // The bound calculation only handles a flat AND/OR. Every
                // branch must be safe: ignoring one can change the truth value.
                if (child instanceof ConnectiveP ||
                    !this.supportsCountPredicate(child)) {
                    return false;
                }
            }
            return !((ConnectiveP<?>) predicate).getPredicates().isEmpty();
        }

        final PBiPredicate<?, ?> biPredicate = predicate.getBiPredicate();
        final Object value = predicate.getValue();
        if (value instanceof Number) {
            // Bound derivation uses doubleValue() and ceil(). Restrict it to
            // standard number types and the range where adjacent integer
            // counts remain distinguishable in the predicate's numeric type.
            if (!(value instanceof Byte || value instanceof Short ||
                  value instanceof Integer || value instanceof Long ||
                  value instanceof Float || value instanceof Double)) {
                return false;
            }
            final double bound = ((Number) value).doubleValue();
            final double exactIntegerLimit = value instanceof Float ?
                                             16777215D : 9007199254740991D;
            if (!Double.isFinite(bound) || Math.abs(bound) > exactIntegerLimit) {
                return false;
            }
            for (Compare compare : Compare.values()) {
                if (biPredicate.equals(compare) ||
                    biPredicate.equals(new NotP.NotPBiPredicate<>(compare))) {
                    return true;
                }
            }
            return false;
        }

        if (!(value instanceof Collection) ||
            !RANGE_PREDICATES.containsKey(biPredicate)) {
            // In particular, NotP(collection) and custom predicates have no
            // proven bound. Keep the complete count/is traversal for them.
            return false;
        }
        final Collection<?> values = (Collection<?>) value;
        if (values.isEmpty()) {
            return false;
        }
        Class<?> numberType = null;
        for (Object item : values) {
            if (!(item instanceof Byte || item instanceof Short ||
                  item instanceof Integer || item instanceof Long) ||
                ((Number) item).longValue() == Long.MAX_VALUE ||
                (numberType != null && numberType != item.getClass())) {
                return false;
            }
            numberType = item.getClass();
        }
        return true;
    }
}
