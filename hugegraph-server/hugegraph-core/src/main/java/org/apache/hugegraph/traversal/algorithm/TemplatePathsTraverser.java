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

package org.apache.hugegraph.traversal.algorithm;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.structure.HugeVertex;
import org.apache.hugegraph.traversal.algorithm.steps.EdgeStep;
import org.apache.hugegraph.traversal.algorithm.steps.RepeatEdgeStep;
import org.apache.hugegraph.traversal.algorithm.strategy.TraverseStrategy;
import org.apache.hugegraph.util.E;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

public class TemplatePathsTraverser extends HugeTraverser {

    public TemplatePathsTraverser(HugeGraph graph) {
        super(graph);
    }

    public WrappedPathSet templatePaths(Iterator<Vertex> sources,
                                        Iterator<Vertex> targets,
                                        List<RepeatEdgeStep> steps,
                                        boolean withRing, long capacity,
                                        long limit) {
        checkCapacity(capacity);
        checkLimit(limit);

        List<Id> sourceList = newList();
        while (sources.hasNext()) {
            sourceList.add(((HugeVertex) sources.next()).id());
        }
        int sourceSize = sourceList.size();
        E.checkState(sourceSize >= 1 && sourceSize <= MAX_VERTICES,
                     "The number of source vertices must in [1, %s], " +
                     "but got: %s", MAX_VERTICES, sourceList.size());
        List<Id> targetList = newList();
        while (targets.hasNext()) {
            targetList.add(((HugeVertex) targets.next()).id());
        }
        int targetSize = targetList.size();
        E.checkState(targetSize >= 1 && targetSize <= MAX_VERTICES,
                     "The number of target vertices must in [1, %s], " +
                     "but got: %s", MAX_VERTICES, sourceList.size());

        int totalSteps = 0;
        for (RepeatEdgeStep step : steps) {
            totalSteps += step.maxTimes();
        }

        boolean concurrent = totalSteps >= this.concurrentDepth();
        TraverseStrategy strategy = TraverseStrategy.create(
                concurrent, this.graph());
        Traverser traverser = new Traverser(this, strategy,
                                            sourceList, targetList, steps,
                                            withRing, capacity, limit, concurrent);
        do {
            // Forward
            traverser.forward();
            if (traverser.finished()) {
                Set<Path> paths = traverser.paths();
                return new WrappedPathSet(paths, traverser.edgeResults.getEdges(paths));
            }

            // Backward
            traverser.backward();
            if (traverser.finished()) {
                Set<Path> paths = traverser.paths();
                return new WrappedPathSet(paths, traverser.edgeResults.getEdges(paths));
            }
        } while (true);
    }

    private static class Traverser extends PathTraverser {

        protected final List<RepeatEdgeStep> steps;

        protected boolean withRing;

        protected int sourceIndex;
        protected int targetIndex;

        protected boolean sourceFinishOneStep;
        protected boolean targetFinishOneStep;

        public Traverser(HugeTraverser traverser, TraverseStrategy strategy,
                         Collection<Id> sources, Collection<Id> targets,
                         List<RepeatEdgeStep> steps, boolean withRing,
                         long capacity, long limit, boolean concurrent) {
            super(traverser, strategy, sources, targets, capacity, limit, concurrent);

            this.steps = steps;
            this.withRing = withRing;
            for (RepeatEdgeStep step : steps) {
                this.totalSteps += step.maxTimes();
            }

            this.sourceIndex = 0;
            this.targetIndex = this.steps.size() - 1;

            this.sourceFinishOneStep = false;
            this.targetFinishOneStep = false;
        }

        @Override
        public RepeatEdgeStep nextStep(boolean forward) {
            return forward ? this.forwardStep() : this.backwardStep();
        }

        @Override
        public void beforeTraverse(boolean forward) {
            this.clearNewVertices();
            this.reInitAllIfNeeded(forward);
        }

        @Override
        public void afterTraverse(EdgeStep step, boolean forward) {

            Map<Id, List<Node>> all = forward ? this.sourcesAll :
                                      this.targetsAll;
            this.addNewVerticesToAll(all);
            this.reInitCurrentStepIfNeeded(step, forward);
            this.stepCount++;
        }

        @Override
        protected void processOneForForward(Id sourceV, Id targetV) {
            for (Node source : this.sources.get(sourceV)) {
                // If have loop, skip target
                if (!this.withRing && source.contains(targetV)) {
                    continue;
                }

                // If cross point exists, path found, concat them
                if (this.lastSuperStep() &&
                    this.targetsAll.containsKey(targetV)) {
                    for (Node target : this.targetsAll.get(targetV)) {
                        List<Id> path = joinPath(source, target, this.withRing);
                        if (!path.isEmpty()) {
                            this.paths.add(new Path(targetV, path));
                            if (this.reachLimit()) {
                                return;
                            }
                        }
                    }
                }

                // Add node to next start-nodes
                this.addNodeToNewVertices(targetV, new Node(targetV, source));
            }
        }

        @Override
        protected void processOneForBackward(Id sourceV, Id targetV) {
            for (Node source : this.targets.get(sourceV)) {
                // If have loop, skip target
                if (!this.withRing && source.contains(targetV)) {
                    continue;
                }

                // If cross point exists, path found, concat them
                if (this.lastSuperStep() &&
                    this.sourcesAll.containsKey(targetV)) {
                    for (Node target : this.sourcesAll.get(targetV)) {
                        List<Id> path = joinPath(source, target, this.withRing);
                        if (!path.isEmpty()) {
                            Path newPath = new Path(targetV, path);
                            newPath.reverse();
                            this.paths.add(newPath);
                            if (this.reachLimit()) {
                                return;
                            }
                        }
                    }
                }

                // Add node to next start-nodes
                this.addNodeToNewVertices(targetV, new Node(targetV, source));
            }
        }

        private void reInitAllIfNeeded(boolean forward) {
            if (forward) {
                /*
                 * Re-init source all if last forward finished one super step
                 * and current step is not last super step
                 */
                if (this.sourceFinishOneStep && !this.lastSuperStep()) {
                    this.sourcesAll = this.newMultiValueMap();
                    this.sourceFinishOneStep = false;
                }
            } else {
                /*
                 * Re-init target all if last forward finished one super step
                 * and current step is not last super step
                 */
                if (this.targetFinishOneStep && !this.lastSuperStep()) {
                    this.targetsAll = this.newMultiValueMap();
                    this.targetFinishOneStep = false;
                }
            }
        }

        @Override
        protected void reInitCurrentStepIfNeeded(EdgeStep step,
                                                 boolean forward) {
            RepeatEdgeStep currentStep = (RepeatEdgeStep) step;
            currentStep.decreaseTimes();
            if (forward) {
                // Re-init sources
                if (currentStep.remainTimes() > 0) {
                    this.sources = this.newVertices;
                } else {
                    this.sources = this.sourcesAll;
                    this.sourceFinishOneStep = true;
                }
            } else {
                // Re-init targets
                if (currentStep.remainTimes() > 0) {
                    this.targets = this.newVertices;
                } else {
                    this.targets = this.targetsAll;
                    this.targetFinishOneStep = true;
                }
            }
        }

        public RepeatEdgeStep forwardStep() {
            RepeatEdgeStep currentStep = null;
            // Find next step to backward
            for (int i = 0; i < this.steps.size(); i++) {
                RepeatEdgeStep step = this.steps.get(i);
                if (step.remainTimes() > 0) {
                    currentStep = step;
                    this.sourceIndex = i;
                    break;
                }
            }
            return currentStep;
        }

        public RepeatEdgeStep backwardStep() {
            RepeatEdgeStep currentStep = null;
            // Find next step to backward
            for (int i = this.steps.size() - 1; i >= 0; i--) {
                RepeatEdgeStep step = this.steps.get(i);
                if (step.remainTimes() > 0) {
                    currentStep = step;
                    this.targetIndex = i;
                    break;
                }
            }
            return currentStep;
        }

        public boolean lastSuperStep() {
            return this.targetIndex == this.sourceIndex ||
                   this.targetIndex == this.sourceIndex + 1;
        }
    }

    public static class WrappedPathSet {

        private final Set<Path> paths;
        private final Set<Edge> edges;

        public WrappedPathSet(Set<Path> paths, Set<Edge> edges) {
            this.paths = paths;
            this.edges = edges;
        }

        public Set<Path> paths() {
            return paths;
        }

        public Set<Edge> edges() {
            return edges;
        }
    }
}
