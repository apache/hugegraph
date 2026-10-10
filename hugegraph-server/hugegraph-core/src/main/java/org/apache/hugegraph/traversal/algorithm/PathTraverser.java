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

import static org.apache.hugegraph.traversal.algorithm.HugeTraverser.NO_LIMIT;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.structure.HugeEdge;
import org.apache.hugegraph.traversal.algorithm.HugeTraverser.EdgeRecord;
import org.apache.hugegraph.traversal.algorithm.steps.EdgeStep;
import org.apache.hugegraph.traversal.algorithm.strategy.TraverseStrategy;
import org.apache.tinkerpop.gremlin.structure.Edge;

public abstract class PathTraverser {

    protected final HugeTraverser traverser;
    protected final long capacity;
    protected final long limit;
    protected int stepCount;
    protected int totalSteps; // TODO: delete or implement abstract method

    protected Map<Id, List<HugeTraverser.Node>> sources;
    protected Map<Id, List<HugeTraverser.Node>> sourcesAll;
    protected Map<Id, List<HugeTraverser.Node>> targets;
    protected Map<Id, List<HugeTraverser.Node>> targetsAll;

    protected Map<Id, List<HugeTraverser.Node>> newVertices;

    protected Set<HugeTraverser.Path> paths;

    protected TraverseStrategy traverseStrategy;
    protected EdgeRecord edgeResults;

    public PathTraverser(HugeTraverser traverser, TraverseStrategy strategy,
                         Collection<Id> sources, Collection<Id> targets,
                         long capacity, long limit, boolean concurrent) {
        this.traverser = traverser;
        this.traverseStrategy = strategy;

        this.capacity = capacity;
        this.limit = limit;

        this.stepCount = 0;

        this.sources = this.newMultiValueMap();
        this.sourcesAll = this.newMultiValueMap();
        this.targets = this.newMultiValueMap();
        this.targetsAll = this.newMultiValueMap();

        for (Id id : sources) {
            this.addNode(this.sources, id, new HugeTraverser.Node(id));
        }
        for (Id id : targets) {
            this.addNode(this.targets, id, new HugeTraverser.Node(id));
        }
        this.sourcesAll.putAll(this.sources);
        this.targetsAll.putAll(this.targets);

        this.paths = this.newPathSet();

        this.edgeResults = new EdgeRecord(concurrent);
    }

    public void forward() {
        EdgeStep currentStep = this.nextStep(true);
        if (currentStep == null) {
            return;
        }

        this.beforeTraverse(true);

        // Traversal vertices of previous level
        this.traverseOneLayer(this.sources, currentStep, this::forward);

        this.afterTraverse(currentStep, true);
    }

    public void backward() {
        EdgeStep currentStep = this.nextStep(false);
        if (currentStep == null) {
            return;
        }

        this.beforeTraverse(false);

        currentStep.swithDirection();
        // Traversal vertices of previous level
        this.traverseOneLayer(this.targets, currentStep, this::backward);
        currentStep.swithDirection();

        this.afterTraverse(currentStep, false);
    }

    public abstract EdgeStep nextStep(boolean forward);

    public void beforeTraverse(boolean forward) {
        this.clearNewVertices();
    }

    public void traverseOneLayer(Map<Id, List<HugeTraverser.Node>> vertices,
                                 EdgeStep step,
                                 BiConsumer<Id, EdgeStep> consumer) {
        this.traverseStrategy.traverseOneLayer(vertices, step, consumer);
    }

    public void afterTraverse(EdgeStep step, boolean forward) {
        this.reInitCurrentStepIfNeeded(step, forward);
        this.stepCount++;
    }

    private void forward(Id v, EdgeStep step) {
        this.traverseOne(v, step, true);
    }

    private void backward(Id v, EdgeStep step) {
        this.traverseOne(v, step, false);
    }

    private void traverseOne(Id v, EdgeStep step, boolean forward) {
        if (this.reachLimit()) {
            return;
        }

        Iterator<Edge> edges = this.traverser.edgesOfVertex(v, step);
        while (edges.hasNext()) {
            HugeEdge edge = (HugeEdge) edges.next();
            Id target = edge.id().otherVertexId();
            this.traverser.edgeIterCounter.addAndGet(1L);

            this.edgeResults.addEdge(v, target, edge);

            this.processOne(v, target, forward);
        }
        this.traverser.vertexIterCounter.addAndGet(1L);
    }

    private void processOne(Id source, Id target, boolean forward) {
        if (forward) {
            this.processOneForForward(source, target);
        } else {
            this.processOneForBackward(source, target);
        }
    }

    protected abstract void processOneForForward(Id source, Id target);

    protected abstract void processOneForBackward(Id source, Id target);

    protected abstract void reInitCurrentStepIfNeeded(EdgeStep step,
                                                      boolean forward);

    public void clearNewVertices() {
        this.newVertices = this.newMultiValueMap();
    }

    public void addNodeToNewVertices(Id id, HugeTraverser.Node node) {
        this.addNode(this.newVertices, id, node);
    }

    public Map<Id, List<HugeTraverser.Node>> newMultiValueMap() {
        return this.traverseStrategy.newMultiValueMap();
    }

    public Set<HugeTraverser.Path> newPathSet() {
        return this.traverseStrategy.newPathSet();
    }

    public void addNode(Map<Id, List<HugeTraverser.Node>> vertices, Id id,
                        HugeTraverser.Node node) {
        this.traverseStrategy.addNode(vertices, id, node);
    }

    public void addNewVerticesToAll(Map<Id, List<HugeTraverser.Node>> targets) {
        this.traverseStrategy.addNewVerticesToAll(this.newVertices, targets);
    }

    public Set<HugeTraverser.Path> paths() {
        return this.paths;
    }

    public int pathCount() {
        return this.paths.size();
    }

    protected boolean finished() {
        return this.stepCount >= this.totalSteps || this.reachLimit();
    }

    protected boolean reachLimit() {
        HugeTraverser.checkCapacity(this.capacity, this.accessedNodes(),
                                    "template paths");
        return this.limit != NO_LIMIT && this.pathCount() >= this.limit;
    }

    protected int accessedNodes() {
        int size = 0;
        for (List<HugeTraverser.Node> value : this.sourcesAll.values()) {
            size += value.size();
        }
        for (List<HugeTraverser.Node> value : this.targetsAll.values()) {
            size += value.size();
        }
        return size;
    }
}
