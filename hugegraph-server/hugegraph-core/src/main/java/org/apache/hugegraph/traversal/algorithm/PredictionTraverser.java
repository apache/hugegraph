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

import java.util.Set;

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.traversal.algorithm.steps.EdgeStep;
import org.apache.hugegraph.type.define.Directions;
import org.apache.hugegraph.util.E;

import com.google.common.collect.ImmutableList;

public class PredictionTraverser extends OltpTraverser {

    public PredictionTraverser(HugeGraph graph) {
        super(graph);
    }

    public double adamicAdar(Id source, Id target, Directions dir,
                             String label, long degree, int limit) {
        Set<Id> neighbors = checkAndGetCommonNeighbors(source, target, dir,
                                                       label, degree, limit);
        EdgeStep step = label == null ? new EdgeStep(graph(), dir) :
                        new EdgeStep(graph(), dir, ImmutableList.of(label));

        double sum = 0.0;
        for (Id vid : neighbors) {
            long currentDegree = this.edgesCount(vid, step);
            if (currentDegree > 0) {
                sum += 1.0 / Math.log(currentDegree);
            }
        }
        return sum;
    }

    public double resourceAllocation(Id source, Id target, Directions dir,
                                     String label, long degree, int limit) {
        Set<Id> neighbors = checkAndGetCommonNeighbors(source, target, dir,
                                                       label, degree, limit);
        EdgeStep step = label == null ? new EdgeStep(graph(), dir) :
                        new EdgeStep(graph(), dir, ImmutableList.of(label));

        double sum = 0.0;
        for (Id vid : neighbors) {
            long currentDegree = this.edgesCount(vid, step);
            if (currentDegree > 0) {
                sum += 1.0 / currentDegree;
            }
        }
        return sum;
    }

    private Set<Id> checkAndGetCommonNeighbors(Id source, Id target,
                                               Directions dir, String label,
                                               long degree, int limit) {
        E.checkNotNull(source, "source id");
        E.checkNotNull(target, "the target id");
        this.checkVertexExist(source, "source");
        this.checkVertexExist(target, "target");
        E.checkNotNull(dir, "direction");
        checkDegree(degree);
        SameNeighborTraverser traverser = new SameNeighborTraverser(graph());
        return traverser.sameNeighbors(source, target, dir, label, degree, limit);
    }
}
