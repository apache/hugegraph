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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.type.define.Directions;
import org.apache.hugegraph.util.CollectionUtil;
import org.apache.hugegraph.util.E;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

public class SameNeighborTraverser extends HugeTraverser {

    public SameNeighborTraverser(HugeGraph graph) {
        super(graph);
    }

    public Set<Id> sameNeighbors(Id vertex, Id other, Directions direction,
                                 String label, long degree, int limit) {
        E.checkNotNull(vertex, "vertex id");
        E.checkNotNull(other, "the other vertex id");
        this.checkVertexExist(vertex, "vertex");
        this.checkVertexExist(other, "other vertex");
        E.checkNotNull(direction, "direction");
        checkDegree(degree);
        checkLimit(limit);

        Id labelId = this.getEdgeLabelIdOrNull(label);

        Set<Id> sourceNeighbors = IteratorUtils.set(this.adjacentVertices(
                vertex, direction, labelId, degree));
        Set<Id> targetNeighbors = IteratorUtils.set(this.adjacentVertices(
                other, direction, labelId, degree));
        Set<Id> sameNeighbors = (Set<Id>) CollectionUtil.intersect(
                sourceNeighbors, targetNeighbors);

        this.vertexIterCounter.addAndGet(2L);
        this.edgeIterCounter.addAndGet(sourceNeighbors.size());
        this.edgeIterCounter.addAndGet(targetNeighbors.size());

        if (limit != NO_LIMIT) {
            int end = Math.min(sameNeighbors.size(), limit);
            sameNeighbors = CollectionUtil.subSet(sameNeighbors, 0, end);
        }
        return sameNeighbors;
    }

    public Set<Id> sameNeighbors(List<Id> vertexIds, Directions direction,
                                 List<String> labels, long degree, int limit) {
        E.checkNotNull(vertexIds, "vertex ids");
        E.checkArgument(vertexIds.size() >= 2, "vertex_list size can't " +
                                               "be less than 2");
        for (Id id : vertexIds) {
            this.checkVertexExist(id, "vertex");
        }
        E.checkNotNull(direction, "direction");
        checkDegree(degree);
        checkLimit(limit);

        List<Id> labelsId = new ArrayList<>();
        if (labels != null) {
            for (String label : labels) {
                labelsId.add(this.getEdgeLabelIdOrNull(label));
            }
        }

        Set<Id> sameNeighbors = new HashSet<>();
        for (int i = 0; i < vertexIds.size(); i++) {
            Set<Id> vertexNeighbors = IteratorUtils.set(this.adjacentVertices(
                    vertexIds.get(i), direction, labelsId, degree));
            if (i == 0) {
                sameNeighbors = vertexNeighbors;
            } else {
                sameNeighbors = (Set<Id>) CollectionUtil.intersect(
                        sameNeighbors, vertexNeighbors);
            }
            this.vertexIterCounter.addAndGet(1L);
            this.edgeIterCounter.addAndGet(vertexNeighbors.size());
        }

        if (limit != NO_LIMIT) {
            int end = Math.min(sameNeighbors.size(), limit);
            sameNeighbors = CollectionUtil.subSet(sameNeighbors, 0, end);
        }
        return sameNeighbors;
    }
}
