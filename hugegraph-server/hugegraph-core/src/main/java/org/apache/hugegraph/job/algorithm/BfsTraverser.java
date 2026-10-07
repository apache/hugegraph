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

package org.apache.hugegraph.job.algorithm;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Stack;

import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.job.UserJob;
import org.apache.hugegraph.structure.HugeEdge;
import org.apache.hugegraph.type.define.Directions;
import org.apache.tinkerpop.gremlin.structure.Edge;

public abstract class BfsTraverser<T extends BfsTraverser.Node>
        extends AbstractAlgorithm.AlgoTraverser
        implements AutoCloseable {

    private final Stack<Id> traversedVertices = new Stack<>();

    public BfsTraverser(UserJob<Object> job) {
        super(job);
    }

    protected void compute(Id startVertex, Directions direction,
                           Id edgeLabel, long degree, long depth) {
        Map<Id, T> localNodes = this.traverse(startVertex, direction,
                                              edgeLabel, degree, depth);
        this.backtrack(startVertex, localNodes);
    }

    protected Map<Id, T> traverse(Id startVertex, Directions direction,
                                  Id edgeLabel, long degree, long depth) {
        Map<Id, T> localNodes = new HashMap<>();
        localNodes.put(startVertex, this.createStartNode());

        LinkedList<Id> traversingVertices = new LinkedList<>();
        traversingVertices.add(startVertex);
        while (!traversingVertices.isEmpty()) {
            Id source = traversingVertices.removeFirst();
            this.traversedVertices.push(source);
            T sourceNode = localNodes.get(source);
            if (depth != NO_LIMIT && sourceNode.distance() >= depth) {
                continue;
            }
            // TODO: sample the edges
            Iterator<Edge> edges = this.edgesOfVertex(source, direction,
                                                      edgeLabel, degree);
            while (edges.hasNext()) {
                HugeEdge edge = (HugeEdge) edges.next();
                Id target = edge.otherVertex().id();
                T targetNode = localNodes.get(target);
                boolean firstTime = false;
                // Edge's targetNode is arrived at first time
                if (targetNode == null) {
                    firstTime = true;
                    targetNode = this.createNode(sourceNode);
                    localNodes.put(target, targetNode);
                    traversingVertices.addLast(target);
                }
                if (targetNode.distance() == sourceNode.distance() + 1) {
                    this.meetNode(target, targetNode, source,
                                  sourceNode, firstTime);
                }
            }
        }
        return localNodes;
    }

    protected void backtrack(Id startVertex, Map<Id, T> localNodes) {
        while (!this.traversedVertices.empty()) {
            Id currentVertex = this.traversedVertices.pop();
            this.backtrack(startVertex, currentVertex, localNodes);
        }
    }

    protected abstract T createStartNode();

    protected abstract T createNode(T parentNode);

    /**
     * This method is invoked when currentVertex.distance() equals
     * parentVertex.distance() + 1.
     */
    protected abstract void meetNode(Id currentVertex, T currentNode,
                                     Id parentVertex, T parentNode,
                                     boolean firstTime);

    protected abstract void backtrack(Id startVertex, Id currentVertex,
                                      Map<Id, T> localNodes);

    public static class Node {

        private Id[] parents;
        private int pathCount;
        private final int distance;

        public Node(Node parentNode) {
            this(0, parentNode.distance + 1);
        }

        public Node(int pathCount, int distance) {
            this.pathCount = pathCount;
            this.distance = distance;
            this.parents = new Id[0];
        }

        public int distance() {
            return this.distance;
        }

        public Id[] parents() {
            return this.parents;
        }

        public void addParent(Id parentId) {
            // TODO: test if need to allocate more memory in advance
            Id[] newParents = new Id[this.parents.length + 1];
            System.arraycopy(this.parents, 0, newParents, 0,
                             this.parents.length);
            newParents[newParents.length - 1] = parentId;
            this.parents = newParents;
        }

        public void addParentNode(Node node, Id parentId) {
            this.pathCount += node.pathCount;
            this.addParent(parentId);
        }

        protected int pathCount() {
            return this.pathCount;
        }
    }
}
