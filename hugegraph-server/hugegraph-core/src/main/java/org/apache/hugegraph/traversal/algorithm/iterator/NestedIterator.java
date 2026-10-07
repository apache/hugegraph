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

package org.apache.hugegraph.traversal.algorithm.iterator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.iterator.WrappedIterator;
import org.apache.hugegraph.structure.HugeEdge;
import org.apache.hugegraph.traversal.algorithm.HugeTraverser;
import org.apache.hugegraph.traversal.algorithm.steps.Steps;
import org.apache.hugegraph.util.collection.ObjectIntMapping;
import org.apache.hugegraph.util.collection.ObjectIntMappingFactory;
import org.apache.tinkerpop.gremlin.structure.Edge;

public class NestedIterator extends WrappedIterator<Edge> {

    private final int MAX_CACHED_COUNT = 1000;
    /**
     * Set<Id> visited: visited vertex-ids of all parent-tree
     * used to exclude visited vertex
     */
    private final boolean nearest;
    private final Set<Id> visited;
    private final int MAX_VISITED_COUNT = 100000;

    // cache for edges, initial capacity to avoid memory fragment
    private final List<HugeEdge> cache;
    private final Map<Long, Integer> parentEdgePointerMap;

    private final Iterator<Edge> parentIterator;
    private final HugeTraverser traverser;
    private final Steps steps;
    private final ObjectIntMapping<Id> idMapping;
    private HugeEdge currentEdge;
    private int cachePointer;
    private Iterator<Edge> currentIterator;

    public NestedIterator(HugeTraverser traverser,
                          Iterator<Edge> parentIterator,
                          Steps steps,
                          Set<Id> visited,
                          boolean nearest) {
        this.traverser = traverser;
        this.parentIterator = parentIterator;
        this.steps = steps;
        this.visited = visited;
        this.nearest = nearest;

        this.cache = new ArrayList<>(MAX_CACHED_COUNT);
        this.parentEdgePointerMap = new HashMap<>();

        this.cachePointer = 0;
        this.currentEdge = null;
        this.currentIterator = null;

        this.idMapping = ObjectIntMappingFactory.newObjectIntMapping(false);
    }

    private static Long makeVertexPairIndex(int source, int target) {
        return ((long) source & 0xFFFFFFFFL) |
               (((long) target << 32) & 0xFFFFFFFF00000000L);
    }

    @Override
    public boolean hasNext() {
        if (this.currentIterator == null || !this.currentIterator.hasNext()) {
            return fetch();
        }
        return true;
    }

    @Override
    public Edge next() {
        return this.currentIterator.next();
    }

    @Override
    protected Iterator<?> originIterator() {
        return this.parentIterator;
    }

    @Override
    protected boolean fetch() {
        while (this.currentIterator == null || !this.currentIterator.hasNext()) {
            if (this.currentIterator != null) {
                this.currentIterator = null;
            }

            if (this.cache.size() == this.cachePointer && !this.fillCache()) {
                return false;
            }

            this.currentEdge = this.cache.get(this.cachePointer);
            this.cachePointer++;
            this.currentIterator =
                    traverser.edgesOfVertex(this.currentEdge.id().otherVertexId(), steps);
            this.traverser.vertexIterCounter.addAndGet(1L);

        }
        return true;
    }

    private boolean fillCache() {
        // fill cache from parent
        while (this.parentIterator.hasNext() && this.cache.size() < MAX_CACHED_COUNT) {
            HugeEdge edge = (HugeEdge) this.parentIterator.next();
            Id vertexId = edge.id().otherVertexId();

            this.traverser.edgeIterCounter.addAndGet(1L);

            if (!this.nearest || !this.visited.contains(vertexId)) {
                // update parent edge cache pointer
                int parentEdgePointer = -1;
                if (this.parentIterator instanceof NestedIterator) {
                    parentEdgePointer = ((NestedIterator) this.parentIterator).currentEdgePointer();
                }

                this.parentEdgePointerMap.put(makeEdgeIndex(edge), parentEdgePointer);

                this.cache.add(edge);
                if (this.visited.size() < MAX_VISITED_COUNT) {
                    this.visited.add(vertexId);
                }
            }
        }
        return this.cache.size() > this.cachePointer;
    }

    public List<HugeEdge> pathEdges() {
        List<HugeEdge> edges = new ArrayList<>();
        HugeEdge currentEdge = this.currentEdge;
        if (this.parentIterator instanceof NestedIterator) {
            NestedIterator parent = (NestedIterator) this.parentIterator;
            int parentEdgePointer = this.parentEdgePointerMap.get(makeEdgeIndex(currentEdge));
            edges.addAll(parent.pathEdges(parentEdgePointer));
        }
        edges.add(currentEdge);
        return edges;
    }

    private List<HugeEdge> pathEdges(int edgePointer) {
        List<HugeEdge> edges = new ArrayList<>();
        HugeEdge edge = this.cache.get(edgePointer);
        if (this.parentIterator instanceof NestedIterator) {
            NestedIterator parent = (NestedIterator) this.parentIterator;
            int parentEdgePointer = this.parentEdgePointerMap.get(makeEdgeIndex(edge));
            edges.addAll(parent.pathEdges(parentEdgePointer));
        }
        edges.add(edge);
        return edges;
    }

    public int currentEdgePointer() {
        return this.cachePointer - 1;
    }

    private Long makeEdgeIndex(HugeEdge edge) {
        int sourceV = this.code(edge.id().ownerVertexId());
        int targetV = this.code(edge.id().otherVertexId());
        return makeVertexPairIndex(sourceV, targetV);
    }

    private int code(Id id) {
        if (id.number()) {
            long l = id.asLong();
            if (0 <= l && l <= Integer.MAX_VALUE) {
                return (int) l;
            }
        }
        int code = this.idMapping.object2Code(id);
        assert code > 0;
        return -code;
    }
}
