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

import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import org.apache.hugegraph.exception.HugeException;
import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.id.EdgeId;
import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.query.Query;
import org.apache.hugegraph.structure.HugeEdge;
import org.apache.hugegraph.traversal.algorithm.records.KoutRecords;
import org.apache.hugegraph.traversal.algorithm.steps.Steps;
import org.apache.hugegraph.type.define.Directions;
import org.apache.hugegraph.util.E;
import org.apache.tinkerpop.gremlin.structure.Edge;

public class KoutTraverser extends OltpTraverser {

    public KoutTraverser(HugeGraph graph) {
        super(graph);
    }

    public Set<Id> kout(Id sourceV, Directions dir, String label,
                        int depth, boolean nearest,
                        long degree, long capacity, long limit) {
        E.checkNotNull(sourceV, "source vertex id");
        this.checkVertexExist(sourceV, "source vertex");
        E.checkNotNull(dir, "direction");
        checkPositive(depth, "k-out max_depth");
        checkDegree(degree);
        checkCapacity(capacity);
        checkLimit(limit);
        if (capacity != NO_LIMIT) {
            // Capacity must > limit because sourceV is counted in capacity
            E.checkArgument(capacity >= limit && limit != NO_LIMIT,
                            "Capacity can't be less than limit, " +
                            "but got capacity '%s' and limit '%s'",
                            capacity, limit);
        }

        Id labelId = this.getEdgeLabelIdOrNull(label);

        Set<Id> sources = newIdSet();
        Set<Id> neighbors = newIdSet();
        Set<Id> visited = nearest ? newIdSet() : null;

        neighbors.add(sourceV);

        ConcurrentVerticesConsumer consumer;

        long remaining = capacity == NO_LIMIT ? NO_LIMIT : capacity - 1;

        while (depth-- > 0) {
            // Just get limit nodes in last layer if limit < remaining capacity
            if (depth == 0 && limit != NO_LIMIT &&
                (limit < remaining || remaining == NO_LIMIT)) {
                remaining = limit;
            }

            if (visited != null) {
                visited.addAll(neighbors);
            }

            // swap sources and neighbors
            Set<Id> tmp = neighbors;
            neighbors = sources;
            sources = tmp;

            // start
            consumer = new ConcurrentVerticesConsumer(sourceV, visited, remaining, neighbors);

            this.vertexIterCounter.addAndGet(sources.size());
            this.edgeIterCounter.addAndGet(neighbors.size());

            traverseIdsByBfs(sources.iterator(), dir, labelId, degree, capacity, consumer);

            sources.clear();

            if (capacity != NO_LIMIT) {
                // Update 'remaining' value to record remaining capacity
                remaining -= neighbors.size();

                if (remaining <= 0 && depth > 0) {
                    throw new HugeException(
                            "Reach capacity '%s' while remaining depth '%s'",
                            capacity, depth);
                }
            }
        }

        return neighbors;
    }

    public KoutRecords customizedKout(Id source, Steps steps,
                                      int maxDepth, boolean nearest,
                                      long capacity, long limit) {
        E.checkNotNull(source, "source vertex id");
        this.checkVertexExist(source, "source vertex");
        checkPositive(maxDepth, "k-out max_depth");
        checkCapacity(capacity);
        checkLimit(limit);
        long[] depth = new long[1];
        depth[0] = maxDepth;

        KoutRecords records = new KoutRecords(true, source, nearest, 0);

        Consumer<Edge> consumer = edge -> {
            if (this.reachLimit(limit, depth[0], records.size())) {
                return;
            }
            EdgeId edgeId = ((HugeEdge) edge).id();
            records.addPath(edgeId.ownerVertexId(), edgeId.otherVertexId());
            records.edgeResults().addEdge(edgeId.ownerVertexId(), edgeId.otherVertexId(), edge);
        };

        while (depth[0]-- > 0) {
            List<Id> sources = records.ids(Query.NO_LIMIT);
            records.startOneLayer(true);
            traverseIdsByBfs(sources.iterator(), steps, capacity, consumer);
            this.vertexIterCounter.addAndGet(sources.size());
            records.finishOneLayer();
            checkCapacity(capacity, records.accessed(), depth[0]);
        }
        return records;
    }

    public KoutRecords dfsKout(Id source, Steps steps,
                               int maxDepth, boolean nearest,
                               long capacity, long limit) {
        E.checkNotNull(source, "source vertex id");
        this.checkVertexExist(source, "source vertex");
        checkPositive(maxDepth, "k-out max_depth");
        checkCapacity(capacity);
        checkLimit(limit);

        Set<Id> all = newIdSet();
        all.add(source);

        KoutRecords records = new KoutRecords(false, source, nearest, maxDepth);
        Iterator<Edge> iterator = this.createNestedIterator(source, steps, maxDepth, all, nearest);
        while (iterator.hasNext()) {
            HugeEdge edge = (HugeEdge) iterator.next();
            this.edgeIterCounter.addAndGet(1L);

            Id target = edge.id().otherVertexId();
            if (!nearest || !all.contains(target)) {
                records.addFullPath(HugeTraverser.pathEdges(iterator, edge));
            }

            if (limit != NO_LIMIT && records.size() >= limit ||
                capacity != NO_LIMIT && all.size() > capacity) {
                break;
            }
        }

        return records;
    }

    private void checkCapacity(long capacity, long accessed, long depth) {
        if (capacity == NO_LIMIT) {
            return;
        }
        if (accessed >= capacity && depth > 0) {
            throw new HugeException(
                    "Reach capacity '%s' while remaining depth '%s'",
                    capacity, depth);
        }
    }

    private boolean reachLimit(long limit, long depth, int size) {
        return limit != NO_LIMIT && depth <= 0 && size >= limit;
    }
}
