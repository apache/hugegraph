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
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.struct.schema.EdgeLabel;
import org.apache.hugegraph.structure.HugeEdge;
import org.apache.hugegraph.structure.HugeVertex;
import org.apache.hugegraph.type.define.Directions;
import org.apache.hugegraph.type.define.Frequency;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.InsertionOrderUtil;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import jakarta.ws.rs.core.MultivaluedHashMap;
import jakarta.ws.rs.core.MultivaluedMap;

public class FusiformSimilarityTraverser extends HugeTraverser {

    private long accessed = 0L;

    public FusiformSimilarityTraverser(HugeGraph graph) {
        super(graph);
    }

    private static void checkGroupArgs(String groupProperty, int minGroups) {
        if (groupProperty == null) {
            E.checkArgument(minGroups == 0,
                            "Can't set min group count when " +
                            "group property not set");
        } else {
            E.checkArgument(!groupProperty.isEmpty(),
                            "The group property can't be empty");
            E.checkArgument(minGroups > 0,
                            "Must set min group count when " +
                            "group property set");
        }
    }

    public SimilarsMap fusiformSimilarity(Iterator<Vertex> vertices,
                                          Directions direction, String label,
                                          int minNeighbors, double alpha,
                                          int minSimilars, int top,
                                          String groupProperty, int minGroups,
                                          long degree, long capacity, long limit,
                                          boolean withIntermediary) {
        checkCapacity(capacity);
        checkLimit(limit);
        checkGroupArgs(groupProperty, minGroups);

        int foundCount = 0;
        SimilarsMap results = new SimilarsMap();
        while (vertices.hasNext()) {
            checkCapacity(capacity, ++this.accessed, "fusiform similarity");
            HugeVertex vertex = (HugeVertex) vertices.next();
            // Find fusiform similarity for current vertex
            Set<Similar> result = this.fusiformSimilarityForVertex(
                    vertex, direction, label,
                    minNeighbors, alpha, minSimilars, top,
                    groupProperty, minGroups, degree, capacity,
                    withIntermediary);
            if (result.isEmpty()) {
                continue;
            }
            results.put(vertex.id(), result);
            // Reach limit
            if (limit != NO_LIMIT && ++foundCount >= limit) {
                break;
            }
        }
        this.reset();
        return results;
    }

    private Set<Similar> fusiformSimilarityForVertex(
            HugeVertex vertex, Directions direction,
            String label, int minNeighbors, double alpha,
            int minSimilars, int top, String groupProperty,
            int minGroups, long degree, long capacity,
            boolean withIntermediary) {
        boolean matched = this.matchMinNeighborCount(vertex, direction, label,
                                                     minNeighbors, degree);
        if (!matched) {
            // Ignore current vertex if its neighbors number is not enough
            return ImmutableSet.of();
        }
        Id labelId = this.getEdgeLabelIdOrNull(label);
        // Get similar nodes and counts
        Iterator<Edge> edges = this.edgesOfVertex(vertex.id(), direction,
                                                  labelId, degree);
        Map<Id, MutableInt> similars = newMap();
        MultivaluedMap<Id, Id> intermediaries = new MultivaluedHashMap<>();
        Set<Id> neighbors = newIdSet();
        long vertexCount = 1L;
        while (edges.hasNext()) {
            Id target = ((HugeEdge) edges.next()).id().otherVertexId();
            if (neighbors.contains(target)) {
                continue;
            }
            neighbors.add(target);
            checkCapacity(capacity, ++this.accessed, "fusiform similarity");

            Directions backDir = direction.opposite();
            Iterator<Edge> backEdges = this.edgesOfVertex(target, backDir,
                                                          labelId, degree);
            vertexCount += 1L;
            Set<Id> currentSimilars = newIdSet();
            while (backEdges.hasNext()) {
                Id node = ((HugeEdge) backEdges.next()).id().otherVertexId();
                if (currentSimilars.contains(node)) {
                    continue;
                }
                currentSimilars.add(node);
                if (withIntermediary) {
                    intermediaries.add(node, target);
                }

                MutableInt count = similars.get(node);
                if (count == null) {
                    count = new MutableInt(0);
                    similars.put(node, count);
                    checkCapacity(capacity, ++this.accessed,
                                  "fusiform similarity");
                }
                count.increment();
            }
        }
        this.edgeIterCounter.addAndGet(this.accessed);
        this.vertexIterCounter.addAndGet(vertexCount);

        // Delete source vertex
        assert similars.containsKey(vertex.id());
        similars.remove(vertex.id());
        if (similars.isEmpty()) {
            return ImmutableSet.of();
        }
        // Match alpha
        double neighborNum = neighbors.size();
        Map<Id, Double> matchedAlpha = newMap();
        for (Map.Entry<Id, MutableInt> entry : similars.entrySet()) {
            double score = entry.getValue().intValue() / neighborNum;
            if (score >= alpha) {
                matchedAlpha.put(entry.getKey(), score);
            }
        }
        if (matchedAlpha.size() < minSimilars) {
            return ImmutableSet.of();
        }

        // Sorted and topN if needed
        Map<Id, Double> topN;
        if (top > 0) {
            topN = topN(matchedAlpha, true, top);
        } else {
            topN = matchedAlpha;
        }
        // Filter by groupCount by property
        if (groupProperty != null) {
            Set<Object> values = newSet();
            // Add groupProperty value of source vertex
            values.add(vertex.value(groupProperty));
            for (Id id : topN.keySet()) {
                Vertex v = graph().vertices(id).next();
                values.add(v.value(groupProperty));
            }
            if (values.size() < minGroups) {
                return ImmutableSet.of();
            }
        }
        // Construct result
        Set<Similar> result = InsertionOrderUtil.newSet();
        for (Map.Entry<Id, Double> entry : topN.entrySet()) {
            Id similar = entry.getKey();
            double score = entry.getValue();
            List<Id> inters = withIntermediary ?
                              intermediaries.get(similar) :
                              ImmutableList.of();
            result.add(new Similar(similar, score, inters));
        }
        return result;
    }

    private boolean matchMinNeighborCount(HugeVertex vertex,
                                          Directions direction,
                                          String label,
                                          int minNeighbors,
                                          long degree) {
        Iterator<Edge> edges;
        long neighborCount;
        EdgeLabel edgeLabel = null;
        Id labelId = null;
        if (label != null) {
            edgeLabel = this.graph().edgeLabel(label);
            labelId = edgeLabel.id();
        }
        if (edgeLabel != null && edgeLabel.frequency() == Frequency.SINGLE) {
            edges = this.edgesOfVertex(vertex.id(), direction,
                                       labelId, minNeighbors);
            neighborCount = IteratorUtils.count(edges);
        } else {
            edges = this.edgesOfVertex(vertex.id(), direction, labelId, degree);
            Set<Id> neighbors = newIdSet();
            while (edges.hasNext()) {
                Id target = ((HugeEdge) edges.next()).id().otherVertexId();
                neighbors.add(target);
                if (neighbors.size() >= minNeighbors) {
                    break;
                }
            }
            neighborCount = neighbors.size();
        }
        return neighborCount >= minNeighbors;
    }

    private void reset() {
        this.accessed = 0L;
    }

    public static class Similar {

        private final Id id;
        private final double score;
        private final List<Id> intermediaries;

        public Similar(Id id, double score, List<Id> intermediaries) {
            this.id = id;
            this.score = score;
            assert newSet(intermediaries).size() == intermediaries.size() :
                    "Invalid intermediaries";
            this.intermediaries = intermediaries;
        }

        public Id id() {
            return this.id;
        }

        public double score() {
            return this.score;
        }

        public List<Id> intermediaries() {
            return this.intermediaries;
        }

        public Map<String, Object> toMap() {
            return ImmutableMap.of("id", this.id, "score", this.score,
                                   "intermediaries", this.intermediaries);
        }
    }

    public static class SimilarsMap {

        private final Map<Id, Set<Similar>> similars = newMap();

        public int size() {
            return this.similars.size();
        }

        public Set<Map.Entry<Id, Set<Similar>>> entrySet() {
            return this.similars.entrySet();
        }

        public void put(Id id, Set<Similar> similars) {
            this.similars.put(id, similars);
        }

        public Set<Id> vertices() {
            Set<Id> vertices = newIdSet();
            vertices.addAll(this.similars.keySet());
            for (Set<Similar> similars : this.similars.values()) {
                for (Similar similar : similars) {
                    vertices.add(similar.id());
                    vertices.addAll(similar.intermediaries());
                }
            }
            return vertices;
        }

        public Map<Id, Set<Map<String, Object>>> toMap() {
            Map<Id, Set<Map<String, Object>>> results = newMap();
            for (Map.Entry<Id, Set<Similar>> entry : this.similars.entrySet()) {
                Id source = entry.getKey();
                Set<Similar> similars = entry.getValue();
                Set<Map<String, Object>> result = InsertionOrderUtil.newSet();
                for (Similar similar : similars) {
                    result.add(similar.toMap());
                }
                results.put(source, result);
            }
            return results;
        }

        public boolean isEmpty() {
            return this.similars.isEmpty();
        }
    }
}
