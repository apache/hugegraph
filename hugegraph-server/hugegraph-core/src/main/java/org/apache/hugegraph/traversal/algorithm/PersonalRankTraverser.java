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
import java.util.function.BiFunction;

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.struct.schema.EdgeLabel;
import org.apache.hugegraph.struct.schema.VertexLabel;
import org.apache.hugegraph.structure.HugeVertex;
import org.apache.hugegraph.type.define.Directions;
import org.apache.hugegraph.util.E;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

public class PersonalRankTraverser extends HugeTraverser {

    private final double alpha;
    private final long degree;
    private final int maxDepth;

    public PersonalRankTraverser(HugeGraph graph, double alpha,
                                 long degree, int maxDepth) {
        super(graph);
        this.alpha = alpha;
        this.degree = degree;
        this.maxDepth = maxDepth;
    }

    public Map<Id, Double> personalRank(Id source, String label,
                                        WithLabel withLabel) {
        E.checkNotNull(source, "source vertex id");
        this.checkVertexExist(source, "source vertex");
        E.checkArgumentNotNull(label, "The edge label can't be null");

        Map<Id, Double> ranks = newMap();
        ranks.put(source, 1.0);

        Id labelId = this.getEdgeLabelIdOrNull(label);
        Directions dir = this.getStartDirection(source, label);

        Set<Id> outSeeds = newIdSet();
        Set<Id> inSeeds = newIdSet();
        if (dir == Directions.OUT) {
            outSeeds.add(source);
        } else {
            inSeeds.add(source);
        }

        Set<Id> rootAdjacencies = newIdSet();
        for (long i = 0; i < this.maxDepth; i++) {
            Map<Id, Double> newRanks = this.calcNewRanks(outSeeds, inSeeds,
                                                         labelId, ranks);
            ranks = this.compensateRoot(source, newRanks);
            if (i == 0) {
                rootAdjacencies.addAll(ranks.keySet());
            }
        }
        // Remove directly connected neighbors
        removeAll(ranks, rootAdjacencies);
        // Remove unnecessary label
        if (withLabel == WithLabel.SAME_LABEL) {
            removeAll(ranks, dir == Directions.OUT ? inSeeds : outSeeds);
        } else if (withLabel == WithLabel.OTHER_LABEL) {
            removeAll(ranks, dir == Directions.OUT ? outSeeds : inSeeds);
        }
        return ranks;
    }

    private Map<Id, Double> calcNewRanks(Set<Id> outSeeds, Set<Id> inSeeds,
                                         Id label, Map<Id, Double> ranks) {
        Map<Id, Double> newRanks = newMap();
        BiFunction<Set<Id>, Directions, Set<Id>> neighborIncrRanks;
        neighborIncrRanks = (seeds, dir) -> {
            Set<Id> tmpSeeds = newIdSet();
            for (Id seed : seeds) {
                Double oldRank = ranks.get(seed);
                E.checkState(oldRank != null, "Expect rank of seed exists");

                Iterator<Id> iter = this.adjacentVertices(seed, dir, label,
                                                          this.degree);
                List<Id> neighbors = IteratorUtils.list(iter);

                long degree = neighbors.size();
                if (degree == 0L) {
                    newRanks.put(seed, oldRank);
                    continue;
                }
                double incrRank = oldRank * this.alpha / degree;

                // Collect all neighbors increment
                for (Id neighbor : neighbors) {
                    tmpSeeds.add(neighbor);
                    // Assign an initial value when firstly update neighbor rank
                    double rank = newRanks.getOrDefault(neighbor, 0.0);
                    newRanks.put(neighbor, rank + incrRank);
                }
            }
            return tmpSeeds;
        };

        Set<Id> tmpInSeeds = neighborIncrRanks.apply(outSeeds, Directions.OUT);
        Set<Id> tmpOutSeeds = neighborIncrRanks.apply(inSeeds, Directions.IN);

        outSeeds.addAll(tmpOutSeeds);
        inSeeds.addAll(tmpInSeeds);
        return newRanks;
    }

    private Map<Id, Double> compensateRoot(Id root, Map<Id, Double> newRanks) {
        double rank = newRanks.getOrDefault(root, 0.0);
        rank += (1 - this.alpha);
        newRanks.put(root, rank);
        return newRanks;
    }

    private Directions getStartDirection(Id source, String label) {
        // NOTE: The outer layer needs to ensure that the vertex Id is valid
        HugeVertex vertex = (HugeVertex) graph().vertices(source).next();
        VertexLabel vertexLabel = vertex.schemaLabel();
        EdgeLabel edgeLabel = this.graph().edgeLabel(label);
        Id sourceLabel = edgeLabel.sourceLabel();
        Id targetLabel = edgeLabel.targetLabel();

        E.checkArgument(edgeLabel.linkWithLabel(vertexLabel.id()),
                        "The vertex '%s' doesn't link with edge label '%s'",
                        source, label);
        E.checkArgument(!sourceLabel.equals(targetLabel),
                        "The edge label for personal rank must " +
                        "link different vertex labels");
        if (sourceLabel.equals(vertexLabel.id())) {
            return Directions.OUT;
        } else {
            assert targetLabel.equals(vertexLabel.id());
            return Directions.IN;
        }
    }

    private static void removeAll(Map<Id, Double> map, Set<Id> keys) {
        for (Id key : keys) {
            map.remove(key);
        }
    }

    public enum WithLabel {
        SAME_LABEL,
        OTHER_LABEL,
        BOTH_LABEL
    }
}
