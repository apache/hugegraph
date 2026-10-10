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

package org.apache.hugegraph.traversal.algorithm.strategy;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.traversal.algorithm.OltpTraverser;
import org.apache.hugegraph.traversal.algorithm.steps.EdgeStep;

public class ConcurrentTraverseStrategy extends OltpTraverser
        implements TraverseStrategy {

    public ConcurrentTraverseStrategy(HugeGraph graph) {
        super(graph);
    }

    @Override
    public Map<Id, List<Node>> newMultiValueMap() {
        return new OltpTraverser.ConcurrentMultiValuedMap<>();
    }

    @Override
    public void traverseOneLayer(Map<Id, List<Node>> vertices,
                                 EdgeStep step,
                                 BiConsumer<Id, EdgeStep> biConsumer) {
        traverseIds(vertices.keySet().iterator(), (id) -> {
            biConsumer.accept(id, step);
        });
    }

    @Override
    public Set<Path> newPathSet() {
        return ConcurrentHashMap.newKeySet();
    }

    @Override
    public void addNode(Map<Id, List<Node>> vertices, Id id, Node node) {
        ((ConcurrentMultiValuedMap<Id, Node>) vertices).add(id, node);
    }

    @Override
    public void addNewVerticesToAll(Map<Id, List<Node>> newVertices,
                                    Map<Id, List<Node>> targets) {
        ConcurrentMultiValuedMap<Id, Node> vertices =
                (ConcurrentMultiValuedMap<Id, Node>) targets;
        for (Map.Entry<Id, List<Node>> entry : newVertices.entrySet()) {
            vertices.addAll(entry.getKey(), entry.getValue());
        }
    }
}
