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
import java.util.function.BiConsumer;

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.traversal.algorithm.HugeTraverser;
import org.apache.hugegraph.traversal.algorithm.steps.EdgeStep;

public interface TraverseStrategy {

    void traverseOneLayer(Map<Id, List<HugeTraverser.Node>> vertices,
                          EdgeStep step, BiConsumer<Id, EdgeStep> consumer);

    Map<Id, List<HugeTraverser.Node>> newMultiValueMap();

    Set<HugeTraverser.Path> newPathSet();

    void addNode(Map<Id, List<HugeTraverser.Node>> vertices, Id id,
                 HugeTraverser.Node node);

    void addNewVerticesToAll(Map<Id, List<HugeTraverser.Node>> newVertices,
                             Map<Id, List<HugeTraverser.Node>> targets);

    static TraverseStrategy create(boolean concurrent, HugeGraph graph) {
        return concurrent ? new ConcurrentTraverseStrategy(graph) :
               new SingleTraverseStrategy(graph);
    }
}
