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

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.query.ConditionQuery;
import org.apache.hugegraph.iterator.FilterIterator;
import org.apache.hugegraph.struct.schema.EdgeLabel;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.type.define.Directions;
import org.apache.hugegraph.type.define.HugeKeys;
import org.apache.tinkerpop.gremlin.structure.Edge;

public class EdgeExistenceTraverser extends HugeTraverser {

    public EdgeExistenceTraverser(HugeGraph graph) {
        super(graph);
    }

    public Iterator<Edge> queryEdgeExistence(Id sourceId, Id targetId, String label,
                                             String sortValues, long limit) {
        // If no label provided, fallback to a slow query by filtering
        if (label == null || label.isEmpty()) {
            return queryByNeighbors(sourceId, targetId, limit);
        }

        EdgeLabel edgeLabel = graph().edgeLabel(label);
        ConditionQuery conditionQuery = new ConditionQuery(HugeType.EDGE);
        conditionQuery.eq(HugeKeys.OWNER_VERTEX, sourceId);
        conditionQuery.eq(HugeKeys.OTHER_VERTEX, targetId);
        conditionQuery.eq(HugeKeys.LABEL, edgeLabel.id());
        conditionQuery.eq(HugeKeys.DIRECTION, Directions.OUT);
        conditionQuery.limit(limit);

        if (edgeLabel.existSortKeys() && !sortValues.isEmpty()) {
            conditionQuery.eq(HugeKeys.SORT_VALUES, sortValues);
        } else {
            conditionQuery.eq(HugeKeys.SORT_VALUES, "");
        }
        return graph().edges(conditionQuery);
    }

    private Iterator<Edge> queryByNeighbors(Id sourceId, Id targetId, long limit) {
        return new FilterIterator<>(edgesOfVertex(sourceId, Directions.OUT, (Id) null, limit),
                                    edge -> targetId.equals(edge.inVertex().id()));
    }
}
