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

package org.apache.hugegraph.traversal.algorithm.records;

import static org.apache.hugegraph.traversal.algorithm.HugeTraverser.NO_LIMIT;

import java.util.List;
import java.util.Stack;

import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.structure.HugeEdge;
import org.apache.hugegraph.traversal.algorithm.HugeTraverser.PathSet;
import org.apache.hugegraph.traversal.algorithm.records.record.Record;
import org.apache.hugegraph.traversal.algorithm.records.record.RecordType;
import org.apache.hugegraph.type.define.CollectionType;
import org.apache.hugegraph.util.collection.CollectionFactory;
import org.apache.hugegraph.util.collection.IntIterator;

public class KoutRecords extends SingleWayMultiPathsRecords {

    // Non-zero depth is used for deepFirst traverse mode.
    // In such case, startOneLayer/finishOneLayer should not be called,
    // instead, we should use addFullPath
    private final int depth;

    public KoutRecords(boolean concurrent, Id source, boolean nearest, int depth) {
        super(RecordType.INT, concurrent, source, nearest);

        // add depth(num) records to record each layer
        this.depth = depth;
        for (int i = 0; i < depth; i++) {
            this.records().push(this.newRecord());
        }
        assert (this.records().size() == (depth + 1));

        // init top layer's parentRecord
        this.currentRecord(this.records().peek(), null);
    }

    @Override
    public int size() {
        return this.currentRecord().size();
    }

    @Override
    public List<Id> ids(long limit) {
        List<Id> ids = CollectionFactory.newList(CollectionType.EC);
        IntIterator iterator = this.records().peek().keys();
        while ((limit == NO_LIMIT || limit-- > 0L) && iterator.hasNext()) {
            ids.add(this.id(iterator.next()));
        }
        return ids;
    }

    @Override
    public PathSet paths(long limit) {
        PathSet paths = new PathSet();
        Stack<Record> records = this.records();
        IntIterator iterator = records.peek().keys();
        while ((limit == NO_LIMIT || limit-- > 0L) && iterator.hasNext()) {
            paths.add(this.linkPath(records.size() - 1, iterator.next()));
        }
        return paths;
    }

    public void addFullPath(List<HugeEdge> edges) {
        assert (depth == edges.size());

        int sourceCode = this.code(edges.get(0).id().ownerVertexId());
        int targetCode;
        for (int i = 0; i < edges.size(); i++) {
            HugeEdge edge = edges.get(i);
            Id sourceV = edge.id().ownerVertexId();
            Id targetV = edge.id().otherVertexId();

            assert (this.code(sourceV) == sourceCode);

            this.edgeResults().addEdge(sourceV, targetV, edge);

            targetCode = this.code(targetV);
            Record record = this.records().elementAt(i + 1);
            if (this.sourceCode == targetCode) {
                break;
            }

            this.addPathToRecord(sourceCode, targetCode, record);
            sourceCode = targetCode;
        }
    }
}
