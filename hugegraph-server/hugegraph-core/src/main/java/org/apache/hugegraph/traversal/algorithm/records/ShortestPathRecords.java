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

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;
import java.util.function.Function;

import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.traversal.algorithm.HugeTraverser.Path;
import org.apache.hugegraph.traversal.algorithm.HugeTraverser.PathSet;
import org.apache.hugegraph.traversal.algorithm.records.record.Int2IntRecord;
import org.apache.hugegraph.traversal.algorithm.records.record.Record;
import org.apache.hugegraph.traversal.algorithm.records.record.RecordType;
import org.apache.hugegraph.util.collection.CollectionFactory;
import org.apache.hugegraph.util.collection.IntMap;
import org.apache.hugegraph.util.collection.IntSet;

public class ShortestPathRecords extends DoubleWayMultiPathsRecords {

    private final IntSet accessedVertices;
    private boolean pathFound;

    public ShortestPathRecords(Id sourceV, Id targetV) {
        super(RecordType.INT, false, sourceV, targetV);

        this.accessedVertices = CollectionFactory.newIntSet();
        this.accessedVertices.add(this.code(sourceV));
        this.accessedVertices.add(this.code(targetV));
        this.pathFound = false;
    }

    @Override
    public PathSet findPath(Id target, Function<Id, Boolean> filter,
                            boolean all, boolean ring) {
        assert !ring;
        PathSet paths = new PathSet();
        int targetCode = this.code(target);
        int parentCode = this.current();
        // If cross point exists, shortest path found, concat them
        if (this.movingForward() && this.targetContains(targetCode) ||
            !this.movingForward() && this.sourceContains(targetCode)) {
            if (!filter.apply(target)) {
                return paths;
            }
            paths.add(this.movingForward() ?
                      this.linkPath(parentCode, targetCode) :
                      this.linkPath(targetCode, parentCode));
            this.pathFound = true;
            if (!all) {
                return paths;
            }
        }
        /*
         * Not found shortest path yet, node is added to current layer if:
         * 1. not in sources and newVertices yet
         * 2. path of node doesn't have loop
         */
        if (!this.pathFound && this.isNew(targetCode)) {
            this.addPath(targetCode, parentCode);
        }
        return paths;
    }

    private boolean isNew(int node) {
        return !this.currentRecord().containsKey(node) &&
               !this.accessedVertices.contains(node);
    }

    private Path linkPath(int source, int target) {
        Path sourcePath = this.linkSourcePath(source);
        Path targetPath = this.linkTargetPath(target);
        sourcePath.reverse();
        List<Id> ids = new ArrayList<>(sourcePath.vertices());
        ids.addAll(targetPath.vertices());
        return new Path(ids);
    }

    private Path linkSourcePath(int source) {
        return this.linkPath(this.sourceRecords(), source);
    }

    private Path linkTargetPath(int target) {
        return this.linkPath(this.targetRecords(), target);
    }

    private Path linkPath(Stack<Record> all, int node) {
        int size = all.size();
        List<Id> ids = new ArrayList<>(size);
        ids.add(this.id(node));
        int value = node;
        for (int i = size - 1; i > 0; i--) {
            IntMap layer = ((Int2IntRecord) all.elementAt(i)).layer();
            value = layer.get(value);
            ids.add(this.id(value));
        }
        return new Path(ids);
    }
}
