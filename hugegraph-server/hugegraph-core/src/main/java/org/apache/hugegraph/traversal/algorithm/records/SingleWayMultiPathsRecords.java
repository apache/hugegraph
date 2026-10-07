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

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Stack;
import java.util.function.Function;

import org.apache.hugegraph.exception.HugeException;
import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.perf.PerfUtil.Watched;
import org.apache.hugegraph.traversal.algorithm.HugeTraverser.EdgeRecord;
import org.apache.hugegraph.traversal.algorithm.HugeTraverser.Path;
import org.apache.hugegraph.traversal.algorithm.HugeTraverser.PathSet;
import org.apache.hugegraph.traversal.algorithm.records.record.Int2IntRecord;
import org.apache.hugegraph.traversal.algorithm.records.record.Record;
import org.apache.hugegraph.traversal.algorithm.records.record.RecordType;
import org.apache.hugegraph.type.define.CollectionType;
import org.apache.hugegraph.util.collection.CollectionFactory;
import org.apache.hugegraph.util.collection.IntIterator;
import org.apache.hugegraph.util.collection.IntMap;
import org.apache.hugegraph.util.collection.IntSet;

public abstract class SingleWayMultiPathsRecords extends AbstractRecords {

    protected final int sourceCode;
    private final Stack<Record> records;
    private final boolean nearest;
    private final IntSet accessedVertices;
    private final EdgeRecord edgeResults;
    private IntIterator parentRecordKeys;

    public SingleWayMultiPathsRecords(RecordType type, boolean concurrent,
                                      Id source, boolean nearest) {
        super(type, concurrent);

        this.nearest = nearest;

        this.sourceCode = this.code(source);
        Record firstRecord = this.newRecord();
        firstRecord.addPath(this.sourceCode, 0);
        this.records = new Stack<>();
        this.records.push(firstRecord);
        this.edgeResults = new EdgeRecord(concurrent);

        this.accessedVertices = CollectionFactory.newIntSet();
    }

    @Override
    public void startOneLayer(boolean forward) {
        Record parentRecord = this.records.peek();
        this.currentRecord(this.newRecord(), parentRecord);
        this.parentRecordKeys = parentRecord.keys();
    }

    @Override
    public void finishOneLayer() {
        this.records.push(this.currentRecord());
    }

    @Override
    public boolean hasNextKey() {
        return this.parentRecordKeys.hasNext();
    }

    @Override
    public Id nextKey() {
        return this.id(this.parentRecordKeys.next());
    }

    @Override
    public PathSet findPath(Id target, Function<Id, Boolean> filter,
                            boolean all, boolean ring) {
        PathSet paths = new PathSet();
        for (int i = 1; i < this.records.size(); i++) {
            IntIterator iterator = this.records.get(i).keys();
            while (iterator.hasNext()) {
                paths.add(this.linkPath(i, iterator.next()));
            }
        }
        return paths;
    }

    @Override
    public long accessed() {
        return this.accessedVertices.size();
    }

    public Iterator<Id> keys() {
        return new IntIterator.MapperInt2ObjectIterator<>(this.parentRecordKeys, this::id);
    }

    @Watched
    public void addPath(Id source, Id target) {
        this.addPathToRecord(this.code(source), this.code(target), this.currentRecord());
    }

    public void addPathToRecord(int sourceCode, int targetCode, Record record) {
        if (this.nearest && this.accessedVertices.contains(targetCode) ||
            !this.nearest && record.containsKey(targetCode) ||
            targetCode == this.sourceCode) {
            return;
        }
        record.addPath(targetCode, sourceCode);
        this.accessedVertices.add(targetCode);
    }

    protected final Path linkPath(int target) {
        List<Id> ids = CollectionFactory.newList(CollectionType.EC);
        // Find the layer where the target is located
        int foundLayer = -1;
        for (int i = 0; i < this.records.size(); i++) {
            IntMap layer = this.layer(i);
            if (!layer.containsKey(target)) {
                continue;
            }

            foundLayer = i;
            // Collect self node
            ids.add(this.id(target));
            break;
        }
        // If a layer found, then concat parents
        if (foundLayer > 0) {
            for (int i = foundLayer; i > 0; i--) {
                IntMap layer = this.layer(i);
                // Uptrack parents
                target = layer.get(target);
                ids.add(this.id(target));
            }
        }
        return new Path(ids);
    }

    protected final Path linkPath(int layerIndex, int target) {
        List<Id> ids = CollectionFactory.newList(CollectionType.EC);
        IntMap layer = this.layer(layerIndex);
        if (!layer.containsKey(target)) {
            throw new HugeException("Failed to get path for %s",
                                    this.id(target));
        }
        // Collect self node
        ids.add(this.id(target));
        // Concat parents
        for (int i = layerIndex; i > 0; i--) {
            layer = this.layer(i);
            // Uptrack parents
            target = layer.get(target);
            ids.add(this.id(target));
        }
        Collections.reverse(ids);
        return new Path(ids);
    }

    protected final IntMap layer(int layerIndex) {
        Record record = this.records.elementAt(layerIndex);
        return ((Int2IntRecord) record).layer();
    }

    protected final Stack<Record> records() {
        return this.records;
    }

    public EdgeRecord edgeResults() {
        return edgeResults;
    }

    public abstract int size();

    public abstract List<Id> ids(long limit);

    public abstract PathSet paths(long limit);
}
