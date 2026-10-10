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

import org.apache.commons.collections.CollectionUtils;
import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.perf.PerfUtil.Watched;
import org.apache.hugegraph.traversal.algorithm.HugeTraverser.Path;
import org.apache.hugegraph.traversal.algorithm.HugeTraverser.PathSet;
import org.apache.hugegraph.traversal.algorithm.records.record.Record;
import org.apache.hugegraph.traversal.algorithm.records.record.RecordType;
import org.apache.hugegraph.util.collection.IntIterator;

import com.google.common.collect.Lists;

public abstract class DoubleWayMultiPathsRecords extends AbstractRecords {

    private final Stack<Record> sourceRecords;
    private final Stack<Record> targetRecords;

    private IntIterator parentRecordKeys;
    private int currentKey;
    private boolean movingForward;
    private long accessed;

    public DoubleWayMultiPathsRecords(RecordType type, boolean concurrent,
                                      Id sourceV, Id targetV) {
        super(type, concurrent);
        int sourceCode = this.code(sourceV);
        int targetCode = this.code(targetV);
        Record firstSourceRecord = this.newRecord();
        Record firstTargetRecord = this.newRecord();
        firstSourceRecord.addPath(sourceCode, 0);
        firstTargetRecord.addPath(targetCode, 0);
        this.sourceRecords = new Stack<>();
        this.targetRecords = new Stack<>();
        this.sourceRecords.push(firstSourceRecord);
        this.targetRecords.push(firstTargetRecord);

        this.accessed = 2L;
    }

    @Override
    public void startOneLayer(boolean forward) {
        this.movingForward = forward;
        Record parentRecord = this.movingForward ?
                              this.sourceRecords.peek() :
                              this.targetRecords.peek();
        this.currentRecord(this.newRecord(), parentRecord);
        this.parentRecordKeys = parentRecord.keys();
    }

    @Override
    public void finishOneLayer() {
        Record record = this.currentRecord();
        if (this.movingForward) {
            this.sourceRecords.push(record);
        } else {
            this.targetRecords.push(record);
        }
        this.accessed += record.size();
    }

    @Watched
    @Override
    public boolean hasNextKey() {
        return this.parentRecordKeys.hasNext();
    }

    @Watched
    @Override
    public Id nextKey() {
        this.currentKey = this.parentRecordKeys.next();
        return this.id(this.currentKey);
    }

    public boolean parentsContain(int id) {
        Record parentRecord = this.parentRecord();
        if (parentRecord == null) {
            return false;
        }

        IntIterator parents = parentRecord.get(this.currentKey);
        while (parents.hasNext()) {
            int parent = parents.next();
            if (parent == id) {
                // Find backtrace path, stop
                return true;
            }
        }
        return false;
    }

    @Override
    public long accessed() {
        return this.accessed;
    }

    public boolean sourcesLessThanTargets() {
        return this.sourceRecords.peek().size() <=
               this.targetRecords.peek().size();
    }

    @Watched
    protected final PathSet linkPath(int source, int target, boolean ring) {
        PathSet paths = new PathSet();
        PathSet sources = this.linkSourcePath(source);
        PathSet targets = this.linkTargetPath(target);
        for (Path tpath : targets) {
            tpath.reverse();
            for (Path spath : sources) {
                if (!ring) {
                    // Avoid loop in path
                    if (CollectionUtils.containsAny(spath.vertices(),
                                                    tpath.vertices())) {
                        continue;
                    }
                }
                List<Id> ids = new ArrayList<>(spath.vertices());
                ids.addAll(tpath.vertices());
                Id crosspoint = this.id(this.movingForward ? target : source);
                paths.add(new Path(crosspoint, ids));
            }
        }
        return paths;
    }

    private PathSet linkSourcePath(int source) {
        return this.linkPathLayer(this.sourceRecords, source,
                                  this.sourceRecords.size() - 1);
    }

    private PathSet linkTargetPath(int target) {
        return this.linkPathLayer(this.targetRecords, target,
                                  this.targetRecords.size() - 1);
    }

    private PathSet linkPathLayer(Stack<Record> all, int id, int layerIndex) {
        PathSet results = new PathSet();
        if (layerIndex == 0) {
            Id sid = this.id(id);
            results.add(new Path(Lists.newArrayList(sid)));
            return results;
        }

        Id current = this.id(id);
        Record layer = all.elementAt(layerIndex);
        IntIterator iterator = layer.get(id);
        while (iterator.hasNext()) {
            int parent = iterator.next();
            PathSet paths = this.linkPathLayer(all, parent, layerIndex - 1);
            paths.append(current);
            results.addAll(paths);
        }
        return results;
    }

    @Watched
    protected final void addPath(int current, int parent) {
        this.currentRecord().addPath(current, parent);
    }

    protected final boolean sourceContains(int node) {
        return this.sourceRecords.peek().containsKey(node);
    }

    protected final boolean targetContains(int node) {
        return this.targetRecords.peek().containsKey(node);
    }

    protected final Stack<Record> sourceRecords() {
        return this.sourceRecords;
    }

    protected final Stack<Record> targetRecords() {
        return this.targetRecords;
    }

    protected final boolean movingForward() {
        return this.movingForward;
    }

    protected final int current() {
        return this.currentKey;
    }
}
