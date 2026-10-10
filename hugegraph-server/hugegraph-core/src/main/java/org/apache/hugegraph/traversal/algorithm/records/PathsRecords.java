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

import java.util.function.Function;

import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.perf.PerfUtil.Watched;
import org.apache.hugegraph.traversal.algorithm.HugeTraverser.PathSet;
import org.apache.hugegraph.traversal.algorithm.records.record.RecordType;

public class PathsRecords extends DoubleWayMultiPathsRecords {

    public PathsRecords(boolean concurrent, Id sourceV, Id targetV) {
        super(RecordType.ARRAY, concurrent, sourceV, targetV);
    }

    @Watched
    @Override
    public PathSet findPath(Id target, Function<Id, Boolean> filter,
                            boolean all, boolean ring) {
        assert all;
        int targetCode = this.code(target);
        int parentCode = this.current();
        PathSet paths = PathSet.EMPTY;

        // Traverse backtrace is not allowed, stop now
        if (this.parentsContain(targetCode)) {
            return paths;
        }

        // Add to current layer
        this.addPath(targetCode, parentCode);
        // If cross point exists, path found, concat them
        if (this.movingForward() && this.targetContains(targetCode)) {
            paths = this.linkPath(parentCode, targetCode, ring);
        }
        if (!this.movingForward() && this.sourceContains(targetCode)) {
            paths = this.linkPath(targetCode, parentCode, ring);
        }
        return paths;
    }
}
