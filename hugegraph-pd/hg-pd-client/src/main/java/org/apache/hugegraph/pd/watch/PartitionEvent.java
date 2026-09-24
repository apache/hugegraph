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

package org.apache.hugegraph.pd.watch;

import java.util.Objects;

import org.apache.hugegraph.pd.grpc.watch.WatchChangeType;

public class PartitionEvent {

    private String graph;
    private int partitionId;
    private ChangeType changeType;

    public PartitionEvent(String graph, int partitionId, ChangeType changeType) {
        this.graph = graph;
        this.partitionId = partitionId;
        this.changeType = changeType;
    }

    public String getGraph() {
        return this.graph;
    }

    public int getPartitionId() {
        return this.partitionId;
    }

    public ChangeType getChangeType() {
        return this.changeType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PartitionEvent that = (PartitionEvent) o;
        return partitionId == that.partitionId && Objects.equals(graph, that.graph) &&
               changeType == that.changeType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(graph, partitionId, changeType);
    }

    @Override
    public String toString() {
        return "PartitionEvent{" +
               "graph='" + graph + '\'' +
               ", partitionId=" + partitionId +
               ", changeType=" + changeType +
               '}';
    }

    public enum ChangeType {
        UNKNOWN,
        ADD,
        ALTER,
        DEL;

        public static ChangeType grpcTypeOf(WatchChangeType grpcType) {
            switch (grpcType) {
                case WATCH_CHANGE_TYPE_ADD:
                    return ADD;
                case WATCH_CHANGE_TYPE_ALTER:
                    return ALTER;
                case WATCH_CHANGE_TYPE_DEL:
                    return DEL;
                default:
                    return UNKNOWN;
            }
        }
    }
}
