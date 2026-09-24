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

import org.apache.hugegraph.pd.grpc.watch.NodeEventType;

public class NodeEvent {

    private String graph;
    private long nodeId;
    private EventType eventType;

    public NodeEvent(String graph, long nodeId, EventType eventType) {
        this.graph = graph;
        this.nodeId = nodeId;
        this.eventType = eventType;
    }

    public String getGraph() {
        return graph;
    }

    public long getNodeId() {
        return nodeId;
    }

    public EventType getEventType() {
        return eventType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        NodeEvent nodeEvent = (NodeEvent) o;
        return nodeId == nodeEvent.nodeId && Objects.equals(graph,
                                                            nodeEvent.graph) &&
               eventType == nodeEvent.eventType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(graph, nodeId, eventType);
    }

    @Override
    public String toString() {
        return "NodeEvent{" +
               "graph='" + graph + '\'' +
               ", nodeId=" + nodeId +
               ", eventType=" + eventType +
               '}';
    }

    public enum EventType {
        UNKNOWN,
        NODE_ONLINE,
        NODE_OFFLINE,
        NODE_RAFT_CHANGE,
        NODE_PD_LEADER_CHANGE;

        public static EventType grpcTypeOf(NodeEventType grpcType) {
            switch (grpcType) {
                case NODE_EVENT_TYPE_NODE_ONLINE:
                    return NODE_ONLINE;
                case NODE_EVENT_TYPE_NODE_OFFLINE:
                    return NODE_OFFLINE;
                case NODE_EVENT_TYPE_NODE_RAFT_CHANGE:
                    return NODE_RAFT_CHANGE;
                case NODE_EVENT_TYPE_PD_LEADER_CHANGE:
                    return NODE_PD_LEADER_CHANGE;
                default:
                    return UNKNOWN;
            }

        }

    }
}
