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

import static org.apache.hugegraph.pd.common.HgAssert.isArgumentNotNull;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.hugegraph.pd.grpc.watch.NodeEventType;
import org.apache.hugegraph.pd.grpc.watch.WatchResponse;
import org.apache.hugegraph.pd.grpc.watch.WatchType;

/**
 * The subject of partition change.
 */
@ThreadSafe
final class NodeChangeSubject extends AbstractWatchSubject {

    NodeChangeSubject() {
        super(WatchType.WATCH_TYPE_STORE_NODE_CHANGE);
    }

    @Override
    String toNoticeString(WatchResponse res) {
        StringBuilder sb = new StringBuilder();
        return sb.append("graph:").append(res.getNodeResponse().getGraph())
                 .append(",")
                 .append("nodeId:").append(res.getNodeResponse().getNodeId())
                 .toString();
    }

    public void notifyWatcher(NodeEventType nodeEventType, String graph, long nodeId) {
        isArgumentNotNull(nodeEventType, "nodeEventType");

        super.notifyWatcher(builder -> {
            builder.setNodeResponse(
                    builder.getNodeResponseBuilder().clear()
                           .setGraph(graph)
                           .setNodeId(nodeId)
                           .setNodeEventType(nodeEventType)
                           .build()
            );

        });
    }

    @Override
    public void notifyError(int code, String message) {
        super.notifyError(code, message);
    }
}
