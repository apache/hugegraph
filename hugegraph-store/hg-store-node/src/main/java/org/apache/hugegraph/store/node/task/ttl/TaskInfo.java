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

package org.apache.hugegraph.store.node.task.ttl;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hugegraph.store.business.BusinessHandlerImpl;
import org.apache.hugegraph.store.node.grpc.HgStoreNodeService;

import lombok.Data;

/**
 * @date 2024/5/7
 **/
@Data
public class TaskInfo {

    String graph;
    boolean isRaft;
    transient BusinessHandlerImpl handler;
    long startTime;
    String[] tables;
    ConcurrentHashMap<String, AtomicLong> tableCounter;
    transient TaskSubmitter taskSubmitter;

    public TaskInfo(BusinessHandlerImpl handler, String graph, boolean isRaft, long startTime,
                    String[] tables, HgStoreNodeService service) {
        this.handler = handler;
        this.graph = graph;
        this.isRaft = isRaft;
        this.tables = tables;
        this.startTime = startTime;
        this.tableCounter = new ConcurrentHashMap(tables.length);
        for (String table : tables) {
            tableCounter.put(table, new AtomicLong());
        }
        this.taskSubmitter =
                isRaft ? new RaftTaskSubmitter(service, handler) :
                new DefaulTaskSubmitter(service, handler);
    }

    public List<Integer> getPartitionIds() {
        return isRaft ? handler.getLeaderPartitionIds(graph) : handler.getPartitionIds(graph);
    }
}
