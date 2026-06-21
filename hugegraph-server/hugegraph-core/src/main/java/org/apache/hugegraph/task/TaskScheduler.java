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

package org.apache.hugegraph.task;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.backend.id.Id;

public interface TaskScheduler {

    long NO_LIMIT = -1L;
    long PAGE_SIZE = 500L;
    long QUERY_INTERVAL = 100L;
    int MAX_PENDING_TASKS = 10000;

    HugeGraph graph();

    int pendingTasks();

    <V> void restoreTasks();

    <V> Future<?> schedule(HugeTask<V> task);

    <V> void cancel(HugeTask<V> task);

    <V> void save(HugeTask<V> task);

    <V> HugeTask<V> delete(Id id, boolean force);

    <V> HugeTask<V> task(Id id);

    default <V> HugeTask<V> task(Id id, boolean withResult) {
        return this.task(id);
    }

    <V> Iterator<HugeTask<V>> tasks(List<Id> ids);

    default <V> Iterator<HugeTask<V>> tasks(List<Id> ids,
                                            boolean withResult) {
        return this.tasks(ids);
    }

    <V> Iterator<HugeTask<V>> tasks(TaskStatus status,
                                    long limit, String page);

    default <V> Iterator<HugeTask<V>> tasks(TaskStatus status, long limit,
                                            String page, boolean withResult) {
        return this.tasks(status, limit, page);
    }

    void init();

    boolean close();

    <V> HugeTask<V> waitUntilTaskCompleted(Id id, long seconds)
            throws TimeoutException;

    <V> HugeTask<V> waitUntilTaskCompleted(Id id)
            throws TimeoutException;

    void waitUntilAllTasksCompleted(long seconds)
            throws TimeoutException;

    void checkRequirement(String op);

    <V> V call(Callable<V> callable);

    <V> V call(Runnable runnable);

    ServerInfoManager serverManager();

    String graphName();

    String spaceGraphName();

    void taskDone(HugeTask<?> task);
}
