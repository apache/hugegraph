/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hugegraph.task;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import org.apache.hugegraph.HugeGraphParams;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.query.Condition;
import org.apache.hugegraph.backend.query.ConditionQuery;
import org.apache.hugegraph.backend.query.QueryResults;
import org.apache.hugegraph.backend.store.BackendStore;
import org.apache.hugegraph.exception.NotFoundException;
import org.apache.hugegraph.iterator.ListIterator;
import org.apache.hugegraph.iterator.MapperIterator;
import org.apache.hugegraph.schema.PropertyKey;
import org.apache.hugegraph.schema.VertexLabel;
import org.apache.hugegraph.structure.HugeVertex;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.type.define.HugeKeys;
import org.apache.hugegraph.util.E;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import com.google.common.collect.ImmutableMap;

/**
 * Base class of task & result scheduler
 */
public abstract class TaskAndResultScheduler implements TaskScheduler {

    /**
     * Which graph the scheduler belongs to
     */
    protected final HugeGraphParams graph;
    protected final String graphSpace;
    protected final String graphName;

    /**
     * Task transactions, for persistence
     */
    protected volatile TaskAndResultTransaction taskTx = null;

    private final ServerInfoManager serverManager;

    public TaskAndResultScheduler(
            HugeGraphParams graph,
            ExecutorService serverInfoDbExecutor) {
        E.checkNotNull(graph, "graph");

        this.graph = graph;
        this.graphSpace = graph.graph().graphSpace();
        this.graphName = graph.name();

        this.serverManager = new ServerInfoManager(graph, serverInfoDbExecutor);
    }

    @Override
    public <V> void save(HugeTask<V> task) {
        E.checkArgumentNotNull(task, "Task can't be null");
        String rawResult = task.result();

        // Save task without result;
        this.call(() -> {
            // Construct vertex from task
            HugeVertex vertex = this.tx().constructTaskVertex(task);
            // Delete index of old vertex to avoid stale index
            this.tx().deleteIndex(vertex);
            // Add or update task info to backend store
            return this.tx().addVertex(vertex);
        });

        // Save result outcome
        if (rawResult != null) {
            HugeTaskResult result =
                    new HugeTaskResult(HugeTaskResult.genId(task.id()));
            result.result(rawResult);

            this.call(() -> {
                // Construct vertex from task
                HugeVertex vertex = this.tx().constructTaskResultVertex(result);
                // Add or update task info to backend store
                return this.tx().addVertex(vertex);
            });
        }
    }

    @Override
    public <V> HugeTask<V> task(Id id) {
        return this.task(id, true);
    }

    @Override
    public <V> HugeTask<V> task(Id id, boolean withResult) {
        HugeTask<V> task = this.call(() -> {
            Iterator<Vertex> vertices = this.tx().queryTaskInfos(id);
            Vertex vertex = QueryResults.one(vertices);
            if (vertex == null) {
                return null;
            }
            return HugeTask.fromVertex(vertex, withResult);
        });

        if (task == null) {
            throw new NotFoundException("Can't find task with id '%s'", id);
        }

        if (withResult) {
            HugeTaskResult taskResult = queryTaskResult(id);
            if (taskResult != null) {
                task.result(taskResult);
            }
        }

        return task;
    }

    @Override
    public <V> Iterator<HugeTask<V>> tasks(List<Id> ids) {
        return this.tasks(ids, false);
    }

    @Override
    public <V> Iterator<HugeTask<V>> tasks(List<Id> ids,
                                           boolean withResult) {
        if (!withResult) {
            return this.tasksWithoutResult(ids);
        }
        return this.queryTask(ids);
    }

    @Override
    public <V> Iterator<HugeTask<V>> tasks(TaskStatus status, long limit,
                                           String page) {
        return this.tasks(status, limit, page, false);
    }

    @Override
    public <V> Iterator<HugeTask<V>> tasks(TaskStatus status, long limit,
                                           String page, boolean withResult) {
        if (!withResult) {
            if (status == null) {
                return this.queryTaskWithoutResult(ImmutableMap.of(), limit,
                                                   page);
            }
            return this.queryTaskWithoutResult(HugeTask.P.STATUS,
                                               status.code(), limit, page);
        }
        if (status == null) {
            return this.queryTask(ImmutableMap.of(), limit, page);
        }
        return this.queryTask(HugeTask.P.STATUS, status.code(), limit, page);
    }

    protected <V> Iterator<HugeTask<V>> queryTask(String key, Object value,
                                                  long limit, String page) {
        return this.queryTask(ImmutableMap.of(key, value), limit, page);
    }

    protected <V> Iterator<HugeTask<V>> queryTask(Map<String, Object> conditions,
                                                  long limit, String page) {
        Iterator<HugeTask<V>> ts = this.call(() -> {
            ConditionQuery query = new ConditionQuery(HugeType.TASK);
            if (page != null) {
                query.page(page);
            }
            VertexLabel vl = this.graph().vertexLabel(HugeTask.P.TASK);
            query.eq(HugeKeys.LABEL, vl.id());
            for (Map.Entry<String, Object> entry : conditions.entrySet()) {
                PropertyKey pk = this.graph().propertyKey(entry.getKey());
                query.query(Condition.eq(pk.id(), entry.getValue()));
            }
            query.showHidden(true);
            if (limit != NO_LIMIT) {
                query.limit(limit);
            }
            Iterator<Vertex> vertices = this.tx().queryTaskInfos(query);
            Iterator<HugeTask<V>> tasks =
                    new MapperIterator<>(vertices, HugeTask::fromVertex);
            // Convert iterator to list to avoid across thread tx accessed
            return QueryResults.toList(tasks);
        });

        return new MapperIterator<>(ts, (task) -> {
            HugeTaskResult taskResult = queryTaskResult(task.id());
            if (taskResult != null) {
                task.result(taskResult);
            }
            return task;
        });
    }

    protected <V> Iterator<HugeTask<V>> queryTask(List<Id> ids) {
        ListIterator<HugeTask<V>> ts = this.call(
                () -> {
                    Object[] idArray = ids.toArray(new Id[ids.size()]);
                    Iterator<Vertex> vertices = this.tx()
                                                    .queryTaskInfos(idArray);
                    Iterator<HugeTask<V>> tasks =
                            new MapperIterator<>(vertices,
                                                 HugeTask::fromVertex);
                    // Convert iterator to list to avoid across thread tx accessed
                    return QueryResults.toList(tasks);
                });

        Iterator<HugeTaskResult> results = queryTaskResult(ids);

        HashMap<String, HugeTaskResult> resultCaches = new HashMap<>();
        while (results.hasNext()) {
            HugeTaskResult entry = results.next();
            resultCaches.put(entry.taskResultId(), entry);
        }

        return new MapperIterator<>(ts, (task) -> {
            HugeTaskResult taskResult =
                    resultCaches.get(HugeTaskResult.genId(task.id()));
            if (taskResult != null) {
                task.result(taskResult);
            }
            return task;
        });
    }

    protected <V> HugeTask<V> taskWithoutResult(Id id) {
        HugeTask<V> result = this.call(() -> {
            Iterator<Vertex> vertices = this.tx().queryTaskInfos(id);
            Vertex vertex = QueryResults.one(vertices);
            if (vertex == null) {
                return null;
            }
            return HugeTask.fromVertex(vertex, false);
        });

        if (result == null) {
            throw new NotFoundException("Can't find task with id '%s'", id);
        }

        return result;
    }

    protected <V> Iterator<HugeTask<V>> tasksWithoutResult(List<Id> ids) {
        return this.call(() -> {
            Object[] idArray = ids.toArray(new Id[ids.size()]);
            Iterator<Vertex> vertices = this.tx().queryTaskInfos(idArray);
            Iterator<HugeTask<V>> tasks =
                new MapperIterator<>(vertices, vertex -> {
                    return HugeTask.fromVertex(vertex, false);
                });
            // Convert iterator to list to avoid across thread tx accessed
            return QueryResults.toList(tasks);
        });
    }

    protected <V> Iterator<HugeTask<V>> tasksWithoutResult(TaskStatus status,
                                                           long limit,
                                                           String page) {
        if (status == null) {
            return this.queryTaskWithoutResult(ImmutableMap.of(), limit, page);
        }
        return this.queryTaskWithoutResult(HugeTask.P.STATUS, status.code(),
                                           limit, page);
    }

    protected <V> Iterator<HugeTask<V>> queryTaskWithoutResult(String key,
                                                               Object value,
                                                               long limit, String page) {
        return this.queryTaskWithoutResult(ImmutableMap.of(key, value), limit, page);
    }

    protected <V> Iterator<HugeTask<V>> queryTaskWithoutResult(Map<String,
            Object> conditions, long limit, String page) {
        return this.call(() -> {
            ConditionQuery query = new ConditionQuery(HugeType.TASK);
            if (page != null) {
                query.page(page);
            }
            VertexLabel vl = this.graph().vertexLabel(HugeTask.P.TASK);
            query.eq(HugeKeys.LABEL, vl.id());
            for (Map.Entry<String, Object> entry : conditions.entrySet()) {
                PropertyKey pk = this.graph().propertyKey(entry.getKey());
                query.query(Condition.eq(pk.id(), entry.getValue()));
            }
            query.showHidden(true);
            if (limit != NO_LIMIT) {
                query.limit(limit);
            }
            Iterator<Vertex> vertices = this.tx().queryTaskInfos(query);
            Iterator<HugeTask<V>> tasks =
                new MapperIterator<>(vertices, vertex -> {
                    return HugeTask.fromVertex(vertex, false);
                });
            // Convert iterator to list to avoid across thread tx accessed
            return QueryResults.toList(tasks);
        });
    }

    protected void deleteTaskResultFromTx(Id taskId) {
        Iterator<Vertex> vertices =
            this.tx().queryTaskInfos(HugeTaskResult.genId(taskId));
        HugeVertex vertex = (HugeVertex) QueryResults.one(vertices);
        if (vertex != null) {
            this.tx().removeTaskVertex(vertex);
        }
    }

    protected HugeTaskResult queryTaskResult(Id taskid) {
        HugeTaskResult result = this.call(() -> {
            Iterator<Vertex> vertices =
                    this.tx().queryTaskInfos(HugeTaskResult.genId(taskid));
            Vertex vertex = QueryResults.one(vertices);
            if (vertex == null) {
                return null;
            }

            return HugeTaskResult.fromVertex(vertex);
        });

        return result;
    }

    protected Iterator<HugeTaskResult> queryTaskResult(List<Id> taskIds) {
        return this.call(() -> {
            Object[] idArray =
                    taskIds.stream().map(HugeTaskResult::genId).toArray();
            Iterator<Vertex> vertices = this.tx()
                                            .queryTaskInfos(idArray);
            Iterator<HugeTaskResult> tasks =
                    new MapperIterator<>(vertices,
                                         HugeTaskResult::fromVertex);
            // Convert iterator to list to avoid across thread tx accessed
            return QueryResults.toList(tasks);
        });
    }

    protected TaskAndResultTransaction tx() {
        // NOTE: only the owner thread can access task tx
        if (this.taskTx == null) {
            /*
             * NOTE: don't synchronized(this) due to scheduler thread hold
             * this lock through scheduleTasks(), then query tasks and wait
             * for db-worker thread after call(), the tx may not be initialized
             * but can't catch this lock, then cause deadlock.
             * We just use this.serverManager as a monitor here
             */
            synchronized (this.serverManager) {
                if (this.taskTx == null) {
                    BackendStore store = this.graph.loadSystemStore();
                    TaskAndResultTransaction tx = new TaskAndResultTransaction(this.graph, store);
                    assert this.taskTx == null; // may be reentrant?
                    this.taskTx = tx;
                }
            }
        }
        assert this.taskTx != null;
        return this.taskTx;
    }

    @Override
    public ServerInfoManager serverManager() {
        return this.serverManager;
    }
}
