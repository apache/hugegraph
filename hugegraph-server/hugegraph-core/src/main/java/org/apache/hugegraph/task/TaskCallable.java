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

import java.util.Date;
import java.util.Set;
import java.util.concurrent.Callable;

import org.apache.hugegraph.HugeException;
import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.HugeGraphParams;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Log;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.slf4j.Logger;

import com.google.common.collect.ImmutableSet;

public abstract class TaskCallable<V> implements Callable<V> {

    private static final Logger LOG = Log.logger(TaskCallable.class);

    private static final String ERROR_COMMIT = "Failed to commit changes: ";
    private static final Set<String> ERROR_MESSAGES = ImmutableSet.of(
            /*
             * "The max length of bytes is" exception message occurs when
             * task input size exceeds TASK_INPUT_SIZE_LIMIT or task result size
             * exceeds TASK_RESULT_SIZE_LIMIT
             */
            "The max length of bytes is"
    );

    private HugeTask<V> task = null;
    private HugeGraph graph = null;

    private volatile long lastSaveTime = System.currentTimeMillis();
    private volatile long saveInterval = 1000 * 30;

    public TaskCallable() {
        // pass
    }

    protected void done() {
        this.closeTx();
    }

    protected void cancelled() {
        // Do nothing, subclasses may override this method
    }

    protected void closeTx() {
        Transaction tx = this.graph().tx();
        if (tx.isOpen()) {
            tx.close();
        }
    }

    public void setMinSaveInterval(long seconds) {
        E.checkArgument(seconds > 0,
                        "Must set interval > 0, but got '%s'", seconds);
        this.saveInterval = seconds * 1000L;
    }

    public void updateProgress(int progress) {
        HugeTask<V> task = this.task();
        task.progress(progress);

        long elapse = System.currentTimeMillis() - this.lastSaveTime;
        if (elapse > this.saveInterval) {
            this.save();
            this.lastSaveTime = System.currentTimeMillis();
        }
    }

    public int progress() {
        HugeTask<V> task = this.task();
        return task.progress();
    }

    protected void save() {
        HugeTask<V> task = this.task();
        task.updateTime(new Date());
        try {
            this.graph().taskScheduler().save(task);
        } catch (Throwable e) {
            if (task.completed()) {
                /*
                 * Failed to save task and the status is stable(can't be update)
                 * just log the task, and try again.
                 */
                LOG.error("Failed to save task with error \"{}\": {}",
                          e, task.asMap(false));
                String message = e.getMessage();
                if (message.contains(ERROR_COMMIT) && needSaveWithEx(message)) {
                    task.failToSave(e);
                    this.graph().taskScheduler().save(task);
                    return;
                }
            }
            throw e;
        }
    }

    protected void graph(HugeGraph graph) {
        this.graph = graph;
    }

    public HugeGraph graph() {
        E.checkState(this.graph != null,
                     "Can't call graph() before scheduling task");
        return this.graph;
    }

    protected void task(HugeTask<V> task) {
        this.task = task;
    }

    public HugeTask<V> task() {
        E.checkState(this.task != null,
                     "Can't call task() before scheduling task");
        return this.task;
    }

    @SuppressWarnings("unchecked")
    public static <V> TaskCallable<V> fromClass(String className) {
        try {
            Class<?> clazz = Class.forName(className);
            return (TaskCallable<V>) clazz.newInstance();
        } catch (Exception e) {
            throw new HugeException("Failed to load task: %s", e, className);
        }
    }

    private static boolean needSaveWithEx(String message) {
        for (String error : ERROR_MESSAGES) {
            if (message.contains(error)) {
                return true;
            }
        }
        return false;
    }

    public static <V> TaskCallable<V> empty(Exception e) {
        return new TaskCallable<V>() {
            @Override
            public V call() throws Exception {
                throw e;
            }
        };
    }

    public abstract static class SysTaskCallable<V> extends TaskCallable<V> {

        private HugeGraphParams params = null;

        protected void params(HugeGraphParams params) {
            this.params = params;
        }

        protected HugeGraphParams params() {
            E.checkState(this.params != null,
                         "Can't call scheduler() before scheduling task");
            return this.params;
        }
    }
}
