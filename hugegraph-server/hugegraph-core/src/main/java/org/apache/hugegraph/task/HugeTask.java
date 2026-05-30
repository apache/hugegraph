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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.FutureTask;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import org.apache.hugegraph.HugeException;
import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.id.IdGenerator;
import org.apache.hugegraph.backend.serializer.BytesBuffer;
import org.apache.hugegraph.config.CoreOptions;
import org.apache.hugegraph.exception.LimitExceedException;
import org.apache.hugegraph.exception.NotFoundException;
import org.apache.hugegraph.job.ComputerJob;
import org.apache.hugegraph.job.EphemeralJob;
import org.apache.hugegraph.job.GremlinJob;
import org.apache.hugegraph.job.schema.SchemaJob;
import org.apache.hugegraph.type.define.SerialEnum;
import org.apache.hugegraph.util.Blob;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.InsertionOrderUtil;
import org.apache.hugegraph.util.JsonUtil;
import org.apache.hugegraph.util.Log;
import org.apache.hugegraph.util.StringEncoding;
import org.apache.tinkerpop.gremlin.structure.Graph.Hidden;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.slf4j.Logger;

public class HugeTask<V> extends FutureTask<V> {

    private static final Logger LOG = Log.logger(HugeTask.class);

    private static final float DECOMPRESS_RATIO = 10.0F;

    private transient TaskScheduler scheduler = null;

    private final TaskCallable<V> callable;

    private String type;
    private String name;
    private final Id id;
    private final Id parent;
    private Set<Id> dependencies;
    private String description;
    private String context;
    private Date create;
    private Id server;
    private int load;

    private volatile TaskStatus status;
    private volatile int progress;
    private volatile Date update;
    private volatile int retries;
    private volatile String input;
    private volatile String result;

    public HugeTask(Id id, Id parent, String callable, String input) {
        this(id, parent, TaskCallable.fromClass(callable));
        this.input(input);
    }

    public HugeTask(Id id, Id parent, TaskCallable<V> callable) {
        super(callable);

        E.checkArgumentNotNull(id, "Task id can't be null");
        E.checkArgument(id.number(), "Invalid task id type, it must be number");

        assert callable != null;
        this.callable = callable;
        this.type = null;
        this.name = null;
        this.id = id;
        this.parent = parent;
        this.dependencies = null;
        this.description = null;
        this.context = null;
        this.status = TaskStatus.NEW;
        this.progress = 0;
        this.create = new Date();
        this.update = null;
        this.retries = 0;
        this.input = null;
        this.result = null;
        this.server = null;
        this.load = 1;
    }

    public Id id() {
        return this.id;
    }

    public Id parent() {
        return this.parent;
    }

    public Set<Id> dependencies() {
        return Collections.unmodifiableSet(this.dependencies);
    }

    public void depends(Id id) {
        E.checkState(this.status == TaskStatus.NEW,
                     "Can't add dependency in status '%s'", this.status);
        if (this.dependencies == null) {
            this.dependencies = InsertionOrderUtil.newSet();
        }
        this.dependencies.add(id);
    }

    public TaskStatus status() {
        return this.status;
    }

    public void type(String type) {
        this.type = type;
    }

    public String type() {
        return this.type;
    }

    public void name(String name) {
        this.name = name;
    }

    public String name() {
        return this.name;
    }

    public void description(String description) {
        this.description = description;
    }

    public String description() {
        return this.description;
    }

    public final void context(String context) {
        E.checkArgument(this.context == null,
                        "Task context must be set once, but already set '%s'",
                        this.context);
        E.checkArgument(this.status == TaskStatus.NEW,
                        "Task context must be set in state NEW instead of %s",
                        this.status);
        this.context = context;
    }

    public final String context() {
        return this.context;
    }

    public void progress(int progress) {
        this.progress = progress;
    }

    public int progress() {
        return this.progress;
    }

    public void createTime(Date create) {
        this.create = create;
    }

    public Date createTime() {
        return this.create;
    }

    public void updateTime(Date update) {
        this.update = update;
    }

    public Date updateTime() {
        return this.update;
    }

    public void retry() {
        ++this.retries;
    }

    public int retries() {
        return this.retries;
    }

    public void input(String input) {
        // checkPropertySize(input, P.INPUT);
        this.input = input;
    }

    public String input() {
        return this.input;
    }

    public String result() {
        return this.result;
    }

    public synchronized void result(HugeTaskResult result) {
        this.result = result.result();
    }

    private synchronized boolean result(TaskStatus status, String result) {
        checkPropertySize(result, P.RESULT);
        if (this.status(status)) {
            this.result = result;
            return true;
        }
        return false;
    }

    public void server(Id server) {
        this.server = server;
    }

    public Id server() {
        return this.server;
    }

    public void load(int load) {
        this.load = load;
    }

    public int load() {
        return this.load;
    }

    public boolean completed() {
        return TaskStatus.COMPLETED_STATUSES.contains(this.status);
    }

    public boolean success() {
        return this.status == TaskStatus.SUCCESS;
    }

    public boolean cancelled() {
        return this.status == TaskStatus.CANCELLED || this.isCancelled();
    }

    public boolean cancelling() {
        return this.status == TaskStatus.CANCELLING;
    }

    public boolean computer() {
        return ComputerJob.COMPUTER.equals(this.type);
    }

    public boolean schemaTask() {
        return this.callable instanceof SchemaJob;
    }

    public boolean gremlinTask() {
        return this.callable instanceof GremlinJob;
    }

    public boolean ephemeralTask() {
        return this.callable instanceof EphemeralJob;
    }

    @Override
    public String toString() {
        return String.format("HugeTask(%s)%s", this.id, this.asMap());
    }

    @Override
    public void run() {
        if (this.cancelled()) {
            // A task is running after cancelled which scheduled/queued before
            return;
        }

        TaskManager.setContext(this.context());
        try {
            assert this.status.code() < TaskStatus.RUNNING.code() : this.status;
            if (this.checkDependenciesSuccess()) {
                /*
                 * FIXME: worker node may reset status to RUNNING here, and the
                 *        status in DB is CANCELLING that set by master node,
                 *        it will lead to cancel() operation not to take effect.
                 */
                this.status(TaskStatus.RUNNING);
                super.run();
            }
        } catch (Throwable e) {
            this.setException(e);
        } finally {
            LOG.debug("Task is finished {}", this);
            TaskManager.resetContext();
        }
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        // NOTE: Gremlin sleep() can't be interrupted by default
        // https://mrhaki.blogspot.com/2016/10/groovy-goodness-interrupted-sleeping.html
        boolean cancelled = super.cancel(mayInterruptIfRunning);
        if (!cancelled) {
            return cancelled;
        }

        try {
            if (this.status(TaskStatus.CANCELLED)) {
                // Callback for saving status to store
                this.callable.cancelled();
            } else {
                // Maybe worker node is still running then set status SUCCESS
                cancelled = false;
            }
        } catch (Throwable e) {
            LOG.error("An exception occurred when calling cancelled()", e);
        }
        return cancelled;
    }

    public boolean fail(Throwable e) {
        E.checkNotNull(e, "exception");
        if (!(this.cancelled() && HugeException.isInterrupted(e))) {
            e.printStackTrace();
            LOG.warn("An exception occurred when running task: {}",
                     this.id(), e);
            // Update status to FAILED if exception occurred(not interrupted)
            return this.result(TaskStatus.FAILED, e.toString());
        }
        return false;
    }

    public void failToSave(Throwable e) {
        if (!this.fail(e)) {
            // Can't update status, just set result to error message
            this.result = e.toString();
        }
    }

    @Override
    protected void done() {
        try {
            // Callback for saving status to store
            this.callable.done();
        } catch (Throwable e) {
            LOG.error("An exception occurred when calling done()", e);
        } finally {
            this.scheduler().taskDone(this);
        }
    }

    @Override
    protected void set(V v) {
        String result = JsonUtil.toJson(v);
        checkPropertySize(result, P.RESULT);
        if (!this.result(TaskStatus.SUCCESS, result)) {
            assert this.completed();
        }
        // Will call done() and may cause to save to store
        super.set(v);
    }

    @Override
    protected void setException(Throwable e) {
        this.fail(e);
        super.setException(e);
    }

    protected void scheduler(TaskScheduler scheduler) {
        this.scheduler = scheduler;
    }

    protected TaskScheduler scheduler() {
        E.checkState(this.scheduler != null,
                     "Can't call scheduler() before scheduling task");
        return this.scheduler;
    }

    protected boolean checkDependenciesSuccess() {
        if (this.dependencies == null || this.dependencies.isEmpty()) {
            return true;
        }
        TaskScheduler scheduler = this.scheduler();
        for (Id dependency : this.dependencies) {
            HugeTask<?> task = scheduler.task(dependency);
            if (!task.completed()) {
                // Dependent task not completed, re-schedule self
                scheduler.schedule(this);
                return false;
            } else if (task.status() == TaskStatus.CANCELLED) {
                this.result(TaskStatus.CANCELLED, String.format(
                        "Cancelled due to dependent task '%s' cancelled",
                        dependency));
                this.done();
                return false;
            } else if (task.status() == TaskStatus.FAILED) {
                this.result(TaskStatus.FAILED, String.format(
                        "Failed due to dependent task '%s' failed",
                        dependency));
                this.done();
                return false;
            }
        }
        return true;
    }

    protected TaskCallable<V> callable() {
        E.checkNotNull(this.callable, "callable");
        return this.callable;
    }

    protected synchronized boolean status(TaskStatus status) {
        E.checkNotNull(status, "status");
        if (status.code() > TaskStatus.NEW.code()) {
            E.checkState(this.type != null, "Task type can't be null");
            E.checkState(this.name != null, "Task name can't be null");
        }
        if (!this.completed()) {
            assert this.status.code() < status.code() ||
                   status == TaskStatus.RESTORING :
                    this.status + " => " + status + " (task " + this.id + ")";
            this.status = status;
            return true;
        }
        return false;
    }

    public synchronized void overwriteStatus(TaskStatus status) {
        this.status = status;
    }

    protected void property(String key, Object value) {
        E.checkNotNull(key, "property key");
        switch (key) {
            case P.TYPE:
                this.type = (String) value;
                break;
            case P.NAME:
                this.name = (String) value;
                break;
            case P.CALLABLE:
                // pass
                break;
            case P.STATUS:
                this.status = SerialEnum.fromCode(TaskStatus.class,
                                                  (byte) value);
                break;
            case P.PROGRESS:
                this.progress = (int) value;
                break;
            case P.CREATE:
                this.create = (Date) value;
                break;
            case P.RETRIES:
                this.retries = (int) value;
                break;
            case P.DESCRIPTION:
                this.description = (String) value;
                break;
            case P.CONTEXT:
                this.context = (String) value;
                break;
            case P.UPDATE:
                this.update = (Date) value;
                break;
            case P.DEPENDENCIES:
                @SuppressWarnings("unchecked")
                Set<Long> values = (Set<Long>) value;
                this.dependencies = values.stream().map(IdGenerator::of)
                                          .collect(toOrderSet());
                break;
            case P.INPUT:
                this.input = StringEncoding.decompress(((Blob) value).bytes(),
                                                       DECOMPRESS_RATIO);
                break;
            case P.RESULT:
                this.result = StringEncoding.decompress(((Blob) value).bytes(),
                                                        DECOMPRESS_RATIO);
                break;
            case P.SERVER:
                this.server = IdGenerator.of((String) value);
                break;
            default:
                throw new AssertionError("Unsupported key: " + key);
        }
    }

    protected synchronized Object[] asArray() {
        E.checkState(this.type != null, "Task type can't be null");
        E.checkState(this.name != null, "Task name can't be null");

        List<Object> list = new ArrayList<>(28);

        list.add(T.label);
        list.add(P.TASK);

        list.add(T.id);
        list.add(this.id);

        list.add(P.TYPE);
        list.add(this.type);

        list.add(P.NAME);
        list.add(this.name);

        list.add(P.CALLABLE);
        list.add(this.callable.getClass().getName());

        list.add(P.STATUS);
        list.add(this.status.code());

        list.add(P.PROGRESS);
        list.add(this.progress);

        list.add(P.CREATE);
        list.add(this.create);

        list.add(P.RETRIES);
        list.add(this.retries);

        if (this.description != null) {
            list.add(P.DESCRIPTION);
            list.add(this.description);
        }

        if (this.context != null) {
            list.add(P.CONTEXT);
            list.add(this.context);
        }

        if (this.update != null) {
            list.add(P.UPDATE);
            list.add(this.update);
        }

        if (this.dependencies != null) {
            list.add(P.DEPENDENCIES);
            list.add(this.dependencies.stream().map(Id::asLong)
                                      .collect(toOrderSet()));
        }

        if (this.input != null) {
            byte[] bytes = StringEncoding.compress(this.input);
            checkPropertySize(bytes.length, P.INPUT);
            list.add(P.INPUT);
            list.add(bytes);
        }

        if (this.result != null) {
            byte[] bytes = StringEncoding.compress(this.result);
            checkPropertySize(bytes.length, P.RESULT);
            list.add(P.RESULT);
            list.add(bytes);
        }

        if (this.server != null) {
            list.add(P.SERVER);
            list.add(this.server.asString());
        }

        return list.toArray();
    }

    protected synchronized Object[] asArrayWithoutResult() {
        E.checkState(this.type != null, "Task type can't be null");
        E.checkState(this.name != null, "Task name can't be null");

        List<Object> list = new ArrayList<>(28);

        list.add(T.label);
        list.add(P.TASK);

        list.add(T.id);
        list.add(this.id);

        list.add(P.TYPE);
        list.add(this.type);

        list.add(P.NAME);
        list.add(this.name);

        list.add(P.CALLABLE);
        list.add(this.callable.getClass().getName());

        list.add(P.STATUS);
        list.add(this.status.code());

        list.add(P.PROGRESS);
        list.add(this.progress);

        list.add(P.CREATE);
        list.add(this.create);

        list.add(P.RETRIES);
        list.add(this.retries);

        if (this.description != null) {
            list.add(P.DESCRIPTION);
            list.add(this.description);
        }

        if (this.context != null) {
            list.add(P.CONTEXT);
            list.add(this.context);
        }

        if (this.update != null) {
            list.add(P.UPDATE);
            list.add(this.update);
        }

        if (this.dependencies != null) {
            list.add(P.DEPENDENCIES);
            list.add(this.dependencies.stream().map(Id::asLong)
                                      .collect(toOrderSet()));
        }

        if (this.input != null) {
            byte[] bytes = StringEncoding.compress(this.input);
            checkPropertySize(bytes.length, P.INPUT);
            list.add(P.INPUT);
            list.add(bytes);
        }

        if (this.server != null) {
            list.add(P.SERVER);
            list.add(this.server.asString());
        }

        return list.toArray();
    }

    public Map<String, Object> asMap() {
        return this.asMap(true);
    }

    public synchronized Map<String, Object> asMap(boolean withDetails) {
        E.checkState(this.type != null, "Task type can't be null");
        E.checkState(this.name != null, "Task name can't be null");

        Map<String, Object> map = new HashMap<>();

        map.put(Hidden.unHide(P.ID), this.id);

        map.put(Hidden.unHide(P.TYPE), this.type);
        map.put(Hidden.unHide(P.NAME), this.name);
        map.put(Hidden.unHide(P.STATUS), this.status.string());
        map.put(Hidden.unHide(P.PROGRESS), this.progress);
        map.put(Hidden.unHide(P.CREATE), this.create);
        map.put(Hidden.unHide(P.RETRIES), this.retries);

        if (this.description != null) {
            map.put(Hidden.unHide(P.DESCRIPTION), this.description);
        }
        if (this.update != null) {
            map.put(Hidden.unHide(P.UPDATE), this.update);
        }
        if (this.dependencies != null) {
            Set<Long> value = this.dependencies.stream().map(Id::asLong)
                                               .collect(toOrderSet());
            map.put(Hidden.unHide(P.DEPENDENCIES), value);
        }

        if (this.server != null) {
            map.put(Hidden.unHide(P.SERVER), this.server.asString());
        }

        if (withDetails) {
            map.put(Hidden.unHide(P.CALLABLE),
                    this.callable.getClass().getName());
            if (this.input != null) {
                map.put(Hidden.unHide(P.INPUT), this.input);
            }
            if (this.result != null) {
                map.put(Hidden.unHide(P.RESULT), this.result);
            }
        }

        return map;
    }

    public static <V> HugeTask<V> fromVertex(Vertex vertex) {
        String callableName = vertex.value(P.CALLABLE);
        TaskCallable<V> callable;
        try {
            callable = TaskCallable.fromClass(callableName);
        } catch (Exception e) {
            callable = TaskCallable.empty(e);
        }

        HugeTask<V> task = new HugeTask<>((Id) vertex.id(), null, callable);
        for (Iterator<VertexProperty<Object>> iter = vertex.properties();
             iter.hasNext(); ) {
            VertexProperty<Object> prop = iter.next();
            task.property(prop.key(), prop.value());
        }
        return task;
    }

    private static <V> Collector<V, ?, Set<V>> toOrderSet() {
        return Collectors.toCollection(InsertionOrderUtil::newSet);
    }

    private void checkPropertySize(String property, String propertyName) {
        byte[] bytes = StringEncoding.compress(property);
        checkPropertySize(bytes.length, propertyName);
    }

    private void checkPropertySize(int propertyLength, String propertyName) {
        long propertyLimit = BytesBuffer.MAX_PROPERTIES;
        HugeGraph graph = this.scheduler().graph();
        if (propertyName.equals(P.INPUT)) {
            propertyLimit = graph.option(CoreOptions.TASK_INPUT_SIZE_LIMIT);
        } else if (propertyName.equals(P.RESULT)) {
            propertyLimit = graph.option(CoreOptions.TASK_RESULT_SIZE_LIMIT);
        }

        if (propertyLength > propertyLimit) {
            throw new LimitExceedException(
                    "Task %s size %s exceeded limit %s bytes",
                    P.unhide(propertyName), propertyLength, propertyLimit);
        }
    }

    public void syncWait() {
        // This method is just called by tests
        HugeTask<?> task = null;
        try {
            task = this.scheduler().waitUntilTaskCompleted(this.id());
        } catch (Throwable e) {
            if (this.callable() instanceof EphemeralJob &&
                e.getClass() == NotFoundException.class &&
                e.getMessage().contains("Can't find task with id")) {
                /*
                 * The task with EphemeralJob won't saved in backends and
                 * will be removed from memory when completed
                 */
                return;
            }
            throw new HugeException("Failed to wait for task '%s' completed",
                                    e, this.id);
        }
        assert task != null;
        /*
         * This can be enabled for debug to expose schema-clear errors early,
         * but also lead to some negative tests failed,
         */
        boolean debugTest = false;
        if (debugTest && !task.success()) {
            throw new HugeException("Task '%s' is failed with error: %s",
                                    task.id(), task.result());
        }
    }

    public static final class P {

        public static final String TASK = Hidden.hide("task");

        public static final String ID = T.id.getAccessor();
        public static final String LABEL = T.label.getAccessor();

        public static final String TYPE = "~task_type";
        public static final String NAME = "~task_name";
        public static final String CALLABLE = "~task_callable";
        public static final String DESCRIPTION = "~task_description";
        public static final String CONTEXT = "~task_context";
        public static final String STATUS = "~task_status";
        public static final String PROGRESS = "~task_progress";
        public static final String CREATE = "~task_create";
        public static final String UPDATE = "~task_update";
        public static final String RETRIES = "~task_retries";
        public static final String INPUT = "~task_input";
        public static final String RESULT = "~task_result";
        public static final String DEPENDENCIES = "~task_dependencies";
        public static final String SERVER = "~task_server";

        //public static final String PARENT = hide("parent");
        //public static final String CHILDREN = hide("children");

        public static String unhide(String key) {
            final String prefix = Hidden.hide("task_");
            if (key.startsWith(prefix)) {
                return key.substring(prefix.length());
            }
            return key;
        }
    }
}
