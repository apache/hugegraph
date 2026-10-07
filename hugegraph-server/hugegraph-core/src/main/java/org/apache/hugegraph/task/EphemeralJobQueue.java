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

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hugegraph.HugeGraphParams;
import org.apache.hugegraph.query.Query;
import org.apache.hugegraph.backend.tx.GraphTransaction;
import org.apache.hugegraph.job.EphemeralJob;
import org.apache.hugegraph.job.EphemeralJobBuilder;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

public class EphemeralJobQueue {

    private static final Logger LOG = Log.logger(EphemeralJobQueue.class);

    private static final long CAPACITY = 100 * Query.COMMIT_BATCH;

    private final BlockingQueue<EphemeralJob<?>> pendingQueue;

    private final AtomicReference<State> state;

    private final HugeGraphParams graph;

    private enum State {
        INIT,
        EXECUTE,
    }

    public EphemeralJobQueue(HugeGraphParams graph) {
        this.state = new AtomicReference<>(State.INIT);
        this.graph = graph;
        this.pendingQueue = new ArrayBlockingQueue<>((int) CAPACITY);
    }

    public boolean add(EphemeralJob<?> job) {
        if (job == null) {
            return false;
        }

        if (!this.pendingQueue.offer(job)) {
            LOG.warn("The pending queue of EphemeralJobQueue is full, {} job " +
                     "will be ignored", job.type());
            return false;
        }

        this.reScheduleIfNeeded();
        return true;
    }

    protected HugeGraphParams params() {
        return this.graph;
    }

    protected void clear() {
        this.pendingQueue.clear();
    }

    protected EphemeralJob<?> poll() {
        return this.pendingQueue.poll();
    }

    public void consumeComplete() {
        this.state.compareAndSet(State.EXECUTE, State.INIT);
    }

    public void reScheduleIfNeeded() {
        if (this.state.compareAndSet(State.INIT, State.EXECUTE)) {
            try {
                BatchEphemeralJob job = new BatchEphemeralJob(this);
                EphemeralJobBuilder.of(this.graph.graph())
                                   .name("batch-ephemeral-job")
                                   .job(job)
                                   .schedule();
            } catch (Throwable e) {
                // Maybe if it fails, consider clearing all the data in the pendingQueue,
                // or start a scheduled retry task to retry until success.
                LOG.warn("Failed to schedule BatchEphemeralJob", e);
                this.pendingQueue.clear();
                this.state.compareAndSet(State.EXECUTE, State.INIT);
            }
        }
    }

    public boolean isEmpty() {
        return this.pendingQueue.isEmpty();
    }

    public static class BatchEphemeralJob extends EphemeralJob<Object> {

        private static final long PAGE_SIZE = Query.COMMIT_BATCH;
        private static final String BATCH_EPHEMERAL_JOB = "batch-ephemeral-job";
        private static final long MAX_CONSUME_COUNT = 2 * PAGE_SIZE;

        private WeakReference<EphemeralJobQueue> queueWeakReference;

        public BatchEphemeralJob(EphemeralJobQueue queue) {
            this.queueWeakReference = new WeakReference<>(queue);
        }

        @Override
        public String type() {
            return BATCH_EPHEMERAL_JOB;
        }

        @Override
        public Object execute() throws Exception {
            boolean stop = false;
            Object result = null;
            int consumeCount = 0;
            InterruptedException interruptedException = null;
            EphemeralJobQueue queue;
            List<EphemeralJob<?>> batchJobs = new ArrayList<>();
            while (!stop) {
                if (interruptedException == null && Thread.currentThread().isInterrupted()) {
                    interruptedException = new InterruptedException();
                }

                queue = this.queueWeakReference.get();
                if (queue == null) {
                    stop = true;
                    continue;
                }

                if (queue.isEmpty() || consumeCount > MAX_CONSUME_COUNT ||
                    interruptedException != null) {
                    queue.consumeComplete();
                    stop = true;
                    if (!queue.isEmpty()) {
                        queue.reScheduleIfNeeded();
                    }
                    continue;
                }

                try {
                    while (!queue.isEmpty() && batchJobs.size() < PAGE_SIZE) {
                        EphemeralJob<?> job = queue.poll();
                        if (job == null) {
                            continue;
                        }
                        batchJobs.add(job);
                    }

                    if (batchJobs.isEmpty()) {
                        continue;
                    }

                    consumeCount += batchJobs.size();
                    result = this.executeBatchJob(batchJobs, result);

                } catch (InterruptedException e) {
                    interruptedException = e;
                } finally {
                    batchJobs.clear();
                }
            }

            if (interruptedException != null) {
                Thread.currentThread().interrupt();
                throw interruptedException;
            }

            return result;
        }

        private Object executeBatchJob(List<EphemeralJob<?>> jobs, Object prevResult) throws
                                                                                      Exception {
            GraphTransaction graphTx = this.params().systemTransaction();
            GraphTransaction systemTx = this.params().graphTransaction();
            Object result = prevResult;
            for (EphemeralJob<?> job : jobs) {
                this.initJob(job);
                Object obj = job.call();
                if (job instanceof Reduce) {
                    result = ((Reduce) job).reduce(result, obj);
                }
            }

            graphTx.commit();
            systemTx.commit();

            return result;
        }

        private void initJob(EphemeralJob<?> job) {
            job.graph(this.graph());
            job.params(this.params());
        }

        @Override
        public Object call() throws Exception {
            try {
                return super.call();
            } catch (Throwable e) {
                LOG.warn("Failed to execute BatchEphemeralJob", e);
                EphemeralJobQueue queue = this.queueWeakReference.get();
                if (e instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                    if (queue != null) {
                        queue.clear();
                        queue.consumeComplete();
                    }
                    throw e;
                }

                if (queue != null) {
                    queue.consumeComplete();
                    if (!queue.isEmpty()) {
                        queue.reScheduleIfNeeded();
                    }
                }
                throw e;
            }
        }
    }

    public interface Reduce<T> {

        T reduce(T t1, T t2);
    }
}
