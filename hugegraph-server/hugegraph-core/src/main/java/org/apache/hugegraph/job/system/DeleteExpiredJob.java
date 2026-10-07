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

package org.apache.hugegraph.job.system;

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.config.CoreOptions;
import org.apache.hugegraph.job.EphemeralJob;
import org.apache.hugegraph.job.EphemeralJobBuilder;
import org.apache.hugegraph.job.system.JobCounters.JobCounter;
import org.apache.hugegraph.structure.HugeElement;
import org.apache.hugegraph.structure.Index;
import org.apache.hugegraph.task.HugeTask;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

public abstract class DeleteExpiredJob<T> extends EphemeralJob<T> {

    protected static final Logger LOG = Log.logger(DeleteExpiredJob.class);

    private static final int MAX_JOBS = 1000;
    protected static final JobCounters JOB_COUNTERS = new JobCounters();

    public static <V> void asyncDeleteExpiredObject(HugeGraph graph, V object) {
        E.checkArgumentNotNull(object, "The object can't be null");
        JobCounters.JobCounter jobCounter = JOB_COUNTERS.jobCounter(graph);
        if (!jobCounter.addAndTriggerDelete(object)) {
            return;
        }
        if (jobCounter.jobs() >= MAX_JOBS) {
            LOG.debug("Pending delete expired objects jobs size {} has " +
                      "reached the limit {}, abandon {}",
                      jobCounter.jobs(), MAX_JOBS, object);
            return;
        }
        jobCounter.increment();
        EphemeralJob<V> job = newDeleteExpiredElementJob(jobCounter, object);
        jobCounter.clear(object);
        HugeTask<?> task;
        try {
            task = EphemeralJobBuilder.<V>of(graph)
                                      .name("delete_expired_object")
                                      .job(job)
                                      .schedule();
        } catch (Throwable e) {
            jobCounter.decrement();
            if (e.getMessage().contains("Pending tasks size") &&
                e.getMessage().contains("has exceeded the max limit")) {
                // Reach tasks limit, just ignore it
                return;
            }
            throw e;
        }
        /*
         * If TASK_SYNC_DELETION is true, wait async thread done before
         * continue. This is used when running tests.
         */
        if (graph.option(CoreOptions.TASK_SYNC_DELETION)) {
            task.syncWait();
        }
    }

    public static <V> EphemeralJob<V> newDeleteExpiredElementJob(
            JobCounter jobCounter, V object) {
        if (object instanceof HugeElement) {
            return new DeleteExpiredElementJob<>(jobCounter.elements());
        } else {
            assert object instanceof Index;
            return new DeleteExpiredIndexJob<>(jobCounter.indexes());
        }
    }
}
