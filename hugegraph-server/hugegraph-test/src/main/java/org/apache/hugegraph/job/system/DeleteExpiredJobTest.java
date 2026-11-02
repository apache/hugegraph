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
import org.apache.hugegraph.structure.HugeElement;
import org.apache.hugegraph.task.TaskScheduler;
import org.apache.hugegraph.testutil.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class DeleteExpiredJobTest {

    @Test
    public void testPreservesSchedulingFailureWithoutMessage() {
        this.checkSchedulingFailure("null-message", new IllegalStateException());
    }

    @Test
    public void testPreservesSchedulingErrorWithoutMessage() {
        this.checkSchedulingFailure("null-error-message", new AssertionError());
    }

    @Test
    public void testPreservesUnrelatedSchedulingFailure() {
        this.checkSchedulingFailure("unrelated-message", new IllegalArgumentException("Pending tasks size unknown"));
    }

    @Test
    public void testPendingTaskLimitIsStillIgnored() {
        HugeGraph graph = newGraph("pending-limit", new IllegalArgumentException(
                "Pending tasks size 3 has exceeded the max limit 2"));
        DeleteExpiredJob.asyncDeleteExpiredObject(graph, Mockito.mock(HugeElement.class));
        Assert.assertEquals(0, DeleteExpiredJob.JOB_COUNTERS.jobCounter(graph).jobs());
        Mockito.verify(graph.taskScheduler()).schedule(Mockito.any());
    }

    private void checkSchedulingFailure(String name, Throwable expected) {
        HugeGraph graph = newGraph(name, expected);
        Assert.assertSame(expected, Assert.assertThrows(Throwable.class,
                () -> DeleteExpiredJob.asyncDeleteExpiredObject(graph, Mockito.mock(HugeElement.class))));
        Assert.assertEquals(0, DeleteExpiredJob.JOB_COUNTERS.jobCounter(graph).jobs());
        Mockito.verify(graph.taskScheduler()).schedule(Mockito.any());
    }

    private static HugeGraph newGraph(String name, Throwable failure) {
        HugeGraph graph = Mockito.mock(HugeGraph.class);
        TaskScheduler scheduler = Mockito.mock(TaskScheduler.class);
        Mockito.when(graph.spaceGraphName()).thenReturn("delete-expired-test-" + name);
        Mockito.when(graph.option(CoreOptions.TASK_TTL_DELETE_BATCH)).thenReturn(1);
        Mockito.when(graph.taskScheduler()).thenReturn(scheduler);
        Mockito.doThrow(failure).when(scheduler).schedule(Mockito.any());
        return graph;
    }
}
