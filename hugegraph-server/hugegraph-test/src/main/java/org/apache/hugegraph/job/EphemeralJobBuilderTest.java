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

package org.apache.hugegraph.job;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.task.HugeTask;
import org.apache.hugegraph.task.TaskScheduler;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.testutil.Whitebox;
import org.junit.Test;
import org.mockito.Mockito;

public class EphemeralJobBuilderTest {

    @Test(timeout = 30000)
    public void testConcurrentSchedulingUsesUniqueNegativeIds() throws Exception {
        int threads = 8;
        int perThread = 1000;
        HugeGraph graph = newGraph();
        ExecutorService producers = Executors.newFixedThreadPool(threads);
        CountDownLatch ready = new CountDownLatch(threads);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<List<Long>>> submitted = new ArrayList<>();
        try {
            for (int i = 0; i < threads; i++) {
                submitted.add(producers.submit(() -> {
                    ready.countDown();
                    Assert.assertTrue(start.await(5L, TimeUnit.SECONDS));
                    List<Long> ids = new ArrayList<>(perThread);
                    for (int j = 0; j < perThread; j++) {
                        ids.add(schedule(graph).id().asLong());
                    }
                    return ids;
                }));
            }
            Assert.assertTrue(ready.await(5L, TimeUnit.SECONDS));
            start.countDown();
            Set<Long> unique = new HashSet<>();
            for (Future<List<Long>> future : submitted) {
                for (long id : future.get(10L, TimeUnit.SECONDS)) {
                    Assert.assertTrue("Ephemeral task IDs must remain negative", id < 0L);
                    Assert.assertTrue("Duplicate ephemeral task ID: " + id, unique.add(id));
                }
            }
            Assert.assertEquals(threads * perThread, unique.size());
        } finally {
            start.countDown();
            producers.shutdownNow();
            Assert.assertTrue(producers.awaitTermination(5L, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testTaskIdsWrapWithoutEnteringPersistentIdRange() {
        AtomicInteger counter = Whitebox.getInternalState(EphemeralJobBuilder.class, "EPHEMERAL_TASK_ID");
        int previous = counter.getAndSet(Integer.MIN_VALUE + 1);
        try {
            HugeGraph graph = newGraph();
            Assert.assertEquals((long) Integer.MIN_VALUE + 1L, schedule(graph).id().asLong());
            Assert.assertEquals((long) Integer.MIN_VALUE, schedule(graph).id().asLong());
            Assert.assertEquals(-1L, schedule(graph).id().asLong());
            Assert.assertEquals(-2L, schedule(graph).id().asLong());
        } finally {
            counter.set(previous);
        }
    }

    private static HugeGraph newGraph() {
        HugeGraph graph = Mockito.mock(HugeGraph.class);
        Mockito.when(graph.taskScheduler()).thenReturn(Mockito.mock(TaskScheduler.class));
        return graph;
    }

    private static HugeTask<Object> schedule(HugeGraph graph) {
        return EphemeralJobBuilder.of(graph)
                                  .name("ephemeral-id-test")
                                  .job(new EphemeralJob<>() {
                                      @Override
                                      public String type() {
                                          return "test";
                                      }

                                      @Override
                                      public Object execute() {
                                          return null;
                                      }
                                  })
                                  .schedule();
    }
}
