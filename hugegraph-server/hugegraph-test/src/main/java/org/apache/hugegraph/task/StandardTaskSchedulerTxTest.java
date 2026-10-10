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

import java.util.Collections;

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.id.IdGenerator;
import org.apache.hugegraph.core.BaseCoreTest;
import org.junit.Assert;
import org.junit.Test;

public class StandardTaskSchedulerTxTest extends BaseCoreTest {

    @Test
    public void testTaskQueryDoesNotOpenUpperGraphTransaction() {
        HugeGraph graph = this.graph();
        TaskScheduler scheduler = graph.taskScheduler();
        HugeTask<Object> task = new HugeTask<>(IdGenerator.of(9999999L), null,
                                              new TaskAndResultSchedulerTest.EmptyCallable());
        task.type("test");
        task.name("transaction-lifecycle-query");
        boolean saved = false;
        try {
            // The task worker must also be quiescent after graph startup.
            Assert.assertFalse(scheduler.call(() -> graph.tx().isOpen()));

            scheduler.save(task);
            saved = true;
            scheduler.tasks(TaskStatus.NEW, 1L, null).hasNext();
            Assert.assertFalse(scheduler.call(() -> graph.tx().isOpen()));

            Assert.assertTrue(scheduler.tasks(Collections.singletonList(task.id())).hasNext());
            Assert.assertFalse(scheduler.call(() -> graph.tx().isOpen()));
        } finally {
            try {
                if (saved) {
                    scheduler.delete(task.id(), true);
                }
            } finally {
                scheduler.call(() -> {
                    if (graph.tx().isOpen()) {
                        graph.tx().rollback();
                    }
                    return null;
                });
            }
        }
    }
}
