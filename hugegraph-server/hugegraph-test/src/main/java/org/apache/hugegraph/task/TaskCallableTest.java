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

import java.io.IOException;

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.exception.HugeException;
import org.apache.hugegraph.testutil.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class TaskCallableTest {

    @Test
    public void testLoadTaskWithPublicDefaultConstructor() {
        Assert.assertTrue(TaskCallable.fromClass(ValidCallable.class.getName()) instanceof ValidCallable);
    }

    @Test
    public void testLoadRejectsNonTaskBeforeCallingConstructor() {
        NotATask.constructed = false;
        HugeException failure = Assert.assertThrows(HugeException.class,
                () -> TaskCallable.fromClass(NotATask.class.getName()));
        Assert.assertTrue(failure.getCause() instanceof ClassCastException);
        Assert.assertFalse(NotATask.constructed);
    }

    @Test
    public void testLoadPreservesConstructorFailure() {
        HugeException failure = Assert.assertThrows(HugeException.class,
                () -> TaskCallable.fromClass(FailingCallable.class.getName()));
        Assert.assertSame(FailingCallable.FAILURE, failure.getCause());
    }

    @Test
    public void testLoadPreservesConstructorError() {
        Assert.assertSame(FatalCallable.FAILURE, Assert.assertThrows(AssertionError.class,
                () -> TaskCallable.fromClass(FatalCallable.class.getName())));
    }

    @Test
    public void testLoadRejectsPrivateConstructor() {
        HugeException failure = Assert.assertThrows(HugeException.class,
                () -> TaskCallable.fromClass(PrivateCallable.class.getName()));
        Assert.assertTrue(failure.getCause() instanceof IllegalAccessException);
    }

    @Test
    public void testSavePreservesFailureWithoutMessage() {
        RuntimeException failure = new IllegalStateException();
        TaskScheduler scheduler = Mockito.mock(TaskScheduler.class);
        HugeGraph graph = Mockito.mock(HugeGraph.class);
        Mockito.when(graph.taskScheduler()).thenReturn(scheduler);
        HugeTask<Object> task = Mockito.mock(HugeTask.class);
        Mockito.when(task.completed()).thenReturn(true);
        Mockito.doThrow(failure).when(scheduler).save(task);

        TaskCallable<Object> callable = new ValidCallable();
        callable.graph(graph);
        callable.task(task);
        Assert.assertSame(failure, Assert.assertThrows(IllegalStateException.class, callable::save));
        Mockito.verify(scheduler).save(task);
        Mockito.verify(task, Mockito.never()).failToSave(Mockito.any());
    }

    @Test
    public void testSaveStillRetriesOversizedCompletedResult() {
        RuntimeException failure = new IllegalArgumentException(
                "Failed to commit changes: The max length of bytes is 1024");
        TaskScheduler scheduler = Mockito.mock(TaskScheduler.class);
        HugeGraph graph = Mockito.mock(HugeGraph.class);
        Mockito.when(graph.taskScheduler()).thenReturn(scheduler);
        HugeTask<Object> task = Mockito.mock(HugeTask.class);
        Mockito.when(task.completed()).thenReturn(true);
        Mockito.doThrow(failure).doNothing().when(scheduler).save(task);

        TaskCallable<Object> callable = new ValidCallable();
        callable.graph(graph);
        callable.task(task);
        callable.save();
        Mockito.verify(scheduler, Mockito.times(2)).save(task);
        Mockito.verify(task).failToSave(failure);
    }

    public static class ValidCallable extends TaskCallable<Object> {

        @Override
        public Object call() {
            return null;
        }
    }

    public static class NotATask {

        private static boolean constructed;

        public NotATask() {
            constructed = true;
        }
    }

    public static class FailingCallable extends ValidCallable {

        private static final IOException FAILURE = new IOException("constructor failure");

        public FailingCallable() throws IOException {
            throw FAILURE;
        }
    }

    public static class FatalCallable extends ValidCallable {

        private static final AssertionError FAILURE = new AssertionError("constructor error");

        public FatalCallable() {
            throw FAILURE;
        }
    }

    public static class PrivateCallable extends ValidCallable {

        private PrivateCallable() {
        }
    }
}
