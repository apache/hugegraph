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

package org.apache.hugegraph.job.algorithm;

import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.config.CoreOptions;
import org.apache.hugegraph.exception.HugeException;
import org.apache.hugegraph.id.IdGenerator;
import org.apache.hugegraph.job.UserJob;
import org.apache.hugegraph.job.algorithm.AbstractAlgorithm.AlgoTraverser;
import org.apache.hugegraph.job.algorithm.comm.LouvainTraverser;
import org.apache.hugegraph.task.HugeTask;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.testutil.Whitebox;
import org.apache.hugegraph.type.define.CollectionType;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;
import org.junit.Test;
import org.mockito.Mockito;

public class AlgorithmConsumerLifecycleTest {

    @Test(timeout = 10000)
    public void testTraversalClosesCursorAfterSuccess() {
        TestCursor cursor = new TestCursor();
        try (TestTraverser traverser = new TestTraverser(newJob(), cursor)) {
            Assert.assertEquals(1L, traverser.traverse(null, null, vertex -> { }));
            Assert.assertEquals(1, cursor.closes);
        }
    }

    @Test(timeout = 10000)
    public void testTraversalPreservesProducerFailureWithDoneAndCloseFailures() {
        RuntimeException producer = new IllegalArgumentException("producer failed");
        RuntimeException done = new IllegalStateException("commit failed");
        RuntimeException close = new IllegalStateException("cursor close failed");
        TestCursor cursor = new TestCursor();
        cursor.readFailure = producer;
        cursor.closeFailure = close;
        try (TestTraverser traverser = new TestTraverser(newJob(), cursor)) {
            Throwable failure = Assert.assertThrows(IllegalArgumentException.class,
                    () -> traverser.traverse(null, null, vertex -> { }, () -> { throw done; }));
            Assert.assertSame(producer, failure);
            Assert.assertEquals(2, failure.getSuppressed().length);
            Assert.assertSame(done, failure.getSuppressed()[0]);
            Assert.assertSame(close, HugeException.rootCause(failure.getSuppressed()[1]));
            Assert.assertEquals(1, cursor.closes);
        }
    }

    @Test(timeout = 10000)
    public void testTraversalPreservesWorkerFailureWhenClosingCursorFails() {
        RuntimeException worker = new IllegalArgumentException("worker failed");
        RuntimeException close = new IllegalStateException("cursor close failed");
        TestCursor cursor = new TestCursor();
        cursor.closeFailure = close;
        try (TestTraverser traverser = new TestTraverser(newJob(), cursor)) {
            Throwable failure = Assert.assertThrows(IllegalArgumentException.class,
                    () -> traverser.traverse(null, null, vertex -> { throw worker; }));
            Assert.assertSame(worker, failure);
            Assert.assertEquals(1, failure.getSuppressed().length);
            Assert.assertSame(close, HugeException.rootCause(failure.getSuppressed()[0]));
            Assert.assertEquals(1, cursor.closes);
        }
    }

    @Test(timeout = 10000)
    public void testLouvainMoveClosesCursorAndPreservesWorkerFailure() {
        RuntimeException worker = new IllegalArgumentException("vertex read failed");
        RuntimeException close = new IllegalStateException("cursor close failed");
        TestCursor cursor = new TestCursor();
        cursor.closeFailure = close;
        Mockito.when(cursor.vertex.label()).thenReturn("person");
        Mockito.when(cursor.vertex.id()).thenThrow(worker);
        try (TestLouvainTraverser traverser = new TestLouvainTraverser(newJob(), cursor)) {
            Throwable failure = Assert.assertThrows(IllegalArgumentException.class,
                    () -> Whitebox.invoke(LouvainTraverser.class, "moveCommunities", traverser, 0));
            Assert.assertSame(worker, failure);
            Assert.assertEquals(1, failure.getSuppressed().length);
            Assert.assertSame(close, HugeException.rootCause(failure.getSuppressed()[0]));
            Assert.assertEquals(1, cursor.closes);
        }
    }

    @Test(timeout = 10000)
    public void testLouvainMergeReportsCommitFailureWithoutRetry() {
        RuntimeException failure = new IllegalStateException("commit failed");
        UserJob<Object> job = newJob();
        Transaction transaction = Mockito.mock(Transaction.class);
        Mockito.when(job.graph().tx()).thenReturn(transaction);
        Mockito.doThrow(failure).when(transaction).commit();
        try (LouvainTraverser traverser = new LouvainTraverser(job, 1, -1L, null, null, true)) {
            Assert.assertSame(failure, Assert.assertThrows(IllegalStateException.class,
                    () -> Whitebox.invoke(LouvainTraverser.class, "mergeCommunities", traverser, 0)));
            Mockito.verify(transaction).commit();
        }
    }

    @Test(timeout = 15000)
    public void testInterruptedProducerFailsTraversalAndClosesCursor() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch lastRead = new CountDownLatch(1);
        CountDownLatch completed = new CountDownLatch(1);
        AtomicInteger processed = new AtomicInteger();
        AtomicBoolean interrupted = new AtomicBoolean();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        // One active item and a full worker queue leave the last item unaccepted.
        TestCursor cursor = new TestCursor(Consumers.QUEUE_WORKER_SIZE + 3);
        cursor.lastRead = lastRead::countDown;
        TestTraverser traverser = new TestTraverser(newJob(), cursor);
        Thread producer = new Thread(() -> {
            try {
                traverser.traverse(null, null, vertex -> {
                    processed.incrementAndGet();
                    entered.countDown();
                    try {
                        release.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }, completed::countDown);
            } catch (Throwable e) {
                failure.set(e);
                interrupted.set(Thread.currentThread().isInterrupted());
            }
        }, "algorithm-producer-interrupt-test");
        try {
            producer.start();
            Assert.assertTrue(entered.await(5L, TimeUnit.SECONDS));
            Assert.assertTrue(lastRead.await(5L, TimeUnit.SECONDS));
            producer.interrupt();
            producer.join(5000L);
            Assert.assertFalse(producer.isAlive());
            Assert.assertTrue(failure.get() instanceof HugeException);
            Assert.assertTrue(HugeException.rootCause(failure.get()) instanceof InterruptedException);
            Assert.assertTrue(interrupted.get());
            Assert.assertEquals(1, cursor.closes);
            Assert.assertTrue(completed.await(5L, TimeUnit.SECONDS));
            Assert.assertEquals(1, processed.get());
        } finally {
            release.countDown();
            producer.interrupt();
            producer.join(5000L);
            traverser.executor.shutdownNow();
            Assert.assertTrue(traverser.executor.awaitTermination(5L, TimeUnit.SECONDS));
        }
    }

    @SuppressWarnings("unchecked")
    private static UserJob<Object> newJob() {
        HugeGraph graph = Mockito.mock(HugeGraph.class);
        Mockito.when(graph.option(CoreOptions.OLTP_COLLECTION_TYPE)).thenReturn(CollectionType.JCF);
        UserJob<Object> job = Mockito.mock(UserJob.class);
        HugeTask<Object> task = Mockito.mock(HugeTask.class);
        Mockito.when(task.id()).thenReturn(IdGenerator.of(1L));
        Mockito.when(job.graph()).thenReturn(graph);
        Mockito.when(job.task()).thenReturn(task);
        return job;
    }

    private static final class TestTraverser extends AlgoTraverser {

        private final Iterator<Vertex> vertices;

        private TestTraverser(UserJob<Object> job, Iterator<Vertex> vertices) {
            super(job, "algorithm-lifecycle-test", 1);
            this.vertices = vertices;
        }

        @Override
        protected Iterator<Vertex> vertices(Object label, Object clabel, long limit) {
            return this.vertices;
        }
    }

    private static final class TestLouvainTraverser extends LouvainTraverser {

        private final Iterator<Vertex> vertices;

        private TestLouvainTraverser(UserJob<Object> job, Iterator<Vertex> vertices) {
            super(job, 1, -1L, null, null, true);
            this.vertices = vertices;
        }

        @Override
        protected Iterator<Vertex> vertices(Object label, long limit) {
            return this.vertices;
        }
    }

    private static final class TestCursor implements CloseableIterator<Vertex> {

        private final Vertex vertex = Mockito.mock(Vertex.class);
        private final Iterator<Vertex> vertices;
        private Runnable lastRead;
        private RuntimeException readFailure;
        private RuntimeException closeFailure;
        private int closes;

        private TestCursor() {
            this(1);
        }

        private TestCursor(int count) {
            this.vertices = Collections.nCopies(count, this.vertex).iterator();
        }

        @Override
        public boolean hasNext() {
            if (this.readFailure != null) {
                throw this.readFailure;
            }
            return this.vertices.hasNext();
        }

        @Override
        public Vertex next() {
            Vertex next = this.vertices.next();
            if (!this.vertices.hasNext() && this.lastRead != null) {
                this.lastRead.run();
            }
            return next;
        }

        @Override
        public void close() {
            this.closes++;
            if (this.closeFailure != null) {
                throw this.closeFailure;
            }
        }
    }
}
