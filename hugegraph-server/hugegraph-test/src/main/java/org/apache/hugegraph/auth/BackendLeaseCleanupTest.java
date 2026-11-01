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

package org.apache.hugegraph.auth;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.apache.hugegraph.HugeFactory;
import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.auth.HugeGraphAuthProxy.Context;
import org.apache.hugegraph.auth.HugeGraphAuthProxy.ContextTask;
import org.apache.hugegraph.backend.store.rocksdb.RocksDBSessions;
import org.apache.hugegraph.backend.store.rocksdb.RocksDBStore;
import org.apache.hugegraph.core.BaseCoreTest;
import org.apache.hugegraph.id.IdGenerator;
import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.type.define.Directions;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.testutil.Whitebox;
import org.apache.hugegraph.traversal.algorithm.OltpTraverser;
import org.apache.hugegraph.util.Consumers;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

public class BackendLeaseCleanupTest extends BaseCoreTest {

    @Before
    public void initSchema() {
        graph().schema().vertexLabel("lifecycle").useCustomizeNumberId().create();
        HugeFactory.closeCurrentThreadTransactions();
    }

    @After
    public void cleanup() {
        try {
            HugeFactory.closeCurrentThreadTransactions();
        } finally {
            HugeGraphAuthProxy.resetContext();
        }
    }

    @Test
    public void testFailedTaskRollsBackDespiteCommitOnClose() {
        AtomicInteger rollbackEvents = new AtomicInteger();
        HugeGraphAuthProxy.setContext(Context.admin());
        IllegalStateException failure = new IllegalStateException("task failed after writing");
        ContextTask task = new ContextTask(() -> {
            Transaction tx = graph().tx();
            tx.onClose(Transaction.CLOSE_BEHAVIOR.COMMIT);
            tx.onReadWrite(Transaction.READ_WRITE_BEHAVIOR.MANUAL);
            tx.open();
            tx.addTransactionListener(status -> rollbackEvents.incrementAndGet());
            graph().addVertex(T.label, "lifecycle", T.id, 1L);
            throw failure;
        });
        HugeGraphAuthProxy.resetContext();

        Assert.assertSame(failure, Assert.assertThrows(IllegalStateException.class, task::run));
        Assert.assertFalse(graph().tx().isOpen());
        ThreadLocal<?> backend = Whitebox.getInternalState(graph().tx(), "transactions");
        Assert.assertNull(backend.get());
        Assert.assertNull(HugeGraphAuthProxy.getContext());
        Assert.assertEquals(1, rollbackEvents.get());
        // Reuse the worker thread: AUTO and default rollback must be restored.
        Assert.assertEquals(0L, graph().traversal().V().count().next().longValue());
        graph().addVertex(T.label, "lifecycle", T.id, 2L);
        graph().tx().close();
        Assert.assertEquals(0L, graph().traversal().V().count().next().longValue());
        Assert.assertEquals(1, rollbackEvents.get());
    }

    @Test
    public void testSuccessfulTaskKeepsExplicitCommitAndResetsThreadState() {
        AtomicInteger commitEvents = new AtomicInteger();
        HugeGraphAuthProxy.setContext(Context.admin());
        ContextTask task = new ContextTask(() -> {
            Transaction tx = graph().tx();
            tx.onClose(Transaction.CLOSE_BEHAVIOR.COMMIT);
            tx.onReadWrite(Transaction.READ_WRITE_BEHAVIOR.MANUAL);
            tx.open();
            tx.addTransactionListener(status -> commitEvents.incrementAndGet());
            graph().addVertex(T.label, "lifecycle", T.id, 1L);
            tx.commit();
        });
        HugeGraphAuthProxy.resetContext();

        task.run();

        Assert.assertFalse(graph().tx().isOpen());
        ThreadLocal<?> backend = Whitebox.getInternalState(graph().tx(), "transactions");
        Assert.assertNull(backend.get());
        Assert.assertNull(HugeGraphAuthProxy.getContext());
        Assert.assertEquals(1L, graph().traversal().V().count().next().longValue());
        graph().addVertex(T.label, "lifecycle", T.id, 2L);
        graph().tx().close();
        Assert.assertEquals(1L, graph().traversal().V().count().next().longValue());
        Assert.assertEquals(1, commitEvents.get());
    }
    @Test
    public void testCommittedRocksDBWorkerReturnsBackendLeases() throws Exception {
        Assume.assumeTrue("rocksdb".equals(graph().backend()));
        ExecutorService worker = Executors.newSingleThreadExecutor();
        try {
            worker.submit(() -> {
                try {
                    graph().addVertex(T.label, "lifecycle", T.id, 1L);
                    graph().tx().commit();
                    RocksDBStore store = Whitebox.getInternalState(params().graphTransaction(), "store");
                    Map<String, RocksDBSessions> databases = Whitebox.getInternalState(store, "dbs");
                    long threadId = Thread.currentThread().getId();
                    for (RocksDBSessions sessions : databases.values()) {
                        Map<Long, ?> leases = Whitebox.getInternalState(sessions, "sessions");
                        Assert.assertTrue(leases.containsKey(threadId));
                    }
                    HugeFactory.closeCurrentThreadTransactions();
                    for (RocksDBSessions sessions : databases.values()) {
                        Map<Long, ?> leases = Whitebox.getInternalState(sessions, "sessions");
                        Assert.assertFalse(leases.containsKey(threadId));
                    }
                    // Reuse this exact worker and confirm the committed row survives.
                    Assert.assertEquals(1L, graph().traversal().V().count().next().longValue());
                } finally {
                    HugeFactory.closeCurrentThreadTransactions();
                }
            }).get(30, TimeUnit.SECONDS);
        } finally {
            worker.shutdownNow();
        }
    }

    @Test
    public void testOltpSingleItemReturnsWorkerLeases() {
        this.assertOltpWorkerCleanup(false, null);
    }

    @Test
    public void testOltpBatchReturnsWorkerLeases() {
        this.assertOltpWorkerCleanup(true, null);
    }

    @Test
    public void testOltpFailureReturnsWorkerLeasesAndPreservesFailure() {
        this.assertOltpWorkerCleanup(false, new IllegalStateException("worker failed"));
    }

    @Test
    public void testOltpStopExecutionReturnsWorkerLeases() {
        this.assertOltpWorkerCleanup(true, new Consumers.StopExecution("stop traversal"));
    }

    @Test
    public void testOltpInlineTraversalKeepsCallerTransaction() {
        Assume.assumeTrue("rocksdb".equals(graph().backend()));
        graph().addVertex(T.label, "lifecycle", T.id, 99L);
        Transaction caller = graph().tx();
        try (LeaseTraverser traverser = new LeaseTraverser(graph())) {
            traverser.inline(() -> Assert.assertTrue(graph().vertices(99L).hasNext()));
        }
        Assert.assertTrue(caller.isOpen());
        ThreadLocal<?> owners = Whitebox.getInternalState(caller, "transactions");
        Assert.assertNotNull(owners.get());
        caller.rollback();
        Assert.assertFalse(graph().vertices(99L).hasNext());
    }

    @Test
    public void testLazyBfsQueryReturnsWorkerLeases() {
        this.assertLazyBfsWorkerCleanup(OltpTraverser.NO_LIMIT, null);
    }

    @Test
    public void testLazyBfsFailureReturnsWorkerLeases() {
        this.assertLazyBfsWorkerCleanup(OltpTraverser.NO_LIMIT,
                                       new IllegalStateException("BFS consumer failed"));
    }

    @Test
    public void testLazyBfsCapacityStopReturnsWorkerLeases() {
        this.assertLazyBfsWorkerCleanup(1L, null);
    }

    private void assertLazyBfsWorkerCleanup(long capacity, RuntimeException failure) {
        Assume.assumeTrue("rocksdb".equals(graph().backend()));
        Id label = graph().schema().edgeLabel("lifecycle_link")
                          .sourceLabel("lifecycle").targetLabel("lifecycle")
                          .create().id();
        Vertex source = graph().addVertex(T.label, "lifecycle", T.id, 1L);
        Vertex target = graph().addVertex(T.label, "lifecycle", T.id, 2L);
        source.addEdge("lifecycle_link", target);
        graph().tx().commit();
        HugeFactory.closeCurrentThreadTransactions();
        graph().addVertex(T.label, "lifecycle", T.id, 99L);
        Transaction caller = graph().tx();
        RocksDBStore store = Whitebox.getInternalState(params().graphTransaction(), "store");
        Map<String, RocksDBSessions> databases = Whitebox.getInternalState(store, "dbs");
        AtomicReference<WorkerLeases> leases = new AtomicReference<>();
        AtomicInteger visited = new AtomicInteger();
        try (LeaseTraverser traverser = new LeaseTraverser(graph())) {
            Runnable run = () -> traverser.edges(label, capacity, () -> {
                visited.incrementAndGet();
                WorkerLeases observed = new WorkerLeases(Thread.currentThread().getId(),
                                                         new ArrayList<>(databases.values()));
                observed.assertPresent();
                leases.set(observed);
                if (failure != null) {
                    throw failure;
                }
            });
            if (failure != null) {
                Assert.assertSame(failure, Assert.assertThrows(IllegalStateException.class, run::run));
            } else {
                run.run();
            }
            if (capacity != OltpTraverser.NO_LIMIT) {
                Assert.assertEquals(capacity, traverser.edgeIterCounter.get());
                Assert.assertNotNull(traverser.stop.get());
            } else {
                Assert.assertNull(traverser.stop.get());
            }
        }
        Assert.assertEquals(1, visited.get());
        Assert.assertNotNull(leases.get());
        leases.get().assertAbsent();
        Assert.assertTrue(caller.isOpen());
        Assert.assertTrue(graph().vertices(99L).hasNext());
        caller.rollback();
        Assert.assertFalse(graph().vertices(99L).hasNext());
    }

    private void assertOltpWorkerCleanup(boolean batch, RuntimeException failure) {
        Assume.assumeTrue("rocksdb".equals(graph().backend()));
        // The caller's uncommitted write must survive worker and traverser close.
        graph().addVertex(T.label, "lifecycle", T.id, 99L);
        Transaction caller = graph().tx();
        long callerThreadId = Thread.currentThread().getId();
        AtomicReference<WorkerLeases> leases = new AtomicReference<>();
        Runnable work = () -> {
            Assert.assertTrue(Thread.currentThread().getId() != callerThreadId);
            graph().addVertex(T.label, "lifecycle", T.id, 7L);
            RocksDBStore store = Whitebox.getInternalState(params().graphTransaction(), "store");
            Map<String, RocksDBSessions> databases = Whitebox.getInternalState(store, "dbs");
            WorkerLeases observed = new WorkerLeases(Thread.currentThread().getId(),
                                                     new ArrayList<>(databases.values()));
            observed.assertPresent();
            leases.set(observed);
            if (failure != null) {
                throw failure;
            }
        };
        try (LeaseTraverser traverser = new LeaseTraverser(graph())) {
            Runnable run = () -> {
                if (batch) {
                    traverser.batch(work);
                } else {
                    traverser.one(work);
                }
            };
            if (failure != null && !(failure instanceof Consumers.StopExecution)) {
                Assert.assertSame(failure, Assert.assertThrows(IllegalStateException.class, run::run));
            } else {
                run.run();
            }
        }
        Assert.assertNotNull(leases.get());
        leases.get().assertAbsent();
        Assert.assertTrue(caller.isOpen());
        Assert.assertTrue(graph().vertices(99L).hasNext());
        // Worker cleanup rolls back, even if the caller commits its own write.
        caller.commit();
        Assert.assertFalse(graph().vertices(7L).hasNext());
        Assert.assertTrue(graph().vertices(99L).hasNext());
    }

    private static final class WorkerLeases {

        private final long threadId;
        private final List<RocksDBSessions> databases;

        private WorkerLeases(long threadId, List<RocksDBSessions> databases) {
            this.threadId = threadId;
            this.databases = databases;
        }

        private void assertPresent() {
            Assert.assertFalse(this.databases.isEmpty());
            for (RocksDBSessions sessions : this.databases) {
                Map<Long, ?> owners = Whitebox.getInternalState(sessions, "sessions");
                Assert.assertTrue(owners.containsKey(this.threadId));
            }
        }

        private void assertAbsent() {
            for (RocksDBSessions sessions : this.databases) {
                Map<Long, ?> owners = Whitebox.getInternalState(sessions, "sessions");
                Assert.assertFalse(owners.containsKey(this.threadId));
            }
        }
    }

    private static final class LeaseTraverser extends OltpTraverser {

        private final AtomicReference<Consumers.StopExecution> stop = new AtomicReference<>();

        private LeaseTraverser(HugeGraph graph) {
            super(graph);
        }

        private void one(Runnable work) {
            this.traverseByOne(Collections.singletonList(1).iterator(),
                               ignored -> work.run(), "lease-single-item");
        }

        private void batch(Runnable work) {
            this.traverseByBatch(Collections.singletonList(Collections.singletonList(1).iterator()).iterator(),
                                 ignored -> work.run(), "lease-batch", 1);
        }

        private void edges(Id label, long capacity, Runnable work) {
            this.traverseIdsByBfs(Collections.singletonList(IdGenerator.of(1L)).iterator(),
                                   Directions.OUT, label, NO_LIMIT, capacity,
                                   ignored -> work.run());
        }

        @Override
        protected <K> long traverseByBatch(Iterator<Iterator<K>> sources,
                                           Consumer<Iterator<K>> consumer,
                                           String taskName, int concurrentWorkers) {
            return super.traverseByBatch(sources, batch -> {
                try {
                    consumer.accept(batch);
                } catch (Consumers.StopExecution stop) {
                    this.stop.set(stop);
                    throw stop;
                }
            }, taskName, concurrentWorkers);
        }

        private void inline(Runnable work) {
            this.traverseIds(Collections.singletonList(IdGenerator.of(1L)).iterator(),
                             ignored -> work.run(), false);
        }
    }
}
