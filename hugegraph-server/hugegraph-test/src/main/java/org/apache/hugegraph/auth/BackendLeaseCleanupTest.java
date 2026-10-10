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

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hugegraph.HugeFactory;
import org.apache.hugegraph.auth.HugeGraphAuthProxy.Context;
import org.apache.hugegraph.auth.HugeGraphAuthProxy.ContextTask;
import org.apache.hugegraph.backend.store.rocksdb.RocksDBSessions;
import org.apache.hugegraph.backend.store.rocksdb.RocksDBStore;
import org.apache.hugegraph.core.BaseCoreTest;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.testutil.Whitebox;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Transaction;
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

}
