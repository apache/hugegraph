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

import java.lang.reflect.Constructor;

import org.apache.hugegraph.HugeFactory;
import org.apache.hugegraph.HugeGraphParams;
import org.apache.hugegraph.backend.store.BackendStore;
import org.apache.hugegraph.backend.tx.GraphTransaction;
import org.apache.hugegraph.backend.tx.ISchemaTransaction;
import org.apache.hugegraph.exception.HugeException;
import org.apache.hugegraph.core.BaseCoreTest;
import org.apache.hugegraph.id.IdGenerator;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.testutil.Whitebox;
import org.apache.hugegraph.traversal.algorithm.KneighborTraverser;
import org.apache.hugegraph.type.define.Directions;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class TransactionLifecycleTest extends BaseCoreTest {

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
    public void testTraverserClosePreservesCallerWrites() {
        graph().addVertex(T.label, "lifecycle", T.id, 1L);
        graph().tx().commit();

        try (KneighborTraverser traverser = new KneighborTraverser(graph())) {
            Assert.assertTrue(traverser.kneighbor(IdGenerator.of(1L), Directions.BOTH,
                                                 null, 1, 100L, 100L).isEmpty());
            graph().addVertex(T.label, "lifecycle", T.id, 2L);
        }

        Assert.assertTrue(graph().tx().isOpen());
        graph().tx().commit();
        HugeFactory.closeCurrentThreadTransactions();
        Assert.assertEquals(2L, graph().traversal().V().count().next().longValue());
    }

    @Test
    public void testFailedCleanupReleasesOwnersBeforeWorkerReuse() throws Exception {
        this.checkFailedCleanupReleasesOwnersBeforeWorkerReuse(false);
    }

    @Test
    public void testFailedRollbackPreservesCauseAndReleasesOwnersBeforeWorkerReuse() throws Exception {
        this.checkFailedCleanupReleasesOwnersBeforeWorkerReuse(true);
    }

    private void checkFailedCleanupReleasesOwnersBeforeWorkerReuse(boolean failRollback) throws Exception {
        Class<?> holderType = Class.forName("org.apache.hugegraph.StandardHugeGraph$Txs");
        Class<?> systemType = Class.forName("org.apache.hugegraph.StandardHugeGraph$SysTransaction");
        GraphTransaction graphTx = Mockito.mock(GraphTransaction.class);
        BackendStore systemStore = Mockito.mock(BackendStore.class);
        Constructor<?> systemConstructor = systemType.getDeclaredConstructor(
                HugeGraphParams.class, BackendStore.class);
        systemConstructor.setAccessible(true);
        GraphTransaction systemTx =
                (GraphTransaction) systemConstructor.newInstance(this.params(), systemStore);
        ISchemaTransaction schemaTx = Mockito.mock(ISchemaTransaction.class);
        RuntimeException first = new IllegalStateException("graph close failed");
        RuntimeException second = new IllegalArgumentException("system close failed");
        RuntimeException rollbackFailure = new IllegalStateException("rollback failed");
        if (failRollback) {
            Mockito.doThrow(rollbackFailure).when(graphTx).rollback();
        }
        Mockito.doThrow(first).when(graphTx).close();
        Mockito.doThrow(second).when(systemStore).close();
        Constructor<?> constructor = holderType.getDeclaredConstructor(
                ISchemaTransaction.class, systemType, GraphTransaction.class);
        constructor.setAccessible(true);
        Object failedOwners = constructor.newInstance(schemaTx, systemTx, graphTx);
        ThreadLocal<Object> owners = Whitebox.getInternalState(graph().tx(), "transactions");
        Assert.assertNull(owners.get());
        owners.set(failedOwners);

        // Dirty request behavior must also be reset when backend close throws.
        graph().tx().onReadWrite(Transaction.READ_WRITE_BEHAVIOR.MANUAL);
        graph().tx().onClose(Transaction.CLOSE_BEHAVIOR.COMMIT);
        if (failRollback) {
            graph().tx().open();
        }
        Throwable failure = Assert.assertThrows(
                HugeException.class, HugeFactory::closeCurrentThreadTransactions);
        if (failRollback) {
            Assert.assertSame(rollbackFailure, failure.getCause());
            Assert.assertEquals(1, rollbackFailure.getSuppressed().length);
            Assert.assertSame(first, rollbackFailure.getSuppressed()[0]);
        } else {
            Assert.assertSame(first, failure.getCause());
        }
        Assert.assertEquals(1, first.getSuppressed().length);
        Assert.assertSame(second, first.getSuppressed()[0]);
        Mockito.verify(graphTx).close();
        // SysTransaction owns both its primary transaction and index transaction.
        Mockito.verify(systemStore, Mockito.times(2)).close();
        Mockito.verify(schemaTx).close();
        Assert.assertNull(owners.get());
        Assert.assertFalse(graph().tx().isOpen());

        // The same thread must acquire fresh owners and retain default AUTO/ROLLBACK.
        graph().addVertex(T.label, "lifecycle", T.id, 1L);
        Assert.assertNotNull(owners.get());
        Assert.assertNotSame(failedOwners, owners.get());
        graph().tx().close();
        Assert.assertEquals(0L, graph().traversal().V().count().next().longValue());
        graph().addVertex(T.label, "lifecycle", T.id, 2L);
        graph().tx().commit();
        HugeFactory.closeCurrentThreadTransactions();
        Assert.assertNull(owners.get());
        Assert.assertEquals(1L, graph().traversal().V().count().next().longValue());
    }
}
