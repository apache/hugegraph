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

package org.apache.hugegraph.unit.core;

import org.apache.hugegraph.HugeException;
import org.apache.hugegraph.StandardHugeGraph;
import org.apache.hugegraph.backend.cache.CachedSchemaTransactionV2;
import org.apache.hugegraph.backend.store.BackendStore;
import org.apache.hugegraph.backend.store.BackendStoreProvider;
import org.apache.hugegraph.backend.tx.ISchemaTransaction;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.task.TaskScheduler;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.testutil.Whitebox;
import org.apache.hugegraph.unit.BaseUnitTest;
import org.apache.hugegraph.unit.FakeObjects;
import org.apache.hugegraph.util.LockUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

public class StandardHugeGraphClearBackendTest extends BaseUnitTest {

    private static final String SPACE_GRAPH = "space-graph";

    private StandardHugeGraph graph;
    private BackendStoreProvider provider;
    private CachedSchemaTransactionV2 schemaTransaction;

    @Before
    public void setup() {
        HugeConfig config = FakeObjects.newConfig();
        this.graph = Mockito.mock(StandardHugeGraph.class,
                                  Mockito.CALLS_REAL_METHODS);
        this.provider = Mockito.mock(BackendStoreProvider.class);
        this.schemaTransaction = Mockito.mock(CachedSchemaTransactionV2.class);
        BackendStore schemaStore = Mockito.mock(BackendStore.class);
        BackendStore systemStore = Mockito.mock(BackendStore.class);
        BackendStore graphStore = Mockito.mock(BackendStore.class);
        TaskScheduler scheduler = Mockito.mock(TaskScheduler.class);

        Whitebox.setInternalState(this.graph, "configuration", config);
        Whitebox.setInternalState(this.graph, "storeProvider", this.provider);
        Whitebox.setInternalState(this.graph, "name", "graph");
        Whitebox.setInternalState(this.graph, "graphSpace", "space");

        Mockito.doReturn(scheduler).when(this.graph).taskScheduler();
        Mockito.doReturn(this.schemaTransaction)
               .when(this.graph).schemaTransaction();
        Mockito.when(this.provider.isHstore()).thenReturn(true);
        Mockito.when(this.provider.loadSchemaStore(config))
               .thenReturn(schemaStore);
        Mockito.when(this.provider.loadSystemStore(config))
               .thenReturn(systemStore);
        Mockito.when(this.provider.loadGraphStore(config))
               .thenReturn(graphStore);
        LockUtil.init(SPACE_GRAPH);
    }

    @After
    public void teardown() {
        LockUtil.destroy(SPACE_GRAPH);
    }

    @Test
    public void testHstoreClearSchemaBeforeStore() {
        this.graph.clearBackend();

        InOrder order = Mockito.inOrder(this.schemaTransaction, this.provider);
        order.verify(this.schemaTransaction).clear();
        order.verify(this.provider).clear();
    }

    @Test
    public void testHstoreSchemaFailureStopsStoreClear() {
        Mockito.doThrow(new HugeException("schema clear failed"))
               .when(this.schemaTransaction).clear();

        Assert.assertThrows(HugeException.class, this.graph::clearBackend);
        Mockito.verify(this.provider, Mockito.never()).clear();
    }

    @Test
    public void testHstoreStoreFailurePropagates() {
        Mockito.doThrow(new HugeException("store clear failed"))
               .when(this.provider).clear();

        Assert.assertThrows(HugeException.class, this.graph::clearBackend);
        Mockito.verify(this.schemaTransaction).clear();
    }

    @Test
    public void testHstoreRejectsUnexpectedSchemaTransaction() {
        ISchemaTransaction unexpected = Mockito.mock(ISchemaTransaction.class);
        Mockito.doReturn(unexpected).when(this.graph).schemaTransaction();

        Assert.assertThrows(IllegalStateException.class,
                            this.graph::clearBackend);
        Mockito.verify(this.provider, Mockito.never()).clear();
    }

    @Test
    public void testRocksdbDoesNotClearV2SchemaMetadata() {
        Mockito.when(this.provider.isHstore()).thenReturn(false);

        this.graph.clearBackend();

        Mockito.verify(this.schemaTransaction, Mockito.never()).clear();
        Mockito.verify(this.provider).clear();
    }
}
