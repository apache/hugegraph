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

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.HugeGraphParams;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.store.BackendFeatures;
import org.apache.hugegraph.backend.tx.GraphTransaction;
import org.apache.hugegraph.backend.tx.ISchemaTransaction;
import org.apache.hugegraph.masterelection.GlobalMasterInfo;
import org.apache.hugegraph.schema.VertexLabel;
import org.apache.hugegraph.structure.HugeVertex;
import org.apache.hugegraph.task.HugeServerInfo;
import org.apache.hugegraph.task.ServerInfoManager;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.type.define.NodeRole;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

public class ServerInfoManagerTest {

    private HugeGraphParams graphParams;
    private GraphTransaction tx;
    private ServerInfoManager serverInfoManager;

    @Before
    public void setup() {
        this.graphParams = Mockito.mock(HugeGraphParams.class);
        Mockito.when(this.graphParams.spaceGraphName())
               .thenReturn("DEFAULT-hugegraph");

        HugeGraph graph = Mockito.mock(HugeGraph.class);
        BackendFeatures features = Mockito.mock(BackendFeatures.class);
        Mockito.when(features.supportsQueryByPage()).thenReturn(false);
        Mockito.when(graph.backendStoreFeatures()).thenReturn(features);
        Mockito.when(this.graphParams.graph()).thenReturn(graph);

        ISchemaTransaction schema = Mockito.mock(ISchemaTransaction.class);
        VertexLabel serverLabel = Mockito.mock(VertexLabel.class);
        Mockito.when(schema.getVertexLabel(HugeServerInfo.P.SERVER))
               .thenReturn(serverLabel);
        Mockito.when(this.graphParams.schemaTransaction()).thenReturn(schema);

        this.tx = Mockito.mock(GraphTransaction.class);
        Mockito.when(this.tx.queryServerInfos(ArgumentMatchers.any(Id.class)))
               .thenReturn(Collections.emptyIterator());
        HugeVertex vertex = Mockito.mock(HugeVertex.class);
        Mockito.when(vertex.id()).thenReturn(GlobalMasterInfo.master("server-2")
                                                             .nodeId());
        Mockito.when(this.tx.constructVertex(Mockito.eq(false),
                                             ArgumentMatchers.any()))
               .thenReturn(vertex);
        Mockito.when(this.tx.addVertex(vertex)).thenReturn(vertex);
        Mockito.when(this.graphParams.systemTransaction()).thenReturn(this.tx);

        DirectExecutorService executor = new DirectExecutorService();
        this.serverInfoManager = new ServerInfoManager(this.graphParams,
                                                       executor);
    }

    @Test
    public void testSelfNodeIdReturnsNullWhenNotInitialized() {
        Assert.assertNull(this.serverInfoManager.selfNodeId());
    }

    @Test
    public void testInitServerInfoRejectsExistingAliveMaster() {
        HugeServerInfo existedMaster = Mockito.mock(HugeServerInfo.class);
        Mockito.when(existedMaster.role()).thenReturn(NodeRole.MASTER);
        Mockito.when(existedMaster.alive()).thenReturn(true);
        Mockito.when(existedMaster.id())
               .thenReturn(GlobalMasterInfo.master("server-1").nodeId());
        ServerInfoManager manager = new TestServerInfoManager(
                this.graphParams, new DirectExecutorService(),
                Collections.singleton(existedMaster));

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            manager.initServerInfo(GlobalMasterInfo.master("server-2"));
        }, e -> {
            Assert.assertContains("Already existed master", e.getMessage());
        });

        Mockito.verify(this.tx, Mockito.never())
               .constructVertex(Mockito.eq(false), ArgumentMatchers.any());
    }

    private static class TestServerInfoManager extends ServerInfoManager {

        private final Collection<HugeServerInfo> serverInfos;

        public TestServerInfoManager(HugeGraphParams graph,
                                     DirectExecutorService executor,
                                     Collection<HugeServerInfo> serverInfos) {
            super(graph, executor);
            this.serverInfos = serverInfos;
        }

        @Override
        protected Iterator<HugeServerInfo> serverInfos(long limit,
                                                       String page) {
            return this.serverInfos.iterator();
        }
    }

    private static class DirectExecutorService extends AbstractExecutorService {

        private boolean shutdown;

        @Override
        public void shutdown() {
            this.shutdown = true;
        }

        @Override
        public java.util.List<Runnable> shutdownNow() {
            this.shutdown = true;
            return Collections.emptyList();
        }

        @Override
        public boolean isShutdown() {
            return this.shutdown;
        }

        @Override
        public boolean isTerminated() {
            return this.shutdown;
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) {
            return true;
        }

        @Override
        public void execute(Runnable command) {
            command.run();
        }

        @Override
        public <T> Future<T> submit(Callable<T> task) {
            FutureTask<T> future = new FutureTask<>(task);
            String originName = Thread.currentThread().getName();
            Thread.currentThread().setName("server-info-db-worker-test");
            try {
                future.run();
            } finally {
                Thread.currentThread().setName(originName);
            }
            return future;
        }
    }
}
