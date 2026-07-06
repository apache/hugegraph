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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.HugeGraphParams;
import org.apache.hugegraph.backend.BackendException;
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
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

public class MasterServerInfoManagerTest {

    private HugeGraphParams graphParams;
    private GraphTransaction tx;

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

    @Test
    public void testInitServerInfoFailsIfExistingMasterCheckFails() {
        HugeServerInfo existedMaster = Mockito.mock(HugeServerInfo.class);
        Mockito.when(existedMaster.role()).thenReturn(NodeRole.MASTER);
        Mockito.when(existedMaster.alive())
               .thenThrow(new IllegalStateException("failed to check alive"));
        Mockito.when(existedMaster.id())
               .thenReturn(GlobalMasterInfo.master("server-1").nodeId());
        ServerInfoManager manager = new TestServerInfoManager(
                this.graphParams, new DirectExecutorService(),
                Collections.singleton(existedMaster));

        Assert.assertThrows(BackendException.class, () -> {
            manager.initServerInfo(GlobalMasterInfo.master("server-2"));
        }, e -> {
            Assert.assertContains("Failed to check existing master nodes",
                                  e.getMessage());
        });

        Mockito.verify(this.tx, Mockito.never())
               .constructVertex(Mockito.eq(false), ArgumentMatchers.any());
    }

    @Test
    public void testInitServerInfoFailsIfCurrentServerInfoClockSkews() {
        GlobalMasterInfo master = GlobalMasterInfo.master("server-2");
        Date skewedUpdateTime = new Date(System.currentTimeMillis() +
                                         TimeUnit.MINUTES.toMillis(1L));
        Vertex vertex = serverInfoVertex(master.nodeId(), NodeRole.MASTER,
                                         skewedUpdateTime);
        Mockito.when(this.tx.queryServerInfos(master.nodeId()))
               .thenReturn(Collections.singleton(vertex).iterator());
        ServerInfoManager manager = new TestServerInfoManager(
                this.graphParams, new DirectExecutorService(),
                Collections.emptyList());

        Assert.assertThrows(BackendException.class, () -> {
            manager.initServerInfo(master);
        }, e -> {
            Assert.assertContains("maybe skew", e.getMessage());
        });

        Mockito.verify(this.tx, Mockito.never())
               .constructVertex(Mockito.eq(false), ArgumentMatchers.any());
    }

    @Test
    public void testInitServerInfoFailsIfWaitingForCurrentServerInfoInterrupted() {
        GlobalMasterInfo master = GlobalMasterInfo.master("server-2");
        Date recentUpdateTime = new Date(System.currentTimeMillis() -
                                         TimeUnit.SECONDS.toMillis(9L));
        Vertex vertex = serverInfoVertex(master.nodeId(), NodeRole.MASTER,
                                         recentUpdateTime);
        Mockito.when(this.tx.queryServerInfos(master.nodeId()))
               .thenReturn(Collections.singleton(vertex).iterator());
        ServerInfoManager manager = new TestServerInfoManager(
                this.graphParams, new DirectExecutorService(),
                Collections.emptyList());

        Thread.currentThread().interrupt();
        try {
            Assert.assertThrows(BackendException.class, () -> {
                manager.initServerInfo(master);
            }, e -> {
                Assert.assertContains("Interrupted when waiting for server info expired",
                                      e.getMessage());
            });
        } finally {
            Thread.interrupted();
        }

        Mockito.verify(this.tx, Mockito.never())
               .constructVertex(Mockito.eq(false), ArgumentMatchers.any());
    }

    private static Vertex serverInfoVertex(Id id, NodeRole role,
                                           Date updateTime) {
        Vertex vertex = Mockito.mock(Vertex.class);
        Mockito.when(vertex.id()).thenReturn(id);
        List<VertexProperty<Object>> properties = Arrays.asList(
                vertexProperty(HugeServerInfo.P.ROLE, role.code()),
                vertexProperty(HugeServerInfo.P.UPDATE_TIME, updateTime)
        );
        Mockito.when(vertex.properties()).thenAnswer(i -> properties.iterator());
        return vertex;
    }

    private static VertexProperty<Object> vertexProperty(String key,
                                                         Object value) {
        @SuppressWarnings("unchecked")
        VertexProperty<Object> property = Mockito.mock(VertexProperty.class);
        Mockito.when(property.key()).thenReturn(key);
        Mockito.when(property.value()).thenReturn(value);
        return property;
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
