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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.HugeGraphParams;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.id.IdGenerator;
import org.apache.hugegraph.concurrent.PausableScheduledThreadPool;
import org.apache.hugegraph.config.CoreOptions;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.config.ServerOptions;
import org.apache.hugegraph.core.GraphManager;
import org.apache.hugegraph.event.EventHub;
import org.apache.hugegraph.exception.NotFoundException;
import org.apache.hugegraph.schema.VertexLabel;
import org.apache.hugegraph.structure.HugeVertex;
import org.apache.hugegraph.task.DistributedTaskScheduler;
import org.apache.hugegraph.task.HugeTask;
import org.apache.hugegraph.task.TaskCallable;
import org.apache.hugegraph.task.TaskStatus;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.testutil.Whitebox;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.type.define.IdStrategy;
import org.apache.hugegraph.type.define.NodeRole;
import org.apache.hugegraph.unit.FakeObjects;
import org.apache.hugegraph.util.ExecutorUtil;
import org.junit.Test;
import org.mockito.Mockito;

public class TaskSchedulerServerInfoTest {

    @Test
    public void testDistributedCheckRequirementDoesNotNeedServerInfo() {
        HugeGraph graph = Mockito.mock(HugeGraph.class);
        Mockito.when(graph.graphSpace()).thenReturn("DEFAULT");

        HugeGraphParams params = Mockito.mock(HugeGraphParams.class);
        Mockito.when(params.graph()).thenReturn(graph);
        Mockito.when(params.name()).thenReturn("hugegraph");
        Mockito.when(params.spaceGraphName()).thenReturn("DEFAULT-hugegraph");
        Mockito.when(params.configuration()).thenReturn(newConfig());

        PausableScheduledThreadPool schedulerExecutor =
                ExecutorUtil.newPausableScheduledThreadPool(
                        1, "distributed-scheduler-test-%d");
        ExecutorService taskDbExecutor = Executors.newSingleThreadExecutor();
        ExecutorService schemaTaskExecutor = Executors.newSingleThreadExecutor();
        ExecutorService olapTaskExecutor = Executors.newSingleThreadExecutor();
        ExecutorService gremlinTaskExecutor = Executors.newSingleThreadExecutor();
        ExecutorService ephemeralTaskExecutor = Executors.newSingleThreadExecutor();

        try {
            DistributedTaskScheduler scheduler = new DistributedTaskScheduler(
                    params, schedulerExecutor, taskDbExecutor, schemaTaskExecutor,
                    olapTaskExecutor, gremlinTaskExecutor, ephemeralTaskExecutor);
            scheduler.checkRequirement("schedule");
        } finally {
            schedulerExecutor.shutdownNow();
            taskDbExecutor.shutdownNow();
            schemaTaskExecutor.shutdownNow();
            olapTaskExecutor.shutdownNow();
            gremlinTaskExecutor.shutdownNow();
            ephemeralTaskExecutor.shutdownNow();
        }
    }

    @Test
    public void testDistributedCancelTreatsMissingRetryTaskAsGone() {
        HugeGraph graph = Mockito.mock(HugeGraph.class);
        Mockito.when(graph.graphSpace()).thenReturn("DEFAULT");

        HugeGraphParams params = Mockito.mock(HugeGraphParams.class);
        Mockito.when(params.graph()).thenReturn(graph);
        Mockito.when(params.name()).thenReturn("hugegraph");
        Mockito.when(params.spaceGraphName()).thenReturn("DEFAULT-hugegraph");
        Mockito.when(params.configuration()).thenReturn(newConfig());

        PausableScheduledThreadPool schedulerExecutor =
                ExecutorUtil.newPausableScheduledThreadPool(
                        1, "distributed-cancel-test-%d");
        ExecutorService taskDbExecutor = Executors.newSingleThreadExecutor();
        ExecutorService schemaTaskExecutor = Executors.newSingleThreadExecutor();
        ExecutorService olapTaskExecutor = Executors.newSingleThreadExecutor();
        ExecutorService gremlinTaskExecutor = Executors.newSingleThreadExecutor();
        ExecutorService ephemeralTaskExecutor = Executors.newSingleThreadExecutor();

        try {
            DistributedTaskScheduler scheduler = new DistributedTaskScheduler(
                    params, schedulerExecutor, taskDbExecutor, schemaTaskExecutor,
                    olapTaskExecutor, gremlinTaskExecutor, ephemeralTaskExecutor) {

                @Override
                protected boolean updateStatus(Id id, TaskStatus prestatus,
                                               TaskStatus status) {
                    return false;
                }

                @Override
                protected <V> HugeTask<V> taskWithoutResult(Id id) {
                    throw new NotFoundException("Can't find task with id '%s'", id);
                }
            };
            Id id = IdGenerator.of(999997);
            HugeTask<?> task = new HugeTask<>(id, null, new TaskCallable<Object>() {
                @Override
                public Object call() {
                    return null;
                }
            });
            task.type("test");
            task.name("missing-retry-task");

            scheduler.cancel(task);
        } finally {
            schedulerExecutor.shutdownNow();
            taskDbExecutor.shutdownNow();
            schemaTaskExecutor.shutdownNow();
            olapTaskExecutor.shutdownNow();
            gremlinTaskExecutor.shutdownNow();
            ephemeralTaskExecutor.shutdownNow();
        }
    }

    @Test
    public void testGraphManagerDoesNotGenerateServerId() {
        HugeConfig config = newConfig();

        GraphManager manager = new GraphManager(config, new EventHub("test"));
        try {
            Assert.assertEquals("", config.get(ServerOptions.SERVER_ID));
            Assert.assertNull(manager.globalNodeRoleInfo().nodeId());
        } finally {
            manager.close();
        }
    }

    @Test
    public void testGraphManagerIgnoresRemovedRoleElectionOptions() {
        // Old rest-server.properties files may still set these keys
        PropertiesConfiguration conf = new PropertiesConfiguration();
        conf.setProperty("server.role_election", true);
        conf.setProperty("server.role.fail_count", 5);
        conf.setProperty(ServerOptions.SERVER_ROLE.name(), "worker");
        HugeConfig config = new HugeConfig(conf);

        GraphManager manager = new GraphManager(config, new EventHub("test"));
        try {
            Assert.assertEquals(NodeRole.WORKER,
                                manager.globalNodeRoleInfo().nodeRole());
        } finally {
            manager.close();
        }
    }

    @Test
    public void testLegacyServerLabelsKeepServerType() {
        // Graphs created by older versions may still store these vertices
        FakeObjects fakeObject = new FakeObjects();
        for (String name : new String[]{"~server", "~role_data"}) {
            VertexLabel label = fakeObject.newVertexLabel(IdGenerator.of(name), name,
                                                          IdStrategy.CUSTOMIZE_STRING);
            HugeVertex vertex = new HugeVertex(fakeObject.graph(), null, label);
            Assert.assertEquals(HugeType.SERVER, vertex.type());
        }

        VertexLabel person = fakeObject.newVertexLabel(IdGenerator.of(1), "person",
                                                       IdStrategy.CUSTOMIZE_STRING);
        HugeVertex vertex = new HugeVertex(fakeObject.graph(), null, person);
        Assert.assertEquals(HugeType.VERTEX, vertex.type());
    }

    @Test
    public void testGraphManagerDoesNotInjectPdPeersForStandaloneRocksDB() {
        HugeConfig serverConfig = newConfig();
        GraphManager manager = new GraphManager(serverConfig, new EventHub("test"));
        HugeConfig graphConfig = newGraphConfig("rocksdb");

        try {
            Whitebox.invoke(manager.getClass(), "transferPdPeersConfig",
                            manager, graphConfig);

            Assert.assertFalse(graphConfig.containsKey(CoreOptions.PD_PEERS.name()));
        } finally {
            manager.close();
        }
    }

    @Test
    public void testGraphManagerInjectsPdPeersForHStoreGraph() {
        HugeConfig serverConfig = newConfig();
        GraphManager manager = new GraphManager(serverConfig, new EventHub("test"));
        HugeConfig graphConfig = newGraphConfig("hstore");
        HugeConfig mixedCaseGraphConfig = newGraphConfig("HStore");

        try {
            Whitebox.invoke(manager.getClass(), "transferPdPeersConfig",
                            manager, graphConfig);
            Whitebox.invoke(manager.getClass(), "transferPdPeersConfig",
                            manager, mixedCaseGraphConfig);

            Assert.assertEquals(serverConfig.get(ServerOptions.PD_PEERS),
                                graphConfig.get(CoreOptions.PD_PEERS));
            Assert.assertEquals(serverConfig.get(ServerOptions.PD_PEERS),
                                mixedCaseGraphConfig.get(CoreOptions.PD_PEERS));
        } finally {
            manager.close();
        }
    }

    private static HugeConfig newConfig() {
        return new HugeConfig(new PropertiesConfiguration());
    }

    private static HugeConfig newGraphConfig(String backend) {
        PropertiesConfiguration conf = new PropertiesConfiguration();
        conf.setProperty(CoreOptions.BACKEND.name(), backend);
        return new HugeConfig(conf);
    }
}
