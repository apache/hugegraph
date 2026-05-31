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
import org.apache.hugegraph.concurrent.PausableScheduledThreadPool;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.config.ServerOptions;
import org.apache.hugegraph.core.GraphManager;
import org.apache.hugegraph.event.EventHub;
import org.apache.hugegraph.task.DistributedTaskScheduler;
import org.apache.hugegraph.testutil.Assert;
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
        ExecutorService serverInfoDbExecutor = Executors.newSingleThreadExecutor();

        DistributedTaskScheduler scheduler = new DistributedTaskScheduler(
                params, schedulerExecutor, taskDbExecutor, schemaTaskExecutor,
                olapTaskExecutor, gremlinTaskExecutor, ephemeralTaskExecutor,
                serverInfoDbExecutor);
        try {
            scheduler.checkRequirement("schedule");
        } finally {
            schedulerExecutor.shutdownNow();
            taskDbExecutor.shutdownNow();
            schemaTaskExecutor.shutdownNow();
            olapTaskExecutor.shutdownNow();
            gremlinTaskExecutor.shutdownNow();
            ephemeralTaskExecutor.shutdownNow();
            serverInfoDbExecutor.shutdownNow();
        }
    }

    @Test
    public void testGraphManagerDoesNotGenerateServerIdWhenElectionDisabled() {
        HugeConfig config = newConfig();

        GraphManager manager = new GraphManager(config, new EventHub("test"));

        Assert.assertEquals("", config.get(ServerOptions.SERVER_ID));
        Assert.assertNull(manager.globalNodeRoleInfo().nodeId());
    }

    @Test
    public void testGraphManagerRejectsRoleElection() {
        PropertiesConfiguration conf = new PropertiesConfiguration();
        conf.setProperty(ServerOptions.ENABLE_SERVER_ROLE_ELECTION.name(), true);
        HugeConfig config = new HugeConfig(conf);

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            new GraphManager(config, new EventHub("test"));
        }, e -> {
            Assert.assertContains("The server.role_election is no longer supported",
                                  e.getMessage());
        });
    }

    private static HugeConfig newConfig() {
        return new HugeConfig(new PropertiesConfiguration());
    }
}
