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

import java.util.Collections;
import java.util.Map;

import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.core.GraphManager;
import org.apache.hugegraph.event.EventHub;
import org.apache.hugegraph.meta.MetaManager;
import org.apache.hugegraph.space.GraphSpace;
import org.apache.hugegraph.space.Service;
import org.apache.hugegraph.task.TaskScheduler;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.testutil.Whitebox;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.junit.Test;
import org.mockito.Mockito;

public class GraphManagerClearGraphSpaceTest {

    /**
     * Graph and service keys are "{graphspace}-{name}", and a graph space
     * name may be a prefix of another one, so clearing "gs" must leave the
     * graphs and services of "gs2" and "gs_x" alone
     */
    @Test
    public void testClearKeepsGraphsOfSpacesNamedLikeIt() {
        HugeGraph cleared = mockGraph("gs-g");
        HugeGraph kept = mockGraph("gs2-g");
        HugeGraph keptUnderscore = mockGraph("gs_x-g");
        MetaManager meta = Mockito.mock(MetaManager.class);

        // A mock hub: the executor of EventHub is static, and a test that ran
        // earlier in the suite may have shut it down (HugeFactory.shutdown)
        GraphManager manager = new GraphManager(
                new HugeConfig(new PropertiesConfiguration()),
                Mockito.mock(EventHub.class));
        try {
            Whitebox.setInternalState(manager, "PDExist", true);
            Whitebox.setInternalState(manager, "metaManager", meta);
            Whitebox.setInternalState(manager, "localGraphs",
                                      Collections.emptySet());
            Map<String, GraphSpace> spaces =
                    Whitebox.getInternalState(manager, "graphSpaces");
            for (String space : new String[]{"gs", "gs2", "gs_x"}) {
                spaces.put(space, new GraphSpace(space));
            }
            Map<String, Graph> graphs =
                    Whitebox.getInternalState(manager, "graphs");
            graphs.put("gs-g", cleared);
            graphs.put("gs2-g", kept);
            graphs.put("gs_x-g", keptUnderscore);
            Map<String, Service> services =
                    Whitebox.getInternalState(manager, "services");
            services.put("gs-s", Mockito.mock(Service.class));
            services.put("gs2-s", Mockito.mock(Service.class));
            services.put("gs_x-s", Mockito.mock(Service.class));

            manager.clearGraphSpace("gs");

            Mockito.verify(cleared).clearBackend();
            Mockito.verify(kept, Mockito.never()).clearBackend();
            Mockito.verify(keptUnderscore, Mockito.never()).clearBackend();
            Assert.assertFalse(graphs.containsKey("gs-g"));
            Assert.assertTrue(graphs.containsKey("gs2-g"));
            Assert.assertTrue(graphs.containsKey("gs_x-g"));

            Mockito.verify(meta).service("gs", "s");
            Mockito.verify(meta, Mockito.never()).service("gs2", "s");
            Mockito.verify(meta, Mockito.never()).service("gs_x", "s");
        } finally {
            manager.close();
        }
    }

    private static HugeGraph mockGraph(String spaceGraphName) {
        HugeGraph graph = Mockito.mock(HugeGraph.class);
        // No tasks to drain: the drop closes task admission and goes on
        TaskScheduler scheduler = Mockito.mock(TaskScheduler.class);
        Mockito.when(scheduler.close()).thenReturn(true);
        Mockito.when(graph.taskScheduler()).thenReturn(scheduler);
        Mockito.when(graph.spaceGraphName()).thenReturn(spaceGraphName);
        return graph;
    }
}
