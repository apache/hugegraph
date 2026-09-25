/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hugegraph.unit.core;

import java.lang.reflect.Field;
import java.util.Map;

import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.hugegraph.HugeException;
import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.core.GraphManager;
import org.apache.hugegraph.event.EventHub;
import org.apache.hugegraph.meta.MetaDriver;
import org.apache.hugegraph.meta.MetaManager;
import org.apache.hugegraph.meta.managers.GraphMetaManager;
import org.apache.hugegraph.meta.managers.SpaceMetaManager;
import org.apache.hugegraph.space.GraphSpace;
import org.apache.hugegraph.task.TaskScheduler;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.testutil.Whitebox;
import org.apache.hugegraph.unit.BaseUnitTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

public class GraphManagerDropGraphTest extends BaseUnitTest {

    private static final String CLUSTER = "drop-test";
    private static final String VERSION_KEY =
            "HUGEGRAPH/drop-test/SCHEMA_VERSION/DEFAULT/g";

    private MetaDriver driver;
    private HugeGraph graph;
    private Object originalGraphManager;
    private Object originalSpaceManager;
    private GraphManager manager;

    @Before
    public void setup() throws Exception {
        this.driver = Mockito.mock(MetaDriver.class);
        this.originalGraphManager = swapMetaManagerField(
                "graphMetaManager", new GraphMetaManager(this.driver, CLUSTER));
        this.originalSpaceManager = swapMetaManagerField(
                "spaceMetaManager", new SpaceMetaManager(this.driver, CLUSTER));
        this.manager = new GraphManager(
                new HugeConfig(new PropertiesConfiguration()),
                new EventHub("drop-graph-test"));
        // Take the PD branch of dropGraph() with one open graph "DEFAULT-g"
        Whitebox.setInternalState(this.manager, "PDExist", true);
        this.graph = Mockito.mock(HugeGraph.class);
        Mockito.when(this.graph.taskScheduler())
               .thenReturn(Mockito.mock(TaskScheduler.class));
        Map<String, Object> graphs = Whitebox.getInternalState(this.manager,
                                                               "graphs");
        graphs.put("DEFAULT-g", this.graph);
        Map<String, GraphSpace> spaces = Whitebox.getInternalState(
                                         this.manager, "graphSpaces");
        spaces.put("DEFAULT", new GraphSpace("DEFAULT"));
    }

    @After
    public void teardown() throws Exception {
        try {
            Whitebox.setInternalState(this.manager, "PDExist", false);
            this.manager.close();
        } finally {
            swapMetaManagerField("graphMetaManager", this.originalGraphManager);
            swapMetaManagerField("spaceMetaManager", this.originalSpaceManager);
        }
    }

    @Test
    public void testDropGraphDeletesSchemaVersionAfterClear()
                throws Exception {
        this.manager.dropGraph("DEFAULT", "g", true);

        InOrder order = Mockito.inOrder(this.graph, this.driver);
        order.verify(this.graph).clearBackend();
        order.verify(this.driver).delete(VERSION_KEY);
        order.verify(this.graph, Mockito.atLeastOnce()).close();
    }

    @Test
    public void testDropGraphContinuesWhenSchemaVersionDeleteFails()
                throws Exception {
        Mockito.doThrow(new HugeException("pd down"))
               .when(this.driver).delete(VERSION_KEY);

        this.manager.dropGraph("DEFAULT", "g", true);

        Mockito.verify(this.graph, Mockito.atLeastOnce()).close();
        Map<String, Object> graphs = Whitebox.getInternalState(this.manager,
                                                               "graphs");
        Assert.assertFalse(graphs.containsKey("DEFAULT-g"));
    }

    private static Object swapMetaManagerField(String field,
                                               Object replacement)
                                               throws Exception {
        Field f = MetaManager.class.getDeclaredField(field);
        f.setAccessible(true);
        Object previous = f.get(MetaManager.instance());
        f.set(MetaManager.instance(), replacement);
        return previous;
    }
}
