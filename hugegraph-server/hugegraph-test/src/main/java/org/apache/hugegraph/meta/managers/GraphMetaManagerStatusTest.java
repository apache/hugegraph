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

package org.apache.hugegraph.meta.managers;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hugegraph.meta.GraphStatusEntry;
import org.apache.hugegraph.meta.MetaDriver;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.type.define.GraphStatus;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

public class GraphMetaManagerStatusTest {

    private static final String CLUSTER = "cluster";
    private static final String GRAPH_SPACE = "gs1";
    private static final String GRAPH = "graph1";
    private static final String SERVER = "server-1";

    private static final String STATUS_PREFIX =
            "HUGEGRAPH/cluster/GRAPHSPACE/gs1/GRAPH_STATUS/graph1/";
    private static final String SERVER_STATUS_KEY =
            "HUGEGRAPH/cluster/GRAPHSPACE/gs1/GRAPH_STATUS/graph1/server-1";
    private static final String OTHER_SERVER_STATUS_KEY =
            "HUGEGRAPH/cluster/GRAPHSPACE/gs1/GRAPH_STATUS/graph1/server-2";

    @Test
    public void testUpdateGraphStatusPutsPerServerKey() {
        MetaDriver driver = Mockito.mock(MetaDriver.class);
        GraphMetaManager manager = new GraphMetaManager(driver, CLUSTER);
        GraphStatusEntry entry = new GraphStatusEntry(SERVER,
                                                      GraphStatus.LOADING,
                                                      "opening", 1024L);

        manager.updateGraphStatus(GRAPH_SPACE, GRAPH, entry);

        ArgumentCaptor<String> key = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> value = ArgumentCaptor.forClass(String.class);
        Mockito.verify(driver).put(key.capture(), value.capture());

        Assert.assertEquals(SERVER_STATUS_KEY, key.getValue());
        GraphStatusEntry stored = GraphStatusEntry.fromValue(value.getValue());
        Assert.assertNotNull(stored);
        Assert.assertEquals(SERVER, stored.server());
        Assert.assertEquals(GraphStatus.LOADING, stored.status());
        Assert.assertEquals("opening", stored.message());
        Assert.assertEquals(1024L, stored.updateTime());
    }

    @Test
    public void testGetGraphStatusKeyedByServerId() {
        Map<String, String> keyValues = new LinkedHashMap<>();
        keyValues.put(SERVER_STATUS_KEY, statusJson("server-1", "READY", 11));
        keyValues.put(OTHER_SERVER_STATUS_KEY,
                      statusJson("server-2", "LOADING", 12));
        MetaDriver driver = driverReturning(keyValues);
        GraphMetaManager manager = new GraphMetaManager(driver, CLUSTER);

        Map<String, GraphStatusEntry> status =
                manager.getGraphStatus(GRAPH_SPACE, GRAPH);

        Mockito.verify(driver).scanWithPrefix(STATUS_PREFIX);
        Assert.assertEquals(2, status.size());
        Assert.assertEquals(GraphStatus.READY, status.get("server-1").status());
        Assert.assertEquals(11L, status.get("server-1").updateTime());
        Assert.assertEquals(GraphStatus.LOADING,
                            status.get("server-2").status());
    }

    @Test
    public void testGetGraphStatusSkipsUnparseableValue() {
        Map<String, String> keyValues = new LinkedHashMap<>();
        keyValues.put(SERVER_STATUS_KEY, statusJson("server-1", "READY", 11));
        keyValues.put(OTHER_SERVER_STATUS_KEY, "{not-json");
        MetaDriver driver = driverReturning(keyValues);
        GraphMetaManager manager = new GraphMetaManager(driver, CLUSTER);

        Map<String, GraphStatusEntry> status =
                manager.getGraphStatus(GRAPH_SPACE, GRAPH);

        // The unreadable value is dropped, the readable one is kept
        Assert.assertEquals(1, status.size());
        Assert.assertEquals(GraphStatus.READY, status.get("server-1").status());
        Assert.assertFalse(status.containsKey("server-2"));
    }

    @Test
    public void testGetGraphStatusReturnsEmptyWhenNothingReported() {
        MetaDriver driver = driverReturning(Collections.emptyMap());
        GraphMetaManager manager = new GraphMetaManager(driver, CLUSTER);

        Map<String, GraphStatusEntry> status =
                manager.getGraphStatus(GRAPH_SPACE, GRAPH);

        Mockito.verify(driver).scanWithPrefix(STATUS_PREFIX);
        Assert.assertNotNull(status);
        Assert.assertTrue(status.isEmpty());
    }

    @Test
    public void testRemoveGraphStatusDeletesPerServerKey() {
        MetaDriver driver = Mockito.mock(MetaDriver.class);
        GraphMetaManager manager = new GraphMetaManager(driver, CLUSTER);

        manager.removeGraphStatus(GRAPH_SPACE, GRAPH, SERVER);

        Mockito.verify(driver).delete(SERVER_STATUS_KEY);
        Mockito.verify(driver, Mockito.never())
               .deleteWithPrefix(Mockito.anyString());
    }

    @Test
    public void testClearGraphStatusDeletesWholeGraphPrefix() {
        MetaDriver driver = Mockito.mock(MetaDriver.class);
        GraphMetaManager manager = new GraphMetaManager(driver, CLUSTER);

        manager.clearGraphStatus(GRAPH_SPACE, GRAPH);

        Mockito.verify(driver).deleteWithPrefix(STATUS_PREFIX);
        Mockito.verify(driver, Mockito.never()).delete(Mockito.anyString());
    }

    private static MetaDriver driverReturning(Map<String, String> keyValues) {
        MetaDriver driver = Mockito.mock(MetaDriver.class);
        Mockito.when(driver.scanWithPrefix(Mockito.anyString()))
               .thenReturn(keyValues);
        return driver;
    }

    private static String statusJson(String server, String status,
                                     long updateTime) {
        return String.format("{\"server\":\"%s\",\"status\":\"%s\"," +
                             "\"update_time\":%s}",
                             server, status, updateTime);
    }
}
