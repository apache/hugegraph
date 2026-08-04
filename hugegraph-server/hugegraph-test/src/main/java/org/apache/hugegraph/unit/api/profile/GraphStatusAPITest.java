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

package org.apache.hugegraph.unit.api.profile;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.api.profile.GraphsAPI;
import org.apache.hugegraph.core.GraphManager;
import org.apache.hugegraph.exception.NotFoundException;
import org.apache.hugegraph.meta.GraphStatusEntry;
import org.apache.hugegraph.meta.MetaManager;
import org.apache.hugegraph.pd.client.DiscoveryClientImpl;
import org.apache.hugegraph.pd.grpc.discovery.NodeInfo;
import org.apache.hugegraph.pd.grpc.discovery.NodeInfos;
import org.apache.hugegraph.pd.grpc.discovery.Query;
import org.apache.hugegraph.space.GraphSpace;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.testutil.Whitebox;
import org.apache.hugegraph.type.define.GraphStatus;
import org.apache.hugegraph.unit.BaseUnitTest;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.junit.Test;
import org.mockito.Mockito;

import jakarta.ws.rs.ForbiddenException;
import jakarta.ws.rs.core.SecurityContext;
import sun.misc.Unsafe;

public class GraphStatusAPITest extends BaseUnitTest {

    private static final String GRAPHSPACE = "DEFAULT";
    private static final String GRAPH = "hugegraph";
    private static final String SERVER_1 = "127.0.0.1:8080";
    private static final String SERVER_2 = "127.0.0.1:8081";
    private static final String SERVER_URL = "http://" + SERVER_1;

    private static final String GRAPHSPACE_KEY = "graphspace";
    private static final String GRAPH_KEY = "graph";
    private static final String STATUS_KEY = "status";
    private static final String READY_COUNT_KEY = "ready_count";
    private static final String TOTAL_COUNT_KEY = "total_count";
    private static final String EXPECTED_COUNT_KEY = "expected_count";
    private static final String SERVERS_KEY = "servers";
    private static final String SERVER_KEY = "server";
    private static final String MESSAGE_KEY = "message";

    private static final String SERVER_ID_LABEL = "SERVER_ID";
    private static final String GRAPHSPACE_LABEL = "GRAPHSPACE";

    @Test
    public void testStatusIsReadyWhenEveryReplicaReported() {
        GraphManager manager = pdManager(
                reported(entry(SERVER_1, GraphStatus.READY, null),
                         entry(SERVER_2, GraphStatus.READY, null)),
                2, true);

        Map<String, Object> result = status(manager);

        Assert.assertEquals(GRAPHSPACE, result.get(GRAPHSPACE_KEY));
        Assert.assertEquals(GRAPH, result.get(GRAPH_KEY));
        Assert.assertEquals(GraphStatus.READY.name(), result.get(STATUS_KEY));
        Assert.assertEquals(2, result.get(READY_COUNT_KEY));
        Assert.assertEquals(2, result.get(TOTAL_COUNT_KEY));
        Assert.assertEquals(2, result.get(EXPECTED_COUNT_KEY));

        List<Map<String, Object>> servers = servers(result);
        Assert.assertEquals(2, servers.size());
        Assert.assertEquals(SERVER_1, servers.get(0).get(SERVER_KEY));
        Assert.assertEquals(SERVER_2, servers.get(1).get(SERVER_KEY));
    }

    @Test
    public void testStatusIsLoadingWhenAReplicaHasNotReported() {
        GraphManager manager = pdManager(
                reported(entry(SERVER_1, GraphStatus.READY, null)), 3, true);

        Map<String, Object> result = status(manager);

        Assert.assertEquals(GraphStatus.LOADING.name(), result.get(STATUS_KEY));
        Assert.assertEquals(1, result.get(READY_COUNT_KEY));
        Assert.assertEquals(1, result.get(TOTAL_COUNT_KEY));
        Assert.assertEquals(3, result.get(EXPECTED_COUNT_KEY));
        Assert.assertEquals(1, servers(result).size());
    }

    @Test
    public void testStatusIsFailedWhenAReplicaFailed() {
        GraphManager manager = pdManager(
                reported(entry(SERVER_1, GraphStatus.READY, null),
                         entry(SERVER_2, GraphStatus.FAILED, "backend down")),
                2, true);

        Map<String, Object> result = status(manager);

        Assert.assertEquals(GraphStatus.FAILED.name(), result.get(STATUS_KEY));
        Assert.assertEquals(1, result.get(READY_COUNT_KEY));
        Assert.assertEquals(2, result.get(TOTAL_COUNT_KEY));

        Map<String, Object> failed = servers(result).get(1);
        Assert.assertEquals(GraphStatus.FAILED.name(), failed.get(STATUS_KEY));
        Assert.assertEquals("backend down", failed.get(MESSAGE_KEY));
    }

    @Test
    public void testStatusIsUnknownBeforeAnyReplicaReports() {
        // The graph config is already written, no server reported status yet
        GraphManager manager = pdManager(Collections.emptyMap(), 2, true);

        Map<String, Object> result = status(manager);

        Assert.assertEquals("UNKNOWN", result.get(STATUS_KEY));
        Assert.assertEquals(0, result.get(READY_COUNT_KEY));
        Assert.assertEquals(0, result.get(TOTAL_COUNT_KEY));
        Assert.assertEquals(2, result.get(EXPECTED_COUNT_KEY));
        Assert.assertTrue(servers(result).isEmpty());
    }

    @Test
    public void testStatusCountsServersRatherThanAddresses() {
        // One server registers one node per url it listens on, the servers
        // that should report are counted, not the urls they answer on
        NodeInfos infos = NodeInfos.newBuilder()
                                   .addInfo(node(SERVER_1, "server-a",
                                                 GRAPHSPACE))
                                   .addInfo(node("10.0.0.1:8080", "server-a",
                                                 GRAPHSPACE))
                                   .addInfo(node(SERVER_2, "server-b",
                                                 GRAPHSPACE))
                                   .build();
        GraphManager manager = pdManager(
                reported(entry("server-a", GraphStatus.READY, null),
                         entry("server-b", GraphStatus.READY, null)),
                discoveryClient(infos), true);

        Map<String, Object> result = status(manager);

        Assert.assertEquals(2, result.get(EXPECTED_COUNT_KEY));
        Assert.assertEquals(GraphStatus.READY.name(), result.get(STATUS_KEY));
    }

    @Test
    public void testStatusSkipsServersOfAnotherGraphSpace() {
        // A server registered to another graph space never serves this graph
        NodeInfos infos = NodeInfos.newBuilder()
                                   .addInfo(node(SERVER_1, "server-a",
                                                 GRAPHSPACE))
                                   .addInfo(node(SERVER_2, "server-b",
                                                 "other"))
                                   .build();
        GraphManager manager = pdManager(
                reported(entry("server-a", GraphStatus.READY, null)),
                discoveryClient(infos), true);

        Map<String, Object> result = status(manager);

        Assert.assertEquals(1, result.get(EXPECTED_COUNT_KEY));
        Assert.assertEquals(GraphStatus.READY.name(), result.get(STATUS_KEY));
    }

    @Test
    public void testStatusIsLoadingWhenTheExpectedCountIsUnknown() {
        // Asking pd failed, so it can't be told whether the servers that
        // reported ready are all the servers of the cluster
        DiscoveryClientImpl pdClient = Mockito.mock(DiscoveryClientImpl.class);
        Mockito.when(pdClient.getNodeInfos(Mockito.any(Query.class)))
               .thenThrow(new IllegalStateException("pd is unreachable"));
        GraphManager manager = pdManager(
                reported(entry(SERVER_1, GraphStatus.READY, null)),
                pdClient, true);

        Map<String, Object> result = status(manager);

        Assert.assertEquals(GraphStatus.LOADING.name(), result.get(STATUS_KEY));
        Assert.assertEquals(1, result.get(READY_COUNT_KEY));
        Assert.assertTrue(result.containsKey(EXPECTED_COUNT_KEY));
        Assert.assertNull(result.get(EXPECTED_COUNT_KEY));
    }

    @Test
    public void testStatusNotFoundForUnknownGraph() {
        GraphManager manager = pdManager(Collections.emptyMap(), 2, false);

        Assert.assertThrows(NotFoundException.class, () -> {
            new GraphsAPI().status(manager, GRAPHSPACE, GRAPH,
                                   securityContext(true));
        }, e -> {
            Assert.assertContains("Graph 'hugegraph' does not exist",
                                  e.getMessage());
        });
    }

    @Test
    public void testStatusNotFoundForDroppedGraphWithLeftoverStatus() {
        // The graph is gone but the removal of one status entry didn't make
        // it, the answer is still that the graph does not exist
        GraphManager manager = pdManager(
                reported(entry(SERVER_1, GraphStatus.READY, null)), 1, false);

        Assert.assertThrows(NotFoundException.class, () -> {
            new GraphsAPI().status(manager, GRAPHSPACE, GRAPH,
                                   securityContext(true));
        }, e -> {
            Assert.assertContains("Graph 'hugegraph' does not exist",
                                  e.getMessage());
        });
    }

    @Test
    public void testStatusNotFoundForUnknownGraphSpace() {
        GraphManager manager = pdManager(
                reported(entry(SERVER_1, GraphStatus.READY, null)), 1, true);
        Whitebox.setInternalState(manager, "graphSpaces",
                                  new ConcurrentHashMap<String, GraphSpace>());

        Assert.assertThrows(NotFoundException.class, () -> {
            new GraphsAPI().status(manager, GRAPHSPACE, GRAPH,
                                   securityContext(true));
        }, e -> {
            Assert.assertContains("Graph space", e.getMessage());
        });
    }

    @Test
    public void testStatusIsReadyForLocalGraphWithoutPd() {
        GraphManager manager = standaloneManager(true);

        Map<String, Object> result = status(manager);

        Assert.assertEquals(GRAPHSPACE, result.get(GRAPHSPACE_KEY));
        Assert.assertEquals(GRAPH, result.get(GRAPH_KEY));
        Assert.assertEquals(GraphStatus.READY.name(), result.get(STATUS_KEY));
        Assert.assertEquals(1, result.get(READY_COUNT_KEY));
        Assert.assertEquals(1, result.get(TOTAL_COUNT_KEY));
        Assert.assertEquals(1, result.get(EXPECTED_COUNT_KEY));

        List<Map<String, Object>> servers = servers(result);
        Assert.assertEquals(1, servers.size());
        Assert.assertEquals(SERVER_1, servers.get(0).get(SERVER_KEY));
        Assert.assertEquals(GraphStatus.READY.name(),
                            servers.get(0).get(STATUS_KEY));
    }

    @Test
    public void testStatusNotFoundForUnknownGraphWithoutPd() {
        GraphManager manager = standaloneManager(false);

        Assert.assertThrows(NotFoundException.class, () -> {
            new GraphsAPI().status(manager, GRAPHSPACE, GRAPH,
                                   securityContext(true));
        }, e -> {
            Assert.assertContains("Graph 'hugegraph' does not exist",
                                  e.getMessage());
        });
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> status(GraphManager manager) {
        return status(manager, securityContext(true));
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> status(GraphManager manager,
                                              SecurityContext sc) {
        Object result = new GraphsAPI().status(manager, GRAPHSPACE, GRAPH, sc);
        Assert.assertInstanceOf(Map.class, result);
        return (Map<String, Object>) result;
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> servers(Map<String, Object> map) {
        Object servers = map.get(SERVERS_KEY);
        Assert.assertInstanceOf(List.class, servers);
        return (List<Map<String, Object>>) servers;
    }

    @Test
    public void testStatusIsForbiddenWithoutReadOnTheGraph() {
        /*
         * A member of the graph space may hold a permission on another graph
         * of the space only. The other reads of this resource verify the
         * permission while opening the graph, this one can't
         */
        GraphManager manager = pdManager(
                reported(entry(SERVER_1, GraphStatus.READY, null)), 1, true);

        Assert.assertThrows(ForbiddenException.class, () -> {
            status(manager, securityContext(false));
        }, e -> {
            Assert.assertContains("not allowed to read graph 'hugegraph'",
                                  e.getMessage());
        });
    }

    private static SecurityContext securityContext(boolean allowed) {
        SecurityContext sc = Mockito.mock(SecurityContext.class);
        Mockito.when(sc.isUserInRole(Mockito.anyString()))
               .thenReturn(allowed);
        return sc;
    }

    private static GraphStatusEntry entry(String server, GraphStatus status,
                                          String message) {
        return new GraphStatusEntry(server, status, message, 1L);
    }

    private static Map<String, GraphStatusEntry> reported(
            GraphStatusEntry... entries) {
        Map<String, GraphStatusEntry> status = new LinkedHashMap<>();
        for (GraphStatusEntry entry : entries) {
            status.put(entry.server(), entry);
        }
        return status;
    }

    private static GraphManager pdManager(Map<String, GraphStatusEntry> status,
                                          int replicas, boolean configExists) {
        return pdManager(status, discoveryClient(replicas), configExists);
    }

    private static GraphManager pdManager(Map<String, GraphStatusEntry> status,
                                          DiscoveryClientImpl pdClient,
                                          boolean configExists) {
        GraphManager manager = allocateGraphManager();
        Whitebox.setInternalState(manager, "PDExist", true);
        Whitebox.setInternalState(manager, "cluster", "hg");
        Whitebox.setInternalState(manager, "serviceID", "hugegraph-service");
        Whitebox.setInternalState(manager, "url", SERVER_URL);
        Whitebox.setInternalState(manager, "serverId", SERVER_1);
        Whitebox.setInternalState(manager, "pdClient", pdClient);

        Map<String, GraphSpace> spaces = new ConcurrentHashMap<>();
        spaces.put(GRAPHSPACE, new GraphSpace(GRAPHSPACE));
        Whitebox.setInternalState(manager, "graphSpaces", spaces);

        Map<String, Map<String, Object>> configs = new LinkedHashMap<>();
        if (configExists) {
            configs.put(GRAPHSPACE + "-" + GRAPH, Collections.emptyMap());
        }
        MetaManager metaManager = Mockito.mock(MetaManager.class);
        Mockito.when(metaManager.getGraphStatus(GRAPHSPACE, GRAPH))
               .thenReturn(status);
        Mockito.when(metaManager.graphConfigs(GRAPHSPACE)).thenReturn(configs);
        Mockito.when(metaManager.getGraphConfig(GRAPHSPACE, GRAPH))
               .thenReturn(configExists ? Collections.emptyMap() : null);
        Whitebox.setInternalState(manager, "metaManager", metaManager);

        Whitebox.setInternalState(manager, "graphs",
                                  new ConcurrentHashMap<String, Graph>());
        return manager;
    }

    private static GraphManager standaloneManager(boolean graphOpened) {
        GraphManager manager = allocateGraphManager();
        Whitebox.setInternalState(manager, "PDExist", false);
        Whitebox.setInternalState(manager, "url", SERVER_URL);
        Whitebox.setInternalState(manager, "serverId", SERVER_1);

        Map<String, Graph> graphs = new ConcurrentHashMap<>();
        if (graphOpened) {
            graphs.put(GRAPHSPACE + "-" + GRAPH,
                       Mockito.mock(HugeGraph.class));
        }
        Whitebox.setInternalState(manager, "graphs", graphs);
        return manager;
    }

    private static DiscoveryClientImpl discoveryClient(int replicas) {
        NodeInfos.Builder infos = NodeInfos.newBuilder();
        for (int i = 0; i < replicas; i++) {
            infos.addInfo(NodeInfo.newBuilder()
                                  .setAddress("127.0.0.1:" + (8080 + i))
                                  .build());
        }
        return discoveryClient(infos.build());
    }

    private static DiscoveryClientImpl discoveryClient(NodeInfos infos) {
        DiscoveryClientImpl client = Mockito.mock(DiscoveryClientImpl.class);
        Mockito.when(client.getNodeInfos(Mockito.any(Query.class)))
               .thenReturn(infos);
        return client;
    }

    private static NodeInfo node(String address, String server,
                                 String graphSpace) {
        NodeInfo.Builder node = NodeInfo.newBuilder().setAddress(address);
        if (server != null) {
            node.putLabels(SERVER_ID_LABEL, server);
        }
        if (graphSpace != null) {
            node.putLabels(GRAPHSPACE_LABEL, graphSpace);
        }
        return node.build();
    }

    private static GraphManager allocateGraphManager() {
        try {
            Field field = Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            Unsafe unsafe = (Unsafe) field.get(null);
            return (GraphManager) unsafe.allocateInstance(GraphManager.class);
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }
}
