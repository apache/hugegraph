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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.config.CoreOptions;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.config.ServerOptions;
import org.apache.hugegraph.core.GraphManager;
import org.apache.hugegraph.dist.RegisterUtil;
import org.apache.hugegraph.event.EventHub;
import org.apache.hugegraph.meta.GraphStatusEntry;
import org.apache.hugegraph.meta.MetaManager;
import org.apache.hugegraph.space.GraphSpace;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.testutil.Whitebox;
import org.apache.hugegraph.type.define.GraphStatus;
import org.apache.hugegraph.util.Events;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;

/**
 * The status a server reports for a graph it opens: a graph that starts to
 * open is loading, a graph that reached the gremlin bindings is ready, and a
 * graph that failed anywhere on the way is failed.
 */
public class GraphManagerStatusTest {

    private static final String GRAPH_SPACE = "DEFAULT";
    private static final String GRAPH = "status_graph";
    private static final String SPACE_GRAPH = GRAPH_SPACE + "-" + GRAPH;
    private static final String SERVER = "server-1";

    @BeforeClass
    public static void setup() {
        RegisterUtil.registerBackends();
        reviveEventExecutor();
    }

    /**
     * The executor of EventHub is a static field shared by the whole suite and
     * an earlier test class shuts it down when it closes the factory. The pool
     * is built with a caller runs policy, which silently drops the task it is
     * handed once it is shut down, so the future of an event is never done and
     * every wait on it blocks forever. A new EventHub keeps the dead pool, its
     * init returns early while the field is set, so hand the field a live pool
     * before a graph is opened. The threads are daemons, they never hold the
     * jvm of the suite up
     */
    private static void reviveEventExecutor() {
        ExecutorService executor =
                Whitebox.getInternalState(EventHub.class, "executor");
        if (executor != null && !executor.isShutdown()) {
            return;
        }
        ExecutorService revived = Executors.newCachedThreadPool(task -> {
            Thread thread = new Thread(task, "status-test-event-worker");
            thread.setDaemon(true);
            return thread;
        });
        Whitebox.setInternalState(EventHub.class, "executor", revived);
    }

    @Test
    public void testServerIdIsConfiguredIdWhenSet() {
        PropertiesConfiguration properties = new PropertiesConfiguration();
        properties.setProperty(ServerOptions.SERVER_ID.name(), "server/one 1");
        GraphManager manager = newManager(properties);

        try {
            // The status key is joined by '/', it can't occur in a server id
            Assert.assertEquals("server_one_1", manager.serverId());
        } finally {
            manager.close();
        }
    }

    @Test
    public void testServerIdIsStableAndTellsColocatedServersApart() {
        /*
         * The rest server url is a bind address rather than an identity, the
         * replicas of a container image all carry the same one. The id has to
         * survive a restart of a server and separate the servers of a host
         */
        GraphManager first = newManager(urlProperties("http://0.0.0.0:8080"));
        GraphManager restarted =
                newManager(urlProperties("http://0.0.0.0:8080"));
        GraphManager other = newManager(urlProperties("http://0.0.0.0:8081"));

        try {
            Assert.assertEquals(first.serverId(), restarted.serverId());
            Assert.assertNotEquals(first.serverId(), other.serverId());
            Assert.assertFalse(first.serverId().contains("/"));
        } finally {
            first.close();
            restarted.close();
            other.close();
        }
    }

    @Test
    public void testCreateReportsLoadingThenReadyOnBound() {
        List<GraphStatusEntry> reported = new ArrayList<>();
        GraphManager manager = newManager(reported, null);
        try {
            HugeGraph graph = manager.createGraph(GRAPH_SPACE, GRAPH, "admin",
                                                  graphConfig(), false);
            Assert.assertNotNull(graph);

            // The graph is open but not bound to the gremlin server yet
            Assert.assertEquals(1, reported.size());
            assertEntry(reported.get(0), GraphStatus.LOADING);

            notifyBound(manager, graph);

            Assert.assertEquals(2, reported.size());
            assertEntry(reported.get(1), GraphStatus.READY);
            Assert.assertNull(reported.get(1).message());
        } finally {
            close(manager);
        }
    }

    @Test
    public void testBoundIsReportedOnceWhenListenersAreRegisteredTwice() {
        /*
         * The listeners are registered by the constructor and again by init,
         * a graph that is bound once must not be reported twice
         */
        List<GraphStatusEntry> reported = new ArrayList<>();
        GraphManager manager = newManager(reported, null);
        try {
            HugeGraph graph = manager.createGraph(GRAPH_SPACE, GRAPH, "admin",
                                                  graphConfig(), false);
            Whitebox.invoke(manager.getClass(), "listenChanges", manager);
            reported.clear();

            notifyBound(manager, graph);

            Assert.assertEquals(1, reported.size());
            assertEntry(reported.get(0), GraphStatus.READY);
        } finally {
            close(manager);
        }
    }

    @Test
    public void testFailureAfterTheBackendIsOpenReportsFailed() {
        /*
         * The backend of the graph opens, the meta update that comes after it
         * doesn't. The entry has to end up failed rather than stay loading
         * forever, the client already got an error for its request
         */
        List<GraphStatusEntry> reported = new ArrayList<>();
        GraphManager manager = newManager(
                reported, new IllegalStateException("meta is unreachable"));
        try {
            Assert.assertThrows(IllegalStateException.class, () -> {
                manager.createGraph(GRAPH_SPACE, GRAPH, "admin",
                                    graphConfig(), false);
            });

            Assert.assertEquals(2, reported.size());
            assertEntry(reported.get(0), GraphStatus.LOADING);
            assertEntry(reported.get(1), GraphStatus.FAILED);
            // The type of the failure is kept, its message is bounded
            Assert.assertContains("IllegalStateException",
                                  reported.get(1).message());
        } finally {
            close(manager);
        }
    }

    @Test
    public void testFailedBindingReportsFailed() {
        /*
         * The graph create event is notified without waiting for it, so a
         * binding that throws reaches nobody: without a report of its own the
         * graph would stay loading with nothing left to end it
         */
        List<GraphStatusEntry> reported = new ArrayList<>();
        GraphManager manager = newManager(reported, null);
        try {
            HugeGraph graph = manager.createGraph(GRAPH_SPACE, GRAPH, "admin",
                                                  graphConfig(), false);
            reported.clear();

            EventHub hub = Whitebox.getInternalState(manager, "eventHub");
            hub.call(Events.GRAPH_BIND_FAILED, graph);

            Assert.assertEquals(1, reported.size());
            assertEntry(reported.get(0), GraphStatus.FAILED);
            Assert.assertNotNull(reported.get(0).message());
        } finally {
            close(manager);
        }
    }

    @Test
    public void testAGraphThatCantBeCreatedIsNotLeftRegistered() {
        /*
         * A graph that failed after it was registered would be answered for
         * while it reads failed, and this server would never open it again,
         * so the failure would stay for good. It has to be taken back out so
         * that opening it can be tried again
         */
        List<GraphStatusEntry> reported = new ArrayList<>();
        GraphManager manager = newManager(
                reported, new IllegalStateException("meta is unreachable"));
        try {
            Assert.assertThrows(IllegalStateException.class, () -> {
                manager.createGraph(GRAPH_SPACE, GRAPH, "admin",
                                    graphConfig(), false);
            });

            assertEntry(reported.get(reported.size() - 1),
                        GraphStatus.FAILED);
            Assert.assertNull(manager.graph(GRAPH_SPACE + "-" + GRAPH));
        } finally {
            close(manager);
        }
    }

    private static void assertEntry(GraphStatusEntry entry,
                                    GraphStatus status) {
        Assert.assertEquals(status, entry.status());
        Assert.assertEquals(SERVER, entry.server());
        Assert.assertTrue(entry.updateTime() > 0L);
    }

    private static void notifyBound(GraphManager manager, HugeGraph graph) {
        /*
         * call() invokes the sole registered listener in this thread, unlike
         * notify() which hands the event to the executor and answers a future
         * the caller has to wait on. It also asserts that the graph manager
         * holds exactly one listener for the event, it throws when the event
         * has no listener at all and when it has more than one
         */
        EventHub hub = Whitebox.getInternalState(manager, "eventHub");
        hub.call(Events.GRAPH_BOUND, graph);
    }

    private static Map<String, Object> graphConfig() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(CoreOptions.BACKEND.name(), "memory");
        configs.put(CoreOptions.SERIALIZER.name(), "text");
        configs.put(CoreOptions.STORE.name(), GRAPH);
        return configs;
    }

    private static PropertiesConfiguration urlProperties(String url) {
        PropertiesConfiguration properties = new PropertiesConfiguration();
        properties.setProperty(ServerOptions.REST_SERVER_URL.name(), url);
        return properties;
    }

    private static GraphManager newManager(PropertiesConfiguration props) {
        return new GraphManager(new HugeConfig(props),
                                new EventHub("status-test"));
    }

    /**
     * @param reported      collects every status this server writes
     * @param updateFailure thrown by the meta update that follows the open of
     *                      the backend, null to let the creation succeed
     */
    private static GraphManager newManager(List<GraphStatusEntry> reported,
                                           RuntimeException updateFailure) {
        GraphManager manager = newManager(new PropertiesConfiguration());
        Whitebox.setInternalState(manager, "PDExist", true);
        Whitebox.setInternalState(manager, "serverId", SERVER);

        Map<String, GraphSpace> spaces = new ConcurrentHashMap<>();
        spaces.put(GRAPH_SPACE, new GraphSpace(GRAPH_SPACE));
        Whitebox.setInternalState(manager, "graphSpaces", spaces);

        MetaManager metaManager = Mockito.mock(MetaManager.class);
        Mockito.when(metaManager.graphSpace(GRAPH_SPACE))
               .thenReturn(new GraphSpace(GRAPH_SPACE));
        Mockito.doAnswer((InvocationOnMock invocation) -> {
            reported.add(invocation.getArgument(2));
            return null;
        }).when(metaManager).updateGraphStatus(Mockito.anyString(),
                                               Mockito.anyString(),
                                               Mockito.any());
        if (updateFailure != null) {
            Mockito.doThrow(updateFailure).when(metaManager)
                   .updateGraphSpaceConfig(Mockito.anyString(),
                                           Mockito.any());
        }
        Whitebox.setInternalState(manager, "metaManager", metaManager);
        return manager;
    }

    private static void close(GraphManager manager) {
        try {
            HugeGraph graph = manager.graph(SPACE_GRAPH);
            if (graph != null) {
                graph.clearBackend();
                graph.close();
            }
        } catch (Exception ignored) {
            // The graph may have failed to open, nothing to close then
        }
        manager.close();
    }
}
