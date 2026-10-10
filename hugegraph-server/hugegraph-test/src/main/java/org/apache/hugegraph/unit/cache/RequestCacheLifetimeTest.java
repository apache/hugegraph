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

package org.apache.hugegraph.unit.cache;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Collections;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.hugegraph.HugeFactory;
import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.HugeGraphParams;
import org.apache.hugegraph.api.graph.VertexAPI;
import org.apache.hugegraph.api.graph.EdgeAPI;
import org.apache.hugegraph.core.GraphManager;
import org.apache.hugegraph.util.JsonUtil;
import org.apache.hugegraph.backend.cache.CachedGraphTransaction;
import org.apache.hugegraph.backend.cache.CachedSchemaTransaction;
import org.apache.hugegraph.util.Events;
import org.apache.hugegraph.dist.RegisterUtil;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.testutil.Whitebox;
import org.apache.hugegraph.unit.FakeObjects;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import sun.misc.Unsafe;

public class RequestCacheLifetimeTest {

    @Test
    public void testVertexSchemaAppendAfterRequestCleanupWithWarmElementCaches() throws Exception {
        this.checkSchemaAppendAfterRequestCleanup(false);
    }

    @Test
    public void testEdgeSchemaAppendAfterRequestCleanupWithWarmElementCaches() throws Exception {
        this.checkSchemaAppendAfterRequestCleanup(true);
    }

    @Test
    public void testFailedReopenPreservesCauseAndDeletesDirectory() throws Exception {
        Path path = Files.createTempDirectory("request-cache-reopen-failure");
        RuntimeException failure = null;
        try {
            this.checkSchemaAppendAfterRequestCleanup(false, path, true);
            Assert.fail("The invalid backend must fail during reopen");
        } catch (RuntimeException expected) {
            failure = expected;
        }
        System.out.println("REOPEN_FAILURE=" + failure + "; DIRECTORY_EXISTS=" + Files.exists(path));
        Assert.assertNotNull(failure);
        Assert.assertTrue(failure instanceof org.apache.hugegraph.exception.HugeException);
        Assert.assertEquals("Failed to load backend store provider", failure.getMessage());
        Assert.assertFalse(Files.exists(path));
    }

    private void checkSchemaAppendAfterRequestCleanup(boolean edgeFirst) throws Exception {
        Path path = Files.createTempDirectory("request-cache-lifetime");
        this.checkSchemaAppendAfterRequestCleanup(edgeFirst, path, false);
    }

    private void checkSchemaAppendAfterRequestCleanup(boolean edgeFirst, Path path,
                                                     boolean failReopen) throws Exception {
        RegisterUtil.registerBackends();
        HugeConfig config = FakeObjects.newConfig();
        config.setProperty("backend", "rocksdb");
        config.setProperty("serializer", "binary");
        config.setProperty("store", edgeFirst ? "request_cache_lifetime_edge" : "request_cache_lifetime_vertex");
        config.setProperty("rocksdb.data_path", path.toString());
        config.setProperty("rocksdb.wal_path", path.toString());
        HugeGraph graph = HugeFactory.open(config);
        try {
            graph.initBackend();
            graph.schema().vertexLabel("person").useCustomizeNumberId().create();
            graph.schema().edgeLabel("knows").sourceLabel("person").targetLabel("person").create();
            Vertex first = graph.addVertex(T.label, "person", T.id, 1L);
            Vertex second = graph.addVertex(T.label, "person", T.id, 2L);
            Edge edge = first.addEdge("knows", second);
            Object edgeId = edge.id();
            graph.tx().commit();
            Vertex warm = graph.vertices(1L).next();
            warm.edges(Direction.OUT, "knows").next();
            HugeGraphParams params = Whitebox.getInternalState(graph, "params");
            CachedGraphTransaction tx = (CachedGraphTransaction) params.graphTransaction();
            Assert.assertTrue((long) Whitebox.invoke(tx, "verticesCache", "size") > 0L);
            Assert.assertTrue((long) Whitebox.invoke(tx, "edgesCache", "size") > 0L);
            HugeFactory.closeCurrentThreadTransactions();

            graph.schema().propertyKey("added").asText().create();
            graph.schema().vertexLabel("person").properties("added").nullableKeys("added").append();
            graph.schema().edgeLabel("knows").properties("added").nullableKeys("added").append();
            HugeFactory.closeCurrentThreadTransactions();

            Vertex refetched = graph.vertices(1L).next();
            Edge refetchedEdge = refetched.edges(Direction.OUT, "knows").next();
            if (edgeFirst) {
                refetchedEdge.property("added", "edge-value");
                refetched.property("added", "vertex-value");
            } else {
                refetched.property("added", "vertex-value");
                refetchedEdge.property("added", "edge-value");
            }
            graph.tx().commit();
            HugeFactory.closeCurrentThreadTransactions();
            Assert.assertEquals("vertex-value", graph.vertices(1L).next().value("added"));
            Assert.assertEquals("edge-value", graph.edges(edgeId).next().value("added"));
            HugeFactory.closeCurrentThreadTransactions();
            updateThroughApi(graph, VertexAPI.class, "JsonVertex", "1", "vertex-api-value");
            HugeFactory.closeCurrentThreadTransactions();
            updateThroughApi(graph, EdgeAPI.class, "JsonEdge", edgeId.toString(), "edge-api-value");
            HugeFactory.closeCurrentThreadTransactions();
            Assert.assertEquals("vertex-api-value", graph.vertices(1L).next().value("added"));
            Assert.assertEquals("edge-api-value", graph.edges(edgeId).next().value("added"));
            CachedGraphTransaction idleGraph = (CachedGraphTransaction) params.graphTransaction();
            CachedSchemaTransaction idleSchema = (CachedSchemaTransaction) params.schemaTransaction();
            HugeFactory.closeCurrentThreadTransactions();
            Assert.assertTrue((long) Whitebox.invoke(idleGraph, "verticesCache", "size") > 0L);
            Assert.assertTrue((long) Whitebox.invoke(idleSchema, "idCache", "size") > 0L);
            params.loadGraphStore().provider().storeEventHub()
                  .notify(Events.STORE_CLEAR, params.loadGraphStore().provider()).get();
            Assert.assertEquals(0L, Whitebox.invoke(idleGraph, "verticesCache", "size"));
            Assert.assertEquals(0L, Whitebox.invoke(idleGraph, "edgesCache", "size"));
            Assert.assertEquals(0L, Whitebox.invoke(idleSchema, "idCache", "size"));
            graph.close();
            graph = null;
            if (failReopen) {
                config.setProperty("backend", "invalid_reopen_backend");
            }
            graph = HugeFactory.open(config);
            Assert.assertEquals("vertex-api-value", graph.vertices(1L).next().value("added"));
            Assert.assertEquals("edge-api-value", graph.edges(edgeId).next().value("added"));
        } finally {
            try {
                HugeFactory.closeCurrentThreadTransactions();
                if (graph != null) {
                    try {
                        graph.clearBackend();
                    } finally {
                        graph.close();
                    }
                }
            } finally {
                try (java.util.stream.Stream<Path> files = Files.walk(path)) {
                    for (Path file : files.sorted(java.util.Comparator.reverseOrder()).toArray(Path[]::new)) {
                        Files.deleteIfExists(file);
                    }
                }
            }
        }
    }

    private static void updateThroughApi(HugeGraph graph, Class<?> api, String bodyClass,
                                         String id, String value) throws Exception {
        // Match existing API fixtures: avoid constructor-started RPC/PD services.
        Field field = Unsafe.class.getDeclaredField("theUnsafe");
        field.setAccessible(true);
        GraphManager manager = (GraphManager) ((Unsafe) field.get(null)).allocateInstance(GraphManager.class);
        Whitebox.setInternalState(manager, "graphs", Collections.singletonMap(graph.spaceGraphName(), graph));
        Class<?> bodyType = Class.forName(api.getName() + "$" + bodyClass);
        Object body = JsonUtil.fromJson("{\"properties\":{\"added\":\"" + value + "\"}}", bodyType);
        Method update = api.getMethod("update", GraphManager.class, String.class, String.class,
                                      String.class, String.class, bodyType);
        String response = (String) update.invoke(api.getConstructor().newInstance(), manager,
                                                "DEFAULT", graph.name(), id, "append", body);
        Assert.assertTrue(response.contains(value));
    }
}
