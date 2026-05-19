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
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hugegraph.HugeFactory;
import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.HugeGraphParams;
import org.apache.hugegraph.backend.cache.Cache;
import org.apache.hugegraph.backend.cache.CachedGraphTransaction;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.id.IdGenerator;
import org.apache.hugegraph.backend.tx.GraphTransaction;
import org.apache.hugegraph.event.EventListener;
import org.apache.hugegraph.schema.VertexLabel;
import org.apache.hugegraph.structure.HugeEdge;
import org.apache.hugegraph.structure.HugeVertex;
import org.apache.hugegraph.structure.HugeVertexProperty;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.testutil.Whitebox;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.type.define.IdStrategy;
import org.apache.hugegraph.unit.BaseUnitTest;
import org.apache.hugegraph.unit.FakeObjects;
import org.apache.hugegraph.util.Events;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class CachedGraphTransactionTest extends BaseUnitTest {

    private CachedGraphTransaction cache;
    private HugeGraphParams params;
    private HugeGraph graph;

    @Before
    public void setup() {
        this.graph = HugeFactory.open(FakeObjects.newConfig());
        this.params = Whitebox.getInternalState(this.graph, "params");
        this.cache = new CachedGraphTransaction(this.params,
                                                this.params.loadGraphStore());
    }

    @After
    public void teardown() throws Exception {
        try {
            if (this.cache != null) {
                this.cache.close();
            }
        } finally {
            this.cache = null;
            if (this.graph != null) {
                this.graph.clearBackend();
                this.graph.close();
                this.graph = null;
            }
        }
    }

    private CachedGraphTransaction cache() {
        Assert.assertNotNull(this.cache);
        return this.cache;
    }

    @SuppressWarnings("unchecked")
    private static ConcurrentMap<String, Object> graphCacheEventListeners()
            throws Exception {
        Field field = CachedGraphTransaction.class
                                            .getDeclaredField(
                                                    "GRAPH_CACHE_EVENT_LISTENERS");
        field.setAccessible(true);
        return (ConcurrentMap<String, Object>) field.get(null);
    }

    @SuppressWarnings("unchecked")
    private static ConcurrentMap<String, Boolean> storeEventListenStatus()
            throws Exception {
        Field field = GraphTransaction.class
                                      .getDeclaredField("storeEventListenStatus");
        field.setAccessible(true);
        return (ConcurrentMap<String, Boolean>) field.get(null);
    }

    private static void restoreStoreListenerStatusForKnownTeardownBug(
            ConcurrentMap<String, Boolean> storeListeners, String graphName) {
        // Closing a secondary transaction can consume storeEventListenStatus due
        // to the follow-up bug documented in CachedGraphTransaction.unlistenChanges().
        // Restore it so teardown can still unregister the primary store listener.
        storeListeners.putIfAbsent(graphName, true);
    }

    private static EventListener holderListener(Object holder) {
        return Whitebox.getInternalState(holder, "listener");
    }

    private static int holderRefCount(Object holder) {
        Integer refCount = Whitebox.getInternalState(holder, "refCount");
        return refCount;
    }

    private HugeVertex newVertex(Id id) {
        HugeGraph graph = this.cache().graph();
        graph.schema().propertyKey("name").asText()
             .checkExist(false).create();
        graph.schema().vertexLabel("person")
             .idStrategy(IdStrategy.CUSTOMIZE_NUMBER)
             .properties("name").nullableKeys("name")
             .checkExist(false)
             .create();
        VertexLabel vl = graph.vertexLabel("person");
        return new HugeVertex(graph, id, vl);
    }

    private HugeEdge newEdge(HugeVertex out, HugeVertex in) {
        HugeGraph graph = this.cache().graph();
        graph.schema().edgeLabel("person_know_person")
             .sourceLabel("person")
             .targetLabel("person")
             .checkExist(false)
             .create();
        return out.addEdge("person_know_person", in);
    }

    @Test
    public void testEventClear() throws Exception {
        CachedGraphTransaction cache = this.cache();

        cache.addVertex(this.newVertex(IdGenerator.of(1)));
        cache.addVertex(this.newVertex(IdGenerator.of(2)));
        cache.commit();

        Assert.assertTrue(cache.queryVertices(IdGenerator.of(1)).hasNext());
        Assert.assertTrue(cache.queryVertices(IdGenerator.of(2)).hasNext());
        Assert.assertEquals(2L,
                            Whitebox.invoke(cache, "verticesCache", "size"));

        this.params.graphEventHub().notify(Events.CACHE, "clear", null).get();

        Assert.assertEquals(0L,
                            Whitebox.invoke(cache, "verticesCache", "size"));

        Assert.assertTrue(cache.queryVertices(IdGenerator.of(1)).hasNext());
        Assert.assertEquals(1L,
                            Whitebox.invoke(cache, "verticesCache", "size"));
        Assert.assertTrue(cache.queryVertices(IdGenerator.of(2)).hasNext());
        Assert.assertEquals(2L,
                            Whitebox.invoke(cache, "verticesCache", "size"));
    }

    @Test
    public void testEventInvalid() throws Exception {
        CachedGraphTransaction cache = this.cache();

        cache.addVertex(this.newVertex(IdGenerator.of(1)));
        cache.addVertex(this.newVertex(IdGenerator.of(2)));
        cache.commit();

        Assert.assertTrue(cache.queryVertices(IdGenerator.of(1)).hasNext());
        Assert.assertTrue(cache.queryVertices(IdGenerator.of(2)).hasNext());
        Assert.assertEquals(2L,
                            Whitebox.invoke(cache, "verticesCache", "size"));

        this.params.graphEventHub().notify(Events.CACHE, "invalid",
                                           HugeType.VERTEX, IdGenerator.of(1))
                   .get();

        Assert.assertEquals(1L,
                            Whitebox.invoke(cache, "verticesCache", "size"));
        Assert.assertTrue(cache.queryVertices(IdGenerator.of(2)).hasNext());
        Assert.assertEquals(1L,
                            Whitebox.invoke(cache, "verticesCache", "size"));
        Assert.assertTrue(cache.queryVertices(IdGenerator.of(1)).hasNext());
        Assert.assertEquals(2L,
                            Whitebox.invoke(cache, "verticesCache", "size"));
    }

    @Test
    public void testClearCacheEmitsActionClear() throws Exception {
        // Producers must emit the present-tense ACTION_CLEAR / ACTION_INVALID,
        // not the legacy past-tense variants - otherwise local listeners that
        // match only the present-tense actions silently drop the event.
        CachedGraphTransaction cache = this.cache();
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<String> action = new AtomicReference<>();
        EventListener listener = event -> {
            Object[] args = event.args();
            if (args.length > 0 && args[0] instanceof String) {
                action.set((String) args[0]);
                latch.countDown();
            }
            return true;
        };
        this.params.graphEventHub().listen(Events.CACHE, listener);
        try {
            cache.clearCache(HugeType.VERTEX, true);

            Assert.assertTrue(latch.await(1L, TimeUnit.SECONDS));
            Assert.assertEquals(Cache.ACTION_CLEAR, action.get());
        } finally {
            this.params.graphEventHub().unlisten(Events.CACHE, listener);
        }
    }

    @Test
    public void testVertexMutationEmitsActionInvalid() throws Exception {
        CachedGraphTransaction cache = this.cache();
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<String> action = new AtomicReference<>();
        EventListener listener = event -> {
            Object[] args = event.args();
            if (args.length > 0 && Cache.ACTION_INVALID.equals(args[0])) {
                action.set((String) args[0]);
                latch.countDown();
            }
            return true;
        };
        this.params.graphEventHub().listen(Events.CACHE, listener);
        try {
            cache.addVertex(this.newVertex(IdGenerator.of(1)));
            cache.commit();

            Assert.assertTrue(latch.await(1L, TimeUnit.SECONDS));
            Assert.assertEquals(Cache.ACTION_INVALID, action.get());
        } finally {
            this.params.graphEventHub().unlisten(Events.CACHE, listener);
        }
    }

    @Test
    public void testClosingNonOwnerKeepsGraphCacheListenerRegistered()
            throws Exception {
        ConcurrentMap<String, Object> cacheListeners =
                graphCacheEventListeners();
        ConcurrentMap<String, Boolean> storeListeners =
                storeEventListenStatus();

        String graphName = this.params.spaceGraphName();
        Object holder = cacheListeners.get(graphName);
        Assert.assertNotNull(holder);
        EventListener registered = holderListener(holder);
        int refCount = holderRefCount(holder);

        CachedGraphTransaction second = new CachedGraphTransaction(
                this.params, this.params.loadGraphStore());
        Assert.assertSame(holder, cacheListeners.get(graphName));
        Assert.assertEquals(refCount + 1, holderRefCount(holder));

        try {
            second.close();

            Assert.assertSame(holder, cacheListeners.get(graphName));
            Assert.assertEquals(refCount, holderRefCount(holder));
            Assert.assertTrue(this.params.graphEventHub()
                                         .listeners(Events.CACHE)
                                         .contains(registered));
        } finally {
            restoreStoreListenerStatusForKnownTeardownBug(storeListeners,
                                                           graphName);
        }
    }

    @Test
    public void testCacheListenerSurvivesOwnerClose() throws Exception {
        ConcurrentMap<String, Object> cacheListeners =
                graphCacheEventListeners();
        String graphName = this.params.spaceGraphName();
        CachedGraphTransaction owner = this.cache();
        CachedGraphTransaction second = new CachedGraphTransaction(
                this.params, this.params.loadGraphStore());

        Object holder = cacheListeners.get(graphName);
        Assert.assertNotNull(holder);
        EventListener registered = holderListener(holder);
        int refCount = holderRefCount(holder);
        Assert.assertTrue(refCount >= 2);

        owner.close();
        this.cache = second;

        Assert.assertSame(holder, cacheListeners.get(graphName));
        Assert.assertEquals(refCount - 1, holderRefCount(holder));
        Assert.assertTrue(this.params.graphEventHub()
                                     .listeners(Events.CACHE)
                                     .contains(registered));

        second.addVertex(this.newVertex(IdGenerator.of(1)));
        second.addVertex(this.newVertex(IdGenerator.of(2)));
        second.commit();
        Assert.assertTrue(second.queryVertices(IdGenerator.of(1)).hasNext());
        Assert.assertTrue(second.queryVertices(IdGenerator.of(2)).hasNext());
        Assert.assertEquals(2L,
                            Whitebox.invoke(second, "verticesCache", "size"));

        this.params.graphEventHub().notify(Events.CACHE, Cache.ACTION_INVALID,
                                           HugeType.VERTEX, IdGenerator.of(1))
                   .get();

        Assert.assertEquals(1L,
                            Whitebox.invoke(second, "verticesCache", "size"));
    }

    @Test
    public void testLastCloseRemovesGraphCacheListener() throws Exception {
        ConcurrentMap<String, Object> cacheListeners =
                graphCacheEventListeners();
        String graphName = this.params.spaceGraphName();
        CachedGraphTransaction owner = this.cache();
        CachedGraphTransaction second = new CachedGraphTransaction(
                this.params, this.params.loadGraphStore());

        Object holder = cacheListeners.get(graphName);
        Assert.assertNotNull(holder);
        EventListener registered = holderListener(holder);
        Assert.assertTrue(holderRefCount(holder) >= 2);

        owner.close();
        second.close();
        this.cache = null;
        this.params.graphTransaction().close();

        Assert.assertFalse(cacheListeners.containsKey(graphName));
        Assert.assertFalse(this.params.graphEventHub()
                                      .listeners(Events.CACHE)
                                      .contains(registered));

        this.graph.clearBackend();
        this.graph.close();
        this.graph = null;

        HugeGraph reopened = HugeFactory.open(FakeObjects.newConfig());
        this.graph = reopened;
        this.params = Whitebox.getInternalState(reopened, "params");
        Object reopenedHolder = cacheListeners.get(graphName);
        Assert.assertNotNull(reopenedHolder);
        Assert.assertNotSame(holder, reopenedHolder);
        int reopenedRefCount = holderRefCount(reopenedHolder);
        CachedGraphTransaction third = new CachedGraphTransaction(
                this.params, this.params.loadGraphStore());
        this.cache = third;
        Object newHolder = cacheListeners.get(graphName);
        Assert.assertSame(reopenedHolder, newHolder);
        Assert.assertEquals(reopenedRefCount + 1, holderRefCount(newHolder));
    }

    @Test
    public void testEdgeCacheClearWhenDeleteVertex() {
        CachedGraphTransaction cache = this.cache();
        HugeVertex v1 = this.newVertex(IdGenerator.of(1));
        HugeVertex v2 = this.newVertex(IdGenerator.of(2));
        HugeVertex v3 = this.newVertex(IdGenerator.of(3));

        cache.addVertex(v1);
        cache.addVertex(v2);
        cache.commit();
        HugeEdge edge = this.newEdge(v1, v2);
        cache.addEdge(edge);
        cache.commit();
        Assert.assertTrue(cache.queryEdgesByVertex(IdGenerator.of(1)).hasNext());
        Assert.assertTrue(cache.queryEdgesByVertex(IdGenerator.of(2)).hasNext());

        Assert.assertEquals(2L,
                            Whitebox.invoke(cache, "edgesCache", "size"));
        cache.removeVertex(v3);
        cache.commit();
        Assert.assertEquals(0L,
                            Whitebox.invoke(cache, "edgesCache", "size"));

        Assert.assertTrue(cache.queryEdgesByVertex(IdGenerator.of(1)).hasNext());
        Assert.assertTrue(cache.queryEdgesByVertex(IdGenerator.of(2)).hasNext());
        Assert.assertEquals(2L,
                            Whitebox.invoke(cache, "edgesCache", "size"));

        cache.removeVertex(v1);
        cache.commit();

        Assert.assertEquals(0L,
                            Whitebox.invoke(cache, "edgesCache", "size"));
        Assert.assertFalse(cache.queryEdgesByVertex(IdGenerator.of(2)).hasNext());
    }

    @Test
    public void testEdgeCacheClearWhenUpdateVertex() {
        CachedGraphTransaction cache = this.cache();
        HugeVertex v1 = this.newVertex(IdGenerator.of(1));
        HugeVertex v2 = this.newVertex(IdGenerator.of(2));
        HugeVertex v3 = this.newVertex(IdGenerator.of(3));

        cache.addVertex(v1);
        cache.addVertex(v2);
        cache.commit();
        HugeEdge edge = this.newEdge(v1, v2);
        cache.addEdge(edge);
        cache.commit();
        Assert.assertTrue(cache.queryEdgesByVertex(IdGenerator.of(1)).hasNext());
        Assert.assertTrue(cache.queryEdgesByVertex(IdGenerator.of(2)).hasNext());

        Assert.assertEquals(2L,
                            Whitebox.invoke(cache, "edgesCache", "size"));

        cache.addVertexProperty(new HugeVertexProperty<>(v3,
                                                         cache.graph().schema()
                                                              .getPropertyKey("name"),
                                                         "test-name"));
        cache.commit();
        Assert.assertEquals(0L,
                            Whitebox.invoke(cache, "edgesCache", "size"));

        Assert.assertTrue(cache.queryEdgesByVertex(IdGenerator.of(1)).hasNext());
        Assert.assertTrue(cache.queryEdgesByVertex(IdGenerator.of(2)).hasNext());
        Assert.assertEquals(2L,
                            Whitebox.invoke(cache, "edgesCache", "size"));

        cache.addVertexProperty(new HugeVertexProperty<>(v1,
                                                         cache.graph().schema()
                                                              .getPropertyKey("name"),
                                                         "test-name"));
        cache.commit();

        Assert.assertEquals(0L,
                            Whitebox.invoke(cache, "edgesCache", "size"));
        String name = cache.queryEdgesByVertex(IdGenerator.of(1)).next().outVertex()
                           .value("name");
        Assert.assertEquals("test-name", name);
    }
}
