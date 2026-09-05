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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hugegraph.HugeFactory;
import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.HugeGraphParams;
import org.apache.hugegraph.backend.cache.Cache;
import org.apache.hugegraph.backend.cache.CachedGraphTransaction;
import org.apache.hugegraph.backend.cache.OffheapCache;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.id.IdGenerator;
import org.apache.hugegraph.backend.query.Condition;
import org.apache.hugegraph.backend.query.ConditionQuery.OptimizedType;
import org.apache.hugegraph.backend.query.ConditionQuery;
import org.apache.hugegraph.backend.query.IdQuery;
import org.apache.hugegraph.backend.query.Query;
import org.apache.hugegraph.backend.query.QueryResultContext;
import org.apache.hugegraph.backend.query.QueryResults;
import org.apache.hugegraph.backend.store.BackendStore;
import org.apache.hugegraph.backend.store.BackendStoreProvider;
import org.apache.hugegraph.backend.store.ram.RamTable;
import org.apache.hugegraph.event.EventHub;
import org.apache.hugegraph.event.EventListener;
import org.apache.hugegraph.schema.VertexLabel;
import org.apache.hugegraph.structure.HugeEdge;
import org.apache.hugegraph.structure.HugeElement;
import org.apache.hugegraph.structure.HugeVertex;
import org.apache.hugegraph.structure.HugeVertexProperty;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.testutil.Whitebox;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.type.define.Directions;
import org.apache.hugegraph.type.define.HugeKeys;
import org.apache.hugegraph.type.define.IdStrategy;
import org.apache.hugegraph.type.define.SchemaStatus;
import org.apache.hugegraph.unit.BaseUnitTest;
import org.apache.hugegraph.unit.FakeObjects;
import org.apache.hugegraph.util.Events;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.config.Property;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

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
    private static ConcurrentMap<String, Object> storeEventListeners()
            throws Exception {
        Field field = CachedGraphTransaction.class
                                            .getDeclaredField(
                                                    "STORE_EVENT_LISTENERS");
        field.setAccessible(true);
        return (ConcurrentMap<String, Object>) field.get(null);
    }

    private static EventListener holderListener(Object holder) {
        return Whitebox.getInternalState(holder, "listener");
    }

    private static int holderRefCount(Object holder) {
        Integer refCount = Whitebox.getInternalState(holder, "refCount");
        return refCount;
    }

    private static BackendStoreProvider holderProvider(Object holder) {
        return Whitebox.getInternalState(holder, "provider");
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

        String graphName = this.params.spaceGraphName();
        Object holder = cacheListeners.get(graphName);
        Assert.assertNotNull(holder);
        EventListener registered = holderListener(holder);
        int refCount = holderRefCount(holder);

        CachedGraphTransaction second = new CachedGraphTransaction(
                this.params, this.params.loadGraphStore());
        Assert.assertSame(holder, cacheListeners.get(graphName));
        Assert.assertEquals(refCount + 1, holderRefCount(holder));

        second.close();

        Assert.assertSame(holder, cacheListeners.get(graphName));
        Assert.assertEquals(refCount, holderRefCount(holder));
        Assert.assertTrue(this.params.graphEventHub()
                                     .listeners(Events.CACHE)
                                     .contains(registered));
    }

    @Test
    public void testClosingNonOwnerKeepsStoreListenerRegistered()
            throws Exception {
        ConcurrentMap<String, Object> storeListeners = storeEventListeners();

        String graphName = this.params.spaceGraphName();
        Object holder = storeListeners.get(graphName);
        Assert.assertNotNull(holder);
        EventListener registered = holderListener(holder);
        int refCount = holderRefCount(holder);

        CachedGraphTransaction second = new CachedGraphTransaction(
                this.params, this.params.loadGraphStore());
        Assert.assertSame(holder, storeListeners.get(graphName));
        Assert.assertEquals(refCount + 1, holderRefCount(holder));

        second.close();

        // Non-owner close must decrement the refcount, not drop the entry
        Assert.assertSame(holder, storeListeners.get(graphName));
        Assert.assertEquals(refCount, holderRefCount(holder));
        Assert.assertSame(registered, holderListener(holder));
    }

    @Test
    public void testLastCloseRemovesStoreListener() throws Exception {
        ConcurrentMap<String, Object> storeListeners = storeEventListeners();

        String graphName = this.params.spaceGraphName();
        CachedGraphTransaction owner = this.cache();
        CachedGraphTransaction second = new CachedGraphTransaction(
                this.params, this.params.loadGraphStore());

        Object holder = storeListeners.get(graphName);
        Assert.assertNotNull(holder);
        Assert.assertTrue(holderRefCount(holder) >= 2);

        owner.close();
        second.close();
        this.cache = null;
        this.params.graphTransaction().close();

        Assert.assertFalse(storeListeners.containsKey(graphName));
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
    public void testStoreListenerSurvivesOwnerClose() throws Exception {
        ConcurrentMap<String, Object> storeListeners = storeEventListeners();
        String graphName = this.params.spaceGraphName();
        CachedGraphTransaction owner = this.cache();
        CachedGraphTransaction second = new CachedGraphTransaction(
                this.params, this.params.loadGraphStore());

        Object holder = storeListeners.get(graphName);
        Assert.assertNotNull(holder);
        EventListener registered = holderListener(holder);
        BackendStoreProvider provider = holderProvider(holder);
        int refCount = holderRefCount(holder);
        Assert.assertTrue(refCount >= 2);

        owner.close();
        this.cache = second;

        Assert.assertSame(holder, storeListeners.get(graphName));
        Assert.assertEquals(refCount - 1, holderRefCount(holder));
        Assert.assertTrue(provider.storeEventHub()
                                  .listeners(EventHub.ANY_EVENT)
                                  .contains(registered));

        second.addVertex(this.newVertex(IdGenerator.of(1)));
        second.addVertex(this.newVertex(IdGenerator.of(2)));
        second.commit();
        Assert.assertTrue(second.queryVertices(IdGenerator.of(1)).hasNext());
        Assert.assertTrue(second.queryVertices(IdGenerator.of(2)).hasNext());
        Assert.assertEquals(2L,
                            Whitebox.invoke(second, "verticesCache", "size"));

        // Owner is closed first; the surviving transaction must still observe
        // the store event through the ref-counted provider listener and clear
        // its cache.
        provider.storeEventHub().notify(Events.STORE_CLEAR, provider).get();

        Assert.assertEquals(0L,
                            Whitebox.invoke(second, "verticesCache", "size"));
    }

    @Test
    public void testReopenGraphReRegistersStoreListener() throws Exception {
        ConcurrentMap<String, Object> storeListeners = storeEventListeners();
        String graphName = this.params.spaceGraphName();
        CachedGraphTransaction owner = this.cache();

        Object holder = storeListeners.get(graphName);
        Assert.assertNotNull(holder);
        EventListener registered = holderListener(holder);
        BackendStoreProvider provider = holderProvider(holder);

        owner.close();
        this.cache = null;
        this.params.graphTransaction().close();

        // Last close drops the registry entry and unregisters the listener.
        Assert.assertFalse(storeListeners.containsKey(graphName));
        Assert.assertFalse(provider.storeEventHub()
                                   .listeners(EventHub.ANY_EVENT)
                                   .contains(registered));

        this.graph.clearBackend();
        this.graph.close();
        this.graph = null;

        HugeGraph reopened = HugeFactory.open(FakeObjects.newConfig());
        this.graph = reopened;
        this.params = Whitebox.getInternalState(reopened, "params");
        CachedGraphTransaction third = new CachedGraphTransaction(
                this.params, this.params.loadGraphStore());
        this.cache = third;

        // Reopen registers a fresh holder for the same graph name, and its
        // listener is wired to the store provider again (no stale leftover).
        // The provider instance is pooled and reused across reopen, so the
        // provider-replacement branch is not exercised here.
        Object reopenedHolder = storeListeners.get(graphName);
        Assert.assertNotNull(reopenedHolder);
        Assert.assertNotSame(holder, reopenedHolder);
        EventListener reopenedListener = holderListener(reopenedHolder);
        Assert.assertNotSame(registered, reopenedListener);
        Assert.assertTrue(holderProvider(reopenedHolder).storeEventHub()
                                       .listeners(EventHub.ANY_EVENT)
                                       .contains(reopenedListener));
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
    public void testWarmPrimaryKeyCandidatesAreFilteredForEachQuery() {
        this.graph.schema().propertyKey("key").asText().create();
        this.graph.schema().propertyKey("age").asInt().create();
        this.graph.schema().vertexLabel("primary").properties("key", "age")
                  .primaryKeys("key").create();
        Vertex vertex = this.graph.addVertex(T.label, "primary", "key", "marko", "age", 20);
        this.graph.tx().commit();
        this.cache.clearCache(null, false);
        Cache<Id, Object> vertices = Whitebox.getInternalState(this.cache, "verticesCache");
        ConditionQuery miss = this.primaryQuery(21);
        Assert.assertFalse(this.cache.queryVertices(miss).hasNext());
        Assert.assertEquals(1L, vertices.size());
        long hits = vertices.hits();
        Vertex found = QueryResults.one(this.cache.queryVertices(this.primaryQuery(20)));
        Assert.assertEquals(vertex.id(), found.id());
        Assert.assertEquals(hits + 1L, vertices.hits());
        Assert.assertFalse(this.cache.queryVertices(this.primaryQuery(21)).hasNext());
        Assert.assertEquals(hits + 2L, vertices.hits());
    }

    private ConditionQuery primaryQuery(int age) {
        ConditionQuery query = new ConditionQuery(HugeType.VERTEX);
        query.eq(HugeKeys.LABEL, this.graph.vertexLabel("primary").id());
        query.query(Condition.eq(this.graph.propertyKey("key").id(), "marko"));
        query.query(Condition.eq(this.graph.propertyKey("age").id(), age));
        return query;
    }

    @Test
    public void testMixedVertexCacheHitsRetainInputOrder() {
        HugeVertex first = this.newVertex(IdGenerator.of(1L));
        HugeVertex second = this.newVertex(IdGenerator.of(2L));
        HugeVertex third = this.newVertex(IdGenerator.of(3L));
        for (HugeVertex vertex : Arrays.asList(first, second, third)) {
            this.cache.addVertex(vertex);
        }
        this.cache.commit();
        this.cache.clearCache(null, false);
        QueryResults.one(this.cache.queryVertices(second.id()));
        Cache<Id, Object> vertices = Whitebox.getInternalState(this.cache, "verticesCache");
        long hits = vertices.hits();
        IdQuery query = new IdQuery(HugeType.VERTEX);
        query.query(third.id()).query(second.id()).query(first.id());
        List<Object> ids = new ArrayList<>();
        this.cache.queryVertices(query).forEachRemaining(vertex -> ids.add(vertex.id()));
        Assert.assertEquals(Arrays.asList(third.id(), second.id(), first.id()), ids);
        Assert.assertEquals(hits + 1L, vertices.hits());
    }

    @Test
    public void testColdSpecialVertexQueryRetainsResultType() {
        BackendStore store = Mockito.spy(this.params.loadGraphStore());
        CachedGraphTransaction transaction = new CachedGraphTransaction(this.params, store);
        try {
            transaction.clearCache(null, false);
            for (HugeType type : Arrays.asList(HugeType.TASK, HugeType.SERVER)) {
                IdQuery query = new IdQuery(type);
                query.query(IdGenerator.of(999L));
                Mockito.doAnswer(invocation -> {
                    Query fetched = invocation.getArgument(0);
                    Assert.assertEquals(type, fetched.resultType());
                    Assert.assertEquals(1, fetched.idsSize());
                    return Collections.emptyIterator();
                }).when(store).query(Mockito.any(Query.class));
                Assert.assertFalse(transaction.queryVertices(query).hasNext());
            }
            Mockito.verify(store, Mockito.times(2)).query(Mockito.any(Query.class));
        } finally {
            transaction.close();
        }
    }

    @Test
    public void testEdgeBatchCacheRoundTripsThroughOffheap() throws Exception {
        HugeVertex first = this.newVertex(IdGenerator.of(1L));
        HugeVertex second = this.newVertex(IdGenerator.of(2L));
        this.cache.addVertex(first);
        this.cache.addVertex(second);
        this.cache.commit();
        HugeEdge edge = this.newEdge(first, second);
        this.cache.addEdge(edge);
        this.cache.commit();
        BackendStore store = Mockito.spy(this.params.loadGraphStore());
        CachedGraphTransaction transaction = new CachedGraphTransaction(this.params, store);
        OffheapCache offheap = new OffheapCache(this.graph, 100, 1024, 1);
        offheap.enableMetrics(true);
        Whitebox.setInternalState(transaction, "edgesCache", offheap);
        try {
            Edge cold = QueryResults.one(transaction.queryEdgesByVertex(first.id()));
            Assert.assertEquals(edge.id(), cold.id());
            Assert.assertEquals(1L, offheap.size());
            long hits = offheap.hits();
            Mockito.clearInvocations(store);
            Edge warm = QueryResults.one(transaction.queryEdgesByVertex(first.id()));
            Assert.assertEquals(edge.id(), warm.id());
            Assert.assertTrue(offheap.hits() > hits);
            Mockito.verify(store, Mockito.never()).query(Mockito.any(Query.class));
        } finally {
            transaction.close();
            AutoCloseable nativeCache = Whitebox.getInternalState(offheap, "cache");
            nativeCache.close();
        }
    }

    @Test
    public void testRamTableHitDoesNotReadBackendBeforeOptimization() {
        HugeVertex first = this.newVertex(IdGenerator.of(1L));
        HugeVertex second = this.newVertex(IdGenerator.of(2L));
        HugeEdge edge = this.newEdge(first, second);
        RamTable table = new RamTable(this.graph, 16, 16);
        table.addEdge(true, 1L, 2L, Directions.OUT, (int) edge.schemaLabel().id().asLong());
        HugeGraphParams params = Mockito.spy(this.params);
        Mockito.doReturn(table).when(params).ramtable();
        BackendStore store = Mockito.spy(this.params.loadGraphStore());
        CachedGraphTransaction transaction = new CachedGraphTransaction(params, store);
        try {
            transaction.clearCache(null, false);
            ConditionQuery query = new ConditionQuery(HugeType.EDGE);
            query.eq(HugeKeys.OWNER_VERTEX, first.id());
            query.eq(HugeKeys.DIRECTION, Directions.OUT);
            query.eq(HugeKeys.LABEL, edge.schemaLabel().id());
            Mockito.clearInvocations(store);
            Edge found =
                    QueryResults.one(transaction.queryEdges(query));
            Assert.assertEquals(edge.id(), found.id());
            Mockito.verify(store, Mockito.never()).query(Mockito.any(Query.class));
        } finally {
            transaction.close();
        }
    }

    @Test
    public void testRejectedUndefinedRecordWarnsOnceForPublicAndRawQueries() {
        this.newVertex(IdGenerator.of(1L));
        Id id = IdGenerator.of(999L);
        Cache<Id, Object> vertices = Whitebox.getInternalState(this.cache, "verticesCache");
        vertices.update(id, HugeVertex.undefined(this.graph, id));
        ConditionQuery origin = new ConditionQuery(HugeType.VERTEX);
        origin.query(Condition.eq(this.graph.propertyKey("name").id(), "absent"));
        origin.optimized(OptimizedType.PRIMARY_KEY);
        origin.showHidden(true);
        String loggerName = "org.apache.hugegraph.backend.tx.AbstractTransaction";
        LoggerContext logging = (LoggerContext) LogManager.getContext(false);
        Configuration configuration = logging.getConfiguration();
        LoggerConfig previous = configuration.getLoggers().get(loggerName);
        WarningCounter counter = new WarningCounter();
        counter.start();
        LoggerConfig logger = new LoggerConfig(loggerName, Level.WARN, false);
        logger.addAppender(counter, Level.WARN, null);
        configuration.removeLogger(loggerName);
        configuration.addLogger(loggerName, logger);
        logging.updateLoggers();
        try {
            Assert.assertFalse(this.cache.queryVertices(new IdQuery(origin, id)).hasNext());
            Assert.assertEquals(1, counter.count);
            Iterator<HugeVertex> raw = Whitebox.invoke(
                    CachedGraphTransaction.class, new Class<?>[]{Query.class},
                    "queryVerticesFromBackend", this.cache, new IdQuery(origin, id));
            Assert.assertFalse(raw.hasNext());
            Assert.assertEquals(2, counter.count);
        } finally {
            configuration.removeLogger(loggerName);
            if (previous != null) {
                configuration.addLogger(loggerName, previous);
            }
            logging.updateLoggers();
            counter.stop();
        }
    }

    private static final class WarningCounter extends AbstractAppender {

        private int count;

        private WarningCounter() {
            super("BatchWarningCounter", null, null, false, Property.EMPTY_ARRAY);
        }

        @Override
        public void append(LogEvent event) {
            if (event.getMessage().getFormattedMessage().startsWith("Left record is found:")) {
                this.count++;
            }
        }
    }

    @Test
    public void testPostFilterDefersDeletingLabelToInvalidFilter() {
        HugeVertex vertex = this.newVertex(IdGenerator.of(1));
        vertex.schemaLabel().status(SchemaStatus.DELETING);

        Id name = this.graph.propertyKey("name").id();
        ConditionQuery query = new ConditionQuery(HugeType.VERTEX);
        query.query(Condition.eq(name, "marko"));
        query.optimized(OptimizedType.INDEX);

        Class<?>[] classes = new Class<?>[]{QueryResultContext.class, HugeElement.class};
        QueryResultContext context = new QueryResultContext(query);
        boolean unmatched = Whitebox.invoke(
                CachedGraphTransaction.class, classes,
                "filterUnmatchedRecord", this.cache, context, vertex);
        Assert.assertTrue(unmatched);
        boolean invalid = Whitebox.invoke(
                CachedGraphTransaction.class, classes,
                "filterInvalidRecord", this.cache, context, vertex);
        Assert.assertFalse(invalid);
    }

    @Test
    public void testQueryVertexByIdKeepsDeletingLabel() {
        CachedGraphTransaction cache = this.cache();
        HugeVertex v1 = this.newVertex(IdGenerator.of(1));
        cache.addVertex(v1);
        cache.commit();

        v1.schemaLabel().status(SchemaStatus.DELETING);

        Assert.assertTrue(cache.queryVertices(v1.id()).hasNext());
        // Re-query to exercise cached records too
        Assert.assertTrue(cache.queryVertices(v1.id()).hasNext());
    }

    @Test
    public void testQueryEdgeByIdKeepsDeletingLabel() {
        CachedGraphTransaction cache = this.cache();
        HugeVertex v1 = this.newVertex(IdGenerator.of(1));
        HugeVertex v2 = this.newVertex(IdGenerator.of(2));
        cache.addVertex(v1);
        cache.addVertex(v2);
        cache.commit();

        HugeEdge edge = this.newEdge(v1, v2);
        cache.addEdge(edge);
        cache.commit();

        edge.schemaLabel().status(SchemaStatus.DELETING);

        Assert.assertTrue(cache.queryEdges(edge.id()).hasNext());
        // Re-query to exercise cached records too
        Assert.assertTrue(cache.queryEdges(edge.id()).hasNext());
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
