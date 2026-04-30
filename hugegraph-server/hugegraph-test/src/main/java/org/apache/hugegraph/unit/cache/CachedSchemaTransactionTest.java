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

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.apache.hugegraph.HugeFactory;
import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.HugeGraphParams;
import org.apache.hugegraph.backend.cache.Cache;
import org.apache.hugegraph.backend.cache.CacheManager;
import org.apache.hugegraph.backend.cache.CachedSchemaTransaction;
import org.apache.hugegraph.backend.cache.CachedSchemaTransactionV2;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.id.IdGenerator;
import org.apache.hugegraph.meta.MetaDriver;
import org.apache.hugegraph.meta.MetaManager;
import org.apache.hugegraph.meta.managers.GraphMetaManager;
import org.apache.hugegraph.schema.SchemaElement;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.testutil.Whitebox;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.unit.BaseUnitTest;
import org.apache.hugegraph.unit.FakeObjects;
import org.apache.hugegraph.util.Events;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableMap;

public class CachedSchemaTransactionTest extends BaseUnitTest {

    private CachedSchemaTransaction cache;
    private HugeGraphParams params;

    @Before
    public void setup() {
        HugeGraph graph = HugeFactory.open(FakeObjects.newConfig());
        this.params = Whitebox.getInternalState(graph, "params");
        this.cache = new CachedSchemaTransaction(this.params,
                                                 this.params.loadSchemaStore());
    }

    @After
    public void teardown() throws Exception {
        this.cache().graph().clearBackend();
        this.cache().graph().close();
    }

    private CachedSchemaTransaction cache() {
        Assert.assertNotNull(this.cache);
        return this.cache;
    }

    @Test
    public void testEventClear() throws Exception {
        CachedSchemaTransaction cache = this.cache();

        FakeObjects objects = new FakeObjects("unit-test");
        cache.addPropertyKey(objects.newPropertyKey(IdGenerator.of(1),
                                                    "fake-pk-1"));
        cache.addPropertyKey(objects.newPropertyKey(IdGenerator.of(2),
                                                    "fake-pk-2"));

        Assert.assertEquals(2L, Whitebox.invoke(cache, "idCache", "size"));
        Assert.assertEquals(2L, Whitebox.invoke(cache, "nameCache", "size"));

        Assert.assertEquals("fake-pk-1",
                            cache.getPropertyKey(IdGenerator.of(1)).name());
        Assert.assertEquals(IdGenerator.of(1),
                            cache.getPropertyKey("fake-pk-1").id());

        Assert.assertEquals("fake-pk-2",
                            cache.getPropertyKey(IdGenerator.of(2)).name());
        Assert.assertEquals(IdGenerator.of(2),
                            cache.getPropertyKey("fake-pk-2").id());

        this.params.schemaEventHub().notify(Events.CACHE, "clear", null).get();

        Assert.assertEquals(0L, Whitebox.invoke(cache, "idCache", "size"));
        Assert.assertEquals(0L, Whitebox.invoke(cache, "nameCache", "size"));

        Assert.assertEquals("fake-pk-1",
                            cache.getPropertyKey(IdGenerator.of(1)).name());
        Assert.assertEquals(IdGenerator.of(1),
                            cache.getPropertyKey("fake-pk-1").id());

        Assert.assertEquals("fake-pk-2",
                            cache.getPropertyKey(IdGenerator.of(2)).name());
        Assert.assertEquals(IdGenerator.of(2),
                            cache.getPropertyKey("fake-pk-2").id());

        Assert.assertEquals(2L, Whitebox.invoke(cache, "idCache", "size"));
        Assert.assertEquals(2L, Whitebox.invoke(cache, "nameCache", "size"));
    }

    @Test
    public void testEventInvalid() throws Exception {
        CachedSchemaTransaction cache = this.cache();

        FakeObjects objects = new FakeObjects("unit-test");
        cache.addPropertyKey(objects.newPropertyKey(IdGenerator.of(1),
                                                    "fake-pk-1"));
        cache.addPropertyKey(objects.newPropertyKey(IdGenerator.of(2),
                                                    "fake-pk-2"));

        Assert.assertEquals(2L, Whitebox.invoke(cache, "idCache", "size"));
        Assert.assertEquals(2L, Whitebox.invoke(cache, "nameCache", "size"));

        Assert.assertEquals("fake-pk-1",
                            cache.getPropertyKey(IdGenerator.of(1)).name());
        Assert.assertEquals(IdGenerator.of(1),
                            cache.getPropertyKey("fake-pk-1").id());

        Assert.assertEquals("fake-pk-2",
                            cache.getPropertyKey(IdGenerator.of(2)).name());
        Assert.assertEquals(IdGenerator.of(2),
                            cache.getPropertyKey("fake-pk-2").id());

        this.params.schemaEventHub().notify(Events.CACHE, "invalid",
                                            HugeType.PROPERTY_KEY,
                                            IdGenerator.of(1)).get();

        Assert.assertEquals(1L, Whitebox.invoke(cache, "idCache", "size"));
        Assert.assertEquals(1L, Whitebox.invoke(cache, "nameCache", "size"));

        Assert.assertEquals("fake-pk-1",
                            cache.getPropertyKey(IdGenerator.of(1)).name());
        Assert.assertEquals(IdGenerator.of(1),
                            cache.getPropertyKey("fake-pk-1").id());

        Assert.assertEquals("fake-pk-2",
                            cache.getPropertyKey(IdGenerator.of(2)).name());
        Assert.assertEquals(IdGenerator.of(2),
                            cache.getPropertyKey("fake-pk-2").id());

        Assert.assertEquals(2L, Whitebox.invoke(cache, "idCache", "size"));
        Assert.assertEquals(2L, Whitebox.invoke(cache, "nameCache", "size"));
    }

    @Test
    public void testGetSchema() throws Exception {
        CachedSchemaTransaction cache = this.cache();

        FakeObjects objects = new FakeObjects("unit-test");
        cache.addPropertyKey(objects.newPropertyKey(IdGenerator.of(1),
                                                    "fake-pk-1"));

        this.params.schemaEventHub().notify(Events.CACHE, "clear", null).get();
        Assert.assertEquals("fake-pk-1",
                            cache.getPropertyKey(IdGenerator.of(1)).name());
        Assert.assertEquals(IdGenerator.of(1),
                            cache.getPropertyKey("fake-pk-1").id());

        this.params.schemaEventHub().notify(Events.CACHE, "clear", null).get();
        Assert.assertEquals(IdGenerator.of(1),
                            cache.getPropertyKey("fake-pk-1").id());
        Assert.assertEquals("fake-pk-1",
                            cache.getPropertyKey(IdGenerator.of(1)).name());
    }

    @Test
    public void testClearV2SchemaCacheByGraphName() {
        String graphName = "DEFAULT-unit-test-v2";
        String otherGraphName = "DEFAULT-other-v2";

        Cache<Id, Object> idCache = CacheManager.instance()
                                                .cache("schema-id-" +
                                                       graphName, 10L);
        Cache<Id, Object> nameCache = CacheManager.instance()
                                                  .cache("schema-name-" +
                                                         graphName, 10L);
        Cache<Id, Object> otherIdCache = CacheManager.instance()
                                                     .cache("schema-id-" +
                                                            otherGraphName,
                                                            10L);
        Object arrayCaches = idCache.attachment(newV2SchemaCaches(10));
        Id arrayCacheId = IdGenerator.of(1);
        SchemaElement arrayCacheSchema =
                new FakeObjects("unit-test-v2")
                        .newPropertyKey(arrayCacheId, "fake-pk-array");

        try {
            clearV2SchemaCaches(arrayCaches);
            setV2SchemaCache(arrayCaches, HugeType.PROPERTY_KEY, arrayCacheId,
                             arrayCacheSchema);
            idCache.update(IdGenerator.of(1), "fake-pk-by-id");
            nameCache.update(IdGenerator.of("fake-pk"), "fake-pk-by-name");
            otherIdCache.update(IdGenerator.of(2), "other-pk-by-id");

            Assert.assertEquals(1L, idCache.size());
            Assert.assertEquals(1L, nameCache.size());
            Assert.assertEquals(1L, otherIdCache.size());
            Assert.assertSame(arrayCacheSchema,
                              getV2SchemaCache(arrayCaches,
                                               HugeType.PROPERTY_KEY,
                                               arrayCacheId));

            Whitebox.invokeStatic(CachedSchemaTransactionV2.class,
                                  new Class<?>[]{String.class},
                                  "clearSchemaCache", graphName);

            Assert.assertEquals(0L, idCache.size());
            Assert.assertEquals(0L, nameCache.size());
            Assert.assertEquals(1L, otherIdCache.size());
            Assert.assertNull(getV2SchemaCache(arrayCaches,
                                               HugeType.PROPERTY_KEY,
                                               arrayCacheId));
        } finally {
            clearV2SchemaCaches(arrayCaches);
            idCache.clear();
            nameCache.clear();
            otherIdCache.clear();
        }
    }

    private static Object newV2SchemaCaches(int size) {
        for (Class<?> clazz :
             CachedSchemaTransactionV2.class.getDeclaredClasses()) {
            if (!"SchemaCaches".equals(clazz.getSimpleName())) {
                continue;
            }
            try {
                Constructor<?> constructor =
                        clazz.getDeclaredConstructor(int.class);
                constructor.setAccessible(true);
                return constructor.newInstance(size);
            } catch (ReflectiveOperationException e) {
                throw new AssertionError("Failed to create SchemaCaches", e);
            }
        }
        throw new AssertionError("SchemaCaches class not found");
    }

    private static void clearV2SchemaCaches(Object arrayCaches) {
        Whitebox.invoke(arrayCaches.getClass(), "clear", arrayCaches);
    }

    private static void setV2SchemaCache(Object arrayCaches, HugeType type,
                                         Id id, SchemaElement schema) {
        Whitebox.invoke(arrayCaches.getClass(),
                        new Class<?>[]{HugeType.class, Id.class,
                                       SchemaElement.class},
                        "set", arrayCaches, type, id, schema);
    }

    private static SchemaElement getV2SchemaCache(Object arrayCaches,
                                                  HugeType type, Id id) {
        return Whitebox.invoke(arrayCaches.getClass(),
                               new Class<?>[]{HugeType.class, Id.class},
                               "get", arrayCaches, type, id);
    }

    @Test
    public void testListenSchemaCacheClearIsIdempotent() throws Exception {
        // Once the JVM-global registration flag is set, every subsequent
        // call to listenSchemaCacheClear() must short-circuit before
        // touching MetaManager — even under concurrent invocation. Pre-set
        // the flag, race N threads, and verify none of them propagated an
        // exception (which would happen if MetaManager.instance()
        // .listenSchemaCacheClear were invoked without an initialised
        // driver).
        Field flagField = CachedSchemaTransactionV2.class
                .getDeclaredField("metaEventListenerRegistered");
        flagField.setAccessible(true);
        AtomicBoolean flag = (AtomicBoolean) flagField.get(null);
        boolean previous = flag.getAndSet(true);
        try {
            int threads = 8;
            CountDownLatch start = new CountDownLatch(1);
            CountDownLatch done = new CountDownLatch(threads);
            AtomicInteger failures = new AtomicInteger();
            for (int i = 0; i < threads; i++) {
                new Thread(() -> {
                    try {
                        start.await();
                        Whitebox.invokeStatic(CachedSchemaTransactionV2.class,
                                              "listenSchemaCacheClear");
                    } catch (Throwable t) {
                        failures.incrementAndGet();
                    } finally {
                        done.countDown();
                    }
                }).start();
            }
            start.countDown();
            Assert.assertTrue("listenSchemaCacheClear race timed out",
                              done.await(10, TimeUnit.SECONDS));
            Assert.assertEquals("listenSchemaCacheClear must short-circuit " +
                                "when already registered", 0, failures.get());
            Assert.assertTrue("registration flag must remain set", flag.get());
        } finally {
            flag.set(previous);
        }
    }

    @Test
    public void testClearSchemaCacheClearsArrayAttachmentMaps()
            throws Exception {
        // clearSchemaCache() must wipe idCache, nameCache and every internal
        // IntObjectMap (pks/vls/els/ils) inside the array attachment so
        // stale entries are not served after a meta event.
        String graphName = "DEFAULT-unit-test-v2-array";
        Cache<Id, Object> idCache =
                CacheManager.instance().cache("schema-id-" + graphName, 10L);
        Cache<Id, Object> nameCache =
                CacheManager.instance().cache("schema-name-" + graphName, 10L);
        // Size must comfortably exceed the largest id below: IntObjectMap
        // grows by doubling and refuses to write past currentSize even after
        // a single expansion, so a small capacity rejects mid-range keys.
        Object arrayCaches = idCache.attachment(newV2SchemaCaches(64));
        Id pkId = IdGenerator.of(1);
        Id vlId = IdGenerator.of(2);
        Id elId = IdGenerator.of(3);
        Id ilId = IdGenerator.of(4);
        FakeObjects fakeObjects = new FakeObjects("unit-test-v2-array");
        SchemaElement pk = fakeObjects.newPropertyKey(pkId, "fake-pk");

        try {
            clearV2SchemaCaches(arrayCaches);
            setV2SchemaCache(arrayCaches, HugeType.PROPERTY_KEY, pkId, pk);
            setV2SchemaCache(arrayCaches, HugeType.VERTEX_LABEL, vlId, pk);
            setV2SchemaCache(arrayCaches, HugeType.EDGE_LABEL, elId, pk);
            setV2SchemaCache(arrayCaches, HugeType.INDEX_LABEL, ilId, pk);
            idCache.update(pkId, "fake-pk-by-id");
            nameCache.update(IdGenerator.of("fake-pk"), "fake-pk-by-name");

            Assert.assertEquals(1L, idCache.size());
            Assert.assertEquals(1L, nameCache.size());
            Assert.assertNotNull(getV2SchemaCache(arrayCaches,
                                                  HugeType.PROPERTY_KEY, pkId));

            Whitebox.invokeStatic(CachedSchemaTransactionV2.class,
                                  new Class<?>[]{String.class},
                                  "clearSchemaCache", graphName);

            Assert.assertEquals(0L, idCache.size());
            Assert.assertEquals(0L, nameCache.size());
            for (String mapName : new String[]{"pks", "vls", "els", "ils"}) {
                Object intMap = readField(arrayCaches, mapName);
                assertIntObjectMapEmpty(intMap, mapName);
            }
            Map<HugeType, Boolean> cachedTypes = readField(arrayCaches,
                                                           "cachedTypes");
            Assert.assertTrue("cachedTypes must be empty after clear",
                              cachedTypes.isEmpty());
        } finally {
            clearV2SchemaCaches(arrayCaches);
            idCache.clear();
            nameCache.clear();
        }
    }

    // TASK_SYNC_DELETION gating of removeSchema notifications and the
    // unconditional addSchema notification require an initialised
    // CachedSchemaTransactionV2 instance, which in turn needs an hstore
    // backend and a connected MetaManager. Both prerequisites are out of
    // scope for this unit test class. They are exercised end-to-end by the
    // hstore integration tests in CoreTestSuite. TODO(#2617): port these
    // assertions into a dedicated CachedSchemaTransactionV2IT once
    // mockito-inline becomes available so MetaManager.instance() can be
    // stubbed without an hstore cluster.

    @Test
    public void testHandleSchemaCacheClearEventSkipsLocalSource()
            throws Exception {
        String graphName = "DEFAULT-meta-local-source-v2";
        Cache<Id, Object> idCache =
                CacheManager.instance().cache("schema-id-" + graphName, 10L);
        Cache<Id, Object> nameCache =
                CacheManager.instance()
                            .cache("schema-name-" + graphName, 10L);

        MetaDriver mockDriver = Mockito.mock(MetaDriver.class);
        Object localResponse = new Object();
        Object remoteResponse = new Object();
        String localSource = schemaCacheClearSource();
        Mockito.when(mockDriver.extractValuesFromResponse(localResponse))
               .thenReturn(Collections.singletonList(
                       MetaManager.schemaCacheClearEventValue(graphName,
                                                              localSource)));
        Mockito.when(mockDriver.extractValuesFromResponse(remoteResponse))
               .thenReturn(Collections.singletonList(
                       MetaManager.schemaCacheClearEventValue(graphName,
                                                              "remote")));

        MetaDriver originalDriver = swapMetaDriver(mockDriver);
        try {
            idCache.update(IdGenerator.of(1), "v");
            nameCache.update(IdGenerator.of("n"), "v");

            Whitebox.invokeStatic(CachedSchemaTransactionV2.class,
                                  new Class<?>[]{Object.class},
                                  "handleSchemaCacheClearEvent",
                                  localResponse);

            Assert.assertEquals("local echo must not clear id cache",
                                1L, idCache.size());
            Assert.assertEquals("local echo must not clear name cache",
                                1L, nameCache.size());

            Whitebox.invokeStatic(CachedSchemaTransactionV2.class,
                                  new Class<?>[]{Object.class},
                                  "handleSchemaCacheClearEvent",
                                  remoteResponse);

            Assert.assertEquals(0L, idCache.size());
            Assert.assertEquals(0L, nameCache.size());
        } finally {
            swapMetaDriver(originalDriver);
            idCache.clear();
            nameCache.clear();
        }
    }

    @Test
    public void testHandleSchemaCacheClearEventClearsTargetGraphOnly()
            throws Exception {
        // End-to-end coverage of the meta-event consumer:
        //   publish (response) -> MetaManager extract -> clearSchemaCache
        // We bypass the live etcd/PD watch by stubbing MetaDriver on the
        // MetaManager singleton and invoking the package-private consumer
        // directly. This validates that only the targeted graph's caches are
        // cleared and that other graphs in the same JVM are left untouched.
        String targetGraph = "DEFAULT-meta-target-v2";
        String otherGraph = "DEFAULT-meta-other-v2";

        Cache<Id, Object> targetIdCache =
                CacheManager.instance().cache("schema-id-" + targetGraph, 10L);
        Cache<Id, Object> targetNameCache =
                CacheManager.instance()
                            .cache("schema-name-" + targetGraph, 10L);
        Cache<Id, Object> otherIdCache =
                CacheManager.instance().cache("schema-id-" + otherGraph, 10L);

        MetaDriver mockDriver = Mockito.mock(MetaDriver.class);
        Object response = new Object();
        Mockito.when(mockDriver.extractValuesFromResponse(response))
               .thenReturn(Arrays.asList(targetGraph));

        MetaDriver originalDriver = swapMetaDriver(mockDriver);
        try {
            targetIdCache.update(IdGenerator.of(1), "v");
            targetNameCache.update(IdGenerator.of("n"), "v");
            otherIdCache.update(IdGenerator.of(2), "v");

            Assert.assertEquals(1L, targetIdCache.size());
            Assert.assertEquals(1L, targetNameCache.size());
            Assert.assertEquals(1L, otherIdCache.size());

            Whitebox.invokeStatic(CachedSchemaTransactionV2.class,
                                  new Class<?>[]{Object.class},
                                  "handleSchemaCacheClearEvent", response);

            Assert.assertEquals(0L, targetIdCache.size());
            Assert.assertEquals(0L, targetNameCache.size());
            Assert.assertEquals("Other graph caches must remain untouched",
                                1L, otherIdCache.size());
        } finally {
            swapMetaDriver(originalDriver);
            targetIdCache.clear();
            targetNameCache.clear();
            otherIdCache.clear();
        }
    }

    @Test
    public void testHandleSchemaCacheClearEventNullGraphsIsNoop()
            throws Exception {
        // A response that yields no graph names (extractor returns null) must
        // be a strict noop: caches stay populated.
        String graphName = "DEFAULT-meta-noop-v2";
        Cache<Id, Object> idCache =
                CacheManager.instance().cache("schema-id-" + graphName, 10L);

        MetaDriver mockDriver = Mockito.mock(MetaDriver.class);
        Object response = new Object();
        Mockito.when(mockDriver.extractValuesFromResponse(response))
               .thenReturn(null);

        MetaDriver originalDriver = swapMetaDriver(mockDriver);
        try {
            idCache.update(IdGenerator.of(1), "v");
            Assert.assertEquals(1L, idCache.size());

            Whitebox.invokeStatic(CachedSchemaTransactionV2.class,
                                  new Class<?>[]{Object.class},
                                  "handleSchemaCacheClearEvent", response);

            Assert.assertEquals("noop response must not clear any cache",
                                1L, idCache.size());
        } finally {
            swapMetaDriver(originalDriver);
            idCache.clear();
        }
    }

    @Test
    public void testHandleSchemaCacheClearEventClearsMultipleGraphs()
            throws Exception {
        // A single meta event may carry multiple graph names; every one of
        // them must have its V2 caches cleared.
        String graphA = "DEFAULT-meta-multi-a";
        String graphB = "DEFAULT-meta-multi-b";
        Cache<Id, Object> idA =
                CacheManager.instance().cache("schema-id-" + graphA, 10L);
        Cache<Id, Object> idB =
                CacheManager.instance().cache("schema-id-" + graphB, 10L);

        MetaDriver mockDriver = Mockito.mock(MetaDriver.class);
        Object response = new Object();
        Mockito.when(mockDriver.extractValuesFromResponse(response))
               .thenReturn(Arrays.asList(graphA, graphB));

        MetaDriver originalDriver = swapMetaDriver(mockDriver);
        try {
            idA.update(IdGenerator.of(1), "v");
            idB.update(IdGenerator.of(2), "v");
            Assert.assertEquals(1L, idA.size());
            Assert.assertEquals(1L, idB.size());

            Whitebox.invokeStatic(CachedSchemaTransactionV2.class,
                                  new Class<?>[]{Object.class},
                                  "handleSchemaCacheClearEvent", response);

            Assert.assertEquals(0L, idA.size());
            Assert.assertEquals(0L, idB.size());
        } finally {
            swapMetaDriver(originalDriver);
            idA.clear();
            idB.clear();
        }
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void testListenSchemaCacheClearRegistersOnlyOnce() throws Exception {
        // Two CachedSchemaTransactionV2 instances in the same JVM must share
        // the JVM-global meta listener: only ONE underlying watch should be
        // installed even if listenSchemaCacheClear() is invoked multiple
        // times. We assert this directly against the MetaDriver mock.
        MetaDriver mockDriver = Mockito.mock(MetaDriver.class);
        GraphMetaManager mockGraphMgr =
                new GraphMetaManager(mockDriver, "test-cluster");

        AtomicBoolean flag = metaListenerFlag();
        boolean previousFlag = flag.getAndSet(false);
        MetaDriver originalDriver = swapMetaDriver(mockDriver);
        Object originalGraphMgr = swapGraphMetaManager(mockGraphMgr);
        try {
            Whitebox.invokeStatic(CachedSchemaTransactionV2.class,
                                  "listenSchemaCacheClear");
            Whitebox.invokeStatic(CachedSchemaTransactionV2.class,
                                  "listenSchemaCacheClear");
            Whitebox.invokeStatic(CachedSchemaTransactionV2.class,
                                  "listenSchemaCacheClear");

            ArgumentCaptor<Consumer> captor =
                    ArgumentCaptor.forClass(Consumer.class);
            Mockito.verify(mockDriver, Mockito.times(1))
                   .listen(Mockito.anyString(), captor.capture());
            Assert.assertNotNull("registered consumer must not be null",
                                 captor.getValue());
            Assert.assertTrue("flag must be set after successful registration",
                              flag.get());
        } finally {
            flag.set(previousFlag);
            swapMetaDriver(originalDriver);
            swapGraphMetaManager(originalGraphMgr);
        }
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void testListenSchemaCacheClearEndToEnd() throws Exception {
        // Full publish -> callback -> clear path: register the listener via
        // the production code, capture the consumer that was wired into the
        // MetaDriver, then invoke it as the watch would and assert the V2
        // caches for the named graph are cleared.
        String graphName = "DEFAULT-end-to-end-v2";
        Cache<Id, Object> idCache =
                CacheManager.instance().cache("schema-id-" + graphName, 10L);
        Cache<Id, Object> nameCache =
                CacheManager.instance()
                            .cache("schema-name-" + graphName, 10L);

        MetaDriver mockDriver = Mockito.mock(MetaDriver.class);
        Object response = new Object();
        Mockito.when(mockDriver.extractValuesFromResponse(response))
               .thenReturn(Collections.singletonList(graphName));
        GraphMetaManager mockGraphMgr =
                new GraphMetaManager(mockDriver, "test-cluster");

        AtomicBoolean flag = metaListenerFlag();
        boolean previousFlag = flag.getAndSet(false);
        MetaDriver originalDriver = swapMetaDriver(mockDriver);
        Object originalGraphMgr = swapGraphMetaManager(mockGraphMgr);
        try {
            idCache.update(IdGenerator.of(1), "v");
            nameCache.update(IdGenerator.of("n"), "v");
            Assert.assertEquals(1L, idCache.size());
            Assert.assertEquals(1L, nameCache.size());

            Whitebox.invokeStatic(CachedSchemaTransactionV2.class,
                                  "listenSchemaCacheClear");

            ArgumentCaptor<Consumer> captor =
                    ArgumentCaptor.forClass(Consumer.class);
            Mockito.verify(mockDriver)
                   .listen(Mockito.anyString(), captor.capture());

            // Simulate the meta server publishing a schema-cache-clear event:
            // invoke the consumer captured above with a synthetic response.
            captor.getValue().accept(response);

            Assert.assertEquals(0L, idCache.size());
            Assert.assertEquals(0L, nameCache.size());
        } finally {
            flag.set(previousFlag);
            swapMetaDriver(originalDriver);
            swapGraphMetaManager(originalGraphMgr);
            idCache.clear();
            nameCache.clear();
        }
    }

    private static AtomicBoolean metaListenerFlag() throws Exception {
        Field f = CachedSchemaTransactionV2.class
                .getDeclaredField("metaEventListenerRegistered");
        f.setAccessible(true);
        return (AtomicBoolean) f.get(null);
    }

    private static String schemaCacheClearSource() throws Exception {
        Field f = CachedSchemaTransactionV2.class
                .getDeclaredField("SCHEMA_CACHE_CLEAR_SOURCE");
        f.setAccessible(true);
        return (String) f.get(null);
    }

    private static MetaDriver swapMetaDriver(MetaDriver replacement)
            throws Exception {
        Field f = MetaManager.class.getDeclaredField("metaDriver");
        f.setAccessible(true);
        MetaDriver previous = (MetaDriver) f.get(MetaManager.instance());
        f.set(MetaManager.instance(), replacement);
        return previous;
    }

    private static Object swapGraphMetaManager(Object replacement)
            throws Exception {
        Field f = MetaManager.class.getDeclaredField("graphMetaManager");
        f.setAccessible(true);
        Object previous = f.get(MetaManager.instance());
        f.set(MetaManager.instance(), replacement);
        return previous;
    }

    @SuppressWarnings("unchecked")
    private static <T> T readField(Object target, String name)
            throws ReflectiveOperationException {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        return (T) field.get(target);
    }

    private static void assertIntObjectMapEmpty(Object intMap, String label)
            throws ReflectiveOperationException {
        Object array = readField(intMap, "array");
        if (array instanceof Object[]) {
            for (Object slot : (Object[]) array) {
                Assert.assertNull(label + " slot must be null after clear",
                                  slot);
            }
            return;
        }
        // Older IntObjectMap implementations expose a size accessor instead
        // of a raw array; fall back to that if reflection finds no array.
        Object size = Whitebox.invoke(intMap.getClass(), "size", intMap);
        Assert.assertEquals(label + " must report size 0 after clear",
                            0, ((Number) size).intValue());
    }

    @Test
    public void testResetCachedAllIfReachedCapacity() throws Exception {
        CachedSchemaTransaction cache = this.cache();

        Object old = Whitebox.getInternalState(cache, "idCache.capacity");
        Whitebox.setInternalState(cache, "idCache.capacity", 2);
        try {
            Assert.assertEquals(0L, Whitebox.invoke(cache, "idCache", "size"));

            FakeObjects objects = new FakeObjects("unit-test");
            cache.addPropertyKey(objects.newPropertyKey(IdGenerator.of(1),
                                                        "fake-pk-1"));
            Assert.assertEquals(1L, Whitebox.invoke(cache, "idCache", "size"));
            Assert.assertEquals(1, cache.getPropertyKeys().size());
            Whitebox.invoke(CachedSchemaTransaction.class, "cachedTypes", cache);
            Assert.assertEquals(ImmutableMap.of(HugeType.PROPERTY_KEY, true),
                                Whitebox.invoke(CachedSchemaTransaction.class,
                                                "cachedTypes", cache));

            cache.addPropertyKey(objects.newPropertyKey(IdGenerator.of(3),
                                                        "fake-pk-2"));
            cache.addPropertyKey(objects.newPropertyKey(IdGenerator.of(2),
                                                        "fake-pk-3"));

            Assert.assertEquals(2L, Whitebox.invoke(cache, "idCache", "size"));
            Assert.assertEquals(3, cache.getPropertyKeys().size());
            Assert.assertEquals(ImmutableMap.of(),
                                Whitebox.invoke(CachedSchemaTransaction.class,
                                                "cachedTypes", cache));
        } finally {
            Whitebox.setInternalState(cache, "idCache.capacity", old);
        }
    }
}
