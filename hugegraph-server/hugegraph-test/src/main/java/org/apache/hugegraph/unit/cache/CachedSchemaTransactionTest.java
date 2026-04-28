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

import org.apache.hugegraph.HugeFactory;
import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.HugeGraphParams;
import org.apache.hugegraph.backend.cache.Cache;
import org.apache.hugegraph.backend.cache.CacheManager;
import org.apache.hugegraph.backend.cache.CachedSchemaTransaction;
import org.apache.hugegraph.backend.cache.CachedSchemaTransactionV2;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.id.IdGenerator;
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
