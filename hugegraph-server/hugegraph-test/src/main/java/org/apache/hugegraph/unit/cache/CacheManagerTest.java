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

import java.lang.reflect.Proxy;
import java.util.Map;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hugegraph.backend.cache.Cache;
import org.apache.hugegraph.backend.cache.CacheManager;
import org.apache.hugegraph.backend.cache.OffheapCache;
import org.apache.hugegraph.backend.cache.RamCache;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.id.IdGenerator;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.testutil.Whitebox;
import org.apache.hugegraph.unit.BaseUnitTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableMap;

public class CacheManagerTest extends BaseUnitTest {

    private Map<String, Cache<Id, Object>> originCaches;
    private Map<String, Cache<Id, Object>> mockCaches;

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Before
    public void setup() {
        CacheManager manager = CacheManager.instance();

        // Mock caches
        this.mockCaches = Mockito.mock(ConcurrentHashMap.class);

        this.originCaches = (Map) Whitebox.getInternalState(manager, "caches");
        Whitebox.setInternalState(manager, "caches", this.mockCaches);
    }

    @After
    public void teardown() {
        CacheManager manager = CacheManager.instance();

        Assert.assertNotNull(this.originCaches);
        Whitebox.setInternalState(manager, "caches", this.originCaches);
    }

    @Test
    public void testCacheInstance() {
        // Don't mock
        teardown();

        CacheManager manager = CacheManager.instance();

        Cache<Id, Object> c1 = manager.cache("c1");
        Cache<Id, Object> c12 = manager.cache("c1");
        Cache<Id, Object> c13 = manager.cache("c1", 123);
        Assert.assertEquals(c1, c12);
        Assert.assertEquals(c1, c13);
        Assert.assertEquals(c1.capacity(), c13.capacity());

        Cache<Id, Object> c2 = manager.offheapCache(null, "c2", 1, 11);
        Cache<Id, Object> c22 = manager.offheapCache(null, "c2", 2, 22);
        Cache<Id, Object> c23 = manager.offheapCache(null, "c2", 3, 33);
        Assert.assertEquals(c2, c22);
        Assert.assertEquals(c2, c23);
        Assert.assertEquals(c2.capacity(), c23.capacity());

        Cache<Id, Object> c3 = manager.levelCache(null, "c3", 1, 1, 11);
        Cache<Id, Object> c32 = manager.levelCache(null, "c3", 2, 2, 22);
        Cache<Id, Object> c33 = manager.levelCache(null, "c3", 3, 3, 33);
        Assert.assertEquals(c3, c32);
        Assert.assertEquals(c3, c33);
        Assert.assertEquals(c3.capacity(), c33.capacity());

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            manager.cache("c2");
        }, e -> {
            Assert.assertContains("Invalid cache implement:", e.getMessage());
            Assert.assertContains("OffheapCache", e.getMessage());
        });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            manager.cache("c3");
        }, e -> {
            Assert.assertContains("Invalid cache implement:", e.getMessage());
            Assert.assertContains("LevelCache", e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            manager.offheapCache(null, "c1", 1, 11);
        }, e -> {
            Assert.assertContains("Invalid cache implement:", e.getMessage());
            Assert.assertContains("RamCache", e.getMessage());
        });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            manager.offheapCache(null, "c3", 1, 11);
        }, e -> {
            Assert.assertContains("Invalid cache implement:", e.getMessage());
            Assert.assertContains("LevelCache", e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            manager.levelCache(null, "c1", 1, 1, 11);
        }, e -> {
            Assert.assertContains("Invalid cache implement:", e.getMessage());
            Assert.assertContains("RamCache", e.getMessage());
        });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            manager.levelCache(null, "c2", 1, 1, 11);
        }, e -> {
            Assert.assertContains("Invalid cache implement:", e.getMessage());
            Assert.assertContains("OffheapCache", e.getMessage());
        });

        this.originCaches.remove("c1");
        this.originCaches.remove("c2");
        this.originCaches.remove("c3");
    }

    @Test
    public void testCacheEnableMetrics() {
        // Don't mock
        teardown();

        CacheManager manager = CacheManager.instance();

        Cache<Id, Object> c1 = manager.cache("m1");
        Cache<Id, Object> c2 = manager.cache("m2");
        Cache<Id, Object> c3 = manager.offheapCache(null, "m3", 1, 11);
        Cache<Id, Object> c4 = manager.levelCache(null, "m4", 1, 1, 11);

        Assert.assertEquals(false, c1.enableMetrics(false));
        Assert.assertEquals(false, c2.enableMetrics(false));
        Assert.assertEquals(false, c3.enableMetrics(false));
        Assert.assertEquals(false, c4.enableMetrics(false));

        Assert.assertEquals(false, CacheManager.cacheEnableMetrics("m1", true));
        Assert.assertEquals(true, c1.enableMetrics(true));

        Assert.assertEquals(false, CacheManager.cacheEnableMetrics("m2", true));
        Assert.assertEquals(true, c2.enableMetrics(true));

        Assert.assertEquals(false, CacheManager.cacheEnableMetrics("m3", true));
        Assert.assertEquals(true, c3.enableMetrics(true));

        Assert.assertEquals(false, CacheManager.cacheEnableMetrics("m4", true));
        Assert.assertEquals(true, c4.enableMetrics(true));

        Assert.assertEquals(true, CacheManager.cacheEnableMetrics("m1", false));
        Assert.assertEquals(false, c1.enableMetrics(true));

        Assert.assertEquals(true, CacheManager.cacheEnableMetrics("m2", false));
        Assert.assertEquals(false, c2.enableMetrics(true));

        Assert.assertEquals(true, CacheManager.cacheEnableMetrics("m3", false));
        Assert.assertEquals(false, c3.enableMetrics(true));

        Assert.assertEquals(true, CacheManager.cacheEnableMetrics("m4", false));
        Assert.assertEquals(false, c4.enableMetrics(true));
    }

    @Test
    public void testCacheGetPut() {
        final String name = "test-cache";

        CacheManager manager = CacheManager.instance();

        Mockito.when(this.mockCaches.containsKey(name)).thenReturn(false);
        @SuppressWarnings("rawtypes") final Cache[] cache = new Cache[1];
        Mockito.when(this.mockCaches.putIfAbsent(Mockito.anyString(),
                                                 Mockito.any()))
               .thenAnswer(i -> cache[0] = (Cache<?, ?>) i.getArguments()[1]);
        Mockito.when(this.mockCaches.get(name)).thenAnswer(i -> cache[0]);

        Cache<Id, Object> cache1 = manager.cache(name);

        Assert.assertNotNull(cache1);
        Mockito.verify(this.mockCaches).putIfAbsent(name, cache1);

        Mockito.when(this.mockCaches.containsKey(name)).thenReturn(true);
        Mockito.when(this.mockCaches.get(name)).thenReturn(cache1);
        Cache<Id, Object> cache2 = manager.cache(name);

        Assert.assertSame(cache1, cache2);
        Mockito.verify(this.mockCaches, Mockito.atMost(1))
               .putIfAbsent(Mockito.anyString(), Mockito.any());
    }

    @Test
    public void testCachePutGetWithCapacity() {
        final String name = "test-cache";
        final int capacity = 12345;

        CacheManager manager = CacheManager.instance();

        Mockito.when(this.mockCaches.containsKey(name)).thenReturn(false);
        @SuppressWarnings("rawtypes") final Cache[] cache = new Cache[1];
        Mockito.when(this.mockCaches.putIfAbsent(Mockito.anyString(),
                                                 Mockito.any()))
               .thenAnswer(i -> cache[0] = (Cache<?, ?>) i.getArguments()[1]);
        Mockito.when(this.mockCaches.get(name)).thenAnswer(i -> cache[0]);

        Cache<Id, Object> cache1 = manager.cache(name, capacity);

        Assert.assertNotNull(cache1);
        Assert.assertEquals(capacity, cache1.capacity());
        Mockito.verify(this.mockCaches).putIfAbsent(name, cache1);

        Mockito.when(this.mockCaches.containsKey(name)).thenReturn(true);
        Mockito.when(this.mockCaches.get(name)).thenReturn(cache1);
        Cache<Id, Object> cache2 = manager.cache(name, capacity);

        Assert.assertEquals(capacity, cache2.capacity());
        Assert.assertSame(cache1, cache2);

        Assert.assertSame(cache1, manager.cache(name));
        Assert.assertSame(cache1, manager.cache(name, 0));
        Assert.assertSame(cache1, manager.cache(name, 1));
        Assert.assertSame(cache1, manager.cache(name, capacity));
        Assert.assertSame(cache1, manager.cache(name, capacity + 10));

        Mockito.verify(this.mockCaches, Mockito.atMost(1))
               .putIfAbsent(Mockito.anyString(), Mockito.any());
    }

    @Test
    public void testCacheList() {
        CacheManager manager = CacheManager.instance();

        Cache<Id, Object> cache1 = Mockito.mock(RamCache.class);
        Cache<Id, Object> cache2 = Mockito.mock(OffheapCache.class);

        Mockito.when(this.mockCaches.get("cache-1")).thenReturn(cache1);
        Mockito.when(this.mockCaches.get("cache-2")).thenReturn(cache2);
        Mockito.when(this.mockCaches.size()).thenReturn(2);

        Map<String, Cache<Id, Object>> caches = manager.caches();

        Assert.assertEquals(2, caches.size());
        Assert.assertSame(cache1, caches.get("cache-1"));
        Assert.assertSame(cache2, caches.get("cache-2"));

        Assert.assertArrayEquals(this.mockCaches.values().toArray(),
                                 caches.values().toArray());
    }

    @Test
    public void testCacheListModify() {
        Assert.assertThrows(UnsupportedOperationException.class, () -> {
            CacheManager manager = CacheManager.instance();
            manager.caches().put("test", null);
        });
    }

    @Test
    public void testCacheExpire() {
        CacheManager manager = CacheManager.instance();

        Cache<Id, Object> cache1 = new RamCache();
        cache1.expire(26 * 1000L);

        Cache<Id, Object> cache2 = new RamCache();
        cache2.expire(0);

        Cache<Id, Object> mockCache1 = Mockito.spy(cache1);
        Cache<Id, Object> mockCache2 = Mockito.spy(cache2);
        Map<String, Cache<Id, Object>> caches = ImmutableMap.of("cache1",
                                                                mockCache1,
                                                                "cache2",
                                                                mockCache2);
        Mockito.when(this.mockCaches.entrySet()).thenReturn(caches.entrySet());

        TimerTask task = Whitebox.invoke(CacheManager.class, "scheduleTimer",
                                         manager, 0.1F);
        try {
            cache1.update(IdGenerator.of("fake-id"), "fake-value",
                          -30 * 1000L);
            cache2.update(IdGenerator.of("fake-id"), "fake-value");

            waitTillNext(3);

            Mockito.verify(mockCache1, Mockito.atLeastOnce()).tick();
            Mockito.verify(mockCache2, Mockito.atLeastOnce()).tick();

            Assert.assertEquals(0, cache1.size());
            Assert.assertEquals(1, cache2.size());
        } finally {
            task.cancel();
        }
    }

    @SuppressWarnings({"unused", "unchecked"})
    private static Cache<Id, Object> newCacheProxy(Cache<Id, Object> cache) {
        Object p = Proxy.newProxyInstance(Cache.class.getClassLoader(),
                                          new Class[]{Cache.class},
                                          (proxy, method, args) -> {
                                              return method.invoke(cache, args);
                                          });
        return (Cache<Id, Object>) p;
    }
}
