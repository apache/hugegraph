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

import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.backend.cache.Cache;
import org.apache.hugegraph.backend.cache.LevelCache;
import org.apache.hugegraph.backend.cache.OffheapCache;
import org.apache.hugegraph.backend.cache.RamCache;
import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.id.IdGenerator;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.testutil.Whitebox;
import org.apache.hugegraph.unit.BaseUnitTest;
import org.apache.hugegraph.util.Blob;
import org.apache.hugegraph.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableList;

public abstract class CacheTest extends BaseUnitTest {

    @Before
    public void setup() {
        // pass
    }

    @After
    public void teardown() throws Exception {
        // pass
    }

    protected abstract Cache<Id, Object> newCache();

    protected abstract Cache<Id, Object> newCache(long capacity);

    protected abstract void checkSize(Cache<Id, Object> cache, long size,
                                      Map<Id, Object> kvs);

    protected abstract void checkNotInCache(Cache<Id, Object> cache, Id id);

    protected abstract void checkInCache(Cache<Id, Object> cache, Id id);

    public static class LimitMap extends LinkedHashMap<Id, Object> {

        private static final long serialVersionUID = 1L;
        private final int limit;

        public LimitMap(int limit) {
            super(limit);
            this.limit = limit;
        }

        @Override
        protected boolean removeEldestEntry(Map.Entry<Id, Object> eldest) {
            return this.size() > this.limit;
        }
    }

    public static class RamCacheTest extends CacheTest {

        @Override
        protected Cache<Id, Object> newCache() {
            return new RamCache();
        }

        @Override
        protected Cache<Id, Object> newCache(long capacity) {
            return new RamCache(capacity);
        }

        @Override
        protected void checkSize(Cache<Id, Object> cache, long size,
                                 Map<Id, Object> kvs) {
            Assert.assertEquals(size, cache.size());
            if (kvs != null) {
                for (Map.Entry<Id, Object> kv : kvs.entrySet()) {
                    Assert.assertEquals(kv.getValue(), cache.get(kv.getKey()));
                }
            }
        }

        @Override
        protected void checkInCache(Cache<Id, Object> cache, Id id) {
            Assert.assertTrue(cache.containsKey(id));

            Object queue = Whitebox.getInternalState(cache, "queue");
            Assert.assertThrows(RuntimeException.class, () -> {
                Whitebox.invoke(queue.getClass(), new Class[]{Object.class},
                                "checkNotInQueue", queue, id);
            }, e -> {
                Assert.assertContains("should be not in", e.getMessage());
            });

            @SuppressWarnings("unchecked")
            Map<Id, ?> map = (Map<Id, ?>) Whitebox.getInternalState(cache,
                                                                    "map");
            Assert.assertTrue(Whitebox.invoke(queue.getClass(),
                                              "checkPrevNotInNext",
                                              queue, map.get(id)));
        }

        @Override
        protected void checkNotInCache(Cache<Id, Object> cache, Id id) {
            Assert.assertFalse(cache.containsKey(id));

            Object queue = Whitebox.getInternalState(cache, "queue");
            Assert.assertTrue(Whitebox.invoke(queue.getClass(),
                                              new Class[]{Object.class},
                                              "checkNotInQueue", queue, id));
        }
    }

    public static class OffheapCacheTest extends CacheTest {

        private static final long ENTRY_SIZE = 40L;
        private static final int SEGMENTS = 4;
        private final HugeGraph graph = Mockito.mock(HugeGraph.class);

        @Override
        protected Cache<Id, Object> newCache() {
            return newCache(10000L);
        }

        @Override
        protected Cache<Id, Object> newCache(long capacity) {
            return new OffheapCache(this.graph(), capacity, ENTRY_SIZE, SEGMENTS);
        }

        @Override
        protected void checkSize(Cache<Id, Object> cache, long size,
                                 Map<Id, Object> kvs) {
            // NOTE: offheap cache is calculated based on bytes, not accurate
            long apprSize = (long) (size * 1.2);
            Assert.assertLte(apprSize, cache.size());
            if (kvs != null) {
                long matched = 0L;
                for (Map.Entry<Id, Object> kv : kvs.entrySet()) {
                    Object value = cache.get(kv.getKey());
                    if (kv.getValue().equals(value)) {
                        matched++;
                    }
                }
                Assert.assertGt(0.8d, matched / (double) size);
            }
        }

        @Override
        protected void checkInCache(Cache<Id, Object> cache, Id id) {
            Assert.assertTrue(cache.containsKey(id));
        }

        @Override
        protected void checkNotInCache(Cache<Id, Object> cache, Id id) {
            Assert.assertFalse(cache.containsKey(id));
        }

        protected HugeGraph graph() {
            return this.graph;
        }

        @Test
        public void testUpdateAndGetWithInvalidDataType() {
            Cache<Id, Object> cache = newCache();
            Id id = IdGenerator.of("1");

            cache.update(id, 'c');
            Assert.assertNull(cache.get(id));

            cache.update(id, new Object());
            Assert.assertNull(cache.get(id));

            cache.update(id, new byte[]{1});
            Assert.assertNull(cache.get(id));

            cache.update(id, "string");
            Assert.assertEquals("string", cache.get(id));
        }
    }

    public static class LevelCacheTest extends OffheapCacheTest {

        @Override
        protected Cache<Id, Object> newCache() {
            return newCache(10000L);
        }

        @Override
        protected Cache<Id, Object> newCache(long capacity) {
            RamCache l1cache = new RamCache(capacity);
            OffheapCache l2cache = (OffheapCache) super.newCache(capacity);
            return new LevelCache(l1cache, l2cache);
        }

        @Test
        @Override
        public void testUpdateAndGetWithInvalidDataType() {
            // LevelCache includes level-1 RamCache, which can cache any object
            Cache<Id, Object> cache = newCache();
            Id id = IdGenerator.of("1");

            cache.update(id, 'c');
            Assert.assertEquals('c', cache.get(id));

            Object obj = new Object();
            cache.update(id, obj);
            Assert.assertEquals(obj, cache.get(id));

            byte[] bytes = new byte[]{1};
            cache.update(id, bytes);
            Assert.assertArrayEquals(bytes, (byte[]) cache.get(id));

            cache.update(id, "string");
            Assert.assertEquals("string", cache.get(id));
        }
    }

    @Test
    public void testUpdateAndGet() {
        Cache<Id, Object> cache = newCache();
        Id id = IdGenerator.of("1");
        Assert.assertNull(cache.get(id));

        cache.update(id, "value-1");
        Assert.assertEquals("value-1", cache.get(id));

        cache.update(id, "value-2");
        Assert.assertEquals("value-2", cache.get(id));
    }

    @Test
    public void testUpdateAndGetWithDataType() {
        Cache<Id, Object> cache = newCache();
        Id id = IdGenerator.of("1");
        Assert.assertNull(cache.get(id));

        /*
         * BOOLEAN
         * BYTE
         * STRING
         * INT
         * LONG
         * FLOAT
         * DOUBLE
         * DATE
         * UUID
         * BLOB
         * LIST
         */
        cache.update(id, true);
        Assert.assertEquals(true, cache.get(id));
        cache.update(id, false);
        Assert.assertEquals(false, cache.get(id));

        cache.update(id, (byte) 123);
        Assert.assertEquals((byte) 123, cache.get(id));

        cache.update(id, "string");
        Assert.assertEquals("string", cache.get(id));

        cache.update(id, 123);
        Assert.assertEquals(123, cache.get(id));

        cache.update(id, 999999999999L);
        Assert.assertEquals(999999999999L, cache.get(id));

        cache.update(id, 3.14f);
        Assert.assertEquals(3.14f, cache.get(id));

        cache.update(id, 0.123456789d);
        Assert.assertEquals(0.123456789d, cache.get(id));

        Date now = new Date();
        cache.update(id, now);
        Assert.assertEquals(now, cache.get(id));

        UUID uuid = UUID.randomUUID();
        cache.update(id, uuid);
        Assert.assertEquals(uuid, cache.get(id));

        byte[] bytes = new byte[]{1, 33, 88};
        cache.update(id, Blob.wrap(bytes));
        Assert.assertEquals(Blob.wrap(bytes), cache.get(id));

        List<Integer> list = ImmutableList.of(1, 3, 5);
        cache.update(id, list);
        Assert.assertEquals(list, cache.get(id));

        List<Long> list2 = ImmutableList.of(1L, 3L, 5L);
        cache.update(id, list2);
        Assert.assertEquals(list2, cache.get(id));

        List<Double> list3 = ImmutableList.of(1.2, 3.4, 5.678);
        cache.update(id, list3);
        Assert.assertEquals(list3, cache.get(id));

        List<Object> listAny = ImmutableList.of(1, 2L, 3.4f, 5.678d, "string");
        cache.update(id, listAny);
        Assert.assertEquals(listAny, cache.get(id));
    }

    @Test
    public void testUpdateAndGetWithSameSizeAndCapacity() {
        int limit = 1000;
        Cache<Id, Object> cache = newCache(limit);
        Map<Id, Object> map = new LimitMap(limit);

        for (int i = 0; i < limit; i++) {
            Id id = IdGenerator.of("key-" + i);
            cache.update(id, "value-" + i);
            map.put(id, "value-" + i);
        }
        this.checkSize(cache, limit, map);
    }

    @Test
    public void testGetOrFetch() {
        Cache<Id, Object> cache = newCache();
        Id id = IdGenerator.of("1");
        Assert.assertNull(cache.get(id));

        Assert.assertEquals("value-1", cache.getOrFetch(id, key -> {
            return "value-1";
        }));

        cache.update(id, "value-2");
        Assert.assertEquals("value-2", cache.getOrFetch(id, key -> {
            return "value-1";
        }));
    }

    @Test
    public void testUpdateIfAbsent() {
        Cache<Id, Object> cache = newCache();
        Id id = IdGenerator.of("1");
        cache.updateIfAbsent(id, "value-1");
        Assert.assertEquals("value-1", cache.get(id));
    }

    @Test
    public void testUpdateIfAbsentWithExistKey() {
        Cache<Id, Object> cache = newCache();
        Id id = IdGenerator.of("1");
        cache.update(id, "value-1");
        cache.updateIfAbsent(id, "value-2");
        Assert.assertEquals("value-1", cache.get(id));
    }

    @Test
    public void testUpdateIfPresent() {
        Cache<Id, Object> cache = newCache();
        Id id = IdGenerator.of("1");
        cache.updateIfPresent(id, "value-1");
        Assert.assertEquals(null, cache.get(id));

        cache.update(id, "value-1");
        Assert.assertEquals("value-1", cache.get(id));
        cache.updateIfPresent(id, "value-2");
        Assert.assertEquals("value-2", cache.get(id));
    }

    @Test
    public void testInvalidate() {
        Cache<Id, Object> cache = newCache();
        Id id = IdGenerator.of("1");
        cache.update(id, "value-1");
        Assert.assertEquals("value-1", cache.get(id));
        Assert.assertEquals(1L, cache.size());

        this.checkInCache(cache, id);

        cache.invalidate(id);
        Assert.assertEquals(null, cache.get(id));
        Assert.assertEquals(0L, cache.size());

        this.checkNotInCache(cache, id);
    }

    @Test
    public void testClear() {
        Cache<Id, Object> cache = newCache();
        Id id = IdGenerator.of("1");
        cache.update(id, "value-1");
        Assert.assertEquals("value-1", cache.get(id));
        id = IdGenerator.of("2");
        cache.update(id, "value-2");
        cache.clear();
        Assert.assertEquals(null, cache.get(id));
        Assert.assertEquals(0, cache.size());
    }

    @Test
    public void testCapacity() {
        Cache<Id, Object> cache = newCache(10);
        Assert.assertEquals(10, cache.capacity());

        cache = newCache(1024);
        Assert.assertEquals(1024, cache.capacity());

        cache = newCache(8);
        Assert.assertEquals(8, cache.capacity());

        cache = newCache(1);
        Assert.assertEquals(1, cache.capacity());

        cache = newCache(0);
        Assert.assertEquals(0, cache.capacity());

        int huge = (int) (200 * Bytes.GB);
        cache = newCache(huge);
        Assert.assertEquals(huge, cache.capacity());

        // The min capacity is 0
        cache = newCache(-1);
        Assert.assertEquals(0, cache.capacity());
    }

    @Test
    public void testSize() {
        Cache<Id, Object> cache = newCache();
        Id id = IdGenerator.of("1");
        cache.update(id, "value-1");
        id = IdGenerator.of("2");
        cache.update(id, "value-2");
        Assert.assertEquals(2, cache.size());
    }

    @Test
    public void testSizeWithReachCapacity() {
        int limit = 1000;
        Cache<Id, Object> cache = newCache(limit);
        Map<Id, Object> map = new LimitMap(limit);

        for (int i = 0; i < 5 * limit; i++) {
            Id id = IdGenerator.of("key-" + i);
            cache.update(id, "value-" + i);
            map.put(id, "value-" + i);
        }
        this.checkSize(cache, limit, map);
    }

    @Test
    public void testHitsAndMiss() {
        Cache<Id, Object> cache = newCache();
        Assert.assertEquals(false, cache.enableMetrics(true));

        Assert.assertEquals(0L, cache.hits());
        Assert.assertEquals(0L, cache.miss());

        Id id = IdGenerator.of("1");
        cache.update(id, "value-1");
        Assert.assertEquals(0L, cache.hits());
        Assert.assertEquals(0L, cache.miss());

        cache.get(IdGenerator.of("not-exist"));
        Assert.assertEquals(0L, cache.hits());
        Assert.assertEquals(1L, cache.miss());

        cache.get(IdGenerator.of("1"));
        Assert.assertEquals(1L, cache.hits());
        Assert.assertEquals(1L, cache.miss());

        cache.get(IdGenerator.of("not-exist"));
        Assert.assertEquals(1L, cache.hits());
        Assert.assertEquals(2L, cache.miss());

        cache.get(IdGenerator.of("1"));
        Assert.assertEquals(2L, cache.hits());
        Assert.assertEquals(2L, cache.miss());
    }

    @Test
    public void testEnableMetrics() {
        Cache<Id, Object> cache = newCache();
        Assert.assertEquals(false, cache.enableMetrics(false));
        Assert.assertEquals(false, cache.enableMetrics(true));

        Assert.assertEquals(0L, cache.hits());
        Assert.assertEquals(0L, cache.miss());

        Id id = IdGenerator.of("1");
        cache.update(id, "value-1");
        Assert.assertEquals(0L, cache.hits());
        Assert.assertEquals(0L, cache.miss());

        cache.get(IdGenerator.of("not-exist"));
        Assert.assertEquals(0L, cache.hits());
        Assert.assertEquals(1L, cache.miss());

        cache.get(IdGenerator.of("1"));
        Assert.assertEquals(1L, cache.hits());
        Assert.assertEquals(1L, cache.miss());

        cache.get(IdGenerator.of("not-exist"));
        Assert.assertEquals(1L, cache.hits());
        Assert.assertEquals(2L, cache.miss());

        cache.get(IdGenerator.of("1"));
        Assert.assertEquals(2L, cache.hits());
        Assert.assertEquals(2L, cache.miss());

        Assert.assertEquals(true, cache.enableMetrics(false));

        Assert.assertEquals(0L, cache.hits());
        Assert.assertEquals(0L, cache.miss());

        cache.get(IdGenerator.of("not-exist"));
        cache.get(IdGenerator.of("1"));
        Assert.assertEquals(0L, cache.hits());
        Assert.assertEquals(0L, cache.miss());
    }

    @Test
    public void testExpire() {
        Cache<Id, Object> cache = newCache();

        Assert.assertEquals(0L, cache.expire());
        cache.expire(2000L); // 2 seconds
        Assert.assertEquals(2000L, cache.expire());

        cache.update(IdGenerator.of("1"), "value-1");
        cache.update(IdGenerator.of("2"), "value-2");

        Assert.assertEquals(2, cache.size());

        waitTillNext(2);
        cache.tick();

        Assert.assertNull(cache.get(IdGenerator.of("1")));
        Assert.assertNull(cache.get(IdGenerator.of("2")));
        Assert.assertEquals(0, cache.size());
    }

    @Test
    public void testExpireWithAddNewItem() {
        Cache<Id, Object> cache = newCache();
        cache.expire(2000L); // 2s

        cache.update(IdGenerator.of("1"), "value-1");
        cache.update(IdGenerator.of("2"), "value-2");

        Assert.assertEquals(2, cache.size());

        waitTillNext(1);
        cache.tick();

        cache.update(IdGenerator.of("3"), "value-3");
        cache.tick();

        Assert.assertEquals(3, cache.size());

        waitTillNext(1);
        cache.tick();

        Assert.assertNull(cache.get(IdGenerator.of("1")));
        Assert.assertNull(cache.get(IdGenerator.of("2")));
        Assert.assertNotNull(cache.get(IdGenerator.of("3")));
        Assert.assertEquals(1, cache.size());

        waitTillNext(1);
        cache.tick();

        // NOTE: OffheapCache should expire item by access
        Assert.assertNull(cache.get(IdGenerator.of("3")));
        Assert.assertEquals(0, cache.size());
    }

    @Test
    public void testExpireWithZeroSecond() {
        Cache<Id, Object> cache = newCache();
        cache.update(IdGenerator.of("1"), "value-1");
        cache.update(IdGenerator.of("2"), "value-2");

        Assert.assertEquals(2, cache.size());

        cache.expire(0);
        waitTillNext(1);
        cache.tick();

        Assert.assertEquals(2, cache.size());
    }

    private static final int THREADS_NUM = 8;

    @Test
    public void testMultiThreadsUpdate() {
        Cache<Id, Object> cache = newCache(THREADS_NUM * 10000 * 10);

        runWithThreads(THREADS_NUM, () -> {
            for (int i = 0; i < 10000 * 10; i++) {
                Id id = IdGenerator.of(
                        Thread.currentThread().getName() + "-" + i);
                cache.update(id, "value-" + i);
            }
        });
        Assert.assertEquals(THREADS_NUM * 10000 * 10, cache.size());
    }

    @Test
    public void testMultiThreadsUpdateWithGtCapacity() {
        int limit = 80;
        Cache<Id, Object> cache = newCache(limit);

        runWithThreads(THREADS_NUM, () -> {
            int size = 10000 * 100;
            for (int i = 0; i < size; i++) {
                Id id = IdGenerator.of(
                        Thread.currentThread().getName() + "-" + i);
                cache.update(id, "value-" + i);
            }
        });
        this.checkSize(cache, limit, null);
    }

    @Test
    public void testMultiThreadsUpdateAndCheck() {
        Cache<Id, Object> cache = newCache();

        runWithThreads(THREADS_NUM, () -> {
            Map<Id, Object> all = new HashMap<>(1000);

            for (int i = 0; i < 1000; i++) {
                Id id = IdGenerator.of(Thread.currentThread().getName() +
                                       "-" + i);
                String value = "value-" + i;
                cache.update(id, value);

                all.put(id, value);
            }

            for (Map.Entry<Id, Object> entry : all.entrySet()) {
                Id key = entry.getKey();
                Assert.assertEquals(entry.getValue(), cache.get(key));
                this.checkInCache(cache, key);
            }
        });
        Assert.assertEquals(THREADS_NUM * 1000, cache.size());
    }

    @Test
    public void testMultiThreadsGetWith2Items() {
        Cache<Id, Object> cache = newCache(10);

        Id id1 = IdGenerator.of("1");
        cache.update(id1, "value-1");
        Id id2 = IdGenerator.of("2");
        cache.update(id2, "value-2");
        Assert.assertEquals(2, cache.size());

        runWithThreads(THREADS_NUM, () -> {
            for (int i = 0; i < 10000 * 100; i++) {
                Assert.assertEquals("value-1", cache.get(id1));
                Assert.assertEquals("value-2", cache.get(id2));
                Assert.assertEquals("value-1", cache.get(id1));
            }
        });
        Assert.assertEquals(2, cache.size());
    }

    @Test
    public void testMultiThreadsGetWith3Items() {
        Cache<Id, Object> cache = newCache(10);

        Id id1 = IdGenerator.of("1");
        cache.update(id1, "value-1");
        Id id2 = IdGenerator.of("2");
        cache.update(id2, "value-2");
        Id id3 = IdGenerator.of("3");
        cache.update(id3, "value-3");
        Assert.assertEquals(3, cache.size());

        runWithThreads(THREADS_NUM, () -> {
            for (int i = 0; i < 10000 * 100; i++) {
                Assert.assertEquals("value-1", cache.get(id1));
                Assert.assertEquals("value-2", cache.get(id2));
                Assert.assertEquals("value-2", cache.get(id2));
                Assert.assertEquals("value-3", cache.get(id3));
                Assert.assertEquals("value-1", cache.get(id1));
            }
        });
        Assert.assertEquals(3, cache.size());
    }

    @Test
    public void testMultiThreadsGetAndUpdate() {
        Cache<Id, Object> cache = newCache(10);

        Id id1 = IdGenerator.of("1");
        cache.update(id1, "value-1");
        Id id2 = IdGenerator.of("2");
        cache.update(id2, "value-2");
        Id id3 = IdGenerator.of("3");
        cache.update(id3, "value-3");
        Assert.assertEquals(3, cache.size());

        runWithThreads(THREADS_NUM, () -> {
            for (int i = 0; i < 10000 * 100; i++) {
                Assert.assertEquals("value-1", cache.get(id1));
                Assert.assertEquals("value-2", cache.get(id2));
                Assert.assertEquals("value-2", cache.get(id2));
                Assert.assertEquals("value-3", cache.get(id3));
                cache.update(id2, "value-2");
                Assert.assertEquals("value-1", cache.get(id1));
            }
        });
        Assert.assertEquals(3, cache.size());
    }

    @Test
    public void testMultiThreadsGetAndUpdateWithGtCapacity() {
        Cache<Id, Object> cache = newCache(10);

        runWithThreads(THREADS_NUM, () -> {
            for (int i = 0; i < 10000 * 20; i++) {
                for (int k = 0; k < 15; k++) {
                    Id id = IdGenerator.of(k);
                    Object value = cache.get(id);
                    if (value != null) {
                        Assert.assertEquals("value-" + k, value);
                    } else {
                        cache.update(id, "value-" + k);
                    }
                }
            }
        });
        // In fact, the size may be any value(such as 43)
        Assert.assertTrue(cache.size() < 10 + THREADS_NUM);
    }

    @Test
    public void testKeyExpired() {
        Cache<Id, Object> cache = newCache();
        cache.expire(2000L);

        Id key = IdGenerator.of("key");
        cache.update(key, "value", -1000L);

        waitTillNext(1);
        cache.tick();

        Assert.assertFalse(cache.containsKey(key));

        cache.update(key, "value", -2000L);
        cache.tick();

        Assert.assertFalse(cache.containsKey(key));
    }
}
