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

package org.apache.hugegraph.unit.util.collection;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.unit.BaseUnitTest;
import org.apache.hugegraph.util.collection.IntIterator;
import org.apache.hugegraph.util.collection.IntMap;
import org.apache.hugegraph.util.collection.IntMapByDynamicHash;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class IntMapTest extends BaseUnitTest {

    @Before
    public void setup() {
        // pass
    }

    @After
    public void teardown() throws Exception {
        // pass
    }

    static final int THREADS_NUM = 4;
    static final int BATCH_COUNT = 2000;
    static final int EACH_COUNT = 10000;

    @Test
    public void testIntFixedMap() {
        IntMap map = fixed(EACH_COUNT);
        testIntMap(map);
    }

    @Test
    public void testIntFixedMapBySegments() {
        IntMap map = fixedBySegments(EACH_COUNT, 4);
        testIntMap(map);

        IntMap map2 = fixedBySegments(EACH_COUNT, 400);
        testIntMap(map2);

        IntMap map3 = fixedBySegments(EACH_COUNT, 4000);
        testIntMap(map3);
    }

    private void testIntMap(IntMap map) {
        Assert.assertEquals(0, map.size());
        Map<Integer, Integer> jucMap = new HashMap<>();

        Assert.assertEquals(0, map.size());
        Assert.assertTrue(map.concurrent());

        int modUpdate = 1 + new Random().nextInt(100);
        Random random = new Random();
        for (int k = 0; k < EACH_COUNT; k++) {
            if (k % modUpdate == 0) {
                int v = random.nextInt(EACH_COUNT);
                map.put(k, v);
                jucMap.put(k, v);
            }
        }

        Assert.assertEquals(jucMap.size(), map.size());
        for (int k = 0; k < EACH_COUNT; k++) {
            if (jucMap.containsKey(k)) {
                Assert.assertTrue("expect " + k, map.containsKey(k));
                Assert.assertEquals((int) jucMap.get(k), map.get(k));
            } else {
                Assert.assertFalse("unexpect " + k, map.containsKey(k));
            }
        }

        for (int k = 0; k < EACH_COUNT; k++) {
            int v = random.nextInt(EACH_COUNT);
            map.put(k, v);
            jucMap.put(k, v);
        }

        Assert.assertEquals(EACH_COUNT, map.size());
        for (int k = 0; k < EACH_COUNT; k++) {
            Assert.assertTrue("expect " + k, map.containsKey(k));
            Assert.assertEquals((int) jucMap.get(k), map.get(k));
        }

        int modRemove = 1 + new Random().nextInt(100);
        for (int k = 0; k < EACH_COUNT; k++) {
            if (k % modRemove == 0) {
                map.remove(k);
                jucMap.remove(k);
            }
        }

        Assert.assertEquals(jucMap.size(), map.size());
        for (int k = 0; k < EACH_COUNT; k++) {
            if (jucMap.containsKey(k)) {
                Assert.assertTrue("expect " + k, map.containsKey(k));
                Assert.assertEquals((int) jucMap.get(k), map.get(k));
            } else {
                Assert.assertFalse("unexpect " + k, map.containsKey(k));
            }
        }

        int outOfBoundKey = EACH_COUNT;

        Assert.assertFalse(map.containsKey(outOfBoundKey));

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            map.put(outOfBoundKey, 0);
        }, e -> {
            Assert.assertContains("out of bound", e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            map.remove(outOfBoundKey);
        }, e -> {
            Assert.assertContains("out of bound", e.getMessage());
        });

        map.clear();
        Assert.assertEquals(0, map.size());
        for (int k = 0; k < EACH_COUNT; k++) {
            Assert.assertFalse("unexpect " + k, map.containsKey(k));
        }
    }

    @Test
    public void testIntFixedMapConcurrent() {
        IntMap map = fixed(EACH_COUNT);

        runWithThreads(THREADS_NUM, () -> {
            for (int i = 0; i < BATCH_COUNT; i++) {
                for (int k = 0; k < EACH_COUNT; k++) {
                    map.containsKey(k);
                    map.put(k, k);
                }
                map.get(i);
            }
        });

        Assert.assertEquals(EACH_COUNT, map.size());
        for (int k = 0; k < EACH_COUNT; k++) {
            Assert.assertTrue("expect " + k, map.containsKey(k));
        }
    }

    @Test
    public void testIntFixedMapBySegmentsConcurrent() {
        IntMap map = fixedBySegments(EACH_COUNT, 4);

        runWithThreads(THREADS_NUM, () -> {
            for (int i = 0; i < BATCH_COUNT; i++) {
                for (int k = 0; k < EACH_COUNT; k++) {
                    map.containsKey(k);
                    map.put(k, k);
                }
                map.get(i);
            }
        });

        Assert.assertEquals(EACH_COUNT, map.size());
        for (int k = 0; k < EACH_COUNT; k++) {
            Assert.assertTrue("expect " + k, map.containsKey(k));
        }
    }

    @Test
    public void testIntFixedMapBySegmentsPut() {
        BiFunction<Integer, Integer, Integer> testMap = (capacity, segs) -> {
            IntMap map = fixedBySegments(capacity, segs);

            for (int i = 0; i < capacity; i++) {
                Assert.assertTrue(map.put(i, i * i));
                Assert.assertEquals(i * i, map.get(i));
                Assert.assertTrue(map.containsKey(i));
                Assert.assertFalse(map.containsKey(i + 1));

                int u = -i - 1;
                Assert.assertTrue(map.put(u, u));
                Assert.assertEquals(u, map.get(u));
                Assert.assertTrue(map.containsKey(u));
                Assert.assertFalse(map.containsKey(u - 1));
            }

            IntIterator values = map.values();
            for (int i = -capacity; i < 0; i++) {
                Assert.assertTrue(values.hasNext());
                Assert.assertEquals(i, values.next());
            }
            for (int i = 0; i < capacity; i++) {
                Assert.assertTrue(values.hasNext());
                Assert.assertEquals(i * i, values.next());
            }

            IntIterator keys = map.keys();
            for (int i = -capacity; i < 0; i++) {
                Assert.assertTrue(keys.hasNext());
                Assert.assertEquals(i, keys.next());
            }
            for (int i = 0; i < capacity; i++) {
                Assert.assertTrue(keys.hasNext());
                Assert.assertEquals(i, keys.next());
            }

            Assert.assertThrows(IllegalArgumentException.class, () -> {
                map.put(capacity, 1);
            }, e -> {
                Assert.assertContains("out of bound", e.getMessage());
            });

            Assert.assertThrows(IllegalArgumentException.class, () -> {
                map.put(capacity + 1, 1);
            }, e -> {
                Assert.assertContains("out of bound", e.getMessage());
            });

            return map.size() / 2;
        };

        Assert.assertEquals(10, testMap.apply(10, 1));
        Assert.assertEquals(10, testMap.apply(10, 2));
        Assert.assertEquals(10, testMap.apply(10, 3));
        Assert.assertEquals(10, testMap.apply(10, 4));
        Assert.assertEquals(10, testMap.apply(10, 5));
        Assert.assertEquals(10, testMap.apply(10, 7));
        Assert.assertEquals(10, testMap.apply(10, 9));
        Assert.assertEquals(10, testMap.apply(10, 10));

        Assert.assertEquals(2000, testMap.apply(2000, 1));
        Assert.assertEquals(2000, testMap.apply(2000, 2));
        Assert.assertEquals(2000, testMap.apply(2000, 3));
        Assert.assertEquals(2000, testMap.apply(2000, 100));
        Assert.assertEquals(2000, testMap.apply(2000, 999));
        Assert.assertEquals(2000, testMap.apply(2000, 1000));
        Assert.assertEquals(2000, testMap.apply(2000, 1001));
        Assert.assertEquals(2000, testMap.apply(2000, 1999));
        Assert.assertEquals(2000, testMap.apply(2000, 2000));

        Assert.assertEquals(10000000, testMap.apply(10000000, 100));

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            Assert.assertEquals(10, testMap.apply(10, 0));
        }, e -> {
            Assert.assertContains("Invalid segments", e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            Assert.assertEquals(10, testMap.apply(10, 11));
        }, e -> {
            Assert.assertContains("Invalid capacity", e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            Assert.assertEquals(2000, testMap.apply(2000, 2001));
        }, e -> {
            Assert.assertContains("Invalid capacity", e.getMessage());
        });
    }

    @Test
    public void testIntFixedMapBySegmentsPutRandom() {
        int count = 100;
        BiFunction<Integer, Integer, Integer> testMap = (capacity, segs) -> {
            IntMap map = fixedBySegments(capacity, segs);

            Random random = new Random();
            for (int i = 0; i < count; i++) {
                int k = random.nextInt(capacity);

                Assert.assertTrue(map.put(k, k * k));
                Assert.assertEquals(k * k, map.get(k));
                Assert.assertTrue(map.containsKey(k));

                int u = -k - 1;
                Assert.assertTrue(map.put(u, u));
                Assert.assertEquals(u, map.get(u));
                Assert.assertTrue(map.containsKey(u));

                int r = i % 2 == 0 ? k : u;
                Assert.assertTrue(map.containsKey(r));
                Assert.assertTrue(map.remove(r));
                Assert.assertFalse(map.containsKey(r));
            }

            return map.size();
        };

        Assert.assertEquals(count, testMap.apply(100000000, 100));
        Assert.assertEquals(count, testMap.apply(Integer.MAX_VALUE, 40000));
    }

    @Test
    public void testIntFixedMapBySegmentsKeys() {
        IntMap map = fixedBySegments(Integer.MAX_VALUE, 40);
        map.put(Integer.MAX_VALUE - 1, 1);

        Assert.assertEquals(1, IteratorUtils.count(map.keys().asIterator()));

        for (int i = 0; i < 10000; i++) {
            IntIterator iter = map.keys();
            Assert.assertTrue(iter.hasNext());
            Assert.assertEquals(Integer.MAX_VALUE - 1, iter.next());
            Assert.assertFalse(iter.hasNext());

            Assert.assertThrows(NoSuchElementException.class, iter::next, e -> {
                Assert.assertNull(e.getMessage());
            });
        }
    }

    @Test
    public void testIntFixedMapBySegmentsKeysWithMultiSegs() {
        int segments = 400;
        int segmentSize = Integer.MAX_VALUE / segments;
        int step = 50;
        IntMap map = fixedBySegments(Integer.MAX_VALUE, segments);
        for (int k = 0; k < segments; k += step) {
            map.put(segmentSize * k, k);
        }

        Assert.assertEquals(map.size(),
                            IteratorUtils.count(map.keys().asIterator()));

        for (int i = 0; i < 10; i++) {
            IntIterator iter = map.keys();
            for (int k = 0; k < segments; k += step) {
                Assert.assertTrue(iter.hasNext());
                Assert.assertEquals(segmentSize * k, iter.next());
            }

            Assert.assertFalse(iter.hasNext());
            Assert.assertThrows(NoSuchElementException.class, iter::next, e -> {
                Assert.assertNull(e.getMessage());
            });
        }
    }

    @Test
    public void testIntFixedMapBySegmentsValues() {
        IntMap map = fixedBySegments(Integer.MAX_VALUE, 40);
        map.put(Integer.MAX_VALUE - 1, 1);

        Assert.assertEquals(1, IteratorUtils.count(map.values().asIterator()));

        for (int i = 0; i < 10; i++) {
            IntIterator iter = map.values();
            Assert.assertTrue(iter.hasNext());
            Assert.assertEquals(1, iter.next());
            Assert.assertFalse(iter.hasNext());

            Assert.assertThrows(NoSuchElementException.class, iter::next, e -> {
                Assert.assertNull(e.getMessage());
            });
        }
    }

    @Test
    public void testIntFixedMapBySegmentsValuesWithMultiSegs() {
        int segments = 400;
        int segmentSize = Integer.MAX_VALUE / segments;
        int step = 50;
        IntMap map = fixedBySegments(Integer.MAX_VALUE, segments);
        for (int k = 0; k < segments; k += step) {
            map.put(segmentSize * k, k);
        }

        Assert.assertEquals(map.size(),
                            IteratorUtils.count(map.values().asIterator()));

        for (int i = 0; i < 10; i++) {
            IntIterator iter = map.values();
            for (int k = 0; k < segments; k += step) {
                Assert.assertTrue(iter.hasNext());
                Assert.assertEquals(k, iter.next());
            }

            Assert.assertFalse(iter.hasNext());
            Assert.assertThrows(NoSuchElementException.class, iter::next, e -> {
                Assert.assertNull(e.getMessage());
            });
        }
    }

    @Test
    public void testIntMapByDynamicHashSingleThread() {
        IntMap map = new IntMapByDynamicHash();
        int mapSize = 2000;
        for (int i = 0; i < mapSize; i++) {
            map.put(i, i + 1);
            Assert.assertTrue(map.containsKey(i));
            Assert.assertFalse(map.containsKey(i + mapSize));
            Assert.assertEquals(i + 1, map.get(i));
        }

        for (int i = mapSize - 1; i >= 0; i--) {
            map.put(i, i - 1);
            Assert.assertTrue(map.containsKey(i));
            Assert.assertFalse(map.containsKey(i + mapSize));
            Assert.assertEquals(i - 1, map.get(i));
        }

        Assert.assertEquals(mapSize, map.size());
        map.clear();
        Assert.assertEquals(0, map.size());
    }

    @Test
    public void testIntMapByDynamicHashSparseKeysAcrossGrowth() {
        IntMap map = new IntMapByDynamicHash(4);
        int count = 32;
        int stride = 1024;
        for (int i = 0; i < count; i++) {
            Assert.assertTrue(map.put(i * stride, i + 1));
            Assert.assertEquals(i + 1, map.size());
            for (int k = 0; k <= i; k++) {
                Assert.assertTrue(map.containsKey(k * stride));
                Assert.assertEquals(k + 1, map.get(k * stride));
            }
        }
        for (int i = 0; i < count; i++) {
            Assert.assertTrue(map.put(i * stride, -i - 1));
            Assert.assertEquals(-i - 1, map.get(i * stride));
            Assert.assertEquals(count, map.size());
        }
        for (int i = 0; i < count; i++) {
            Assert.assertTrue(map.remove(i * stride));
            Assert.assertFalse(map.containsKey(i * stride));
            Assert.assertFalse(map.remove(i * stride));
            Assert.assertEquals(count - i - 1, map.size());
            for (int k = i + 1; k < count; k++) {
                Assert.assertEquals(-k - 1, map.get(k * stride));
            }
        }
    }

    @Test
    public void testIntMapByDynamicHashMultiThread() throws Exception {
        IntMap map = new IntMapByDynamicHash();
        int cpus = 16;
        int mapSize = 100;
        ExecutorService executor = Executors.newFixedThreadPool(cpus);
        List<Future<?>> workers = new ArrayList<>();
        try {
            for (int i = 0; i < cpus; i++) {
                int index = i;
                workers.add(executor.submit(() -> {
                    for (int j = 0; j < mapSize; j++) {
                        int key = j + index * mapSize;
                        Assert.assertTrue(map.put(key, j));
                        Assert.assertEquals(j, map.get(key));
                    }
                }));
            }
            for (Future<?> worker : workers) {
                worker.get(10, TimeUnit.SECONDS);
            }
            Assert.assertEquals(cpus * mapSize, map.size());
            for (int i = 0; i < cpus * mapSize; i++) {
                Assert.assertTrue(map.containsKey(i));
                Assert.assertEquals(i % mapSize, map.get(i));
            }
        } finally {
            executor.shutdownNow();
            Assert.assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testIntMapByDynamicHashConcurrentGrowth() throws Throwable {
        int threads = 16;
        int keysPerThread = 100;
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(55);
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        Throwable primaryFailure = null;
        try {
            // Each fresh map crosses multiple table generations; this is bounded work,
            // not a retry of a failed test or concurrent access to another worker's keys.
            for (int capacity : new int[]{4, 16, 64}) {
                for (int scenario = 0; scenario < 300; scenario++) {
                    IntMap map = new IntMapByDynamicHash(capacity);
                    CountDownLatch started = new CountDownLatch(threads);
                    CountDownLatch start = new CountDownLatch(1);
                    List<Future<?>> workers = new ArrayList<>();
                    try {
                        for (int worker = 0; worker < threads; worker++) {
                            int index = worker;
                            workers.add(executor.submit(() -> {
                                started.countDown();
                                Assert.assertTrue(start.await(5, TimeUnit.SECONDS));
                                for (int item = 0; item < keysPerThread; item++) {
                                    int key = index * keysPerThread + item;
                                    Assert.assertTrue(map.put(key, item));
                                    Assert.assertEquals(item, map.get(key));
                                    Assert.assertTrue(map.containsKey(key));
                                    if (item % 2 != 0) {
                                        Assert.assertTrue(map.remove(key));
                                        Assert.assertFalse(map.containsKey(key));
                                    }
                                }
                                return null;
                            }));
                        }
                        long remaining = deadline - System.nanoTime();
                        Assert.assertTrue("Concurrent growth budget exhausted", remaining > 0L);
                        Assert.assertTrue(started.await(Math.min(remaining, TimeUnit.SECONDS.toNanos(5)),
                                                        TimeUnit.NANOSECONDS));
                        start.countDown();
                        for (Future<?> worker : workers) {
                            remaining = deadline - System.nanoTime();
                            Assert.assertTrue("Concurrent growth budget exhausted", remaining > 0L);
                            worker.get(remaining, TimeUnit.NANOSECONDS);
                        }
                        Assert.assertEquals(threads * keysPerThread / 2, map.size());
                        for (int key = 0; key < threads * keysPerThread; key++) {
                            if (key % 2 == 0) {
                                Assert.assertTrue(map.containsKey(key));
                                Assert.assertEquals(key % keysPerThread, map.get(key));
                            } else {
                                Assert.assertFalse(map.containsKey(key));
                            }
                        }
                    } finally {
                        start.countDown();
                    }
                }
            }
        } catch (Throwable failure) {
            primaryFailure = failure;
            throw failure;
        } finally {
            try {
                executor.shutdownNow();
                Assert.assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
            } catch (Throwable failure) {
                if (primaryFailure == null) {
                    throw failure;
                }
                if (primaryFailure != failure) {
                    primaryFailure.addSuppressed(failure);
                }
            }
        }
    }

    @Test
    public void testIntMapByDynamicHashKeys() {
        IntMap map = new IntMapByDynamicHash();
        for (int i = 0; i < 10000; i++) {
            map.put(i, i + 100);
        }
        IntIterator iterator = map.keys();
        for (int i = 0; i < 10000; i++) {
            Assert.assertTrue(iterator.hasNext());
            Assert.assertEquals(i, iterator.next());
        }
        Assert.assertFalse(iterator.hasNext());
    }

    @Test
    public void testIntMapByDynamicHashValues() {
        IntMap map = new IntMapByDynamicHash();
        for (int i = 0; i < 10000; i++) {
            map.put(i, i + 100);
        }
        IntIterator iterator = map.values();
        for (int i = 0; i < 10000; i++) {
            Assert.assertTrue(iterator.hasNext());
            Assert.assertEquals(i + 100, iterator.next());
        }
        Assert.assertFalse(iterator.hasNext());
    }

    private IntMap fixed(int capacity) {
        return new IntMap.IntMapByFixedAddr(capacity);
    }

    private IntMap fixedBySegments(int capacity, int segments) {
        return new IntMap.IntMapBySegments(capacity, segments);
    }
}
