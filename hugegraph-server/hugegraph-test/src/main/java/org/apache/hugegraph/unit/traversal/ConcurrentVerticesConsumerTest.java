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

package org.apache.hugegraph.unit.traversal;

import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.config.CoreOptions;
import org.apache.hugegraph.exception.HugeException;
import org.apache.hugegraph.id.EdgeId;
import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.id.IdGenerator;
import org.apache.hugegraph.traversal.algorithm.HugeTraverser;
import org.apache.hugegraph.traversal.algorithm.KoutTraverser;
import org.apache.hugegraph.traversal.algorithm.OltpTraverser.ConcurrentVerticesConsumer;
import org.apache.hugegraph.type.define.CollectionType;
import org.apache.hugegraph.type.define.Directions;
import org.apache.hugegraph.util.Consumers;
import org.apache.hugegraph.util.collection.IdSet;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class ConcurrentVerticesConsumerTest {

    @Test
    public void testSerializeResultInsertion() throws Exception {
        assertSerialized(HugeTraverser.NO_LIMIT);
    }

    @Test
    public void testCheckLimitTogetherWithInsertion() throws Exception {
        assertSerialized(1L);
    }

    @Test
    public void testConcurrentStringIds() throws Exception {
        assertNativeResults(false, HugeTraverser.NO_LIMIT);
    }

    @Test
    public void testConcurrentNumericIds() throws Exception {
        assertNativeResults(true, HugeTraverser.NO_LIMIT);
    }

    @Test
    public void testConcurrentDuplicatesAndLimit() throws Exception {
        assertNativeResults(false, 32L);
        assertNativeResults(true, 32L);
    }

    @Test
    public void testSourceExcludedAndDuplicateDoNotCount() {
        Id source = IdGenerator.of("source");
        Id excluded = IdGenerator.of("excluded");
        Id first = IdGenerator.of("first");
        Id second = IdGenerator.of("second");
        Set<Id> neighbors = new IdSet(CollectionType.EC);
        ConcurrentVerticesConsumer consumer = new ConcurrentVerticesConsumer(
                source, Collections.singleton(excluded), 2L, neighbors);
        consumer.accept(edge(source));
        consumer.accept(edge(excluded));
        consumer.accept(edge(first));
        consumer.accept(edge(first));
        consumer.accept(edge(second));
        Assert.assertEquals(new HashSet<>(Arrays.asList(first, second)), neighbors);
        Assert.assertThrows(Consumers.StopExecution.class,
                            () -> consumer.accept(edge(IdGenerator.of("third"))));
    }

    @Test
    public void testKoutLayersNearestLimitAndCapacity() {
        Id marko = IdGenerator.of("marko");
        Id peter = IdGenerator.of("peter");
        Id ripple = IdGenerator.of("ripple");
        Id josh = IdGenerator.of("josh");
        Map<Id, List<Id>> edges = Map.of(
                marko, Arrays.asList(peter, ripple),
                peter, Arrays.asList(marko, josh, ripple),
                ripple, Arrays.asList(marko, peter));
        HugeGraph graph = Mockito.mock(HugeGraph.class);
        Mockito.when(graph.option(CoreOptions.OLTP_COLLECTION_TYPE)).thenReturn(CollectionType.EC);
        Mockito.when(graph.option(CoreOptions.OLTP_CONCURRENT_THREADS)).thenReturn(0);
        KoutTraverser traverser = new KoutTraverser(graph) {
            @Override
            protected void traverseIdsByBfs(Iterator<Id> sources, Directions dir,
                                            Id label, long degree, long capacity,
                                            Consumer<EdgeId> consumer) {
                ExecutorService executor = Executors.newFixedThreadPool(8);
                try {
                    List<Future<?>> futures = new ArrayList<>();
                    while (sources.hasNext()) {
                        Id source = sources.next();
                        futures.add(executor.submit(() -> {
                            for (Id target : edges.getOrDefault(source, Collections.emptyList())) {
                                try {
                                    consumer.accept(edge(target));
                                } catch (Consumers.StopExecution ignored) {
                                    break;
                                }
                            }
                        }));
                    }
                    for (Future<?> future : futures) {
                        future.get(10L, TimeUnit.SECONDS);
                    }
                } catch (Exception e) {
                    throw new AssertionError(e);
                } finally {
                    executor.shutdownNow();
                    try {
                        Assert.assertTrue(executor.awaitTermination(5L, TimeUnit.SECONDS));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new AssertionError(e);
                    }
                }
            }
        };
        Assert.assertEquals(Collections.singleton(josh), traverser.kout(
                marko, Directions.BOTH, null, 2, true, 100L, -1L, -1L));
        Assert.assertEquals(new HashSet<>(Arrays.asList(peter, ripple, josh)), traverser.kout(
                marko, Directions.BOTH, null, 2, false, 100L, -1L, -1L));
        Assert.assertEquals(1, traverser.kout(
                marko, Directions.BOTH, null, 2, false, 100L, -1L, 1L).size());
        Assert.assertThrows(HugeException.class, () -> traverser.kout(
                marko, Directions.BOTH, null, 2, false, 100L, 2L, 1L));
    }

    private static void assertSerialized(long limit) throws Exception {
        OverlapDetectingSet neighbors = new OverlapDetectingSet();
        ConcurrentVerticesConsumer consumer = new ConcurrentVerticesConsumer(
                IdGenerator.of("source"), null, limit, neighbors);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<?> first = executor.submit(() -> accept(consumer, IdGenerator.of("first")));
            Assert.assertTrue(neighbors.firstAdd.await(5L, TimeUnit.SECONDS));
            Future<?> second = executor.submit(() -> accept(consumer, IdGenerator.of("second")));
            first.get(10L, TimeUnit.SECONDS);
            second.get(10L, TimeUnit.SECONDS);
            Assert.assertEquals("Concurrent writes to the result set", 1, neighbors.maxActive.get());
            Assert.assertEquals(limit == 1L ? 1 : 2, neighbors.size());
        } finally {
            executor.shutdownNow();
            Assert.assertTrue(executor.awaitTermination(5L, TimeUnit.SECONDS));
        }
    }

    private static void assertNativeResults(boolean numeric, long limit) throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(8);
        try {
            for (int round = 0; round < 64; round++) {
                Set<Id> neighbors = new IdSet(CollectionType.EC);
                ConcurrentVerticesConsumer consumer = new ConcurrentVerticesConsumer(
                        IdGenerator.of("source"), null, limit, neighbors);
                CountDownLatch start = new CountDownLatch(1);
                List<Future<?>> futures = new ArrayList<>();
                Set<Id> expected = new HashSet<>();
                for (int key = 0; key < 64; key++) {
                    expected.add(id(numeric, key));
                }
                for (int worker = 0; worker < 8; worker++) {
                    futures.add(executor.submit(() -> {
                        await(start);
                        for (int key = 0; key < 64; key++) {
                            accept(consumer, id(numeric, key));
                        }
                    }));
                }
                start.countDown();
                for (Future<?> future : futures) {
                    future.get(10L, TimeUnit.SECONDS);
                }
                if (limit == HugeTraverser.NO_LIMIT) {
                    Assert.assertEquals(expected, neighbors);
                } else {
                    Assert.assertEquals(limit, neighbors.size());
                    Assert.assertTrue(expected.containsAll(neighbors));
                }
            }
        } finally {
            executor.shutdownNow();
            Assert.assertTrue(executor.awaitTermination(5L, TimeUnit.SECONDS));
        }
    }

    private static Id id(boolean numeric, int key) {
        return numeric ? IdGenerator.of((long) key) : IdGenerator.of("vertex-" + key);
    }

    private static void accept(ConcurrentVerticesConsumer consumer, Id target) {
        try {
            consumer.accept(edge(target));
        } catch (Consumers.StopExecution ignored) {
            // Reaching the finite result limit is normal traversal termination.
        }
    }

    private static EdgeId edge(Id target) {
        Id label = IdGenerator.of(1L);
        return new EdgeId(IdGenerator.of("owner"), Directions.OUT,
                          label, label, "", target);
    }

    private static void await(CountDownLatch latch) {
        try {
            Assert.assertTrue(latch.await(5L, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
    }

    private static class OverlapDetectingSet extends AbstractSet<Id> {

        private final Set<Id> values = Collections.synchronizedSet(new HashSet<>());
        private final CountDownLatch firstAdd = new CountDownLatch(1);
        private final CountDownLatch overlappingAdd = new CountDownLatch(1);
        private final AtomicInteger active = new AtomicInteger();
        private final AtomicInteger maxActive = new AtomicInteger();

        @Override
        public boolean add(Id id) {
            int active = this.active.incrementAndGet();
            this.maxActive.accumulateAndGet(active, Math::max);
            try {
                if (this.firstAdd.getCount() != 0L) {
                    this.firstAdd.countDown();
                    // Give the other worker a bounded opportunity to enter add().
                    this.overlappingAdd.await(2L, TimeUnit.SECONDS);
                } else {
                    this.overlappingAdd.countDown();
                }
                return this.values.add(id);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError(e);
            } finally {
                this.active.decrementAndGet();
            }
        }

        @Override
        public Iterator<Id> iterator() {
            return this.values.iterator();
        }

        @Override
        public int size() {
            return this.values.size();
        }
    }
}
