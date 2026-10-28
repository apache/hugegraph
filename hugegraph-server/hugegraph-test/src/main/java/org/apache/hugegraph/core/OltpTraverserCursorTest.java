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

package org.apache.hugegraph.core;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.configuration2.MapConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.hugegraph.HugeFactory;
import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.backend.query.QueryBatch;
import org.apache.hugegraph.backend.query.QueryResults;
import org.apache.hugegraph.backend.store.rocksdb.RocksDBSessions;
import org.apache.hugegraph.backend.tx.GraphTransaction;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.id.IdGenerator;
import org.apache.hugegraph.iterator.MapperIterator;
import org.apache.hugegraph.query.Query;
import org.apache.hugegraph.testutil.Utils;
import org.apache.hugegraph.testutil.Whitebox;
import org.apache.hugegraph.traversal.algorithm.OltpTraverser;
import org.apache.hugegraph.type.define.Directions;
import org.apache.hugegraph.util.Consumers;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.rocksdb.RocksIterator;

public class OltpTraverserCursorTest {

    @Before
    public void requireRocksDB() {
        Assume.assumeTrue("Native cursor regression requires RocksDB",
                          "rocksdb".equals(Utils.getConf().getString("backend")));
        CoreTestSuite.initEnv();
    }

    @Test
    public void testConsumerFailureClosesNativeCursor() throws Exception {
        this.checkNativeCursor("consumer");
    }

    @Test
    public void testStopExecutionClosesNativeCursor() throws Exception {
        this.checkNativeCursor("stop");
    }

    @Test
    public void testInterruptedConsumerClosesNativeCursor() throws Exception {
        this.checkNativeCursor("interrupt");
    }

    @Test
    public void testPrepareFailureClosesNativeCursor() throws Exception {
        this.checkNativeCursor("prepare");
    }

    @Test
    public void testCloseFailureIsSuppressedAfterNativeCursorCloses() throws Exception {
        this.checkNativeCursor("close");
    }

    @Test
    public void testAsyncCapacityStopClosesNativeCursor() throws Exception {
        this.checkNativeCursor("async-capacity");
    }

    @Test
    public void testAsyncCapacityStopPropagatesCloseFailure() throws Exception {
        this.checkNativeCursor("async-capacity-close");
    }

    @Test
    public void testAsyncCapacityStopReleasesBlockedProducer() throws Exception {
        this.checkNativeCursor("async-capacity-producer");
    }

    @Test
    public void testAsyncCapacityCloseFailureReleasesBlockedProducer() throws Exception {
        this.checkNativeCursor("async-capacity-producer-close");
    }

    @Test
    public void testAsyncConsumerFailureKeepsSuppressedCloseFailure() throws Exception {
        this.checkNativeCursor("async-consumer-close");
    }

    @Test
    public void testAsyncBusinessFailureReleasesBlockedProducer() throws Exception {
        this.checkNativeCursor("async-consumer-producer-close");
    }

    @Test
    public void testAsyncFatalErrorReleasesBlockedProducer() throws Exception {
        this.checkNativeCursor("async-fatal-producer");
    }

    @Test
    public void testCapacityStopEndsActualLazyTraversal() throws Exception {
        this.checkNativeCursor("lazy-producer");
    }

    private void checkNativeCursor(String outcome) throws Exception {
        Path directory = Files.createTempDirectory("oltp-cursor-");
        HugeConfig config = new HugeConfig(new MapConfiguration(Collections.emptyMap()));
        config.setProperty("backend", "rocksdb");
        config.setProperty("serializer", "binary");
        config.setProperty("store", "cursor_" + UUID.randomUUID().toString().replace("-", ""));
        config.setProperty("oltp.concurrent_threads", "lazy-producer".equals(outcome) ? 1 : 0);
        config.setProperty("rocksdb.data_path", directory.toString());
        config.setProperty("rocksdb.wal_path", directory.toString());
        HugeGraph graph = null;
        RocksDBSessions control = null;
        Iterator<Edge> edges = null;
        RocksIterator nativeCursor = null;
        int producerReads = -1;
        try {
            graph = HugeFactory.open(config);
            graph.initBackend();
            graph.schema().propertyKey("seq").asInt().create();
            graph.schema().vertexLabel("vertex").useCustomizeNumberId().create();
            graph.schema().edgeLabel("linked").sourceLabel("vertex").targetLabel("vertex")
                 .multiTimes().properties("seq").sortKeys("seq").create();
            Vertex source = graph.addVertex(T.label, "vertex", T.id, 1L);
            Vertex target = graph.addVertex(T.label, "vertex", T.id, 2L);
            // The backend prefetches 500 edges; record 501 keeps its native cursor open.
            for (int i = 0; i < 501; i++) {
                source.addEdge("linked", target, "seq", i);
            }
            graph.tx().commit();
            Id label = graph.schema().getEdgeLabel("linked").id();
            Query query = GraphTransaction.constructEdgesQuery(IdGenerator.of(1L), Directions.OUT,
                                                               new Id[]{label});
            edges = graph.edges(query);
            Assert.assertTrue(edges.hasNext());
            nativeCursor = nativeCursor(edges, Collections.newSetFromMap(new IdentityHashMap<>()));
            Assert.assertNotNull("Expected the real RocksDB scan cursor", nativeCursor);
            Assert.assertTrue(nativeCursor.isOwningHandle());
            Object store = graph.storeProvider().loadGraphStore(config);
            RocksDBSessions sessions = Whitebox.getInternalState(store, "sessions");
            // Keep the native DB alive during handle observation after caller FINISHED cleanup.
            control = sessions.copy(config, "cursor-control", "g");
            control.session();
            if ("lazy-producer".equals(outcome)) {
                // This isolated JVM fixture owns the single-worker pool used below.
                OltpTraverser.destroy();
            }
            Probe traverser = new Probe(graph);
            Throwable failure = outcome.contains("fatal") ? new AssertionError("fatal failure") :
                                "stop".equals(outcome) ? new Consumers.StopExecution("limit") :
                                new IllegalStateException("business failure");
            RuntimeException closeFailure = new IllegalStateException("close failure");
            if (outcome.endsWith("close")) {
                edges = new MapperIterator<Edge, Edge>(edges, edge -> edge) {
                    @Override
                    public void close() throws Exception {
                        super.close();
                        throw closeFailure;
                    }
                };
            }
            Iterator<Edge> input = edges;
            OltpTraverser.EdgesConsumer<Edge, ?> consumer;
            if ("prepare".equals(outcome)) {
                consumer = traverser.new EdgesConsumer<Edge, Id>(id -> Assert.fail(), OltpTraverser.NO_LIMIT) {
                    @Override
                    protected Iterator<Id> prepare(Iterator<Edge> iterator) {
                        if (failure instanceof Error error) {
                            throw error;
                        }
                        throw (RuntimeException) failure;
                    }
                };
            } else {
                consumer = traverser.new OneStepEdgeIterConsumer(id -> {
                    if (!outcome.startsWith("async-capacity") && !"lazy-producer".equals(outcome)) {
                        if (failure instanceof Error error) {
                            throw error;
                        }
                        throw (RuntimeException) failure;
                    }
                }, outcome.startsWith("async-capacity") || "lazy-producer".equals(outcome) ?
                   1L : OltpTraverser.NO_LIMIT);
            }
            if ("lazy-producer".equals(outcome)) {
                producerReads = this.consumeLazyProducer(traverser, consumer, input);
                Assert.assertEquals(501L, traverser.edgeIterCounter.get());
            } else if (outcome.startsWith("async-")) {
                AtomicReference<Throwable> handled = new AtomicReference<>();
                Throwable thrown = this.consumeAsync(consumer, input, handled, outcome);
                if (outcome.startsWith("async-capacity")) {
                    Assert.assertEquals(501L, traverser.edgeIterCounter.get());
                    if (outcome.endsWith("close")) {
                        Assert.assertNotNull("Close failure must remain visible after capacity Stop", thrown);
                        Assert.assertFalse(thrown instanceof Consumers.StopExecution);
                        Assert.assertTrue(handled.get() instanceof Consumers.StopExecution);
                        Assert.assertSame(handled.get().getSuppressed()[0], thrown);
                        Assert.assertSame(closeFailure, thrown.getCause());
                    } else {
                        Assert.assertNull(thrown);
                        Assert.assertTrue(handled.get() instanceof Consumers.StopExecution);
                    }
                } else {
                    Assert.assertSame(failure, thrown);
                    Assert.assertSame(failure, handled.get());
                    Assert.assertEquals(outcome.endsWith("close") ? 1 : 0, thrown.getSuppressed().length);
                    if (outcome.endsWith("close")) {
                        Assert.assertSame(closeFailure, thrown.getSuppressed()[0].getCause());
                    }
                }
            } else if ("interrupt".equals(outcome)) {
                Thread.currentThread().interrupt();
                try {
                    consumer.accept(input);
                    Assert.assertTrue(Thread.currentThread().isInterrupted());
                } finally {
                    Thread.interrupted();
                }
            } else {
                RuntimeException thrown = org.apache.hugegraph.testutil.Assert.assertThrows(
                        RuntimeException.class, () -> consumer.accept(input));
                Assert.assertSame(failure, thrown);
                if ("close".equals(outcome)) {
                    Assert.assertEquals(1, thrown.getSuppressed().length);
                    Assert.assertTrue(thrown.getSuppressed()[0].getCause().getMessage().contains("close failure"));
                }
            }
            HugeFactory.closeCurrentThreadTransactions();
            Assert.assertTrue("Control lease must keep the DB alive", control.databaseOpened());
            Assert.assertFalse("Traversal retained its native cursor after " + outcome,
                               nativeCursor.isOwningHandle());
            if ("lazy-producer".equals(outcome)) {
                Assert.assertEquals("Capacity Stop must end the lazy producer", 2, producerReads);
            }
        } finally {
            // These are teardown only; the native handle assertion above must run first.
            try {
                if (nativeCursor != null && nativeCursor.isOwningHandle()) {
                    nativeCursor.close();
                }
                if (edges != null && !outcome.endsWith("close")) {
                    CloseableIterator.closeIterator(edges);
                }
            } finally {
                try {
                    HugeFactory.closeCurrentThreadTransactions();
                    if (control != null) {
                        control.close();
                    }
                    if (graph != null && !graph.closed()) {
                        graph.drop();
                    }
                } finally {
                    if (graph != null) {
                        HugeFactory.remove(graph);
                    }
                    FileUtils.deleteDirectory(directory.toFile());
                }
            }
        }
    }

    private Throwable consumeAsync(OltpTraverser.EdgesConsumer<Edge, ?> consumer,
                                   Iterator<Edge> input, AtomicReference<Throwable> handled,
                                   String outcome) throws Exception {
        ExecutorService worker = Executors.newFixedThreadPool(1);
        ExecutorService waiter = Executors.newSingleThreadExecutor();
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        boolean fullQueue = outcome.startsWith("async-capacity") || outcome.contains("producer");
        try {
            Consumers<Iterator<Edge>> consumers = new Consumers<>(worker, iterator -> {
                entered.countDown();
                try {
                    if (!release.await(5L, TimeUnit.SECONDS)) {
                        throw new AssertionError("Consumer was not released");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(e);
                }
                consumer.accept(iterator);
            }, HugeFactory::closeCurrentThreadTransactions, handled::set, 1);
            consumers.start("cursor-close-failure");
            try {
                consumers.provide(input);
                Assert.assertTrue(entered.await(5L, TimeUnit.SECONDS));
                if (fullQueue) {
                    // One active input and two queued inputs fill the real queue.
                    consumers.provide(Collections.emptyIterator());
                    consumers.provide(Collections.emptyIterator());
                }
            } catch (Throwable failure) {
                throw new AssertionError(failure);
            }
            CountDownLatch calling = new CountDownLatch(1);
            Future<Throwable> result = waiter.submit(() -> {
                calling.countDown();
                Throwable failure = null;
                try {
                    if (outcome.contains("producer")) {
                        consumers.provide(Collections.emptyIterator());
                    }
                } catch (Consumers.StopExecution ignored) {
                    // Stop ends production successfully, as in Oltp's producer loop.
                } catch (Throwable providedFailure) {
                    failure = providedFailure;
                }
                try {
                    consumers.await();
                } catch (Throwable awaitedFailure) {
                    if (failure == null) {
                        failure = awaitedFailure;
                    }
                }
                return failure;
            });
            Assert.assertTrue(calling.await(5L, TimeUnit.SECONDS));
            if (fullQueue) {
                // The producer or await END enqueue must wait on the full queue.
                org.apache.hugegraph.testutil.Assert.assertThrows(
                        java.util.concurrent.TimeoutException.class,
                        () -> result.get(100L, TimeUnit.MILLISECONDS));
            }
            release.countDown();
            return result.get(3L, TimeUnit.SECONDS);
        } finally {
            release.countDown();
            waiter.shutdownNow();
            worker.shutdownNow();
            // The pre-fix producer can swallow one interrupt before entering await().
            for (int i = 0; i < 50 && !waiter.awaitTermination(100L, TimeUnit.MILLISECONDS); i++) {
                waiter.shutdownNow();
            }
            Assert.assertTrue(waiter.isTerminated());
            Assert.assertTrue(worker.awaitTermination(5L, TimeUnit.SECONDS));
        }
    }

    private int consumeLazyProducer(Probe traverser, OltpTraverser.EdgesConsumer<Edge, ?> consumer,
                                     Iterator<Edge> input) throws Exception {
        Consumers.ExecutorPool pool = Whitebox.getInternalState(OltpTraverser.class, "executors");
        ExecutorService worker = pool.getExecutor();
        pool.returnExecutor(worker);
        CountDownLatch releaseConsumer = new CountDownLatch(1);
        class LazyInputs implements Iterator<Iterator<Edge>>, AutoCloseable {

            private int reads;
            private boolean closed;

            @Override
            public boolean hasNext() {
                if (this.reads == 1) {
                    // The first provide() has returned; Stop now occurs before the second one.
                    releaseConsumer.countDown();
                    try {
                        // Runs after the real Consumers task and its worker transaction cleanup.
                        worker.submit(() -> { }).get(5L, TimeUnit.SECONDS);
                    } catch (Exception e) {
                        throw new AssertionError(e);
                    }
                }
                return this.reads < 100;
            }

            @Override
            public Iterator<Edge> next() {
                if (this.reads >= 100) {
                    throw new java.util.NoSuchElementException();
                }
                return this.reads++ == 0 ? input : Collections.emptyIterator();
            }

            @Override
            public void close() {
                this.closed = true;
            }
        }
        LazyInputs source = new LazyInputs();
        try {
            long total = traverser.traverseLazy(source, iterator -> {
                try {
                    Assert.assertTrue("Producer must return from its first provide()",
                                      releaseConsumer.await(5L, TimeUnit.SECONDS));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(e);
                }
                consumer.accept(iterator);
            });
            Assert.assertEquals((long) source.reads, total);
            Assert.assertTrue("Traversal must close its lazy producer", source.closed);
            return source.reads;
        } finally {
            releaseConsumer.countDown();
            OltpTraverser.destroy();
            Assert.assertTrue(worker.awaitTermination(5L, TimeUnit.SECONDS));
        }
    }

    private static RocksIterator nativeCursor(Object value, Set<Object> visited) throws Exception {
        if (value instanceof RocksIterator rocksIterator) {
            return rocksIterator;
        }
        if (!(value instanceof Iterator || value instanceof QueryResults || value instanceof QueryBatch) ||
            !visited.add(value)) {
            return null;
        }
        for (Class<?> type = value.getClass(); type != null &&
             type.getName().startsWith("org.apache.hugegraph."); type = type.getSuperclass()) {
            for (Field field : type.getDeclaredFields()) {
                if (Modifier.isStatic(field.getModifiers())) {
                    continue;
                }
                field.setAccessible(true);
                RocksIterator found = nativeCursor(field.get(value), visited);
                if (found != null) {
                    return found;
                }
            }
        }
        return null;
    }

    private static class Probe extends OltpTraverser {

        Probe(HugeGraph graph) {
            super(graph);
        }

        long traverseLazy(Iterator<Iterator<Edge>> source, java.util.function.Consumer<Iterator<Edge>> consumer) {
            return this.traverseByOne(source, consumer, "native-lazy-producer-stop");
        }
    }
}
