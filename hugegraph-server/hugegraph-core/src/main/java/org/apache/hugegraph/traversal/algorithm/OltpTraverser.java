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

package org.apache.hugegraph.traversal.algorithm;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.backend.id.EdgeId;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.query.EdgesQueryIterator;
import org.apache.hugegraph.config.CoreOptions;
import org.apache.hugegraph.iterator.FilterIterator;
import org.apache.hugegraph.iterator.MapperIterator;
import org.apache.hugegraph.structure.HugeEdge;
import org.apache.hugegraph.traversal.algorithm.steps.Steps;
import org.apache.hugegraph.type.define.Directions;
import org.apache.hugegraph.util.Consumers;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;

import com.google.common.base.Objects;

public abstract class OltpTraverser extends HugeTraverser
        implements AutoCloseable {

    private static final String EXECUTOR_NAME = "oltp";
    private static Consumers.ExecutorPool executors;

    protected OltpTraverser(HugeGraph graph) {
        super(graph);
        if (executors != null) {
            return;
        }
        synchronized (OltpTraverser.class) {
            if (executors != null) {
                return;
            }
            int workers = this.graph()
                              .option(CoreOptions.OLTP_CONCURRENT_THREADS);
            if (workers > 0) {
                executors = new Consumers.ExecutorPool(EXECUTOR_NAME, workers);
            }
        }
    }

    @Override
    public void close() {
        Transaction tx = this.graph().tx();
        if (tx.isOpen()) {
            tx.close();
        }
    }

    public static void destroy() {
        synchronized (OltpTraverser.class) {
            if (executors != null) {
                executors.destroy();
                executors = null;
            }
        }
    }

    protected long traversePairs(Iterator<Pair<Id, Id>> pairs,
                                 Consumer<Pair<Id, Id>> consumer) {
        return this.traverseByOne(pairs, consumer, "traverse-pairs");
    }

    protected long traverseIds(Iterator<Id> ids, Consumer<Id> consumer,
                               boolean concurrent) {
        if (concurrent) {
            return this.traverseIds(ids, consumer);
        } else {
            long count = 0L;
            while (ids.hasNext()) {
                count++;
                consumer.accept(ids.next());
            }
            return count;
        }
    }

    protected long traverseIds(Iterator<Id> ids, Consumer<Id> consumer) {
        return this.traverseByOne(ids, consumer, "traverse-ids");
    }

    protected <K> long traverseByOne(Iterator<K> iterator,
                                     Consumer<K> consumer,
                                     String taskName) {
        if (!iterator.hasNext()) {
            return 0L;
        }

        Consumers<K> consumers = new Consumers<>(executors.getExecutor(),
                                                 consumer, null);
        consumers.start(taskName);
        long total = 0L;
        try {
            while (iterator.hasNext()) {
                total++;
                K v = iterator.next();
                consumers.provide(v);
            }
        } catch (Consumers.StopExecution e) {
            // pass
        } catch (Throwable e) {
            throw Consumers.wrapException(e);
        } finally {
            try {
                consumers.await();
            } catch (Throwable e) {
                throw Consumers.wrapException(e);
            } finally {
                executors.returnExecutor(consumers.executor());
                CloseableIterator.closeIterator(iterator);
            }
        }
        return total;
    }

    protected void traverseIdsByBfs(Iterator<Id> vertices,
                                    Directions dir,
                                    Id label,
                                    long degree,
                                    long capacity,
                                    Consumer<EdgeId> consumer) {
        List<Id> labels = label == null ? Collections.emptyList() :
                          Collections.singletonList(label);
        OneStepEdgeIterConsumer edgeIterConsumer = new OneStepEdgeIterConsumer(consumer, capacity);

        EdgesIterator edgeIter = edgesOfVertices(vertices, dir, labels, degree);

        // parallel out-of-order execution
        this.traverseByBatch(edgeIter, edgeIterConsumer, "traverse-bfs-step", 1);
    }

    protected void traverseIdsByBfs(Iterator<Id> vertices,
                                    Steps steps,
                                    long capacity,
                                    Consumer<Edge> consumer) {
        StepsEdgeIterConsumer edgeIterConsumer =
                new StepsEdgeIterConsumer(consumer, capacity, steps);

        EdgesQueryIterator queryIterator = new EdgesQueryIterator(vertices,
                                                                  steps.direction(),
                                                                  steps.edgeLabels(),
                                                                  steps.degree());

        // get Iterator<Iterator<edges>> from Iterator<Query>
        EdgesIterator edgeIter = new EdgesIterator(queryIterator);

        // parallel out-of-order execution
        this.traverseByBatch(edgeIter, edgeIterConsumer, "traverse-bfs-steps", 1);
    }

    protected <K> long traverseByBatch(Iterator<Iterator<K>> sources,
                                       Consumer<Iterator<K>> consumer,
                                       String taskName, int concurrentWorkers) {
        if (!sources.hasNext()) {
            return 0L;
        }
        AtomicBoolean done = new AtomicBoolean(false);
        Consumers<Iterator<K>> consumers = null;
        try {
            consumers = buildConsumers(consumer, concurrentWorkers, done,
                                       executors.getExecutor());
            return startConsumers(sources, taskName, done, consumers);
        } finally {
            assert consumers != null;
            executors.returnExecutor(consumers.executor());
        }
    }

    private <K> long startConsumers(Iterator<Iterator<K>> sources,
                                    String taskName,
                                    AtomicBoolean done,
                                    Consumers<Iterator<K>> consumers) {
        long total = 0L;
        try {
            consumers.start(taskName);
            while (sources.hasNext() && !done.get()) {
                total++;
                Iterator<K> v = sources.next();
                consumers.provide(v);
            }
        } catch (Consumers.StopExecution e) {
            // pass
        } catch (Throwable e) {
            throw Consumers.wrapException(e);
        } finally {
            try {
                consumers.await();
            } catch (Throwable e) {
                throw Consumers.wrapException(e);
            } finally {
                CloseableIterator.closeIterator(sources);
            }
        }
        return total;
    }

    private <K> Consumers<Iterator<K>> buildConsumers(Consumer<Iterator<K>> consumer,
                                                      int queueSizePerWorker,
                                                      AtomicBoolean done,
                                                      ExecutorService executor) {
        return new Consumers<>(executor,
                               consumer,
                               null,
                               e -> done.set(true),
                               queueSizePerWorker);
    }

    protected Iterator<Vertex> filter(Iterator<Vertex> vertices,
                                      String key, Object value) {
        return new FilterIterator<>(vertices, vertex -> match(vertex, key, value));
    }

    protected boolean match(Element elem, String key, Object value) {
        // check property key exists
        this.graph().propertyKey(key);
        // return true if property value exists & equals to specified value
        Property<Object> p = elem.property(key);
        return p.isPresent() && Objects.equal(p.value(), value);
    }

    public static class ConcurrentMultiValuedMap<K, V>
            extends ConcurrentHashMap<K, List<V>> {

        private static final long serialVersionUID = -7249946839643493614L;

        public ConcurrentMultiValuedMap() {
            super();
        }

        public void add(K key, V value) {
            List<V> values = this.getValues(key);
            values.add(value);
        }

        public void addAll(K key, List<V> value) {
            List<V> values = this.getValues(key);
            values.addAll(value);
        }

        public List<V> getValues(K key) {
            List<V> values = this.get(key);
            if (values == null) {
                values = new CopyOnWriteArrayList<>();
                List<V> old = this.putIfAbsent(key, values);
                if (old != null) {
                    values = old;
                }
            }
            return values;
        }
    }

    public static class ConcurrentVerticesConsumer implements Consumer<EdgeId> {

        private final Id sourceV;
        private final Set<Id> excluded;
        private final Set<Id> neighbors;
        private final long limit;
        private final AtomicInteger count;

        public ConcurrentVerticesConsumer(Id sourceV, Set<Id> excluded, long limit,
                                          Set<Id> neighbors) {
            this.sourceV = sourceV;
            this.excluded = excluded;
            this.limit = limit;
            this.neighbors = neighbors;
            this.count = new AtomicInteger(0);
        }

        @Override
        public void accept(EdgeId edgeId) {
            if (this.limit != NO_LIMIT && count.get() >= this.limit) {
                throw new Consumers.StopExecution("reach limit");
            }

            Id targetV = edgeId.otherVertexId();
            if (this.sourceV.equals(targetV)) {
                return;
            }

            if (this.excluded != null && this.excluded.contains(targetV)) {
                return;
            }

            if (this.neighbors.add(targetV)) {
                if (this.limit != NO_LIMIT) {
                    this.count.getAndIncrement();
                }
            }
        }
    }

    public abstract class EdgesConsumer<T, E> implements Consumer<Iterator<T>> {

        private final Consumer<E> consumer;
        private final long capacity;

        public EdgesConsumer(Consumer<E> consumer, long capacity) {
            this.consumer = consumer;
            this.capacity = capacity;
        }

        protected abstract Iterator<E> prepare(Iterator<T> iter);

        @Override
        public void accept(Iterator<T> edgeIter) {
            Iterator<E> ids = prepare(edgeIter);
            long counter = 0;
            while (ids.hasNext()) {
                if (Thread.currentThread().isInterrupted()) {
                    LOG.warn("Consumer is Interrupted");
                    break;
                }
                counter++;
                this.consumer.accept(ids.next());
            }
            long total = edgeIterCounter.addAndGet(counter);
            // traverse by batch & improve performance
            if (this.capacity != NO_LIMIT && total >= this.capacity) {
                throw new Consumers.StopExecution("reach capacity");
            }
        }
    }

    public class OneStepEdgeIterConsumer extends EdgesConsumer<Edge, EdgeId> {

        public OneStepEdgeIterConsumer(Consumer<EdgeId> consumer, long capacity) {
            super(consumer, capacity);
        }

        @Override
        protected Iterator<EdgeId> prepare(Iterator<Edge> edgeIter) {
            return new MapperIterator<>(edgeIter, (e) -> ((HugeEdge) e).id());
        }
    }

    public class StepsEdgeIterConsumer extends EdgesConsumer<Edge, Edge> {

        private final Steps steps;

        public StepsEdgeIterConsumer(Consumer<Edge> consumer, long capacity,
                                     Steps steps) {
            super(consumer, capacity);
            this.steps = steps;
        }

        @Override
        protected Iterator<Edge> prepare(Iterator<Edge> edgeIter) {
            return edgesOfVertexStep(edgeIter, this.steps);
        }
    }
}
