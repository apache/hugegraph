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

package org.apache.hugegraph.store.client.query;

import java.util.Comparator;
import java.util.List;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.hugegraph.store.HgKvIterator;
import org.apache.hugegraph.store.query.Tuple2;

public class StreamSortedIterator<E> implements HgKvIterator<E> {

    private final List<HgKvIterator<E>> iterators;
    private final PriorityBlockingQueue<Tuple2<E, Integer>> pq;

    private final AtomicBoolean initialized = new AtomicBoolean(false);

    public StreamSortedIterator(List<HgKvIterator<E>> iterators, Comparator<E> comparator) {
        this.iterators = iterators;
        // Note: PriorityBlockingQueue is used instead of PriorityQueue to ensure thread safety during parallel initialization.
        this.pq = new PriorityBlockingQueue<>(iterators.size(),
                                              (o1, o2) -> comparator.compare(o1.getV1(),
                                                                             o2.getV1()));
    }

    /**
     * Initialize a priority queue by taking the first element from each iterator
     * Parallel processing: dispatch requests to each iterator
     */
    private void initializeQueue() {
        if (this.initialized.get()) {
            return;
        }
        AtomicInteger index = new AtomicInteger(0);
        this.iterators.stream()
                      .map(itr -> new Tuple2<>(itr, index.getAndIncrement()))
                      .collect(Collectors.toList())
                      .parallelStream()
                      .forEach(tuple -> {
                          var itr = tuple.getV1();
                          if (itr.hasNext()) {
                              pq.offer(new Tuple2<>(itr.next(), tuple.getV2()));
                          }
                      });

        this.initialized.set(true);
    }

    private HgKvIterator<E> getTopIterator() {
        var entry = pq.peek();
        if (entry != null) {
            return iterators.get(entry.getV2());
        }
        return null;
    }

    @Override
    public byte[] key() {
        initializeQueue();
        var itr = getTopIterator();
        if (itr != null) {
            return itr.key();
        }
        return null;
    }

    @Override
    public byte[] value() {
        initializeQueue();
        var itr = getTopIterator();
        if (itr != null) {
            return itr.value();
        }
        return null;
    }

    @Override
    public void close() {
        iterators.forEach(HgKvIterator::close);
    }

    @Override
    public byte[] position() {
        initializeQueue();
        var itr = getTopIterator();
        if (itr != null) {
            return itr.position();
        }
        return null;
    }

    @Override
    public void seek(byte[] position) {
        initializeQueue();
        var itr = getTopIterator();
        if (itr != null) {
            itr.seek(position);
        }
    }

    @Override
    public boolean hasNext() {
        initializeQueue();
        return !this.pq.isEmpty();
    }

    @Override
    public E next() {
        var pair = this.pq.poll();
        assert pair != null;
        if (iterators.get(pair.getV2()).hasNext()) {
            this.pq.offer(Tuple2.of(iterators.get(pair.getV2()).next(), pair.getV2()));
        }
        return pair.getV1();
    }
}
