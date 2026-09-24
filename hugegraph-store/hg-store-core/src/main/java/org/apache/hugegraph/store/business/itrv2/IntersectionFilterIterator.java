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

package org.apache.hugegraph.store.business.itrv2;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hugegraph.rocksdb.access.RocksDBSession;
import org.apache.hugegraph.rocksdb.access.ScanIterator;
import org.apache.hugegraph.store.business.itrv2.io.SortShuffleSerializer;
import org.apache.hugegraph.store.util.SortShuffle;

/**
 * Current usage(two or more iterator)
 * Issue: Iterator might have internal duplicates. How should we address this?
 */
public class IntersectionFilterIterator implements ScanIterator {

    private static final Integer MAX_SIZE = 100000;
    protected Map<Object, Integer> map;
    private final ScanIterator iterator;
    private final IntersectionWrapper wrapper;
    private boolean processed = false;
    private Iterator innerIterator;
    private SortShuffle<RocksDBSession.BackendColumn> sortShuffle;

    private int size = -1;

    @Deprecated
    public IntersectionFilterIterator(ScanIterator iterator, IntersectionWrapper<?> wrapper) {
        this.iterator = iterator;
        this.wrapper = wrapper;
        this.map = new HashMap<>();
    }

    /**
     * Compute intersection of multiple iterators
     * Issue: For multi-list iterators, cannot guarantee each element exists individually;
     * requires external deduplication. But ensures total count
     *
     * @param iterator iterator
     * @param wrapper  bitmap
     * @param size     the element count in the iterator by filtering
     */
    public IntersectionFilterIterator(ScanIterator iterator, IntersectionWrapper<?> wrapper,
                                      int size) {
        this(iterator, wrapper);
        this.size = size;
    }

    @Override
    public boolean hasNext() {
        if (!processed) {
            try {
                dedup();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            processed = true;
        }

        return innerIterator.hasNext();
    }

    // TODO: optimize serializer
    private void saveElements() throws IOException, ClassNotFoundException {
        for (var entry : this.map.entrySet()) {
            for (int i = 0; i < entry.getValue(); i++) {
                sortShuffle.append((RocksDBSession.BackendColumn) entry.getKey());
            }
        }

        this.map.clear();
    }

    /**
     * todo: If an iterator contains duplicates, there is currently no solution. The cost of
     *  deduplication is too high
     *
     * @throws IOException
     * @throws ClassNotFoundException
     */
    protected void dedup() throws IOException, ClassNotFoundException {
        while (this.iterator.hasNext()) {
            var object = this.iterator.next();
            if (wrapper.contains(object)) {
                this.map.put(object, map.getOrDefault(object, 0) + 1);
                if (this.map.size() >= MAX_SIZE) {
                    if (this.sortShuffle == null) {
                        this.sortShuffle =
                                new SortShuffle<RocksDBSession.BackendColumn>((o1, o2) -> Arrays.compare(o1.name, o2.name),
                                                  SortShuffleSerializer.ofBackendColumnSerializer());
                    }
                    saveElements();
                }
            }
        }

        // last batch
        if (this.sortShuffle != null) {
            saveElements();
            this.sortShuffle.finish();
        }

        if (this.sortShuffle == null) {
            // The map is not fully populated
            this.innerIterator =
                    new MapValueFilterIterator<>(this.map, x -> x == size || size == -1 && x > 1);
        } else {
            // need reading from a file
            var fileIterator =
                    this.sortShuffle.getIterator();
            this.innerIterator = new ReduceIterator<>(fileIterator,
                                                      (o1, o2) -> Arrays.compare(o1.name, o2.name),
                                                      this.size);
        }
    }

    @Override
    public boolean isValid() {
        if (this.processed) {
            return false;
        }
        return iterator.isValid();
    }

    @Override
    public <T> T next() {
        return (T) this.innerIterator.next();
    }

    @Override
    public void close() {
        this.iterator.close();
        this.map.clear();
    }

    @Override
    public long count() {
        return this.iterator.count();
    }

    @Override
    public byte[] position() {
        return this.iterator.position();
    }

    @Override
    public void seek(byte[] position) {
        this.iterator.seek(position);
    }

    /**
     * Keep only duplicate elements
     *
     * @param <E>
     */
    public static class ReduceIterator<E> implements Iterator<E> {

        private E prev = null;

        private E current = null;

        private E data = null;

        private int count = 0;

        private final Iterator<E> iterator;

        private final Comparator<E> comparator;

        private final int adjacent;

        public ReduceIterator(Iterator<E> iterator, Comparator<E> comparator, int adjacent) {
            this.count = 0;
            this.iterator = iterator;
            this.comparator = comparator;
            this.adjacent = adjacent;
        }

        /**
         * Consecutive duplicate elimination. When prev == current, record data. When not equal,
         * return previous data. Note: Final result may contain duplicates.
         */
        @Override
        public boolean hasNext() {
            while (iterator.hasNext()) {
                if (prev == null) {
                    prev = iterator.next();
                    continue;
                }

                current = iterator.next();
                if (comparator.compare(prev, current) == 0) {
                    data = current;
                    count += 1;
                } else {
                    // count starts from 0, so the size is count + 1
                    if (count > 0 && this.adjacent == -1 || count + 1 == this.adjacent) {
                        count = 0;
                        prev = current;
                        return true;
                    } else {
                        count = 0;
                        prev = current;
                    }
                }
            }

            // last result
            if (count > 0) {
                count = 0;
                return true;
            }

            return false;
        }

        @Override
        public E next() {
            return data;
        }
    }

}
