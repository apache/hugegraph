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
import java.util.NoSuchElementException;

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
    private boolean closed;
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
        if (this.closed) {
            return false;
        }
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
        if (this.closed || this.processed) {
            return false;
        }
        return iterator.isValid();
    }

    @Override
    public <T> T next() {
        if (this.closed) {
            throw new NoSuchElementException();
        }
        return (T) this.innerIterator.next();
    }

    @Override
    public void close() {
        this.closed = true;
        try (SortShuffle<?> shuffle = this.sortShuffle) {
            this.iterator.close();
        } finally {
            this.sortShuffle = null;
            this.innerIterator = null;
            this.map.clear();
        }
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

        private boolean ready;

        private boolean exhausted;

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
         * Return the last element of each matching group. A group must contain exactly
         * {@code adjacent} elements, or at least two when {@code adjacent == -1}.
         */
        @Override
        public boolean hasNext() {
            if (this.ready) {
                return true;
            }
            if (this.exhausted) {
                return false;
            }
            while (iterator.hasNext()) {
                E next = iterator.next();
                if (prev == null) {
                    prev = next;
                    current = next;
                    continue;
                }

                if (comparator.compare(prev, next) == 0) {
                    current = next;
                    count += 1;
                    continue;
                }

                boolean matches = matchesGroup();
                if (matches) {
                    data = current;
                }
                count = 0;
                prev = next;
                current = next;
                if (matches) {
                    this.ready = true;
                    return true;
                }
            }

            this.exhausted = true;
            if (prev != null && matchesGroup()) {
                data = current;
                this.ready = true;
                return true;
            }

            return false;
        }

        @Override
        public E next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            E result = this.data;
            this.data = null;
            this.ready = false;
            return result;
        }

        private boolean matchesGroup() {
            // count excludes the first element of the group.
            return this.adjacent == -1 ? this.count > 0 : this.count + 1 == this.adjacent;
        }
    }

}
