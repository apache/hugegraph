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
import java.io.Serializable;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

import org.apache.hugegraph.pd.common.HgAssert;
import org.apache.hugegraph.rocksdb.access.ScanIterator;
import org.apache.hugegraph.store.business.itrv2.io.SortShuffleSerializer;
import org.apache.hugegraph.store.util.SortShuffle;

public class UnionFilterIterator<T extends Serializable> implements ScanIterator {

    private static final Integer MAP_SIZE = 10000;

    private final ScanIterator iterator;

    private final IntersectionWrapper<T> wrapper;
    private final Comparator<T> comparator;
    protected Map<T, Integer> map;
    private Iterator<T> innerIterator;
    private SortShuffle<T> sortShuffle;
    private final SortShuffleSerializer<T> serializer;
    private Object current;
    private boolean isProcessed = false;

    public UnionFilterIterator(ScanIterator iterator, IntersectionWrapper<T> wrapper,
                               Comparator<T> comparator, SortShuffleSerializer<T> serializer) {
        HgAssert.isNotNull(wrapper, "wrapper is null");
        this.iterator = iterator;
        this.wrapper = wrapper;
        this.map = new HashMap<>();
        this.comparator = comparator;
        this.serializer = serializer;
    }

    /**
     * save current element to ortShuffle
     */
    private void saveElement() {
        for (var entry : this.map.entrySet()) {
            try {
                sortShuffle.append(entry.getKey());
                if (entry.getValue() > 1) {
                    sortShuffle.append(entry.getKey());
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        this.map.clear();
    }

    @Override
    public boolean hasNext() {
        while (this.iterator.hasNext()) {
            var obj = (T) this.iterator.next();
            // batch get or index lookup may generate null
            if (obj == null) {
                continue;
            }

            // Definitely unique
            if (!wrapper.contains(obj)) {
                this.current = obj;
                return true;
            } else {
                this.map.put(obj, map.getOrDefault(obj, 0) + 1);
                if (this.map.size() > MAP_SIZE) {
                    if (this.sortShuffle == null) {
                        sortShuffle = new SortShuffle<>(this.comparator, this.serializer);
                    }
                    saveElement();
                }
            }
        }

        if (!isProcessed) {
            if (sortShuffle != null) {
                try {
                    saveElement();
                    sortShuffle.finish();

                    var fileIterator = sortShuffle.getIterator();
                    this.innerIterator = new NoRepeatValueIterator<>(fileIterator, this.comparator);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            } else {
                this.innerIterator = new MapValueFilterIterator<>(this.map, x -> x > 0);
            }

            isProcessed = true;
        }

        var ret = this.innerIterator.hasNext();
        if (ret) {
            this.current = this.innerIterator.next();
            return true;
        }

        if (sortShuffle != null) {
            sortShuffle.close();
            sortShuffle = null;
        }

        return false;
    }

    @Override
    public boolean isValid() {
        // todo: check logic
        return this.iterator.isValid() || hasNext();
    }

    @Override
    public <X> X next() {
        if (current == null) {
            throw new NoSuchElementException();
        }

        return (X) current;
    }

    @Override
    public void close() {
        this.iterator.close();
        if (this.sortShuffle != null) {
            this.sortShuffle.close();
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

    private static class NoRepeatValueIterator<E> implements Iterator<E> {

        private final Iterator<E> iterator;
        private final Comparator<E> comparator;
        private E prev = null;
        private E data = null;
        private int count = 0;

        public NoRepeatValueIterator(Iterator<E> iterator, Comparator<E> comparator) {
            this.count = 0;
            this.iterator = iterator;
            this.comparator = comparator;
        }

        @Override
        public boolean hasNext() {
            while (iterator.hasNext()) {
                var n = iterator.next();
                if (prev == null) {
                    // prev = iterator.next();
                    prev = n;
                    continue;
                }

                // E current = iterator.next();
                E current = n;

                if (comparator.compare(prev, current) == 0) {
                    count += 1;
                } else {
                    if (count > 0) {
                        // --- pre is dup
                        prev = current;
                    } else {
                        data = prev;
                        prev = current;
                        return true;
                    }
                    count = 0;
                }
            }

            // last result
            if (count == 0) {
                data = prev;
                count = 1;
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
